import type { ChangeStream } from "mongodb";

import type { BackgroundJobType } from "../../job";
import { errorMessage } from "../errorMessage";
import type { JobsRepository, UpsertChangeStreamEvent } from "../jobsRepository";
import type { Logger } from "../logger";
import { ActionableQueues } from "./actionableQueues";
import { FutureQueues } from "./futureQueues";
import type { QueueConsumer, QueueConsumerStartOptions } from "./queueConsumer";

const CHANGE_STREAM_RETRY_BASE_DELAY_MS = 1000;
const CHANGE_STREAM_RETRY_MAX_DELAY_MS = 30_000;
const CHANGE_STREAM_MAX_RETRIES = 10;

export class FairQueueConsumer extends EventTarget implements QueueConsumer {
  private readonly jobsRepository: JobsRepository;
  private readonly logger: Logger | undefined;
  private readonly consumedQueues: string[];
  private readonly consumedQueuesSet: Set<string>;
  private readonly actionableQueues = new ActionableQueues();
  private readonly futureQueues = new FutureQueues();
  private jobsChangeStream: ChangeStream<BackgroundJobType<unknown>> | undefined;
  private refreshQueuesInterval?: NodeJS.Timeout;
  private recreateChangeStreamTimeout: NodeJS.Timeout | undefined;
  private changeStreamRetryCount = 0;
  private stopped = false;

  public constructor(queues: string[], jobsRepository: JobsRepository, logger?: Logger) {
    super();
    this.consumedQueues = queues;
    this.consumedQueuesSet = new Set(queues);
    this.jobsRepository = jobsRepository;
    this.logger = logger;
  }

  public async start({ useChangeStream, refreshQueuesIntervalMS }: QueueConsumerStartOptions) {
    if (this.consumedQueues.length === 0) {
      return;
    }

    this.stopped = false;
    this.changeStreamRetryCount = 0;

    await this.startQueuesRefresh(refreshQueuesIntervalMS);

    if (useChangeStream && !this.jobsChangeStream) {
      this.startListeningForQueueChanges();
    }
  }

  public async startQueuesRefresh(interval: number) {
    await this.refreshActionableQueuesFromDB();
    this.refreshQueuesInterval = setInterval(() => {
      this.refreshActionableQueuesFromDBSafely();
    }, interval);
  }

  public startListeningForQueueChanges() {
    const stream = this.jobsRepository.watchUpserts(this.consumedQueues);
    this.jobsChangeStream = stream;

    stream.on("change", (event: unknown) => {
      this.resetStreamRetryOnSuccess();

      const typedEvent = event as UpsertChangeStreamEvent;
      const queue = typedEvent.fullDocument?.queue;
      const nextRunAt = typedEvent.fullDocument?.nextRunAt;
      const lockedAt = typedEvent.fullDocument?.lockedAt;

      if (
        queue === undefined ||
        nextRunAt === undefined ||
        !this.consumedQueuesSet.has(queue) ||
        lockedAt !== undefined
      ) {
        return;
      }

      this.futureQueues.setActionableAt(queue, nextRunAt);
      this.dispatchNewJobEvent();
    });

    stream.on("error", (error: unknown) => {
      this.handleChangeStreamError(stream, error);
    });
  }

  public async stop() {
    this.stopped = true;

    if (this.refreshQueuesInterval) {
      clearInterval(this.refreshQueuesInterval);
    }

    if (this.recreateChangeStreamTimeout) {
      clearTimeout(this.recreateChangeStreamTimeout);
      this.recreateChangeStreamTimeout = undefined;
    }

    if (this.jobsChangeStream) {
      const stream = this.jobsChangeStream;
      this.jobsChangeStream = undefined;
      stream.removeAllListeners();
      try {
        await stream.close();
      } catch {
        // Might already be closed
      }
    }
  }

  private resetStreamRetryOnSuccess() {
    this.changeStreamRetryCount = 0;
  }

  private calculateRetryDelayWithBackoff() {
    return Math.min(
      CHANGE_STREAM_RETRY_BASE_DELAY_MS * 2 ** this.changeStreamRetryCount,
      CHANGE_STREAM_RETRY_MAX_DELAY_MS,
    );
  }

  private handleChangeStreamError(
    stream: ChangeStream<BackgroundJobType<unknown>>,
    error: unknown,
  ) {
    this.logger?.error(`FairQueueConsumer change stream error: ${errorMessage(error)}`);

    if (this.stopped || this.jobsChangeStream !== stream) {
      return;
    }

    this.jobsChangeStream = undefined;
    stream.removeAllListeners();
    void stream.close().catch(() => {
      // Might already be closed as a result of the error.
    });

    this.scheduleChangeStreamRecreation();
  }

  private scheduleChangeStreamRecreation() {
    if (this.recreateChangeStreamTimeout) {
      return;
    }

    if (this.changeStreamRetryCount >= CHANGE_STREAM_MAX_RETRIES) {
      this.logger?.error(
        `FairQueueConsumer giving up on change stream after ${CHANGE_STREAM_MAX_RETRIES} ` +
          "failed attempts; falling back to interval polling until next restart",
      );
      return;
    }

    const delay = this.calculateRetryDelayWithBackoff();
    this.changeStreamRetryCount += 1;

    this.recreateChangeStreamTimeout = setTimeout(() => {
      this.recreateChangeStreamTimeout = undefined;

      if (this.stopped || this.jobsChangeStream) {
        return;
      }

      try {
        this.startListeningForQueueChanges();
      } catch (error) {
        this.logger?.error(
          `FairQueueConsumer failed to recreate change stream: ${errorMessage(error)}`,
        );
        this.scheduleChangeStreamRecreation();
      }
    }, delay);
  }

  public async refreshActionableQueuesFromDB() {
    const queues = await this.jobsRepository.fetchQueuesWithJobs(this.consumedQueues);
    for (const queue of queues) {
      /**
       * Not adding "undefined" into actionable queues - those are jobs that were failed and were
       * taken off of their respective queues
       **/
      if (queue) {
        this.actionableQueues.add(queue);
      }
    }
  }

  public async acquireNextJob(): Promise<BackgroundJobType<unknown> | undefined> {
    this.promoteQueues();
    let job;

    while (!job) {
      const queue = this.actionableQueues.getRandom();

      if (queue === undefined) {
        return undefined;
      }

      // eslint-disable-next-line no-await-in-loop
      job = await this.jobsRepository.fetchAndLockNextJob([queue]);

      if (!job) {
        // eslint-disable-next-line no-await-in-loop
        await this.removeQueueFromActionable(queue);
      }
    }

    return job;
  }

  public getConsumedQueues(): string[] {
    return [...this.consumedQueues];
  }

  private refreshActionableQueuesFromDBSafely(): void {
    void (async () => {
      try {
        await this.refreshActionableQueuesFromDB();
      } catch {
        // Ignore rejections from detached queue refreshes.
      }
    })();
  }

  private promoteQueues() {
    const queuesToPromote = this.futureQueues.acquireCurrentlyActionable();
    for (const queue of queuesToPromote) {
      this.actionableQueues.add(queue);
    }
  }

  private async removeQueueFromActionable(queue: string) {
    this.actionableQueues.remove(queue);
    this.refreshQueueFutureActionableAtSafely(queue);
  }

  private async refreshQueueFutureActionableAt(queue: string) {
    const job = await this.jobsRepository.fetchNextJob(queue);

    if (!job) {
      return;
    }

    const { nextRunAt } = job;

    if (!nextRunAt) {
      return;
    }

    this.futureQueues.setActionableAt(queue, nextRunAt);
    if (nextRunAt < new Date()) {
      this.dispatchNewJobEvent();
    }
  }

  private refreshQueueFutureActionableAtSafely(queue: string): void {
    void (async () => {
      try {
        await this.refreshQueueFutureActionableAt(queue);
      } catch {
        // Ignore rejections from detached queue future refreshes.
      }
    })();
  }

  private dispatchNewJobEvent() {
    this.dispatchEvent(new Event("newJob"));
  }
}
