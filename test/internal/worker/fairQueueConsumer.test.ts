import { EventEmitter } from "node:events";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { JobsRepository } from "../../../src/lib/internal/jobsRepository";
import { FairQueueConsumer } from "../../../src/lib/internal/worker/fairQueueConsumer";
import { TestLogger } from "../../support/testLogger";

const BASE_DELAY_MS = 1000;
const MAX_DELAY_MS = 30_000;
const MAX_RETRIES = 10;
const QUEUES = ["default"];

/** Minimal ChangeStream stand-in: an EventEmitter with a stubbed `close()`. */
class FakeChangeStream extends EventEmitter {
  public close = vi.fn(async () => {});
}

describe(FairQueueConsumer, () => {
  let logger: TestLogger;
  let streams: FakeChangeStream[];
  let watchUpserts: ReturnType<typeof vi.fn>;
  let jobsRepository: JobsRepository;
  let consumer: FairQueueConsumer;

  function latestStream(): FakeChangeStream {
    return streams.at(-1)!;
  }

  beforeEach(() => {
    vi.useFakeTimers();
    logger = new TestLogger();
    streams = [];
    watchUpserts = vi.fn(() => {
      const stream = new FakeChangeStream();
      streams.push(stream);
      return stream;
    });
    jobsRepository = { watchUpserts } as unknown as JobsRepository;
    consumer = new FairQueueConsumer(QUEUES, jobsRepository, logger);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("does not throw and recreates the stream when the change stream emits an error", () => {
    consumer.startListeningForQueueChanges();

    expect(() => latestStream().emit("error", new Error("non-resumable"))).not.toThrow();
    expect(logger.errorLogs).toHaveLength(1);
    expect(logger.errorLogs[0]!.message).toContain("non-resumable");
    expect(streams[0]!.close).toHaveBeenCalledOnce();

    expect(watchUpserts).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(BASE_DELAY_MS);
    expect(watchUpserts).toHaveBeenCalledTimes(2);
  });

  it("uses exponential backoff between consecutive failures", () => {
    consumer.startListeningForQueueChanges();

    latestStream().emit("error", new Error("fail 1"));
    vi.advanceTimersByTime(BASE_DELAY_MS - 1);
    expect(watchUpserts).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(1);
    expect(watchUpserts).toHaveBeenCalledTimes(2);

    latestStream().emit("error", new Error("fail 2"));
    vi.advanceTimersByTime(BASE_DELAY_MS * 2 - 1);
    expect(watchUpserts).toHaveBeenCalledTimes(2);
    vi.advanceTimersByTime(1);
    expect(watchUpserts).toHaveBeenCalledTimes(3);
  });

  it("gives up and logs after the maximum number of retries, then stops recreating", () => {
    consumer.startListeningForQueueChanges();

    for (let attempt = 0; attempt < MAX_RETRIES; attempt += 1) {
      latestStream().emit("error", new Error(`fail ${attempt}`));
      vi.advanceTimersByTime(MAX_DELAY_MS);
    }

    expect(watchUpserts).toHaveBeenCalledTimes(MAX_RETRIES + 1);

    latestStream().emit("error", new Error("final"));
    vi.advanceTimersByTime(MAX_DELAY_MS);

    expect(watchUpserts).toHaveBeenCalledTimes(MAX_RETRIES + 1);
    expect(logger.errorLogs.some((entry) => entry.message.includes("giving up"))).toBe(true);
  });

  it("resets the retry budget once the recreated stream delivers a change", () => {
    consumer.startListeningForQueueChanges();

    latestStream().emit("error", new Error("fail 1"));
    vi.advanceTimersByTime(BASE_DELAY_MS);
    expect(watchUpserts).toHaveBeenCalledTimes(2);

    latestStream().emit("change", {
      fullDocument: { queue: "default", nextRunAt: new Date() },
    });

    latestStream().emit("error", new Error("fail 2"));
    vi.advanceTimersByTime(BASE_DELAY_MS);
    expect(watchUpserts).toHaveBeenCalledTimes(3);
  });

  it("cancels a pending stream recreation when the consumer is stopped", async () => {
    consumer.startListeningForQueueChanges();

    latestStream().emit("error", new Error("non-resumable"));

    await consumer.stop();
    vi.advanceTimersByTime(MAX_DELAY_MS);

    expect(watchUpserts).toHaveBeenCalledTimes(1);
  });

  it("still dispatches a newJob event for an actionable change", () => {
    const nextRunAt = new Date("2020-01-01T00:00:00.000Z");
    consumer.startListeningForQueueChanges();

    const newJobListener = vi.fn();
    consumer.addEventListener("newJob", newJobListener);

    latestStream().emit("change", { fullDocument: { queue: "default", nextRunAt } });

    expect(newJobListener).toHaveBeenCalledOnce();
  });

  it("ignores changes for queues this consumer does not consume", () => {
    consumer.startListeningForQueueChanges();

    const newJobListener = vi.fn();
    consumer.addEventListener("newJob", newJobListener);

    latestStream().emit("change", {
      fullDocument: { queue: "other", nextRunAt: new Date() },
    });

    expect(newJobListener).not.toHaveBeenCalled();
  });
});
