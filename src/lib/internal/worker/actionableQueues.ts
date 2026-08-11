import { RandomQueueSet } from "./randomQueueSet";

export interface ActionableQueuesOptions {
  /** Source of randomness in `[0, 1)`. Defaults to `Math.random`. */
  random?: (() => number) | undefined;
}

interface RebucketOptions {
  queue: string;
  from: number;
  to: number;
}

interface BucketOptions {
  queue: string;
  inFlight: number;
}

export class ActionableQueues {
  private readonly random: () => number;
  private readonly actionable: RandomQueueSet;

  /**
   * Actionable queues bucketed by how many of their jobs are in flight. Keys are bounded by the
   * worker's concurrency, so both the number of buckets and the cost of finding the smallest key
   * stay small.
   */
  private readonly queuesByInFlight = new Map<number, RandomQueueSet>();

  /** Tracked for every queue, actionable or not, so counts survive a queue leaving the set. */
  private readonly inFlightByQueue = new Map<string, number>();

  private leastInFlight = 0;

  public constructor(options: ActionableQueuesOptions = {}) {
    const { random } = options;
    this.random = random ?? Math.random;
    this.actionable = new RandomQueueSet({ random: this.random });
  }

  public add(queue: string): void {
    if (this.actionable.has(queue)) {
      return;
    }

    this.actionable.add(queue);
    this.addToBucket({ queue, inFlight: this.inFlight(queue) });
  }

  public remove(queue: string): void {
    if (!this.actionable.has(queue)) {
      return;
    }

    this.actionable.delete(queue);
    this.removeFromBucket({ queue, inFlight: this.inFlight(queue) });
  }

  public acquire(queue: string): void {
    const oldInFlight = this.inFlight(queue);
    const newInFlight = oldInFlight + 1;
    this.inFlightByQueue.set(queue, newInFlight);
    this.rebucket({ queue, from: oldInFlight, to: newInFlight });
  }

  public release(queue: string): void {
    const oldInFlight = this.inFlight(queue);
    if (oldInFlight === 0) {
      return;
    }

    const newInFlight = oldInFlight - 1;
    if (newInFlight === 0) {
      this.inFlightByQueue.delete(queue);
    } else {
      this.inFlightByQueue.set(queue, newInFlight);
    }

    this.rebucket({ queue, from: oldInFlight, to: newInFlight });
  }

  /**
   * Returns a uniformly random actionable queue, or `undefined` when none are actionable. Ignores
   * in-flight counts, so a queue holding long-running jobs competes for slots on equal terms with
   * one holding short jobs.
   */
  public getRandom(): string | undefined {
    return this.actionable.getRandom();
  }

  /**
   * Returns a random actionable queue with the fewest jobs in flight, or `undefined` when none are
   * actionable.
   *
   * A queue whose job just finished sits in the smallest non-empty bucket, so it is a candidate for
   * every subsequent selection until it wins one.
   */
  public getLeastInFlight(): string | undefined {
    return this.queuesByInFlight.get(this.leastInFlight)?.getRandom();
  }

  private rebucket(options: RebucketOptions): void {
    const { queue, from, to } = options;
    if (!this.actionable.has(queue)) {
      return;
    }

    this.removeFromBucket({ queue, inFlight: from });
    this.addToBucket({ queue, inFlight: to });
  }

  private addToBucket(options: BucketOptions): void {
    const { queue, inFlight } = options;
    let bucket = this.queuesByInFlight.get(inFlight);
    if (bucket === undefined) {
      bucket = new RandomQueueSet({ random: this.random });
      this.queuesByInFlight.set(inFlight, bucket);
    }

    bucket.add(queue);

    // The sole bucket is the smallest one even when its key is above the previous minimum, which
    // happens when a queue rejoins the set with jobs still in flight.
    if (this.queuesByInFlight.size === 1 || inFlight < this.leastInFlight) {
      this.leastInFlight = inFlight;
    }
  }

  private removeFromBucket(options: BucketOptions): void {
    const { queue, inFlight } = options;
    const bucket = this.queuesByInFlight.get(inFlight);
    if (bucket === undefined) {
      return;
    }

    bucket.delete(queue);
    if (bucket.size > 0) {
      return;
    }

    this.queuesByInFlight.delete(inFlight);
    if (inFlight === this.leastInFlight) {
      this.recalculateLeastInFlight();
    }
  }

  private recalculateLeastInFlight(): void {
    const inFlightCounts = [...this.queuesByInFlight.keys()];
    this.leastInFlight = inFlightCounts.length === 0 ? 0 : Math.min(...inFlightCounts);
  }

  private inFlight(queue: string): number {
    return this.inFlightByQueue.get(queue) ?? 0;
  }
}
