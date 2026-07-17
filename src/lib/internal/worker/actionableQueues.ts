class RandomQueueSet {
  private readonly queues: string[] = [];
  private readonly indexByQueue = new Map<string, number>();

  public get size(): number {
    return this.queues.length;
  }

  public add(queue: string): void {
    if (this.indexByQueue.has(queue)) {
      return;
    }

    this.indexByQueue.set(queue, this.queues.length);
    this.queues.push(queue);
  }

  public delete(queue: string): void {
    const index = this.indexByQueue.get(queue);
    if (index === undefined) {
      return;
    }

    const lastQueue = this.queues.pop();
    this.indexByQueue.delete(queue);

    if (index < this.queues.length && lastQueue !== undefined) {
      this.queues[index] = lastQueue;
      this.indexByQueue.set(lastQueue, index);
    }
  }

  public getRandom(): string | undefined {
    if (this.queues.length === 0) {
      return undefined;
    }

    return this.queues[Math.floor(Math.random() * this.queues.length)];
  }
}

export class ActionableQueues {
  private readonly actionable = new Set<string>();
  private readonly idle = new RandomQueueSet();
  private readonly inFlightByQueue = new Map<string, number>();

  public add(queue: string) {
    if (this.actionable.has(queue)) {
      return;
    }

    this.actionable.add(queue);
    if (this.inFlight(queue) === 0) {
      this.idle.add(queue);
    }
  }

  public remove(queue: string) {
    if (!this.actionable.has(queue)) {
      return;
    }

    this.actionable.delete(queue);
    this.idle.delete(queue);
  }

  public acquire(queue: string): void {
    const newInFlight = this.inFlight(queue) + 1;
    this.inFlightByQueue.set(queue, newInFlight);
    this.idle.delete(queue);
  }

  public release(queue: string): void {
    const oldInFlight = this.inFlight(queue);
    if (oldInFlight === 0) {
      return;
    }

    const newInFlight = oldInFlight - 1;
    if (newInFlight === 0) {
      this.inFlightByQueue.delete(queue);
      if (this.actionable.has(queue)) {
        this.idle.add(queue);
      }
    } else {
      this.inFlightByQueue.set(queue, newInFlight);
    }
  }

  /**
   * Returns a random actionable queue with the fewest jobs in flight, or `undefined` when none are
   * actionable.
   *
   * When any actionable queue is idle it wins outright, so the common case is O(1). Otherwise every
   * actionable queue is in flight, which bounds their number by the worker's concurrency, so the
   * linear scan over the remaining queues is cheap.
   */
  public getLeastInFlight(): string | undefined {
    const idleQueue = this.idle.getRandom();
    if (idleQueue !== undefined) {
      return idleQueue;
    }

    let leastInFlight = Number.POSITIVE_INFINITY;
    const leastLoaded: string[] = [];
    for (const queue of this.actionable) {
      const inFlight = this.inFlight(queue);
      if (inFlight < leastInFlight) {
        leastInFlight = inFlight;
        leastLoaded.length = 0;
        leastLoaded.push(queue);
      } else if (inFlight === leastInFlight) {
        leastLoaded.push(queue);
      }
    }

    if (leastLoaded.length === 0) {
      return undefined;
    }

    return leastLoaded[Math.floor(Math.random() * leastLoaded.length)];
  }

  private inFlight(queue: string): number {
    return this.inFlightByQueue.get(queue) ?? 0;
  }
}
