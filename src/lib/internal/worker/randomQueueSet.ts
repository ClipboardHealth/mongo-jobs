export interface RandomQueueSetOptions {
  /** Source of randomness in `[0, 1)`. */
  random: () => number;
}

/**
 * A set of queue names supporting O(1) add, delete, and uniformly random selection. Deletion swaps
 * in the last element, so iteration order is not stable across deletes.
 */
export class RandomQueueSet {
  private readonly queues: string[] = [];
  private readonly indexByQueue = new Map<string, number>();
  private readonly random: () => number;

  public constructor(options: RandomQueueSetOptions) {
    const { random } = options;
    this.random = random;
  }

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

  public has(queue: string): boolean {
    return this.indexByQueue.has(queue);
  }

  public getRandom(): string | undefined {
    if (this.queues.length === 0) {
      return undefined;
    }

    // Clamped so a `random` that returns exactly 1 selects the last queue rather than nothing.
    const lastIndex = this.queues.length - 1;
    return this.queues[Math.min(Math.floor(this.random() * this.queues.length), lastIndex)];
  }
}
