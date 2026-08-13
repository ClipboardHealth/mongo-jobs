import type { BackgroundJobType } from "../../job";

/**
 * How a worker picks which of its actionable queues to take the next job from.
 *
 * - `random` selects uniformly at random. Dequeue *attempts* are spread evenly, but worker
 *   concurrency is not: a slot stays occupied for the length of the job that took it, so a
 *   backlogged queue of long-running jobs accumulates slots and can starve faster queues.
 * - `leastInFlight` selects uniformly among the actionable queues with the fewest jobs currently
 *   running on this worker, bounding any one queue to its fair share of concurrency. In exchange,
 *   a queue mixing slow and fast jobs no longer earns extra concurrency for its slow ones.
 */
export type QueueSelectionStrategy = "random" | "leastInFlight";

export const DEFAULT_QUEUE_SELECTION_STRATEGY: QueueSelectionStrategy = "random";

export interface QueueConsumerStartOptions {
  useChangeStream: boolean;
  refreshQueuesIntervalMS: number;
}

export interface QueueConsumer extends EventTarget {
  acquireNextJob: () => Promise<BackgroundJobType<unknown> | undefined>;
  release: (job: BackgroundJobType<unknown>) => void;
  start: (options: QueueConsumerStartOptions) => Promise<void>;
  stop: () => Promise<void>;
  getConsumedQueues: () => string[];
}
