import type { BackgroundJobType } from "../../job";

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
