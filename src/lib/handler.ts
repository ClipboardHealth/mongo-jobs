import type { BackgroundJobType } from "./job";

export interface HandlerInterface<T> {
  name: string;
  maxAttempts?: number;
  /** Return a retry delay in milliseconds, or undefined for exponential backoff. */
  // oxlint-disable-next-line typescript/method-signature-style -- method needed for class implementors
  getRetryDelayMS?(options: { error: unknown; attemptsCount: number }): number | undefined;
  // oxlint-disable-next-line typescript/method-signature-style -- method needed for class implementors
  perform(data: T, job?: BackgroundJobType<T>): Promise<void>;
}
