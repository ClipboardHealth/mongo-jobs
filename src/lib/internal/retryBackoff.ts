const MILLIS_IN_SECOND = 1000;

/**
 * Compute the delay before a failed job is retried.
 *
 * Exponential backoff (`2 ** attemptsCount` seconds) with two safeguards:
 * - **Cap**: the delay is clamped to `maxBackoffMS`. This bounds the wait for
 *   high `maxAttempts` values and guards against `2 ** attemptsCount` becoming
 *   `Infinity` for very large attempt counts.
 * - **Equal jitter**: half of the delay is a fixed floor and half is random, so
 *   that many jobs failing at the same time (e.g. during a shared-dependency
 *   outage) don't all retry in lockstep and stampede the recovering dependency.
 *
 * @param attemptsCount - the attempt number that just failed (1-based)
 * @param maxBackoffMS - upper bound for the delay, in milliseconds
 * @param random - injectable RNG (defaults to `Math.random`) for deterministic tests
 * @returns the delay before the next attempt, in milliseconds
 */
export function computeRetryBackoffMS(
  attemptsCount: number,
  maxBackoffMS: number,
  random: () => number = Math.random,
): number {
  const exponentialMS = 2 ** attemptsCount * MILLIS_IN_SECOND;
  const cappedMS = Math.min(exponentialMS, maxBackoffMS);
  return cappedMS / 2 + random() * (cappedMS / 2);
}
