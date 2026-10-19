import { setTimeout } from "node:timers/promises";

import { JobRun } from "./jobRun";

interface WaitForJobRunCountOptions {
  expectedCount: number;
  timeoutMilliseconds?: number;
}

export async function waitForJobRunCount(options: WaitForJobRunCountOptions): Promise<void> {
  const { expectedCount, timeoutMilliseconds = 15_000 } = options;
  const timeoutAt = Date.now() + timeoutMilliseconds;

  for (;;) {
    const jobRunCount = await JobRun.countDocuments();

    if (jobRunCount === expectedCount) {
      return;
    }

    if (Date.now() >= timeoutAt) {
      throw new Error(`Timed out waiting for ${expectedCount} job runs, found ${jobRunCount}`);
    }

    await setTimeout(10);
  }
}
