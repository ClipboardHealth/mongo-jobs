import type { BackgroundJobs, HandlerInterface } from "../src";
import { ensureExistence } from "./support/ensureExistence";
import { createTestContext, type TestContext } from "./support/testContext";
import { TestLogger } from "./support/testLogger";
import { waitForJobCount } from "./support/waitForJobCount";

class RetryDelayError extends Error {
  public retryDelayMS = 1500;
}

describe("Handler retry delay", () => {
  let testContext: TestContext;
  let backgroundJobs: BackgroundJobs;
  let logger: TestLogger;

  beforeEach(async () => {
    logger = new TestLogger();
    testContext = await createTestContext({ logger });
    ({ backgroundJobs } = testContext);
  });

  afterEach(async () => {
    await testContext.tearDown();
  });

  it("retries the same job after its custom delay and retains the attempt limit", async () => {
    const error = new RetryDelayError("Rate limited");
    const runs: Array<{ id: string; startedAt: number }> = [];
    const getRetryDelayMS = vi.fn((options: { error: unknown; attemptsCount: number }): number => {
      return options.attemptsCount === 1 && options.error instanceof RetryDelayError
        ? options.error.retryDelayMS
        : 0;
    });
    const handler: HandlerInterface<object> = {
      name: "CustomRetryDelay",
      maxAttempts: 3,
      getRetryDelayMS,
      perform: async (_data, job) => {
        runs.push({ id: ensureExistence(job)._id.toString(), startedAt: Date.now() });
        throw error;
      },
    };
    backgroundJobs.register(handler, "default");
    const job = ensureExistence(await backgroundJobs.enqueue(handler, {}));

    await backgroundJobs.start(["default"], { newJobCheckWaitMS: 10 });
    await waitForJobCount({
      backgroundJobs,
      expectedCount: 1,
      description: "job waiting for its custom retry delay",
      query: { _id: job._id, attemptsCount: 1, lockedAt: { $exists: false } },
    });

    const waitingJob = await backgroundJobs.jobModel.findById(job._id).orFail();
    const nextRunAt = ensureExistence(waitingJob.nextRunAt).getTime();
    expect(nextRunAt).toBeGreaterThanOrEqual(
      ensureExistence(runs[0]).startedAt + error.retryDelayMS,
    );
    expect(waitingJob.lastError).toContain("Rate limited");
    expect(runs).toHaveLength(1);

    await waitForJobCount({
      backgroundJobs,
      expectedCount: 1,
      description: "job exhausted after three attempts",
      query: { _id: job._id, attemptsCount: 3, failedAt: { $exists: true, $ne: null } },
    });
    await backgroundJobs.stop();

    expect(runs.map((run) => run.id)).toStrictEqual(Array<string>(3).fill(job._id.toString()));
    expect(ensureExistence(runs[1]).startedAt).toBeGreaterThanOrEqual(nextRunAt);
    expect(getRetryDelayMS.mock.calls).toStrictEqual([
      [{ error, attemptsCount: 1 }],
      [{ error, attemptsCount: 2 }],
    ]);
    expect(getRetryDelayMS.mock.calls[0]?.[0].error).toBe(error);
    const failedJob = await backgroundJobs.jobModel.findById(job._id).orFail();
    expect(failedJob.nextRunAt).toBeUndefined();
    expect(failedJob.queue).toBeUndefined();
    await expect(backgroundJobs.jobModel.countDocuments()).resolves.toBe(1);
  });

  it.each([
    { name: "no hook", hook: undefined, shouldLog: false },
    { name: "undefined", hook: () => undefined, shouldLog: false },
    { name: "a negative delay", hook: () => -1, shouldLog: true },
    { name: "NaN", hook: () => Number.NaN, shouldLog: true },
    { name: "infinity", hook: () => Number.POSITIVE_INFINITY, shouldLog: true },
    {
      name: "a delay beyond the supported date range",
      hook: () => Number.MAX_VALUE,
      shouldLog: true,
    },
    {
      name: "an exception",
      hook: () => {
        throw new Error("Cannot calculate retry delay");
      },
      shouldLog: true,
    },
    {
      name: "a thrown bigint",
      hook: () => {
        // oxlint-disable-next-line no-throw-literal -- exercise arbitrary values thrown by a handler hook
        throw 1n;
      },
      shouldLog: true,
    },
    {
      name: "a thrown circular object",
      hook: () => {
        const circular: { self?: unknown } = {};
        circular.self = circular;
        // oxlint-disable-next-line no-throw-literal -- exercise arbitrary values thrown by a handler hook
        throw circular;
      },
      shouldLog: true,
    },
  ])("uses exponential backoff for $name", async ({ hook, shouldLog }) => {
    const handler: HandlerInterface<object> = {
      name: "DefaultRetryDelay",
      ...(hook === undefined ? {} : { getRetryDelayMS: hook }),
      perform: async () => {
        throw new Error("Try again");
      },
    };
    backgroundJobs.register(handler, "default");
    const job = ensureExistence(await backgroundJobs.enqueue(handler, {}));
    const beforeRun = Date.now();

    await backgroundJobs.start(["default"], { newJobCheckWaitMS: 10 });
    await waitForJobCount({
      backgroundJobs,
      expectedCount: 1,
      description: "job scheduled with exponential backoff",
      query: { _id: job._id, attemptsCount: 1, lockedAt: { $exists: false } },
    });
    await backgroundJobs.stop();

    const waitingJob = await backgroundJobs.jobModel.findById(job._id).orFail();
    const nextRunAt = ensureExistence(waitingJob.nextRunAt).getTime();
    expect(nextRunAt).toBeGreaterThanOrEqual(beforeRun + 2000);
    expect(nextRunAt).toBeLessThanOrEqual(Date.now() + 2000);
    expect(waitingJob.lastError).toContain("Try again");
    expect(
      logger.errorLogs.some((entry) => entry.message.includes("calculating retry delay")),
    ).toBe(shouldLog);
  });
});
