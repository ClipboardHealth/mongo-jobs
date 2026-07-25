/* eslint-disable @typescript-eslint/dot-notation */
import type { BackgroundJobs } from "../src";
import { ExampleJob } from "./support/exampleJob";
import { JobRun } from "./support/jobRun";
import { createTestContext, type TestContext } from "./support/testContext";

describe("Graceful shutdown", () => {
  let testContext: TestContext;
  let backgroundJobs: BackgroundJobs;

  beforeEach(async () => {
    testContext = await createTestContext();
    ({ backgroundJobs } = testContext);
    backgroundJobs.register(ExampleJob, "default");
  });

  afterEach(async () => {
    await testContext.tearDown();
  });

  it("does not acquire any job when the worker is already stopped", async () => {
    await backgroundJobs.enqueue(ExampleJob, { myNumber: 1 });

    const worker = backgroundJobs.buildWorker(["default"], {});
    let acquireCalls = 0;
    worker.acquireNextJob = async () => {
      acquireCalls += 1;
      return undefined;
    };

    // A freshly built worker is stopped until start() is called.
    await worker["workerLoop"]();

    expect(acquireCalls).toBe(0);
  });

  it("releases the lock and starts no handler when stop() happens mid-acquire", async () => {
    const created = await backgroundJobs.enqueue(ExampleJob, { myNumber: 1 });

    const worker = backgroundJobs.buildWorker(["default"], {});
    worker.stopped = false;

    // Simulate stop() being called while a job is being acquired: the job is
    // locked in the database, then the worker is flagged as stopped.
    worker.acquireNextJob = async () => {
      worker.stopped = true;
      return await backgroundJobs.jobsRepo.fetchAndLockNextJob(["ExampleJob"]);
    };

    await worker["workerLoop"]();

    const jobAfter = await backgroundJobs.jobModel.findById(created!._id);
    expect(jobAfter?.lockedAt ?? null).toBeNull();
    expect(jobAfter?.attemptsCount).toBe(0);
    await expect(JobRun.countDocuments()).resolves.toBe(0);
  });
});
