import type { BackgroundJobs } from "../src";
import type { HandlerInterface } from "../src/lib/handler";
import { ensureExistence } from "./support/ensureExistence";
import { ExampleJob } from "./support/exampleJob";
import { JobRun } from "./support/jobRun";
import { createTestContext, type TestContext } from "./support/testContext";
import { TestLogger } from "./support/testLogger";

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

class GatedFailingJob implements HandlerInterface<object> {
  public name = "GatedFailingJob";
  public readonly started = deferred();
  public readonly proceed = deferred();

  public async perform() {
    this.started.resolve();
    await this.proceed.promise;
    throw new Error("Job Failed");
  }
}

describe("Graceful shutdown", () => {
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

  it("releases a job locked after stop() began, without running it, before stop() returns", async () => {
    backgroundJobs.register(ExampleJob, "default");
    const job = ensureExistence(await backgroundJobs.enqueue(ExampleJob, { myNumber: 1 }));

    const worker = backgroundJobs.buildWorker(["default"], { useChangeStream: false });
    const acquisitionStarted = deferred();
    const allowAcquisition = deferred();
    const acquireNextJob = worker.acquireNextJob.bind(worker);
    worker.acquireNextJob = async () => {
      acquisitionStarted.resolve();
      await allowAcquisition.promise;
      return await acquireNextJob();
    };

    await worker.start();
    await acquisitionStarted.promise;

    const stopped = worker.stop(5000);
    allowAcquisition.resolve();
    await stopped;

    const jobAfterStop = await backgroundJobs.jobModel.findById(job._id).lean();

    expect(jobAfterStop).not.toBeNull();
    expect(jobAfterStop?.lockedAt).toBeUndefined();
    expect(jobAfterStop?.attemptsCount).toBe(0);
    await expect(JobRun.countDocuments()).resolves.toBe(0);
    expect(logger.infoLogs.map(({ message }) => message)).toContain("Background Jobs: Stopped");
    expect(logger.errorLogs).toHaveLength(0);
  });

  it("waits for a failed job's retry write before stop() returns", async () => {
    const handler = new GatedFailingJob();
    backgroundJobs.register(handler, "default");
    const job = ensureExistence(await backgroundJobs.enqueue(handler, {}));

    const { jobsRepo } = backgroundJobs;
    const updateOne = jobsRepo.updateOne.bind(jobsRepo);
    let retryWritten = false;
    jobsRepo.updateOne = async (...arguments_) => {
      await updateOne(...arguments_);
      retryWritten = true;
    };

    const worker = backgroundJobs.buildWorker(["default"], { useChangeStream: false });
    await worker.start();
    await handler.started.promise;

    const stopped = worker.stop(5000);
    handler.proceed.resolve();
    await stopped;

    expect(retryWritten).toBe(true);
    const jobAfterStop = await backgroundJobs.jobModel.findById(job._id).lean();
    expect(jobAfterStop).not.toBeNull();
    expect(jobAfterStop?.attemptsCount).toBe(1);
    expect(jobAfterStop?.lockedAt).toBeUndefined();
  });

  it("does not release a lock another worker re-acquired after it expired", async () => {
    backgroundJobs.register(ExampleJob, "default");
    await backgroundJobs.enqueue(ExampleJob, { myNumber: 1 });

    const staleJob = ensureExistence(
      await backgroundJobs.jobsRepo.fetchAndLockNextJob(["ExampleJob"]),
    );
    const newLockedAt = new Date(ensureExistence(staleJob.lockedAt).getTime() + 1000);
    await backgroundJobs.jobModel.updateOne({ _id: staleJob._id }, { lockedAt: newLockedAt });

    await backgroundJobs.jobsRepo.releaseLock(staleJob);

    const jobAfterRelease = await backgroundJobs.jobModel.findById(staleJob._id).lean();

    expect(jobAfterRelease).not.toBeNull();
    expect(jobAfterRelease?.lockedAt).toEqual(newLockedAt);
  });
});
