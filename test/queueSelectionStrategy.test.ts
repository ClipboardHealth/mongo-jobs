import type { BackgroundJobs } from "../src";
import { ExampleJob } from "./support/exampleJob";
import { Semaphore } from "./support/semaphore";
import { SemaphoreJob } from "./support/semaphoreJob";
import { createTestContext, type TestContext } from "./support/testContext";
import { TestLogger } from "./support/testLogger";
import { waitForJobRunCount } from "./support/waitForJobRunCount";

const SLOW_QUEUE = "slow";
const FAST_QUEUE = "fast";
const BLOCKED = "blocked";
const MAX_CONCURRENCY = 2;

describe("leastInFlight queue selection", () => {
  let testContext: TestContext;
  let backgroundJobs: BackgroundJobs;
  let semaphore: Semaphore;

  beforeEach(async () => {
    testContext = await createTestContext({ logger: new TestLogger() });
    ({ backgroundJobs } = testContext);
    semaphore = new Semaphore();

    // Never resolved during the test, so every slow job holds its slot until teardown.
    semaphore.setNewPromise(BLOCKED);

    backgroundJobs.register(new SemaphoreJob(semaphore), SLOW_QUEUE);
    backgroundJobs.register(ExampleJob, FAST_QUEUE);
  });

  afterEach(async () => {
    semaphore.cleanup();
    await testContext.tearDown();
  });

  async function enqueueBlockedSlowJobs(count: number): Promise<void> {
    for (let index = 0; index < count; index += 1) {
      // Enqueued in series so the jobs are ordered predictably within the queue.
      // eslint-disable-next-line no-await-in-loop
      await backgroundJobs.enqueue(SemaphoreJob, {
        resolvePromise: `slow-started-${index}`,
        waitPromise: BLOCKED,
      });
    }
  }

  async function enqueueFastJobs(count: number): Promise<void> {
    for (let index = 0; index < count; index += 1) {
      // Enqueued in series so the jobs are ordered predictably within the queue.
      // eslint-disable-next-line no-await-in-loop
      await backgroundJobs.enqueue(ExampleJob, { myNumber: index });
    }
  }

  it("keeps clearing a fast queue while a backlogged queue holds its share of concurrency", async () => {
    const fastJobCount = 6;
    await enqueueBlockedSlowJobs(MAX_CONCURRENCY + 2);
    await enqueueFastJobs(fastJobCount);

    await backgroundJobs.start([SLOW_QUEUE, FAST_QUEUE], {
      maxConcurrency: MAX_CONCURRENCY,
      queueSelectionStrategy: "leastInFlight",
    });

    // The slow queue can never hold more than one of the two slots, so every fast job runs even
    // though the slow backlog is deeper than the worker's concurrency.
    await waitForJobRunCount({ expectedCount: fastJobCount });
  });

  it("remains work-conserving when the only actionable queue has jobs in flight", async () => {
    const fastJobCount = 5;
    await enqueueFastJobs(fastJobCount);

    await backgroundJobs.start([SLOW_QUEUE, FAST_QUEUE], {
      maxConcurrency: MAX_CONCURRENCY,
      queueSelectionStrategy: "leastInFlight",
    });

    await waitForJobRunCount({ expectedCount: fastJobCount });
  });
});
