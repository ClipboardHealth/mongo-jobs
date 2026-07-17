import { ActionableQueues } from "../../../src/lib/internal/worker/actionableQueues";

describe(ActionableQueues, () => {
  let actionableQueues: ActionableQueues;

  beforeEach(() => {
    actionableQueues = new ActionableQueues();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("returns the only actionable queue", () => {
    const queue = "myQueue";
    actionableQueues.add(queue);

    expect(actionableQueues.getLeastInFlight()).toBe(queue);
  });

  it("returns undefined when the only actionable queue is removed", () => {
    const queue = "myQueue";
    actionableQueues.add(queue);
    actionableQueues.remove(queue);

    expect(actionableQueues.getLeastInFlight()).toBeUndefined();
  });

  it("treats repeated additions as idempotent", () => {
    const queue = "myQueue";
    actionableQueues.add(queue);
    actionableQueues.add(queue);
    actionableQueues.add(queue);
    actionableQueues.remove(queue);

    expect(actionableQueues.getLeastInFlight()).toBeUndefined();
  });

  it("will return proper queue in a complex add and remove scenario", () => {
    const queue1 = "queue1";
    const queue2 = "queue2";
    const queue3 = "queue3";

    // Queue1
    actionableQueues.add(queue1);
    // Queue1, queue2
    actionableQueues.add(queue2);
    // Queue1, queue2, queue3

    actionableQueues.add(queue3);
    // Queue1, queue2, queue3
    actionableQueues.add(queue1);
    // Queue1, queue2
    actionableQueues.remove(queue3);
    // Queue1, queue2
    actionableQueues.remove(queue3);
    // Queue1, queue2, queue3
    actionableQueues.add(queue3);
    // Queue1, queue2, queue3
    actionableQueues.add(queue2);
    // Queue1, queue3
    actionableQueues.remove(queue2);
    // Queue3
    actionableQueues.remove(queue1);

    expect(actionableQueues.getLeastInFlight()).toBe(queue3);
  });

  it("prefers the actionable queue with fewer jobs in flight", () => {
    actionableQueues.add("slow");
    actionableQueues.add("fast");

    actionableQueues.acquire("slow");

    expect(actionableQueues.getLeastInFlight()).toBe("fast");
  });

  it("randomly selects among queues tied for least in flight", () => {
    actionableQueues.add("slow");
    actionableQueues.add("fast");
    vi.spyOn(Math, "random").mockReturnValue(0.99);

    expect(actionableQueues.getLeastInFlight()).toBe("fast");
  });

  it("returns to random selection after the only in-flight job is released", () => {
    actionableQueues.add("slow");
    actionableQueues.add("fast");
    actionableQueues.acquire("slow");
    actionableQueues.release("slow");
    const random = vi.spyOn(Math, "random").mockReturnValue(0);

    actionableQueues.getLeastInFlight();

    expect(random).toHaveBeenCalledOnce();
  });

  it("remains work-conserving when only one queue is actionable", () => {
    actionableQueues.add("slow");

    actionableQueues.acquire("slow");
    expect(actionableQueues.getLeastInFlight()).toBe("slow");

    actionableQueues.acquire("slow");
    expect(actionableQueues.getLeastInFlight()).toBe("slow");
  });

  it("makes a released queue least loaded", () => {
    actionableQueues.add("slow");
    actionableQueues.add("fast");
    actionableQueues.acquire("slow");
    actionableQueues.acquire("fast");

    actionableQueues.release("slow");

    expect(actionableQueues.getLeastInFlight()).toBe("slow");
  });

  it("continues tracking in-flight jobs while a queue is not actionable", () => {
    actionableQueues.add("queue");
    actionableQueues.acquire("queue");
    actionableQueues.remove("queue");
    actionableQueues.release("queue");

    actionableQueues.add("queue");

    expect(actionableQueues.getLeastInFlight()).toBe("queue");
  });
});
