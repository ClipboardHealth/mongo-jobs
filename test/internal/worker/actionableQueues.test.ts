import { ActionableQueues } from "../../../src/lib/internal/worker/actionableQueues";

interface StubRandom {
  random: () => number;
  setValue: (value: number) => void;
}

/** Replaces `Math.random` by injection, so no test reaches into a global. */
function stubRandom(): StubRandom {
  let value = 0;

  return {
    random: () => value,
    setValue: (next: number) => {
      value = next;
    },
  };
}

const SELECTORS = ["getRandom", "getLeastInFlight"] as const;

/** Enough spread to reach every element of the sets these tests build. */
const RANDOM_VALUES = [0, 0.25, 0.5, 0.75, 0.99];

describe(ActionableQueues, () => {
  let stub: StubRandom;
  let actionableQueues: ActionableQueues;

  beforeEach(() => {
    stub = stubRandom();
    actionableQueues = new ActionableQueues({ random: stub.random });
  });

  /**
   * The queues a selector can return, sorted. Asserting on the whole set keeps these tests
   * independent of the order `RandomQueueSet` happens to hold its elements in.
   */
  function reachable(select: () => string | undefined): string[] {
    const selected = new Set<string>();

    RANDOM_VALUES.forEach((value) => {
      stub.setValue(value);
      const queue = select();
      if (queue !== undefined) {
        selected.add(queue);
      }
    });

    return [...selected].sort();
  }

  function reachableLeastInFlight(): string[] {
    return reachable(() => actionableQueues.getLeastInFlight());
  }

  describe.each(SELECTORS)("%s", (selector) => {
    function select(): string | undefined {
      return actionableQueues[selector]();
    }

    it("returns the only actionable queue", () => {
      const queue = "myQueue";
      actionableQueues.add(queue);

      expect(reachable(select)).toStrictEqual([queue]);
    });

    it("returns undefined when no queue is actionable", () => {
      expect(select()).toBeUndefined();
    });

    it("returns undefined when the only actionable queue is removed", () => {
      const queue = "myQueue";
      actionableQueues.add(queue);
      actionableQueues.remove(queue);

      expect(select()).toBeUndefined();
    });

    it("treats repeated additions as idempotent", () => {
      const queue = "myQueue";
      actionableQueues.add(queue);
      actionableQueues.add(queue);
      actionableQueues.add(queue);
      actionableQueues.remove(queue);

      expect(select()).toBeUndefined();
    });

    it("ignores removals of queues that are not actionable", () => {
      actionableQueues.add("present");
      actionableQueues.remove("absent");

      expect(reachable(select)).toStrictEqual(["present"]);
    });

    it("can return either of two idle actionable queues", () => {
      actionableQueues.add("first");
      actionableQueues.add("second");

      expect(reachable(select)).toStrictEqual(["first", "second"]);
    });

    it("will return proper queue in a complex add and remove scenario", () => {
      const queue1 = "queue1";
      const queue2 = "queue2";
      const queue3 = "queue3";

      actionableQueues.add(queue1);
      actionableQueues.add(queue2);
      actionableQueues.add(queue3);
      actionableQueues.add(queue1);
      actionableQueues.remove(queue3);
      actionableQueues.remove(queue3);
      actionableQueues.add(queue3);
      actionableQueues.add(queue2);
      actionableQueues.remove(queue2);
      actionableQueues.remove(queue1);

      expect(reachable(select)).toStrictEqual([queue3]);
    });
  });

  describe("getRandom", () => {
    it("ignores in-flight counts, so a busy queue keeps winning its share", () => {
      actionableQueues.add("busy");
      actionableQueues.add("idle");
      actionableQueues.acquire("busy");
      actionableQueues.acquire("busy");

      expect(reachable(() => actionableQueues.getRandom())).toStrictEqual(["busy", "idle"]);
      expect(reachableLeastInFlight()).toStrictEqual(["idle"]);
    });
  });

  describe("getLeastInFlight", () => {
    it("prefers the actionable queue with fewer jobs in flight", () => {
      actionableQueues.add("slow");
      actionableQueues.add("fast");

      actionableQueues.acquire("slow");

      expect(reachableLeastInFlight()).toStrictEqual(["fast"]);
    });

    it("selects among queues tied for least in flight", () => {
      actionableQueues.add("slow");
      actionableQueues.add("fast");
      actionableQueues.acquire("slow");
      actionableQueues.acquire("fast");

      expect(reachableLeastInFlight()).toStrictEqual(["fast", "slow"]);
    });

    it("returns a released queue to contention with a queue that never had jobs", () => {
      actionableQueues.add("released");
      actionableQueues.add("untouched");
      actionableQueues.acquire("released");

      expect(reachableLeastInFlight()).toStrictEqual(["untouched"]);

      actionableQueues.release("released");

      expect(reachableLeastInFlight()).toStrictEqual(["released", "untouched"]);
    });

    it("makes a released queue least loaded when its peers are still busy", () => {
      actionableQueues.add("slow");
      actionableQueues.add("fast");
      actionableQueues.acquire("slow");
      actionableQueues.acquire("fast");

      actionableQueues.release("slow");

      expect(reachableLeastInFlight()).toStrictEqual(["slow"]);
    });

    it("remains work-conserving when only one queue is actionable", () => {
      actionableQueues.add("slow");

      actionableQueues.acquire("slow");
      expect(reachableLeastInFlight()).toStrictEqual(["slow"]);

      actionableQueues.acquire("slow");
      expect(reachableLeastInFlight()).toStrictEqual(["slow"]);
    });

    it("ignores releases for a queue with nothing in flight", () => {
      actionableQueues.add("spuriously-released");
      actionableQueues.add("other");

      actionableQueues.release("spuriously-released");
      actionableQueues.release("spuriously-released");
      actionableQueues.acquire("spuriously-released");

      // Crediting a release below zero would rank it under "other" forever.
      expect(reachableLeastInFlight()).toStrictEqual(["other"]);
    });

    it("continues tracking in-flight jobs while a queue is not actionable", () => {
      actionableQueues.add("requeued");
      actionableQueues.add("quiet");
      actionableQueues.acquire("requeued");
      actionableQueues.acquire("requeued");

      actionableQueues.remove("requeued");
      actionableQueues.release("requeued");
      actionableQueues.add("requeued");

      // One job is still in flight, so "quiet" wins outright rather than tying.
      expect(reachableLeastInFlight()).toStrictEqual(["quiet"]);

      actionableQueues.release("requeued");

      expect(reachableLeastInFlight()).toStrictEqual(["quiet", "requeued"]);
    });

    it("still selects a queue that rejoins the set as the only one, with jobs in flight", () => {
      actionableQueues.add("rejoining");
      actionableQueues.acquire("rejoining");
      actionableQueues.remove("rejoining");

      actionableQueues.add("rejoining");

      expect(reachableLeastInFlight()).toStrictEqual(["rejoining"]);
    });

    it("selects a queue that rejoins alongside a busier one, with jobs in flight", () => {
      actionableQueues.add("rejoining");
      actionableQueues.add("busier");
      actionableQueues.acquire("rejoining");
      actionableQueues.acquire("busier");
      actionableQueues.acquire("busier");
      actionableQueues.remove("rejoining");
      actionableQueues.remove("busier");

      actionableQueues.add("busier");
      actionableQueues.add("rejoining");

      expect(reachableLeastInFlight()).toStrictEqual(["rejoining"]);
    });
  });
});
