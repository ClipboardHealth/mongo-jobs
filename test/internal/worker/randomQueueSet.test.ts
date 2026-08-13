import { RandomQueueSet } from "../../../src/lib/internal/worker/randomQueueSet";

describe(RandomQueueSet, () => {
  let value: number;
  let queueSet: RandomQueueSet;

  beforeEach(() => {
    value = 0;
    queueSet = new RandomQueueSet({ random: () => value });
  });

  /** The queues `getRandom` can return, sorted, so assertions ignore internal ordering. */
  function reachable(): string[] {
    const selected = new Set<string>();

    [0, 0.25, 0.5, 0.75, 0.99].forEach((randomValue) => {
      value = randomValue;
      const queue = queueSet.getRandom();
      if (queue !== undefined) {
        selected.add(queue);
      }
    });

    return [...selected].sort();
  }

  it("starts empty", () => {
    expect(queueSet.size).toBe(0);
    expect(queueSet.has("absent")).toBe(false);
    expect(queueSet.getRandom()).toBeUndefined();
  });

  it("adds queues once", () => {
    queueSet.add("a");
    queueSet.add("a");

    expect(queueSet.size).toBe(1);
    expect(queueSet.has("a")).toBe(true);
  });

  it("can return every member", () => {
    ["a", "b", "c"].forEach((queue) => {
      queueSet.add(queue);
    });

    expect(reachable()).toStrictEqual(["a", "b", "c"]);
  });

  it("ignores deletes of queues it does not hold", () => {
    queueSet.add("a");
    queueSet.delete("b");

    expect(queueSet.size).toBe(1);
    expect(reachable()).toStrictEqual(["a"]);
  });

  it("keeps every remaining queue reachable after deleting from the middle", () => {
    ["a", "b", "c", "d"].forEach((queue) => {
      queueSet.add(queue);
    });

    queueSet.delete("b");

    expect(queueSet.size).toBe(3);
    expect(queueSet.has("b")).toBe(false);
    expect(reachable()).toStrictEqual(["a", "c", "d"]);
  });

  it("keeps every remaining queue reachable after deleting the last queue", () => {
    ["a", "b", "c"].forEach((queue) => {
      queueSet.add(queue);
    });

    queueSet.delete("c");

    expect(reachable()).toStrictEqual(["a", "b"]);
  });

  it("can re-add a deleted queue", () => {
    queueSet.add("a");
    queueSet.delete("a");
    queueSet.add("a");

    expect(queueSet.size).toBe(1);
    expect(reachable()).toStrictEqual(["a"]);
  });

  it("empties fully", () => {
    ["a", "b"].forEach((queue) => {
      queueSet.add(queue);
    });
    queueSet.delete("a");
    queueSet.delete("b");

    expect(queueSet.size).toBe(0);
    expect(queueSet.getRandom()).toBeUndefined();
  });

  it("stays in range when random returns its exclusive upper bound", () => {
    ["a", "b"].forEach((queue) => {
      queueSet.add(queue);
    });

    value = 1;

    expect(queueSet.getRandom()).toBe("b");
  });
});
