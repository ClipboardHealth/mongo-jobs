import { ActionableQueues } from "../../../src/lib/internal/worker/actionableQueues";

const QUEUES = ["q0", "q1", "q2", "q3", "q4"];
const OPERATIONS = ["add", "remove", "acquire", "release"] as const;
const TRIALS = 200;
const OPERATIONS_PER_TRIAL = 40;
const QUERIES_PER_OPERATION = 4;

/**
 * Recomputes the answer from scratch on every query, with no cached minimum. `ActionableQueues`
 * caches one, so mutation orderings that leave the cache stale show up as a disagreement here.
 */
class ReferenceModel {
  private readonly actionable = new Set<string>();
  private readonly inFlightByQueue = new Map<string, number>();

  public add(queue: string): void {
    this.actionable.add(queue);
  }

  public remove(queue: string): void {
    this.actionable.delete(queue);
  }

  public acquire(queue: string): void {
    this.inFlightByQueue.set(queue, this.inFlight(queue) + 1);
  }

  public release(queue: string): void {
    const current = this.inFlight(queue);
    if (current === 0) {
      return;
    }

    if (current === 1) {
      this.inFlightByQueue.delete(queue);
    } else {
      this.inFlightByQueue.set(queue, current - 1);
    }
  }

  public actionableQueues(): string[] {
    return [...this.actionable];
  }

  public minimalQueues(): string[] {
    const queues = this.actionableQueues();
    if (queues.length === 0) {
      return [];
    }

    const fewestInFlight = Math.min(...queues.map((queue) => this.inFlight(queue)));
    return queues.filter((queue) => this.inFlight(queue) === fewestInFlight);
  }

  private inFlight(queue: string): number {
    return this.inFlightByQueue.get(queue) ?? 0;
  }
}

/** Seeded so a failure is reproducible rather than flaky. */
function createSeededRandom(seed: number): () => number {
  let state = seed;

  return () => {
    state = (state * 1_103_515_245 + 12_345) % 2_147_483_648;
    return state / 2_147_483_648;
  };
}

describe(`${ActionableQueues.name} properties`, () => {
  it("agrees with a recompute-from-scratch model across random operation sequences", () => {
    const random = createSeededRandom(12_345);

    for (let trial = 0; trial < TRIALS; trial += 1) {
      const subject = new ActionableQueues({ random });
      const reference = new ReferenceModel();
      const history: string[] = [];

      for (let step = 0; step < OPERATIONS_PER_TRIAL; step += 1) {
        const queue = QUEUES[Math.floor(random() * QUEUES.length)]!;
        const operation = OPERATIONS[Math.floor(random() * OPERATIONS.length)]!;

        history.push(`${operation}(${queue})`);
        subject[operation](queue);
        reference[operation](queue);

        const minimalQueues = reference.minimalQueues();
        const actionableQueues = reference.actionableQueues();

        for (let query = 0; query < QUERIES_PER_OPERATION; query += 1) {
          const context = history.join(" ");

          if (minimalQueues.length === 0) {
            expect(subject.getLeastInFlight(), context).toBeUndefined();
            expect(subject.getRandom(), context).toBeUndefined();
          } else {
            expect(minimalQueues, context).toContain(subject.getLeastInFlight());
            expect(actionableQueues, context).toContain(subject.getRandom());
          }
        }
      }
    }
  });

  it("can select every queue tied for least in flight, so none is silently skipped", () => {
    const random = createSeededRandom(6789);

    for (let trial = 0; trial < TRIALS; trial += 1) {
      const subject = new ActionableQueues({ random });
      const reference = new ReferenceModel();

      for (let step = 0; step < OPERATIONS_PER_TRIAL; step += 1) {
        const queue = QUEUES[Math.floor(random() * QUEUES.length)]!;
        const operation = OPERATIONS[Math.floor(random() * OPERATIONS.length)]!;
        subject[operation](queue);
        reference[operation](queue);
      }

      const minimalQueues = reference.minimalQueues();
      if (minimalQueues.length === 0) {
        continue;
      }

      const selected = new Set<string>();
      for (let query = 0; query < 500; query += 1) {
        const queue = subject.getLeastInFlight();
        if (queue !== undefined) {
          selected.add(queue);
        }
      }

      expect([...selected].sort()).toStrictEqual([...minimalQueues].sort());
    }
  });
});
