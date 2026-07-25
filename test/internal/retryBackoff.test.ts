import { computeRetryBackoffMS } from "../../src/lib/internal/retryBackoff";

const MAX = 10 * 60 * 1000; // 10 minutes

describe(computeRetryBackoffMS, () => {
  it("returns the lower bound of the jitter window when random() = 0", () => {
    // 2 ** 1 = 2s -> capped 2000 -> floor 1000
    expect(computeRetryBackoffMS(1, MAX, () => 0)).toBe(1000);
    // 2 ** 3 = 8s -> capped 8000 -> floor 4000
    expect(computeRetryBackoffMS(3, MAX, () => 0)).toBe(4000);
  });

  it("returns the full delay when random() = 1", () => {
    expect(computeRetryBackoffMS(3, MAX, () => 1)).toBe(8000);
  });

  it("caps the delay at maxBackoffMS", () => {
    // 2 ** 20 s hugely exceeds the cap -> clamps to MAX
    expect(computeRetryBackoffMS(20, MAX, () => 1)).toBe(MAX);
    expect(computeRetryBackoffMS(20, MAX, () => 0)).toBe(MAX / 2);
  });

  it("stays finite for very large attempt counts (2 ** n overflow guard)", () => {
    // 2 ** 5000 = Infinity; Math.min(Infinity, MAX) = MAX
    expect(computeRetryBackoffMS(5000, MAX, () => 1)).toBe(MAX);
  });

  it("lands within [cappedMS / 2, cappedMS]", () => {
    const capped = Math.min(2 ** 5 * 1000, MAX); // 32000
    expect(computeRetryBackoffMS(5, MAX, () => 0.5)).toBe(capped / 2 + 0.5 * (capped / 2));
  });
});
