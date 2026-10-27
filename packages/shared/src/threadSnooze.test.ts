import { describe, expect, it } from "@effect/vitest";

import { isSnoozeActive } from "./threadSnooze.ts";

describe("isSnoozeActive", () => {
  it("is active only while the wake time is ahead", () => {
    expect(isSnoozeActive(1_001, 1_000)).toBe(true);
    expect(isSnoozeActive(1_000, 1_000)).toBe(false);
    expect(isSnoozeActive(999, 1_000)).toBe(false);
  });

  it("never hides a thread without a usable wake time", () => {
    expect(isSnoozeActive(null, 1_000)).toBe(false);
    expect(isSnoozeActive(undefined, 1_000)).toBe(false);
    expect(isSnoozeActive(Number.NaN, 1_000)).toBe(false);
  });
});
