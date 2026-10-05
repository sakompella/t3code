import { describe, expect, it } from "vite-plus/test";

import { effectiveSettled, type ThreadSettleShell } from "./threadSettled.ts";

const SETTLED_AT = "2026-10-05T09:00:00.000Z";
const BEFORE = "2026-10-05T08:00:00.000Z";
const AFTER = "2026-10-05T10:00:00.000Z";

function settledShell(overrides: Partial<ThreadSettleShell> = {}): ThreadSettleShell {
  return {
    settledOverride: "settled",
    settledAt: SETTLED_AT,
    latestRun: { status: "completed", completedAt: BEFORE },
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    ...overrides,
  };
}

describe("effectiveSettled", () => {
  it("keeps a settled thread settled through a wake that completed after the settle", () => {
    expect(
      effectiveSettled(settledShell({ latestRun: { status: "completed", completedAt: AFTER } })),
    ).toBe(true);
    expect(
      effectiveSettled(settledShell({ latestRun: { status: "running", completedAt: null } })),
    ).toBe(true);
  });

  it("raises the hand of a settled thread blocked on the user", () => {
    expect(effectiveSettled(settledShell({ hasPendingApprovals: true }))).toBe(false);
    expect(effectiveSettled(settledShell({ hasPendingUserInput: true }))).toBe(false);
  });

  it("raises the hand only for a failure newer than the settle", () => {
    expect(
      effectiveSettled(settledShell({ latestRun: { status: "failed", completedAt: AFTER } })),
    ).toBe(false);
    // Settling a failed thread is the user saying they saw it.
    expect(
      effectiveSettled(settledShell({ latestRun: { status: "failed", completedAt: BEFORE } })),
    ).toBe(true);
  });

  it("never classifies an unsettled thread as settled", () => {
    expect(effectiveSettled(settledShell({ settledOverride: null, settledAt: null }))).toBe(false);
    expect(effectiveSettled(settledShell({ settledOverride: "active", settledAt: null }))).toBe(
      false,
    );
  });
});
