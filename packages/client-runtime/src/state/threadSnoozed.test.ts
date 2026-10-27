// @effect-diagnostics globalDate:off -- Tests exercise local calendar snooze boundaries.
import { ThreadId } from "@t3tools/contracts";
import { TurnId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  canSnooze,
  effectiveSnoozed,
  hasQueuedTurnStart,
  resolveSnoozePresets,
  threadWokeAt,
  isThreadWokeUnseen,
  type ThreadSnoozeShell,
} from "./threadSettled.ts";

const NOW = "2026-04-10T12:00:00.000Z";
const SNOOZED_AT = "2026-04-10T09:00:00.000Z";
const FUTURE_WAKE = "2026-04-11T09:00:00.000Z";
const PAST_WAKE = "2026-04-10T10:00:00.000Z";

function localDate(year: number, month: number, day: number, hour: number, minute = 0): Date {
  return new Date(year, month - 1, day, hour, minute, 0, 0);
}

type TestShell = ThreadSnoozeShell &
  Parameters<typeof canSnooze>[0] & { readonly snoozedAt: string | null };

function makeShell(input: {
  readonly snoozedUntil?: string | null;
  readonly snoozedAt?: string | null;
  readonly sessionStatus?: "starting" | "running" | "ready" | "error";
  readonly pending?: "approval" | "user-input";
  readonly turnRequestedAt?: string | null;
  readonly turnCompletedAt?: string | null;
}): TestShell {
  const threadId = ThreadId.make("thread-1");
  return {
    snoozedUntil: input.snoozedUntil ?? null,
    snoozedAt: input.snoozedAt ?? (input.snoozedUntil != null ? SNOOZED_AT : null),
    hasPendingApprovals: input.pending === "approval",
    hasPendingUserInput: input.pending === "user-input",
    session:
      input.sessionStatus === undefined
        ? null
        : {
            threadId,
            status: input.sessionStatus,
            providerName: "Codex",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: input.sessionStatus === "error" ? "boom" : null,
            updatedAt: "2026-04-10T11:00:00.000Z",
          },
    latestTurn:
      input.turnCompletedAt === undefined
        ? null
        : {
            turnId: TurnId.make("turn-1"),
            state: "completed",
            requestedAt: input.turnRequestedAt === undefined ? SNOOZED_AT : input.turnRequestedAt,
            startedAt: null,
            completedAt: input.turnCompletedAt,
            assistantMessageId: null,
          },
  };
}

type QueuedTurnShell = Parameters<typeof hasQueuedTurnStart>[0];

function makeQueuedTurnShell(overrides: Partial<QueuedTurnShell> = {}): QueuedTurnShell {
  return { latestUserMessageAt: null, latestTurn: null, session: null, ...overrides };
}

describe("effectiveSnoozed", () => {
  it("stops classifying as snoozed once the wake time passes (timer wake, no event)", () => {
    expect(effectiveSnoozed(makeShell({ snoozedUntil: PAST_WAKE }), { now: NOW })).toBe(false);
    expect(effectiveSnoozed(makeShell({ snoozedUntil: NOW }), { now: NOW })).toBe(false);
  });

  it("never hides on malformed wake data", () => {
    expect(effectiveSnoozed(makeShell({ snoozedUntil: "not-a-date" }), { now: NOW })).toBe(false);
  });

  // Only the timer and an explicit unsnooze end a snooze: whatever the thread
  // does or receives meanwhile leaves it snoozed.
  it.each([
    ["is blocked on an approval", { pending: "approval" }],
    ["is blocked on a question", { pending: "user-input" }],
    ["fails after the snooze", { sessionStatus: "error" }],
    ["fails before the snooze", { sessionStatus: "error", snoozedAt: "2026-04-10T11:30:00.000Z" }],
    ["keeps working", { sessionStatus: "running" }],
    ["completes a run after the snooze", { turnCompletedAt: "2026-04-10T10:30:00.000Z" }],
    ["completed a run before the snooze", { turnCompletedAt: "2026-04-10T08:00:00.000Z" }],
  ] as const)("stays snoozed when the thread %s", (_case, activity) => {
    expect(
      effectiveSnoozed(makeShell({ snoozedUntil: FUTURE_WAKE, ...activity }), { now: NOW }),
    ).toBe(true);
  });

  it("stays snoozed when a run requested after the snooze completes", () => {
    const shell = {
      ...makeShell({ snoozedUntil: FUTURE_WAKE }),
      latestRun: {
        status: "completed",
        requestedAt: "2026-04-10T09:00:00.001Z",
        startedAt: "2026-04-10T09:15:00.000Z",
        completedAt: "2026-04-10T10:30:00.000Z",
      },
    };

    expect(effectiveSnoozed(shell, { now: NOW })).toBe(true);
    expect(threadWokeAt(shell, { now: NOW })).toBe(null);
  });

  it("is awake after an explicit unsnooze clears the fields", () => {
    const unsnoozed = { ...makeShell({ snoozedUntil: FUTURE_WAKE }), snoozedUntil: null };
    expect(effectiveSnoozed(unsnoozed, { now: NOW })).toBe(false);
  });
});

describe("canSnooze", () => {
  it("refuses blocked-on-you work", () => {
    expect(
      canSnooze({ ...makeShell({ pending: "approval" }), latestUserMessageAt: null }, { now: NOW }),
    ).toBe(false);
    expect(
      canSnooze(
        { ...makeShell({ pending: "user-input" }), latestUserMessageAt: null },
        { now: NOW },
      ),
    ).toBe(false);
    expect(
      canSnooze(
        { ...makeShell({}), latestUserMessageAt: null, pendingSubagentRequest: "approval" },
        { now: NOW },
      ),
    ).toBe(false);
  });

  it("refuses a queued turn start — same invisible-pending-work rule as settle", () => {
    expect(
      canSnooze(
        { ...makeShell({}), latestUserMessageAt: "2026-04-10T11:59:30.000Z" },
        { now: NOW },
      ),
    ).toBe(false);
    expect(
      canSnooze(
        { ...makeShell({}), latestUserMessageAt: "2026-04-10T11:00:00.000Z" },
        { now: NOW },
      ),
    ).toBe(true);
  });
});

describe("hasQueuedTurnStart", () => {
  it("clears queued state when a turn adopts the message or the session fails", () => {
    const messageAt = "2026-04-10T11:59:00.000Z";
    const adopted = makeQueuedTurnShell({
      latestUserMessageAt: messageAt,
      latestTurn: {
        turnId: TurnId.make("turn-adopted"),
        state: "running",
        requestedAt: messageAt,
        startedAt: null,
        completedAt: null,
        assistantMessageId: null,
      },
    });
    const failed = makeQueuedTurnShell({
      latestUserMessageAt: messageAt,
      session: {
        threadId: ThreadId.make("thread-failed"),
        status: "error",
        providerName: "Codex",
        runtimeMode: "full-access",
        activeTurnId: null,
        lastError: "failed",
        updatedAt: NOW,
      },
    });
    expect(hasQueuedTurnStart(adopted, { now: NOW })).toBe(false);
    expect(hasQueuedTurnStart(failed, { now: NOW })).toBe(false);
  });
});

describe("threadWokeAt", () => {
  it("reports the timer wake even after an already-requested run completes", () => {
    expect(threadWokeAt(makeShell({ snoozedUntil: PAST_WAKE }), { now: NOW })).toBe(PAST_WAKE);
    const shell = makeShell({
      snoozedUntil: PAST_WAKE,
      turnCompletedAt: "2026-04-10T09:30:00.000Z",
    });
    expect(effectiveSnoozed(shell, { now: NOW })).toBe(false);
    expect(threadWokeAt(shell, { now: NOW })).toBe(PAST_WAKE);
  });

  it("does not report a wake for activity under a running snooze", () => {
    expect(
      threadWokeAt(
        makeShell({
          snoozedUntil: FUTURE_WAKE,
          sessionStatus: "error",
          pending: "approval",
          turnCompletedAt: "2026-04-10T10:30:00.000Z",
        }),
        { now: NOW },
      ),
    ).toBe(null);
  });

  it("reports the timer, not a completion, when both happened", () => {
    expect(
      threadWokeAt(
        makeShell({ snoozedUntil: PAST_WAKE, turnCompletedAt: "2026-04-10T09:30:00.000Z" }),
        { now: NOW },
      ),
    ).toBe(PAST_WAKE);
  });
});

describe("resolveSnoozePresets", () => {
  it("drops the evening choice once evening is near or past", () => {
    expect(resolveSnoozePresets(localDate(2026, 4, 8, 17, 30)).map((preset) => preset.id)).toEqual([
      "hour",
      "three-hours",
      "tomorrow",
      "next-week",
    ]);
  });

  it("drops next week on Sundays, when it lands on the same Monday as tomorrow", () => {
    const presets = resolveSnoozePresets(localDate(2026, 8, 30, 7, 1));
    expect(presets.map((preset) => preset.id)).toEqual([
      "hour",
      "three-hours",
      "evening",
      "tomorrow",
    ]);
    const tomorrow = new Date(presets.find((preset) => preset.id === "tomorrow")!.snoozedUntil);
    expect(tomorrow.getDay()).toBe(1);
  });
});

it("dismisses a wake at its timestamp without hiding a later wake", () => {
  const firstWake = "2026-04-10T10:00:00.000Z";
  const state = {
    wokeAt: firstWake,
    settled: false,
    lastVisitedAt: null,
  };
  expect(isThreadWokeUnseen(state)).toBe(true);
  expect(isThreadWokeUnseen({ ...state, lastVisitedAt: firstWake })).toBe(false);
  expect(isThreadWokeUnseen({ ...state, lastVisitedAt: firstWake, wokeAt: NOW })).toBe(true);
});
