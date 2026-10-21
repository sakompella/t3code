// @effect-diagnostics globalDate:off -- UI snooze presets use local calendar boundaries and Intl labels.
import { isSnoozeActive } from "@t3tools/shared/threadSnooze";
import * as DateTime from "effect/DateTime";

import type { EnvironmentThreadShell, PendingSubagentRequest } from "./models.ts";

interface SettlementRunLike {
  readonly turnId?: unknown;
  readonly assistantMessageId?: unknown;
  readonly status?: string;
  readonly state?: string;
  readonly requestedAt?: string | null;
  readonly startedAt?: string | null;
  readonly completedAt?: string | null;
}

interface SettlementRuntimeLike {
  readonly threadId?: unknown;
  readonly providerName?: unknown;
  readonly runtimeMode?: unknown;
  readonly activeTurnId?: unknown;
  readonly lastError?: unknown;
  readonly status: string;
  readonly updatedAt?: string;
}

interface QueuedThreadShell {
  readonly latestUserMessageAt?: string | null;
  readonly latestTurn?: SettlementRunLike | null;
  readonly latestRun?: SettlementRunLike | null;
  readonly session?: SettlementRuntimeLike | null;
  readonly runtime?: SettlementRuntimeLike | null;
}

/**
 * A queued turn start lives for at most this long: session adoption takes
 * seconds, so a user message still unadopted after the grace window is a
 * failed start (or stale data — shells from older servers can carry user
 * messages with no latestTurn at all), not pending work. Without this bound
 * such threads would be permanently unsettleable.
 */
export const QUEUED_TURN_START_GRACE_MS = 2 * 60 * 1_000;
const DAY_MS = 24 * 60 * 60 * 1_000;

export function hasQueuedTurnStart(
  shell: QueuedThreadShell,
  options: { readonly now: string },
): boolean {
  if (
    shell.runtime?.status === "preparing" ||
    shell.runtime?.status === "queued" ||
    shell.runtime?.status === "starting"
  ) {
    return true;
  }
  if (shell.latestUserMessageAt == null) return false;
  if (shell.session?.status === "error") return false;
  const messageAt = Date.parse(shell.latestUserMessageAt);
  if (Number.isNaN(messageAt)) return false;
  const nowMs = Date.parse(options.now);
  if (Number.isNaN(nowMs)) return false;
  // Bounded on both sides: message timestamps originate on whichever device
  // sent the message, so a clock ahead of this one yields a negative age
  // that would otherwise hold the queued state for the whole skew. Mirrors
  // the decider's guard.
  if (Math.abs(nowMs - messageAt) > QUEUED_TURN_START_GRACE_MS) return false;
  const turn = shell.latestRun ?? shell.latestTurn ?? null;
  if (turn === null) return true;
  return [turn.requestedAt, turn.startedAt, turn.completedAt].every(
    (candidate) => candidate == null || Date.parse(candidate) < messageAt,
  );
}

/**
 * The snooze lifecycle fields. Snooze is an overlay on the active state: a
 * snoozed thread stays "active" in the data model and is only suppressed
 * from the inbox (and from alerts) until its wake time passes or the user
 * unsnoozes it. Nothing the thread does or receives ends it early.
 */
export interface ThreadSnoozeShell {
  readonly snoozedUntil?: string | null;
}

/** The settle fields plus everything needed to detect a raised hand. */
export interface ThreadSettleShell {
  readonly settledOverride: "settled" | "active" | null;
  readonly settledAt: string | null;
  readonly latestTurn?: SettlementRunLike | null;
  readonly latestRun?: SettlementRunLike | null;
  readonly hasPendingApprovals: boolean;
  readonly hasPendingUserInput: boolean;
  readonly pendingSubagentRequest?: PendingSubagentRequest | null;
}

/**
 * Settled resolution. The agent's own wakes (finished background commands,
 * heartbeats, subagent messages) run on a settled thread without un-settling
 * it, so a thread still raises its hand when such a run needs the user: it or
 * one of its subagents is blocked on an approval or a question, or a run
 * failed after the settle.
 * Its completion does not: the user said they were done with it. Like snooze,
 * the server fields stay set; the thread only stops classifying as settled.
 */
export function effectiveSettled(shell: ThreadSettleShell): boolean {
  if (shell.settledOverride !== "settled") return false;
  if (shell.hasPendingApprovals || shell.hasPendingUserInput) return false;
  if (shell.pendingSubagentRequest != null) return false;
  const latestRun = shell.latestRun ?? shell.latestTurn ?? null;
  const failedAfterSettle =
    (latestRun?.status === "failed" || latestRun?.state === "failed") &&
    latestRun.completedAt != null &&
    shell.settledAt !== null &&
    Date.parse(latestRun.completedAt) > Date.parse(shell.settledAt);
  return !failedAfterSettle;
}

/**
 * A thread may be snoozed unless the agent or one of its subagents is
 * blocked on the user right now: hiding a pending approval or user-input
 * request defeats the request, and a queued turn start (a message no turn has
 * adopted yet) is invisible pending work the same way it is for settle. A running session IS snoozable —
 * snooze only affects visibility, never the agent. Requests that arrive after
 * the snooze do not undo it; the open thread still shows them. Client-side
 * twin of the server invariants so the UI can reject before a round trip.
 */
export function canSnooze(
  shell: QueuedThreadShell & {
    readonly source?: Pick<EnvironmentThreadShell["source"], "pendingRuntimeRequest">;
    readonly hasPendingApprovals: boolean;
    readonly hasPendingUserInput: boolean;
    readonly pendingSubagentRequest?: PendingSubagentRequest | null;
  },
  options: { readonly now: string },
): boolean {
  if (shell.hasPendingApprovals || shell.hasPendingUserInput) return false;
  // The server rejects snoozing even for nonblocking questions.
  if (shell.source?.pendingRuntimeRequest != null) return false;
  if (shell.pendingSubagentRequest != null) return false;
  if (hasQueuedTurnStart(shell, options)) return false;
  return true;
}

/**
 * Snoozed resolution: hidden from the inbox while the wake time is in the
 * future, whatever the thread does meanwhile. Approvals, questions, failures,
 * finished runs and new messages (the user's too) all leave it snoozed; only
 * the timer or an explicit unsnooze ends it. Timer wakes are derived — no
 * server event fires when snoozedUntil passes; the stale fields simply stop
 * classifying as snoozed (and feed the woke indicator until the user visits).
 */
export function effectiveSnoozed(
  shell: ThreadSnoozeShell,
  options: { readonly now: string },
): boolean {
  if (shell.snoozedUntil == null) return false;
  return isSnoozeActive(Date.parse(shell.snoozedUntil), Date.parse(options.now));
}

/**
 * When a previously-snoozed thread's timer ran out, or null if it never
 * snoozed / is still snoozed. Used for the "Woke" indicator: the thread
 * reappears in its original sort position (the inbox sort is deliberately
 * static), so the wake signal has to carry the weight. Compare against the
 * client's lastVisitedAt — visiting clears the indicator like it clears unread.
 */
export function threadWokeAt(
  shell: ThreadSnoozeShell,
  options: { readonly now: string },
): string | null {
  if (shell.snoozedUntil == null) return null;
  const wakeAtMs = Date.parse(shell.snoozedUntil);
  if (Number.isNaN(wakeAtMs)) return null;
  return wakeAtMs <= Date.parse(options.now) ? shell.snoozedUntil : null;
}

const HOUR_MS = 60 * 60 * 1_000;
const EVENING_HOUR = 18;
const MORNING_HOUR = 9;

export type SnoozePresetId = "hour" | "three-hours" | "evening" | "tomorrow" | "next-week";

export interface SnoozePreset {
  readonly id: SnoozePresetId;
  readonly label: string;
  readonly whenLabel: string;
  readonly snoozedUntil: string;
}

function snoozeTimeOfDayLabel(date: Date): string {
  return date.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
}

function snoozeAtHour(base: Date, hour: number): Date {
  const next = DateTime.toDate(DateTime.makeUnsafe(base));
  next.setHours(hour, 0, 0, 0);
  return next;
}

// Calendar-day advance instead of adding DAY_MS: fixed millisecond offsets
// land on the wrong local day across DST transitions (a spring-forward day
// is 23 hours, so 23:30 + 24h skips the whole next day).
function addSnoozeDays(base: Date, days: number): Date {
  const next = DateTime.toDate(DateTime.makeUnsafe(base));
  next.setDate(next.getDate() + days);
  return next;
}

export function resolveSnoozePresets(now: Date): ReadonlyArray<SnoozePreset> {
  const inAnHour = DateTime.toDate(DateTime.makeUnsafe(now.getTime() + HOUR_MS));
  const inThreeHours = DateTime.toDate(DateTime.makeUnsafe(now.getTime() + 3 * HOUR_MS));
  const presets: SnoozePreset[] = [
    {
      id: "hour",
      label: "In 1 hour",
      whenLabel: snoozeTimeOfDayLabel(inAnHour),
      snoozedUntil: inAnHour.toISOString(),
    },
    {
      id: "three-hours",
      label: "In 3 hours",
      whenLabel: snoozeTimeOfDayLabel(inThreeHours),
      snoozedUntil: inThreeHours.toISOString(),
    },
  ];

  const evening = snoozeAtHour(now, EVENING_HOUR);
  if (evening.getTime() - now.getTime() > HOUR_MS) {
    presets.push({
      id: "evening",
      label: "This evening",
      whenLabel: snoozeTimeOfDayLabel(evening),
      snoozedUntil: evening.toISOString(),
    });
  }

  const tomorrow = snoozeAtHour(addSnoozeDays(now, 1), MORNING_HOUR);
  presets.push({
    id: "tomorrow",
    label: "Tomorrow",
    whenLabel: snoozeTimeOfDayLabel(tomorrow),
    snoozedUntil: tomorrow.toISOString(),
  });

  const daysUntilMonday = (1 - now.getDay() + 7) % 7 || 7;
  const nextWeek = snoozeAtHour(addSnoozeDays(now, daysUntilMonday), MORNING_HOUR);
  if (nextWeek.getTime() !== tomorrow.getTime()) {
    presets.push({
      id: "next-week",
      label: "Next week",
      whenLabel: `${nextWeek.toLocaleDateString(undefined, { weekday: "short" })} ${snoozeTimeOfDayLabel(nextWeek)}`,
      snoozedUntil: nextWeek.toISOString(),
    });
  }

  return presets;
}

export function snoozeWakeLabel(snoozedUntil: string, options: { readonly now: string }): string {
  const wakeMs = Date.parse(snoozedUntil);
  const nowMs = Date.parse(options.now);
  if (Number.isNaN(wakeMs) || Number.isNaN(nowMs)) return "now";
  const remainingMs = wakeMs - nowMs;
  if (remainingMs <= 0) return "now";
  if (remainingMs < HOUR_MS) return `${Math.max(1, Math.ceil(remainingMs / 60_000))}m`;
  if (remainingMs < DAY_MS) return `${Math.ceil(remainingMs / HOUR_MS)}h`;
  return `${Math.ceil(remainingMs / DAY_MS)}d`;
}

export type CustomSnoozeInput =
  | { readonly mode: "date"; readonly date: string; readonly time: string }
  | {
      readonly mode: "duration";
      readonly amount: string;
      readonly unit: "minutes" | "hours" | "days";
    };

export function resolveCustomSnooze(input: CustomSnoozeInput, now: Date): string | null {
  let wake: Date;
  if (input.mode === "duration") {
    const amount = Number(input.amount);
    if (!Number.isFinite(amount) || amount <= 0) return null;
    const unitMs = { minutes: 60_000, hours: HOUR_MS, days: 24 * HOUR_MS }[input.unit];
    wake = new Date(now.getTime() + amount * unitMs);
  } else {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(input.date) || !/^\d{2}:\d{2}$/.test(input.time)) return null;
    wake = new Date(`${input.date}T${input.time}:00`);
    // Reject rolled-over dates and nonexistent local times during DST changes.
    if (localSnoozeDate(wake) !== input.date || localSnoozeTime(wake) !== input.time) return null;
  }
  return Number.isFinite(wake.getTime()) && wake.getTime() > now.getTime()
    ? wake.toISOString()
    : null;
}

export function localSnoozeDate(date: Date): string {
  return `${String(date.getFullYear()).padStart(4, "0")}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}

export function localSnoozeTime(date: Date): string {
  return `${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

/** Whether a woken thread woke after the user last visited it. */
export function isThreadWokeUnseen(input: {
  readonly wokeAt: string | null;
  readonly settled: boolean;
  readonly lastVisitedAt: string | null | undefined;
}): boolean {
  if (input.wokeAt === null || input.settled) return false;
  const wokeAtMs = Date.parse(input.wokeAt);
  if (Number.isNaN(wokeAtMs)) return false;
  const visitedAtMs = input.lastVisitedAt ? Date.parse(input.lastVisitedAt) : NaN;
  return Number.isNaN(visitedAtMs) || visitedAtMs < wokeAtMs;
}
