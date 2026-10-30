// @effect-diagnostics globalDate:off -- UI snooze presets use local calendar boundaries and Intl labels.
import { isSnoozeActive } from "@t3tools/shared/threadSnooze";
import * as DateTime from "effect/DateTime";
import { DEFAULT_SNOOZE_PRESETS, type SnoozePresetSetting } from "@t3tools/contracts/settings";

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
 * Failed starts and stale shells from older servers can leave user messages unadopted.
 * Bound the pending window so those threads can eventually settle.
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
  readonly pendingGuestMessageCount?: number;
}

/**
 * Settled resolution. The agent's own wakes run on a settled thread without un-settling it, but the
 * thread raises its hand when such a run needs the user (blocked approval or question, guest
 * message, failure after the settle). Its completion does not. Server fields stay set; the thread
 * only stops classifying as settled.
 */
export function effectiveSettled(shell: ThreadSettleShell): boolean {
  if (shell.settledOverride !== "settled") return false;
  if (shell.hasPendingApprovals || shell.hasPendingUserInput) return false;
  if (shell.pendingSubagentRequest != null) return false;
  if ((shell.pendingGuestMessageCount ?? 0) > 0) return false;
  const latestRun = shell.latestRun ?? shell.latestTurn ?? null;
  const failedAfterSettle =
    (latestRun?.status === "failed" || latestRun?.state === "failed") &&
    latestRun.completedAt != null &&
    shell.settledAt !== null &&
    Date.parse(latestRun.completedAt) > Date.parse(shell.settledAt);
  return !failedAfterSettle;
}

/**
 * A thread may be snoozed unless the agent or a subagent is blocked on the user now: hiding a
 * pending approval, user-input request, or queued turn start defeats it. A running session is
 * snoozable since snooze only affects visibility. Client-side twin of the server invariants so the
 * UI can reject before a round trip.
 */
export function canSnooze(
  shell: QueuedThreadShell & {
    readonly source?: Pick<EnvironmentThreadShell["source"], "pendingRuntimeRequest">;
    readonly hasPendingApprovals: boolean;
    readonly hasPendingUserInput: boolean;
    readonly pendingSubagentRequest?: PendingSubagentRequest | null;
    readonly pendingGuestMessageCount?: number;
  },
  options: { readonly now: string },
): boolean {
  if (shell.hasPendingApprovals || shell.hasPendingUserInput) return false;
  // The server rejects snoozing even for nonblocking questions.
  if (shell.source?.pendingRuntimeRequest != null) return false;
  if (shell.pendingSubagentRequest != null) return false;
  // A waiting guest message is a request for the user; snoozing would hide it.
  if ((shell.pendingGuestMessageCount ?? 0) > 0) return false;
  if (hasQueuedTurnStart(shell, options)) return false;
  return true;
}

/**
 * Snoozed resolution: hidden from the inbox while the wake time is in the future, whatever the
 * thread does. Timer wakes are derived; no server event fires when snoozedUntil passes, so the
 * stale fields just stop classifying as snoozed.
 */
export function effectiveSnoozed(
  shell: ThreadSnoozeShell,
  options: { readonly now: string },
): boolean {
  if (shell.snoozedUntil == null) return false;
  return isSnoozeActive(Date.parse(shell.snoozedUntil), Date.parse(options.now));
}

/**
 * When a previously snoozed thread's timer ran out, or null if it never snoozed or is still
 * snoozed. Drives the "Woke" indicator: the thread keeps its sort position, so the wake signal must
 * carry it. Compare against the client's lastVisitedAt; visiting clears it.
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

const WEEKDAY_INDEX = {
  sunday: 0,
  monday: 1,
  tuesday: 2,
  wednesday: 3,
  thursday: 4,
  friday: 5,
  saturday: 6,
} as const;

export interface SnoozePreset {
  readonly id: string;
  readonly label: string;
  readonly whenLabel: string;
  readonly snoozedUntil: string;
}

export interface SnoozePresetOptions {
  readonly presets?: ReadonlyArray<SnoozePresetSetting>;
  readonly formatTime?: (date: Date) => string;
}

function snoozeTimeOfDayLabel(date: Date): string {
  return date.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
}

// Calendar-day advance instead of adding DAY_MS: fixed millisecond offsets
// land on the wrong local day across DST transitions (a spring-forward day
// is 23 hours, so 23:30 + 24h skips the whole next day).
function addSnoozeDays(base: Date, days: number): Date {
  const next = DateTime.toDate(DateTime.makeUnsafe(base));
  next.setDate(next.getDate() + days);
  return next;
}

function pluralize(count: number, unit: string): string {
  return `${count} ${unit}${count === 1 ? "" : "s"}`;
}

/** "3 hours", "90 minutes", "2 days": the largest unit that divides evenly. */
function formatSnoozeDuration(minutes: number): string {
  if (minutes % (24 * 60) === 0) return pluralize(minutes / (24 * 60), "day");
  if (minutes % 60 === 0) return pluralize(minutes / 60, "hour");
  return pluralize(minutes, "minute");
}

function capitalize(word: string): string {
  return word[0]!.toUpperCase() + word.slice(1);
}

function snoozePresetDaysAhead(
  setting: Extract<SnoozePresetSetting, { kind: "at" }>,
  now: Date,
): number {
  if (setting.day === "today") return 0;
  if (setting.day === "tomorrow") return 1;
  return (WEEKDAY_INDEX[setting.day] - now.getDay() + 7) % 7 || 7;
}

/** Labels follow the actual day gap, so a Monday option on a Sunday reads "Tomorrow". */
function snoozePresetLabel(setting: SnoozePresetSetting, daysAhead: number): string {
  if (setting.kind === "after") return `In ${formatSnoozeDuration(setting.minutes)}`;
  if (daysAhead === 0) {
    const hour = Number(setting.time.slice(0, 2));
    return hour < 12 ? "This morning" : hour < 17 ? "This afternoon" : "This evening";
  }
  if (daysAhead === 1) return "Tomorrow";
  if (setting.day === "monday") return "Next week";
  return daysAhead === 7 ? `Next ${capitalize(setting.day)}` : capitalize(setting.day);
}

export function snoozePresetId(setting: SnoozePresetSetting): string {
  return setting.kind === "after"
    ? `after:${setting.minutes}`
    : `at:${setting.day}:${setting.time}`;
}

export function resolveSnoozePreset(
  setting: SnoozePresetSetting,
  now: Date,
  formatTime: (date: Date) => string = snoozeTimeOfDayLabel,
): SnoozePreset | null {
  let wake: Date;
  let daysAhead = 0;
  if (setting.kind === "after") {
    wake = DateTime.toDate(DateTime.makeUnsafe(now.getTime() + setting.minutes * 60_000));
  } else {
    daysAhead = snoozePresetDaysAhead(setting, now);
    wake = addSnoozeDays(now, daysAhead);
    wake.setHours(Number(setting.time.slice(0, 2)), Number(setting.time.slice(3)), 0, 0);
    // A today option whose time has passed is left out until tomorrow.
    if (wake.getTime() <= now.getTime()) return null;
  }
  const label = snoozePresetLabel(setting, daysAhead);
  // Durations and "Next week" don't name the wake day, so a wake on another day shows it.
  const labelNamesDay = setting.kind === "at" && label !== "Next week";
  const time = formatTime(wake);
  return {
    id: snoozePresetId(setting),
    label,
    whenLabel:
      labelNamesDay || wake.toDateString() === now.toDateString()
        ? time
        : `${wake.toLocaleDateString(undefined, { weekday: "short" })} ${time}`,
    snoozedUntil: wake.toISOString(),
  };
}

/** The snooze menu: soonest first, one row per wake minute. */
export function resolveSnoozePresets(
  now: Date,
  options: SnoozePresetOptions = {},
): ReadonlyArray<SnoozePreset> {
  // By minute, so a duration (which keeps the clock's seconds) and a set time in the same
  // minute tie, and the stable sort keeps whichever comes first in settings.
  const wakeMinute = (preset: SnoozePreset) => Math.floor(Date.parse(preset.snoozedUntil) / 60_000);
  const seen = new Set<number>();
  return (
    (options.presets ?? DEFAULT_SNOOZE_PRESETS)
      .flatMap((setting) => resolveSnoozePreset(setting, now, options.formatTime) ?? [])
      // flatMap returned a new array; Hermes has no `toSorted`.
      .sort((a, b) => wakeMinute(a) - wakeMinute(b))
      .filter((preset) => {
        if (seen.has(wakeMinute(preset))) return false;
        seen.add(wakeMinute(preset));
        return true;
      })
  );
}

/**
 * A menu choice as picked. A duration re-resolves from its setting (not the deduped menu) so it
 * starts at selection. A set time keeps the shown wake, which a day change would otherwise shift.
 */
export function pickSnoozePreset(input: {
  readonly shown: ReadonlyArray<SnoozePreset>;
  readonly id: string;
  readonly now: Date;
  readonly presets?: ReadonlyArray<SnoozePresetSetting>;
  readonly formatTime?: (date: Date) => string;
}): SnoozePreset | undefined {
  const original = input.shown.find((preset) => preset.id === input.id);
  const ahead =
    original && Date.parse(original.snoozedUntil) > input.now.getTime() ? original : undefined;
  const setting = (input.presets ?? DEFAULT_SNOOZE_PRESETS).find(
    (candidate) => candidate.kind === "after" && snoozePresetId(candidate) === input.id,
  );
  return (setting && resolveSnoozePreset(setting, input.now, input.formatTime)) ?? ahead;
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
