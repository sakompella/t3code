/**
 * Whether a snooze is in force: its wake time is still ahead. Only the timer or an explicit
 * unsnooze ends a snooze, never thread activity. A missing or NaN wake time never hides a thread.
 */
export function isSnoozeActive(wakeAtMs: number | null | undefined, nowMs: number): boolean {
  return wakeAtMs != null && wakeAtMs > nowMs;
}
