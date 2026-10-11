/**
 * Whether a snooze is in force: its wake time is still ahead. The fork's snooze
 * is strict, so the deadline is the only input. What the thread did or received
 * since never ends it; only the timer or an explicit unsnooze does. Server and
 * clients all classify through this, so agents and the UI agree.
 *
 * A missing or malformed (NaN) wake time never hides a thread.
 */
export function isSnoozeActive(wakeAtMs: number | null | undefined, nowMs: number): boolean {
  return wakeAtMs != null && wakeAtMs > nowMs;
}
