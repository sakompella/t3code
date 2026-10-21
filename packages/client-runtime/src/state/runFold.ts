import type { RunId } from "@t3tools/contracts";
import { formatDuration } from "@t3tools/shared/orchestrationTiming";

/*
 * A finished run folds its work behind "Worked for ..." rows. Steers and
 * delivered notifications split a run into segments. Each segment folds on
 * its own, where its work happened, so the collapsed view keeps time order.
 */

/**
 * The expand-state key of one segment's fold. The first segment uses the bare
 * run id, so a run without steers keeps the key it always had.
 */
export function runFoldSegmentKey(runId: RunId, boundaryEntryId: string | null): string {
  return boundaryEntryId === null ? runId : `${runId}${SEGMENT_KEY_SEPARATOR}${boundaryEntryId}`;
}

/** Whether a key from `runFoldSegmentKey` belongs to one of the run's segments. */
export function runFoldSegmentKeyIsForRun(key: string, runId: RunId): boolean {
  return key === runId || key.startsWith(`${runId}${SEGMENT_KEY_SEPARATOR}`);
}

const SEGMENT_KEY_SEPARATOR = "#segment:";

export interface RunFoldSegmentSpan {
  /** When the segment began: its prompt, steer or notification, else its first entry. */
  readonly start: string;
  /** When its last entry or reply last changed. */
  readonly end: string;
  readonly isFirst: boolean;
  readonly isLast: boolean;
}

/**
 * The fold row label of one segment. When the run's recorded start and end
 * are both known, they replace the first segment's start and the last
 * segment's end. "You stopped" belongs to the segment the user stopped.
 */
export function runFoldSegmentLabel(input: {
  readonly segment: RunFoldSegmentSpan;
  readonly run: { readonly startedAt: string | null; readonly completedAt: string | null } | null;
  readonly stopped: boolean;
}): string {
  const { segment, run } = input;
  const runBounds = run?.startedAt && run.completedAt ? run : null;
  const start = runBounds && segment.isFirst ? runBounds.startedAt : segment.start;
  const end = runBounds && segment.isLast ? runBounds.completedAt : segment.end;
  const elapsedMs = elapsedBetween(start, end);
  const duration = elapsedMs === null ? null : formatDuration(elapsedMs);
  if (input.stopped && segment.isLast) {
    return duration ? `You stopped after ${duration}` : "You stopped this response";
  }
  return duration ? `Worked for ${duration}` : "Worked";
}

function elapsedBetween(startIso: string | null, endIso: string | null): number | null {
  const start = Date.parse(startIso ?? "");
  const end = Date.parse(endIso ?? "");
  return Number.isFinite(start) && Number.isFinite(end) ? Math.max(0, end - start) : null;
}
