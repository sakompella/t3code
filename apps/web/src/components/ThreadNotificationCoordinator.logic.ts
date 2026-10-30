import { WORKING_EXIT_GRACE_MS } from "@t3tools/client-runtime/state/thread-inbox";
import { effectiveSnoozed } from "@t3tools/client-runtime/state/thread-settled";
import { isOrchestrationV2RoutineRun, type OrchestrationV2ThreadShell } from "@t3tools/contracts";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";

import { resolveSidebarThreadStatus, type SidebarThreadStatus } from "./Sidebar.logic";

export type ThreadNotificationKind = "input" | "completion";

export interface ThreadNotificationObservation {
  /** Run and status key while the thread needs attention, otherwise null. */
  readonly attention: string | null;
  /** The thread is idle and its latest task run completed. */
  readonly settled: boolean;
  /** When that run completed, if it did. */
  readonly completedAt: number | null;
}

export interface ThreadNotificationOptions {
  /** Archived or snoozed: nothing notifies, and what happens meanwhile never notifies later. */
  readonly muted: boolean;
  /** A stopped run: its completion never notifies. */
  readonly quiet: boolean;
}

/**
 * Reads one shell snapshot for the notification gate. A routine heartbeat
 * check stands for the latest run someone asked for, so it neither alerts nor
 * lets an earlier completion alert again, and a stopped check does not
 * silence that run. A snoozed thread is muted.
 */
export function observeThreadForNotification(
  thread: Pick<
    EnvironmentThreadShell,
    | "archivedAt"
    | "snoozedUntil"
    | "latestRun"
    | "latestTaskRunCompletedAt"
    | "latestTaskRunStatus"
    | "runtime"
    | "hasPendingApprovals"
    | "hasPendingUserInput"
    | "pendingSubagentRequest"
    | "pendingGuestMessageCount"
  > &
    Pick<OrchestrationV2ThreadShell, "latestRunStopRequested">,
  now: string,
): {
  readonly status: SidebarThreadStatus;
  readonly observation: ThreadNotificationObservation;
  readonly options: ThreadNotificationOptions;
} {
  let status = resolveSidebarThreadStatus(thread);
  if (status === "ready" && thread.latestRun?.status === "failed") status = "failed";
  const attention =
    status === "input" ||
    status === "request" ||
    status === "approval" ||
    status === "failed" ||
    status === "limited"
      ? `${thread.latestRun?.runId ?? ""}:${status}`
      : null;
  const completedAt = completedTaskAt(thread, status);
  const routine = thread.latestRun !== null && isOrchestrationV2RoutineRun(thread.latestRun);
  return {
    status,
    observation: { attention, settled: completedAt !== null, completedAt },
    options: {
      muted: thread.archivedAt != null || effectiveSnoozed(thread, { now }),
      quiet: !routine && thread.latestRunStopRequested === true,
    },
  };
}

/**
 * When the work a snapshot accounts for completed a task, or null. Shell updates are coalesced, so
 * a routine check stands for the latest run someone asked for (latestTaskRunCompletedAt and
 * latestTaskRunStatus). A run that ended without completing, or has no status from an older server,
 * never counts.
 */
function completedTaskAt(
  thread: Pick<
    EnvironmentThreadShell,
    "latestRun" | "latestTaskRunCompletedAt" | "latestTaskRunStatus"
  >,
  status: SidebarThreadStatus,
): number | null {
  const run = thread.latestRun;
  // Commands left running (a dev server) read as ready; subagents and monitors wait.
  if (run === null || status !== "ready") return null;
  const endedRun = isOrchestrationV2RoutineRun(run)
    ? { completedAt: thread.latestTaskRunCompletedAt, status: thread.latestTaskRunStatus }
    : run;
  if (endedRun.status !== "completed") return null;
  const at = Date.parse(endedRun.completedAt ?? "");
  return Number.isFinite(at) ? at : null;
}

/**
 * Attention notifies at once. Completion waits out the Working shelf's exit grace,
 * because an orchestrator run can end moments before its continuation starts. Any
 * observation that is not settled during the wait cancels it.
 */
export function createThreadNotificationGate<Key>(
  notify: (key: Key, kind: ThreadNotificationKind) => void,
) {
  const pending = new Map<Key, { at: number; timer: ReturnType<typeof setTimeout> }>();
  // The newest completion already notified, or deliberately skipped.
  const handled = new Map<Key, number>();
  // The newest completion seen settled, kept while the thread works or waits.
  const lastSettled = new Map<Key, number>();
  const cancel = (key: Key) => {
    clearTimeout(pending.get(key)?.timer);
    pending.delete(key);
  };
  const skip = (key: Key, at: number | null) => {
    if (at !== null && at > (handled.get(key) ?? -Infinity)) handled.set(key, at);
  };
  return {
    observe(
      key: Key,
      prior: ThreadNotificationObservation | undefined,
      next: ThreadNotificationObservation,
      options: ThreadNotificationOptions,
    ): void {
      if (next.settled && next.completedAt !== null) lastSettled.set(key, next.completedAt);
      if (!next.settled || options.muted || options.quiet) cancel(key);
      // The first sighting, muted threads, and stopped runs never notify completion.
      if (!prior || options.muted || options.quiet) skip(key, next.completedAt);
      if (!prior || options.muted) return;
      if (next.attention && next.attention !== prior.attention) {
        // Attention replaces the completion before it, so that run must not notify later.
        skip(key, lastSettled.get(key) ?? null);
        cancel(key);
        notify(key, "input");
        return;
      }
      const at = next.completedAt;
      if (!next.settled || at === null || at <= (handled.get(key) ?? -Infinity)) return;
      if (pending.get(key)?.at === at) return;
      cancel(key);
      pending.set(key, {
        at,
        timer: setTimeout(() => {
          pending.delete(key);
          handled.set(key, at);
          notify(key, "completion");
        }, WORKING_EXIT_GRACE_MS),
      });
    },
    /** Forgets threads that left the snapshot. */
    retain(keys: ReadonlySet<Key>): void {
      for (const key of pending.keys()) if (!keys.has(key)) cancel(key);
      for (const key of handled.keys()) if (!keys.has(key)) handled.delete(key);
      for (const key of lastSettled.keys()) if (!keys.has(key)) lastSettled.delete(key);
    },
    clear(): void {
      for (const key of pending.keys()) cancel(key);
      handled.clear();
      lastSettled.clear();
    },
  };
}
