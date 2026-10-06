import { isOrchestrationV2RoutineRun } from "@t3tools/contracts";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";

import { resolveSidebarThreadStatus, type SidebarThreadStatus } from "./Sidebar.logic";

/** What the coordinator remembers of a thread between shell snapshots. */
export interface ThreadNotificationMemory {
  readonly attention: string | null;
  readonly completion: number | null;
}

export type ThreadNotificationKind = "input" | "completion";

export interface ThreadNotificationObservation {
  readonly status: SidebarThreadStatus;
  readonly memory: ThreadNotificationMemory;
  /** Null when nothing new happened. */
  readonly kind: ThreadNotificationKind | null;
}

/**
 * Compares a thread with what was last seen and says whether it warrants an
 * alert (toast, desktop notification, sound). A thread seen for the first
 * time, as after a reconnect, only sets the memory. A routine heartbeat check
 * neither alerts nor lets an earlier completion alert again.
 */
export function observeThreadForNotification(
  thread: Pick<
    EnvironmentThreadShell,
    | "latestRun"
    | "latestTaskRunCompletedAt"
    | "latestTaskRunStatus"
    | "runtime"
    | "hasPendingApprovals"
    | "hasPendingUserInput"
  >,
  prior: ThreadNotificationMemory | undefined,
): ThreadNotificationObservation {
  let status = resolveSidebarThreadStatus(thread);
  if (status === "ready" && thread.latestRun?.status === "failed") status = "failed";
  const attention =
    status === "input" || status === "approval" || status === "failed" || status === "limited"
      ? `${thread.latestRun?.runId ?? ""}:${status}`
      : null;
  const ended = endedWork(thread, status);
  const completion = ended?.at ?? prior?.completion ?? null;
  const memory = { attention, completion };
  const kind =
    prior === undefined
      ? null
      : attention && attention !== prior.attention
        ? "input"
        : ended?.isCompletion && (prior.completion === null || ended.at > prior.completion)
          ? "completion"
          : null;
  return { status, memory, kind };
}

/**
 * The end of work a snapshot accounts for, and whether it finished a task.
 * Shell updates are coalesced, so a check can start or end in the same update
 * a user's run ended in. A routine check therefore stands for the latest run
 * someone asked for (latestTaskRunCompletedAt and latestTaskRunStatus). A run
 * that ended without completing, or whose status an older server did not
 * send, is remembered but not announced, so it never alerts as a completion.
 */
function endedWork(
  thread: Pick<
    EnvironmentThreadShell,
    "latestRun" | "latestTaskRunCompletedAt" | "latestTaskRunStatus"
  >,
  status: SidebarThreadStatus,
): { readonly at: number; readonly isCompletion: boolean } | null {
  const run = thread.latestRun;
  if (run === null) return null;
  const endedRun = isOrchestrationV2RoutineRun(run)
    ? { completedAt: thread.latestTaskRunCompletedAt, status: thread.latestTaskRunStatus }
    : run;
  const at = Date.parse(endedRun.completedAt ?? "");
  if (!Number.isFinite(at)) return null;
  if (endedRun.status !== "completed") return { at, isCompletion: false };
  // Commands left running (a dev server) read as ready; subagents and monitors wait.
  return status === "ready" ? { at, isCompletion: true } : null;
}
