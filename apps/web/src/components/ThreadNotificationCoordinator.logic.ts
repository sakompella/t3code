import { runCompletedTask } from "@t3tools/client-runtime/state/thread-heartbeats";
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
 * that finishes keeps the last completion, so it neither alerts nor lets the
 * same completion alert again.
 */
export function observeThreadForNotification(
  thread: Pick<
    EnvironmentThreadShell,
    "latestRun" | "runtime" | "hasPendingApprovals" | "hasPendingUserInput"
  >,
  prior: ThreadNotificationMemory | undefined,
): ThreadNotificationObservation {
  let status = resolveSidebarThreadStatus(thread);
  if (status === "ready" && thread.latestRun?.status === "failed") status = "failed";
  const attention =
    status === "input" || status === "approval" || status === "failed" || status === "limited"
      ? `${thread.latestRun?.runId ?? ""}:${status}`
      : null;
  const completedAt = Date.parse(thread.latestRun?.completedAt ?? "");
  // Commands left running (a dev server) read as ready; subagents and monitors wait.
  const completion =
    status === "ready" && runCompletedTask(thread.latestRun) && Number.isFinite(completedAt)
      ? completedAt
      : (prior?.completion ?? null);
  const memory = { attention, completion };
  const kind =
    prior === undefined
      ? null
      : attention && attention !== prior.attention
        ? "input"
        : completion !== null && (prior.completion === null || completion > prior.completion)
          ? "completion"
          : null;
  return { status, memory, kind };
}
