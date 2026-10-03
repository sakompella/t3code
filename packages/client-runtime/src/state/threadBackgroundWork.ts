/**
 * Background work to show next to a running turn, derived from the thread
 * projection. The server's pending roster is settled-only (it drives idle
 * status, settlement, and alerts), so a client that wants to show work during a
 * turn derives it here instead.
 */
import type {
  OrchestrationV2PendingBackgroundTask,
  OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { deriveOutlivingBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";

import { presentPendingBackgroundWork } from "./threadExecution.ts";
import { resolveActiveThreadRun } from "./threadWorkflows.ts";

type Projection = OrchestrationV2ThreadProjection;

/**
 * Empty when no turn is running. The running turn's own steps already show in
 * the feed, so they stay out; roster jobs and older runs' leftovers remain.
 */
export function deriveRunningTurnBackgroundWork(
  projection: Pick<Projection, "runs" | "providerThreads" | "turnItems" | "thread">,
): ReadonlyArray<OrchestrationV2PendingBackgroundTask> {
  const activeRun = resolveActiveThreadRun(projection);
  if (activeRun === null) return [];
  return deriveOutlivingBackgroundWork({
    providerThreads: projection.providerThreads,
    turnItems: projection.turnItems,
    activeProviderThreadId: projection.thread.activeProviderThreadId,
    runs: projection.runs,
    foregroundRunIds: new Set([String(activeRun.id)]),
  });
}

export function backgroundWorkTasksEqual(
  left: ReadonlyArray<OrchestrationV2PendingBackgroundTask>,
  right: ReadonlyArray<OrchestrationV2PendingBackgroundTask>,
): boolean {
  return (
    left.length === right.length &&
    left.every((task, index) => {
      const other = right[index]!;
      return (
        task.taskId === other.taskId &&
        task.kind === other.kind &&
        task.description === other.description
      );
    })
  );
}

export interface BackgroundWorkPillSegment {
  readonly label: string;
  readonly accessibilityLabel: string;
}

/**
 * The pill segment for commands and other non-subagent work. Subagents have
 * their own segment, so counting them here would show them twice.
 */
export function resolveBackgroundWorkPillSegment(
  tasks: ReadonlyArray<OrchestrationV2PendingBackgroundTask>,
): BackgroundWorkPillSegment | null {
  const presentation = presentPendingBackgroundWork(
    tasks.filter((task) => task.kind !== "subagent"),
    { turnRunning: true },
  );
  if (presentation === null) return null;
  return {
    label: `${presentation.items.length} background`,
    // A single item's title already names it ("Running: npm run dev").
    accessibilityLabel:
      presentation.items.length === 1
        ? presentation.title
        : `${presentation.title}: ${presentation.items.map((item) => item.label).join(", ")}`,
  };
}
