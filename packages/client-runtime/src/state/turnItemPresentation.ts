import {
  ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";

import type { ThreadStructureTurnItem } from "./models.ts";

const WORKSPACE_PREPARATION_INPUT = "Preparing workspace";

export function turnItemIsWorkspacePreparation(item: OrchestrationV2TurnItem): boolean {
  return (
    (item.type === "command_execution" && item.input === WORKSPACE_PREPARATION_INPUT) ||
    (item.type === "error" &&
      item.status === "cancelled" &&
      item.failure.code === ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE)
  );
}

export function workspacePreparationRetryRunIds(
  runs: ReadonlyArray<Pick<OrchestrationV2Run, "id" | "status" | "workspacePreparation">>,
  items: ReadonlyArray<ThreadStructureTurnItem>,
): ReadonlySet<OrchestrationV2Run["id"]> {
  const failedRuns = new Set(
    runs.flatMap((run) =>
      run.status === "failed" && run.workspacePreparation !== undefined ? [run.id] : [],
    ),
  );
  const retryable = new Set<OrchestrationV2Run["id"]>();
  if (failedRuns.size === 0) return retryable;
  for (const item of items) {
    if (
      item.type === "error" &&
      item.status === "failed" &&
      item.failure.code === ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE &&
      item.runId !== null &&
      failedRuns.has(item.runId)
    )
      retryable.add(item.runId);
  }
  return retryable;
}

/**
 * Whether a system notice is routine (`info`, `progress`) rather than a
 * warning. A notice without a tone predates tones and stays a warning.
 */
export function systemNoticeIsRoutine(item: OrchestrationV2TurnItem): boolean {
  return item.type === "system_notice" && (item.tone === "info" || item.tone === "progress");
}

/**
 * A `progress` notice describes work in flight. Once it completes it says
 * nothing the timeline needs, so clients drop it instead of leaving a past-tense row.
 */
export function turnItemIsFinishedProgressNotice(item: OrchestrationV2TurnItem): boolean {
  return item.type === "system_notice" && item.tone === "progress" && item.status === "completed";
}

/**
 * A row shows one line and cuts the rest. Text longer than this, or with a
 * line break, may be cut. It is a length rule, not a measurement: clients
 * differ in width, and the rule must give the same answer everywhere.
 */
const NOTICE_ROW_TEXT_LIMIT = 48;

const noticeRowMayCut = (text: string) =>
  text.length > NOTICE_ROW_TEXT_LIMIT || text.includes("\n");

/**
 * What a notice or notification row shows when opened, or `null` when the row
 * has nothing more to show and stays closed. A notice's message is its row
 * label, so it opens only when the row can cut it off. A notification opens for
 * its detail, and for its summary when that can be cut off too.
 */
export function noticeExpandedText(item: OrchestrationV2TurnItem): string | null {
  switch (item.type) {
    case "system_notice":
      return noticeRowMayCut(item.message) ? item.message : null;
    case "notification": {
      const parts = [
        noticeRowMayCut(item.summary) ? item.summary : undefined,
        item.detail?.trim() || undefined,
      ].filter((part) => part !== undefined);
      return parts.length > 0 ? parts.join("\n\n") : null;
    }
    default:
      return null;
  }
}
