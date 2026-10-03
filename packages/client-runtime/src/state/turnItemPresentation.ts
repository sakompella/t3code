import type { OrchestrationV2TurnItem } from "@t3tools/contracts";

const WORKSPACE_PREPARATION_INPUT = "Preparing workspace";

/** Workspace setup is client bookkeeping; preparation failures have their own error item. */
export function turnItemIsWorkspacePreparation(item: OrchestrationV2TurnItem): boolean {
  return item.type === "command_execution" && item.input === WORKSPACE_PREPARATION_INPUT;
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
