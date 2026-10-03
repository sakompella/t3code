import type { OrchestrationV2TurnItem } from "@t3tools/contracts";

/**
 * What an expanded command or Python row says about the run, shown under its
 * source. Raw output stays out of the inspector (see `toolItemForDisplay`), so
 * this is the exit code, or a plain failure note when the run has none.
 */
export function workLogCodeResult(item: OrchestrationV2TurnItem, failed: boolean): string | null {
  if (item.type === "command_execution" && item.exitCode !== undefined) {
    return `Process exited with code ${item.exitCode}`;
  }
  return failed ? "Tool call failed" : null;
}
