import type { OrchestrationV2TurnItem, ProviderDriverKind } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { piRecordField as recordField, piRecordString as recordString } from "./PiRpc.ts";
import { classifyIpythonCell, previewPythonCell } from "./primeAgentIpythonCell.ts";
import type { PiItemHooks, PiItemSink } from "./PiAdapterV2State.ts";

function pythonCellTitle(code: string): string {
  const preview = previewPythonCell(code);
  return preview.length === 0 ? "Python" : preview;
}

export function makePrimeAgentTools(input: {
  readonly driver: ProviderDriverKind;
  readonly workspaceRelativePath: (path: string) => string;
  readonly items: Pick<PiItemHooks, "emit" | "emitItemNode" | "baseItemFields">;
}) {
  const { driver, workspaceRelativePath } = input;
  const { emit, emitItemNode, baseItemFields } = input.items;
  /**
   * Prime Agent's edit helper reports each change on the cell result as
   * `details.diffs: [{ path, oldStr, newStr }]`. Surface them as file
   * changes under the cell so the diff view works like other providers'.
   */
  const emitIpythonFileChanges = Effect.fnUntraced(function* (
    turn: PiItemSink,
    toolCallId: string,
    resultRecord: unknown,
    emittedAt: DateTime.Utc,
  ) {
    const diffs = recordField(recordField(resultRecord, "details"), "diffs");
    if (!Array.isArray(diffs)) return;
    const startedAt = turn.toolStartedAt.get(toolCallId) ?? emittedAt;
    for (const [index, diff] of diffs.entries()) {
      const reportedPath = recordString(diff, "path")?.trim();
      const fileName = reportedPath === undefined ? undefined : workspaceRelativePath(reportedPath);
      const oldStr = recordString(diff, "oldStr");
      const newStr = recordString(diff, "newStr");
      if (fileName === undefined || fileName.length === 0) continue;
      if (oldStr === undefined || newStr === undefined) continue;
      const nativeItemId = `${toolCallId}:diff:${index}`;
      yield* emitItemNode(turn, nativeItemId, "tool_call", "completed", startedAt, emittedAt);
      yield* emit({
        type: "turn_item.updated",
        driver,
        turnItem: {
          ...baseItemFields(turn, nativeItemId, startedAt, emittedAt),
          status: "completed",
          completedAt: emittedAt,
          title: "edit",
          type: "file_change",
          fileName,
          oldStr,
          newStr,
        },
      });
    }
  });

  const emitCell = Effect.fnUntraced(function* (
    turn: PiItemSink,
    input: {
      readonly toolCallId: string;
      readonly args: unknown;
      readonly resultRecord: unknown;
      readonly shared: ReturnType<PiItemHooks["baseItemFields"]> &
        Pick<OrchestrationV2TurnItem, "status" | "completedAt">;
      readonly outputText: string;
      readonly completed: boolean;
      readonly emittedAt: DateTime.Utc;
    },
  ) {
    const { toolCallId, args, resultRecord, shared, outputText, completed, emittedAt } = input;
    const cell = classifyIpythonCell(recordString(args, "code") ?? "");
    yield* emit({
      type: "turn_item.updated",
      driver,
      turnItem:
        cell.kind === "bash"
          ? {
              ...shared,
              title: "bash",
              type: "command_execution",
              input: cell.command,
              ...(outputText.length > 0 ? { output: outputText } : {}),
            }
          : {
              ...shared,
              title: pythonCellTitle(cell.code),
              type: "dynamic_tool",
              toolName: "python",
              input: { code: cell.code },
              ...(outputText.length > 0 ? { output: outputText } : {}),
            },
    });
    if (completed) yield* emitIpythonFileChanges(turn, toolCallId, resultRecord, emittedAt);
  });
  return { emitCell };
}
