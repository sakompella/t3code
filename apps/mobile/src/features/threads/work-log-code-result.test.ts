import { describe, expect, it } from "vite-plus/test";

import type { OrchestrationV2TurnItem } from "@t3tools/contracts";

import { workLogCodeResult } from "./work-log-code-result";

const commandItem = (fields: { readonly exitCode?: number }) =>
  ({ type: "command_execution", input: "git status", ...fields }) as OrchestrationV2TurnItem;

const pythonItem = { type: "dynamic_tool", toolName: "ipython", input: { code: "1/0" } } as const;

describe("workLogCodeResult", () => {
  it.each([
    [0, "Process exited with code 0"],
    [2, "Process exited with code 2"],
  ])("reports exit code %i for a command", (exitCode, expected) => {
    expect(workLogCodeResult(commandItem({ exitCode }), exitCode !== 0)).toBe(expected);
  });

  it("notes a failed command that has no exit code", () => {
    expect(workLogCodeResult(commandItem({}), true)).toBe("Tool call failed");
  });

  it("notes a failed Python cell and says nothing for a clean one", () => {
    const item = pythonItem as unknown as OrchestrationV2TurnItem;
    expect(workLogCodeResult(item, true)).toBe("Tool call failed");
    expect(workLogCodeResult(item, false)).toBeNull();
  });
});
