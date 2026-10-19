import { describe, expect, it } from "vite-plus/test";

import type { OrchestrationV2TurnItem } from "@t3tools/contracts";

import {
  pythonCellLabel,
  splitCodeHighlightWindow,
  workEntryBodyCode,
  workEntryLabelCode,
} from "./entryCode.js";
import { commandDisplayText } from "./commandLabel.js";
import type { WorkLogPresentationEntry } from "./presentation.js";

const entry: WorkLogPresentationEntry = {
  id: "entry-1",
  createdAt: "2026-01-01T00:00:00.000Z",
  label: "Tool call",
  tone: "tool",
};

function pythonEntry(
  code: string | undefined,
  fields: Partial<WorkLogPresentationEntry> = {},
): WorkLogPresentationEntry {
  return {
    ...entry,
    itemType: "dynamic_tool",
    structuredPayload: {
      type: "dynamic_tool",
      toolName: "python",
      input: code === undefined ? {} : { code },
    } as OrchestrationV2TurnItem,
    ...fields,
  };
}

function commandEntry(command: string): WorkLogPresentationEntry {
  return { ...entry, itemType: "command_execution", command, rawCommand: command };
}

describe("workEntryLabelCode", () => {
  it("marks a command label as shell code", () => {
    const command = commandEntry("ls -la src");
    const label = commandDisplayText("ls -la src");
    expect(workEntryLabelCode(command, label)).toEqual({ code: label, language: "shellscript" });
  });

  it("treats a shell wrapper's script as the code the row shows", () => {
    const command = commandEntry("/bin/zsh -lc 'ls -la src'");
    expect(workEntryLabelCode(command, "ls -la src")).toEqual({
      code: "ls -la src",
      language: "shellscript",
    });
  });

  it("accepts a label a client collapsed to one line", () => {
    const command = commandEntry("echo a &&\n  echo b");
    expect(workEntryLabelCode(command, "echo a && echo b")?.language).toBe("shellscript");
  });

  it("leaves prose labels alone", () => {
    const command = commandEntry("ls -la src");
    expect(workEntryLabelCode(command, "Ran 3 commands")).toBeNull();
    expect(workEntryLabelCode(command, "Running ls")).toBeNull();
    expect(workEntryLabelCode(entry, "Tool call")).toBeNull();
  });

  it("marks a Python cell label as Python and drops the legacy prefix", () => {
    const python = pythonEntry("print(1)", { toolTitle: "print(1)" });
    expect(workEntryLabelCode(python, "print(1)")).toEqual({
      code: "print(1)",
      language: "python",
    });
    expect(workEntryLabelCode(python, "Python: print(1)")).toEqual({
      code: "print(1)",
      language: "python",
    });
    expect(
      workEntryLabelCode(pythonEntry("print(1)", { toolTitle: "Python" }), "Python"),
    ).toBeNull();
  });

  it("does not mark a prose label over a Python cell as code", () => {
    const python = pythonEntry("print(1)", { toolTitle: "print(1)" });
    expect(workEntryLabelCode(python, "Used 3 tools")).toBeNull();
  });
});

describe("pythonCellLabel", () => {
  it("keeps the provider's spelling of the cell title", () => {
    expect(pythonCellLabel(pythonEntry("x", { toolTitle: "import os" }))).toBe("import os");
    expect(pythonCellLabel(pythonEntry("x", { toolTitle: "Python: import os" }))).toBe("import os");
  });

  it("is null for other tools", () => {
    expect(pythonCellLabel(entry)).toBeNull();
    expect(pythonCellLabel(commandEntry("ls"))).toBeNull();
  });
});

describe("workEntryBodyCode", () => {
  it("selects the whole Python cell", () => {
    const python = pythonEntry("import os\nprint(os.getcwd())", { toolTitle: "import os" });
    expect(workEntryBodyCode(python, "import os")).toEqual({
      code: "import os\nprint(os.getcwd())",
      language: "python",
    });
  });

  it("has no body for a cell without source", () => {
    expect(workEntryBodyCode(pythonEntry(undefined, { toolTitle: "x" }), "x")).toBeNull();
    expect(workEntryBodyCode(pythonEntry("  \n", { toolTitle: "x" }), "x")).toBeNull();
  });

  it("selects the raw command rather than the unwrapped label", () => {
    const command = commandEntry("/bin/zsh -lc 'ls -la src'");
    expect(workEntryBodyCode(command, "ls -la src")).toEqual({
      code: "/bin/zsh -lc 'ls -la src'",
      language: "shellscript",
    });
  });

  it("has no body for prose labels", () => {
    expect(workEntryBodyCode(commandEntry("ls"), "Ran 3 commands")).toBeNull();
    expect(workEntryBodyCode(entry, "Tool call")).toBeNull();
  });
});

describe("splitCodeHighlightWindow", () => {
  it("highlights only the first line of a label", () => {
    expect(splitCodeHighlightWindow("import os\nprint(1)", "label")).toEqual({
      head: "import os",
      tail: "\nprint(1)",
    });
  });

  it("caps how much of a long label is tokenized and keeps every character", () => {
    const code = "x".repeat(1_000);
    const { head, tail } = splitCodeHighlightWindow(code, "label");
    expect(head.length).toBeLessThan(code.length);
    expect(head + tail).toBe(code);
  });

  it("keeps multiple lines in a body and caps very large ones", () => {
    expect(splitCodeHighlightWindow("a\nb", "body")).toEqual({ head: "a\nb", tail: "" });
    const huge = "y".repeat(100_000);
    const { head, tail } = splitCodeHighlightWindow(huge, "body");
    expect(head.length).toBeLessThan(huge.length);
    expect(head + tail).toBe(huge);
  });
});
