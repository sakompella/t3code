import { describe, expect, it } from "@effect/vitest";

import {
  awaitsHandle,
  classifyIpythonCell,
  detachedBashJobs,
  previewPythonCell,
} from "./primeAgentIpythonCell.ts";

describe("classifyIpythonCell", () => {
  it("treats a %%bash cell magic as a shell command", () => {
    expect(classifyIpythonCell("\n  %%bash\ngit status\npnpm test\n")).toEqual({
      kind: "bash",
      command: "git status\npnpm test",
    });
  });

  it("treats a lone bash() helper call as a shell command", () => {
    expect(classifyIpythonCell("r = await bash('echo hi'); print(r.output)")).toEqual({
      kind: "bash",
      command: "echo hi",
    });
    expect(
      classifyIpythonCell('import os\n# run tests\nawait bash("pnpm test", timeout=120)'),
    ).toEqual({ kind: "bash", command: "pnpm test" });
  });

  it("decodes escapes in normal strings and keeps raw strings verbatim", () => {
    expect(classifyIpythonCell("bash(\"printf 'a\\\\tb\\n'\")")).toEqual({
      kind: "bash",
      command: "printf 'a\\tb\n'",
    });
    expect(classifyIpythonCell("bash(r'grep -E \"\\d+\" x')")).toEqual({
      kind: "bash",
      command: 'grep -E "\\d+" x',
    });
    expect(classifyIpythonCell('bash("""\nset -e\nls\n""")')).toEqual({
      kind: "bash",
      command: "\nset -e\nls\n",
    });
  });

  it("keeps cells that do more than run one command as Python", () => {
    const cases = [
      "from pathlib import Path; Path('x.txt').write_text('a')",
      "cmd = 'ls'\nawait bash(cmd)",
      "await bash('ls')\nPath('a').write_text('b')",
      "await bash('ls' + suffix)",
      'bash("\\x41")',
      "print('%%bash')",
    ];
    for (const code of cases) {
      expect(classifyIpythonCell(code)).toEqual({ kind: "python", code });
    }
  });
});

describe("previewPythonCell", () => {
  it("summarizes a cell by its first statement after imports and comments", () => {
    expect(previewPythonCell("import os\n# compute\n\nprint(6 *   7)\nx = 1")).toBe("print(6 * 7)");
    expect(previewPythonCell("from pathlib import Path")).toBe("");
    expect(previewPythonCell(`data = ${"[1, 2, 3, 4, 5]".repeat(8)}`)).toHaveLength(64);
  });
});

describe("detachedBashJobs", () => {
  it("finds handles the creating cell never awaits", () => {
    // The shape Prime Agent used live for "run it in the background".
    expect(
      detachedBashJobs(
        "late_job = bash('sleep 40 && echo late'); print(late_job.pid, late_job.running)",
      ),
    ).toEqual([{ variable: "late_job", command: "sleep 40 && echo late" }]);
    expect(detachedBashJobs('import time\nbash("make watch")')).toEqual([
      { variable: null, command: "make watch" },
    ]);
  });

  it("ignores commands the cell waits for", () => {
    expect(detachedBashJobs("r = await bash('pnpm test'); print(r.output)")).toEqual([]);
    expect(detachedBashJobs("job = bash('sleep 1')\nprint(job.pid)\nres = await job")).toEqual([]);
    expect(detachedBashJobs("print('no shell here')")).toEqual([]);
  });

  it("recognizes a later cell consuming a handle", () => {
    expect(awaitsHandle("out = await late_job\nprint(out)", "late_job")).toBe(true);
    expect(awaitsHandle("print(late_job.running)", "late_job")).toBe(false);
    expect(awaitsHandle("await late_jobs_list", "late_job")).toBe(false);
  });
});
