import { describe, expect, it } from "@effect/vitest";

import { classifyIpythonCell } from "./primeAgentIpythonCell.ts";

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
