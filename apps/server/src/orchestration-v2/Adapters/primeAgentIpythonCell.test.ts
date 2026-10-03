import { describe, expect, it } from "@effect/vitest";

import {
  awaitsHandle,
  assignmentOffset,
  endsHandle,
  classifyIpythonCell,
  detachedBashJobs,
  previewPythonCell,
  readsHandle,
  reportedCommandMatches,
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

  it.each([
    {
      name: "assigned await of an f-string, printing output",
      code: "r = await bash(f'''sed -n 1842,1900p {dac}; rg -n \"x\" {src}/main.ts''')\nprint(r.output)",
      command: 'sed -n 1842,1900p {dac}; rg -n "x" {src}/main.ts',
    },
    {
      name: "sliced output print",
      code: "r = await bash(f'ls {d}')\nprint(r.output[:4000])",
      command: "ls {d}",
    },
    {
      name: "background handle",
      code: "h = bash('npx vp test run a.test.ts')",
      command: "npx vp test run a.test.ts",
    },
    {
      name: "background handle printing its pid",
      code: "h = bash('npx vp test run a.test.ts')\nprint(h.pid)",
      command: "npx vp test run a.test.ts",
    },
    { name: "bare await", code: 'await bash("make")', command: "make" },
    {
      name: "keyword args on several lines",
      code: "x = await bash(\n  f'pnpm -C {pkg} test',\n  timeout=120,\n)\nprint(x.exit_code)",
      command: "pnpm -C {pkg} test",
    },
    {
      name: "escaped braces in an f-string",
      code: "await bash(f\"awk '{{print $1}}' {path}\")",
      command: "awk '{print $1}' {path}",
    },
    {
      name: "raw f-string",
      code: "await bash(rf'grep -E \"\\d+\" {path}')",
      command: 'grep -E "\\d+" {path}',
    },
    {
      name: "plain string is not an f-string",
      code: "await bash('echo {not_a_placeholder}')",
      command: "echo {not_a_placeholder}",
    },
  ])("treats $name as a shell command", ({ code, command }) => {
    expect(classifyIpythonCell(code)).toEqual({ kind: "bash", command });
  });

  it("keeps noise and result prints from changing a bash classification", () => {
    const prefixes = ["", "import os\n", "# go\n", "print('start')\n"];
    const calls = ["r = await bash(f'ls {d}')", "h = bash('ls')", "await bash('ls', timeout=5)"];
    const suffixes = [
      "",
      "\nprint(r.output)",
      "\nprint(r.output[:4000])",
      "; print(r.exit_code)",
      "\nprint(h.pid, h.running)",
      "\n\n# done",
    ];
    for (const prefix of prefixes)
      for (const call of calls)
        for (const suffix of suffixes) {
          expect(classifyIpythonCell(prefix + call + suffix).kind).toBe("bash");
        }
  });

  it("keeps cells that do more than run one command as Python", () => {
    const cases = [
      "from pathlib import Path; Path('x.txt').write_text('a')",
      "cmd = 'ls'\nawait bash(cmd)",
      "await bash('ls')\nPath('a').write_text('b')",
      "await bash('ls' + suffix)",
      'bash("\\x41")',
      "print('%%bash')",
      "await bash('a')\nawait bash('b')",
      "r = await bash(f'a {x}')\nr2 = await bash(f'b {r.output}')",
      "print(await bash('ls'))",
      "for d in dirs:\n    await bash(f'ls {d}')",
      "await bash(f'ls {d}' + suffix)",
      "await bash(f'ls {d}') and cleanup()",
      "r = await bash(f'ls')\nvalue = r.output.split()",
      "await bash(f'ls {d}', timeout=compute(3))",
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
    ).toEqual([{ variable: "late_job", command: "sleep 40 && echo late", isTemplate: false }]);
    expect(detachedBashJobs('import time\nbash("make watch")')).toEqual([
      { variable: null, command: "make watch", isTemplate: false },
    ]);
    expect(detachedBashJobs("h = bash(f'npx vp test run {file}')\nprint(h.pid)")).toEqual([
      { variable: "h", command: "npx vp test run {file}", isTemplate: true },
    ]);
  });

  it("ignores commands the cell waits for", () => {
    expect(detachedBashJobs("r = await bash('pnpm test'); print(r.output)")).toEqual([]);
    expect(detachedBashJobs("job = bash('sleep 1')\nprint(job.pid)\nres = await job")).toEqual([]);
    expect(detachedBashJobs("print('no shell here')")).toEqual([]);
  });

  it("ignores a handle the creating cell kills", () => {
    expect(detachedBashJobs("job = bash('sleep 300')\njob.kill()")).toEqual([]);
  });

  it("recognizes a later cell killing a handle", () => {
    expect(endsHandle("job.kill(); print(job.output())", "job")).toBe(true);
    expect(endsHandle("job . kill ( 9 )", "job")).toBe(true);
    expect(endsHandle("print(job.tail(5), job.poll())", "job")).toBe(false);
    expect(endsHandle("other_job.kill()", "job")).toBe(false);
    expect(endsHandle("jobs.kill()", "job")).toBe(false);
  });

  it("recognizes a later cell consuming a handle", () => {
    expect(awaitsHandle("out = await late_job\nprint(out)", "late_job")).toBe(true);
    expect(awaitsHandle("print(late_job.running)", "late_job")).toBe(false);
    expect(awaitsHandle("await late_jobs_list", "late_job")).toBe(false);
    expect(awaitsHandle("rs = await asyncio.gather(a, late_job)", "late_job")).toBe(true);
    expect(awaitsHandle("rs = await asyncio.gather(a, b)", "late_job")).toBe(false);
  });

  it("ignores handles the creating cell gathers", () => {
    expect(
      detachedBashJobs("a = bash('sleep 1')\nb = bash('sleep 2')\nawait asyncio.gather(a, b)"),
    ).toEqual([]);
  });

  it("ignores a wait only for the job whose name it still holds", () => {
    expect(detachedBashJobs("h = bash('sleep 1')\nh = bash('sleep 2')\nawait h")).toEqual([
      { variable: "h", command: "sleep 1", isTemplate: false },
    ]);
    expect(detachedBashJobs("h = bash('sleep 1')\nawait h\nh = bash('sleep 2')")).toEqual([
      { variable: "h", command: "sleep 2", isTemplate: false },
    ]);
  });

  it("finds where a cell assigns a name again", () => {
    expect(assignmentOffset("print(h.pid)\nh = bash('x')", "h")).toBe("print(h.pid)".length);
    expect(assignmentOffset("a = 1; h = 2", "h")).toBe("a = 1".length);
    expect(assignmentOffset("h = 1\nh = 2", "h", 1)).toBe("h = 1".length);
    expect(assignmentOffset("if h == 1: pass\nx_h = 3\nh += 1", "h")).toBe(
      "if h == 1: pass\nx_h = 3\nh += 1".length,
    );
  });

  it("ignores a command with nothing in it", () => {
    expect(detachedBashJobs("job = bash('  ')")).toEqual([]);
    expect(detachedBashJobs("job = bash('')")).toEqual([]);
  });

  it("recognizes a later cell reading a handle's result without awaiting it", () => {
    expect(readsHandle("print(job.running)\nout = job.output()", "job")).toBe(true);
    expect(readsHandle("print(job . tail (5))", "job")).toBe(true);
    expect(readsHandle("if job.poll() is None: pass", "job")).toBe(true);
    expect(readsHandle("print(job.running, job.pid)", "job")).toBe(false);
    expect(readsHandle("print(other_job.output())", "job")).toBe(false);
  });
});

describe("reportedCommandMatches", () => {
  const literal = { variable: "h", command: "sleep 40 && echo late", isTemplate: false };
  const template = { variable: "h", command: "npx vp test run {file} --bail", isTemplate: true };

  it("matches a literal command, whole or truncated", () => {
    expect(reportedCommandMatches(literal, "sleep 40 && echo late")).toBe(true);
    expect(reportedCommandMatches(literal, "sleep 40\n... [command truncated]")).toBe(true);
    expect(reportedCommandMatches(literal, "sleep 41")).toBe(false);
  });

  it("matches an f-string job against its expanded command", () => {
    expect(reportedCommandMatches(template, "npx vp test run a/b.test.ts --bail")).toBe(true);
    expect(reportedCommandMatches(template, "npx vp test run  --bail")).toBe(true);
    expect(reportedCommandMatches(template, "npx vp test run a.ts")).toBe(false);
    expect(reportedCommandMatches(template, "npx vp te\n... [command truncated]")).toBe(true);
    expect(reportedCommandMatches(template, "make\n... [command truncated]")).toBe(false);
  });
});
