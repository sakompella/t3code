/**
 * Classifies Prime Agent `ipython` tool cells so the timeline can show shell
 * work as commands and everything else as Python.
 *
 * Prime Agent runs every action through one persistent IPython kernel. Its
 * own TUI (core/tools/ipython-cell-code.js and code-preview.js, MIT) labels a
 * cell `bash` when it starts with a `%%bash` cell magic, or when its main
 * statement is a call to the kernel's `bash("...")` helper with a plain string
 * literal. This ports those two rules; Prime Agent's line scoring for
 * one-line previews is presentation-only and intentionally not copied.
 */

export type IpythonCell =
  | { readonly kind: "bash"; readonly command: string }
  | { readonly kind: "python"; readonly code: string };

const BASH_CELL_MAGIC = /^(?:[ \t]*\r?\n)*[ \t]*%%bash\b[^\r\n]*(?:\r?\n|$)/;
const BASH_HELPER_CALL =
  /^(?:[A-Za-z_][A-Za-z0-9_]*\s*=\s*)?(?:await\s+)?bash\s*\(\s*([rR]?)("""|'''|"|')/;
// Statements that can surround a helper call without making the cell "Python work".
const NOISE_STATEMENT = /^(?:$|#|import\s|from\s+\S+\s+import\s|print\s*\()/;
const SIMPLE_ESCAPES: Record<string, string> = {
  "\n": "",
  "\\": "\\",
  '"': '"',
  "'": "'",
  n: "\n",
  r: "\r",
  t: "\t",
};

function scanStringLiteral(
  code: string,
  start: number,
  quote: string,
  raw: boolean,
): { readonly value: string; readonly end: number } | null {
  let value = "";
  let index = start;
  while (index < code.length) {
    const char = code[index]!;
    if (char === "\\" && index + 1 < code.length) {
      const next = code[index + 1]!;
      if (raw) {
        value += char + next;
      } else {
        const escaped = SIMPLE_ESCAPES[next];
        // Numeric and unicode escapes would need a real Python parser.
        if (escaped === undefined) return null;
        value += escaped;
      }
      index += 2;
      continue;
    }
    if (code.startsWith(quote, index)) return { value, end: index + quote.length };
    if (quote.length === 1 && char === "\n") return null;
    value += char;
    index += 1;
  }
  return null;
}

function isNoise(statement: string): boolean {
  return NOISE_STATEMENT.test(statement.trim());
}

/** Returns the helper's command when the cell is only a `bash("...")` call plus noise. */
function bashHelperCommand(code: string): string | null {
  const lines = code.split("\n");
  const firstWork = lines.findIndex((line) => !isNoise(line));
  if (firstWork < 0) return null;
  const body = lines.slice(firstWork).join("\n").trimStart();
  const call = BASH_HELPER_CALL.exec(body);
  if (call === null) return null;
  const literal = scanStringLiteral(body, call[0].length, call[2]!, call[1] !== "");
  if (literal === null) return null;
  // The command must be the first argument; keyword arguments may follow,
  // but anything that computes the command is left as Python.
  const closing = /^\s*(?:,[^\n;()]*)?\)/.exec(body.slice(literal.end));
  if (closing === null) return null;
  const trailing = body.slice(literal.end + closing[0].length);
  const trailingStatements = trailing.split(/[;\n]/);
  return trailingStatements.every(isNoise) ? literal.value : null;
}

export function classifyIpythonCell(code: string): IpythonCell {
  const magic = BASH_CELL_MAGIC.exec(code);
  if (magic !== null) {
    return { kind: "bash", command: code.slice(magic[0].length).trimEnd() };
  }
  const command = bashHelperCommand(code);
  return command === null ? { kind: "python", code } : { kind: "bash", command };
}

const PREVIEW_MAX_LENGTH = 64;
const PREVIEW_SKIPPED_LINE = /^(?:$|#|import\s|from\s+\S+\s+import\s)/;

/** One line summarizing a Python cell: its first statement that is not an import or comment. */
export function previewPythonCell(code: string): string {
  const line = code
    .split("\n")
    .map((candidate) => candidate.trim())
    .find((candidate) => !PREVIEW_SKIPPED_LINE.test(candidate));
  const collapsed = (line ?? "").replace(/\s+/g, " ");
  return collapsed.length <= PREVIEW_MAX_LENGTH
    ? collapsed
    : `${collapsed.slice(0, PREVIEW_MAX_LENGTH - 1).trimEnd()}…`;
}

export interface DetachedBashJob {
  /** The handle's variable, or null when the handle was discarded. */
  readonly variable: string | null;
  readonly command: string;
}

const UNAWAITED_BASH_HELPER =
  /(?:^|[\n;])[ \t]*(?:([A-Za-z_][A-Za-z0-9_]*)\s*=\s*)?bash\s*\(\s*([rR]?)("""|'''|"|')/g;

/**
 * Shell commands a cell started as background jobs. Prime Agent treats a
 * `bash(...)` handle as detached when the cell that created it never awaits
 * it; only those jobs report back later with an `async_bash_completion`.
 */
export function detachedBashJobs(code: string): ReadonlyArray<DetachedBashJob> {
  const jobs: Array<DetachedBashJob> = [];
  for (const match of code.matchAll(UNAWAITED_BASH_HELPER)) {
    const variable = match[1] ?? null;
    const literal = scanStringLiteral(
      code,
      (match.index ?? 0) + match[0].length,
      match[3]!,
      match[2] !== "",
    );
    if (literal === null || literal.value.length === 0) continue;
    if (variable !== null && awaitsHandle(code, variable)) continue;
    jobs.push({ variable, command: literal.value });
  }
  return jobs;
}

/** Whether a cell awaits a handle, which consumes its result and its completion notice. */
export function awaitsHandle(code: string, variable: string): boolean {
  return new RegExp(`\\bawait\\s+${variable}\\b`).test(code);
}
