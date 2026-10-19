import { commandDisplayText } from "./commandLabel.ts";
import type { WorkLogPresentationEntry } from "./presentation.ts";

/** Languages a work row's code can have. Clients map these to their highlighter. */
export type WorkEntryCodeLanguage = "python" | "shellscript";

/** Code a tool row shows, with the language its syntax highlighting uses. */
export interface WorkEntryCode {
  readonly code: string;
  readonly language: WorkEntryCodeLanguage;
}

type CodeEntry = Pick<
  WorkLogPresentationEntry,
  "command" | "rawCommand" | "structuredPayload" | "toolTitle" | "label"
>;

// Older Prime Agent rows were titled "Python: <preview>"; the code alone is enough.
const LEGACY_PYTHON_LABEL_PREFIX = "Python: ";

function withoutLegacyPythonPrefix(label: string): string {
  return label.startsWith(LEGACY_PYTHON_LABEL_PREFIX)
    ? label.slice(LEGACY_PYTHON_LABEL_PREFIX.length)
    : label;
}

const collapseWhitespace = (value: string) => value.replace(/\s+/gu, " ").trim();

/**
 * The label of a Python cell row: the provider's title, which is the cell's
 * first line. Kept apart from prose labels so it is never recapitalized.
 * Null when the entry is not a Python cell.
 */
export function pythonCellLabel(entry: CodeEntry): string | null {
  const item = entry.structuredPayload;
  if (item?.type !== "dynamic_tool" || item.toolName !== "python") return null;
  return withoutLegacyPythonPrefix(entry.toolTitle || entry.label);
}

/**
 * The code a row label shows, a shell command or a Python cell preview, so a
 * client can render it in its code font and highlight it. Returns null when
 * the label is prose, such as a group heading or a live "Running git" label.
 * Whitespace differences are ignored, so a client may pass a label it has
 * collapsed to one line.
 */
export function workEntryLabelCode(entry: CodeEntry, label: string): WorkEntryCode | null {
  if (
    entry.command &&
    collapseWhitespace(label) === collapseWhitespace(commandDisplayText(entry.command))
  ) {
    return { code: label, language: "shellscript" };
  }
  const python = pythonCellLabel(entry);
  if (python === null || python === "Python") return null;
  const code = withoutLegacyPythonPrefix(label);
  return collapseWhitespace(code) === collapseWhitespace(python)
    ? { code, language: "python" }
    : null;
}

function pythonCellSource(entry: CodeEntry): string | null {
  const item = entry.structuredPayload;
  if (item?.type !== "dynamic_tool" || item.input === null || typeof item.input !== "object") {
    return null;
  }
  const code = (item.input as Record<string, unknown>).code;
  return typeof code === "string" && code.trim().length > 0 ? code : null;
}

/**
 * The full code behind a row whose label is code: the whole Python cell, or
 * the command as the provider ran it. Null when the label is prose or the
 * entry carries no code.
 */
export function workEntryBodyCode(entry: CodeEntry, label: string): WorkEntryCode | null {
  const labelCode = workEntryLabelCode(entry, label);
  if (labelCode === null) return null;
  if (labelCode.language === "python") {
    const code = pythonCellSource(entry);
    return code === null ? null : { code, language: "python" };
  }
  const command = (entry.rawCommand ?? entry.command)?.trim();
  return command ? { code: command, language: labelCode.language } : null;
}

/** How much of a piece of code is worth tokenizing for the place it is shown. */
export type CodeHighlightWindowKind = "label" | "body";

// A row label is one truncated line; nothing past the visible width is tokenized.
const MAX_LABEL_HIGHLIGHT_CHARS = 400;
// An expanded body scrolls inside a small box. Past this, plain text is fine.
const MAX_BODY_HIGHLIGHT_CHARS = 20_000;

/**
 * Splits code into the part that is tokenized and the rest, which renders as
 * plain text so the visible characters never change. A label highlights its
 * first line only.
 */
export function splitCodeHighlightWindow(
  code: string,
  kind: CodeHighlightWindowKind,
): { readonly head: string; readonly tail: string } {
  const limit = kind === "label" ? MAX_LABEL_HIGHLIGHT_CHARS : MAX_BODY_HIGHLIGHT_CHARS;
  const firstLineEnd = kind === "label" ? code.indexOf("\n") : -1;
  const end = Math.min(limit, firstLineEnd === -1 ? code.length : firstLineEnd);
  return { head: code.slice(0, end), tail: code.slice(end) };
}
