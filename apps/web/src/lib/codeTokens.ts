import type { DiffsHighlighter } from "@pierre/diffs";

import { resolveDiffThemeName } from "./diffRendering";
import { LRUCache } from "./lruCache";

export type HighlightLanguage = "python" | "shellscript";

export interface CodeToken {
  readonly content: string;
  readonly color?: string | undefined;
  /** Shiki font style bit flags: 1 italic, 2 bold, 4 underline. */
  readonly fontStyle?: number | undefined;
}

export type CodeLines = ReadonlyArray<ReadonlyArray<CodeToken>>;

/** How much of a piece of code is worth tokenizing for the place it is shown. */
export type HighlightWindowKind = "label" | "body";

// A row label is one truncated line; nothing past the visible width is tokenized.
const MAX_LABEL_HIGHLIGHT_CHARS = 400;
// An expanded body scrolls inside a small box. Past this, plain text is fine.
const MAX_BODY_HIGHLIGHT_CHARS = 20_000;

const MAX_TOKEN_CACHE_ENTRIES = 500;
const MAX_TOKEN_CACHE_BYTES = 8 * 1024 * 1024;

const tokenCache = new LRUCache<CodeLines>(MAX_TOKEN_CACHE_ENTRIES, MAX_TOKEN_CACHE_BYTES);

/**
 * Splits code into the part that is tokenized and the rest, which renders as
 * plain text so the visible characters never change. A label highlights its
 * first line only.
 */
export function splitHighlightWindow(
  code: string,
  kind: HighlightWindowKind,
): { readonly head: string; readonly tail: string } {
  const limit = kind === "label" ? MAX_LABEL_HIGHLIGHT_CHARS : MAX_BODY_HIGHLIGHT_CHARS;
  const firstLineEnd = kind === "label" ? code.indexOf("\n") : -1;
  const end = Math.min(limit, firstLineEnd === -1 ? code.length : firstLineEnd);
  return { head: code.slice(0, end), tail: code.slice(end) };
}

export function codeTokenCacheKey(code: string, language: HighlightLanguage, theme: string) {
  return `${theme}\u0000${language}\u0000${code}`;
}

function plainLines(code: string): CodeLines {
  return code.split("\n").map((line) => [{ content: line }]);
}

/**
 * Tokenizes code with the shared highlighter, memoized by (code, language,
 * theme) in a bounded LRU so a virtualized row that remounts while scrolling
 * never tokenizes twice. A language the highlighter cannot handle yields plain
 * lines, and that result is cached too.
 */
export function tokenizeCode(
  highlighter: Pick<DiffsHighlighter, "codeToTokens">,
  code: string,
  language: HighlightLanguage,
  theme: "light" | "dark",
): CodeLines {
  const themeName = resolveDiffThemeName(theme);
  const key = codeTokenCacheKey(code, language, themeName);
  const cached = tokenCache.get(key);
  if (cached) return cached;

  let lines: CodeLines;
  try {
    lines = highlighter
      .codeToTokens(code, { lang: language, theme: themeName })
      .tokens.map((line) =>
        line.map(({ content, color, fontStyle }) => ({ content, color, fontStyle })),
      );
  } catch {
    lines = plainLines(code);
  }
  tokenCache.set(key, lines, code.length * 8 + 256);
  return lines;
}
