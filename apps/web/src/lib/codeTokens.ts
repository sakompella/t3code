import type { DiffsHighlighter } from "@pierre/diffs";
import type { WorkEntryCodeLanguage } from "@t3tools/client-runtime/work-log/entry-code";

import { resolveDiffThemeName } from "./diffRendering";
import { LRUCache } from "./lruCache";

export type HighlightLanguage = WorkEntryCodeLanguage;

export interface CodeToken {
  readonly content: string;
  readonly color?: string | undefined;
  /** Shiki font style bit flags: 1 italic, 2 bold, 4 underline. */
  readonly fontStyle?: number | undefined;
}

export type CodeLines = ReadonlyArray<ReadonlyArray<CodeToken>>;

const MAX_TOKEN_CACHE_ENTRIES = 500;
const MAX_TOKEN_CACHE_BYTES = 8 * 1024 * 1024;

const tokenCache = new LRUCache<CodeLines>(MAX_TOKEN_CACHE_ENTRIES, MAX_TOKEN_CACHE_BYTES);

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
