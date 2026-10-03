import type { DiffsHighlighter } from "@pierre/diffs";
import { describe, expect, it, vi } from "vite-plus/test";

import { codeTokenCacheKey, splitHighlightWindow, tokenizeCode } from "./codeTokens";
import { getSyntaxHighlighterPromise } from "./syntaxHighlighting";

function fakeHighlighter(codeToTokens: DiffsHighlighter["codeToTokens"]) {
  return { codeToTokens: vi.fn(codeToTokens) };
}

const oneLine = (code: string) => ({
  tokens: [[{ content: code, offset: 0, color: "#fff", fontStyle: 0 }]],
});

describe("splitHighlightWindow", () => {
  it("highlights only the first line of a label", () => {
    expect(splitHighlightWindow("import os\nprint(1)", "label")).toEqual({
      head: "import os",
      tail: "\nprint(1)",
    });
  });

  it("caps how much of a long label is tokenized and keeps every character", () => {
    const code = "x".repeat(1_000);
    const { head, tail } = splitHighlightWindow(code, "label");
    expect(head.length).toBeLessThan(code.length);
    expect(head + tail).toBe(code);
  });

  it("keeps multiple lines in a body and caps very large ones", () => {
    expect(splitHighlightWindow("a\nb", "body")).toEqual({ head: "a\nb", tail: "" });
    const huge = "y".repeat(100_000);
    const { head, tail } = splitHighlightWindow(huge, "body");
    expect(head.length).toBeLessThan(huge.length);
    expect(head + tail).toBe(huge);
  });
});

describe("tokenizeCode", () => {
  it("tokenizes once per (code, language, theme)", () => {
    const highlighter = fakeHighlighter((code) => oneLine(code));
    const first = tokenizeCode(highlighter, "echo cache-key-test", "shellscript", "dark");
    expect(tokenizeCode(highlighter, "echo cache-key-test", "shellscript", "dark")).toBe(first);
    expect(highlighter.codeToTokens).toHaveBeenCalledTimes(1);

    tokenizeCode(highlighter, "echo cache-key-test", "shellscript", "light");
    tokenizeCode(highlighter, "echo cache-key-test", "python", "dark");
    expect(highlighter.codeToTokens).toHaveBeenCalledTimes(3);
  });

  it("falls back to cached plain lines when the highlighter throws", () => {
    const highlighter = fakeHighlighter(() => {
      throw new Error("unsupported");
    });
    const lines = tokenizeCode(highlighter, "a\nb", "python", "dark");
    expect(lines).toEqual([[{ content: "a" }], [{ content: "b" }]]);
    tokenizeCode(highlighter, "a\nb", "python", "dark");
    expect(highlighter.codeToTokens).toHaveBeenCalledTimes(1);
  });

  it("keys by code, language and theme", () => {
    const keys = [
      codeTokenCacheKey("a", "python", "pierre-dark"),
      codeTokenCacheKey("a", "shellscript", "pierre-dark"),
      codeTokenCacheKey("a", "python", "pierre-light"),
      codeTokenCacheKey("b", "python", "pierre-dark"),
    ];
    expect(new Set(keys).size).toBe(keys.length);
  });

  it("colors Python and shell with the real highlighter", async () => {
    for (const [language, code] of [
      ["python", "def f(x): return 'a' + str(x)"],
      ["shellscript", 'git log --oneline | head -5 && echo "done"'],
    ] as const) {
      const highlighter = await getSyntaxHighlighterPromise(language);
      const [line] = tokenizeCode(highlighter, code, language, "dark");
      expect(line?.map((token) => token.content).join("")).toBe(code);
      expect(new Set(line?.map((token) => token.color)).size).toBeGreaterThan(1);
    }
  });
});
