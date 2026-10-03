/* oxlint-disable react/no-array-index-key -- Token lines and tokens are positional and never reorder. */
import { Fragment, memo, Suspense, use, type CSSProperties } from "react";

import {
  splitHighlightWindow,
  tokenizeCode,
  type CodeLines,
  type CodeToken,
  type HighlightLanguage,
  type HighlightWindowKind,
} from "~/lib/codeTokens";
import { getSyntaxHighlighterPromise } from "~/lib/syntaxHighlighting";

import { RenderErrorBoundary } from "../RenderErrorBoundary";

function tokenStyle(token: CodeToken): CSSProperties {
  const fontStyle = token.fontStyle ?? 0;
  return {
    ...(token.color ? { color: token.color } : {}),
    ...(fontStyle & 1 ? { fontStyle: "italic" } : {}),
    ...(fontStyle & 2 ? { fontWeight: 700 } : {}),
    ...(fontStyle & 4 ? { textDecoration: "underline" } : {}),
  };
}

function TokenLines(props: { readonly lines: CodeLines }) {
  return props.lines.map((tokens, lineIndex) => (
    <Fragment key={lineIndex}>
      {lineIndex > 0 ? "\n" : null}
      {tokens.map((token, tokenIndex) => (
        <span key={tokenIndex} style={tokenStyle(token)}>
          {token.content}
        </span>
      ))}
    </Fragment>
  ));
}

function HighlightedHead(props: {
  readonly code: string;
  readonly language: HighlightLanguage;
  readonly theme: "light" | "dark";
}) {
  const highlighter = use(getSyntaxHighlighterPromise(props.language));
  return <TokenLines lines={tokenizeCode(highlighter, props.code, props.language, props.theme)} />;
}

/**
 * Inline syntax-highlighted code. The same characters render while the
 * highlighter loads, so the swap never moves layout. Only the head is
 * tokenized (see `splitHighlightWindow`); the tail stays plain text.
 */
export const HighlightedCode = memo(function HighlightedCode(props: {
  readonly code: string;
  readonly language: HighlightLanguage;
  readonly theme: "light" | "dark";
  readonly kind: HighlightWindowKind;
}) {
  const { head, tail } = splitHighlightWindow(props.code, props.kind);
  const plainHead = <>{head}</>;
  return (
    <>
      <RenderErrorBoundary fallback={plainHead} resetKeys={[head, props.language]}>
        <Suspense fallback={plainHead}>
          <HighlightedHead code={head} language={props.language} theme={props.theme} />
        </Suspense>
      </RenderErrorBoundary>
      {tail}
    </>
  );
});
