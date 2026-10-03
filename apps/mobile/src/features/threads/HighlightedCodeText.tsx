import {
  splitCodeHighlightWindow,
  type CodeHighlightWindowKind,
  type WorkEntryCodeLanguage,
} from "@t3tools/client-runtime/work-log/entry-code";
import { memo, useMemo, type ComponentProps, type ReactNode } from "react";
import { Platform, type TextStyle } from "react-native";

import { AppText as Text } from "../../components/AppText";
import { cn } from "../../lib/cn";
import type { ReviewDiffTheme, ReviewHighlightedToken } from "../review/shikiReviewHighlighter";
import { useMarkdownCodeHighlight } from "./markdownCodeHighlightState";

function tokenStyle(token: ReviewHighlightedToken): TextStyle {
  const fontStyle = token.fontStyle ?? 0;
  return {
    ...(token.color ? { color: token.color } : null),
    ...(fontStyle & 1 ? { fontStyle: "italic" as const } : null),
    ...(fontStyle & 2 ? { fontWeight: "700" as const } : null),
  };
}

/** Lines of tokens as nested text. Keys are character offsets, which never reorder. */
function renderTokenLines(
  lines: ReadonlyArray<ReadonlyArray<ReviewHighlightedToken>>,
): ReactNode[] {
  const nodes: ReactNode[] = [];
  let offset = 0;
  lines.forEach((line, lineIndex) => {
    if (lineIndex > 0) {
      nodes.push("\n");
      offset += 1;
    }
    for (const token of line) {
      nodes.push(
        <Text key={offset} style={tokenStyle(token)}>
          {token.content}
        </Text>,
      );
      offset += token.content.length;
    }
  });
  return nodes;
}

/**
 * Monospace code text with Shiki colors. It shows the same characters in the
 * same font before the tokens arrive, so a row never changes size. Only the
 * head of the code is tokenized (see `splitCodeHighlightWindow`) and the
 * result is cached by the shared highlight atom, so a row that scrolls out and
 * back does no new work. Mounted rows are the only ones that tokenize.
 */
export const HighlightedCodeText = memo(function HighlightedCodeText(props: {
  readonly code: string;
  readonly language: WorkEntryCodeLanguage;
  readonly theme: ReviewDiffTheme;
  readonly kind: CodeHighlightWindowKind;
  readonly className?: string;
  readonly opacity?: number;
  readonly selectable?: boolean;
  readonly numberOfLines?: number;
  readonly onTextLayout?: ComponentProps<typeof Text>["onTextLayout"];
}) {
  const { head, tail } = useMemo(
    () => splitCodeHighlightWindow(props.code, props.kind),
    [props.code, props.kind],
  );
  const tokens = useMarkdownCodeHighlight({
    code: head,
    enabled: head.length > 0,
    language: props.language,
    theme: props.theme,
  });
  const body = useMemo(() => (tokens ? renderTokenLines(tokens) : head), [tokens, head]);

  return (
    <Text
      className={cn("font-mono", props.className)}
      numberOfLines={props.numberOfLines}
      ellipsizeMode="tail"
      selectable={props.selectable ?? false}
      onTextLayout={props.onTextLayout}
      style={[
        props.opacity === undefined ? null : { opacity: props.opacity },
        Platform.OS === "android" ? { includeFontPadding: false } : null,
      ]}
    >
      {body}
      {tail}
    </Text>
  );
});
