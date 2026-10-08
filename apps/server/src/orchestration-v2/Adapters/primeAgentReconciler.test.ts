import { describe, expect, it } from "@effect/vitest";

import { matchStoppedCopies, transcriptMessageKey } from "./primeAgentReconciler.ts";

const text = (value: string) => [{ type: "text", text: value }];
const prompt = (timestamp: number) => ({ role: "user", content: "Go", timestamp });
const reply = (timestamp: number, shown = "") => ({
  role: "assistant",
  content: text(shown),
  timestamp,
});
const copy = (timestamp: number, held = "") => ({
  ...reply(timestamp, held),
  stopReason: "aborted",
});
const custom = (timestamp: number) => ({ role: "custom", customType: "note", timestamp });
const toolResult = (timestamp: number) => ({ role: "toolResult", toolCallId: "call", timestamp });
const key = (message: unknown) => transcriptMessageKey(message)!;
const textOf = (message: unknown): string => {
  const content: unknown = (message as { readonly content?: unknown }).content;
  return Array.isArray(content) ? String(content[0]?.text ?? "") : "";
};

interface Shape {
  readonly name: string;
  readonly history: ReadonlyArray<unknown>;
  /** What the turn streamed, a reply with the text it showed. */
  readonly streamed: ReadonlyArray<unknown>;
  /** Streamed replies whose end T3 has not shown. */
  readonly unended: ReadonlyArray<unknown>;
  /** Copies that arrived live, with how many streamed messages arrived before each. */
  readonly arrivals?: ReadonlyArray<readonly [unknown, number]>;
  readonly removed?: ReadonlyArray<unknown>;
  readonly accounted?: ReadonlyArray<unknown>;
  /** Each matched copy with the timestamp of the reply it replaced. */
  readonly matches: ReadonlyArray<readonly [unknown, number]>;
}

const shapes: ReadonlyArray<Shape> = [
  {
    name: "a stopped reply's copy takes its place",
    history: [prompt(900), copy(3000, "Cut, then whole")],
    streamed: [prompt(900), reply(1000, "Cut")],
    unended: [reply(1000)],
    matches: [[copy(3000), 1000]],
  },
  {
    name: "a copy stamped before its reply still matches",
    history: [prompt(900), copy(950, "Cut")],
    streamed: [prompt(900), reply(1000, "Cut")],
    unended: [reply(1000)],
    matches: [[copy(950), 1000]],
  },
  {
    name: "a reply the history still holds was not replaced",
    history: [prompt(900), reply(1000), toolResult(1500), copy(5000, "Cut, other")],
    streamed: [prompt(900), reply(1000, "Cut")],
    unended: [reply(1000)],
    matches: [],
  },
  {
    name: "a copy that does not hold what the reply showed is its own reply",
    history: [prompt(900), copy(3000, "Other")],
    streamed: [prompt(900), reply(1000, "Cut")],
    unended: [reply(1000)],
    matches: [],
  },
  {
    name: "a message stored after the copy does not hide it",
    history: [prompt(900), copy(3000, "Cut"), custom(4000)],
    streamed: [prompt(900), reply(1000, "Cut")],
    unended: [reply(1000)],
    matches: [[copy(3000), 1000]],
  },
  {
    name: "a copy stored after what the turn saw next belongs to a later reply",
    history: [
      prompt(900),
      copy(3000, "First whole"),
      prompt(4000),
      reply(5000),
      prompt(6000),
      copy(8000, "First again"),
    ],
    streamed: [prompt(900), reply(1000, "First"), prompt(4000), reply(5000)],
    unended: [reply(1000)],
    matches: [[copy(3000), 1000]],
  },
  {
    name: "two copies between what the turn saw are ambiguous",
    history: [prompt(900), copy(2000, "Cut, one"), copy(3000, "Cut, two"), prompt(4000)],
    streamed: [prompt(900), reply(1000, "Cut"), prompt(4000)],
    unended: [reply(1000)],
    matches: [],
  },
  {
    name: "a reply whose prompt was lost takes the copy of the run its turn started",
    history: [prompt(900), copy(3000, "Check the logs"), prompt(1500), copy(5000, "Check it")],
    streamed: [reply(2000, "Check")],
    unended: [reply(2000)],
    accounted: [prompt(900)],
    matches: [[copy(5000), 2000]],
  },
  {
    name: "a copy stored after a reply the turn saw later is not the earlier reply's",
    history: [
      prompt(900),
      copy(3000, "Cut, one"),
      prompt(6000),
      reply(7000),
      prompt(8000),
      copy(9000, "Cut, two"),
    ],
    streamed: [reply(1000, "Cut"), reply(7000, "Later")],
    unended: [reply(1000)],
    matches: [[copy(3000), 1000]],
  },
  {
    name: "copies in two runs the turn may have started are ambiguous",
    history: [prompt(900), copy(3000, "Check the logs"), prompt(1500), copy(5000, "Check it")],
    streamed: [reply(2000, "Check")],
    unended: [reply(2000)],
    matches: [],
  },
  {
    name: "the earliest fitting copy that arrived after the reply is its copy",
    history: [prompt(900), copy(3000, "Check first"), prompt(6000), copy(8000, "Check later")],
    streamed: [prompt(900), reply(1000, "Check")],
    arrivals: [
      [copy(3000), 2],
      [copy(8000), 2],
    ],
    unended: [reply(1000)],
    matches: [[copy(3000), 1000]],
  },
  {
    name: "fitting copies known only from the history are ambiguous",
    history: [prompt(900), copy(3000, "Check first"), prompt(6000), copy(8000, "Check later")],
    streamed: [prompt(900), reply(1000, "Check")],
    unended: [reply(1000)],
    matches: [],
  },
  {
    name: "an earlier copy known only from the history makes a later live one ambiguous",
    history: [prompt(900), copy(3000, "Check first"), prompt(4000), copy(6000, "Check later")],
    streamed: [prompt(900), reply(5000, "Check")],
    arrivals: [[copy(6000), 2]],
    unended: [reply(5000)],
    matches: [],
  },
  {
    name: "a later run's copy arriving right after a reply is not proof",
    history: [prompt(900), copy(3000, "Check first"), prompt(4000), copy(6000, "Check later")],
    streamed: [prompt(900), reply(1000, "Check")],
    arrivals: [[copy(6000), 2]],
    unended: [reply(1000)],
    matches: [],
  },
  {
    name: "a copy that arrived before the reply started is not its copy",
    history: [prompt(900), copy(3000, "Check first"), prompt(4000), copy(6000, "Check later")],
    streamed: [prompt(900), reply(5000, "Check")],
    arrivals: [
      [copy(3000), 1],
      [copy(6000), 2],
    ],
    unended: [reply(5000)],
    matches: [[copy(6000), 5000]],
  },
  {
    name: "two stopped replies each take the copy that arrived after them",
    history: [prompt(900), copy(3000, "Check first"), prompt(4000), copy(6000, "Check later")],
    streamed: [prompt(900), reply(1000, "Check"), reply(5000, "Check")],
    arrivals: [
      [copy(3000), 2],
      [copy(6000), 3],
    ],
    unended: [reply(1000), reply(5000)],
    matches: [
      [copy(3000), 1000],
      [copy(6000), 5000],
    ],
  },
  {
    name: "a copy known only from the history that fits two replies goes to neither",
    history: [prompt(900), copy(3000, "Check first")],
    streamed: [prompt(900), reply(1000, "Check"), reply(5000, "Check")],
    unended: [reply(1000), reply(5000)],
    matches: [],
  },
  {
    name: "a lost steer before the copy does not hide it",
    history: [prompt(900), prompt(1500), copy(3000, "Cut, then whole")],
    streamed: [prompt(900), reply(2000, "Cut")],
    unended: [reply(2000)],
    matches: [[copy(3000), 2000]],
  },
  {
    name: "a copy stored before what the turn saw first belongs to an earlier turn",
    history: [prompt(900), copy(3000, "Check"), prompt(1500)],
    streamed: [prompt(1500), reply(2000, "Check")],
    unended: [reply(2000)],
    matches: [],
  },
  {
    name: "a reply a compaction or retry may have dropped is not matched",
    history: [custom(4000), copy(5000, "Cut, then whole")],
    streamed: [prompt(900), reply(1000, "Cut")],
    unended: [reply(1000)],
    removed: [reply(1000)],
    matches: [],
  },
  {
    name: "a copy an earlier read placed is not matched again",
    history: [prompt(900), copy(3000, "Cut")],
    streamed: [prompt(900), reply(1000, "Cut")],
    unended: [reply(1000)],
    accounted: [copy(3000)],
    matches: [],
  },
  {
    name: "an aborted reply the turn streamed is its own reply",
    history: [prompt(900), copy(3000, "Cut")],
    streamed: [prompt(900), reply(1000, "Cut"), copy(3000, "Cut")],
    unended: [reply(1000)],
    matches: [],
  },
  {
    name: "a reply that showed its end is not matched",
    history: [prompt(900), copy(3000, "Cut")],
    streamed: [prompt(900), reply(1000, "Cut")],
    unended: [],
    matches: [],
  },
  {
    name: "two stopped replies take their copies in order",
    history: [prompt(900), copy(3000, "One"), prompt(4000), copy(7000, "Two")],
    streamed: [prompt(900), reply(1000, "One"), prompt(4000), reply(6000, "Two")],
    unended: [reply(1000), reply(6000)],
    matches: [
      [copy(3000), 1000],
      [copy(7000), 6000],
    ],
  },
];

describe("matchStoppedCopies", () => {
  it.each(shapes)("$name", (shape) => {
    const shown = new Map(shape.streamed.map((message) => [key(message), textOf(message)]));
    const matches = matchStoppedCopies({
      history: shape.history,
      streamed: shape.streamed.map(key),
      arrivals: new Map((shape.arrivals ?? []).map(([message, after]) => [key(message), after])),
      unended: new Set(shape.unended.map(key)),
      removed: new Set((shape.removed ?? []).map(key)),
      accounted: new Set((shape.accounted ?? []).map(key)),
      holdsShown: (stored, replyKey) => textOf(stored).startsWith(shown.get(replyKey) ?? ""),
    });
    expect(new Map(matches)).toEqual(
      new Map(shape.matches.map(([message, replaced]) => [key(message), replaced])),
    );
  });
});
