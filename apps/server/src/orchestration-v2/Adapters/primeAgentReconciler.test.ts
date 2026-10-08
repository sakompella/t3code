import { describe, expect, it } from "@effect/vitest";

import { matchStoppedCopies, transcriptMessageKey } from "./primeAgentReconciler.ts";

const prompt = (timestamp: number) => ({ role: "user", content: "Go", timestamp });
const reply = (timestamp: number) => ({ role: "assistant", content: [], timestamp });
const copy = (timestamp: number) => ({ ...reply(timestamp), stopReason: "aborted" });
const custom = (timestamp: number) => ({ role: "custom", customType: "note", timestamp });
const toolResult = (timestamp: number) => ({ role: "toolResult", toolCallId: "call", timestamp });
const key = (message: unknown) => transcriptMessageKey(message)!;

interface Shape {
  readonly name: string;
  readonly history: ReadonlyArray<unknown>;
  readonly streamed: ReadonlyArray<unknown>;
  /** Streamed replies whose end T3 has not shown. */
  readonly unended: ReadonlyArray<unknown>;
  readonly streamedBeforeCompaction?: number;
  readonly accounted?: ReadonlyArray<unknown>;
  /** Each matched copy with the timestamp of the reply it replaced. */
  readonly matches: ReadonlyArray<readonly [unknown, number]>;
}

const shapes: ReadonlyArray<Shape> = [
  {
    name: "a stopped reply's copy takes its slot",
    history: [prompt(900), copy(3000)],
    streamed: [prompt(900), reply(1000)],
    unended: [reply(1000)],
    matches: [[copy(3000), 1000]],
  },
  {
    name: "a copy stamped before its reply still matches",
    history: [prompt(900), copy(950)],
    streamed: [prompt(900), reply(1000)],
    unended: [reply(1000)],
    matches: [[copy(950), 1000]],
  },
  {
    name: "a reply the history still holds was not replaced",
    history: [prompt(900), reply(1000), toolResult(1500), copy(5000)],
    streamed: [prompt(900), reply(1000)],
    unended: [reply(1000)],
    matches: [],
  },
  {
    name: "a message stored after the copy does not hide it",
    history: [prompt(900), copy(3000), custom(4000)],
    streamed: [prompt(900), reply(1000)],
    unended: [reply(1000)],
    matches: [[copy(3000), 1000]],
  },
  {
    name: "an earlier turn's copy stays apart when the turn's prompt was lost",
    history: [prompt(900), copy(3000), prompt(1500), copy(5000)],
    streamed: [reply(2000)],
    unended: [reply(2000)],
    matches: [[copy(5000), 2000]],
  },
  {
    name: "a copy stored before what the turn saw first belongs to an earlier turn",
    history: [prompt(900), copy(3000), prompt(1500)],
    streamed: [prompt(1500), reply(2000)],
    unended: [reply(2000)],
    matches: [],
  },
  {
    name: "a reply streamed before a compaction is not matched",
    history: [custom(4000), copy(5000)],
    streamed: [prompt(900), reply(1000)],
    unended: [reply(1000)],
    streamedBeforeCompaction: 2,
    matches: [],
  },
  {
    name: "a reply streamed after a compaction is matched",
    history: [custom(4000), prompt(4500), copy(5000)],
    streamed: [prompt(900), prompt(4500), reply(4600)],
    unended: [reply(4600)],
    streamedBeforeCompaction: 1,
    matches: [[copy(5000), 4600]],
  },
  {
    name: "a copy an earlier read placed is not matched again",
    history: [prompt(900), copy(3000)],
    streamed: [prompt(900), reply(1000)],
    unended: [reply(1000)],
    accounted: [copy(3000)],
    matches: [],
  },
  {
    name: "an aborted reply the turn streamed is its own reply",
    history: [prompt(900), copy(3000)],
    streamed: [prompt(900), reply(1000), copy(3000)],
    unended: [reply(1000)],
    matches: [],
  },
  {
    name: "a reply that showed its end is not matched",
    history: [prompt(900), copy(3000)],
    streamed: [prompt(900), reply(1000)],
    unended: [],
    matches: [],
  },
  {
    name: "two stopped replies take their copies in order",
    history: [prompt(900), copy(3000), prompt(4000), copy(7000)],
    streamed: [prompt(900), reply(1000), prompt(4000), reply(6000)],
    unended: [reply(1000), reply(6000)],
    matches: [
      [copy(3000), 1000],
      [copy(7000), 6000],
    ],
  },
];

describe("matchStoppedCopies", () => {
  it.each(shapes)("$name", (shape) => {
    const matches = matchStoppedCopies({
      history: shape.history,
      streamed: shape.streamed.map(key),
      unended: new Set(shape.unended.map(key)),
      streamedBeforeCompaction: shape.streamedBeforeCompaction ?? 0,
      accounted: new Set((shape.accounted ?? []).map(key)),
    });
    expect(new Map(matches)).toEqual(
      new Map(shape.matches.map(([message, replaced]) => [key(message), replaced])),
    );
  });
});
