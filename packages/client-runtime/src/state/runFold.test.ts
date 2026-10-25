import { RunId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import { runFoldSegmentKey, runFoldSegmentKeyIsForRun, runFoldSegmentLabel } from "./runFold.ts";

const at = (second: number) =>
  DateTime.formatIso(DateTime.makeUnsafe(1767225600000 + second * 1000));
const span = (start: number, end: number, isFirst: boolean, isLast: boolean) => ({
  start: at(start),
  end: at(end),
  isFirst,
  isLast,
});

describe("runFoldSegmentKey", () => {
  it("keeps the bare run id for the first segment and a distinct key for later ones", () => {
    const run = RunId.make("run-1");
    expect(runFoldSegmentKey(run, null)).toBe("run-1");
    expect(runFoldSegmentKey(run, "steer-a")).not.toBe(runFoldSegmentKey(run, "steer-b"));
  });

  it("matches every segment of its own run and none of another run", () => {
    const run = RunId.make("run-1");
    const keys = [runFoldSegmentKey(run, null), runFoldSegmentKey(run, "steer-a")];
    expect(keys.every((key) => runFoldSegmentKeyIsForRun(key, run))).toBe(true);
    const otherRunKeys = [RunId.make("run-10"), RunId.make("run")].flatMap((other) => [
      runFoldSegmentKey(other, null),
      runFoldSegmentKey(other, "steer-a"),
    ]);
    expect(otherRunKeys.some((key) => runFoldSegmentKeyIsForRun(key, run))).toBe(false);
  });
});

describe("runFoldSegmentLabel", () => {
  const run = { startedAt: at(0), completedAt: at(50) };

  it("uses the run's recorded bounds for a run with one segment", () => {
    expect(runFoldSegmentLabel({ segment: span(2, 20, true, true), run, stopped: false })).toBe(
      "Worked for 50s",
    );
  });

  it("times middle segments from their own bounds", () => {
    expect(runFoldSegmentLabel({ segment: span(10, 25, false, false), run, stopped: false })).toBe(
      "Worked for 15s",
    );
    expect(runFoldSegmentLabel({ segment: span(2, 20, true, false), run, stopped: false })).toBe(
      "Worked for 20s",
    );
    expect(runFoldSegmentLabel({ segment: span(30, 40, false, true), run, stopped: false })).toBe(
      "Worked for 20s",
    );
  });

  it("falls back to segment bounds when the run's timing is incomplete", () => {
    expect(
      runFoldSegmentLabel({
        segment: span(2, 20, true, true),
        run: { startedAt: at(0), completedAt: null },
        stopped: false,
      }),
    ).toBe("Worked for 18s");
  });

  it("says the user stopped only on the last segment", () => {
    expect(runFoldSegmentLabel({ segment: span(30, 40, false, true), run, stopped: true })).toBe(
      "You stopped after 20s",
    );
    expect(runFoldSegmentLabel({ segment: span(2, 20, true, false), run, stopped: true })).toBe(
      "Worked for 20s",
    );
  });
});
