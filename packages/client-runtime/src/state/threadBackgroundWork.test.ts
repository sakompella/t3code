import { describe, expect, it } from "vite-plus/test";

import {
  backgroundWorkTasksEqual,
  deriveRunningTurnBackgroundWork,
  resolveBackgroundWorkPillSegment,
} from "./threadBackgroundWork.ts";

const projection = (runStatus: "running" | "completed", activeProviderThreadId = "pt-1") =>
  ({
    thread: { activeProviderThreadId },
    runs: [{ id: "run-1", ordinal: 1, status: runStatus }],
    providerThreads: [
      {
        id: "pt-1",
        pendingBackgroundTasks: [{ taskId: "bash:1", description: "sleep 60", kind: "command" }],
      },
      { id: "pt-other", pendingBackgroundTasks: [{ taskId: "bash:2", kind: "command" }] },
    ],
    turnItems: [
      {
        id: "step",
        runId: "run-1",
        type: "command_execution",
        status: "running",
        title: "npm test",
        nativeItemRef: null,
      },
    ],
  }) as never;

describe("deriveRunningTurnBackgroundWork", () => {
  it("lists the roster job and leaves the running step to the feed", () => {
    expect(deriveRunningTurnBackgroundWork(projection("running"))).toEqual([
      { taskId: "bash:1", description: "sleep 60", kind: "command" },
    ]);
  });

  it("is empty once no turn runs, where the server's settled roster takes over", () => {
    expect(deriveRunningTurnBackgroundWork(projection("completed"))).toEqual([]);
  });
});

describe("backgroundWorkTasksEqual", () => {
  it("compares by task, so unchanged projections keep the same value", () => {
    const task = { taskId: "a", kind: "command", description: "x" } as const;
    expect(backgroundWorkTasksEqual([task], [{ ...task }])).toBe(true);
    expect(backgroundWorkTasksEqual([task], [{ ...task, description: "y" }])).toBe(false);
    expect(backgroundWorkTasksEqual([task], [])).toBe(false);
  });
});

describe("resolveBackgroundWorkPillSegment", () => {
  it("counts commands and leaves subagents to their own segment", () => {
    expect(
      resolveBackgroundWorkPillSegment([
        { taskId: "a", kind: "command", description: "npm run dev" },
        { taskId: "b", kind: "subagent" },
      ]),
    ).toEqual({
      label: "1 background",
      accessibilityLabel: "Running: npm run dev",
    });
    expect(resolveBackgroundWorkPillSegment([{ taskId: "b", kind: "subagent" }])).toBeNull();
    expect(resolveBackgroundWorkPillSegment([])).toBeNull();
  });
});
