import { RunId } from "@t3tools/contracts";
import { deriveOutlivingBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import { describe, expect, it } from "vite-plus/test";

import type { TimelineEntry } from "../../session-logic";
import { deriveMessagesTimelineRows, deriveTimelineLiveRunIds } from "./MessagesTimeline.logic";

/**
 * The composer banner and the timeline's live activity row both present
 * running work. A task must never be in both.
 */

const CURRENT_RUN = "run-2";
const EARLIER_RUN = "run-1";
const latestRun = {
  runId: RunId.make(CURRENT_RUN),
  status: "running" as const,
  startedAt: "2026-01-01T00:00:00Z",
  completedAt: null,
};

type Step = {
  readonly id: string;
  readonly runId: string;
  readonly kind: "command" | "message" | "user";
  readonly status: "running" | "completed";
};

const at = (index: number) => new Date(Date.UTC(2026, 0, 1, 0, 0, index + 1)).toISOString();

function timelineEntryOf(step: Step, index: number): TimelineEntry {
  if (step.kind === "message" || step.kind === "user") {
    return {
      id: step.id,
      kind: "message",
      createdAt: at(index),
      message: {
        id: step.id,
        role: step.kind === "user" ? "user" : "assistant",
        text: "working on it",
        createdAt: at(index),
        updatedAt: at(index),
        streaming: false,
        runId: step.runId,
      },
    } as never;
  }
  return {
    id: step.id,
    kind: "work",
    createdAt: at(index),
    entry: {
      id: step.id,
      createdAt: at(index),
      runId: step.runId as never,
      label: step.id,
      command: step.id,
      requestKind: "command",
      tone: "tool",
      toolLifecycleStatus: step.status === "running" ? "inProgress" : "completed",
    },
  } as never;
}

/** What each surface presents as running, as task ids. */
function presentedTaskIds(steps: ReadonlyArray<Step>, roster: ReadonlyArray<string>) {
  const timelineEntries = steps.map(timelineEntryOf);
  const rows = deriveMessagesTimelineRows({
    timelineEntries,
    latestRun,
    isWorking: true,
    activeTurnStartedAt: "2026-01-01T00:00:00Z",
    turnDiffSummaries: [],
    supportsConversationRollback: false,
  });
  const liveRowIds = rows.flatMap((row) =>
    row.kind === "work-live"
      ? row.groupedEntries.filter((e) => e.toolLifecycleStatus === "inProgress").map((e) => e.id)
      : [],
  );
  const banner = deriveOutlivingBackgroundWork({
    providerThreads: [
      {
        id: "pt-1" as never,
        pendingBackgroundTasks: roster.map((taskId) => ({ taskId, kind: "command" as const })),
      },
    ],
    turnItems: steps
      .filter((step) => step.kind === "command")
      .map((step) => ({
        id: step.id,
        runId: step.runId,
        type: "command_execution" as const,
        status: step.status,
        title: step.id,
      })),
    foregroundRunIds: deriveTimelineLiveRunIds({
      timelineEntries,
      latestRun,
      runningRunId: null,
      isWorking: true,
    }),
  });
  return { liveRowIds, bannerIds: banner.map((task) => task.taskId) };
}

describe("banner and live timeline row", () => {
  it("do not both show a running command that later steps passed", () => {
    const { liveRowIds, bannerIds } = presentedTaskIds(
      [
        { id: "slow", runId: CURRENT_RUN, kind: "command", status: "running" },
        { id: "edit", runId: CURRENT_RUN, kind: "command", status: "completed" },
        { id: "tests", runId: CURRENT_RUN, kind: "command", status: "completed" },
      ],
      [],
    );
    expect(liveRowIds).toEqual(["slow"]);
    expect(bannerIds).toEqual([]);
  });

  it("do not both show a running command from before an assistant message", () => {
    const { liveRowIds, bannerIds } = presentedTaskIds(
      [
        { id: "slow", runId: CURRENT_RUN, kind: "command", status: "running" },
        { id: "said", runId: CURRENT_RUN, kind: "message", status: "completed" },
        { id: "tests", runId: CURRENT_RUN, kind: "command", status: "completed" },
      ],
      [],
    );
    expect(liveRowIds).toEqual(["slow"]);
    expect(bannerIds).toEqual([]);
  });

  it("split work: the live row keeps its step, the banner takes the rest", () => {
    const { liveRowIds, bannerIds } = presentedTaskIds(
      [
        { id: "dev-server", runId: EARLIER_RUN, kind: "command", status: "running" },
        { id: "prompt", runId: CURRENT_RUN, kind: "user", status: "completed" },
        { id: "build", runId: CURRENT_RUN, kind: "command", status: "running" },
      ],
      ["bash:1"],
    );
    expect(liveRowIds).toEqual(["build"]);
    expect(bannerIds.toSorted()).toEqual(["bash:1", "dev-server"]);
  });

  it("never share a task id, for generated timelines", () => {
    // Small deterministic generator (no property-test library in this repo).
    let seed = 20261002;
    const next = (bound: number) => {
      seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
      return seed % bound;
    };
    for (let round = 0; round < 300; round += 1) {
      const steps: Step[] = Array.from({ length: 1 + next(7) }, (_, index) => ({
        id: `step-${index}`,
        runId: next(4) === 0 ? EARLIER_RUN : CURRENT_RUN,
        kind: (["command", "command", "message", "user"] as const)[next(4)]!,
        status: next(2) === 0 ? "running" : "completed",
      }));
      // Roster ids sometimes name a step, as when a provider reports its own job.
      const roster = Array.from({ length: next(3) }, () =>
        next(2) === 0 ? `step-${next(7)}` : `bash:${next(3)}`,
      );
      const { liveRowIds, bannerIds } = presentedTaskIds(steps, roster);
      expect(
        bannerIds.filter((id) => liveRowIds.includes(id)),
        JSON.stringify({ steps, roster }),
      ).toEqual([]);
    }
  });
});
