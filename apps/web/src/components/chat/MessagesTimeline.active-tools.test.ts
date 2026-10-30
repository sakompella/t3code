import { RunId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { deriveMessagesTimelineRows, liveWorkEntryLabel } from "./MessagesTimeline.logic";

describe("long active tool groups", () => {
  it.each([false, true])(
    "preserves chronological order and the latest activity, expanded=%s",
    (expanded) => {
      const runId = RunId.make("long-active-run");
      const createdAt = "2026-10-12T00:00:00Z";
      const timelineEntries = Array.from({ length: 1_000 }, (_, index) => ({
        id: `work-entry-${index}`,
        kind: "work" as const,
        createdAt,
        entry: {
          id: `work-${index}`,
          createdAt,
          runId,
          label: `Tool ${index}`,
          tone: index % 2 === 0 ? ("thinking" as const) : ("tool" as const),
          itemType: index % 2 === 0 ? ("reasoning" as const) : ("command_execution" as const),
          ...(index % 2 === 0 ? { detail: `Thought ${index}` } : {}),
          toolLifecycleStatus: index === 999 ? ("inProgress" as const) : ("completed" as const),
        },
      }));
      const rows = deriveMessagesTimelineRows({
        timelineEntries,
        isWorking: true,
        runningRunId: runId,
        activeTurnStartedAt: createdAt,
        turnDiffSummaries: [],
        supportsConversationRollback: false,
        expandedWorkGroupIds: new Set(expanded ? ["work-group:work-entry-0"] : []),
      });
      const allIds = timelineEntries.map(({ entry }) => entry.id);
      const live = rows.find((row) => row.kind === "work-live");
      const details = rows.filter((row) => row.kind === "work");
      if (expanded) {
        // An expanded live group shows its toggle and every step, marking the running one.
        expect(live).toBeUndefined();
        expect(rows.find((row) => row.kind === "work-toggle")?.groupId).toBe(
          "work-group:work-entry-0",
        );
        expect(details).toHaveLength(1);
        expect(details[0]?.groupedEntries.map(({ id }) => id)).toEqual(allIds);
        expect(details[0]?.liveEntryId).toBe("work-999");
        return;
      }
      expect(live).toBeDefined();
      expect(live?.entry.id).toBe("work-999");
      expect(live && liveWorkEntryLabel(live.entry, undefined, live.active)).toContain("999");
      expect(live?.thought?.id).toBe("work-998");
      expect(live?.groupedEntries.map(({ id }) => id)).toEqual(allIds);
      expect(live?.groupId).toBe("work-group:work-entry-0");
      expect(details).toHaveLength(0);
    },
  );
});
