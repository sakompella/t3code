import { describe, expect, it } from "vite-plus/test";
import type { OrchestrationV2PendingBackgroundTask } from "@t3tools/contracts";
import {
  backgroundWorkHoldsCompletion,
  deriveOutlivingBackgroundWork,
  derivePendingBackgroundWork,
  turnItemUpdateCanEndBackgroundWork,
} from "./orchestrationV2PendingBackgroundWork.ts";

describe("turnItemUpdateCanEndBackgroundWork", () => {
  it.each([
    ["a finished subagent", true, { type: "subagent", status: "completed" }],
    ["a failed background command", true, { type: "command_execution", status: "failed" }],
    ["an idle dynamic tool", true, { type: "dynamic_tool", status: "idle" }],
    ["a running command's output", false, { type: "command_execution", status: "running" }],
    ["a waiting subagent", false, { type: "subagent", status: "waiting" }],
    ["a finished assistant message", false, { type: "assistant_message", status: "completed" }],
  ] as const)("%s: %s", (_case, canEnd, item) => {
    expect(turnItemUpdateCanEndBackgroundWork(item)).toBe(canEnd);
  });
});

describe("backgroundWorkHoldsCompletion", () => {
  const task = (
    taskId: string,
    kind: OrchestrationV2PendingBackgroundTask["kind"],
  ): OrchestrationV2PendingBackgroundTask => ({ taskId, kind });

  it.each([
    ["nothing pending", false, []],
    ["a dev server", false, [task("dev", "command")]],
    ["two long-lived shells", false, [task("web", "command"), task("api", "command")]],
    ["a subagent", true, [task("review", "subagent")]],
    ["a monitor", true, [task("watch", "monitor")]],
    ["a command and a monitor", true, [task("dev", "command"), task("watch", "monitor")]],
    ["a command and a subagent", true, [task("dev", "command"), task("review", "subagent")]],
    // Also what an unknown or missing kind decodes to.
    ["work the provider cannot name", true, [task("opaque", "background_task")]],
  ] as const)("with %s pending, holds completion: %s", (_case, holds, tasks) => {
    expect(backgroundWorkHoldsCompletion(tasks)).toBe(holds);
  });
});

describe("derivePendingBackgroundWork", () => {
  it("returns empty while the latest run is not settled", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-1" as never, ordinal: 1, status: "running" },
      providerThreads: [
        {
          id: "pt-1" as never,
          pendingBackgroundTasks: [{ taskId: "bg-1", description: "sleep 20", kind: "command" }],
        },
      ],
      turnItems: [
        {
          id: "item-1" as never,
          type: "command_execution",
          status: "running",
          title: "npm test",
          nativeItemRef: null,
          input: "npm test",
        },
      ],
    });
    expect(tasks).toEqual([]);
  });

  it("does not keep a settled parent waiting for an idle child", () => {
    expect(
      derivePendingBackgroundWork({
        latestRun: { id: "run-1" as never, ordinal: 1, status: "completed" },
        providerThreads: [],
        turnItems: [{ id: "idle-child", type: "subagent", status: "idle", title: "Review" }],
      }),
    ).toEqual([]);
  });

  it("returns pending work when the latest run is waiting (post-success, pre-checkpoint)", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-1" as never, ordinal: 1, status: "waiting" },
      providerThreads: [{ id: "pt-1" as never }],
      turnItems: [
        {
          id: "item-1" as never,
          type: "command_execution",
          status: "running",
          title: "npm test",
          nativeItemRef: { nativeId: "cmd-1" },
          input: "npm test",
        },
      ],
    });
    expect(tasks).toEqual([{ taskId: "cmd-1", description: "npm test", kind: "command" }]);
  });

  it("still returns empty when the latest run is running even with background items", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-1" as never, ordinal: 1, status: "running" },
      providerThreads: [
        {
          id: "pt-1" as never,
          pendingBackgroundTasks: [{ taskId: "bg-1", description: "sleep 20", kind: "command" }],
        },
      ],
      turnItems: [
        {
          id: "item-1" as never,
          type: "subagent",
          status: "running",
          title: "review",
          nativeItemRef: { nativeId: "sub-1" },
          prompt: "review",
        },
      ],
    });
    expect(tasks).toEqual([]);
  });

  it("excludes rolled_back items when the latest run is waiting", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-2" as never, ordinal: 2, status: "waiting" },
      providerThreads: [{ id: "pt-1" as never }],
      runs: [
        { id: "run-1" as never, ordinal: 1, status: "rolled_back" },
        { id: "run-2" as never, ordinal: 2, status: "waiting" },
      ],
      turnItems: [
        {
          id: "item-old" as never,
          type: "command_execution",
          status: "running",
          title: "from rolled-back run",
          runId: "run-1",
          nativeItemRef: { nativeId: "cmd-old" },
          input: "sleep 99",
        },
        {
          id: "item-new" as never,
          type: "command_execution",
          status: "running",
          title: "still pending",
          runId: "run-2",
          nativeItemRef: { nativeId: "cmd-new" },
          input: "npm test",
        },
      ],
    });
    expect(tasks).toEqual([
      {
        taskId: "cmd-new",
        description: "still pending",
        kind: "command",
      },
    ]);
  });

  it("returns the provider-thread roster after settlement", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-1" as never, ordinal: 1, status: "completed" },
      providerThreads: [
        {
          id: "pt-1" as never,
          pendingBackgroundTasks: [
            { taskId: "bg-1", description: "Run Codex review", kind: "command" },
          ],
        },
      ],
      turnItems: [],
      activeProviderThreadId: "pt-1",
    });
    expect(tasks).toEqual([{ taskId: "bg-1", description: "Run Codex review", kind: "command" }]);
  });

  it("includes nonterminal turn items and excludes completed ones", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-1" as never, ordinal: 1, status: "completed" },
      providerThreads: [{ id: "pt-1" as never }],
      turnItems: [
        {
          id: "item-1" as never,
          type: "command_execution",
          status: "running",
          title: "npm test",
          nativeItemRef: { nativeId: "cmd-1" },
          input: "npm test",
        },
        {
          id: "item-2" as never,
          type: "command_execution",
          status: "completed",
          title: "done",
          nativeItemRef: { nativeId: "cmd-2" },
          input: "echo done",
        },
      ],
    });
    expect(tasks).toEqual([{ taskId: "cmd-1", description: "npm test", kind: "command" }]);
  });

  it("trims normalized background-work descriptions", () => {
    const base = {
      latestRun: { id: "run-1" as never, ordinal: 1, status: "completed" as const },
      providerThreads: [
        {
          id: "pt-1" as never,
          pendingBackgroundTasks: [
            {
              taskId: "native-task",
              description: "  native background work  ",
              kind: "background_task" as const,
            },
          ],
        },
      ],
    };
    expect(
      derivePendingBackgroundWork({
        ...base,
        turnItems: [
          {
            id: "item-command" as never,
            type: "command_execution",
            status: "running",
            title: "  npm test  ",
            nativeItemRef: null,
          },
          {
            id: "item-tool" as never,
            type: "dynamic_tool",
            status: "running",
            title: null,
            nativeItemRef: null,
            toolName: "  browser.search  ",
          } as never,
        ],
      }),
    ).toEqual([
      {
        taskId: "native-task",
        description: "native background work",
        kind: "background_task",
      },
      {
        taskId: "item-command",
        description: "npm test",
        kind: "command",
      },
      {
        taskId: "item-tool",
        description: "browser.search",
        kind: "background_task",
      },
    ]);
  });

  it("dedupes roster entries against turn items by native task id", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-1" as never, ordinal: 1, status: "completed" },
      providerThreads: [
        {
          id: "pt-1" as never,
          pendingBackgroundTasks: [
            { taskId: "task-9", description: "Agent review", kind: "background_task" },
          ],
        },
      ],
      turnItems: [
        {
          id: "item-sub" as never,
          type: "subagent",
          status: "running",
          title: "Agent review",
          nativeItemRef: { nativeId: "task-9" },
          prompt: "review the plan",
        },
      ],
    });
    expect(tasks).toEqual([
      { taskId: "task-9", description: "Agent review", kind: "background_task" },
    ]);
  });

  it("excludes Grok persistent monitors", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-1" as never, ordinal: 1, status: "completed" },
      providerThreads: [{ id: "pt-1" as never }],
      turnItems: [
        {
          id: "item-1" as never,
          type: "dynamic_tool",
          status: "running",
          title: "monitor logs",
          nativeItemRef: { nativeId: "mon-1" },
          input: { persistent: true, command: "tail -f" },
        },
        {
          id: "item-2" as never,
          type: "dynamic_tool",
          status: "running",
          title: "finite monitor",
          nativeItemRef: { nativeId: "mon-2" },
          input: { persistent: false, command: "sleep 5" },
        },
      ],
    });
    expect(tasks).toEqual([
      { taskId: "mon-2", description: "finite monitor", kind: "background_task" },
    ]);
  });

  it("returns multiple tasks with stable ordering from insertion", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-1" as never, ordinal: 1, status: "completed" },
      providerThreads: [
        {
          id: "pt-1" as never,
          pendingBackgroundTasks: [
            { taskId: "bg-1", description: "first", kind: "command" },
            { taskId: "bg-2", description: "second", kind: "command" },
          ],
        },
      ],
      turnItems: [
        {
          id: "item-1" as never,
          type: "command_execution",
          status: "running",
          title: "third",
          nativeItemRef: { nativeId: "cmd-3" },
          input: "third",
        },
      ],
    });
    expect(tasks.map((task) => task.taskId)).toEqual(["bg-1", "bg-2", "cmd-3"]);
  });

  it("excludes turn items owned by a rolled_back run", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-1" as never, ordinal: 1, status: "rolled_back" },
      providerThreads: [{ id: "pt-1" as never }],
      runs: [{ id: "run-1" as never, ordinal: 1, status: "rolled_back" }],
      turnItems: [
        {
          id: "item-1" as never,
          type: "command_execution",
          status: "running",
          title: "abandoned",
          runId: "run-1",
          nativeItemRef: { nativeId: "cmd-1" },
          input: "sleep 99",
        },
      ],
    });
    expect(tasks).toEqual([]);
  });

  it("returns empty when the latest run is rolled_back even with a nonempty roster", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-1" as never, ordinal: 1, status: "rolled_back" },
      providerThreads: [
        {
          id: "pt-1" as never,
          pendingBackgroundTasks: [{ taskId: "bg-1", description: "sleep 20", kind: "command" }],
        },
      ],
      turnItems: [
        {
          id: "item-1" as never,
          type: "command_execution",
          status: "running",
          title: "abandoned",
          runId: "run-1",
          nativeItemRef: { nativeId: "cmd-1" },
          input: "sleep 99",
        },
      ],
    });
    expect(tasks).toEqual([]);
  });

  it("excludes an older rolled_back nonterminal item when the latest run is completed", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-2" as never, ordinal: 2, status: "completed" },
      providerThreads: [{ id: "pt-1" as never }],
      runs: [
        { id: "run-1" as never, ordinal: 1, status: "rolled_back" },
        { id: "run-2" as never, ordinal: 2, status: "completed" },
      ],
      turnItems: [
        {
          id: "item-old" as never,
          type: "command_execution",
          status: "running",
          title: "from rolled-back run",
          runId: "run-1",
          nativeItemRef: { nativeId: "cmd-old" },
          input: "sleep 99",
        },
        {
          id: "item-new" as never,
          type: "command_execution",
          status: "running",
          title: "still pending",
          runId: "run-2",
          nativeItemRef: { nativeId: "cmd-new" },
          input: "npm test",
        },
      ],
    });
    expect(tasks).toEqual([
      {
        taskId: "cmd-new",
        description: "still pending",
        kind: "command",
      },
    ]);
  });

  it("does not reclassify an older active run as background work", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-2" as never, ordinal: 2, status: "cancelled" },
      providerThreads: [
        {
          id: "pt-1" as never,
          pendingBackgroundTasks: [{ taskId: "provider-task", kind: "background_task" }],
        },
      ],
      runs: [
        { id: "run-1" as never, ordinal: 1, status: "running" },
        { id: "run-2" as never, ordinal: 2, status: "cancelled" },
      ],
      turnItems: [
        {
          id: "item-active" as never,
          type: "command_execution",
          status: "running",
          title: "foreground work",
          runId: "run-1",
          nativeItemRef: { nativeId: "cmd-active" },
          input: "vp check",
        },
      ],
    });

    expect(tasks).toEqual([]);
  });

  it("includes items with a null run id even when runs list has rolled_back rows", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-1" as never, ordinal: 1, status: "completed" },
      providerThreads: [{ id: "pt-1" as never }],
      runs: [{ id: "run-1" as never, ordinal: 1, status: "rolled_back" }],
      turnItems: [
        {
          id: "item-null-run" as never,
          type: "command_execution",
          status: "running",
          title: "orphan item",
          runId: null,
          nativeItemRef: { nativeId: "cmd-null" },
          input: "echo orphan",
        },
      ],
    });
    expect(tasks).toEqual([
      {
        taskId: "cmd-null",
        description: "orphan item",
        kind: "command",
      },
    ]);
  });
});

describe("derivePendingBackgroundWork kinds", () => {
  it("keeps the roster's kinds and names turn items by their type", () => {
    const tasks = derivePendingBackgroundWork({
      latestRun: { id: "run-1" as never, ordinal: 1, status: "completed" },
      providerThreads: [
        {
          id: "pt-1" as never,
          pendingBackgroundTasks: [
            { taskId: "bash", kind: "command" },
            { taskId: "watch", kind: "monitor" },
            { taskId: "workflow", kind: "background_task" },
          ],
        },
      ],
      turnItems: [
        {
          id: "item-sub" as never,
          type: "subagent",
          status: "running",
          title: "Review src/math.ts",
          nativeItemRef: { nativeId: "sub" },
          childThreadId: "thread:child" as never,
        },
        {
          id: "item-cmd" as never,
          type: "command_execution",
          status: "running",
          title: null,
          nativeItemRef: { nativeId: "cmd" },
          input: "npm test",
        },
      ],
    });
    expect(tasks).toEqual([
      { taskId: "bash", kind: "command" },
      { taskId: "watch", kind: "monitor" },
      { taskId: "workflow", kind: "background_task" },
      {
        taskId: "sub",
        description: "Review src/math.ts",
        kind: "subagent",
        childThreadId: "thread:child",
      },
      { taskId: "cmd", description: "npm test", kind: "command" },
    ]);
  });
});

describe("deriveOutlivingBackgroundWork", () => {
  const item = (
    id: string,
    runId: string | null,
    type: "command_execution" | "subagent" | "assistant_message",
    status: "running" | "completed",
  ) => ({ id, runId, type, status, title: id });
  const bashJob = { taskId: "bash:1", description: "sleep 60", kind: "command" } as const;
  const rosterOf = (...tasks: Array<OrchestrationV2PendingBackgroundTask>) => [
    { id: "pt-1" as never, pendingBackgroundTasks: tasks },
  ];

  it("lists a roster job that a finished cell left behind", () => {
    expect(
      deriveOutlivingBackgroundWork({
        providerThreads: rosterOf(bashJob),
        turnItems: [item("cell-1", "run-2", "command_execution", "completed")],
        foregroundRunIds: new Set(["run-2"]),
      }),
    ).toEqual([bashJob]);
  });

  it("keeps an active step of the running turn out, even after later steps finished", () => {
    expect(
      deriveOutlivingBackgroundWork({
        providerThreads: [],
        turnItems: [
          item("slow", "run-2", "command_execution", "running"),
          item("edit", "run-2", "assistant_message", "completed"),
          item("tests", "run-2", "command_execution", "completed"),
        ],
        foregroundRunIds: new Set(["run-2"]),
      }),
    ).toEqual([]);
  });

  it("keeps parallel steps and a step before an assistant message out", () => {
    expect(
      deriveOutlivingBackgroundWork({
        providerThreads: [],
        turnItems: [
          item("a", "run-2", "subagent", "running"),
          item("said", "run-2", "assistant_message", "completed"),
          item("b", "run-2", "subagent", "running"),
        ],
        foregroundRunIds: new Set(["run-2"]),
      }),
    ).toEqual([]);
  });

  it("drops a roster task whose item is the live row", () => {
    expect(
      deriveOutlivingBackgroundWork({
        providerThreads: rosterOf(bashJob, { taskId: "other", kind: "command" }),
        turnItems: [
          {
            ...item("live", "run-2", "command_execution", "running"),
            nativeItemRef: { nativeId: "bash:1" },
          },
        ],
        foregroundRunIds: new Set(["run-2"]),
      }),
    ).toEqual([{ taskId: "other", kind: "command" }]);
  });

  it("lists active work an earlier run left behind", () => {
    expect(
      deriveOutlivingBackgroundWork({
        providerThreads: [],
        turnItems: [
          item("dev-server", "run-1", "command_execution", "running"),
          item("review", "run-2", "subagent", "running"),
        ],
        foregroundRunIds: new Set(["run-2"]),
      }),
    ).toEqual([{ taskId: "dev-server", description: "dev-server", kind: "command" }]);
  });

  it("drops work from rolled back runs", () => {
    expect(
      deriveOutlivingBackgroundWork({
        providerThreads: [],
        runs: [{ id: "run-1" as never, ordinal: 1, status: "rolled_back" }],
        turnItems: [item("old", "run-1", "command_execution", "running")],
        foregroundRunIds: new Set(["run-2"]),
      }),
    ).toEqual([]);
  });
});
