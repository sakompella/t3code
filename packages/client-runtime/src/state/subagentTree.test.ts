import { NodeId, type OrchestrationV2Subagent } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import {
  buildLiveSubagentTree,
  selectSubagentsForBackgroundTasks,
  type SubagentTreeSource,
} from "./subagentTree.js";

const ROOT = NodeId.make("node:run:root");

function subagent(
  id: string,
  options: {
    readonly parent?: string;
    readonly status?: OrchestrationV2Subagent["status"];
    readonly startedSecond?: number;
    readonly title?: string | null;
    readonly prompt?: string;
  } = {},
): SubagentTreeSource {
  const at = DateTime.makeUnsafe(Date.UTC(2026, 9, 1, 0, 0, options.startedSecond ?? 0));
  return {
    id: NodeId.make(id),
    parentNodeId: options.parent === undefined ? ROOT : NodeId.make(options.parent),
    title: options.title === undefined ? id : options.title,
    prompt: options.prompt ?? `task for ${id}`,
    model: "cpa-claude/claude-opus-5-5",
    status: options.status ?? "running",
    childThreadId: null,
    startedAt: at,
    completedAt: null,
    updatedAt: at,
  };
}

const shape = (nodes: ReturnType<typeof buildLiveSubagentTree>["roots"]): unknown =>
  nodes.map((node) => [node.title, node.descendantCount, shape(node.children)]);

describe("buildLiveSubagentTree", () => {
  it("nests live subagents under their parent subagent in start order", () => {
    const tree = buildLiveSubagentTree([
      subagent("beta", { startedSecond: 2 }),
      subagent("gamma", { parent: "alpha", startedSecond: 3, status: "pending" }),
      subagent("alpha", { startedSecond: 1 }),
      subagent("delta", { parent: "gamma", startedSecond: 4 }),
    ]);
    expect(shape(tree.roots)).toEqual([
      ["alpha", 2, [["gamma", 1, [["delta", 0, []]]]]],
      ["beta", 0, []],
    ]);
    expect([tree.runningCount, tree.waitingCount]).toEqual([3, 1]);
  });

  it("drops finished subagents and promotes their live children to roots", () => {
    const tree = buildLiveSubagentTree([
      subagent("alpha", { status: "completed" }),
      subagent("gamma", { parent: "alpha" }),
      subagent("beta", { status: "failed" }),
    ]);
    expect(shape(tree.roots)).toEqual([["gamma", 0, []]]);
  });

  it("titles untitled subagents from their task, cut to one line", () => {
    const tree = buildLiveSubagentTree([
      subagent("a", { title: null, prompt: `Review\n${"the parser ".repeat(12)}` }),
    ]);
    expect(tree.roots[0]?.title).toHaveLength(80);
    expect(tree.roots[0]?.title.startsWith("Review the parser")).toBe(true);
  });

  it("is empty when nothing is live", () => {
    expect(buildLiveSubagentTree([subagent("a", { status: "completed" })])).toEqual({
      roots: [],
      runningCount: 0,
      waitingCount: 0,
    });
  });
});

describe("selectSubagentsForBackgroundTasks", () => {
  const titles = (
    subagents: ReadonlyArray<SubagentTreeSource>,
    tasks: Parameters<typeof selectSubagentsForBackgroundTasks>[1],
  ) => selectSubagentsForBackgroundTasks(subagents, tasks).map((subagent) => subagent.title);

  it("leaves out a live subagent that no task names", () => {
    expect(
      titles(
        [subagent("old", { startedSecond: 1 }), subagent("fresh", { startedSecond: 2 })],
        [
          { taskId: "bash:1", kind: "command" },
          { taskId: "old", kind: "subagent" },
        ],
      ),
    ).toEqual(["old"]);
  });

  it("selects nothing when only commands are pending", () => {
    expect(titles([subagent("fresh")], [{ taskId: "bash:1", kind: "command" }])).toEqual([]);
  });

  it("keeps live ancestors for structure but not their other children", () => {
    const subagents = [
      subagent("parent", { startedSecond: 1 }),
      subagent("named-child", { parent: "parent", startedSecond: 2 }),
      subagent("fresh-sibling", { parent: "parent", startedSecond: 3 }),
    ];
    const tree = buildLiveSubagentTree(
      selectSubagentsForBackgroundTasks(subagents, [{ taskId: "named-child", kind: "subagent" }]),
    );
    expect(shape(tree.roots)).toEqual([["parent", 1, [["named-child", 0, []]]]]);
    expect([tree.runningCount, tree.waitingCount]).toEqual([2, 0]);
  });

  it("matches by native task id or child thread", () => {
    const byNative = {
      ...subagent("node-a"),
      nativeTaskRef: { driver: "claudeAgent", nativeId: "toolu_1", strength: "strong" },
    } as unknown as SubagentTreeSource;
    const byThread = {
      ...subagent("node-b"),
      childThreadId: "child-1",
    } as unknown as SubagentTreeSource;
    expect(
      titles(
        [byNative, byThread, subagent("other")],
        [
          { taskId: "toolu_1", kind: "subagent" },
          { taskId: "x", kind: "subagent", childThreadId: "child-1" as never },
        ],
      ),
    ).toEqual(["node-a", "node-b"]);
  });

  it("ignores finished subagents and survives a parent cycle", () => {
    expect(
      titles(
        [
          subagent("done", { status: "completed" }),
          subagent("a", { parent: "b" }),
          subagent("b", { parent: "a" }),
        ],
        [
          { taskId: "done", kind: "subagent" },
          { taskId: "a", kind: "subagent" },
        ],
      ),
    ).toEqual(["a", "b"]);
  });
});
