import { NodeId, type OrchestrationV2Subagent } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import { buildLiveSubagentTree, type SubagentTreeSource } from "./subagentTree.js";

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
