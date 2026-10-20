/**
 * Live subagent tree for the background-work panel, shared by web and mobile.
 *
 * Providers that nest subagents (Prime Agent's `rlm.spawn` children spawning
 * their own) parent each child on its parent subagent's node, so the tree
 * follows `parentNodeId`. Only live subagents appear; a live child whose
 * parent already finished becomes a root rather than disappearing.
 */
import type {
  OrchestrationV2PendingBackgroundTask,
  OrchestrationV2Subagent,
} from "@t3tools/contracts";
import { isOrchestrationV2WorkActive } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

export type SubagentTreeSource = Pick<
  OrchestrationV2Subagent,
  | "id"
  | "parentNodeId"
  | "title"
  | "prompt"
  | "model"
  | "status"
  | "progress"
  | "childThreadId"
  | "startedAt"
  | "completedAt"
  | "updatedAt"
>;

export interface SubagentTreeNode {
  readonly subagent: SubagentTreeSource;
  readonly title: string;
  readonly children: ReadonlyArray<SubagentTreeNode>;
  /** Live subagents anywhere below this one. */
  readonly descendantCount: number;
}

export interface SubagentTree {
  readonly roots: ReadonlyArray<SubagentTreeNode>;
  readonly runningCount: number;
  /** Live but not working: queued, or blocked on the user. */
  readonly waitingCount: number;
}

const TITLE_MAX_LENGTH = 80;

function subagentTitle(subagent: SubagentTreeSource): string {
  const title = subagent.title?.trim();
  if (title !== undefined && title.length > 0) return title;
  const prompt = subagent.prompt.replace(/\s+/g, " ").trim();
  return prompt.length > TITLE_MAX_LENGTH
    ? `${prompt.slice(0, TITLE_MAX_LENGTH - 1).trimEnd()}…`
    : prompt;
}

function startOrder(subagent: SubagentTreeSource): number {
  return DateTime.toEpochMillis(subagent.startedAt ?? subagent.updatedAt);
}

export function buildLiveSubagentTree(subagents: ReadonlyArray<SubagentTreeSource>): SubagentTree {
  const live = subagents
    .filter((subagent) => isOrchestrationV2WorkActive(subagent.status))
    // `filter` returned a new array; Hermes has no `toSorted`.
    .sort((left, right) => startOrder(left) - startOrder(right));
  const liveIds = new Set(live.map((subagent) => String(subagent.id)));
  const childrenByParent = new Map<string, Array<SubagentTreeSource>>();
  for (const subagent of live) {
    const parentId = String(subagent.parentNodeId);
    if (!liveIds.has(parentId)) continue;
    const siblings = childrenByParent.get(parentId) ?? [];
    siblings.push(subagent);
    childrenByParent.set(parentId, siblings);
  }

  const build = (
    subagent: SubagentTreeSource,
    ancestors: ReadonlySet<string>,
  ): SubagentTreeNode => {
    const id = String(subagent.id);
    const path = new Set(ancestors).add(id);
    // A parent cycle in bad data must not recurse forever.
    const children = (childrenByParent.get(id) ?? [])
      .filter((child) => !path.has(String(child.id)))
      .map((child) => build(child, path));
    return {
      subagent,
      title: subagentTitle(subagent),
      children,
      descendantCount: children.reduce((total, child) => total + 1 + child.descendantCount, 0),
    };
  };

  const roots = live
    .filter((subagent) => !liveIds.has(String(subagent.parentNodeId)))
    .map((subagent) => build(subagent, new Set()));
  return {
    roots,
    runningCount: live.filter((subagent) => subagent.status === "running").length,
    waitingCount: live.filter((subagent) => subagent.status !== "running").length,
  };
}

/**
 * The live subagents that pending background tasks name, plus their live
 * ancestors so the tree keeps its structure. Other live subagents, such as a
 * foreground one the timeline already shows, stay out.
 */
export function selectSubagentsForBackgroundTasks<
  Source extends SubagentTreeSource & {
    readonly nativeTaskRef?: OrchestrationV2Subagent["nativeTaskRef"];
  },
>(
  subagents: ReadonlyArray<Source>,
  tasks: ReadonlyArray<OrchestrationV2PendingBackgroundTask>,
): ReadonlyArray<Source> {
  const subagentTasks = tasks.filter((task) => task.kind === "subagent");
  const taskIds = new Set(subagentTasks.map((task) => task.taskId));
  const childThreadIds = new Set(
    subagentTasks.flatMap((task) =>
      task.kind === "subagent" && task.childThreadId ? [task.childThreadId] : [],
    ),
  );
  const live = subagents.filter((subagent) => isOrchestrationV2WorkActive(subagent.status));
  const liveById = new Map(live.map((subagent) => [String(subagent.id), subagent]));
  const selectedIds = new Set<string>();
  for (const subagent of live) {
    const nativeId = subagent.nativeTaskRef?.nativeId;
    const named =
      taskIds.has(String(subagent.id)) ||
      (nativeId != null && taskIds.has(nativeId)) ||
      (subagent.childThreadId !== null && childThreadIds.has(subagent.childThreadId));
    if (!named) continue;
    // Walk up through live parents; the visited check also stops a parent cycle.
    for (
      let node: Source | undefined = subagent;
      node !== undefined && !selectedIds.has(String(node.id));
      node = liveById.get(String(node.parentNodeId))
    ) {
      selectedIds.add(String(node.id));
    }
  }
  return live.filter((subagent) => selectedIds.has(String(subagent.id)));
}
