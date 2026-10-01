import type { SubagentTree, SubagentTreeNode } from "@t3tools/client-runtime/state/subagent-tree";
import type { ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { Fragment, useState } from "react";

import { AgentElapsed } from "./AgentElapsed";
import { InlineButton } from "../ui/button";

/**
 * Live subagents as an indented tree, modeled on Prime Agent's agents view:
 * a status glyph, the agent's name, its model and current activity, and how
 * long it has been running. A parent with live children gets a disclosure line
 * that collapses them.
 */
export function SubagentTreeList(props: {
  readonly tree: SubagentTree;
  /** Background shell commands, listed after the agents. */
  readonly commands: ReadonlyArray<{ readonly taskId: string; readonly label: string }>;
  readonly onOpenThread: (threadId: ThreadId) => void;
}) {
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(() => new Set());
  const toggle = (id: string) =>
    setCollapsed((current) => {
      const next = new Set(current);
      if (!next.delete(id)) next.add(id);
      return next;
    });

  const renderNode = (node: SubagentTreeNode, depth: number) => {
    const id = String(node.subagent.id);
    const isCollapsed = collapsed.has(id);
    return (
      <Fragment key={id}>
        <SubagentTreeRow node={node} depth={depth} onOpenThread={props.onOpenThread} />
        {node.children.length === 0 ? null : (
          <li style={{ paddingInlineStart: `${depth + 1}rem` }}>
            <button
              type="button"
              aria-expanded={!isCollapsed}
              onClick={() => toggle(id)}
              className="flex h-5 cursor-pointer items-center gap-1.5 text-muted-foreground hover:text-foreground"
            >
              <span aria-hidden className="w-3 text-center">
                {isCollapsed ? "▸" : "▾"}
              </span>
              {node.descendantCount} {node.descendantCount === 1 ? "subagent" : "subagents"} running
            </button>
          </li>
        )}
        {isCollapsed ? null : node.children.map((child) => renderNode(child, depth + 1))}
      </Fragment>
    );
  };

  return (
    <ul aria-label="Background work" className="m-0 mt-1 list-none space-y-0.5 p-0 text-xs">
      {props.tree.roots.map((node) => renderNode(node, 0))}
      {props.commands.map((command) => (
        <li key={command.taskId} className="flex min-w-0 items-center gap-1.5">
          <span aria-hidden className="w-3 shrink-0 text-center font-semibold text-foreground">
            ◈
          </span>
          <span className="min-w-0 truncate font-mono text-foreground">{command.label}</span>
          <span className="ms-auto shrink-0 text-muted-foreground/70">command</span>
        </li>
      ))}
    </ul>
  );
}

function SubagentTreeRow(props: {
  readonly node: SubagentTreeNode;
  readonly depth: number;
  readonly onOpenThread: (threadId: ThreadId) => void;
}) {
  const { subagent, title } = props.node;
  const running = subagent.status === "running";
  const childThreadId = subagent.childThreadId;
  const activity = subagent.progress?.trim();
  return (
    <li
      className="flex min-w-0 items-center gap-1.5"
      style={{ paddingInlineStart: `${props.depth}rem` }}
    >
      <span
        aria-label={running ? "running" : "waiting"}
        className={
          running
            ? "w-3 shrink-0 text-center font-semibold text-foreground"
            : "w-3 shrink-0 text-center text-warning"
        }
      >
        {running ? "◈" : "◐"}
      </span>
      <span className="min-w-0 shrink truncate font-semibold text-foreground">
        {childThreadId === null ? (
          title
        ) : (
          <InlineButton
            aria-label={`Open subagent ${title}`}
            onClick={() => props.onOpenThread(childThreadId)}
          >
            {title}
          </InlineButton>
        )}
      </span>
      {subagent.model === null ? null : (
        <span className="min-w-0 shrink-[2] truncate text-muted-foreground">{subagent.model}</span>
      )}
      {activity === undefined || activity.length === 0 ? null : (
        <span className="min-w-0 shrink-[3] truncate text-muted-foreground/70">{activity}</span>
      )}
      <span className="ms-auto shrink-0 text-muted-foreground/70">
        <AgentElapsed
          agent={{
            status: subagent.status,
            startedAt: subagent.startedAt === null ? null : DateTime.formatIso(subagent.startedAt),
            completedAt:
              subagent.completedAt === null ? null : DateTime.formatIso(subagent.completedAt),
          }}
        />
      </span>
    </li>
  );
}
