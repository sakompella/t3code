/**
 * Source for the T3-owned `t3-code` skill that Prime Agent loads with
 * `--skill`. Prime Agent reaches T3's MCP server through the kernel's own
 * `mcp` module, so the skill replaces the native tool registrations and the
 * orchestration prompt block that the Pi extension injects for other agents.
 *
 * Pi requires the skill's directory name to equal its `name`.
 */
export const PI_T3_CODE_SKILL_NAME = "t3-code";
export const PI_T3_CODE_SKILL_FILENAME = "SKILL.md";

export const PI_T3_CODE_SKILL_SOURCE = `---
name: t3-code
description: Drive T3 Code, the app running this session, from the Python kernel. Use it to delegate work to another provider or model, start or inspect T3 threads, schedule recurring tasks, manage projects, worktrees and pull requests, and answer pending requests. Use when the user asks for T3 orchestration or names a T3 tool. The tools live on the \`t3-code\` MCP server and are called through the \`mcp\` module.
---

# T3 Code

The \`t3-code\` MCP server holds T3's tools. They are not separate model tools. Call them from the kernel:

- List them: \`await mcp.list_tools("t3-code")\`. Read a tool's description and input schema before you call it.
- Call one: \`await mcp.call_tool("t3-code", "<tool>", {...})\`.
- Start with \`orchestrator_capabilities\` for the live provider instances and model ids.

## Rules

- **Subagents.** Use \`rlm.spawn\` by default. Use \`delegate_task\` only when the work needs another provider or model, or when the user asks for a T3-owned task. Keep the returned \`taskId\` for \`task_status\` and \`task_cancel\`. \`childThreadId\` is backing storage; never replace delegation with a new thread.
- **New threads.** Use \`t3_thread_launch\` or \`create_threads\` only when the user asks for a separate, new, or top-level thread. "Subagent" or "in parallel" is not such a request.
- **Workspace.** \`t3_thread_launch\` picks where the new thread runs. Pass \`workspaceStrategy\`: \`{"type":"worktree","baseRef":"main","branch":"feat/x","startFromOrigin":false}\`, \`{"type":"existing_worktree","worktreePath":"/abs/path"}\`, or \`{"type":"root"}\`. Omitting it means the project root, not this thread's worktree. Put the task in \`message\`. Uncommitted edits are not copied. A \`git worktree add\` in the prompt does not bind the thread. Find checkouts with \`t3_worktree_list\`.
- **Stacked work.** Set \`baseRef\` to the parent branch and \`startFromOrigin\` to \`false\` to build on its local commits.
- **Schedules.** Pass \`schedule\` to \`schedule_task\` as an object, never as JSON text: \`{"type":"interval","everyMs":3600000}\` or \`{"type":"fixed_time","timeOfDay":"09:00","weekdays":[1,2,3,4,5]}\`. Runs return to this thread unless \`bindToCurrentThread\` is \`false\`. Tell the user the cadence and next run time.
- **Retries.** Reuse the same \`clientRequestId\` when you retry a tool that takes one. \`t3_thread_launch\` has none: after an error or a lost reply, check \`t3_thread_list\` before you retry.
- **Waiting.** Do not poll in a loop. A finished async task or thread run wakes this thread, so end the turn. Call \`task_status\` or \`t3_thread_wait\` once if you need the result now.
`;
