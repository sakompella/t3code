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
description: T3 Code orchestration from the kernel, through the \`t3-code\` MCP server. Use to delegate to another provider or model, launch or read T3 threads, schedule recurring work, manage projects, worktrees or pull requests, drive the preview browser or a device, or answer a pending request.
---

# T3 Code

1. Find the tool: \`await mcp.list_tools("t3-code")\`. For provider instances and model ids, call \`orchestrator_capabilities\`.
2. Read that tool's description and input schema. They carry its rules and examples.
3. Call it: \`await mcp.call_tool("t3-code", name, args)\`. The step is done when you hold the result. For async work, keep the returned id and end the turn; its completion wakes this thread.

## Choosing

- **Subagents:** \`rlm.spawn\`. \`delegate_task\` is for another provider or model, or for a T3-owned task the user asked for.
- **Threads:** \`t3_thread_launch\` or \`create_threads\` only for a new top-level thread the user asked for. Set its workspace in \`workspaceStrategy\`: the launch binds the thread to it, and an omitted strategy means the project root.
- **Retries:** reuse the same \`clientRequestId\`. \`t3_thread_launch\` takes none, so check \`t3_thread_list\` before you launch again.
`;
