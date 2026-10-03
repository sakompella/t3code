import * as NodeModule from "node:module";
import * as NodeVM from "node:vm";
import { assert, describe, it } from "@effect/vitest";

import * as Schema from "effect/Schema";

import {
  PI_T3_MCP_EXTENSION_SOURCE,
  T3_MCP_BEARER_ENV,
  T3_MCP_URL_ENV,
  T3_NAVIGATE_TREE_COMMAND,
  T3_NAVIGATE_TREE_RESULT_MARKER,
  T3_PI_MCP_TOOLS_ENV,
  T3_ROLLBACK_NOTICE,
} from "./piT3McpExtensionSource.ts";

const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

type RequestHook = (
  event: { payload: unknown },
  ctx: { model: { provider: string } },
) => Record<string, unknown> | undefined;

interface NavigateTreeContext {
  readonly navigateTree: (entryId: string) => Promise<{ cancelled: boolean }>;
  readonly ui: { readonly notify: (message: string, type: string) => void };
}

interface RegisteredCommand {
  readonly handler: (args: string, ctx: NavigateTreeContext) => Promise<void>;
}

interface SentMessage {
  readonly message: { readonly customType: string; readonly content: string };
  readonly options: { readonly deliverAs?: string; readonly triggerTurn?: boolean };
}

const FAKE_T3_TOOLS = ["orchestrator_capabilities", "delegate_task"];

/** Answers the extension's MCP handshake and tool listing like T3's server. */
const fakeT3McpFetch = async (_url: string, init: { readonly body: string }) => {
  const { id, method } = decodeJson(init.body) as { id?: number; method: string };
  const result =
    method === "tools/list"
      ? { tools: FAKE_T3_TOOLS.map((name) => ({ name, description: name, inputSchema: {} })) }
      : {};
  return {
    ok: true,
    status: 200,
    headers: { get: (name: string) => (name === "content-type" ? "application/json" : null) },
    text: async () => JSON.stringify({ jsonrpc: "2.0", id, result }),
  };
};

async function loadExtension(env: Record<string, string> = {}): Promise<{
  readonly handlers: Map<string, RequestHook>;
  readonly commands: Map<string, RegisteredCommand>;
  readonly sentMessages: Array<SentMessage>;
  readonly registeredTools: Array<string>;
}> {
  const handlers = new Map<string, RequestHook>();
  const commands = new Map<string, RegisteredCommand>();
  const sentMessages: Array<SentMessage> = [];
  const registeredTools: Array<string> = [];
  // Execute the shipped extension; Typebox is replaced by a pass-through stub.
  const source = NodeModule.stripTypeScriptTypes(
    PI_T3_MCP_EXTENSION_SOURCE.replace('import { Type } from "typebox";', "").replace(
      "export default async function",
      "async function",
    ),
  );
  await NodeVM.runInNewContext(`${source}\nt3McpExtension(pi)`, {
    process: { env },
    fetch: fakeT3McpFetch,
    AbortSignal,
    Type: { Unsafe: (schema: unknown) => schema, Object: () => ({}) },
    pi: {
      on: (name: string, handler: RequestHook) => handlers.set(name, handler),
      registerCommand: (name: string, command: RegisteredCommand) => commands.set(name, command),
      registerTool: (tool: { readonly name: string }) => registeredTools.push(tool.name),
      sendMessage: (message: SentMessage["message"], options: SentMessage["options"]) =>
        sentMessages.push({ message, options }),
    },
  });
  return { handlers, commands, sentMessages, registeredTools };
}

async function loadRequestHook(): Promise<RequestHook> {
  const { handlers } = await loadExtension();
  const hook = handlers.get("before_provider_request");
  assert.isDefined(hook);
  return hook!;
}

describe("Pi upstream output-budget workaround", () => {
  it.each(["max_tokens", "max_completion_tokens"])(
    "caps %s without changing the conversation or tools",
    async (key) => {
      const hook = await loadRequestHook();
      const payload = {
        model: "moonshotai/kimi-k2.6",
        messages: [{ role: "user", content: "hello" }],
        tools: [{ type: "function", function: { name: "read" } }],
        [key]: 231_969,
      };
      const result = hook({ payload }, { model: { provider: "openrouter" } });
      assert.equal(result?.[key], 32_768);
      assert.strictEqual(result?.messages, payload.messages);
      assert.strictEqual(result?.tools, payload.tools);
      assert.equal(result?.model, payload.model);
      assert.equal(payload[key], 231_969);
    },
  );

  it("preserves smaller budgets and other providers' payloads", async () => {
    const hook = await loadRequestHook();
    for (const payload of [{ max_tokens: 8192 }, { max_completion_tokens: 32_768 }, {}, null]) {
      assert.isUndefined(hook({ payload }, { model: { provider: "openrouter" } }));
    }
    assert.isUndefined(
      hook({ payload: { max_tokens: 231_969 } }, { model: { provider: "anthropic" } }),
    );
  });
});

describe("T3 in-place rollback command", () => {
  const runCommand = async (args: string, navigateTree: NavigateTreeContext["navigateTree"]) => {
    const { commands, sentMessages } = await loadExtension();
    const command = commands.get(T3_NAVIGATE_TREE_COMMAND);
    assert.isDefined(command);
    const notices: Array<string> = [];
    const navigatedTo: Array<string> = [];
    await command!.handler(args, {
      navigateTree: (entryId) => {
        navigatedTo.push(entryId);
        return navigateTree(entryId);
      },
      ui: { notify: (message) => notices.push(message) },
    });
    assert.lengthOf(notices, 1);
    assert.isTrue(notices[0]!.startsWith(T3_NAVIGATE_TREE_RESULT_MARKER));
    return {
      navigatedTo,
      sentMessages,
      result: decodeJson(notices[0]!.slice(T3_NAVIGATE_TREE_RESULT_MARKER.length)),
    };
  };

  it("navigates to the requested entry and reports success with the request id", async () => {
    const { navigatedTo, result, sentMessages } = await runCommand(
      "  t3-nav-1   u2 ",
      async () => ({ cancelled: false }),
    );
    assert.deepEqual(navigatedTo, ["u2"]);
    assert.deepEqual(result, { requestId: "t3-nav-1", outcome: "ok" });
    // The agent learns on its next turn that live tool state was not rewound.
    assert.deepEqual(
      sentMessages.map(({ message, options }) => [message.content, options.deliverAs]),
      [[T3_ROLLBACK_NOTICE, "nextTurn"]],
    );
  });

  it("reports extension vetoes and navigation errors instead of throwing", async () => {
    const vetoed = await runCommand("t3-nav-2 u3", async () => ({ cancelled: true }));
    assert.deepEqual(vetoed.result, { requestId: "t3-nav-2", outcome: "cancelled" });
    assert.lengthOf(vetoed.sentMessages, 0);
    const failed = await runCommand("t3-nav-3 missing", async () => {
      throw new Error("Entry missing not found");
    });
    assert.deepEqual(failed.result, {
      requestId: "t3-nav-3",
      outcome: "error",
      error: "Entry missing not found",
    });
    assert.lengthOf(failed.sentMessages, 0);
  });
});

describe("T3 tools in the extension", () => {
  const t3Env = {
    [T3_MCP_URL_ENV]: "http://127.0.0.1:43123/mcp",
    [T3_MCP_BEARER_ENV]: "secret-token",
  };

  it("registers T3's tools natively and adds the orchestration prompt by default", async () => {
    const { registeredTools, handlers } = await loadExtension(t3Env);
    assert.deepEqual(
      registeredTools,
      FAKE_T3_TOOLS.map((name) => `mcp__t3-code__${name}`),
    );
    const beforeStart = handlers.get("before_agent_start") as unknown as (event: {
      systemPrompt: string;
    }) => { systemPrompt: string };
    assert.include(beforeStart({ systemPrompt: "base" }).systemPrompt, "T3 Code orchestration");
  });

  it("leaves T3's tools to the agent's own MCP client in kernel mode", async () => {
    const { registeredTools, handlers, commands } = await loadExtension({
      ...t3Env,
      [T3_PI_MCP_TOOLS_ENV]: "kernel",
    });
    assert.deepEqual(registeredTools, []);
    assert.isFalse(handlers.has("before_agent_start"));
    assert.isFalse(handlers.has("session_start"));
    // Permissions, the output cap, and in-place rollback still load.
    assert.isTrue(handlers.has("tool_call"));
    assert.isTrue(handlers.has("before_provider_request"));
    assert.isTrue(commands.has(T3_NAVIGATE_TREE_COMMAND));
  });
});
