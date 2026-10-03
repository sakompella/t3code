// @effect-diagnostics nodeBuiltinImport:off -- Pi extensions run outside Effect; these tests exercise their native filesystem boundary.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeModule from "node:module";
import * as NodeVM from "node:vm";
import { assert, describe, expect, it } from "@effect/vitest";

import * as Schema from "effect/Schema";

import {
  PI_T3_MCP_EXTENSION_SOURCE,
  T3_MCP_BEARER_ENV,
  T3_MCP_URL_ENV,
  T3_NAVIGATE_TREE_COMMAND,
  T3_NAVIGATE_TREE_RESULT_MARKER,
  T3_PI_MCP_TOOLS_ENV,
  T3_ROLLBACK_NOTICE,
} from "./mcpExtensionSource.ts";
import { loadMcpBridge } from "./mcpBridge.testkit.ts";

const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

type RequestHook = (
  event: { payload: unknown },
  ctx: { model: { provider: string } },
) => Record<string, unknown> | undefined;

interface SkillCommand {
  readonly name: string;
  readonly source: string;
  readonly sourceInfo: { readonly path: string };
}

type InputHook = (
  event: { text: string; images?: ReadonlyArray<unknown> },
  ctx: { ui: { notify: (message: string, level: string) => void } },
) => Promise<{ action: string; text: string; images?: ReadonlyArray<unknown> } | undefined>;

async function loadHooks(commands: ReadonlyArray<SkillCommand> = []) {
  const handlers = new Map<string, unknown>();
  const source = NodeModule.stripTypeScriptTypes(
    PI_T3_MCP_EXTENSION_SOURCE.replace(/^import .*;$/gm, "").replace(
      "export default async function",
      "async function",
    ),
  );
  await NodeVM.runInNewContext(`${source}\nt3McpExtension(pi)`, {
    process: { env: {} },
    NodeFSP,
    NodePath,
    stripFrontmatter: (content: string) => content,
    pi: {
      on: (name: string, handler: unknown) => handlers.set(name, handler),
      getCommands: () => commands,
      registerCommand: () => undefined,
    },
  });
  return handlers;
}

describe("Pi MCP tool results", () => {
  it("returns mixed screenshot blocks and typed script output without repeating structured text", async () => {
    const content = [
      { type: "text", text: "Browser screenshot" },
      { type: "image", data: "AAAA", mimeType: "image/png", _meta: { private: true } },
      { type: "text", text: "Screenshot captured" },
    ] as const;
    const result = { content, structuredContent: { count: 2 }, _meta: { private: true } };
    const bridge = await loadMcpBridge({ modern: true, result });
    const tool = bridge.tools.find((tool) => tool.name === "mcp__t3-code__preview_snapshot")!;
    const output = await tool.execute("snapshot", {});
    assert.deepEqual(output.content, [
      content[0],
      { type: "image", data: "AAAA", mimeType: "image/png" },
      content[2],
    ]);
    assert.deepEqual(output.structuredContent, { content, structuredContent: { count: 2 } });
    assert.deepEqual(tool.outputSchema, {
      type: "object",
      properties: {
        content: { type: "array", items: { type: "object" } },
        structuredContent: { type: "object", properties: { count: { type: "number" } } },
        isError: { type: "boolean" },
        _meta: { type: "object" },
      },
      required: ["content"],
    });
  });

  it.each([false, true])(
    "preserves image-only results and error status, isError=%s",
    async (isError) => {
      const image = { type: "image", data: "AAAA", mimeType: "image/png" };
      const result = { content: [image], isError };
      const bridge = await loadMcpBridge({ result });
      const output = await bridge.tools[0]!.execute("image", {});
      assert.deepEqual(output.content[0], image);
      assert.equal(output.content.length, isError ? 2 : 1);
      if (isError) assert.include(output.content[1]?.text ?? "", "returned an error");
      assert.equal(output.isError, isError ? true : undefined);
      assert.deepEqual(output.structuredContent, result);
    },
  );

  it("uses structured output as model text when content is empty", async () => {
    const result = { content: [], structuredContent: { count: 2 } };
    const bridge = await loadMcpBridge({ result });
    const output = await bridge.tools[0]!.execute("structured", {});
    assert.deepEqual(output.content, [{ type: "text", text: '{"count":2}' }]);
    assert.deepEqual(output.structuredContent, result);
  });

  it("propagates cancellation without returning a successful result", async () => {
    const bridge = await loadMcpBridge();
    const controller = new AbortController();
    controller.abort();
    await expect(bridge.tools[0]!.execute("cancelled", {}, controller.signal)).rejects.toThrow(
      "aborted",
    );
  });
});

describe("Pi MCP tool exposure", () => {
  const discoveryContext = {
    model: { api: "anthropic-messages" },
    ui: { notify: () => undefined },
  };

  it.each(["session_start", "session_tree", "model_select", "before_agent_start"])(
    "declares optional T3 tools for OpenAI-compatible gateways at %s",
    async (hookName) => {
      const bridge = await loadMcpBridge({ modern: true, toolSearchAvailable: true });
      const hook = bridge.handlers.get(hookName);
      assert.isDefined(hook);
      await hook!(
        { systemPrompt: "Pi system prompt" },
        { model: { api: "openai-completions" }, ui: { notify: assert.fail } },
      );
      assert.isTrue(
        bridge.tools
          .filter((tool) => tool.name.startsWith("mcp__t3-code__"))
          .every((tool) => bridge.getActiveTools().includes(tool.name)),
      );
      const result = await bridge.tools
        .find((tool) => tool.name === "mcp__t3-code__preview_snapshot")!
        .execute("gateway-call", {});
      assert.equal(result.content[0]?.text, "browser snapshot");
      assert.equal(bridge.tools.filter((tool) => tool.exposure === "hidden").length, 4);
    },
  );

  it("keeps orchestration direct and optional bridge tools discoverable on modern Pi", async () => {
    const bridge = await loadMcpBridge({ modern: true, toolSearchAvailable: true });
    await bridge.handlers.get("session_start")!({ systemPrompt: "" }, discoveryContext);
    const start = bridge.handlers.get("before_agent_start");
    assert.isDefined(start);
    const prompt = await start!({ systemPrompt: "Pi system prompt" }, discoveryContext);
    assert.include(prompt.systemPrompt, "orchestrator_capabilities");
    assert.equal(bridge.servers.length, 0);
    assert.equal(bridge.tools.length, 8);
    assert.deepEqual(bridge.getActiveTools(), ["read", "tool_search"]);
    assert.deepEqual(
      bridge.tools
        .filter((tool) => tool.exposure !== "hidden")
        .map((tool) => [tool.name, tool.exposure]),
      [
        ["mcp__t3-code__orchestrator_capabilities", "direct"],
        ["mcp__t3-code__delegate_task", "direct"],
        ["mcp__t3-code__task_status", "direct"],
        ["mcp__t3-code__preview_snapshot", "deferred"],
      ],
    );
    assert.deepEqual(
      bridge.tools.filter((tool) => tool.exposure === "hidden").map((tool) => tool.name),
      [
        "mcp__t3_code__orchestrator_capabilities",
        "mcp__t3_code__delegate_task",
        "mcp__t3_code__task_status",
        "mcp__t3_code__preview_snapshot",
      ],
    );
    const result = await bridge.tools
      .find((tool) => tool.name === "mcp__t3-code__preview_snapshot")!
      .execute("call-1", { depth: 2 });
    assert.equal(result.content[0]?.text, "browser snapshot");
    assert.equal(bridge.requests.at(-1)?.method, "tools/call");
  });

  it("restores gateway declarations after model switches and older tree loadouts without excluded tools", async () => {
    const bridge = await loadMcpBridge({
      modern: true,
      toolSearchAvailable: true,
      allowsTool: (name) => name !== "mcp__t3-code__delegate_task",
    });
    const event = { systemPrompt: "" };
    await bridge.handlers.get("session_start")!(event, discoveryContext);
    const gatewayContext = { model: { api: "openai-completions" }, ui: { notify: assert.fail } };
    await bridge.handlers.get("model_select")!(event, gatewayContext);
    bridge.restoreActiveTools(["read", "mcp__t3-code__task_status"]);
    await bridge.handlers.get("session_tree")!(event, gatewayContext);
    assert.deepEqual(bridge.getActiveTools(), [
      "read",
      "mcp__t3-code__task_status",
      "tool_search",
      "mcp__t3-code__orchestrator_capabilities",
      "mcp__t3-code__preview_snapshot",
    ]);
    await bridge.handlers.get("model_select")!(event, discoveryContext);
    assert.equal(
      bridge.tools.find((tool) => tool.name === "mcp__t3-code__preview_snapshot")?.exposure,
      "deferred",
    );
    assert.include(bridge.getActiveTools(), "mcp__t3-code__preview_snapshot");
  });

  it.each(["legacy Pi", "disabled tool search"])(
    "keeps tool execution available with %s",
    async (mode) => {
      const bridge = await loadMcpBridge({
        modern: mode !== "legacy Pi",
        toolSearchAvailable: mode === "disabled tool search",
        toolSearchDisabled: mode === "disabled tool search",
      });
      if (mode !== "legacy Pi") {
        const start = bridge.handlers.get("session_start");
        await start!({ systemPrompt: "Pi system prompt" }, discoveryContext);
      }
      assert.equal(bridge.tools.filter((tool) => tool.exposure !== "hidden").length, 4);
      assert.isTrue(
        bridge.tools
          .filter((tool) => tool.exposure !== "hidden")
          .every((tool) => tool.exposure === undefined || tool.exposure === "direct"),
      );
      const tool = bridge.tools.find((tool) => tool.name === "mcp__t3-code__preview_snapshot");
      assert.isDefined(tool);
      const controller = new AbortController();
      const result = await tool!.execute("call-1", { depth: 2 }, controller.signal);
      assert.equal(result.content[0]?.text, "browser snapshot");
      assert.strictEqual(bridge.transports.at(-1)?.signal, controller.signal);
      assert.equal(bridge.transports.at(-1)?.url, "http://fixture.invalid/mcp");
      assert.equal(bridge.transports.at(-1)?.authorization, "Bearer fixture-token");
      assert.deepEqual(JSON.parse(JSON.stringify(bridge.requests.at(-1))), {
        jsonrpc: "2.0",
        id: 3,
        method: "tools/call",
        params: { name: "preview_snapshot", arguments: { depth: 2 } },
      });
      assert.isUndefined(tool?.promptSnippet);
      assert.isUndefined(tool?.promptGuidelines);
    },
  );

  it("preserves legacy wildcard tool selection", async () => {
    const bridge = await loadMcpBridge({
      modern: true,
      toolSearchAvailable: true,
      allowsTool: (name) =>
        name === "read" || name === "tool_search" || name.startsWith("mcp__t3-code__"),
    });
    await bridge.handlers.get("session_start")!({ systemPrompt: "" }, discoveryContext);
    assert.equal(bridge.tools.length, 4);
    assert.equal(bridge.tools.filter((tool) => tool.exposure === "direct").length, 3);
    const tool = bridge.tools.find((tool) => tool.name === "mcp__t3-code__preview_snapshot");
    assert.isDefined(tool);
    assert.equal((await tool!.execute("selected", {})).content[0]?.text, "browser snapshot");
  });

  it.each([false, true])(
    "reconciles tree loadouts while honoring search exclusion: %s",
    async (excludeSearch) => {
      const bridge = await loadMcpBridge({
        modern: true,
        toolSearchAvailable: true,
        allowsTool: (name) =>
          name !== "mcp__t3-code__delegate_task" && (!excludeSearch || name !== "tool_search"),
      });
      await bridge.handlers.get("session_start")!({ systemPrompt: "" }, discoveryContext);
      bridge.restoreActiveTools([
        "read",
        "mcp__t3-code__task_status",
        "mcp__t3-code__preview_snapshot",
      ]);
      const tree = bridge.handlers.get("session_tree");
      assert.isDefined(tree);
      await tree!({ systemPrompt: "" }, discoveryContext);
      assert.deepEqual(bridge.getActiveTools(), [
        "read",
        "mcp__t3-code__task_status",
        "mcp__t3-code__preview_snapshot",
        ...(!excludeSearch ? ["tool_search"] : []),
      ]);
      assert.isFalse(
        bridge.tools.some(
          (tool) => tool.exposure !== "hidden" && tool.name.endsWith("__delegate_task"),
        ),
      );
    },
  );
});

describe("Pi tool discovery permissions", () => {
  it.each(["approval-required", "auto-accept-edits", "auto"])(
    "allows annotated T3 reads in %s while gating mutations and replacements",
    async (runtimeMode) => {
      const bridge = await loadMcpBridge({ modern: true, runtimeMode });
      const hook = bridge.handlers.get("tool_call") as unknown as (
        event: { toolName: string; input: unknown },
        ctx: { ui: { confirm: (title: string) => Promise<boolean> } },
      ) => Promise<{ block: true; reason: string } | undefined>;
      const confirmations: string[] = [];
      const ctx = {
        ui: {
          confirm: async (title: string) => {
            confirmations.push(title);
            return false;
          },
        },
      };
      for (const prefix of ["mcp__t3-code__", "mcp__t3_code__"]) {
        assert.isUndefined(
          await hook({ toolName: prefix + "orchestrator_capabilities", input: {} }, ctx),
        );
        assert.equal(
          (await hook({ toolName: prefix + "delegate_task", input: {} }, ctx))?.block,
          true,
        );
        assert.equal(
          (await hook({ toolName: prefix + "task_status", input: {} }, ctx))?.block,
          true,
        );
      }
      assert.equal(confirmations.length, 4);
      bridge.setBridgeSourcePath("/user/extensions/replacement.ts");
      assert.equal(
        (await hook({ toolName: "mcp__t3-code__orchestrator_capabilities", input: {} }, ctx))
          ?.block,
        true,
      );
      assert.equal(
        (await hook({ toolName: "mcp__other__orchestrator_capabilities", input: {} }, ctx))?.block,
        true,
      );
      assert.equal(confirmations.length, 6);
    },
  );

  it("allows discovery without confirmation and still gates the discovered tool", async () => {
    type ToolCallHook = (
      event: { toolName: string; input: unknown },
      ctx: {
        signal: AbortSignal;
        ui: {
          confirm: (
            title: string,
            detail: string,
            options: { signal: AbortSignal },
          ) => Promise<boolean>;
        };
      },
    ) => Promise<{ block: true; reason: string } | undefined>;
    let toolCall: ToolCallHook | undefined;
    let searchPath = "builtin:tool-search";
    const source = NodeModule.stripTypeScriptTypes(
      PI_T3_MCP_EXTENSION_SOURCE.replace(/^import .*;$/gm, "").replace(
        "export default async function",
        "async function",
      ),
    );
    await NodeVM.runInNewContext(`${source}\nt3McpExtension(pi)`, {
      process: { env: { T3_PI_RUNTIME_MODE: "approval-required" } },
      pi: {
        on: (name: string, handler: ToolCallHook) => {
          if (name === "tool_call") toolCall = handler;
        },
        getAllTools: () => [{ name: "tool_search", sourceInfo: { path: searchPath } }],
        registerCommand: () => undefined,
      },
    });
    assert.isDefined(toolCall);
    const confirmations: string[] = [];
    const controller = new AbortController();
    const ctx = {
      signal: controller.signal,
      ui: {
        confirm: async (title: string, _detail: string, options: { signal: AbortSignal }) => {
          assert.strictEqual(options.signal, controller.signal);
          confirmations.push(title);
          return false;
        },
      },
    };
    assert.isUndefined(
      await toolCall!({ toolName: "tool_search", input: { query: "preview_snapshot" } }, ctx),
    );
    assert.equal(confirmations.length, 0);
    const result = await toolCall!({ toolName: "mcp__t3-code__preview_snapshot", input: {} }, ctx);
    assert.equal(result?.block, true);
    assert.deepEqual(confirmations, ["Allow mcp__t3-code__preview_snapshot?"]);

    searchPath = "/extensions/custom-search.ts";
    const replaced = await toolCall!({ toolName: "tool_search", input: {} }, ctx);
    assert.equal(replaced?.block, true);
    assert.equal(confirmations.at(-1), "Allow tool_search?");
  });
});

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
  readonly handlers: Map<string, unknown>;
  readonly commands: Map<string, RegisteredCommand>;
  readonly sentMessages: Array<SentMessage>;
  readonly registeredTools: Array<string>;
}> {
  const handlers = new Map<string, unknown>();
  const commands = new Map<string, RegisteredCommand>();
  const sentMessages: Array<SentMessage> = [];
  const registeredTools: Array<string> = [];
  // Execute the shipped extension; Typebox is replaced by a pass-through stub.
  const source = NodeModule.stripTypeScriptTypes(
    PI_T3_MCP_EXTENSION_SOURCE.replace(/^import .*;$/gm, "").replace(
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
      on: (name: string, handler: unknown) => handlers.set(name, handler),
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
  return hook as RequestHook;
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

async function loadInputHook(commands: ReadonlyArray<SkillCommand>) {
  const handlers = await loadHooks(commands);
  const hook = handlers.get("input");
  assert.isDefined(hook);
  return hook as InputHook;
}

describe("Pi skill references", () => {
  it("loads every selected skill once while preserving inline prose, whitespace, and images", async () => {
    const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "pi-skill-mentions-"));
    try {
      const alpha = NodePath.join(directory, "alpha.md");
      const beta = NodePath.join(directory, "beta.md");
      await NodeFSP.writeFile(alpha, "ALPHA_INSTRUCTIONS");
      await NodeFSP.writeFile(beta, "BETA_INSTRUCTIONS");
      const hook = await loadInputHook([
        { name: "skill:alpha", source: "skill", sourceInfo: { path: alpha } },
        { name: "skill:beta", source: "skill", sourceInfo: { path: beta } },
      ]);
      const text =
        "Please use the $alpha philosophy, then $beta and $alpha\n```ts\n  const x = 1;\n```";
      const images = [{ type: "image", data: "fixture" }];
      const result = await hook({ text, images }, { ui: { notify: assert.fail } });
      assert.equal(result?.action, "transform");
      assert.isTrue(result?.text.startsWith(text + "\n\n"));
      assert.equal(result?.text.split("ALPHA_INSTRUCTIONS").length, 2);
      assert.equal(result?.text.split("BETA_INSTRUCTIONS").length, 2);
      assert.include(result?.text ?? "", `References are relative to ${directory}.`);
      assert.strictEqual(result?.images, images);
    } finally {
      await NodeFSP.rm(directory, { recursive: true });
    }
  });

  it("leaves unknown references and native skill commands for Pi", async () => {
    const hook = await loadInputHook([
      { name: "skill:alpha", source: "skill", sourceInfo: { path: "/unused" } },
    ]);
    for (const text of [
      "Explain $HOME",
      "/skill:alpha use $alpha",
      "Hello",
      "$missing",
      "Use $alpha, then continue",
      "Use ($alpha)",
    ]) {
      assert.isUndefined(await hook({ text }, { ui: { notify: assert.fail } }));
    }
  });

  it("loads additional chips without duplicating a leading native skill", async () => {
    const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "pi-skill-mentions-"));
    try {
      const path = NodePath.join(directory, "beta.md");
      await NodeFSP.writeFile(path, "BETA_INSTRUCTIONS");
      const hook = await loadInputHook([
        { name: "skill:alpha", source: "skill", sourceInfo: { path: "/unused" } },
        { name: "skill:beta", source: "skill", sourceInfo: { path } },
      ]);
      const text = "/skill:alpha use $alpha and $beta";
      const result = await hook({ text }, { ui: { notify: assert.fail } });
      assert.isTrue(result?.text.startsWith(text + "\n\n"));
      assert.notInclude(result?.text ?? "", '<skill name="alpha"');
      assert.include(result?.text ?? "", "BETA_INSTRUCTIONS");
    } finally {
      await NodeFSP.rm(directory, { recursive: true });
    }
  });

  it("reports an unreadable skill without deleting its reference or other selected instructions", async () => {
    const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "pi-skill-mentions-"));
    try {
      const path = NodePath.join(directory, "readable.md");
      await NodeFSP.writeFile(path, "READABLE_INSTRUCTIONS");
      const hook = await loadInputHook([
        {
          name: "skill:missing",
          source: "skill",
          sourceInfo: { path: NodePath.join(directory, "missing.md") },
        },
        { name: "skill:readable", source: "skill", sourceInfo: { path } },
      ]);
      const notices: string[] = [];
      const text = "Use $missing and $readable";
      const result = await hook(
        { text },
        {
          ui: {
            notify: (message) => {
              notices.push(message);
            },
          },
        },
      );
      assert.isTrue(result?.text.startsWith(text + "\n\n"));
      assert.include(result?.text ?? "", "READABLE_INSTRUCTIONS");
      assert.equal(notices.length, 1);
      assert.include(notices[0] ?? "", "Could not load skill missing");
    } finally {
      await NodeFSP.rm(directory, { recursive: true });
    }
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

describe("T3 extension handlers on a stale ctx", () => {
  // What Pi and Prime Agent throw from a retired runner's ctx getters.
  const staleMessage =
    "This extension ctx is stale after session replacement or reload. Do not use a captured pi or command ctx after ctx.newSession(), ctx.fork(), ctx.switchSession(), or ctx.reload().";
  const throwingCtx = (message: string) =>
    new Proxy(
      {},
      {
        get: () => {
          throw new Error(message);
        },
      },
    );
  const staleCtx = throwingCtx(staleMessage);
  const brokenCtx = throwingCtx("ctx exploded");
  const uncappedPayload = { max_tokens: 231_969 };

  const loadHandler = async <Event, Ctx, Result>(
    name: string,
    env: Record<string, string> = {},
  ) => {
    const { handlers } = await loadExtension(env);
    const handler = handlers.get(name);
    assert.isDefined(handler);
    return handler as (event: Event, ctx: Ctx) => Result;
  };

  it("sends a provider request from a replaced session on, uncapped and without an error", async () => {
    const hook = await loadHandler<{ payload: unknown }, unknown, unknown>(
      "before_provider_request",
    );
    assert.isUndefined(hook({ payload: uncappedPayload }, staleCtx));
  });

  it("still reports other ctx failures from a provider request", async () => {
    const hook = await loadHandler<{ payload: unknown }, unknown, unknown>(
      "before_provider_request",
    );
    assert.throws(() => hook({ payload: uncappedPayload }, brokenCtx), "ctx exploded");
  });

  describe("tool approval", () => {
    const approvalEnv = { T3_PI_RUNTIME_MODE: "approval-required" };
    const bash = { toolName: "bash", input: { command: "rm -rf build" } };
    const loadToolCall = () =>
      loadHandler<typeof bash, unknown, Promise<{ block: boolean; reason: string } | undefined>>(
        "tool_call",
        approvalEnv,
      );

    it("blocks the tool when the approval prompt belongs to a replaced session", async () => {
      const toolCall = await loadToolCall();
      const result = await toolCall(bash, staleCtx);
      assert.isTrue(result?.block);
      assert.match(result?.reason ?? "", /bash was blocked.*session was replaced/);
    });

    it("lets an approved tool run and blocks a declined one", async () => {
      const toolCall = await loadToolCall();
      const answering = (approved: boolean) => ({ ui: { confirm: async () => approved } });
      assert.isUndefined(await toolCall(bash, answering(true)));
      assert.deepEqual(await toolCall(bash, answering(false)), {
        block: true,
        reason: "bash was declined in T3 Code.",
      });
    });

    it("rejects, rather than approving, when the prompt fails for another reason", async () => {
      const toolCall = await loadToolCall();
      let failure: unknown;
      await toolCall(bash, brokenCtx).catch((error: unknown) => (failure = error));
      assert.match(String(failure), /ctx exploded/);
    });
  });

  it("starts a replaced session without a warning, and warns a live one", async () => {
    const sessionStart = await loadHandler<unknown, unknown, Promise<void>>("session_start");
    await sessionStart({}, staleCtx);
    const warnings: Array<string> = [];
    await sessionStart({}, { ui: { notify: (message: string) => warnings.push(message) } });
    assert.lengthOf(warnings, 1);
    assert.match(warnings[0]!, /T3_MCP_URL/);
  });

  it("leaves a rollback on a replaced session unreported instead of throwing", async () => {
    const { commands } = await loadExtension();
    const handler = commands.get(T3_NAVIGATE_TREE_COMMAND)?.handler;
    assert.isDefined(handler);
    await handler!("t3-nav-9 u1", staleCtx as unknown as NavigateTreeContext);
  });
});
