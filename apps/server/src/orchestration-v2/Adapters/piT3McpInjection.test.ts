import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

import {
  PI_T3_MCP_EXTENSION_FILENAME,
  T3_MCP_BEARER_ENV,
  T3_MCP_URL_ENV,
  T3_PI_MCP_TOOLS_ENV,
  T3_PI_RUNTIME_MODE_ENV,
} from "./piT3McpExtensionSource.ts";
import {
  buildPiRpcLaunch,
  materializePiT3McpExtension,
  materializePiT3Skill,
  resolvePiLaunchArgs,
} from "./piT3McpInjection.ts";
import { PI_T3_CODE_SKILL_NAME } from "./piT3SkillSource.ts";

const threadId = ThreadId.make("thread-pi-t3-mcp");

const mcpSession = {
  environmentId: EnvironmentId.make("environment-pi-t3-mcp"),
  threadId,
  providerSessionId: "mcp-session-pi",
  providerInstanceId: ProviderInstanceId.make("pi"),
  endpoint: "http://127.0.0.1:43123/mcp",
  authorizationHeader: "Bearer secret-pi-token",
  browserToolsAvailable: true,
};

describe("pi T3 MCP injection", () => {
  it("always adds the permission bridge and configures MCP when available", () => {
    const resolvedArgs = resolvePiLaunchArgs(
      "--extension=/home/user/.pi/agent/extensions/demo.ts --session-dir=/tmp/pi-sessions --provider=anthropic --model=claude-sonnet --tools='' --name=-review --extension-flag=kept",
    );
    assert.isTrue(resolvedArgs.ok);
    if (!resolvedArgs.ok) return;
    const launch = buildPiRpcLaunch({
      launchArgs: resolvedArgs.args,
      environment: { PATH: "/usr/bin" },
      mcpSession,
      extensionPath: "/tmp/cache/pi-t3-mcp-extension.ts",
      runtimeMode: "approval-required",
    });
    assert.deepEqual(launch.args, [
      "--mode",
      "rpc",
      "--extension",
      "/home/user/.pi/agent/extensions/demo.ts",
      "--session-dir",
      "/tmp/pi-sessions",
      "--provider",
      "anthropic",
      "--model",
      "claude-sonnet",
      "--tools",
      "",
      "--name",
      "-review",
      "--extension-flag=kept",
      "--extension",
      "/tmp/cache/pi-t3-mcp-extension.ts",
    ]);
    assert.notInclude(launch.args, "--no-extensions");
    assert.equal(launch.env[T3_MCP_URL_ENV], "http://127.0.0.1:43123/mcp");
    assert.equal(launch.env[T3_MCP_BEARER_ENV], "secret-pi-token");
    assert.equal(launch.env[T3_PI_RUNTIME_MODE_ENV], "approval-required");

    const permissionOnly = buildPiRpcLaunch({
      launchArgs: [],
      environment: {
        [T3_MCP_URL_ENV]: "http://127.0.0.1:9999/stale",
        [T3_MCP_BEARER_ENV]: "stale-token",
      },
      mcpSession: undefined,
      extensionPath: "/tmp/cache/pi-t3-mcp-extension.ts",
      runtimeMode: "auto-accept-edits",
    });
    assert.deepEqual(permissionOnly.args, [
      "--mode",
      "rpc",
      "--extension",
      "/tmp/cache/pi-t3-mcp-extension.ts",
    ]);
    assert.isFalse(permissionOnly.hasT3Mcp);
    assert.isUndefined(permissionOnly.env[T3_MCP_URL_ENV]);
    assert.isUndefined(permissionOnly.env[T3_MCP_BEARER_ENV]);
    assert.equal(permissionOnly.env[T3_PI_RUNTIME_MODE_ENV], "auto-accept-edits");
  });

  it("falls back to Pi's first supported mode for legacy auto threads", () => {
    const launch = buildPiRpcLaunch({
      launchArgs: [],
      environment: {},
      mcpSession: undefined,
      extensionPath: "/tmp/cache/pi-t3-mcp-extension.ts",
      runtimeMode: "auto",
    });

    assert.equal(launch.env[T3_PI_RUNTIME_MODE_ENV], "approval-required");
  });

  it("starts the process on a given session file ahead of the user's launch arguments", () => {
    const base = {
      launchArgs: ["--model", "haiku"],
      environment: {},
      mcpSession: undefined,
      extensionPath: undefined,
    };

    assert.deepEqual(buildPiRpcLaunch({ ...base, resumeSessionFile: "/sessions/a.jsonl" }).args, [
      "--mode",
      "rpc",
      "--resume",
      "/sessions/a.jsonl",
      "--model",
      "haiku",
    ]);
    assert.notInclude(buildPiRpcLaunch(base).args, "--resume");
  });

  it("forces tools and user extensions off for unattended text generation", () => {
    const launch = buildPiRpcLaunch({
      launchArgs: [
        "--tools",
        "read,write",
        "--extension",
        "/home/user/.pi/agent/extensions/demo.ts",
        "--extension=./second.ts",
        "--provider",
        "anthropic",
      ],
      environment: {},
      mcpSession,
      extensionPath: "/tmp/cache/pi-t3-mcp-extension.ts",
      ephemeral: true,
      disableExtensions: true,
      disableTools: true,
    });
    assert.deepEqual(launch.args, [
      "--mode",
      "rpc",
      "--no-session",
      "--provider",
      "anthropic",
      "--no-extensions",
      "--no-tools",
    ]);
    assert.isFalse(launch.hasT3Mcp);
    assert.deepInclude(resolvePiLaunchArgs("--mode text"), {
      ok: false,
      message: "Pi launch argument '--mode' is controlled by T3 Code and cannot be overridden.",
    });
    assert.deepInclude(resolvePiLaunchArgs("--session old.jsonl"), { ok: false });
    assert.deepInclude(resolvePiLaunchArgs("prompt pi immediately"), { ok: false });
    assert.deepInclude(resolvePiLaunchArgs("--plan @instructions.md"), { ok: false });
  });

  it("rejects --provider without --model, which Pi 1.0 refuses at startup", () => {
    const rejection = {
      ok: false,
      message: "Pi launch argument '--provider' requires '--model'.",
    };
    assert.deepInclude(resolvePiLaunchArgs("--provider openrouter"), rejection);
    assert.deepInclude(resolvePiLaunchArgs("--provider=openrouter --models gpt-6"), rejection);
    assert.isTrue(resolvePiLaunchArgs("--provider openrouter --model=deepseek/v4").ok);
    assert.isTrue(resolvePiLaunchArgs("--model deepseek/v4").ok);
  });

  it.effect("materializes the MCP bridge with namespaced tool registration", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cacheDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pi-extensions-" });
      const mcpDest = yield* materializePiT3McpExtension(cacheDir);
      assert.isTrue(mcpDest.endsWith(PI_T3_MCP_EXTENSION_FILENAME));
      const mcpSource = yield* fs.readFileString(mcpDest);
      assert.include(mcpSource, "export default async function t3McpExtension");
      assert.include(mcpSource, "before_agent_start");
      assert.include(mcpSource, 'pi.on("tool_call"');
      assert.include(mcpSource, "Allow ${event.toolName}?");
      assert.include(mcpSource, '"mcp-protocol-version"');
      assert.include(mcpSource, '"tools/call"');
      assert.include(mcpSource, "mcp__t3-code__");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it("never registers native T3 tools for an agent with its own MCP client", () => {
    const base = {
      launchArgs: [],
      environment: {},
      extensionPath: "/tmp/cache/pi-t3-mcp-extension.ts",
    };
    const skillPath = "/tmp/cache/pi-t3-skills/t3-code";
    const declared = buildPiRpcLaunch({ ...base, mcpSession, kernelMcp: { skillPath } });
    assert.deepEqual(declared.args.slice(-2), ["--skill", skillPath]);
    assert.equal(declared.env[T3_PI_MCP_TOOLS_ENV], "kernel");

    // Undeclared: no skill, yet still no native tools and the permission hook stays.
    const undeclared = buildPiRpcLaunch({
      ...base,
      mcpSession,
      kernelMcp: { skillPath: undefined },
    });
    assert.notInclude(undeclared.args, "--skill");
    assert.include(undeclared.args, "--extension");
    assert.equal(undeclared.env[T3_PI_MCP_TOOLS_ENV], "kernel");

    // Without a T3 credential the skill would describe tools that cannot work.
    const noCredential = buildPiRpcLaunch({
      ...base,
      mcpSession: undefined,
      kernelMcp: { skillPath },
    });
    assert.notInclude(noCredential.args, "--skill");
    assert.equal(noCredential.env[T3_PI_MCP_TOOLS_ENV], "kernel");

    // No extension means nothing registers tools, so there is nothing to switch off.
    const noExtension = buildPiRpcLaunch({
      ...base,
      mcpSession,
      kernelMcp: { skillPath },
      disableExtensions: true,
    });
    assert.notProperty(noExtension.env, T3_PI_MCP_TOOLS_ENV);
  });

  it("keeps native tools for an agent without its own MCP client", () => {
    const launch = buildPiRpcLaunch({
      launchArgs: [],
      // A value inherited from the server must not switch off native tools.
      environment: { [T3_PI_MCP_TOOLS_ENV]: "kernel" },
      mcpSession,
      extensionPath: "/tmp/cache/pi-t3-mcp-extension.ts",
    });
    assert.notInclude(launch.args, "--skill");
    assert.notProperty(launch.env, T3_PI_MCP_TOOLS_ENV);
  });

  it.effect("materializes a skill whose directory, name, and description Pi accepts", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cacheDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pi-skill-" });
      const skillDir = yield* materializePiT3Skill(cacheDir);
      assert.isTrue(skillDir.endsWith(`/${PI_T3_CODE_SKILL_NAME}`));
      const content = yield* fs.readFileString(`${skillDir}/SKILL.md`);
      const frontmatter = /^---\n([\s\S]*?)\n---\n/.exec(content)?.[1] ?? "";
      assert.equal(/^name: (.+)$/m.exec(frontmatter)?.[1], "t3-code");
      const description = /^description: (.+)$/m.exec(frontmatter)?.[1] ?? "";
      assert.isTrue(description.length > 0 && description.length <= 1024);
      assert.include(content, 'await mcp.list_tools("t3-code")');
      assert.include(content, 'await mcp.call_tool("t3-code"');
      assert.notInclude(content, "acp-mcp-call");
      // Rewriting an unchanged skill must be a no-op.
      assert.equal(yield* materializePiT3Skill(cacheDir), skillDir);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
