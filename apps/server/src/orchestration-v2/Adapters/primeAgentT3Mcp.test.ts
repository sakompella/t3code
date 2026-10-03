import { assert, describe, it } from "@effect/vitest";

import { isT3CodeMcpDeclared, t3CodeMcpSetupHint } from "./primeAgentT3Mcp.ts";

const ENDPOINT = "http://127.0.0.1:3773/mcp";
const entry = { type: "http", url: ENDPOINT, bearerTokenEnvVar: "T3_MCP_BEARER_TOKEN" };
const settings = (server: unknown) => JSON.stringify({ mcpServers: { "t3-code": server } });

describe("isT3CodeMcpDeclared", () => {
  it("accepts the entry the hint tells the user to add", () => {
    assert.isTrue(isT3CodeMcpDeclared(settings(entry), ENDPOINT));
    assert.isTrue(isT3CodeMcpDeclared(settings({ ...entry, url: `${ENDPOINT}/` }), ENDPOINT));
    assert.isTrue(isT3CodeMcpDeclared(settings({ ...entry, enabled: true }), ENDPOINT));
  });

  it("rejects anything that would not reach this server with this credential", () => {
    const rejected = [
      undefined,
      "not json",
      "{}",
      JSON.stringify({ mcpServers: { other: entry } }),
      settings(null),
      settings({ ...entry, url: "http://127.0.0.1:4000/mcp" }),
      settings({ ...entry, bearerTokenEnvVar: undefined }),
      settings({ ...entry, bearerTokenEnvVar: "OTHER_TOKEN" }),
      settings({ ...entry, enabled: false }),
      settings({ type: "stdio", command: "t3" }),
    ];
    for (const text of rejected) assert.isFalse(isT3CodeMcpDeclared(text, ENDPOINT), String(text));
  });

  it("writes a hint whose entry the check accepts", () => {
    const hint = t3CodeMcpSetupHint({
      displayName: "Prime Agent",
      settingsPath: "/home/u/.prime/agent/settings.json",
      endpoint: ENDPOINT,
    });
    const json = /(\{"t3-code":.*\})/.exec(hint)?.[1] ?? "";
    assert.isTrue(isT3CodeMcpDeclared(JSON.stringify({ mcpServers: JSON.parse(json) }), ENDPOINT));
    assert.include(hint, "/home/u/.prime/agent/settings.json");
  });
});
