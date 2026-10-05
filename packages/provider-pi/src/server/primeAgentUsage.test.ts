import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";

import { primeAgentUsageFormat, primeAgentUsageReader } from "./usage.ts";

const assistantLine = JSON.stringify({
  type: "message",
  id: "4aab6945",
  parentId: "813dc39a",
  timestamp: "2026-10-04T07:48:03.962Z",
  message: {
    role: "assistant",
    api: "anthropic-messages",
    provider: "cpa-claude",
    model: "claude-opus-5-5",
    responseId: "response-1",
    timestamp: 1_791_100_083_962,
    usage: { input: 2, output: 7, cacheRead: 11, cacheWrite: 13, cost: { total: 0.5 } },
  },
});

const parse = (line: string) =>
  primeAgentUsageFormat.parseLine(line, primeAgentUsageFormat.state!.initial());

describe("Prime Agent usage", () => {
  it("counts an assistant response under the Prime Agent series", () => {
    const [record] = parse(assistantLine);
    assert.equal(record?.provider, "primeAgent");
    assert.equal(record?.model, "cpa-claude/claude-opus-5-5");
    assert.deepEqual(record?.totals, {
      uncachedInputTokens: 2,
      cachedInputTokens: 11,
      cacheCreationTokens: 13,
      outputTokens: 7,
      reasoningTokens: 0,
    });
    assert.equal(record?.reportedCostUsd, 0.5);
  });

  it("does not count a child attribution as usage of its own", () => {
    const attribution = JSON.stringify({
      type: "child_usage_attributed",
      id: "d0e59c5e",
      timestamp: "2026-10-07T18:31:29.284Z",
      targetId: "4aab6945",
      childUsage: { input: 22, output: 3310, cost: { total: 0.56 } },
    });
    assert.deepEqual(parse(attribution), []);
  });

  it.effect("reads parent and subagent sessions under the agent home", () =>
    Effect.gen(function* () {
      const instance = (environment: NodeJS.ProcessEnv) => ({
        instanceId: ProviderInstanceId.make("primeAgent"),
        config: undefined,
        environment,
        configured: false,
      });
      const defaults = yield* primeAgentUsageReader.kind === "transcripts"
        ? primeAgentUsageReader.directories(instance({}))
        : Effect.die("Prime Agent usage reads transcripts");
      assert.deepEqual(
        defaults.map((directory) => directory.dir),
        ["/home/user/.prime/agent/sessions", "/home/user/.prime/agent/session-artifacts"],
      );
      const custom = yield* primeAgentUsageReader.kind === "transcripts"
        ? primeAgentUsageReader.directories(
            instance({ PRIME_AGENT_CODING_AGENT_DIR: "~/agents/work" }),
          )
        : Effect.die("Prime Agent usage reads transcripts");
      assert.deepEqual(
        custom.map((directory) => directory.dir),
        ["/home/user/agents/work/sessions", "/home/user/agents/work/session-artifacts"],
      );
    }).pipe(
      Effect.provideService(HostProcess.HomeDirectory, "/home/user"),
      Effect.provide(NodeServices.layer),
    ),
  );
});
