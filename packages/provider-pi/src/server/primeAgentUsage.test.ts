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

  // A fork or rewrite persists the parent's in-memory folded usage, followed by
  // the attributions that give its own usage back.
  it.each([
    { name: "a raw parent", folded: false },
    { name: "a folded parent copy", folded: true },
  ])("counts $name at its own usage", ({ folded }) => {
    const usage = (input: number, output: number, cost: number) => ({
      input,
      output,
      cacheRead: 100,
      cacheWrite: 10,
      cost: { total: cost },
    });
    const parent = JSON.stringify({
      ...JSON.parse(assistantLine),
      id: "target01",
      message: {
        ...JSON.parse(assistantLine).message,
        usage: folded ? usage(2 + 50 + 30, 7 + 5 + 3, 0.9) : usage(2, 7, 0.5),
      },
    });
    const attribution = (id: string, child: number, aggregate: number, cost: number) =>
      JSON.stringify({
        type: "child_usage_attributed",
        id,
        timestamp: "2026-10-04T07:50:00.000Z",
        targetId: "target01",
        childUsage: { input: child, output: child / 10, cost: { total: cost } },
        aggregateUsage: usage(aggregate, 7 + (aggregate - 2) / 10, 0.5 + cost),
      });
    const state = primeAgentUsageFormat.state!.initial();
    const records = [
      parent,
      attribution("attr0001", 50, 52, 0.25),
      attribution("attr0002", 30, 82, 0.15),
    ].flatMap((line) => primeAgentUsageFormat.parseLine(line, state));
    assert.lengthOf(records, 1);
    assert.deepEqual(records[0]?.totals, {
      uncachedInputTokens: 2,
      cachedInputTokens: 100,
      cacheCreationTokens: 10,
      outputTokens: 7,
      reasoningTokens: 0,
    });
    assert.closeTo(records[0]?.reportedCostUsd ?? 0, 0.5, 1e-9);
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
