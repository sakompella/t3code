// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it } from "@effect/vitest";
import type { TranscriptUsageFormat } from "@t3tools/provider-core/server/usage";
import { primeAgentUsageFormat } from "@t3tools/provider-pi/server/usage";

import { readTranscriptRecords } from "./usageTranscriptReader.ts";
import { decodeScanCache, dedupeWithinFile, encodeScanCache } from "./usageScanCache.ts";

const formats = new Map([
  ["primeAgent" as const, primeAgentUsageFormat as TranscriptUsageFormat<unknown>],
]);

const entry = {
  type: "message",
  id: "target01",
  timestamp: "2026-08-01T10:00:00.000Z",
  message: {
    role: "assistant",
    provider: "anthropic",
    model: "claude-opus-5-5",
    timestamp: Date.parse("2026-08-01T10:00:00Z"),
    responseId: "response-1",
    usage: { input: 2, output: 7, cacheRead: 11, cacheWrite: 13, cost: { total: 0.5 } },
  },
};

describe("Prime Agent usage scans", () => {
  it.each([{ streaming: false }, { streaming: true }])(
    "corrects cached folded usage when a later attribution arrives (streaming $streaming)",
    async ({ streaming }) => {
      const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "prime-late-attribution-"));
      const file = NodePath.join(root, "fork.jsonl");
      try {
        const childUsage = { input: 10, output: 3, cacheRead: 0, cacheWrite: 0, cost: { total: 0.75 } };
        const aggregateUsage = {
          input: 12,
          output: 10,
          cacheRead: 11,
          cacheWrite: 13,
          cost: { total: 1.25 },
        };
        const folded = { ...entry, message: { ...entry.message, usage: aggregateUsage } };
        const attribution = JSON.stringify({
          type: "child_usage_attributed",
          id: "attribution",
          timestamp: "2026-08-01T10:05:00.000Z",
          targetId: entry.id,
          childUsage,
          aggregateUsage,
        });
        await NodeFSP.writeFile(file, JSON.stringify(folded) + "\n" + attribution.slice(0, 45));
        const options = { streamingThresholdBytes: streaming ? 1 : Number.POSITIVE_INFINITY };
        const first = (await readTranscriptRecords(file, primeAgentUsageFormat, undefined, options))!;
        const stat = await NodeFSP.stat(file);
        const restored = decodeScanCache(
          encodeScanCache(
            new Map([
              [
                file,
                {
                  size: stat.size,
                  mtimeMs: stat.mtimeMs,
                  provider: "primeAgent" as const,
                  records: first.records,
                  tailRecords: first.tailRecords,
                  position: first.position,
                },
              ],
            ]),
          ),
          formats,
        ).get(file)!;
        await NodeFSP.appendFile(file, attribution.slice(45) + "\n");
        const next = (await readTranscriptRecords(
          file,
          primeAgentUsageFormat,
          restored.position,
          options,
        ))!;
        const combined = dedupeWithinFile(
          [...(next.resumed ? restored.records : []), ...next.records],
          new Set(),
        );
        const complete = (await readTranscriptRecords(
          file,
          primeAgentUsageFormat,
          undefined,
          options,
        ))!;
        expect(next.resumed).toBe(false);
        expect(combined).toEqual(complete.records);
        expect(combined[0]?.totals).toEqual({
          uncachedInputTokens: 2,
          outputTokens: 7,
          cachedInputTokens: 11,
          cacheCreationTokens: 13,
          reasoningTokens: 0,
        });
        expect(combined[0]?.reportedCostUsd).toBe(0.5);
      } finally {
        await NodeFSP.rm(root, { recursive: true, force: true });
      }
    },
  );
});
