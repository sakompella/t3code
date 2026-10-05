// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, it, expect } from "@effect/vitest";

import { parsePiLine, parsePiRecord, totalTokens } from "./usageTranscripts.ts";
import { readTranscriptRecords } from "./usageTranscriptReader.ts";
import { decodeScanCache, encodeScanCache, dedupeWithinFile } from "./usageScanCache.ts";
import { priceUsage, parseRateTable } from "./usagePricing.ts";
import { UsageAggregator } from "./usageAggregation.ts";
import { UsageDay } from "@t3tools/contracts";

const entry = {
  type: "message",
  id: "entry-1",
  message: {
    role: "assistant",
    model: "claude-opus-5-5",
    timestamp: Date.parse("2026-08-01T10:00:00Z"),
    responseId: "response-1",
    content: [{ type: "text", text: "hello" }],
    usage: {
      input: 2,
      output: 7,
      cacheRead: 11,
      cacheWrite: 13,
      totalTokens: 33,
      cost: { total: 0.5 },
    },
  },
};

const rates = parseRateTable({
  "claude-opus-5-5": { input_cost_per_token: 0.000004, output_cost_per_token: 0.00002 },
});

describe("Pi and Prime Agent usage", () => {
  it.each(["pi", "primeAgent"] as const)(
    "reads %s token categories without adding totalTokens again",
    (provider) => {
      const record = parsePiRecord(entry, provider, "session-1");
      expect(record?.totals).toEqual({
        uncachedInputTokens: 2,
        outputTokens: 7,
        cachedInputTokens: 11,
        cacheCreationTokens: 13,
        reasoningTokens: 0,
      });
      expect(totalTokens(record!.totals)).toBe(33);
      expect(priceUsage(rates, record!).costUsd).toBe(0.5);
    },
  );

  it("uses model rates when configured prices are zero", () => {
    const record = parsePiRecord(
      {
        ...entry,
        message: { ...entry.message, usage: { ...entry.message.usage, cost: { total: 0 } } },
      },
      "primeAgent",
      "session-1",
    )!;
    expect(record.reportedCostUsd).toBeNull();
    expect(priceUsage(rates, record).costSource).toBe("modelPriced");
    expect(priceUsage(rates, record).costUsd).toBeCloseTo(0.000244);
  });

  it("does not count tool results or child attribution summaries", () => {
    expect(
      parsePiRecord({ ...entry, type: "child_usage_attributed" }, "primeAgent", "parent"),
    ).toBeNull();
    expect(
      parsePiRecord({ ...entry, message: { ...entry.message, role: "toolResult" } }, "pi", "s"),
    ).toBeNull();
    expect(parsePiLine('{"type":"message"', "pi", "s")).toBeNull();
    expect(
      parsePiRecord({ ...entry, message: { ...entry.message, timestamp: "invalid" } }, "pi", "s"),
    ).toBeNull();
  });

  it("drops fork/copied responses across parent and child files, without conflating entry IDs", () => {
    const parent = parsePiRecord(entry, "primeAgent", "parent")!;
    const fork = parsePiRecord(entry, "primeAgent", "fork")!;
    const child = parsePiRecord(
      { ...entry, message: { ...entry.message, responseId: "child-response" } },
      "primeAgent",
      "child",
    )!;
    const aggregator = new UsageAggregator({
      timeZone: "UTC",
      sinceDay: UsageDay.make("2026-08-01"),
      untilDay: UsageDay.make("2026-08-01"),
      rates,
    });
    for (const record of [parent, fork, child]) aggregator.add(record);
    expect(aggregator.finish().buckets.reduce((sum, bucket) => sum + bucket.records, 0)).toBe(2);
    const noId = { ...entry, message: { ...entry.message, responseId: "" } };
    expect(parsePiRecord(noId, "pi", "a")?.dedupeKey).not.toBe(
      parsePiRecord(noId, "pi", "b")?.dedupeKey,
    );
  });

  it("preserves counts through streaming projection, partial writes, resume and disk cache", async () => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "pi-usage-"));
    const file = NodePath.join(root, "session-1.jsonl");
    try {
      const line = JSON.stringify(entry);
      const childLine = JSON.stringify({
        ...entry,
        message: { ...entry.message, responseId: "child" },
      });
      await NodeFSP.writeFile(file, line + "\n" + childLine.slice(0, 50));
      const first = (await readTranscriptRecords(file, "primeAgent", undefined, {
        streamingThresholdBytes: 1,
      }))!;
      expect(first.records).toHaveLength(1);
      expect(first.tailRecords).toHaveLength(0);
      await NodeFSP.appendFile(file, childLine.slice(50) + "\n");
      const next = (await readTranscriptRecords(file, "primeAgent", first.position, {
        streamingThresholdBytes: 1,
      }))!;
      expect(next.resumed).toBe(true);
      const all = dedupeWithinFile([...first.records, ...next.records], new Set());
      const full = (await readTranscriptRecords(file, "primeAgent"))!;
      expect(all).toEqual(full.records);
      const stat = await NodeFSP.stat(file);
      const cache = new Map([
        [
          file,
          {
            size: stat.size,
            mtimeMs: stat.mtimeMs,
            provider: "primeAgent" as const,
            records: all,
            tailRecords: [],
            position: next.position,
          },
        ],
      ]);
      expect(decodeScanCache(encodeScanCache(cache)).get(file)?.records).toEqual(all);
    } finally {
      await NodeFSP.rm(root, { recursive: true, force: true });
    }
  });
});
