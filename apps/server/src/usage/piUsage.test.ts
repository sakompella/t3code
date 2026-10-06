// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, it, expect } from "@effect/vitest";

import { PI_SUMMARY_MODEL, parsePiRecord, totalTokens } from "./usageTranscripts.ts";
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
    // A copied aborted response keeps its entry ID and time; another response does not.
    expect(parsePiRecord(noId, "pi", "a")?.dedupeKey).toBe(
      parsePiRecord(noId, "pi", "fork")?.dedupeKey,
    );
    expect(parsePiRecord(noId, "pi", "a")?.dedupeKey).not.toBe(
      parsePiRecord({ ...noId, message: { ...noId.message, timestamp: 1 } }, "pi", "b")?.dedupeKey,
    );
  });

  // Prime Agent folds child usage into the parent response in memory. A fork or
  // rewrite persists that folded value under the same response ID.
  it.each([
    { order: "original first", streaming: false },
    { order: "fork first", streaming: false },
    { order: "fork first", streaming: true },
  ])(
    "counts a parent response once at its own usage ($order, streaming $streaming)",
    async ({ order, streaming }) => {
      const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "pi-fold-"));
      try {
        const usage = (input: number, output: number, cost: number) => ({
          input,
          output,
          cacheRead: 100,
          cacheWrite: 10,
          totalTokens: input + output + 110,
          cost: { total: cost },
        });
        const parentLine = (folded: boolean) =>
          JSON.stringify({
            ...entry,
            id: "target01",
            message: {
              ...entry.message,
              usage: folded ? usage(2 + 50 + 30, 7 + 5 + 3, 0.5) : usage(2, 7, 0.5),
            },
          });
        const attribution = (id: string, child: number, aggregate: number) =>
          JSON.stringify({
            type: "child_usage_attributed",
            id,
            targetId: "target01",
            childUsage: { ...usage(child, child / 10, 0), cacheRead: 0, cacheWrite: 0 },
            aggregateUsage: usage(aggregate, 7 + (aggregate - 2) / 10, 0.5),
          });
        const attributions = [attribution("attr0001", 50, 52), attribution("attr0002", 30, 82)];
        const toolLine = JSON.stringify({ type: "message", message: { role: "toolResult" } });
        const original = NodePath.join(root, "original.jsonl");
        const fork = NodePath.join(root, "fork.jsonl");
        await NodeFSP.writeFile(
          original,
          [parentLine(false), toolLine, ...attributions, ""].join("\n"),
        );
        await NodeFSP.writeFile(
          fork,
          [parentLine(true), "{broken", ...attributions, ""].join("\n"),
        );
        const aggregator = new UsageAggregator({
          timeZone: "UTC",
          sinceDay: UsageDay.make("2026-08-01"),
          untilDay: UsageDay.make("2026-08-01"),
          rates,
        });
        const files = order === "original first" ? [original, fork] : [fork, original];
        for (const file of files) {
          const parsed = (await readTranscriptRecords(file, "primeAgent", undefined, {
            streamingThresholdBytes: streaming ? 1 : Number.POSITIVE_INFINITY,
          }))!;
          for (const record of parsed.records) aggregator.add(record);
        }
        const [bucket, ...rest] = aggregator.finish().buckets;
        expect(rest).toEqual([]);
        expect(bucket?.records).toBe(1);
        expect(bucket?.totals).toEqual({
          uncachedInputTokens: 2,
          outputTokens: 7,
          cachedInputTokens: 100,
          cacheCreationTokens: 10,
          reasoningTokens: 0,
        });
        expect(bucket?.costUsd).toBeCloseTo(0.5);
      } finally {
        await NodeFSP.rm(root, { recursive: true, force: true });
      }
    },
  );

  it.each([
    { provider: "pi" as const, streaming: false },
    { provider: "primeAgent" as const, streaming: false },
    { provider: "primeAgent" as const, streaming: true },
  ])(
    "corrects cached folded usage when a later attribution finishes ($provider, streaming $streaming)",
    async ({ provider, streaming }) => {
      const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "pi-late-attribution-"));
      const file = NodePath.join(root, "fork.jsonl");
      try {
        const childUsage = {
          input: 10,
          output: 3,
          cacheRead: 0,
          cacheWrite: 0,
          cost: { total: 0.75 },
        };
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
          targetId: entry.id,
          childUsage,
          aggregateUsage,
        });
        await NodeFSP.writeFile(file, JSON.stringify(folded) + "\n" + attribution.slice(0, 45));
        const options = { streamingThresholdBytes: streaming ? 1 : Number.POSITIVE_INFINITY };
        const first = (await readTranscriptRecords(file, provider, undefined, options))!;
        const stat = await NodeFSP.stat(file);
        const restored = decodeScanCache(
          encodeScanCache(
            new Map([
              [
                file,
                {
                  size: stat.size,
                  mtimeMs: stat.mtimeMs,
                  provider,
                  records: first.records,
                  tailRecords: first.tailRecords,
                  position: first.position,
                },
              ],
            ]),
          ),
        ).get(file)!;
        await NodeFSP.appendFile(file, attribution.slice(45) + "\n");
        const next = (await readTranscriptRecords(file, provider, restored.position, options))!;
        const combined = dedupeWithinFile(
          [...(next.resumed ? restored.records : []), ...next.records],
          new Set(),
        );
        const complete = (await readTranscriptRecords(file, provider, undefined, options))!;
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

  it("counts compaction and branch summaries once, with their recorded cost", async () => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "pi-summary-"));
    try {
      const summary = (type: string, id: string, cost: number) =>
        JSON.stringify({
          type,
          id,
          timestamp: "2026-08-01T11:00:00.000Z",
          summary: "...",
          usage: { input: 4, output: 20, cacheRead: 0, cacheWrite: 300, cost: { total: cost } },
        });
      const lines = [
        summary("compaction", "comp0001", 2),
        summary("branch_summary", "branch01", 0),
      ];
      const aggregator = new UsageAggregator({
        timeZone: "UTC",
        sinceDay: UsageDay.make("2026-08-01"),
        untilDay: UsageDay.make("2026-08-01"),
        rates,
      });
      for (const name of ["session.jsonl", "fork.jsonl"]) {
        const file = NodePath.join(root, name);
        await NodeFSP.writeFile(file, lines.join("\n") + "\n");
        for (const record of (await readTranscriptRecords(file, "pi"))!.records) {
          expect(record.model).toBe(PI_SUMMARY_MODEL);
          aggregator.add(record);
        }
      }
      const buckets = aggregator.finish().buckets;
      expect(buckets.reduce((sum, bucket) => sum + bucket.records, 0)).toBe(2);
      expect(buckets.reduce((sum, bucket) => sum + bucket.totals.outputTokens, 0)).toBe(40);
      expect(buckets.reduce((sum, bucket) => sum + bucket.costUsd, 0)).toBe(2);
    } finally {
      await NodeFSP.rm(root, { recursive: true, force: true });
    }
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
