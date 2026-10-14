/**
 * Native Pi and Prime Agent session usage, including calls billed outside the
 * main conversation. Prime Agent is a Pi fork and writes the same session log.
 */
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import {
  parseTimestampMs,
  tokenCount,
  totalTokens,
  type ProviderUsageReader,
  type TranscriptUsageFormat,
  type UsageRecord,
} from "@t3tools/provider-core/server/usage";
import * as HostProcess from "@t3tools/shared/HostProcess";
import type { PiSettings } from "../settings.ts";
import { PRIME_AGENT_FLAVOR, resolvePiAgentDir } from "./flavor.ts";

type PiUsageProvider = "pi" | "primeAgent";

const PiScanState = Schema.Struct({ sessionId: Schema.mutableKey(Schema.String) });
const Usage = Schema.Struct({
  input: Schema.optional(Schema.Unknown),
  output: Schema.optional(Schema.Unknown),
  cacheRead: Schema.optional(Schema.Unknown),
  cacheWrite: Schema.optional(Schema.Unknown),
  reasoning: Schema.optional(Schema.Unknown),
  cost: Schema.optional(Schema.Struct({ total: Schema.Unknown })),
});
const Identity = { id: Schema.NonEmptyString, timestamp: Schema.String };
const Model = { provider: Schema.NonEmptyString, model: Schema.NonEmptyString };
const Entry = Schema.Union([
  Schema.Struct({ type: Schema.Literal("session"), id: Schema.NonEmptyString }),
  Schema.Struct({
    ...Identity,
    type: Schema.Literal("message"),
    message: Schema.Union([
      Schema.Struct({
        ...Model,
        role: Schema.Literal("assistant"),
        responseModel: Schema.optional(Schema.NonEmptyString),
        usage: Usage,
      }),
      Schema.Struct({ role: Schema.Literal("toolResult"), usage: Usage }),
    ]),
  }),
  Schema.Struct({ ...Identity, ...Model, type: Schema.Literal("usage"), usage: Usage }),
  Schema.Struct({
    ...Identity,
    type: Schema.Literals(["compaction", "branch_summary"]),
    usage: Usage,
  }),
]);
const decodeEntry = Schema.decodeUnknownOption(Entry);

function parseRecord(
  provider: PiUsageProvider,
  parsed: unknown,
  state: typeof PiScanState.Type,
): readonly UsageRecord[] {
  const decoded = decodeEntry(parsed);
  if (Option.isNone(decoded)) return [];
  const entry = decoded.value;
  if (entry.type === "session") {
    state.sessionId = entry.id;
    return [];
  }
  const timestampMs = parseTimestampMs(entry.timestamp);
  if (timestampMs === null) return [];
  const source = entry.type === "message" ? entry.message : entry;
  const usage = source.usage;
  // Pi's auxiliary tool and summary records carry no model attribution. Match
  // Pi's own breakdown rather than assigning them to the conversation model.
  const rateModel =
    "model" in source
      ? "responseModel" in source
        ? (source.responseModel ?? source.model)
        : source.model
      : undefined;
  const model = "provider" in source ? `${source.provider}/${rateModel}` : "Tools/summaries";
  const outputTokens = tokenCount(usage.output);
  const totals = {
    // Pi's input/cache counters are already disjoint.
    uncachedInputTokens: tokenCount(usage.input),
    cachedInputTokens: tokenCount(usage.cacheRead),
    cacheCreationTokens: tokenCount(usage.cacheWrite),
    outputTokens,
    reasoningTokens: Math.min(outputTokens, tokenCount(usage.reasoning)),
  };
  const cost = usage.cost?.total;
  // Pi defaults omitted catalog rates to zero. Let shared pricing estimate
  // API-equivalent cost when the transcript has no positive cost estimate.
  const reportedCostUsd =
    typeof cost === "number" && Number.isFinite(cost) && cost > 0 ? cost : null;
  if (totalTokens(totals) === 0 && reportedCostUsd === null) return [];
  return [
    {
      provider,
      timestampMs,
      model,
      ...(rateModel === undefined ? {} : { rateModel }),
      sessionId: state.sessionId,
      totals,
      reportedCostUsd,
      speed: "standard",
      // Forks copy entries verbatim, including ID/timestamp. Entry IDs are only
      // eight random hex digits, so retain timestamp/model to avoid collisions
      // across unrelated sessions.
      dedupeKey: JSON.stringify([provider, entry.id, entry.timestamp, model]),
    },
  ];
}

const makePiUsageFormat = (
  provider: PiUsageProvider,
): TranscriptUsageFormat<typeof PiScanState.Type> => ({
  selectFields: {
    type: true,
    id: true,
    timestamp: true,
    provider: true,
    model: true,
    usage: true,
    message: { role: true, provider: true, model: true, responseModel: true, usage: true },
  },
  usageMarkers: ['"usage"', '"session"'],
  parseLine: (line, state) => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      return [];
    }
    return parseRecord(provider, parsed, state);
  },
  parseProjected: (projected, state) => parseRecord(provider, projected, state),
  state: { initial: () => ({ sessionId: "" }), schema: PiScanState },
});

export const piUsageFormat = makePiUsageFormat("pi");

const decodeAttribution = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literal("child_usage_attributed"),
    targetId: Schema.NonEmptyString,
    childUsage: Usage,
    aggregateUsage: Usage,
  }),
);

type MutableUsageRecord = { -readonly [Key in keyof UsageRecord]: UsageRecord[Key] };

/**
 * The responses one parse of a file has produced so far, by entry id. They only
 * matter within that parse, so they sit beside the serializable scan state.
 */
const parsedResponses = new WeakMap<typeof PiScanState.Type, Map<string, MutableUsageRecord>>();
/** Parses whose attribution named a response an earlier, cached parse produced. */
const lateAttributions = new WeakSet<typeof PiScanState.Type>();

const usageCost = (usage: typeof Usage.Type): number => {
  const cost = usage.cost?.total;
  return typeof cost === "number" && Number.isFinite(cost) ? cost : 0;
};

/**
 * Prime Agent folds a subagent's usage into its parent response in memory.
 * Appended lines stay raw, but a fork or file rewrite persists the folded value
 * under the same entry, followed by the copied `child_usage_attributed` entries
 * that give the parent's own usage back. The first attribution for a response
 * restores its own totals; on a raw line it changes nothing.
 */
function restoreOwnUsage(parsed: unknown, state: typeof PiScanState.Type): boolean {
  const attribution = decodeAttribution(parsed);
  if (Option.isNone(attribution)) return false;
  const responses = parsedResponses.get(state);
  const record = responses?.get(attribution.value.targetId);
  if (responses === undefined || record === undefined) {
    lateAttributions.add(state);
    return true;
  }
  responses.delete(attribution.value.targetId);
  const { aggregateUsage, childUsage } = attribution.value;
  const own = (key: "input" | "output" | "cacheRead" | "cacheWrite") =>
    Math.max(0, tokenCount(aggregateUsage[key]) - tokenCount(childUsage[key]));
  const totals = {
    uncachedInputTokens: own("input"),
    cachedInputTokens: own("cacheRead"),
    cacheCreationTokens: own("cacheWrite"),
    outputTokens: own("output"),
    reasoningTokens: 0,
  };
  const unchanged =
    totals.uncachedInputTokens === record.totals.uncachedInputTokens &&
    totals.cachedInputTokens === record.totals.cachedInputTokens &&
    totals.cacheCreationTokens === record.totals.cacheCreationTokens &&
    totals.outputTokens === record.totals.outputTokens;
  // A raw line already holds these totals; keep its exact recorded cost.
  if (totalTokens(totals) === 0 || unchanged) return true;
  const ownCost = usageCost(aggregateUsage) - usageCost(childUsage);
  record.totals = {
    ...totals,
    reasoningTokens: Math.min(totals.outputTokens, record.totals.reasoningTokens),
  };
  record.reportedCostUsd = ownCost > 0 ? ownCost : null;
  return true;
}

function parsePrimeAgentRecord(
  parsed: unknown,
  state: typeof PiScanState.Type,
): readonly UsageRecord[] {
  if (restoreOwnUsage(parsed, state)) return [];
  const records = parseRecord("primeAgent", parsed, state);
  const entryId =
    Predicate.isObject(parsed) && typeof parsed["id"] === "string" ? parsed["id"] : "";
  const [record] = records;
  if (entryId && record !== undefined && records.length === 1) {
    const responses = parsedResponses.get(state) ?? new Map<string, MutableUsageRecord>();
    parsedResponses.set(state, responses);
    responses.set(entryId, record as MutableUsageRecord);
  }
  return records;
}

const piFormat = makePiUsageFormat("primeAgent");

export const primeAgentUsageFormat: TranscriptUsageFormat<typeof PiScanState.Type> = {
  ...piFormat,
  selectFields: {
    ...piFormat.selectFields,
    targetId: true,
    childUsage: true,
    aggregateUsage: true,
  },
  usageMarkers: [...piFormat.usageMarkers, '"child_usage_attributed"'],
  parseLine: (line, state) => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      return [];
    }
    return parsePrimeAgentRecord(parsed, state);
  },
  parseProjected: parsePrimeAgentRecord,
  state: {
    ...piFormat.state!,
    // A resumed parse starts without the records before its resume point, so
    // an attribution for one of them repairs the cached totals by a full parse.
    needsFullParse: (state) => lateAttributions.has(state),
  },
};

export const piUsageReader: ProviderUsageReader<PiSettings, Path.Path> = {
  kind: "transcripts",
  provider: "pi",
  format: piUsageFormat,
  directories: Effect.fn("piUsageReader.directories")(function* ({ environment }) {
    const path = yield* Path.Path;
    const home = yield* HostProcess.HomeDirectory;
    const sessionDir = environment.PI_CODING_AGENT_SESSION_DIR?.trim();
    if (sessionDir) return [{ dir: path.resolve(expandHomePath(sessionDir, home)) }];
    const agentDir = environment.PI_CODING_AGENT_DIR?.trim();
    return [
      {
        dir: path.resolve(
          agentDir ? expandHomePath(agentDir, home) : path.join(home, ".pi", "agent"),
          "sessions",
        ),
      },
    ];
  }),
};

/**
 * Prime Agent keeps parent sessions in `sessions` and subagent sessions under
 * `session-artifacts`, so both count.
 */
export const primeAgentUsageReader: ProviderUsageReader<PiSettings, Path.Path> = {
  kind: "transcripts",
  provider: "primeAgent",
  format: primeAgentUsageFormat,
  directories: Effect.fn("primeAgentUsageReader.directories")(function* ({ environment }) {
    const path = yield* Path.Path;
    const root = path.resolve(
      resolvePiAgentDir({
        agentDir: PRIME_AGENT_FLAVOR.agentDir,
        environment,
        home: yield* HostProcess.HomeDirectory,
      }),
    );
    return [{ dir: path.join(root, "sessions") }, { dir: path.join(root, "session-artifacts") }];
  }),
};
