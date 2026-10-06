/**
 * Pure parsers for the provider CLIs' on-disk session transcripts.
 *
 * Each parser is a line-at-a-time reducer so callers can stream large files
 * without materialising them. None of them touch the filesystem.
 *
 * @module usageTranscripts
 */
import type { UsageProviderKind, UsageTokenTotals } from "@t3tools/contracts";

/**
 * Billing speed of a request. Faster speeds bill at a model-specific premium.
 * Claude fast mode and Codex `priority` are `fast`; Codex `ultrafast` is its
 * own, more expensive tier.
 */
export type UsageSpeed = "standard" | "fast" | "ultrafast";

export interface UsageRecord {
  readonly provider: UsageProviderKind;
  readonly timestampMs: number;
  readonly model: string;
  /**
   * Rate-table key when the provider's display name carries tiers the table
   * does not know, such as Cursor's `claude-opus-5-5-high`. Defaults to `model`.
   */
  readonly rateModel?: string;
  readonly sessionId: string;
  readonly totals: UsageTokenTotals;
  readonly reportedCostUsd: number | null;
  /** Only Claude Code and Codex record a speed; other providers are `standard`. */
  readonly speed: UsageSpeed;
  /**
   * Key for cross-file de-duplication, or `null` when the record is inherently
   * unique and needs no dedup.
   */
  readonly dedupeKey: string | null;
}

const EMPTY_TOTALS: UsageTokenTotals = {
  uncachedInputTokens: 0,
  cachedInputTokens: 0,
  cacheCreationTokens: 0,
  outputTokens: 0,
  reasoningTokens: 0,
};

function int(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? Math.trunc(value) : 0;
}

function parseTimestampMs(value: unknown): number | null {
  if (typeof value !== "string") return null;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? null : parsed;
}

export function addTotals(a: UsageTokenTotals, b: UsageTokenTotals): UsageTokenTotals {
  return {
    uncachedInputTokens: a.uncachedInputTokens + b.uncachedInputTokens,
    cachedInputTokens: a.cachedInputTokens + b.cachedInputTokens,
    cacheCreationTokens: a.cacheCreationTokens + b.cacheCreationTokens,
    outputTokens: a.outputTokens + b.outputTokens,
    reasoningTokens: a.reasoningTokens + b.reasoningTokens,
  };
}

export function totalTokens(totals: UsageTokenTotals): number {
  // reasoningTokens is a subset of outputTokens and must not be added again.
  return (
    totals.uncachedInputTokens +
    totals.cachedInputTokens +
    totals.cacheCreationTokens +
    totals.outputTokens
  );
}

export function isPiUsageProvider(provider: unknown): provider is "pi" | "primeAgent" {
  return provider === "pi" || provider === "primeAgent";
}

/**
 * Cheap substring gate applied before `JSON.parse`.
 *
 * Transcripts are mostly tool output; only a minority of lines carry usage. On
 * a 30-day window this skips roughly half the lines outright and is worth about
 * an order of magnitude.
 */
export function mightCarryUsage(line: string, provider: UsageProviderKind): boolean {
  if (provider === "claude") return line.includes('"usage"');
  if (isPiUsageProvider(provider))
    return line.includes('"usage"') || line.includes('"child_usage_attributed"');
  if (provider === "grok") return line.includes('"turn_completed"');
  return line.includes('"token_count"');
}

/**
 * Grok reports cost in integer ticks where `1 USD = 10^10` ticks. See Grok
 * headless `total_cost_usd_ticks`. Convert to dollars for pricing.
 */
export const GROK_COST_USD_TICKS_PER_DOLLAR = 10_000_000_000;

function grokCostTicksToUsd(ticks: unknown): number | null {
  if (typeof ticks !== "number" || !Number.isFinite(ticks) || ticks < 0) return null;
  return ticks / GROK_COST_USD_TICKS_PER_DOLLAR;
}

/* Pi and Prime Agent share the same session JSONL format. */

/** Usage fields shared by Pi assistant messages and summary entries. */
function parsePiUsage(
  usage: unknown,
): { readonly totals: UsageTokenTotals; readonly costUsd: number } | null {
  if (typeof usage !== "object" || usage === null) return null;
  const totals = {
    uncachedInputTokens: int("input" in usage ? usage.input : undefined),
    cachedInputTokens: int("cacheRead" in usage ? usage.cacheRead : undefined),
    cacheCreationTokens: int("cacheWrite" in usage ? usage.cacheWrite : undefined),
    outputTokens: int("output" in usage ? usage.output : undefined),
    reasoningTokens: 0,
  };
  const cost = "cost" in usage ? usage.cost : undefined;
  const total =
    typeof cost === "object" && cost !== null && "total" in cost ? cost.total : undefined;
  return {
    totals,
    costUsd: typeof total === "number" && Number.isFinite(total) && total > 0 ? total : 0,
  };
}

/**
 * Per-parse state for one Pi transcript.
 *
 * Prime Agent folds a subagent's usage into the parent response in memory. Appended
 * lines stay raw, but a fork (`--fork`), branch or file rewrite writes the folded
 * value under the same response ID. The attribution entries copied with it give
 * back the parent's own usage. If a resumed scan sees an attribution for an
 * earlier response, the reader reparses that file to repair its cached totals.
 */
export interface PiScanState {
  readonly recordsByEntryId: Map<string, { readonly out: UsageRecord[]; readonly index: number }>;
}

export function initialPiScanState(): PiScanState {
  return { recordsByEntryId: new Map() };
}

/** The log does not say which model wrote a compaction or branch summary. */
export const PI_SUMMARY_MODEL = "summary (model not recorded)";

/** Applies one parsed Pi entry: adds usage records or restores a folded parent response. */
export function applyPiEntry(
  parsed: unknown,
  provider: "pi" | "primeAgent",
  sessionId: string,
  state: PiScanState,
  out: UsageRecord[],
): void {
  if (typeof parsed !== "object" || parsed === null || !("type" in parsed)) return;
  const entryId = "id" in parsed && typeof parsed.id === "string" ? parsed.id : "";
  if (parsed.type === "child_usage_attributed") {
    const targetId =
      "targetId" in parsed && typeof parsed.targetId === "string" ? parsed.targetId : "";
    const target = state.recordsByEntryId.get(targetId);
    if (target === undefined) return;
    // Only the first attribution separates the response from its children.
    state.recordsByEntryId.delete(targetId);
    const aggregate = parsePiUsage("aggregateUsage" in parsed ? parsed.aggregateUsage : null);
    const child = parsePiUsage("childUsage" in parsed ? parsed.childUsage : null);
    const record = target.out[target.index];
    if (aggregate === null || child === null || record === undefined) return;
    const own: UsageTokenTotals = {
      uncachedInputTokens: Math.max(
        0,
        aggregate.totals.uncachedInputTokens - child.totals.uncachedInputTokens,
      ),
      cachedInputTokens: Math.max(
        0,
        aggregate.totals.cachedInputTokens - child.totals.cachedInputTokens,
      ),
      cacheCreationTokens: Math.max(
        0,
        aggregate.totals.cacheCreationTokens - child.totals.cacheCreationTokens,
      ),
      outputTokens: Math.max(0, aggregate.totals.outputTokens - child.totals.outputTokens),
      reasoningTokens: 0,
    };
    // A raw line already holds these totals; keep its exact recorded cost.
    if (totalTokens(own) === 0 || totalsEqual(own, record.totals)) return;
    const ownCost = aggregate.costUsd - child.costUsd;
    target.out[target.index] = {
      ...record,
      totals: own,
      reportedCostUsd: ownCost > 0 ? ownCost : null,
    };
    return;
  }
  if (parsed.type === "compaction" || parsed.type === "branch_summary") {
    const usage = parsePiUsage("usage" in parsed ? parsed.usage : null);
    const timestampMs = parseTimestampMs("timestamp" in parsed ? parsed.timestamp : undefined);
    if (usage === null || timestampMs === null || totalTokens(usage.totals) === 0) return;
    out.push({
      provider,
      timestampMs,
      model: PI_SUMMARY_MODEL,
      sessionId,
      totals: usage.totals,
      reportedCostUsd: usage.costUsd > 0 ? usage.costUsd : null,
      speed: "standard",
      // Copies keep the entry ID and timestamp.
      dedupeKey: entryId ? `pi-summary:${entryId}:${timestampMs}` : null,
    });
    return;
  }
  const record = parsePiRecord(parsed, provider, sessionId);
  if (record === null) return;
  out.push(record);
  if (entryId) state.recordsByEntryId.set(entryId, { out, index: out.length - 1 });
}

function totalsEqual(a: UsageTokenTotals, b: UsageTokenTotals): boolean {
  return (
    a.uncachedInputTokens === b.uncachedInputTokens &&
    a.cachedInputTokens === b.cachedInputTokens &&
    a.cacheCreationTokens === b.cacheCreationTokens &&
    a.outputTokens === b.outputTokens &&
    a.reasoningTokens === b.reasoningTokens
  );
}

/** Parses one Pi assistant message. Summaries and attributions need {@link applyPiEntry}. */
export function parsePiRecord(
  parsed: unknown,
  provider: "pi" | "primeAgent",
  sessionId: string,
): UsageRecord | null {
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    !("type" in parsed) ||
    parsed.type !== "message"
  )
    return null;
  if (!("message" in parsed)) return null;
  const message = parsed.message;
  if (
    typeof message !== "object" ||
    message === null ||
    !("role" in message) ||
    message.role !== "assistant"
  )
    return null;
  if (!("usage" in message) || !("model" in message) || !("timestamp" in message)) return null;
  const { model, timestamp } = message;
  const usage = parsePiUsage(message.usage);
  if (usage === null || typeof model !== "string" || !model.trim()) return null;
  const timestampMs = typeof timestamp === "number" ? timestamp : parseTimestampMs(timestamp);
  if (timestampMs === null || !Number.isFinite(timestampMs)) return null;
  if (totalTokens(usage.totals) === 0) return null;
  const responseId =
    "responseId" in message && typeof message.responseId === "string" ? message.responseId : "";
  const entryId = "id" in parsed && typeof parsed.id === "string" ? parsed.id : "";
  return {
    provider,
    timestampMs,
    model,
    sessionId,
    totals: usage.totals,
    // Zero is also Pi's placeholder for models without configured prices.
    reportedCostUsd: usage.costUsd > 0 ? usage.costUsd : null,
    speed: "standard",
    // A response ID survives copied/forked sessions. Without one (aborted
    // requests), Pi's short entry ID plus the response time also survive a copy
    // and do not collide across sessions.
    dedupeKey: responseId
      ? `pi-response:${responseId}`
      : entryId
        ? `pi-entry:${entryId}:${timestampMs}`
        : null,
  };
}

/* -------------------------------------------------------------------------- */
/* Claude Code                                                                */
/* -------------------------------------------------------------------------- */

/**
 * Parses one line of a Claude Code transcript.
 *
 * T3 Code writes one record per assistant *content block*, and every one of
 * those records repeats the same complete `usage` object for the parent
 * message. Summing them overcounts by roughly 2.4x on a real workload, so the
 * caller must drop repeats by `dedupeKey` and keep the first.
 */
export function parseClaudeLine(line: string): UsageRecord | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  return parseClaudeRecord(parsed);
}

export function parseClaudeRecord(parsed: unknown): UsageRecord | null {
  if (typeof parsed !== "object" || parsed === null) return null;

  const record = parsed as Record<string, unknown>;
  if (record["type"] !== "assistant") return null;

  const message = record["message"];
  if (typeof message !== "object" || message === null) return null;
  const messageRecord = message as Record<string, unknown>;

  const usage = messageRecord["usage"];
  if (typeof usage !== "object" || usage === null) return null;
  const usageRecord = usage as Record<string, unknown>;

  const timestampMs = parseTimestampMs(record["timestamp"]);
  if (timestampMs === null) return null;

  const model = typeof messageRecord["model"] === "string" ? messageRecord["model"] : "";
  if (model.length === 0) return null;

  const messageId = typeof messageRecord["id"] === "string" ? messageRecord["id"] : null;
  const requestId = typeof record["requestId"] === "string" ? record["requestId"] : null;
  // Matches ccusage: prefer the message/request pair, fall back to whichever
  // half exists. Records with neither cannot be de-duplicated.
  const dedupeKey =
    messageId === null && requestId === null ? null : `${messageId ?? ""}:${requestId ?? ""}`;

  const cost = record["costUSD"];

  return {
    provider: "claude",
    timestampMs,
    model,
    sessionId: typeof record["sessionId"] === "string" ? record["sessionId"] : "",
    totals: {
      uncachedInputTokens: int(usageRecord["input_tokens"]),
      cachedInputTokens: int(usageRecord["cache_read_input_tokens"]),
      cacheCreationTokens: int(usageRecord["cache_creation_input_tokens"]),
      outputTokens: int(usageRecord["output_tokens"]),
      // Anthropic folds thinking tokens into output and does not break them out.
      reasoningTokens: 0,
    },
    reportedCostUsd: typeof cost === "number" && Number.isFinite(cost) ? cost : null,
    speed: usageRecord["speed"] === "fast" ? "fast" : "standard",
    dedupeKey,
  };
}

/* -------------------------------------------------------------------------- */
/* Codex                                                                      */
/* -------------------------------------------------------------------------- */

/**
 * Rolling state for a single Codex rollout file.
 *
 * Codex `token_count` events carry no model or service tier, so both are
 * carried forward: the model from the most recent `turn_context`, the tier from
 * the most recent `thread_settings_applied`. Sessions that switch either
 * mid-run attribute correctly from the switch onward.
 */
export interface CodexScanState {
  model: string;
  speed: UsageSpeed;
  sessionId: string;
  lastUsageSignature: string | null;
  sawSessionMeta: boolean;
  /** While true, leading usage events are re-stamped copies of parent history. */
  suppressingForkCopies: boolean;
  forkCopyAnchorMs: number;
}

export function initialCodexScanState(): CodexScanState {
  return {
    model: "",
    speed: "standard",
    sessionId: "",
    lastUsageSignature: null,
    sawSessionMeta: false,
    suppressingForkCopies: false,
    forkCopyAnchorMs: 0,
  };
}

/**
 * A forked or subagent rollout opens with the parent's full history copied in,
 * every line re-stamped to the fork instant. Those copies are written in one
 * synchronous burst (observed gaps 0-40ms), while the child's first genuine
 * usage event only lands after a real model turn (observed 5s+). One second of
 * separation splits the two cleanly; `ccusage` uses the same threshold.
 */
const FORK_COPY_MAX_GAP_MS = 1000;

/** Whether a `session_meta` payload marks the rollout as a fork or subagent. */
function isForkedSessionMeta(payload: Record<string, unknown>): boolean {
  if (typeof payload["forked_from_id"] === "string") return true;
  const source = payload["source"];
  if (typeof source !== "object" || source === null) return false;
  const subagent = (source as Record<string, unknown>)["subagent"];
  if (typeof subagent !== "object" || subagent === null) return false;
  const spawn = (subagent as Record<string, unknown>)["thread_spawn"];
  if (typeof spawn !== "object" || spawn === null) return false;
  return typeof (spawn as Record<string, unknown>)["parent_thread_id"] === "string";
}

/**
 * Feeds one line of a Codex rollout into `state`, returning a record when the
 * line was a usage event.
 *
 * Deltas come from `last_token_usage`. Summing those across a session
 * reconciles with the session's final `total_token_usage`, provided
 * consecutive duplicate events are dropped, which this does.
 */
export function parseCodexLine(line: string, state: CodexScanState): UsageRecord | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  return parseCodexRecord(parsed, state);
}

export function parseCodexRecord(parsed: unknown, state: CodexScanState): UsageRecord | null {
  if (typeof parsed !== "object" || parsed === null) return null;

  const record = parsed as Record<string, unknown>;
  const payload = record["payload"];
  if (typeof payload !== "object" || payload === null) return null;
  const payloadRecord = payload as Record<string, unknown>;
  const payloadType = payloadRecord["type"];

  if (record["type"] === "session_meta") {
    // Only the first meta describes this file's own session. A forked rollout
    // repeats the ancestors' metas right after it; letting those through would
    // reassign every subsequent record to an ancestor session.
    if (state.sawSessionMeta) return null;
    state.sawSessionMeta = true;
    const id = payloadRecord["id"] ?? payloadRecord["session_id"];
    if (typeof id === "string") state.sessionId = id;
    const metaTimestampMs = parseTimestampMs(record["timestamp"]);
    if (metaTimestampMs !== null && isForkedSessionMeta(payloadRecord)) {
      state.suppressingForkCopies = true;
      state.forkCopyAnchorMs = metaTimestampMs;
    }
    return null;
  }

  if (record["type"] === "turn_context") {
    if (typeof payloadRecord["model"] === "string") state.model = payloadRecord["model"];
    return null;
  }

  if (payloadType === "thread_settings_applied") {
    const settings = payloadRecord["thread_settings"];
    if (typeof settings === "object" && settings !== null) {
      state.speed = codexSpeed((settings as Record<string, unknown>)["service_tier"]);
    }
    return null;
  }

  if (payloadType !== "token_count") return null;

  const info = payloadRecord["info"];
  if (typeof info !== "object" || info === null) return null;
  const last = (info as Record<string, unknown>)["last_token_usage"];
  if (typeof last !== "object" || last === null) return null;
  const lastRecord = last as Record<string, unknown>;

  // Only an event that is otherwise eligible may consume the duplicate
  // signature. A token_count arriving before its turn_context (no model yet)
  // must not poison it, or the re-emitted copy after the model is known would
  // be skipped as a duplicate and those tokens never counted.
  const timestampMs = parseTimestampMs(record["timestamp"]);
  if (timestampMs === null) return null;
  if (state.model.length === 0) return null;

  // Codex re-emits an unchanged token_count on some stream boundaries. Summing
  // those would double count, so identical consecutive payloads are skipped.
  const signature = JSON.stringify(lastRecord);
  if (signature === state.lastUsageSignature) return null;
  state.lastUsageSignature = signature;

  // In a forked rollout the copied parent history was already counted from the
  // parent's own file. Drop the leading burst; the first usage event separated
  // from its predecessor by a real turn's worth of time ends it for good.
  if (state.suppressingForkCopies) {
    if (timestampMs - state.forkCopyAnchorMs < FORK_COPY_MAX_GAP_MS) {
      state.forkCopyAnchorMs = timestampMs;
      return null;
    }
    state.suppressingForkCopies = false;
  }

  const inputTokens = int(lastRecord["input_tokens"]);
  const cachedInputTokens = int(lastRecord["cached_input_tokens"]);
  const cacheCreationTokens = int(lastRecord["cache_write_input_tokens"]);
  const outputTokens = int(lastRecord["output_tokens"]);

  const totals: UsageTokenTotals = {
    // Codex reports `input_tokens` inclusive of the cached portion.
    uncachedInputTokens: Math.max(0, inputTokens - cachedInputTokens - cacheCreationTokens),
    cachedInputTokens,
    cacheCreationTokens,
    outputTokens,
    // Reported inside output_tokens, surfaced separately for the token mix.
    reasoningTokens: Math.min(outputTokens, int(lastRecord["reasoning_output_tokens"])),
  };

  if (totalTokens(totals) === 0) return null;

  return {
    provider: "codex",
    timestampMs,
    model: state.model,
    sessionId: state.sessionId,
    totals,
    // Codex does not report cost in the rollout.
    reportedCostUsd: null,
    speed: state.speed,
    // Events surviving the fork-copy suppression above are unique to this
    // rollout, so they need no global dedup.
    dedupeKey: null,
  };
}

/**
 * Maps a Codex `service_tier` to its billing speed. Codex omits the field when
 * no tier was requested, which bills as standard, as do `default` and
 * `standard`. `fast` is accepted as an alias of `priority`.
 */
function codexSpeed(serviceTier: unknown): UsageSpeed {
  if (serviceTier === "priority" || serviceTier === "fast") return "fast";
  if (serviceTier === "ultrafast") return "ultrafast";
  return "standard";
}

/* -------------------------------------------------------------------------- */
/* Grok Build                                                                 */
/* -------------------------------------------------------------------------- */

interface GrokUsageTotals {
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly cachedReadTokens: number;
  readonly cacheCreationTokens: number;
  readonly reasoningTokens: number;
  readonly costUsdTicks: number | null;
}

function readGrokUsageTotals(value: unknown): GrokUsageTotals | null {
  if (typeof value !== "object" || value === null) return null;
  const record = value as Record<string, unknown>;
  return {
    inputTokens: int(record["inputTokens"]),
    outputTokens: int(record["outputTokens"]),
    cachedReadTokens: int(record["cachedReadTokens"]),
    cacheCreationTokens: int(record["cacheCreationTokens"]),
    reasoningTokens: int(record["reasoningTokens"]),
    costUsdTicks:
      typeof record["costUsdTicks"] === "number" && Number.isFinite(record["costUsdTicks"])
        ? record["costUsdTicks"]
        : null,
  };
}

function grokTotalsToUsage(totals: GrokUsageTotals): UsageTokenTotals {
  const cachedInputTokens = totals.cachedReadTokens;
  const cacheCreationTokens = totals.cacheCreationTokens;
  // Grok reports `inputTokens` inclusive of the cached portion, matching Codex.
  const uncachedInputTokens = Math.max(
    0,
    totals.inputTokens - cachedInputTokens - cacheCreationTokens,
  );
  const outputTokens = totals.outputTokens;
  return {
    uncachedInputTokens,
    cachedInputTokens,
    cacheCreationTokens,
    outputTokens,
    reasoningTokens: Math.min(outputTokens, totals.reasoningTokens),
  };
}

/**
 * Parses one line of a Grok Build `updates.jsonl` session log.
 *
 * Usage lands on `turn_completed` session updates. Per-model breakdowns live
 * under `usage.modelUsage`; when present each model becomes its own record.
 *
 * Returns every record for the line (0 or more). Callers stream line-by-line
 * and flatten.
 */
export function parseGrokLine(line: string): readonly UsageRecord[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return [];
  }
  return parseGrokRecord(parsed);
}

export function parseGrokRecord(parsed: unknown): readonly UsageRecord[] {
  if (typeof parsed !== "object" || parsed === null) return [];

  const record = parsed as Record<string, unknown>;
  const params = record["params"];
  if (typeof params !== "object" || params === null) return [];
  const paramsRecord = params as Record<string, unknown>;

  const update = paramsRecord["update"];
  if (typeof update !== "object" || update === null) return [];
  const updateRecord = update as Record<string, unknown>;
  if (updateRecord["sessionUpdate"] !== "turn_completed") return [];

  const usage = updateRecord["usage"];
  if (typeof usage !== "object" || usage === null) return [];
  const usageRecord = usage as Record<string, unknown>;

  const sessionId = typeof paramsRecord["sessionId"] === "string" ? paramsRecord["sessionId"] : "";
  const promptId = typeof updateRecord["prompt_id"] === "string" ? updateRecord["prompt_id"] : null;

  // Prefer the high-resolution agent clock; fall back to the outer unix seconds.
  const meta = paramsRecord["_meta"];
  let timestampMs: number | null = null;
  if (typeof meta === "object" && meta !== null) {
    const agentTimestampMs = (meta as Record<string, unknown>)["agentTimestampMs"];
    if (typeof agentTimestampMs === "number" && Number.isFinite(agentTimestampMs)) {
      timestampMs = agentTimestampMs;
    }
  }
  if (timestampMs === null) {
    const timestamp = record["timestamp"];
    if (typeof timestamp === "number" && Number.isFinite(timestamp)) {
      timestampMs = timestamp > 1e12 ? timestamp : timestamp * 1000;
    }
  }
  if (timestampMs === null) return [];

  const topLevel = readGrokUsageTotals(usageRecord);
  if (topLevel === null) return [];

  const modelUsage = usageRecord["modelUsage"];
  const modelEntries: Array<{ model: string; totals: GrokUsageTotals }> = [];
  if (typeof modelUsage === "object" && modelUsage !== null) {
    for (const [model, raw] of Object.entries(modelUsage as Record<string, unknown>)) {
      if (model.length === 0) continue;
      const totals = readGrokUsageTotals(raw);
      if (totals === null) continue;
      modelEntries.push({ model, totals });
    }
  }

  if (modelEntries.length === 0) {
    if (totalTokens(grokTotalsToUsage(topLevel)) === 0) return [];
    return [
      {
        provider: "grok",
        timestampMs,
        model: "grok",
        sessionId,
        totals: grokTotalsToUsage(topLevel),
        reportedCostUsd: grokCostTicksToUsd(topLevel.costUsdTicks),
        speed: "standard",
        // No prompt id means we cannot tell two same-second updates apart.
        dedupeKey: promptId === null ? null : `${sessionId}:${promptId}:grok`,
      },
    ];
  }

  // Cost allocation:
  // 1. Emitted models with their own costUsdTicks keep those values.
  // 2. Remaining aggregate cost (top-level minus those per-model ticks,
  //    clamped at 0) is pro-rated across emitted models that lack ticks,
  //    by token share among the unticked models only.
  // 3. When no model has per-model ticks, remaining equals the full
  //    aggregate and every emitted model gets a token-share slice.
  // Zero-token rows are never emitted and never count toward used ticks.
  const topLevelCostUsd = grokCostTicksToUsd(topLevel.costUsdTicks);
  let usedTickedCostUsd = 0;
  let untickedTokenDenominator = 0;
  for (const entry of modelEntries) {
    const tokens = totalTokens(grokTotalsToUsage(entry.totals));
    if (tokens === 0) continue;
    if (entry.totals.costUsdTicks !== null) {
      usedTickedCostUsd += grokCostTicksToUsd(entry.totals.costUsdTicks) ?? 0;
    } else {
      untickedTokenDenominator += tokens;
    }
  }
  const remainingCostUsd =
    topLevelCostUsd === null ? null : Math.max(0, topLevelCostUsd - usedTickedCostUsd);

  const results: UsageRecord[] = [];
  for (const entry of modelEntries) {
    const totals = grokTotalsToUsage(entry.totals);
    if (totalTokens(totals) === 0) continue;

    let reportedCostUsd = grokCostTicksToUsd(entry.totals.costUsdTicks);
    if (reportedCostUsd === null && remainingCostUsd !== null && untickedTokenDenominator > 0) {
      reportedCostUsd = remainingCostUsd * (totalTokens(totals) / untickedTokenDenominator);
    }

    results.push({
      provider: "grok",
      timestampMs,
      model: entry.model,
      sessionId,
      totals,
      reportedCostUsd,
      speed: "standard",
      dedupeKey: promptId === null ? null : `${sessionId}:${promptId}:${entry.model}`,
    });
  }
  return results;
}

export { EMPTY_TOTALS };
