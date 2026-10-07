import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

/** Idle gaps at least this long are the likely cause of a miss (Anthropic's default cache TTL). */
const CACHE_TTL_MS = 5 * 60 * 1000;
/** A smaller shortfall is cache-breakpoint granularity, not a miss. */
const NOISE_FLOOR_TOKENS = 1024;
/** A miss is worth a notice when it re-bills this many tokens or this many dollars. */
const NOTICE_MIN_TOKENS = 20_000;
const NOTICE_MIN_DOLLARS = 0.1;
const NOTICE_SHOW_DOLLARS_FROM = 0.01;

const NonNegative = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));

/**
 * The part of a Prime Agent assistant message this detector reads. Messages
 * arrive as untyped JSON (live events and `get_messages`), so they are
 * decoded here once; anything else in the transcript fails to decode and is
 * not this detector's concern.
 */
const AssistantEntry = Schema.Struct({
  role: Schema.Literal("assistant"),
  provider: Schema.String,
  model: Schema.String,
  timestamp: Schema.Finite,
  responseId: Schema.optionalKey(Schema.String),
  usage: Schema.Struct({
    input: NonNegative,
    output: NonNegative,
    cacheRead: NonNegative,
    cacheWrite: NonNegative,
    totalTokens: NonNegative,
    cost: Schema.optionalKey(
      Schema.Struct({
        input: Schema.optionalKey(NonNegative),
        cacheRead: Schema.optionalKey(NonNegative),
        cacheWrite: Schema.optionalKey(NonNegative),
      }),
    ),
  }),
});

/** Compaction and branch summaries replace the context on purpose. */
const SummaryEntry = Schema.Struct({
  role: Schema.Literals(["compactionSummary", "branchSummary"]),
  timestamp: Schema.optionalKey(Schema.Finite),
});

const decodeEntry = Schema.decodeUnknownOption(Schema.Union([AssistantEntry, SummaryEntry]));

type AssistantEntry = typeof AssistantEntry.Type;

/** The last request seen; its whole prompt should be a cache read on the next one. */
export interface CacheBaseline {
  readonly promptTokens: number;
  readonly modelKey: string;
  readonly timestamp: number;
  /**
   * Some request since the last reset reported cache activity. It tells a
   * total miss on a provider that reports reads and writes from a provider
   * that never reports caching.
   */
  readonly reportedCache: boolean;
}

/**
 * What the detector knows about one thread. A message counts once: a
 * message older than `floor` is history behind what was already counted, and
 * one at `floor` counts unless its identity is in `atFloor`. Distinct
 * messages may share a timestamp, so the timestamp alone never decides.
 */
export interface CacheMissState {
  readonly baseline: CacheBaseline | undefined;
  readonly floor: number;
  readonly atFloor: ReadonlySet<string>;
}

export const emptyCacheMissState: CacheMissState = {
  baseline: undefined,
  floor: Number.NEGATIVE_INFINITY,
  atFloor: new Set(),
};

/** A counted cache miss on one assistant message. */
export interface CacheMiss {
  /** Prompt tokens of the previous request that were not read from cache. */
  readonly missedTokens: number;
  /** Extra dollars paid over a full cache hit; 0 when the message's costs do not show a cache-read rate. */
  readonly missedDollars: number;
  /** Time since the previous request, which last refreshed the cache. */
  readonly idleMs: number;
  readonly modelChanged: boolean;
}

/**
 * Where a message came from. A live `message_end` carries the parent's own
 * usage. A stored message may have had its RLM children's usage folded in
 * afterwards, which makes it describe no single request.
 */
export type MessageSource = "live" | "stored";

export interface CacheMissStep {
  readonly state: CacheMissState;
  readonly miss: CacheMiss | undefined;
  /** Stable identity of the message that paid for `miss`, for the notice's id. */
  readonly identity: string | undefined;
}

const promptTokensOf = (usage: AssistantEntry["usage"]) =>
  usage.input + usage.cacheRead + usage.cacheWrite;

/**
 * A folded message keeps `totalTokens` at the parent's own size while its
 * other counts include the children's.
 */
const hasFoldedChildUsage = (usage: AssistantEntry["usage"]) =>
  usage.totalTokens > 0 && promptTokensOf(usage) + usage.output > usage.totalTokens;

function detectMiss(
  baseline: CacheBaseline | undefined,
  entry: AssistantEntry,
): CacheMiss | undefined {
  const { usage } = entry;
  const promptTokens = promptTokensOf(usage);
  const reportsCache = usage.cacheRead + usage.cacheWrite > 0;
  if (baseline === undefined || promptTokens <= 0 || (!reportsCache && !baseline.reportedCache)) {
    return undefined;
  }
  const missedTokens = Math.min(baseline.promptTokens, promptTokens) - usage.cacheRead;
  if (missedTokens <= NOISE_FLOOR_TOKENS) return undefined;

  // Missed tokens were billed as input or cache writes, so the paid rate
  // comes from this message's own costs. The cache-read rate needs a
  // message that read some cache; otherwise it is unknown and counts as 0.
  const paidTokens = usage.input + usage.cacheWrite;
  const paidDollars = (usage.cost?.input ?? 0) + (usage.cost?.cacheWrite ?? 0);
  const paidPerToken = paidTokens > 0 ? paidDollars / paidTokens : 0;
  const readPerToken = usage.cacheRead > 0 ? (usage.cost?.cacheRead ?? 0) / usage.cacheRead : 0;
  return {
    missedTokens,
    missedDollars: missedTokens * Math.max(0, paidPerToken - readPerToken),
    idleMs: Math.max(0, entry.timestamp - baseline.timestamp),
    modelChanged: `${entry.provider}/${entry.model}` !== baseline.modelKey,
  };
}

/** `responseId` when the provider gave one, else the role and timestamp the reconciler keys messages by. */
const identityOf = (entry: AssistantEntry): string =>
  entry.responseId === undefined
    ? JSON.stringify(["assistant", entry.timestamp])
    : JSON.stringify(["response", entry.responseId]);

/** Moves the floor to a message at `timestamp`, remembering `identity` when it is at the floor. */
const advanced = (
  state: CacheMissState,
  baseline: CacheBaseline | undefined,
  timestamp: number,
  identity: string | undefined,
): CacheMissState => {
  const atFloor = timestamp === state.floor ? state.atFloor : new Set<string>();
  return {
    baseline,
    floor: timestamp,
    atFloor: identity === undefined ? atFloor : new Set([...atFloor, identity]),
  };
};

const unchanged = (state: CacheMissState): CacheMissStep => ({
  state,
  miss: undefined,
  identity: undefined,
});

/**
 * Counts one transcript message. Call it in chronological order, or in any
 * order a message may arrive: a message already counted, or older than what
 * was, changes nothing. Compaction and branch summaries reset the baseline,
 * because the next prompt is new content rather than re-billed content. A
 * model switch does not reset it; the new model re-bills the prompt.
 */
export function observeCacheMiss(
  state: CacheMissState,
  message: unknown,
  source: MessageSource,
): CacheMissStep {
  const decoded = decodeEntry(message);
  if (Option.isNone(decoded)) return unchanged(state);
  const entry = decoded.value;

  if (entry.role !== "assistant") {
    // A summary at or before the floor is history behind a reset already taken.
    const { timestamp } = entry;
    if (timestamp === undefined) return unchanged(resetCacheBaseline(state));
    if (timestamp <= state.floor) return unchanged(state);
    return unchanged(advanced(state, undefined, timestamp, undefined));
  }

  const identity = identityOf(entry);
  if (entry.timestamp < state.floor) return unchanged(state);
  if (entry.timestamp === state.floor && state.atFloor.has(identity)) return unchanged(state);

  if (source === "stored" && hasFoldedChildUsage(entry.usage)) {
    return { ...unchanged(state), state: advanced(state, undefined, entry.timestamp, identity) };
  }

  const promptTokens = promptTokensOf(entry.usage);
  const baseline: CacheBaseline | undefined =
    promptTokens <= 0
      ? state.baseline
      : {
          promptTokens,
          modelKey: `${entry.provider}/${entry.model}`,
          timestamp: entry.timestamp,
          reportedCache:
            (state.baseline?.reportedCache ?? false) ||
            entry.usage.cacheRead + entry.usage.cacheWrite > 0,
        };
  return {
    state: advanced(state, baseline, entry.timestamp, identity),
    miss: detectMiss(state.baseline, entry),
    identity,
  };
}

/** The state after a whole stored history, without notices: all of it is past. */
export function seedCacheMissState(messages: ReadonlyArray<unknown>): CacheMissState {
  return messages.reduce<CacheMissState>(
    (state, message) => observeCacheMiss(state, message, "stored").state,
    emptyCacheMissState,
  );
}

/** Forgets the baseline after the context changed on purpose (compaction, rollback). */
export const resetCacheBaseline = (state: CacheMissState): CacheMissState => ({
  ...state,
  baseline: undefined,
});

const formatThousands = (tokens: number) => `${Math.round(tokens / 1000).toLocaleString("en-US")}k`;

/**
 * Notice text for a miss worth showing, or undefined below the threshold. It
 * states only what the usage shows: a model switch, or an idle gap past the
 * cache lifetime.
 */
export function describeCacheMiss(miss: CacheMiss): string | undefined {
  if (miss.missedTokens < NOTICE_MIN_TOKENS && miss.missedDollars < NOTICE_MIN_DOLLARS) {
    return undefined;
  }
  const label = miss.modelChanged
    ? "Cache miss after model switch"
    : miss.idleMs >= CACHE_TTL_MS
      ? `Cache miss after ${Math.round(miss.idleMs / 60_000)}m idle`
      : "Cache miss";
  const cost =
    miss.missedDollars >= NOTICE_SHOW_DOLLARS_FROM ? ` (~$${miss.missedDollars.toFixed(2)})` : "";
  return `${label}: ${formatThousands(miss.missedTokens)} tokens re-billed${cost}`;
}
