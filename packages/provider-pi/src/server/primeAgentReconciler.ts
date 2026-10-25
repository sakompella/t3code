import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type { ProviderDriverKind } from "@t3tools/contracts";
import {
  piRecordField as recordField,
  piRecordNumber as recordNumber,
  piRecordString as recordString,
  piStateIsIdle,
  type PiRpcConnection,
  type PiRpcRecord,
} from "./rpc.ts";
import type { ActivePiTurn } from "./adapterState.ts";

/**
 * How often the reconciler looks at a session that may have lost events: a
 * turn that went silent, or an idle session with background work. An idle
 * session that shows no change is looked at less and less often, down to
 * once per `MAX_IDLE_INTERVALS` intervals.
 */
const CHECK_INTERVAL = Duration.seconds(15);
const MAX_IDLE_INTERVALS = 8;
/** A history read that never matches the idle state settles the turn without it after this many tries. */
const MAX_SETTLE_READS = 3;
const STATE_READ_TIMEOUT_MS = 2_000;
/** A long session's history is large, and the agent is idle while it is read. */
const HISTORY_READ_TIMEOUT_MS = 10_000;
/** Stop waits on this read, and a mid-run repair holds the event stream for it, so it gives up sooner. */
const SHORT_HISTORY_READ_TIMEOUT_MS = 2_000;

/**
 * The identity of a stored message, as Prime Agent itself keys kept messages
 * across compaction: who wrote it and when. Tool results and injected
 * messages can share a millisecond, so their call id or kind is part of it.
 * Undefined for a message without a timestamp, which cannot be told apart.
 */
export function transcriptMessageKey(message: unknown): string | undefined {
  const timestamp = recordNumber(message, "timestamp");
  if (timestamp === undefined) return undefined;
  return JSON.stringify([
    recordString(message, "role") ?? null,
    timestamp,
    recordString(message, "toolCallId") ?? recordString(message, "customType") ?? null,
  ]);
}

function isReplyKey(key: string): boolean {
  const [role]: ReadonlyArray<unknown> = JSON.parse(key);
  return role === "assistant";
}

function keyTimestamp(key: string): number {
  const [, timestamp]: ReadonlyArray<unknown> = JSON.parse(key);
  return Number(timestamp);
}

/** The message a session event is about, for the events that carry one. */
function eventMessage(event: PiRpcRecord): unknown {
  switch (event["type"]) {
    case "message_start":
    case "message_update":
    case "message_end":
      return event["message"];
    default:
      return undefined;
  }
}

function isAbortedReply(message: unknown): boolean {
  return (
    recordString(message, "role") === "assistant" &&
    recordString(message, "stopReason") === "aborted"
  );
}

/** A message the agent loop did not write: a prompt, a wake, a notice. Each run starts with one. */
function startsRun(message: unknown): boolean {
  const role = recordString(message, "role");
  return role !== "assistant" && role !== "toolResult";
}

/**
 * Matches each stored aborted copy (`finishAbortedMessage`, no message_start) to the reply it
 * replaced. A copy fits when it holds all the text the turn showed, is stored after the last
 * message seen before the reply and before anything seen after it, and did not arrive live earlier.
 * A reply matches only a unique fit. Returns each copy's key with the replaced reply's timestamp.
 */
export function matchStoppedCopies(input: {
  readonly history: ReadonlyArray<unknown>;
  /** Keys of the messages the turn streamed, in first-arrival order. */
  readonly streamed: ReadonlyArray<string>;
  /** For each copy that arrived live, how many of `streamed` had arrived before it. */
  readonly arrivals: ReadonlyMap<string, number>;
  /** The streamed replies T3 does not show in full. */
  readonly unended: ReadonlySet<string>;
  /** Unended replies a compaction or a retry may have dropped from the history. */
  readonly removed: ReadonlySet<string>;
  /** Stored messages an earlier read or turn already placed. */
  readonly accounted: ReadonlySet<string>;
  /** Whether `copy` holds all the text T3 shows of the reply `replyKey`. */
  readonly holdsShown: (copy: unknown, replyKey: string) => boolean;
}): ReadonlyMap<string, number> {
  const { history, arrivals } = input;
  const keys = history.map(transcriptMessageKey);
  const position = new Map<string, number>();
  keys.forEach((key, index) => key !== undefined && position.set(key, index));
  const streamed = new Set(input.streamed);
  const copies = keys.flatMap((key, index) =>
    key !== undefined &&
    isAbortedReply(history[index]) &&
    !streamed.has(key) &&
    !input.accounted.has(key)
      ? [{ key, index }]
      : [],
  );
  const runStarts = history.flatMap((message, index) => (startsRun(message) ? [index] : []));
  const startedThisTurn = (index: number) => {
    const runStart = runStarts.findLast((start) => start < index);
    return runStart !== undefined && !input.accounted.has(keys[runStart] ?? "");
  };
  const storedPositions = (streamedKeys: ReadonlyArray<string>) =>
    streamedKeys.flatMap((key) => position.get(key) ?? []);
  /**
   * A stored message no earlier turn accounted for that can stand for a reply
   * dropped without a copy: the reply a retry stored after it, or a
   * compaction's summary.
   */
  const mayHaveDropped = (message: unknown) => {
    const key = transcriptMessageKey(message);
    if (key === undefined || input.accounted.has(key)) return false;
    const role = recordString(message, "role");
    return role === "compactionSummary" || (role === "assistant" && !isAbortedReply(message));
  };

  const chosen = input.streamed.flatMap((key, index) => {
    if (position.has(key) || !input.unended.has(key) || input.removed.has(key)) return [];
    if (!isReplyKey(key)) return [];
    const seenBefore = storedPositions(input.streamed.slice(0, index));
    const anchor = seenBefore.length === 0 ? undefined : Math.max(...seenBefore);
    const end = Math.min(history.length, ...storedPositions(input.streamed.slice(index + 1)));
    const fits = copies.filter((copy) => {
      const arrivedAfter = arrivals.get(copy.key);
      return (
        copy.index > (anchor ?? -1) &&
        copy.index < end &&
        (anchor !== undefined || startedThisTurn(copy.index)) &&
        (arrivedAfter === undefined || arrivedAfter > index) &&
        input.holdsShown(history[copy.index], key)
      );
    });
    const [earliest] = fits;
    if (earliest === undefined || (!arrivals.has(earliest.key) && fits.length > 1)) return [];
    const between = history.slice((anchor ?? -1) + 1, earliest.index);
    if (between.some(mayHaveDropped)) return [];
    return [{ key, copy: earliest.key }];
  });
  const claims = new Map<string, number>();
  for (const { copy } of chosen) claims.set(copy, (claims.get(copy) ?? 0) + 1);
  const matches = new Map<string, number>();
  for (const { key, copy } of chosen) {
    if (claims.get(copy) === 1) matches.set(copy, keyTimestamp(key));
  }
  return matches;
}

/**
 * What `get_state` says about the stored conversation, so two reads show whether it changed. The
 * 0.10 fork reports its tree leaf; its `messageCount` spans every branch and omits stored notices,
 * so it never matches the history. 0.9.8 has no leaf, and its count equals the `get_messages`
 * length.
 */
type PiHistoryMark = string | number;

const historyMarkOf = (state: unknown): PiHistoryMark | undefined =>
  recordString(state, "leafId") ?? recordNumber(state, "messageCount");

type TranscriptRead =
  | {
      readonly _tag: "read";
      readonly messages: ReadonlyArray<unknown>;
      readonly historyMark: PiHistoryMark;
    }
  /** The agent started new work while its history was read. */
  | { readonly _tag: "busy" }
  /** No read, or a history from another moment than the idle state. */
  | { readonly _tag: "unconfirmed" };

/** The stored conversation, or undefined when it could not be read. */
const readHistory = (request: PiRpcConnection["request"], timeoutMs: number) =>
  request({ type: "get_messages" }, timeoutMs).pipe(
    Effect.map((data): ReadonlyArray<unknown> | undefined => {
      const messages = recordField(data, "messages");
      return Array.isArray(messages) ? messages : undefined;
    }),
    Effect.orElseSucceed(() => undefined),
  );

/**
 * Reads the stored conversation of an agent `idleState` showed idle and confirms with a second
 * state read. Stock RPC has no atomic snapshot, so this narrows the race: the history counts only
 * if the agent is still idle and both states count exactly its messages.
 */
const readSettledTranscript = Effect.fnUntraced(function* (
  request: PiRpcConnection["request"],
  idleState: unknown,
): Effect.fn.Return<TranscriptRead> {
  const messages = yield* readHistory(request, HISTORY_READ_TIMEOUT_MS);
  const after = yield* request({ type: "get_state" }, STATE_READ_TIMEOUT_MS).pipe(Effect.option);
  if (Option.isNone(after) || messages === undefined) return { _tag: "unconfirmed" };
  if (!piStateIsIdle(after.value)) return { _tag: "busy" };
  const historyMark = historyMarkOf(idleState) ?? messages.length;
  const unchanged = (historyMarkOf(after.value) ?? messages.length) === historyMark;
  const counts = typeof historyMark === "string" || historyMark === messages.length;
  if (!unchanged || !counts) return { _tag: "unconfirmed" };
  return { _tag: "read", messages, historyMark };
});

interface TranscriptLedger {
  /** Stored messages T3 already accounted for: projected, seen live, or older than the session. */
  readonly keys: Set<string>;
  /** Tool calls whose outcome T3 already shows. */
  readonly toolCalls: Set<string>;
  /** The history mark of the last confirmed read, to tell when the history changed. */
  historyMark: PiHistoryMark;
  /**
   * Messages stored before this time are history T3 did not stream. Set
   * when the first read failed, so no key list says what came before.
   */
  readonly storedBefore: number;
}

function toolCallIdsOf(messages: ReadonlyArray<unknown>): Array<string> {
  return messages.flatMap((message) =>
    recordString(message, "role") === "toolResult"
      ? (recordString(message, "toolCallId") ?? [])
      : [],
  );
}

/**
 * Makes T3's projection match what Prime Agent stored, for a stream that can drop any event (see
 * `PiFlavor.lossyStream`). `get_messages` is the authority; messages match items by native identity
 * (assistant timestamp, tool call id). Stop-cut replies match via `matchStoppedCopies` or
 * `placeHeldCopies`. Idempotent: each message is projected once, then accounted for.
 */
export function makePrimeAgentReconciler<E>(input: {
  readonly enabled: boolean;
  readonly driver: ProviderDriverKind;
  readonly request: PiRpcConnection["request"];
  readonly activeTurn: () => ActivePiTurn | null;
  /** Whether an idle session may still wake on its own. */
  readonly expectsWakes: () => boolean;
  /** Whether a wake whose start T3 saw is still waiting for its continuation offer. */
  readonly hasUnofferedWake: () => boolean;
  /** Asks the pump to settle a turn that went silent, if the agent is idle. */
  readonly probeQuietTurn: (turn: ActivePiTurn) => Effect.Effect<void>;
  /** Queues a record behind the events already read, for the pump to apply. */
  readonly enqueue: (record: PiRpcRecord) => Effect.Effect<void>;
  readonly items: {
    /** Brings an assistant message's items to its stored text and closes them. */
    readonly upsertAssistantMessage: (
      turn: ActivePiTurn,
      message: unknown,
      /** The timestamp of the reply a stopped copy replaced, whose items it takes. */
      replaced?: number,
    ) => Effect.Effect<void, E>;
    /** Whether `copy` holds all the text T3 shows of the reply stamped `replaced`. */
    readonly holdsShownText: (turn: ActivePiTurn, copy: unknown, replaced: number) => boolean;
    readonly startTool: (turn: ActivePiTurn, toolCall: unknown) => Effect.Effect<void, E>;
    readonly endTool: (turn: ActivePiTurn, toolResult: unknown) => Effect.Effect<void, E>;
    /** Handles a stored message other than a reply as if its `message_start` had arrived. */
    readonly replayMessageStart: (message: unknown) => Effect.Effect<void, E>;
    /** Counts a stored message toward the prompt-cache notices, which warn in the turn that reads it. */
    readonly observeCache: (turn: ActivePiTurn, message: unknown) => Effect.Effect<void>;
  };
  /** Takes the history of a session T3 just attached to as the prompt-cache state; empty when unreadable. */
  readonly seedCache: (messages: ReadonlyArray<unknown>) => void;
  /** Applies what stored messages that woke an idle agent say, and offers their wake. */
  readonly recoverWake: (wakeMessages: ReadonlyArray<unknown>) => Effect.Effect<void, E>;
}) {
  const { enabled, request, activeTurn, items } = input;
  /** Null until `baseline` established which messages predate T3's view of the session. */
  let ledger: TranscriptLedger | null = null;

  const unaccounted = (messages: ReadonlyArray<unknown>) =>
    messages.filter((message) => {
      const key = transcriptMessageKey(message);
      return (
        key !== undefined &&
        ledger !== null &&
        !ledger.keys.has(key) &&
        (recordNumber(message, "timestamp") ?? 0) >= ledger.storedBefore
      );
    });

  /**
   * Marks a whole confirmed history as accounted for. A read without a state
   * read knows only its length, which a fork's leaf never matches.
   */
  const commit = (
    messages: ReadonlyArray<unknown>,
    historyMark: PiHistoryMark = messages.length,
  ) => {
    if (ledger === null) return;
    for (const message of messages) {
      const key = transcriptMessageKey(message);
      if (key !== undefined) ledger.keys.add(key);
    }
    for (const toolCallId of toolCallIdsOf(messages)) ledger.toolCalls.add(toolCallId);
    ledger.historyMark = historyMark;
  };

  /** Marks messages T3 chose not to show, so no history read offers them as a lost wake. */
  const account = (messages: ReadonlyArray<unknown>) => {
    if (ledger === null) return;
    for (const message of messages) {
      const key = transcriptMessageKey(message);
      if (key !== undefined) ledger.keys.add(key);
    }
  };

  /** The session the ledger was seeded from. */
  let seededSession: string | null = null;

  /**
   * Everything `sessionFile` stored so far is history T3 did not stream. If the read fails, earlier
   * content is told apart by timestamp. A session already seeded keeps its ledger so unaccounted
   * messages still reach T3.
   */
  const baseline = Effect.fnUntraced(function* (sessionFile: string) {
    if (!enabled || (ledger !== null && seededSession === sessionFile)) return;
    ledger = null;
    seededSession = sessionFile;
    const readAt = yield* Clock.currentTimeMillis;
    const messages = yield* readHistory(request, HISTORY_READ_TIMEOUT_MS);
    ledger = {
      keys: new Set(),
      toolCalls: new Set(),
      historyMark: 0,
      storedBefore: messages === undefined ? readAt : 0,
    };
    if (messages !== undefined) commit(messages);
    input.seedCache(messages ?? []);
  });

  /** A live event about a message or tool an earlier turn already accounted for. */
  const isLate = (event: PiRpcRecord): boolean => {
    if (!enabled || ledger === null) return false;
    const key = transcriptMessageKey(eventMessage(event));
    if (key !== undefined) return ledger.keys.has(key);
    const type = event["type"];
    if (
      type === "tool_execution_start" ||
      type === "tool_execution_update" ||
      type === "tool_execution_end"
    ) {
      const toolCallId = recordString(event, "toolCallId");
      return toolCallId !== undefined && ledger.toolCalls.has(toolCallId);
    }
    return false;
  };

  /** Replies the turn showed in part whose message_end has not arrived. */
  const unendedReplyKeys = (turn: ActivePiTurn) =>
    [...turn.seenMessageKeys].filter((key) => !turn.shownMessageKeys.has(key) && isReplyKey(key));

  /**
   * An aborted reply end whose message the turn never streamed: stock's copy
   * of a reply Stop cut off. Which reply it replaced is left to the history.
   */
  const isStoppedCopy = (turn: ActivePiTurn, event: PiRpcRecord) => {
    if (!enabled || event["type"] !== "message_end") return false;
    const message = eventMessage(event);
    const key = transcriptMessageKey(message);
    return key !== undefined && isAbortedReply(message) && !turn.seenMessageKeys.has(key);
  };

  /**
   * Remembers which stored messages the turn's own events already showed,
   * and which in full. A stopped copy is held for a history read to place,
   * and the abort brings that read, so the ends it leaves in doubt wait for
   * it.
   */
  const noteSeen = (turn: ActivePiTurn, event: PiRpcRecord) => {
    const type = event["type"];
    // Compaction and retry drop messages from the history without a copy.
    if (type === "compaction_start" || type === "auto_retry_start") {
      for (const end of unendedReplyKeys(turn)) turn.removedReplies.add(end);
    }
    const message = eventMessage(event);
    const key = transcriptMessageKey(message);
    if (key === undefined) return;
    if (isStoppedCopy(turn, event)) {
      turn.heldCopies.set(key, { message, arrivedAfter: turn.seenMessageKeys.size });
      for (const end of unendedReplyKeys(turn)) turn.soughtEnds.add(end);
      return;
    }
    turn.seenMessageKeys.add(key);
    if (showsInFull(turn, type, message)) turn.shownMessageKeys.add(key);
  };

  /** Whether `message` is a stopped copy the turn holds unshown. */
  const holdsCopy = (turn: ActivePiTurn, message: unknown) => {
    const key = transcriptMessageKey(message);
    return key !== undefined && turn.heldCopies.has(key);
  };

  /** Whether an event about `message` leaves T3 showing all of it. */
  const showsInFull = (turn: ActivePiTurn, type: unknown, message: unknown) => {
    switch (recordString(message, "role")) {
      case "assistant":
        return type === "message_end";
      case "toolResult": {
        // A tool's outcome comes from its end event, which precedes its result message.
        const toolCallId = recordString(message, "toolCallId");
        return (
          toolCallId !== undefined &&
          turn.toolStartedAt.has(toolCallId) &&
          !turn.openTools.has(toolCallId)
        );
      }
      default:
        return true;
    }
  };

  /**
   * Projects the stored messages T3 has not accounted for into `turn`. A
   * stopped copy in `replacements` lands on the items of the reply it
   * replaced.
   */
  const project = Effect.fnUntraced(function* (
    turn: ActivePiTurn,
    messages: ReadonlyArray<unknown>,
    replacements: ReadonlyMap<string, number> = new Map(),
  ) {
    for (const message of unaccounted(messages)) {
      switch (recordString(message, "role")) {
        case "assistant": {
          const replaced = replacements.get(transcriptMessageKey(message) ?? "");
          yield* items.upsertAssistantMessage(turn, message, replaced);
          const content = recordField(message, "content");
          for (const block of Array.isArray(content) ? content : []) {
            const toolCallId = recordString(block, "id");
            if (recordString(block, "type") !== "toolCall" || toolCallId === undefined) continue;
            if (!turn.toolStartedAt.has(toolCallId)) yield* items.startTool(turn, block);
          }
          break;
        }
        case "toolResult": {
          const toolCallId = recordString(message, "toolCallId");
          if (toolCallId === undefined) break;
          // A tool that already ended live keeps that outcome.
          if (turn.openTools.has(toolCallId) || !turn.toolStartedAt.has(toolCallId)) {
            yield* items.endTool(turn, message);
          }
          break;
        }
        default: {
          const key = transcriptMessageKey(message);
          if (key !== undefined && !turn.seenMessageKeys.has(key)) {
            yield* items.replayMessageStart(message);
          }
        }
      }
    }
    // Cache usage has its own identity, finer than the ledger's, so it reads
    // every reply since attach in order, not only the unaccounted ones.
    for (const message of messages) {
      if (ledger !== null && (recordNumber(message, "timestamp") ?? 0) >= ledger.storedBefore) {
        yield* items.observeCache(turn, message);
      }
    }
  });

  /** Projects a whole stored history into `turn`, placing every copy it holds. */
  const projectHistory = Effect.fnUntraced(function* (
    turn: ActivePiTurn,
    messages: ReadonlyArray<unknown>,
  ) {
    if (ledger === null) return;
    yield* project(
      turn,
      messages,
      matchStoppedCopies({
        history: messages,
        streamed: [...turn.seenMessageKeys],
        arrivals: new Map(
          [...turn.heldCopies].map(([key, held]) => [key, held.arrivedAfter] as const),
        ),
        unended: new Set(unendedReplyKeys(turn)),
        removed: turn.removedReplies,
        accounted: ledger.keys,
        holdsShown: (copy, replyKey) => items.holdsShownText(turn, copy, keyTimestamp(replyKey)),
      }),
    );
    for (const message of messages) turn.heldCopies.delete(transcriptMessageKey(message) ?? "");
  });

  /**
   * Places, once, the stopped copies no history read placed by turn end. A copy that holds all T3
   * shows of the latest unended reply finishes it; any other is a reply of its own. The reply stays
   * unaccounted so a later read still projects it under its own key.
   */
  const placeHeldCopies = Effect.fnUntraced(function* (turn: ActivePiTurn) {
    const open = unendedReplyKeys(turn).filter((key) => !turn.removedReplies.has(key));
    for (const [key, { message: copy }] of turn.heldCopies) {
      const latest = open.at(-1);
      const replaced =
        latest !== undefined && items.holdsShownText(turn, copy, keyTimestamp(latest))
          ? open.pop()
          : undefined;
      yield* items.upsertAssistantMessage(
        turn,
        copy,
        replaced === undefined ? undefined : keyTimestamp(replaced),
      );
      turn.seenMessageKeys.add(key);
      turn.shownMessageKeys.add(key);
    }
    turn.heldCopies.clear();
  });

  const logOutcome = (turn: ActivePiTurn | null, outcome: string) =>
    Effect.logInfo("orchestration-v2.prime-agent-transcript-reconciled", {
      driver: input.driver,
      providerTurnId: turn?.providerTurn.id ?? null,
      outcome,
    }).pipe(Effect.withSpan("PrimeAgentReconciler.reconcile"));

  /**
   * The ends `event` shows the stream moved past without delivering, as
   * reply keys and tool call ids, minus those a gap read already looked for.
   * Prime Agent ends a reply before it runs the reply's tools or ends the
   * turn, and runs every tool to its end before the next reply.
   */
  const provenLostEnds = (turn: ActivePiTurn, event: PiRpcRecord): ReadonlyArray<string> => {
    const message = eventMessage(event);
    const key = transcriptMessageKey(message);
    const startsReply =
      recordString(message, "role") === "assistant" &&
      key !== undefined &&
      !turn.seenMessageKeys.has(key) &&
      !isStoppedCopy(turn, event);
    const type = event["type"];
    const endsWork =
      startsReply || type === "tool_execution_start" || type === "turn_end" || type === "agent_end";
    if (!endsWork) return [];
    // Parallel tools start while others run, so a tool start proves no tool's end lost.
    const openTools = type === "tool_execution_start" ? [] : [...turn.openTools.keys()];
    return [...unendedReplyKeys(turn), ...openTools].filter((end) => !turn.soughtEnds.has(end));
  };

  /**
   * Projects the stored versions of the replies and tools a gap in the
   * turn's stream left without an end. Each lost end gets one such read;
   * if it cannot repair one, the settle read does.
   */
  const repairGap = Effect.fnUntraced(function* (
    turn: ActivePiTurn,
    lostEnds: ReadonlyArray<string>,
  ) {
    for (const end of lostEnds) turn.soughtEnds.add(end);
    const messages = yield* readHistory(request, SHORT_HISTORY_READ_TIMEOUT_MS);
    if (messages === undefined) return yield* logOutcome(turn, "gap-unreadable");
    const unended = new Set(unendedReplyKeys(turn));
    const lost = messages.filter((message) => {
      switch (recordString(message, "role")) {
        case "assistant":
          return unended.has(transcriptMessageKey(message) ?? "");
        case "toolResult":
          return turn.openTools.has(recordString(message, "toolCallId") ?? "");
        default:
          return false;
      }
    });
    yield* project(turn, lost);
    for (const message of lost) {
      const key = transcriptMessageKey(message);
      if (key !== undefined) turn.shownMessageKeys.add(key);
    }
    yield* logOutcome(turn, lost.length > 0 ? "gap-repaired" : "gap-not-stored");
  });

  /**
   * Repairs what a gap in a running turn's stream lost, before the event proving it applies:
   * replies cut off before message_end and tools whose end never came. Stored versions replace them
   * and keep their place. A failed read leaves them open for the settle read; nothing is committed.
   */
  const repairLostEnds = (turn: ActivePiTurn, event: PiRpcRecord) => {
    if (!enabled || ledger === null || turn.transcript !== "streaming") return Effect.void;
    const lostEnds = provenLostEnds(turn, event);
    return lostEnds.length > 0 ? repairGap(turn, lostEnds) : Effect.void;
  };

  /**
   * Reconciles a turn whose settle probe found the agent idle. Returns
   * whether the turn may settle now; otherwise the caller probes again.
   */
  const reconcileSettlingTurn = Effect.fnUntraced(function* (
    turn: ActivePiTurn,
    idleState: unknown,
  ) {
    if (!enabled || turn.transcript !== "streaming") return true;
    const read = yield* readSettledTranscript(request, idleState);
    if (read._tag === "busy") return false;
    if (read._tag === "unconfirmed") {
      turn.settleReads += 1;
      if (turn.settleReads < MAX_SETTLE_READS) return false;
      // The turn is over either way. What it did not show in full stays
      // unaccounted, for a later read to project.
      turn.transcript = "unreconciled";
      yield* logOutcome(turn, "unconfirmed");
      return true;
    }
    yield* projectHistory(turn, read.messages);
    commit(read.messages, read.historyMark);
    turn.transcript = "reconciled";
    return true;
  });

  /**
   * Reconciles a turn that ends without an idle confirmation: Stop, a
   * rejected prompt. Everything stored by then belongs to it. A failed read
   * leaves the turn as the stream left it.
   */
  const reconcileEndingTurn = Effect.fnUntraced(function* (turn: ActivePiTurn) {
    if (!enabled || turn.transcript !== "streaming" || ledger === null) return;
    turn.transcript = "unreconciled";
    const messages = yield* readHistory(request, SHORT_HISTORY_READ_TIMEOUT_MS);
    if (messages === undefined) return yield* logOutcome(turn, "unreadable");
    yield* projectHistory(turn, messages);
    commit(messages);
    turn.transcript = "reconciled";
  });

  /**
   * Accounts for what the turn showed live: after a read, every message it saw; without one, partly
   * shown messages stay unaccounted so a later read still projects them. Late tool events are
   * dropped either way since a read brings their stored outcome.
   */
  const closeTurn = (turn: ActivePiTurn) => {
    if (!enabled || ledger === null) return;
    const accounted =
      turn.transcript === "reconciled" ? turn.seenMessageKeys : turn.shownMessageKeys;
    for (const key of accounted) ledger.keys.add(key);
    for (const toolCallId of turn.toolStartedAt.keys()) ledger.toolCalls.add(toolCallId);
  };

  /**
   * Applies a confirmed history read while no turn runs. Messages no turn
   * accounted for are a wake whose events were lost: what they say is
   * applied now, and a continuation run is offered to show them. They stay
   * unaccounted, so the run that adopts the wake projects them.
   */
  const applyIdleTranscript = Effect.fnUntraced(function* (
    messages: ReadonlyArray<unknown>,
    historyMark: PiHistoryMark,
  ) {
    if (!enabled || ledger === null) return;
    const lost = unaccounted(messages);
    ledger.historyMark = historyMark;
    if (lost.length === 0 && !input.hasUnofferedWake()) return;
    yield* logOutcome(null, "recovered-wake");
    yield* input.recoverWake(
      lost.filter((message) => {
        const role = recordString(message, "role");
        return role !== "assistant" && role !== "toolResult";
      }),
    );
  });

  /** Reads an idle session's history when it may hold work T3 never saw. Returns whether anything moved. */
  const checkIdleSession = Effect.gen(function* () {
    const state = yield* request({ type: "get_state" }, STATE_READ_TIMEOUT_MS).pipe(Effect.option);
    if (Option.isNone(state)) return false;
    // A busy agent's events, or a later check, will show the work.
    if (!piStateIsIdle(state.value)) return true;
    if (
      ledger !== null &&
      historyMarkOf(state.value) === ledger.historyMark &&
      !input.hasUnofferedWake()
    ) {
      return false;
    }
    const read = yield* readSettledTranscript(request, state.value);
    if (read._tag === "read") {
      yield* input.enqueue({
        type: "t3.reconcile_idle",
        messages: read.messages,
        historyMark: read.historyMark,
      });
    }
    return true;
  });

  /**
   * The one cadence for lost events. A turn whose event count stopped
   * moving may have lost its end, so the pump confirms it against get_state.
   * An idle session with background work is checked for lost wakes, less
   * often while nothing changes.
   */
  const watch = Effect.gen(function* () {
    let lastSeen: { readonly turn: ActivePiTurn; readonly eventCount: number } | null = null;
    let idleIntervals = 1;
    let skipped = 0;
    while (true) {
      yield* Effect.sleep(CHECK_INTERVAL);
      const turn = activeTurn();
      const previous = lastSeen;
      lastSeen = turn === null ? null : { turn, eventCount: turn.sessionEventCount };
      if (turn !== null) {
        idleIntervals = 1;
        skipped = 0;
        // Even a turn that showed no agent event: all of them can be lost.
        if (previous?.turn === turn && previous.eventCount === turn.sessionEventCount) {
          yield* input.probeQuietTurn(turn);
        }
        continue;
      }
      if (!input.expectsWakes()) continue;
      skipped += 1;
      if (skipped < idleIntervals) continue;
      skipped = 0;
      const moved = yield* checkIdleSession;
      idleIntervals = moved ? 1 : Math.min(idleIntervals * 2, MAX_IDLE_INTERVALS);
    }
  });

  return {
    account,
    baseline,
    isLate,
    noteSeen,
    holdsCopy,
    placeHeldCopies,
    repairLostEnds,
    reconcileSettlingTurn,
    reconcileEndingTurn,
    closeTurn,
    applyIdleTranscript,
    watch: enabled ? watch : Effect.void,
  };
}
