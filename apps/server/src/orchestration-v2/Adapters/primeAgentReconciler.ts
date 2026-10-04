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
} from "./PiRpc.ts";
import type { ActivePiTurn } from "./PiAdapterV2State.ts";

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
/** Stop waits on this read, so it gives up sooner. */
const ENDING_HISTORY_READ_TIMEOUT_MS = 2_000;

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

type TranscriptRead =
  | { readonly _tag: "read"; readonly messages: ReadonlyArray<unknown> }
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
 * Reads the stored conversation of an agent that `idleState` showed idle,
 * and confirms it with a second state read. Stock RPC has no atomic
 * snapshot, so this narrows the race rather than closing it: the history
 * counts only if the agent is still idle and both states count exactly the
 * messages it holds.
 */
const readSettledTranscript = Effect.fnUntraced(function* (
  request: PiRpcConnection["request"],
  idleState: unknown,
): Effect.fn.Return<TranscriptRead> {
  const messages = yield* readHistory(request, HISTORY_READ_TIMEOUT_MS);
  const after = yield* request({ type: "get_state" }, STATE_READ_TIMEOUT_MS).pipe(Effect.option);
  if (Option.isNone(after) || messages === undefined) return { _tag: "unconfirmed" };
  if (!piStateIsIdle(after.value)) return { _tag: "busy" };
  const counted = (state: unknown) => recordNumber(state, "messageCount") ?? messages.length;
  if (counted(idleState) !== messages.length || counted(after.value) !== messages.length) {
    return { _tag: "unconfirmed" };
  }
  return { _tag: "read", messages };
});

interface TranscriptLedger {
  /** Stored messages T3 already accounted for: projected, seen live, or older than the session. */
  readonly keys: Set<string>;
  /** Tool calls whose outcome T3 already shows. */
  readonly toolCalls: Set<string>;
  /** How many messages the last confirmed read held, to tell when the history changed. */
  messageCount: number;
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
 * Makes T3's projection match what Prime Agent stored, for a stream that can
 * drop any event (see `PiFlavor.lossyStream`).
 *
 * The stored conversation (`get_messages`) is the authority. Its messages are
 * matched to T3's items by native identity: an assistant message by its
 * timestamp, which the live stream keys its items by too, and a tool by its
 * call id. Text is never matched by prefix. The reconciler runs inside the
 * event pump, so it never interleaves with live events, and every run is
 * idempotent: a message is projected into the turn that stored it, then
 * accounted for, and later reads or late live events for it change nothing.
 *
 * It runs when a turn settles, when Stop ends one, and on a backed-off quiet
 * check while an idle session has background work, which is where wakes are
 * lost whole. What the history cannot show stays unknown: a child's outcome
 * comes only from the notice Prime Agent stores for its parent.
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
    ) => Effect.Effect<void, E>;
    readonly startTool: (turn: ActivePiTurn, toolCall: unknown) => Effect.Effect<void, E>;
    readonly endTool: (turn: ActivePiTurn, toolResult: unknown) => Effect.Effect<void, E>;
    /** Handles a stored message other than a reply as if its `message_start` had arrived. */
    readonly replayMessageStart: (message: unknown) => Effect.Effect<void, E>;
  };
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

  /** Marks a whole confirmed history as accounted for. */
  const commit = (messages: ReadonlyArray<unknown>) => {
    if (ledger === null) return;
    for (const message of messages) {
      const key = transcriptMessageKey(message);
      if (key !== undefined) ledger.keys.add(key);
    }
    for (const toolCallId of toolCallIdsOf(messages)) ledger.toolCalls.add(toolCallId);
    ledger.messageCount = messages.length;
  };

  /** The session the ledger was seeded from. */
  let seededSession: string | null = null;

  /**
   * Everything `sessionFile` stored so far is history T3 did not stream:
   * either earlier turns or work from before this process attached. When
   * the read fails, what was stored before it is told apart by its
   * timestamp. A session already seeded keeps its ledger, so the messages
   * it has not accounted for yet still reach T3.
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
      messageCount: 0,
      storedBefore: messages === undefined ? readAt : 0,
    };
    if (messages !== undefined) commit(messages);
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

  /** Remembers which stored messages the turn's own events already showed, and which in full. */
  const noteSeen = (turn: ActivePiTurn, event: PiRpcRecord) => {
    const message = eventMessage(event);
    const key = transcriptMessageKey(message);
    if (key === undefined) return;
    turn.seenMessageKeys.add(key);
    if (showsInFull(turn, event["type"], message)) turn.shownMessageKeys.add(key);
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

  /** Projects the stored messages T3 has not accounted for into `turn`. */
  const project = Effect.fnUntraced(function* (
    turn: ActivePiTurn,
    messages: ReadonlyArray<unknown>,
  ) {
    for (const message of unaccounted(messages)) {
      switch (recordString(message, "role")) {
        case "assistant": {
          yield* items.upsertAssistantMessage(turn, message);
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
  });

  const logOutcome = (turn: ActivePiTurn | null, outcome: string) =>
    Effect.logInfo("orchestration-v2.prime-agent-transcript-reconciled", {
      driver: input.driver,
      providerTurnId: turn?.providerTurn.id ?? null,
      outcome,
    }).pipe(Effect.withSpan("PrimeAgentReconciler.reconcile"));

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
    yield* project(turn, read.messages);
    commit(read.messages);
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
    const messages = yield* readHistory(request, ENDING_HISTORY_READ_TIMEOUT_MS);
    if (messages === undefined) return yield* logOutcome(turn, "unreadable");
    yield* project(turn, messages);
    commit(messages);
    turn.transcript = "reconciled";
  });

  /**
   * Accounts for what the turn showed live. After a read, that is every
   * message it saw, even one stored only after the read. Without a read, a
   * message it showed only in part stays unaccounted, so a later read still
   * projects what was stored. Late events of its tools are dropped either
   * way: a read brings their stored outcome.
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
  const applyIdleTranscript = Effect.fnUntraced(function* (messages: ReadonlyArray<unknown>) {
    if (!enabled || ledger === null) return;
    const lost = unaccounted(messages);
    ledger.messageCount = messages.length;
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
    const messageCount = recordNumber(state.value, "messageCount");
    if (ledger !== null && messageCount === ledger.messageCount && !input.hasUnofferedWake()) {
      return false;
    }
    const read = yield* readSettledTranscript(request, state.value);
    if (read._tag === "read") {
      yield* input.enqueue({ type: "t3.reconcile_idle", messages: read.messages });
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
    baseline,
    isLate,
    noteSeen,
    reconcileSettlingTurn,
    reconcileEndingTurn,
    closeTurn,
    applyIdleTranscript,
    watch: enabled ? watch : Effect.void,
  };
}
