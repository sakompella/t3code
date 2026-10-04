import type {
  NodeId,
  ThreadId,
  RunId,
  ProviderThreadId,
  ProviderTurnId,
  OrchestrationV2ProviderTurn,
  OrchestrationV2ProviderThread,
  OrchestrationV2ExecutionNode,
  OrchestrationV2ProviderRetry,
  OrchestrationV2ProviderFailure,
  OrchestrationV2RuntimeRequest,
  OrchestrationV2TurnItem,
  OrchestrationV2ProviderRef,
} from "@t3tools/contracts";
import type * as DateTime from "effect/DateTime";
import type * as Effect from "effect/Effect";
import type * as ProviderAdapter from "../ProviderAdapter.ts";
import type { makeProviderFailure } from "../ProviderFailure.ts";
import type { PiRpcRecord } from "./PiRpc.ts";

export interface PiStreamItemState {
  readonly nativeItemId: string;
  readonly kind: "assistant_message" | "reasoning";
  text: string;
  completed: boolean;
  flushScheduled: boolean;
  readonly startedAt: DateTime.Utc;
}

export type PiCompactionStatus = "running" | "completed" | "failed" | "cancelled";

export interface PiCompactionState {
  readonly nativeItemId: string;
  readonly startedAt: DateTime.Utc;
}

export interface PiProviderRetryState {
  readonly retry: OrchestrationV2ProviderRetry;
  readonly failure: OrchestrationV2ProviderFailure;
  readonly startedAt: DateTime.Utc;
  readonly itemOrdinal: number;
}

export interface ActivePiTurn {
  readonly turnInput: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly startedAt: DateTime.Utc;
  readonly itemOrdinals: Map<string, number>;
  nextItemOrdinal: number;
  /** Increments on assistant `message_start` so content indexes stay unique. */
  messageOrdinal: number;
  readonly streamItems: Map<string, PiStreamItemState>;
  readonly toolArgs: Map<string, unknown>;
  /**
   * First-seen time per `toolCallId`. Later update/end events reuse it so a
   * tool keeps one start timestamp and reports a real duration.
   */
  readonly toolStartedAt: Map<string, DateTime.Utc>;
  /** Latest start/update event of each tool that has not ended, to settle it if its end never comes. */
  readonly openTools: Map<string, PiRpcRecord>;
  interrupted: boolean;
  /**
   * Whether any agent run activity was observed. Command-only prompts (pure
   * extension slash commands) never start an agent run and never emit
   * `agent_settled`; their deferred prompt ack plus an idle probe settles
   * the turn instead.
   */
  sawAgentActivity: boolean;
  /** Only slash-command prompts can complete without starting an agent run. */
  readonly promptMayBeCommandOnly: boolean;
  /** Pi reports context as unknown immediately after compaction; keep its estimate for the meter. */
  latestCompactionAfterTokens: number | null;
  /** Last streamed usage total already emitted on the running turn. */
  lastLiveUsedTokens: number | null;
  /** Invalidates idle snapshots when new work starts after a settle probe. */
  settleProbeGeneration: number;
  /** An extension may start compaction immediately after Pi emits agent_settled. */
  settleWhenIdle: boolean;
  /**
   * Session events the pump has read since the turn began. A turn whose count
   * stops moving may have lost its closing events (see `PiFlavor.lossyStream`).
   */
  sessionEventCount: number;
  /** Stored messages this turn's own events showed; see `transcriptMessageKey`. */
  readonly seenMessageKeys: Set<string>;
  /** Prime Agent's stored conversation was reconciled into the turn, or given up on. */
  transcriptReconciled: boolean;
  /** History reads that did not match the idle state at settle. */
  settleReads: number;
  sawCompaction: boolean;
  /** RPC compact is in flight; Pi abort does not cancel it. */
  manualCompactInFlight: boolean;
  activeCompaction: PiCompactionState | null;
  activeProviderRetry: PiProviderRetryState | null;
  /**
   * The "Finishing up…" row shown between the final reply and the end of the
   * run, while Prime Agent still reviews its own harness.
   */
  finishingUp: {
    readonly nativeItemId: string;
    readonly startedAt: DateTime.Utc;
    /** The row is only shown once the wait runs long enough to notice. */
    shown: boolean;
  } | null;
  /** Counts synthetic notices so each gets its own id within the turn. */
  noticeCount: number;
  /**
   * The wake message a continuation turn's own notification already reports,
   * so the buffered wake events do not report it twice.
   */
  wakeTrigger?: unknown;
  failure: ReturnType<typeof makeProviderFailure> | null;
  /** Session-tree refs read just before Stop terminates Pi, when no read is possible later. */
  stopTreeRefs?: PiTurnTreeRefs | null;
  /**
   * The turn's start entry, read once its prompt is persisted. A fork from an
   * earlier turn needs it while this turn still runs, and a later read would
   * mistake a steer's user entry for the turn start.
   */
  startEntryId?: string | null;
}

export interface PiTurnTreeRefs {
  readonly turnStartEntryId: string | null;
  readonly leafId: string | null;
}

export interface PendingPiPrompt {
  readonly nativeRequestId: string;
  readonly method: "select" | "confirm" | "input" | "editor";
  readonly questionId: string;
  readonly approvalKey: string;
  runtimeRequest: OrchestrationV2RuntimeRequest;
  readonly node: OrchestrationV2ExecutionNode;
  readonly turnItem: OrchestrationV2TurnItem;
}

export interface PendingPiWake {
  readonly events: Array<PiRpcRecord>;
  offered: boolean;
  readonly generation: number;
}

/** Where emitted items land: a turn on the parent thread, or a child's own thread. */
export interface PiItemScope {
  readonly threadId: ThreadId;
  readonly runId: RunId | null;
  readonly rootNodeId: NodeId;
  readonly providerThreadId: ProviderThreadId | null;
  readonly providerTurnId: ProviderTurnId | null;
  /** Keeps native item ids unique per turn or child. */
  readonly idPrefix: string;
}

/**
 * The transcript of one Prime Agent child, kept in the child's own T3 thread.
 * It carries the same item bookkeeping as `ActivePiTurn`, so the turn's item
 * emitters serve both.
 */
export interface PiChildTranscript {
  readonly scope: PiItemScope;
  readonly childThreadId: ThreadId;
  readonly childRootNodeId: NodeId;
  /** Parent thread, which sends the child's task prompt. */
  readonly parentThreadId: ThreadId;
  readonly itemOrdinals: Map<string, number>;
  nextItemOrdinal: number;
  /** The timestamp of the message being streamed; see `observeChildEvent`. */
  messageOrdinal: number;
  readonly streamItems: Map<string, PiStreamItemState>;
  readonly toolArgs: Map<string, unknown>;
  readonly toolStartedAt: Map<string, DateTime.Utc>;
  /** Latest start/update event of each tool that has not ended, to settle it on teardown. */
  readonly openTools: Map<string, PiRpcRecord>;
  /** Last status emitted on the child's root node. */
  status: OrchestrationV2ExecutionNode["status"] | null;
  /** The session this adapter observes, once `observe` succeeded. */
  observedSessionId: string | null;
  /** Sessions `observe` already failed for, so a busy roster does not retry in a loop. */
  readonly failedSessionIds: Set<string>;
}

export type PiItemSink = ActivePiTurn | PiChildTranscript;

export interface PiThreadState {
  providerThread: OrchestrationV2ProviderThread;
  activeTurn: ActivePiTurn | null;
}

export interface PiItemHooks {
  readonly emit: (event: ProviderAdapter.ProviderAdapterV2Event) => Effect.Effect<void>;
  readonly providerRef: (
    nativeId: string,
    strength?: "strong" | "weak",
  ) => OrchestrationV2ProviderRef;
  readonly itemOrdinal: (turn: PiItemSink, nativeItemId: string) => number;
  readonly baseItemFields: (
    turn: PiItemSink,
    nativeItemId: string,
    startedAt: DateTime.Utc,
    updatedAt: DateTime.Utc,
  ) => Pick<
    Extract<OrchestrationV2TurnItem, { type: "dynamic_tool" }>,
    | "id"
    | "threadId"
    | "runId"
    | "nodeId"
    | "providerThreadId"
    | "providerTurnId"
    | "nativeItemRef"
    | "parentItemId"
    | "ordinal"
    | "startedAt"
    | "updatedAt"
  >;
  readonly emitItemNode: (
    turn: PiItemSink,
    nativeItemId: string,
    kind: OrchestrationV2ExecutionNode["kind"],
    status: OrchestrationV2ExecutionNode["status"],
    startedAt: DateTime.Utc,
    completedAt: DateTime.Utc | null,
  ) => Effect.Effect<void>;
  readonly emitToolItem: (
    turn: PiItemSink,
    event: PiRpcRecord,
    phase: "start" | "update" | "end",
    settledAs?: "completed" | "failed" | "interrupted",
  ) => Effect.Effect<void>;
  readonly adoptSnapshot: (
    turn: PiItemSink,
    message: unknown,
    final: boolean,
  ) => Effect.Effect<void>;
  readonly completeOpenStreamItems: (turn: PiItemSink) => Effect.Effect<void>;
  readonly settleOpenTools: (
    turn: PiItemSink,
    status: "completed" | "failed" | "interrupted",
  ) => Effect.Effect<void>;
}
