import type {
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  OrchestrationV2ExecutionNode,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import type * as IdAllocator from "../IdAllocator.ts";
import {
  makeSubagentChildThread,
  makeSubagentConversationArtifacts,
  subagentThreadTitle,
} from "../SubagentProjection.ts";
import {
  piRecordField as recordField,
  piRecordNumber as recordNumber,
  piRecordString as recordString,
  type PiRpcRecord,
  type PiRpcConnection,
} from "./PiRpc.ts";
import type { ActivePiTurn, PiChildTranscript, PiItemHooks } from "./PiAdapterV2State.ts";

interface PiRlmChildState {
  readonly snapshot: unknown;
  readonly startedAt: DateTime.Utc;
  readonly terminal: boolean;
  readonly turn: ActivePiTurn;
}

function rlmChildStatus(
  status: string | undefined,
): "pending" | "running" | "completed" | "failed" | "cancelled" {
  switch (status) {
    case "queued":
      return "pending";
    case "done":
      return "completed";
    case "error":
      return "failed";
    case "cancelled":
      return "cancelled";
    default:
      return "running";
  }
}

/** A short live status line: the child's own progress note, else what it is doing. */
function rlmChildProgress(snapshot: unknown): string | undefined {
  const note = recordString(snapshot, "progressNote");
  if (note !== undefined && note.length > 0) return note.slice(0, 200);
  const activity = recordField(snapshot, "activity");
  const kind = recordString(activity, "kind");
  if (kind === undefined) return undefined;
  const toolName = recordString(activity, "toolName");
  return toolName === undefined ? kind : `${kind} ${toolName}`;
}

export function makePrimeAgentChildThreads(input: {
  readonly driver: ProviderDriverKind;
  readonly instanceId: ProviderInstanceId;
  readonly name: string;
  readonly childThreads: boolean;
  readonly idAllocator: IdAllocator.IdAllocatorV2["Service"];
  readonly request: PiRpcConnection["request"];
  readonly contentText: (content: unknown) => string;
  readonly items: Pick<
    PiItemHooks,
    | "emit"
    | "providerRef"
    | "baseItemFields"
    | "itemOrdinal"
    | "emitToolItem"
    | "adoptSnapshot"
    | "completeOpenStreamItems"
    | "settleOpenTools"
  >;
}) {
  const { driver, instanceId, name, childThreads, idAllocator, request, contentText } = input;
  const {
    emit,
    providerRef,
    baseItemFields,
    itemOrdinal,
    emitToolItem,
    adoptSnapshot,
    completeOpenStreamItems,
    settleOpenTools,
  } = input.items;
  const rlmChildren = new Map<string, PiRlmChildState>();
  const rlmTranscripts = new Map<string, PiChildTranscript>();
  const observedTranscripts = new Map<string, PiChildTranscript>();
  const openChildTranscript = Effect.fnUntraced(function* (input: {
    readonly turn: ActivePiTurn;
    readonly childId: string;
    readonly subagentId: NodeId;
    readonly snapshot: unknown;
    readonly title: string | null;
    readonly prompt: string;
    readonly now: DateTime.Utc;
  }) {
    const { turnInput } = input.turn;
    const childThreadId = idAllocator.derive.threadFromProviderThread({
      driver,
      nativeThreadId: `${turnInput.providerThread.id}:rlm:${input.childId}`,
    });
    const childRootNodeId = idAllocator.derive.nodeFromProviderItem({
      driver,
      nativeItemId: `rlm:${input.childId}:thread-root`,
    });
    const transcript: PiChildTranscript = {
      scope: {
        threadId: childThreadId,
        runId: null,
        rootNodeId: childRootNodeId,
        providerThreadId: null,
        providerTurnId: null,
        idPrefix: `rlm:${input.childId}`,
      },
      childThreadId,
      childRootNodeId,
      parentThreadId: turnInput.threadId,
      itemOrdinals: new Map(),
      nextItemOrdinal: 0,
      messageOrdinal: 0,
      streamItems: new Map(),
      toolArgs: new Map(),
      toolStartedAt: new Map(),
      openTools: new Map(),
      status: null,
      observedSessionId: null,
      failedSessionIds: new Set(),
    };
    rlmTranscripts.set(input.childId, transcript);
    const model = recordString(input.snapshot, "model");
    yield* emit({
      type: "app_thread.created",
      driver,
      appThread: makeSubagentChildThread({
        parentThread: turnInput.appThread,
        childThreadId,
        parentNodeId: input.subagentId,
        activeProviderThreadId: null,
        providerInstanceId: instanceId,
        modelSelection:
          model !== undefined && model !== turnInput.modelSelection.model
            ? { instanceId: instanceId, model }
            : turnInput.modelSelection,
        title: subagentThreadTitle({
          parentTitle: turnInput.appThread.title,
          prompt: input.prompt,
          title: input.title,
          ordinal: rlmTranscripts.size,
        }),
        now: input.now,
        createdBy: "agent",
        creationSource: "provider",
      }),
    });
    return transcript;
  });

  /** The child thread's root turn carries the child's status for clients. */
  const emitChildRootNode = (
    transcript: PiChildTranscript,
    status: OrchestrationV2ExecutionNode["status"],
    nativeTaskId: string,
    startedAt: DateTime.Utc,
    completedAt: DateTime.Utc | null,
  ) =>
    Effect.suspend(() => {
      if (transcript.status === status) return Effect.void;
      transcript.status = status;
      return emit({
        type: "node.updated",
        driver,
        node: {
          id: transcript.childRootNodeId,
          threadId: transcript.childThreadId,
          runId: null,
          parentNodeId: null,
          rootNodeId: transcript.childRootNodeId,
          kind: "root_turn",
          status,
          countsForRun: false,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: providerRef(nativeTaskId),
          runtimeRequestId: null,
          checkpointScopeId: null,
          startedAt,
          completedAt,
        },
      });
    });

  /** A user turn in a child: its task from the parent, or a later agent message. */
  const emitChildUserMessage = Effect.fnUntraced(function* (
    transcript: PiChildTranscript,
    message: unknown,
  ) {
    const text = contentText(recordField(message, "content")).trim();
    if (text.length === 0) return;
    const timestamp = recordNumber(message, "timestamp");
    // Timestamps keep a replayed history and the live stream from
    // emitting the same message twice.
    const nativeItemId = `${transcript.scope.idPrefix}:u${timestamp ?? transcript.nextItemOrdinal}`;
    const now = yield* DateTime.now;
    const artifacts = makeSubagentConversationArtifacts({
      messageId: idAllocator.derive.messageFromProviderItem({ driver, nativeItemId }),
      senderThreadId: transcript.parentThreadId,
      turnItemId: idAllocator.derive.turnItemFromProviderItem({ driver, nativeItemId }),
      threadId: transcript.childThreadId,
      rootNodeId: transcript.childRootNodeId,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: providerRef(nativeItemId),
      role: "user",
      text,
      ordinal: itemOrdinal(transcript, nativeItemId),
      now: Option.getOrElse(DateTime.make(timestamp ?? Number.NaN), () => now),
    });
    yield* emit({ type: "message.updated", driver, message: artifacts.message });
    yield* emit({ type: "turn_item.updated", driver, turnItem: artifacts.turnItem });
  });

  /**
   * Routes one event of an observed child session onto its thread. The
   * child's RPC drops events like the main one, so assistant text comes
   * from message snapshots, and each message is keyed by its own timestamp:
   * a lost `message_start` cannot shift ids, and replaying history over
   * live events is idempotent.
   */
  const observeChildEvent = Effect.fnUntraced(function* (
    transcript: PiChildTranscript,
    event: PiRpcRecord,
  ) {
    switch (event["type"]) {
      case "message_update":
      case "message_end": {
        const message = event["message"];
        const role = recordString(message, "role");
        if (role === "assistant") {
          transcript.messageOrdinal =
            recordNumber(message, "timestamp") ?? transcript.messageOrdinal + 1;
          const final = event["type"] === "message_end";
          yield* adoptSnapshot(transcript, message, final);
          if (final) yield* completeOpenStreamItems(transcript);
          return;
        }
        if (event["type"] === "message_end" && role !== "toolResult") {
          // Custom messages show only when the agent marks them for display.
          if (role === "user" || recordField(message, "display") === true) {
            yield* emitChildUserMessage(transcript, message);
          }
        }
        return;
      }
      case "tool_execution_start":
      case "tool_execution_update":
        yield* emitToolItem(
          transcript,
          event,
          event["type"] === "tool_execution_start" ? "start" : "update",
        );
        return;
      case "tool_execution_end":
        yield* emitToolItem(transcript, event, "end");
        return;
      default:
        return;
    }
  });

  /** Replays `observe`'s history as the events that would have produced it. */
  const replayChildHistory = Effect.fnUntraced(function* (
    transcript: PiChildTranscript,
    messages: ReadonlyArray<unknown>,
  ) {
    for (const message of messages) {
      const role = recordString(message, "role");
      if (role === "toolResult") {
        yield* observeChildEvent(transcript, {
          type: "tool_execution_end",
          toolCallId: recordString(message, "toolCallId"),
          toolName: recordString(message, "toolName"),
          result: {
            content: recordField(message, "content"),
            details: recordField(message, "details"),
          },
          isError: recordField(message, "isError") === true,
        });
        continue;
      }
      yield* observeChildEvent(transcript, { type: "message_end", message });
      const content = role === "assistant" ? recordField(message, "content") : undefined;
      for (const block of Array.isArray(content) ? content : []) {
        if (recordField(block, "type") !== "toolCall") continue;
        yield* observeChildEvent(transcript, {
          type: "tool_execution_start",
          toolCallId: recordString(block, "id"),
          toolName: recordString(block, "name"),
          args: recordField(block, "arguments"),
        });
      }
    }
  });

  /**
   * Ends a child's transcript for good: open text closes (which also turns
   * its pending flushes into no-ops) and tools without an end event settle
   * as `toolStatus`. The child's RPC drops events, so none of this can wait
   * for the stream to say so.
   */
  const finalizeChildTranscript = Effect.fnUntraced(function* (
    transcript: PiChildTranscript,
    toolStatus: "completed" | "failed" | "interrupted",
  ) {
    yield* completeOpenStreamItems(transcript);
    yield* settleOpenTools(transcript, toolStatus);
  });

  const stopObservingChild = Effect.fnUntraced(function* (
    transcript: PiChildTranscript,
    sessionIsAlive: boolean,
  ) {
    const sessionId = transcript.observedSessionId;
    if (sessionId === null) return;
    transcript.observedSessionId = null;
    observedTranscripts.delete(sessionId);
    if (sessionIsAlive) {
      yield* request({ type: "unobserve", activeSessionId: sessionId }).pipe(Effect.ignore);
    }
  });

  /**
   * Ends a child's observation once the child is terminal. While its
   * session is still reachable, its history is read one last time so a lost
   * end event does not cut the final answer; what is still open after that
   * settles to match the child's outcome.
   */
  const finishChildObservation = Effect.fnUntraced(function* (
    transcript: PiChildTranscript,
    status: OrchestrationV2ExecutionNode["status"],
    sessionIsAlive: boolean,
  ) {
    const sessionId = transcript.observedSessionId;
    if (sessionIsAlive && sessionId !== null) {
      const observed = yield* request({ type: "observe", activeSessionId: sessionId }).pipe(
        Effect.option,
      );
      if (Option.isSome(observed)) {
        const history = recordField(observed.value, "messages");
        yield* replayChildHistory(transcript, Array.isArray(history) ? history : []);
      }
    }
    yield* finalizeChildTranscript(
      transcript,
      status === "completed" || status === "failed" ? status : "interrupted",
    );
    yield* stopObservingChild(transcript, sessionIsAlive);
  });

  /**
   * Streams a running child's session into its thread: one observation per
   * live child, started when its session id first shows up. A failed
   * `observe` is not retried for the same session.
   */
  const syncChildObservation = Effect.fnUntraced(function* (
    transcript: PiChildTranscript,
    snapshot: unknown,
  ) {
    const sessionId = recordString(snapshot, "activeSessionId");
    if (
      sessionId === undefined ||
      transcript.observedSessionId === sessionId ||
      transcript.failedSessionIds.has(sessionId)
    ) {
      return;
    }
    yield* stopObservingChild(transcript, true);
    const observed = yield* request({ type: "observe", activeSessionId: sessionId }).pipe(
      Effect.option,
    );
    if (Option.isNone(observed)) {
      transcript.failedSessionIds.add(sessionId);
      yield* Effect.logWarning(`${name} could not observe a child session`, { sessionId });
      return;
    }
    transcript.observedSessionId = sessionId;
    observedTranscripts.set(sessionId, transcript);
    const history = recordField(observed.value, "messages");
    yield* replayChildHistory(transcript, Array.isArray(history) ? history : []);
  });

  /** Mirrors one `rlm_child_update` roster snapshot onto T3's subagent surfaces. */
  const emitRlmChild = Effect.fnUntraced(function* (
    snapshot: unknown,
    currentTurn: ActivePiTurn | null,
    statusOverride?: "interrupted",
  ) {
    const childId = recordString(snapshot, "id");
    if (childId === undefined) return;
    const emittedAt = yield* DateTime.now;
    const previous = rlmChildren.get(childId);
    // Nested children report through the root session with their parent
    // child's id. Their first update can arrive after the root turn settled.
    const parentChildId = recordString(snapshot, "parentId");
    const parentChild = parentChildId === undefined ? undefined : rlmChildren.get(parentChildId);
    // A child outlives the run that spawned it; its card stays on that run.
    const turn = previous?.turn ?? parentChild?.turn ?? currentTurn;
    if (turn === null) return;
    const status = statusOverride ?? rlmChildStatus(recordString(snapshot, "status"));
    const terminal = status !== "pending" && status !== "running";
    // A child that finished in an earlier turn already shows its outcome.
    // Later roster churn (the parent deleting it, a resynced roster) must
    // not rewrite that card from an unrelated turn.
    if (previous === undefined && terminal) return;
    const state: PiRlmChildState = {
      snapshot,
      startedAt: previous?.startedAt ?? emittedAt,
      terminal,
      turn,
    };
    rlmChildren.set(childId, state);
    const nativeTaskId = `rlm:${childId}`;
    const subagentId = idAllocator.derive.nodeFromProviderItem({
      driver,
      nativeItemId: nativeTaskId,
    });
    const title = recordString(snapshot, "sessionName") ?? null;
    const prompt = recordString(snapshot, "label") ?? title ?? "child agent";
    const progress = terminal ? undefined : rlmChildProgress(snapshot);
    const result = terminal
      ? (recordString(snapshot, "error") ?? recordString(snapshot, "answerPreview") ?? null)
      : null;
    const completedAt = terminal ? emittedAt : null;
    const transcript = childThreads
      ? (rlmTranscripts.get(childId) ??
        (yield* openChildTranscript({
          turn,
          childId,
          subagentId,
          snapshot,
          title,
          prompt,
          now: emittedAt,
        })))
      : undefined;
    if (transcript !== undefined) {
      // Close the transcript before the root reports the outcome.
      if (terminal) yield* finishChildObservation(transcript, status, statusOverride === undefined);
      yield* emitChildRootNode(transcript, status, nativeTaskId, state.startedAt, completedAt);
    }
    yield* emit({
      type: "subagent.updated",
      driver,
      subagent: {
        id: subagentId,
        threadId: turn.turnInput.threadId,
        runId: turn.turnInput.runId,
        // Nesting under the parent subagent's node is what lets clients
        // draw Prime Agent's subagent tree.
        parentNodeId:
          parentChildId === undefined
            ? turn.turnInput.rootNodeId
            : idAllocator.derive.nodeFromProviderItem({
                driver,
                nativeItemId: `rlm:${parentChildId}`,
              }),
        origin: "provider_native",
        createdBy: "agent",
        driver,
        providerInstanceId: instanceId,
        providerThreadId: turn.turnInput.providerThread.id,
        childThreadId: transcript?.childThreadId ?? null,
        nativeTaskRef: providerRef(nativeTaskId),
        prompt,
        title,
        model: recordString(snapshot, "model") ?? null,
        status,
        ...(progress === undefined ? {} : { progress }),
        result,
        startedAt: state.startedAt,
        completedAt,
        updatedAt: emittedAt,
      },
    });
    yield* emit({
      type: "turn_item.updated",
      driver,
      turnItem: {
        ...baseItemFields(turn, nativeTaskId, state.startedAt, emittedAt),
        status,
        title,
        completedAt,
        type: "subagent",
        subagentId,
        origin: "provider_native",
        driver,
        providerInstanceId: instanceId,
        childThreadId: transcript?.childThreadId ?? null,
        prompt,
        ...(progress === undefined ? {} : { progress }),
        result,
      },
    });
    if (transcript !== undefined && !terminal) {
      yield* syncChildObservation(transcript, snapshot);
    }
    if (terminal) rlmChildren.delete(childId);
  });

  const handleEvent = Effect.fnUntraced(function* (event: PiRpcRecord, turn: ActivePiTurn | null) {
    switch (event["type"]) {
      case "rlm_child_update":
        yield* emitRlmChild(event["child"], turn);
        return;
      case "observed_session_event": {
        const sessionId = recordString(event, "activeSessionId");
        const transcript = sessionId === undefined ? undefined : observedTranscripts.get(sessionId);
        const observed = event["event"];
        if (transcript !== undefined && Predicate.isObject(observed))
          yield* observeChildEvent(transcript, observed);
        return;
      }
      case "observed_session_closed": {
        const sessionId = recordString(event, "activeSessionId");
        const transcript = sessionId === undefined ? undefined : observedTranscripts.get(sessionId);
        if (sessionId !== undefined && transcript !== undefined) {
          transcript.failedSessionIds.add(sessionId);
          yield* finalizeChildTranscript(transcript, "interrupted");
          yield* stopObservingChild(transcript, false);
        }
        return;
      }
    }
  });
  const interrupt = (turn?: ActivePiTurn) =>
    Effect.forEach(
      Array.from(rlmChildren.values()).filter((child) => turn === undefined || child.turn === turn),
      (child) => emitRlmChild(child.snapshot, null, "interrupted"),
      { discard: true },
    );
  return {
    handleEvent,
    interrupt,
    hasLiveChildren: () => Array.from(rlmChildren.values()).some((child) => !child.terminal),
  };
}
