import {
  CheckpointId,
  CheckpointScopeId,
  type CommandId,
  latestProviderTurnForAttempt,
  type OrchestrationV2ContextHandoff,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2RollbackSummary,
  type OrchestrationV2TurnItem,
  ProviderThreadId,
  type RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import type * as ServerConfig from "../config.ts";
import type * as ServerSettings from "../serverSettings.ts";
import { makeIsManagedWorktreePath } from "../worktreesDirectory.ts";
import { withWorkspaceLease } from "../workspace/workspaceLease.ts";
import {
  isCheckpointRestoreIsolated,
  SHARED_WORKSPACE_RESTORE_MESSAGE,
} from "./CheckpointRestoreSafety.ts";
import { CheckpointServiceV2 } from "./CheckpointService.ts";
import { EventSinkV2 } from "./EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { ProjectionStoreV2 } from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { RuntimePolicyV2 } from "./RuntimePolicy.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";

export const ROLLBACK_FAILED_MESSAGE =
  "The provider could not roll back this conversation. Try again; if it keeps failing, check the provider and server logs.";

export class CheckpointRollbackExecutionError extends Schema.TaggedError<CheckpointRollbackExecutionError>()(
  "CheckpointRollbackExecutionError",
  {
    reason: Schema.Literals([
      "rollback-target-invalid",
      "active-provider-changed",
      "provider-turn-unavailable",
      "unexpected-failure",
      "shared-workspace",
    ]),
    threadId: ThreadId,
    providerThreadId: ProviderThreadId,
    checkpointId: CheckpointId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "rollback-target-invalid":
        return `Rollback target ${this.checkpointId} for provider thread ${this.providerThreadId} on thread ${this.threadId} is incomplete or invalid.`;
      case "active-provider-changed":
        return `Active provider changed before rollback target ${this.checkpointId} could execute on thread ${this.threadId}.`;
      case "provider-turn-unavailable":
        return `Provider turn for rollback target ${this.checkpointId} is unavailable on provider thread ${this.providerThreadId}.`;
      case "shared-workspace":
        return SHARED_WORKSPACE_RESTORE_MESSAGE;
      case "unexpected-failure":
        return ROLLBACK_FAILED_MESSAGE;
    }
  }
}

const isCheckpointRollbackExecutionError = Schema.is(CheckpointRollbackExecutionError);

export interface CheckpointRollbackServiceV2Shape {
  readonly execute: (input: {
    readonly threadId: ThreadId;
    readonly requestId: CommandId;
    readonly providerThreadId: ProviderThreadId;
    readonly checkpointId: CheckpointId;
    readonly scopeId: CheckpointScopeId;
    readonly restoreFiles?: boolean;
    readonly summary?: OrchestrationV2RollbackSummary;
  }) => Effect.Effect<void, CheckpointRollbackExecutionError>;
}

export class CheckpointRollbackServiceV2 extends Context.Service<
  CheckpointRollbackServiceV2,
  CheckpointRollbackServiceV2Shape
>()("t3/orchestration-v2/CheckpointRollbackService/CheckpointRollbackServiceV2") {}

export const layer: Layer.Layer<
  CheckpointRollbackServiceV2,
  never,
  | CheckpointServiceV2
  | GitWorkflowService.GitWorkflowService
  | EventSinkV2
  | IdAllocator.IdAllocatorV2
  | ProjectionStoreV2
  | ProviderSessionManagerV2
  | RuntimePolicyV2
  | FileSystem.FileSystem
  | Path.Path
  | ProjectStore.ProjectStoreV2
  | ServerConfig.ServerConfig
  | ServerSettings.ServerSettingsService
  | ThreadCommandExecutor.ThreadCommandExecutor
> = Layer.effect(
  CheckpointRollbackServiceV2,
  Effect.gen(function* () {
    const checkpoints = yield* CheckpointServiceV2;
    const gitWorkflow = yield* GitWorkflowService.GitWorkflowService;
    const eventSink = yield* EventSinkV2;
    const ids = yield* IdAllocator.IdAllocatorV2;
    const projections = yield* ProjectionStoreV2;
    const sessions = yield* ProviderSessionManagerV2;
    const runtimePolicy = yield* RuntimePolicyV2;
    const fileSystem = yield* FileSystem.FileSystem;
    const projects = yield* ProjectStore.ProjectStoreV2;
    const path = yield* Path.Path;
    const threadCommands = yield* ThreadCommandExecutor.ThreadCommandExecutor;
    const isManagedWorktreePath = yield* makeIsManagedWorktreePath;

    const execute = Effect.fn("orchestrationV2.checkpointRollback.execute")(function* (input: {
      readonly threadId: ThreadId;
      readonly requestId: CommandId;
      readonly providerThreadId: ProviderThreadId;
      readonly checkpointId: CheckpointId;
      readonly scopeId: CheckpointScopeId;
      readonly restoreFiles?: boolean;
      readonly summary?: OrchestrationV2RollbackSummary;
    }) {
      const projection = yield* projections.getThreadRecords(input.threadId, [
        "providerThreads",
        "providerSessions",
        "checkpoints",
        "checkpointScopes",
        "runs",
        "attempts",
        "nodes",
        "providerTurns",
        "contextHandoffs",
      ]);
      const providerThread = projection.providerThreads.find(
        (candidate) => candidate.id === input.providerThreadId,
      );
      const checkpoint = projection.checkpoints.find(
        (candidate) => candidate.id === input.checkpointId,
      );
      const scope = projection.checkpointScopes.find((candidate) => candidate.id === input.scopeId);
      if (
        providerThread === undefined ||
        providerThread.providerSessionId === null ||
        checkpoint === undefined ||
        scope === undefined ||
        checkpoint.scopeId !== scope.id ||
        checkpoint.status !== "ready"
      ) {
        return yield* new CheckpointRollbackExecutionError({
          reason: "rollback-target-invalid",
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
        });
      }
      if (
        providerThread.id !== projection.thread.activeProviderThreadId ||
        providerThread.providerInstanceId !== projection.thread.modelSelection.instanceId
      ) {
        return yield* new CheckpointRollbackExecutionError({
          reason: "active-provider-changed",
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
        });
      }

      const targetOrdinal = checkpoint.appRunOrdinal ?? 0;
      // Rolled-back turns stay in the audit history, but no longer exist in
      // the provider conversation and must not be counted by a later rewind.
      const rolledBackRunIds = new Set(
        projection.runs.filter((run) => run.status === "rolled_back").map((run) => run.id),
      );
      // A rollback that dropped this provider thread's native conversation
      // (see the other provider threads below) restarted it at
      // `firstRunOrdinal`. Turns of earlier runs belong to the dropped one.
      const conversationStart = providerThread.firstRunOrdinal ?? 0;
      const outsideRunIds = new Set(
        projection.runs
          .filter((run) => rolledBackRunIds.has(run.id) || run.ordinal < conversationStart)
          .map((run) => run.id),
      );
      const outsideAttemptIds = new Set(
        projection.attempts
          .filter((attempt) => outsideRunIds.has(attempt.runId))
          .map((attempt) => attempt.id),
      );
      const providerThreadTurns = projection.providerTurns.filter(
        (turn) =>
          turn.providerThreadId === providerThread.id &&
          (turn.runAttemptId === null || !outsideAttemptIds.has(turn.runAttemptId)),
      );
      // A goal run can span several native turns; roll back to its last.
      const providerTurnForRun = (run: (typeof projection.runs)[number]) =>
        latestProviderTurnForAttempt(projection.providerTurns, run.activeAttemptId) ??
        projection.providerTurns.find(
          (turn) =>
            turn.id ===
            projection.attempts.find((attempt) => attempt.id === run.activeAttemptId)
              ?.providerTurnId,
        );
      const targetRun = projection.runs.find((run) => run.ordinal === targetOrdinal);
      if (
        targetOrdinal > 0 &&
        (targetRun === undefined || providerTurnForRun(targetRun) === undefined)
      ) {
        return yield* new CheckpointRollbackExecutionError({
          reason: "provider-turn-unavailable",
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
        });
      }
      // The active provider thread rewinds to its own last turn at or before
      // the target. Turns of an earlier provider reached it only through a
      // context handoff, so a target among them rewinds it to its last earlier
      // turn, or to its start.
      const boundary = projection.runs
        .filter((run) => run.ordinal <= targetOrdinal && !outsideRunIds.has(run.id))
        .toSorted((left, right) => left.ordinal - right.ordinal)
        .flatMap((run) => {
          const providerTurn = providerTurnForRun(run);
          return providerTurn?.providerThreadId === providerThread.id
            ? [{ run, providerTurn }]
            : [];
        })
        .at(-1);
      const rollbackTarget: ProviderAdapter.ProviderAdapterV2RollbackTarget =
        boundary === undefined
          ? { type: "thread_start", checkpointId: checkpoint.id, appRunOrdinal: 0 }
          : {
              type: "provider_turn",
              checkpointId: checkpoint.id,
              appRunOrdinal: boundary.run.ordinal,
              providerTurn: boundary.providerTurn,
            };

      const { worktreePath, branch } = projection.thread;
      // Recreated only where the server would have made it, as turn start does.
      if (
        worktreePath != null &&
        branch != null &&
        !(yield* fileSystem.exists(worktreePath)) &&
        (yield* isManagedWorktreePath(worktreePath))
      ) {
        const project = yield* projects.get(projection.thread.projectId);
        if (Option.isSome(project)) {
          yield* gitWorkflow.pruneWorktrees({ cwd: project.value.workspaceRoot });
          yield* gitWorkflow.createWorktree({
            cwd: project.value.workspaceRoot,
            refName: branch,
            path: worktreePath,
          });
        }
      }

      if (
        input.restoreFiles !== false &&
        !(yield* isCheckpointRestoreIsolated(projection.thread, scope, {
          fileSystem,
          projections,
          projects,
          path,
        }))
      ) {
        return yield* new CheckpointRollbackExecutionError({
          reason: "shared-workspace",
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
        });
      }

      const modelSelection = projection.thread.modelSelection;
      const resolvedRuntimePolicy = yield* runtimePolicy.resolve({
        thread: projection.thread,
        modelSelection,
      });
      const existingSession = projection.providerSessions.find(
        (candidate) => candidate.id === providerThread.providerSessionId,
      );
      const session = yield* sessions.open({
        threadId: input.threadId,
        providerSessionId: providerThread.providerSessionId,
        modelSelection,
        runtimePolicy: resolvedRuntimePolicy,
        ...(existingSession === undefined ? {} : { resumeFromSession: existingSession }),
        ...(providerThread.nativeThreadRef?.nativeId == null
          ? {}
          : { initialNativeThreadId: providerThread.nativeThreadRef.nativeId }),
        ...(providerThread.nativeMetadata?.itemIdentityVersion === undefined
          ? {}
          : {
              initialProviderItemIdentityVersion: providerThread.nativeMetadata.itemIdentityVersion,
            }),
      });

      const runsToRollback = projection.runs.filter(
        (run) =>
          run.ordinal > targetOrdinal &&
          (run.status === "completed" ||
            run.status === "interrupted" ||
            run.status === "failed" ||
            run.status === "cancelled"),
      );

      const snapshot: Pick<
        ProviderAdapter.ProviderAdapterV2RollbackThreadResult,
        "providerThread" | "rollbackSummary"
      > =
        runsToRollback.length === 0
          ? { providerThread }
          : yield* session.rollbackThread({
              providerThread,
              target: rollbackTarget,
              providerThreadTurns,
              ...(input.summary === undefined ? {} : { summary: input.summary }),
            });
      if (input.restoreFiles !== false) yield* checkpoints.restore({ scope, checkpoint });
      const staleCheckpoints = projection.checkpoints.filter(
        (candidate) =>
          candidate.scopeId === scope.id &&
          candidate.appRunOrdinal !== null &&
          candidate.appRunOrdinal > targetOrdinal &&
          candidate.status === "ready",
      );
      if (staleCheckpoints.length > 0) {
        yield* checkpoints.deleteStaleRefs({ scope, checkpoints: staleCheckpoints });
      }

      // Thread commands write full provider thread rows under this lock. Without
      // it, a message sent during the rollback can plan against the old native
      // session and commit after this, undoing the provider's fork.
      yield* threadCommands.withLock(
        input.threadId,
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          const makeEvent = <Event extends OrchestrationV2DomainEvent>(event: Omit<Event, "id">) =>
            Effect.map(
              ids.allocate.event({ threadId: event.threadId }),
              (id) =>
                ({
                  ...event,
                  id,
                }) as Event,
            );
          const events: Array<OrchestrationV2DomainEvent> = [];
          // Other provider threads this rollback discarded turns from cannot be
          // rewound without their sessions. Dropping their native conversation
          // makes the next switch back start a fresh one with the kept history,
          // and that run sets `firstRunOrdinal` again. These go first: the last provider-thread.updated becomes the active one.
          for (const otherProviderThread of projection.providerThreads) {
            if (
              otherProviderThread.id === providerThread.id ||
              otherProviderThread.ownerNodeId !== null ||
              otherProviderThread.nativeThreadRef === null ||
              !runsToRollback.some((run) => run.providerThreadId === otherProviderThread.id)
            ) {
              continue;
            }
            const lastKeptRun = projection.runs.findLast(
              (run) =>
                run.providerThreadId === otherProviderThread.id &&
                run.ordinal <= targetOrdinal &&
                !rolledBackRunIds.has(run.id),
            );
            events.push(
              yield* makeEvent({
                type: "provider-thread.updated",
                threadId: input.threadId,
                driver: otherProviderThread.driver,
                providerInstanceId: otherProviderThread.providerInstanceId,
                occurredAt: now,
                payload: {
                  ...otherProviderThread,
                  nativeThreadRef: null,
                  nativeConversationHeadRef: null,
                  firstRunOrdinal: null,
                  lastRunOrdinal: lastKeptRun?.ordinal ?? null,
                  updatedAt: now,
                },
              }),
            );
          }
          events.push(
            yield* makeEvent({
              type: "provider-thread.updated",
              threadId: input.threadId,
              driver: providerThread.driver,
              providerInstanceId: providerThread.providerInstanceId,
              occurredAt: now,
              payload: {
                ...snapshot.providerThread,
                lastRunOrdinal:
                  rollbackTarget.appRunOrdinal === 0 ? null : rollbackTarget.appRunOrdinal,
                updatedAt: now,
              },
            }),
          );
          for (const staleCheckpoint of staleCheckpoints) {
            events.push(
              yield* makeEvent({
                type: "checkpoint.captured",
                threadId: input.threadId,
                ...(staleCheckpoint.runId === null ? {} : { runId: staleCheckpoint.runId }),
                nodeId: staleCheckpoint.nodeId,
                providerInstanceId: providerThread.providerInstanceId,
                occurredAt: now,
                payload: { ...staleCheckpoint, status: "stale" },
              }),
            );
          }
          for (const run of runsToRollback) {
            const rootNode = projection.nodes.find((candidate) => candidate.id === run.rootNodeId);
            events.push(
              yield* makeEvent({
                type: "run.updated",
                threadId: input.threadId,
                runId: run.id,
                ...(rootNode === undefined ? {} : { nodeId: rootNode.id }),
                providerInstanceId: run.providerInstanceId,
                occurredAt: now,
                payload: { ...run, status: "rolled_back", completedAt: now },
              }),
            );
            if (rootNode !== undefined) {
              events.push(
                yield* makeEvent({
                  type: "node.updated",
                  threadId: input.threadId,
                  runId: run.id,
                  nodeId: rootNode.id,
                  providerInstanceId: run.providerInstanceId,
                  occurredAt: now,
                  payload: { ...rootNode, status: "rolled_back", completedAt: now },
                }),
              );
            }
          }
          // Handoffs this provider thread received in discarded turns left its
          // conversation with them. One that still holds kept history goes out
          // again with its next turn, without the discarded runs.
          const discardedRunIds = new Set(runsToRollback.map((run) => run.id));
          for (const handoff of projection.contextHandoffs) {
            if (
              handoff.status !== "ready" ||
              handoff.toProviderThreadId !== providerThread.id ||
              !discardedRunIds.has(handoff.targetRunId)
            ) {
              continue;
            }
            events.push(
              yield* makeEvent({
                type: "context-handoff.updated",
                threadId: input.threadId,
                providerInstanceId: providerThread.providerInstanceId,
                occurredAt: now,
                payload: {
                  ...handoffWithoutDiscardedRuns(handoff, discardedRunIds),
                  updatedAt: now,
                },
              }),
            );
          }
          if (snapshot.rollbackSummary !== undefined) {
            const item: OrchestrationV2TurnItem = {
              id: TurnItemId.make(`turn-item:rollback-summary:${input.requestId}`),
              threadId: input.threadId,
              runId: null,
              nodeId: null,
              providerThreadId: providerThread.id,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              // The event sink places it after the run it was rewound to.
              ordinal: 0,
              status: "completed",
              title: null,
              startedAt: now,
              completedAt: now,
              updatedAt: now,
              type: "rollback_summary",
              rollbackRequestId: input.requestId,
              rewoundToRunId: targetRun?.id ?? null,
              summary: snapshot.rollbackSummary,
            };
            events.push(
              yield* makeEvent({
                type: "turn-item.updated",
                threadId: input.threadId,
                providerInstanceId: providerThread.providerInstanceId,
                occurredAt: now,
                payload: item,
              }),
            );
          }
          // Re-read under the lock: the head move can take minutes, and the
          // thread may have changed meanwhile.
          const { thread } = yield* projections.getThreadRecords(input.threadId, []);
          if (thread.pendingHeadMove?.requestId === input.requestId) {
            events.push(
              yield* makeEvent({
                type: "thread.metadata-updated",
                threadId: input.threadId,
                providerInstanceId: thread.providerInstanceId,
                occurredAt: now,
                payload: { ...thread, pendingHeadMove: null, updatedAt: now },
              }),
            );
          }
          yield* eventSink.write({ events });
        }),
      );
    });

    return CheckpointRollbackServiceV2.of({
      execute: (input) =>
        Effect.gen(function* () {
          // Storage cleanup removes worktrees under the same lease.
          const { thread } = yield* projections.getThreadRecords(input.threadId, []);
          yield* thread.worktreePath === null
            ? execute(input)
            : withWorkspaceLease(path.resolve(thread.worktreePath), execute(input));
        }).pipe(
          Effect.mapError((cause) =>
            isCheckpointRollbackExecutionError(cause)
              ? cause
              : new CheckpointRollbackExecutionError({
                  reason: "unexpected-failure",
                  threadId: input.threadId,
                  providerThreadId: input.providerThreadId,
                  checkpointId: input.checkpointId,
                  cause,
                }),
          ),
        ),
    });
  }),
);

/**
 * A handoff whose carrying turn a rollback discarded: undelivered, so the next
 * turn sends it again, minus the discarded runs. One that held only discarded
 * runs is superseded.
 */
function handoffWithoutDiscardedRuns(
  handoff: OrchestrationV2ContextHandoff,
  discardedRunIds: ReadonlySet<RunId>,
): OrchestrationV2ContextHandoff {
  const { delivery: _delivery, ...undelivered } = handoff;
  if (handoff.history === undefined) return undelivered;
  const messages = handoff.history.messages.filter(
    (message) => message.runId === null || !discardedRunIds.has(message.runId),
  );
  if (messages.length === 0 && handoff.history.messages.length > 0) {
    return { ...handoff, status: "superseded" };
  }
  return { ...undelivered, history: { ...handoff.history, messages } };
}
