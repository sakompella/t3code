import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it, vi } from "@effect/vitest";
import {
  CheckpointId,
  CheckpointRef,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2DomainEvent,
  ProviderDriverKind,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  CheckpointScopeId,
  CommandId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as SqlClient from "effect/sql/SqlClient";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Layer from "effect/Layer";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import { withWorkspaceLease } from "../workspace/workspaceLease.ts";
import { ProviderWorkspaceMissingError } from "../provider/Errors.ts";
import { resolveCodexRollbackTurnCount } from "./Adapters/CodexAdapterV2.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as CheckpointRollbackService from "./CheckpointRollbackService.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";

const unrelatedProject = Option.some({
  workspaceRoot: "/nonexistent/t3-rollback-project",
} as never);
const layerCheckpointRollbackService = CheckpointRollbackService.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      NodeServices.layer,
      Layer.mock(ServerConfig.ServerConfig)({ worktreesDir: "/synthetic-worktrees" } as never),
      ServerSettings.layerTest(),
      Layer.mock(GitWorkflow.GitWorkflowService)({}),
      Layer.mock(ProjectStore.ProjectStoreV2)({ get: () => Effect.succeed(unrelatedProject) }),
    ),
  ),
);
const requestId = CommandId.make("rollback-request");

it.effect.each(["initial", "waiting"])(
  "rejects a non-ready checkpoint from %s before opening a session or restoring files",
  (state) => {
    const threadId = ThreadId.make("thread:rollback-non-ready");
    const providerThreadId = ProviderThreadId.make("provider-thread:rollback-non-ready");
    const providerSessionId = ProviderSessionId.make("provider-session:rollback-non-ready");
    const checkpointId = CheckpointId.make("checkpoint:rollback-non-ready");
    const scopeId = CheckpointScopeId.make("checkpoint-scope:rollback-non-ready");
    const providerInstanceId = ProviderInstanceId.make("provider_rollback_non_ready");
    const restore = vi.fn(() => Effect.die("checkpoint restore must not run"));
    const open = vi.fn(() => Effect.die("provider session open must not run"));
    const resolveRuntimePolicy = vi.fn(() => Effect.die("runtime policy resolution must not run"));
    const readEntered = Deferred.makeUnsafe<void>();
    const release = Deferred.makeUnsafe<void>();
    let projection = {
      thread: {
        worktreePath: process.cwd(),
        activeProviderThreadId: providerThreadId,
        modelSelection: { instanceId: providerInstanceId, model: "test-model" },
      },
      providerThread: { id: providerThreadId, providerSessionId, providerInstanceId },
      checkpoint: {
        id: checkpointId,
        scopeId,
        status: state === "waiting" ? "ready" : "stale",
        appRunOrdinal: 0,
      },
      scope: { id: scopeId, cwd: process.cwd() },
    } as unknown as ProjectionStore.ProjectionCheckpointRollbackContext;
    const getThread = vi.fn<ProjectionStore.ProjectionStoreV2Shape["getThread"]>(() =>
      Deferred.succeed(readEntered, undefined).pipe(Effect.as(projection.thread)),
    );
    const getCheckpointRollbackContext = vi.fn(() => Effect.succeed(projection));
    const testLayer = layerCheckpointRollbackService.pipe(
      Layer.provide(
        Layer.mergeAll(
          ThreadCommandExecutor.layer,
          Layer.mock(CheckpointService.CheckpointServiceV2)({ restore }),
          Layer.mock(EventSink.EventSinkV2)({}),
          IdAllocator.layer,
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getThread,
            getCheckpointRollbackContext,
            getShellSnapshot: () =>
              Effect.succeed({
                schemaVersion: 1,
                snapshotSequence: 0,
                threads: [],
                archivedThreads: [],
              }),
          }),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({ open }),
          Layer.mock(RuntimePolicy.RuntimePolicyV2)({ resolve: resolveRuntimePolicy }),
        ),
      ),
    );

    return Effect.gen(function* () {
      const service = yield* CheckpointRollbackService.CheckpointRollbackServiceV2;
      const holder =
        state === "waiting"
          ? yield* withWorkspaceLease(process.cwd(), Deferred.await(release)).pipe(
              Effect.forkChild({ startImmediately: true }),
            )
          : undefined;
      const rollback = yield* service
        .execute({
          threadId,
          requestId,
          providerThreadId,
          checkpointId,
          scopeId,
        })
        .pipe(Effect.flip, Effect.forkChild({ startImmediately: true }));
      if (state === "waiting") {
        yield* Deferred.await(readEntered);
        projection = {
          ...projection,
          checkpoint: { ...projection.checkpoint!, status: "stale" },
        };
        yield* Deferred.succeed(release, undefined);
        if (holder !== undefined) yield* Fiber.join(holder);
      }
      const error = yield* Fiber.join(rollback);

      assert.deepEqual(getThread.mock.calls, [[threadId]]);
      assert.deepEqual(getCheckpointRollbackContext.mock.calls, [
        [
          threadId,
          {
            threadId,
            requestId,
            providerThreadId,
            checkpointId,
            scopeId,
          },
        ],
      ]);
      assert.equal(error.reason, "rollback-target-invalid");
      assert.equal(
        error.message,
        `Rollback target ${checkpointId} for provider thread ${providerThreadId} on thread ${threadId} is incomplete or invalid.`,
      );
      assert.equal(error.cause, undefined);
      assert.equal(resolveRuntimePolicy.mock.calls.length, 0);
      assert.equal(open.mock.calls.length, 0);
      assert.equal(restore.mock.calls.length, 0);
    }).pipe(Effect.provide(testLayer));
  },
);

it.effect.each([
  { order: "cleanup-first", restoreFiles: true },
  { order: "rollback-first", restoreFiles: true },
  { order: "cleanup-first", restoreFiles: false, unmanaged: true },
])("locks rollback with %s", ({ order, restoreFiles, unmanaged = false }) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const workspaceRoot = yield* fileSystem.realPath(
      yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-rollback-" }),
    );
    const worktreesDir = path.join(workspaceRoot, "worktrees");
    const worktreePath = unmanaged
      ? path.join(workspaceRoot, "elsewhere", "feature")
      : path.join(worktreesDir, "repo", "feature");
    yield* fileSystem.makeDirectory(worktreePath, { recursive: true });
    const threadId = ThreadId.make("rollback-worktree");
    const projectId = ProjectId.make("rollback-worktree-project");
    const providerThreadId = ProviderThreadId.make("rollback-worktree-provider-thread");
    const providerSessionId = ProviderSessionId.make("rollback-worktree-provider-session");
    const providerInstanceId = ProviderInstanceId.make("rollback-worktree-provider-instance");
    const checkpointId = CheckpointId.make("rollback-worktree-checkpoint");
    const scopeId = CheckpointScopeId.make("rollback-worktree-scope");
    const branch = "feature/rollback";
    const release = Deferred.makeUnsafe<void>();
    const openEntered = Deferred.makeUnsafe<void>();
    const calls: string[] = [];
    let cleanupEntered = false;
    const providerThread = { id: providerThreadId, providerSessionId, providerInstanceId };
    const projection = {
      thread: {
        id: threadId,
        projectId,
        worktreePath,
        branch,
        activeProviderThreadId: providerThreadId,
        modelSelection: { instanceId: providerInstanceId, model: "test-model" },
      },
      providerThread,
      providerSession: undefined,
      checkpoint: { id: checkpointId, scopeId, status: "ready", appRunOrdinal: 0 },
      scope: { id: scopeId, cwd: worktreePath },
      runsToRollback: [],
      targetAttempt: undefined,
      outsideConversationAttemptIds: [],
      keptRuns: [],
      otherProviderThreads: [],
      discardedHandoffs: [],
      staleCheckpoints: [],
      rootNodes: [],
      providerTurns: [],
    } as unknown as ProjectionStore.ProjectionCheckpointRollbackContext;
    const testLayer = CheckpointRollbackService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          ThreadCommandExecutor.layer,
          NodeServices.layer,
          Layer.mock(ServerConfig.ServerConfig)({ worktreesDir } as never),
          ServerSettings.layerTest(),
          Layer.mock(ProjectStore.ProjectStoreV2)({
            get: (id) => {
              assert.equal(id, projectId);
              return Effect.succeedSome({ workspaceRoot } as never);
            },
          }),
          Layer.mock(GitWorkflow.GitWorkflowService)({
            pruneWorktrees: (input) =>
              Effect.sync(() => {
                assert.deepEqual(input, { cwd: workspaceRoot });
                calls.push("prune");
              }),
            createWorktree: (input) =>
              Effect.gen(function* () {
                assert.deepEqual(input, {
                  cwd: workspaceRoot,
                  refName: branch,
                  path: worktreePath,
                });
                calls.push("create");
                yield* fileSystem.makeDirectory(worktreePath).pipe(Effect.orDie);
                return {} as never;
              }),
          }),
          Layer.mock(CheckpointService.CheckpointServiceV2)({
            restore: () =>
              Effect.gen(function* () {
                assert.isTrue(yield* fileSystem.exists(worktreePath).pipe(Effect.orDie));
                calls.push("files");
              }),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            write: () =>
              Effect.sync(() => {
                calls.push("projection");
                return [];
              }),
          }),
          IdAllocator.layer,
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getThread: () => Effect.succeed(projection.thread),
            getCheckpointRollbackContext: () => Effect.succeed(projection),
            getShellSnapshot: () =>
              Effect.succeed({
                schemaVersion: 1,
                snapshotSequence: 0,
                threads: [],
                archivedThreads: [],
              }),
          }),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
            open: (input) =>
              Effect.gen(function* () {
                assert.equal(input.runtimePolicy.cwd, worktreePath);
                if (!(yield* fileSystem.exists(worktreePath).pipe(Effect.orDie))) {
                  return yield* new ProviderWorkspaceMissingError({ threadId, cwd: worktreePath });
                }
                calls.push("session");
                yield* Deferred.succeed(openEntered, undefined);
                if (order === "rollback-first") yield* Deferred.await(release);
                return {} as never;
              }),
          }),
          Layer.mock(RuntimePolicy.RuntimePolicyV2)({
            resolve: ({ thread }) => {
              assert.equal(thread.worktreePath, worktreePath);
              return Effect.succeed({ cwd: thread.worktreePath } as never);
            },
          }),
        ),
      ),
    );
    yield* Effect.gen(function* () {
      const service = yield* CheckpointRollbackService.CheckpointRollbackServiceV2;
      const cleanupEffect = withWorkspaceLease(
        worktreePath,
        Effect.gen(function* () {
          cleanupEntered = true;
          if (order === "cleanup-first") yield* Deferred.await(release);
          yield* fileSystem.remove(worktreePath, { recursive: true, force: true });
          calls.push("cleanup");
        }),
      );
      const cleanup =
        order === "cleanup-first"
          ? yield* cleanupEffect.pipe(Effect.forkChild({ startImmediately: true }))
          : undefined;
      const rollback = yield* service
        .execute({ threadId, requestId, providerThreadId, checkpointId, scopeId, restoreFiles })
        .pipe(Effect.forkChild({ startImmediately: true }));
      if (order === "cleanup-first") assert.deepEqual(calls, []);
      if (order === "rollback-first") yield* Deferred.await(openEntered);
      const laterCleanup =
        order === "rollback-first"
          ? yield* cleanupEffect.pipe(Effect.forkChild({ startImmediately: true }))
          : undefined;
      if (order === "rollback-first") assert.isFalse(cleanupEntered);
      yield* Deferred.succeed(release, undefined);
      if (cleanup !== undefined) yield* Fiber.join(cleanup);
      if (unmanaged) {
        const error = yield* Fiber.join(rollback).pipe(Effect.flip);
        assert.equal(error._tag, "CheckpointRollbackExecutionError");
        assert.deepEqual(calls, ["cleanup"]);
        return;
      }
      yield* Fiber.join(rollback);
      if (laterCleanup !== undefined) yield* Fiber.join(laterCleanup);
      const rollbackCalls = restoreFiles
        ? ["session", "files", "projection"]
        : ["session", "projection"];
      assert.deepEqual(
        calls,
        order === "cleanup-first"
          ? ["cleanup", "prune", "create", ...rollbackCalls]
          : [...rollbackCalls, "cleanup"],
      );
    }).pipe(Effect.provide(testLayer));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

/**
 * Runs 1..n alternate provider threads as `providers` lists them; `p` is the
 * active one. The handoff `p` received with run `carrierOrdinal` holds one
 * message from each run in `handoffRuns`. Runs in `rolledBack` were rolled
 * back earlier.
 */
const rollbackAcrossProviders = (input: {
  readonly providers: ReadonlyArray<"c" | "p">;
  readonly targetOrdinal: number;
  readonly carrierOrdinal: number;
  readonly handoffRuns: ReadonlyArray<number>;
  readonly rolledBack?: ReadonlyArray<number>;
  readonly firstRunOrdinal?: number;
  readonly rollbackSummary?: string;
  readonly pendingHeadMoveRequestId?: CommandId;
}) => {
  const threadId = ThreadId.make("rollback-across-providers");
  const providerSessionId = ProviderSessionId.make("rollback-across-providers-session");
  const instanceId = ProviderInstanceId.make("rollback-across-providers-instance");
  const checkpointId = CheckpointId.make("rollback-across-providers-checkpoint");
  const scopeId = CheckpointScopeId.make("rollback-across-providers-scope");
  const activeProviderThread = {
    id: ProviderThreadId.make("p"),
    providerSessionId,
    providerInstanceId: instanceId,
    firstRunOrdinal: input.firstRunOrdinal ?? input.providers.indexOf("p") + 1,
  };
  const ordinals = input.providers.map((_, index) => index + 1);
  const providerTurns = ordinals.map((ordinal) => ({
    id: `turn-${ordinal}`,
    providerThreadId: input.providers[ordinal - 1],
    runAttemptId: `attempt-${ordinal}`,
    ordinal: input.providers.slice(0, ordinal).filter((p) => p === input.providers[ordinal - 1])
      .length,
    status: "completed",
  }));
  const handoff = {
    id: "handoff",
    threadId,
    targetRunId: `run-${input.carrierOrdinal}`,
    toProviderThreadId: activeProviderThread.id,
    status: "ready",
    history: {
      messages: input.handoffRuns.map((ordinal) => ({ runId: `run-${ordinal}` })),
      coverage: "",
      omittedItems: 0,
    },
    delivery: { nativeThreadId: "native", status: "inline", itemIds: [] },
  };
  const runs = ordinals.map((ordinal) => ({
    id: `run-${ordinal}`,
    ordinal,
    providerThreadId: input.providers[ordinal - 1],
    status: input.rolledBack?.includes(ordinal) ? "rolled_back" : "completed",
    rootNodeId: null,
    activeAttemptId: `attempt-${ordinal}`,
  }));
  const targetAttemptId = `attempt-${input.targetOrdinal}`;
  const runsToRollback = runs.filter(
    (run) => run.ordinal > input.targetOrdinal && run.status !== "rolled_back",
  );
  // What getCheckpointRollbackContext reads for this history.
  const context = {
    thread: {
      worktreePath: process.cwd(),
      activeProviderThreadId: activeProviderThread.id,
      modelSelection: { instanceId, model: "test" },
      ...(input.pendingHeadMoveRequestId === undefined
        ? {}
        : { pendingHeadMove: { requestId: input.pendingHeadMoveRequestId, summarize: true } }),
    },
    providerThread: activeProviderThread,
    providerSession: undefined,
    checkpoint: { id: checkpointId, scopeId, status: "ready", appRunOrdinal: input.targetOrdinal },
    scope: { id: scopeId, cwd: process.cwd() },
    targetAttempt:
      input.targetOrdinal === 0 ? undefined : { id: targetAttemptId, providerTurnId: null },
    providerTurns: providerTurns.filter(
      (turn) =>
        turn.providerThreadId === activeProviderThread.id || turn.runAttemptId === targetAttemptId,
    ),
    outsideConversationAttemptIds: runs
      .filter(
        (run) => run.status === "rolled_back" || run.ordinal < activeProviderThread.firstRunOrdinal,
      )
      .map((run) => run.activeAttemptId),
    keptRuns: runs
      .filter((run) => run.ordinal <= input.targetOrdinal && run.status !== "rolled_back")
      .map((run) => ({
        id: run.id,
        ordinal: run.ordinal,
        providerThreadId: run.providerThreadId,
        activeAttemptId: run.activeAttemptId,
        activeProviderTurnId: null,
      })),
    runsToRollback,
    otherProviderThreads: [],
    discardedHandoffs: runsToRollback.some((run) => run.id === handoff.targetRunId)
      ? [handoff]
      : [],
    rootNodes: [],
    staleCheckpoints: [],
  } as unknown as ProjectionStore.ProjectionCheckpointRollbackContext;
  const rollbacks: Array<ProviderAdapter.ProviderAdapterV2RollbackThreadInput> = [];
  const written: Array<OrchestrationV2DomainEvent> = [];
  const testLayer = layerCheckpointRollbackService.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.effect(ThreadCommandExecutor.ThreadCommandExecutor, KeyedLock.make<ThreadId>()),
        Layer.mock(CheckpointService.CheckpointServiceV2)({}),
        Layer.mock(EventSink.EventSinkV2)({
          write: ({ events }) =>
            Effect.sync(() => {
              written.push(...events);
              return [];
            }),
        }),
        IdAllocator.layer,
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThread: () => Effect.succeed(context.thread),
          getCheckpointRollbackContext: () => Effect.succeed(context),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          open: () =>
            Effect.succeed({
              rollbackThread: (rollback: ProviderAdapter.ProviderAdapterV2RollbackThreadInput) =>
                Effect.sync(() => {
                  rollbacks.push(rollback);
                  return {
                    providerThread: activeProviderThread,
                    ...(input.rollbackSummary === undefined
                      ? {}
                      : { rollbackSummary: input.rollbackSummary }),
                  };
                }),
            } as never),
        }),
        Layer.mock(RuntimePolicy.RuntimePolicyV2)({ resolve: () => Effect.succeed({} as never) }),
      ),
    ),
  );
  return Effect.gen(function* () {
    const service = yield* CheckpointRollbackService.CheckpointRollbackServiceV2;
    yield* service.execute({
      threadId,
      requestId,
      providerThreadId: activeProviderThread.id,
      checkpointId,
      scopeId,
      restoreFiles: false,
    });
    const handoffUpdate = written.find((event) => event.type === "context-handoff.updated");
    const threadUpdate = written.find((event) => event.type === "provider-thread.updated");
    return {
      rollback: rollbacks[0]!,
      lastRunOrdinal:
        threadUpdate?.type === "provider-thread.updated"
          ? threadUpdate.payload.lastRunOrdinal
          : undefined,
      rolledBackRunIds: written.flatMap((event) =>
        event.type === "run.updated" && event.payload.status === "rolled_back"
          ? [event.payload.id]
          : [],
      ),
      handoff:
        handoffUpdate?.type === "context-handoff.updated" ? handoffUpdate.payload : undefined,
      written,
    };
  }).pipe(Effect.provide(testLayer));
};

it.effect(
  "rewinds the active provider thread to its start for a target of an earlier provider",
  () =>
    Effect.gen(function* () {
      // Two runs on one provider, then a switch: the handoff covered both.
      const result = yield* rollbackAcrossProviders({
        providers: ["c", "c", "p"],
        targetOrdinal: 1,
        carrierOrdinal: 3,
        handoffRuns: [1, 2],
      });
      assert.equal(result.rollback.target.type, "thread_start");
      assert.deepEqual(
        result.rollback.providerThreadTurns.map((turn) => String(turn.id)),
        ["turn-3"],
      );
      assert.isNull(result.lastRunOrdinal);
      assert.deepEqual(result.rolledBackRunIds, ["run-2", "run-3"]);
      // The next turn sends the handoff again, without the discarded run.
      assert.equal(result.handoff?.status, "ready");
      assert.isUndefined(result.handoff?.delivery);
      assert.deepEqual(
        result.handoff?.history?.messages.map((message) => String(message.runId)),
        ["run-1"],
      );
    }),
);

it.effect.each([
  { targetOrdinal: 2, handoffStatus: "ready", delivered: false },
  { targetOrdinal: 1, handoffStatus: "superseded", delivered: true },
])(
  "rewinds a provider thread that ran before and after another one to its last turn at or before %s",
  ({ targetOrdinal, handoffStatus, delivered }) =>
    Effect.gen(function* () {
      // Back on `p` after a run on `c`: the handoff with run 3 covered run 2.
      const result = yield* rollbackAcrossProviders({
        providers: ["p", "c", "p"],
        targetOrdinal,
        carrierOrdinal: 3,
        handoffRuns: [2],
      });
      assert.equal(result.rollback.target.type, "provider_turn");
      assert.equal(
        result.rollback.target.type === "provider_turn"
          ? result.rollback.target.providerTurn.id
          : undefined,
        "turn-1",
      );
      assert.equal(result.lastRunOrdinal, 1);
      assert.equal(result.handoff?.status, handoffStatus);
      assert.equal(result.handoff?.delivery !== undefined, delivered);
    }),
);

it.effect(
  "ignores turns of a native conversation an earlier rollback dropped from the active provider thread",
  () =>
    Effect.gen(function* () {
      // `p` ran 1 and 2, `c` ran 3, a rollback to 1 dropped `p`'s native
      // conversation, and run 4 rebuilt it from a full handoff of run 1.
      const result = yield* rollbackAcrossProviders({
        providers: ["p", "p", "c", "p"],
        rolledBack: [2, 3],
        firstRunOrdinal: 4,
        targetOrdinal: 1,
        carrierOrdinal: 4,
        handoffRuns: [1],
      });
      assert.equal(result.rollback.target.type, "thread_start");
      assert.deepEqual(
        result.rollback.providerThreadTurns.map((turn) => String(turn.id)),
        ["turn-4"],
      );
      assert.deepEqual(result.rolledBackRunIds, ["run-4"]);
      assert.equal(result.handoff?.status, "ready");
      assert.isUndefined(result.handoff?.delivery);
    }),
);

it.effect.each([
  { pendingRequest: requestId, cleared: true },
  { pendingRequest: CommandId.make("newer-rollback"), cleared: false },
])(
  "records the provider's summary after the kept run and ends the head move it belongs to (%s)",
  ({ pendingRequest, cleared }) =>
    Effect.gen(function* () {
      const result = yield* rollbackAcrossProviders({
        providers: ["p", "p", "p"],
        targetOrdinal: 1,
        carrierOrdinal: 3,
        handoffRuns: [],
        rollbackSummary: "Kept: the secret code.",
        pendingHeadMoveRequestId: pendingRequest,
      });
      const summaries = result.written.flatMap((event) =>
        event.type === "turn-item.updated" && event.payload.type === "rollback_summary"
          ? [event.payload]
          : [],
      );
      assert.deepEqual(
        summaries.map((item) => ({
          rollbackRequestId: item.rollbackRequestId,
          rewoundToRunId: item.rewoundToRunId,
          runId: item.runId,
          summary: item.summary,
        })),
        [
          {
            rollbackRequestId: requestId,
            rewoundToRunId: RunId.make("run-1"),
            runId: null,
            summary: "Kept: the secret code.",
          },
        ],
      );
      const headMoveEnds = result.written.filter(
        (event) =>
          event.type === "thread.metadata-updated" && event.payload.pendingHeadMove === null,
      );
      assert.equal(headMoveEnds.length, cleared ? 1 : 0);
      if (cleared) assert.equal(result.written.at(-1), headMoveEnds[0]);
    }),
);

it.effect("records no summary row when the provider left none", () =>
  Effect.gen(function* () {
    const result = yield* rollbackAcrossProviders({
      providers: ["p", "p"],
      targetOrdinal: 1,
      carrierOrdinal: 2,
      handoffRuns: [],
    });
    assert.isFalse(result.written.some((event) => event.type === "turn-item.updated"));
  }),
);

const seedRollbackHistory = Effect.gen(function* () {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const now = yield* DateTime.now;
  const threadId = ThreadId.make("rollback-history");
  const providerThreadId = ProviderThreadId.make("rollback-history-provider");
  const providerSessionId = ProviderSessionId.make("rollback-history-session");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const driver = ProviderDriverKind.make("codex");
  const modelSelection = { instanceId: providerInstanceId, model: "test" };
  const scopeId = CheckpointScopeId.make("rollback-history-root");
  const otherScopeId = CheckpointScopeId.make("rollback-history-child");
  const runId = (ordinal: number) => RunId.make(`rollback-history-run-${ordinal}`);
  const nodeId = (ordinal: number) => NodeId.make(`rollback-history-node-${ordinal}`);
  const attemptId = (ordinal: number) => RunAttemptId.make(`rollback-history-attempt-${ordinal}`);
  const turnId = (ordinal: number) => ProviderTurnId.make(`rollback-history-turn-${ordinal}`);
  const checkpointId = (ordinal: number) =>
    CheckpointId.make(`rollback-history-checkpoint-${ordinal}`);
  type EventInput<Event = OrchestrationV2DomainEvent> = Event extends OrchestrationV2DomainEvent
    ? Omit<Event, "id" | "occurredAt" | "threadId">
    : never;
  let sequence = 0;
  const apply = (event: EventInput) =>
    store.apply({
      ...event,
      id: EventId.make(`rollback-history-event-${sequence++}`),
      threadId,
      occurredAt: now,
    } as OrchestrationV2DomainEvent);
  yield* apply({
    type: "thread.created",
    payload: {
      id: threadId,
      createdBy: "user",
      creationSource: "web",
      projectId: ProjectId.make("rollback-history-project"),
      title: "Rollback history",
      providerInstanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: process.cwd(),
      activeProviderThreadId: providerThreadId,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
  });
  const providerThread = {
    id: providerThreadId,
    driver,
    providerInstanceId,
    providerSessionId,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: null,
    nativeConversationHeadRef: null,
    status: "idle" as const,
    firstRunOrdinal: 1,
    lastRunOrdinal: 6,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  yield* apply({ type: "provider-thread.updated", payload: providerThread });
  for (const id of [scopeId, otherScopeId]) {
    yield* apply({
      type: "checkpoint-scope.created",
      payload: {
        id,
        threadId,
        runId: runId(1),
        nodeId: nodeId(1),
        parentScopeId: id === scopeId ? null : scopeId,
        providerThreadId,
        kind: id === scopeId ? "root_run" : "subagent",
        ordinalWithinParent: 0,
        advancesAppRunCount: id === scopeId,
        cwd: process.cwd(),
        createdAt: now,
      },
    });
  }
  const statuses = [
    "completed",
    "completed",
    "completed",
    "interrupted",
    "failed",
    "cancelled",
    "rolled_back",
  ] as const;
  for (const [index, status] of statuses.entries()) {
    const ordinal = index + 1;
    yield* apply({
      type: "run.created",
      payload: {
        id: runId(ordinal),
        threadId,
        ordinal,
        providerInstanceId,
        modelSelection,
        providerThreadId,
        userMessageId: MessageId.make(`rollback-history-message-${ordinal}`),
        rootNodeId: nodeId(ordinal),
        activeAttemptId: attemptId(ordinal),
        status,
        requestedAt: now,
        startedAt: now,
        completedAt: now,
        checkpointId: checkpointId(ordinal),
        contextHandoffId: null,
      },
    });
    yield* apply({
      type: "node.updated",
      payload: {
        id: nodeId(ordinal),
        threadId,
        runId: runId(ordinal),
        parentNodeId: null,
        rootNodeId: nodeId(ordinal),
        kind: "root_turn",
        status,
        countsForRun: true,
        providerThreadId,
        providerTurnId: turnId(ordinal),
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: scopeId,
        startedAt: now,
        completedAt: now,
      },
    });
    // Rollback must exclude every attempt of discarded runs, including an inactive retry.
    for (const attemptOrdinal of ordinal === 7 ? [6.5, 7] : [ordinal]) {
      yield* apply({
        type: "run-attempt.created",
        runId: runId(ordinal),
        payload: {
          id: attemptId(attemptOrdinal),
          runId: runId(ordinal),
          attemptOrdinal: attemptOrdinal === ordinal ? 2 : 1,
          rootNodeId: nodeId(ordinal),
          providerInstanceId,
          providerThreadId,
          providerTurnId: turnId(attemptOrdinal),
          reason: "initial",
          status: "completed",
          startedAt: now,
          completedAt: now,
        },
      });
      yield* apply({
        type: "provider-turn.updated",
        payload: {
          id: turnId(attemptOrdinal),
          providerThreadId,
          nodeId: nodeId(ordinal),
          runAttemptId: attemptId(attemptOrdinal),
          nativeTurnRef: { driver, nativeId: `native-turn-${attemptOrdinal}`, strength: "strong" },
          ordinal: attemptOrdinal * 2,
          status: status === "rolled_back" ? "completed" : status,
          startedAt: now,
          completedAt: now,
        },
      });
    }
  }
  // A continuation shares the target attempt; its first provider turn is no longer the boundary.
  yield* apply({
    type: "provider-turn.updated",
    payload: {
      id: turnId(2.5),
      providerThreadId,
      nodeId: nodeId(2),
      runAttemptId: attemptId(2),
      nativeTurnRef: { driver, nativeId: "native-turn-2-continuation", strength: "strong" },
      ordinal: 5,
      status: "completed",
      startedAt: now,
      completedAt: now,
    },
  });
  for (let ordinal = 0; ordinal <= 8; ordinal++) {
    yield* apply({
      type: "checkpoint.captured",
      payload: {
        id: checkpointId(ordinal),
        threadId,
        scopeId: ordinal === 8 ? otherScopeId : scopeId,
        runId: ordinal === 0 ? null : runId(Math.min(ordinal, 7)),
        nodeId: nodeId(Math.max(1, Math.min(ordinal, 7))),
        parentCheckpointId: null,
        ordinalWithinScope: ordinal,
        appRunOrdinal: ordinal === 0 ? null : ordinal,
        ref: CheckpointRef.make(`refs/t3/rollback-history/${ordinal}`),
        status: ordinal === 7 ? "stale" : "ready",
        files: [],
        capturedAt: now,
      },
    });
  }

  const rollbackCounts: number[] = [];
  const restored: CheckpointId[] = [];
  const deletedRefs: CheckpointId[] = [];
  const committed: OrchestrationV2DomainEvent[] = [];
  const serviceLayer = layerCheckpointRollbackService.pipe(
    Layer.provide(
      Layer.mergeAll(
        ThreadCommandExecutor.layer,
        IdAllocator.layer,
        Layer.mock(CheckpointService.CheckpointServiceV2)({
          restore: ({ checkpoint }) =>
            Effect.sync(() => {
              restored.push(checkpoint.id);
            }),
          deleteStaleRefs: ({ checkpoints }) =>
            Effect.sync(() => {
              deletedRefs.push(...checkpoints.map((checkpoint) => checkpoint.id));
            }),
        }),
        Layer.mock(EventSink.EventSinkV2)({
          write: ({ events }) =>
            Effect.gen(function* () {
              for (const event of events) yield* store.apply(event);
              committed.push(...events);
              return [];
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new EventSink.EventSinkWriteError({
                    eventCount: events.length,
                    cause,
                  }),
              ),
            ),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          open: () =>
            Effect.succeed({
              rollbackThread: (input: ProviderAdapter.ProviderAdapterV2RollbackThreadInput) =>
                Effect.gen(function* () {
                  rollbackCounts.push(yield* resolveCodexRollbackTurnCount(input));
                  return { providerThread: input.providerThread };
                }),
            } as never),
        }),
        Layer.mock(RuntimePolicy.RuntimePolicyV2)({ resolve: () => Effect.succeed({} as never) }),
      ),
    ),
  );
  const rollback = (ordinal: number) =>
    Effect.gen(function* () {
      const service = yield* CheckpointRollbackService.CheckpointRollbackServiceV2;
      yield* service.execute({
        threadId,
        requestId,
        providerThreadId,
        scopeId,
        checkpointId: checkpointId(ordinal),
      });
    }).pipe(Effect.provide(serviceLayer));

  return {
    store,
    now,
    threadId,
    runId,
    nodeId,
    checkpointId,
    rollback,
    rollbackCounts,
    restored,
    deletedRefs,
    committed,
  };
});

it.layer(ProjectionStore.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory)))(
  "rollback history reads",
  (it) => {
    it.effect("rolls back later terminal runs despite obsolete unrelated SQLite payloads", () =>
      Effect.gen(function* () {
        const history = yield* seedRollbackHistory;
        const sql = yield* SqlClient.SqlClient;
        const { threadId, runId, nodeId, checkpointId } = history;
        yield* sql`
          INSERT INTO orchestration_v2_projection_turn_items (
            turn_item_id, thread_id, run_id, node_id, provider_thread_id, provider_turn_id,
            parent_item_id, ordinal, type, status, updated_at, payload_json
          ) VALUES ('rollback-obsolete-transcript', ${threadId}, ${runId(1)}, ${nodeId(1)},
            NULL, NULL, NULL, 1, 'assistant_message', 'completed', ${DateTime.formatIso(history.now)},
            '{"obsolete":"transcript"}')`;
        yield* sql`UPDATE orchestration_v2_projection_runs
          SET payload_json = '{"obsolete":"run"}' WHERE run_id = ${runId(1)}`;
        yield* sql`UPDATE orchestration_v2_projection_nodes
          SET payload_json = '{"obsolete":"node"}' WHERE node_id = ${nodeId(1)}`;
        yield* sql`UPDATE orchestration_v2_projection_checkpoints
          SET payload_json = json_set(payload_json, '$.files', 'obsolete file summary')
          WHERE checkpoint_id IN (${checkpointId(0)}, ${checkpointId(1)}, ${checkpointId(7)}, ${checkpointId(8)})`;
        assert.equal(
          (yield* Effect.exit(history.store.getThreadProjection(threadId)))._tag,
          "Failure",
        );

        yield* history.rollback(2);

        assert.deepEqual(history.rollbackCounts, [4]);
        assert.deepEqual(history.restored, [checkpointId(2)]);
        assert.sameMembers(history.deletedRefs, [3, 4, 5, 6].map(checkpointId));
        const runs = yield* sql<{ ordinal: number; status: string }>`
          SELECT ordinal, status FROM orchestration_v2_projection_runs
          WHERE thread_id = ${threadId} ORDER BY ordinal`;
        assert.deepEqual(
          runs.map((run) => run.status),
          [
            "completed",
            "completed",
            "rolled_back",
            "rolled_back",
            "rolled_back",
            "rolled_back",
            "rolled_back",
          ],
        );
        const nodes = yield* sql<{ status: string }>`
          SELECT status FROM orchestration_v2_projection_nodes
          WHERE thread_id = ${threadId} ORDER BY node_id`;
        assert.deepEqual(
          nodes.map((node) => node.status),
          runs.map((run) => run.status),
        );
        const checkpoints = yield* sql<{ status: string }>`
          SELECT status FROM orchestration_v2_projection_checkpoints
          WHERE thread_id = ${threadId} ORDER BY ordinal_within_scope`;
        assert.deepEqual(
          checkpoints.map((checkpoint) => checkpoint.status),
          ["ready", "ready", "ready", "stale", "stale", "stale", "stale", "stale", "ready"],
        );
      }),
    );

    it.effect(
      "rewinds stopped runs repeatedly without counting discarded provider turns again",
      () =>
        Effect.gen(function* () {
          const history = yield* seedRollbackHistory;
          yield* history.rollback(5);
          yield* history.rollback(2);
          yield* history.rollback(0);
          yield* history.rollback(0);

          assert.deepEqual(history.rollbackCounts, [1, 3, 3]);
          assert.sameMembers(history.deletedRefs, [1, 2, 3, 4, 5, 6].map(history.checkpointId));
          assert.sameMembers(
            history.committed
              .filter((event) => event.type === "run.updated")
              .map((event) => event.payload.id),
            [1, 2, 3, 4, 5, 6].map(history.runId),
          );
          const records = yield* history.store.getThreadRecords(history.threadId, [
            "runs",
            "nodes",
            "checkpoints",
          ]);
          assert.isTrue(records.runs.every((run) => run.status === "rolled_back"));
          assert.isTrue(records.nodes.every((node) => node.status === "rolled_back"));
          assert.sameMembers(
            records.checkpoints
              .filter((checkpoint) => checkpoint.status === "ready")
              .map((checkpoint) => checkpoint.id),
            [history.checkpointId(0), history.checkpointId(8)],
          );
        }),
    );
  },
);
