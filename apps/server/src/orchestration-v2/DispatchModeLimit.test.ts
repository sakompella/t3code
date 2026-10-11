import { assert, it } from "@effect/vitest";
import {
  CommandId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { DispatchModeLimit } from "./DispatchModeLimit.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const instanceId = ProviderInstanceId.make("codex");
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for metadata commands"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const layerDatabase = SqlitePersistence.layerMemory;
const layerTest = Layer.mergeAll(
  layerDatabase,
  ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "dispatch-mode-limit" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: layerDatabase, runEffectWorker: false },
  ),
);

const supervised = { runtimeMode: "approval-required", interactionMode: "default" } as const;

const createThread = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create:${threadId}`),
      threadId,
      projectId: ProjectId.make("project:dispatch-mode-limit"),
      title: "Before",
      modelSelection: { instanceId, model: "gpt-5" },
      ...supervised,
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
  });

it.layer(layerTest)("DispatchModeLimit", (it) => {
  it.effect("refuses a limited command on a thread its user raised, and records nothing", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const threadId = ThreadId.make("thread:limit-raised");
      yield* createThread(threadId);
      yield* orchestrator.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("raise"),
        threadId,
        runtimeMode: "full-access",
      });
      const rename = {
        type: "thread.metadata.update",
        commandId: CommandId.make("rename"),
        threadId,
        title: "After",
      } as const;
      const refused = yield* orchestrator
        .dispatch(rename)
        .pipe(Effect.provideService(DispatchModeLimit, supervised), Effect.flip);
      assert.equal(refused._tag, "OrchestratorThreadAboveModeLimitError");
      assert.equal((yield* projections.getThreadShell(threadId))?.title, "Before");

      yield* orchestrator.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("lower"),
        threadId,
        runtimeMode: "approval-required",
      });
      yield* orchestrator
        .dispatch(rename)
        .pipe(Effect.provideService(DispatchModeLimit, supervised));
      assert.equal((yield* projections.getThreadShell(threadId))?.title, "After");
    }),
  );

  it.effect("names the interaction mode when that is what was raised", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("thread:limit-plan");
      yield* createThread(threadId);
      const refused = yield* orchestrator
        .dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("rename-plan"),
          threadId,
          title: "After",
        })
        .pipe(
          Effect.provideService(DispatchModeLimit, {
            runtimeMode: "full-access",
            interactionMode: "plan",
          }),
          Effect.flip,
        );
      assert.ok(refused._tag === "OrchestratorThreadAboveModeLimitError");
      assert.equal(refused.mode, "interaction");
    }),
  );

  it.effect("refuses to fork a source thread its user raised", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sourceThreadId = ThreadId.make("thread:limit-fork-source");
      const targetThreadId = ThreadId.make("thread:limit-fork-target");
      yield* createThread(sourceThreadId);
      yield* orchestrator.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("raise-source"),
        threadId: sourceThreadId,
        runtimeMode: "full-access",
      });
      const fork = {
        type: "thread.fork",
        commandId: CommandId.make("fork"),
        sourceThreadId,
        targetThreadId,
        sourcePoint: { type: "latest_stable" },
        createdBy: "agent",
        creationSource: "mcp",
      } as const;
      const refused = yield* orchestrator
        .dispatch(fork)
        .pipe(Effect.provideService(DispatchModeLimit, supervised), Effect.flip);
      assert.ok(refused._tag === "OrchestratorThreadAboveModeLimitError");
      assert.equal(refused.threadId, sourceThreadId);
      assert.isNull(yield* projections.getThreadShell(targetThreadId));

      yield* orchestrator.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("lower-source"),
        threadId: sourceThreadId,
        runtimeMode: "approval-required",
      });
      const retried = yield* orchestrator
        .dispatch(fork)
        .pipe(Effect.provideService(DispatchModeLimit, supervised), Effect.flip);
      assert.equal(retried._tag, "OrchestratorDispatchError");
    }),
  );
});

// Like the Prime Agent 0.10 fork: the provider offers only Full access, so it
// runs a thread stored Supervised in full access.
const layerFullAccessOnly = Layer.mergeAll(
  layerDatabase,
  ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "dispatch-mode-limit-full-access-only" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    {
      databaseLayer: layerDatabase,
      runEffectWorker: false,
      runtimePolicyLayer: RuntimePolicy.layerFromProjectStore.pipe(
        Layer.provide(
          Layer.mergeAll(
            ProjectStore.layer.pipe(Layer.provide(layerDatabase)),
            Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
              getInstance: () =>
                Effect.succeed({
                  snapshot: {
                    getSnapshot: Effect.succeed({ supportedRuntimeModes: ["full-access"] }),
                  },
                } as never),
            }),
          ),
        ),
      ),
    },
  ),
);

it.layer(layerFullAccessOnly)(
  "DispatchModeLimit on a provider that runs only Full access",
  (it) => {
    it.effect(
      "refuses a limited command on a thread stored Supervised that runs in full access",
      () =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const threadId = ThreadId.make("thread:limit-full-access-only");
          yield* createThread(threadId);
          const rename = {
            type: "thread.metadata.update",
            commandId: CommandId.make("rename-full-access-only"),
            threadId,
            title: "After",
          } as const;
          const refused = yield* orchestrator
            .dispatch(rename)
            .pipe(Effect.provideService(DispatchModeLimit, supervised), Effect.flip);
          assert.ok(refused._tag === "OrchestratorThreadAboveModeLimitError");
          assert.equal(refused.runtimeMode, "full-access");
          assert.equal((yield* projections.getThreadShell(threadId))?.title, "Before");
        }),
    );

    it.effect("lets a caller change its own thread, whatever mode that runs in", () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const threadId = ThreadId.make("thread:limit-own-thread");
        yield* createThread(threadId);
        yield* orchestrator
          .dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make("rename-own-thread"),
            threadId,
            title: "After",
          })
          .pipe(
            Effect.provideService(DispatchModeLimit, { ...supervised, callerThreadId: threadId }),
          );
        assert.equal((yield* projections.getThreadShell(threadId))?.title, "After");
      }),
    );

    it.effect("refuses a limited caller a new thread that would run in full access", () =>
      Effect.gen(function* () {
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const threadId = ThreadId.make("thread:limit-create-full-access-only");
        const refused = yield* createThread(threadId).pipe(
          Effect.provideService(DispatchModeLimit, supervised),
          Effect.flip,
        );
        assert.ok(refused._tag === "OrchestratorThreadAboveModeLimitError");
        assert.isNull(yield* projections.getThreadShell(threadId));
      }),
    );

    it.effect("lets a caller change its own thread's model within the modes it runs in", () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const threadId = ThreadId.make("thread:limit-own-model");
        yield* createThread(threadId);
        yield* orchestrator
          .dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("own-model"),
            threadId,
            modelSelection: { instanceId, model: "gpt-5-mini" },
          })
          .pipe(
            Effect.provideService(DispatchModeLimit, { ...supervised, callerThreadId: threadId }),
          );
        assert.equal(
          (yield* projections.getThreadShell(threadId))?.modelSelection.model,
          "gpt-5-mini",
        );
      }),
    );
  },
);

// One provider that offers every mode, and one that offers only Full access.
const forkInstanceId = ProviderInstanceId.make("primeAgent");
const layerMixedProviders = Layer.mergeAll(
  layerDatabase,
  ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "dispatch-mode-limit-mixed-providers" },
    ProviderAdapterRegistry.layerFromAdapters([
      adapter,
      { ...adapter, instanceId: forkInstanceId },
    ]),
    {
      databaseLayer: layerDatabase,
      runEffectWorker: false,
      runtimePolicyLayer: RuntimePolicy.layerFromProjectStore.pipe(
        Layer.provide(
          Layer.mergeAll(
            ProjectStore.layer.pipe(Layer.provide(layerDatabase)),
            Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
              getInstance: (id) =>
                Effect.succeed({
                  snapshot: {
                    getSnapshot: Effect.succeed({
                      supportedRuntimeModes: id === forkInstanceId ? ["full-access"] : undefined,
                    }),
                  },
                } as never),
            }),
          ),
        ),
      ),
    },
  ),
);

it.layer(layerMixedProviders)("DispatchModeLimit across a provider change", (it) => {
  it.effect.each([
    { name: "another thread", own: false },
    { name: "its own thread", own: true },
  ])("refuses a limited caller moving $name to a provider that runs above its limit", ({ own }) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const threadId = ThreadId.make(`thread:limit-switch-${own ? "own" : "other"}`);
      yield* createThread(threadId);
      const limit = own ? { ...supervised, callerThreadId: threadId } : supervised;
      for (const type of ["provider.switch", "thread.model-selection.set"] as const) {
        const refused = yield* orchestrator
          .dispatch({
            type,
            commandId: CommandId.make(`${type}:${threadId}`),
            threadId,
            modelSelection: { instanceId: forkInstanceId, model: "default" },
          })
          .pipe(Effect.provideService(DispatchModeLimit, limit), Effect.flip);
        assert.equal(refused._tag, "OrchestratorThreadAboveModeLimitError", type);
        assert.equal(
          (yield* projections.getThreadShell(threadId))?.modelSelection.instanceId,
          instanceId,
        );
      }
    }),
  );
});
