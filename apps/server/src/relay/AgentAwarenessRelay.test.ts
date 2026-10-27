import { assert, describe, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  EnvironmentId,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadShell,
  type OrchestrationV2TurnItem,
  ProjectId,
  ProviderInstanceId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import { RelayAgentActivityState } from "@t3tools/contracts/relay";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as FetchHttpClient from "effect/http/FetchHttpClient";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import {
  PUBLISH_AGENT_ACTIVITY_SECRET,
  RELAY_ENVIRONMENT_CREDENTIAL_SECRET,
  RELAY_ISSUER_SECRET,
  RELAY_URL_SECRET,
} from "../cloud/config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as AgentAwarenessRelay from "./AgentAwarenessRelay.ts";

const THREAD_ID = ThreadId.make("relay-thread");
const SECOND_THREAD_ID = ThreadId.make("relay-thread-2");
const PROJECT_ID = ProjectId.make("relay-project");
const NOW = "2026-09-04T12:00:00.000Z";

function shell(overrides: Partial<OrchestrationV2ThreadShell> = {}): OrchestrationV2ThreadShell {
  return {
    id: THREAD_ID,
    projectId: PROJECT_ID,
    title: "Thread",
    providerInstanceId: ProviderInstanceId.make("codex"),
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test-model" },
    runtimeMode: "full-access",
    interactionMode: "default",
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { rootThreadId: THREAD_ID, parentThreadId: null, relationshipToParent: null },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    activeRunId: null,
    latestVisibleMessage: null,
    hasActionableProposedPlan: false,
    itemCount: 0,
    visibleItemCount: 0,
    lastVisitedAt: null,
    deletedAt: null,
    branch: null,
    linkedPullRequest: null,
    status: "running",
    activityRunStatus: null,
    pendingRuntimeRequest: null,
    pendingBackgroundTasks: [],
    latestRunId: null,
    latestRunRequestedAt: null,
    latestRunStartedAt: null,
    latestRunCompletedAt: null,
    latestUserMessageAt: null,
    createdAt: DateTime.makeUnsafe(NOW),
    updatedAt: DateTime.makeUnsafe(NOW),
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    snoozedUntil: null,
    snoozedAt: null,
    pinnedAt: null,
    ...overrides,
  };
}

const PublishPayload = Schema.Struct({ state: Schema.NullOr(RelayAgentActivityState) });
const decodePublishPayload = Schema.decodeUnknownSync(Schema.fromJsonString(PublishPayload));
const unused = () => Effect.die("Unexpected test dependency call");

describe("startup agent activity", () => {
  it("publishes active work and only terminal runs completed after startup", () => {
    const startedAt = DateTime.toEpochMillis(DateTime.makeUnsafe(NOW));
    const oldCompleted = ThreadId.make("old-completed");
    const newCompleted = ThreadId.make("new-completed");
    const oldFailed = ThreadId.make("old-failed");
    const newFailed = ThreadId.make("new-failed");
    const ids = AgentAwarenessRelay.resolveAgentAwarenessRelayActiveThreadIds({
      environmentId: EnvironmentId.make("relay-env"),
      startedAt,
      now: DateTime.makeUnsafe(NOW),
      projects: [{ id: PROJECT_ID, title: "Project" }],
      threads: [
        shell(),
        shell({
          id: oldCompleted,
          status: "completed",
          latestRunCompletedAt: DateTime.makeUnsafe("2026-09-04T11:59:00.000Z"),
        }),
        shell({
          id: newCompleted,
          status: "completed",
          latestRunCompletedAt: DateTime.makeUnsafe("2026-09-04T12:00:01.000Z"),
        }),
        shell({
          id: oldFailed,
          status: "failed",
          latestRunCompletedAt: DateTime.makeUnsafe("2026-09-04T11:59:00.000Z"),
        }),
        shell({
          id: newFailed,
          status: "failed",
          latestRunCompletedAt: DateTime.makeUnsafe("2026-09-04T12:00:01.000Z"),
        }),
        shell({ id: ThreadId.make("idle"), status: "idle" }),
        shell({ id: ThreadId.make("missing-project"), projectId: ProjectId.make("missing") }),
      ],
    });
    assert.deepStrictEqual(ids, [THREAD_ID, newCompleted, newFailed]);
  });

  it("catches up on a heartbeat run's failure, not on the check finishing", () => {
    const heartbeatRun = RunId.make("heartbeat-run");
    const finishedAt = DateTime.makeUnsafe("2026-09-04T12:00:01.000Z");
    const heartbeat = {
      latestRunId: heartbeatRun,
      latestRunTrigger: "heartbeat" as const,
      latestRunCompletedAt: finishedAt,
    };
    const failed = ThreadId.make("heartbeat-failed");
    const ids = AgentAwarenessRelay.resolveAgentAwarenessRelayActiveThreadIds({
      environmentId: EnvironmentId.make("relay-env"),
      startedAt: DateTime.toEpochMillis(DateTime.makeUnsafe(NOW)),
      now: DateTime.makeUnsafe(NOW),
      projects: [{ id: PROJECT_ID, title: "Project" }],
      threads: [
        shell({ ...heartbeat, id: ThreadId.make("heartbeat-completed"), status: "completed" }),
        shell({
          ...heartbeat,
          id: ThreadId.make("heartbeat-running"),
          activityRunStatus: "running",
          activeRunId: heartbeatRun,
          latestRunCompletedAt: null,
        }),
        shell({ ...heartbeat, id: failed, status: "failed" }),
      ],
    });
    assert.deepStrictEqual(ids, [failed]);
  });
});

describe("catch-up after a snooze", () => {
  it("skips a terminal run that finished by the time an ended snooze was due", () => {
    const hidden = ThreadId.make("hidden-by-snooze");
    const after = ThreadId.make("after-snooze");
    const ids = AgentAwarenessRelay.resolveAgentAwarenessRelayActiveThreadIds({
      environmentId: EnvironmentId.make("relay-env"),
      startedAt: DateTime.toEpochMillis(DateTime.makeUnsafe(NOW)),
      now: DateTime.makeUnsafe("2026-09-04T14:00:00.000Z"),
      projects: [{ id: PROJECT_ID, title: "Project" }],
      threads: [
        shell({
          id: hidden,
          status: "completed",
          snoozedUntil: DateTime.makeUnsafe("2026-09-04T13:00:00.000Z"),
          latestRunCompletedAt: DateTime.makeUnsafe("2026-09-04T12:30:00.000Z"),
        }),
        shell({
          id: after,
          status: "completed",
          snoozedUntil: DateTime.makeUnsafe("2026-09-04T13:00:00.000Z"),
          latestRunCompletedAt: DateTime.makeUnsafe("2026-09-04T13:30:00.000Z"),
        }),
      ],
    });
    assert.deepStrictEqual(ids, [after]);
  });
});

const makeTestRelay = Effect.fnUntraced(function* (
  options: {
    readonly respond?: (attempt: number) => Response;
    readonly failSecretRead?: (name: string) => boolean;
    readonly linked?: boolean;
    readonly readShell?: (threadId: ThreadId) => Effect.Effect<OrchestrationV2ThreadShell | null>;
    readonly domainEvents?: Stream.Stream<OrchestrationV2DomainEvent>;
  } = {},
) {
  const values = new Map<string, Uint8Array>(
    options.linked === false
      ? []
      : [
          [PUBLISH_AGENT_ACTIVITY_SECRET, new TextEncoder().encode("true")],
          [RELAY_URL_SECRET, new TextEncoder().encode("https://relay.example.test")],
          [RELAY_ISSUER_SECRET, new TextEncoder().encode("https://relay.example.test")],
          [RELAY_ENVIRONMENT_CREDENTIAL_SECRET, new TextEncoder().encode("credential-1")],
        ],
  );
  const secretReads: string[] = [];
  const secrets = ServerSecretStore.ServerSecretStore.of({
    get: (name) =>
      Effect.suspend(() => {
        secretReads.push(name);
        if (options.failSecretRead?.(name)) {
          return Effect.fail(
            new ServerSecretStore.SecretStoreReadError({
              resource: name,
              cause: "temporary read failure",
            }),
          );
        }
        return Effect.succeed(Option.fromUndefinedOr(values.get(name)));
      }),
    create: (name, value) => Effect.sync(() => void values.set(name, value)),
    set: (name, value) => Effect.sync(() => void values.set(name, value)),
    remove: (name) => Effect.sync(() => void values.delete(name)),
    getOrCreateRandom: unused,
  });
  const currentShell = yield* Ref.make<OrchestrationV2ThreadShell | null>(shell());
  const shellReads: ThreadId[] = [];
  const catchUp = { shellSnapshotReads: 0 };
  const threads = ThreadManagementService.ThreadManagementService.of({
    getThreadShell: (threadId) =>
      Effect.sync(() => shellReads.push(threadId)).pipe(
        Effect.andThen(options.readShell?.(threadId) ?? Ref.get(currentShell)),
      ),
    getShellSnapshot: () =>
      Effect.sync(() => {
        catchUp.shellSnapshotReads += 1;
        return { schemaVersion: 2, snapshotSequence: 1, threads: [], archivedThreads: [] };
      }),
    listActiveSubagentThreadIds: unused,
    listOlderSettledThreads: unused,
    readShellSnapshot: unused,
    ensureLegacyTranscript: unused,
    dispatch: unused,
    searchThread: () => Effect.die("unused"),
    searchThreadStream: () => Stream.empty,
    getThreadHistoryPage: () => Effect.die("unused"),
    getTimelinePage: () => Effect.die("Unused timeline read"),
    getMessageCount: () => Effect.die("unused message count"),
    getTurnItem: () => Effect.die("unused turn item read"),
    getThreadRecords: () => Effect.die("unused record read"),
    getThreadProjection: unused,
    getCheckpointContext: unused,
    getThreadSnapshot: unused,
    getThreadTranscript: unused,
    getThreadSnapshotWindow: unused,
    getProjectThreadRecords: () => Effect.die("unused project record read"),
    listProjectThreads: unused,
    sendToThread: unused,
    assertProviderInstanceEnabled: unused,
    isProviderInstanceEnabledForProject: unused,
    hasReceipt: unused,
    waitForThread: unused,
    settleAfterRun: unused,
    settleThread: unused,
    interruptThread: unused,
    stopWorkflow: () => Effect.die("unused stopWorkflow"),
    awaitBackgroundTasks: unused,
    stopDelegatedTasks: unused,
    getThreadEventSequence: unused,
    recoverDelegatedTask: unused,
    delegatedTaskResultPending: unused,
    streamStoredEvents: Stream.empty,
    streamStoredEventsFrom: () => Stream.empty,
    streamDomainEvents: options.domainEvents ?? Stream.empty,
  });
  const publications: Array<{
    readonly url: string;
    readonly authorization: string | null;
    readonly state: RelayAgentActivityState | null;
  }> = [];
  const fetch: typeof globalThis.fetch = Object.assign(
    (
      input: Parameters<typeof globalThis.fetch>[0],
      init?: Parameters<typeof globalThis.fetch>[1],
    ) => {
      const body = init?.body;
      if (typeof body !== "string" && !(body instanceof Uint8Array)) {
        return Promise.reject(new Error("Expected a serialized activity publish payload"));
      }
      const payload = decodePublishPayload(
        typeof body === "string" ? body : new TextDecoder().decode(body),
      );
      publications.push({
        url: String(input),
        authorization: new Headers(init?.headers).get("authorization"),
        state: payload.state,
      });
      return Promise.resolve(
        options.respond?.(publications.length) ?? Response.json({ ok: true, deliveries: [] }),
      );
    },
    { preconnect: () => {} },
  );
  const relay = yield* AgentAwarenessRelay.make.pipe(
    Effect.provideService(ServerSecretStore.ServerSecretStore, secrets),
    Effect.provideService(ThreadManagementService.ThreadManagementService, threads),
    Effect.provideService(ServerEnvironment.ServerEnvironment, {
      getEnvironmentId: Effect.succeed(EnvironmentId.make("relay-environment")),
      getDescriptor: unused(),
    }),
    Effect.provideService(ProjectService.ProjectService, {
      create: unused,
      bootstrap: unused,
      update: unused,
      delete: unused,
      getByWorkspaceRoot: unused,
      snapshot: Effect.succeed({ projects: [] } as never),
      listPage: unused,
      getShell: unused,
      listShells: unused,
      getById: () =>
        Effect.succeedSome({
          id: PROJECT_ID,
          title: "Project",
          workspaceRoot: "/workspace",
          defaultModelSelection: null,
          scripts: [],
          createdAt: NOW,
          updatedAt: NOW,
          deletedAt: null,
        }),
    }),
    Effect.provideService(FetchHttpClient.Fetch, fetch),
    Effect.provide(NodeCrypto.layer),
  );
  return { relay, secrets, secretReads, currentShell, shellReads, publications, catchUp };
});

describe("AgentAwarenessRelay", () => {
  it.effect("coalesces queued updates and reruns a thread dirtied during publishing", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const rerunStarted = yield* Deferred.make<void>();
      const releaseRerun = yield* Deferred.make<void>();
      const processed: Array<{ threadId: ThreadId; revision: number }> = [];
      let revision = 1;
      const worker = yield* AgentAwarenessRelay.makeAgentAwarenessPublishWorker((threadId) =>
        Effect.gen(function* () {
          const currentRevision = revision;
          const index = processed.length;
          processed.push({ threadId, revision: currentRevision });
          if (index === 0) {
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
          } else if (threadId === THREAD_ID) {
            yield* Deferred.succeed(rerunStarted, undefined);
            yield* Deferred.await(releaseRerun);
          }
        }),
      );
      yield* worker.enqueue(THREAD_ID);
      yield* Deferred.await(started);
      revision = 2;
      for (let i = 0; i < 100; i++) {
        yield* worker.enqueue(SECOND_THREAD_ID);
        yield* worker.enqueue(THREAD_ID);
      }
      const drained = yield* Deferred.make<void>();
      const draining = yield* worker.drain.pipe(
        Effect.andThen(Deferred.succeed(drained, undefined)),
        Effect.forkChild,
      );
      yield* Deferred.succeed(release, undefined);
      yield* Deferred.await(rerunStarted);
      assert.isFalse(yield* Deferred.isDone(drained));
      yield* Deferred.succeed(releaseRerun, undefined);
      yield* Fiber.join(draining);
      assert.deepEqual(processed, [
        { threadId: THREAD_ID, revision: 1 },
        { threadId: SECOND_THREAD_ID, revision: 2 },
        { threadId: THREAD_ID, revision: 2 },
      ]);
    }),
  );

  it.effect("deduplicates state and republishes title changes", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      yield* Ref.set(
        currentShell,
        shell({ updatedAt: DateTime.makeUnsafe("2026-09-04T13:00:00Z") }),
      );
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications.length, 1);
      yield* Ref.set(currentShell, shell({ title: "Renamed thread" }));
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications.length, 2);
      assert.equal(publications[1]?.state?.threadTitle, "Renamed thread");
    }),
  );

  it.effect("republishes unchanged state with fresh credentials after relinking", () =>
    Effect.gen(function* () {
      const { relay, secrets, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      yield* secrets.set(
        RELAY_ENVIRONMENT_CREDENTIAL_SECRET,
        new TextEncoder().encode("credential-2"),
      );
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications.length, 2);
      assert.equal(publications[0]?.authorization, "Bearer credential-1");
      assert.equal(publications[1]?.authorization, "Bearer credential-2");
      yield* secrets.set(
        RELAY_URL_SECRET,
        new TextEncoder().encode("https://new-relay.example.test"),
      );
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications.length, 3);
      assert.isTrue(publications[2]?.url.startsWith("https://new-relay.example.test/") ?? false);
    }),
  );

  it.effect.each(["completed", "failed"] as const)(
    "publishes a tombstone when a %s thread is settled through a domain event",
    (status) =>
      Effect.gen(function* () {
        const events = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
        const pulls = yield* Queue.unbounded<void>();
        const { relay, currentShell, publications } = yield* makeTestRelay({
          domainEvents: Stream.fromEffectRepeat(
            Queue.offer(pulls, undefined).pipe(Effect.andThen(Queue.take(events))),
          ),
        });
        yield* relay.start();
        yield* Queue.take(pulls);
        yield* relay.publishThread(THREAD_ID);
        const terminal = shell({ status });
        yield* Ref.set(currentShell, terminal);
        yield* relay.publishThread(THREAD_ID);
        yield* TestClock.adjust("5 seconds");
        yield* relay.drain;
        assert.equal(publications[1]?.state?.phase, status);

        const now = yield* DateTime.now;
        const settled = {
          ...terminal,
          settledOverride: "settled" as const,
          settledAt: now,
          lastVisitedAt: null,
        };
        yield* Ref.set(currentShell, settled);
        yield* Queue.offer(events, {
          id: EventId.make("event:settled"),
          type: "thread.settled",
          threadId: THREAD_ID,
          occurredAt: now,
          payload: settled,
        });
        yield* Queue.take(pulls);
        yield* relay.drain;
        assert.equal(publications.length, 2);
        yield* TestClock.adjust("5 seconds");
        yield* relay.drain;
        assert.equal(publications.length, 3);
        assert.equal(publications[2]?.state, null);
      }).pipe(Effect.scoped),
  );

  it.effect("holds a completion through a continuation gap, then publishes it once", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      yield* Ref.set(currentShell, shell({ status: "completed" }));
      yield* relay.publishThread(THREAD_ID);
      yield* Ref.set(currentShell, shell());
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.deepEqual(
        publications.map((publication) => publication.state?.phase),
        ["running"],
      );

      yield* Ref.set(currentShell, shell({ status: "completed" }));
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications.length, 1);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.deepEqual(
        publications.map((publication) => publication.state?.phase),
        ["running", "completed"],
      );
    }),
  );

  it.effect("still publishes a completion when the thread settles during the hold", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      const completed = shell({ status: "completed" });
      yield* Ref.set(currentShell, completed);
      yield* relay.publishThread(THREAD_ID);
      yield* Ref.set(currentShell, {
        ...completed,
        settledOverride: "settled",
        settledAt: yield* DateTime.now,
        lastVisitedAt: null,
      });
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.deepEqual(
        publications.map((publication) => publication.state?.phase ?? null),
        ["running", "completed"],
      );

      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.deepEqual(
        publications.map((publication) => publication.state?.phase ?? null),
        ["running", "completed", null],
      );
    }),
  );

  it.effect("publishes a completion settled during the hold after an earlier snooze ended", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      const snoozeEnded = yield* DateTime.now;
      yield* TestClock.adjust("1 second");
      const completed = shell({
        status: "completed",
        snoozedUntil: snoozeEnded,
        latestRunCompletedAt: yield* DateTime.now,
      });
      yield* Ref.set(currentShell, completed);
      yield* relay.publishThread(THREAD_ID);
      yield* Ref.set(currentShell, {
        ...completed,
        settledOverride: "settled",
        settledAt: yield* DateTime.now,
      });
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.deepEqual(
        publications.map((publication) => publication.state?.phase ?? null),
        ["running", "completed"],
      );
    }),
  );

  it.effect("does not announce a held completion that a short snooze hid before it settled", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      const completed = shell({ status: "completed", latestRunCompletedAt: yield* DateTime.now });
      yield* Ref.set(currentShell, completed);
      yield* relay.publishThread(THREAD_ID);
      yield* Ref.set(currentShell, {
        ...completed,
        snoozedUntil: DateTime.add(yield* DateTime.now, { seconds: 1 }),
        snoozedAt: yield* DateTime.now,
        settledOverride: "settled",
        settledAt: yield* DateTime.now,
      });
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.notInclude(
        publications.map((publication) => publication.state?.phase ?? null),
        "completed",
      );
    }),
  );

  it.effect("drops a held completion when a queued continuation hides the thread", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      yield* Ref.set(currentShell, shell({ status: "completed" }));
      yield* relay.publishThread(THREAD_ID);
      yield* Ref.set(currentShell, shell({ status: "queued" }));
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.notInclude(
        publications.map((publication) => publication.state?.phase ?? null),
        "completed",
      );
    }),
  );

  it.effect("retries a failed completion even after the thread settles", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay({
        respond: (attempt) =>
          attempt === 2
            ? new Response("unavailable", { status: 503 })
            : Response.json({ ok: true, deliveries: [] }),
      });
      yield* relay.publishThread(THREAD_ID);
      const completed = shell({ status: "completed" });
      yield* Ref.set(currentShell, completed);
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      yield* Ref.set(currentShell, {
        ...completed,
        settledOverride: "settled",
        settledAt: yield* DateTime.now,
        lastVisitedAt: null,
      });
      yield* TestClock.adjust("1 second");
      yield* relay.drain;
      assert.deepEqual(
        publications.map((publication) => publication.state?.phase ?? null),
        ["running", "completed", "completed"],
      );

      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.equal(publications.at(-1)?.state ?? null, null);
    }),
  );

  it.effect("publishes nothing for a snoozed thread until its timer ends", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications[0]?.state?.phase, "running");

      const snoozedUntil = DateTime.add(yield* DateTime.now, { hours: 1 });
      const settleTombstone = Effect.gen(function* () {
        yield* TestClock.adjust("5 seconds");
        yield* relay.drain;
      });
      for (const snoozedShell of [
        shell({ snoozedUntil }),
        shell({ status: "completed", snoozedUntil }),
        shell({ status: "failed", snoozedUntil }),
        shell({
          snoozedUntil,
          pendingRuntimeRequest: {
            id: RuntimeRequestId.make("snoozed-question"),
            kind: "user_input",
            createdAt: DateTime.makeUnsafe(NOW),
          },
        }),
      ]) {
        yield* Ref.set(currentShell, snoozedShell);
        yield* relay.publishThread(THREAD_ID);
        yield* settleTombstone;
      }
      assert.isTrue(publications.length > 1);
      assert.isTrue(publications.slice(1).every((publication) => publication.state === null));

      // The timer ending lets a question that is still waiting through.
      yield* Ref.set(
        currentShell,
        shell({
          snoozedUntil,
          pendingRuntimeRequest: {
            id: RuntimeRequestId.make("snoozed-question"),
            kind: "user_input",
            createdAt: DateTime.makeUnsafe(NOW),
          },
        }),
      );
      yield* TestClock.adjust("2 hours");
      yield* relay.publishThread(THREAD_ID);
      yield* settleTombstone;
      assert.equal(publications.at(-1)?.state?.phase, "waiting_for_input");
    }),
  );

  it.effect.each(["completed", "failed"] as const)(
    "does not announce a %s run that finished under a snooze once the timer is over",
    (status) =>
      Effect.gen(function* () {
        const { relay, currentShell, publications } = yield* makeTestRelay();
        yield* relay.publishThread(THREAD_ID);
        const settle = Effect.gen(function* () {
          yield* TestClock.adjust("5 seconds");
          yield* relay.drain;
        });
        const snoozedUntil = DateTime.add(yield* DateTime.now, { hours: 1 });
        const finishedWhileSnoozed = shell({
          status,
          snoozedUntil,
          latestRunId: RunId.make("run-under-snooze"),
          latestRunCompletedAt: DateTime.makeUnsafe("1970-01-01T00:00:10.000Z"),
        });
        yield* Ref.set(currentShell, finishedWhileSnoozed);
        yield* relay.publishThread(THREAD_ID);
        yield* settle;
        const published = publications.length;
        assert.isNull(publications.at(-1)?.state);

        // An unrelated metadata update after the timer republishes the thread
        // and must not replay the hidden outcome.
        yield* TestClock.adjust("2 hours");
        yield* relay.publishThread(THREAD_ID);
        yield* settle;
        assert.equal(publications.length, published);

        // Work that finishes after the snooze is genuinely new.
        yield* Ref.set(
          currentShell,
          shell({
            status,
            snoozedUntil,
            latestRunId: RunId.make("run-after-snooze"),
            latestRunCompletedAt: DateTime.makeUnsafe("1970-01-01T03:00:00.000Z"),
          }),
        );
        yield* relay.publishThread(THREAD_ID);
        yield* settle;
        assert.equal(publications.length, published + 1);
        assert.equal(publications.at(-1)?.state?.phase, status);
      }),
  );

  it.effect.each(["completed", "failed"] as const)(
    "withdraws a published row and does not announce a %s run that finished in a short snooze",
    (status) =>
      Effect.gen(function* () {
        const { relay, currentShell, publications } = yield* makeTestRelay();
        yield* relay.publishThread(THREAD_ID);
        assert.equal(publications.at(-1)?.state?.phase, "running");

        // The snooze is shorter than the 5 s withdrawal confirmation, so the
        // thread is awake again by the time that confirmation runs.
        const snoozedUntil = DateTime.add(yield* DateTime.now, { seconds: 2 });
        yield* Ref.set(
          currentShell,
          shell({
            status,
            snoozedUntil,
            latestRunId: RunId.make("run-in-short-snooze"),
            latestRunCompletedAt: DateTime.makeUnsafe("1970-01-01T00:00:01.000Z"),
          }),
        );
        yield* relay.publishThread(THREAD_ID);
        yield* TestClock.adjust("5 seconds");
        yield* relay.drain;
        assert.isNull(publications.at(-1)?.state);
        assert.isTrue(publications.every((publication) => publication.state?.phase !== status));
        const published = publications.length;

        // Current work after the deadline is genuine and still shows.
        yield* Ref.set(currentShell, shell({ status: "running", snoozedUntil }));
        yield* relay.publishThread(THREAD_ID);
        assert.equal(publications.at(-1)?.state?.phase, "running");
        assert.equal(publications.length, published + 1);

        // So does a run that finishes after it.
        yield* Ref.set(
          currentShell,
          shell({
            status,
            snoozedUntil,
            latestRunId: RunId.make("run-after-short-snooze"),
            latestRunCompletedAt: DateTime.makeUnsafe("1970-01-01T00:00:30.000Z"),
          }),
        );
        yield* relay.publishThread(THREAD_ID);
        yield* TestClock.adjust("5 seconds");
        yield* relay.drain;
        assert.equal(publications.at(-1)?.state?.phase, status);
      }),
  );

  it.effect.each(["completed", "failed"] as const)(
    "does not announce a %s run hidden by a snooze when publishing is turned on after it ends",
    (status) =>
      Effect.gen(function* () {
        const { relay, secrets, currentShell, publications } = yield* makeTestRelay({
          linked: false,
        });
        const snoozedUntil = DateTime.add(yield* DateTime.now, { hours: 1 });
        yield* Ref.set(
          currentShell,
          shell({
            status,
            snoozedUntil,
            latestRunId: RunId.make("run-under-snooze"),
            latestRunCompletedAt: DateTime.makeUnsafe("1970-01-01T00:00:10.000Z"),
          }),
        );
        // Nothing can publish while unlinked, so nothing here sees the outcome.
        yield* relay.publishThread(THREAD_ID);
        yield* TestClock.adjust("2 hours");
        for (const [name, value] of [
          [PUBLISH_AGENT_ACTIVITY_SECRET, "true"],
          [RELAY_URL_SECRET, "https://relay.example.test"],
          [RELAY_ISSUER_SECRET, "https://relay.example.test"],
          [RELAY_ENVIRONMENT_CREDENTIAL_SECRET, "credential-1"],
        ] as const) {
          yield* secrets.set(name, new TextEncoder().encode(value));
        }
        yield* relay.publishThread(THREAD_ID);
        yield* TestClock.adjust("5 seconds");
        yield* relay.drain;
        assert.equal(publications.length, 0);
      }),
  );

  it.effect.each(["completed", "failed"] as const)(
    "does not announce a %s run that finished before an unsnooze, whatever event ordering",
    (status) =>
      Effect.gen(function* () {
        // The unsnooze already cleared the deadline and left snoozeEndedAt; no
        // domain event is delivered at all, so only the shell read can tell.
        const { relay, currentShell, publications } = yield* makeTestRelay();
        yield* relay.publishThread(THREAD_ID);
        assert.equal(publications.at(-1)?.state?.phase, "running");
        const settle = Effect.gen(function* () {
          yield* TestClock.adjust("5 seconds");
          yield* relay.drain;
        });
        const endedAt = DateTime.makeUnsafe("1970-01-01T00:01:00.000Z");
        yield* Ref.set(
          currentShell,
          shell({
            status,
            snoozedUntil: null,
            snoozeEndedAt: endedAt,
            latestRunId: RunId.make("run-before-unsnooze"),
            latestRunCompletedAt: DateTime.makeUnsafe("1970-01-01T00:00:10.000Z"),
          }),
        );
        yield* relay.publishThread(THREAD_ID);
        yield* settle;
        // The earlier running row is withdrawn; nothing announces the outcome.
        assert.isNull(publications.at(-1)?.state);
        assert.isTrue(publications.every((publication) => publication.state?.phase !== status));
        const published = publications.length;

        // A run that finishes after the unsnooze is genuinely new.
        yield* Ref.set(
          currentShell,
          shell({
            status,
            snoozedUntil: null,
            snoozeEndedAt: endedAt,
            latestRunId: RunId.make("run-after-unsnooze"),
            latestRunCompletedAt: DateTime.makeUnsafe("1970-01-01T00:02:00.000Z"),
          }),
        );
        yield* relay.publishThread(THREAD_ID);
        yield* settle;
        assert.equal(publications.length, published + 1);
        assert.equal(publications.at(-1)?.state?.phase, status);
      }),
  );

  it.effect.each(["completed", "failed"] as const)(
    "does not announce a %s run hidden by an unsnooze when publishing is turned on afterwards",
    (status) =>
      Effect.gen(function* () {
        const { relay, secrets, currentShell, publications } = yield* makeTestRelay({
          linked: false,
        });
        yield* Ref.set(
          currentShell,
          shell({
            status,
            snoozeEndedAt: DateTime.makeUnsafe("1970-01-01T00:01:00.000Z"),
            latestRunId: RunId.make("run-before-unsnooze"),
            latestRunCompletedAt: DateTime.makeUnsafe("1970-01-01T00:00:10.000Z"),
          }),
        );
        yield* relay.publishThread(THREAD_ID);
        for (const [name, value] of [
          [PUBLISH_AGENT_ACTIVITY_SECRET, "true"],
          [RELAY_URL_SECRET, "https://relay.example.test"],
          [RELAY_ISSUER_SECRET, "https://relay.example.test"],
          [RELAY_ENVIRONMENT_CREDENTIAL_SECRET, "credential-1"],
        ] as const) {
          yield* secrets.set(name, new TextEncoder().encode(value));
        }
        yield* relay.publishThread(THREAD_ID);
        yield* TestClock.adjust("5 seconds");
        yield* relay.drain;
        assert.equal(publications.length, 0);
      }),
  );

  it("catch-up skips a run that finished by the time an unsnooze ended a snooze", () => {
    const hidden = ThreadId.make("hidden-by-unsnooze");
    const after = ThreadId.make("after-unsnooze");
    const ended = DateTime.makeUnsafe("2026-09-04T13:00:00.000Z");
    const ids = AgentAwarenessRelay.resolveAgentAwarenessRelayActiveThreadIds({
      environmentId: EnvironmentId.make("relay-env"),
      startedAt: DateTime.toEpochMillis(DateTime.makeUnsafe(NOW)),
      now: DateTime.makeUnsafe("2026-09-04T14:00:00.000Z"),
      projects: [{ id: PROJECT_ID, title: "Project" }],
      threads: [
        shell({
          id: hidden,
          status: "completed",
          snoozeEndedAt: ended,
          latestRunCompletedAt: DateTime.makeUnsafe("2026-09-04T12:30:00.000Z"),
        }),
        shell({
          id: after,
          status: "completed",
          snoozeEndedAt: ended,
          latestRunCompletedAt: DateTime.makeUnsafe("2026-09-04T13:30:00.000Z"),
        }),
      ],
    });
    assert.deepStrictEqual(ids, [after]);
  });

  it.effect("sends no completion for a heartbeat check, but still sends its failure", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      const userRun = RunId.make("user-run");
      const heartbeatRun = RunId.make("heartbeat-run");
      yield* Ref.set(
        currentShell,
        shell({ latestRunId: userRun, activeRunId: userRun, activityRunStatus: "running" }),
      );
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications[0]?.state?.phase, "running");

      const heartbeat = { latestRunId: heartbeatRun, latestRunTrigger: "heartbeat" as const };
      const finishedAt = DateTime.add(yield* DateTime.now, { seconds: 1 });
      for (const check of [
        shell({ ...heartbeat, activeRunId: heartbeatRun, activityRunStatus: "running" }),
        shell({ ...heartbeat, status: "completed", latestRunCompletedAt: finishedAt }),
      ]) {
        yield* Ref.set(currentShell, check);
        yield* relay.publishThread(THREAD_ID);
        yield* TestClock.adjust("5 seconds");
        yield* relay.drain;
      }
      // The user's run is over, so its Live Activity is withdrawn, not kept spinning.
      assert.equal(publications[1]?.state, null);
      assert.isFalse(publications.some((publication) => publication.state?.phase === "completed"));

      yield* Ref.set(
        currentShell,
        shell({ ...heartbeat, status: "failed", latestRunCompletedAt: finishedAt }),
      );
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications.at(-1)?.state?.phase, "failed");
    }),
  );

  it.effect("keeps settled live work until its awaited monitor finishes", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      const held = shell({
        status: "completed",
        settledOverride: "settled",
        pendingBackgroundTasks: [{ taskId: "watch", kind: "monitor", awaited: true }],
      });
      yield* Ref.set(currentShell, held);
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.equal(publications.length, 1);
      assert.equal(publications[0]?.state?.phase, "running");

      yield* Ref.set(currentShell, { ...held, pendingBackgroundTasks: [] });
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.equal(publications.length, 2);
      assert.equal(publications[1]?.state, null);
    }),
  );
  it.effect("publishes a held completion when its background item ends, not on tool output", () =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const events = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
      const pulls = yield* Queue.unbounded<void>();
      const { relay, shellReads, publications } = yield* makeTestRelay({
        readShell: (threadId) => store.getThreadShell(threadId).pipe(Effect.orDie),
        domainEvents: Stream.fromEffectRepeat(
          Queue.offer(pulls, undefined).pipe(Effect.andThen(Queue.take(events))),
        ),
      });
      yield* relay.start();
      yield* Queue.take(pulls);
      const deliver = Effect.fnUntraced(function* (event: OrchestrationV2DomainEvent) {
        yield* store.apply(event);
        yield* Queue.offer(events, event);
        yield* Queue.take(pulls);
        yield* relay.drain;
      });

      const now = yield* DateTime.now;
      const runId = RunId.make("run:held-item");
      const run = {
        id: runId,
        threadId: THREAD_ID,
        ordinal: 1,
        providerInstanceId: ProviderInstanceId.make("codex"),
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test-model" },
        providerThreadId: null,
        userMessageId: MessageId.make("message:held-item"),
        rootNodeId: null,
        activeAttemptId: null,
        status: "running" as const,
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      };
      const item = (
        id: string,
        fields: Pick<OrchestrationV2TurnItem, "status"> &
          (
            | { readonly type: "dynamic_tool"; readonly toolName: string; readonly input: unknown }
            | {
                readonly type: "assistant_message";
                readonly messageId: MessageId;
                readonly text: string;
                readonly streaming: boolean;
              }
          ),
      ): OrchestrationV2TurnItem => ({
        id: TurnItemId.make(id),
        threadId: THREAD_ID,
        runId,
        nodeId: NodeId.make("node:held-item"),
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 1,
        title: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        ...fields,
      });
      const itemEvent = (id: string, payload: OrchestrationV2TurnItem) =>
        ({
          id: EventId.make(id),
          type: "turn-item.updated",
          threadId: THREAD_ID,
          runId,
          occurredAt: now,
          payload,
        }) satisfies OrchestrationV2DomainEvent;
      const background = { type: "dynamic_tool" as const, toolName: "watch", input: {} };

      yield* deliver({
        id: EventId.make("event:held-item:thread"),
        type: "thread.created",
        threadId: THREAD_ID,
        occurredAt: now,
        payload: {
          createdBy: "user",
          creationSource: "web",
          id: THREAD_ID,
          projectId: PROJECT_ID,
          title: "Thread",
          providerInstanceId: ProviderInstanceId.make("codex"),
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test-model" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          activeProviderThreadId: null,
          lineage: { rootThreadId: THREAD_ID, parentThreadId: null, relationshipToParent: null },
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
      yield* deliver({
        id: EventId.make("event:held-item:run"),
        type: "run.created",
        threadId: THREAD_ID,
        runId,
        occurredAt: now,
        payload: run,
      });
      yield* deliver(
        itemEvent("event:held-item:tool", item("item:tool", { ...background, status: "running" })),
      );
      yield* deliver({
        id: EventId.make("event:held-item:run-completed"),
        type: "run.updated",
        threadId: THREAD_ID,
        runId,
        occurredAt: now,
        payload: { ...run, status: "completed", completedAt: now },
      });
      assert.deepEqual(
        publications.map((publication) => publication.state?.phase),
        ["running"],
      );

      const readsBeforeOutput = shellReads.length;
      yield* deliver(
        itemEvent(
          "event:held-item:reply",
          item("item:reply", {
            type: "assistant_message",
            messageId: MessageId.make("message:held-item:reply"),
            text: "Still watching",
            streaming: true,
            status: "running",
          }),
        ),
      );
      yield* deliver(
        itemEvent("event:held-item:tick", item("item:tool", { ...background, status: "running" })),
      );
      assert.equal(shellReads.length, readsBeforeOutput);

      yield* deliver(
        itemEvent("event:held-item:end", item("item:tool", { ...background, status: "completed" })),
      );
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.deepEqual(
        publications.map((publication) => publication.state?.phase),
        ["running", "completed"],
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(ProjectionStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory))),
    ),
  );
});
