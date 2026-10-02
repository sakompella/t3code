import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as Orchestrator from "./Orchestrator.ts";
import type { ProviderAdapterV2Event, ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "test-model" };

// A steer whose turn ended before the provider took it must not be lost. A turn
// that failed runs the message as the next run; a Stop (interrupted) parks it in
// the held queue for the user. A steer the provider already took is never re-sent.
for (const ending of ["failed", "interrupted"] as const) {
  for (const timing of ["before delivery", "after delivery"] as const) {
    it.effect(
      `steer on a turn that ${ending} ${timing === "before delivery" ? "before it was delivered" : "after it was delivered"}`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const cwd = yield* checkpointWorkspace(
              `steer-ended-${ending}-${timing.replace(" ", "-")}`,
            );
            const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
            const startedMessageIds: string[] = [];
            let steerCalls = 0;
            const adapter: ProviderAdapterV2Shape = {
              instanceId,
              driver,
              getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
              planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
              openSession: (input) =>
                Effect.gen(function* () {
                  const now = yield* DateTime.now;
                  return {
                    instanceId,
                    driver,
                    providerSessionId: input.providerSessionId,
                    providerSession: {
                      id: input.providerSessionId,
                      driver,
                      providerInstanceId: instanceId,
                      status: "ready",
                      cwd,
                      model: modelSelection.model,
                      capabilities: CodexProviderCapabilitiesV2,
                      createdAt: now,
                      updatedAt: now,
                      lastError: null,
                    },
                    events: Stream.fromQueue(events),
                    ensureThread: ({ threadId }) =>
                      Effect.succeed({
                        id: ProviderThreadId.make(`provider-thread:${threadId}`),
                        driver,
                        providerInstanceId: instanceId,
                        providerSessionId: input.providerSessionId,
                        appThreadId: threadId,
                        ownerNodeId: null,
                        nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
                        nativeConversationHeadRef: null,
                        status: "idle",
                        firstRunOrdinal: null,
                        lastRunOrdinal: null,
                        handoffIds: [],
                        forkedFrom: null,
                        createdAt: now,
                        updatedAt: now,
                      }),
                    resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
                    startTurn: (turn) =>
                      Effect.gen(function* () {
                        startedMessageIds.push(turn.message.messageId);
                        yield* Queue.offer(events, {
                          type: "provider_turn.updated",
                          driver,
                          providerTurn: {
                            id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                            providerThreadId: turn.providerThread.id,
                            nodeId: turn.rootNodeId,
                            runAttemptId: turn.attemptId,
                            nativeTurnRef: {
                              driver,
                              nativeId: `native:${turn.attemptId}`,
                              strength: "strong",
                            },
                            ordinal: turn.providerTurnOrdinal,
                            status: "running",
                            startedAt: now,
                            completedAt: null,
                          },
                        });
                      }),
                    steerTurn: () =>
                      Effect.sync(() => {
                        steerCalls += 1;
                      }),
                    interruptTurn: () => Effect.void,
                    respondToRuntimeRequest: () => Effect.void,
                    readThreadSnapshot: () => Effect.die("unused"),
                    rollbackThread: () => Effect.die("unused"),
                    forkThread: () => Effect.die("unused"),
                  };
                }),
            };
            yield* Effect.gen(function* () {
              const orchestrator = yield* Orchestrator.OrchestratorV2;
              const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
              const threadId = ThreadId.make("thread:steer-ended");
              const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
                orchestrator.streamDomainEvents.pipe(
                  Stream.filter(predicate),
                  Stream.take(1),
                  Stream.runDrain,
                  Effect.forkScoped,
                );
              yield* orchestrator.dispatch({
                type: "thread.create",
                commandId: CommandId.make("create"),
                threadId,
                projectId: ProjectId.make("project:steer-ended"),
                title: "Steer on an ended turn",
                modelSelection,
                runtimeMode: "full-access",
                interactionMode: "default",
                branch: null,
                worktreePath: cwd,
                createdBy: "user",
                creationSource: "web",
              });
              yield* orchestrator.dispatch({
                type: "message.dispatch",
                commandId: CommandId.make("first"),
                threadId,
                messageId: MessageId.make("message:first"),
                text: "first",
                attachments: [],
                dispatchMode: { type: "start_immediately" },
                createdBy: "user",
                creationSource: "web",
              });
              const running = yield* watch(
                (event) =>
                  event.type === "provider-turn.updated" && event.payload.status === "running",
              );
              yield* worker.drain();
              yield* Fiber.join(running);
              const firstRun = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;

              const messageId = MessageId.make("message:steering");
              yield* orchestrator.dispatch({
                type: "message.dispatch",
                commandId: CommandId.make("steer"),
                threadId,
                messageId,
                text: "fix the popover",
                attachments: [],
                dispatchMode: { type: "steer_active", targetRunId: firstRun.id },
                createdBy: "user",
                creationSource: "web",
              });
              if (timing === "after delivery") yield* worker.drain();

              const ended = yield* watch(
                (event) =>
                  event.type === "run.updated" &&
                  event.payload.id === firstRun.id &&
                  event.payload.status === ending,
              );
              const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
              const completedAt = yield* DateTime.now;
              yield* Queue.offer(events, {
                type: "provider_turn.updated",
                driver,
                providerTurn: { ...turn, status: ending, completedAt },
              });
              yield* Queue.offer(
                events,
                ending === "interrupted"
                  ? {
                      type: "turn.terminal",
                      driver,
                      providerThreadId: turn.providerThreadId,
                      providerTurnId: turn.id,
                      runOrdinal: firstRun.ordinal,
                      status: "interrupted",
                      failure: null,
                      threadDisposition: "reusable",
                    }
                  : {
                      type: "turn.terminal",
                      driver,
                      providerThreadId: turn.providerThreadId,
                      providerTurnId: turn.id,
                      runOrdinal: firstRun.ordinal,
                      failureItemOrdinal: 100,
                      status: "failed",
                      failure: {
                        class: "provider_error",
                        message: "The provider failed.",
                        code: null,
                        retryable: false,
                      },
                      threadDisposition: "reusable",
                    },
              );
              yield* Fiber.join(ended);
              yield* worker.drain();

              const afterDrain = yield* orchestrator.getThreadProjection(threadId);
              const steeredRun = afterDrain.runs.find((run) => run.userMessageId === messageId);
              const message = afterDrain.messages.filter((item) => item.id === messageId);
              assert.lengthOf(message, 1);
              assert.equal(steerCalls, timing === "after delivery" ? 1 : 0);

              if (timing === "after delivery") {
                // The provider took the steer into the turn: nothing is sent again.
                assert.deepEqual(startedMessageIds, ["message:first"]);
                assert.equal(message[0]?.runId, firstRun.id);
                assert.isUndefined(steeredRun);
                return;
              }
              if (ending === "failed") {
                assert.deepEqual(startedMessageIds, ["message:first", messageId]);
                assert.equal(message[0]?.runId, steeredRun?.id);
                return;
              }
              // Stopped by the user: the message waits, held, until the user resumes.
              assert.deepEqual(startedMessageIds, ["message:first"]);
              assert.equal(steeredRun?.status, "queued");
              assert.isTrue(steeredRun?.queueHeld);
              assert.equal(message[0]?.runId, steeredRun?.id);
              yield* orchestrator.dispatch({
                type: "queue.resume",
                commandId: CommandId.make("resume"),
                threadId,
              });
              yield* orchestrator.resumeQueuedRuns;
              yield* worker.drain();
              assert.deepEqual(startedMessageIds, ["message:first", messageId]);
            }).pipe(
              Effect.provide(
                makeOrchestratorV2ReplayLayerWithRegistry(
                  { name: `steer-ended-${ending}-${timing}` },
                  ProviderAdapterRegistry.makeSingleLayer(adapter),
                  { runEffectWorker: false },
                ),
              ),
            );
          }),
        ),
    );
  }
}
