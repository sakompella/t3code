import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CheckpointId,
  EnvironmentId,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ChatAttachment,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { handoffBudget } from "@t3tools/provider-core/server/handoffBudget";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { toolOutputImages, compactDynamicToolOutput } from "@t3tools/shared/toolOutput";
import {
  makePiAdapterV2,
  PiAdapterV2Driver,
  PI_PROVIDER,
  PrimeAgentAdapterV2Driver,
  type PiAdapterV2Options,
} from "./adapter.ts";
import { makePiRpcConnection, type PiRpcRecord } from "./rpc.ts";
import { loadMcpBridge } from "./mcpBridge.testkit.ts";
import { turnItemOutputText } from "../../../client-runtime/src/work-log/itemDetail.ts";
import { PI_FLAVOR, PRIME_AGENT_FLAVOR, type PiFlavor } from "./flavor.ts";

const layerTest = Layer.mergeAll(
  NodeServices.layer,
  IdAllocator.layer,
  McpProviderSessions.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
);

const decodeJsonLine = Schema.decodeSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const PI_INSTANCE_ID = ProviderInstanceId.make("pi");
const THREAD_ID = ThreadId.make("thread-pi-test");
const SESSION_ID = ProviderSessionId.make("provider-session-pi-test");
const FAKE_SESSION_FILE = "/fake/.pi/agent/sessions/--workspace--/0001_abc.jsonl";
/** Deliberately outside the valid pid range so a group-kill can never land. */
const FAKE_PID = 999_999_999;

const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: null,
});

const modelSelection = (model: string): ModelSelection => ({
  instanceId: PI_INSTANCE_ID,
  model,
});

interface FakePi {
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly emit: (record: PiRpcRecord, chunkBytes?: number) => Effect.Effect<void>;
  readonly takeRequest: (type: string) => Effect.Effect<PiRpcRecord>;
  readonly queueEntries: (data: unknown) => void;
  /** Data returned by the next active-branch `get_messages` acks, ahead of the transcript. */
  readonly queueMessages: (data: unknown) => void;
  /**
   * Prime Agent's stored conversation from now on: what `get_messages` returns
   * and what `get_state` counts, unless a queued reply overrides one read.
   */
  readonly setTranscript: (messages: ReadonlyArray<unknown>) => void;
  /** Data returned by the next `get_fork_messages` acks, consumed in order. */
  readonly queueForkMessages: (data: unknown) => void;
  readonly vetoNextSwitch: () => void;
  /** Refuse the next `count` `switch_session` requests with `message`, as Prime Agent does while another worker holds the lease. */
  readonly rejectNextSwitches: (count: number, message: string) => void;
  readonly queueState: (data: Record<string, unknown>) => void;
  readonly setStreaming: (streaming: boolean) => void;
  readonly deferNextState: () => void;
  readonly resolveDeferredState: (data: unknown) => Effect.Effect<void>;
  readonly failNextState: () => void;
  readonly deferNextLifecycle: (type: "switch_session" | "new_session" | "fork") => void;
  readonly deferNextModelSelection: () => void;
  readonly queueModels: (models: ReadonlyArray<unknown>) => void;
  readonly vetoNextNewSession: () => void;
  readonly allRequests: () => ReadonlyArray<PiRpcRecord>;
  /** History returned by the next `observe` ack. */
  readonly queueObserved: (messages: ReadonlyArray<unknown>) => void;
  readonly queueStats: (data: unknown) => void;
  /** Data returned by the next `get_commands` acks, consumed in order. */
  readonly queueCommands: (data: unknown) => void;
  /** Make the next `get_commands` ack fail. */
  readonly failNextCommands: () => void;
  /** Data returned by the next `list_heartbeats` acks, consumed in order; none left means no heartbeats. */
  readonly queueHeartbeats: (data: unknown) => void;
  /** Make the next `list_heartbeats` ack fail. */
  readonly failNextHeartbeats: () => void;
  readonly closeStdout: Effect.Effect<void>;
  readonly lastSpawn: () => {
    readonly args: ReadonlyArray<string>;
    readonly env: NodeJS.ProcessEnv;
  };
}

/**
 * Pi 1.0.0's idle `get_state` reply, taken from the `simple` replay fixture
 * (fixtures/simple/pi_transcript.ndjson) minus the model object. Pi omits
 * `model` when none is selected and `sessionName` until one is set.
 */
const recordedIdleState = (sessionFile: string) => ({
  thinkingLevel: "high",
  isStreaming: false,
  isCompacting: false,
  steeringMode: "one-at-a-time",
  followUpMode: "one-at-a-time",
  sessionFile,
  sessionId: "00000000-0000-4000-8000-000000000002",
  autoCompactionEnabled: true,
  messageCount: 0,
  pendingMessageCount: 0,
});

const makeFakePi: Effect.Effect<FakePi> = Effect.gen(function* () {
  const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const requests = yield* Queue.unbounded<PiRpcRecord>();
  const entriesQueue: Array<unknown> = [];
  const messagesQueue: Array<unknown> = [];
  const forkMessagesQueue: Array<unknown> = [];
  const stateQueue: Array<Record<string, unknown>> = [];
  const statsQueue: Array<unknown> = [];
  const observedQueue: Array<ReadonlyArray<unknown>> = [];
  const commandsQueue: Array<{ readonly success: boolean; readonly data?: unknown }> = [];
  const heartbeatsQueue: Array<{ readonly success: boolean; readonly data?: unknown }> = [];
  const allRequests: Array<PiRpcRecord> = [];
  let deferState = false;
  let deferredStateRequest: PiRpcRecord | undefined;
  let failState = false;
  let vetoSwitch = false;
  let rejectedSwitches = { remaining: 0, message: "" };
  let vetoNewSession = false;
  let deferredLifecycle: string | undefined;
  let sessionFile = FAKE_SESSION_FILE;
  let sessionGeneration = 0;
  let models: ReadonlyArray<unknown> = [];
  let isStreaming: boolean | null = null;
  let transcript: ReadonlyArray<unknown> = [];
  let stdinBuffer = "";

  const emit = (record: PiRpcRecord, chunkBytes?: number) =>
    Effect.gen(function* () {
      const bytes = new TextEncoder().encode(`${encodeJsonLine(record)}\n`);
      const size = chunkBytes ?? bytes.byteLength;
      for (let offset = 0; offset < bytes.byteLength; offset += size) {
        yield* Queue.offer(stdout, bytes.subarray(offset, offset + size));
      }
    });

  const respondTo = (record: PiRpcRecord): PiRpcRecord | null => {
    if (typeof record["id"] !== "string" && !(record["type"] === "prompt" && isStreaming !== null))
      return null;
    const base = {
      type: "response",
      id: record["id"],
      command: String(record["type"]),
      success: true,
    };
    switch (record["type"]) {
      case "get_state":
        if (failState) {
          failState = false;
          return { ...base, success: false, error: "state unavailable" };
        }
        return {
          ...base,
          data: {
            ...recordedIdleState(sessionFile),
            isStreaming: isStreaming ?? false,
            messageCount: transcript.length,
            ...stateQueue.shift(),
          },
        };
      case "prompt":
        if (isStreaming && record["streamingBehavior"] === undefined) {
          return {
            ...base,
            success: false,
            error:
              "Agent is already processing. Specify streamingBehavior ('steer' or 'followUp') to queue the message.",
          };
        }
        return base;
      case "get_available_models":
        return { ...base, data: { models } };
      case "new_session": {
        const cancelled = vetoNewSession;
        vetoNewSession = false;
        if (!cancelled) sessionFile = `/fake/new-${++sessionGeneration}.jsonl`;
        return { ...base, data: { cancelled } };
      }
      case "switch_session": {
        if (rejectedSwitches.remaining > 0) {
          rejectedSwitches.remaining -= 1;
          return { ...base, success: false, error: rejectedSwitches.message };
        }
        const cancelled = vetoSwitch;
        vetoSwitch = false;
        return { ...base, data: { cancelled } };
      }
      case "get_entries":
        return { ...base, data: entriesQueue.shift() ?? { entries: [], leafId: null } };
      case "get_messages":
        return { ...base, data: messagesQueue.shift() ?? { messages: transcript } };
      case "get_fork_messages":
        return { ...base, data: forkMessagesQueue.shift() ?? { messages: [] } };
      case "observe":
        return { ...base, data: { messages: observedQueue.shift() ?? [] } };
      case "get_session_stats":
        return { ...base, data: statsQueue.shift() ?? {} };
      case "get_commands":
        return { ...base, ...(commandsQueue.shift() ?? { data: { commands: [] } }) };
      case "list_heartbeats":
        return { ...base, ...(heartbeatsQueue.shift() ?? { data: { heartbeats: [] } }) };
      case "fork":
        return { ...base, data: { text: "Hello pi", cancelled: false } };
      default:
        return base;
    }
  };

  const handleStdinChunk = (chunk: Uint8Array) =>
    Effect.gen(function* () {
      stdinBuffer += new TextDecoder().decode(chunk);
      while (true) {
        const newline = stdinBuffer.indexOf("\n");
        if (newline === -1) return;
        const line = stdinBuffer.slice(0, newline);
        stdinBuffer = stdinBuffer.slice(newline + 1);
        if (line.length === 0) continue;
        const record = decodeJsonLine(line) as PiRpcRecord;
        allRequests.push(record);
        yield* Queue.offer(requests, record);
        if (record["type"] === "get_state" && deferState) {
          deferState = false;
          deferredStateRequest = record;
          continue;
        }
        if (record["type"] === deferredLifecycle) {
          deferredLifecycle = undefined;
          continue;
        }
        const response = respondTo(record);
        if (response !== null) yield* emit(response);
      }
    });

  let lastSpawn: { readonly args: ReadonlyArray<string>; readonly env: NodeJS.ProcessEnv } = {
    args: [],
    env: {},
  };
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.sync(() => {
      if (ChildProcess.isStandardCommand(command)) {
        lastSpawn = {
          args: command.args,
          env: command.options.env ?? {},
        };
      }
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(FAKE_PID),
        exitCode: Effect.never,
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach(handleStdinChunk),
        stdout: Stream.fromQueue(stdout),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );

  const takeRequest = (type: string): Effect.Effect<PiRpcRecord> =>
    Effect.gen(function* () {
      while (true) {
        const record = yield* Queue.take(requests);
        if (record["type"] === type) return record;
      }
    });

  return {
    spawner,
    emit,
    takeRequest,
    queueEntries: (data) => entriesQueue.push(data),
    queueMessages: (data) => messagesQueue.push(data),
    setTranscript: (messages) => {
      transcript = messages;
    },
    queueForkMessages: (data) => forkMessagesQueue.push(data),
    deferNextState: () => {
      deferState = true;
    },
    resolveDeferredState: (data) =>
      Effect.gen(function* () {
        const record = deferredStateRequest;
        assert.isDefined(record);
        deferredStateRequest = undefined;
        yield* emit({
          type: "response",
          id: record!["id"],
          command: "get_state",
          success: true,
          data,
        });
      }),
    failNextState: () => {
      failState = true;
    },
    deferNextLifecycle: (type) => {
      deferredLifecycle = type;
    },
    deferNextModelSelection: () => {
      deferredLifecycle = "set_model";
    },
    queueModels: (value) => {
      models = value;
    },
    vetoNextNewSession: () => {
      vetoNewSession = true;
    },
    allRequests: () => allRequests,
    vetoNextSwitch: () => {
      vetoSwitch = true;
    },
    rejectNextSwitches: (count, message) => {
      rejectedSwitches = { remaining: count, message };
    },
    queueState: (data) => stateQueue.push(data),
    setStreaming: (streaming) => {
      isStreaming = streaming;
    },
    queueObserved: (messages) => observedQueue.push(messages),
    queueStats: (data) => statsQueue.push(data),
    queueCommands: (data) => commandsQueue.push({ success: true, data }),
    failNextCommands: () => commandsQueue.push({ success: false }),
    queueHeartbeats: (data) => heartbeatsQueue.push({ success: true, data }),
    failNextHeartbeats: () => heartbeatsQueue.push({ success: false }),
    closeStdout: Queue.end(stdout),
    lastSpawn: () => lastSpawn,
  } satisfies FakePi;
});

const makeAdapter = Effect.fnUntraced(function* (
  fake: FakePi,
  launchArgs = "",
  forkFake?: FakePi,
  flavor: PiFlavor = PI_FLAVOR,
  continuationRequests?: PiAdapterV2Options["continuationRequests"],
  environment: NodeJS.ProcessEnv = {},
) {
  const spawner =
    forkFake === undefined
      ? fake.spawner
      : ChildProcessSpawner.make((command) =>
          ChildProcess.isStandardCommand(command) && command.args.includes("--fork")
            ? forkFake.spawner.spawn(command)
            : fake.spawner.spawn(command),
        );
  if (continuationRequests !== undefined) {
    const driver = flavor === PRIME_AGENT_FLAVOR ? PrimeAgentAdapterV2Driver : PiAdapterV2Driver;
    return yield* driver.create({
      instanceId: PI_INSTANCE_ID,
      displayName: undefined,
      enabled: true,
      environment: [],
      config: { enabled: true, binaryPath: "pi", launchArgs, customModels: [] },
    }).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.provideService(HostProcess.Environment, environment),
      Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
        ...continuationRequests,
        take: Effect.never,
      }),
    );
  }
  return yield* makePiAdapterV2({
    flavor,
    instanceId: PI_INSTANCE_ID,
    settings: { enabled: true, binaryPath: "pi", launchArgs, customModels: [] },
    environment,
  }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
});

const openRuntime = Effect.fnUntraced(function* (
  fake: FakePi,
  model = "default",
  threadId = THREAD_ID,
  providerSessionId = SESSION_ID,
  forkFake?: FakePi,
  flavor: PiFlavor = PI_FLAVOR,
  continuationRequests?: PiAdapterV2Options["continuationRequests"],
  environment: NodeJS.ProcessEnv = {},
  initialNativeThreadId?: string,
) {
  const adapter = yield* makeAdapter(fake, "", forkFake, flavor, continuationRequests, environment);
  const runtime = yield* adapter.openSession({
    threadId,
    providerSessionId,
    modelSelection: modelSelection(model),
    runtimePolicy,
    ...(initialNativeThreadId === undefined ? {} : { initialNativeThreadId }),
  });
  const emitted = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  yield* runtime.events.pipe(
    Stream.runForEach((event) => Queue.offer(emitted, event)),
    Effect.forkScoped,
  );
  const takeEvent = (predicate: (event: ProviderAdapter.ProviderAdapterV2Event) => boolean) =>
    Effect.gen(function* () {
      while (true) {
        const event = yield* Queue.take(emitted);
        if (predicate(event)) return event;
      }
    });
  return { runtime, takeEvent };
});

const makeAppThread = Effect.fnUntraced(function* (model: string, threadId = THREAD_ID) {
  const now = yield* DateTime.now;
  return {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: "project:fixture:pi" as OrchestrationV2AppThread["projectId"],
    title: "Pi test thread",
    providerInstanceId: PI_INSTANCE_ID,
    modelSelection: modelSelection(model),
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  } satisfies OrchestrationV2AppThread;
});

const startTurn = Effect.fnUntraced(function* (
  runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
  providerThread: OrchestrationV2ProviderThread,
  model = "default",
  attachments: ReadonlyArray<ChatAttachment> = [],
  text = "Hello pi",
  selection?: ModelSelection,
  runOrdinal = 1,
  threadId = THREAD_ID,
  continuation = false,
) {
  const appThread = yield* makeAppThread(model, threadId);
  const runId = RunId.make(`run:${threadId}:${runOrdinal}`);
  yield* runtime.startTurn({
    appThread,
    threadId,
    runId,
    runOrdinal,
    providerTurnOrdinal: runOrdinal,
    attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
    rootNodeId: NodeId.make(`node:${runId}:root`),
    providerThread,
    message: {
      messageId: `message:${threadId}:${runOrdinal}` as never,
      text,
      attachments,
      createdBy: continuation ? "agent" : "user",
      creationSource: continuation ? "provider" : "web",
    },
    modelSelection: selection ?? modelSelection(model),
    runtimePolicy,
  });
});

const expectModelFailure = (errorMessage: string) =>
  Effect.gen(function* () {
    const fake = yield* makeFakePi;
    const { runtime, takeEvent } = yield* openRuntime(fake);
    const providerThread = yield* runtime.ensureThread({
      threadId: THREAD_ID,
      modelSelection: modelSelection("default"),
      runtimePolicy,
    });
    yield* startTurn(runtime, providerThread);
    yield* fake.takeRequest("prompt");
    yield* fake.emit({ type: "agent_start" });
    yield* fake.emit({
      type: "message_end",
      message: {
        role: "assistant",
        content: [],
        stopReason: "error",
        errorMessage,
      },
    });
    yield* fake.emit({ type: "agent_settled" });

    const sessionError = yield* takeEvent(
      (event) =>
        event.type === "provider_session.updated" && event.providerSession.status === "error",
    );
    assert.isTrue(
      sessionError.type === "provider_session.updated" &&
        sessionError.providerSession.lastError === errorMessage,
    );
    const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
    assert.isTrue(
      terminal.type === "turn.terminal" &&
        terminal.status === "failed" &&
        terminal.failure.message === errorMessage,
    );
  }).pipe(Effect.scoped, Effect.provide(layerTest));

describe("PiAdapterV2", () => {
  it.effect.each([false, true])(
    "completes a resumed user turn before native wake events arrive, Pi streaming=%s",
    (streaming) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.setStreaming(streaming);
        yield* runtime.resumeThread({ providerThread });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({
          type: "message_end",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "Resumed prompt completed" }],
            stopReason: "stop",
          },
        });
        fake.queueState({ isStreaming: false });
        yield* fake.emit({ type: "agent_settled" });
        const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([false, true])(
    "serializes rollback fork hooks with native wake ownership, wake=%s",
    (nativeWake) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          PI_FLAVOR,
          {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.deferNextLifecycle("fork");
        const rollingBack = yield* runtime
          .rollbackThread({
            providerThread,
            target: {
              type: "thread_start",
              checkpointId: CheckpointId.make("checkpoint-start"),
              appRunOrdinal: 0,
            },
            providerThreadTurns: [
              {
                id: ProviderTurnId.make("previous-turn"),
                providerThreadId: providerThread.id,
                nodeId: NodeId.make("previous-node"),
                runAttemptId: null,
                nativeTurnRef: {
                  driver: PI_PROVIDER,
                  nativeId: "user-before-wake",
                  strength: "strong",
                },
                ordinal: 1,
                status: "completed",
                startedAt: providerThread.createdAt,
                completedAt: providerThread.createdAt,
              },
            ],
          })
          .pipe(Effect.result, Effect.forkScoped);
        const fork = yield* fake.takeRequest("fork");
        const overlappingTurn = yield* startTurn(runtime, providerThread).pipe(Effect.result);
        assert.equal(overlappingTurn._tag, "Failure");
        const overlappingRegistration = yield* runtime
          .ensureThread({
            threadId: THREAD_ID,
            modelSelection: modelSelection("default"),
            runtimePolicy,
          })
          .pipe(Effect.result);
        assert.equal(overlappingRegistration._tag, "Failure");
        yield* fake.emit({
          type: "extension_ui_request",
          id: "fork-hook-dialog",
          method: "confirm",
          title: "Restore this branch?",
        });
        const forkDialog = yield* takeEvent((event) => event.type === "runtime_request.updated");
        assert.isTrue(forkDialog.type === "runtime_request.updated");
        if (forkDialog.type !== "runtime_request.updated") return;
        assert.isNull(forkDialog.runtimeRequest.providerTurnId);
        yield* runtime.respondToRuntimeRequest({
          requestId: forkDialog.runtimeRequest.id,
          decision: "accept",
        });
        yield* fake.takeRequest("extension_ui_response");
        if (nativeWake) {
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
          yield* fake.emit({
            type: "message_update",
            assistantMessageEvent: {
              type: "text_end",
              contentIndex: 0,
              content: "Discarded branch wake output",
            },
          });
          yield* fake.emit({ type: "agent_settled" });
          const retired = yield* takeEvent(
            (event) =>
              event.type === "provider_session.updated" &&
              event.providerSession.status === "stopped",
          ).pipe(Effect.as(true), Effect.raceFirst(Queue.take(offers).pipe(Effect.as(false))));
          assert.isTrue(retired, "Pi offered native work while rollback was changing its binding");
          const result = yield* Fiber.join(rollingBack);
          assert.equal(result._tag, "Failure");
          assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
          const staleTurn = yield* startTurn(
            runtime,
            providerThread,
            "default",
            [],
            "Synthetic wake",
            undefined,
            1,
            THREAD_ID,
            true,
          ).pipe(Effect.result);
          assert.equal(staleTurn._tag, "Failure");
          assert.isFalse(fake.allRequests().some((request) => request["type"] === "prompt"));
          return;
        }
        fake.queueState({ sessionFile: "/fake/rolled-back-session.jsonl" });
        yield* fake.emit({
          type: "response",
          id: fork["id"],
          command: "fork",
          success: true,
          data: { cancelled: false },
        });
        const result = yield* Fiber.join(rollingBack);
        assert.equal(result._tag, "Success");
        if (result._tag !== "Success") return;
        assert.equal(
          result.success.providerThread.nativeThreadRef?.nativeId,
          "/fake/rolled-back-session.jsonl",
        );
        yield* startTurn(runtime, result.success.providerThread);
        const prompt = yield* fake.takeRequest("prompt");
        assert.equal(prompt["message"], "Hello pi");
        yield* fake.emit({ type: "response", command: "prompt", success: true });
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "agent_settled" });
        const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([false, true])(
    "retains native wake ownership after a rejected joining prompt, compaction aborted=%s",
    (aborted) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          PI_FLAVOR,
          {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* fake.takeRequest("get_state");
        yield* fake.emit({ type: "agent_start" });
        yield* Queue.take(offers);
        yield* fake.emit({ type: "agent_settled" });
        yield* fake.emit({ type: "compaction_start", reason: "manual" });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        const rejection = "Cannot submit a prompt while compaction is in progress.";
        yield* fake.emit({ type: "response", command: "prompt", success: false, error: rejection });
        yield* fake.emit({
          type: "extension_ui_request",
          id: "native-compaction-dialog",
          method: "confirm",
          title: "Continue native work?",
        });
        const dialog = yield* takeEvent((event) => event.type === "runtime_request.updated");
        assert.isTrue(dialog.type === "runtime_request.updated");
        if (dialog.type !== "runtime_request.updated") return;
        assert.isNotNull(dialog.runtimeRequest.providerTurnId);
        yield* runtime.respondToRuntimeRequest({
          requestId: dialog.runtimeRequest.id,
          decision: "accept",
        });
        yield* fake.takeRequest("extension_ui_response");
        if (aborted) {
          yield* fake.emit({ type: "compaction_end", result: null, aborted: true });
          const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
          assert.isTrue(
            terminal.type === "turn.terminal" &&
              terminal.status === "failed" &&
              terminal.failure.message === rejection,
          );
          return;
        }
        fake.queueState({ isStreaming: true });
        yield* fake.emit({
          type: "compaction_end",
          result: { summary: "Native compaction finished" },
          willRetry: true,
        });
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
        yield* fake.emit({
          type: "message_update",
          assistantMessageEvent: {
            type: "text_end",
            contentIndex: 0,
            content: "Native work completed",
          },
        });
        const message = yield* takeEvent((event) => event.type === "message.updated");
        assert.isTrue(message.type === "message.updated" && message.message.runId !== null);
        yield* fake.takeRequest("get_state");
        yield* fake.emit({ type: "agent_settled" });
        const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(
          terminal.type === "turn.terminal" &&
            terminal.status === "failed" &&
            terminal.failure.message === rejection,
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("rejects rollback while native wake output is awaiting ownership", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        undefined,
        PI_FLAVOR,
        {
          offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
        },
      );
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({ type: "agent_start" });
      yield* Queue.take(offers);
      yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: {
          type: "text_end",
          contentIndex: 0,
          content: "Current branch wake output",
        },
      });
      yield* fake.emit({ type: "agent_settled" });
      yield* takeEvent(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "ready",
      );
      const rolledBack = yield* runtime
        .rollbackThread({
          providerThread,
          target: {
            type: "thread_start",
            checkpointId: CheckpointId.make("checkpoint-start"),
            appRunOrdinal: 0,
          },
          providerThreadTurns: [
            {
              id: ProviderTurnId.make("previous-turn"),
              providerThreadId: providerThread.id,
              nodeId: NodeId.make("previous-node"),
              runAttemptId: null,
              nativeTurnRef: {
                driver: PI_PROVIDER,
                nativeId: "user-before-wake",
                strength: "strong",
              },
              ordinal: 1,
              status: "completed",
              startedAt: providerThread.createdAt,
              completedAt: providerThread.createdAt,
            },
          ],
        })
        .pipe(Effect.result);
      assert.equal(
        rolledBack._tag,
        "Failure",
        "Rollback changed the native session while its wake was still buffered",
      );
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "fork"));
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      yield* startTurn(
        runtime,
        providerThread,
        "default",
        [],
        "Synthetic wake",
        undefined,
        1,
        THREAD_ID,
        true,
      );
      const message = yield* takeEvent((event) => event.type === "message.updated");
      assert.equal(
        message.type === "message.updated" && message.message.text,
        "Current branch wake output",
      );
      yield* takeEvent((event) => event.type === "turn.terminal");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("answers model-selection dialogs before a user turn claims a native wake", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        undefined,
        PI_FLAVOR,
        { offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid) },
      );
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({ type: "agent_start" });
      yield* Queue.take(offers);
      const requests: Array<
        Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "runtime_request.updated" }>
      > = [];
      fake.deferNextModelSelection();
      const starting = yield* startTurn(
        runtime,
        providerThread,
        "default",
        [],
        "Continue the review",
        modelSelection("xai/grok-4.6"),
      ).pipe(Effect.forkScoped);
      const modelRequest = yield* fake.takeRequest("set_model");
      yield* fake.emit({
        type: "extension_ui_request",
        id: "model-selection-dialog",
        method: "confirm",
        title: "Use this model?",
      });
      yield* fake.emit({ type: "agent_settled" });
      yield* takeEvent((event) => {
        if (event.type === "runtime_request.updated") requests.push(event);
        return (
          event.type === "provider_session.updated" && event.providerSession.status === "ready"
        );
      });
      const dialog = requests[0]?.runtimeRequest;
      assert.isDefined(dialog, "Pi buffered the dialog needed to finish model selection");
      assert.equal(requests[0]?.threadId, THREAD_ID);
      assert.isNull(dialog!.providerTurnId);
      yield* runtime.respondToRuntimeRequest({ requestId: dialog!.id, decision: "accept" });
      const response = yield* fake.takeRequest("extension_ui_response");
      assert.equal(response["id"], "model-selection-dialog");
      assert.isTrue(response["confirmed"]);
      yield* fake.emit({
        type: "response",
        id: modelRequest["id"],
        command: "set_model",
        success: true,
        data: { provider: "xai", id: "grok-4.6" },
      });
      yield* Fiber.join(starting);
      const prompt = yield* fake.takeRequest("prompt");
      assert.equal(prompt["message"], "Continue the review");
      assert.equal(prompt["streamingBehavior"], "steer");
      const resolved = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.id === dialog!.id,
      );
      assert.isTrue(
        resolved.type === "runtime_request.updated" &&
          resolved.runtimeRequest.status === "resolved",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("adopts a completed native wake without sending a synthetic prompt", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        undefined,
        PI_FLAVOR,
        {
          offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
        },
      );
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({ type: "agent_start" });
      const offer = yield* Queue.take(offers).pipe(
        Effect.raceFirst(
          takeEvent(
            (event) =>
              event.type === "provider_session.updated" && event.providerSession.status === "error",
          ).pipe(
            Effect.andThen(Effect.sync(() => assert.fail("Pi killed a routable native wake"))),
          ),
        ),
      );
      assert.equal(offer.threadId, THREAD_ID);
      assert.equal(offer.providerThreadId, providerThread.id);
      assert.equal(offer.delivery, "adapter_buffered");
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      let dispatchCommitted = false;
      yield* offer.dispatchIfCurrent!(
        Effect.sync(() => {
          dispatchCommitted = true;
        }),
      );
      assert.isTrue(dispatchCommitted);
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: {
          type: "text_delta",
          contentIndex: 0,
          delta: "Background review finished",
        },
      });
      yield* fake.emit({ type: "message_end", message: { role: "assistant" } });
      yield* fake.emit({ type: "agent_settled" });
      yield* takeEvent(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "ready",
      );
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      yield* startTurn(
        runtime,
        providerThread,
        "default",
        [],
        "Synthetic wake",
        undefined,
        1,
        THREAD_ID,
        true,
      );
      const message = yield* takeEvent((event) => event.type === "message.updated");
      assert.equal(
        message.type === "message.updated" && message.message.text,
        "Background review finished",
      );
      yield* takeEvent((event) => event.type === "turn.terminal");
      assert.equal(fake.allRequests().filter((request) => request["type"] === "prompt").length, 0);
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
      let dispatched = false;
      yield* offer.dispatchIfCurrent!(
        Effect.sync(() => {
          dispatched = true;
        }),
      );
      assert.isFalse(dispatched);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([false, true])(
    "a user prompt takes the wake without premature settlement, native wake settled=%s",
    (settled) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          PI_FLAVOR,
          {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* fake.emit({ type: "agent_start" });
        const offer = yield* Queue.take(offers);
        if (settled) {
          yield* fake.emit({ type: "agent_settled" });
          yield* takeEvent(
            (event) =>
              event.type === "provider_session.updated" && event.providerSession.status === "ready",
          );
        }
        yield* startTurn(runtime, providerThread);
        const prompt = yield* fake.takeRequest("prompt");
        assert.equal(prompt["message"], "Hello pi");
        assert.equal(prompt["streamingBehavior"], "steer");
        yield* fake.emit({ type: "response", command: "prompt", success: true });
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({
          type: "extension_ui_request",
          id: "owned-dialog",
          method: "confirm",
          title: "Continue?",
        });
        const dialog = yield* takeEvent((event) => event.type === "runtime_request.updated");
        assert.isTrue(dialog.type === "runtime_request.updated");
        if (dialog.type !== "runtime_request.updated") return;
        assert.isNotNull(dialog.runtimeRequest.providerTurnId);
        yield* offer.clearIfCurrent!();
        yield* runtime.respondToRuntimeRequest({
          requestId: dialog.runtimeRequest.id,
          decision: "accept",
        });
        const response = yield* fake.takeRequest("extension_ui_response");
        assert.isTrue(response["confirmed"]);
        yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
        yield* fake.emit({
          type: "message_update",
          assistantMessageEvent: {
            type: "text_end",
            contentIndex: 0,
            content: "User prompt completed",
          },
        });
        yield* fake.emit({ type: "agent_settled" });
        const message = yield* takeEvent(
          (event) =>
            event.type === "message.updated" && event.message.text === "User prompt completed",
        );
        assert.isTrue(message.type === "message.updated" && message.message.runId !== null);
        yield* takeEvent((event) => event.type === "turn.terminal");
        let dispatched = false;
        yield* offer.dispatchIfCurrent!(
          Effect.sync(() => {
            dispatched = true;
          }),
        );
        assert.isFalse(dispatched);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([false, true])(
    "defers a wake's late settlement until the joined user prompt is acknowledged, aborted=%s",
    (aborted) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          PI_FLAVOR,
          {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* fake.emit({ type: "agent_start" });
        yield* Queue.take(offers);
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_settled", aborted });
        yield* fake.emit({
          type: "extension_ui_request",
          method: "notify",
          message: "Ordering barrier",
        });
        yield* takeEvent(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "dynamic_tool" &&
            event.turnItem.toolName === "notify",
        );
        assert.equal(runtime.providerSession.status, "running");
        fake.queueState({ isStreaming: true });
        yield* fake.emit({ type: "response", command: "prompt", success: true });
        yield* fake.takeRequest("get_state");
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "agent_settled" });
        const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { aborted: false, resumes: false, expected: "completed" },
    { aborted: true, resumes: false, expected: "interrupted" },
    { aborted: true, resumes: true, expected: "completed" },
  ])(
    "settles a completed wake, aborted=$aborted, resumes=$resumes",
    ({ aborted, resumes, expected }) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          PI_FLAVOR,
          {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* fake.emit({ type: "agent_start" });
        yield* Queue.take(offers);
        yield* fake.emit({ type: "agent_settled", aborted });
        yield* takeEvent(
          (event) =>
            event.type === "provider_session.updated" && event.providerSession.status === "ready",
        );
        yield* startTurn(runtime, providerThread, "default", [], "/hello");
        yield* fake.takeRequest("prompt");
        if (resumes) yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "response", command: "prompt", success: true });
        if (resumes) yield* fake.emit({ type: "agent_settled", aborted: false });
        const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === expected);
        yield* startTurn(
          runtime,
          providerThread,
          "default",
          [],
          "Stale continuation",
          undefined,
          2,
          THREAD_ID,
          true,
        );
        yield* takeEvent((event) => event.type === "turn.terminal");
        assert.equal(
          fake.allRequests().filter((request) => request["type"] === "prompt").length,
          1,
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a pending wake on its thread and lets Stop retire its process", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        undefined,
        PI_FLAVOR,
        {
          offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
        },
      );
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({ type: "agent_start" });
      const offer = yield* Queue.take(offers);
      assert.isTrue(yield* runtime.hasPendingBackgroundWorkForThread!(providerThread));
      const otherThread = {
        ...providerThread,
        appThreadId: ThreadId.make("other-thread"),
        id: ProviderThreadId.make("other-provider-thread"),
      };
      assert.isFalse(yield* runtime.hasPendingBackgroundWorkForThread!(otherThread));
      const failure = yield* runtime
        .resumeThread({ providerThread: otherThread })
        .pipe(Effect.flip);
      assert.equal(failure._tag, "ProviderAdapterResumeThreadError");
      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: ProviderTurnId.make("settled-turn"),
        requestRuntimeRestart: true,
      });
      yield* takeEvent(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "stopped",
      );
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
      let dispatched = false;
      yield* offer.dispatchIfCurrent!(
        Effect.sync(() => {
          dispatched = true;
        }),
      );
      assert.isFalse(dispatched);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each(["archive", "stop", "transport death"] as const)(
    "retiring a wake by %s cancels visible dialogs and invalidates its offer",
    (retirement) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const { runtime, takeEvent } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          PI_FLAVOR,
          {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* fake.emit({ type: "agent_start" });
        const offer = yield* Queue.take(offers);
        yield* fake.emit({
          type: "extension_ui_request",
          id: "wake-dialog",
          method: "confirm",
          title: "Continue background work?",
        });
        const dialog = yield* takeEvent((event) => event.type === "runtime_request.updated");
        assert.isTrue(dialog.type === "runtime_request.updated");
        if (dialog.type !== "runtime_request.updated") return;
        assert.isNull(dialog.runtimeRequest.providerTurnId);
        if (retirement === "archive") yield* offer.clearIfCurrent!();
        else if (retirement === "stop") {
          yield* runtime.interruptTurn({
            providerThread,
            providerTurnId: ProviderTurnId.make("settled-turn"),
            requestRuntimeRestart: true,
          });
        } else yield* fake.closeStdout;
        let requestCancelled = false;
        let nodeCancelled = false;
        let itemCancelled = false;
        yield* takeEvent((event) => {
          if (
            event.type === "runtime_request.updated" &&
            event.runtimeRequest.id === dialog.runtimeRequest.id
          )
            requestCancelled = event.runtimeRequest.status === "cancelled";
          if (event.type === "node.updated" && event.node.id === dialog.runtimeRequest.nodeId)
            nodeCancelled = event.node.status === "cancelled";
          if (
            event.type === "turn_item.updated" &&
            event.turnItem.nodeId === dialog.runtimeRequest.nodeId
          )
            itemCancelled = event.turnItem.status === "cancelled";
          return (
            event.type === "provider_session.updated" &&
            event.providerSession.status ===
              (retirement === "transport death" ? "error" : "stopped")
          );
        });
        assert.isTrue(requestCancelled, "Retired wake left a pending runtime request");
        assert.isTrue(nodeCancelled);
        assert.isTrue(itemCancelled);
        assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
        let dispatched = false;
        yield* offer.dispatchIfCurrent!(
          Effect.sync(() => {
            dispatched = true;
          }),
        );
        assert.isFalse(dispatched);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("does not ask Pi for heartbeats, which only Prime Agent has", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });

      assert.isFalse(fake.allRequests().some((request) => request["type"] === "list_heartbeats"));
      assert.isUndefined(providerThread.heartbeats);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("stops provider-initiated work that has no T3 turn owner", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });

      yield* fake.emit({ type: "agent_start" });

      const sessionError = yield* takeEvent(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "error",
      );
      assert.isTrue(
        sessionError.type === "provider_session.updated" &&
          sessionError.providerSession.lastError?.includes("invisible tool execution") === true,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("injects the T3 MCP extension and bearer when a session exists", () =>
    Effect.gen(function* () {
      yield* (yield* McpProviderSessions.McpProviderSessions).set({
        environmentId: EnvironmentId.make("environment-pi-mcp"),
        threadId: THREAD_ID,
        providerSessionId: "mcp-session-pi",
        providerInstanceId: PI_INSTANCE_ID,
        endpoint: "http://127.0.0.1:43123/mcp",
        authorizationHeader: "Bearer secret-pi-token",
        browserToolsAvailable: true,
      });
      const fake = yield* makeFakePi;
      yield* openRuntime(fake);
      const spawn = fake.lastSpawn();
      assert.isTrue(spawn.args.includes("--extension"));
      const extensions = spawn.args.flatMap((arg, index) =>
        arg === "--extension" ? [spawn.args[index + 1]] : [],
      );
      assert.isFalse(spawn.args.includes("--no-extensions"));
      assert.isTrue(extensions.some((path) => path?.endsWith("pi-t3-mcp-extension.ts")));
      assert.equal(spawn.env.T3_MCP_URL, "http://127.0.0.1:43123/mcp");
      assert.equal(spawn.env.T3_MCP_BEARER_TOKEN, "secret-pi-token");
      assert.equal(spawn.env.T3_PI_RUNTIME_MODE, "full-access");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("rejects a resume while a turn is active", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const error = yield* runtime.resumeThread({ providerThread }).pipe(Effect.flip);
      assert.equal(error._tag, "ProviderAdapterResumeThreadError");
      assert.match(String(error.cause), /while a turn is active/);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("leaves Pi's retries alone when it replaces the session", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* runtime.resumeThread({ providerThread });
      assert.isTrue(fake.allRequests().some((request) => request.type === "switch_session"));
      assert.isFalse(fake.allRequests().some((request) => request.type === "abort_retry"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("waits for a slow Pi resume without starting a replacement", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.deferNextLifecycle("switch_session");
      const resumed = yield* runtime.resumeThread({ providerThread }).pipe(Effect.forkChild);
      const request = yield* fake.takeRequest("switch_session");
      yield* TestClock.adjust(Duration.millis(16_820));
      yield* fake.emit({
        type: "response",
        id: request.id,
        command: "switch_session",
        success: true,
        data: { cancelled: false },
      });
      assert.equal((yield* Fiber.join(resumed)).nativeThreadRef?.nativeId, FAKE_SESSION_FILE);
      assert.isFalse(fake.allRequests().some((request) => request.type === "new_session"));
      yield* startTurn(runtime, providerThread, "default");
      yield* fake.takeRequest("prompt");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  describe("Prime Agent starts on the thread's own session", () => {
    const LEASE_REFUSAL = `Error: Session is already active in 0123456789ab: ${FAKE_SESSION_FILE}\n`;

    /** What Prime Agent does when it cannot open the session it was told to resume: print the reason and exit. */
    const exitedProcess = (stderr: string) =>
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(FAKE_PID),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: Stream.empty,
        stderr: Stream.fromIterable([new TextEncoder().encode(stderr)]),
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });

    const resumeFile = (args: ReadonlyArray<string>) => {
      const flag = args.indexOf("--resume");
      return flag === -1 ? undefined : args[flag + 1];
    };

    /** `fake`, except that each launch `refuses` (by its argv and 1-based count) exits at once with `stderr`. */
    const refusingLaunches = (
      fake: FakePi,
      stderr: string,
      refuses: (args: ReadonlyArray<string>, launchCount: number) => boolean,
    ) => {
      const spawnedArgs: Array<ReadonlyArray<string>> = [];
      const spawner = ChildProcessSpawner.make((command) => {
        const args = ChildProcess.isStandardCommand(command) ? command.args : [];
        spawnedArgs.push(args);
        return refuses(args, spawnedArgs.length)
          ? Effect.succeed(exitedProcess(stderr))
          : fake.spawner.spawn(command);
      });
      return { fake: { ...fake, spawner }, spawnedArgs };
    };

    /** The thread a previous process left behind, as the orchestrator stores it. */
    const savedThread = Effect.gen(function* () {
      const previous = yield* makeFakePi;
      const { runtime } = yield* openRuntime(
        previous,
        "default",
        THREAD_ID,
        SESSION_ID,
        undefined,
        PRIME_AGENT_FLAVOR,
      );
      return yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
    });

    const openOnSavedSession = (fake: FakePi, flavor: PiFlavor = PRIME_AGENT_FLAVOR) =>
      Effect.gen(function* () {
        const adapter = yield* makeAdapter(fake, "", undefined, flavor);
        return yield* adapter.openSession({
          threadId: THREAD_ID,
          providerSessionId: SESSION_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
          initialNativeThreadId: FAKE_SESSION_FILE,
        });
      });

    const replacementRequests = (fake: FakePi) =>
      fake
        .allRequests()
        .filter(
          (request) => request["type"] === "switch_session" || request["type"] === "new_session",
        );

    /**
     * Runs a launch, advancing the clock by one retry interval each time it
     * reports a held lease. The report is the launch's last step before its
     * retry wait, so the wait is pending when the clock moves.
     */
    const waitOutLaunch = <A, E, R>(launch: Effect.Effect<A, E, R>) =>
      Effect.gen(function* () {
        const leaseRefusals = yield* Queue.unbounded<void>();
        const reportsLeaseRefusal = Logger.make(({ logLevel, message }) => {
          const [text] = message as ReadonlyArray<unknown>;
          if (
            logLevel === "Info" &&
            String(text).endsWith("could not start on the thread's session")
          ) {
            Queue.offerUnsafe(leaseRefusals, undefined);
          }
        });
        const fiber = yield* launch.pipe(
          Effect.provide(Logger.layer([reportsLeaseRefusal])),
          Effect.provideService(References.MinimumLogLevel, "Info"),
          Effect.forkChild,
        );
        while (
          yield* Effect.raceFirst(
            Queue.take(leaseRefusals).pipe(Effect.as(true)),
            Fiber.await(fiber).pipe(Effect.as(false)),
          )
        ) {
          yield* TestClock.adjust(Duration.seconds(1));
        }
        return yield* Fiber.join(fiber);
      });

    it.effect("resumes an existing thread without replacing the process's session", () =>
      Effect.gen(function* () {
        const providerThread = yield* savedThread;
        const fake = yield* makeFakePi;
        const runtime = yield* openOnSavedSession(fake);
        assert.equal(resumeFile(fake.lastSpawn().args), FAKE_SESSION_FILE);

        yield* runtime.resumeThread({ providerThread });
        // A changed model selection loads the thread again.
        const resumed = yield* runtime.resumeThread({
          providerThread,
          modelSelection: modelSelection("other"),
        });

        assert.equal(resumed.nativeThreadRef?.nativeId, FAKE_SESSION_FILE);
        assert.deepEqual(replacementRequests(fake), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("starts a new thread on the session the process created", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          PRIME_AGENT_FLAVOR,
        );
        yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });

        assert.isUndefined(resumeFile(fake.lastSpawn().args));
        assert.deepEqual(replacementRequests(fake), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("leaves Pi, which has no such controllers, to switch sessions", () =>
      Effect.gen(function* () {
        const providerThread = yield* savedThread;
        const fake = yield* makeFakePi;
        const runtime = yield* openOnSavedSession(fake, PI_FLAVOR);

        yield* runtime.resumeThread({ providerThread });

        assert.isUndefined(resumeFile(fake.lastSpawn().args));
        assert.equal(replacementRequests(fake).length, 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("retries the same session until the previous owner releases it", () =>
      Effect.gen(function* () {
        const providerThread = yield* savedThread;
        const fake = yield* makeFakePi;
        const launches = refusingLaunches(
          fake,
          LEASE_REFUSAL,
          (_, launchCount) => launchCount <= 3,
        );

        const runtime = yield* waitOutLaunch(openOnSavedSession(launches.fake));
        const resumed = yield* runtime.resumeThread({ providerThread });

        assert.deepEqual(launches.spawnedArgs.map(resumeFile), Array(4).fill(FAKE_SESSION_FILE));
        assert.equal(resumed.nativeThreadRef?.nativeId, FAKE_SESSION_FILE);
        assert.deepEqual(replacementRequests(fake), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("falls back to a new session when the lease is never released", () =>
      Effect.gen(function* () {
        const providerThread = yield* savedThread;
        const fake = yield* makeFakePi;
        const launches = refusingLaunches(
          fake,
          LEASE_REFUSAL,
          (args) => resumeFile(args) !== undefined,
        );

        // One attempt plus the 15 retries, then a process on its own session.
        const runtime = yield* waitOutLaunch(openOnSavedSession(launches.fake));
        const failed = yield* runtime.resumeThread({ providerThread }).pipe(Effect.flip);
        const replacement = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
          existingProviderThread: { ...providerThread, nativeThreadRef: null },
        });

        assert.equal(
          launches.spawnedArgs.filter((args) => resumeFile(args) !== undefined).length,
          16,
        );
        assert.isUndefined(resumeFile(fake.lastSpawn().args));
        assert.equal(failed._tag, "ProviderAdapterResumeThreadError");
        assert.isDefined(replacement.nativeThreadRef);
        assert.deepEqual(replacementRequests(fake), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("does not retry other launch failures", () =>
      Effect.gen(function* () {
        const providerThread = yield* savedThread;
        const fake = yield* makeFakePi;
        const launches = refusingLaunches(
          fake,
          "Error: Session file is corrupt\n",
          (_, launchCount) => launchCount === 1,
        );

        const runtime = yield* waitOutLaunch(openOnSavedSession(launches.fake));
        const failed = yield* runtime.resumeThread({ providerThread }).pipe(Effect.flip);

        assert.deepEqual(launches.spawnedArgs.map(resumeFile), [FAKE_SESSION_FILE, undefined]);
        assert.equal(failed._tag, "ProviderAdapterResumeThreadError");
        assert.deepEqual(replacementRequests(fake), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    /** Rolls `providerThread` back to its first of two turns, as the checkpoint executor asks. */
    const rollBackSavedThread = (
      runtime: ProviderAdapterV2SessionRuntime,
      providerThread: OrchestrationV2ProviderThread,
    ) => {
      const turn = (ordinal: number): OrchestrationV2ProviderTurn => ({
        id: ProviderTurnId.make(`turn-${ordinal}`),
        providerThreadId: providerThread.id,
        nodeId: NodeId.make(`node-${ordinal}`),
        runAttemptId: null,
        nativeTurnRef: {
          driver: PRIME_AGENT_FLAVOR.driverKind,
          nativeId: `u${ordinal}`,
          strength: "strong",
        },
        ordinal,
        status: "completed",
        startedAt: null,
        completedAt: null,
      });
      return runtime.rollbackThread({
        providerThread,
        target: {
          type: "provider_turn",
          checkpointId: CheckpointId.make("checkpoint-1"),
          appRunOrdinal: 1,
          providerTurn: turn(1),
        },
        providerThreadTurns: [turn(1), turn(2)],
      });
    };

    it.effect("rolls back the saved thread before any turn has loaded it", () =>
      Effect.gen(function* () {
        const providerThread = yield* savedThread;
        const fake = yield* makeFakePi;
        const runtime = yield* openOnSavedSession(fake);

        const snapshot = yield* rollBackSavedThread(runtime, providerThread);

        const fork = fake.allRequests().find((request) => request.type === "fork");
        assert.equal(fork?.["entryId"], "u2");
        assert.equal(snapshot.providerThread.id, providerThread.id);
        assert.deepEqual(replacementRequests(fake), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("fails the rollback when the process could not start on the saved session", () =>
      Effect.gen(function* () {
        const providerThread = yield* savedThread;
        const fake = yield* makeFakePi;
        const launches = refusingLaunches(
          fake,
          "Error: Session file is corrupt\n",
          (_, launchCount) => launchCount === 1,
        );
        const runtime = yield* waitOutLaunch(openOnSavedSession(launches.fake));

        const failed = yield* rollBackSavedThread(runtime, providerThread).pipe(Effect.flip);

        assert.equal(failed._tag, "ProviderAdapterRollbackThreadError");
        assert.include(String(failed.cause), "could not start on session");
        assert.isFalse(fake.allRequests().some((request) => request.type === "fork"));
        assert.deepEqual(replacementRequests(fake), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  describe("resume while the previous owner still holds the session lease", () => {
    const LEASE_HELD =
      "Session is already active in worker-1: /fake/.pi/agent/sessions/0001_abc.jsonl";
    const switchRequests = (fake: FakePi) =>
      fake.allRequests().filter((request) => request["type"] === "switch_session");

    /** Lets a resume run through its retry waits until it finishes or `releases` waits have passed. */
    const waitOutRetries = <A, E>(fake: FakePi, resumed: Fiber.Fiber<A, E>, releases: number) =>
      Effect.gen(function* () {
        for (let waited = 0; waited < releases; waited++) {
          const next = yield* Effect.raceFirst(
            fake.takeRequest("switch_session"),
            Fiber.await(resumed).pipe(Effect.as(undefined)),
          );
          if (next === undefined) return;
          yield* TestClock.adjust(Duration.seconds(1));
        }
      });

    it.effect("resumes the same native session once the lease is released", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.rejectNextSwitches(3, LEASE_HELD);
        const resumed = yield* runtime.resumeThread({ providerThread }).pipe(Effect.forkChild);
        yield* waitOutRetries(fake, resumed, 3);
        const resumedThread = yield* Fiber.join(resumed);
        assert.equal(resumedThread.nativeThreadRef?.nativeId, FAKE_SESSION_FILE);
        assert.equal(switchRequests(fake).length, 4);
        assert.isFalse(fake.allRequests().some((request) => request["type"] === "new_session"));
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("gives up after the retry window with an ordinary resume failure", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.rejectNextSwitches(Number.POSITIVE_INFINITY, LEASE_HELD);
        const resumed = yield* runtime
          .resumeThread({ providerThread })
          .pipe(Effect.flip, Effect.forkChild);
        yield* waitOutRetries(fake, resumed, 100);
        const error = yield* Fiber.join(resumed);
        assert.equal(error._tag, "ProviderAdapterResumeThreadError");
        assert.equal(switchRequests(fake).length, 16);
        assert.isFalse(fake.allRequests().some((request) => request["type"] === "new_session"));
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("stops waiting when the resume is cancelled", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.rejectNextSwitches(Number.POSITIVE_INFINITY, LEASE_HELD);
        const resumed = yield* runtime.resumeThread({ providerThread }).pipe(Effect.forkChild);
        yield* fake.takeRequest("switch_session");
        yield* Fiber.interrupt(resumed);
        yield* TestClock.adjust(Duration.seconds(60));
        assert.equal(switchRequests(fake).length, 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("does not retry other resume failures", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.rejectNextSwitches(1, "Session file not found: /fake/missing.jsonl");
        const resumed = yield* runtime
          .resumeThread({ providerThread })
          .pipe(Effect.flip, Effect.forkChild);
        const error = yield* Fiber.join(resumed);
        assert.equal(error._tag, "ProviderAdapterResumeThreadError");
        assert.equal(switchRequests(fake).length, 1);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });

  it.effect("creates a distinct native session after a failed resume", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.vetoNextSwitch();
      yield* runtime.resumeThread({ providerThread }).pipe(Effect.flip);
      const replacement = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: { ...providerThread, nativeThreadRef: null },
      });
      assert.equal(replacement.id, providerThread.id);
      assert.notEqual(
        replacement.nativeThreadRef?.nativeId,
        providerThread.nativeThreadRef?.nativeId,
      );
      assert.equal(
        fake.allRequests().filter((request) => request.type === "new_session").length,
        1,
      );
      yield* startTurn(runtime, replacement, "default");
      yield* fake.takeRequest("prompt");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each(["veto", "same identity"] as const)(
    "rejects a replacement with %s",
    (invalidReplacement) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        if (invalidReplacement === "veto") fake.vetoNextNewSession();
        else fake.queueState({ sessionFile: FAKE_SESSION_FILE });
        const error = yield* runtime
          .ensureThread({
            threadId: THREAD_ID,
            modelSelection: modelSelection("default"),
            runtimePolicy,
            existingProviderThread: { ...providerThread, nativeThreadRef: null },
          })
          .pipe(Effect.flip);
        assert.equal(error._tag, "ProviderAdapterEnsureThreadError");
        assert.match(
          String(error.cause),
          invalidReplacement === "veto" ? /cancelled/ : /distinct session/,
        );
        yield* startTurn(runtime, providerThread, "default").pipe(Effect.flip);
        assert.isFalse(fake.allRequests().some((request) => request.type === "prompt"));
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("retires a timed-out lifecycle process before a late switch can race replacement", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.deferNextLifecycle("switch_session");
      const resumed = yield* runtime
        .resumeThread({ providerThread })
        .pipe(Effect.flip, Effect.forkChild);
      const request = yield* fake.takeRequest("switch_session");
      yield* TestClock.adjust(Duration.seconds(60));
      const error = yield* Fiber.join(resumed);
      assert.match(String(error.cause), /timed out after 60000ms/);
      yield* takeEvent(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "error",
      );
      yield* fake.emit({
        type: "response",
        id: request.id,
        command: "switch_session",
        success: true,
        data: { cancelled: false },
      });
      yield* runtime
        .ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
          existingProviderThread: { ...providerThread, nativeThreadRef: null },
        })
        .pipe(Effect.flip);
      assert.isFalse(
        fake
          .allRequests()
          .some((request) => request.type === "new_session" || request.type === "prompt"),
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "uses the lifecycle deadline for fresh sessions and drops replaced native metadata",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        fake.deferNextLifecycle("new_session");
        fake.queueState({ sessionFile: "/fake/fresh-after-delay.jsonl" });
        const replacing = yield* runtime
          .ensureThread({
            threadId: THREAD_ID,
            modelSelection: modelSelection("default"),
            runtimePolicy,
            existingProviderThread: {
              ...providerThread,
              nativeThreadRef: null,
              nativeConversationHeadRef: {
                driver: PI_PROVIDER,
                nativeId: "old-leaf",
                strength: "strong",
              },
              contextUsage: { usedTokens: 314_551, maxTokens: 1_000_000 },
            },
          })
          .pipe(Effect.forkChild);
        const request = yield* fake.takeRequest("new_session");
        yield* TestClock.adjust(Duration.millis(16_820));
        yield* fake.emit({
          type: "response",
          id: request.id,
          command: "new_session",
          success: true,
          data: { cancelled: false },
        });
        const replacement = yield* Fiber.join(replacing);
        assert.equal(replacement.nativeThreadRef?.nativeId, "/fake/fresh-after-delay.jsonl");
        assert.isNull(replacement.contextUsage);
        assert.isNull(replacement.nativeConversationHeadRef);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("replaces a native session when the first resume's state refresh fails", () =>
    Effect.gen(function* () {
      const original = yield* makeFakePi;
      const originalRuntime = yield* openRuntime(original);
      const providerThread = yield* originalRuntime.runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      fake.failNextState();
      yield* runtime.resumeThread({ providerThread }).pipe(Effect.flip);
      const replacement = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: { ...providerThread, nativeThreadRef: null },
      });
      assert.notEqual(
        replacement.nativeThreadRef?.nativeId,
        providerThread.nativeThreadRef?.nativeId,
      );
      yield* startTurn(runtime, replacement, "default");
      yield* fake.takeRequest("prompt");
      assert.isTrue(fake.allRequests().some((request) => request.type === "new_session"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("retires an interrupted switch before accepting further requests", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.deferNextLifecycle("switch_session");
      const resumed = yield* runtime.resumeThread({ providerThread }).pipe(Effect.forkChild);
      yield* fake.takeRequest("switch_session");
      yield* Fiber.interrupt(resumed);
      yield* takeEvent(
        (event) =>
          event.type === "provider_session.updated" && event.providerSession.status === "error",
      );
      yield* runtime
        .ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
          existingProviderThread: { ...providerThread, nativeThreadRef: null },
        })
        .pipe(Effect.flip);
      assert.isFalse(fake.allRequests().some((request) => request.type === "new_session"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("budgets legacy native history with Pi's selected model capacity", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueState({
        sessionFile: FAKE_SESSION_FILE,
        model: { provider: "anthropic", id: "large", contextWindow: 1_000_000 },
      });
      fake.queueModels([{ provider: "anthropic", id: "small", contextWindow: 32_000 }]);
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const budget = (model: string) =>
        handoffBudget({
          tokenCap: 16_000,
          userText: "$handoff",
          attachments: [],
          providerThread,
          nativeContextEstimate: 307_543,
          modelContextWindow: runtime.getModelContextWindow?.(modelSelection(model)),
        });
      assert.equal(budget("default"), 16_000);
      assert.equal(budget("anthropic/large"), 16_000);
      assert.equal(runtime.getModelContextWindow?.(modelSelection("anthropic/small")), 32_000);
      assert.equal(budget("anthropic/small"), 0);
      assert.isUndefined(runtime.getModelContextWindow?.(modelSelection("anthropic/unknown")));
      assert.isUndefined(
        runtime.getModelContextWindow?.({
          instanceId: ProviderInstanceId.make("other-pi"),
          model: "anthropic/large",
        }),
      );
      fake.queueState({
        sessionFile: "/fake/replacement.jsonl",
        model: { provider: "anthropic", id: "small", contextWindow: 32_000 },
      });
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: { ...providerThread, nativeThreadRef: null },
      });
      assert.equal(runtime.getModelContextWindow?.(modelSelection("default")), 32_000);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("adopts the run's provider thread identity instead of minting a second row", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const now = yield* DateTime.now;
      const placeholder: OrchestrationV2ProviderThread = {
        id: ProviderThreadId.make("thread:provider:pi:native-thread:pending:run:thread-pi-test:1"),
        driver: PI_PROVIDER,
        providerInstanceId: PI_INSTANCE_ID,
        providerSessionId: SESSION_ID,
        appThreadId: THREAD_ID,
        ownerNodeId: null,
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
        status: "not_loaded",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: placeholder,
      });
      assert.equal(providerThread.id, placeholder.id);
      assert.equal(providerThread.nativeThreadRef?.nativeId, FAKE_SESSION_FILE);
      assert.isFalse(fake.allRequests().some((request) => request.type === "new_session"));
      const updated = yield* takeEvent((event) => event.type === "provider_thread.updated");
      assert.isTrue(
        updated.type === "provider_thread.updated" && updated.providerThread.id === placeholder.id,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("resets applied thinking when returning to Pi default", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueState({
        model: { provider: "xai", id: "grok-4.6" },
        thinkingLevel: "medium",
        sessionFile: FAKE_SESSION_FILE,
        sessionId: "abc",
      });
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });

      yield* startTurn(runtime, providerThread, "default", [], "Hello pi", {
        instanceId: PI_INSTANCE_ID,
        model: "xai/grok-4.6",
        options: [{ id: "thinking", value: "high" }],
      });
      const modelRequest = yield* fake.takeRequest("set_model");
      assert.equal(modelRequest["provider"], "xai");
      assert.equal(modelRequest["modelId"], "grok-4.6");
      const levelRequest = yield* fake.takeRequest("set_thinking_level");
      assert.equal(levelRequest["level"], "high");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_end", messages: [], willRetry: false });
      yield* fake.emit({ type: "agent_settled" });
      yield* takeEvent((event) => event.type === "turn.terminal");

      yield* startTurn(
        runtime,
        providerThread,
        "default",
        [],
        "Hello pi",
        {
          instanceId: PI_INSTANCE_ID,
          model: "default",
        },
        2,
      );
      const replayModel = yield* fake.takeRequest("set_model");
      assert.equal(replayModel["provider"], "xai");
      assert.equal(replayModel["modelId"], "grok-4.6");
      const resetLevel = yield* fake.takeRequest("set_thinking_level");
      assert.equal(resetLevel["level"], "medium");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("preserves selected skill references for the Pi extension to expand", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });

      yield* startTurn(
        runtime,
        providerThread,
        "default",
        [],
        "Review this change please $repo-review",
      );
      const prompt = yield* fake.takeRequest("prompt");
      assert.equal(prompt["message"], "Review this change please $repo-review");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([
    { historical: false, label: "the latest turn" },
    { historical: true, label: "a historical turn" },
  ])("natively forks $label into an independent session", ({ historical }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const forkFake = yield* makeFakePi;
      const forkFile = "/fake/forked.jsonl";
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        forkFake,
      );
      const source = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const turn = (ordinal: number): OrchestrationV2ProviderTurn => ({
        id: ProviderTurnId.make(`turn-${ordinal}`),
        providerThreadId: source.id,
        nodeId: NodeId.make(`node-${ordinal}`),
        runAttemptId: null,
        nativeTurnRef: { driver: PI_PROVIDER, nativeId: `u${ordinal}`, strength: "strong" },
        ordinal,
        status: "completed",
        startedAt: null,
        completedAt: null,
      });
      forkFake.queueState({ sessionFile: forkFile });
      fake.queueState({ sessionFile: forkFile });
      const target = ThreadId.make("fork-target");
      const forked = yield* runtime.forkThread({
        sourceProviderThread: source,
        sourceProviderTurns: historical ? [turn(1), turn(2)] : [turn(1)],
        providerTurnId: turn(1).id,
        targetThreadId: target,
      });
      assert.equal(forked.appThreadId, target);
      assert.equal(forked.nativeThreadRef?.nativeId, forkFile);
      assert.notEqual(forked.id, source.id);
      assert.equal(source.nativeThreadRef?.nativeId, FAKE_SESSION_FILE);
      const args = forkFake.lastSpawn().args;
      assert.equal(args[args.indexOf("--fork") + 1], FAKE_SESSION_FILE);
      assert.include(args, "--no-extensions");
      assert.include(args, "--no-tools");
      assert.notInclude(args, "--no-session");
      assert.deepEqual(
        forkFake
          .allRequests()
          .filter((request) => request.type === "fork")
          .map((request) => request.entryId),
        historical ? ["u2"] : [],
      );
      assert.isFalse(
        fake.allRequests().some((request) => request.type === "fork" || request.type === "clone"),
      );
      const adopted = { ...forked, id: ProviderThreadId.make("pending-fork-row") };
      yield* startTurn(runtime, adopted, "default", [], "Continue", undefined, 1, target);
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_settled" });
      const updated = yield* takeEvent(
        (event) =>
          event.type === "provider_thread.updated" && event.providerThread.appThreadId === target,
      );
      assert.isTrue(
        updated.type === "provider_thread.updated" && updated.providerThread.id === adopted.id,
      );
      yield* takeEvent((event) => event.type === "turn.terminal");
      yield* runtime.resumeThread({ providerThread: adopted });
      assert.equal(
        fake.allRequests().findLast((request) => request.type === "switch_session")?.sessionPath,
        forkFile,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("presents explicitly namespaced MCP extension tools", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      for (const type of ["tool_execution_start", "tool_execution_end"]) {
        yield* fake.emit({
          type,
          toolCallId: "weather-call",
          toolName: "mcp__weather__get_weather",
          args: { city: "Berlin" },
          result: { content: [{ type: "text", text: "Sunny" }] },
          isError: false,
        });
        const event = yield* takeEvent(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
        );
        if (event.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool")
          return yield* Effect.die("Expected an MCP tool item");
        assert.equal(event.turnItem.title, "get weather");
        assert.equal(
          event.turnItem.status,
          type === "tool_execution_start" ? "running" : "completed",
        );
        assert.deepEqual(event.turnItem.toolSource, {
          key: "mcp:weather",
          name: "weather",
          kind: "integration",
        });
        assert.deepEqual(event.turnItem.input, { city: "Berlin" });
      }
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "keeps native and MCP images available to the shared asset reader with typed results",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        const image = {
          type: "image",
          mimeType: "image/png",
          data: Buffer.alloc(64 * 1024, 0x61).toString("base64"),
        };
        const note = "Read image file [image/png]";
        const content = [{ type: "text", text: note }, image];
        const structured = { count: 2, ready: true, paths: ["one.png", "two.png"] };
        const scriptResult = { content, structuredContent: { threadId: "child-thread" } };
        const cases = [
          {
            toolName: "read",
            result: { content, structuredContent: { ...image, note } },
            expected: { content },
          },
          {
            toolName: "image_generate",
            result: { content, structuredContent: structured },
            expected: { content, structuredContent: structured },
          },
          {
            toolName: "mcp__t3-code__preview_snapshot",
            result: {
              content,
              structuredContent: scriptResult,
              details: { server: "t3-code", tool: "preview_snapshot" },
            },
            expected: { content, structuredContent: { threadId: "child-thread" } },
          },
          {
            toolName: "structured_tool",
            result: { content: [], structuredContent: [false, 2, null, { ready: true }] },
            expected: { content: [], structuredContent: [false, 2, null, { ready: true }] },
          },
          {
            toolName: "mcp__custom__extension",
            result: { content, structuredContent: { content: ["domain content"], ready: true } },
            expected: { content, structuredContent: { content: ["domain content"], ready: true } },
          },
          {
            toolName: "mcp__t3-code__preview_snapshot",
            result: {
              content: [
                { type: "text", text: "first" },
                { type: "text", text: "second" },
              ],
              structuredContent: {
                content: [
                  { type: "text", text: "first" },
                  { type: "text", text: "second" },
                ],
              },
              details: { server: "t3-code", tool: "preview_snapshot" },
            },
            expected: {
              content: [
                { type: "text", text: "first" },
                { type: "text", text: "second" },
              ],
            },
          },
        ];
        for (const [index, test] of cases.entries()) {
          for (const phase of ["update", "end"] as const) {
            yield* fake.emit({
              type: `tool_execution_${phase}`,
              toolCallId: `image-${index}`,
              toolName: test.toolName,
              ...(phase === "end" ? { result: test.result } : { partialResult: test.result }),
              isError: false,
            });
            const event = yield* takeEvent(
              (event) =>
                event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
            );
            if (event.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool")
              return yield* Effect.die("Expected an image tool item");
            assert.equal(event.turnItem.status, phase === "end" ? "completed" : "running");
            assert.deepEqual(event.turnItem.output, test.expected);
            assert.deepEqual(
              toolOutputImages(event.turnItem.output),
              test.expected.content.some((block) => block.type === "image")
                ? [{ mimeType: "image/png", data: image.data }]
                : [],
            );
            if (index === 0) assert.equal(turnItemOutputText(event.turnItem), note);
            if (index === 5) assert.equal(turnItemOutputText(event.turnItem), "first\nsecond");
            if (index === 2) {
              assert.deepEqual(compactDynamicToolOutput(event.turnItem.output), {
                threadId: "child-thread",
              });
              assert.equal(JSON.stringify(event.turnItem.output).split(image.data).length - 1, 1);
            }
          }
        }
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "keeps large tool text readable, drops identical read mirrors, and bounds structured values",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        const text = "hello\nworld\n" + "🙂".repeat(40_000);
        const content = [{ type: "text", text }];
        const cases = [
          { content, structuredContent: text },
          { content, structuredContent: { oversized: "x".repeat(128 * 1024) } },
          { content, structuredContent: Array.from({ length: 10_000 }, () => 1) },
          {
            content,
            structuredContent: Array.from({ length: 40 }).reduce<unknown>(
              (nested) => ({ nested }),
              null,
            ),
          },
        ];
        for (const [index, result] of cases.entries()) {
          yield* fake.emit({
            type: "tool_execution_end",
            toolCallId: `structured-${index}`,
            toolName: "read",
            result,
            isError: true,
          });
          const event = yield* takeEvent(
            (event) => event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
          );
          if (event.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool")
            return yield* Effect.die("Expected a tool item");
          assert.equal(event.turnItem.status, "failed");
          assert.notProperty(event.turnItem.output, "structuredContent");
          const displayed = turnItemOutputText(event.turnItem);
          if (index === 0) {
            assert.equal(event.turnItem.output, text);
            assert.equal(displayed, text);
          } else {
            assert.equal(
              displayed,
              text + "\nStructured output omitted because it exceeds the stored result limit.",
            );
            assert.deepEqual(Object.keys(event.turnItem.output as object), ["content"]);
          }
        }
        yield* fake.emit({
          type: "tool_execution_end",
          toolCallId: "distinct-metadata",
          toolName: "read",
          result: { content, structuredContent: { path: "read.ts", ready: true } },
        });
        const event = yield* takeEvent(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
        );
        if (event.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool")
          return yield* Effect.die("Expected a tool item");
        assert.deepEqual(event.turnItem.output, {
          content,
          structuredContent: { path: "read.ts", ready: true },
        });
        assert.equal(turnItemOutputText(event.turnItem), text);
        const note = "Read image file [image/png]";
        const image = { type: "image", data: "AAAA", mimeType: "image/png" };
        const imageContent = [{ type: "text", text: note }, image];
        for (const structuredContent of [
          { ...image, note, width: 1200 },
          { ...image, note: "A distinct note" },
          { ...image, note, data: "AQID" },
        ]) {
          yield* fake.emit({
            type: "tool_execution_end",
            toolCallId: "distinct-image-metadata",
            toolName: "read",
            result: { content: imageContent, structuredContent },
          });
          const event = yield* takeEvent(
            (event) => event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
          );
          if (event.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool")
            return yield* Effect.die("Expected a read image tool item");
          assert.deepEqual(event.turnItem.output, { content: imageContent, structuredContent });
          assert.equal(turnItemOutputText(event.turnItem), note);
        }
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "delivers realistic MCP screenshots through the bridge, JSONL transport, and adapter",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        for (const size of [3.25 * 1024 * 1024, 10 * 1024 * 1024]) {
          const pixels = Buffer.alloc(size, 0x61);
          const data = pixels.toString("base64");
          const content = [
            { type: "text", text: "Screenshot captured" },
            { type: "image", data, mimeType: "image/png" },
          ];
          const metadata = { screenshot: { width: 1200, height: 800 }, ready: true };
          const bridge = yield* Effect.promise(() =>
            loadMcpBridge({ modern: true, result: { content, structuredContent: metadata } }),
          );
          const tool = bridge.tools.find((tool) => tool.name === "mcp__t3-code__preview_snapshot")!;
          const result = yield* Effect.promise(() => tool.execute("screenshot", {}));
          assert.deepEqual(result.structuredContent, { content, structuredContent: metadata });
          yield* fake.emit(
            {
              type: "tool_execution_end",
              toolCallId: "screenshot",
              toolName: tool.name,
              result,
              isError: false,
            },
            64 * 1024,
          );
          yield* fake.emit({
            type: "tool_execution_end",
            toolCallId: "after-screenshot",
            toolName: "marker",
            result: { content: [{ type: "text", text: "after" }] },
          });
          const event = yield* takeEvent(
            (event) => event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool",
          );
          if (event.type !== "turn_item.updated" || event.turnItem.type !== "dynamic_tool")
            return yield* Effect.die("Expected a screenshot tool item");
          assert.equal(event.turnItem.toolName, tool.name);
          assert.equal(event.turnItem.status, "completed");
          assert.deepEqual(event.turnItem.output, { content, structuredContent: metadata });
          assert.equal(turnItemOutputText(event.turnItem), "Screenshot captured");
          const image = toolOutputImages(event.turnItem.output)[0];
          assert.equal(image?.mimeType, "image/png");
          assert.deepEqual(Buffer.from(image!.data!, "base64"), pixels);
          yield* takeEvent(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "dynamic_tool" &&
              event.turnItem.toolName === "marker",
          );
        }
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("observes official subagent results without inventing child threads", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "tool_execution_update",
        toolCallId: "call_sub",
        toolName: "subagent",
        partialResult: {
          content: [{ type: "text", text: "(running...)" }],
          details: {
            mode: "single",
            results: [
              {
                agent: "scout",
                task: "map the repo",
                exitCode: 0,
                stderr: "",
                sessionFile: "/ignored/custom-extension-session.jsonl",
                messages: [
                  { role: "assistant", content: [{ type: "text", text: "scanning files" }] },
                ],
              },
            ],
          },
        },
      });
      const running = yield* takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.status === "running",
      );
      assert.isTrue(
        running.type === "subagent.updated" &&
          running.subagent.title === "scout" &&
          running.subagent.prompt === "map the repo" &&
          running.subagent.progress === "scanning files" &&
          running.subagent.childThreadId === null,
      );

      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "call_sub",
        toolName: "subagent",
        isError: false,
        result: {
          content: [{ type: "text", text: "done" }],
          details: {
            mode: "single",
            results: [
              {
                agent: "scout",
                task: "map the repo",
                exitCode: 0,
                stopReason: "stop",
                stderr: "",
                messages: [
                  { role: "assistant", content: [{ type: "text", text: "repo has one file" }] },
                ],
              },
            ],
          },
        },
      });
      const doneCard = yield* takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.status === "completed",
      );
      assert.isTrue(
        doneCard.type === "subagent.updated" &&
          doneCard.subagent.result === "repo has one file" &&
          doneCard.subagent.childThreadId === null,
      );
      const subagentItem = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "subagent" &&
          event.turnItem.status === "completed",
      );
      assert.isTrue(
        subagentItem.type === "turn_item.updated" &&
          subagentItem.turnItem.type === "subagent" &&
          subagentItem.turnItem.childThreadId === null,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("persists edit patches and write content on file change items", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const patch = "--- a.ts\n+++ a.ts\n@@ -1 +1 @@\n-old\n+new\n";
      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId: "call_edit",
        toolName: "edit",
        args: { path: "a.ts", edits: [{ oldText: "old", newText: "new" }] },
      });
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "call_edit",
        toolName: "edit",
        isError: false,
        result: {
          content: [{ type: "text", text: "Successfully replaced 1 block(s) in a.ts." }],
          details: { diff: "-1 old\n+1 new", patch, firstChangedLine: 1 },
        },
      });
      const edit = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "file_change" &&
          event.turnItem.status === "completed",
      );
      assert.isTrue(
        edit.type === "turn_item.updated" &&
          edit.turnItem.type === "file_change" &&
          edit.turnItem.fileName === "a.ts" &&
          edit.turnItem.diffStr === patch,
      );

      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId: "call_write",
        toolName: "write",
        args: { path: "b.ts", content: "export {};\n" },
      });
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "call_write",
        toolName: "write",
        isError: false,
        result: { content: [{ type: "text", text: "Successfully wrote to b.ts" }] },
      });
      const write = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "file_change" &&
          event.turnItem.status === "completed",
      );
      assert.isTrue(
        write.type === "turn_item.updated" &&
          write.turnItem.type === "file_change" &&
          write.turnItem.fileName === "b.ts" &&
          write.turnItem.newStr === "export {};\n",
      );

      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId: "call_edit_failed",
        toolName: "edit",
        args: { path: "c.ts", edits: [{ oldText: "missing", newText: "new" }] },
      });
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "call_edit_failed",
        toolName: "edit",
        isError: true,
        result: { content: [{ type: "text", text: "Could not find the text in c.ts." }] },
      });
      const failedEdit = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "file_change" &&
          event.turnItem.status === "failed",
      );
      assert.isTrue(
        failedEdit.type === "turn_item.updated" &&
          failedEdit.turnItem.type === "file_change" &&
          failedEdit.turnItem.diffStr === "Could not find the text in c.ts.",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([false, true])("follows an extension rewind with summarize=%s", (summarize) =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const root = { type: "model_change", id: "root", parentId: null };
      const user = (id: string, parentId: string) => ({
        type: "message",
        id,
        parentId,
        message: { role: "user", content: [{ type: "text", text: id }] },
      });
      const reply = (id: string, parentId: string) => ({
        type: "message",
        id,
        parentId,
        message: { role: "assistant", content: [] },
      });
      fake.queueEntries({ entries: [root], leafId: "root" });
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const settle = Effect.fnUntraced(function* (
        text: string,
        runOrdinal: number,
        ...listings: ReadonlyArray<unknown>
      ) {
        yield* startTurn(runtime, providerThread, "default", [], text, undefined, runOrdinal);
        yield* fake.takeRequest("prompt");
        for (const listing of listings) fake.queueEntries(listing);
        yield* fake.emit({ type: "response", command: "prompt", success: true });
        if (!text.startsWith("/")) {
          yield* fake.emit({ type: "agent_start" });
          yield* fake.emit({ type: "agent_settled" });
        }
        const turn = yield* takeEvent(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
        );
        const thread = yield* takeEvent(
          (event) =>
            event.type === "provider_thread.updated" && event.providerThread.status === "idle",
        );
        yield* takeEvent((event) => event.type === "turn.terminal");
        return {
          turnRef:
            turn.type === "provider_turn.updated" ? turn.providerTurn.nativeTurnRef : undefined,
          head:
            thread.type === "provider_thread.updated"
              ? thread.providerThread.nativeConversationHeadRef?.nativeId
              : undefined,
          retained:
            thread.type === "provider_thread.updated" ? thread.retainedNativeTurnIds : undefined,
        };
      });

      yield* settle("prompt A", 1, {
        entries: [user("uA", "root"), reply("aA", "uA")],
        leafId: "aA",
      });
      const promptB = yield* settle("prompt B", 2, {
        entries: [user("uB", "aA"), reply("aB", "uB")],
        leafId: "aB",
      });
      assert.equal(promptB.turnRef?.nativeId, "uB");
      assert.isUndefined(promptB.retained);

      const rewoundLeaf = summarize ? "summary" : "aA";
      const summary = summarize ? [{ type: "branch_summary", id: "summary", parentId: "aA" }] : [];
      const rewind = yield* settle(
        "/rewind",
        3,
        { entries: summary, leafId: rewoundLeaf },
        {
          entries: [
            root,
            user("uA", "root"),
            reply("aA", "uA"),
            user("uB", "aA"),
            reply("aB", "uB"),
            ...summary,
          ],
          leafId: rewoundLeaf,
        },
      );
      assert.deepEqual(rewind.retained, ["uA"]);
      assert.equal(rewind.head, rewoundLeaf);
      if (summarize) {
        assert.equal(rewind.turnRef?.strength, "weak");
      } else {
        assert.isNull(rewind.turnRef);
      }

      const promptC = yield* settle("prompt C", 4, {
        entries: [
          ...(summarize ? [] : [user("uB", "aA"), reply("aB", "uB")]),
          user("uC", rewoundLeaf),
          reply("aC", "uC"),
        ],
        leafId: "aC",
      });
      assert.deepEqual(promptC.turnRef, {
        driver: PI_PROVIDER,
        nativeId: "uC",
        strength: "strong",
      });
      assert.isUndefined(promptC.retained);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each(["turn window", "full tree"] as const)(
    "finishes a turn whose %s has a parentId cycle",
    (cycleIn) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const root = { type: "model_change", id: "root", parentId: null };
        const cycle = [
          { type: "message", id: "x", parentId: "y", message: { role: "user", content: [] } },
          { type: "message", id: "y", parentId: "x", message: { role: "assistant", content: [] } },
        ];
        fake.queueEntries({ entries: [root], leafId: "root" });
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* startTurn(runtime, providerThread, "default", [], "prompt", undefined, 1);
        yield* fake.takeRequest("prompt");
        if (cycleIn === "turn window") {
          fake.queueEntries({ entries: cycle, leafId: "x" });
        } else {
          fake.queueEntries({ entries: [], leafId: "x" });
          fake.queueEntries({ entries: [root, ...cycle], leafId: "x" });
        }
        yield* fake.emit({ type: "response", command: "prompt", success: true });
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "agent_settled" });
        yield* takeEvent(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
        );
        const thread = yield* takeEvent(
          (event) =>
            event.type === "provider_thread.updated" && event.providerThread.status === "idle",
        );
        assert.isUndefined(
          thread.type === "provider_thread.updated" ? thread.retainedNativeTurnIds : null,
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each(["missing parent", "invalid parent", "duplicate id"] as const)(
    "does not reconcile malformed session entries: %s",
    (malformed) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const root = { type: "model_change", id: "root", parentId: null };
        fake.queueEntries({ entries: [root], leafId: "root" });
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        const entry = {
          type: "message",
          id: "x",
          message: { role: "user", content: [] },
          ...(malformed === "missing parent"
            ? {}
            : { parentId: malformed === "invalid parent" ? 42 : null }),
        };
        fake.queueEntries({ entries: [], leafId: "x" });
        fake.queueEntries({
          entries: [
            root,
            entry,
            ...(malformed === "duplicate id" ? [{ ...entry, parentId: "root" }] : []),
          ],
          leafId: "x",
        });
        yield* fake.emit({ type: "response", command: "prompt", success: true });
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "agent_settled" });
        yield* takeEvent(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
        );
        const thread = yield* takeEvent(
          (event) =>
            event.type === "provider_thread.updated" && event.providerThread.status === "idle",
        );
        assert.isUndefined(
          thread.type === "provider_thread.updated" ? thread.retainedNativeTurnIds : null,
        );
        yield* takeEvent((event) => event.type === "turn.terminal");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("does not reconcile a replacement session tree as a native rewind", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueEntries({
        entries: [{ type: "model_change", id: "old-leaf", parentId: null }],
        leafId: "old-leaf",
      });
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const finish = Effect.fnUntraced(function* () {
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "response", command: "prompt", success: true });
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "agent_settled" });
        const update = yield* takeEvent(
          (event) =>
            event.type === "provider_thread.updated" && event.providerThread.status === "idle",
        );
        yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isUndefined(
          update.type === "provider_thread.updated" ? update.retainedNativeTurnIds : null,
        );
      });
      fake.queueEntries({ entries: [] });
      yield* finish();
      fake.queueEntries({
        entries: [
          {
            type: "message",
            id: "new-user",
            parentId: null,
            message: { role: "user", content: [] },
          },
        ],
        leafId: "new-user",
      });
      yield* finish();
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("rolls back past turns that left nothing in the session tree", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const turn = (ordinal: number, entryId: string | null): OrchestrationV2ProviderTurn => ({
        id: ProviderTurnId.make(`turn-${ordinal}`),
        providerThreadId: providerThread.id,
        nodeId: NodeId.make(`node-${ordinal}`),
        runAttemptId: null,
        nativeTurnRef:
          entryId === null ? null : { driver: PI_PROVIDER, nativeId: entryId, strength: "strong" },
        ordinal,
        status: "completed",
        startedAt: null,
        completedAt: null,
      });
      const rollBackToFirst = (turns: ReadonlyArray<OrchestrationV2ProviderTurn>) =>
        runtime.rollbackThread({
          providerThread,
          target: {
            type: "provider_turn",
            checkpointId: CheckpointId.make("checkpoint-1"),
            appRunOrdinal: 1,
            providerTurn: turns[0]!,
          },
          providerThreadTurns: turns,
        });
      const forkedEntries = () =>
        fake
          .allRequests()
          .filter((request) => request.type === "fork")
          .map((request) => request.entryId);

      yield* rollBackToFirst([turn(1, "u1"), turn(2, null)]);
      assert.deepEqual(forkedEntries(), []);
      yield* rollBackToFirst([turn(1, "u1"), turn(2, null), turn(3, "u3")]);
      assert.deepEqual(forkedEntries(), ["u3"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("settles a command-only prompt from its deferred ack and idle probe", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/command-only");
      yield* fake.takeRequest("prompt");
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-cmd",
        method: "notify",
        message: "done",
        notifyType: "info",
      });
      yield* fake.emit({ type: "response", command: "prompt", success: true });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("leaves /compacted as an ordinary prompt", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/compacted please");
      const prompt = yield* fake.takeRequest("prompt");
      assert.equal(prompt["message"], "/compacted please");
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "compact"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("fails a compact that never started", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/compact");
      yield* fake.takeRequest("compact");
      yield* fake.emit({
        type: "response",
        command: "compact",
        success: false,
        error: "Nothing to compact (session too small)",
      });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(
        terminal.type === "turn.terminal" &&
          terminal.status === "failed" &&
          terminal.failure.message === "Nothing to compact (session too small)",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("restarts Pi when Stop interrupts a user compact", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/compact");
      yield* fake.takeRequest("compact");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      const providerTurnId =
        running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
      assert.isDefined(providerTurnId);
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      yield* runtime.interruptTurn({ providerThread, providerTurnId: providerTurnId! });
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "abort"));
      yield* fake.closeStdout;
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("steers /compact as RPC compact instead of a prompt", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      const providerTurnId =
        running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
      yield* fake.emit({ type: "agent_start" });
      yield* runtime.steerTurn({
        threadId: THREAD_ID,
        runId: RunId.make("run:thread-pi-test:1"),
        providerThread,
        providerTurnId: providerTurnId!,
        message: {
          messageId: "message:thread-pi-test:steer-compact" as never,
          text: "/compact keep the tests",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
      });
      const compact = yield* fake.takeRequest("compact");
      assert.equal(compact["customInstructions"], "keep the tests");
      assert.isUndefined(compact["streamingBehavior"]);
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      fake.queueState({ isStreaming: false, isCompacting: false, pendingMessageCount: 0 });
      yield* fake.emit({
        type: "compaction_end",
        reason: "manual",
        result: { summary: "smaller", tokensBefore: 10_000, estimatedTokensAfter: 2_000 },
        aborted: false,
        willRetry: false,
      });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "compaction" &&
          event.turnItem.status === "completed",
      );
      yield* fake.emit({ type: "response", command: "compact", success: true });
      yield* fake.emit({ type: "agent_settled" });
      yield* fake.takeRequest("get_state");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("persists current xAI capacity text for the thread error banner", () =>
    expectModelFailure("The model is currently at capacity due to high demand."),
  );

  it.effect("persists extension-normalized xAI capacity text for the thread error banner", () =>
    expectModelFailure(
      "Provider overloaded: The model is currently at capacity due to high demand.",
    ),
  );

  it.effect.each([
    { toolName: "bash", exitCode: 0, legacy: false },
    { toolName: "powershell", exitCode: 2, legacy: false },
    { toolName: "bash", exitCode: 1, legacy: true },
    { toolName: "powershell", exitCode: 0, legacy: true },
  ])(
    "normalizes $toolName command results, exit=$exitCode legacy=$legacy",
    ({ toolName, exitCode, legacy }) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({
          type: "tool_execution_start",
          toolCallId: "shell-call",
          toolName,
          args: { command: "echo hello" },
        });
        const running = yield* takeEvent(
          (event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
        );
        assert.isTrue(
          running.type === "turn_item.updated" &&
            running.turnItem.type === "command_execution" &&
            running.turnItem.status === "running" &&
            running.turnItem.input === "echo hello",
        );
        yield* fake.emit({
          type: "tool_execution_end",
          toolCallId: "shell-call",
          toolName,
          isError: exitCode !== 0,
          result: {
            content: [{ type: "text", text: "hello" }],
            details: { exitCode: legacy ? exitCode : 99 },
            ...(legacy ? {} : { structuredContent: { exit_code: exitCode } }),
          },
        });
        const completed = yield* takeEvent(
          (event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "command_execution",
        );
        assert.isTrue(
          completed.type === "turn_item.updated" &&
            completed.turnItem.type === "command_execution" &&
            completed.turnItem.title === toolName &&
            completed.turnItem.input === "echo hello" &&
            completed.turnItem.output === "hello" &&
            completed.turnItem.exitCode === exitCode &&
            completed.turnItem.status === (exitCode === 0 ? "completed" : "failed"),
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("emits session-start dialogs before a turn exists", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-trust",
        method: "confirm",
        title: "Run project extensions?",
        message: "This project has .pi/extensions.",
      });
      const pending = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      const requestId =
        pending.type === "runtime_request.updated" ? pending.runtimeRequest.id : undefined;
      yield* runtime.respondToRuntimeRequest({ requestId: requestId!, decision: "accept" });
      const uiResponse = yield* fake.takeRequest("extension_ui_response");
      assert.equal(uiResponse["id"], "ui-trust");
      assert.equal(uiResponse["confirmed"], true);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("expires native dialogs and rejects stale answers without caching approval", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const dialog = {
        type: "extension_ui_request",
        method: "confirm",
        title: "Allow extension?",
        message: "Approve this operation",
      };
      yield* fake.emit({ ...dialog, id: "timed-dialog", timeout: 1000 });
      const pending = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "approval_request",
      );
      assert.isTrue(
        pending.type === "turn_item.updated" && pending.turnItem.type === "approval_request",
      );
      if (pending.type !== "turn_item.updated" || pending.turnItem.type !== "approval_request")
        return;
      const requestId = pending.turnItem.requestId;
      yield* TestClock.adjust(Duration.seconds(1));
      yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" &&
          event.runtimeRequest.id === requestId &&
          event.runtimeRequest.status === "cancelled",
      );
      assert.equal((yield* fake.takeRequest("extension_ui_response"))["cancelled"], true);
      const late = yield* Effect.result(
        runtime.respondToRuntimeRequest({
          requestId: pending.turnItem.requestId,
          decision: "acceptForSession",
        }),
      );
      assert.equal(late._tag, "Failure");
      yield* fake.emit({ ...dialog, id: "fresh-dialog", timeout: 1000 });
      const fresh = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" &&
          event.runtimeRequest.status === "pending" &&
          event.runtimeRequest.nativeRequestRef?.nativeId === "fresh-dialog",
      );
      if (fresh.type !== "runtime_request.updated") return;
      yield* runtime.respondToRuntimeRequest({
        requestId: fresh.runtimeRequest.id,
        decision: "accept",
      });
      assert.equal((yield* fake.takeRequest("extension_ui_response"))["id"], "fresh-dialog");
      yield* fake.emit({ ...dialog, id: "zero-timeout", timeout: 0 });
      const zero = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" &&
          event.runtimeRequest.status === "pending" &&
          event.runtimeRequest.nativeRequestRef?.nativeId === "zero-timeout",
      );
      yield* TestClock.adjust(Duration.seconds(60));
      if (zero.type !== "runtime_request.updated") return;
      yield* runtime.respondToRuntimeRequest({
        requestId: zero.runtimeRequest.id,
        decision: "decline",
      });
      const response = yield* fake.takeRequest("extension_ui_response");
      assert.equal(response["id"], "zero-timeout");
      assert.equal(response["confirmed"], false);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("cancels startup dialogs when Pi exits before a turn exists", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({
        type: "extension_ui_request",
        id: "startup",
        method: "confirm",
        title: "Trust project?",
      });
      const pending = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      if (pending.type !== "runtime_request.updated") return;
      yield* fake.closeStdout;
      const cancelled = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" &&
          event.runtimeRequest.id === pending.runtimeRequest.id &&
          event.runtimeRequest.status === "cancelled",
      );
      assert.isTrue(cancelled.type === "runtime_request.updated");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("remembers session approvals only for identical confirmation content", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-trust",
        method: "confirm",
        title: "Run project extensions?",
        message: "This project has .pi/extensions.",
      });
      const pending = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      const requestId =
        pending.type === "runtime_request.updated" ? pending.runtimeRequest.id : undefined;
      yield* runtime.respondToRuntimeRequest({
        requestId: requestId!,
        decision: "acceptForSession",
      });
      const uiResponse = yield* fake.takeRequest("extension_ui_response");
      assert.equal(uiResponse["id"], "ui-trust");
      assert.equal(uiResponse["confirmed"], true);
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-trust-again",
        method: "confirm",
        title: "Run project extensions?",
        message: "This project has .pi/extensions.",
      });
      assert.equal((yield* fake.takeRequest("extension_ui_response"))["id"], "ui-trust-again");
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-other",
        method: "confirm",
        title: "Run project extensions?",
        message: "A different project.",
      });
      const other = yield* takeEvent(
        (event) =>
          event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
      );
      assert.isTrue(
        other.type === "runtime_request.updated" &&
          other.runtimeRequest.nativeRequestRef?.nativeId === "ui-other",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("offers an explicit empty value for extension input dialogs", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-input",
        method: "input",
        title: "Optional value",
      });
      const event = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "user_input_request",
      );
      assert.isTrue(
        event.type === "turn_item.updated" && event.turnItem.type === "user_input_request",
      );
      if (event.type !== "turn_item.updated" || event.turnItem.type !== "user_input_request")
        return;
      assert.equal(event.turnItem.questions[0]?.options[0]?.value, "");
      yield* runtime.respondToRuntimeRequest({
        requestId: event.turnItem.requestId,
        answers: { "ui-input": "" },
      });
      const response = yield* fake.takeRequest("extension_ui_response");
      assert.equal(response["value"], "");
      assert.isUndefined(response["cancelled"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps the complete editor prefill editable and returns exact edited text", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const prefill = `  fix(server): retain full editor content\n\n${"detail\n".repeat(500)}  `;
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-editor",
        method: "editor",
        title: "Commit message",
        prefill,
      });
      const event = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "user_input_request",
      );
      if (event.type !== "turn_item.updated" || event.turnItem.type !== "user_input_request")
        return;
      assert.equal(event.turnItem.questions[0]?.initialAnswer, prefill);
      assert.deepEqual(event.turnItem.questions[0]?.options, []);
      assert.equal(event.turnItem.questions[0]?.question, "Commit message");
      const edited = "  fix(server): edited commit message\n\n";
      yield* runtime.respondToRuntimeRequest({
        requestId: event.turnItem.requestId,
        answers: { "ui-editor": edited },
      });
      const response = yield* fake.takeRequest("extension_ui_response");
      assert.equal(response["value"], edited);
      assert.isUndefined(response["cancelled"]);
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-editor-cleared",
        method: "editor",
        title: "Commit message",
        prefill,
      });
      const cleared = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "user_input_request" &&
          event.turnItem.questions.some((question) => question.id === "ui-editor-cleared"),
      );
      if (cleared.type !== "turn_item.updated" || cleared.turnItem.type !== "user_input_request")
        return;
      yield* runtime.respondToRuntimeRequest({
        requestId: cleared.turnItem.requestId,
        answers: { "ui-editor-cleared": "" },
      });
      const emptyResponse = yield* fake.takeRequest("extension_ui_response");
      assert.equal(emptyResponse["value"], "");
      assert.isUndefined(emptyResponse["cancelled"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("raises bridge edit confirmations as file-change approvals", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      for (const [id, title, requestKind] of [
        ["ui-edit", "Allow edit?", "file-change"],
        ["ui-bash", "Allow bash?", "command"],
        ["ui-ext", "Deploy to staging?", "command"],
      ] as const) {
        yield* fake.emit({ type: "extension_ui_request", id, method: "confirm", title });
        const item = yield* takeEvent(
          (event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "approval_request",
        );
        assert.isTrue(
          item.type === "turn_item.updated" &&
            item.turnItem.type === "approval_request" &&
            item.turnItem.requestKind === requestKind,
          `${title} should be ${requestKind}`,
        );
      }
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("reads a thread snapshot from pi's active branch", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      fake.queueMessages({
        messages: [
          {
            role: "user",
            content: "hello pi",
            timestamp: 1700000000000,
          },
          {
            role: "assistant",
            content: [{ type: "text", text: "hello back" }],
            timestamp: 1700000001000,
          },
          { role: "toolResult", content: [] },
        ],
      });
      const snapshot = yield* runtime.readThreadSnapshot({ providerThread });
      assert.equal(snapshot.messages.length, 2);
      assert.equal(snapshot.messages[0]!.role, "user");
      assert.equal(snapshot.messages[0]!.text, "hello pi");
      assert.equal(snapshot.messages[1]!.role, "assistant");
      assert.equal(snapshot.messages[1]!.text, "hello back");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps snapshot message identities distinct across native sessions", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      const first = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      const messages = {
        messages: [{ role: "user", content: "same text", timestamp: 1700000000000 }],
      };
      fake.queueMessages(messages);
      const a = yield* runtime.readThreadSnapshot({ providerThread: first });
      fake.queueState({ sessionFile: "/fake/another-session.jsonl" });
      const second = yield* runtime.ensureThread({
        threadId: ThreadId.make("second-thread"),
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: {
          ...first,
          nativeThreadRef: {
            driver: PI_PROVIDER,
            nativeId: "/fake/another-session.jsonl",
            strength: "strong",
          },
        },
      });
      fake.queueMessages(messages);
      const b = yield* runtime.readThreadSnapshot({ providerThread: second });
      assert.notEqual(a.messages[0]!.id, b.messages[0]!.id);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("rejects a nonpersistent session UUID instead of treating it as a resumable path", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      fake.queueState({ sessionFile: undefined });
      const result = yield* runtime
        .ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        })
        .pipe(Effect.result);
      assert.equal(result._tag, "Failure");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a settled turn's late prompt rejection off the next turn", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread, "default", [], "/my-command");
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_settled" });
      const firstTerminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(firstTerminal.type === "turn.terminal" && firstTerminal.status === "completed");

      yield* startTurn(runtime, providerThread, "default", [], "Second turn", undefined, 2);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({
        type: "response",
        command: "prompt",
        success: false,
        error: "late command rejection",
      });
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_settled" });
      const secondTerminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(
        secondTerminal.type === "turn.terminal" && secondTerminal.status === "completed",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows compaction progress and completes the same activity row", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "compaction_start", reason: "threshold" });

      const runningNode = yield* takeEvent(
        (event) => event.type === "node.updated" && event.node.kind === "system",
      );
      const runningItem = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      assert.isTrue(
        runningNode.type === "node.updated" &&
          runningNode.node.status === "running" &&
          runningItem.type === "turn_item.updated" &&
          runningItem.turnItem.type === "compaction" &&
          runningItem.turnItem.status === "running" &&
          runningItem.turnItem.title === "Compacting context...",
      );

      yield* fake.emit({
        type: "compaction_end",
        reason: "threshold",
        result: { summary: "smaller", tokensBefore: 200_000, estimatedTokensAfter: 3_400 },
        aborted: false,
        willRetry: false,
      });
      const completedNode = yield* takeEvent(
        (event) => event.type === "node.updated" && event.node.kind === "system",
      );
      const completedItem = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      assert.isTrue(
        runningNode.type === "node.updated" &&
          completedNode.type === "node.updated" &&
          runningItem.type === "turn_item.updated" &&
          runningItem.turnItem.type === "compaction" &&
          completedItem.type === "turn_item.updated" &&
          completedItem.turnItem.type === "compaction" &&
          completedNode.node.id === runningNode.node.id &&
          completedNode.node.status === "completed" &&
          completedItem.turnItem.id === runningItem.turnItem.id &&
          completedItem.turnItem.ordinal === runningItem.turnItem.ordinal &&
          completedItem.turnItem.startedAt === runningItem.turnItem.startedAt &&
          completedItem.turnItem.status === "completed" &&
          completedItem.turnItem.title === "Context compacted" &&
          completedItem.turnItem.beforeTokenCount === 200_000 &&
          completedItem.turnItem.afterTokenCount === 3_400,
      );

      yield* fake.emit({ type: "agent_settled" });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("uses distinct compaction IDs for first turns in separate threads", () =>
    Effect.gen(function* () {
      const firstFake = yield* makeFakePi;
      const { runtime: firstRuntime, takeEvent: takeFirstEvent } = yield* openRuntime(firstFake);
      const firstProviderThread = yield* firstRuntime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(firstRuntime, firstProviderThread);
      yield* firstFake.takeRequest("prompt");
      yield* firstFake.emit({ type: "agent_start" });
      yield* firstFake.emit({ type: "compaction_start", reason: "threshold" });
      const first = yield* takeFirstEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      const secondThreadId = ThreadId.make("thread-pi-test-second");
      const secondFake = yield* makeFakePi;
      const { runtime: secondRuntime, takeEvent: takeSecondEvent } = yield* openRuntime(
        secondFake,
        "default",
        secondThreadId,
        ProviderSessionId.make("provider-session-pi-test-second"),
      );
      const secondProviderThread = yield* secondRuntime.ensureThread({
        threadId: secondThreadId,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(
        secondRuntime,
        secondProviderThread,
        "default",
        [],
        "Hello from another thread",
        undefined,
        1,
        secondThreadId,
      );
      yield* secondFake.takeRequest("prompt");
      yield* secondFake.emit({ type: "agent_start" });
      yield* secondFake.emit({ type: "compaction_start", reason: "threshold" });
      const second = yield* takeSecondEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      assert.isTrue(
        first.type === "turn_item.updated" &&
          first.turnItem.type === "compaction" &&
          second.type === "turn_item.updated" &&
          second.turnItem.type === "compaction" &&
          first.turnItem.ordinal === second.turnItem.ordinal &&
          first.turnItem.id !== second.turnItem.id,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows aborted compactions as stopped", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      const running = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      yield* fake.emit({
        type: "compaction_end",
        reason: "manual",
        aborted: true,
        willRetry: false,
      });
      const stopped = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      assert.isTrue(
        running.type === "turn_item.updated" &&
          running.turnItem.type === "compaction" &&
          stopped.type === "turn_item.updated" &&
          stopped.turnItem.type === "compaction" &&
          stopped.turnItem.id === running.turnItem.id &&
          stopped.turnItem.status === "cancelled" &&
          stopped.turnItem.title === "Context compaction stopped",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps the turn open and updates one retry row through final failure", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");

      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_end", messages: [], willRetry: true });
      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 1,
        maxAttempts: 3,
        delayMs: 3_000,
        errorMessage: "529 overloaded",
      });
      const firstRetry = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(
        firstRetry.type === "turn_item.updated" &&
          firstRetry.turnItem.type === "error" &&
          firstRetry.turnItem.status === "running" &&
          firstRetry.turnItem.title === "Provider retry" &&
          firstRetry.turnItem.failure.retryable === true &&
          firstRetry.turnItem.retry?.attempt === 1 &&
          firstRetry.turnItem.retry.maxAttempts === 3 &&
          firstRetry.turnItem.retry.retryDelayMs === 3_000,
      );

      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 3,
        maxAttempts: 3,
        delayMs: 12_000,
        errorMessage: "529 still overloaded",
      });
      const lastRetry = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(
        firstRetry.type === "turn_item.updated" &&
          firstRetry.turnItem.type === "error" &&
          lastRetry.type === "turn_item.updated" &&
          lastRetry.turnItem.type === "error" &&
          lastRetry.turnItem.id === firstRetry.turnItem.id &&
          lastRetry.turnItem.startedAt === firstRetry.turnItem.startedAt &&
          lastRetry.turnItem.retry?.attempt === 3,
      );

      yield* fake.emit({
        type: "auto_retry_end",
        success: false,
        attempt: 3,
        finalError: "529 overloaded",
      });
      const failedRetry = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "error" &&
          event.turnItem.status === "failed",
      );
      assert.isTrue(
        firstRetry.type === "turn_item.updated" &&
          firstRetry.turnItem.type === "error" &&
          failedRetry.type === "turn_item.updated" &&
          failedRetry.turnItem.type === "error" &&
          failedRetry.turnItem.id === firstRetry.turnItem.id &&
          failedRetry.turnItem.title === "Provider error" &&
          failedRetry.turnItem.failure.retryable === false &&
          failedRetry.turnItem.retry?.attempt === 3 &&
          failedRetry.turnItem.retry.maxAttempts === 3,
      );
      yield* fake.emit({ type: "agent_settled" });

      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "failed");
      assert.isTrue(
        firstRetry.type === "turn_item.updated" &&
          firstRetry.turnItem.type === "error" &&
          terminal.type === "turn.terminal" &&
          terminal.status === "failed" &&
          terminal.failure.message.includes("overloaded") &&
          terminal.retry?.attempt === 3 &&
          terminal.retry.maxAttempts === 3 &&
          terminal.retryStartedAt === firstRetry.turnItem.startedAt,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("preserves exhausted retry failure through non-retrying compaction", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 5,
        maxAttempts: 5,
        delayMs: 48_000,
        errorMessage: "socket timed out",
      });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      yield* fake.emit({
        type: "auto_retry_end",
        success: false,
        attempt: 5,
        finalError: "socket timed out",
      });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "error" &&
          event.turnItem.status === "failed",
      );
      yield* fake.emit({ type: "compaction_start", reason: "threshold" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );
      yield* fake.emit({
        type: "compaction_end",
        reason: "threshold",
        result: { summary: "smaller", tokensBefore: 200_000, estimatedTokensAfter: 3_400 },
        aborted: false,
        willRetry: false,
      });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "compaction" &&
          event.turnItem.status === "completed",
      );
      yield* fake.emit({ type: "agent_settled" });

      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(
        terminal.type === "turn.terminal" &&
          terminal.status === "failed" &&
          terminal.failure.message === "socket timed out" &&
          terminal.retry?.attempt === 5 &&
          terminal.retry.maxAttempts === 5,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("marks retry progress recovered when Pi succeeds", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "message_end",
        message: {
          role: "assistant",
          content: [],
          stopReason: "error",
          errorMessage: "temporary network failure",
        },
      });
      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 1,
        maxAttempts: 5,
        delayMs: 3_000,
        errorMessage: "temporary network failure",
      });
      const running = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      yield* fake.emit({ type: "auto_retry_end", success: true, attempt: 1 });
      const recovered = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(
        running.type === "turn_item.updated" &&
          running.turnItem.type === "error" &&
          recovered.type === "turn_item.updated" &&
          recovered.turnItem.type === "error" &&
          recovered.turnItem.id === running.turnItem.id &&
          recovered.turnItem.status === "completed" &&
          recovered.turnItem.title === "Provider recovered",
      );

      yield* fake.emit({ type: "agent_settled" });
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect.each([undefined, false, true])(
    "preserves the native settlement outcome, aborted=%s",
    (aborted) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({
          type: "agent_settled",
          ...(aborted === undefined ? {} : { aborted }),
        });
        const completed = yield* takeEvent(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status !== "running",
        );
        const status = aborted === true ? "interrupted" : "completed";
        assert.isTrue(
          completed.type === "provider_turn.updated" && completed.providerTurn.status === status,
        );
        const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(
          terminal.type === "turn.terminal" &&
            terminal.status === status &&
            terminal.failure === null,
        );
        assert.equal(runtime.providerSession.status, "ready");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("stops retry progress when Pi aborts the native run", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 2,
        maxAttempts: 5,
        delayMs: 6_000,
        errorMessage: "temporary network failure",
      });
      const retrying = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      yield* fake.emit({ type: "agent_settled", aborted: true });
      const stopped = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(
        retrying.type === "turn_item.updated" &&
          stopped.type === "turn_item.updated" &&
          stopped.turnItem.id === retrying.turnItem.id &&
          stopped.turnItem.status === "interrupted",
      );
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("stops active retry progress when the turn is interrupted", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const runningTurn = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      const providerTurnId =
        runningTurn.type === "provider_turn.updated" ? runningTurn.providerTurn.id : undefined;
      assert.isDefined(providerTurnId);

      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 2,
        maxAttempts: 5,
        delayMs: 6_000,
        errorMessage: "temporary network failure",
      });
      const retrying = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      yield* runtime.interruptTurn({ providerThread, providerTurnId: providerTurnId! });
      yield* fake.takeRequest("abort");
      yield* fake.emit({ type: "agent_settled" });

      const stopped = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(
        retrying.type === "turn_item.updated" &&
          retrying.turnItem.type === "error" &&
          stopped.type === "turn_item.updated" &&
          stopped.turnItem.type === "error" &&
          stopped.turnItem.id === retrying.turnItem.id &&
          stopped.turnItem.status === "interrupted" &&
          stopped.turnItem.title === "Provider retry stopped",
      );
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps extension-started compaction and recovery in the settled turn", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });

      fake.queueState({ isStreaming: false, isCompacting: true, pendingMessageCount: 0 });
      yield* fake.emit({ type: "agent_settled" });
      yield* fake.takeRequest("get_state");
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      fake.queueState({ isStreaming: true, isCompacting: false, pendingMessageCount: 0 });
      yield* fake.emit({
        type: "compaction_end",
        reason: "manual",
        result: { summary: "smaller", tokensBefore: 10_000, estimatedTokensAfter: 2_000 },
        aborted: false,
        willRetry: false,
      });
      yield* fake.emit({ type: "agent_start" });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "compaction" &&
          event.turnItem.status === "completed",
      );
      yield* fake.takeRequest("get_state");

      fake.queueState({ isStreaming: false, isCompacting: false, pendingMessageCount: 0 });
      yield* fake.emit({ type: "agent_settled" });
      yield* fake.takeRequest("get_state");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps working after a settle probe fails before detached compaction", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });

      fake.failNextState();
      yield* fake.emit({ type: "agent_settled" });
      yield* fake.takeRequest("get_state");
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      fake.queueState({ isStreaming: false, isCompacting: false, pendingMessageCount: 0 });
      yield* fake.emit({
        type: "compaction_end",
        reason: "manual",
        result: { summary: "smaller", tokensBefore: 10_000, estimatedTokensAfter: 2_000 },
        aborted: false,
        willRetry: false,
      });
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "compaction" &&
          event.turnItem.status === "completed",
      );
      yield* fake.takeRequest("get_state");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("restarts Pi when Stop interrupts detached compaction", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      assert.equal(running.type, "provider_turn.updated");
      const providerTurnId =
        running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
      assert.isDefined(providerTurnId);
      yield* fake.emit({ type: "agent_start" });

      fake.queueState({ isStreaming: false, isCompacting: true, pendingMessageCount: 0 });
      yield* fake.emit({ type: "agent_settled" });
      yield* fake.takeRequest("get_state");
      yield* fake.emit({ type: "compaction_start", reason: "manual" });
      yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "compaction",
      );

      yield* runtime.interruptTurn({ providerThread, providerTurnId: providerTurnId! });
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "abort"));
      yield* fake.closeStdout;
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("ignores an idle snapshot made stale by a steer", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      assert.equal(running.type, "provider_turn.updated");
      const providerTurnId =
        running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
      assert.isDefined(providerTurnId);
      yield* fake.emit({ type: "agent_start" });

      fake.deferNextState();
      yield* fake.emit({ type: "agent_settled" });
      yield* fake.takeRequest("get_state");
      yield* runtime.steerTurn({
        threadId: THREAD_ID,
        runId: RunId.make("run:thread-pi-test:1"),
        providerThread,
        providerTurnId: providerTurnId!,
        message: {
          messageId: "message:thread-pi-test:late-steer" as never,
          text: "Continue after settlement",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
      });
      yield* fake.takeRequest("prompt");
      yield* fake.resolveDeferredState({
        isStreaming: false,
        isCompacting: false,
        pendingMessageCount: 0,
      });

      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: { role: "assistant" } });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: { type: "text_end", contentIndex: 0, content: "Recovered" },
      });
      yield* fake.emit({
        type: "message_end",
        message: {
          role: "assistant",
          content: [{ type: "text", text: "Recovered" }],
          stopReason: "stop",
        },
      });
      const assistantItem = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "assistant_message" &&
          event.turnItem.streaming === false,
      );
      assert.isTrue(
        assistantItem.type === "turn_item.updated" &&
          assistantItem.turnItem.type === "assistant_message" &&
          assistantItem.turnItem.text === "Recovered",
      );

      fake.queueState({ isStreaming: false, isCompacting: false, pendingMessageCount: 0 });
      yield* fake.emit({ type: "agent_settled" });
      yield* fake.takeRequest("get_state");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

describe("PiRpc early process exit", () => {
  const makeHandle = (options: {
    readonly exitCode: Effect.Effect<ChildProcessSpawner.ExitCode>;
    readonly stderr: Stream.Stream<Uint8Array>;
  }) =>
    ChildProcessSpawner.makeHandle({
      pid: ChildProcessSpawner.ProcessId(FAKE_PID),
      exitCode: options.exitCode,
      isRunning: Effect.succeed(true),
      kill: () => Effect.void,
      unref: Effect.succeed(Effect.void),
      stdin: Sink.drain,
      stdout: Stream.empty,
      stderr: options.stderr,
      all: Stream.empty,
      getInputFd: () => Sink.drain,
      getOutputFd: () => Stream.empty,
    });

  it.effect("reports a nonzero exit code instead of an unexplained stdout close", () =>
    Effect.gen(function* () {
      const secret = "API_KEY=super-secret\n";
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          makeHandle({
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
            stderr: Stream.fromIterable([new TextEncoder().encode(secret)]),
          }),
        ),
      );
      const connection = yield* makePiRpcConnection({
        command: "pi",
        args: ["--mode", "rpc"],
        cwd: undefined,
        env: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

      const error = yield* Queue.take(connection.events).pipe(Effect.flip);
      assert.equal(error._tag, "PiRpcError");
      assert.equal(error.operation, "read");
      assert.equal(error.detail, "pi process exited with code 1");
      assert.isFalse((error.detail ?? "").includes("API_KEY"));
      assert.isFalse((error.detail ?? "").includes("super-secret"));
      assert.isFalse(error.message.includes("API_KEY"));
      assert.isFalse(error.message.includes("super-secret"));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps the unexplained stdout-close message when the process has not exited", () =>
    Effect.gen(function* () {
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          makeHandle({
            exitCode: Effect.never,
            stderr: Stream.empty,
          }),
        ),
      );
      const connection = yield* makePiRpcConnection({
        command: "pi",
        args: ["--mode", "rpc"],
        cwd: undefined,
        env: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

      const fiber = yield* Effect.forkChild(Queue.take(connection.events).pipe(Effect.flip));
      yield* TestClock.adjust(Duration.millis(300));
      const error = yield* Fiber.join(fiber);
      assert.equal(error._tag, "PiRpcError");
      assert.equal(error.operation, "read");
      assert.equal(error.detail, "pi process closed stdout");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps the exit-code diagnosis when stdin breaks while exit is still pending", () =>
    Effect.gen(function* () {
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(FAKE_PID),
            exitCode: Effect.sleep(Duration.millis(50)).pipe(
              Effect.andThen(Effect.succeed(ChildProcessSpawner.ExitCode(1))),
            ),
            isRunning: Effect.succeed(true),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.fail(
              PlatformError.systemError({
                _tag: "Unknown",
                module: "ChildProcess",
                method: "stdin",
                description: "broken pipe",
              }),
            ),
            stdout: Stream.empty,
            stderr: Stream.empty,
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
          }),
        ),
      );
      const connection = yield* makePiRpcConnection({
        command: "pi",
        args: ["--mode", "rpc"],
        cwd: undefined,
        env: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

      const fiber = yield* Effect.forkChild(Queue.take(connection.events).pipe(Effect.flip));
      yield* TestClock.adjust(Duration.millis(300));
      const error = yield* Fiber.join(fiber);
      assert.equal(error._tag, "PiRpcError");
      assert.equal(error.operation, "read");
      assert.equal(error.detail, "pi process exited with code 1");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("PiAdapterV2 with the Prime Agent flavor", () => {
  const openPrimeThread = Effect.fnUntraced(function* (fake: FakePi, threadId = THREAD_ID) {
    const { runtime, takeEvent } = yield* openRuntime(
      fake,
      "default",
      threadId,
      SESSION_ID,
      undefined,
      PRIME_AGENT_FLAVOR,
    );
    const providerThread = yield* runtime.ensureThread({
      threadId,
      modelSelection: modelSelection("default"),
      runtimePolicy,
    });
    return { runtime, takeEvent, providerThread };
  });

  /** Prime Agent keeps an action active through retry waits and post-run work. */
  const busyState = {
    sessionActions: {
      queuedCount: 0,
      steering: [],
      followUps: [],
      active: { kind: "turn", phase: "running" },
    },
  };

  /** Advances virtual time in busy-probe steps until the fake sees another `get_state`. */
  const takeReprobe = (fake: FakePi) =>
    Effect.gen(function* () {
      const reprobe = yield* fake.takeRequest("get_state").pipe(Effect.forkScoped);
      for (let step = 0; step < 20 && reprobe.pollUnsafe() === undefined; step += 1) {
        yield* TestClock.adjust(Duration.millis(100));
        yield* Effect.yieldNow;
      }
      return yield* Fiber.join(reprobe);
    });

  it.effect("cancels a pending auto-retry before it replaces the session", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, providerThread } = yield* openPrimeThread(fake);
      const requestTypes = () => fake.allRequests().map((request) => request.type);
      assert.notInclude(requestTypes(), "abort_retry");

      // The thread now lives in another session file, so the process must swap.
      yield* runtime.resumeThread({
        providerThread: {
          ...providerThread,
          nativeThreadRef: {
            driver: PRIME_AGENT_FLAVOR.driverKind,
            nativeId: "/fake/other.jsonl",
            strength: "strong",
          },
        },
      });

      const types = requestTypes();
      assert.isAbove(types.indexOf("abort_retry"), -1);
      assert.isBelow(types.indexOf("abort_retry"), types.indexOf("switch_session"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("settles a turn after agent_end only once get_state shows no active work", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });

      fake.queueState(busyState);
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      // The idle re-probe answers with the fake's default idle state.
      yield* takeReprobe(fake);

      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // Prime Agent answers get_state through its daemon. Under heavy load a reply
  // can miss the probe timeout while the agent keeps working. Three misses in a
  // row used to end the process, which then showed as an unexpected exit.
  it.effect("keeps a busy turn and its process when get_state goes unanswered", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });

      fake.queueState(busyState);
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      yield* Effect.gen(function* () {
        for (let miss = 0; miss < 4; miss += 1) {
          fake.failNextState();
          yield* takeReprobe(fake);
        }
        // The re-probe after the misses answers with the fake's idle state.
        yield* takeReprobe(fake);
      }).pipe(Effect.forkScoped);

      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a retrying turn open across agent_end and settles on the recovered run", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "message_end",
        message: {
          role: "assistant",
          content: [],
          stopReason: "error",
          errorMessage: "overloaded",
        },
      });

      fake.queueState(busyState);
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 1,
        maxAttempts: 3,
        delayMs: 2_000,
        errorMessage: "overloaded",
      });
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "auto_retry_end", success: true, attempt: 1 });
      yield* fake.emit({ type: "agent_end", messages: [] });

      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  type TakeEvent = (
    predicate: (event: ProviderAdapterV2Event) => boolean,
  ) => Effect.Effect<ProviderAdapterV2Event>;

  /** Completed assistant texts of a turn, in order, and how the turn ended. */
  const takeRepliesAndOutcome = (takeEvent: TakeEvent) =>
    Effect.gen(function* () {
      const texts: Array<string> = [];
      for (;;) {
        const event = yield* takeEvent(
          (candidate) =>
            candidate.type === "turn.terminal" ||
            (candidate.type === "turn_item.updated" &&
              candidate.turnItem.type === "assistant_message" &&
              candidate.turnItem.status === "completed"),
        );
        if (event.type === "turn.terminal") return { texts, status: event.status };
        if (event.type === "turn_item.updated" && "text" in event.turnItem) {
          texts.push(event.turnItem.text);
        }
      }
    });

  /** Completed assistant texts of a turn, in order, until the turn ends. */
  const takeCompletedReplies = (takeEvent: TakeEvent) =>
    takeRepliesAndOutcome(takeEvent).pipe(Effect.map((outcome) => outcome.texts));

  const assistantSnapshot = (timestamp: number, text: string) => ({
    role: "assistant",
    content: [{ type: "text", text }],
    timestamp,
  });

  /**
   * Advances the clock to the watchdog tick that probes a quiet turn, and takes
   * the probe. The first probe comes two 15 s ticks in, each later one a tick
   * after the last. Stopping at the tick keeps the 2 s reads that follow from
   * timing out under the clock.
   */
  const takeQuietProbe = (fake: FakePi, after: Duration.Input = Duration.seconds(30)) =>
    Effect.gen(function* () {
      yield* TestClock.adjust(after);
      return yield* fake.takeRequest("get_state");
    });

  // Prime Agent's daemon socket drops events when it backs up, and its RPC
  // never says so. A dropped agent_end must not leave the turn running.
  it.effect("settles a turn whose message_end, turn_end and agent_end were all dropped", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "The final reply."),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "The final reply." },
      });

      yield* takeQuietProbe(fake);

      assert.deepEqual(yield* takeCompletedReplies(takeEvent), ["The final reply."]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a quiet turn open while get_state shows the agent still running", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });

      const terminalFiber = yield* takeEvent((event) => event.type === "turn.terminal").pipe(
        Effect.forkScoped,
      );
      // A long tool call says nothing, and get_state reports it as streaming.
      fake.queueState({ isStreaming: true });
      fake.queueState({ isStreaming: true });
      yield* takeQuietProbe(fake);
      yield* takeQuietProbe(fake, Duration.seconds(15));
      assert.isUndefined(terminalFiber.pollUnsafe());

      yield* takeQuietProbe(fake, Duration.seconds(15));
      const terminal = yield* Fiber.join(terminalFiber);
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  const fullReply =
    "The cause is a race. **Show the cause.** When the process exits, the socket closes first, " +
    "so the last events are lost. Fix: read the final message from the record.";
  const cutReply = fullReply.slice(0, fullReply.indexOf("the socket"));

  /** A reply cut off at `cutReply`: its later updates, message_end, and agent_end were dropped. */
  const streamCutReply = (fake: FakePi) =>
    Effect.gen(function* () {
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, cutReply),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: cutReply },
      });
    });

  /** The user prompt Prime Agent stores at the start of a turn. */
  const storedPrompt = (timestamp: number, text = "Hello pi") => ({
    role: "user",
    content: [{ type: "text", text }],
    timestamp,
  });

  /** A finished assistant message as Prime Agent stores it. */
  const storedReply = (timestamp: number, text: string) => ({
    ...assistantSnapshot(timestamp, text),
    stopReason: "stop",
  });

  /** The stored conversation after a turn whose reply is `fullReply`. */
  const recordedTurn = [storedPrompt(900), storedReply(1000, fullReply)];

  it.effect("completes a reply cut off by dropped events with Prime Agent's recorded text", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* streamCutReply(fake);
      fake.setTranscript(recordedTurn);

      yield* takeQuietProbe(fake);

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [fullReply],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // Thread 83be3aac lost 7 whole replies in one hour: none of their events
  // reached T3, so there was no item to lengthen.
  it.effect("recovers a reply whose every event was dropped", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      fake.setTranscript(recordedTurn);
      yield* fake.emit({ type: "agent_end", messages: [] });

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [fullReply],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("settles a turn that went quiet without showing one agent event", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      fake.setTranscript(recordedTurn);

      yield* takeQuietProbe(fake);

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [fullReply],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("lengthens a cut-off reply whose item was already completed", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* streamCutReply(fake);
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, cutReply),
        assistantMessageEvent: { type: "text_end", contentIndex: 0, content: cutReply },
      });
      fake.setTranscript(recordedTurn);

      yield* takeQuietProbe(fake);

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [cutReply, fullReply],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /**
   * What the turn shows, in the order items first show it: each reply once
   * complete, tool states, and notices. Read until the turn ends, or until
   * it shows `until`.
   */
  const takeShownTimeline = (takeEvent: TakeEvent, until?: string) =>
    Effect.gen(function* () {
      const shown: Array<string> = [];
      for (;;) {
        const event = yield* takeEvent(
          (candidate) =>
            candidate.type === "turn.terminal" ||
            (candidate.type === "turn_item.updated" &&
              (candidate.turnItem.type === "dynamic_tool" ||
                candidate.turnItem.type === "system_notice" ||
                (candidate.turnItem.type === "assistant_message" &&
                  candidate.turnItem.status === "completed"))),
        );
        if (event.type !== "turn_item.updated") return shown;
        const item = event.turnItem;
        const entry =
          item.type === "assistant_message"
            ? item.text
            : item.type === "system_notice"
              ? "notice"
              : `tool:${item.status}`;
        if (shown.at(-1) !== entry) shown.push(entry);
        if (entry === until) return shown;
      }
    });

  const removalThinking = { type: "thinking", thinking: "Remove it everywhere." };
  const removalReply =
    "I'll remove The Simpsons everywhere: Sonarr, the `nu1` library, qBittorrent, and the local files.";
  const removalToolCall = {
    type: "toolCall",
    id: "toolu_01M9ZHmB7NfmoEamSRU4fqzW",
    name: "ipython",
    arguments: { code: "print(jobs)" },
  };
  const storedRemovalReply = {
    role: "assistant",
    content: [removalThinking, { type: "text", text: removalReply }, removalToolCall],
    stopReason: "toolUse",
    timestamp: 1000,
  };
  const storedRemovalResult = {
    role: "toolResult",
    toolCallId: removalToolCall.id,
    toolName: "ipython",
    content: [{ type: "text", text: "[]" }],
    isError: false,
    timestamp: 1500,
  };
  const removalToolStart = {
    type: "tool_execution_start",
    toolCallId: removalToolCall.id,
    toolName: removalToolCall.name,
    args: removalToolCall.arguments,
  };
  const removalToolEnd = {
    type: "tool_execution_end",
    toolCallId: removalToolCall.id,
    toolName: removalToolCall.name,
    result: { content: storedRemovalResult.content },
    isError: false,
  };
  const removedReply = storedReply(2000, "Removed.");

  /** Streams `removalReply` up to `cutAt`, as the daemon sent it before backing up. */
  const streamRemovalReplyUntil = (fake: FakePi, cutAt: number) =>
    Effect.gen(function* () {
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "message_start",
        message: { role: "assistant", content: [], timestamp: 1000 },
      });
      const text = removalReply.slice(0, cutAt);
      yield* fake.emit({
        type: "message_update",
        message: {
          role: "assistant",
          content: [removalThinking, { type: "text", text }],
          timestamp: 1000,
        },
        assistantMessageEvent: { type: "text_delta", contentIndex: 1, delta: text },
      });
    });

  // Thread 87b755f3 (2026-10-08): the daemon backed up mid-reply, so the
  // stream lost the rest of the reply and its message_end, then carried on
  // with the tool call. The cut reply was shown complete until the run ended.
  it.effect("repairs a reply whose message_end was dropped before its tool starts", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");

      yield* streamRemovalReplyUntil(fake, removalReply.indexOf("nu1"));
      // Prime Agent stores the reply at its message_end, before the tool runs.
      fake.setTranscript([storedPrompt(900), storedRemovalReply]);
      yield* fake.emit(removalToolStart);
      yield* fake.emit(removalToolEnd);
      fake.setTranscript([
        storedPrompt(900),
        storedRemovalReply,
        storedRemovalResult,
        removedReply,
      ]);
      yield* fake.emit({ type: "message_start", message: { ...removedReply, content: [] } });
      yield* fake.emit({ type: "message_end", message: removedReply });
      yield* fake.emit({ type: "agent_end", messages: [] });

      assert.deepEqual(yield* takeShownTimeline(takeEvent), [
        removalReply,
        "tool:running",
        "tool:completed",
        "Removed.",
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a cut reply open until its stored version can be read", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");

      fake.setTranscript([storedPrompt(900)]);
      yield* streamRemovalReplyUntil(fake, removalReply.indexOf("nu1"));
      yield* fake.emit(removalToolStart);
      yield* fake.emit(removalToolEnd);
      yield* fake.emit({ type: "message_start", message: { ...removedReply, content: [] } });
      yield* fake.emit({ type: "message_end", message: removedReply });
      const shownUnread = yield* takeShownTimeline(takeEvent, "Removed.");
      fake.setTranscript([
        storedPrompt(900),
        storedRemovalReply,
        storedRemovalResult,
        removedReply,
      ]);
      yield* fake.emit({ type: "agent_end", messages: [] });

      assert.deepEqual(
        [...shownUnread, ...(yield* takeShownTimeline(takeEvent))],
        ["tool:running", "tool:completed", "Removed.", removalReply],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("ends a tool whose tool_execution_end was dropped once the next reply starts", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");

      yield* streamRemovalReplyUntil(fake, removalReply.length);
      yield* fake.emit({ type: "message_end", message: storedRemovalReply });
      yield* fake.emit(removalToolStart);
      fake.setTranscript([
        storedPrompt(900),
        storedRemovalReply,
        storedRemovalResult,
        removedReply,
      ]);
      yield* fake.emit({ type: "message_start", message: { ...removedReply, content: [] } });
      yield* fake.emit({ type: "message_end", message: removedReply });
      yield* fake.emit({ type: "agent_end", messages: [] });

      assert.deepEqual(yield* takeShownTimeline(takeEvent), [
        removalReply,
        "tool:running",
        "tool:completed",
        "Removed.",
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // The same run lost its final reply's message_end too. Its turn_end came,
  // but Prime Agent then refined its harness for half a minute before idling.
  it.effect("repairs a final reply whose message_end was dropped once its turn ends", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");

      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: { ...removedReply, content: [] } });
      yield* fake.emit({
        type: "message_update",
        message: storedReply(2000, "Remo"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Remo" },
      });
      fake.setTranscript([storedPrompt(900), removedReply]);
      yield* fake.emit({ type: "turn_end", message: removedReply, toolResults: [] });
      yield* fake.emit({
        type: "refine_complete",
        result: { id: "refine_1", summary: "Nothing to keep.", appliedEdits: [] },
      });
      yield* fake.emit({ type: "agent_end", messages: [] });

      assert.deepEqual(yield* takeShownTimeline(takeEvent), ["Removed.", "notice"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  const historyReads = (fake: FakePi) =>
    fake.allRequests().filter((request) => request["type"] === "get_messages").length;

  it.effect("looks up a lost reply end once, then leaves it to the settle read", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      fake.setTranscript([storedPrompt(900)]);
      const readsBeforeReply = historyReads(fake);

      yield* streamRemovalReplyUntil(fake, removalReply.indexOf("nu1"));
      yield* fake.emit(removalToolStart);
      yield* fake.emit(removalToolEnd);
      yield* fake.emit({ type: "message_start", message: { ...removedReply, content: [] } });
      yield* fake.emit({ type: "message_end", message: removedReply });
      const shownUnread = yield* takeShownTimeline(takeEvent, "Removed.");
      yield* fake.emit({ type: "turn_end", message: removedReply, toolResults: [] });
      fake.setTranscript([
        storedPrompt(900),
        storedRemovalReply,
        storedRemovalResult,
        removedReply,
      ]);
      yield* fake.emit({ type: "agent_end", messages: [] });

      assert.deepEqual(
        [...shownUnread, ...(yield* takeShownTimeline(takeEvent))],
        ["tool:running", "tool:completed", "Removed.", removalReply],
      );
      assert.equal(historyReads(fake) - readsBeforeReply, 2, "one gap read, then the settle read");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // Stop cuts a reply off. Stock Prime Agent ends it with a copy stamped at
  // abort time and no message_start of its own (agent-loop.js
  // finishAbortedMessage), and stores only the copy.
  it.effect("shows a reply Stop cut off once, ended by its aborted copy", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      assert.isTrue(running.type === "provider_turn.updated");
      if (running.type !== "provider_turn.updated") return;
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, cutReply),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: cutReply },
      });
      const abortedCopy = {
        ...assistantSnapshot(5000, cutReply),
        stopReason: "aborted",
        errorMessage: "Request was aborted",
      };
      fake.setTranscript([storedPrompt(900), abortedCopy]);
      const readsBeforeStop = historyReads(fake);

      yield* runtime.interruptTurn({ providerThread, providerTurnId: running.providerTurn.id });
      yield* fake.emit({ type: "message_end", message: abortedCopy });
      yield* fake.emit({ type: "turn_end", message: abortedCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [abortedCopy] });

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [cutReply],
        status: "interrupted",
      });
      assert.equal(historyReads(fake) - readsBeforeStop, 1, "only Stop reads the history");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows a reply Stop cut off once when Stop restarts Prime Agent before its end", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      assert.isTrue(running.type === "provider_turn.updated");
      if (running.type !== "provider_turn.updated") return;
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, cutReply),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: cutReply },
      });
      fake.setTranscript([
        storedPrompt(900),
        { ...assistantSnapshot(5000, cutReply), stopReason: "aborted" },
      ]);

      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: running.providerTurn.id,
        requestRuntimeRestart: true,
      });
      yield* fake.closeStdout;

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [cutReply],
        status: "interrupted",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /** The final text of each reply item the turn shows, in the order the turn placed them. */
  const takeFinalReplyTexts = (takeEvent: TakeEvent) =>
    Effect.gen(function* () {
      const items = new Map<string, { readonly ordinal: number; readonly text: string }>();
      for (;;) {
        const event = yield* takeEvent(
          (candidate) =>
            candidate.type === "turn.terminal" ||
            (candidate.type === "turn_item.updated" &&
              candidate.turnItem.type === "assistant_message"),
        );
        if (event.type !== "turn_item.updated" || event.turnItem.type !== "assistant_message") {
          return [...items.values()]
            .sort((left, right) => left.ordinal - right.ordinal)
            .map((item) => item.text);
        }
        items.set(event.turnItem.id, {
          ordinal: event.turnItem.ordinal,
          text: event.turnItem.text,
        });
      }
    });

  /**
   * The completed texts of each reply item, in the order the turn placed the
   * items. An item whose text changed after it completed lists every text.
   */
  const takeCompletedReplyTexts = (takeEvent: TakeEvent) =>
    Effect.gen(function* () {
      const items = new Map<string, { readonly ordinal: number; readonly texts: Array<string> }>();
      for (;;) {
        const event = yield* takeEvent(
          (candidate) =>
            candidate.type === "turn.terminal" ||
            (candidate.type === "turn_item.updated" &&
              candidate.turnItem.type === "assistant_message" &&
              candidate.turnItem.status === "completed"),
        );
        if (event.type !== "turn_item.updated" || event.turnItem.type !== "assistant_message") {
          return [...items.values()]
            .sort((left, right) => left.ordinal - right.ordinal)
            .map((item) => item.texts);
        }
        const item = items.get(event.turnItem.id) ?? {
          ordinal: event.turnItem.ordinal,
          texts: [],
        };
        if (item.texts.at(-1) !== event.turnItem.text) item.texts.push(event.turnItem.text);
        items.set(event.turnItem.id, item);
      }
    });

  const takeRunningTurnId = (takeEvent: TakeEvent) =>
    Effect.gen(function* () {
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      assert.isTrue(running.type === "provider_turn.updated");
      return running.type === "provider_turn.updated" ? running.providerTurn.id : undefined;
    });

  // An aborted copy carries no message_start, so a lost start leaves nothing
  // that ties it to an earlier reply whose end was lost too.
  it.effect("keeps an earlier cut reply when a later reply whose start was lost is stopped", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "First reply"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "First reply" },
      });
      fake.setTranscript([storedPrompt(900)]);
      yield* fake.emit(removalToolStart);
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "dynamic_tool" &&
          event.turnItem.status === "running",
      );
      yield* fake.emit(removalToolEnd);
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "dynamic_tool" &&
          event.turnItem.status === "completed",
      );
      const secondCopy = { ...assistantSnapshot(5000, "Second reply"), stopReason: "aborted" };
      fake.setTranscript([
        storedPrompt(900),
        {
          ...assistantSnapshot(1000, "First reply"),
          stopReason: "toolUse",
          content: [{ type: "text", text: "First reply" }, removalToolCall],
        },
        storedRemovalResult,
        secondCopy,
      ]);
      yield* fake.emit({ type: "turn_start" });
      yield* fake.emit({ type: "message_end", message: secondCopy });
      yield* fake.emit({ type: "turn_end", message: secondCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [secondCopy] });

      assert.deepEqual(yield* takeFinalReplyTexts(takeEvent), ["First reply", "Second reply"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // The daemon drops one unbroken run of events while it is backed up, here
  // from the first reply's end to the second reply's last update.
  it.effect("keeps an earlier cut reply when everything up to a later aborted end was lost", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "First reply"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "First reply" },
      });
      const secondCopy = { ...assistantSnapshot(5000, "Second reply"), stopReason: "aborted" };
      fake.setTranscript([
        storedPrompt(900),
        {
          ...assistantSnapshot(1000, "First reply"),
          stopReason: "toolUse",
          content: [{ type: "text", text: "First reply" }, removalToolCall],
        },
        storedRemovalResult,
        secondCopy,
      ]);
      yield* fake.emit({ type: "message_end", message: secondCopy });
      yield* fake.emit({ type: "turn_end", message: secondCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [secondCopy] });

      assert.deepEqual(yield* takeFinalReplyTexts(takeEvent), ["First reply", "Second reply"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // Stop read the first turn's history before its aborted copy was stored, so
  // the copy is still unaccounted when a later Stop reads the history again.
  it.effect("keeps an earlier turn's aborted copy off the reply a later Stop cut off", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const firstTurnId = yield* takeRunningTurnId(takeEvent);
      if (firstTurnId === undefined) return;
      const firstText = "I'll check the logs first.";
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, firstText),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: firstText },
      });
      fake.setTranscript([storedPrompt(900)]);
      yield* runtime.interruptTurn({ providerThread, providerTurnId: firstTurnId });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* takeRepliesAndOutcome(takeEvent);

      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const secondTurnId = yield* takeRunningTurnId(takeEvent);
      if (secondTurnId === undefined) return;
      const secondText = "I'll check the config.";
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(4000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(4000, "I'll check"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "I'll check" },
      });
      fake.setTranscript([
        storedPrompt(900),
        { ...assistantSnapshot(3000, firstText), stopReason: "aborted" },
        storedPrompt(3500, "And the config?"),
        { ...assistantSnapshot(5000, secondText), stopReason: "aborted" },
      ]);
      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: secondTurnId,
        requestRuntimeRestart: true,
      });
      yield* fake.closeStdout;

      assert.deepEqual(yield* takeFinalReplyTexts(takeEvent), [secondText, firstText]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // One unbroken dropped run, from the first reply's end to the second reply's
  // last update, and the second reply starts the way the first one did. Only
  // the stored history can tell the two replies apart.
  it.effect("keeps an earlier cut reply when a later stopped reply starts the same way", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "I'll check"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "I'll check" },
      });
      const secondCopy = {
        ...assistantSnapshot(5000, "I'll check the other config."),
        stopReason: "aborted",
      };
      fake.setTranscript([
        storedPrompt(900),
        {
          ...assistantSnapshot(1000, "I'll check the first config."),
          stopReason: "toolUse",
          content: [{ type: "text", text: "I'll check the first config." }, removalToolCall],
        },
        storedRemovalResult,
        secondCopy,
      ]);
      yield* fake.emit({ type: "message_end", message: secondCopy });
      yield* fake.emit({ type: "turn_end", message: secondCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [secondCopy] });

      assert.deepEqual(yield* takeFinalReplyTexts(takeEvent), [
        "I'll check the first config.",
        "I'll check the other config.",
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // The later reply's copy has a block at an index where the earlier reply
  // stored a tool call, not text. The history must place that block too.
  it.effect("shows each block of a later stopped reply once after the history splits it off", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "I'll check"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "I'll check" },
      });
      const secondCopy = {
        ...assistantSnapshot(5000, ""),
        content: [
          { type: "text", text: "I'll check the other config." },
          { type: "text", text: "Then I'll fix it." },
        ],
        stopReason: "aborted",
      };
      fake.setTranscript([
        storedPrompt(900),
        {
          ...assistantSnapshot(1000, "I'll check the first config."),
          stopReason: "toolUse",
          content: [{ type: "text", text: "I'll check the first config." }, removalToolCall],
        },
        storedRemovalResult,
        secondCopy,
      ]);
      yield* fake.emit({ type: "message_end", message: secondCopy });
      yield* fake.emit({ type: "turn_end", message: secondCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [secondCopy] });

      assert.deepEqual(yield* takeFinalReplyTexts(takeEvent), [
        "I'll check the first config.",
        "I'll check the other config.",
        "Then I'll fix it.",
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // Updates lost before Stop: the copy holds a block the stream never showed.
  it.effect("shows a block only the aborted copy holds once Stop reads the history", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const turnId = yield* takeRunningTurnId(takeEvent);
      if (turnId === undefined) return;
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "I'll check"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "I'll check" },
      });
      const abortedCopy = {
        ...assistantSnapshot(5000, ""),
        content: [
          { type: "text", text: "I'll check the config." },
          { type: "text", text: "Then I'll fix it." },
        ],
        stopReason: "aborted",
      };
      fake.setTranscript([storedPrompt(900), abortedCopy]);

      yield* runtime.interruptTurn({ providerThread, providerTurnId: turnId });
      yield* fake.emit({ type: "message_end", message: abortedCopy });
      yield* fake.emit({ type: "turn_end", message: abortedCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [abortedCopy] });

      assert.deepEqual(yield* takeFinalReplyTexts(takeEvent), [
        "I'll check the config.",
        "Then I'll fix it.",
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // Stop reads the history before any of the later reply's events arrive, so
  // the stream is still in the earlier reply, which the history holds whole.
  it.effect("keeps an earlier cut reply when Stop restarts Prime Agent during a later reply", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const turnId = yield* takeRunningTurnId(takeEvent);
      if (turnId === undefined) return;
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "I'll check"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "I'll check" },
      });
      fake.setTranscript([
        storedPrompt(900),
        {
          ...assistantSnapshot(1000, "I'll check the first config."),
          stopReason: "toolUse",
          content: [{ type: "text", text: "I'll check the first config." }, removalToolCall],
        },
        storedRemovalResult,
        { ...assistantSnapshot(5000, "I'll check the other config."), stopReason: "aborted" },
      ]);
      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: turnId,
        requestRuntimeRestart: true,
      });
      yield* fake.closeStdout;

      assert.deepEqual(yield* takeFinalReplyTexts(takeEvent), [
        "I'll check the first config.",
        "I'll check the other config.",
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // A background admission starts another run after the abort, before the
  // turn settles. The stopped copy keeps the place it arrived in, and no reply
  // shows a text it does not end with.
  it.effect(
    "keeps a stopped copy before a later run's reply and never shows it on another reply",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({
          type: "message_update",
          message: assistantSnapshot(1000, "First cut"),
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "First cut" },
        });
        const copy = { ...assistantSnapshot(5000, "Second reply"), stopReason: "aborted" };
        yield* fake.emit({ type: "message_end", message: copy });
        yield* fake.emit({ type: "turn_end", message: copy, toolResults: [] });
        yield* fake.emit({ type: "agent_end", messages: [copy] });
        yield* fake.emit({ type: "agent_start" });
        const later = assistantSnapshot(7000, "Third reply");
        fake.setTranscript([
          storedPrompt(900),
          {
            ...assistantSnapshot(1000, "First whole"),
            stopReason: "toolUse",
            content: [{ type: "text", text: "First whole" }, removalToolCall],
          },
          storedRemovalResult,
          copy,
          storedPrompt(6000, "Background notice"),
          later,
        ]);
        yield* fake.emit({ type: "message_start", message: assistantSnapshot(7000, "") });
        yield* fake.emit({
          type: "message_update",
          message: later,
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Third reply" },
        });
        yield* fake.emit({ type: "message_end", message: later });
        yield* fake.emit({ type: "agent_end", messages: [later] });

        assert.deepEqual(yield* takeCompletedReplyTexts(takeEvent), [
          ["First whole"],
          ["Second reply"],
          ["Third reply"],
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // Prime Agent can append a custom message after an abort, so the copy is
  // not the last message Stop reads.
  it.effect("shows a reply Stop cut off once when a custom message follows its copy", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const turnId = yield* takeRunningTurnId(takeEvent);
      if (turnId === undefined) return;
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "Reply cut"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Reply cut" },
      });
      fake.setTranscript([
        storedPrompt(900),
        { ...assistantSnapshot(3000, "Reply cut, then full"), stopReason: "aborted" },
        {
          role: "custom",
          customType: "agent-message",
          timestamp: 4000,
          content: "Child finished",
          display: true,
        },
      ]);
      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: turnId,
        requestRuntimeRestart: true,
      });
      yield* fake.closeStdout;

      assert.deepEqual(yield* takeFinalReplyTexts(takeEvent), ["Reply cut, then full"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // Compaction can drop a streamed reply from the history, so its absence no
  // longer shows that a copy replaced it.
  it.effect("keeps a reply compacted out of the history apart from a later stopped copy", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "First cut"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "First cut" },
      });
      yield* fake.emit({ type: "compaction_start", reason: "threshold" });
      yield* fake.emit({
        type: "compaction_end",
        reason: "threshold",
        result: { summary: "Earlier work", firstKeptEntryId: "e1", tokensBefore: 1000 },
        aborted: false,
        willRetry: false,
      });
      const copy = { ...assistantSnapshot(5000, "First cut, then more"), stopReason: "aborted" };
      fake.setTranscript([
        { role: "compactionSummary", summary: "Earlier work", tokensBefore: 1000, timestamp: 4000 },
        copy,
      ]);
      yield* fake.emit({ type: "message_end", message: copy });
      yield* fake.emit({ type: "turn_end", message: copy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [copy] });

      assert.deepEqual(yield* takeFinalReplyTexts(takeEvent), [
        "First cut",
        "First cut, then more",
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // Stop's history read fails. The copy is placed once, by a fixed rule, and a
  // reply the stream only guessed at stays unaccounted for a later read.
  it.effect("recovers an earlier reply in the next turn when Stop could not read the history", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const firstTurnId = yield* takeRunningTurnId(takeEvent);
      if (firstTurnId === undefined) return;
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "I'll check"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "I'll check" },
      });
      const secondCopy = {
        ...assistantSnapshot(5000, "I'll check the other config."),
        stopReason: "aborted",
      };
      yield* fake.emit({ type: "message_end", message: secondCopy });
      yield* fake.emit({ type: "turn_end", message: secondCopy, toolResults: [] });
      fake.queueMessages({});
      yield* runtime.interruptTurn({ providerThread, providerTurnId: firstTurnId });
      yield* fake.emit({ type: "agent_end", messages: [secondCopy] });
      assert.deepEqual(yield* takeFinalReplyTexts(takeEvent), ["I'll check the other config."]);

      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const reply = assistantSnapshot(7000, "Done.");
      fake.setTranscript([
        storedPrompt(900),
        {
          ...assistantSnapshot(1000, "I'll check the first config."),
          stopReason: "toolUse",
          content: [{ type: "text", text: "I'll check the first config." }, removalToolCall],
        },
        storedRemovalResult,
        secondCopy,
        storedPrompt(6000, "Go on"),
        reply,
      ]);
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(7000, "") });
      yield* fake.emit({
        type: "message_update",
        message: reply,
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Done." },
      });
      yield* fake.emit({ type: "message_end", message: reply });
      yield* fake.emit({ type: "agent_end", messages: [reply] });

      assert.deepEqual(yield* takeFinalReplyTexts(takeEvent), [
        "Done.",
        "I'll check the first config.",
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("finishes a cut reply with its copy once when Stop could not read the history", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const turnId = yield* takeRunningTurnId(takeEvent);
      if (turnId === undefined) return;
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, cutReply),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: cutReply },
      });
      const abortedCopy = { ...assistantSnapshot(5000, fullReply), stopReason: "aborted" };
      fake.setTranscript([storedPrompt(900), abortedCopy]);
      fake.queueMessages({});

      yield* runtime.interruptTurn({ providerThread, providerTurnId: turnId });
      yield* fake.emit({ type: "message_end", message: abortedCopy });
      yield* fake.emit({ type: "turn_end", message: abortedCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [abortedCopy] });

      assert.deepEqual(yield* takeCompletedReplyTexts(takeEvent), [[fullReply]]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "keeps a copy that does not hold the cut reply apart when Stop could not read the history",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        const turnId = yield* takeRunningTurnId(takeEvent);
        if (turnId === undefined) return;
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
        yield* fake.emit({
          type: "message_update",
          message: assistantSnapshot(1000, "First reply"),
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "First reply" },
        });
        const secondCopy = { ...assistantSnapshot(5000, "Second reply"), stopReason: "aborted" };
        yield* fake.emit({ type: "message_end", message: secondCopy });
        fake.queueMessages({});

        yield* runtime.interruptTurn({ providerThread, providerTurnId: turnId });
        yield* fake.emit({ type: "turn_end", message: secondCopy, toolResults: [] });
        yield* fake.emit({ type: "agent_end", messages: [secondCopy] });

        assert.deepEqual(yield* takeCompletedReplyTexts(takeEvent), [
          ["First reply"],
          ["Second reply"],
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // Two stopped replies and a whole reply between them, in three runs. Only
  // the earlier copy is stored between the first reply and what followed it.
  it.effect("matches a stopped reply to the copy stored before what followed it", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const firstCopy = { ...assistantSnapshot(3000, "First full"), stopReason: "aborted" };
      const middle = assistantSnapshot(5000, "Middle reply");
      const lastCopy = { ...assistantSnapshot(8000, "Last full"), stopReason: "aborted" };
      yield* fake.emit({ type: "message_start", message: storedPrompt(900) });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "First"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "First" },
      });
      yield* fake.emit({ type: "message_end", message: firstCopy });
      yield* fake.emit({ type: "turn_end", message: firstCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [firstCopy] });
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: storedPrompt(4000, "Continue") });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(5000, "") });
      yield* fake.emit({
        type: "message_update",
        message: middle,
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Middle reply" },
      });
      yield* fake.emit({ type: "message_end", message: middle });
      yield* fake.emit({ type: "turn_end", message: middle, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [middle] });
      yield* fake.emit({ type: "agent_start" });
      fake.setTranscript([
        storedPrompt(900),
        firstCopy,
        storedPrompt(4000, "Continue"),
        middle,
        storedPrompt(6000, "Continue again"),
        lastCopy,
      ]);
      yield* fake.emit({ type: "message_end", message: lastCopy });
      yield* fake.emit({ type: "turn_end", message: lastCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [lastCopy] });

      assert.deepEqual(yield* takeCompletedReplyTexts(takeEvent), [
        ["First full"],
        ["Middle reply"],
        ["Last full"],
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // Prime Agent drops a failed reply from its history before it retries
  // (agent-session.js _retryAfterDelay), so no copy replaced it.
  it.effect("keeps a reply a retry removed apart from the retried reply's copy", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: storedPrompt(900) });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "Failed partial"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Failed partial" },
      });
      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 1,
        maxAttempts: 3,
        delayMs: 0,
        errorMessage: "Transient provider error",
      });
      const copy = { ...assistantSnapshot(3000, "Failed partial, retried"), stopReason: "aborted" };
      fake.setTranscript([storedPrompt(900), copy]);
      yield* fake.emit({ type: "message_end", message: copy });
      yield* fake.emit({ type: "turn_end", message: copy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [copy] });

      assert.deepEqual(yield* takeFinalReplyTexts(takeEvent), [
        "Failed partial",
        "Failed partial, retried",
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // The history places the copy, and its text confirms it: a stock copy holds
  // all of the reply it replaced.
  it.effect("keeps a stopped copy that does not hold the cut reply's text apart", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const turnId = yield* takeRunningTurnId(takeEvent);
      if (turnId === undefined) return;
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "First reply"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "First reply" },
      });
      fake.setTranscript([
        storedPrompt(900),
        { ...assistantSnapshot(5000, "Second reply"), stopReason: "aborted" },
      ]);
      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: turnId,
        requestRuntimeRestart: true,
      });
      yield* fake.closeStdout;

      assert.deepEqual(yield* takeFinalReplyTexts(takeEvent), ["First reply", "Second reply"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // The copy holds a block the stream never showed, and a later run's reply
  // streams before the turn settles. That block keeps the copy's place.
  it.effect("keeps a block only a stopped copy holds before a later run's reply", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "First cut"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "First cut" },
      });
      const copy = {
        ...assistantSnapshot(5000, ""),
        content: [
          { type: "text", text: "First cut, then whole" },
          { type: "text", text: "Extra block" },
        ],
        stopReason: "aborted",
      };
      yield* fake.emit({ type: "message_end", message: copy });
      yield* fake.emit({ type: "turn_end", message: copy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [copy] });
      yield* fake.emit({ type: "agent_start" });
      const later = assistantSnapshot(7000, "Later reply");
      fake.setTranscript([storedPrompt(900), copy, storedPrompt(6000, "Background notice"), later]);
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(7000, "") });
      yield* fake.emit({
        type: "message_update",
        message: later,
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Later reply" },
      });
      yield* fake.emit({ type: "message_end", message: later });
      yield* fake.emit({ type: "agent_end", messages: [later] });

      assert.deepEqual(yield* takeCompletedReplyTexts(takeEvent), [
        ["First cut, then whole"],
        ["Extra block"],
        ["Later reply"],
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "keeps a reply a retry removed apart from a copy when Stop could not read the history",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        const turnId = yield* takeRunningTurnId(takeEvent);
        if (turnId === undefined) return;
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
        yield* fake.emit({
          type: "message_update",
          message: assistantSnapshot(1000, "Failed partial"),
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Failed partial" },
        });
        yield* fake.emit({
          type: "auto_retry_start",
          attempt: 1,
          maxAttempts: 3,
          delayMs: 0,
          errorMessage: "Transient provider error",
        });
        const copy = {
          ...assistantSnapshot(3000, "Failed partial, retried"),
          stopReason: "aborted",
        };
        fake.setTranscript([storedPrompt(900), copy]);
        fake.queueMessages({});

        yield* runtime.interruptTurn({ providerThread, providerTurnId: turnId });
        yield* fake.emit({ type: "message_end", message: copy });
        yield* fake.emit({ type: "turn_end", message: copy, toolResults: [] });
        yield* fake.emit({ type: "agent_end", messages: [copy] });

        assert.deepEqual(yield* takeCompletedReplyTexts(takeEvent), [
          ["Failed partial"],
          ["Failed partial, retried"],
        ]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // A later run's prompt, start and updates were all lost, and its stopped
  // copy starts the way the earlier stopped reply did. The stored prompt
  // closes the earlier reply's run, so only the copy before it is that reply's.
  it.effect("keeps a later run's stopped copy off an earlier stopped reply", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const firstCopy = { ...assistantSnapshot(3000, "Check first"), stopReason: "aborted" };
      const laterCopy = { ...assistantSnapshot(8000, "Check later"), stopReason: "aborted" };
      yield* fake.emit({ type: "message_start", message: storedPrompt(900) });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "Check"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Check" },
      });
      yield* fake.emit({ type: "message_end", message: firstCopy });
      fake.setTranscript([
        storedPrompt(900),
        firstCopy,
        storedPrompt(6000, "Continue again"),
        laterCopy,
      ]);
      yield* fake.emit({ type: "message_end", message: laterCopy });
      yield* fake.emit({ type: "turn_end", message: laterCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [laterCopy] });

      assert.deepEqual(yield* takeCompletedReplyTexts(takeEvent), [
        ["Check first"],
        ["Check later"],
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // Two stopped replies start the same way, and the prompt between their runs
  // was lost. Each copy arrived right after the reply it ends, and a copy that
  // arrived before a reply started cannot be that reply's.
  it.effect("never swaps two stopped replies when the prompt between them was lost", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const firstCopy = { ...assistantSnapshot(3000, "Check first"), stopReason: "aborted" };
      const laterCopy = { ...assistantSnapshot(6000, "Check later"), stopReason: "aborted" };
      yield* fake.emit({ type: "message_start", message: storedPrompt(900) });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "Check"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Check" },
      });
      yield* fake.emit({ type: "message_end", message: firstCopy });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(5000, "Check"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Check" },
      });
      yield* fake.emit({ type: "message_end", message: laterCopy });
      fake.setTranscript([storedPrompt(900), firstCopy, storedPrompt(4000, "Continue"), laterCopy]);
      yield* fake.emit({ type: "turn_end", message: laterCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [laterCopy] });

      assert.deepEqual(yield* takeCompletedReplyTexts(takeEvent), [
        ["Check first"],
        ["Check later"],
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // An earlier reply's events were all lost, and so was the prompt after it.
  // Its copy starts the way the later stopped reply did. Two copies fit the
  // later reply and the earlier one is known only from the history, so the
  // reply keeps its cut text rather than risk another reply's.
  it.effect("keeps an earlier run's unseen copy off a later stopped reply", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const firstCopy = { ...assistantSnapshot(3000, "Check first"), stopReason: "aborted" };
      const laterCopy = { ...assistantSnapshot(6000, "Check later"), stopReason: "aborted" };
      yield* fake.emit({ type: "message_start", message: storedPrompt(900) });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(5000, "Check"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Check" },
      });
      yield* fake.emit({ type: "message_end", message: laterCopy });
      fake.setTranscript([storedPrompt(900), firstCopy, storedPrompt(4000, "Continue"), laterCopy]);
      yield* fake.emit({ type: "turn_end", message: laterCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [laterCopy] });

      assert.deepEqual(yield* takeCompletedReplyTexts(takeEvent), [
        ["Check"],
        ["Check later"],
        ["Check first"],
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // The mirror of the case above, the same to T3: the stopped reply's own copy
  // was lost live, and a later run's copy arrived right after it. Whichever
  // way the history is read, no reply shows another reply's text.
  it.effect("never puts a later run's copy on a stopped reply whose own copy was lost live", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const firstCopy = { ...assistantSnapshot(3000, "Check first"), stopReason: "aborted" };
      const laterCopy = { ...assistantSnapshot(6000, "Check later"), stopReason: "aborted" };
      yield* fake.emit({ type: "message_start", message: storedPrompt(900) });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "Check"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Check" },
      });
      yield* fake.emit({ type: "message_end", message: laterCopy });
      fake.setTranscript([storedPrompt(900), firstCopy, storedPrompt(4000, "Continue"), laterCopy]);
      yield* fake.emit({ type: "turn_end", message: laterCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [laterCopy] });

      const [cutReply, ...others] = yield* takeCompletedReplyTexts(takeEvent);
      assert.notInclude(cutReply ?? [], "Check later");
      assert.deepEqual(
        [cutReply ?? [], ...others]
          .flat()
          .filter((text) => text !== "Check")
          .toSorted(),
        ["Check first", "Check later"],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // The first reply's start and updates were lost, but its copy arrived live
  // and took its place, before an extension notice and the later reply. A
  // copy that arrived before a reply started cannot be that reply's.
  it.effect("keeps a copy that arrived before a later reply in its own place", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: storedPrompt(900) });
      const firstCopy = { ...assistantSnapshot(3000, "Check first"), stopReason: "aborted" };
      const laterCopy = { ...assistantSnapshot(6000, "Check later"), stopReason: "aborted" };
      yield* fake.emit({ type: "message_end", message: firstCopy });
      yield* fake.emit({
        type: "extension_ui_request",
        method: "notify",
        message: "Between runs",
        notifyType: "info",
      });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(5000, "Check"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Check" },
      });
      fake.setTranscript([storedPrompt(900), firstCopy, storedPrompt(4000, "Continue"), laterCopy]);
      yield* fake.emit({ type: "message_end", message: laterCopy });
      yield* fake.emit({ type: "turn_end", message: laterCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [laterCopy] });

      const timeline = new Map<string, { readonly ordinal: number; readonly text: string }>();
      for (;;) {
        const event = yield* takeEvent(
          (candidate) =>
            candidate.type === "turn.terminal" ||
            (candidate.type === "turn_item.updated" && candidate.turnItem.status === "completed"),
        );
        if (event.type !== "turn_item.updated") break;
        const item = event.turnItem;
        if (item.type === "assistant_message") {
          timeline.set(item.id, { ordinal: item.ordinal, text: item.text });
        } else if (item.type === "dynamic_tool" && item.toolName === "notify") {
          timeline.set(item.id, { ordinal: item.ordinal, text: "Between runs" });
        }
      }
      assert.deepEqual(
        [...timeline.values()]
          .sort((left, right) => left.ordinal - right.ordinal)
          .map((item) => item.text),
        ["Check first", "Between runs", "Check later"],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // One dropped range, from the first reply's start to the next prompt's end.
  // The later reply is stopped at once, so its copy is exactly what it showed.
  // The older copy also holds that text, and the RPC stream carries no event
  // sequence, so this looks the same as the mirror case (the reply's own copy
  // lost, a later run's copy arriving right after it). Both stay misses.
  it.effect("keeps a reply apart when its live copy and an older unseen copy both fit", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const olderCopy = { ...assistantSnapshot(3000, "Check first"), stopReason: "aborted" };
      const ownCopy = { ...assistantSnapshot(6000, "Check"), stopReason: "aborted" };
      yield* fake.emit({ type: "message_start", message: storedPrompt(900) });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(5000, "Check"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Check" },
      });
      yield* fake.emit({ type: "message_end", message: ownCopy });
      fake.setTranscript([storedPrompt(900), olderCopy, storedPrompt(4000, "Continue"), ownCopy]);
      yield* fake.emit({ type: "turn_end", message: ownCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [ownCopy] });

      assert.deepEqual(yield* takeCompletedReplyTexts(takeEvent), [
        ["Check"],
        ["Check"],
        ["Check first"],
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // One dropped range, from the first reply's failed end through a retry that
  // succeeded and a later run's prompt and updates. The retry dropped the
  // failed reply from the history, so no copy replaced it. Only the later
  // run's aborted end arrives, and it starts the way the failed reply did.
  it.effect("keeps a reply a lost retry replaced apart from a later run's copy", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "turn_start" });
      yield* fake.emit({ type: "message_start", message: storedPrompt(900) });
      yield* fake.emit({ type: "message_end", message: storedPrompt(900) });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "Check"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Check" },
      });
      const laterCopy = { ...assistantSnapshot(11000, "Check later"), stopReason: "aborted" };
      fake.setTranscript([
        storedPrompt(900),
        { ...assistantSnapshot(5000, "Check recovered"), stopReason: "stop" },
        storedPrompt(8900, "Continue"),
        laterCopy,
      ]);
      yield* fake.emit({ type: "message_end", message: laterCopy });
      yield* fake.emit({ type: "turn_end", message: laterCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [laterCopy] });

      assert.deepEqual(yield* takeCompletedReplyTexts(takeEvent), [
        ["Check"],
        ["Check later"],
        ["Check recovered"],
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // The wall clock moved back between the turns, so the first turn's copy is
  // stamped after the reply the second turn was streaming.
  it.effect(
    "keeps an earlier turn's aborted copy off a later cut reply when the clock moved back",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        const firstTurnId = yield* takeRunningTurnId(takeEvent);
        if (firstTurnId === undefined) return;
        const firstText = "I'll check the logs first.";
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
        yield* fake.emit({
          type: "message_update",
          message: assistantSnapshot(1000, firstText),
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: firstText },
        });
        fake.setTranscript([storedPrompt(900)]);
        yield* runtime.interruptTurn({ providerThread, providerTurnId: firstTurnId });
        yield* fake.emit({ type: "agent_end", messages: [] });
        yield* takeRepliesAndOutcome(takeEvent);

        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        const secondTurnId = yield* takeRunningTurnId(takeEvent);
        if (secondTurnId === undefined) return;
        const secondText = "I'll check the config.";
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "message_start", message: assistantSnapshot(2000, "") });
        yield* fake.emit({
          type: "message_update",
          message: assistantSnapshot(2000, "I'll check"),
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "I'll check" },
        });
        fake.setTranscript([
          storedPrompt(900),
          { ...assistantSnapshot(3000, firstText), stopReason: "aborted" },
          storedPrompt(1500, "And the config?"),
          { ...assistantSnapshot(5000, secondText), stopReason: "aborted" },
        ]);
        yield* runtime.interruptTurn({
          providerThread,
          providerTurnId: secondTurnId,
          requestRuntimeRestart: true,
        });
        yield* fake.closeStdout;

        assert.deepEqual(yield* takeFinalReplyTexts(takeEvent), [secondText, firstText]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // Stock stamps the copy with Date.now() at abort, which a clock moved back
  // makes earlier than the reply it replaces.
  it.effect("shows a reply Stop cut off once when the clock moved back before the abort", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const turnId = yield* takeRunningTurnId(takeEvent);
      if (turnId === undefined) return;
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, cutReply),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: cutReply },
      });
      const abortedCopy = { ...assistantSnapshot(950, cutReply), stopReason: "aborted" };
      fake.setTranscript([storedPrompt(900), abortedCopy]);
      const readsBeforeStop = historyReads(fake);

      yield* runtime.interruptTurn({ providerThread, providerTurnId: turnId });
      yield* fake.emit({ type: "message_end", message: abortedCopy });
      yield* fake.emit({ type: "turn_end", message: abortedCopy, toolResults: [] });
      yield* fake.emit({ type: "agent_end", messages: [abortedCopy] });

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [cutReply],
        status: "interrupted",
      });
      assert.equal(historyReads(fake) - readsBeforeStop, 1, "only Stop reads the history");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("completes a cut-off reply when Stop aborts the run", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      assert.isTrue(running.type === "provider_turn.updated");
      if (running.type !== "provider_turn.updated") return;
      yield* streamCutReply(fake);
      fake.setTranscript(recordedTurn);

      yield* runtime.interruptTurn({ providerThread, providerTurnId: running.providerTurn.id });

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [fullReply],
        status: "interrupted",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("completes a cut-off reply when Stop restarts Prime Agent", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const running = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      assert.isTrue(running.type === "provider_turn.updated");
      if (running.type !== "provider_turn.updated") return;
      yield* streamCutReply(fake);
      fake.setTranscript(recordedTurn);

      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: running.providerTurn.id,
        requestRuntimeRestart: true,
      });
      yield* fake.closeStdout;

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [fullReply],
        status: "interrupted",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("adds only the messages this turn stored, not earlier history", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const earlier = [storedPrompt(400), storedReply(500, "An earlier turn's reply.")];
      fake.setTranscript(earlier);
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* streamCutReply(fake);
      fake.setTranscript([...earlier, ...recordedTurn]);

      yield* takeQuietProbe(fake);

      assert.deepEqual(yield* takeCompletedReplies(takeEvent), [fullReply]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("adds only this turn's messages when the first history read failed", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const earlier = [storedPrompt(400), storedReply(500, "An earlier turn's reply.")];
      fake.setTranscript(earlier);
      fake.queueMessages({});
      yield* TestClock.setTime(600);
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      fake.setTranscript([...earlier, ...recordedTurn]);
      yield* fake.emit({ type: "agent_end", messages: [] });

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [fullReply],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("adds only this turn's messages on a thread that launched on its saved session", () =>
    Effect.gen(function* () {
      const { providerThread } = yield* openPrimeThread(yield* makeFakePi);
      const fake = yield* makeFakePi;
      const earlier = [storedPrompt(400), storedReply(500, "An earlier turn's reply.")];
      fake.setTranscript(earlier);
      const { runtime, takeEvent } = yield* openRuntime(
        fake,
        "default",
        THREAD_ID,
        SESSION_ID,
        undefined,
        PRIME_AGENT_FLAVOR,
        undefined,
        {},
        FAKE_SESSION_FILE,
      );
      const resumed = yield* runtime.resumeThread({ providerThread });
      yield* startTurn(runtime, resumed);
      yield* fake.takeRequest("prompt");
      yield* streamCutReply(fake);
      fake.setTranscript([...earlier, ...recordedTurn]);

      yield* takeQuietProbe(fake);

      assert.deepEqual(yield* takeCompletedReplies(takeEvent), [fullReply]);
      assert.isFalse(
        fake.allRequests().some((request) => request["type"] === "switch_session"),
        "the thread stayed on the session the process launched on",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a lost reply when the thread loads again on the same session", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      const lostReply = storedReply(300, "A reply whose events were all dropped.");
      fake.setTranscript([lostReply]);
      // A new model selection loads the thread again, on the session it is already on.
      const reloaded = yield* runtime.resumeThread({
        providerThread,
        modelSelection: modelSelection("other"),
      });
      yield* startTurn(runtime, reloaded);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      fake.setTranscript([lostReply, ...recordedTurn]);
      yield* fake.emit({ type: "agent_end", messages: [] });

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: ["A reply whose events were all dropped.", fullReply],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // Thread b0d440d5 run 68: on a 68-run session the get_messages reply was
  // over the old 8 MiB record cap, so the read failed and the reply stayed cut.
  it.effect("completes a cut-off reply on a session whose history is over 8 MiB", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const longHistory = {
        role: "toolResult",
        toolCallId: "earlier-call",
        content: [{ type: "text", text: "x".repeat(9 * 1024 * 1024) }],
        timestamp: 500,
      };
      fake.setTranscript([longHistory]);
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* streamCutReply(fake);
      fake.setTranscript([longHistory, ...recordedTurn]);

      yield* takeQuietProbe(fake);

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [fullReply],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /** Runs turn `runOrdinal`, whose stored reply is `reply` and whose events were all dropped. */
  const runTurnWithDroppedReply = (
    fake: FakePi,
    session: Effect.Success<ReturnType<typeof openPrimeThread>>,
    runOrdinal: number,
    before: ReadonlyArray<unknown>,
    reply: { readonly timestamp: number; readonly text: string },
  ) =>
    Effect.gen(function* () {
      yield* startTurn(
        session.runtime,
        session.providerThread,
        "default",
        [],
        `turn ${runOrdinal}`,
        undefined,
        runOrdinal,
      );
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const transcript = [
        ...before,
        storedPrompt(reply.timestamp - 1, `turn ${runOrdinal}`),
        storedReply(reply.timestamp, reply.text),
      ];
      fake.setTranscript(transcript);
      yield* fake.emit({ type: "agent_end", messages: [] });
      return { transcript, outcome: yield* takeRepliesAndOutcome(session.takeEvent) };
    });

  it.effect("shows each recovered reply once, in the turn that stored it", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      const first = yield* runTurnWithDroppedReply(fake, session, 1, [], {
        timestamp: 1000,
        text: "First reply.",
      });
      const second = yield* runTurnWithDroppedReply(fake, session, 2, first.transcript, {
        timestamp: 2000,
        text: "Second reply.",
      });

      assert.deepEqual(first.outcome, { texts: ["First reply."], status: "completed" });
      assert.deepEqual(second.outcome, { texts: ["Second reply."], status: "completed" });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("ignores a recovered reply's events that arrive after its turn settled", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      const first = yield* runTurnWithDroppedReply(fake, session, 1, [], {
        timestamp: 1000,
        text: "First reply.",
      });
      assert.deepEqual(first.outcome.texts, ["First reply."]);

      yield* startTurn(
        session.runtime,
        session.providerThread,
        "default",
        [],
        "turn 2",
        undefined,
        2,
      );
      yield* fake.takeRequest("prompt");
      // The first reply's delayed events finally come through.
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(1000, "First reply."),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "First reply." },
      });
      yield* fake.emit({ type: "message_end", message: storedReply(1000, "First reply.") });
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(2000, "") });
      yield* fake.emit({ type: "message_end", message: storedReply(2000, "Second reply.") });
      fake.setTranscript([
        ...first.transcript,
        storedPrompt(1999, "turn 2"),
        storedReply(2000, "Second reply."),
      ]);
      yield* fake.emit({ type: "agent_end", messages: [] });

      assert.deepEqual(yield* takeRepliesAndOutcome(session.takeEvent), {
        texts: ["Second reply."],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /** Steps virtual time in settle-probe steps until the turn ends, for probes that ask again. */
  const awaitRepliesAndOutcome = (takeEvent: TakeEvent) =>
    Effect.gen(function* () {
      const outcome = yield* takeRepliesAndOutcome(takeEvent).pipe(Effect.forkScoped);
      for (let step = 0; step < 50 && outcome.pollUnsafe() === undefined; step += 1) {
        yield* TestClock.adjust(Duration.millis(100));
        yield* Effect.yieldNow;
      }
      return yield* Fiber.join(outcome);
    });

  it.effect("keeps a cut-off reply for a later read when its turn settled without one", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      yield* startTurn(session.runtime, session.providerThread);
      yield* fake.takeRequest("prompt");
      yield* streamCutReply(fake);
      fake.setTranscript(recordedTurn);
      for (let read = 0; read < 3; read += 1) fake.queueMessages({});

      yield* takeQuietProbe(fake);
      assert.deepEqual(yield* awaitRepliesAndOutcome(session.takeEvent), {
        texts: [cutReply],
        status: "completed",
      });

      const next = yield* runTurnWithDroppedReply(fake, session, 2, recordedTurn, {
        timestamp: 2000,
        text: "Second reply.",
      });
      assert.deepEqual(next.outcome, {
        texts: [fullReply, "Second reply."],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // get_state and get_messages are separate reads, so the history can be from
  // another moment than the idle state that settles the turn.
  it.effect("reads the history again when it does not match the idle state", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      fake.setTranscript(recordedTurn);
      fake.queueMessages({ messages: [storedPrompt(900)] });
      yield* fake.emit({ type: "agent_end", messages: [] });

      // The settle probe asks again shortly and this time reads it all.
      assert.deepEqual(yield* awaitRepliesAndOutcome(takeEvent), {
        texts: [fullReply],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps the turn open when the agent goes busy while its history is read", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      // The settle probe sees idle and the history matches it, but by the
      // state read after it a steer started more work, whose events are lost.
      fake.queueState({ messageCount: 2 });
      fake.queueMessages({ messages: recordedTurn });
      fake.queueState({ ...busyState, messageCount: 3 });
      fake.setTranscript([
        ...recordedTurn,
        storedPrompt(1100, "And then?"),
        storedReply(1200, "Then the follow-up."),
      ]);
      yield* fake.emit({ type: "agent_end", messages: [] });

      assert.deepEqual(yield* awaitRepliesAndOutcome(takeEvent), {
        texts: [fullReply, "Then the follow-up."],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  const storedLookup = (timestamp: number) => [
    {
      role: "assistant",
      content: [{ type: "toolCall", id: "call-1", name: "lookup", arguments: { key: "a" } }],
      stopReason: "toolUse",
      timestamp,
    },
    {
      role: "toolResult",
      toolCallId: "call-1",
      toolName: "lookup",
      content: [{ type: "text", text: "found it" }],
      isError: false,
      timestamp: timestamp + 1,
    },
  ];

  const takeCompletedLookup = (takeEvent: TakeEvent) =>
    takeEvent(
      (event) =>
        event.type === "turn_item.updated" &&
        event.turnItem.type === "dynamic_tool" &&
        event.turnItem.status === "completed",
    ).pipe(
      Effect.map((event) =>
        event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool"
          ? { input: event.turnItem.input, output: event.turnItem.output }
          : undefined,
      ),
    );

  it.effect("completes a tool whose end was dropped with its recorded result", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId: "call-1",
        toolName: "lookup",
        args: { key: "a" },
      });
      fake.setTranscript([storedPrompt(900), ...storedLookup(1000)]);

      yield* takeQuietProbe(fake);

      assert.deepEqual(yield* takeCompletedLookup(takeEvent), {
        input: { key: "a" },
        output: "found it",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("recovers a tool call whose every event was dropped", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      fake.setTranscript([storedPrompt(900), ...storedLookup(1000)]);
      yield* fake.emit({ type: "agent_end", messages: [] });

      assert.deepEqual(yield* takeCompletedLookup(takeEvent), {
        input: { key: "a" },
        output: "found it",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a reply whose message_start was dropped apart from the previous reply", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: assistantSnapshot(1000, "") });
      yield* fake.emit({ type: "message_end", message: assistantSnapshot(1000, "First reply.") });
      // The second message's start is lost.
      yield* fake.emit({
        type: "message_update",
        message: assistantSnapshot(2000, "Second"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Second" },
      });
      yield* fake.emit({ type: "message_end", message: assistantSnapshot(2000, "Second reply.") });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");

      assert.deepEqual(yield* takeCompletedReplies(takeEvent), ["First reply.", "Second reply."]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "keeps replies apart when the first message_start is dropped but a later one arrives",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "message_end", message: assistantSnapshot(1000, "First reply.") });
        yield* fake.emit({ type: "message_start", message: assistantSnapshot(2000, "") });
        yield* fake.emit({
          type: "message_update",
          message: assistantSnapshot(2000, "Second"),
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Second" },
        });
        yield* fake.emit({
          type: "message_end",
          message: assistantSnapshot(2000, "Second reply."),
        });
        yield* fake.emit({ type: "agent_end", messages: [] });
        yield* fake.takeRequest("get_state");

        assert.deepEqual(yield* takeCompletedReplies(takeEvent), ["First reply.", "Second reply."]);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  const finalReply = {
    type: "turn_end",
    message: { role: "assistant", content: [{ type: "text", text: "done" }], stopReason: "stop" },
  } as const;

  const systemNotices = (events: ReadonlyArray<ProviderAdapterV2Event>) =>
    events.flatMap((event) =>
      event.type === "turn_item.updated" && event.turnItem.type === "system_notice"
        ? [
            {
              message: event.turnItem.message,
              tone: event.turnItem.tone,
              status: event.turnItem.status,
            },
          ]
        : [],
    );

  it.effect("shows a Finishing up row while Prime Agent is busy after the final reply", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(finalReply);
      const runningRow = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "system_notice" &&
          event.turnItem.status === "running",
      ).pipe(Effect.forkScoped);
      // The row appears after a delay on the test clock.
      for (let step = 0; step < 40 && runningRow.pollUnsafe() === undefined; step += 1) {
        yield* TestClock.adjust(Duration.millis(100));
        yield* Effect.yieldNow;
      }
      const running = yield* Fiber.join(runningRow);
      assert.isTrue(
        running.type === "turn_item.updated" &&
          running.turnItem.type === "system_notice" &&
          running.turnItem.message === "Finishing up…" &&
          running.turnItem.tone === "progress",
      );

      yield* fake.emit({ type: "agent_end", messages: [] });
      const seen: Array<ProviderAdapterV2Event> = [];
      yield* takeEvent((event) => {
        seen.push(event);
        return event.type === "turn.terminal";
      });
      assert.deepStrictEqual(systemNotices(seen), [
        { message: "Finished up", tone: "progress", status: "completed" },
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows no Finishing up row when the run ends right after the final reply", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(finalReply);
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      const seen: Array<ProviderAdapterV2Event> = [];
      yield* takeEvent((event) => {
        seen.push(event);
        return event.type === "turn.terminal";
      });
      assert.deepStrictEqual(systemNotices(seen), []);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /**
   * Shows the Finishing up row for the turn that just started. The refine
   * notice is a barrier: events are handled in order, so once it arrives the
   * final reply has armed the delay and the test clock can run it out.
   */
  const showFinishingUp = Effect.fnUntraced(function* (
    fake: FakePi,
    takeEvent: (
      predicate: (event: ProviderAdapterV2Event) => boolean,
    ) => Effect.Effect<ProviderAdapterV2Event>,
  ) {
    yield* fake.emit({ type: "agent_start" });
    yield* fake.emit(finalReply);
    yield* fake.emit({ type: "refine_complete", result: { id: "barrier", summary: "barrier" } });
    yield* takeEvent(isSystemNotice);
    yield* TestClock.adjust(Duration.millis(1_500));
    const running = yield* takeEvent(isSystemNotice);
    assert.isTrue(running.type === "turn_item.updated" && running.turnItem.status === "running");
    return running;
  });

  const isSystemNotice = (event: ProviderAdapterV2Event) =>
    event.type === "turn_item.updated" && event.turnItem.type === "system_notice";

  const noticeIds = (event: ProviderAdapterV2Event) =>
    event.type === "turn_item.updated"
      ? { id: event.turnItem.id, nodeId: event.turnItem.nodeId }
      : undefined;

  it.effect("gives threads at the same turn ordinal their own notice ids", () =>
    Effect.gen(function* () {
      const threadA = ThreadId.make("thread:notice-a");
      const threadB = ThreadId.make("thread:notice-b");
      const fakeA = yield* makeFakePi;
      const fakeB = yield* makeFakePi;
      const a = yield* openPrimeThread(fakeA, threadA);
      const b = yield* openPrimeThread(fakeB, threadB);
      yield* startTurn(a.runtime, a.providerThread, "default", [], "Hello", undefined, 1, threadA);
      yield* startTurn(b.runtime, b.providerThread, "default", [], "Hello", undefined, 1, threadB);
      yield* fakeA.takeRequest("prompt");
      yield* fakeB.takeRequest("prompt");

      const runningA = yield* showFinishingUp(fakeA, a.takeEvent);
      const runningB = yield* showFinishingUp(fakeB, b.takeEvent);
      yield* fakeA.emit({ type: "agent_end", messages: [] });
      yield* fakeB.emit({ type: "agent_end", messages: [] });
      const doneA = yield* a.takeEvent(isSystemNotice);
      const doneB = yield* b.takeEvent(isSystemNotice);

      assert.deepStrictEqual(noticeIds(doneA), noticeIds(runningA));
      assert.deepStrictEqual(noticeIds(doneB), noticeIds(runningB));
      assert.notStrictEqual(noticeIds(runningA)?.id, noticeIds(runningB)?.id);
      assert.notStrictEqual(noticeIds(runningA)?.nodeId, noticeIds(runningB)?.nodeId);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("ignores refine events on plain Pi", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "refine_complete", result: { id: "r1", summary: "x" } });
      yield* fake.emit({ type: "refine_failed", error: "boom" });
      yield* fake.emit({ type: "agent_settled" });
      const seen: Array<ProviderAdapterV2Event> = [];
      yield* takeEvent((event) => {
        seen.push(event);
        return event.type === "turn.terminal";
      });
      assert.deepStrictEqual(systemNotices(seen), []);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("emits no Finishing up row after a quick end, even once the delay has passed", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(finalReply);
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      yield* takeEvent((event) => event.type === "turn.terminal");

      // A stale timer would wake here. The next turn's start events are
      // handled after it, so a notice from the first turn would show up first.
      yield* TestClock.adjust(Duration.millis(5_000));
      yield* startTurn(runtime, providerThread, "default", [], "Again", undefined, 2);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const seen: Array<ProviderAdapterV2Event> = [];
      yield* takeEvent((event) => {
        seen.push(event);
        return event.type === "provider_turn.updated" && event.providerTurn.status === "running";
      });
      assert.deepStrictEqual(systemNotices(seen), []);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows no Finishing up row for a plain Pi final reply", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(finalReply);
      yield* TestClock.adjust(Duration.millis(5_000));
      yield* fake.emit({ type: "agent_settled" });
      const seen: Array<ProviderAdapterV2Event> = [];
      yield* takeEvent((event) => {
        seen.push(event);
        return event.type === "turn.terminal";
      });
      assert.deepStrictEqual(systemNotices(seen), []);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  type PrimeSession = Effect.Success<ReturnType<typeof openPrimeThread>>;
  const endsTurn = (fake: FakePi) =>
    Effect.gen(function* () {
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
    });

  const endings: ReadonlyArray<{
    readonly name: string;
    readonly end: (
      fake: FakePi,
      session: PrimeSession,
      providerTurnId: string,
    ) => Effect.Effect<void>;
  }> = [
    {
      name: "turn_start",
      end: (fake) => Effect.andThen(fake.emit({ type: "turn_start" }), endsTurn(fake)),
    },
    {
      name: "agent_start",
      end: (fake) => Effect.andThen(fake.emit({ type: "agent_start" }), endsTurn(fake)),
    },
    {
      name: "assistant message_start",
      end: (fake) =>
        Effect.andThen(
          fake.emit({ type: "message_start", message: { role: "assistant", content: [] } }),
          endsTurn(fake),
        ),
    },
    {
      name: "tool start",
      end: (fake) =>
        Effect.gen(function* () {
          yield* fake.emit({
            type: "tool_execution_start",
            toolCallId: "late_tool",
            toolName: "read",
            args: { path: "/tmp/a" },
          });
          yield* endsTurn(fake);
        }),
    },
    {
      name: "provider failure",
      end: (fake) =>
        Effect.gen(function* () {
          yield* fake.emit({
            type: "message_end",
            message: { role: "assistant", content: [], stopReason: "error", errorMessage: "boom" },
          });
          yield* endsTurn(fake);
        }),
    },
    {
      name: "interrupt",
      end: (fake, session, providerTurnId) =>
        Effect.gen(function* () {
          yield* session.runtime
            .interruptTurn({
              providerThread: session.providerThread,
              providerTurnId: providerTurnId as never,
            })
            .pipe(Effect.orDie);
          yield* fake.takeRequest("abort");
          yield* fake.closeStdout;
        }),
    },
    { name: "stdout closure", end: (fake) => fake.closeStdout },
  ];

  it.effect.each(endings)("completes a shown Finishing up row once on $name", ({ end }) =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      yield* startTurn(session.runtime, session.providerThread);
      yield* fake.takeRequest("prompt");
      const running = yield* showFinishingUp(fake, session.takeEvent);
      assert.isTrue(running.type === "turn_item.updated");
      if (running.type !== "turn_item.updated") return;

      yield* end(fake, session, running.turnItem.providerTurnId ?? "");
      const seen: Array<ProviderAdapterV2Event> = [];
      yield* session.takeEvent((event) => {
        seen.push(event);
        return event.type === "turn.terminal";
      });
      // Time passing after the end must not bring the row back.
      yield* TestClock.adjust(Duration.millis(5_000));

      const notices = seen.filter(isSystemNotice);
      assert.lengthOf(notices, 1);
      const [completed] = notices;
      assert.isTrue(
        completed?.type === "turn_item.updated" &&
          completed.turnItem.type === "system_notice" &&
          completed.turnItem.status === "completed" &&
          completed.turnItem.message === "Finished up" &&
          completed.turnItem.tone === "progress" &&
          completed.turnItem.id === running.turnItem.id &&
          completed.turnItem.ordinal === running.turnItem.ordinal,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("reports a finished refinement and a failed one as notices", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "refine_complete",
        result: { id: "refine_1", summary: "Create a memory for the fork fix.", appliedEdits: [] },
      });
      yield* fake.emit({ type: "refine_failed", error: "planner timed out" });
      yield* fake.emit({ type: "agent_end", messages: [] });
      const seen: Array<ProviderAdapterV2Event> = [];
      yield* takeEvent((event) => {
        seen.push(event);
        return event.type === "turn.terminal";
      });
      assert.deepStrictEqual(systemNotices(seen), [
        {
          message: "Refined its harness: Create a memory for the fork fix.",
          tone: "info",
          status: "completed",
        },
        {
          message: "Harness refinement failed: planner timed out",
          tone: "warning",
          status: "completed",
        },
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("rebuilds streamed text from message snapshots when Prime Agent drops events", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: { role: "assistant", content: [] } });

      // Each update carries the message so far, but the deltas between them,
      // both block ends, and the whole third block never reach the adapter.
      yield* fake.emit({
        type: "message_update",
        message: { role: "assistant", content: [{ type: "thinking", thinking: "Plan: check" }] },
        assistantMessageEvent: { type: "thinking_delta", contentIndex: 0, delta: "check" },
      });
      yield* fake.emit({
        type: "message_update",
        message: {
          role: "assistant",
          content: [
            { type: "thinking", thinking: "Plan: check, then reply" },
            { type: "text", text: "Hello, wor" },
          ],
        },
        assistantMessageEvent: { type: "text_delta", contentIndex: 1, delta: "wor" },
      });
      yield* fake.emit({
        type: "message_end",
        message: {
          role: "assistant",
          content: [
            { type: "thinking", thinking: "Plan: check, then reply" },
            { type: "text", text: "Hello, world. All done." },
            { type: "text", text: "Second block." },
          ],
          stopReason: "stop",
        },
      });

      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");

      const completedTexts: Array<string> = [];
      for (;;) {
        const event = yield* takeEvent(
          (candidate) =>
            candidate.type === "turn.terminal" ||
            (candidate.type === "turn_item.updated" &&
              (candidate.turnItem.type === "assistant_message" ||
                candidate.turnItem.type === "reasoning") &&
              candidate.turnItem.status === "completed"),
        );
        if (event.type === "turn.terminal") break;
        if (event.type === "turn_item.updated" && "text" in event.turnItem) {
          completedTexts.push(event.turnItem.text);
        }
      }
      assert.sameMembers(completedTexts, [
        "Plan: check, then reply",
        "Hello, world. All done.",
        "Second block.",
      ]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows ipython cells as bash commands, python tools, and file changes", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });

      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId: "cell_bash",
        toolName: "ipython",
        args: { code: "r = await bash('pnpm test'); print(r.output)" },
      });
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "cell_bash",
        toolName: "ipython",
        result: { content: [{ type: "text", text: "ok\n" }], details: { status: "ok" } },
        isError: false,
      });
      const command = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "command_execution" &&
          event.turnItem.status === "completed",
      );
      assert.isTrue(
        command.type === "turn_item.updated" &&
          command.turnItem.type === "command_execution" &&
          command.turnItem.input === "pnpm test" &&
          command.turnItem.output === "ok\n",
      );

      // A failed cell reports its error on the result while the event flag stays false.
      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId: "cell_edit",
        toolName: "ipython",
        args: { code: "edit('src/a.ts', 'old', 'new')" },
      });
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "cell_edit",
        toolName: "ipython",
        result: {
          content: [{ type: "text", text: "Edited src/a.ts" }],
          isError: true,
          details: {
            status: "error",
            // Prime Agent reports absolute paths; the timeline shows them workspace-relative.
            diffs: [
              { path: `${process.cwd()}/src/a.ts`, oldStr: "old", newStr: "new", startLine: 3 },
            ],
          },
        },
        isError: false,
      });
      const python = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "dynamic_tool" &&
          event.turnItem.status !== "running",
      );
      assert.isTrue(
        python.type === "turn_item.updated" &&
          python.turnItem.type === "dynamic_tool" &&
          python.turnItem.toolName === "python" &&
          python.turnItem.title === "edit('src/a.ts', 'old', 'new')" &&
          python.turnItem.status === "failed",
      );
      const fileChange = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "file_change",
      );
      assert.isTrue(
        fileChange.type === "turn_item.updated" &&
          fileChange.turnItem.type === "file_change" &&
          fileChange.turnItem.fileName === "src/a.ts" &&
          fileChange.turnItem.oldStr === "old" &&
          fileChange.turnItem.newStr === "new",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  const rlmChild = (status: string, extra: Record<string, unknown> = {}) => ({
    type: "rlm_child_update",
    child: {
      id: "sub-1",
      sessionName: "worker",
      model: "cpa-claude/claude-opus-5-5",
      label: "Reply to your parent with child-ok",
      status,
      sessionDir: "/fake/.prime/agent/session-artifacts/s/sub-1",
      ...extra,
    },
  });

  /** Starts a turn whose parent spawns a child and then ends its own run. */
  const settleWithRunningChild = (
    fake: FakePi,
    session: Effect.Success<ReturnType<typeof openPrimeThread>>,
  ) =>
    Effect.gen(function* () {
      yield* startTurn(session.runtime, session.providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(rlmChild("queued"));
      yield* fake.emit(
        rlmChild("running", { activity: { kind: "executing", toolName: "ipython" } }),
      );
      const running = yield* session.takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.status === "running",
      );
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      const terminal = yield* session.takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      return running.type === "subagent.updated" ? running.subagent : undefined;
    });

  it.effect("settles the parent's turn and keeps a running subagent as background work", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      const running = yield* settleWithRunningChild(fake, session);
      assert.isTrue(
        running?.title === "worker" &&
          running.prompt === "Reply to your parent with child-ok" &&
          running.progress === "executing ipython",
      );
      // The settled turn still has work in flight, which keeps T3 listening
      // and lets the composer offer to stop it.
      assert.isTrue(yield* session.runtime.hasPendingBackgroundWork!);
      assert.isTrue(
        yield* session.runtime.hasPendingBackgroundWorkForThread!(session.providerThread),
      );

      yield* fake.emit(rlmChild("done", { answerPreview: "I sent child-ok to the parent." }));
      const done = yield* session.takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.status === "completed",
      );
      assert.isTrue(
        done.type === "subagent.updated" &&
          done.subagent.id === running?.id &&
          done.subagent.runId === running.runId &&
          done.subagent.result === "I sent child-ok to the parent.",
      );
      assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);

      // A later turn deletes the finished child; its card from turn 1 stays completed.
      yield* startTurn(
        session.runtime,
        session.providerThread,
        "default",
        [],
        "clean up",
        undefined,
        2,
      );
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(rlmChild("cancelled", { error: "Deleted by parent orchestrator" }));
      yield* fake.emit({ type: "agent_end", messages: [] });
      const next = yield* session.takeEvent(
        (event) => event.type === "turn.terminal" || event.type === "subagent.updated",
      );
      assert.equal(next.type, "turn.terminal");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "streams a child's session into its thread with no setup step stock Prime Agent can refuse",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const session = yield* openPrimeThread(fake);
        fake.queueObserved([
          { role: "user", content: "Reply to your parent with child-ok", timestamp: 1000 },
          { role: "assistant", content: [{ type: "text", text: "On it." }], timestamp: 1001 },
        ]);
        yield* startTurn(session.runtime, session.providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit(rlmChild("running", { activeSessionId: "active-1" }));
        const running = yield* session.takeEvent(
          (event) => event.type === "subagent.updated" && event.subagent.childThreadId !== null,
        );
        const childThreadId =
          running.type === "subagent.updated" ? running.subagent.childThreadId : null;
        const reply = yield* session.takeEvent(
          (event) =>
            event.type === "message.updated" &&
            event.message.threadId === childThreadId &&
            event.message.text === "On it.",
        );
        assert.equal(reply.type, "message.updated");
        // A daemon-side attach from a second client is refused for every child of a
        // T3-owned session ("Unknown active session"), so nothing may gate `observe`.
        assert.deepEqual(
          fake
            .allRequests()
            .filter((request) => request["type"] === "prompt" || request["type"] === "observe")
            .map((request) => request["type"]),
          ["prompt", "observe"],
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("gives a subagent its parent execution node before its child thread", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      yield* startTurn(session.runtime, session.providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(rlmChild("running"));
      const events: Array<ProviderAdapterV2Event> = [];
      while (events.at(-1)?.type !== "subagent.updated") {
        events.push(yield* session.takeEvent(() => true));
      }
      const subagent = events.at(-1);
      assert.isTrue(subagent?.type === "subagent.updated");
      if (subagent?.type !== "subagent.updated") return;
      const nodeIndex = events.findIndex(
        (event) => event.type === "node.updated" && event.node.id === subagent.subagent.id,
      );
      const shellIndex = events.findIndex((event) => event.type === "app_thread.created");
      assert.isAtLeast(nodeIndex, 0);
      assert.isBelow(nodeIndex, shellIndex);
      const node = events[nodeIndex];
      assert.isTrue(
        node?.type === "node.updated" &&
          node.node.kind === "subagent" &&
          node.node.status === "running" &&
          node.node.threadId === subagent.subagent.threadId &&
          node.node.runId === subagent.subagent.runId &&
          node.node.parentNodeId === subagent.subagent.parentNodeId &&
          node.node.completedAt === null,
      );

      yield* fake.emit(rlmChild("done", { answerPreview: "child-ok" }));
      const done = yield* session.takeEvent(
        (event) =>
          event.type === "node.updated" &&
          event.node.id === subagent.subagent.id &&
          event.node.status === "completed",
      );
      assert.isTrue(done.type === "node.updated" && done.node.completedAt !== null);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("streams a running child's session into its own thread", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      fake.queueObserved([
        { role: "user", content: "Reply to your parent with child-ok", timestamp: 1000 },
        { role: "assistant", content: [{ type: "text", text: "On it." }], timestamp: 1001 },
      ]);
      yield* startTurn(session.runtime, session.providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(rlmChild("queued"));
      yield* fake.emit(rlmChild("running", { activeSessionId: "active-1" }));
      const running = yield* session.takeEvent(
        (event) =>
          event.type === "subagent.updated" &&
          event.subagent.status === "running" &&
          event.subagent.childThreadId !== null,
      );
      const childThreadId =
        running.type === "subagent.updated" ? running.subagent.childThreadId : null;
      assert.isNotNull(childThreadId);
      assert.equal((yield* fake.takeRequest("observe"))["activeSessionId"], "active-1");

      // The history lands in the child thread, and live events follow it.
      yield* fake.emit({
        type: "observed_session_event",
        activeSessionId: "active-1",
        event: {
          type: "message_end",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "child-ok sent." }],
            timestamp: 1002,
          },
        },
      });
      const reply = yield* session.takeEvent(
        (event) =>
          event.type === "message.updated" &&
          event.message.threadId === childThreadId &&
          event.message.role === "assistant" &&
          event.message.text === "child-ok sent.",
      );
      assert.equal(reply.type, "message.updated");

      yield* fake.emit(rlmChild("done", { activeSessionId: "active-1" }));
      assert.equal((yield* fake.takeRequest("unobserve"))["activeSessionId"], "active-1");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("puts the child's whole final reply on its card, not Prime Agent's preview", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      const finalReply = `child-ok sent.${" The full report goes on past the preview.".repeat(8)}`;
      const preview = finalReply.slice(0, 159);
      yield* startTurn(session.runtime, session.providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(rlmChild("running", { activeSessionId: "active-1" }));
      yield* fake.takeRequest("observe");
      yield* fake.emit({
        type: "observed_session_event",
        activeSessionId: "active-1",
        event: { type: "message_end", message: assistantSnapshot(1001, "Working on it.") },
      });
      fake.queueObserved([
        assistantSnapshot(1001, "Working on it."),
        {
          ...assistantSnapshot(1002, finalReply),
          content: [
            { type: "thinking", thinking: "Done." },
            { type: "text", text: finalReply },
          ],
        },
        // A later reply with no text leaves the answer as it was.
        { ...assistantSnapshot(1003, ""), content: [{ type: "toolCall", id: "t", name: "x" }] },
      ]);
      yield* fake.emit(rlmChild("done", { activeSessionId: "active-1", answerPreview: preview }));

      const done = yield* session.takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.status === "completed",
      );
      assert.equal(done.type === "subagent.updated" ? done.subagent.result : null, finalReply);
      const card = yield* session.takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "subagent" &&
          event.turnItem.status === "completed",
      );
      assert.equal(
        card.type === "turn_item.updated" && card.turnItem.type === "subagent"
          ? card.turnItem.result
          : null,
        finalReply,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /** Starts a child with an observed session, and leaves it with a running tool and an open reply. */
  const startChildWithOpenWork = (
    fake: FakePi,
    session: Effect.Success<ReturnType<typeof openPrimeThread>>,
  ) =>
    Effect.gen(function* () {
      yield* startTurn(session.runtime, session.providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(rlmChild("queued"));
      yield* fake.emit(rlmChild("running", { activeSessionId: "active-1" }));
      const running = yield* session.takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.childThreadId !== null,
      );
      yield* fake.takeRequest("observe");
      yield* fake.emit(sentinelChild("running"));
      yield* session.takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.title === "sentinel",
      );
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      yield* session.takeEvent((event) => event.type === "turn.terminal");

      const observed = (event: Record<string, unknown>) =>
        fake.emit({ type: "observed_session_event", activeSessionId: "active-1", event });
      yield* observed({
        type: "tool_execution_start",
        toolCallId: "child-tool",
        toolName: "bash",
        args: { command: "sleep 30" },
      });
      yield* observed({
        type: "message_update",
        message: assistantSnapshot(1001, "Half an ans"),
        assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Half an ans" },
      });
      // Once the sentinel answers, the pump has handled both events above.
      yield* fake.emit(sentinelChild("running"));
      yield* session.takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.title === "sentinel",
      );
      return running.type === "subagent.updated" ? running.subagent.childThreadId : null;
    });

  /** A second child that is updated last, so its update marks the end of the child's events. */
  const sentinelChild = (status: string) => ({
    type: "rlm_child_update",
    child: {
      id: "sub-sentinel",
      sessionName: "sentinel",
      label: "sentinel",
      status,
      sessionDir: "/fake/.prime/agent/session-artifacts/s/sub-sentinel",
    },
  });

  /** The child thread's items up to the sentinel's end. */
  const childItemsUntilSentinelEnds = (
    session: Effect.Success<ReturnType<typeof openPrimeThread>>,
    childThreadId: unknown,
  ) =>
    Effect.gen(function* () {
      const items: Array<{ type: string; status: string; streaming?: boolean }> = [];
      for (;;) {
        const event = yield* session.takeEvent(() => true);
        if (
          event.type === "subagent.updated" &&
          event.subagent.title === "sentinel" &&
          event.subagent.status !== "running"
        ) {
          return items;
        }
        if (event.type === "turn_item.updated" && event.turnItem.threadId === childThreadId) {
          items.push({
            type: event.turnItem.type,
            status: event.turnItem.status,
            ...("streaming" in event.turnItem ? { streaming: event.turnItem.streaming } : {}),
          });
        }
      }
    });

  /** Lets a scheduled 50 ms stream flush come due, then checks it published nothing running. */
  const assertNoLaterRunningItem = (
    session: Effect.Success<ReturnType<typeof openPrimeThread>>,
    childThreadId: unknown,
  ) =>
    Effect.gen(function* () {
      const late = yield* session
        .takeEvent(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.threadId === childThreadId &&
            event.turnItem.status === "running",
        )
        .pipe(Effect.timeoutOption(Duration.seconds(1)), Effect.forkScoped);
      yield* TestClock.adjust(Duration.millis(200));
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.seconds(1));
      assert.isTrue(Option.isNone(yield* Fiber.join(late)));
    });

  it.effect("finishes a child's open reply and tool from its final history when done", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      const childThreadId = yield* startChildWithOpenWork(fake, session);

      // The stream lost the child's last events; its history still has them.
      fake.queueObserved([
        {
          ...assistantSnapshot(1001, "Half an answer, now whole."),
          content: [
            { type: "text", text: "Half an answer, now whole." },
            {
              type: "toolCall",
              id: "child-tool",
              name: "bash",
              arguments: { command: "sleep 30" },
            },
          ],
        },
        {
          role: "toolResult",
          toolCallId: "child-tool",
          toolName: "bash",
          content: [{ type: "text", text: "" }],
          isError: false,
          timestamp: 1002,
        },
      ]);
      yield* fake.emit(rlmChild("done", { activeSessionId: "active-1" }));
      yield* fake.takeRequest("observe");
      assert.equal((yield* fake.takeRequest("unobserve"))["activeSessionId"], "active-1");
      yield* fake.emit(sentinelChild("done"));

      const items = yield* childItemsUntilSentinelEnds(session, childThreadId);
      const last = (type: string) => items.findLast((item) => item.type === type);
      assert.deepEqual(last("assistant_message"), {
        type: "assistant_message",
        status: "completed",
        streaming: false,
      });
      assert.equal(last("command_execution")?.status, "completed");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("settles a child's tool and stops its pending flush when the process dies", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      const childThreadId = yield* startChildWithOpenWork(fake, session);

      yield* session.runtime.interruptTurn({
        providerThread: session.providerThread,
        providerTurnId: ProviderTurnId.make("provider-turn:settled"),
        requestRuntimeRestart: true,
      });
      yield* fake.closeStdout;
      const items = yield* childItemsUntilSentinelEnds(session, childThreadId);
      const last = (type: string) => items.findLast((item) => item.type === type);
      assert.equal(last("command_execution")?.status, "interrupted");
      assert.equal(last("assistant_message")?.status, "completed");
      // The 50 ms flush scheduled by the half reply must not publish a running item now.
      yield* assertNoLaterRunningItem(session, childThreadId);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("settles a child's running tool as interrupted when its session closes", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      const childThreadId = yield* startChildWithOpenWork(fake, session);

      yield* fake.emit({ type: "observed_session_closed", activeSessionId: "active-1" });
      yield* fake.emit(sentinelChild("done"));
      const items = yield* childItemsUntilSentinelEnds(session, childThreadId);
      const last = (type: string) => items.findLast((item) => item.type === type);
      assert.equal(last("command_execution")?.status, "interrupted");
      assert.deepEqual(last("assistant_message"), {
        type: "assistant_message",
        status: "completed",
        streaming: false,
      });
      yield* assertNoLaterRunningItem(session, childThreadId);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("nests a subagent's own children under it, even after the turn settled", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      const parent = yield* settleWithRunningChild(fake, session);
      yield* fake.emit({
        type: "rlm_child_update",
        child: {
          id: "sub-2",
          parentId: "sub-1",
          sessionName: "gamma",
          label: "Run sleep 20",
          status: "running",
          sessionDir: "/fake/.prime/agent/session-artifacts/s/sub-2",
        },
      });
      const nested = yield* session.takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.title === "gamma",
      );
      assert.isTrue(
        nested.type === "subagent.updated" &&
          nested.subagent.parentNodeId === parent?.id &&
          nested.subagent.runId === parent.runId,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("draws a nested child in its parent child's thread, not on the parent thread", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      const parent = yield* settleWithRunningChild(fake, session);
      const parentChildThreadId = parent?.childThreadId ?? null;
      assert.isNotNull(parentChildThreadId);
      yield* fake.emit({
        type: "rlm_child_update",
        child: {
          id: "sub-2",
          parentId: "sub-1",
          sessionName: "gamma",
          label: "Run sleep 20",
          status: "running",
          sessionDir: "/fake/.prime/agent/session-artifacts/s/sub-2",
        },
      });
      const seen: Array<ProviderAdapterV2Event> = [];
      do seen.push(yield* session.takeEvent(() => true));
      while (
        !seen.some(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "subagent" &&
            event.turnItem.title === "gamma",
        )
      );

      const thread = seen.find((event) => event.type === "app_thread.created");
      assert.isTrue(
        thread?.type === "app_thread.created" &&
          thread.appThread.lineage.parentThreadId === parentChildThreadId,
      );
      const card = seen.at(-1);
      assert.isTrue(
        card?.type === "turn_item.updated" &&
          card.turnItem.threadId === parentChildThreadId &&
          card.turnItem.runId === null,
      );
      // The parent thread keeps the record, so its subagent tray still nests the child.
      const record = seen.find(
        (event) => event.type === "subagent.updated" && event.subagent.title === "gamma",
      );
      assert.isTrue(
        record?.type === "subagent.updated" &&
          record.subagent.threadId === THREAD_ID &&
          record.subagent.parentNodeId === parent?.id,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("moves a nested child under its parent child once T3 first sees the parent", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      yield* startTurn(session.runtime, session.providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const untilCard = (title: string) =>
        Effect.gen(function* () {
          const seen: Array<ProviderAdapterV2Event> = [];
          do seen.push(yield* session.takeEvent(() => true));
          while (
            !seen.some(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.type === "subagent" &&
                event.turnItem.title === title,
            )
          );
          return seen;
        });
      const nested = {
        type: "rlm_child_update",
        child: { id: "sub-2", parentId: "sub-1", sessionName: "gamma", status: "running" },
      };

      // Prime Agent's roster can report a nested child before its parent child.
      yield* fake.emit(nested);
      const beforeParent = yield* untilCard("gamma");
      yield* fake.emit(rlmChild("running"));
      const parentSeen = yield* untilCard("worker");
      yield* fake.emit(nested);
      const afterParent = yield* untilCard("gamma");

      const parentChildThreadId =
        parentSeen.flatMap((event) =>
          event.type === "app_thread.created" ? [event.appThread.id] : [],
        )[0] ?? null;
      const nestedLineage = [...beforeParent, ...afterParent].flatMap((event) =>
        event.type === "app_thread.created" ? [event.appThread.lineage.parentThreadId] : [],
      );
      assert.deepEqual(nestedLineage, [THREAD_ID, parentChildThreadId]);
      const card = afterParent.at(-1);
      assert.isTrue(
        card?.type === "turn_item.updated" && card.turnItem.threadId === parentChildThreadId,
      );
      yield* fake.emit(nested);
      const again = yield* untilCard("gamma");
      assert.isFalse(again.some((event) => event.type === "app_thread.created"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a nested child under the root when its parent child finished unseen", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      yield* startTurn(session.runtime, session.providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const nested = (status: string) => ({
        type: "rlm_child_update",
        child: { id: "sub-2", parentId: "sub-1", sessionName: "gamma", status },
      });
      const seen: Array<ProviderAdapterV2Event> = [];
      const untilCard = (status: string) =>
        Effect.gen(function* () {
          let event: ProviderAdapterV2Event;
          do {
            event = yield* session.takeEvent(() => true);
            seen.push(event);
          } while (
            !(
              event.type === "turn_item.updated" &&
              event.turnItem.type === "subagent" &&
              event.turnItem.status === status
            )
          );
        });

      // A parent child T3 first sees finished is dropped, so it never gets a thread.
      yield* fake.emit(rlmChild("done"));
      yield* fake.emit(nested("running"));
      yield* untilCard("running");
      yield* fake.emit(nested("done"));
      yield* untilCard("completed");

      const threads = seen.flatMap((event) =>
        event.type === "app_thread.created" ? [event.appThread.lineage.parentThreadId] : [],
      );
      assert.deepEqual(threads, [THREAD_ID]);
      const cards = seen.flatMap((event) =>
        event.type === "turn_item.updated" && event.turnItem.type === "subagent"
          ? [event.turnItem.threadId]
          : [],
      );
      assert.deepEqual(cards, [THREAD_ID, THREAD_ID]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "reopens a finished child's thread for follow-up work and routes its nested children",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const session = yield* openPrimeThread(fake);
        fake.queueObserved([{ role: "user", content: "Reply to your parent", timestamp: 1000 }]);
        yield* startTurn(session.runtime, session.providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit(rlmChild("running", { activeSessionId: "active-1" }));
        const running = yield* session.takeEvent(
          (event) => event.type === "subagent.updated" && event.subagent.childThreadId !== null,
        );
        const child = running.type === "subagent.updated" ? running.subagent : undefined;
        assert.isDefined(child);
        yield* fake.takeRequest("observe");
        yield* fake.emit({ type: "agent_end", messages: [] });
        yield* fake.takeRequest("get_state");
        yield* session.takeEvent((event) => event.type === "turn.terminal");

        // The task finishes, which closes the child's transcript...
        yield* fake.emit(rlmChild("done", { activeSessionId: "active-1" }));
        yield* fake.takeRequest("observe");
        yield* fake.takeRequest("unobserve");
        yield* session.takeEvent(
          (event) => event.type === "subagent.updated" && event.subagent.status === "completed",
        );
        assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);

        /** Every event up to the first one matching `done`, so none can slip by unchecked. */
        const eventsUntil = (done: (event: ProviderAdapterV2Event) => boolean) =>
          Effect.gen(function* () {
            const seen: Array<ProviderAdapterV2Event> = [];
            do seen.push(yield* session.takeEvent(() => true));
            while (!done(seen[seen.length - 1]!));
            return seen;
          });
        const nestedChild = (id: string, title: string, status: string) => ({
          type: "rlm_child_update",
          child: {
            id,
            parentId: "sub-1",
            sessionName: title,
            label: title,
            status,
            sessionDir: `/fake/.prime/agent/session-artifacts/s/${id}`,
          },
        });

        // ...but Prime Agent keeps the session. The parent messages it, and the
        // roster reports follow-up work with the task status still `done`.
        fake.queueObserved([
          { role: "user", content: "Reply to your parent", timestamp: 1000 },
          assistantSnapshot(2001, "Follow-up reply."),
        ]);
        yield* fake.emit(
          rlmChild("done", { activeSessionId: "active-1", activity: { kind: "writing" } }),
        );
        assert.equal((yield* fake.takeRequest("observe"))["activeSessionId"], "active-1");
        const followUp = yield* eventsUntil(
          (event) =>
            event.type === "message.updated" &&
            event.message.threadId === child?.childThreadId &&
            event.message.text === "Follow-up reply.",
        );
        assert.isTrue(yield* session.runtime.hasPendingBackgroundWork!);

        // The root turn is idle, yet a nested child of the finished child still routes.
        yield* fake.emit(nestedChild("sub-2", "gamma", "running"));
        const nesting = yield* eventsUntil(
          (event) => event.type === "subagent.updated" && event.subagent.title === "gamma",
        );
        const nested = nesting[nesting.length - 1];
        assert.isTrue(
          nested?.type === "subagent.updated" &&
            nested.subagent.parentNodeId === child?.id &&
            nested.subagent.runId === child.runId,
        );
        yield* fake.emit(nestedChild("sub-2", "gamma", "done"));

        // The follow-up ends and the transcript closes again.
        yield* fake.emit(rlmChild("done", { activeSessionId: "active-1" }));
        yield* fake.takeRequest("observe");
        assert.equal((yield* fake.takeRequest("unobserve"))["activeSessionId"], "active-1");
        assert.isFalse(yield* session.runtime.hasPendingBackgroundWork!);
        yield* fake.emit(nestedChild("sub-3", "marker", "running"));
        const ending = yield* eventsUntil(
          (event) => event.type === "subagent.updated" && event.subagent.title === "marker",
        );

        // The child thread works again while the follow-up runs, as a resumed
        // Claude subagent does, and settles when it ends.
        const childRootStates = (events: ReadonlyArray<ProviderAdapterV2Event>) =>
          events.flatMap((event) =>
            event.type === "node.updated" &&
            event.node.kind === "root_turn" &&
            event.node.threadId === child?.childThreadId
              ? [{ status: event.node.status, completed: event.node.completedAt !== null }]
              : [],
          );
        assert.deepEqual(childRootStates(followUp), [{ status: "running", completed: false }]);
        assert.deepEqual(childRootStates(nesting), []);
        assert.deepEqual(childRootStates(ending), [{ status: "completed", completed: true }]);
        // The card keeps the original task's outcome.
        const cardRewrites = [...followUp, ...nesting, ...ending].filter(
          (event) => event.type === "subagent.updated" && event.subagent.id === child?.id,
        );
        assert.deepEqual(cardRewrites, []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("gives a child started by a finished child's follow-up to the live run", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      const finished = yield* settleWithRunningChild(fake, session);
      yield* fake.emit(rlmChild("done", { answerPreview: "child-ok" }));
      yield* session.takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.status === "completed",
      );

      // A later run messages the finished child, whose follow-up starts a child.
      // The first run no longer listens for the finished task, so the card goes to the live run.
      yield* startTurn(
        session.runtime,
        session.providerThread,
        "default",
        [],
        "Ask the worker again",
        undefined,
        2,
      );
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "rlm_child_update",
        child: {
          id: "sub-2",
          parentId: "sub-1",
          sessionName: "gamma",
          label: "Run sleep 20",
          status: "running",
          sessionDir: "/fake/.prime/agent/session-artifacts/s/sub-2",
        },
      });
      const nested = yield* session.takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.title === "gamma",
      );
      const liveRunId = RunId.make(`run:${THREAD_ID}:2`);
      assert.isTrue(
        nested.type === "subagent.updated" &&
          nested.subagent.parentNodeId === finished?.id &&
          nested.subagent.runId === liveRunId,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("puts the card of a child started under a failed run's child on the live run", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      yield* startTurn(session.runtime, session.providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(rlmChild("running"));
      const running = yield* session.takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.status === "running",
      );
      assert.isTrue(running.type === "subagent.updated");
      // Retries run out, so the run fails while its child keeps working.
      yield* fake.emit({
        type: "auto_retry_start",
        attempt: 1,
        maxAttempts: 1,
        delayMs: 0,
        errorMessage: "overloaded",
      });
      yield* fake.emit({
        type: "auto_retry_end",
        success: false,
        attempt: 1,
        finalError: "overloaded",
      });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      const terminal = yield* session.takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "failed");

      const nestedChild = (id: string) => ({
        type: "rlm_child_update",
        child: {
          id,
          parentId: "sub-1",
          sessionName: id,
          label: id,
          status: "running",
          sessionDir: `/fake/.prime/agent/session-artifacts/s/${id}`,
        },
      });
      // The failed run stopped listening, so a later live run takes the card.
      yield* startTurn(
        session.runtime,
        session.providerThread,
        "default",
        [],
        "Continue",
        undefined,
        2,
      );
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(nestedChild("live-nested"));
      const card = yield* session.takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.title === "live-nested",
      );
      const liveRunId = RunId.make(`run:${THREAD_ID}:2`);
      assert.isTrue(card.type === "subagent.updated" && card.subagent.runId === liveRunId);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("logs what became of every child update, with the reason for a drop", () =>
    Effect.gen(function* () {
      const traced = yield* Queue.unbounded<Record<string, unknown>>();
      const captureChildUpdateLogs = Logger.layer(
        [
          Logger.make(({ fiber, message }) => {
            if (
              Array.isArray(message) &&
              message[0] === "orchestration-v2.prime-agent-rlm-child-update"
            ) {
              Queue.offerUnsafe(traced, fiber.getRef(References.CurrentLogAnnotations));
            }
          }),
        ],
        { mergeWithExisting: false },
      );
      yield* Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const session = yield* openPrimeThread(fake);
        // The turn has settled and sub-1 is still running when the updates below arrive.
        yield* settleWithRunningChild(fake, session);
        const earlier = [yield* Queue.take(traced), yield* Queue.take(traced)];
        assert.deepEqual(
          earlier.map((line) => [line.status, line.outcome, line.routedVia]),
          [
            ["pending", "emitted", "current-turn"],
            ["running", "emitted", "known-child"],
          ],
        );
        const settledTurnRunId = earlier[0]?.runId;
        const nextLine = Effect.gen(function* () {
          const line = yield* Queue.take(traced);
          return [line.childId, line.outcome, line.reason, line.runId];
        });
        // No turn is running, so a child nobody has seen has nowhere to go.
        yield* fake.emit(rlmChild("running", { id: "sub-late" }));
        assert.deepEqual(yield* nextLine, ["sub-late", "dropped", "no-turn", null]);
        yield* fake.emit(rlmChild("done"));
        assert.deepEqual(yield* nextLine, ["sub-1", "emitted", null, settledTurnRunId]);
        // A later turn is running: now a repeat or a stranger is dropped for what it is.
        yield* startTurn(
          session.runtime,
          session.providerThread,
          "default",
          [],
          "Again",
          undefined,
          2,
        );
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit(rlmChild("done"));
        assert.deepEqual(yield* nextLine, ["sub-1", "dropped", "terminal-already-settled", null]);
        yield* fake.emit(rlmChild("done", { id: "sub-ghost" }));
        assert.deepEqual(yield* nextLine, [
          "sub-ghost",
          "dropped",
          "terminal-for-unseen-child",
          null,
        ]);
      }).pipe(Effect.provide(captureChildUpdateLogs));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("stops background subagents from a settled turn by restarting the session", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThread(fake);
      yield* settleWithRunningChild(fake, session);
      yield* session.runtime.interruptTurn({
        providerThread: session.providerThread,
        providerTurnId: ProviderTurnId.make("provider-turn:settled"),
        requestRuntimeRestart: true,
      });
      yield* fake.closeStdout;
      const stopped = yield* session.takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.status === "interrupted",
      );
      assert.equal(stopped.type, "subagent.updated");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("lists background shell jobs until the kernel reports them finished", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThreadWithWakes(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      const code = "late_job = bash('sleep 30 && echo late'); print(late_job.pid)";
      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId: "cell_bg",
        toolName: "ipython",
        args: { code },
      });
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId: "cell_bg",
        toolName: "ipython",
        result: { content: [{ type: "text", text: "4242\n" }], details: { status: "ok" } },
        isError: false,
      });
      const listed = yield* takeEvent(
        (event) =>
          event.type === "provider_thread.updated" &&
          (event.providerThread.pendingBackgroundTasks?.length ?? 0) > 0,
      );
      assert.isTrue(
        listed.type === "provider_thread.updated" &&
          listed.providerThread.pendingBackgroundTasks?.[0]?.kind === "command" &&
          listed.providerThread.pendingBackgroundTasks?.[0]?.description ===
            "sleep 30 && echo late",
      );
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);

      // The finished job wakes the agent; its notice clears the list right away.
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "message_start",
        message: {
          role: "custom",
          customType: "async_bash_completion",
          content: "[bash-done pid:4242 exit:0]",
          details: { pid: 4242, command: "sleep 30 && echo late", exitCode: 0 },
        },
      });
      const cleared = yield* takeEvent(
        (event) =>
          event.type === "provider_thread.updated" &&
          (event.providerThread.pendingBackgroundTasks?.length ?? 0) === 0,
      );
      assert.equal(cleared.type, "provider_thread.updated");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /** The session id the fake reports from `get_state`; heartbeats are matched on it. */
  const FAKE_SESSION_ID = "00000000-0000-4000-8000-000000000002";

  const heartbeatJob = (job: Record<string, unknown>) => ({
    job: {
      source: "rlm_heartbeat",
      status: "active",
      sessionId: FAKE_SESSION_ID,
      prompt: "Check the deploy.",
      schedule: { kind: "interval", expression: "every 15m", intervalMs: 900_000 },
      ...job,
    },
  });

  const heartbeatIds = (event: ProviderAdapterV2Event) =>
    event.type === "provider_thread.updated"
      ? event.providerThread.heartbeats?.map((heartbeat) => heartbeat.id)
      : undefined;

  it.effect("publishes the heartbeats of the session it opens, not another session's", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueHeartbeats({
        heartbeats: [
          heartbeatJob({ id: "deploy", label: "deploy watch", nextRunAt: "2026-10-03T15:40:00Z" }),
          heartbeatJob({ id: "elsewhere", sessionId: "another-session" }),
          heartbeatJob({
            id: "paused",
            status: "paused",
            prompt: "\nFirst line\nSecond line",
            schedule: { kind: "interval", expression: "every 5m", intervalMs: 300_000 },
          }),
        ],
      });
      const { runtime, providerThread } = yield* openPrimeThread(fake);

      assert.deepEqual(providerThread.heartbeats, [
        {
          id: "deploy",
          description: "deploy watch",
          schedule: "every 15m",
          paused: false,
          nextRunAt: "2026-10-03T15:40:00Z",
        },
        { id: "paused", description: "First line", schedule: "every 5m", paused: true },
      ]);
      // A heartbeat is configuration. It must not keep the thread working.
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
      assert.deepEqual(providerThread.pendingBackgroundTasks, []);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("follows a heartbeat as turns settle: changed, then removed", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueHeartbeats({ heartbeats: [heartbeatJob({ id: "deploy" })] });
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      assert.deepEqual(
        providerThread.heartbeats?.map((heartbeat) => heartbeat.id),
        ["deploy"],
      );

      // The agent adds a second heartbeat and the first one runs on a new schedule.
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      fake.queueHeartbeats({
        heartbeats: [
          heartbeatJob({
            id: "deploy",
            schedule: { kind: "interval", expression: "every 2h", intervalMs: 7_200_000 },
            nextRunAt: "2026-10-03T17:00:00Z",
          }),
          heartbeatJob({ id: "tests", label: "tests" }),
        ],
      });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      const changed = yield* takeEvent((event) => heartbeatIds(event)?.length === 2);
      assert.deepEqual(
        changed.type === "provider_thread.updated"
          ? changed.providerThread.heartbeats?.map(
              (heartbeat) => `${heartbeat.id} ${heartbeat.schedule} ${heartbeat.nextRunAt}`,
            )
          : undefined,
        ["deploy every 2h 2026-10-03T17:00:00Z", "tests every 15m undefined"],
      );
      yield* takeEvent((event) => event.type === "turn.terminal");

      // The agent ends both heartbeats.
      yield* startTurn(runtime, providerThread, "default", [], "Stop watching", undefined, 2);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      const removed = yield* takeEvent((event) => heartbeatIds(event)?.length === 0);
      assert.equal(removed.type, "provider_thread.updated");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps the heartbeats it showed when the agent cannot list them", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueHeartbeats({ heartbeats: [heartbeatJob({ id: "deploy" })] });
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      fake.failNextHeartbeats();
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      const seen = yield* drainUntilTerminal(takeEvent);

      assert.isTrue(
        seen.some(
          (event) =>
            event.type === "provider_thread.updated" && event.providerThread.status === "idle",
        ),
      );
      for (const event of seen) {
        if (event.type !== "provider_thread.updated") continue;
        assert.deepEqual(
          event.providerThread.heartbeats?.map((heartbeat) => heartbeat.id),
          ["deploy"],
        );
      }
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows a heartbeat that fires during a turn, with the prompt it ran", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* emitAssistantReply(fake, "Working on it.");
      yield* takeCompletedReply(takeEvent);

      yield* fake.emit({
        type: "message_start",
        message: {
          role: "custom",
          customType: "heartbeat_prompt",
          content: "[heartbeat: every 5m run#3]\n\nCheck the deploy.",
          details: { jobId: "deploy", schedule: "every 5m", runCount: 3 },
        },
      });
      const notice = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "notification",
      );

      assert.isTrue(notice.type === "turn_item.updated");
      if (notice.type !== "turn_item.updated" || notice.turnItem.type !== "notification") return;
      assert.equal(notice.turnItem.summary, "Heartbeat");
      assert.deepEqual(notice.turnItem.source, { kind: "heartbeat", heartbeatId: "deploy" });
      assert.equal(notice.turnItem.detail, "Check the deploy.");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("shows a heartbeat that wakes an idle agent, with the prompt it ran", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { offers } = yield* openPrimeThreadWithWakes(fake);

      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "message_start",
        message: {
          role: "custom",
          customType: "heartbeat_prompt",
          content: "[heartbeat: every 5m run#3]\n\nCheck the deploy.",
          details: { jobId: "deploy", schedule: "every 5m", runCount: 3 },
        },
      });
      yield* emitAssistantReply(fake, "The deploy is fine.");
      const offer = yield* Queue.take(offers);

      assert.equal(offer.notification?.summary, "Heartbeat");
      assert.deepEqual(offer.notification?.source, { kind: "heartbeat", heartbeatId: "deploy" });
      assert.equal(offer.notification?.detail, "Check the deploy.");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /** Collects the events a turn publishes until it settles. */
  const drainUntilTerminal = (
    takeEvent: (
      predicate: (event: ProviderAdapterV2Event) => boolean,
    ) => Effect.Effect<ProviderAdapterV2Event>,
  ) =>
    Effect.gen(function* () {
      const seen: Array<ProviderAdapterV2Event> = [];
      yield* takeEvent((event) => {
        seen.push(event);
        return event.type === "turn.terminal";
      });
      return seen;
    });

  const emitCell = (fake: FakePi, toolCallId: string, code: string, text = "ok\n") =>
    Effect.gen(function* () {
      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId,
        toolName: "ipython",
        args: { code },
      });
      yield* fake.emit({
        type: "tool_execution_end",
        toolCallId,
        toolName: "ipython",
        result: { content: [{ type: "text", text }], details: { status: "ok" } },
        isError: false,
      });
    });

  const rosterLengths = (events: ReadonlyArray<ProviderAdapterV2Event>) =>
    events.flatMap((event) =>
      event.type === "provider_thread.updated"
        ? [event.providerThread.pendingBackgroundTasks?.length ?? 0]
        : [],
    );

  it.effect("stops listing a background shell job once a later cell kills its handle", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThreadWithWakes(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* emitCell(fake, "cell_start", "job = bash('sleep 300'); print(job.pid)");
      yield* takeEvent(
        (event) =>
          event.type === "provider_thread.updated" &&
          (event.providerThread.pendingBackgroundTasks?.length ?? 0) === 1,
      );
      // Reading the result after the kill withdraws the kernel's completion
      // notice, so the kill is the only end signal T3 gets.
      yield* emitCell(fake, "cell_kill", "job.kill(); print(job.output())");
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      const seen = yield* drainUntilTerminal(takeEvent);
      assert.deepEqual(rosterLengths(seen).slice(0, 1), [0]);
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /**
   * Cells and kernel notices from a real Prime Agent session (a Windows box
   * set up over a tailnet). Every job but the unread one below was read by a
   * later cell, so Prime Agent either withdrew its completion notice or sent
   * it while the job still ran. Secrets are replaced by `TOKEN`.
   */
  const readJobsSession: ReadonlyArray<
    { readonly cell: string } | { readonly finished: string; readonly exitCode: number }
  > = [
    {
      cell: "rd_net_handle = bash('dscacheutil -q host -a name rs-ny.rustdesk.com; nc -vz -G 5 rs-ny.rustdesk.com 21116 2>&1')",
    },
    {
      cell: "print(rd_net_handle.running)\nrd_net_output = rd_net_handle.output()\nprint(rd_net_output)",
    },
    { cell: "rd_tc_help_handle = bash(', tailcat --help')" },
    { cell: "print(rd_tc_help_handle.output()[-5500:])" },
    {
      cell: 'rd_enable_cmd = "Get-Process"\nrd_enable_handle = bash(f", tailcat ssh {code} {shlex.quote(rd_enable_cmd)} < /dev/null")',
    },
    {
      cell: 'print(rd_enable_handle.running, rd_enable_handle.output())\nrd_tunnel_handle = bash(f", tailcat ssh {code} -N -o ExitOnForwardFailure=yes -L 127.0.0.1:21118:127.0.0.1:21118 < /dev/null")',
    },
    {
      cell: "print(rd_tunnel_handle.running); print(rd_tunnel_handle.output())\nrd_test_handle = bash('nc -vz -G 3 127.0.0.1 21118 2>&1')",
    },
    { finished: "nc -vz -G 3 127.0.0.1 21118 2>&1", exitCode: 1 },
    {
      cell: "rd_tunnel_handle = bash(f\"ssh -N -o BatchMode=yes -o ExitOnForwardFailure=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new -o {shlex.quote('ProxyCommand=, tailcat '+code+' 22')} -L 127.0.0.1:21118:127.0.0.1:21118 {code} < /dev/null\")",
    },
    {
      cell: "print(rd_tunnel_handle.running); print(rd_tunnel_handle.output())\nrd_test_handle = bash('nc -vz -G 3 127.0.0.1 21118 2>&1')",
    },
    { cell: "print(rd_test_handle.output()); print(rd_tunnel_handle.output()[-1200:])" },
    {
      cell: 'rd_tunnel_handle.kill()\nrd_tailcat_probe = "(Get-Command tailcat -ErrorAction Stop).Source; tailcat serve --help"\nrd_tailcat_probe_handle = bash(f", tailcat ssh {code} {shlex.quote(rd_tailcat_probe)} < /dev/null")',
    },
    { cell: "print(rd_tailcat_probe_handle.output()[:6500])" },
    {
      finished:
        "ssh -N -o BatchMode=yes -o ExitOnForwardFailure=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new -o 'ProxyCommand=, tailcat TOKEN 22' -L 127.0.0.1:21118:127.0.0.1:21118 TOKEN < /dev/null",
      exitCode: -15,
    },
    {
      cell: 'rd_tc_find = "Get-Process"\nrd_tc_find_handle = bash(f", tailcat ssh {code} {shlex.quote(rd_tc_find)} < /dev/null")',
    },
    { cell: "print(rd_tc_find_handle.output())" },
    {
      cell: 'rd_tc_start = "Get-Process"\nrd_tc_start_handle = bash(f", tailcat ssh {code} {shlex.quote(rd_tc_start)} < /dev/null")',
    },
    {
      cell: 'print(rd_tc_start_handle.output())\nrd_tc_addr_cmd = "Get-Process"\nrd_tc_addr_handle = bash(f", tailcat ssh {code} {shlex.quote(rd_tc_addr_cmd)} < /dev/null")',
    },
    { cell: "rd_tc_addr_output=rd_tc_addr_handle.output(); print(rd_tc_addr_output)" },
    {
      cell: 'import json\nrd_forward_handle = bash(f", tailcat forward --bind=127.0.0.1 {rd_forward_code} 21118:21118")',
    },
    {
      cell: "print(rd_forward_handle.running); print(rd_forward_handle.output())\nrd_forward_test = bash('nc -vz -G 3 127.0.0.1 21118 2>&1')",
    },
    { cell: "print(rd_forward_test.output()); print(rd_forward_handle.output())" },
    {
      cell: "ethan_test_handle = bash('tailscale status; tailscale ping --c 3 --timeout 5s ethan')",
    },
    {
      cell: "print(ethan_test_handle.running)\nethan_test_output = ethan_test_handle.output()\nprint(ethan_test_output)",
    },
    {
      cell: 'power_cmd = "Get-Process"\npower_handle = bash(f", tailcat ssh {code} {shlex.quote(power_cmd)} < /dev/null")',
    },
    {
      cell: "print(power_handle.running); power_output=power_handle.output(); print(power_output)",
    },
    { finished: ", tailcat ssh TOKEN 'Get-Process' < /dev/null", exitCode: 255 },
    {
      cell: "power_output = power_handle.output(); print(power_output[-2500:])\npower_reach_handle = bash('tailscale ping --c 1 --timeout 5s 100.120.125.3')",
    },
    { finished: "tailscale ping --c 1 --timeout 5s 100.120.125.3", exitCode: 1 },
    { cell: "print(power_reach_handle.output())" },
  ];

  it.effect("lists only the jobs no cell read after a long session of read handles", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThreadWithWakes(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      for (const [index, step] of readJobsSession.entries()) {
        if ("cell" in step) {
          yield* emitCell(fake, `cell_${index}`, step.cell);
          continue;
        }
        yield* fake.emit({
          type: "message_start",
          message: {
            role: "custom",
            customType: "async_bash_completion",
            details: { pid: 1000 + index, command: step.finished, exitCode: step.exitCode },
          },
        });
      }
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      const seen = yield* drainUntilTerminal(takeEvent);
      const rosters = seen.flatMap((event) =>
        event.type === "provider_thread.updated"
          ? [event.providerThread.pendingBackgroundTasks ?? []]
          : [],
      );
      assert.deepEqual(rosters.at(-1), []);
      assert.isTrue(rosters.flat().every((task) => (task.description ?? "").trim().length > 0));
      // A job whose result was read may still run, so the session stays up for it.
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "takes a background shell job off the roster once a later cell reads it, but keeps the session alive",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent, providerThread } = yield* openPrimeThreadWithWakes(fake);
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        yield* emitCell(fake, "cell_start", "job = bash('sleep 300'); print(job.pid)");
        yield* takeEvent(
          (event) =>
            event.type === "provider_thread.updated" &&
            (event.providerThread.pendingBackgroundTasks?.length ?? 0) === 1,
        );
        // Prime Agent drops the completion notice of a job that was done at the
        // read, and nothing tells T3 whether this one was. The job may still run.
        yield* emitCell(fake, "cell_peek", "print(job.tail(5))");
        yield* fake.emit({ type: "agent_end", messages: [] });
        yield* fake.takeRequest("get_state");
        const seen = yield* drainUntilTerminal(takeEvent);
        assert.deepEqual(rosterLengths(seen).slice(0, 1), [0]);
        assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("does not list a job its own cell killed", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThreadWithWakes(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* emitCell(fake, "cell_both", "job = bash('sleep 300'); job.kill()");
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      const seen = yield* drainUntilTerminal(takeEvent);
      assert.isTrue(rosterLengths(seen).every((length) => length === 0));
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("settles a cell whose end event never came when the turn ends", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "tool_execution_start",
        toolCallId: "cell_lost",
        toolName: "ipython",
        args: { code: "print(open('f').read())" },
      });
      yield* fake.emit({
        type: "tool_execution_update",
        toolCallId: "cell_lost",
        toolName: "ipython",
        args: { code: "print(open('f').read())" },
        partialResult: { content: [{ type: "text", text: "mcp" }] },
      });
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      const seen = yield* drainUntilTerminal(takeEvent);
      const cellStatuses = seen.flatMap((event) =>
        event.type === "turn_item.updated" && event.turnItem.nativeItemRef?.nativeId === "cell_lost"
          ? [event.turnItem.status]
          : [],
      );
      assert.deepEqual([cellStatuses[0], cellStatuses.at(-1)], ["running", "completed"]);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /** A cell that started but whose end event never arrives. */
  const emitLostCell = (fake: FakePi, toolCallId: string, code: string) =>
    fake.emit({ type: "tool_execution_start", toolCallId, toolName: "ipython", args: { code } });

  const cellStatuses = (events: ReadonlyArray<ProviderAdapterV2Event>, toolCallId: string) =>
    events.flatMap((event) =>
      event.type === "turn_item.updated" && event.turnItem.nativeItemRef?.nativeId === toolCallId
        ? [event.turnItem.status]
        : [],
    );

  it.effect("does not list a job for a cell whose end was lost when the turn ends", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThreadWithWakes(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* emitLostCell(fake, "cell_lost", "job = bash('sleep 300'); print(job.pid)");
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      const seen = yield* drainUntilTerminal(takeEvent);
      assert.deepEqual(cellStatuses(seen, "cell_lost").at(-1), "completed");
      assert.isTrue(rosterLengths(seen).every((length) => length === 0));
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps a running job when a cell that would kill it never reported its end", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThreadWithWakes(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* emitCell(fake, "cell_start", "job = bash('sleep 300'); print(job.pid)");
      yield* emitLostCell(fake, "cell_kill", "job.kill()");
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      yield* drainUntilTerminal(takeEvent);
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("fails a cell whose end was lost and lists no job when the process dies", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThreadWithWakes(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* emitLostCell(fake, "cell_lost", "job = bash('sleep 300'); print(job.pid)");
      yield* fake.closeStdout;
      const seen = yield* drainUntilTerminal(takeEvent);
      assert.deepEqual(cellStatuses(seen, "cell_lost").at(-1), "failed");
      assert.isTrue(rosterLengths(seen).every((length) => length === 0));
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "interrupts a cell whose end was lost and lists no job when Stop kills the process",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent, providerThread } = yield* openPrimeThreadWithWakes(fake);
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        const runningTurn = yield* takeEvent(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "running",
        );
        const providerTurnId =
          runningTurn.type === "provider_turn.updated" ? runningTurn.providerTurn.id : undefined;
        yield* fake.emit({ type: "agent_start" });
        yield* emitLostCell(fake, "cell_lost", "job = bash('sleep 300'); print(job.pid)");
        yield* runtime.interruptTurn({ providerThread, providerTurnId: providerTurnId! });
        yield* fake.closeStdout;
        const seen = yield* drainUntilTerminal(takeEvent);
        assert.deepEqual(cellStatuses(seen, "cell_lost").at(-1), "interrupted");
        assert.isTrue(rosterLengths(seen).every((length) => length === 0));
        assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("drops a persisted job roster the reopened process never started", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, providerThread } = yield* openPrimeThread(fake);
      const reopened = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: {
          ...providerThread,
          nativeThreadRef: null,
          pendingBackgroundTasks: [{ taskId: "bash:1", kind: "command", description: "sleep 300" }],
        },
      });
      assert.deepEqual(reopened.pendingBackgroundTasks, []);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps the jobs a live process started when its thread is registered again", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThreadWithWakes(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* emitCell(fake, "cell_start", "job = bash('sleep 300'); print(job.pid)");
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      yield* drainUntilTerminal(takeEvent);
      const reopened = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
        existingProviderThread: { ...providerThread, nativeThreadRef: null },
      });
      assert.deepEqual(
        reopened.pendingBackgroundTasks?.map((task) => task.description),
        ["sleep 300"],
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /** One assistant reply, as Prime Agent streams it. */
  const emitAssistantReply = (fake: FakePi, text: string) =>
    Effect.gen(function* () {
      yield* fake.emit({ type: "message_start", message: { role: "assistant", content: [] } });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: { type: "text_end", contentIndex: 0, content: text },
      });
      yield* fake.emit({
        type: "message_end",
        message: { role: "assistant", content: [{ type: "text", text }], stopReason: "stop" },
      });
    });

  const takeCompletedReply = (
    takeEvent: (
      predicate: (event: ProviderAdapterV2Event) => boolean,
    ) => Effect.Effect<ProviderAdapterV2Event>,
  ) =>
    takeEvent(
      (event) =>
        event.type === "turn_item.updated" &&
        event.turnItem.type === "assistant_message" &&
        event.turnItem.status === "completed",
    );

  it.effect("shows why the agent went on when a finished background command joins its run", () =>
    Effect.gen(function* () {
      // Thread a8bf335f, run 32: the plan reply, then a `bash()` job that was
      // started earlier finished and the agent wrote a second reply.
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* emitAssistantReply(fake, "Here is the plan.");
      yield* takeCompletedReply(takeEvent);

      yield* fake.emit({
        type: "message_start",
        message: {
          role: "custom",
          customType: "async_bash_completion",
          content: "[bash-done pid:69972 exit:0]",
          details: { pid: 69972, command: "tailcat ssh verify", exitCode: 0 },
        },
      });
      const notice = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "notification",
      );
      yield* emitAssistantReply(fake, "The previously started verification finished.");
      const second = yield* takeCompletedReply(takeEvent);

      assert.isTrue(notice.type === "turn_item.updated");
      if (notice.type !== "turn_item.updated" || notice.turnItem.type !== "notification") return;
      assert.equal(notice.turnItem.summary, "Background command finished");
      assert.deepEqual(notice.turnItem.source, { kind: "command" });
      assert.equal(notice.turnItem.outcome, "completed");
      assert.equal(notice.turnItem.status, "completed");
      assert.equal(
        notice.turnItem.nativeItemRef?.nativeId,
        `${notice.turnItem.providerTurnId}:wake:1`,
      );
      // The notification sits in the run before the reply it explains.
      assert.isTrue(
        second.type === "turn_item.updated" && second.turnItem.ordinal > notice.turnItem.ordinal,
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("names a message from a child agent that joins a running turn", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      // Kernel bookkeeping says nothing a user needs.
      yield* fake.emit({
        type: "message_start",
        message: { role: "custom", customType: "ipython_state_restored", content: "" },
      });
      yield* fake.emit({
        type: "message_start",
        message: {
          role: "custom",
          customType: "agent_message",
          content: "[agent-message from child:worker]\n\nchild-ok",
          details: {
            message: "child-ok",
            from: { sessionName: "worker", runtimeKind: "subagent" },
            fromRelationship: "child",
          },
        },
      });
      yield* fake.emit({
        type: "message_start",
        message: { role: "custom", customType: "async_bash_completion", details: {} },
      });
      const first = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "notification",
      );
      const second = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "notification",
      );

      assert.isTrue(first.type === "turn_item.updated" && first.turnItem.type === "notification");
      if (first.type !== "turn_item.updated" || first.turnItem.type !== "notification") return;
      if (second.type !== "turn_item.updated" || second.turnItem.type !== "notification") return;
      assert.equal(first.turnItem.summary, "Message from worker");
      assert.equal(first.turnItem.detail, "child-ok");
      assert.deepEqual(first.turnItem.source, { kind: "subagent" });
      assert.equal(second.turnItem.summary, "Background command finished");
      assert.isUndefined(
        second.turnItem.detail,
        "a completion that names no command has no detail",
      );
      assert.notEqual(first.turnItem.id, second.turnItem.id);
      assert.notEqual(first.turnItem.nodeId, second.turnItem.nodeId);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("names the command and handle of a background job that finishes during a run", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* emitCell(fake, "cell_bg", "build = bash('make build'); print(build.pid)");
      yield* fake.emit({
        type: "message_start",
        message: {
          role: "custom",
          customType: "async_bash_completion",
          content: "[bash-done pid:51 exit:2]",
          details: { pid: 51, command: "make build", exitCode: 2 },
        },
      });
      const notice = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "notification",
      );

      assert.isTrue(notice.type === "turn_item.updated" && notice.turnItem.type === "notification");
      if (notice.type !== "turn_item.updated" || notice.turnItem.type !== "notification") return;
      assert.equal(notice.turnItem.detail, "make build\n\nExit code 2\n\nHandle: build");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("names the command of a background job that wakes an idle agent", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread, offers } = yield* openPrimeThreadWithWakes(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* emitCell(fake, "cell_bg", "lint = bash('make lint'); print(lint.pid)");
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      yield* takeEvent((event) => event.type === "turn.terminal");

      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "message_start",
        message: {
          role: "custom",
          customType: "async_bash_completion",
          content: "[bash-done pid:52 exit:0]",
          details: { pid: 52, command: "make lint", exitCode: 0 },
        },
      });
      yield* emitAssistantReply(fake, "Lint is clean.");
      const offer = yield* Queue.take(offers);

      assert.equal(offer.notification?.summary, "Background command finished");
      assert.equal(offer.notification?.detail, "make lint\n\nExit code 0\n\nHandle: lint");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("does not repeat a continuation run's own wake notification", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread, offers } = yield* openPrimeThreadWithWakes(fake);
      yield* emitChildMessageWake(fake);
      yield* Queue.take(offers);
      yield* startTurn(
        runtime,
        providerThread,
        "default",
        [],
        "Background activity updated",
        undefined,
        1,
        THREAD_ID,
        "provider",
      );
      yield* fake.takeRequest("get_state");
      const adapterNotices: string[] = [];
      yield* takeEvent((event) => {
        if (event.type === "turn_item.updated" && event.turnItem.type === "notification") {
          adapterNotices.push(event.turnItem.summary);
        }
        return event.type === "turn.terminal";
      });
      // The orchestrator shows the first wake message as the run's own
      // notification, so the adapter says nothing more about it.
      assert.deepEqual(adapterNotices, []);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("names the wake message when the user's turn adopts it", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread, offers } = yield* openPrimeThreadWithWakes(fake);
      yield* emitChildMessageWake(fake);
      yield* Queue.take(offers);
      yield* startTurn(runtime, providerThread, "default", [], "What happened?");
      yield* fake.takeRequest("prompt");
      const notice = yield* takeEvent(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "notification",
      );
      assert.isTrue(
        notice.type === "turn_item.updated" &&
          notice.turnItem.type === "notification" &&
          notice.turnItem.summary === "Message from worker",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("restarts Prime Agent when Stop interrupts a turn with a live subagent", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      const runningTurn = yield* takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      const providerTurnId =
        runningTurn.type === "provider_turn.updated" ? runningTurn.providerTurn.id : undefined;
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(rlmChild("running"));
      yield* takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.status === "running",
      );

      yield* runtime.interruptTurn({ providerThread, providerTurnId: providerTurnId! });
      yield* fake.closeStdout;
      const stopped = yield* takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.status === "interrupted",
      );
      assert.equal(stopped.type, "subagent.updated");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  const openPrimeThreadWithWakes = Effect.fnUntraced(function* (fake: FakePi) {
    const offers = yield* Queue.unbounded<ProviderContinuationRequest>();
    const { runtime, takeEvent } = yield* openRuntime(
      fake,
      "default",
      THREAD_ID,
      SESSION_ID,
      undefined,
      PRIME_AGENT_FLAVOR,
      { offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid) },
    );
    const providerThread = yield* runtime.ensureThread({
      threadId: THREAD_ID,
      modelSelection: modelSelection("default"),
      runtimePolicy,
    });
    return { runtime, takeEvent, providerThread, offers };
  });

  /** What Prime Agent emits when a child's message wakes the idle parent. */
  const emitChildMessageWake = (fake: FakePi) =>
    Effect.gen(function* () {
      yield* fake.emit({ type: "agent_start" });
      // Prime Agent restores a parked kernel before delivering the wake message.
      yield* fake.emit({
        type: "message_start",
        message: { role: "custom", customType: "ipython_state_restored", content: "" },
      });
      yield* fake.emit({
        type: "message_start",
        message: {
          role: "custom",
          customType: "agent_message",
          content: "[agent-message from child:worker]\n\nchild-ok",
          details: {
            message: "child-ok",
            from: { sessionName: "worker", runtimeKind: "subagent" },
            fromRelationship: "child",
          },
        },
      });
      yield* fake.emit({ type: "message_start", message: { role: "assistant", content: [] } });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: {
          type: "text_delta",
          contentIndex: 0,
          delta: "The child said child-ok",
        },
      });
      yield* fake.emit({
        type: "message_update",
        assistantMessageEvent: {
          type: "text_end",
          contentIndex: 0,
          content: "The child said child-ok",
        },
      });
      yield* fake.emit({ type: "agent_end", messages: [] });
    });

  it.effect("routes a child's final message while the parent's wake is still buffered", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* openPrimeThreadWithWakes(fake);
      yield* startTurn(session.runtime, session.providerThread);
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit(rlmChild("queued"));
      yield* fake.emit(rlmChild("running", { activeSessionId: "active-1" }));
      const running = yield* session.takeEvent(
        (event) => event.type === "subagent.updated" && event.subagent.childThreadId !== null,
      );
      const childThreadId =
        running.type === "subagent.updated" ? running.subagent.childThreadId : null;
      yield* fake.takeRequest("observe");
      yield* fake.emit({ type: "agent_end", messages: [] });
      yield* fake.takeRequest("get_state");
      yield* session.takeEvent((event) => event.type === "turn.terminal");

      // The parent wakes on its own; its events now wait for a continuation.
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({
        type: "observed_session_event",
        activeSessionId: "active-1",
        event: {
          type: "message_end",
          message: {
            role: "assistant",
            content: [{ type: "text", text: "child-ok sent." }],
            timestamp: 1002,
          },
        },
      });
      const reply = yield* session.takeEvent(
        (event) =>
          event.type === "message.updated" &&
          event.message.threadId === childThreadId &&
          event.message.text === "child-ok sent.",
      );
      assert.equal(reply.type, "message.updated");
      // The terminal roster update drops the route only after the message used it.
      yield* fake.emit(rlmChild("done", { activeSessionId: "active-1" }));
      assert.equal((yield* fake.takeRequest("unobserve"))["activeSessionId"], "active-1");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("hands a self-wake to a continuation run instead of stopping the session", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread, offers } = yield* openPrimeThreadWithWakes(fake);
      yield* emitChildMessageWake(fake);

      const offer = yield* Queue.take(offers);
      assert.equal(offer.threadId, THREAD_ID);
      assert.deepEqual(offer.notification?.source, { kind: "subagent" });
      assert.equal(offer.notification?.summary, "Message from worker");
      assert.equal(offer.notification?.detail, "child-ok");
      const dispatched = yield* offer.dispatchIfCurrent!(Effect.succeed("dispatched"));
      assert.isTrue(Option.isSome(dispatched));

      const promptsBefore = fake.allRequests().filter((r) => r["type"] === "prompt").length;
      yield* startTurn(
        runtime,
        providerThread,
        "default",
        [],
        "Background activity updated",
        undefined,
        1,
        THREAD_ID,
        "provider",
      );
      const reply = yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "assistant_message" &&
          event.turnItem.status === "completed",
      );
      assert.isTrue(
        reply.type === "turn_item.updated" &&
          reply.turnItem.type === "assistant_message" &&
          reply.turnItem.text === "The child said child-ok",
      );
      yield* fake.takeRequest("get_state");
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      assert.equal(
        fake.allRequests().filter((r) => r["type"] === "prompt").length,
        promptsBefore,
        "a continuation run replays the wake and prompts nothing",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("lets the user's next turn adopt a pending wake and steer it", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread, offers } = yield* openPrimeThreadWithWakes(fake);
      yield* emitChildMessageWake(fake);
      const offer = yield* Queue.take(offers);

      yield* startTurn(runtime, providerThread, "default", [], "What happened?");
      const prompt = yield* fake.takeRequest("prompt");
      assert.equal(prompt["streamingBehavior"], "steer");
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "assistant_message" &&
          event.turnItem.status === "completed",
      );
      const stale = yield* offer.dispatchIfCurrent!(Effect.succeed("dispatched"));
      assert.isTrue(Option.isNone(stale), "the adopted wake's continuation must not dispatch");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /** Lets every fiber that a step woke run before the next step. */
  const drainFibers = Effect.gen(function* () {
    for (let turn = 0; turn < 20; turn += 1) yield* Effect.yieldNow;
  });

  /** Steps virtual time through quiet checks until `done`, or gives up after an hour. */
  const tickUntil = (done: () => boolean) =>
    Effect.gen(function* () {
      for (let step = 0; step < 240 && !done(); step += 1) {
        yield* TestClock.adjust(Duration.seconds(15));
        yield* drainFibers;
      }
    });

  const storedChildMessage = (timestamp: number) => ({
    role: "custom",
    customType: "agent_message",
    content: "[agent-message from child:worker]\n\nchild-ok",
    display: true,
    details: {
      message: "child-ok",
      from: { sessionName: "worker", runtimeKind: "subagent" },
      fromRelationship: "child",
    },
    timestamp,
  });

  /** Opens a session whose first turn left a child running, with that turn stored. */
  const settleWithChildAndHistory = (fake: FakePi) =>
    Effect.gen(function* () {
      const session = yield* openPrimeThreadWithWakes(fake);
      const history = [storedPrompt(900), storedReply(1000, "A worker is on it.")];
      fake.setTranscript(history);
      const child = yield* settleWithRunningChild(fake, session);
      return { ...session, history, child };
    });

  /** Takes the one wake offer the quiet checks make for work T3 never saw. */
  const takeRecoveredOffer = (offers: Queue.Queue<ProviderContinuationRequest>) =>
    Effect.gen(function* () {
      yield* tickUntil(() => Queue.sizeUnsafe(offers) > 0);
      const offer = yield* Queue.poll(offers);
      assert.isTrue(Option.isSome(offer), "a quiet check offers the lost wake");
      return Option.getOrThrow(offer);
    });

  /** Dispatches a wake offer as a continuation run and returns how that run ended. */
  const runContinuation = (
    session: Effect.Success<ReturnType<typeof openPrimeThreadWithWakes>>,
    offer: ProviderContinuationRequest,
  ) =>
    Effect.gen(function* () {
      const dispatched = yield* offer.dispatchIfCurrent!(Effect.succeed("dispatched"));
      assert.isTrue(Option.isSome(dispatched));
      yield* startTurn(
        session.runtime,
        session.providerThread,
        "default",
        [],
        "Background activity updated",
        undefined,
        2,
        THREAD_ID,
        "provider",
      );
      return yield* takeRepliesAndOutcome(session.takeEvent);
    });

  it.effect("recovers a wake whose every event was dropped as one continuation run", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* settleWithChildAndHistory(fake);
      fake.setTranscript([
        ...session.history,
        storedChildMessage(2000),
        storedReply(2001, "The child said child-ok"),
      ]);

      const offer = yield* takeRecoveredOffer(session.offers);
      assert.equal(offer.notification?.summary, "Message from worker");
      assert.deepEqual(yield* runContinuation(session, offer), {
        texts: ["The child said child-ok"],
        status: "completed",
      });

      // Later checks read the same history and find nothing new to offer.
      yield* tickUntil(() => false);
      assert.equal(Queue.sizeUnsafe(session.offers), 0);
      // The child only sent a message; nothing says it ended.
      assert.isTrue(yield* session.runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("still names the heartbeat behind a wake whose every event was dropped", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* settleWithChildAndHistory(fake);
      fake.setTranscript([
        ...session.history,
        {
          role: "custom",
          customType: "heartbeat_prompt",
          content: "[heartbeat: every 5m run#3]\n\nCheck the deploy.",
          display: true,
          details: { jobId: "deploy", schedule: "every 5m", runCount: 3 },
          timestamp: 2000,
        },
        storedReply(2001, "The deploy is fine."),
      ]);

      const offer = yield* takeRecoveredOffer(session.offers);
      // Clients read the run as a routine check only through this source.
      assert.deepEqual(offer.notification?.source, { kind: "heartbeat", heartbeatId: "deploy" });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("offers a wake whose reply and end were dropped once the agent is idle", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const session = yield* settleWithChildAndHistory(fake);
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: storedChildMessage(2000) });
      fake.setTranscript([
        ...session.history,
        storedChildMessage(2000),
        storedReply(2001, "The child said child-ok"),
      ]);

      const offer = yield* takeRecoveredOffer(session.offers);
      assert.equal(offer.notification?.summary, "Message from worker");
      assert.deepEqual(yield* runContinuation(session, offer), {
        texts: ["The child said child-ok"],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /** What Prime Agent stores in the parent when a child ends without a reply of its own. */
  const childNotices = [
    {
      name: "a finished child",
      notice: {
        role: "custom",
        customType: "rlm_child_terminal_notice",
        content: "[child-exited: no-reply child:worker]",
        display: true,
        details: {
          kind: "completed_without_reply",
          childId: "sub-1",
          sessionName: "worker",
          lastAssistantTextPreview: "child-ok sent.",
        },
        timestamp: 2000,
      },
      status: "completed",
      result: "child-ok sent.",
    },
    {
      name: "a failed child",
      notice: {
        role: "custom",
        customType: "rlm_child_failure",
        content: "[child-failed child:worker]\n\nkernel died",
        display: true,
        details: { childId: "sub-1", sessionName: "worker", error: "kernel died" },
        timestamp: 2000,
      },
      status: "failed",
      result: "kernel died",
    },
  ] as const;

  it.effect.each(childNotices)(
    "settles $name from the notice its parent stored when the update was dropped",
    ({ notice, status, result }) =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const session = yield* settleWithChildAndHistory(fake);
        const settled = yield* session
          .takeEvent(
            (event) => event.type === "subagent.updated" && event.subagent.status !== "running",
          )
          .pipe(Effect.forkScoped);
        fake.setTranscript([...session.history, notice, storedReply(2001, "Noted.")]);

        yield* tickUntil(() => settled.pollUnsafe() !== undefined);

        const update = yield* Fiber.join(settled);
        assert.isTrue(update.type === "subagent.updated");
        if (update.type !== "subagent.updated") return;
        assert.deepEqual(
          {
            id: update.subagent.id,
            status: update.subagent.status,
            result: update.subagent.result,
          },
          { id: session.child?.id, status, result },
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  /** Runs one turn whose end-of-turn branch lists `userEntries` as its user entries. */
  const runPrimeTurn = (
    session: Effect.Success<ReturnType<typeof openPrimeThread>>,
    fake: FakePi,
    runOrdinal: number,
    userEntries: ReadonlyArray<string>,
  ) =>
    Effect.gen(function* () {
      yield* startTurn(
        session.runtime,
        session.providerThread,
        "default",
        [],
        `turn ${runOrdinal}`,
        undefined,
        runOrdinal,
      );
      yield* fake.takeRequest("prompt");
      fake.queueForkMessages({
        messages: userEntries.map((entryId) => ({ entryId, text: entryId })),
      });
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_end", messages: [] });
      const settled = yield* session.takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
      );
      assert.isTrue(settled.type === "provider_turn.updated");
      return settled.type === "provider_turn.updated" ? settled.providerTurn : undefined;
    });

  const rollBackToFirstTurn = (
    session: Effect.Success<ReturnType<typeof openPrimeThread>>,
    turns: ReadonlyArray<OrchestrationV2ProviderTurn | undefined>,
  ) =>
    session.runtime.rollbackThread({
      providerThread: session.providerThread,
      target: {
        type: "provider_turn",
        checkpointId: CheckpointId.make("checkpoint-1"),
        appRunOrdinal: 1,
        providerTurn: turns[0]!,
      },
      providerThreadTurns: turns.map((turn) => turn!),
    });

  it.effect("rolls back at the first user entry the discarded turn added", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      // ensureThread baselines the branch before the first turn.
      fake.queueForkMessages({ messages: [] });
      const session = yield* openPrimeThread(fake);
      const first = yield* runPrimeTurn(session, fake, 1, ["u1"]);
      const second = yield* runPrimeTurn(session, fake, 2, ["u1", "u2"]);
      assert.equal(first?.nativeTurnRef?.nativeId, "u1");
      assert.equal(second?.nativeTurnRef?.nativeId, "u2");

      // Without T3's extension command, rollback forks a new session file.
      yield* rollBackToFirstTurn(session, [first, second]);
      const fork = fake.allRequests().find((request) => request["type"] === "fork");
      assert.equal(fork?.["entryId"], "u2");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("records a running turn's start entry before a steer adds another", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueForkMessages({ messages: [] });
      const session = yield* openPrimeThread(fake);
      yield* runPrimeTurn(session, fake, 1, ["u1"]);

      yield* startTurn(
        session.runtime,
        session.providerThread,
        "default",
        [],
        "turn 2",
        undefined,
        2,
      );
      yield* fake.takeRequest("prompt");
      fake.queueForkMessages({ messages: [{ entryId: "u1" }, { entryId: "u2" }] });
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "message_start", message: { role: "assistant", content: [] } });
      // A fork from turn 1 cuts at this ref, so it must exist while turn 2 runs.
      const running = yield* session.takeEvent(
        (event) =>
          event.type === "provider_turn.updated" &&
          event.providerTurn.status === "running" &&
          event.providerTurn.nativeTurnRef?.strength === "strong",
      );
      assert.isTrue(running.type === "provider_turn.updated");
      if (running.type === "provider_turn.updated") {
        assert.equal(running.providerTurn.nativeTurnRef?.nativeId, "u2");
      }

      // The steer's user entry is new at the end of the turn, but not its start.
      fake.queueForkMessages({
        messages: [{ entryId: "u1" }, { entryId: "u2" }, { entryId: "u3" }],
      });
      yield* fake.emit({ type: "agent_end", messages: [] });
      const settled = yield* session.takeEvent(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
      );
      assert.isTrue(settled.type === "provider_turn.updated");
      if (settled.type === "provider_turn.updated") {
        assert.equal(settled.providerTurn.nativeTurnRef?.nativeId, "u2");
      }
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  const T3_COMMANDS = { commands: [{ name: "t3-navigate-tree", source: "extension" }] };

  /** Answers the next in-place rollback command the way T3's extension reports it. */
  const reportNavigation = (fake: FakePi, outcome: Record<string, unknown>) =>
    Effect.gen(function* () {
      const prompt = yield* fake.takeRequest("prompt");
      const [command, requestId, entryId] = String(prompt["message"]).split(" ");
      assert.equal(command, "/t3-navigate-tree");
      yield* fake.emit({
        type: "extension_ui_request",
        id: "notify-nav",
        method: "notify",
        message: `t3-navigate-tree-result:${encodeJsonLine({ requestId, ...outcome })}`,
        notifyType: "info",
      });
      return entryId;
    });

  it.effect("rolls back in place through T3's extension command when it is loaded", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueCommands(T3_COMMANDS);
      fake.queueForkMessages({ messages: [] });
      const session = yield* openPrimeThread(fake);
      const first = yield* runPrimeTurn(session, fake, 1, ["u1"]);
      const second = yield* runPrimeTurn(session, fake, 2, ["u1", "u2"]);

      const rollback = yield* rollBackToFirstTurn(session, [first, second]).pipe(Effect.forkChild);
      const navigatedEntry = yield* reportNavigation(fake, { outcome: "ok" });
      const snapshot = yield* Fiber.join(rollback);

      assert.equal(navigatedEntry, "u2");
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "fork"));
      assert.equal(
        snapshot.providerThread.nativeThreadRef?.nativeId,
        session.providerThread.nativeThreadRef?.nativeId,
        "an in-place rollback keeps the same session file",
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("reports an extension veto of the in-place rollback as a failure", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueCommands(T3_COMMANDS);
      fake.queueForkMessages({ messages: [] });
      const session = yield* openPrimeThread(fake);
      const first = yield* runPrimeTurn(session, fake, 1, ["u1"]);
      const second = yield* runPrimeTurn(session, fake, 2, ["u1", "u2"]);

      const rollback = yield* rollBackToFirstTurn(session, [first, second]).pipe(
        Effect.flip,
        Effect.forkChild,
      );
      yield* reportNavigation(fake, { outcome: "cancelled" });
      const error = yield* Fiber.join(rollback);
      assert.include(String(error.cause), "cancelled the rollback");
      assert.isFalse(fake.allRequests().some((request) => request["type"] === "fork"));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  describe("prompt-cache miss notices", () => {
    const MINUTE = 60_000;

    interface CacheTurn {
      readonly timestamp: number;
      readonly input?: number;
      readonly cacheRead?: number;
      readonly cacheWrite?: number;
      readonly output?: number;
      readonly totalTokens?: number;
      readonly model?: string;
      readonly responseId?: string;
      readonly stopReason?: string;
    }

    /** A parent reply as Prime Agent reports its usage, at Anthropic-like prices. */
    const usageReply = (turn: CacheTurn) => {
      const { input = 0, cacheRead = 0, cacheWrite = 0, output = 100 } = turn;
      return {
        ...assistantSnapshot(turn.timestamp, "A reply."),
        stopReason: turn.stopReason ?? "stop",
        provider: "anthropic",
        model: turn.model ?? "claude",
        ...(turn.responseId === undefined ? {} : { responseId: turn.responseId }),
        usage: {
          input,
          output,
          cacheRead,
          cacheWrite,
          totalTokens: turn.totalTokens ?? input + output + cacheRead + cacheWrite,
          cost: {
            input: input * 3e-6,
            cacheRead: cacheRead * 0.3e-6,
            cacheWrite: cacheWrite * 3.75e-6,
          },
        },
      };
    };

    /** A prompt of 50k tokens written to the cache. */
    const warm = usageReply({
      timestamp: 1_000,
      input: 10,
      cacheWrite: 50_000,
      responseId: "warm",
    });
    /** The same prompt re-billed in full ten minutes later. */
    const missed = usageReply({
      timestamp: 1_000 + 10 * MINUTE,
      input: 10,
      cacheWrite: 52_000,
      responseId: "missed",
    });
    const healthy = (timestamp: number, responseId: string) =>
      usageReply({ timestamp, input: 10, cacheRead: 50_000, cacheWrite: 2_000, responseId });
    /** What Prime Agent stores once RLM children's usage is folded into the reply. */
    const folded = (reply: ReturnType<typeof usageReply>) => ({
      ...reply,
      usage: {
        ...reply.usage,
        input: reply.usage.input + 9_000,
        cacheRead: reply.usage.cacheRead + 9_000,
      },
    });

    interface Notice {
      readonly id: string;
      readonly message: string;
    }

    /** The cache-miss notices of the turn that runs until it ends. */
    const takeCacheNotices = (takeEvent: TakeEvent) =>
      Effect.gen(function* () {
        const notices: Array<Notice> = [];
        for (;;) {
          const event = yield* takeEvent(
            (candidate) =>
              candidate.type === "turn.terminal" ||
              (candidate.type === "turn_item.updated" &&
                candidate.turnItem.type === "system_notice" &&
                candidate.turnItem.title === "Prompt cache miss"),
          );
          if (event.type !== "turn_item.updated" || event.turnItem.type !== "system_notice") {
            return notices;
          }
          assert.equal(event.turnItem.tone, "warning");
          notices.push({ id: event.turnItem.id, message: event.turnItem.message });
        }
      });

    /**
     * One turn. `live` are the replies whose `message_end` reached T3, and
     * `stored` is the conversation Prime Agent holds when the turn settles.
     */
    const runCacheTurn = (
      session: Effect.Success<ReturnType<typeof openPrimeThread>>,
      fake: FakePi,
      runOrdinal: number,
      replies: { readonly live?: ReadonlyArray<unknown>; readonly stored: ReadonlyArray<unknown> },
    ) =>
      Effect.gen(function* () {
        yield* startTurn(
          session.runtime,
          session.providerThread,
          "default",
          [],
          `turn ${runOrdinal}`,
          undefined,
          runOrdinal,
        );
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        for (const message of replies.live ?? []) {
          yield* fake.emit({ type: "message_end", message });
        }
        fake.setTranscript(replies.stored);
        yield* fake.emit({ type: "agent_end", messages: [] });
        return yield* takeCacheNotices(session.takeEvent);
      });

    it.effect("warns once when a live reply re-bills the prompt after an idle gap", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const session = yield* openPrimeThread(fake);
        // Prime Agent has stored nothing yet, so only the live events speak.
        assert.deepEqual(yield* runCacheTurn(session, fake, 1, { live: [warm], stored: [] }), []);
        const notices = yield* runCacheTurn(session, fake, 2, { live: [missed], stored: [] });
        assert.deepEqual(
          notices.map((notice) => notice.message),
          ["Cache miss after 10m idle: 50k tokens re-billed (~$0.19)"],
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("names a model switch instead of an idle gap", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const session = yield* openPrimeThread(fake);
        yield* runCacheTurn(session, fake, 1, { live: [warm], stored: [] });
        const switched = usageReply({
          timestamp: 2_000,
          input: 52_010,
          model: "other",
          responseId: "s",
        });
        const notices = yield* runCacheTurn(session, fake, 2, { live: [switched], stored: [] });
        assert.lengthOf(notices, 1);
        assert.include(notices[0]?.message, "Cache miss after model switch: 50k tokens re-billed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "stays quiet for a healthy session, a noise-sized miss, and an errored reply without usage",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakePi;
          const session = yield* openPrimeThread(fake);
          const noisy = usageReply({
            timestamp: 3_000,
            cacheRead: 49_500,
            cacheWrite: 700,
            responseId: "noise",
          });
          const errored = usageReply({
            timestamp: 4_000,
            output: 0,
            stopReason: "error",
            responseId: "err",
          });
          const all = [warm, healthy(2_000, "h"), noisy, errored];
          const notices = yield* runCacheTurn(session, fake, 1, { live: all, stored: all });
          assert.deepEqual(notices, []);

          // The errored reply left the baseline alone, so a real miss after it still shows.
          const later = usageReply({
            timestamp: 5_000 + 10 * MINUTE,
            input: 10,
            cacheWrite: 52_000,
            responseId: "later",
          });
          const next = yield* runCacheTurn(session, fake, 2, {
            live: [later],
            stored: [...all, later],
          });
          assert.lengthOf(next, 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("compares the first reply after a reopen with the session's last stored reply", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        fake.setTranscript([storedPrompt(900), warm]);
        const session = yield* openPrimeThread(fake);
        const notices = yield* runCacheTurn(session, fake, 1, {
          live: [missed],
          stored: [storedPrompt(900), warm, missed],
        });
        assert.lengthOf(notices, 1);
        assert.include(notices[0]?.message, "Cache miss after 10m idle");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("does not report misses already in the history of a reopened session", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const history = [warm, missed, healthy(1_000 + 11 * MINUTE, "h")];
        fake.setTranscript(history);
        const session = yield* openPrimeThread(fake);
        const next = healthy(1_000 + 12 * MINUTE, "next");
        const notices = yield* runCacheTurn(session, fake, 1, {
          live: [next],
          stored: [...history, next],
        });
        assert.deepEqual(notices, []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("starts without a baseline after a stored history that ends in a folded reply", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const childRun = folded(healthy(2_000, "child-run"));
        const history = [warm, { ...childRun, usage: { ...childRun.usage, totalTokens: 52_200 } }];
        fake.setTranscript(history);
        const session = yield* openPrimeThread(fake);
        const notices = yield* runCacheTurn(session, fake, 1, {
          live: [missed],
          stored: [...history, missed],
        });
        assert.deepEqual(notices, []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect(
      "counts a reply once when its live event and its stored, folded copy both arrive",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakePi;
          const session = yield* openPrimeThread(fake);
          const storedWarm = {
            ...folded(warm),
            usage: { ...folded(warm).usage, totalTokens: 50_110 },
          };
          assert.deepEqual(
            yield* runCacheTurn(session, fake, 1, { live: [warm], stored: [storedWarm] }),
            [],
          );
          const storedMissed = {
            ...folded(missed),
            usage: { ...folded(missed).usage, totalTokens: 52_110 },
          };
          const notices = yield* runCacheTurn(session, fake, 2, {
            live: [missed],
            stored: [storedWarm, storedMissed],
          });
          assert.lengthOf(
            notices,
            1,
            "the stored copies neither reset the baseline nor warn again",
          );
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("takes a reply whose events were all dropped from the stored conversation once", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const session = yield* openPrimeThread(fake);
        yield* runCacheTurn(session, fake, 1, { live: [warm], stored: [warm] });
        const notices = yield* runCacheTurn(session, fake, 2, { stored: [warm, missed] });
        assert.lengthOf(notices, 1);
        const next = healthy(1_000 + 11 * MINUTE, "next");
        const later = yield* runCacheTurn(session, fake, 3, {
          live: [next],
          stored: [warm, missed, next],
        });
        assert.deepEqual(later, []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("counts distinct replies that share a timestamp, each with its own notice", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const session = yield* openPrimeThread(fake);
        yield* runCacheTurn(session, fake, 1, { live: [warm], stored: [warm] });
        const at = 1_000 + 10 * MINUTE;
        const first = usageReply({ timestamp: at, input: 10, cacheWrite: 52_000, responseId: "a" });
        const second = usageReply({
          timestamp: at,
          input: 10,
          cacheWrite: 60_000,
          model: "other",
          responseId: "b",
        });
        const notices = yield* runCacheTurn(session, fake, 2, {
          live: [first, second],
          stored: [warm, first, second],
        });
        assert.lengthOf(notices, 2);
        assert.notEqual(notices[0]?.id, notices[1]?.id);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect("does not warn about the first reply after a compaction", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const session = yield* openPrimeThread(fake);
        yield* runCacheTurn(session, fake, 1, { live: [warm], stored: [warm] });
        yield* startTurn(
          session.runtime,
          session.providerThread,
          "default",
          [],
          "turn 2",
          undefined,
          2,
        );
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "compaction_start", reason: "threshold" });
        yield* fake.emit({
          type: "compaction_end",
          result: { summary: "s", tokensBefore: 50_000, estimatedTokensAfter: 30_000 },
        });
        const afterCompaction = usageReply({
          timestamp: 1_000 + 10 * MINUTE,
          input: 10,
          cacheWrite: 30_000,
          responseId: "c",
        });
        yield* fake.emit({ type: "message_end", message: afterCompaction });
        fake.setTranscript([warm, afterCompaction]);
        yield* fake.emit({ type: "agent_end", messages: [] });
        assert.deepEqual(yield* takeCacheNotices(session.takeEvent), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    it.effect.each(["forking the session", "moving the branch in place"] as const)(
      "does not warn about the first reply after a rollback by %s",
      (rollback) =>
        Effect.gen(function* () {
          const fake = yield* makeFakePi;
          if (rollback === "moving the branch in place") fake.queueCommands(T3_COMMANDS);
          fake.queueForkMessages({ messages: [] });
          const session = yield* openPrimeThread(fake);
          const turns = [];
          for (const [ordinal, reply] of [warm, healthy(2_000, "h")].entries()) {
            fake.queueForkMessages({
              messages: Array.from({ length: ordinal + 1 }, (_, index) => ({
                entryId: `u${index + 1}`,
              })),
            });
            yield* startTurn(
              session.runtime,
              session.providerThread,
              "default",
              [],
              `turn ${ordinal + 1}`,
              undefined,
              ordinal + 1,
            );
            yield* fake.takeRequest("prompt");
            yield* fake.emit({ type: "agent_start" });
            yield* fake.emit({ type: "message_end", message: reply });
            yield* fake.emit({ type: "agent_end", messages: [] });
            // Both the completed provider turn and the terminal event, so neither is left to a later wait.
            let providerTurn: OrchestrationV2ProviderTurn | undefined;
            let terminal = false;
            while (providerTurn === undefined || !terminal) {
              const event = yield* session.takeEvent(
                (candidate) =>
                  candidate.type === "turn.terminal" ||
                  (candidate.type === "provider_turn.updated" &&
                    candidate.providerTurn.status === "completed"),
              );
              if (event.type === "turn.terminal") terminal = true;
              if (event.type === "provider_turn.updated") providerTurn = event.providerTurn;
            }
            turns.push(providerTurn);
          }
          if (rollback === "forking the session") {
            yield* rollBackToFirstTurn(session, turns);
          } else {
            const rolledBack = yield* rollBackToFirstTurn(session, turns).pipe(Effect.forkChild);
            yield* reportNavigation(fake, { outcome: "ok" });
            yield* Fiber.join(rolledBack);
          }

          const afterRollback = usageReply({
            timestamp: 1_000 + 10 * MINUTE,
            input: 10,
            cacheWrite: 52_000,
            responseId: "r",
          });
          const notices = yield* runCacheTurn(session, fake, 3, {
            live: [afterRollback],
            stored: [warm, afterRollback],
          });
          assert.deepEqual(notices, []);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    describe("replies that share a timestamp with an earlier stored reply", () => {
      /** Same timestamp as `warm`, another response, and a different model, so it re-bills the prompt. */
      const sibling = usageReply({
        timestamp: 1_000,
        input: 52_010,
        model: "other",
        responseId: "sibling",
      });

      it.effect(
        "warns once for a live reply after a reopen, despite its twin and a later catch-up",
        () =>
          Effect.gen(function* () {
            const fake = yield* makeFakePi;
            fake.setTranscript([storedPrompt(900), warm]);
            const session = yield* openPrimeThread(fake);
            const notices = yield* runCacheTurn(session, fake, 1, {
              // The old reply is delivered again, then the new one.
              live: [warm, sibling],
              stored: [storedPrompt(900), warm, sibling],
            });
            assert.lengthOf(notices, 1);
            assert.include(
              notices[0]?.message,
              "Cache miss after model switch: 50k tokens re-billed",
            );

            const quiet = yield* runCacheTurn(session, fake, 2, {
              stored: [storedPrompt(900), warm, sibling],
            });
            assert.deepEqual(quiet, []);
          }).pipe(Effect.scoped, Effect.provide(layerTest)),
      );

      it.effect("warns for a sibling in a later turn, once A's turn has closed", () =>
        Effect.gen(function* () {
          const fake = yield* makeFakePi;
          const session = yield* openPrimeThread(fake);
          yield* runCacheTurn(session, fake, 1, { live: [warm], stored: [warm] });
          const live = yield* runCacheTurn(session, fake, 2, {
            live: [sibling],
            stored: [warm, sibling],
          });
          assert.lengthOf(live, 1);
          assert.include(live[0]?.message, "Cache miss after model switch");
          assert.deepEqual(yield* runCacheTurn(session, fake, 3, { stored: [warm, sibling] }), []);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
      );

      it.effect("warns for a sibling that only a later turn's stored read shows", () =>
        Effect.gen(function* () {
          const fake = yield* makeFakePi;
          const session = yield* openPrimeThread(fake);
          yield* runCacheTurn(session, fake, 1, { live: [warm], stored: [warm] });
          const notices = yield* runCacheTurn(session, fake, 2, { stored: [warm, sibling] });
          assert.lengthOf(notices, 1);
          assert.deepEqual(yield* runCacheTurn(session, fake, 3, { stored: [warm, sibling] }), []);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
      );

      it.effect("warns for a reply that only the stored conversation shows", () =>
        Effect.gen(function* () {
          const fake = yield* makeFakePi;
          fake.setTranscript([storedPrompt(900), warm]);
          const session = yield* openPrimeThread(fake);
          const notices = yield* runCacheTurn(session, fake, 1, {
            stored: [storedPrompt(900), warm, sibling],
          });
          assert.lengthOf(notices, 1);
          assert.include(notices[0]?.message, "Cache miss after model switch");
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
      );

      it.effect("stays quiet when only the reopened session's own reply arrives again", () =>
        Effect.gen(function* () {
          const fake = yield* makeFakePi;
          fake.setTranscript([storedPrompt(900), warm]);
          const session = yield* openPrimeThread(fake);
          const notices = yield* runCacheTurn(session, fake, 1, {
            live: [warm],
            stored: [storedPrompt(900), warm],
          });
          assert.deepEqual(notices, []);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
      );
    });

    it.effect(
      "keeps a stored compaction summary from resetting the baseline on every catch-up",
      () =>
        Effect.gen(function* () {
          const fake = yield* makeFakePi;
          const summary = { role: "compactionSummary", summary: "s", timestamp: 1_500 };
          const afterCompaction = usageReply({
            timestamp: 2_000,
            input: 10,
            cacheWrite: 30_000,
            responseId: "post",
          });
          const history = [summary, warm, afterCompaction];
          fake.setTranscript(history);
          const session = yield* openPrimeThread(fake);
          const next = usageReply({
            timestamp: 3_000,
            input: 10,
            cacheRead: 30_000,
            cacheWrite: 1_000,
            responseId: "next",
          });
          assert.deepEqual(
            yield* runCacheTurn(session, fake, 1, { live: [next], stored: [...history, next] }),
            [],
          );
          // The settle read listed the summary again; it must not have cleared what `next` set.
          const later = usageReply({
            timestamp: 3_000 + 10 * MINUTE,
            input: 10,
            cacheWrite: 32_000,
            responseId: "later",
          });
          const notices = yield* runCacheTurn(session, fake, 2, {
            live: [later],
            stored: [...history, next, later],
          });
          assert.lengthOf(notices, 1);
        }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );

    describe.each(["compactionSummary", "branchSummary"] as const)(
      "a first-seen %s with the timestamp of a counted reply",
      (role) => {
        const summary = { role, summary: "s", timestamp: 1_000 };
        const afterSummary = usageReply({
          timestamp: 1_000,
          input: 10,
          cacheWrite: 30_000,
          responseId: "post",
        });

        it.effect("resets the baseline, so the next cold request is not a miss", () =>
          Effect.gen(function* () {
            const fake = yield* makeFakePi;
            const session = yield* openPrimeThread(fake);
            assert.deepEqual(
              yield* runCacheTurn(session, fake, 1, { live: [warm], stored: [warm, summary] }),
              [],
            );
            const cold = usageReply({
              timestamp: 1_000 + 10 * MINUTE,
              input: 10,
              cacheWrite: 52_000,
              responseId: "cold",
            });
            const notices = yield* runCacheTurn(session, fake, 2, {
              live: [cold],
              stored: [warm, summary, cold],
            });
            assert.deepEqual(notices, []);
          }).pipe(Effect.scoped, Effect.provide(layerTest)),
        );

        it.effect("is not counted again when read later, so a real miss still warns", () =>
          Effect.gen(function* () {
            const fake = yield* makeFakePi;
            const session = yield* openPrimeThread(fake);
            yield* runCacheTurn(session, fake, 1, { live: [warm], stored: [warm, summary] });
            yield* runCacheTurn(session, fake, 2, {
              live: [afterSummary],
              stored: [warm, summary, afterSummary],
            });
            const miss = usageReply({
              timestamp: 1_000 + 10 * MINUTE,
              input: 10,
              cacheWrite: 32_000,
              responseId: "miss",
            });
            const notices = yield* runCacheTurn(session, fake, 3, {
              live: [miss],
              stored: [warm, summary, afterSummary, miss],
            });
            assert.lengthOf(notices, 1);
          }).pipe(Effect.scoped, Effect.provide(layerTest)),
        );
      },
    );

    it.effect("shows nothing for plain Pi", () =>
      Effect.gen(function* () {
        const fake = yield* makeFakePi;
        const { runtime, takeEvent } = yield* openRuntime(fake);
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        });
        yield* startTurn(runtime, providerThread);
        yield* fake.takeRequest("prompt");
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({ type: "message_end", message: warm });
        yield* fake.emit({ type: "message_end", message: missed });
        yield* fake.emit({ type: "agent_end", messages: [] });
        yield* fake.emit({ type: "agent_settled" });
        assert.deepEqual(yield* takeCacheNotices(takeEvent), []);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
    );
  });
});

describe("PiAdapterV2 subagent capabilities", () => {
  it.effect("exposes subagent thread ids only where children get their own thread", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      for (const flavor of [PI_FLAVOR, PRIME_AGENT_FLAVOR]) {
        const adapter = yield* makeAdapter(fake, "", undefined, flavor);
        const capabilities = yield* adapter.getCapabilities();
        assert.equal(capabilities.subagents.exposesSubagentThreadIds, flavor.childThreads);
      }
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("has the session store child threads only where children get their own thread", () =>
    Effect.gen(function* () {
      for (const flavor of [PI_FLAVOR, PRIME_AGENT_FLAVOR]) {
        const fake = yield* makeFakePi;
        const { runtime } = yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          flavor,
        );
        assert.equal(runtime.childThreadScope, flavor.childThreads ? "session" : undefined);
      }
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

describe("PiAdapterV2 reaching T3 through the kernel's MCP client", () => {
  const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
  const ENDPOINT = "http://127.0.0.1:43123/mcp";

  /** A Prime Agent config dir whose settings file declares `mcpServers`, or has no file. */
  const makeAgentDir = Effect.fnUntraced(function* (mcpServers?: Record<string, unknown>) {
    const fs = yield* FileSystem.FileSystem;
    const agentDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-prime-agent-dir-" });
    if (mcpServers !== undefined) {
      yield* fs.writeFileString(`${agentDir}/settings.json`, encodeJson({ mcpServers }));
    }
    return agentDir;
  });

  const declaredEntry = {
    "t3-code": { type: "http", url: ENDPOINT, bearerTokenEnvVar: "T3_MCP_BEARER_TOKEN" },
  };

  const withMcpSession = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.sync(() =>
      McpProviderSession.setMcpProviderSession({
        environmentId: EnvironmentId.make("environment-pi-mcp"),
        threadId: THREAD_ID,
        providerSessionId: "mcp-session-prime",
        providerInstanceId: PI_INSTANCE_ID,
        endpoint: ENDPOINT,
        authorizationHeader: "Bearer secret-prime-token",
        browserToolsAvailable: true,
      }),
    ).pipe(
      Effect.andThen(effect),
      Effect.ensuring(Effect.sync(() => McpProviderSession.clearMcpProviderSession(THREAD_ID))),
    );

  const openPrime = (fake: FakePi, agentDir: string) =>
    openRuntime(fake, "default", THREAD_ID, SESSION_ID, undefined, PRIME_AGENT_FLAVOR, undefined, {
      PRIME_AGENT_CODING_AGENT_DIR: agentDir,
    });

  const t3SetupNotices = (events: ReadonlyArray<ProviderAdapterV2Event>) =>
    events.flatMap((event) =>
      event.type === "turn_item.updated" &&
      event.turnItem.type === "system_notice" &&
      event.turnItem.title === "T3 Code MCP setup"
        ? [event.turnItem]
        : [],
    );

  it.effect("loads the t3-code skill and no native T3 tools once the server is declared", () =>
    withMcpSession(
      Effect.gen(function* () {
        const agentDir = yield* makeAgentDir(declaredEntry);
        const fake = yield* makeFakePi;
        yield* openPrime(fake, agentDir);
        const { args, env } = fake.lastSpawn();
        const skillIndex = args.indexOf("--skill");
        assert.isAbove(skillIndex, -1);
        assert.match(args[skillIndex + 1] ?? "", /\/pi-t3-skills\/t3-code$/);
        assert.equal(env.T3_PI_MCP_TOOLS, "kernel");
        assert.equal(env.T3_MCP_BEARER_TOKEN, "secret-prime-token");
      }),
    ).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "registers no native tools and says once what to add when the server is not declared",
    () =>
      withMcpSession(
        Effect.gen(function* () {
          const agentDir = yield* makeAgentDir();
          const fake = yield* makeFakePi;
          const { runtime, takeEvent } = yield* openPrime(fake, agentDir);
          const { args, env } = fake.lastSpawn();
          assert.notInclude(args, "--skill");
          assert.equal(env.T3_PI_MCP_TOOLS, "kernel");
          // The permission hook lives in the extension, so Supervised stays enforced.
          assert.include(args, "--extension");

          const providerThread = yield* runtime.ensureThread({
            threadId: THREAD_ID,
            modelSelection: modelSelection("default"),
            runtimePolicy,
          });
          const finishTurn = Effect.fnUntraced(function* (ordinal: number) {
            yield* startTurn(runtime, providerThread, "default", [], "Hello", undefined, ordinal);
            yield* fake.takeRequest("prompt");
            yield* fake.emit({ type: "agent_start" });
            yield* fake.emit({ type: "agent_end", messages: [] });
            yield* fake.takeRequest("get_state");
            const seen: Array<ProviderAdapterV2Event> = [];
            yield* takeEvent((event) => {
              seen.push(event);
              return event.type === "turn.terminal";
            });
            return t3SetupNotices(seen);
          });

          const [hint, ...extra] = yield* finishTurn(1);
          assert.lengthOf(extra, 0);
          assert.equal(hint?.type === "system_notice" ? hint.tone : undefined, "warning");
          const message = hint?.type === "system_notice" ? hint.message : "";
          assert.include(message, `${agentDir}/settings.json`);
          assert.include(
            message,
            encodeJson({
              "t3-code": { type: "http", url: ENDPOINT, bearerTokenEnvVar: "T3_MCP_BEARER_TOKEN" },
            }),
          );
          assert.lengthOf(yield* finishTurn(2), 0);
        }),
      ).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("treats a server declared for another port as undeclared", () =>
    withMcpSession(
      Effect.gen(function* () {
        const agentDir = yield* makeAgentDir({
          "t3-code": { ...declaredEntry["t3-code"], url: "http://127.0.0.1:3773/mcp" },
        });
        const fake = yield* makeFakePi;
        yield* openPrime(fake, agentDir);
        const { args, env } = fake.lastSpawn();
        assert.notInclude(args, "--skill");
        assert.equal(env.T3_PI_MCP_TOOLS, "kernel");
      }),
    ).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("never uses the skill for plain Pi, even with a declared server", () =>
    withMcpSession(
      Effect.gen(function* () {
        const agentDir = yield* makeAgentDir(declaredEntry);
        const fake = yield* makeFakePi;
        yield* openRuntime(
          fake,
          "default",
          THREAD_ID,
          SESSION_ID,
          undefined,
          PI_FLAVOR,
          undefined,
          {
            PI_CODING_AGENT_DIR: agentDir,
            PRIME_AGENT_CODING_AGENT_DIR: agentDir,
          },
        );
        const { args, env } = fake.lastSpawn();
        assert.notInclude(args, "--skill");
        assert.notProperty(env, "T3_PI_MCP_TOOLS");
      }),
    ).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
