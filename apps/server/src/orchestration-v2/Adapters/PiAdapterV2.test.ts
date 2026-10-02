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
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import * as ServerConfig from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import * as IdAllocator from "../IdAllocator.ts";
import {
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2SessionRuntime,
} from "../ProviderAdapter.ts";
import { handoffBudget } from "../ContextHandoffBudget.ts";
import type { ProviderContinuationRequest } from "../ProviderContinuationRequests.ts";
import { makePiAdapterV2, PI_PROVIDER } from "./PiAdapterV2.ts";
import { PI_FLAVOR, PRIME_AGENT_FLAVOR, type PiFlavor } from "./PiFlavor.ts";
import { makePiRpcConnection, type PiRpcRecord } from "./PiRpc.ts";

const serverConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-pi-v2-adapter-",
}).pipe(Layer.provide(NodeServices.layer));

const testLayer = Layer.mergeAll(NodeServices.layer, IdAllocator.layer, serverConfigLayer);

const decodeJsonLine = Schema.decodeSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const PI_INSTANCE_ID = ProviderInstanceId.make("pi");
const THREAD_ID = ThreadId.make("thread-pi-test");
const SESSION_ID = ProviderSessionId.make("provider-session-pi-test");
const FAKE_SESSION_FILE = "/fake/.pi/agent/sessions/--workspace--/0001_abc.jsonl";
/** Deliberately outside the valid pid range so a group-kill can never land. */
const FAKE_PID = 999_999_999;

const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
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
  readonly emit: (record: PiRpcRecord) => Effect.Effect<void>;
  readonly takeRequest: (type: string) => Effect.Effect<PiRpcRecord>;
  /** Data returned by the next `get_entries` acks, consumed in order. */
  readonly queueEntries: (data: unknown) => void;
  /** Data returned by the next active-branch `get_messages` acks. */
  readonly queueMessages: (data: unknown) => void;
  /** Data returned by the next `get_fork_messages` acks, consumed in order. */
  readonly queueForkMessages: (data: unknown) => void;
  /** Make the next `switch_session` ack report an extension veto. */
  readonly vetoNextSwitch: () => void;
  /** Fields overriding the recorded idle state in the next `get_state` acks, in order. */
  readonly queueState: (data: Record<string, unknown>) => void;
  /** Hold the next `get_state` response until the test resolves it. */
  readonly deferNextState: () => void;
  /** Resolve the held `get_state` request. */
  readonly resolveDeferredState: (data: unknown) => Effect.Effect<void>;
  /** Reject the next `get_state` request. */
  readonly failNextState: () => void;
  readonly deferNextLifecycle: (type: "switch_session" | "new_session") => void;
  readonly queueModels: (models: ReadonlyArray<unknown>) => void;
  readonly vetoNextNewSession: () => void;
  /** Every request received by the fake process. */
  readonly allRequests: () => ReadonlyArray<PiRpcRecord>;
  /** Data returned by the next `get_session_stats` acks, consumed in order. */
  readonly queueStats: (data: unknown) => void;
  /** Data returned by the next `get_commands` acks, consumed in order. */
  readonly queueCommands: (data: unknown) => void;
  /** Make the next `get_commands` ack fail. */
  readonly failNextCommands: () => void;
  /** Close the fake process stdout stream. */
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

/**
 * In-process fake `pi --mode rpc` for races and failures a live Pi cannot
 * produce on demand: captures every stdin record, auto-acks requests, and lets
 * tests push protocol events to stdout. Behaviour a real Pi can show belongs
 * in a replay fixture instead (see PiAdapterV2.testkit.ts).
 */
const makeFakePi: Effect.Effect<FakePi> = Effect.gen(function* () {
  const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const requests = yield* Queue.unbounded<PiRpcRecord>();
  const entriesQueue: Array<unknown> = [];
  const messagesQueue: Array<unknown> = [];
  const forkMessagesQueue: Array<unknown> = [];
  const stateQueue: Array<Record<string, unknown>> = [];
  const statsQueue: Array<unknown> = [];
  const commandsQueue: Array<{ readonly success: boolean; readonly data?: unknown }> = [];
  const allRequests: Array<PiRpcRecord> = [];
  let deferState = false;
  let deferredStateRequest: PiRpcRecord | undefined;
  let failState = false;
  let vetoSwitch = false;
  let vetoNewSession = false;
  let deferredLifecycle: string | undefined;
  let sessionFile = FAKE_SESSION_FILE;
  let sessionGeneration = 0;
  let models: ReadonlyArray<unknown> = [];
  let stdinBuffer = "";

  const emit = (record: PiRpcRecord) =>
    Queue.offer(stdout, new TextEncoder().encode(`${encodeJsonLine(record)}\n`)).pipe(
      Effect.asVoid,
    );

  const respondTo = (record: PiRpcRecord): PiRpcRecord | null => {
    if (typeof record["id"] !== "string") return null;
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
        // Queued data overrides fields of the recorded idle state, so a test
        // that only cares about the session file still gets a real shape.
        return { ...base, data: { ...recordedIdleState(sessionFile), ...stateQueue.shift() } };
      case "get_available_models":
        return { ...base, data: { models } };
      case "new_session": {
        const cancelled = vetoNewSession;
        vetoNewSession = false;
        if (!cancelled) sessionFile = `/fake/new-${++sessionGeneration}.jsonl`;
        return { ...base, data: { cancelled } };
      }
      case "switch_session": {
        const cancelled = vetoSwitch;
        vetoSwitch = false;
        return { ...base, data: { cancelled } };
      }
      case "get_entries":
        return { ...base, data: entriesQueue.shift() ?? { entries: [], leafId: null } };
      case "get_messages":
        return { ...base, data: messagesQueue.shift() ?? { messages: [] } };
      case "get_fork_messages":
        return { ...base, data: forkMessagesQueue.shift() ?? { messages: [] } };
      case "get_session_stats":
        return { ...base, data: statsQueue.shift() ?? {} };
      case "get_commands":
        return { ...base, ...(commandsQueue.shift() ?? { data: { commands: [] } }) };
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
    queueState: (data) => stateQueue.push(data),
    queueStats: (data) => statsQueue.push(data),
    queueCommands: (data) => commandsQueue.push({ success: true, data }),
    failNextCommands: () => commandsQueue.push({ success: false }),
    closeStdout: Queue.end(stdout),
    lastSpawn: () => lastSpawn,
  } satisfies FakePi;
});

const makeAdapter = Effect.fnUntraced(function* (
  fake: FakePi,
  launchArgs = "",
  forkFake?: FakePi,
  flavor: PiFlavor = PI_FLAVOR,
  continuationRequests?: Parameters<typeof makePiAdapterV2>[0]["continuationRequests"],
) {
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  return makePiAdapterV2({
    flavor,
    ...(continuationRequests === undefined ? {} : { continuationRequests }),
    instanceId: PI_INSTANCE_ID,
    settings: { enabled: true, binaryPath: "pi", launchArgs, customModels: [] },
    environment: {},
    spawner:
      forkFake === undefined
        ? fake.spawner
        : ChildProcessSpawner.make((command) =>
            ChildProcess.isStandardCommand(command) && command.args.includes("--fork")
              ? forkFake.spawner.spawn(command)
              : fake.spawner.spawn(command),
          ),
    fileSystem,
    idAllocator,
    serverConfig,
  });
});

const openRuntime = Effect.fnUntraced(function* (
  fake: FakePi,
  model = "default",
  threadId = THREAD_ID,
  providerSessionId = SESSION_ID,
  forkFake?: FakePi,
  flavor: PiFlavor = PI_FLAVOR,
  continuationRequests?: Parameters<typeof makePiAdapterV2>[0]["continuationRequests"],
) {
  const adapter = yield* makeAdapter(fake, "", forkFake, flavor, continuationRequests);
  const runtime = yield* adapter.openSession({
    threadId,
    providerSessionId,
    modelSelection: modelSelection(model),
    runtimePolicy,
  });
  const emitted = yield* Queue.unbounded<ProviderAdapterV2Event>();
  yield* runtime.events.pipe(
    Stream.runForEach((event) => Queue.offer(emitted, event)),
    Effect.forkScoped,
  );
  const takeEvent = (predicate: (event: ProviderAdapterV2Event) => boolean) =>
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
  runtime: ProviderAdapterV2SessionRuntime,
  providerThread: OrchestrationV2ProviderThread,
  model = "default",
  attachments: ReadonlyArray<ChatAttachment> = [],
  text = "Hello pi",
  selection?: ModelSelection,
  runOrdinal = 1,
  threadId = THREAD_ID,
  creationSource: "web" | "provider" = "web",
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
      createdBy: creationSource === "provider" ? "agent" : "user",
      creationSource,
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
  }).pipe(Effect.scoped, Effect.provide(testLayer));

describe("PiAdapterV2", () => {
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("injects the T3 MCP extension and bearer when a session exists", () =>
    Effect.gen(function* () {
      McpProviderSession.setMcpProviderSession({
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
    }).pipe(
      Effect.ensuring(Effect.sync(() => McpProviderSession.clearMcpProviderSession(THREAD_ID))),
      Effect.scoped,
      Effect.provide(testLayer),
    ),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      // New native sessions have their own default, even within one process.
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("adopts the run's provider thread identity instead of minting a second row", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const now = yield* DateTime.now;
      // The placeholder row the orchestrator creates for a first run: no
      // native identity yet. The adapter must bind the pi session to this
      // row instead of registering a second session-file-keyed row, or the
      // projection ends up with two live rows per app thread.
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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

      // An explicit effort on a concrete model.
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

      // Back to Pi default with no explicit thinking choice of its own.
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("expands a selected $ skill through Pi's native skill command", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueCommands({
        commands: [
          {
            name: "skill:repo-review",
            description: "Review this repository.",
            source: "skill",
            sourceInfo: {
              path: "/workspace/.agents/skills/repo-review/SKILL.md",
              scope: "project",
            },
          },
        ],
      });
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
      assert.equal(prompt["message"], "/skill:repo-review Review this change please");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("expands every selected $ skill through Pi native skill commands", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      fake.queueCommands({
        commands: [
          {
            name: "skill:repo-review",
            source: "skill",
            sourceInfo: {
              path: "/workspace/.agents/skills/repo-review/SKILL.md",
              scope: "project",
            },
          },
          {
            name: "skill:deploy",
            source: "skill",
            sourceInfo: {
              path: "/workspace/.agents/skills/deploy/SKILL.md",
              scope: "project",
            },
          },
        ],
      });
      const { runtime } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection: modelSelection("default"),
        runtimePolicy,
      });

      yield* startTurn(runtime, providerThread, "default", [], "use $repo-review and $deploy");
      const prompt = yield* fake.takeRequest("prompt");
      assert.equal(prompt["message"], "/skill:repo-review /skill:deploy use  and");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      // ProviderTurnStartService adopts the fork into its pending row.
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      // A pure extension command: dialog + notify, then the deferred ack —
      // pi emits no agent_start/agent_settled at all.
      yield* fake.emit({
        type: "extension_ui_request",
        id: "ui-cmd",
        method: "notify",
        message: "done",
        notifyType: "info",
      });
      yield* fake.emit({ type: "response", command: "prompt", success: true });
      // The adapter probes get_state (auto-acked idle by the fake), then
      // settles the turn as completed.
      const terminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("persists current xAI capacity text for the thread error banner", () =>
    expectModelFailure("The model is currently at capacity due to high demand."),
  );

  it.effect("persists extension-normalized xAI capacity text for the thread error banner", () =>
    expectModelFailure(
      "Provider overloaded: The model is currently at capacity due to high demand.",
    ),
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
      // Project-trust style prompt before any turn exists.
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      // Project-trust style prompt before any turn exists.
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("rejects a nonpersistent session UUID instead of treating it as a resumable path", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime } = yield* openRuntime(fake);
      // A --no-session Pi keeps its session in memory and reports no file.
      fake.queueState({ sessionFile: undefined });
      const result = yield* runtime
        .ensureThread({
          threadId: THREAD_ID,
          modelSelection: modelSelection("default"),
          runtimePolicy,
        })
        .pipe(Effect.result);
      assert.equal(result._tag, "Failure");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      // An extension command can hold its prompt ack open past settlement.
      yield* startTurn(runtime, providerThread, "default", [], "/my-command");
      yield* fake.takeRequest("prompt");
      yield* fake.emit({ type: "agent_start" });
      yield* fake.emit({ type: "agent_settled" });
      const firstTerminal = yield* takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(firstTerminal.type === "turn.terminal" && firstTerminal.status === "completed");

      yield* startTurn(runtime, providerThread, "default", [], "Second turn", undefined, 2);
      yield* fake.takeRequest("prompt");
      // The rejection answers the first turn's prompt. It must not consume or
      // fail the second turn's prompt acknowledgement.
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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

      // Extension ctx.compact() waits for this first settlement, then starts
      // compaction in a detached continuation.
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});

describe("PiRpc framing", () => {
  it.effect("reassembles records across chunk boundaries and strips CR", () =>
    Effect.gen(function* () {
      const stdout = yield* Queue.unbounded<Uint8Array>();
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(FAKE_PID),
            exitCode: Effect.never,
            isRunning: Effect.succeed(true),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.drain,
            stdout: Stream.fromQueue(stdout),
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

      const push = (text: string) =>
        Queue.offer(stdout, new TextEncoder().encode(text)).pipe(Effect.asVoid);
      yield* push('{"type":"agent_');
      yield* push('start"}\r\n{"type":"agent_settled"}\nnot json\n{"type":"queue_update"}\n');

      yield* push("x".repeat(8 * 1024 * 1024));
      yield* push('x{"type":"must_not_emit"}\n{"type":"after_oversized"}\n');

      const first = yield* Queue.take(connection.events);
      assert.equal(first["type"], "agent_start");
      const second = yield* Queue.take(connection.events);
      assert.equal(second["type"], "agent_settled");
      const third = yield* Queue.take(connection.events);
      assert.equal(third["type"], "queue_update");
      assert.equal((yield* Queue.take(connection.events))["type"], "after_oversized");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

// This fails before a provider transcript exists, so a replay fixture is not
// an honest fit. The boundary is the stdio transport seeing stdout end.
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  const finalReply = {
    type: "turn_end",
    message: { role: "assistant", content: [{ type: "text", text: "done" }], stopReason: "stop" },
  } as const;

  const systemNotices = (events: ReadonlyArray<ProviderAdapterV2Event>) =>
    events.flatMap((event) =>
      event.type === "turn_item.updated" && event.turnItem.type === "system_notice"
        ? [{ message: event.turnItem.message, status: event.turnItem.status }]
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
          running.turnItem.message === "Finishing up…",
      );

      yield* fake.emit({ type: "agent_end", messages: [] });
      const seen: Array<ProviderAdapterV2Event> = [];
      yield* takeEvent((event) => {
        seen.push(event);
        return event.type === "turn.terminal";
      });
      assert.deepStrictEqual(systemNotices(seen), [
        { message: "Finished up", status: "completed" },
      ]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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

  for (const { name, end } of endings) {
    it.effect(`completes a shown Finishing up row once on ${name}`, () =>
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
            completed.turnItem.id === running.turnItem.id &&
            completed.turnItem.ordinal === running.turnItem.ordinal,
        );
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
    );
  }

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
        { message: "Refined its harness: Create a memory for the fork fix.", status: "completed" },
        { message: "Harness refinement failed: planner timed out", status: "completed" },
      ]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
          python.turnItem.title === "Python: edit('src/a.ts', 'old', 'new')" &&
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("lets the user's next turn adopt a pending wake and queue behind it", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread, offers } = yield* openPrimeThreadWithWakes(fake);
      yield* emitChildMessageWake(fake);
      const offer = yield* Queue.take(offers);

      yield* startTurn(runtime, providerThread, "default", [], "What happened?");
      const prompt = yield* fake.takeRequest("prompt");
      assert.equal(prompt["streamingBehavior"], "followUp");
      yield* takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "assistant_message" &&
          event.turnItem.status === "completed",
      );
      const stale = yield* offer.dispatchIfCurrent!(Effect.succeed("dispatched"));
      assert.isTrue(Option.isNone(stale), "the adopted wake's continuation must not dispatch");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});
