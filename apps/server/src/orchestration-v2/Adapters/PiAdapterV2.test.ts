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
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
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
  /** Data returned by the next active-branch `get_messages` acks, ahead of the transcript. */
  readonly queueMessages: (data: unknown) => void;
  /**
   * Prime Agent's stored conversation from now on: what `get_messages` returns
   * and what `get_state` counts, unless a queued reply overrides one read.
   */
  readonly setTranscript: (messages: ReadonlyArray<unknown>) => void;
  /** Data returned by the next `get_fork_messages` acks, consumed in order. */
  readonly queueForkMessages: (data: unknown) => void;
  /** Make the next `switch_session` ack report an extension veto. */
  readonly vetoNextSwitch: () => void;
  /** Refuse the next `count` `switch_session` requests with `message`, as Prime Agent does while another worker holds the lease. */
  readonly rejectNextSwitches: (count: number, message: string) => void;
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
  /** History returned by the next `observe` ack. */
  readonly queueObserved: (messages: ReadonlyArray<unknown>) => void;
  /** Data returned by the next `get_session_stats` acks, consumed in order. */
  readonly queueStats: (data: unknown) => void;
  /** Data returned by the next `get_commands` acks, consumed in order. */
  readonly queueCommands: (data: unknown) => void;
  /** Make the next `get_commands` ack fail. */
  readonly failNextCommands: () => void;
  /** Data returned by the next `list_heartbeats` acks, consumed in order; none left means no heartbeats. */
  readonly queueHeartbeats: (data: unknown) => void;
  /** Make the next `list_heartbeats` ack fail. */
  readonly failNextHeartbeats: () => void;
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
  let transcript: ReadonlyArray<unknown> = [];
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
        return {
          ...base,
          data: {
            ...recordedIdleState(sessionFile),
            messageCount: transcript.length,
            ...stateQueue.shift(),
          },
        };
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
  continuationRequests?: Parameters<typeof makePiAdapterV2>[0]["continuationRequests"],
  environment: NodeJS.ProcessEnv = {},
) {
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  return makePiAdapterV2({
    flavor,
    ...(continuationRequests === undefined ? {} : { continuationRequests }),
    instanceId: PI_INSTANCE_ID,
    settings: { enabled: true, binaryPath: "pi", launchArgs, customModels: [] },
    environment,
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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

    /** Lets a launch run through its retry waits until it finishes. */
    const waitOutLaunch = <A, E, R>(launch: Effect.Effect<A, E, R>) =>
      Effect.gen(function* () {
        const finished = yield* Deferred.make<void>();
        const fiber = yield* launch.pipe(
          Effect.ensuring(Deferred.succeed(finished, undefined)),
          Effect.forkChild,
        );
        for (let waited = 0; waited < 100 && !(yield* Deferred.isDone(finished)); waited++) {
          yield* TestClock.adjust(Duration.seconds(1));
          // Real time, so the launch's own process I/O can progress between ticks.
          yield* Effect.sleep(Duration.millis(5)).pipe(
            Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
          );
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
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
    );

    it.effect("leaves Pi, which has no such controllers, to switch sessions", () =>
      Effect.gen(function* () {
        const providerThread = yield* savedThread;
        const fake = yield* makeFakePi;
        const runtime = yield* openOnSavedSession(fake, PI_FLAVOR);

        yield* runtime.resumeThread({ providerThread });

        assert.isUndefined(resumeFile(fake.lastSpawn().args));
        assert.equal(replacementRequests(fake).length, 1);
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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

  /** Steps virtual time until the fake sees a `get_state`, as a quiet-turn probe would send. */
  const takeQuietProbe = (fake: FakePi) =>
    Effect.gen(function* () {
      const probe = yield* fake.takeRequest("get_state").pipe(Effect.forkScoped);
      for (let step = 0; step < 20 && probe.pollUnsafe() === undefined; step += 1) {
        yield* TestClock.adjust(Duration.seconds(5));
        yield* Effect.yieldNow;
      }
      return yield* Fiber.join(probe);
    });

  /**
   * Like `takeQuietProbe`, but the clock stops at the watchdog tick that sends
   * the probe, so the 2 s reads that follow cannot time out under it.
   */
  const takeQuietProbeAtItsTick = (fake: FakePi) =>
    Effect.gen(function* () {
      yield* TestClock.adjust(Duration.seconds(30));
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      yield* takeQuietProbe(fake);
      assert.isUndefined(terminalFiber.pollUnsafe());

      yield* takeQuietProbe(fake);
      const terminal = yield* Fiber.join(terminalFiber);
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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

      yield* takeQuietProbeAtItsTick(fake);

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [fullReply],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("settles a turn that went quiet without showing one agent event", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      const { runtime, takeEvent, providerThread } = yield* openPrimeThread(fake);
      yield* startTurn(runtime, providerThread);
      yield* fake.takeRequest("prompt");
      fake.setTranscript(recordedTurn);

      yield* takeQuietProbeAtItsTick(fake);

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [fullReply],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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

      yield* takeQuietProbeAtItsTick(fake);

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [cutReply, fullReply],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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

      yield* takeQuietProbeAtItsTick(fake);

      assert.deepEqual(yield* takeCompletedReplies(takeEvent), [fullReply]);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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

      yield* takeQuietProbeAtItsTick(fake);

      assert.deepEqual(yield* takeCompletedReplies(takeEvent), [fullReply]);
      assert.isFalse(
        fake.allRequests().some((request) => request["type"] === "switch_session"),
        "the thread stayed on the session the process launched on",
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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

      yield* takeQuietProbeAtItsTick(fake);

      assert.deepEqual(yield* takeRepliesAndOutcome(takeEvent), {
        texts: [fullReply],
        status: "completed",
      });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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

      yield* takeQuietProbeAtItsTick(fake);

      assert.deepEqual(yield* takeCompletedLookup(takeEvent), {
        input: { key: "a" },
        output: "found it",
      });
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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

  it.effect(
    "keeps a finished child's thread live for follow-up work and routes its nested children",
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

        // None of it rewrote the original task's card or its root node.
        const rewrites = [...followUp, ...nesting, ...ending].filter(
          (event) =>
            (event.type === "subagent.updated" && event.subagent.id === child?.id) ||
            (event.type === "node.updated" &&
              event.node.kind === "root_turn" &&
              event.node.threadId === child?.childThreadId),
        );
        assert.deepEqual(rewrites, []);
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      assert.deepEqual(notice.turnItem.source, { kind: "background_task" });
      assert.equal(notice.turnItem.detail, "Check the deploy.");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      assert.deepEqual(offer.notification?.source, { kind: "background_task" });
      assert.equal(offer.notification?.detail, "Check the deploy.");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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

describe("PiAdapterV2 subagent capabilities", () => {
  it.effect("exposes subagent thread ids only where children get their own thread", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakePi;
      for (const flavor of [PI_FLAVOR, PRIME_AGENT_FLAVOR]) {
        const adapter = yield* makeAdapter(fake, "", undefined, flavor);
        const capabilities = yield* adapter.getCapabilities();
        assert.equal(capabilities.subagents.exposesSubagentThreadIds, flavor.childThreads);
      }
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    ).pipe(Effect.scoped, Effect.provide(testLayer)),
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
      ).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    ).pipe(Effect.scoped, Effect.provide(testLayer)),
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
    ).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});
