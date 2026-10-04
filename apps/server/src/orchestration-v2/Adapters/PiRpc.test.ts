import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";

import {
  isPiSessionLeaseRefusal,
  makeJsonlFramer,
  makePiRpcConnection,
  type PiFrame,
} from "./PiRpc.ts";

// Reports its pid, then idles until signalled, like an idle `pi --mode rpc`.
const IDLE_PI_SCRIPT = `console.log(JSON.stringify({ type: "ready", pid: process.pid })); setInterval(() => {}, 1000);`;

// Like Prime Agent, closes its session on SIGTERM before exiting, and records
// that it finished.
const SLOW_SHUTDOWN_PI_SCRIPT = `
  console.log(JSON.stringify({ type: "ready", pid: process.pid }));
  setInterval(() => {}, 1000);
  process.on("SIGTERM", () => setTimeout(() => {
    require("node:fs").writeFileSync(process.env.SHUTDOWN_MARKER, "done");
    process.exit(0);
  }, 1500));
`;

const processGroupIsGone = (pid: number) => {
  try {
    process.kill(-pid, 0);
    return false;
  } catch {
    return true;
  }
};

describe.skipIf(HostProcessPlatform.defaultValue() === "win32")("PiRpc termination", () => {
  // Server shutdown closes every session, and the desktop app force-kills the
  // server soon after asking it to stop.
  it.live("returns as soon as the pi process group exits on SIGTERM", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const connection = yield* makePiRpcConnection({
        command: process.execPath,
        args: ["-e", IDLE_PI_SCRIPT],
        cwd: undefined,
        env: { PATH: process.env.PATH },
      }).pipe(Scope.provide(scope));
      const ready = yield* Queue.take(connection.events);
      const pid = Number(ready["pid"]);

      const closeStartedAt = yield* Clock.currentTimeMillis;
      yield* Scope.close(scope, Exit.void);
      const closeMillis = (yield* Clock.currentTimeMillis) - closeStartedAt;

      assert.isTrue(processGroupIsGone(pid));
      assert.isBelow(closeMillis, 500);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  // Prime Agent's SIGTERM handler asks its daemon to stop the session's
  // worker, which frees the session lease. A SIGKILL first leaves the lease
  // held for the daemon's 30 s disconnect timer.
  it.live.each([
    { name: "kills a shutdown that outlasts the default grace", grace: undefined, finished: false },
    { name: "lets a shutdown finish within terminationGrace", grace: "5 seconds", finished: true },
  ] as const)("$name", ({ grace, finished }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const marker = path.join(yield* fs.makeTempDirectoryScoped(), "shutdown");
      const scope = yield* Scope.make();
      const connection = yield* makePiRpcConnection({
        command: process.execPath,
        args: ["-e", SLOW_SHUTDOWN_PI_SCRIPT],
        cwd: undefined,
        env: { PATH: process.env.PATH, SHUTDOWN_MARKER: marker },
        ...(grace === undefined ? {} : { terminationGrace: grace }),
      }).pipe(Scope.provide(scope));
      yield* Queue.take(connection.events);

      yield* Scope.close(scope, Exit.void);

      assert.strictEqual(yield* fs.exists(marker), finished);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

/** Deliberately outside the valid pid range so a group-kill can never land. */
const FAKE_PID = 999_999_999;

const encoder = new TextEncoder();

/** Opens a connection whose stdout the test feeds by hand. */
const openConnection = Effect.fnUntraced(function* (maxRecordChars?: number) {
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
    ...(maxRecordChars === undefined ? {} : { maxRecordChars }),
  }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

  const pushBytes = (bytes: Uint8Array) => Queue.offer(stdout, bytes).pipe(Effect.asVoid);
  const pushText = (text: string) => pushBytes(encoder.encode(text));
  /** Feeds `bytes` in `chunkBytes`-sized slices, ignoring UTF-8 boundaries. */
  const pushChunked = (bytes: Uint8Array, chunkBytes: number) =>
    Effect.forEach(
      Array.from({ length: Math.ceil(bytes.length / chunkBytes) }, (_, i) =>
        bytes.subarray(i * chunkBytes, (i + 1) * chunkBytes),
      ),
      pushBytes,
      { discard: true },
    );
  /** Request ids are `t3-<n>` in call order; start the request, then let it register. */
  const startRequest = Effect.fnUntraced(function* (type: string) {
    const fiber = yield* Effect.forkChild(connection.request({ type }));
    yield* Effect.yieldNow;
    return fiber;
  });

  return { connection, pushText, pushBytes, pushChunked, startRequest };
});

const response = (id: string, data: unknown) =>
  JSON.stringify({ id, type: "response", command: "get_messages", success: true, data });

describe("PiRpc framing", () => {
  it.effect("reassembles records across chunk boundaries and strips CR", () =>
    Effect.gen(function* () {
      const { connection, pushText } = yield* openConnection();
      yield* pushText('{"type":"agent_');
      yield* pushText('start"}\r\n{"type":"agent_settled"}\nnot json\n{"type":"queue_update"}\n');

      const types = [];
      for (let i = 0; i < 3; i++) types.push((yield* Queue.take(connection.events))["type"]);
      assert.deepEqual(types, ["agent_start", "agent_settled", "queue_update"]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("delivers a reply over the old 8 MiB limit, split inside UTF-8 sequences", () =>
    Effect.gen(function* () {
      const { pushChunked, startRequest } = yield* openConnection();
      const fiber = yield* startRequest("get_messages");

      const text = "a😀é".repeat(2_500_000);
      assert.isAbove(text.length, 9 * 1024 * 1024);
      const bytes = encoder.encode(`${response("t3-0", { messages: [{ text }] })}\n`);
      yield* pushChunked(bytes, 65_537);

      const data = yield* Fiber.join(fiber);
      assert.deepEqual(data, { messages: [{ text }] });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps multi-byte characters intact when every byte is its own chunk", () =>
    Effect.gen(function* () {
      const { connection, pushChunked } = yield* openConnection();
      yield* pushChunked(encoder.encode('{"type":"note","text":"é😀日"}\n'), 1);
      assert.equal((yield* Queue.take(connection.events))["text"], "é😀日");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // Key order and spacing are Pi's to choose, so an oversized record is never
  // matched to a request by its text.
  it.effect("fails the pending request when its reply is oversized, whatever the key order", () =>
    Effect.gen(function* () {
      const { connection, pushText, startRequest } = yield* openConnection(1024);
      const oversized = yield* startRequest("get_messages");

      const big = `{"type" : "response", "data": {"text": "${"x".repeat(4096)}"}, "id": "t3-0"}`;
      yield* pushText(big.slice(0, 100));
      yield* pushText(big.slice(100, 3000));
      yield* pushText(`${big.slice(3000)}\n{"type":"after_oversized"}\n`);

      const error = yield* Fiber.join(oversized).pipe(Effect.asVoid, Effect.flip);
      assert.equal(error._tag, "PiRpcRecordTooLargeError");
      assert.equal(error._tag === "PiRpcRecordTooLargeError" && error.operation, "get_messages");
      assert.equal(error._tag === "PiRpcRecordTooLargeError" && error.maxChars, 1024);
      assert.equal((yield* Queue.take(connection.events))["type"], "after_oversized");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("fails every pending request on an oversized event, then serves new requests", () =>
    Effect.gen(function* () {
      const { pushText, startRequest } = yield* openConnection(1024);
      const first = yield* startRequest("get_messages");
      const second = yield* startRequest("get_state");

      yield* pushText(`{"type":"tool_output","text":"${"y".repeat(2000)}"}\n`);

      for (const [fiber, operation] of [
        [first, "get_messages"],
        [second, "get_state"],
      ] as const) {
        const error = yield* Fiber.join(fiber).pipe(Effect.asVoid, Effect.flip);
        assert.equal(error._tag === "PiRpcRecordTooLargeError" && error.operation, operation);
      }

      const later = yield* startRequest("get_state");
      yield* pushText(`${response("t3-2", "ok")}\n`);
      assert.equal(yield* Fiber.join(later), "ok");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("makeJsonlFramer", () => {
  const lines = ['{"a":1}', "", '{"b":"é😀"}', "x".repeat(50), "{}"];
  const input = `${lines.join("\n")}\n`;
  const text = (frames: ReadonlyArray<PiFrame>) =>
    frames.map((frame) => (frame._tag === "Line" ? frame.text : `<oversized ${frame.chars}>`));

  it("yields the same frames for every chunk size", () => {
    const expected = lines.filter((line) => line.length > 0);
    for (let size = 1; size <= input.length; size++) {
      const frame = makeJsonlFramer(1024);
      const frames: PiFrame[] = [];
      for (let i = 0; i < input.length; i += size) frames.push(...frame(input.slice(i, i + size)));
      assert.deepEqual(text(frames), expected, `chunk size ${size}`);
    }
  });

  it("resyncs at the next newline after an oversized line", () => {
    for (let size = 1; size <= input.length; size++) {
      const frame = makeJsonlFramer(10);
      const frames: PiFrame[] = [];
      for (let i = 0; i < input.length; i += size) frames.push(...frame(input.slice(i, i + size)));
      assert.deepEqual(text(frames), ['{"a":1}', "<oversized 11>", "<oversized 50>", "{}"]);
    }
  });
});

describe("PiRpc launch failure", () => {
  // Prime Agent refuses `--resume` for a session another worker holds by
  // printing this and exiting 1, before it speaks the protocol.
  const LEASE_REFUSAL_SCRIPT = `
    console.error("Error: Session is already active in 0123456789ab: /sessions/a.jsonl");
    process.exit(1);
  `;

  it.live("reports what pi wrote to stderr once a process that never answered has exited", () =>
    Effect.gen(function* () {
      const connection = yield* makePiRpcConnection({
        command: process.execPath,
        args: ["-e", LEASE_REFUSAL_SCRIPT],
        cwd: undefined,
        env: { PATH: process.env.PATH },
      });

      const reply = yield* Effect.exit(connection.request({ type: "get_state" }));

      assert.isTrue(Exit.isFailure(reply));
      assert.isTrue(isPiSessionLeaseRefusal(yield* connection.stderrAfterExit));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it("recognizes only the lease refusal", () => {
    assert.isFalse(isPiSessionLeaseRefusal("Error: Session file not found: /sessions/a.jsonl"));
  });
});
