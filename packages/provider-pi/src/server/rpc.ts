// Pi frames are LF-delimited with trailing CR stripped; readline also splits U+2028/U+2029 and corrupts them.
// A record over `maxRecordChars` fails pending requests with `PiRpcRecordTooLargeError`.
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import {
  MAX_TOOL_OUTPUT_IMAGES,
  MAX_TOOL_OUTPUT_IMAGE_BASE64_LENGTH,
} from "@t3tools/shared/toolOutput";

import { signalProcessGroup } from "@t3tools/provider-core/server/processGroup";

export class PiRpcError extends Schema.TaggedError<PiRpcError>()("PiRpcError", {
  operation: Schema.String,
  detail: Schema.optional(Schema.String),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `Pi RPC ${this.operation} failed${this.detail === undefined ? "" : `: ${this.detail}`}.`;
  }
}

const isPiRpcError = Schema.is(PiRpcError);

export class PiRpcTimeoutError extends Schema.TaggedError<PiRpcTimeoutError>()(
  "PiRpcTimeoutError",
  {
    operation: Schema.String,
    timeoutMs: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  },
) {
  override get message(): string {
    return `Pi RPC ${this.operation} failed: timed out after ${this.timeoutMs}ms.`;
  }
}

const SESSION_LEASE_HELD_TEXT = "Session is already active";

/**
 * Prime Agent refuses `switch_session` while another worker still holds the
 * session lease, and reports only the message text. The holder releases the
 * lease once it finishes shutting down, so the refusal is transient.
 */
export function isPiSessionLeaseContention(error: unknown): boolean {
  return (
    isPiRpcError(error) &&
    error.operation === "switch_session" &&
    typeof error.cause === "string" &&
    error.cause.startsWith(SESSION_LEASE_HELD_TEXT)
  );
}

const UNKNOWN_ACTIVE_SESSION_PREFIX = "Unknown active session: ";

/**
 * The active session id a Prime Agent daemon answers a request with when it
 * does not host that session, read from the failed response's error text.
 */
export function unknownActiveSessionId(errorText: unknown): string | undefined {
  if (typeof errorText !== "string" || !errorText.startsWith(UNKNOWN_ACTIVE_SESSION_PREFIX)) {
    return undefined;
  }
  const id = errorText.slice(UNKNOWN_ACTIVE_SESSION_PREFIX.length).trim();
  return id.length === 0 ? undefined : id;
}

export class PiRpcRecordTooLargeError extends Schema.TaggedError<PiRpcRecordTooLargeError>()(
  "PiRpcRecordTooLargeError",
  {
    operation: Schema.String,
    chars: Schema.Int,
    maxChars: Schema.Int,
  },
) {
  override get message(): string {
    return `Pi RPC ${this.operation} failed: a stdout record of ${this.chars} chars exceeds the ${this.maxChars} char limit.`;
  }
}

/**
 * Prime Agent opens a session at launch (`--resume <file>`) only when no other
 * worker holds its lease, and otherwise exits with this message on stderr. The
 * holder releases the lease once it finishes shutting down, so the refusal is
 * transient.
 */
export function isPiSessionLeaseRefusal(stderr: string): boolean {
  return stderr.includes(SESSION_LEASE_HELD_TEXT);
}

/** A `--resume` launch the daemon refused because another worker still holds the session. */
export class PiSessionLeaseHeldError extends Schema.TaggedError<PiSessionLeaseHeldError>()(
  "PiSessionLeaseHeldError",
  { sessionFile: Schema.String },
) {
  override get message(): string {
    return `Pi session ${this.sessionFile} is still held by another worker.`;
  }
}

export type PiRpcRecord = Record<string, unknown>;

export function piRecordField(input: unknown, key: string): unknown {
  return Predicate.isObject(input) ? input[key] : undefined;
}

export function piRecordString(input: unknown, key: string): string | undefined {
  const value = piRecordField(input, key);
  return Predicate.isString(value) ? value : undefined;
}

export function piRecordNumber(input: unknown, key: string): number | undefined {
  const value = piRecordField(input, key);
  return Predicate.isNumber(value) && Number.isFinite(value) ? value : undefined;
}

/**
 * Whether a `get_state` payload shows no remaining agent work. Pi reports a
 * pending message count; Prime Agent keeps `sessionActions.active` set until a
 * run's retry chain and post-run work finish, and counts queued input.
 */
export function piStateIsIdle(state: unknown): boolean {
  const sessionActions = piRecordField(state, "sessionActions");
  return (
    piRecordField(state, "isStreaming") !== true &&
    piRecordField(state, "isCompacting") !== true &&
    (piRecordNumber(state, "pendingMessageCount") ?? 0) === 0 &&
    (piRecordNumber(sessionActions, "queuedCount") ?? 0) === 0 &&
    piRecordField(sessionActions, "active") === undefined
  );
}

export function parsePiModelSlug(slug: string): { provider: string; modelId: string } | null {
  const separator = slug.indexOf("/");
  if (separator <= 0 || separator === slug.length - 1) return null;
  return { provider: slug.slice(0, separator), modelId: slug.slice(separator + 1) };
}

export interface PiRpcSpawnOptions {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string | undefined;
  readonly env: NodeJS.ProcessEnv;
  /**
   * How long SIGTERM may take before SIGKILL, ending as soon as the process
   * group exits. Defaults to one second. An agent that closes its session in
   * its SIGTERM handler needs longer, or the kill cuts that shutdown off.
   */
  readonly terminationGrace?: Duration.Input;
  /** Longest stdout record accepted, in characters. */
  readonly maxRecordChars?: number;
}

export interface PiRpcConnection {
  readonly send: (record: PiRpcRecord) => Effect.Effect<void, PiRpcError>;
  /**
   * Correlated request: assigns an `id`, waits for the matching response
   * record, and returns its `data` (undefined when the command carries none).
   * Fails on `success: false`, transport death, timeout, or a reply over the
   * record size limit. A `null` timeout waits for the response, for commands
   * that must not be abandoned midway.
   */
  readonly request: (
    record: PiRpcRecord,
    timeoutMs?: number | null,
  ) => Effect.Effect<unknown, PiRpcError | PiRpcTimeoutError | PiRpcRecordTooLargeError>;
  readonly events: Queue.Queue<PiRpcRecord, PiRpcError>;
  readonly exited: Effect.Effect<number, PiRpcError>;
  /**
   * The end of what pi wrote to stderr, read once the process has closed it
   * (empty if it has not within half a second). Only for classifying a failed
   * launch: stderr can carry credentials or prompt text, so never log it.
   */
  readonly stderrAfterExit: Effect.Effect<string>;
  readonly terminate: Effect.Effect<void>;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const DEFAULT_TERMINATION_GRACE = Duration.seconds(1);
const TERMINATION_POLL = Schedule.spaced("20 millis");

interface PendingPiRequest {
  readonly operation: string;
  readonly deferred: Deferred.Deferred<unknown, PiRpcError | PiRpcRecordTooLargeError>;
}

// MCP duplicates images in model content and CallToolResult; budget both plus bounded overhead.
// About 232M chars, far above the largest Prime Agent `get_messages` reply seen (about 13M).
const DEFAULT_MAX_PI_RECORD_CHARS =
  2 * MAX_TOOL_OUTPUT_IMAGES * MAX_TOOL_OUTPUT_IMAGE_BASE64_LENGTH + 8 * 1024 * 1024;
const STDERR_TAIL_CHARS = 2_000;
const STDERR_CLOSE_WAIT = Duration.millis(500);

export type PiFrame =
  | { readonly _tag: "Line"; readonly text: string }
  | { readonly _tag: "Oversized"; readonly chars: number };

/**
 * Splits decoded stdout text into LF-delimited frames. Pieces of a line are
 * collected and joined once at its newline. Past `maxChars` a line stops being
 * collected and surfaces as `Oversized`, carrying only its length.
 */
export function makeJsonlFramer(maxChars: number) {
  let pieces: string[] = [];
  let chars = 0;

  const push = (piece: string) => {
    chars += piece.length;
    if (chars <= maxChars) {
      pieces.push(piece);
    } else {
      pieces = [];
    }
  };

  const finishLine = (): PiFrame | undefined => {
    const text = pieces.join("");
    const frame: PiFrame | undefined =
      chars > maxChars
        ? { _tag: "Oversized", chars }
        : chars === 0
          ? undefined
          : { _tag: "Line", text: text.endsWith("\r") ? text.slice(0, -1) : text };
    pieces = [];
    chars = 0;
    return frame;
  };

  return (chunk: string): ReadonlyArray<PiFrame> => {
    const frames: PiFrame[] = [];
    let start = 0;
    while (start < chunk.length) {
      const newline = chunk.indexOf("\n", start);
      push(chunk.slice(start, newline < 0 ? chunk.length : newline));
      if (newline < 0) break;
      const frame = finishLine();
      if (frame !== undefined) frames.push(frame);
      start = newline + 1;
    }
    return frames;
  };
}

const UnknownFromJsonString = Schema.fromJsonString(Schema.Unknown);
const decodeJsonLine = Schema.decodeSync(UnknownFromJsonString);
const encodeJsonLine = Schema.encodeSync(UnknownFromJsonString);

const PI_ERROR_DETAIL_MAX_CHARS = 200;

function summarizePiError(error: unknown): string {
  const text = typeof error === "string" ? error : JSON.stringify(error);
  if (text === undefined) return "unknown error";
  return text.length > PI_ERROR_DETAIL_MAX_CHARS
    ? `${text.slice(0, PI_ERROR_DETAIL_MAX_CHARS)}…`
    : text;
}

// Pi 0.84+ may exit 1 during import without any protocol output.
function describePiStdoutClosure(exitCode: number | undefined): string {
  if (exitCode === undefined || !Number.isFinite(exitCode)) {
    return "pi process closed stdout";
  }
  return `pi process exited with code ${exitCode}`;
}

function parsePiRecord(line: string): PiRpcRecord | undefined {
  try {
    const parsed: unknown = decodeJsonLine(line);
    return Predicate.isObject(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Kill the pi process group: SIGTERM, a grace that ends as soon as the group
 * exits, then SIGKILL.
 *
 * `hasExited` is consulted before each signal. Once the original child is
 * gone its pid/pgid can be recycled by the OS, so escalating blindly could
 * deliver SIGKILL to an unrelated process.
 */
const terminatePiProcess = Effect.fn("PiRpc.terminatePiProcess")(function* (
  kill: (signal: NodeJS.Signals) => boolean,
  hasExited: () => boolean,
  grace: Duration.Input,
) {
  if (hasExited()) return;
  if (!kill("SIGTERM")) return;
  yield* Effect.sync(hasExited).pipe(
    Effect.repeat({ until: (exited) => exited, schedule: TERMINATION_POLL }),
    Effect.timeoutOption(grace),
  );
  if (hasExited()) return;
  kill("SIGKILL");
});

export const makePiRpcConnection = Effect.fnUntraced(function* (options: PiRpcSpawnOptions) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const platform = yield* HostProcess.Platform;
  const scope = yield* Effect.scope;

  const spawnCommand = yield* resolveSpawnCommand(options.command, [...options.args], {
    env: options.env,
  }).pipe(Effect.mapError((cause) => new PiRpcError({ operation: "spawn", cause })));
  const child = yield* spawner
    .spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
        env: options.env,
        extendEnv: false,
        shell: spawnCommand.shell,
        detached: platform !== "win32",
      }),
    )
    .pipe(Effect.mapError((cause) => new PiRpcError({ operation: "spawn", cause })));

  let childExited = false;
  let diagnosingStdoutClose = false;

  const killProcessGroup = (signal: NodeJS.Signals): boolean => {
    try {
      if (platform === "win32") {
        process.kill(Number(child.pid), signal);
      } else {
        signalProcessGroup(Number(child.pid), signal);
      }
      return true;
    } catch {
      return false;
    }
  };

  const hasExited = (): boolean => {
    if (childExited) return true;
    try {
      if (platform === "win32") process.kill(Number(child.pid), 0);
      else signalProcessGroup(Number(child.pid), 0);
      return false;
    } catch {
      return true;
    }
  };

  // Windows process.kill leaves extension children and pipes alive; taskkill /T reaps the tree.
  const terminateWindowsTree = Effect.gen(function* () {
    if (hasExited()) return;
    const taskkill = yield* spawner.spawn(
      ChildProcess.make("taskkill", ["/PID", String(child.pid), "/T", "/F"]),
    );
    yield* taskkill.exitCode;
  }).pipe(Effect.scoped, Effect.ignore);

  const terminateProcess =
    platform === "win32"
      ? terminateWindowsTree
      : terminatePiProcess(
          killProcessGroup,
          hasExited,
          options.terminationGrace ?? DEFAULT_TERMINATION_GRACE,
        );

  // Register teardown immediately after spawn so interrupted setup cannot leak a detached process.
  yield* Scope.addFinalizer(scope, terminateProcess.pipe(Effect.ignore, Effect.uninterruptible));

  const maxRecordChars = options.maxRecordChars ?? DEFAULT_MAX_PI_RECORD_CHARS;
  const pendingRequests = new Map<string, PendingPiRequest>();
  const events = yield* Queue.unbounded<PiRpcRecord, PiRpcError>();
  const outgoing = yield* Queue.unbounded<Uint8Array, PiRpcError>();
  const transportDown = yield* Deferred.make<never, PiRpcError>();
  const exitDeferred = yield* Deferred.make<number, PiRpcError>();
  let nextRequestId = 0;

  const failTransport = Effect.fn("PiRpc.failTransport")(function* (error: PiRpcError) {
    const claimed = yield* Deferred.fail(transportDown, error);
    if (!claimed) return;
    for (const [key, pending] of pendingRequests) {
      pendingRequests.delete(key);
      yield* Deferred.fail(pending.deferred, error);
    }
    yield* Queue.fail(outgoing, error);
    yield* Queue.fail(events, error);
  });

  const routeRecord =
    /* Protocol records and writes include streaming deltas; avoid a span per record. */
    Effect.fnUntraced(function* (record: PiRpcRecord) {
      if (record["type"] === "response" && typeof record["id"] === "string") {
        const pending = pendingRequests.get(record["id"]);
        if (pending !== undefined) {
          pendingRequests.delete(record["id"]);
          if (record["success"] === true) {
            yield* Deferred.succeed(pending.deferred, record["data"]);
          } else {
            yield* Deferred.fail(
              pending.deferred,
              new PiRpcError({
                operation: String(record["command"] ?? "request"),
                detail: summarizePiError(record["error"]),
                ...(record["error"] === undefined ? {} : { cause: record["error"] }),
              }),
            );
          }
          return;
        }
      }
      yield* Queue.offer(events, record);
    });

  // Watch exit before reading stdout so immediate crashes populate exitDeferred for diagnosis.
  yield* child.exitCode.pipe(
    Effect.matchEffect({
      onFailure: (cause) =>
        Deferred.fail(exitDeferred, new PiRpcError({ operation: "exit", cause })),
      onSuccess: (code) =>
        Effect.suspend(() => {
          childExited = true;
          return Deferred.succeed(exitDeferred, Number(code));
        }),
    }),
    Effect.forkIn(scope),
  );

  // An oversized record is never parsed, so we cannot tell which request it
  // answers, or whether it is a response at all. Fail every pending request: a
  // wrong guess would leave the right one hanging until its timeout. The
  // callers' requests are rare and short, so a spurious failure is cheap.
  const rejectOversized = Effect.fn("PiRpc.rejectOversized")(function* (
    frame: Extract<PiFrame, { _tag: "Oversized" }>,
  ) {
    yield* Effect.logWarning("Dropping oversized pi stdout record.", {
      chars: frame.chars,
      maxChars: maxRecordChars,
      failedRequests: pendingRequests.size,
    });
    for (const [id, pending] of pendingRequests) {
      pendingRequests.delete(id);
      yield* Deferred.fail(
        pending.deferred,
        new PiRpcRecordTooLargeError({
          operation: pending.operation,
          chars: frame.chars,
          maxChars: maxRecordChars,
        }),
      );
    }
  });

  const routeFrame =
    /* Protocol records include streaming deltas; avoid a span per frame. */
    Effect.fnUntraced(function* (frame: PiFrame) {
      if (frame._tag === "Oversized") return yield* rejectOversized(frame);
      const record = parsePiRecord(frame.text);
      if (record === undefined) {
        yield* Effect.logDebug("Dropping non-JSON pi stdout line.", {
          lineLength: frame.text.length,
        });
        return;
      }
      yield* routeRecord(record);
    });

  yield* Effect.gen(function* () {
    const frameLines = makeJsonlFramer(maxRecordChars);
    yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) =>
        Effect.forEach(frameLines(chunk), routeFrame, { discard: true }),
      ),
    );
    yield* Effect.forEach(frameLines("\n"), routeFrame, { discard: true });
  }).pipe(
    Effect.matchCauseEffect({
      onFailure: (cause) => failTransport(new PiRpcError({ operation: "read", cause })),
      // The enclosing operation owns the span for this inline handler.
      onSuccess: Effect.fnUntraced(function* () {
        // Claim stdout-close diagnosis before waiting so broken stdin cannot replace the exit error.
        diagnosingStdoutClose = true;
        const polled = yield* Deferred.poll(exitDeferred);
        const maybeExitCode = Option.isSome(polled)
          ? yield* polled.value.pipe(
              Effect.asSome,
              Effect.orElseSucceed(() => Option.none<number>()),
            )
          : yield* Deferred.await(exitDeferred).pipe(
              Effect.timeoutOption(Duration.millis(250)),
              // Stdout-close waits need wall time; TestClock would strand Stop-with-restart without an exit.
              Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
              Effect.orElseSucceed(() => Option.none<number>()),
            );
        return yield* failTransport(
          new PiRpcError({
            operation: "read",
            detail: describePiStdoutClosure(
              Option.isSome(maybeExitCode) ? maybeExitCode.value : undefined,
            ),
          }),
        );
      }),
    }),
    Effect.forkIn(scope),
  );

  let stderrTail = "";
  const stderrClosed = yield* Deferred.make<void>();
  yield* child.stderr.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) => {
      stderrTail = (stderrTail + chunk).slice(-STDERR_TAIL_CHARS);
      return chunk.trim().length === 0
        ? Effect.void
        : // Log stderr length only; unbounded text may contain credentials or prompts.
          Effect.logDebug("pi stderr", { stderrLength: chunk.length });
    }),
    Effect.ignore,
    Effect.ensuring(Deferred.succeed(stderrClosed, undefined)),
    Effect.forkIn(scope),
  );

  // Start the reader first so stdout-close diagnosis can win over broken-pipe stdin.
  yield* Stream.fromQueue(outgoing).pipe(
    Stream.run(child.stdin),
    Effect.catchCause((cause) =>
      diagnosingStdoutClose
        ? Effect.void
        : failTransport(new PiRpcError({ operation: "write", cause })),
    ),
    Effect.forkIn(scope),
  );

  const send =
    /* Protocol records and writes include streaming deltas; avoid a span per record. */
    Effect.fnUntraced(function* (record: PiRpcRecord): Effect.fn.Return<void, PiRpcError> {
      const accepted = yield* Queue.offer(
        outgoing,
        new TextEncoder().encode(`${encodeJsonLine(record)}\n`),
      );
      if (!accepted) {
        return yield* Deferred.await(transportDown);
      }
    });

  const request = Effect.fn("PiRpc.request")(function* (
    record: PiRpcRecord,
    timeoutMs: number | null = DEFAULT_REQUEST_TIMEOUT_MS,
  ): Effect.fn.Return<unknown, PiRpcError | PiRpcTimeoutError | PiRpcRecordTooLargeError> {
    const id = `t3-${nextRequestId++}`;
    const operation = String(record["type"] ?? "request");
    const deferred = yield* Deferred.make<unknown, PiRpcError | PiRpcRecordTooLargeError>();
    pendingRequests.set(id, { operation, deferred });
    yield* send({ ...record, id }).pipe(
      Effect.tapError(() => Effect.sync(() => pendingRequests.delete(id))),
    );
    const response = Effect.raceFirst(Deferred.await(deferred), Deferred.await(transportDown));
    return yield* (
      timeoutMs === null
        ? response
        : response.pipe(
            Effect.timeoutOrElse({
              duration: Duration.millis(timeoutMs),
              orElse: () => Effect.fail(new PiRpcTimeoutError({ operation, timeoutMs })),
            }),
          )
    ).pipe(
      Effect.onInterrupt(() => Effect.sync(() => pendingRequests.delete(id))),
      Effect.onError(() => Effect.sync(() => pendingRequests.delete(id))),
    );
  });

  return {
    send,
    request,
    events,
    exited: Deferred.await(exitDeferred),
    stderrAfterExit: Deferred.await(stderrClosed).pipe(
      Effect.timeoutOption(STDERR_CLOSE_WAIT),
      // Same reason as the stdout-close wait above: adapter tests run under TestClock.
      Effect.provideService(Clock.Clock, Clock.Clock.defaultValue()),
      Effect.map(() => stderrTail),
    ),
    terminate: failTransport(
      new PiRpcError({ operation: "terminate", detail: "pi process was stopped" }),
    ).pipe(Effect.andThen(terminateProcess), Effect.ignore, Effect.uninterruptible),
  } satisfies PiRpcConnection;
});
