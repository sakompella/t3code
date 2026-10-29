import {
  DESKTOP_ATTACH_SECRET_NAME,
  currentDesktopBootstrapToken,
  encodeDesktopAttachSecret,
} from "@t3tools/shared/desktopBootstrapToken";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";

import * as DesktopObservability from "../app/DesktopObservability.ts";
import type * as DesktopBackendManager from "./DesktopBackendManager.ts";

export const POLL_INTERVAL = Duration.seconds(2);
const PROBE_TIMEOUT = Duration.seconds(1);
// About 4 s past the first miss: long enough for a deploy's restart, short enough to explain a stopped server.
export const MISSES_BEFORE_REPORT = 3;
const ENVIRONMENT_DESCRIPTOR_PATH = "/.well-known/t3/environment";

const { logInfo, logWarning } = DesktopObservability.makeComponentLogger(
  "desktop-external-backend",
);

// Only the fields this app needs, so a newer or older server still decodes.
const decodeRuntimeState = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Struct({ pid: Schema.Int, origin: Schema.String })),
);
const EnvironmentDescriptor = Schema.Struct({ environmentId: Schema.String });

export type ExternalServerProbe =
  | { readonly _tag: "Up"; readonly httpBaseUrl: URL; readonly attachSecret: string }
  | { readonly _tag: "Down"; readonly reason: string };

const down = (reason: string): ExternalServerProbe => ({ _tag: "Down", reason });

/**
 * Finds the server that owns a T3 state directory, as `t3 pair` does: its server-runtime.json
 * names the origin, and the server answering there must report this home's environment id.
 * Sends nothing to the server process itself.
 */
export const probeExternalServer = Effect.fn("desktop.externalBackend.probe")(function* (
  stateDir: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const httpClient = yield* HttpClient.HttpClient;
  const readText = (file: string) =>
    fileSystem.readFileString(path.join(stateDir, file)).pipe(
      Effect.map((text) => text.trim()),
      Effect.option,
    );

  const runtimeText = yield* readText("server-runtime.json");
  const runtime = yield* Option.match(runtimeText, {
    onNone: () => Effect.succeedNone,
    onSome: (text) => decodeRuntimeState(text).pipe(Effect.option),
  });
  if (Option.isNone(runtime)) {
    return down(`No server has recorded itself in ${path.join(stateDir, "server-runtime.json")}.`);
  }
  const { pid, origin } = runtime.value;

  const answered = yield* httpClient
    .execute(HttpClientRequest.get(new URL(ENVIRONMENT_DESCRIPTOR_PATH, origin).href))
    .pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      Effect.flatMap(HttpClientResponse.schemaBodyJson(EnvironmentDescriptor)),
      Effect.timeout(PROBE_TIMEOUT),
      Effect.option,
    );
  if (Option.isNone(answered)) {
    return down(`The server in server-runtime.json (pid ${pid}) does not answer at ${origin}.`);
  }
  const homeEnvironmentId = yield* readText("environment-id");
  if (!Option.contains(homeEnvironmentId, answered.value.environmentId)) {
    return down(`${origin} is another T3 server, not the one for ${stateDir}.`);
  }

  // ServerSecretStore keeps each secret as <stateDir>/secrets/<name>.bin.
  const secretPath = path.join(stateDir, "secrets", `${DESKTOP_ATTACH_SECRET_NAME}.bin`);
  const secret = yield* fileSystem.readFile(secretPath).pipe(Effect.option);
  if (Option.isNone(secret)) {
    return down(
      `The server at ${origin} is too old for this app to attach: ${secretPath} is missing. Update the server.`,
    );
  }
  return {
    _tag: "Up",
    httpBaseUrl: new URL(origin),
    attachSecret: encodeDesktopAttachSecret(secret.value),
  } satisfies ExternalServerProbe;
});

export interface ExternalBackendSpec extends Pick<
  DesktopBackendManager.BackendInstanceSpec,
  "id" | "label" | "onReady" | "onShutdown" | "onStartupFailure"
> {
  readonly stateDir: string;
}

interface ExternalBackendState {
  readonly desiredRunning: boolean;
  readonly ready: boolean;
  readonly attached: Option.Option<{ readonly httpBaseUrl: URL; readonly attachSecret: string }>;
  readonly misses: number;
  readonly monitor: Option.Option<Fiber.Fiber<void>>;
}

/**
 * A backend instance for a server this app neither starts nor stops. Start watches the server
 * that owns the T3 home and reports it ready whenever it answers; stop only ends the watch.
 */
export const makeExternalBackendInstance = Effect.fn("makeExternalBackendInstance")(function* (
  spec: ExternalBackendSpec,
): Effect.fn.Return<
  DesktopBackendManager.DesktopBackendInstance,
  never,
  FileSystem.FileSystem | Path.Path | HttpClient.HttpClient | Scope.Scope
> {
  const parentScope = yield* Scope.Scope;
  const probeContext = yield* Effect.context<
    FileSystem.FileSystem | Path.Path | HttpClient.HttpClient
  >();
  const mutex = yield* Semaphore.make(1);
  const state = yield* Ref.make<ExternalBackendState>({
    desiredRunning: false,
    ready: false,
    attached: Option.none(),
    misses: 0,
    monitor: Option.none(),
  });

  const apply = Effect.fn("desktop.externalBackend.apply")(function* (probe: ExternalServerProbe) {
    const previous = yield* Ref.getAndUpdate(state, (current) =>
      probe._tag === "Up"
        ? {
            ...current,
            ready: true,
            misses: 0,
            attached: Option.some({
              httpBaseUrl: probe.httpBaseUrl,
              attachSecret: probe.attachSecret,
            }),
          }
        : // Keep the last connection so clients keep reconnecting to it.
          { ...current, ready: false, misses: current.misses + 1 },
    );
    if (probe._tag === "Up") {
      const moved = Option.exists(
        previous.attached,
        (attached) => attached.httpBaseUrl.href !== probe.httpBaseUrl.href,
      );
      if (previous.ready && !moved) return;
      if (previous.ready) yield* spec.onShutdown?.() ?? Effect.void;
      yield* logInfo("attached to external server", { url: probe.httpBaseUrl.href });
      yield* spec.onReady?.(probe.httpBaseUrl) ?? Effect.void;
      return;
    }
    if (previous.ready) {
      yield* logWarning("external server stopped answering", { reason: probe.reason });
      yield* spec.onShutdown?.() ?? Effect.void;
    }
    if (previous.misses + 1 === MISSES_BEFORE_REPORT) {
      yield* (
        spec.onStartupFailure?.({
          reason:
            "T3 Code is set to use the server that runs for this T3 home, and it is not running.",
          output: `${probe.reason}\n\nStart that server. T3 Code connects as soon as it answers, and never starts or stops it.`,
          externalServer: true,
        }) ?? Effect.void
      );
    }
  }, Effect.uninterruptible);

  const monitor = probeExternalServer(spec.stateDir).pipe(
    Effect.provide(probeContext),
    Effect.flatMap(apply),
    Effect.repeat(Schedule.spaced(POLL_INTERVAL)),
    Effect.asVoid,
  );

  const start = mutex.withPermits(1)(
    Effect.gen(function* () {
      if (Option.isSome((yield* Ref.get(state)).monitor)) return;
      const fiber = yield* Effect.forkIn(monitor, parentScope);
      yield* Ref.update(state, (current) => ({
        ...current,
        desiredRunning: true,
        monitor: Option.some(fiber),
      }));
    }),
  );

  const stop = () =>
    mutex.withPermits(1)(
      Effect.gen(function* () {
        const { monitor } = yield* Ref.get(state);
        if (Option.isSome(monitor)) yield* Fiber.interrupt(monitor.value);
        const previous = yield* Ref.getAndUpdate(state, (current) => ({
          ...current,
          desiredRunning: false,
          ready: false,
          misses: 0,
          monitor: Option.none(),
        }));
        if (previous.ready) yield* spec.onShutdown?.() ?? Effect.void;
      }),
    );

  const currentConfig = Effect.gen(function* () {
    const { attached } = yield* Ref.get(state);
    if (Option.isNone(attached))
      return Option.none<DesktopBackendManager.DesktopBackendConnection>();
    const now = yield* Clock.currentTimeMillis;
    // Clients exchange this token, not the secret: the app's own secret would derive the wrong one.
    return Option.some({
      httpBaseUrl: attached.value.httpBaseUrl,
      preflightFailure: Option.none(),
      bootstrap: {
        desktopBootstrapToken: currentDesktopBootstrapToken(attached.value.attachSecret, now),
      },
    } satisfies DesktopBackendManager.DesktopBackendConnection);
  });

  const snapshot = Ref.get(state).pipe(
    Effect.map((current): DesktopBackendManager.DesktopBackendSnapshot => ({
      desiredRunning: current.desiredRunning,
      ready: current.ready,
      activePid: Option.none(),
      restartAttempt: 0,
      restartScheduled: false,
    })),
  );

  const waitForReady = (timeout: Duration.Duration) =>
    Ref.get(state).pipe(
      Effect.repeat({
        until: (current) => current.ready || !current.desiredRunning,
        schedule: Schedule.spaced(Duration.millis(100)),
      }),
      Effect.map((current) => current.ready),
      Effect.timeoutOption(timeout),
      Effect.map(Option.getOrElse(() => false)),
    );

  yield* Effect.addFinalizer(() => stop());

  return {
    id: spec.id,
    label: spec.label,
    start,
    stop,
    currentConfig,
    snapshot,
    waitForReady,
  } satisfies DesktopBackendManager.DesktopBackendInstance;
});
