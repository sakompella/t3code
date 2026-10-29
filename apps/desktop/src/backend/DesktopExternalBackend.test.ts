import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  DESKTOP_ATTACH_SECRET_NAME,
  encodeDesktopAttachSecret,
  isValidDesktopBootstrapToken,
} from "@t3tools/shared/desktopBootstrapToken";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";

import * as DesktopBackendManager from "./DesktopBackendManager.ts";
import * as DesktopExternalBackend from "./DesktopExternalBackend.ts";
import {
  ATTACH_SECRET,
  ENVIRONMENT_ID,
  type FakeServer,
  layerFakeServerHttp,
  writeServerState,
} from "./externalServerTestHarness.ts";

const probe = (stateDir: string, server: FakeServer) =>
  DesktopExternalBackend.probeExternalServer(stateDir).pipe(
    Effect.provide(layerFakeServerHttp(Ref.makeUnsafe(server))),
  );

/** A state directory held in memory, so a probe finishes without waiting on real I/O. */
const layerFakeHome = (stateDir: string) =>
  FileSystem.layerNoop({
    readFileString: (file) =>
      file === `${stateDir}/server-runtime.json`
        ? Effect.succeed(
            JSON.stringify({ version: 1, pid: 4242, port: 3773, origin: "http://127.0.0.1:3773" }),
          )
        : file === `${stateDir}/environment-id`
          ? Effect.succeed(ENVIRONMENT_ID)
          : Effect.die(`unexpected read of ${file}`),
    readFile: (file) =>
      file === `${stateDir}/secrets/${DESKTOP_ATTACH_SECRET_NAME}.bin`
        ? Effect.succeed(ATTACH_SECRET)
        : Effect.die(`unexpected read of ${file}`),
  });

type Event =
  | { readonly _tag: "ready"; readonly url: string }
  | { readonly _tag: "shutdown" }
  | { readonly _tag: "failing"; readonly externalServer: boolean };

it.layer(NodeServices.layer)("DesktopExternalBackend", (it) => {
  describe("probeExternalServer", () => {
    it.effect("finds the server that owns the T3 home", () =>
      Effect.gen(function* () {
        const stateDir = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped();
        yield* writeServerState(stateDir, { pid: 4242 });

        const result = yield* probe(stateDir, { up: true, environmentId: ENVIRONMENT_ID });

        assert.deepEqual(result, {
          _tag: "Up",
          httpBaseUrl: new URL("http://127.0.0.1:3773"),
          attachSecret: encodeDesktopAttachSecret(ATTACH_SECRET),
        });
      }),
    );

    it.effect("reports why no server can be attached", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const empty = yield* fileSystem.makeTempDirectoryScoped();
        assert.equal(
          (yield* probe(empty, { up: true, environmentId: ENVIRONMENT_ID }))._tag,
          "Down",
        );

        const stateDir = yield* fileSystem.makeTempDirectoryScoped();
        yield* writeServerState(stateDir, { pid: 4242 });
        // A crashed server leaves its runtime file behind.
        assert.equal(
          (yield* probe(stateDir, { up: false, environmentId: ENVIRONMENT_ID }))._tag,
          "Down",
        );
        // Another home's server took the recorded port; its tokens would never match.
        assert.equal((yield* probe(stateDir, { up: true, environmentId: "other" }))._tag, "Down");

        const oldServer = yield* fileSystem.makeTempDirectoryScoped();
        yield* writeServerState(oldServer, { pid: 4242, withAttachSecret: false });
        const old = yield* probe(oldServer, { up: true, environmentId: ENVIRONMENT_ID });
        assert.isTrue(old._tag === "Down" && old.reason.includes("Update the server"));
      }),
    );
  });

  it.effect("follows the server down and back up, and hands out tokens it accepts", () =>
    Effect.gen(function* () {
      const stateDir = "/home/userdata";
      const server = yield* Ref.make<FakeServer>({ up: true, environmentId: ENVIRONMENT_ID });
      const events = yield* Queue.unbounded<Event>();
      const instance = yield* DesktopExternalBackend.makeExternalBackendInstance({
        id: DesktopBackendManager.PRIMARY_INSTANCE_ID,
        label: Effect.succeed("Local environment"),
        stateDir,
        onReady: (url) => Queue.offer(events, { _tag: "ready", url: url.href }),
        onShutdown: () => Queue.offer(events, { _tag: "shutdown" }),
        onStartupFailure: (failure) =>
          Queue.offer(events, { _tag: "failing", externalServer: failure.externalServer === true }),
      }).pipe(Effect.provide(Layer.merge(layerFakeServerHttp(server), layerFakeHome(stateDir))));
      const nextPoll = Effect.yieldNow.pipe(
        Effect.andThen(TestClock.adjust(DesktopExternalBackend.POLL_INTERVAL)),
      );

      yield* instance.start;
      assert.deepEqual(yield* Queue.take(events), { _tag: "ready", url: "http://127.0.0.1:3773/" });
      const config = yield* instance.currentConfig;
      assert.isTrue(Option.isSome(config));
      const token = Option.getOrThrow(config).bootstrap.desktopBootstrapToken;
      assert.isTrue(
        isValidDesktopBootstrapToken(encodeDesktopAttachSecret(ATTACH_SECRET), token, 0),
      );

      yield* Ref.set(server, { up: false, environmentId: ENVIRONMENT_ID });
      yield* nextPoll;
      assert.deepEqual(yield* Queue.take(events), { _tag: "shutdown" });
      yield* nextPoll;
      yield* nextPoll;
      assert.deepEqual(yield* Queue.take(events), { _tag: "failing", externalServer: true });
      // Clients keep the last address so they reconnect when a deploy restarts the server.
      assert.isTrue(Option.isSome(yield* instance.currentConfig));

      yield* Ref.set(server, { up: true, environmentId: ENVIRONMENT_ID });
      yield* nextPoll;
      assert.deepEqual(yield* Queue.take(events), { _tag: "ready", url: "http://127.0.0.1:3773/" });
      assert.isTrue((yield* instance.snapshot).ready);
      assert.isTrue(Option.isNone((yield* instance.snapshot).activePid));

      yield* instance.stop();
      assert.deepEqual(yield* Queue.take(events), { _tag: "shutdown" });
      assert.isFalse((yield* instance.snapshot).desiredRunning);
    }).pipe(Effect.provide(TestClock.layer())),
  );
});
