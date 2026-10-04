import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";

import { makePiRpcConnection } from "./PiRpc.ts";

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
