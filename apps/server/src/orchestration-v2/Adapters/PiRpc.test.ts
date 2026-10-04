import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";

import { makePiRpcConnection } from "./PiRpc.ts";

// Reports its pid, then idles until signalled, like an idle `pi --mode rpc`.
const IDLE_PI_SCRIPT = `console.log(JSON.stringify({ type: "ready", pid: process.pid })); setInterval(() => {}, 1000);`;

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
});
