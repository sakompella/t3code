// @effect-diagnostics nodeBuiltinImport:off - the child must be a real OS process to observe its environment.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  HostProcessExecutablePath,
  HostProcessIsElectron,
  HostProcessIsExecutable,
  HostProcessPlatform,
} from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as NodeChildProcess from "node:child_process";

import { ensureAgentDeviceShim } from "./AgentDeviceShim.ts";

const onWindows = HostProcessPlatform.defaultValue() === "win32";

describe("agent-device shim", () => {
  it.effect.skipIf(onWindows)(
    "runs an Electron runtime as Node from a shell that does not inherit ELECTRON_RUN_AS_NODE",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped();
        const entryPath = path.join(directory, "entry.mjs");
        yield* fs.writeFileString(
          entryPath,
          "console.log(`ELECTRON_RUN_AS_NODE=${process.env.ELECTRON_RUN_AS_NODE}`);\n",
        );
        const shimDir = yield* ensureAgentDeviceShim({ entryPath, stateDir: directory }).pipe(
          Effect.provideService(HostProcessExecutablePath, process.execPath),
          Effect.provideService(HostProcessIsExecutable, false),
          Effect.provideService(HostProcessIsElectron, true),
        );
        const { ELECTRON_RUN_AS_NODE: _inherited, ...agentShellEnv } = process.env;
        const result = NodeChildProcess.spawnSync(
          path.join(shimDir, "agent-device"),
          ["--version"],
          {
            env: agentShellEnv,
            encoding: "utf8",
          },
        );
        expect(result.stdout.trim()).toBe("ELECTRON_RUN_AS_NODE=1");
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect.skipIf(onWindows)("sets nothing when the runtime is plain Node", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped();
      const shimDir = yield* ensureAgentDeviceShim({
        entryPath: path.join(directory, "entry.mjs"),
        stateDir: directory,
      }).pipe(
        Effect.provideService(HostProcessExecutablePath, process.execPath),
        Effect.provideService(HostProcessIsExecutable, false),
        Effect.provideService(HostProcessIsElectron, false),
      );
      expect(yield* fs.readFileString(path.join(shimDir, "agent-device"))).not.toContain(
        "ELECTRON_RUN_AS_NODE",
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("scopes the Windows launcher's environment to its own invocation", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped();
      const shimDir = yield* ensureAgentDeviceShim({
        entryPath: path.join(directory, "entry.mjs"),
        stateDir: directory,
      }).pipe(
        Effect.provideService(HostProcessPlatform, "win32"),
        Effect.provideService(HostProcessExecutablePath, "C:\\T3\\T3.exe"),
        Effect.provideService(HostProcessIsExecutable, false),
        Effect.provideService(HostProcessIsElectron, true),
      );
      const lines = (yield* fs.readFileString(path.join(shimDir, "agent-device.cmd"))).split(
        "\r\n",
      );
      const setlocal = lines.indexOf("setlocal");
      const assignment = lines.indexOf('set "ELECTRON_RUN_AS_NODE=1"');
      expect(setlocal).toBeGreaterThan(-1);
      expect(assignment).toBeGreaterThan(setlocal);
      expect(lines.some((line) => line.includes("endlocal"))).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
