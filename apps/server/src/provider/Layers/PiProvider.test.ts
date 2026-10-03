import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { PI_FLAVOR, PRIME_AGENT_FLAVOR } from "../../orchestration-v2/Adapters/PiFlavor.ts";
import { checkPiProviderStatus } from "./PiProvider.ts";

const encoder = new TextEncoder();

function processHandle(input: {
  readonly stdout?: string;
  readonly stderr?: string;
  readonly exitCode?: number;
}) {
  const bytes = (value: string | undefined) =>
    value === undefined || value.length === 0
      ? Stream.empty
      : Stream.succeed(encoder.encode(value));
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(900_000_001),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(input.exitCode ?? 0)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: bytes(input.stdout),
    stderr: bytes(input.stderr),
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

function piProbeSpawner(version: string, binary = "pi") {
  return ChildProcessSpawner.make((command) => {
    const args = ChildProcess.isStandardCommand(command) ? command.args : [];
    return Effect.succeed(
      args.includes("--version")
        ? processHandle({ stdout: `${binary} ${version}\n` })
        : processHandle({ stderr: "RPC startup failed", exitCode: 1 }),
    );
  });
}

const settings = {
  enabled: true,
  binaryPath: "pi",
  launchArgs: "",
  customModels: [],
} as const;

describe("PiProvider", () => {
  it.effect("requires the first published Pi version with entries and settlement hooks", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(PI_FLAVOR, settings).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, piProbeSpawner("0.80.3")),
      );
      assert.equal(snapshot.status, "error");
      assert.equal(snapshot.version, "0.80.3");
      assert.include(snapshot.message ?? "", `Pi ${PI_FLAVOR.minimumVersion} or newer`);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps compatible Pi selectable when optional discovery fails", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(PI_FLAVOR, settings).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, piProbeSpawner("0.84.3")),
      );
      assert.equal(snapshot.status, "ready");
      assert.equal(snapshot.auth.status, "unknown");
      assert.deepEqual(
        snapshot.models.map((model) => model.slug),
        ["default"],
      );
      assert.include(snapshot.message ?? "", "could not refresh its models and commands");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("probes Prime Agent with its own binary, version floor, and runtime modes", () =>
    Effect.gen(function* () {
      const tooOld = yield* checkPiProviderStatus(PRIME_AGENT_FLAVOR, {
        ...settings,
        binaryPath: "",
      }).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          piProbeSpawner("0.9.5", "prime-agent"),
        ),
      );
      assert.equal(tooOld.status, "error");
      assert.include(tooOld.message ?? "", `Prime Agent ${PRIME_AGENT_FLAVOR.minimumVersion}`);

      const current = yield* checkPiProviderStatus(PRIME_AGENT_FLAVOR, {
        ...settings,
        binaryPath: "",
      }).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          piProbeSpawner("0.9.8", "prime-agent"),
        ),
      );
      assert.equal(current.status, "ready");
      assert.equal(current.displayName, "Prime Agent");
      assert.deepEqual(current.supportedRuntimeModes, ["approval-required", "full-access"]);
      assert.deepEqual(
        current.models.map((model) => model.name),
        ["Prime Agent default"],
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  for (const flavor of [PI_FLAVOR, PRIME_AGENT_FLAVOR]) {
    it.effect(`waits out a ${flavor.displayName} --version that a busy machine slows down`, () =>
      Effect.gen(function* () {
        const slowVersionSpawner = ChildProcessSpawner.make((command) => {
          const args = ChildProcess.isStandardCommand(command) ? command.args : [];
          return Effect.succeed(
            args.includes("--version")
              ? ChildProcessSpawner.makeHandle({
                  ...processHandle({}),
                  stdout: Stream.fromEffect(
                    Effect.sleep("8 seconds").pipe(
                      Effect.as(
                        encoder.encode(`${flavor.defaultBinary} ${flavor.minimumVersion}\n`),
                      ),
                    ),
                  ),
                })
              : processHandle({ stderr: "RPC startup failed", exitCode: 1 }),
          );
        });
        const probe = yield* checkPiProviderStatus(flavor, {
          ...settings,
          binaryPath: "",
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, slowVersionSpawner),
          Effect.forkChild,
        );
        yield* TestClock.adjust("8 seconds");
        const snapshot = yield* Fiber.join(probe);
        assert.equal(snapshot.status, "ready");
        assert.equal(snapshot.version, flavor.minimumVersion);
      }).pipe(Effect.provide(NodeServices.layer)),
    );
  }
});
