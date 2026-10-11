import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as PlatformError from "effect/PlatformError";
import * as Fiber from "effect/Fiber";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { TestClock } from "effect/testing";

import { PI_FLAVOR, PRIME_AGENT_FLAVOR } from "./flavor.ts";
import { checkPiProviderStatus, detectPiFlavor } from "./status.ts";
import { PRIME_AGENT_098_HELP, PRIME_AGENT_FORK_HELP } from "./primeAgentHelp.testkit.ts";

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

function piProbeSpawner(
  version: string,
  binary = "pi",
  help: Parameters<typeof processHandle>[0] = { stdout: PRIME_AGENT_098_HELP },
) {
  return ChildProcessSpawner.make((command) => {
    const args = ChildProcess.isStandardCommand(command) ? command.args : [];
    return Effect.succeed(
      args.includes("--version")
        ? processHandle({ stdout: `${binary} ${version}\n` })
        : args.includes("--help")
          ? processHandle(help)
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

  const supervisedOrFull = ["approval-required", "full-access"];
  const fork = { displayName: "Prime Agent (fork, 0.10)", runtimeModes: ["full-access"] };
  it.effect.each([
    {
      version: "0.9.8",
      help: PRIME_AGENT_098_HELP,
      displayName: "Prime Agent",
      runtimeModes: supervisedOrFull,
    },
    { version: "0.10.0", help: PRIME_AGENT_FORK_HELP, ...fork },
    { version: "0.10.1-beta.3", help: PRIME_AGENT_FORK_HELP, ...fork },
    { version: "0.10", help: PRIME_AGENT_FORK_HELP, ...fork },
    { version: "nightly", help: PRIME_AGENT_FORK_HELP, ...fork },
  ])(
    "probes Prime Agent $version as $displayName from what its help offers",
    ({ version, help, displayName, runtimeModes }) =>
      Effect.gen(function* () {
        const snapshot = yield* checkPiProviderStatus(PRIME_AGENT_FLAVOR, {
          ...settings,
          binaryPath: "",
        }).pipe(
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            piProbeSpawner(version, "prime-agent", { stdout: help }),
          ),
        );
        assert.equal(snapshot.status, "ready");
        assert.equal(snapshot.displayName, displayName);
        // The fork has no hook to ask before a tool runs.
        assert.deepEqual<ReadonlyArray<string> | undefined>(
          snapshot.supportedRuntimeModes,
          runtimeModes,
        );
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("warns about launch args the fork rejects and probes without them", () =>
    Effect.gen(function* () {
      const launches: Array<ReadonlyArray<string>> = [];
      const spawner = ChildProcessSpawner.make((command) => {
        const args = ChildProcess.isStandardCommand(command) ? command.args : [];
        if (args.includes("--version")) {
          return Effect.succeed(processHandle({ stdout: "prime-agent 0.10.0\n" }));
        }
        if (args.includes("--help")) {
          return Effect.succeed(processHandle({ stdout: PRIME_AGENT_FORK_HELP }));
        }
        launches.push(args);
        return Effect.succeed(processHandle({ stderr: "RPC startup failed", exitCode: 1 }));
      });
      const snapshot = yield* checkPiProviderStatus(PRIME_AGENT_FLAVOR, {
        ...settings,
        binaryPath: "",
        launchArgs: "--extension /tmp/user-extension.ts --no-tools --model openai/gpt-5",
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
      assert.equal(snapshot.status, "warning");
      assert.include(snapshot.message ?? "", "--extension /tmp/user-extension.ts --no-tools");
      assert.isAbove(launches.length, 0);
      for (const args of launches) {
        assert.notInclude(args, "--extension");
        assert.notInclude(args, "--no-tools");
        assert.include(args, "--model");
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  const undetectable = [
    { name: "a help run that fails", help: { stderr: "boom", exitCode: 2 } },
    { name: "help that names no Prime Agent options", help: { stdout: "usage: something else\n" } },
  ];
  it.effect.each(undetectable)("refuses to guess the Prime Agent flavor from $name", ({ help }) =>
    Effect.gen(function* () {
      const exit = yield* detectPiFlavor(
        PRIME_AGENT_FLAVOR,
        { ...settings, binaryPath: "/opt/bin/prime-agent" },
        {},
      ).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          piProbeSpawner("0.9.8", "prime-agent", help),
        ),
        Effect.exit,
      );
      assert.isTrue(Exit.isFailure(exit), "an unknown CLI must not launch as 0.9.8");
      if (Exit.isFailure(exit)) assert.include(Cause.pretty(exit.cause), "/opt/bin/prime-agent");

      const snapshot = yield* checkPiProviderStatus(PRIME_AGENT_FLAVOR, {
        ...settings,
        binaryPath: "/opt/bin/prime-agent",
      }).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          piProbeSpawner("0.9.8", "prime-agent", help),
        ),
      );
      assert.equal(snapshot.status, "error");
      assert.include(snapshot.message ?? "", "/opt/bin/prime-agent");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("refuses to guess the Prime Agent flavor from a help run that cannot start", () =>
    Effect.gen(function* () {
      const exit = yield* detectPiFlavor(
        PRIME_AGENT_FLAVOR,
        { ...settings, binaryPath: "" },
        {},
      ).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Effect.fail(
              PlatformError.systemError({
                _tag: "NotFound",
                module: "ChildProcess",
                method: "spawn",
                description: "prime-agent not found",
              }),
            ),
          ),
        ),
        Effect.exit,
      );
      assert.isTrue(Exit.isFailure(exit));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("refuses to guess the Prime Agent flavor from a help run that never finishes", () =>
    Effect.gen(function* () {
      const probe = yield* detectPiFlavor(
        PRIME_AGENT_FLAVOR,
        { ...settings, binaryPath: "" },
        {},
      ).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() =>
            Effect.succeed(
              ChildProcessSpawner.makeHandle({ ...processHandle({}), stdout: Stream.never }),
            ),
          ),
        ),
        Effect.exit,
        Effect.forkChild,
      );
      yield* TestClock.adjust("1 minute");
      assert.isTrue(Exit.isFailure(yield* Fiber.join(probe)));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each([PI_FLAVOR, PRIME_AGENT_FLAVOR])(
    "waits out a $displayName --version that a busy machine slows down",
    (flavor) =>
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
              : args.includes("--help")
                ? processHandle({ stdout: PRIME_AGENT_098_HELP })
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
});
