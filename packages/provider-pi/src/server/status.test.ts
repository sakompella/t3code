import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as PlatformError from "effect/PlatformError";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { TestClock } from "effect/testing";
import type { ServerProviderModel } from "@t3tools/contracts";

import { PI_FLAVOR, PRIME_AGENT_FLAVOR } from "./flavor.ts";
import { checkPiProviderStatus, detectPiFlavor, parseDiscoveredModels } from "./status.ts";
import {
  forkCapabilitiesWith,
  PRIME_AGENT_098_CAPABILITIES,
  PRIME_AGENT_098_HELP,
  PRIME_AGENT_FORK_CAPABILITIES,
  PRIME_AGENT_FORK_HELP,
  STOCK_PRIME_AGENT_010_CAPABILITIES,
} from "./primeAgentHelp.testkit.ts";

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

type ProbeOutput = Parameters<typeof processHandle>[0];

/**
 * A CLI that answers `--version`, `--help` and `--capabilities` as given (by
 * default as 0.9.8 does) and fails every RPC launch. `probes` records the
 * arguments of every run.
 */
function piProbeSpawner(
  version: string,
  binary = "pi",
  help: ProbeOutput = { stdout: PRIME_AGENT_098_HELP },
  capabilities: ProbeOutput = PRIME_AGENT_098_CAPABILITIES,
  probes: Array<ReadonlyArray<string>> = [],
) {
  return ChildProcessSpawner.make((command) => {
    const args = ChildProcess.isStandardCommand(command) ? command.args : [];
    probes.push(args);
    return Effect.succeed(
      args.includes("--version")
        ? processHandle({ stdout: `${binary} ${version}\n` })
        : args.includes("--help")
          ? processHandle(help)
          : args.includes("--capabilities")
            ? processHandle(capabilities)
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
      const tooOld = yield* checkPiProviderStatus(
        PRIME_AGENT_FLAVOR,
        {
          ...settings,
          binaryPath: "",
        },
        {},
      ).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          piProbeSpawner("0.9.5", "prime-agent"),
        ),
      );
      assert.equal(tooOld.status, "error");
      assert.include(tooOld.message ?? "", `Prime Agent ${PRIME_AGENT_FLAVOR.minimumVersion}`);

      const current = yield* checkPiProviderStatus(
        PRIME_AGENT_FLAVOR,
        {
          ...settings,
          binaryPath: "",
        },
        {},
      ).pipe(
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
      name: "0.9.8",
      version: "0.9.8",
      help: PRIME_AGENT_098_HELP,
      capabilities: PRIME_AGENT_098_CAPABILITIES,
      displayName: "Prime Agent",
      runtimeModes: supervisedOrFull,
    },
    {
      name: "the slice-4 fork",
      version: "0.10.0",
      help: PRIME_AGENT_FORK_HELP,
      capabilities: { stdout: PRIME_AGENT_FORK_CAPABILITIES },
      ...fork,
    },
    {
      name: "a fork that advertises more commands",
      version: "0.10.1",
      help: PRIME_AGENT_FORK_HELP,
      capabilities: {
        stdout: forkCapabilitiesWith({
          version: "0.10.1",
          rpc: [...JSON.parse(PRIME_AGENT_FORK_CAPABILITIES).rpc, "get_goal", "set_goal"],
          extra: { future: true },
        }),
      },
      ...fork,
    },
  ])(
    "probes Prime Agent $name as $displayName",
    ({ version, help, capabilities, displayName, runtimeModes }) =>
      Effect.gen(function* () {
        const snapshot = yield* checkPiProviderStatus(
          PRIME_AGENT_FLAVOR,
          {
            ...settings,
            binaryPath: "",
          },
          {},
        ).pipe(
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            piProbeSpawner(version, "prime-agent", { stdout: help }, capabilities),
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

  it.effect("asks for capabilities only with the one flag, and never asks 0.9.8", () =>
    Effect.gen(function* () {
      const withLaunchArgs = { ...settings, binaryPath: "", launchArgs: "--model openai/gpt-5" };
      const forkProbes: Array<ReadonlyArray<string>> = [];
      const forkFlavor = yield* detectPiFlavor(PRIME_AGENT_FLAVOR, withLaunchArgs, {}).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          piProbeSpawner(
            "0.10.0",
            "prime-agent",
            { stdout: PRIME_AGENT_FORK_HELP },
            { stdout: PRIME_AGENT_FORK_CAPABILITIES },
            forkProbes,
          ),
        ),
      );
      assert.isFalse(forkFlavor.extensions);
      assert.deepInclude(forkProbes, ["--capabilities"]);
      for (const args of forkProbes) {
        if (args.includes("--capabilities")) assert.deepEqual(args, ["--capabilities"]);
      }

      const stockProbes: Array<ReadonlyArray<string>> = [];
      const stockFlavor = yield* detectPiFlavor(PRIME_AGENT_FLAVOR, withLaunchArgs, {}).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          piProbeSpawner(
            "0.9.8",
            "prime-agent",
            { stdout: PRIME_AGENT_098_HELP },
            PRIME_AGENT_098_CAPABILITIES,
            stockProbes,
          ),
        ),
      );
      assert.isTrue(stockFlavor.extensions);
      // 0.9.8 runs print mode on an unknown flag, so it must never get one.
      assert.isFalse(stockProbes.some((args) => args.includes("--capabilities")));
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
        if (args.includes("--capabilities")) {
          return Effect.succeed(processHandle({ stdout: PRIME_AGENT_FORK_CAPABILITIES }));
        }
        launches.push(args);
        return Effect.succeed(processHandle({ stderr: "RPC startup failed", exitCode: 1 }));
      });
      const snapshot = yield* checkPiProviderStatus(
        PRIME_AGENT_FLAVOR,
        {
          ...settings,
          binaryPath: "",
          launchArgs: "--extension /tmp/user-extension.ts --no-tools --model openai/gpt-5",
        },
        {},
      ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
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

  const STOCK_010_REFUSAL = "stock Prime Agent 0.10 is not supported; install the T3 fork";
  const forkCli = (capabilities: ProbeOutput) => ({
    version: "0.10.0",
    help: { stdout: PRIME_AGENT_FORK_HELP },
    capabilities,
  });
  const undetectable: ReadonlyArray<{
    readonly name: string;
    readonly version: string;
    readonly help: ProbeOutput;
    readonly capabilities?: ProbeOutput;
    readonly reason?: string;
  }> = [
    { name: "a help run that fails", version: "0.9.8", help: { stderr: "boom", exitCode: 2 } },
    {
      name: "help that names no Prime Agent options",
      version: "0.9.8",
      help: { stdout: "usage: something else\n" },
    },
    {
      name: "stock Prime Agent 0.10, which has no --capabilities",
      ...forkCli(STOCK_PRIME_AGENT_010_CAPABILITIES),
      reason: STOCK_010_REFUSAL,
    },
    {
      name: "a +t3fork version suffix without the capabilities contract",
      ...forkCli(STOCK_PRIME_AGENT_010_CAPABILITIES),
      version: "0.10.0+t3fork.1",
      reason: STOCK_010_REFUSAL,
    },
    {
      name: "a version T3 cannot read",
      version: "nightly",
      help: { stdout: PRIME_AGENT_FORK_HELP },
    },
    {
      name: "a capabilities run that prints nothing",
      ...forkCli({ stdout: "", exitCode: 0 }),
      reason: "--capabilities",
    },
    {
      name: "capabilities that are not JSON",
      ...forkCli({ stdout: "fork=t3fork contract=1\n" }),
      reason: "--capabilities",
    },
    {
      name: "capabilities that are a JSON array",
      ...forkCli({ stdout: "[1]\n" }),
      reason: "--capabilities",
    },
    {
      name: "a valid contract from a failed run",
      ...forkCli({ stdout: PRIME_AGENT_FORK_CAPABILITIES, exitCode: 2 }),
    },
    {
      name: "another fork's marker",
      ...forkCli({ stdout: forkCapabilitiesWith({ fork: "otherfork" }) }),
      reason: "t3fork",
    },
    {
      name: "an unknown contract",
      ...forkCli({ stdout: forkCapabilitiesWith({ contract: 2 }) }),
      reason: "contract 2",
    },
    {
      name: "a contract given as a string",
      ...forkCli({ stdout: forkCapabilitiesWith({ contract: "1" }) }),
      reason: "contract",
    },
    {
      name: "a fractional contract",
      ...forkCli({ stdout: forkCapabilitiesWith({ contract: 1.5 }) }),
      reason: "contract",
    },
    {
      name: "capabilities without a version",
      ...forkCli({ stdout: forkCapabilitiesWith({ version: null }) }),
      reason: "--capabilities",
    },
    {
      name: "capabilities whose rpc list is not a list of names",
      ...forkCli({ stdout: forkCapabilitiesWith({ rpc: ["prompt", 7] }) }),
      reason: "--capabilities",
    },
    {
      name: "capabilities without navigate_tree",
      ...forkCli({
        stdout: forkCapabilitiesWith({
          rpc: JSON.parse(PRIME_AGENT_FORK_CAPABILITIES).rpc.filter(
            (command: string) => command !== "navigate_tree",
          ),
        }),
      }),
      reason: "navigate_tree",
    },
  ];
  it.effect.each(undetectable)(
    "refuses to launch Prime Agent from $name",
    ({ version, help, capabilities, reason }) =>
      Effect.gen(function* () {
        const exit = yield* detectPiFlavor(
          PRIME_AGENT_FLAVOR,
          { ...settings, binaryPath: "/opt/bin/prime-agent" },
          {},
        ).pipe(
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            piProbeSpawner(version, "prime-agent", help, capabilities),
          ),
          Effect.exit,
        );
        assert.isTrue(Exit.isFailure(exit), "an unknown CLI must not launch");
        if (Exit.isFailure(exit)) {
          assert.include(Cause.pretty(exit.cause), "/opt/bin/prime-agent");
          if (reason !== undefined) assert.include(Cause.pretty(exit.cause), reason);
        }

        const snapshot = yield* checkPiProviderStatus(PRIME_AGENT_FLAVOR, {
          ...settings,
          binaryPath: "/opt/bin/prime-agent",
        }).pipe(
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            piProbeSpawner(version, "prime-agent", help, capabilities),
          ),
        );
        assert.equal(snapshot.status, "error");
        assert.include(snapshot.message ?? "", "/opt/bin/prime-agent");
        if (reason !== undefined) assert.include(snapshot.message ?? "", reason);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  // The RPC commands the fork adapter sends; the contract must name each one.
  const COMMANDS_T3_CALLS = new Set([
    "prompt",
    "abort",
    "abort_retry",
    "compact",
    "new_session",
    "switch_session",
    "get_state",
    "get_available_models",
    "set_model",
    "set_thinking_level",
    "set_session_name",
    "get_messages",
    "get_session_stats",
    "get_fork_messages",
    "get_commands",
    "fork",
    "navigate_tree",
    "list_heartbeats",
    "observe",
    "unobserve",
  ]);
  it.effect("refuses a fork without a command T3 calls, and only then", () =>
    Effect.gen(function* () {
      const advertised: ReadonlyArray<string> = JSON.parse(PRIME_AGENT_FORK_CAPABILITIES).rpc;
      for (const dropped of advertised) {
        const exit = yield* detectPiFlavor(
          PRIME_AGENT_FLAVOR,
          { ...settings, binaryPath: "" },
          {},
        ).pipe(
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            piProbeSpawner("0.10.0", "prime-agent", undefined, {
              stdout: forkCapabilitiesWith({
                rpc: advertised.filter((command) => command !== dropped),
              }),
            }),
          ),
          Effect.exit,
        );
        if (COMMANDS_T3_CALLS.has(dropped)) {
          assert.isTrue(Exit.isFailure(exit), `a fork without ${dropped} must not launch`);
          if (Exit.isFailure(exit)) assert.include(Cause.pretty(exit.cause), dropped);
        } else {
          assert.isTrue(Exit.isSuccess(exit), `${dropped} is not needed`);
        }
      }
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

  it.effect("refuses a 0.10 CLI whose capabilities run never finishes", () =>
    Effect.gen(function* () {
      const probe = yield* detectPiFlavor(
        PRIME_AGENT_FLAVOR,
        { ...settings, binaryPath: "" },
        {},
      ).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make((command) => {
            const args = ChildProcess.isStandardCommand(command) ? command.args : [];
            return Effect.succeed(
              args.includes("--version")
                ? processHandle({ stdout: "prime-agent 0.10.0\n" })
                : ChildProcessSpawner.makeHandle({ ...processHandle({}), stdout: Stream.never }),
            );
          }),
        ),
        Effect.exit,
        Effect.forkChild,
      );
      yield* TestClock.adjust("1 minute");
      const exit = yield* Fiber.join(probe);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) assert.include(Cause.pretty(exit.cause), "--capabilities");
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

describe("parseDiscoveredModels", () => {
  const reasoningModel = (id: string, thinkingLevelMap: Record<string, unknown> = {}) => ({
    provider: "cpa",
    id,
    reasoning: true,
    thinkingLevelMap,
  });
  const defaultLevelOf = (model: ServerProviderModel | undefined) => {
    const thinking = model?.capabilities?.optionDescriptors?.find(({ id }) => id === "thinking");
    return thinking?.type === "select"
      ? thinking.options.find((option) => option.isDefault)?.id
      : undefined;
  };

  it("declares Pi's configured level as the default of every model, not only the active one", () => {
    const models = parseDiscoveredModels(
      {
        models: [
          reasoningModel("active", { xhigh: "xhigh" }),
          reasoningModel("other", { xhigh: "xhigh", max: "max" }),
        ],
      },
      { model: { provider: "cpa", id: "active" }, thinkingLevel: "high" },
    );
    assert.deepEqual(
      models.map((model) => [model.slug, defaultLevelOf(model)]),
      [
        ["cpa/active", "high"],
        ["cpa/other", "high"],
      ],
    );
  });

  it("clamps the configured level into each model's supported levels", () => {
    const models = parseDiscoveredModels(
      {
        models: [
          reasoningModel("active"),
          reasoningModel("no-max", { xhigh: "xhigh" }),
          reasoningModel("no-off", { off: null }),
        ],
      },
      { model: { provider: "cpa", id: "active" }, thinkingLevel: "off" },
    );
    assert.deepEqual(
      models.map((model) => defaultLevelOf(model)),
      ["off", "off", "minimal"],
    );
  });

  it("declares no default when the active model cannot think", () => {
    const models = parseDiscoveredModels(
      {
        models: [{ provider: "cpa", id: "plain", reasoning: false }, reasoningModel("thinker")],
      },
      { model: { provider: "cpa", id: "plain" }, thinkingLevel: "off" },
    );
    assert.deepEqual(
      models.map((model) => defaultLevelOf(model)),
      [undefined, undefined],
    );
  });
});

describe("detectPiFlavor identity cache", () => {
  const forkSpawner = (probes: Array<ReadonlyArray<string>>) =>
    piProbeSpawner(
      "0.10.0",
      "prime-agent",
      { stdout: PRIME_AGENT_FORK_HELP },
      { stdout: PRIME_AGENT_FORK_CAPABILITIES },
      probes,
    );
  const stockSpawner = (probes: Array<ReadonlyArray<string>>) =>
    piProbeSpawner(
      "0.9.8",
      "prime-agent",
      { stdout: PRIME_AGENT_098_HELP },
      PRIME_AGENT_098_CAPABILITIES,
      probes,
    );
  const stockOneTenSpawner = (probes: Array<ReadonlyArray<string>>) =>
    piProbeSpawner(
      "0.10.0",
      "prime-agent",
      { stdout: PRIME_AGENT_FORK_HELP },
      STOCK_PRIME_AGENT_010_CAPABILITIES,
      probes,
    );

  /** A real executable the detector can resolve, standing in for the Prime Agent binary. */
  const installBinary = (directory: string, name: string, contents: string) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = `${directory}/${name}`;
      yield* fileSystem.writeFileString(path, contents);
      yield* fileSystem.chmod(path, 0o755);
      return path;
    });

  const detect = (
    binaryPath: string,
    spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
    environment: NodeJS.ProcessEnv = {},
  ) =>
    detectPiFlavor(PRIME_AGENT_FLAVOR, { ...settings, binaryPath }, environment).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    );

  it.effect("probes a binary once, however often a session opens", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped();
        const binary = yield* installBinary(directory, "prime-agent", "#!/bin/sh\n");
        const probes: Array<ReadonlyArray<string>> = [];

        const first = yield* detect(binary, forkSpawner(probes));
        const probesForFirstOpen = probes.length;
        const second = yield* detect(binary, forkSpawner(probes));

        assert.isAbove(probesForFirstOpen, 0);
        assert.equal(probes.length, probesForFirstOpen, "the second open must not probe");
        assert.strictEqual(second, first);
        assert.isFalse(second.extensions);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("finds the same binary through PATH, and keeps stock 0.9.8 on its own flavor", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped();
        yield* installBinary(directory, "prime-agent", "#!/bin/sh\n");
        const environment = { PATH: directory };
        const probes: Array<ReadonlyArray<string>> = [];

        const first = yield* detect("prime-agent", stockSpawner(probes), environment);
        const probesForFirstOpen = probes.length;
        const second = yield* detect("prime-agent", stockSpawner(probes), environment);

        assert.equal(probes.length, probesForFirstOpen, "the second open must not probe");
        assert.strictEqual(second, first);
        assert.isTrue(second.extensions);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("probes again when the binary file changes, and per binary path", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped();
        const binary = yield* installBinary(directory, "prime-agent", "#!/bin/sh\n");
        const other = yield* installBinary(directory, "other-agent", "#!/bin/sh\n");
        const probes: Array<ReadonlyArray<string>> = [];

        yield* detect(binary, stockSpawner(probes));
        const afterFirst = probes.length;

        yield* detect(other, stockSpawner(probes));
        const afterOther = probes.length;
        assert.isAbove(afterOther, afterFirst, "another path is another binary");

        yield* fileSystem.writeFileString(binary, "#!/bin/sh\n# upgraded in place\n");
        const upgraded = yield* detect(binary, forkSpawner(probes));
        assert.isAbove(probes.length, afterOther, "a changed file must be probed again");
        assert.isFalse(upgraded.extensions, "and the new answer wins");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("probes again when same-size content replaces the file under a restored mtime", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped();
        const archiveTime = 1_700_000_000; // whole seconds, as an archive stores them
        const binary = yield* installBinary(directory, "prime-agent", "#!/bin/sh\n# stock\n");
        yield* fileSystem.utimes(binary, archiveTime, archiveTime);
        const probes: Array<ReadonlyArray<string>> = [];

        const stock = yield* detect(binary, stockSpawner(probes));
        assert.isTrue(stock.extensions);
        const afterFirst = probes.length;

        // What `tar -x`, `cp -p` and `rsync -t` do: new bytes, same size, same mtime.
        yield* fileSystem.writeFileString(binary, "#!/bin/sh\n# forks\n");
        yield* fileSystem.utimes(binary, archiveTime, archiveTime);
        const fork = yield* detect(binary, forkSpawner(probes));

        assert.isAbove(probes.length, afterFirst, "the replaced file must be probed again");
        assert.isFalse(fork.extensions);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps two shim names for one real file apart", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped();
        const manager = yield* installBinary(directory, "version-manager", "#!/bin/sh\n");
        const stockShim = `${directory}/stock-shim`;
        const forkShim = `${directory}/fork-shim`;
        yield* fileSystem.symlink(manager, stockShim);
        yield* fileSystem.symlink(manager, forkShim);
        const probes: Array<ReadonlyArray<string>> = [];

        const stock = yield* detect(stockShim, stockSpawner(probes));
        assert.isTrue(stock.extensions);
        const afterFirst = probes.length;
        const fork = yield* detect(forkShim, forkSpawner(probes));

        assert.isAbove(probes.length, afterFirst, "another shim name is another answer");
        assert.isFalse(fork.extensions);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "keeps no answer when the file was replaced after the status check read its version",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fileSystem = yield* FileSystem.FileSystem;
          const directory = yield* fileSystem.makeTempDirectoryScoped();
          const binary = yield* installBinary(directory, "prime-agent", "#!/bin/sh\n");
          const stock = stockSpawner([]);
          const replacedDuringVersionRead = ChildProcessSpawner.make((command) =>
            (ChildProcess.isStandardCommand(command) && command.args.includes("--version")
              ? fileSystem.writeFileString(binary, "#!/bin/sh\n# the fork replaced it\n")
              : Effect.void
            ).pipe(Effect.flatMap(() => stock.spawn(command))),
          );
          yield* checkPiProviderStatus(
            PRIME_AGENT_FLAVOR,
            { ...settings, binaryPath: binary },
            {},
          ).pipe(
            Effect.provideService(
              ChildProcessSpawner.ChildProcessSpawner,
              replacedDuringVersionRead,
            ),
          );

          const probes: Array<ReadonlyArray<string>> = [];
          const fork = yield* detect(binary, forkSpawner(probes));

          assert.isAbove(
            probes.length,
            0,
            "the replaced file is probed, not answered from the old version",
          );
          assert.isFalse(fork.extensions);
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("never keeps a refusal: a binary that gets fixed in place is launched", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = yield* fileSystem.makeTempDirectoryScoped();
        const binary = yield* installBinary(directory, "prime-agent", "#!/bin/sh\n");
        const probes: Array<ReadonlyArray<string>> = [];

        const refused = yield* detect(binary, stockOneTenSpawner(probes)).pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(refused));

        const fixed = yield* detect(binary, forkSpawner(probes));
        assert.isFalse(fixed.extensions);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});
