import {
  type CustomModelSetting,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import type { PiSettings } from "../settings.ts";
import { causeErrorTag } from "@t3tools/shared/observability";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { compareSemverVersions } from "@t3tools/shared/semver";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import {
  buildPiRpcLaunch,
  piLaunchArgsForFlavor,
  rejectedPiLaunchArgsNotice,
  resolvePiLaunchArgs,
} from "./mcpInjection.ts";
import {
  makePiRpcConnection,
  PiRpcError,
  PiRpcTimeoutError,
  piRecordField as recordField,
  piRecordString as recordString,
} from "./rpc.ts";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import {
  identifyForkPiCli,
  identifyStockPiCli,
  piCliProbeForVersion,
  type PiCliIdentity,
  type PiFlavor,
} from "./flavor.ts";
import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  type ProviderProbeResult,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
  type ServerProviderPresentation,
} from "@t3tools/provider-core/server/snapshotProbe";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "@t3tools/provider-core/server/maintenanceResolver";
import {
  EMPTY_PI_MODEL_CAPABILITIES,
  thinkingCapabilitiesForPiModel,
} from "./thinkingCapabilities.ts";
import {
  parsePiDiscoveredCommands,
  withPiBuiltinSlashCommands,
  type PiDiscoveredCommands,
} from "./commands.ts";

const piPresentation = (flavor: PiFlavor): ServerProviderPresentation => ({
  displayName: flavor.displayName,
  // Pi left early access upstream; Prime Agent is still new here.
  ...(flavor.tools === "ipython" ? { badgeLabel: "Early Access" } : {}),
  showInteractionModeToggle: false,
  // Approvals come only from T3's extension, so a CLI without extensions
  // runs every tool unasked. Prime Agent runs every tool as one ipython cell,
  // so the approval hook cannot tell an edit from a command before it runs.
  supportedRuntimeModes: !flavor.extensions
    ? ["full-access"]
    : flavor.tools === "ipython"
      ? ["approval-required", "full-access"]
      : ["approval-required", "auto-accept-edits", "full-access"],
  reportsContextWindow: true,
  requiresNewThreadForModelChange: false,
});

// Both CLIs start a full agent runtime for these probes, and several agents
// often run on the same machine. A tight limit reads a busy machine as a
// broken install and drops the command list the composer offers.
const VERSION_PROBE_TIMEOUT_MS = 15_000;
const PI_RPC_DISCOVERY_TIMEOUT_MS = 30_000;
const piDefaultModel = (flavor: PiFlavor): ServerProviderModel => ({
  slug: "default",
  name: `${flavor.displayName} default`,
  isCustom: false,
  capabilities: EMPTY_PI_MODEL_CAPABILITIES,
});

interface PiDiscovery extends PiDiscoveredCommands {
  readonly models: ReadonlyArray<ServerProviderModel>;
  readonly authenticated: boolean;
}

function piModelsFromSettings(
  flavor: PiFlavor,
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
  discovered: ReadonlyArray<ServerProviderModel> = [],
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(
    [piDefaultModel(flavor), ...discovered],
    customModels ?? [],
    EMPTY_PI_MODEL_CAPABILITIES,
  );
}

function parseDiscoveredModels(data: unknown, state: unknown): ReadonlyArray<ServerProviderModel> {
  const models = recordField(data, "models");
  if (!Array.isArray(models)) return [];
  const activeModel = recordField(state, "model");
  const activeProvider = recordString(activeModel, "provider");
  const activeModelId = recordString(activeModel, "id");
  const seen = new Set<string>();
  const parsed: Array<ServerProviderModel> = [];
  for (const model of models) {
    const provider = recordString(model, "provider");
    const id = recordString(model, "id");
    if (provider === undefined || id === undefined) continue;
    const slug = `${provider}/${id}`;
    if (seen.has(slug)) continue;
    seen.add(slug);
    parsed.push({
      slug,
      name: recordString(model, "name") ?? slug,
      subProvider: provider,
      isCustom: false,
      capabilities: thinkingCapabilitiesForPiModel(
        model,
        provider === activeProvider && id === activeModelId
          ? recordString(state, "thinkingLevel")
          : undefined,
      ),
    });
  }
  return parsed;
}

const makePiDiscoveryConnection = Effect.fnUntraced(function* (
  flavor: PiFlavor,
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv,
  launchArgs: ReadonlyArray<string>,
  cwd?: string,
) {
  const launch = buildPiRpcLaunch({
    launchArgs,
    environment,
    mcpSession: undefined,
    extensionPath: undefined,
    ephemeral: true,
  });
  const connection = yield* makePiRpcConnection({
    command: piSettings.binaryPath || flavor.defaultBinary,
    args: launch.args,
    cwd,
    env: launch.env,
  });
  yield* Stream.fromQueue(connection.events).pipe(
    Stream.runDrain,
    Effect.ignore,
    Effect.forkScoped,
  );
  return connection;
});

const discoverPiViaRpc = (
  flavor: PiFlavor,
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv,
  launchArgs: ReadonlyArray<string>,
  cwd?: string,
) =>
  Effect.gen(function* () {
    const connection = yield* makePiDiscoveryConnection(
      flavor,
      piSettings,
      environment,
      launchArgs,
      cwd,
    );
    const stateData = yield* connection.request({ type: "get_state" });
    const modelsData = yield* connection.request({ type: "get_available_models" });
    const commandsData = yield* connection
      .request({ type: "get_commands" })
      .pipe(Effect.orElseSucceed(() => undefined));
    const discoveredModels = parseDiscoveredModels(modelsData, stateData);
    const { slashCommands, skills } = parsePiDiscoveredCommands(commandsData);
    return {
      models: discoveredModels,
      slashCommands: withPiBuiltinSlashCommands(slashCommands),
      skills,
      authenticated: discoveredModels.length > 0,
    } satisfies PiDiscovery;
  }).pipe(Effect.scoped);

export const discoverPiCommandsForCwd = Effect.fn("discoverPiCommandsForCwd")(
  function* (
    flavor: PiFlavor,
    piSettings: PiSettings,
    environment: NodeJS.ProcessEnv,
    cwd: string,
  ) {
    const launchArgs = resolvePiLaunchArgs(piSettings.launchArgs);
    if (!launchArgs.ok) {
      return yield* new PiRpcError({ operation: "launch", detail: launchArgs.message });
    }
    const cliFlavor = yield* detectPiFlavor(flavor, piSettings, environment).pipe(
      Effect.mapError((error) => new PiRpcError({ operation: "launch", detail: error.message })),
    );
    const connection = yield* makePiDiscoveryConnection(
      flavor,
      piSettings,
      environment,
      piLaunchArgsForFlavor(cliFlavor, launchArgs.args).accepted,
      cwd,
    );
    // A failed read must not replace a previously usable workspace catalog with an empty one.
    const commandsData = yield* connection.request({ type: "get_commands" });
    const { slashCommands, skills } = parsePiDiscoveredCommands(commandsData);
    return { slashCommands: withPiBuiltinSlashCommands(slashCommands), skills };
  },
  Effect.scoped,
  Effect.timeoutOrElse({
    duration: PI_RPC_DISCOVERY_TIMEOUT_MS,
    orElse: () =>
      Effect.fail(
        new PiRpcTimeoutError({ operation: "discovery", timeoutMs: PI_RPC_DISCOVERY_TIMEOUT_MS }),
      ),
  }),
);

const runPiCliCommand = (
  flavor: PiFlavor,
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv,
  flag: "--version" | "--help" | "--capabilities",
) =>
  Effect.gen(function* () {
    const command = piSettings.binaryPath || flavor.defaultBinary;
    const spawnCommand = yield* resolveSpawnCommand(command, [flag], {
      env: environment,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

/** The configured binary is a CLI of this driver that T3 could not identify or does not support. */
export class PiCliUnsupportedError extends Schema.TaggedError<PiCliUnsupportedError>()(
  "PiCliUnsupportedError",
  {
    binary: Schema.String,
    reason: Schema.String,
  },
) {
  override get message(): string {
    return `T3 Code did not launch \`${this.binary}\`: ${this.reason}.`;
  }
}

/**
 * The flavor the configured binary speaks, from its `--version` and then its
 * `--help` (below 0.10) or `--capabilities` (0.10 on, the T3 fork). A CLI T3
 * cannot identify is never launched. A caller that already read `--version`
 * passes its output.
 */
export const detectPiFlavor = (
  flavor: PiFlavor,
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv,
  versionOutput?: string,
): Effect.Effect<PiFlavor, PiCliUnsupportedError, ChildProcessSpawner.ChildProcessSpawner> => {
  if (flavor.successor === undefined) return Effect.succeed(flavor);
  const binary = piSettings.binaryPath || flavor.defaultBinary;
  const unsupported = (reason: string) => new PiCliUnsupportedError({ binary, reason });
  const run = (flag: "--version" | "--help" | "--capabilities") =>
    runPiCliCommand(flavor, piSettings, environment, flag).pipe(
      Effect.mapError((error) => unsupported(`\`${flag}\` failed to run: ${error.message}`)),
      Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
      Effect.flatMap((output) =>
        Option.isNone(output)
          ? Effect.fail(unsupported(`\`${flag}\` timed out after ${VERSION_PROBE_TIMEOUT_MS}ms`))
          : Effect.succeed(output.value),
      ),
    );
  const output = (flag: "--version" | "--help") =>
    run(flag).pipe(
      Effect.flatMap((result) =>
        result.code === 0
          ? Effect.succeed(`${result.stdout}\n${result.stderr}`)
          : Effect.fail(unsupported(`\`${flag}\` exited with code ${result.code}`)),
      ),
    );
  const launchable = (identity: PiCliIdentity) =>
    identity._tag === "Supported"
      ? Effect.succeed(identity.flavor)
      : Effect.fail(unsupported(identity.reason));
  const version = versionOutput === undefined ? output("--version") : Effect.succeed(versionOutput);
  return version.pipe(
    Effect.flatMap((versionText) => {
      const probe = piCliProbeForVersion(versionText);
      switch (probe._tag) {
        case "Unsupported":
          return Effect.fail(unsupported(probe.reason));
        case "Help":
          return output("--help").pipe(
            Effect.flatMap((help) => launchable(identifyStockPiCli(flavor, help))),
          );
        case "Capabilities":
          return run("--capabilities").pipe(
            Effect.flatMap((result) =>
              launchable(
                identifyForkPiCli(flavor, { exitCode: result.code, stdout: result.stdout }),
              ),
            ),
          );
      }
    }),
  );
};

export function buildInitialPiProviderSnapshot(
  flavor: PiFlavor,
  piSettings: PiSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = piModelsFromSettings(flavor, piSettings.customModels);
    if (!piSettings.enabled) {
      return buildServerProvider({
        presentation: piPresentation(flavor),
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: `${flavor.displayName} is disabled in T3 Code settings.`,
        },
      });
    }
    return buildServerProvider({
      presentation: piPresentation(flavor),
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: `Checking ${flavor.displayName} CLI availability...`,
      },
    });
  });
}

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  flavor: PiFlavor,
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
  cwd?: string,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = piModelsFromSettings(flavor, piSettings.customModels);

  if (!piSettings.enabled) {
    return buildServerProvider({
      presentation: piPresentation(flavor),
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: `${flavor.displayName} is disabled in T3 Code settings.`,
      },
    });
  }

  const versionResult = yield* runPiCliCommand(flavor, piSettings, environment, "--version").pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* Effect.logWarning(`${flavor.displayName} CLI health check failed.`, {
      errorTag: error._tag,
    });
    return buildServerProvider({
      presentation: piPresentation(flavor),
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? flavor.installHint
          : `Failed to execute ${flavor.displayName} CLI health check.`,
      },
    });
  }

  if (Option.isNone(versionResult.success)) {
    return buildServerProvider({
      presentation: piPresentation(flavor),
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: `${flavor.displayName} CLI is installed but timed out while running \`${flavor.defaultBinary} --version\`.`,
      },
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    return buildServerProvider({
      presentation: piPresentation(flavor),
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: `${flavor.displayName} CLI is installed but failed to run.`,
      },
    });
  }

  const detection = yield* detectPiFlavor(
    flavor,
    piSettings,
    environment,
    `${versionOutput.stdout}\n${versionOutput.stderr}`,
  ).pipe(Effect.result);
  if (Result.isFailure(detection)) {
    return buildServerProvider({
      presentation: piPresentation(flavor),
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: detection.failure.message,
      },
    });
  }
  const cliFlavor = detection.success;

  if (version === null) {
    return buildServerProvider({
      presentation: piPresentation(flavor),
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: `T3 Code could not determine the ${flavor.displayName} version. ${flavor.displayName} ${flavor.minimumVersion} or newer is required.`,
      },
    });
  }

  if (
    cliFlavor === flavor &&
    version !== null &&
    compareSemverVersions(version, flavor.minimumVersion) < 0
  ) {
    return buildServerProvider({
      presentation: piPresentation(flavor),
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: `${flavor.displayName} ${version} is unsupported. Update to ${flavor.displayName} ${flavor.minimumVersion} or newer.`,
      },
    });
  }

  const resolvedLaunchArgs = resolvePiLaunchArgs(piSettings.launchArgs);
  if (!resolvedLaunchArgs.ok) {
    return buildServerProvider({
      presentation: piPresentation(cliFlavor),
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: resolvedLaunchArgs.message,
      },
    });
  }

  const { accepted: launchArgs, rejected: rejectedLaunchArgs } = piLaunchArgsForFlavor(
    cliFlavor,
    resolvedLaunchArgs.args,
  );
  /** Launch args T3 left out are a setup problem only the user can fix. */
  const withLaunchArgsWarning = (probe: ProviderProbeResult): ProviderProbeResult =>
    rejectedLaunchArgs.length === 0
      ? probe
      : {
          ...probe,
          status: probe.status === "ready" ? "warning" : probe.status,
          message: [rejectedPiLaunchArgsNotice(cliFlavor, rejectedLaunchArgs), probe.message]
            .filter((part) => part !== undefined)
            .join(" "),
        };

  const discoveryExit = yield* discoverPiViaRpc(
    flavor,
    piSettings,
    environment,
    launchArgs,
    cwd,
  ).pipe(Effect.timeoutOption(PI_RPC_DISCOVERY_TIMEOUT_MS), Effect.exit);
  if (Exit.isFailure(discoveryExit)) {
    yield* Effect.logWarning(`${flavor.displayName} RPC discovery failed.`, {
      errorTag: causeErrorTag(discoveryExit.cause),
    });
    return buildServerProvider({
      presentation: piPresentation(cliFlavor),
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: withLaunchArgsWarning({
        installed: true,
        version,
        status: "ready",
        auth: { status: "unknown" },
        message: `${flavor.displayName} is available, but T3 Code could not refresh its models and commands. The live session will retry startup.`,
      }),
    });
  }
  if (Option.isNone(discoveryExit.value)) {
    return buildServerProvider({
      presentation: piPresentation(cliFlavor),
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: withLaunchArgsWarning({
        installed: true,
        version,
        status: "ready",
        auth: { status: "unknown" },
        message: `${flavor.displayName} is available, but model and command discovery needs interactive input. The live session will handle it.`,
      }),
    });
  }

  const discovery = discoveryExit.value.value;
  const models = piModelsFromSettings(flavor, piSettings.customModels, discovery.models);
  return buildServerProvider({
    presentation: piPresentation(cliFlavor),
    enabled: piSettings.enabled,
    checkedAt,
    models,
    slashCommands: discovery.slashCommands,
    skills: discovery.skills,
    probe: withLaunchArgsWarning({
      installed: true,
      version,
      status: discovery.authenticated ? "ready" : "warning",
      auth: {
        status: discovery.authenticated ? "authenticated" : "unauthenticated",
        type: flavor.defaultBinary,
      },
      ...(discovery.authenticated
        ? {}
        : {
            message: flavor.loginHint,
          }),
    }),
  });
});

export const enrichPiSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
}): Effect.Effect<
  void,
  never,
  HttpClient.HttpClient | ProviderLatestVersions.ProviderLatestVersions
> => {
  const { snapshot, publishSnapshot } = input;
  return enrichProviderSnapshotWithVersionAdvisory(snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.flatMap((enrichedSnapshot) => publishSnapshot(enrichedSnapshot)),
    Effect.catchCause((cause) =>
      Effect.logWarning("Pi version advisory enrichment failed", {
        driver: snapshot.driver,
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
};
