/**
 * PiDriver — v1 `ProviderDriver`s for the Pi coding agent and its hard fork
 * Prime Agent, composing the orchestrator-v2 adapter (`PiAdapterV2`), the
 * snapshot/probe layer (`PiProvider`), and Pi-backed text generation. The
 * two share one implementation and differ only by `PiFlavor`.
 *
 * Agent state (sessions, settings, extensions, auth) lives in the user's own
 * `~/.pi/agent` or `~/.prime/agent`, so continuation identity uses the
 * default instance grouping.
 */
import { PROVIDER_DISPLAY_NAMES, type ServerProvider } from "@t3tools/contracts";
import { PiSettings, PrimeAgentSettings } from "../settings.ts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { piUsageReader, primeAgentUsageReader } from "./usage.ts";
import { makePiTextGeneration } from "./textGeneration.ts";
import {
  PiAdapterV2Driver,
  PrimeAgentAdapterV2Driver,
  type PiAdapterV2DriverEnv,
} from "./adapter.ts";
import { PI_FLAVOR, PRIME_AGENT_FLAVOR, type PiFlavor } from "./flavor.ts";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import {
  buildInitialPiProviderSnapshot,
  checkPiProviderStatus,
  discoverPiCommandsForCwd,
  enrichPiSnapshot,
} from "./status.ts";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import type { ServerProviderDraft } from "@t3tools/provider-core/server/snapshotProbe";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import {
  makeCachedProviderMaintenanceResolution,
  makeManualOnlyProviderMaintenanceCapabilities,
  makePackageManagedProviderMaintenanceResolver,
  resolveProviderMaintenanceCapabilitiesEffect,
  type ProviderMaintenanceCapabilitiesResolver,
} from "@t3tools/provider-core/server/maintenanceResolver";
import type { ProviderUsageReader } from "@t3tools/provider-core/server/usage";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "@t3tools/provider-core/server/snapshotSettings";

const PI_UPDATE = makePackageManagedProviderMaintenanceResolver({
  provider: PI_FLAVOR.driverKind,
  npmPackageName: "@earendil-works/pi-coding-agent",
  nativeUpdate: { args: ["update", "--self"] },
});

// Prime Agent ships as versioned tarballs, not a package T3 can update.
const PRIME_AGENT_UPDATE: ProviderMaintenanceCapabilitiesResolver = {
  resolve: () =>
    Effect.succeed(
      makeManualOnlyProviderMaintenanceCapabilities({
        provider: PRIME_AGENT_FLAVOR.driverKind,
        packageName: null,
      }),
    ),
};

export type PiDriverEnv =
  | PiAdapterV2DriverEnv
  | ProviderHost.ProviderHost
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | ProviderLatestVersions.ProviderLatestVersions
  | Path.Path;

/**
 * An instance name equal to the driver's brand label is the default, not a
 * choice, so it does not hide the detected flavor's label ("Prime Agent
 * (fork, 0.10)"). A name the user picked still wins.
 */
export const withInstanceIdentity =
  (driverKind: PiFlavor["driverKind"]) =>
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: ServerProviderDraft): ServerProvider => ({
    ...snapshot,
    instanceId: input.instanceId,
    driver: driverKind,
    ...(input.displayName && input.displayName !== PROVIDER_DISPLAY_NAMES[driverKind]
      ? { displayName: input.displayName }
      : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

function makePiFlavorDriver(input: {
  readonly flavor: PiFlavor;
  readonly configSchema: typeof PiSettings | typeof PrimeAgentSettings;
  readonly defaultConfig: PiSettings;
  readonly adapterDriver: typeof PiAdapterV2Driver;
  readonly update: ProviderMaintenanceCapabilitiesResolver;
  readonly usage?: ProviderUsageReader<PiSettings, Path.Path>;
}): ProviderDriver<PiSettings, PiDriverEnv, Path.Path> {
  const { flavor, adapterDriver, update: UPDATE } = input;
  const DRIVER_KIND = flavor.driverKind;
  return {
    driverKind: DRIVER_KIND,
    metadata: {
      displayName: flavor.displayName,
      supportsMultipleInstances: true,
    },
    ...(input.usage ? { usage: input.usage } : {}),
    configSchema: input.configSchema,
    defaultConfig: (): PiSettings => input.defaultConfig,
    create: Effect.fn("PiDriver.create")(function* ({
      instanceId,
      displayName,
      accentColor,
      environment,
      enabled,
      config,
    }) {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const pathService = yield* Path.Path;
      const httpClient = yield* HttpClient.HttpClient;
      const latestVersions = yield* ProviderLatestVersions.ProviderLatestVersions;
      const host = yield* ProviderHost.ProviderHost;
      const { cwd } = host.paths;
      const processEnv = yield* mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity(DRIVER_KIND)({
        instanceId,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies PiSettings;
      const resolveMaintenance = yield* makeCachedProviderMaintenanceResolution(
        resolveProviderMaintenanceCapabilitiesEffect(UPDATE, {
          binaryPath: effectiveConfig.binaryPath,
          env: processEnv,
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, pathService),
        ),
      );

      const orchestrationAdapter = yield* adapterDriver
        .create({
          instanceId,
          displayName,
          accentColor,
          environment,
          enabled,
          config,
        })
        .pipe(
          Effect.mapError(
            (cause) =>
              new ProviderDriverError({
                driver: DRIVER_KIND,
                instanceId,
                detail: `Failed to build ${flavor.displayName} orchestration adapter.`,
                cause,
              }),
          ),
        );
      const textGeneration = yield* makePiTextGeneration(flavor, effectiveConfig, processEnv);

      const checkProvider = checkPiProviderStatus(flavor, effectiveConfig, processEnv, cwd).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );

      const snapshotSettings = yield* makeProviderSnapshotSettingsSource(effectiveConfig);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<PiSettings>>({
        resolveMaintenance,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialPiProviderSnapshot(flavor, settings.provider).pipe(
            Effect.map(stampIdentity),
          ),
        checkProvider,
        enrichSnapshot: ({ settings, snapshot: currentSnapshot, publishSnapshot }) =>
          resolveMaintenance().pipe(
            Effect.flatMap((maintenanceCapabilities) =>
              enrichPiSnapshot({
                snapshot: currentSnapshot,
                maintenanceCapabilities,
                enableProviderUpdateChecks: settings.enableProviderUpdateChecks,
                publishSnapshot,
              }),
            ),
            Effect.provideService(HttpClient.HttpClient, httpClient),
            Effect.provideService(ProviderLatestVersions.ProviderLatestVersions, latestVersions),
          ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build ${flavor.displayName} snapshot.`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        snapshotForCwd: (workspaceCwd) =>
          !effectiveConfig.enabled
            ? snapshot.getSnapshot
            : Effect.all([
                snapshot.getSnapshot,
                discoverPiCommandsForCwd(flavor, effectiveConfig, processEnv, workspaceCwd).pipe(
                  Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
                  Effect.mapError(
                    (cause) =>
                      new ProviderDriverError({
                        driver: DRIVER_KIND,
                        instanceId,
                        detail: `Failed to discover ${flavor.displayName} workspace commands.`,
                        cause,
                      }),
                  ),
                ),
              ]).pipe(
                Effect.map(([machineSnapshot, commands]) => ({
                  ...machineSnapshot,
                  ...commands,
                  slashCommandsPending: false,
                })),
              ),
        orchestrationAdapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
  };
}

export const PiDriver = makePiFlavorDriver({
  flavor: PI_FLAVOR,
  configSchema: PiSettings,
  defaultConfig: Schema.decodeSync(PiSettings)({}),
  adapterDriver: PiAdapterV2Driver,
  update: PI_UPDATE,
  usage: piUsageReader,
});

export const PrimeAgentDriver = makePiFlavorDriver({
  flavor: PRIME_AGENT_FLAVOR,
  configSchema: PrimeAgentSettings,
  defaultConfig: Schema.decodeSync(PrimeAgentSettings)({}),
  adapterDriver: PrimeAgentAdapterV2Driver,
  update: PRIME_AGENT_UPDATE,
  usage: primeAgentUsageReader,
});
