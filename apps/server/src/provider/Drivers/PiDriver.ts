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
import { PiSettings, PrimeAgentSettings, type ServerProvider } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import * as ServerConfig from "../../config.ts";
import * as ServerSettings from "../../serverSettings.ts";
import { makePiTextGeneration } from "../../textGeneration/PiTextGeneration.ts";
import {
  PiAdapterV2Driver,
  PrimeAgentAdapterV2Driver,
  type PiAdapterV2DriverEnv,
} from "../../orchestration-v2/Adapters/PiAdapterV2.ts";
import {
  PI_FLAVOR,
  PRIME_AGENT_FLAVOR,
  type PiFlavor,
} from "../../orchestration-v2/Adapters/PiFlavor.ts";
import { ProviderDriverError } from "../Errors.ts";
import {
  buildInitialPiProviderSnapshot,
  checkPiProviderStatus,
  enrichPiSnapshot,
} from "../Layers/PiProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  makeCachedProviderMaintenanceResolution,
  makeManualOnlyProviderMaintenanceCapabilities,
  makePackageManagedProviderMaintenanceResolver,
  resolveProviderMaintenanceCapabilitiesEffect,
  type ProviderMaintenanceCapabilitiesResolver,
} from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const PI_UPDATE = makePackageManagedProviderMaintenanceResolver({
  provider: PI_FLAVOR.driverKind,
  npmPackageName: "@earendil-works/pi-coding-agent",
  nativeUpdate: null,
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
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | Path.Path
  | ServerConfig.ServerConfig
  | ServerSettings.ServerSettingsService;

const withInstanceIdentity =
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
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

function makePiFlavorDriver(input: {
  readonly flavor: PiFlavor;
  readonly configSchema: typeof PiSettings | typeof PrimeAgentSettings;
  readonly defaultConfig: PiSettings;
  readonly adapterDriver: typeof PiAdapterV2Driver;
  readonly update: ProviderMaintenanceCapabilitiesResolver;
}): ProviderDriver<PiSettings, PiDriverEnv> {
  const { flavor, adapterDriver, update: UPDATE } = input;
  const DRIVER_KIND = flavor.driverKind;
  return {
    driverKind: DRIVER_KIND,
    metadata: {
      displayName: flavor.displayName,
      supportsMultipleInstances: true,
    },
    configSchema: input.configSchema,
    defaultConfig: (): PiSettings => input.defaultConfig,
    create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const fileSystem = yield* FileSystem.FileSystem;
        const pathService = yield* Path.Path;
        const httpClient = yield* HttpClient.HttpClient;
        const { cwd } = yield* ServerConfig.ServerConfig;
        const serverSettings = yield* ServerSettings.ServerSettingsService;
        const processEnv = mergeProviderInstanceEnvironment(environment);
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

        const snapshotSettings = makeProviderSnapshotSettingsSource(
          effectiveConfig,
          serverSettings,
        );
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
                  httpClient,
                }),
              ),
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
});

export const PrimeAgentDriver = makePiFlavorDriver({
  flavor: PRIME_AGENT_FLAVOR,
  configSchema: PrimeAgentSettings,
  defaultConfig: Schema.decodeSync(PrimeAgentSettings)({}),
  adapterDriver: PrimeAgentAdapterV2Driver,
  update: PRIME_AGENT_UPDATE,
});
