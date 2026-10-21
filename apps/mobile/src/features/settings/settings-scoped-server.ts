import {
  applyProjectProviderEnablement,
  isNullableProjectSettingsOverride,
  newThreadRuntimeMode,
  type NewThreadDefaults,
  PROJECT_SCOPED_SERVER_SETTING_KEYS,
  runtimeModesForNewThreads,
  type RuntimeMode,
  type EnvironmentId,
  type ProjectId,
  type ProjectScopedServerSettingKey,
  type ServerProvider,
  type ServerSettings,
  type ServerSettingsPatch,
} from "@t3tools/contracts";
import {
  clearProjectSettingsOverrides,
  resolveProjectSettings,
} from "@t3tools/shared/projectSettings";

import type { SettingsTarget } from "./settings-environment-filter";

export interface ScopedMobileSettingsTarget {
  readonly environment: SettingsTarget;
  readonly projectId: ProjectId | null;
  readonly settings: ServerSettings;
  readonly sources: ReturnType<typeof resolveProjectSettings>["sources"];
}

export function resolveMobileSettingsTargets(
  environments: readonly SettingsTarget[],
  members: readonly { readonly environmentId: EnvironmentId; readonly id: ProjectId }[] | null,
): readonly ScopedMobileSettingsTarget[] {
  if (members === null) {
    return environments.map((environment) => ({
      environment,
      projectId: null,
      ...resolveProjectSettings(environment.serverConfig.settings, null),
    }));
  }
  const byId = new Map(environments.map((environment) => [environment.environmentId, environment]));
  return members.flatMap((member) => {
    const environment = byId.get(member.environmentId);
    if (!environment) return [];
    return [
      {
        environment,
        projectId: member.id,
        ...resolveProjectSettings(environment.serverConfig.settings, member.id),
      },
    ];
  });
}

export function planMobileScopedSettingsPatch(
  targets: readonly ScopedMobileSettingsTarget[],
  projectSelected: boolean,
  patch: ServerSettingsPatch,
) {
  if (!projectSelected) {
    return targets.map((target) => ({ environmentId: target.environment.environmentId, patch }));
  }
  const keys = Object.keys(patch);
  if (
    keys.some(
      (key) => !PROJECT_SCOPED_SERVER_SETTING_KEYS.includes(key as ProjectScopedServerSettingKey),
    )
  )
    return [];
  const writes = new Map<EnvironmentId, Record<string, unknown>>();
  for (const target of targets) {
    if (
      target.projectId === null ||
      target.environment.serverConfig.environment.capabilities.projectSettingsOverrides !== true
    )
      continue;
    const current =
      target.environment.serverConfig.settings.projectSettingsOverrides[target.projectId] ?? {};
    const next: Record<string, unknown> = { ...current };
    for (const [key, value] of Object.entries(patch)) {
      if (
        value === null &&
        !isNullableProjectSettingsOverride(key as ProjectScopedServerSettingKey)
      ) {
        delete next[key];
      } else {
        next[key] = value;
      }
    }
    const overrides = writes.get(target.environment.environmentId) ?? {};
    overrides[target.projectId] = next;
    writes.set(target.environment.environmentId, overrides);
  }
  return [...writes].map(([environmentId, projectSettingsOverrides]) => ({
    environmentId,
    patch: { projectSettingsOverrides } as ServerSettingsPatch,
  }));
}

export function uniformMobileSetting<K extends keyof ServerSettings>(
  targets: readonly Pick<ScopedMobileSettingsTarget, "settings">[],
  key: K,
): ServerSettings[K] | null {
  const reference = targets[0];
  if (!reference) return null;
  const value = reference.settings[key];
  return targets.every((target) => target.settings[key] === value) ? value : null;
}

/** Provider statuses per environment; they live apart from the environment presentations. */
type ProvidersByEnvironment = ReadonlyMap<EnvironmentId, ReadonlyArray<ServerProvider>>;
type NewThreadTarget = Pick<ScopedMobileSettingsTarget, "environment" | "projectId" | "settings">;

const newThreadDefaults = (
  targets: readonly NewThreadTarget[],
  providersByEnvironment: ProvidersByEnvironment,
): NewThreadDefaults[] =>
  targets.flatMap((target) => {
    const providers = providersByEnvironment.get(target.environment.environmentId);
    return providers === undefined
      ? []
      : [
          {
            providers: applyProjectProviderEnablement(
              providers,
              target.environment.serverConfig.settings,
              target.projectId,
            ),
            defaultModelSelection: target.settings.defaultModelSelection,
            defaultRuntimeMode: target.settings.defaultRuntimeMode,
          },
        ];
  });

/** The default permissions new threads can run with on every target. */
export function offeredDefaultRuntimeModes(
  targets: readonly NewThreadTarget[],
  providersByEnvironment: ProvidersByEnvironment,
): ReadonlyArray<RuntimeMode> {
  return runtimeModesForNewThreads(newThreadDefaults(targets, providersByEnvironment));
}

/** The permissions new threads run with on every target, or null when they differ. */
export function displayedDefaultRuntimeMode(
  targets: readonly NewThreadTarget[],
  providersByEnvironment: ProvidersByEnvironment,
): RuntimeMode | null {
  return newThreadRuntimeMode(newThreadDefaults(targets, providersByEnvironment));
}

export function planMobileScopedSettingsClear(
  targets: readonly ScopedMobileSettingsTarget[],
  keys: readonly ProjectScopedServerSettingKey[],
) {
  const writes = new Map<EnvironmentId, Record<string, unknown>>();
  for (const target of targets) {
    if (
      target.projectId === null ||
      target.environment.serverConfig.environment.capabilities.projectSettingsOverrides !== true
    )
      continue;
    const overrides = writes.get(target.environment.environmentId) ?? {};
    overrides[target.projectId] = clearProjectSettingsOverrides(
      target.environment.serverConfig.settings,
      target.projectId,
      keys,
    );
    writes.set(target.environment.environmentId, overrides);
  }
  return [...writes].map(([environmentId, projectSettingsOverrides]) => ({
    environmentId,
    patch: { projectSettingsOverrides } as ServerSettingsPatch,
  }));
}
