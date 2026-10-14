import {
  DEFAULT_SERVER_SETTINGS,
  type EnvironmentId,
  type ProjectId,
  type ProviderInstanceId,
  RuntimeMode,
  type ServerSettings,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import type { SettingsTarget } from "./settings-environment-filter";
import {
  displayedDefaultRuntimeMode,
  offeredDefaultRuntimeModes,
  planMobileScopedSettingsClear,
  planMobileScopedSettingsPatch,
  resolveMobileSettingsTargets,
  uniformMobileSetting,
} from "./settings-scoped-server";

const firstId = "first" as EnvironmentId;
const secondId = "second" as EnvironmentId;
const firstProject = "first-project" as ProjectId;
const secondProject = "second-project" as ProjectId;

describe("mobile usage-limit settings across environments", () => {
  it.each(["autoResumeLimitedThreads", "snoozeLimitedThreads"] as const)(
    "shows %s as mixed and can enable it everywhere without changing other settings",
    (key) => {
      const targets = resolveMobileSettingsTargets(
        [
          environment(firstId, { ...DEFAULT_SERVER_SETTINGS, [key]: true }),
          environment(secondId, { ...DEFAULT_SERVER_SETTINGS, [key]: false }),
        ],
        null,
      );

      expect(uniformMobileSetting(targets, key)).toBeNull();
      const writes = planMobileScopedSettingsPatch(targets, false, { [key]: true });
      expect(writes).toEqual([
        { environmentId: firstId, patch: { [key]: true } },
        { environmentId: secondId, patch: { [key]: true } },
      ]);
      const updatedTargets = targets.map((target) => ({
        ...target,
        settings: {
          ...target.settings,
          [key]:
            writes.find((write) => write.environmentId === target.environment.environmentId)?.patch[
              key
            ] ?? target.settings[key],
        },
      }));
      expect(uniformMobileSetting(updatedTargets, key)).toBe(true);
      expect(uniformMobileSetting(targets.slice(0, 1), key)).toBe(true);
      expect(uniformMobileSetting(targets.slice(1), key)).toBe(false);
      expect(uniformMobileSetting([], key)).toBeNull();
    },
  );
});

function environment(environmentId: EnvironmentId, settings: ServerSettings): SettingsTarget {
  return {
    environmentId,
    serverConfig: {
      settings,
      environment: { capabilities: { projectSettingsOverrides: true } },
    },
  } as SettingsTarget;
}

describe("mobile project settings scope", () => {
  it("edits each checkout's own override without changing either environment default", () => {
    const firstSettings: ServerSettings = {
      ...DEFAULT_SERVER_SETTINGS,
      responseStreamingMode: "paragraph",
      projectSettingsOverrides: { [firstProject]: { defaultAutoPull: true } },
    };
    const secondSettings: ServerSettings = {
      ...DEFAULT_SERVER_SETTINGS,
      responseStreamingMode: "turn",
      projectSettingsOverrides: {},
    };
    const targets = resolveMobileSettingsTargets(
      [environment(firstId, firstSettings), environment(secondId, secondSettings)],
      [
        { environmentId: firstId, id: firstProject },
        { environmentId: secondId, id: secondProject },
      ],
    );

    const writes = planMobileScopedSettingsPatch(targets, true, {
      responseStreamingMode: "turn",
    });
    expect(writes).toEqual([
      {
        environmentId: firstId,
        patch: {
          projectSettingsOverrides: {
            [firstProject]: { defaultAutoPull: true, responseStreamingMode: "turn" },
          },
        },
      },
      {
        environmentId: secondId,
        patch: { projectSettingsOverrides: { [secondProject]: { responseStreamingMode: "turn" } } },
      },
    ]);
    expect(firstSettings.responseStreamingMode).toBe("paragraph");
    expect(secondSettings.responseStreamingMode).toBe("turn");
  });

  it("removes a project override when a picker sends null for a key that cannot store it", () => {
    const settings: ServerSettings = {
      ...DEFAULT_SERVER_SETTINGS,
      projectSettingsOverrides: {
        [firstProject]: { defaultThreadEnvMode: "worktree", defaultAutoPull: true },
      },
    };
    const targets = resolveMobileSettingsTargets(
      [environment(firstId, settings)],
      [{ environmentId: firstId, id: firstProject }],
    );
    expect(planMobileScopedSettingsPatch(targets, true, { defaultThreadEnvMode: null })).toEqual([
      {
        environmentId: firstId,
        patch: { projectSettingsOverrides: { [firstProject]: { defaultAutoPull: true } } },
      },
    ]);
    expect(planMobileScopedSettingsPatch(targets, true, { defaultModelSelection: null })).toEqual([
      {
        environmentId: firstId,
        patch: {
          projectSettingsOverrides: {
            [firstProject]: {
              defaultThreadEnvMode: "worktree",
              defaultAutoPull: true,
              defaultModelSelection: null,
            },
          },
        },
      },
    ]);
  });

  it("resets only the selected page's override and rejects environment-wide writes", () => {
    const settings: ServerSettings = {
      ...DEFAULT_SERVER_SETTINGS,
      projectSettingsOverrides: {
        [firstProject]: { defaultAutoPull: true, responseStreamingMode: "turn" },
      },
    };
    const targets = resolveMobileSettingsTargets(
      [environment(firstId, settings)],
      [{ environmentId: firstId, id: firstProject }],
    );

    expect(planMobileScopedSettingsClear(targets, ["responseStreamingMode"])).toEqual([
      {
        environmentId: firstId,
        patch: { projectSettingsOverrides: { [firstProject]: { defaultAutoPull: true } } },
      },
    ]);
    expect(
      planMobileScopedSettingsPatch(targets, true, { enableProviderUpdateChecks: false }),
    ).toEqual([]);
  });
});

describe("mobile default permissions", () => {
  const primeAgent = "primeAgent" as ProviderInstanceId;
  const codex = "codex" as ProviderInstanceId;
  const withProviders = (
    target: SettingsTarget,
    providers: ReadonlyArray<{
      readonly instanceId: ProviderInstanceId;
      readonly supportedRuntimeModes?: ReadonlyArray<RuntimeMode>;
    }>,
  ) =>
    ({
      ...target,
      serverConfig: {
        ...target.serverConfig,
        providers: providers.map((provider) => ({ enabled: true, status: "ready", ...provider })),
      },
    }) as unknown as SettingsTarget;
  const forkDefault: ServerSettings = {
    ...DEFAULT_SERVER_SETTINGS,
    defaultRuntimeMode: "approval-required",
    defaultModelSelection: { instanceId: primeAgent, model: "default" },
  };

  it("offers only Full access when new threads default to the Prime Agent 0.10 fork", () => {
    const targets = resolveMobileSettingsTargets(
      [
        withProviders(environment(firstId, forkDefault), [
          { instanceId: primeAgent, supportedRuntimeModes: ["full-access"] },
        ]),
      ],
      null,
    );
    expect(offeredDefaultRuntimeModes(targets)).toEqual(["full-access"]);
  });

  it("offers only the modes every selected environment's default provider runs", () => {
    const targets = resolveMobileSettingsTargets(
      [
        withProviders(environment(firstId, forkDefault), [
          { instanceId: primeAgent, supportedRuntimeModes: ["full-access"] },
        ]),
        withProviders(
          environment(secondId, {
            ...DEFAULT_SERVER_SETTINGS,
            defaultModelSelection: { instanceId: codex, model: "gpt-5" },
          }),
          [{ instanceId: codex }],
        ),
      ],
      null,
    );
    expect(offeredDefaultRuntimeModes(targets)).toEqual(["full-access"]);
    expect(offeredDefaultRuntimeModes(targets.slice(1))).toEqual(RuntimeMode.literals);
  });

  it("offers only Full access when the default is unset and the fork is the only provider", () => {
    const targets = resolveMobileSettingsTargets(
      [
        withProviders(
          environment(firstId, { ...DEFAULT_SERVER_SETTINGS, defaultModelSelection: null }),
          [{ instanceId: primeAgent, supportedRuntimeModes: ["full-access"] }],
        ),
      ],
      null,
    );
    expect(offeredDefaultRuntimeModes(targets)).toEqual(["full-access"]);
  });

  it("shows the mode of the provider an unset default starts on, per environment", () => {
    const unset: ServerSettings = {
      ...DEFAULT_SERVER_SETTINGS,
      defaultRuntimeMode: "approval-required",
      defaultModelSelection: null,
    };
    const codexFirst = withProviders(environment(firstId, unset), [
      { instanceId: codex },
      { instanceId: primeAgent, supportedRuntimeModes: ["full-access"] },
    ]);
    const forkFirst = withProviders(environment(secondId, unset), [
      { instanceId: primeAgent, supportedRuntimeModes: ["full-access"] },
      { instanceId: codex },
    ]);
    const codexTargets = resolveMobileSettingsTargets([codexFirst], null);
    expect(displayedDefaultRuntimeMode(codexTargets)).toBe("approval-required");
    expect(offeredDefaultRuntimeModes(codexTargets)).toContain("approval-required");
    expect(displayedDefaultRuntimeMode(resolveMobileSettingsTargets([forkFirst], null))).toBe(
      "full-access",
    );
    expect(
      displayedDefaultRuntimeMode(resolveMobileSettingsTargets([codexFirst, forkFirst], null)),
    ).toBeNull();
  });

  it("follows a project's own provider switches to the provider its new threads start on", () => {
    const settings: ServerSettings = {
      ...DEFAULT_SERVER_SETTINGS,
      defaultRuntimeMode: "approval-required",
      defaultModelSelection: { instanceId: codex, model: "gpt-5" },
      projectSettingsOverrides: {
        // Codex is off here, so new threads start on the fork.
        [firstProject]: { providerInstances: { [codex]: { enabled: false } } },
      },
    };
    const target = withProviders(environment(firstId, settings), [
      { instanceId: codex },
      { instanceId: primeAgent, supportedRuntimeModes: ["full-access"] },
    ]);
    const members = [firstProject, secondProject].map((id) => ({ environmentId: firstId, id }));
    const [codexOff, codexOn] = resolveMobileSettingsTargets([target], members);

    expect(offeredDefaultRuntimeModes([codexOff!])).toEqual(["full-access"]);
    expect(displayedDefaultRuntimeMode([codexOff!])).toBe("full-access");
    expect(offeredDefaultRuntimeModes([codexOn!])).toEqual(RuntimeMode.literals);
    expect(displayedDefaultRuntimeMode([codexOn!])).toBe("approval-required");
    expect(displayedDefaultRuntimeMode([codexOff!, codexOn!])).toBeNull();
  });
});
