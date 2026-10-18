import { describe, expect, it } from "vite-plus/test";

import {
  ProviderInstanceId,
  providerForNewThread,
  providerRuntimeMode,
  type ModelSelection,
  type RuntimeMode,
  type ServerConfig,
  type ServerProviderState,
} from "@t3tools/contracts";

import {
  displayedDefaultRuntimeMode,
  offeredDefaultRuntimeModes,
} from "../features/settings/settings-scoped-server";
import { scheduledTaskDefaultModel } from "../features/settings/scheduledTaskDraft";
import {
  buildModelOptions,
  resolveDefaultableModelSelection,
  resolveNewTaskModelSelection,
} from "./modelOptions";

type FixtureProvider = {
  readonly instanceId: string;
  readonly driver: string;
  readonly enabled: true;
  readonly installed: true;
  readonly status: ServerProviderState;
  readonly availability?: "available" | "unavailable";
  readonly auth: { readonly status: "authenticated" };
  readonly models: ReadonlyArray<{
    readonly slug: string;
    readonly name: string;
    readonly isDefault: true;
    readonly capabilities: null;
  }>;
  readonly supportedRuntimeModes?: ReadonlyArray<RuntimeMode>;
};

const provider = (
  instanceId: string,
  status: ServerProviderState,
  options: Partial<FixtureProvider> = {},
): FixtureProvider => ({
  instanceId,
  driver: instanceId,
  enabled: true,
  installed: true,
  status,
  auth: { status: "authenticated" },
  models: [{ slug: "default", name: "default", isDefault: true, capabilities: null }],
  ...options,
});

/** Runs only in Full access, like the Prime Agent fork. */
const fork = (status: ServerProviderState, options: Partial<FixtureProvider> = {}) =>
  provider("primeAgent", status, { supportedRuntimeModes: ["full-access"], ...options });
const codex = (status: ServerProviderState, options: Partial<FixtureProvider> = {}) =>
  provider("codex", status, options);

const configOf = (providers: ReadonlyArray<FixtureProvider>) =>
  ({ providers }) as unknown as ServerConfig;

const selectionOn = (instanceId: string): ModelSelection => ({
  instanceId: ProviderInstanceId.make(instanceId),
  model: "default",
});

/** The new-task flow's resolution of a fresh draft, as `new-task-flow-provider` wires it. */
function startOfFreshDraft(
  config: ServerConfig,
  defaultModelSelection: ModelSelection | null,
  stickySelection: ModelSelection | null = null,
) {
  const projectDefaultSelection = resolveDefaultableModelSelection(
    config,
    defaultModelSelection,
    null,
  );
  const sticky = resolveDefaultableModelSelection(config, stickySelection, null);
  return resolveNewTaskModelSelection({
    draftSelection: null,
    projectDefaultSelection,
    configuredDefaultSelection: defaultModelSelection,
    stickySelection: sticky,
    config,
    projectId: null,
    modelOptions: buildModelOptions(config, projectDefaultSelection ?? sticky, undefined, null),
  });
}

/** The provider every other surface starts a new thread on. */
function sharedStartProvider(
  providers: ReadonlyArray<FixtureProvider>,
  defaultModelSelection: ModelSelection | null,
) {
  return providerForNewThread(
    providers.map((candidate) => ({
      ...candidate,
      isAvailable: candidate.availability !== "unavailable",
    })),
    defaultModelSelection?.instanceId,
  );
}

/** What Settings shows and the server then runs for a thread stored as `stored`. */
function settingsAndServerModes(
  providers: ReadonlyArray<FixtureProvider>,
  defaultModelSelection: ModelSelection | null,
  startedOn: ModelSelection | null,
  stored: RuntimeMode,
) {
  const target = {
    environment: { environmentId: "fixture", serverConfig: configOf(providers) },
    projectId: null,
    settings: { defaultModelSelection, defaultRuntimeMode: stored },
  };
  const providersByEnvironment = new Map([["fixture", providers]]) as never;
  const actualProvider = providers.find(
    (candidate) => candidate.instanceId === startedOn?.instanceId,
  );
  return {
    shown: displayedDefaultRuntimeMode([target] as never, providersByEnvironment),
    offered: offeredDefaultRuntimeModes([target] as never, providersByEnvironment),
    runs: providerRuntimeMode(stored, actualProvider?.supportedRuntimeModes),
  };
}

const startScenarios: ReadonlyArray<{
  readonly name: string;
  readonly providers: ReadonlyArray<FixtureProvider>;
  readonly defaultModelSelection: ModelSelection | null;
}> = [
  {
    name: "a warning Codex ahead of a ready fork",
    providers: [codex("warning"), fork("ready")],
    defaultModelSelection: null,
  },
  {
    name: "a warning fork ahead of a ready Codex",
    providers: [fork("warning"), codex("ready")],
    defaultModelSelection: null,
  },
  {
    name: "two ready providers",
    providers: [codex("ready"), fork("ready")],
    defaultModelSelection: null,
  },
  {
    name: "an errored Codex ahead of a ready fork",
    providers: [codex("error"), fork("ready")],
    defaultModelSelection: null,
  },
  {
    name: "a default on an unavailable fork, with Codex ready",
    providers: [fork("ready", { availability: "unavailable" }), codex("ready")],
    defaultModelSelection: selectionOn("primeAgent"),
  },
  {
    name: "a default on an unavailable Codex, with the fork ready",
    providers: [codex("ready", { availability: "unavailable" }), fork("ready")],
    defaultModelSelection: selectionOn("codex"),
  },
  {
    name: "a default on a warning fork behind a ready Codex",
    providers: [codex("ready"), fork("warning")],
    defaultModelSelection: selectionOn("primeAgent"),
  },
  {
    name: "a default on a warning Codex behind a ready fork",
    providers: [fork("ready"), codex("warning")],
    defaultModelSelection: selectionOn("codex"),
  },
];

describe("the provider a fresh mobile thread starts on", () => {
  it.each(startScenarios)(
    "is the provider every surface starts on, for $name",
    ({ providers, defaultModelSelection }) => {
      const config = configOf(providers);
      const started = startOfFreshDraft(config, defaultModelSelection);

      expect(started?.instanceId).toBe(
        sharedStartProvider(providers, defaultModelSelection)?.instanceId,
      );

      const { shown, offered, runs } = settingsAndServerModes(
        providers,
        defaultModelSelection,
        started,
        "approval-required",
      );
      expect(shown).toBe(runs);
      expect(offered).toContain(runs);
    },
  );

  it.each(startScenarios)(
    "is the provider scheduled tasks start on, for $name",
    ({ providers, defaultModelSelection }) => {
      const config = {
        ...configOf(providers),
        settings: {
          defaultModelSelection,
          projectSettingsOverrides: {},
          // Settings record every provider the snapshot reports as enabled.
          providerInstances: Object.fromEntries(
            providers.map((provider) => [
              provider.instanceId,
              { driver: provider.driver, enabled: provider.enabled },
            ]),
          ),
        },
      } as unknown as ServerConfig;

      expect(scheduledTaskDefaultModel(config, null)?.instanceId).toBe(
        sharedStartProvider(providers, defaultModelSelection)?.instanceId,
      );
    },
  );

  it("keeps the device's last manual pick over the start provider", () => {
    const providers = [codex("warning"), fork("ready")];
    const sticky = selectionOn("codex");

    expect(startOfFreshDraft(configOf(providers), null, sticky)).toBe(sticky);
  });

  it("keeps an explicit draft pick over everything", () => {
    const providers = [codex("warning"), fork("ready")];
    const config = configOf(providers);
    const draft = selectionOn("codex");

    expect(
      resolveNewTaskModelSelection({
        draftSelection: draft,
        projectDefaultSelection: null,
        configuredDefaultSelection: null,
        stickySelection: null,
        config,
        projectId: null,
        modelOptions: buildModelOptions(config, draft, undefined, null),
      }),
    ).toBe(draft);
  });

  it("starts nowhere when no provider has a model to start with", () => {
    const providers = [codex("ready", { models: [] })];

    expect(startOfFreshDraft(configOf(providers), null)).toBeNull();
  });
});
