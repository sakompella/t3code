import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
  UsageLimitSourceId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import {
  type LimitAccount,
  collectProviderUsageLimits,
  sameUsageLimitCommandCoverage,
  withUsageLimitsCommands,
  collectLimitAccounts,
  collectExternalUsageLinks,
  collectLimitNotices,
  collectLimitPools,
  elapsedShare,
  groupLimitWindows,
  paceOf,
  paceRemainingPercent,
  usesChatGptSharing,
} from "./usageLimits.ts";

const now = Date.parse("2026-09-03T12:00:00.000Z");
const FIVE_MINUTES = 5 * 60_000;

const window = {
  id: "five_hour",
  kind: "session",
  label: "Session",
  usedPercent: 40,
  windowDurationMins: 300,
  resetsAt: "2026-09-03T14:00:00.000Z",
} as const;

function provider(overrides: Partial<ServerProvider>): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make("codex"),
    driver: ProviderDriverKind.make("codex"),
    enabled: true,
    installed: true,
    version: null,
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-09-03T11:00:00.000Z",
    models: [],
    slashCommands: [],
    skills: [],
    ...overrides,
  };
}

describe("pace", () => {
  it("places the clock three fifths through a five-hour window with two hours left", () => {
    expect(elapsedShare(window, now)).toBeCloseTo(0.6);
    expect(paceRemainingPercent(window, now)).toBe(40);
    expect(paceOf(window, now)).toBe("under");
    expect(paceOf({ ...window, usedPercent: 62 }, now)).toBe("on");
    expect(paceOf({ ...window, usedPercent: 80 }, now)).toBe("ahead");
  });
});

describe("pools", () => {
  const checkedAt = "2026-09-03T11:00:00.000Z";
  const weekly = {
    id: "seven_day",
    kind: "weekly",
    label: "Weekly",
    windowDurationMins: 7 * 24 * 60,
    resetsAt: "2026-09-06T12:00:00.000Z",
  } as const;
  const claude = ProviderDriverKind.make("claudeAgent");
  const source = {
    id: UsageLimitSourceId.make("hub"),
    kind: "cliproxy" as const,
    label: "hub",
    checkedAt,
  };
  const laptop = { entry: { target: { label: "Laptop" } } };

  it("merges one account reported natively on two environments and by a hub into one entry", () => {
    const native = provider({
      driver: claude,
      instanceId: ProviderInstanceId.make("claude"),
      auth: { status: "authenticated", email: "Same@example.com" },
      usageLimits: { checkedAt, windows: [{ ...window, usedPercent: 40 }] },
    });
    const input = new Map([
      [EnvironmentId.make("env-a"), { ...laptop, serverConfig: { providers: [native] } }],
      [
        EnvironmentId.make("env-b"),
        {
          entry: { target: { label: "Desktop" } },
          serverConfig: {
            providers: [
              {
                ...native,
                usageLimits: {
                  checkedAt: "2026-09-03T11:30:00.000Z",
                  windows: [{ ...window, usedPercent: 55 }],
                },
              },
            ],
            usageLimitSources: [
              {
                ...source,
                accounts: [
                  {
                    id: "claude-same@example.com.json",
                    driver: claude,
                    email: "same@example.com",
                    plan: "Claude Subscription",
                    usageLimits: { checkedAt, windows: [{ ...window, usedPercent: 10 }] },
                  },
                ],
              },
            ],
          },
        },
      ],
    ]);
    const accounts = collectLimitAccounts(input);
    expect(accounts).toHaveLength(1);
    expect(accounts[0]).toMatchObject({
      key: "env-a:claude",
      sourceLabel: "Desktop · hub",
      redeem: { environmentId: "env-b", input: { instanceId: "claude" } },
      environments: [
        { environmentId: "env-a", label: "Laptop" },
        { environmentId: "env-b", label: "Desktop" },
      ],
    });
    expect(accounts[0]?.limits.windows[0]?.usedPercent).toBe(55);
  });

  it("skips a provider that is not signed in, even with limits from an earlier read", () => {
    const codex = provider({
      auth: { status: "unauthenticated" },
      usageLimits: { checkedAt, windows: [window] },
    });
    const presentations = (providers: ServerProvider[]) =>
      new Map([[EnvironmentId.make("env-a"), { ...laptop, serverConfig: { providers } }]]);
    expect(collectLimitAccounts(presentations([codex]))).toEqual([]);
    expect(
      collectLimitAccounts(presentations([{ ...codex, auth: { status: "authenticated" } }])),
    ).toHaveLength(1);
  });

  it("merges OpenCode Go limits from machines with the same API key", () => {
    const go = provider({
      driver: ProviderDriverKind.make("opencode"),
      instanceId: ProviderInstanceId.make("opencode"),
      auth: { status: "authenticated" },
      usageLimits: {
        checkedAt,
        credentialFingerprint: "shared-go-key",
        windows: [{ ...window, id: "go_rolling", usedPercent: 3 }],
      },
    });
    const input = new Map([
      [EnvironmentId.make("env-a"), { ...laptop, serverConfig: { providers: [go] } }],
      [
        EnvironmentId.make("env-b"),
        {
          entry: { target: { label: "Desktop" } },
          serverConfig: {
            providers: [
              {
                ...go,
                usageLimits: {
                  ...go.usageLimits!,
                  checkedAt: "2026-09-03T11:30:00.000Z",
                  windows: [{ ...window, id: "go_rolling", usedPercent: 4 }],
                },
              },
            ],
          },
        },
      ],
    ]);
    const accounts = collectLimitAccounts(input);
    expect(accounts).toHaveLength(1);
    expect(accounts[0]?.environments).toEqual([
      { environmentId: "env-a", label: "Laptop" },
      { environmentId: "env-b", label: "Desktop" },
    ]);
    expect(collectLimitPools(accounts, now)[0]?.windows[0]?.members).toHaveLength(1);
    expect(accounts[0]?.limits.windows[0]?.usedPercent).toBe(4);

    const differentKey = {
      ...go,
      usageLimits: { ...go.usageLimits!, credentialFingerprint: "other-go-key" },
    };
    input.set(EnvironmentId.make("env-b"), {
      entry: { target: { label: "Desktop" } },
      serverConfig: { providers: [differentKey] },
    });
    expect(collectLimitAccounts(input)).toHaveLength(2);

    input.set(EnvironmentId.make("env-a"), {
      ...laptop,
      serverConfig: {
        providers: [{ ...go, auth: { status: "authenticated", email: "same@example.com" } }],
      },
    });
    input.set(EnvironmentId.make("env-b"), {
      entry: { target: { label: "Desktop" } },
      serverConfig: {
        providers: [
          { ...differentKey, auth: { status: "authenticated", email: "SAME@example.com" } },
        ],
      },
    });
    expect(collectLimitAccounts(input)).toHaveLength(1);
  });

  it("takes windows from a fresher hub read but credits and redeem from the native instance", () => {
    const native = provider({
      driver: claude,
      instanceId: ProviderInstanceId.make("claude"),
      auth: { status: "authenticated", email: "same@example.com" },
      usageLimits: {
        checkedAt,
        windows: [{ ...window, usedPercent: 40 }],
        resetCredits: { availableCount: 2 },
      },
    });
    const input = new Map([
      [
        EnvironmentId.make("env-a"),
        {
          ...laptop,
          serverConfig: {
            providers: [native],
            usageLimitSources: [
              {
                ...source,
                accounts: [
                  {
                    id: "claude-same@example.com.json",
                    driver: claude,
                    email: "same@example.com",
                    usageLimits: {
                      checkedAt: "2026-09-03T11:30:00.000Z",
                      windows: [{ ...window, usedPercent: 55 }],
                    },
                  },
                ],
              },
            ],
          },
        },
      ],
    ]);
    const [account] = collectLimitAccounts(input);
    expect(account?.limits.windows[0]?.usedPercent).toBe(55);
    expect(account?.limits.resetCredits?.availableCount).toBe(2);
    expect(account?.redeem).toEqual({ environmentId: "env-a", input: { instanceId: "claude" } });
    expect(account?.environments).toEqual([{ environmentId: "env-a", label: "Laptop" }]);
  });

  it("redeems through the hub when it holds a credit, even with a fresher native read", () => {
    const native = provider({
      driver: claude,
      instanceId: ProviderInstanceId.make("claude"),
      auth: { status: "authenticated", email: "same@example.com" },
      usageLimits: {
        checkedAt: "2026-09-03T11:30:00.000Z",
        windows: [{ ...window, usedPercent: 40 }],
        resetCredits: { availableCount: 3, nextCreditId: "native-credit" },
      },
    });
    const input = new Map([
      [
        EnvironmentId.make("env-a"),
        {
          ...laptop,
          serverConfig: {
            providers: [native],
            usageLimitSources: [
              {
                ...source,
                accounts: [
                  {
                    id: "claude-same@example.com.json",
                    driver: claude,
                    email: "same@example.com",
                    usageLimits: {
                      checkedAt,
                      windows: [{ ...window, usedPercent: 55 }],
                      resetCredits: { availableCount: 2, nextCreditId: "hub-credit" },
                    },
                  },
                ],
              },
            ],
          },
        },
      ],
    ]);
    const [account] = collectLimitAccounts(input);
    expect(account?.redeem).toEqual({
      environmentId: "env-a",
      input: { sourceId: "hub", accountId: "claude-same@example.com.json", creditId: "hub-credit" },
    });
    expect(account?.limits.resetCredits?.availableCount).toBe(3);
  });

  it("redeems on the environment whose snapshot supplied the credits on show", () => {
    const stale = provider({
      auth: { status: "authenticated", email: "same@example.com" },
      usageLimits: {
        checkedAt,
        windows: [window],
        resetCredits: { availableCount: 0 },
      },
    });
    const fresh = {
      ...stale,
      usageLimits: {
        checkedAt: "2026-09-03T11:30:00.000Z",
        windows: [window],
        resetCredits: { availableCount: 2 },
      },
    };
    const input = new Map([
      [EnvironmentId.make("env-a"), { ...laptop, serverConfig: { providers: [stale] } }],
      [
        EnvironmentId.make("env-b"),
        { entry: { target: { label: "Desktop" } }, serverConfig: { providers: [fresh] } },
      ],
    ]);
    const [account] = collectLimitAccounts(input);
    expect(account?.limits.resetCredits?.availableCount).toBe(2);
    expect(account?.redeem).toEqual({ environmentId: "env-b", input: { instanceId: "codex" } });
  });

  it("keeps distinct hub accounts redeemable through their own source", () => {
    const hubAccounts = ["first", "second"].map((id) => ({
      id,
      driver: ProviderDriverKind.make("codex"),
      email: `${id}@example.com`,
      usageLimits: {
        checkedAt,
        windows: [window],
        resetCredits: { availableCount: 2, nextCreditId: `${id}-credit` },
      },
    }));
    const input = new Map([
      [
        EnvironmentId.make("env-a"),
        {
          ...laptop,
          serverConfig: {
            providers: [],
            usageLimitSources: [{ ...source, accounts: hubAccounts }],
          },
        },
      ],
    ]);
    expect(collectLimitAccounts(input).map((account) => account.redeem)).toEqual(
      hubAccounts.map((account) => ({
        environmentId: "env-a",
        input: { sourceId: "hub", accountId: account.id, creditId: `${account.id}-credit` },
      })),
    );
  });

  it("does not give old credits the timestamp of a newer window-only read", () => {
    const snapshots = [
      { checkedAt, windows: [window], resetCredits: { availableCount: 2 } },
      { checkedAt: "2026-09-03T12:00:00.000Z", windows: [window] },
      {
        checkedAt: "2026-09-03T11:30:00.000Z",
        windows: [window],
        resetCredits: { availableCount: 1 },
      },
    ];
    const input = new Map(
      snapshots.map((usageLimits, i) => [
        EnvironmentId.make(`env-${i}`),
        {
          ...laptop,
          serverConfig: {
            providers: [
              provider({
                auth: { status: "authenticated", email: "same@example.com" },
                usageLimits,
              }),
            ],
          },
        },
      ]),
    );
    const [account] = collectLimitAccounts(input);
    expect(account?.limits.checkedAt).toBe("2026-09-03T12:00:00.000Z");
    expect(account?.limits.resetCredits?.availableCount).toBe(1);
    expect(account?.redeem?.environmentId).toBe("env-2");
  });

  it("keeps one email in two workspaces apart while merging each workspace's duplicates", () => {
    const login = (instanceId: string, workspaceId: string, usedPercent: number) =>
      provider({
        instanceId: ProviderInstanceId.make(instanceId),
        auth: { status: "authenticated", email: "same@example.com", workspaceId },
        usageLimits: { checkedAt, windows: [{ ...weekly, usedPercent }] },
      });
    const hubAccount = (id: string, workspaceId?: string) => ({
      id,
      driver: ProviderDriverKind.make("codex"),
      email: "same@example.com",
      ...(workspaceId ? { workspaceId } : {}),
      usageLimits: { checkedAt, windows: [{ ...weekly, usedPercent: 50 }] },
    });
    const input = new Map([
      [
        EnvironmentId.make("env-a"),
        {
          ...laptop,
          serverConfig: {
            providers: [login("business", "ws-business", 8), login("personal", "ws-plus", 26)],
            usageLimitSources: [
              { ...source, accounts: [hubAccount("plus.json", "ws-plus"), hubAccount("x.json")] },
            ],
          },
        },
      ],
      [
        EnvironmentId.make("env-b"),
        {
          entry: { target: { label: "Desktop" } },
          serverConfig: { providers: [login("personal", "ws-plus", 26)] },
        },
      ],
    ]);
    const accounts = collectLimitAccounts(input);
    expect(accounts.map((account) => account.key)).toEqual([
      "env-a:business",
      "env-a:personal",
      "hub:x.json",
    ]);
    expect(accounts[0]?.limits.windows[0]?.usedPercent).toBe(8);
    const withoutEmail = (id: string, workspaceId: string) => {
      const { email: _email, ...account } = hubAccount(id, workspaceId);
      return account;
    };
    const sameFile = new Map([
      [
        EnvironmentId.make("env-a"),
        {
          ...laptop,
          serverConfig: {
            providers: [],
            usageLimitSources: [{ ...source, accounts: [withoutEmail("a.json", "ws-1")] }],
          },
        },
      ],
      [
        EnvironmentId.make("env-b"),
        {
          ...laptop,
          serverConfig: {
            providers: [],
            usageLimitSources: [{ ...source, accounts: [withoutEmail("a.json", "ws-2")] }],
          },
        },
      ],
    ]);
    expect(collectLimitAccounts(sameFile).map((account) => account.key)).toEqual([
      "hub:a.json:ws-1",
      "hub:a.json:ws-2",
    ]);
    expect(accounts[1]?.environments.map((environment) => environment.label)).toEqual([
      "Laptop",
      "Desktop",
    ]);
  });

  it("joins a report without a workspace to the only workspace signed in with its email", () => {
    const native = provider({
      driver: claude,
      auth: { status: "authenticated", email: "same@example.com", workspaceId: "org-a" },
      usageLimits: { checkedAt, windows: [window] },
    });
    const hubAccount = {
      id: "claude.json",
      driver: claude,
      email: "same@example.com",
      usageLimits: { checkedAt, windows: [window] },
    };
    const withOther = (other: ServerProvider) =>
      new Map([
        [
          EnvironmentId.make("env-a"),
          {
            ...laptop,
            serverConfig: {
              providers: [native, other],
              usageLimitSources: [{ ...source, accounts: [hubAccount] }],
            },
          },
        ],
      ]);
    const unrelated = provider({ instanceId: ProviderInstanceId.make("codex-2") });
    expect(collectLimitAccounts(withOther(unrelated)).map((account) => account.key)).toEqual([
      "env-a:codex",
    ]);
    const unreadable = provider({
      ...native,
      instanceId: ProviderInstanceId.make("team"),
      auth: { ...native.auth, workspaceId: "org-b" },
      usageLimits: { checkedAt, windows: [], unavailable: { reason: "probeFailed" } },
    });
    expect(collectLimitAccounts(withOther(unreadable)).map((account) => account.key)).toEqual([
      "env-a:codex",
      "hub:claude.json",
    ]);
  });

  it("keys a hub account without an email by hub, so two environments on one hub share it", () => {
    const seat = {
      id: "claude-team-seat.json",
      driver: claude,
      usageLimits: { checkedAt, windows: [window] },
    };
    const hub = { ...source, accounts: [seat] };
    const input = new Map([
      [
        EnvironmentId.make("env-a"),
        { ...laptop, serverConfig: { providers: [], usageLimitSources: [hub] } },
      ],
      [
        EnvironmentId.make("env-b"),
        {
          entry: { target: { label: "Desktop" } },
          serverConfig: { providers: [], usageLimitSources: [hub] },
        },
      ],
    ]);
    const accounts = collectLimitAccounts(input);
    expect(accounts.map((account) => account.key)).toEqual(["hub:claude-team-seat.json"]);
    expect(accounts[0]?.displayName).toBe("claude-team-seat");
  });

  it("pools Codex windows by kind, whichever slot reported them", () => {
    const codexAccount = (id: string, slots: readonly string[]) => ({
      id,
      driver: ProviderDriverKind.make("codex"),
      usageLimits: {
        checkedAt,
        windows: slots.map((slot) => ({ ...weekly, id: slot, usedPercent: 50 })),
      },
    });
    const pooled = (accounts: ReturnType<typeof codexAccount>[]) =>
      collectLimitPools(
        collectLimitAccounts(
          new Map([
            [
              EnvironmentId.make("env-a"),
              {
                ...laptop,
                serverConfig: { providers: [], usageLimitSources: [{ ...source, accounts }] },
              },
            ],
          ]),
        ),
        now,
      )[0]?.windows ?? [];
    for (const order of [
      [codexAccount("business", ["primary"]), codexAccount("plus", ["secondary"])],
      [codexAccount("plus", ["secondary"]), codexAccount("business", ["primary"])],
    ]) {
      const windows = pooled(order);
      expect(windows.map((window) => window.id)).toEqual(["weekly"]);
      expect(windows[0]?.members).toHaveLength(2);
    }
    expect(
      pooled([codexAccount("a", ["secondary"]), codexAccount("b", ["primary", "secondary"])]).map(
        (window) => window.id,
      ),
    ).toEqual(["weekly", "weekly:secondary"]);
  });

  it("pools windows by id across accounts and orders resets by when they land", () => {
    const checkedAt = "2026-09-03T11:58:00.000Z";
    const input = new Map([
      [
        EnvironmentId.make("env-a"),
        {
          ...laptop,
          serverConfig: {
            providers: [],
            usageLimitSources: [
              {
                ...source,
                accounts: [
                  {
                    id: "a",
                    driver: claude,
                    usageLimits: {
                      checkedAt,
                      windows: [
                        { ...window, usedPercent: 80, resetsAt: "2026-09-03T13:00:00.000Z" },
                        { ...weekly, usedPercent: 20 },
                      ],
                    },
                  },
                  {
                    id: "b",
                    driver: claude,
                    usageLimits: {
                      checkedAt,
                      windows: [{ ...window, usedPercent: 40 }],
                    },
                  },
                  {
                    id: "c",
                    driver: ProviderDriverKind.make("codex"),
                    usageLimits: { checkedAt, windows: [{ ...weekly, usedPercent: 50 }] },
                  },
                  {
                    id: "unsupported",
                    driver: claude,
                    usageLimits: {
                      checkedAt,
                      windows: [],
                      unavailable: { reason: "unsupported" as const },
                    },
                  },
                ],
              },
            ],
          },
        },
      ],
    ]);
    const pools = collectLimitPools(collectLimitAccounts(input), now);
    expect(pools.map((pool) => [pool.driver, pool.accounts.length])).toEqual([
      ["claudeAgent", 2],
      ["codex", 1],
    ]);
    const [session, week] = pools[0]!.windows;
    const untimed = collectLimitPools(
      collectLimitAccounts(input).map((account) =>
        account.key === "hub:b"
          ? {
              ...account,
              limits: {
                ...account.limits,
                windows: account.limits.windows.map((w) => ({ ...w, resetsAt: undefined })),
              },
            }
          : account,
      ),
      now,
    );
    expect(untimed[0]?.windows[0]?.pace).toBe("on");
    expect(session).toMatchObject({
      id: "five_hour",
      remainingPercent: 40,
      usedPercent: 60,
      pace: "under",
    });
    expect(
      session?.resets.map((reset) => [reset.member.account.key, reset.restoresPercent]),
    ).toEqual([
      ["hub:a", 40],
      ["hub:b", 20],
    ]);
    expect(week).toMatchObject({ id: "seven_day", remainingPercent: 80, members: [{}] });
    // Codex reports `primary` for both its five-hour and (on Go) monthly window.
    const mixed = collectLimitPools(
      [
        ...collectLimitAccounts(input),
        {
          key: "go",
          driver: claude,
          displayName: "Go",
          email: undefined,
          plan: undefined,
          accentColor: undefined,
          environments: [],
          sourceLabel: null,
          redeem: null,
          refreshIntervalMs: FIVE_MINUTES,
          limits: {
            checkedAt,
            windows: [
              {
                id: "five_hour",
                kind: "monthly",
                label: "Monthly",
                usedPercent: 82,
                windowDurationMins: 30 * 24 * 60,
                resetsAt: "2026-09-14T12:00:00.000Z",
              },
            ],
          },
        },
      ],
      now,
    );
    expect(mixed[0]?.windows.map((window) => [window.kind, window.members.length])).toEqual([
      ["session", 2],
      ["weekly", 1],
      ["monthly", 1],
    ]);
    expect(session?.members.map((member) => member.account.key)).toEqual(["hub:a", "hub:b"]);
    expect(pools[0]?.accounts.map((account) => account.key)).toEqual(["hub:a", "hub:b"]);
  });
});

describe("pooled account columns", () => {
  const weekly = {
    ...window,
    id: "seven_day",
    kind: "weekly",
    label: "Weekly",
    windowDurationMins: 7 * 24 * 60,
  } as const;
  const account = (key: string, windows: LimitAccount["limits"]["windows"]): LimitAccount => ({
    key,
    driver: ProviderDriverKind.make("claudeAgent"),
    displayName: key,
    email: undefined,
    plan: undefined,
    accentColor: undefined,
    environments: [],
    sourceLabel: "Hub",
    redeem: null,
    refreshIntervalMs: FIVE_MINUTES,
    limits: { checkedAt: "2026-09-03T11:58:00.000Z", windows },
  });
  const keys = (pool: ReturnType<typeof collectLimitPools>[number]) =>
    pool.windows.map((row) =>
      row.columns.map((member) => (member.window ? member.account.key : null)),
    );

  describe("grouped by source", () => {
    const hub = (key: string, usedPercent: number, sourceLabel = "Hub") => ({
      ...account(key, [{ ...window, usedPercent }]),
      sourceLabel,
    });
    const direct = (key: string, usedPercent: number): LimitAccount => ({
      ...account(key, [{ ...window, usedPercent }]),
      sourceLabel: null,
      environments: [{ environmentId: EnvironmentId.make("local"), label: "Local" }],
    });
    const grouped = (accounts: readonly LimitAccount[]) =>
      groupLimitWindows(collectLimitPools(accounts, now)[0]!, now);

    it("puts the hub first and pools each group on its own", () => {
      const [session] = grouped([direct("own-a", 10), hub("hub-a", 80), hub("hub-b", 60)]);
      expect(
        session?.groups.map((group) => [
          group.label,
          group.window.remainingPercent,
          group.window.columns.map((column) => column.account.key),
        ]),
      ).toEqual([
        ["CLIProxyAPI", 30, ["hub-a", "hub-b"]],
        ["Direct logins", 90, ["own-a"]],
      ]);
    });

    it("lists a hub account that is also signed in directly under its hub", () => {
      const both = { ...hub("both", 20), environments: direct("x", 0).environments };
      const [session] = grouped([both, hub("hub-b", 60), direct("own-a", 10)]);
      expect(
        session?.groups.map((group) => [
          group.label,
          group.window.columns.map((column) => column.account.key),
        ]),
      ).toEqual([
        ["CLIProxyAPI", ["both", "hub-b"]],
        ["Direct logins", ["own-a"]],
      ]);
    });

    it("names several hubs by their own label, in label order, before direct logins", () => {
      const [session] = grouped([
        direct("own-a", 10),
        hub("zed", 50, "Zed hub"),
        hub("alpha", 50, "Alpha hub"),
      ]);
      expect(session?.groups.map((group) => group.label)).toEqual([
        "Alpha hub",
        "Zed hub",
        "Direct logins",
      ]);
    });

    it.each([
      ["hub", [hub("hub-a", 80), hub("hub-b", 60)]],
      ["direct", [direct("own-a", 10), direct("own-b", 30)]],
    ])("keeps one unlabeled group when only %s accounts exist", (_, accounts) => {
      const [session] = grouped(accounts);
      expect(session?.groups).toEqual([{ label: null, window: session?.window }]);
    });

    it("omits a group from a window it has no reading for, and keeps labels on the rest", () => {
      const own = direct("own-a", 10);
      const weeklyOnly = { ...own, limits: { ...own.limits, windows: [weekly] } };
      expect(
        grouped([hub("hub-a", 80), weeklyOnly]).map(({ window, groups }) => [
          window.kind,
          groups.map((group) => group.label),
        ]),
      ).toEqual([
        ["session", ["CLIProxyAPI"]],
        ["weekly", ["Direct logins"]],
      ]);
    });
  });

  // regression: #20349 sessions stay visible while the weekly quota is exhausted
  it.each([
    ["claudeAgent", "13:00", 40, "14:00", 100],
    ["codex", "14:00", 100, "14:00", 100],
    ["claudeAgent", "15:00", 100, "15:00", 100],
    ["codex", "15:00", 40, "14:00", 60],
  ] as const)(
    "%s session resetting at %s with %i percent spent becomes usable at %s with %i percent restored",
    (driver, sessionReset, usedPercent, usableAt, restoresPercent) => {
      const at = (time: string) => `2026-09-03T${time}:00.000Z`;
      const a = {
        ...account("a", [
          {
            ...window,
            id: driver === "codex" ? "primary" : window.id,
            usedPercent,
            resetsAt: at(sessionReset),
          },
          {
            ...weekly,
            id: driver === "codex" ? "secondary" : weekly.id,
            usedPercent: 100,
            resetsAt: at("14:00"),
          },
        ]),
        driver: ProviderDriverKind.make(driver),
      };
      const [session] = collectLimitPools([a], now)[0]!.windows;
      expect.soft(session?.remainingPercent).toBe(0);
      expect(session?.resets[0]).toMatchObject({
        at: Date.parse(at(usableAt)),
        restoresPercent,
      });
    },
  );

  it("excludes weekly-blocked sessions from the pool without changing snapshots or model allowances", () => {
    const blocked = account("blocked", [
      window,
      { ...weekly, usedPercent: 100 },
      { ...weekly, id: "seven_day_fable", usedPercent: 20 },
    ]);
    const accounts = [
      blocked,
      account("available", [
        { ...window, usedPercent: 20 },
        { ...weekly, usedPercent: 99 },
        { ...weekly, id: "seven_day_fable", usedPercent: 40 },
      ]),
      { ...blocked, key: "cursor", driver: ProviderDriverKind.make("cursor") },
    ];
    const snapshot = structuredClone(accounts);
    const [claude, cursor] = collectLimitPools(accounts, now);
    expect.soft(claude?.windows.map((row) => row.remainingPercent)).toEqual([40, 1, 70]);
    expect.soft(cursor?.windows.map((row) => row.remainingPercent)).toEqual([60, 0, 80]);
    expect(accounts).toEqual(snapshot);
  });

  it("keeps session columns across rows with opposite reset and usage orders", () => {
    const accounts = [
      account("a", [
        { ...weekly, usedPercent: 80, resetsAt: "2026-09-05T12:00:00.000Z" },
        { ...window, usedPercent: 10, resetsAt: "2026-09-03T15:00:00.000Z" },
      ]),
      account("b", [
        { ...weekly, usedPercent: 20, resetsAt: "2026-09-06T12:00:00.000Z" },
        { ...window, usedPercent: 90, resetsAt: "2026-09-03T13:00:00.000Z" },
      ]),
    ];
    const [pool] = collectLimitPools(accounts, now);
    expect(pool!.accounts.map((account) => account.key)).toEqual(["b", "a"]);
    expect(keys(pool!)).toEqual([
      ["b", "a"],
      ["b", "a"],
    ]);
    expect(pool!.windows[1]!.resets.map((reset) => reset.member.account.key)).toEqual(["a", "b"]);
    expect(pool!.windows[1]!.remainingPercent).toBe(50);
    expect(keys(collectLimitPools(accounts.toReversed(), now)[0]!)).toEqual(keys(pool!));
  });

  it.each([undefined, "invalid", "2026-09-03T11:30:00.000Z"])(
    "keeps blocked sessions empty without predicting a refill from a missing or stale clock (%s)",
    (resetsAt) => {
      const [pool] = collectLimitPools(
        [
          account("session-clock", [
            { ...window, resetsAt },
            { ...weekly, usedPercent: 100 },
          ]),
          account("weekly-clock", [window, { ...weekly, usedPercent: 100, resetsAt }]),
        ],
        now,
      );
      expect(pool!.windows[0]).toMatchObject({ remainingPercent: 0, pace: null, resets: [] });
      expect(pool!.windows[0]!.columns.map((column) => column.window?.resetsAt)).toEqual([
        undefined,
        undefined,
      ]);
    },
  );

  it("orders by usable session refill and clears suppression after a fresh weekly reading", () => {
    const blocked = account("blocked", [
      { ...window, resetsAt: "2026-09-03T13:00:00.000Z" },
      { ...weekly, usedPercent: 100, resetsAt: "2026-09-03T15:00:00.000Z" },
    ]);
    const available = account("available", [window]);
    const [before] = collectLimitPools([blocked, available], now);
    expect(before!.accounts.map((account) => account.key)).toEqual(["available", "blocked"]);
    expect(before!.windows[0]!.remainingPercent).toBe(30);
    const refreshed = {
      ...blocked,
      limits: {
        checkedAt: "2026-09-03T12:00:00.000Z",
        windows: [blocked.limits.windows[0]!, { ...weekly, usedPercent: 10 }],
      },
    };
    const [after] = collectLimitPools([refreshed, available], now);
    expect(after!.accounts.map((account) => account.key)).toEqual(["blocked", "available"]);
    expect(after!.windows[0]!.remainingPercent).toBe(60);
    expect(after!.windows[0]!.resets[0]).toMatchObject({
      at: Date.parse("2026-09-03T13:00:00.000Z"),
      restoresPercent: 20,
    });
  });

  it("preserves gaps without counting missing windows toward pooled quota", () => {
    const [pool] = collectLimitPools(
      [
        account("a", [window]),
        account("b", [
          { ...window, resetsAt: "2026-09-03T15:00:00.000Z" },
          { ...weekly, usedPercent: 80 },
        ]),
        account("c", [weekly]),
      ],
      now,
    );
    expect(keys(pool!)).toEqual([
      ["a", "b", null],
      [null, "b", "c"],
    ]);
    expect(pool!.windows[1]!.members.map((member) => member.account.key)).toEqual(["b", "c"]);
    expect(pool!.windows[1]!.remainingPercent).toBe(40);
    expect(pool!.windows[1]!.resets.map((reset) => reset.restoresPercent)).toEqual([40, 20]);
  });

  it("falls back to weekly resets when no account reports a session", () => {
    const [pool] = collectLimitPools(
      [
        account("a", [{ ...weekly, resetsAt: "2026-09-06T12:00:00.000Z" }]),
        account("b", [{ ...weekly, resetsAt: "2026-09-05T12:00:00.000Z" }]),
      ],
      now,
    );
    expect(keys(pool!)).toEqual([["b", "a"]]);
  });

  describe("a window whose reset time has passed", () => {
    const rolledOver = {
      ...window,
      usedPercent: 94,
      resetsAt: "2026-09-03T11:59:00.000Z",
    } as const;
    const readBefore = (key: string, windows: LimitAccount["limits"]["windows"]) => {
      const reading = account(key, windows);
      return { ...reading, limits: { ...reading.limits, checkedAt: "2026-09-03T11:57:00.000Z" } };
    };

    it("is full again, with no reset clock, pace or refill, and adds none of its old usage to the pool", () => {
      const [pool] = collectLimitPools(
        [readBefore("a", [rolledOver]), account("b", [{ ...window, usedPercent: 40 }])],
        now,
      );
      const [session] = pool!.windows;
      const a = session!.members.find((member) => member.account.key === "a")!;
      expect(a.window).toMatchObject({ usedPercent: 0, resetsAt: undefined });
      expect(paceOf(a.window, now)).toBe(null);
      expect(session!.remainingPercent).toBe(80);
      expect(session!.resets.map((reset) => reset.member.account.key)).toEqual(["b"]);
      expect(session!.pace).toBe("under");
    });

    it("has no pace for the pool when it is the only member", () => {
      const [pool] = collectLimitPools([readBefore("a", [rolledOver])], now);
      expect(pool!.windows[0]).toMatchObject({ remainingPercent: 100, pace: null, resets: [] });
    });

    it("does not leave the session blocked by a weekly window that has since reset", () => {
      const [pool] = collectLimitPools(
        [
          readBefore("a", [
            { ...window, usedPercent: 30 },
            { ...weekly, usedPercent: 100, resetsAt: "2026-09-03T11:59:00.000Z" },
          ]),
        ],
        now,
      );
      expect(pool!.windows.map((row) => row.remainingPercent)).toEqual([70, 100]);
    });

    it("counts as reset exactly when its reset time arrives", () => {
      const at = (resetsAt: string) =>
        collectLimitPools([readBefore("a", [{ ...window, usedPercent: 94, resetsAt }])], now)[0]!
          .windows[0]!.remainingPercent;
      expect(at("2026-09-03T12:00:00.000Z")).toBe(100);
      expect(at("2026-09-03T12:00:00.001Z")).toBe(6);
    });

    it("is trusted as reported when the reading was taken after that reset time", () => {
      const [pool] = collectLimitPools(
        [account("a", [{ ...rolledOver, resetsAt: "2026-09-03T11:30:00.000Z" }])],
        now,
      );
      expect(pool!.windows[0]).toMatchObject({ remainingPercent: 6 });
    });
  });

  describe("a stale reading", () => {
    const stale = (key: string, windows: LimitAccount["limits"]["windows"], ageMs: number) => {
      const reading = account(key, windows);
      return {
        ...reading,
        limits: {
          ...reading.limits,
          checkedAt: DateTime.formatIso(DateTime.makeUnsafe(now - ageMs)),
        },
      };
    };

    it("stays on its bar, marked, but adds nothing to the totals, pace or restores", () => {
      const old = stale(
        "old",
        [{ ...window, usedPercent: 94, resetsAt: "2026-09-03T13:00:00.000Z" }],
        19 * 3_600_000,
      );
      const [pool] = collectLimitPools([old, account("b", [{ ...window, usedPercent: 40 }])], now);
      const [session] = pool!.windows;
      expect(keys(pool!)).toEqual([["b", "old"]]);
      expect(session!.columns.map((member) => member.window && member.stale)).toEqual([
        false,
        true,
      ]);
      const oldMember = session!.members.find((member) => member.account.key === "old")!;
      expect(oldMember.window).toMatchObject({ usedPercent: 94, resetsAt: undefined });
      expect(session).toMatchObject({ remainingPercent: 60, stale: false, pace: "under" });
      expect(session!.resets.map((reset) => reset.member.account.key)).toEqual(["b"]);
      expect(session!.resets[0]!.restoresPercent).toBe(40);
    });

    it("drops a window whose reset time has passed, keeping the account's other windows", () => {
      const old = stale(
        "old",
        [
          { ...window, usedPercent: 94, resetsAt: "2026-09-03T11:59:59.000Z" },
          { ...weekly, usedPercent: 30 },
        ],
        19 * 3_600_000,
      );
      const [pool] = collectLimitPools([old, account("b", [{ ...window, usedPercent: 40 }])], now);
      expect(pool!.windows.map((row) => row.kind)).toEqual(["session", "weekly"]);
      expect(keys(pool!)).toEqual([
        ["b", null],
        [null, "old"],
      ]);
    });

    it("marks a pool with no live member as stale and still shows what was last read", () => {
      const [pool] = collectLimitPools(
        [stale("old", [{ ...window, usedPercent: 94 }], 19 * 3_600_000)],
        now,
      );
      expect(pool!.windows[0]).toMatchObject({
        stale: true,
        remainingPercent: 6,
        pace: null,
        resets: [],
      });
    });

    it("is judged against the refresh interval of the server it came from", () => {
      const read = (ageMs: number, refreshIntervalMs: number) =>
        collectLimitPools([{ ...stale("a", [window], ageMs), refreshIntervalMs }], now)[0]!
          .windows[0]!.stale;
      expect(read(3 * FIVE_MINUTES, FIVE_MINUTES)).toBe(false);
      expect(read(3 * FIVE_MINUTES + 1, FIVE_MINUTES)).toBe(true);
      expect(read(40 * 60_000, 15 * 60_000)).toBe(false);
      expect(read(0, FIVE_MINUTES)).toBe(false);
      expect(read(-60_000, FIVE_MINUTES)).toBe(false);
    });

    it("leaves a live reading's window exactly as reported", () => {
      const [pool] = collectLimitPools([account("a", [window])], now);
      expect(pool!.windows[0]!.members[0]).toMatchObject({
        window,
        stale: false,
        refillPercent: 40,
      });
    });
  });
});

describe("refresh interval of a reading", () => {
  const read = (settings?: typeof DEFAULT_SERVER_SETTINGS) =>
    collectLimitAccounts(
      new Map([
        [
          EnvironmentId.make("env-a"),
          {
            entry: { target: { label: "Laptop" } },
            serverConfig: {
              ...(settings ? { settings } : {}),
              providers: [
                provider({
                  usageLimits: { checkedAt: "2026-09-03T11:00:00.000Z", windows: [window] },
                }),
              ],
              usageLimitSources: [
                {
                  id: UsageLimitSourceId.make("hub"),
                  kind: "cliproxy" as const,
                  label: "hub",
                  checkedAt: "2026-09-03T11:00:00.000Z",
                  accounts: [
                    {
                      id: "hub-account.json",
                      driver: ProviderDriverKind.make("claudeAgent"),
                      usageLimits: { checkedAt: "2026-09-03T11:00:00.000Z", windows: [window] },
                    },
                  ],
                },
              ],
            },
          },
        ],
      ]),
    ).map((account) => account.refreshIntervalMs);

  it("is the server's provider refresh interval, for its own and its hub's accounts", () => {
    expect(read(DEFAULT_SERVER_SETTINGS)).toEqual([FIVE_MINUTES, FIVE_MINUTES]);
    expect(
      read({
        ...DEFAULT_SERVER_SETTINGS,
        backgroundActivity: { schemaVersion: 1, profile: "battery-saver", overrides: {} },
      }),
    ).toEqual([15 * 60_000, 15 * 60_000]);
  });

  it("is the default when the server's settings are unknown", () => {
    expect(read()).toEqual([FIVE_MINUTES, FIVE_MINUTES]);
  });
});

describe("/usage-limits", () => {
  const limits = { checkedAt: "2026-09-03T11:00:00.000Z", windows: [window] };
  const selected = provider({
    usageLimits: limits,
    auth: { status: "authenticated", email: "same@example.com" },
  });
  const sources = [
    {
      id: UsageLimitSourceId.make("hub"),
      kind: "cliproxy" as const,
      label: "Accounts",
      checkedAt: limits.checkedAt,
      accounts: [
        {
          id: "duplicate",
          driver: selected.driver,
          email: "SAME@example.com",
          usageLimits: limits,
        },
        { id: "oss", driver: selected.driver, plan: "Codex OSS", usageLimits: limits },
        { id: "other-provider", driver: ProviderDriverKind.make("claude"), usageLimits: limits },
      ],
    },
  ];

  it("uses hub credit balances and redemption targets in the composer, including native duplicates", () => {
    const hubs = sources.map((source) => ({
      ...source,
      accounts: source.accounts.map((account) => ({
        ...account,
        usageLimits: {
          ...account.usageLimits,
          resetCredits: { availableCount: 2, nextCreditId: `${account.id}-credit` },
        },
      })),
    }));
    const report = collectProviderUsageLimits(selected.instanceId, [selected], hubs, now);
    const native = report?.accounts.find((account) => account.id === "codex");
    expect(native?.limits.resetCredits?.availableCount).toBe(2);
    expect(native?.resetCreditInput).toEqual({
      sourceId: "hub",
      accountId: "duplicate",
      creditId: "duplicate-credit",
    });
    expect(report?.accounts.find((account) => account.id === "hub:oss")?.resetCreditInput).toEqual({
      sourceId: "hub",
      accountId: "oss",
      creditId: "oss-credit",
    });
  });

  it("gives a hub credit to no native row when its email is signed in to two workspaces", () => {
    const workspaces = ["ws-a", "ws-b"].map((workspaceId) =>
      provider({
        instanceId: ProviderInstanceId.make(workspaceId),
        usageLimits: limits,
        auth: { status: "authenticated", email: "same@example.com", workspaceId },
      }),
    );
    const hub = [
      {
        ...sources[0]!,
        accounts: [
          {
            id: "duplicate",
            driver: selected.driver,
            email: "same@example.com",
            usageLimits: { ...limits, resetCredits: { availableCount: 2, nextCreditId: "c" } },
          },
        ],
      },
    ];
    const report = collectProviderUsageLimits(workspaces[0]!.instanceId, workspaces, hub, now);
    expect(report?.accounts.map((account) => account.resetCreditInput)).toEqual([
      { sourceId: "hub", accountId: "duplicate", creditId: "c" },
      { instanceId: "ws-a" },
      { instanceId: "ws-b" },
    ]);
  });

  it("redeems a native duplicate through the hub even when the native snapshot is fresher", () => {
    const fresher = provider({
      usageLimits: {
        checkedAt: "2026-09-03T11:30:00.000Z",
        windows: [window],
        resetCredits: { availableCount: 3, nextCreditId: "native-credit" },
      },
      auth: { status: "authenticated", email: "same@example.com" },
    });
    const stale = [
      {
        id: UsageLimitSourceId.make("hub"),
        kind: "cliproxy" as const,
        label: "Accounts",
        checkedAt: limits.checkedAt,
        accounts: [
          {
            id: "duplicate",
            driver: fresher.driver,
            email: "SAME@example.com",
            usageLimits: {
              ...limits,
              resetCredits: { availableCount: 2, nextCreditId: "hub-credit" },
            },
          },
        ],
      },
    ];
    const report = collectProviderUsageLimits(fresher.instanceId, [fresher], stale, now);
    expect(report?.accounts[0]?.resetCreditInput).toEqual({
      sourceId: "hub",
      accountId: "duplicate",
      creditId: "hub-credit",
    });
    expect(report?.accounts[0]?.limits.resetCredits?.availableCount).toBe(3);
  });

  it("keeps unidentified custom instances separate, filtering by driver", () => {
    const report = collectProviderUsageLimits(
      selected.instanceId,
      [
        selected,
        provider({
          instanceId: ProviderInstanceId.make("codex-work"),
          displayName: "Work",
          usageLimits: { ...limits, resetCredits: { availableCount: 2 } },
        }),
        provider({
          driver: ProviderDriverKind.make("claude"),
          instanceId: ProviderInstanceId.make("claude"),
          usageLimits: limits,
        }),
      ],
      sources,
      now,
    );
    expect(report?.createdAt).toBe("2026-09-03T12:00:00.000Z");
    expect(report?.accounts.map((account) => account.id)).toEqual([
      "hub:oss",
      "codex",
      "codex-work",
    ]);
    expect(report?.accounts[1]).toMatchObject({
      instanceId: selected.instanceId,
      email: selected.auth.email,
    });
    expect(report?.accounts[2]).toMatchObject({
      displayName: "Work",
      limits: { resetCredits: { availableCount: 2 } },
    });
    expect(report?.accounts[0]).toMatchObject({
      label: "Accounts · oss",
      sourceLabel: "CLI Proxy",
      plan: "Codex OSS",
    });
    expect(report?.notices).toEqual([]);
  });

  it("keeps the freshest usable native duplicate and the freshest native credits in either order", () => {
    const failed = provider({
      auth: selected.auth,
      usageLimits: {
        ...limits,
        checkedAt: "2026-09-03T11:40:00.000Z",
        unavailable: { reason: "probeFailed" },
      },
    });
    const credited = provider({
      instanceId: ProviderInstanceId.make("codex-credit"),
      auth: selected.auth,
      usageLimits: {
        checkedAt: "2026-09-03T11:20:00.000Z",
        windows: [{ ...window, usedPercent: 50 }],
        resetCredits: { availableCount: 2 },
      },
    });
    const staleCredit = provider({
      ...credited,
      instanceId: ProviderInstanceId.make("codex-stale-credit"),
      usageLimits: { ...limits, resetCredits: { availableCount: 3 } },
    });
    const freshest = provider({
      instanceId: ProviderInstanceId.make("codex-fresh"),
      auth: selected.auth,
      usageLimits: {
        checkedAt: "2026-09-03T11:30:00.000Z",
        windows: [{ ...window, usedPercent: 55 }],
      },
    });
    const duplicates = [failed, staleCredit, credited, freshest];
    for (const providers of [duplicates, duplicates.toReversed()]) {
      const report = collectProviderUsageLimits(failed.instanceId, providers, sources, now);
      expect(report?.accounts.map((account) => account.id)).toEqual(["hub:oss", "codex-fresh"]);
      expect(report?.accounts[1]).toMatchObject({
        limits: {
          checkedAt: "2026-09-03T11:30:00.000Z",
          windows: [{ usedPercent: 55 }],
          resetCredits: { availableCount: 2 },
        },
        resetCreditInput: { instanceId: "codex-credit" },
      });
      expect(report?.accounts[1]?.limits.unavailable).toBeUndefined();
    }
  });

  it("merges native workspace duplicates only while the workspace evidence is unambiguous", () => {
    const known = provider({
      ...selected,
      instanceId: ProviderInstanceId.make("workspace-a"),
      auth: { ...selected.auth, workspaceId: "ws-a" },
    });
    const fresh = provider({
      ...known,
      instanceId: ProviderInstanceId.make("workspace-a-fresh"),
      usageLimits: { ...limits, checkedAt: "2026-09-03T11:30:00.000Z" },
    });
    const natives = [known, fresh, selected];
    expect(
      collectProviderUsageLimits(selected.instanceId, natives, [], now)?.accounts.map(
        (account) => account.id,
      ),
    ).toEqual(["workspace-a-fresh"]);

    const unreadable = provider({
      instanceId: ProviderInstanceId.make("workspace-b"),
      auth: { ...selected.auth, workspaceId: "ws-b" },
    });
    expect(
      collectProviderUsageLimits(
        selected.instanceId,
        [...natives, unreadable],
        [],
        now,
      )?.accounts.map((account) => account.id),
    ).toEqual(["workspace-a-fresh", "codex"]);
    expect(
      collectProviderUsageLimits(
        selected.instanceId,
        [...natives, { ...unreadable, usageLimits: limits }],
        [],
        now,
      )?.accounts.map((account) => account.id),
    ).toEqual(["workspace-a-fresh", "codex", "workspace-b"]);
  });

  it("supports a source-only provider and keeps duplicates when the native probe failed", () => {
    expect(
      collectProviderUsageLimits(selected.instanceId, [provider({})], sources, now)?.accounts.map(
        (account) => account.id,
      ),
    ).toEqual(["hub:duplicate", "hub:oss"]);
    const failed = provider({ usageLimits: { ...limits, unavailable: { reason: "probeFailed" } } });
    expect(
      collectProviderUsageLimits(selected.instanceId, [failed], sources, now)?.accounts.map(
        (account) => account.id,
      ),
    ).toEqual(["hub:duplicate", "hub:oss", "codex"]);
    expect(collectProviderUsageLimits(selected.instanceId, [provider({})], [], now)).toBeNull();
    expect(
      collectProviderUsageLimits(
        selected.instanceId,
        [provider({ enabled: false, usageLimits: limits })],
        [],
        now,
      ),
    ).toBeNull();
  });

  it("surfaces source errors only for sources that carry the selected driver", () => {
    const failing = { ...sources[0]!, error: "token expired" };
    expect(
      collectProviderUsageLimits(selected.instanceId, [selected], [failing], now)?.notices,
    ).toEqual(["Accounts: token expired"]);
    const claudeOnly = { ...failing, accounts: failing.accounts.slice(2) };
    expect(
      collectProviderUsageLimits(selected.instanceId, [selected], [claudeOnly], now)?.notices,
    ).toEqual([]);
    const unreadable = { ...failing, accounts: [] };
    expect(
      collectProviderUsageLimits(selected.instanceId, [selected], [unreadable], now)?.notices,
    ).toEqual(["Accounts: token expired"]);
    const sourceOnly = collectProviderUsageLimits(
      selected.instanceId,
      [provider({})],
      [unreadable],
      now,
    );
    expect(sourceOnly?.accounts).toEqual([]);
    expect(sourceOnly?.notices).toEqual(["Accounts: token expired"]);
  });

  it("advertises global and workspace commands only for providers present in Limits", () => {
    const withWorkspace = provider({
      workspaceSnapshots: [
        { cwd: "/tmp/project", checkedAt: limits.checkedAt, slashCommands: [], skills: [] },
      ],
    });
    const [supported] = withUsageLimitsCommands([withWorkspace], sources);
    expect(supported?.slashCommands.map((command) => command.name)).toEqual(["usage-limits"]);
    expect(
      supported?.workspaceSnapshots?.[0]?.slashCommands.map((command) => command.name),
    ).toEqual(["usage-limits"]);
    expect(withUsageLimitsCommands([withWorkspace], [])[0]?.slashCommands).toEqual([]);
    const ownCommand = provider({
      slashCommands: [{ name: "usage-limits", description: "Provider's own" }],
    });
    expect(withUsageLimitsCommands([ownCommand], [])[0]?.slashCommands).toEqual([
      { name: "usage-limits", description: "Provider's own" },
    ]);
    const unreadable = { ...sources[0]!, accounts: [], error: "token expired" };
    expect(
      withUsageLimitsCommands([withWorkspace], [unreadable])[0]?.slashCommands.map(
        (command) => command.name,
      ),
    ).toEqual(["usage-limits"]);
    expect(
      withUsageLimitsCommands([selected], [])[0]?.slashCommands.map((command) => command.name),
    ).toEqual(["usage-limits"]);
  });
});

describe("sameUsageLimitCommandCoverage", () => {
  const codexAccount = {
    id: "a",
    driver: ProviderDriverKind.make("codex"),
    usageLimits: { checkedAt: "2026-09-03T11:00:00.000Z", windows: [] },
  };
  const base = {
    id: UsageLimitSourceId.make("hub"),
    kind: "cliproxy" as const,
    label: "Accounts",
    checkedAt: "2026-09-03T11:00:00.000Z",
  };
  it("ignores quota movement but not the drivers offered the command", () => {
    const withCodex = [{ ...base, accounts: [codexAccount] }];
    const withCodexLater = [
      {
        ...base,
        accounts: [
          {
            ...codexAccount,
            usageLimits: { ...codexAccount.usageLimits, checkedAt: "2026-09-03T12:00:00.000Z" },
          },
        ],
      },
    ];
    expect(sameUsageLimitCommandCoverage(withCodex, withCodexLater)).toBe(true);
    expect(sameUsageLimitCommandCoverage(withCodex, [{ ...base, accounts: [] }])).toBe(false);
  });
  it("treats a failed read as a change in coverage, in both directions", () => {
    const empty = [{ ...base, accounts: [] }];
    const failed = [{ ...base, accounts: [], error: "token expired" }];
    expect(sameUsageLimitCommandCoverage(empty, failed)).toBe(false);
    expect(sameUsageLimitCommandCoverage(failed, empty)).toBe(false);
    expect(
      sameUsageLimitCommandCoverage(failed, [{ ...base, accounts: [], error: "still down" }]),
    ).toBe(true);
  });
});

describe("external usage settings", () => {
  it("deduplicates destinations across accounts and environments without inventing quota pools", () => {
    const managed = provider({
      usageLimits: {
        checkedAt: "2026-09-03T11:00:00.000Z",
        windows: [],
        unavailable: { reason: "unsupported", message: "Track usage in ChatGPT." },
        externalUsage: { label: "ChatGPT usage", url: "https://chatgpt.com/#settings/Usage" },
      },
    });
    const presentations = new Map([
      [
        EnvironmentId.make("a"),
        {
          entry: { target: { label: "A" } },
          serverConfig: {
            providers: [managed, { ...managed, instanceId: ProviderInstanceId.make("personal") }],
          },
        },
      ],
      [
        EnvironmentId.make("b"),
        { entry: { target: { label: "B" } }, serverConfig: { providers: [managed] } },
      ],
    ]);
    expect(collectExternalUsageLinks(presentations)).toEqual([
      {
        ...managed.usageLimits!.externalUsage,
        message: "Track usage in ChatGPT.",
        accounts: [`${managed.instanceId} on A`, "personal on A", `${managed.instanceId} on B`],
      },
    ]);
    expect(collectLimitAccounts(presentations)).toEqual([]);
    expect(collectLimitNotices(presentations)).toEqual([]);
  });
});

describe("ChatGPT sharing presentation", () => {
  it("requires verified sharing metadata rather than the Codex driver or login type", () => {
    const codex = provider({ auth: { status: "authenticated", type: "chatgpt" } });
    expect(usesChatGptSharing(codex)).toBe(false);
    expect(
      usesChatGptSharing({ ...codex, auth: { ...codex.auth, subscriptionSharing: true } }),
    ).toBe(true);
    expect(
      usesChatGptSharing({
        ...codex,
        auth: { status: "unauthenticated", subscriptionSharing: true },
      }),
    ).toBe(false);
  });
});
