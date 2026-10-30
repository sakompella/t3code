import {
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
  type ServerProviderUsageWindow,
  type UsageLimitPool,
  type UsageLimitPoolPoint,
  UsageLimitSourceId,
} from "@t3tools/contracts";
import * as Arbitrary from "effect/Arbitrary";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "@effect/vitest";

import { collectLimitAccounts } from "./usageLimits.ts";
import {
  hubPoolSources,
  hubPoolWindows,
  knownRuns,
  labeledResets,
  linkPoolAccounts,
} from "./usageLimitPools.ts";

const checkedAt = "2026-10-30T11:00:00.000Z";
const claude = ProviderDriverKind.make("claudeAgent");
const codex = ProviderDriverKind.make("codex");
const hubId = UsageLimitSourceId.make("hub");
const mac = EnvironmentId.make("mac");
const hp = EnvironmentId.make("hp");
const session = {
  id: "five_hour",
  kind: "session",
  label: "Session",
  usedPercent: 40,
  windowDurationMins: 300,
  resetsAt: "2026-10-30T14:00:00.000Z",
} as const;
const weekly = {
  id: "seven_day",
  kind: "weekly",
  label: "Weekly",
  usedPercent: 20,
  windowDurationMins: 7 * 24 * 60,
  resetsAt: "2026-11-02T12:00:00.000Z",
} as const;

function hubAccount(
  id: string,
  driver: ServerProvider["driver"],
  email: string,
  windows: ServerProviderUsageWindow[] = [session],
) {
  return { id, driver, email, usageLimits: { checkedAt, windows } };
}

const hubSource = (accounts: ReturnType<typeof hubAccount>[], label = "hub.example") => ({
  id: hubId,
  kind: "cliproxy" as const,
  label,
  checkedAt,
  accounts,
});

const nativeClaude: ServerProvider = {
  instanceId: ProviderInstanceId.make("claude"),
  driver: claude,
  enabled: true,
  installed: true,
  version: null,
  status: "ready",
  auth: { status: "authenticated", email: "First@example.com" },
  checkedAt,
  models: [],
  slashCommands: [],
  skills: [],
  usageLimits: { checkedAt, windows: [session] },
};

const check = <A>(arbitrary: Arbitrary.Arbitrary<A>, property: (value: A) => boolean) =>
  Arbitrary.checkEffect(arbitrary, property, { runs: 300 }).pipe(
    Effect.map((result) => expect(Arbitrary.formatCheckFailure(result)).toBeUndefined()),
  );

function pool(accountIds: readonly string[]): UsageLimitPool {
  return {
    provider: "claude",
    accountCount: accountIds.length,
    eligibleCount: accountIds.length,
    loadedCount: accountIds.length,
    capacityPoints: 100 * accountIds.length,
    remainingPoints: null,
    burnPointsPerHour: null,
    forecastState: "collecting",
    firstEmptyAtMs: null,
    poolEmptyAtMs: null,
    nextResetAtMs: null,
    nextResetGainPoints: null,
    recorded: [],
    projected: [],
    resets: [],
    accounts: accountIds.map((id) => ({
      id,
      status: "ok",
      observedAtMs: null,
      remainingPercent: null,
      resetAtMs: null,
      burnPointsPerHour: null,
      emptyAtMs: null,
    })),
  };
}

describe("hub pool accounts", () => {
  it("joins hub accounts to their Limits rows, including one also signed in directly", () => {
    const presentations = new Map([
      [
        mac,
        {
          entry: { target: { label: "Mac" } },
          serverConfig: {
            providers: [nativeClaude],
            usageLimitSources: [
              hubSource([
                hubAccount("claude-first.json", claude, "first@example.com"),
                hubAccount("claude-second.json", claude, "second@example.com"),
              ]),
            ],
          },
        },
      ],
    ]);
    const accounts = collectLimitAccounts(presentations);
    const linked = linkPoolAccounts(
      pool(["claude-first.json", "claude-second.json", "claude-retired.json"]),
      accounts,
      { environmentId: mac, sourceId: hubId },
    );
    expect(linked.map(({ limitAccount }) => limitAccount?.email ?? null)).toEqual([
      "First@example.com",
      "second@example.com",
      null,
    ]);
    // The same source id on another environment is another hub's listing.
    expect(
      linkPoolAccounts(pool(["claude-first.json"]), accounts, {
        environmentId: hp,
        sourceId: hubId,
      })[0]?.limitAccount,
    ).toBeNull();
  });

  it("offers only the windows the provider's hub accounts report", () => {
    const presentations = new Map([
      [
        mac,
        {
          entry: { target: { label: "Mac" } },
          serverConfig: {
            // A direct login's weekly window is not the hub's.
            providers: [
              {
                ...nativeClaude,
                auth: { status: "authenticated" as const, email: "own@example.com" },
                usageLimits: { checkedAt, windows: [session, weekly] },
              },
            ],
            usageLimitSources: [
              hubSource([
                hubAccount("claude.json", claude, "edu@example.com"),
                hubAccount("codex.json", codex, "plus@example.com", [session, weekly]),
              ]),
            ],
          },
        },
      ],
    ]);
    const accounts = collectLimitAccounts(presentations);
    expect(hubPoolWindows(accounts, claude)).toEqual(["five-hour"]);
    expect(hubPoolWindows(accounts, codex)).toEqual(["weekly", "five-hour"]);
  });

  it("asks each hub once, from the first environment that reads it", () => {
    const source = hubSource([hubAccount("claude.json", claude, "edu@example.com")]);
    const presentations = new Map([
      [
        mac,
        {
          entry: { target: { label: "Mac" } },
          serverConfig: {
            providers: [],
            usageLimitSources: [{ ...source, error: "The hub did not answer within 15 seconds." }],
          },
        },
      ],
      [
        hp,
        {
          entry: { target: { label: "hp" } },
          serverConfig: { providers: [], usageLimitSources: [source] },
        },
      ],
      [
        EnvironmentId.make("vm"),
        {
          entry: { target: { label: "VM" } },
          serverConfig: { providers: [], usageLimitSources: [source] },
        },
      ],
    ]);
    expect(hubPoolSources(presentations)).toEqual([
      { environmentId: hp, sourceId: hubId, label: "hub.example" },
    ]);
  });
});

const pointArbitrary = Arbitrary.schema(
  Schema.Array(
    Schema.Struct({
      atMs: Schema.Int,
      remainingPoints: Schema.NullOr(
        Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 400 })),
      ),
    }),
  ),
);

describe("chart geometry", () => {
  it.effect(
    "draws every reading once, in order, and breaks the line only at missing readings",
    () => {
      return check(pointArbitrary, (points: readonly UsageLimitPoolPoint[]) => {
        const runs = knownRuns(points);
        const known = points.filter((point) => point.remainingPoints !== null);
        const blocks = points.filter(
          (point, index) =>
            point.remainingPoints !== null && points[index - 1]?.remainingPoints == null,
        ).length;
        return (
          runs.length === blocks &&
          runs.every((run) => run.length > 0) &&
          JSON.stringify(runs.flat()) === JSON.stringify(known)
        );
      });
    },
  );

  it.effect("keeps reset labels apart from each other and from the Now marker", () => {
    const input = Arbitrary.schema(
      Schema.Struct({
        xs: Schema.Array(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1000 }))),
        nowX: Schema.NullOr(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1000 }))),
      }),
    );
    const gap = 80;
    return check(input, ({ xs, nowX }) => {
      const sorted = xs.toSorted((a, b) => a - b);
      const labels = sorted.filter((_, index) => labeledResets(sorted, nowX, gap)[index]);
      return (
        labels.every((x, index) => index === 0 || x - labels[index - 1]! >= gap) &&
        labels.every((x) => nowX === null || Math.abs(x - nowX) >= gap / 2) &&
        // A reset far from Now and from earlier resets always gets its label.
        (sorted.length === 0 || nowX !== null || labels[0] === sorted[0])
      );
    });
  });
});
