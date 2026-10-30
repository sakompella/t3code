import {
  type EnvironmentId,
  ProviderDriverKind,
  type ServerProvider,
  type UsageLimitPool,
  type UsageLimitPoolAccount,
  type UsageLimitPoolDashboard,
  type UsageLimitPoolPoint,
  type UsageLimitPoolWindow,
  type UsageLimitSourceId,
} from "@t3tools/contracts";

import type { LimitAccount, LimitPresentations } from "./usageLimits.ts";

/**
 * Client side of the quota history a CLIProxyAPI hub keeps (`server.getUsageLimitPools`). The hub
 * records readings and computes the forecast; these helpers only pick what to ask for and join its
 * answer to the accounts Limits already shows. One point is one percent of one account's window.
 */

export const POOL_PROVIDER_DRIVER: Record<UsageLimitPool["provider"], ServerProvider["driver"]> = {
  claude: ProviderDriverKind.make("claudeAgent"),
  codex: ProviderDriverKind.make("codex"),
};

export interface HubPoolSource {
  readonly environmentId: EnvironmentId;
  readonly sourceId: UsageLimitSourceId;
  readonly label: string;
  /** The pool windows this hub's accounts report, per provider, weekly first. */
  readonly windows: ReadonlyMap<ServerProvider["driver"], readonly UsageLimitPoolWindow[]>;
}

const WINDOW_OF_KIND: Partial<Record<string, UsageLimitPoolWindow>> = {
  weekly: "weekly",
  session: "five-hour",
};

/**
 * One source per hub, from the first environment that reads it. Two environments that read the
 * same hub get the same history, so asking both would draw it twice. A hub is known by the auth
 * files it lists: labels and URLs differ between environments that reach the same hub.
 */
export function hubPoolSources(presentations: LimitPresentations): readonly HubPoolSource[] {
  const sources = new Map<string, HubPoolSource>();
  for (const [environmentId, presentation] of presentations) {
    for (const source of presentation.serverConfig?.usageLimitSources ?? []) {
      if (source.error !== undefined || source.accounts.length === 0) continue;
      const identity = JSON.stringify(source.accounts.map((account) => account.id).sort());
      if (sources.has(identity)) continue;
      const kinds = new Map<ServerProvider["driver"], Set<UsageLimitPoolWindow>>();
      for (const account of source.accounts) {
        const reported = kinds.get(account.driver) ?? new Set();
        for (const window of account.usageLimits.windows) {
          const poolWindow = WINDOW_OF_KIND[window.kind];
          if (poolWindow) reported.add(poolWindow);
        }
        kinds.set(account.driver, reported);
      }
      sources.set(identity, {
        environmentId,
        sourceId: source.id,
        label: source.label,
        windows: new Map(
          [...kinds].map(([driver, reported]) => [
            driver,
            (["weekly", "five-hour"] as const).filter((window) => reported.has(window)),
          ]),
        ),
      });
    }
  }
  return [...sources.values()];
}

export function dashboardPool(
  dashboard: UsageLimitPoolDashboard | null,
  driver: ServerProvider["driver"],
): UsageLimitPool | null {
  return (
    dashboard?.pools.find(
      (pool) => POOL_PROVIDER_DRIVER[pool.provider] === driver && pool.accountCount > 0,
    ) ?? null
  );
}

export interface LinkedPoolAccount {
  readonly account: UsageLimitPoolAccount;
  /** The Limits row listing the same hub account; null when Limits does not show it. */
  readonly limitAccount: LimitAccount | null;
}

/** Joins each hub pool account to the Limits row of the same hub listing. */
export function linkPoolAccounts(
  pool: UsageLimitPool,
  accounts: readonly LimitAccount[],
  source: Pick<HubPoolSource, "environmentId" | "sourceId">,
): readonly LinkedPoolAccount[] {
  const byHubId = new Map<string, LimitAccount>();
  for (const account of accounts) {
    for (const ref of account.hubAccounts) {
      if (ref.environmentId === source.environmentId && ref.sourceId === source.sourceId) {
        byHubId.set(ref.accountId, account);
      }
    }
  }
  return pool.accounts.map((account) => ({
    account,
    limitAccount: byHubId.get(account.id) ?? null,
  }));
}

/** Splits a line where readings are missing, so a gap is never drawn as a value. */
export function knownRuns(
  points: readonly UsageLimitPoolPoint[],
): ReadonlyArray<ReadonlyArray<{ readonly atMs: number; readonly remainingPoints: number }>> {
  const runs: Array<Array<{ readonly atMs: number; readonly remainingPoints: number }>> = [];
  let run: Array<{ readonly atMs: number; readonly remainingPoints: number }> = [];
  for (const point of points) {
    if (point.remainingPoints !== null) {
      run.push({ atMs: point.atMs, remainingPoints: point.remainingPoints });
    } else if (run.length > 0) {
      runs.push(run);
      run = [];
    }
  }
  if (run.length > 0) runs.push(run);
  return runs;
}

/**
 * Which reset step-ups get a "+N%" label: dense five-hour resets would overlap, so a label needs
 * `minGap` pixels from the previous label and from the Now marker. Every reset still gets a tick.
 */
export function labeledResets(
  resetXs: readonly number[],
  nowX: number | null,
  minGap: number,
): readonly boolean[] {
  let lastLabelX = Number.NEGATIVE_INFINITY;
  return resetXs.map((x) => {
    const show = x - lastLabelX >= minGap && (nowX === null || Math.abs(x - nowX) >= minGap / 2);
    if (show) lastLabelX = x;
    return show;
  });
}

const FORECAST_UNAVAILABLE: Record<Exclude<UsageLimitPool["forecastState"], "ready">, string> = {
  collecting: "Collecting history: a forecast needs a few readings over 10 minutes.",
  partial: "No forecast: some accounts have no current reading.",
  stale: "No forecast: the hub's latest readings are stale.",
  "no-accounts": "No forecast: the hub lists no accounts for this window.",
};

/** Why a pool has no projected line, or null when it has one. */
export function forecastUnavailableReason(pool: UsageLimitPool): string | null {
  return pool.forecastState === "ready" ? null : FORECAST_UNAVAILABLE[pool.forecastState];
}

export function formatPoolPercent(points: number): string {
  return `${Math.round(points)}%`;
}

export function formatPoolBurn(pointsPerHour: number): string {
  return `${pointsPerHour.toFixed(pointsPerHour >= 10 ? 0 : 1)}%/h`;
}
