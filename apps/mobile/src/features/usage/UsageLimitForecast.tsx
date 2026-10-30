import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import { useIsFocused } from "@react-navigation/native";
import type {
  ServerProvider,
  UsageLimitPoolLookbackHours,
  UsageLimitPoolWindow,
} from "@t3tools/contracts";
import { formatDuration } from "@t3tools/shared/usageLimits";
import {
  dashboardPool,
  forecastUnavailableReason,
  formatPoolBurn,
  formatPoolPercent,
  type HubPoolSource,
  poolEvents,
} from "@t3tools/shared/usageLimitPools";
import { AsyncResult } from "effect/reactivity";
import { useEffect, useRef, useState } from "react";
import { AppState, View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { SegmentedControl } from "../../components/ui/SegmentedControl";
import { serverEnvironment } from "../../state/server";

const POLL_MS = 60_000;
const NO_VALUE = "n/a";
const WINDOW_OPTIONS = [
  { value: "weekly", label: "Weekly" },
  { value: "five-hour", label: "5-hour" },
] as const satisfies readonly { value: UsageLimitPoolWindow; label: string }[];

const LOOKBACK_OPTIONS = [
  { value: 1, label: "1h", accessibilityLabel: "Burn rate over the last hour" },
  { value: 3, label: "3h", accessibilityLabel: "Burn rate over the last 3 hours" },
  { value: 6, label: "6h", accessibilityLabel: "Burn rate over the last 6 hours" },
] as const satisfies readonly {
  value: UsageLimitPoolLookbackHours;
  label: string;
  accessibilityLabel: string;
}[];

const timeFormat = new Intl.DateTimeFormat(undefined, {
  weekday: "short",
  hour: "numeric",
  minute: "2-digit",
});
const dayFormat = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric" });

/**
 * The hub's pool forecast for one provider as four numbers; the chart is web-only for now.
 * Fetched when shown, on each Limits refresh (`now`) and once a minute while the screen is
 * focused and the app is active.
 */
export function HubPoolStats({
  source,
  driver,
  title,
  now,
}: {
  readonly source: HubPoolSource;
  readonly driver: ServerProvider["driver"];
  readonly title: string;
  readonly now: number;
}) {
  const windows = source.windows.get(driver) ?? [];
  const [chosenWindow, setWindow] = useState<UsageLimitPoolWindow | null>(null);
  const [lookbackHours, setLookbackHours] = useState<UsageLimitPoolLookbackHours>(3);
  const window =
    chosenWindow !== null && windows.includes(chosenWindow) ? chosenWindow : windows[0];
  if (window === undefined) return null;
  return (
    <PoolStatsCard
      source={source}
      driver={driver}
      title={title}
      now={now}
      window={window}
      windowOptions={WINDOW_OPTIONS.filter((option) => windows.includes(option.value))}
      onWindowChange={setWindow}
      lookbackHours={lookbackHours}
      onLookbackChange={setLookbackHours}
    />
  );
}

function PoolStatsCard({
  source,
  driver,
  title,
  now,
  window,
  windowOptions,
  onWindowChange,
  lookbackHours,
  onLookbackChange,
}: {
  readonly source: HubPoolSource;
  readonly driver: ServerProvider["driver"];
  readonly title: string;
  readonly now: number;
  readonly window: UsageLimitPoolWindow;
  readonly windowOptions: ReadonlyArray<(typeof WINDOW_OPTIONS)[number]>;
  readonly onWindowChange: (window: UsageLimitPoolWindow) => void;
  readonly lookbackHours: UsageLimitPoolLookbackHours;
  readonly onLookbackChange: (hours: UsageLimitPoolLookbackHours) => void;
}) {
  const atom = serverEnvironment.usageLimitPools({
    environmentId: source.environmentId,
    input: { sourceId: source.sourceId, window, lookbackHours },
  });
  const dashboard = AsyncResult.getOrElse(useAtomValue(atom), () => null);
  useRefreshWhileFocused(useAtomRefresh(atom), now);
  const pool = dashboardPool(dashboard, driver);
  if (dashboard === null || pool === null) return null;

  const ready = pool.forecastState === "ready";
  const relative = (at: number) =>
    at <= dashboard.nowMs ? "now" : `in ${formatDuration(at - dashboard.nowMs)}`;
  const notBy = `Not by ${dayFormat.format(dashboard.toMs)}`;
  const unavailable = forecastUnavailableReason(pool);
  return (
    <View className="gap-4 rounded-[24px] border-continuous bg-grouped-card p-4">
      <Text className="text-sm font-t3-medium text-foreground">{title} forecast</Text>
      <View className="flex-row items-center gap-2">
        {windowOptions.length > 1 ? (
          <SegmentedControl
            options={windowOptions}
            selected={window}
            onSelect={onWindowChange}
            size="compact"
            className="flex-1"
          />
        ) : null}
        <SegmentedControl
          options={LOOKBACK_OPTIONS}
          selected={lookbackHours}
          onSelect={onLookbackChange}
          size="compact"
          className="flex-1"
        />
      </View>
      <View className="flex-row items-baseline gap-2">
        <Text className="text-3xl font-t3-bold tabular-nums text-foreground">
          {pool.remainingPoints === null ? NO_VALUE : formatPoolPercent(pool.remainingPoints)}
        </Text>
        <Text className="text-sm text-foreground-muted tabular-nums">
          of {formatPoolPercent(pool.capacityPoints)}
        </Text>
      </View>
      <View className="flex-row flex-wrap gap-y-3">
        <Stat
          label="Pool empty"
          value={
            !ready
              ? NO_VALUE
              : pool.poolEmptyAtMs === null
                ? notBy
                : timeFormat.format(pool.poolEmptyAtMs)
          }
          detail={ready && pool.poolEmptyAtMs !== null ? relative(pool.poolEmptyAtMs) : null}
        />
        <Stat
          label="First empty"
          value={
            !ready ? NO_VALUE : pool.firstEmptyAtMs === null ? notBy : relative(pool.firstEmptyAtMs)
          }
          detail={
            ready && pool.firstEmptyAtMs !== null ? timeFormat.format(pool.firstEmptyAtMs) : null
          }
        />
        <Stat
          label="Next reset"
          value={
            pool.nextResetGainPoints !== null
              ? `+${formatPoolPercent(pool.nextResetGainPoints)}`
              : pool.nextResetAtMs === null
                ? NO_VALUE
                : timeFormat.format(pool.nextResetAtMs)
          }
          detail={
            pool.nextResetAtMs !== null && pool.nextResetGainPoints !== null
              ? timeFormat.format(pool.nextResetAtMs)
              : null
          }
        />
        <Stat
          label={`Burn, last ${dashboard.lookbackHours}h`}
          value={
            pool.burnPointsPerHour === null ? NO_VALUE : formatPoolBurn(pool.burnPointsPerHour)
          }
          detail={null}
        />
        {dashboard.events ? (
          <Stat
            label="Limit hits"
            value={String(
              poolEvents(dashboard, pool).filter((event) => event.kind === "limit-hit").length,
            )}
            detail={`since ${dayFormat.format(dashboard.fromMs)}`}
          />
        ) : null}
      </View>
      {unavailable ? <Text className="text-xs text-foreground-muted">{unavailable}</Text> : null}
    </View>
  );
}

function Stat({
  label,
  value,
  detail,
}: {
  readonly label: string;
  readonly value: string;
  readonly detail: string | null;
}) {
  return (
    <View className="w-1/2 gap-0.5 pr-3">
      <Text className="text-xs text-foreground-muted">{label}</Text>
      <Text className="text-sm font-t3-medium text-foreground tabular-nums">{value}</Text>
      {detail ? <Text className="text-xs text-foreground-muted tabular-nums">{detail}</Text> : null}
    </View>
  );
}

function useRefreshWhileFocused(refresh: () => void, now: number) {
  const focused = useIsFocused();
  const lastNow = useRef(now);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- refetches the hub's history when the Limits screen refreshes (its `now` moves)
  useEffect(() => {
    if (lastNow.current === now) return;
    lastNow.current = now;
    refresh();
  }, [now, refresh]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- polls the hub's history once a minute while the screen is focused and the app active
  useEffect(() => {
    if (!focused) return;
    const timer = setInterval(() => {
      if (AppState.currentState === "active") refresh();
    }, POLL_MS);
    return () => clearInterval(timer);
  }, [focused, refresh]);
}
