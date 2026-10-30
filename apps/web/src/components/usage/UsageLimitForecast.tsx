import type {
  ServerProvider,
  UsageLimitPool,
  UsageLimitPoolDashboard,
  UsageLimitPoolLookbackHours,
  UsageLimitPoolWindow,
} from "@t3tools/contracts";
import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import { formatDuration, type LimitAccount } from "@t3tools/shared/usageLimits";
import {
  dashboardPool,
  forecastUnavailableReason,
  formatPoolBurn,
  formatPoolPercent,
  type HubPoolSource,
  hubPoolWindows,
  knownRuns,
  labeledResets,
  linkPoolAccounts,
} from "@t3tools/shared/usageLimitPools";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";
import {
  type PointerEvent,
  type ReactNode,
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
} from "react";

import { usePrimarySettings } from "../../hooks/useSettings";
import { observeResize } from "../../lib/observeResize";
import { serverEnvironment } from "../../state/server";
import { resolveHour12 } from "../../timestampFormat";
import { Toggle, ToggleGroup } from "../ui/toggle-group";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { AccountChip } from "./UsageAccountChip";

const POLL_MS = 60_000;
const NO_VALUE = "n/a";
const HOUR = 3_600_000;
const DAY = 24 * HOUR;
const LOOKBACKS: readonly UsageLimitPoolLookbackHours[] = [1, 3, 6];
const WINDOW_LABEL: Record<UsageLimitPoolWindow, string> = {
  weekly: "Weekly",
  "five-hour": "5-hour",
};
const FRAME = { left: 44, right: 14, top: 30, bottom: 26, height: 240 } as const;

/**
 * History and forecast of one hub's pool for one provider, as the hub records it. Fetched when
 * shown, on each Limits refresh (`now`) and once a minute while the page is visible; nothing ticks
 * in between.
 */
export function HubPoolForecast({
  source,
  driver,
  accounts,
  color,
  title,
  now,
}: {
  readonly source: HubPoolSource;
  readonly driver: ServerProvider["driver"];
  readonly accounts: readonly LimitAccount[];
  readonly color: string;
  readonly title: string;
  readonly now: number;
}) {
  const windows = hubPoolWindows(accounts, driver);
  const [chosenWindow, setWindow] = useState<UsageLimitPoolWindow | null>(null);
  const [lookbackHours, setLookbackHours] = useState<UsageLimitPoolLookbackHours>(3);
  const window =
    chosenWindow !== null && windows.includes(chosenWindow) ? chosenWindow : windows[0];
  if (window === undefined) return null;
  return (
    <ForecastQuery
      source={source}
      driver={driver}
      accounts={accounts}
      color={color}
      title={title}
      now={now}
      window={window}
      windows={windows}
      onWindowChange={setWindow}
      lookbackHours={lookbackHours}
      onLookbackChange={setLookbackHours}
    />
  );
}

function ForecastQuery({
  source,
  driver,
  accounts,
  color,
  title,
  now,
  window,
  windows,
  onWindowChange,
  lookbackHours,
  onLookbackChange,
}: {
  readonly source: HubPoolSource;
  readonly driver: ServerProvider["driver"];
  readonly accounts: readonly LimitAccount[];
  readonly color: string;
  readonly title: string;
  readonly now: number;
  readonly window: UsageLimitPoolWindow;
  readonly windows: readonly UsageLimitPoolWindow[];
  readonly onWindowChange: (window: UsageLimitPoolWindow) => void;
  readonly lookbackHours: UsageLimitPoolLookbackHours;
  readonly onLookbackChange: (hours: UsageLimitPoolLookbackHours) => void;
}) {
  const atom = serverEnvironment.usageLimitPools({
    environmentId: source.environmentId,
    input: { sourceId: source.sourceId, window, lookbackHours },
  });
  const result = useAtomValue(atom);
  const refresh = useAtomRefresh(atom);
  useRefreshWhileVisible(refresh, now);

  const dashboard = AsyncResult.getOrElse(result, () => null);
  const pool = dashboardPool(dashboard, driver);
  // No history on this hub (or no pool for this provider): Limits stays as it was.
  if (dashboard === null || pool === null) {
    return AsyncResult.isFailure(result) ? (
      <p className="text-xs text-muted-foreground">
        Quota history unavailable: {failureMessage(result.cause)}
      </p>
    ) : null;
  }
  const controls = (
    <div className="flex flex-wrap items-center gap-2">
      {windows.length > 1 ? (
        <ToggleGroup
          aria-label="Quota window"
          variant="segmented"
          value={[window]}
          onValueChange={(next) => {
            const value = windows.find((candidate) => candidate === next[0]);
            if (value) onWindowChange(value);
          }}
        >
          {windows.map((option) => (
            <Toggle key={option} value={option}>
              {WINDOW_LABEL[option]}
            </Toggle>
          ))}
        </ToggleGroup>
      ) : null}
      <ToggleGroup
        aria-label="Burn rate window"
        variant="segmented"
        value={[String(lookbackHours)]}
        onValueChange={(next) => {
          const value = LOOKBACKS.find((hours) => String(hours) === next[0]);
          if (value) onLookbackChange(value);
        }}
      >
        {LOOKBACKS.map((hours) => (
          <Toggle key={hours} value={String(hours)} title={`Burn rate over the last ${hours}h`}>
            {hours}h
          </Toggle>
        ))}
      </ToggleGroup>
    </div>
  );
  return (
    <ForecastCard
      pool={pool}
      dashboard={dashboard}
      accounts={linkPoolAccounts(pool, accounts, source)}
      color={color}
      title={title}
      windowLabel={WINDOW_LABEL[window]}
      controls={controls}
    />
  );
}

function failureMessage(cause: Cause.Cause<unknown>): string {
  const error = Cause.squash(cause);
  return error instanceof Error ? error.message : "the hub did not answer.";
}

/** Refetches after each Limits refresh and once a minute while the page is visible. */
function useRefreshWhileVisible(refresh: () => void, now: number) {
  const lastNow = useRef(now);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- refetches the hub's history when the Limits page refreshes (its `now` moves)
  useEffect(() => {
    if (lastNow.current === now) return;
    lastNow.current = now;
    refresh();
  }, [now, refresh]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- polls the hub's history once a minute, skipping hidden pages
  useEffect(() => {
    const timer = setInterval(() => {
      if (document.visibilityState === "visible") refresh();
    }, POLL_MS);
    return () => clearInterval(timer);
  }, [refresh]);
}

function ForecastCard({
  pool,
  dashboard,
  accounts,
  color,
  title,
  windowLabel,
  controls,
}: {
  readonly pool: UsageLimitPool;
  readonly dashboard: UsageLimitPoolDashboard;
  readonly accounts: ReturnType<typeof linkPoolAccounts>;
  readonly color: string;
  readonly title: string;
  readonly windowLabel: string;
  readonly controls: ReactNode;
}) {
  const timestampFormat = usePrimarySettings((settings) => settings.timestampFormat);
  const formatTime = timeFormatter(resolveHour12(timestampFormat));
  const nowMs = dashboard.nowMs;
  const ready = pool.forecastState === "ready";
  const relative = (at: number) => (at <= nowMs ? "now" : `in ${formatDuration(at - nowMs)}`);
  const unavailable = forecastUnavailableReason(pool);
  return (
    <div className="flex flex-col gap-4 rounded-lg border border-border/60 p-4">
      <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2">
        <span className="flex min-w-0 items-center gap-2 text-sm">
          <span className="font-medium text-foreground">{title}</span>
          <span className="text-xs text-muted-foreground">
            {windowLabel} · {pool.accountCount} {pool.accountCount === 1 ? "account" : "accounts"}
          </span>
          <PoolAccountChips accounts={accounts} formatTime={formatTime} nowMs={nowMs} />
        </span>
        {controls}
      </div>
      <div className="flex flex-wrap items-end justify-between gap-x-8 gap-y-3">
        <span className="flex items-baseline gap-2">
          <span className="text-3xl font-semibold text-foreground tabular-nums">
            {pool.remainingPoints === null ? NO_VALUE : formatPoolPercent(pool.remainingPoints)}
          </span>
          <span className="text-sm text-muted-foreground tabular-nums">
            of {formatPoolPercent(pool.capacityPoints)}
          </span>
        </span>
        <dl className="grid grid-cols-2 gap-x-8 gap-y-2 text-xs sm:grid-cols-4">
          <Stat
            label="Pool empty"
            value={
              !ready
                ? NO_VALUE
                : pool.poolEmptyAtMs === null
                  ? `Not by ${formatDay(dashboard.toMs)}`
                  : formatTime(pool.poolEmptyAtMs)
            }
            detail={ready && pool.poolEmptyAtMs !== null ? relative(pool.poolEmptyAtMs) : null}
          />
          <Stat
            label="First empty"
            value={
              !ready
                ? NO_VALUE
                : pool.firstEmptyAtMs === null
                  ? `Not by ${formatDay(dashboard.toMs)}`
                  : relative(pool.firstEmptyAtMs)
            }
            detail={ready && pool.firstEmptyAtMs !== null ? formatTime(pool.firstEmptyAtMs) : null}
          />
          <Stat
            label="Next reset"
            value={
              pool.nextResetGainPoints !== null
                ? `+${formatPoolPercent(pool.nextResetGainPoints)}`
                : pool.nextResetAtMs === null
                  ? NO_VALUE
                  : formatTime(pool.nextResetAtMs)
            }
            detail={
              pool.nextResetAtMs !== null && pool.nextResetGainPoints !== null
                ? formatTime(pool.nextResetAtMs)
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
        </dl>
      </div>
      <PoolChart pool={pool} dashboard={dashboard} color={color} formatTime={formatTime} />
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
        <LegendLine color={color} label="Recorded" />
        {pool.projected.length > 0 ? <LegendLine color={color} label="Projected" dashed /> : null}
        {unavailable ? <span>{unavailable}</span> : null}
        {dashboard.historyStartedAtMs !== null &&
        dashboard.historyStartedAtMs > dashboard.fromMs ? (
          <span>History since {formatDay(dashboard.historyStartedAtMs)}</span>
        ) : null}
      </div>
    </div>
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
    <div className="flex min-w-0 flex-col gap-0.5">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="text-sm font-medium text-foreground tabular-nums">{value}</dd>
      {detail ? <dd className="text-muted-foreground tabular-nums">{detail}</dd> : null}
    </div>
  );
}

function LegendLine({
  color,
  label,
  dashed = false,
}: {
  readonly color: string;
  readonly label: string;
  readonly dashed?: boolean;
}) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <svg aria-hidden width="16" height="2" className="overflow-visible">
        <line
          x1="0"
          x2="16"
          y1="1"
          y2="1"
          stroke={color}
          strokeWidth="2"
          strokeDasharray={dashed ? "4 3" : undefined}
        />
      </svg>
      {label}
    </span>
  );
}

function PoolAccountChips({
  accounts,
  formatTime,
  nowMs,
}: {
  readonly accounts: ReturnType<typeof linkPoolAccounts>;
  readonly formatTime: (at: number) => string;
  readonly nowMs: number;
}) {
  return (
    <span className="flex items-center gap-1">
      {accounts.map(({ account, limitAccount }) => {
        const name = limitAccount?.displayName ?? limitAccount?.email ?? account.id;
        const parts = [
          account.remainingPercent === null
            ? null
            : `${Math.round(account.remainingPercent)}% left`,
          account.burnPointsPerHour === null ? null : formatPoolBurn(account.burnPointsPerHour),
          account.emptyAtMs === null
            ? null
            : `empty ${account.emptyAtMs <= nowMs ? "now" : `at ${formatTime(account.emptyAtMs)}`}`,
          account.status === "ok" ? null : account.status.replace("-", " "),
        ].filter((part) => part !== null);
        return (
          <Tooltip key={account.id}>
            <TooltipTrigger
              render={<span tabIndex={0} className="inline-flex rounded-full outline-none" />}
            >
              {limitAccount?.email ? (
                <AccountChip email={limitAccount.email} />
              ) : (
                <span className="inline-flex size-4 items-center justify-center rounded-full bg-muted text-3xs font-semibold text-muted-foreground">
                  ?
                </span>
              )}
            </TooltipTrigger>
            <TooltipPopup side="top">
              <div className="flex flex-col gap-0.5">
                <span className="text-foreground">{name}</span>
                {parts.length > 0 ? (
                  <span className="text-muted-foreground tabular-nums">{parts.join(" · ")}</span>
                ) : null}
              </div>
            </TooltipPopup>
          </Tooltip>
        );
      })}
    </span>
  );
}

function timeFormatter(hour12: boolean) {
  const format = new Intl.DateTimeFormat(undefined, {
    weekday: "short",
    hour: "numeric",
    minute: "2-digit",
    hour12,
  });
  return (at: number) => format.format(at);
}

const dayFormat = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric" });
const formatDay = (at: number) => dayFormat.format(at);

/** The five-hour pool steps several times a day, so it gets a closer view than the dashboard's. */
function viewRange(dashboard: UsageLimitPoolDashboard): { from: number; to: number } {
  if (dashboard.window === "weekly") return { from: dashboard.fromMs, to: dashboard.toMs };
  return {
    from: Math.max(dashboard.fromMs, dashboard.nowMs - DAY),
    to: Math.min(dashboard.toMs, dashboard.nowMs + 2 * DAY),
  };
}

/** Local midnights, or every few hours on a short view, at most `maxTicks` of them. */
function timeTicks(from: number, to: number, maxTicks: number): number[] {
  const stepHours = [6, 12, 24, 48, 72].find((hours) => (to - from) / (hours * HOUR) <= maxTicks) ?? 96;
  const tick = new Date(from);
  tick.setHours(0, 0, 0, 0);
  const ticks: number[] = [];
  for (; tick.getTime() <= to; tick.setHours(tick.getHours() + stepHours)) {
    if (tick.getTime() >= from) ticks.push(tick.getTime());
  }
  return ticks;
}

const hourFormat = new Intl.DateTimeFormat(undefined, { hour: "numeric" });
const weekdayFormat = new Intl.DateTimeFormat(undefined, { weekday: "short" });
function formatTick(at: number): string {
  const date = new Date(at);
  return date.getHours() === 0
    ? `${weekdayFormat.format(date)} ${date.getDate()}`
    : hourFormat.format(date);
}

interface Sample {
  readonly atMs: number;
  readonly remainingPoints: number;
  readonly projected: boolean;
}

function chartGeometry(pool: UsageLimitPool, dashboard: UsageLimitPoolDashboard, width: number) {
  const { from, to } = viewRange(dashboard);
  const right = width - FRAME.right;
  const bottom = FRAME.height - FRAME.bottom;
  const inView = (at: number) => at >= from && at <= to;
  const values = [...pool.recorded, ...pool.projected].flatMap((point) =>
    point.remainingPoints !== null && inView(point.atMs) ? [point.remainingPoints] : [],
  );
  const max = Math.max(pool.capacityPoints, ...values) || 100;
  const x = (at: number) => FRAME.left + ((at - from) / (to - from)) * (right - FRAME.left);
  const y = (points: number) => bottom - (points / max) * (bottom - FRAME.top);
  const line = (run: ReadonlyArray<{ atMs: number; remainingPoints: number }>) =>
    run
      .map(
        (point, index) =>
          `${index ? "L" : "M"}${x(point.atMs).toFixed(1)},${y(point.remainingPoints).toFixed(1)}`,
      )
      .join("");
  const area = (run: ReadonlyArray<{ atMs: number; remainingPoints: number }>) =>
    `${line(run)}L${x(run.at(-1)!.atMs).toFixed(1)},${bottom}L${x(run[0]!.atMs).toFixed(1)},${bottom}Z`;
  const recorded = knownRuns(pool.recorded);
  const projected = knownRuns(pool.projected);
  const samples: Sample[] = [
    ...recorded.flat().map((point) => ({ ...point, projected: false })),
    ...projected.flat().map((point) => ({ ...point, projected: true })),
  ]
    .filter((sample) => inView(sample.atMs))
    .toSorted((a, b) => a.atMs - b.atMs);
  const resets = pool.resets.filter((reset) => inView(reset.atMs) && reset.gainPoints > 0);
  const nowX = inView(dashboard.nowMs) ? x(dashboard.nowMs) : null;
  const labels = labeledResets(
    resets.map((reset) => x(reset.atMs)),
    nowX,
    56,
  );
  return {
    from,
    to,
    right,
    bottom,
    max,
    x,
    y,
    nowX,
    samples,
    recorded: recorded.map((run) => ({ line: line(run), area: area(run) })),
    projected: projected.map((run) => ({ line: line(run), area: area(run) })),
    resets: resets.map((reset, index) => ({ ...reset, labeled: labels[index] ?? false })),
    ticks: timeTicks(from, to, Math.max(2, Math.floor((right - FRAME.left) / 60))),
    poolEmptyAtMs:
      pool.poolEmptyAtMs !== null && inView(pool.poolEmptyAtMs) ? pool.poolEmptyAtMs : null,
  };
}

function nearestSample(samples: readonly Sample[], at: number): number {
  let low = 0;
  let high = samples.length - 1;
  while (low < high) {
    const mid = (low + high) >> 1;
    if (samples[mid]!.atMs < at) low = mid + 1;
    else high = mid;
  }
  const previous = samples[low - 1];
  return previous !== undefined && at - previous.atMs < samples[low]!.atMs - at ? low - 1 : low;
}

function PoolChart({
  pool,
  dashboard,
  color,
  formatTime,
}: {
  readonly pool: UsageLimitPool;
  readonly dashboard: UsageLimitPoolDashboard;
  readonly color: string;
  readonly formatTime: (at: number) => string;
}) {
  const id = useId();
  const [container, setContainer] = useState<HTMLDivElement | null>(null);
  const [width, setWidth] = useState(0);
  const [hovered, setHovered] = useState<number | null>(null);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- measures the chart's width so labels keep their CSS size instead of scaling with the SVG
  useLayoutEffect(() => {
    if (!container) return;
    const measure = () => setWidth(container.getBoundingClientRect().width);
    measure();
    return observeResize(container, measure);
  }, [container]);
  const geometry = width > 0 ? chartGeometry(pool, dashboard, width) : null;
  const sample = geometry && hovered !== null ? geometry.samples[hovered] : undefined;

  const inspect = (event: PointerEvent<SVGSVGElement>) => {
    if (!geometry || geometry.samples.length === 0) return;
    const bounds = event.currentTarget.getBoundingClientRect();
    const cursor = event.clientX - bounds.left;
    const at =
      geometry.from +
      ((cursor - FRAME.left) / (geometry.right - FRAME.left)) * (geometry.to - geometry.from);
    setHovered(nearestSample(geometry.samples, at));
  };

  return (
    <div ref={setContainer} className="relative w-full" style={{ height: FRAME.height }}>
      {geometry ? (
        <svg
          width={width}
          height={FRAME.height}
          role="img"
          aria-label={`${pool.provider === "claude" ? "Claude" : "Codex"} pool: recorded and projected quota left`}
          className="block touch-pan-y select-none"
          onPointerMove={inspect}
          onPointerDown={inspect}
          onPointerLeave={() => setHovered(null)}
        >
          <defs>
            <linearGradient id={`${id}-recorded`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity={0.22} />
              <stop offset="100%" stopColor={color} stopOpacity={0} />
            </linearGradient>
            <linearGradient id={`${id}-projected`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity={0.09} />
              <stop offset="100%" stopColor={color} stopOpacity={0} />
            </linearGradient>
            <clipPath id={`${id}-plot`}>
              <rect
                x={FRAME.left}
                y={FRAME.top - 4}
                width={geometry.right - FRAME.left}
                height={geometry.bottom - FRAME.top + 8}
              />
            </clipPath>
          </defs>
          {[0, geometry.max / 2, geometry.max].map((value) => (
            <g key={value} className="text-border">
              <line
                x1={FRAME.left}
                x2={geometry.right}
                y1={geometry.y(value)}
                y2={geometry.y(value)}
                stroke="currentColor"
              />
              <text
                x={FRAME.left - 8}
                y={geometry.y(value) + 3.5}
                textAnchor="end"
                className="fill-muted-foreground text-3xs tabular-nums"
              >
                {formatPoolPercent(value)}
              </text>
            </g>
          ))}
          {geometry.ticks.map((at) => (
            <g key={at}>
              <line
                x1={geometry.x(at)}
                x2={geometry.x(at)}
                y1={FRAME.top}
                y2={geometry.bottom}
                stroke="currentColor"
                strokeOpacity={0.5}
                className="text-border"
              />
              <text
                x={geometry.x(at)}
                y={geometry.bottom + 16}
                textAnchor="middle"
                className="fill-muted-foreground text-3xs"
              >
                {formatTick(at)}
              </text>
            </g>
          ))}
          <g clipPath={`url(#${id}-plot)`}>
            {geometry.recorded.map((run) => (
              <path key={run.line} d={run.area} fill={`url(#${id}-recorded)`} />
            ))}
            {geometry.projected.map((run) => (
              <path key={run.line} d={run.area} fill={`url(#${id}-projected)`} />
            ))}
            {geometry.recorded.map((run) => (
              <path
                key={run.line}
                d={run.line}
                fill="none"
                stroke={color}
                strokeWidth={2}
                strokeLinejoin="round"
                strokeLinecap="round"
              />
            ))}
            {geometry.projected.map((run) => (
              <path
                key={run.line}
                d={run.line}
                fill="none"
                stroke={color}
                strokeWidth={1.75}
                strokeDasharray="6 4"
                strokeLinejoin="round"
              />
            ))}
          </g>
          {geometry.nowX !== null ? (
            <g>
              <line
                x1={geometry.nowX}
                x2={geometry.nowX}
                y1={FRAME.top}
                y2={geometry.bottom}
                stroke="currentColor"
                className="text-muted-foreground"
                strokeOpacity={0.6}
              />
              <text
                x={geometry.nowX}
                y={FRAME.top - 8}
                textAnchor="middle"
                className="fill-muted-foreground text-3xs"
              >
                Now
              </text>
            </g>
          ) : null}
          {geometry.resets.map((reset) => (
            <g key={reset.atMs}>
              <title>{`Reset ${formatTime(reset.atMs)} · +${formatPoolPercent(reset.gainPoints)}`}</title>
              <line
                x1={geometry.x(reset.atMs)}
                x2={geometry.x(reset.atMs)}
                y1={FRAME.top}
                y2={FRAME.top + 6}
                stroke="currentColor"
                className="text-muted-foreground"
              />
              {reset.labeled ? (
                <text
                  x={geometry.x(reset.atMs)}
                  y={FRAME.top - 8}
                  textAnchor="middle"
                  className="fill-muted-foreground text-3xs tabular-nums"
                >
                  +{formatPoolPercent(reset.gainPoints)}
                </text>
              ) : null}
            </g>
          ))}
          {geometry.poolEmptyAtMs !== null ? (
            <g>
              <circle
                cx={geometry.x(geometry.poolEmptyAtMs)}
                cy={geometry.bottom}
                r={4}
                strokeWidth={2}
                stroke={color}
                className="fill-background"
              />
              <text
                x={geometry.x(geometry.poolEmptyAtMs)}
                y={geometry.bottom - 10}
                textAnchor={
                  geometry.x(geometry.poolEmptyAtMs) > geometry.right - 70 ? "end" : "middle"
                }
                className="fill-foreground text-3xs font-medium"
              >
                Empty {formatTime(geometry.poolEmptyAtMs)}
              </text>
            </g>
          ) : null}
          {sample ? (
            <g pointerEvents="none">
              <line
                x1={geometry.x(sample.atMs)}
                x2={geometry.x(sample.atMs)}
                y1={FRAME.top}
                y2={geometry.bottom}
                stroke={color}
                strokeOpacity={0.5}
              />
              <circle
                cx={geometry.x(sample.atMs)}
                cy={geometry.y(sample.remainingPoints)}
                r={4}
                fill={color}
                strokeWidth={2}
                className="stroke-background"
              />
            </g>
          ) : null}
        </svg>
      ) : null}
      {geometry && sample ? (
        <ChartTooltip
          x={geometry.x(sample.atMs)}
          width={width}
          text={`${formatTime(sample.atMs)} · ${formatPoolPercent(sample.remainingPoints)} left · ${sample.projected ? "projected" : "recorded"}`}
        />
      ) : null}
    </div>
  );
}

function ChartTooltip({
  x,
  width,
  text,
}: {
  readonly x: number;
  readonly width: number;
  readonly text: string;
}) {
  const ref = useRef<HTMLDivElement | null>(null);
  const [tooltipWidth, setTooltipWidth] = useState(0);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- measures the tooltip's DOM width before paint so it stays inside the chart
  useLayoutEffect(() => {
    if (ref.current) setTooltipWidth(ref.current.offsetWidth);
  });
  const left = Math.min(Math.max(0, x - tooltipWidth / 2), Math.max(0, width - tooltipWidth));
  return (
    <div
      ref={ref}
      className="surface-glass pointer-events-none absolute top-0 z-10 rounded-lg border border-border/50 px-2 py-1 text-xs whitespace-nowrap text-foreground shadow-lg tabular-nums"
      style={{ left }}
    >
      {text}
    </div>
  );
}
