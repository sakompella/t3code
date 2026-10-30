import * as Schema from "effect/Schema";

import {
  ForwardCompatibleArray,
  IsoDateTime,
  NonNegativeInt,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ProviderDriverKind, ProviderInstanceId } from "./providerInstance.ts";
import { UsageLimitSourceId } from "./usageLimitSourceId.ts";

/** Stable per-provider ids let sparse updates merge with full probes. */
export const ServerProviderUsageWindow = Schema.Struct({
  id: TrimmedNonEmptyString,
  kind: Schema.Literals(["session", "weekly", "monthly", "other"]),
  label: TrimmedNonEmptyString,
  usedPercent: Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 100 })),
  resetsAt: Schema.optional(IsoDateTime),
  windowDurationMins: Schema.optional(NonNegativeInt),
});
export type ServerProviderUsageWindow = typeof ServerProviderUsageWindow.Type;

export const ServerProviderResetCredits = Schema.Struct({
  availableCount: NonNegativeInt,
  nextExpiresAt: Schema.optional(IsoDateTime),
  /** Pins hub redemption to the displayed credit, including retries from another client. */
  nextCreditId: Schema.optional(TrimmedNonEmptyString),
});
export type ServerProviderResetCredits = typeof ServerProviderResetCredits.Type;

/** Clear bars for unavailable accounts; retain last good bars after a failed probe. */
export const ServerProviderUsageLimits = Schema.Struct({
  checkedAt: IsoDateTime,
  windows: ForwardCompatibleArray(ServerProviderUsageWindow),
  credentialFingerprint: Schema.optional(TrimmedNonEmptyString),
  resetCredits: Schema.optional(ServerProviderResetCredits),
  externalUsage: Schema.optional(
    Schema.Struct({
      label: TrimmedNonEmptyString,
      url: TrimmedNonEmptyString,
    }),
  ),
  unavailable: Schema.optional(
    Schema.Struct({
      reason: Schema.Literals(["unsupported", "probeFailed"]),
      message: Schema.optional(TrimmedNonEmptyString),
    }),
  ),
});
export type ServerProviderUsageLimits = typeof ServerProviderUsageLimits.Type;

/** Partial updates merge windows by id; omitted windows remain unchanged. */
export const ProviderUsageLimitsUpdate = Schema.Struct({
  windows: Schema.Array(ServerProviderUsageWindow),
});
export type ProviderUsageLimitsUpdate = typeof ProviderUsageLimitsUpdate.Type;

export const UsageLimitSourceAccount = Schema.Struct({
  id: TrimmedNonEmptyString,
  driver: ProviderDriverKind,
  email: Schema.optional(TrimmedNonEmptyString),
  plan: Schema.optional(TrimmedNonEmptyString),
  workspaceId: Schema.optional(TrimmedNonEmptyString),
  usageLimits: ServerProviderUsageLimits,
});
export type UsageLimitSourceAccount = typeof UsageLimitSourceAccount.Type;

export const UsageLimitSourceSnapshot = Schema.Struct({
  id: UsageLimitSourceId,
  kind: Schema.Literal("cliproxy"),
  label: TrimmedNonEmptyString,
  checkedAt: IsoDateTime,
  accounts: ForwardCompatibleArray(UsageLimitSourceAccount),
  error: Schema.optional(TrimmedNonEmptyString),
});
export type UsageLimitSourceSnapshot = typeof UsageLimitSourceSnapshot.Type;

export const UsageLimitSourceSnapshots = ForwardCompatibleArray(UsageLimitSourceSnapshot);
export type UsageLimitSourceSnapshots = typeof UsageLimitSourceSnapshots.Type;

export const UsageLimitSourceConsumeResetCreditInput = Schema.Struct({
  sourceId: UsageLimitSourceId,
  accountId: TrimmedNonEmptyString,
  creditId: TrimmedNonEmptyString,
});
export type UsageLimitSourceConsumeResetCreditInput =
  typeof UsageLimitSourceConsumeResetCreditInput.Type;

export const ProviderConsumeResetCreditInput = Schema.Union([
  Schema.Struct({ instanceId: ProviderInstanceId }),
  UsageLimitSourceConsumeResetCreditInput,
]);
export type ProviderConsumeResetCreditInput = typeof ProviderConsumeResetCreditInput.Type;

/**
 * Quota history and forecast a CLIProxyAPI hub keeps (`GET v0/management/quota/pools`, the Prox
 * pool dashboard shape). The hub records and projects; clients only draw. Remaining values are
 * points: 100 per account at its base plan, so a pool of two accounts holds 200.
 */
export const UsageLimitPoolWindow = Schema.Literals(["weekly", "five-hour"]);
export type UsageLimitPoolWindow = typeof UsageLimitPoolWindow.Type;

export const UsageLimitPoolLookbackHours = Schema.Literals([1, 3, 6]);
export type UsageLimitPoolLookbackHours = typeof UsageLimitPoolLookbackHours.Type;

export const UsageLimitPoolsInput = Schema.Struct({
  sourceId: UsageLimitSourceId,
  window: UsageLimitPoolWindow,
  lookbackHours: UsageLimitPoolLookbackHours,
});
export type UsageLimitPoolsInput = typeof UsageLimitPoolsInput.Type;

const EpochMillis = Schema.Finite;
const Points = Schema.Finite;

export const UsageLimitPoolPoint = Schema.Struct({
  atMs: EpochMillis,
  remainingPoints: Schema.NullOr(Points),
});
export type UsageLimitPoolPoint = typeof UsageLimitPoolPoint.Type;

export const UsageLimitPoolReset = Schema.Struct({
  atMs: EpochMillis,
  gainPoints: Points,
});
export type UsageLimitPoolReset = typeof UsageLimitPoolReset.Type;

export const UsageLimitPoolAccount = Schema.Struct({
  /** The hub's auth file id, the same id as `UsageLimitSourceAccount.id`. */
  id: TrimmedNonEmptyString,
  status: Schema.Literals(["ok", "stale", "error", "disabled", "missing-window"]),
  observedAtMs: Schema.NullOr(EpochMillis),
  remainingPercent: Schema.NullOr(Schema.Finite),
  resetAtMs: Schema.NullOr(EpochMillis),
  burnPointsPerHour: Schema.NullOr(Points),
  /** When this account alone runs out at its own measured burn, if before its reset. */
  emptyAtMs: Schema.NullOr(EpochMillis),
});
export type UsageLimitPoolAccount = typeof UsageLimitPoolAccount.Type;

export const UsageLimitPool = Schema.Struct({
  provider: Schema.Literals(["claude", "codex"]),
  accountCount: NonNegativeInt,
  eligibleCount: NonNegativeInt,
  loadedCount: NonNegativeInt,
  capacityPoints: Points,
  remainingPoints: Schema.NullOr(Points),
  burnPointsPerHour: Schema.NullOr(Points),
  /** Only `ready` pools have a projection; the others say why not. */
  forecastState: Schema.Literals(["ready", "collecting", "partial", "stale", "no-accounts"]),
  firstEmptyAtMs: Schema.NullOr(EpochMillis),
  poolEmptyAtMs: Schema.NullOr(EpochMillis),
  nextResetAtMs: Schema.NullOr(EpochMillis),
  nextResetGainPoints: Schema.NullOr(Points),
  recorded: Schema.Array(UsageLimitPoolPoint),
  projected: Schema.Array(UsageLimitPoolPoint),
  resets: Schema.Array(UsageLimitPoolReset),
  accounts: ForwardCompatibleArray(UsageLimitPoolAccount),
});
export type UsageLimitPool = typeof UsageLimitPool.Type;

export const UsageLimitPoolDashboard = Schema.Struct({
  nowMs: EpochMillis,
  fromMs: EpochMillis,
  toMs: EpochMillis,
  window: UsageLimitPoolWindow,
  lookbackHours: UsageLimitPoolLookbackHours,
  historyStartedAtMs: Schema.NullOr(EpochMillis),
  pools: ForwardCompatibleArray(UsageLimitPool),
});
export type UsageLimitPoolDashboard = typeof UsageLimitPoolDashboard.Type;

/** Null when the hub keeps no quota history (a CLIProxyAPI build without it). */
export const UsageLimitPoolsResult = Schema.NullOr(UsageLimitPoolDashboard);
export type UsageLimitPoolsResult = typeof UsageLimitPoolsResult.Type;

export class UsageLimitSourceError extends Schema.TaggedError<UsageLimitSourceError>()(
  "UsageLimitSourceError",
  { detail: Schema.String },
) {
  override get message(): string {
    return this.detail;
  }
}

export const ProviderConsumeResetCreditOutcome = Schema.Literals([
  "reset",
  "nothingToReset",
  "noCredit",
  "alreadyRedeemed",
]);
export type ProviderConsumeResetCreditOutcome = typeof ProviderConsumeResetCreditOutcome.Type;

export const ProviderConsumeResetCreditResult = Schema.Struct({
  outcome: ProviderConsumeResetCreditOutcome,
  warning: Schema.optional(TrimmedNonEmptyString),
});
export type ProviderConsumeResetCreditResult = typeof ProviderConsumeResetCreditResult.Type;

export const UsageLimitsReport = Schema.Struct({
  createdAt: IsoDateTime,
  accounts: Schema.Array(
    Schema.Struct({
      id: TrimmedNonEmptyString,
      driver: ProviderDriverKind,
      label: TrimmedNonEmptyString,
      plan: Schema.optional(TrimmedNonEmptyString),
      email: Schema.optional(TrimmedNonEmptyString),
      sourceLabel: Schema.optional(TrimmedNonEmptyString),
      instanceId: Schema.optional(ProviderInstanceId),
      resetCreditInput: Schema.optional(ProviderConsumeResetCreditInput),
      displayName: Schema.optional(Schema.String),
      accentColor: Schema.optional(Schema.String),
      limits: ServerProviderUsageLimits,
    }),
  ),
  notices: Schema.Array(Schema.String),
});
export type UsageLimitsReport = typeof UsageLimitsReport.Type;
