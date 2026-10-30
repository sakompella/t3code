import type { ProviderInteractionMode, RuntimeMode, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Ref from "effect/Ref";

export interface DispatchModes {
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
}

export interface DispatchModeRefusal extends DispatchModes {
  readonly threadId: ThreadId;
  readonly mode: "runtime" | "interaction";
}

/**
 * Recheck target modes under the command lock; the user can raise them after preflight. Unset for
 * the user's own commands. Modes are those a thread runs in once the command applies (a provider
 * switch can broaden them, see `RuntimePolicyV2`); the caller may keep, not broaden, its own.
 */
export interface DispatchModeLimitValue extends DispatchModes {
  readonly callerThreadId?: ThreadId;
  readonly refused?: Ref.Ref<DispatchModeRefusal | undefined>;
}

export const DispatchModeLimit = Context.Reference<DispatchModeLimitValue | undefined>(
  "t3/orchestration-v2/DispatchModeLimit",
  { defaultValue: () => undefined },
);

const runtimeModeRank: Record<RuntimeMode, number> = {
  "approval-required": 0,
  "auto-accept-edits": 1,
  auto: 2,
  "full-access": 3,
};
const interactionModeRank: Record<ProviderInteractionMode, number> = { plan: 0, default: 1 };

export const exceededDispatchModeLimit = (
  limit: DispatchModes,
  modes: DispatchModes,
): "runtime" | "interaction" | undefined =>
  runtimeModeRank[modes.runtimeMode] > runtimeModeRank[limit.runtimeMode]
    ? "runtime"
    : interactionModeRank[modes.interactionMode] > interactionModeRank[limit.interactionMode]
      ? "interaction"
      : undefined;
