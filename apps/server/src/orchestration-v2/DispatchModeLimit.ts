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
 * The broadest modes a command may touch, for commands an MCP caller sends.
 * A caller checks its target's modes before dispatching, but the target's
 * user can raise them meanwhile; the orchestrator re-checks this limit inside
 * the thread's command lock, where nothing else can change them. Unset for
 * the user's own commands, which have no limit.
 *
 * A thread's modes here are the modes it runs in: a provider that does not
 * offer its stored runtime mode runs it in another (see `RuntimePolicyV2`).
 * The calling thread is always within its own limit.
 *
 * The orchestrator also records its refusal in `refused`, so the sender can
 * report it however the code in between wrapped the dispatch error.
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
