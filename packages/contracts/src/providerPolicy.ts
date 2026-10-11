import {
  ChatImageAttachment,
  ChatFileAttachment,
  PROVIDER_SEND_TURN_MAX_ATTACHMENTS,
} from "./chatAttachment.ts";
import * as Schema from "effect/Schema";

import { TrimmedNonEmptyString } from "./baseSchemas.ts";

export const ProviderApprovalPolicy = Schema.Literals([
  "untrusted",
  "on-failure",
  "on-request",
  "never",
]);
export type ProviderApprovalPolicy = typeof ProviderApprovalPolicy.Type;

export const ProviderSandboxMode = Schema.Literals([
  "read-only",
  "workspace-write",
  "danger-full-access",
]);
export type ProviderSandboxMode = typeof ProviderSandboxMode.Type;

export const RuntimeMode = Schema.Literals([
  "approval-required",
  "auto-accept-edits",
  "auto",
  "full-access",
]);
export type RuntimeMode = typeof RuntimeMode.Type;
export const DEFAULT_RUNTIME_MODE: RuntimeMode = "full-access";

/** The runtime modes a provider offers, in `RuntimeMode` order; every mode when it states none. */
export function offeredRuntimeModes(
  supportedRuntimeModes: ReadonlyArray<RuntimeMode> | undefined,
): ReadonlyArray<RuntimeMode> {
  return supportedRuntimeModes === undefined || supportedRuntimeModes.length === 0
    ? RuntimeMode.literals
    : RuntimeMode.literals.filter((mode) => supportedRuntimeModes.includes(mode));
}

/**
 * The mode a provider runs a thread in. A mode the provider does not offer
 * (a thread set before it stopped offering it, or a stale client) runs in the
 * most restrictive mode it does offer, rather than having T3 imitate it.
 * Clients show this mode, and the server runs and authorizes it.
 */
export function providerRuntimeMode(
  runtimeMode: RuntimeMode,
  supportedRuntimeModes: ReadonlyArray<RuntimeMode> | undefined,
): RuntimeMode {
  const offered = offeredRuntimeModes(supportedRuntimeModes);
  return offered.includes(runtimeMode) ? runtimeMode : (offered[0] ?? runtimeMode);
}

/** Where new threads start: a target's providers and its default model and permissions. */
export interface NewThreadDefaults {
  readonly providers: ReadonlyArray<{
    readonly instanceId: string;
    readonly enabled?: boolean;
    readonly supportedRuntimeModes?: ReadonlyArray<RuntimeMode> | undefined;
  }>;
  readonly defaultModelSelection: { readonly instanceId: string } | null;
  readonly defaultRuntimeMode: RuntimeMode;
}

/**
 * The runtime modes a new thread can run in on `target`: its default
 * provider's, or, when the default is unset or names no enabled provider, the
 * modes every enabled provider offers, since it may start on any of them.
 */
function newThreadOfferedModes(target: NewThreadDefaults): ReadonlyArray<RuntimeMode> {
  const enabled = target.providers.filter((provider) => provider.enabled !== false);
  const byDefault = enabled.find(
    (provider) => provider.instanceId === target.defaultModelSelection?.instanceId,
  );
  return (byDefault === undefined ? enabled : [byDefault]).reduce<ReadonlyArray<RuntimeMode>>(
    (offered, provider) => {
      const providerOffers = offeredRuntimeModes(provider.supportedRuntimeModes);
      return offered.filter((mode) => providerOffers.includes(mode));
    },
    RuntimeMode.literals,
  );
}

/** The default permissions a picker may offer: modes new threads can run in on every target. */
export function runtimeModesForNewThreads(
  targets: ReadonlyArray<NewThreadDefaults>,
): ReadonlyArray<RuntimeMode> {
  return targets.reduce<ReadonlyArray<RuntimeMode>>((offered, target) => {
    const targetOffers = newThreadOfferedModes(target);
    return offered.filter((mode) => targetOffers.includes(mode));
  }, RuntimeMode.literals);
}

/** The mode new threads run in on every target, or null when targets differ. */
export function newThreadRuntimeMode(
  targets: ReadonlyArray<NewThreadDefaults>,
): RuntimeMode | null {
  const modes = new Set(
    targets.map((target) =>
      providerRuntimeMode(target.defaultRuntimeMode, newThreadOfferedModes(target)),
    ),
  );
  const [mode] = modes;
  return modes.size === 1 && mode !== undefined ? mode : null;
}

export const ProviderInteractionMode = Schema.Literals(["default", "plan"]);
export type ProviderInteractionMode = typeof ProviderInteractionMode.Type;
export const DEFAULT_PROVIDER_INTERACTION_MODE: ProviderInteractionMode = "default";

export const ProviderRequestKind = Schema.Literals([
  "command",
  "file-read",
  "file-change",
  "mcp-elicitation",
  "permission",
]);
export type ProviderRequestKind = typeof ProviderRequestKind.Type;

export const AssistantDeliveryMode = Schema.Literals(["buffered", "streaming"]);
export type AssistantDeliveryMode = typeof AssistantDeliveryMode.Type;

export const ProviderApprovalDecision = Schema.Literals([
  "accept",
  "acceptForSession",
  "acceptAlways",
  "decline",
  "cancel",
]);
export type ProviderApprovalDecision = typeof ProviderApprovalDecision.Type;

export const ProviderApprovalOption = Schema.Struct({
  decision: ProviderApprovalDecision,
  label: TrimmedNonEmptyString,
  warning: Schema.optional(TrimmedNonEmptyString),
});
export type ProviderApprovalOption = typeof ProviderApprovalOption.Type;

export const ProviderUserInputAnswers = Schema.Record(Schema.String, Schema.Unknown);
export type ProviderUserInputAnswers = typeof ProviderUserInputAnswers.Type;

export const UserInputAttachments = Schema.Record(
  Schema.String,
  Schema.Array(Schema.Union([ChatImageAttachment, ChatFileAttachment])).pipe(
    Schema.check(Schema.isMaxLength(PROVIDER_SEND_TURN_MAX_ATTACHMENTS)),
  ),
);
export type UserInputAttachments = typeof UserInputAttachments.Type;

export const UserInputAttachmentAnswerPayload = Schema.Struct({
  requestId: TrimmedNonEmptyString,
  questionTextById: Schema.optional(Schema.Record(Schema.String, Schema.String)),
  answers: ProviderUserInputAnswers,
  attachmentsByQuestionId: UserInputAttachments,
});
export type UserInputAttachmentAnswerPayload = typeof UserInputAttachmentAnswerPayload.Type;
