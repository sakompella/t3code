import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import type { OrchestrationV2ProviderThread, ProviderDriverKind } from "@t3tools/contracts";
import {
  detachedBashJobs,
  endsHandle,
  reportedCommandMatches,
  type DetachedBashJob,
} from "./primeAgentIpythonCell.ts";
import type { OrchestrationV2Notification } from "@t3tools/contracts";
import {
  piRecordField as recordField,
  piRecordString as recordString,
  type PiRpcRecord,
} from "./PiRpc.ts";
import type { ActivePiTurn, PiItemHooks } from "./PiAdapterV2State.ts";

/**
 * Agent work that belongs to a wake turn. Dialogs, acks, and child session
 * observation keep flowing live: a child's events must not wait on the
 * parent's turn, or its roster update can drop the route first.
 */
export function isPiWakeEvent(event: PiRpcRecord): boolean {
  switch (event["type"]) {
    case "response":
    case "rlm_child_update":
    case "observed_session_event":
    case "observed_session_closed":
    case "extension_ui_request":
    case "extension_error":
    case "t3.settle_probe":
    case "t3.flush_extension_errors":
      return false;
    default:
      return true;
  }
}

const NOTIFICATION_DETAIL_MAX_LENGTH = 2_000;
const COMMAND_DETAIL_MAX_LENGTH = 500;

function clip(text: string, maxLength: number): string {
  return text.length <= maxLength ? text : `${text.slice(0, maxLength - 1).trimEnd()}…`;
}

/** Notification fields that carry `detail` only when there is something to show. */
function withDetail(detail: string | undefined): { readonly detail?: string } {
  const text = detail?.trim();
  return text ? { detail: clip(text, NOTIFICATION_DETAIL_MAX_LENGTH) } : {};
}

/**
 * The command a background job ran, how it ended, and the handle variable it
 * was started with when the adapter tracked it. Nothing when the message does
 * not say which command finished.
 */
function backgroundCommandDetail(
  details: unknown,
  handleVariable: string | null,
): string | undefined {
  const command = recordString(details, "command")?.trim();
  if (!command) return undefined;
  const exitCode = recordField(details, "exitCode");
  return [
    clip(command, COMMAND_DETAIL_MAX_LENGTH),
    typeof exitCode === "number" ? `Exit code ${exitCode}` : undefined,
    handleVariable === null ? undefined : `Handle: ${handleVariable}`,
  ]
    .filter((line) => line !== undefined)
    .join("\n\n");
}

/** What a heartbeat ran: the message's text after its `[heartbeat: ...]` header line. */
function heartbeatPrompt(message: unknown): string | undefined {
  return recordString(message, "content")?.replace(/^\[[^\]\n]*\]\s*/, "");
}

/**
 * Finds the handle variable of the tracked background job a completion
 * message reports, or null when the job is unknown or kept no handle.
 */
export type BackgroundJobHandleOf = (message: unknown) => string | null;

/**
 * What a custom message Prime Agent injected means to the user, or null for
 * messages with no known meaning. Bookkeeping notices such as
 * `ipython_state_restored` have none. `detail` is set only where the message
 * carries content beyond its summary, so a row without it has nothing to expand.
 */
export function piCustomMessageNotification(
  message: unknown,
  handleOf: BackgroundJobHandleOf = () => null,
): OrchestrationV2Notification | null {
  const customType = recordString(message, "customType");
  const details = recordField(message, "details");
  if (customType === "agent_message") {
    const sender = recordString(recordField(details, "from"), "sessionName") ?? "an agent";
    return {
      source:
        recordString(details, "fromRelationship") === "child"
          ? { kind: "subagent" }
          : { kind: "background_task" },
      outcome: "updated",
      summary: `Message from ${sender}`,
      ...withDetail(recordString(details, "message")),
    };
  }
  if (customType === "async_bash_completion") {
    return {
      source: { kind: "command" },
      outcome: "completed",
      summary: "Background command finished",
      ...withDetail(backgroundCommandDetail(details, handleOf(message))),
    };
  }
  if (customType === "heartbeat_prompt") {
    return {
      source: { kind: "background_task" },
      outcome: "updated",
      summary: "Heartbeat",
      ...withDetail(heartbeatPrompt(message)),
    };
  }
  if (customType === "rlm_child_failure") {
    return {
      source: { kind: "subagent" },
      outcome: "failed",
      summary: "Subagent failed",
      ...withDetail(recordString(details, "error")),
    };
  }
  if (customType === "rlm_child_terminal_notice") {
    return {
      source: { kind: "subagent" },
      outcome: "completed",
      summary: "Subagent finished",
      ...withDetail(
        recordString(details, "reason") ?? recordString(details, "lastAssistantTextPreview"),
      ),
    };
  }
  return null;
}

/**
 * The first message an idle agent was woken with that has a known meaning,
 * ahead of its own reply.
 */
export function piWakeTrigger(
  events: ReadonlyArray<PiRpcRecord>,
  handleOf?: BackgroundJobHandleOf,
): {
  readonly message: unknown;
  readonly notification: OrchestrationV2Notification;
} | null {
  for (const event of events) {
    if (event["type"] !== "message_start") continue;
    const message = event["message"];
    if (recordString(message, "role") === "assistant") return null;
    const notification = piCustomMessageNotification(message, handleOf);
    if (notification !== null) return { message, notification };
  }
  return null;
}

/** Says what woke the agent, or that it resumed work when nothing says more. */
export function piWakeNotification(
  events: ReadonlyArray<PiRpcRecord>,
  agentName: string,
  handleOf?: BackgroundJobHandleOf,
): OrchestrationV2Notification {
  return (
    piWakeTrigger(events, handleOf)?.notification ?? {
      source: { kind: "background_task" },
      outcome: "updated",
      summary: `${agentName} resumed work`,
    }
  );
}

/**
 * Prime Agent can inject a message into a run that is already going, e.g. a
 * background command that finished. The agent answers it in the same run, so
 * without a row the reply looks like it came from nowhere, and the run folds
 * the earlier reply away. The row is display only: Prime Agent already
 * consumed the message, so nothing is queued or woken.
 */
export function makePrimeAgentWakeNotices(input: {
  readonly enabled: boolean;
  readonly driver: ProviderDriverKind;
  readonly handleOf: BackgroundJobHandleOf;
  readonly items: Pick<PiItemHooks, "emit" | "emitItemNode" | "baseItemFields">;
}) {
  const { enabled, driver, handleOf } = input;
  const { emit, emitItemNode, baseItemFields } = input.items;

  const emitMidRunWakeNotice = Effect.fnUntraced(function* (turn: ActivePiTurn, message: unknown) {
    if (!enabled || message === turn.wakeTrigger) return;
    const notification = piCustomMessageNotification(message, handleOf);
    if (notification === null) return;
    const emittedAt = yield* DateTime.now;
    // Provider item ordinals restart in every thread and attempt, so the id
    // needs the provider turn id; the count tells one message from the next.
    turn.noticeCount += 1;
    const nativeItemId = `${turn.providerTurn.id}:wake:${turn.noticeCount}`;
    yield* emitItemNode(turn, nativeItemId, "system", "completed", emittedAt, emittedAt);
    yield* emit({
      type: "turn_item.updated",
      driver,
      turnItem: {
        ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
        status: "completed",
        title: notification.summary,
        completedAt: emittedAt,
        type: "notification",
        ...notification,
      },
    });
  });

  return { emitMidRunWakeNotice };
}

export function makePrimeAgentBackgroundJobs(
  publish: (
    patch: Pick<OrchestrationV2ProviderThread, "pendingBackgroundTasks">,
  ) => Effect.Effect<void>,
) {
  const backgroundJobs = new Map<string, DetachedBashJob>();
  /** Completion messages by the job they reported, which has left the roster by the time a row is built. */
  const finishedJobs = new WeakMap<object, DetachedBashJob>();
  let backgroundJobCounter = 0;
  const tasks = () =>
    Array.from(backgroundJobs, ([taskId, job]) => ({
      taskId,
      kind: "command" as const,
      description: job.command,
    }));
  const publishBackgroundJobs = Effect.fnUntraced(function* () {
    yield* publish({ pendingBackgroundTasks: tasks() });
  });

  /** Tracks jobs a finished cell started in the background, and drops those it awaited or killed. */
  const trackBackgroundJobs = Effect.fnUntraced(function* (code: string) {
    let changed = false;
    for (const [taskId, job] of backgroundJobs) {
      if (job.variable !== null && endsHandle(code, job.variable)) {
        backgroundJobs.delete(taskId);
        changed = true;
      }
    }
    for (const job of detachedBashJobs(code)) {
      backgroundJobs.set(`bash:${++backgroundJobCounter}`, job);
      changed = true;
    }
    if (changed) yield* publishBackgroundJobs();
  });

  /** The kernel reports a detached job's end with an `async_bash_completion` message. */
  const completeBackgroundJob = Effect.fnUntraced(function* (message: unknown) {
    if (recordString(message, "customType") !== "async_bash_completion") return;
    const reported = recordString(recordField(message, "details"), "command");
    if (reported === undefined) return;
    for (const [taskId, job] of backgroundJobs) {
      if (reportedCommandMatches(job, reported)) {
        if (typeof message === "object" && message !== null) finishedJobs.set(message, job);
        backgroundJobs.delete(taskId);
        yield* publishBackgroundJobs();
        return;
      }
    }
  });

  return {
    /** The jobs this process knows are running: what a persisted roster has to agree with. */
    tasks,
    trackBackgroundJobs,
    completeBackgroundJob,
    /** The handle variable the job a completion message reported was started with. */
    handleOf: ((message) =>
      typeof message === "object" && message !== null
        ? (finishedJobs.get(message)?.variable ?? null)
        : null) satisfies BackgroundJobHandleOf,
    hasPendingJobs: () => backgroundJobs.size > 0,
    clear: () =>
      Effect.suspend(() => {
        if (backgroundJobs.size === 0) return Effect.void;
        backgroundJobs.clear();
        return publishBackgroundJobs();
      }),
  };
}
