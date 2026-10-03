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

/**
 * What a custom message Prime Agent injected means to the user, or null for
 * messages with no known meaning. Bookkeeping notices such as
 * `ipython_state_restored` have none.
 */
export function piCustomMessageNotification(message: unknown): OrchestrationV2Notification | null {
  const customType = recordString(message, "customType");
  if (customType === "agent_message") {
    const details = recordField(message, "details");
    const sender = recordString(recordField(details, "from"), "sessionName") ?? "an agent";
    const text = recordString(details, "message");
    return {
      source:
        recordString(details, "fromRelationship") === "child"
          ? { kind: "subagent" }
          : { kind: "background_task" },
      outcome: "updated",
      summary: `Message from ${sender}`,
      ...(text === undefined ? {} : { detail: text.slice(0, 2_000) }),
    };
  }
  if (customType === "async_bash_completion") {
    return {
      source: { kind: "command" },
      outcome: "completed",
      summary: "Background command finished",
    };
  }
  if (customType === "heartbeat_prompt") {
    return { source: { kind: "background_task" }, outcome: "updated", summary: "Heartbeat" };
  }
  if (customType === "rlm_child_failure") {
    return { source: { kind: "subagent" }, outcome: "failed", summary: "Subagent failed" };
  }
  if (customType === "rlm_child_terminal_notice") {
    return { source: { kind: "subagent" }, outcome: "completed", summary: "Subagent finished" };
  }
  return null;
}

/**
 * The first message an idle agent was woken with that has a known meaning,
 * ahead of its own reply.
 */
export function piWakeTrigger(events: ReadonlyArray<PiRpcRecord>): {
  readonly message: unknown;
  readonly notification: OrchestrationV2Notification;
} | null {
  for (const event of events) {
    if (event["type"] !== "message_start") continue;
    const message = event["message"];
    if (recordString(message, "role") === "assistant") return null;
    const notification = piCustomMessageNotification(message);
    if (notification !== null) return { message, notification };
  }
  return null;
}

/** Says what woke the agent, or that it resumed work when nothing says more. */
export function piWakeNotification(
  events: ReadonlyArray<PiRpcRecord>,
  agentName: string,
): OrchestrationV2Notification {
  return (
    piWakeTrigger(events)?.notification ?? {
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
  readonly items: Pick<PiItemHooks, "emit" | "emitItemNode" | "baseItemFields">;
}) {
  const { enabled, driver } = input;
  const { emit, emitItemNode, baseItemFields } = input.items;

  const emitMidRunWakeNotice = Effect.fnUntraced(function* (turn: ActivePiTurn, message: unknown) {
    if (!enabled || message === turn.wakeTrigger) return;
    const notification = piCustomMessageNotification(message);
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
    hasPendingJobs: () => backgroundJobs.size > 0,
    clear: () =>
      Effect.suspend(() => {
        if (backgroundJobs.size === 0) return Effect.void;
        backgroundJobs.clear();
        return publishBackgroundJobs();
      }),
  };
}
