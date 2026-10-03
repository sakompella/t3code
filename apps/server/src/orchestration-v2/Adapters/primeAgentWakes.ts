import * as Effect from "effect/Effect";
import type { OrchestrationV2ProviderThread } from "@t3tools/contracts";
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
 * Says what woke the agent, from the first message it was woken with that has
 * a known meaning. Bookkeeping notices such as `ipython_state_restored` are
 * skipped.
 */
export function piWakeNotification(
  events: ReadonlyArray<PiRpcRecord>,
  agentName: string,
): OrchestrationV2Notification {
  for (const event of events) {
    if (event["type"] !== "message_start") continue;
    const message = event["message"];
    if (recordString(message, "role") === "assistant") break;
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
  }
  return {
    source: { kind: "background_task" },
    outcome: "updated",
    summary: `${agentName} resumed work`,
  };
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
