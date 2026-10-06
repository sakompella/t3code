/**
 * The heartbeats a thread's provider session runs, for a quiet line in the
 * composer area. A heartbeat is configuration, not work: it is kept out of the
 * pending-background-work roster so it never makes a thread look busy.
 */
import {
  isOrchestrationV2RoutineRun,
  type OrchestrationV2ProviderHeartbeat,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import type { EnvironmentThreadShell } from "./models.ts";

type Projection = OrchestrationV2ThreadProjection;

const NO_HEARTBEATS: ReadonlyArray<OrchestrationV2ProviderHeartbeat> = [];

/** The active provider thread's heartbeats; a handed-off thread's old ones do not run. */
export function deriveThreadHeartbeats(
  projection: Pick<Projection, "providerThreads" | "thread">,
): ReadonlyArray<OrchestrationV2ProviderHeartbeat> {
  const activeId = projection.thread.activeProviderThreadId;
  if (activeId === null) return NO_HEARTBEATS;
  const heartbeats = projection.providerThreads.find(
    (thread) => thread.id === activeId,
  )?.heartbeats;
  return heartbeats === undefined || heartbeats.length === 0 ? NO_HEARTBEATS : heartbeats;
}

export function heartbeatsEqual(
  left: ReadonlyArray<OrchestrationV2ProviderHeartbeat>,
  right: ReadonlyArray<OrchestrationV2ProviderHeartbeat>,
): boolean {
  return (
    left.length === right.length &&
    left.every((heartbeat, index) => {
      const other = right[index]!;
      return (
        heartbeat.id === other.id &&
        heartbeat.description === other.description &&
        heartbeat.schedule === other.schedule &&
        heartbeat.paused === other.paused &&
        heartbeat.nextRunAt === other.nextRunAt
      );
    })
  );
}

export interface HeartbeatPresentation {
  readonly title: string;
  /** Next run, or what the heartbeat is for. Null when there is nothing to add. */
  readonly detail: string | null;
}

/**
 * A static timestamp, never a countdown: the time of day, with the weekday
 * when the run falls on another day than `now`.
 */
export function formatHeartbeatNextRun(nextRunAt: string, now: Date): string {
  const date = DateTime.toDate(DateTime.makeUnsafe(nextRunAt));
  const time = date.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
  if (date.toDateString() === now.toDateString()) return time;
  return `${date.toLocaleDateString(undefined, { weekday: "short" })} ${time}`;
}

function heartbeatWhen(heartbeat: OrchestrationV2ProviderHeartbeat, now: Date): string | null {
  if (heartbeat.paused) return "paused";
  return heartbeat.nextRunAt === undefined
    ? null
    : `next ${formatHeartbeatNextRun(heartbeat.nextRunAt, now)}`;
}

const capitalize = (text: string) => `${text.charAt(0).toUpperCase()}${text.slice(1)}`;

/** Null when the thread has no heartbeat. */
export function presentHeartbeats(
  heartbeats: ReadonlyArray<OrchestrationV2ProviderHeartbeat>,
  now: Date,
): HeartbeatPresentation | null {
  const [only] = heartbeats;
  if (only === undefined) return null;
  if (heartbeats.length === 1) {
    const when = heartbeatWhen(only, now);
    const detail = [only.description, when === null ? undefined : capitalize(when)]
      .filter((part) => part !== undefined)
      .join(" · ");
    return { title: `Heartbeat ${only.schedule}`, detail: detail.length === 0 ? null : detail };
  }
  const lines = heartbeats.map((heartbeat) => {
    const when = heartbeatWhen(heartbeat, now);
    return when === null ? heartbeat.schedule : `${heartbeat.schedule}, ${when}`;
  });
  return { title: `${heartbeats.length} heartbeats`, detail: lines.join("; ") };
}

/**
 * When the thread last ended work the user may not have seen, for unread
 * state: the latest run's end, unless that run was a routine heartbeat check.
 * Then it is the end of the latest run someone asked for, so a check neither
 * marks the thread unread nor hides an unseen completion from before it.
 */
export function threadUnreadCompletionAt(
  thread: Pick<EnvironmentThreadShell, "latestRun" | "latestTaskRunCompletedAt">,
): string | null {
  const run = thread.latestRun;
  if (run === null) return null;
  return isOrchestrationV2RoutineRun(run)
    ? (thread.latestTaskRunCompletedAt ?? null)
    : run.completedAt;
}
