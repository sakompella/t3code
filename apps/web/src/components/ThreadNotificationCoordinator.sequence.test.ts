import { ProviderInstanceId, RunId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  observeThreadForNotification,
  type ThreadNotificationKind,
  type ThreadNotificationMemory,
} from "./ThreadNotificationCoordinator.logic";

type ObservedThread = Parameters<typeof observeThreadForNotification>[0];

const working = (runId: string, trigger: "heartbeat" | null = null): ObservedThread => ({
  latestRun: {
    runId: RunId.make(runId),
    status: "running",
    requestedAt: null,
    startedAt: null,
    completedAt: null,
    assistantMessageId: null,
    trigger,
  },
  runtime: {
    status: "running",
    activeRunId: RunId.make(runId),
    providerInstanceId: ProviderInstanceId.make("primeAgent"),
    providerName: "Prime Agent",
    lastError: null,
    updatedAt: "2026-10-05T10:00:00.000Z",
  },
  hasPendingApprovals: false,
  hasPendingUserInput: false,
});

const ended = (
  thread: ObservedThread,
  status: "completed" | "failed",
  completedAt: string,
): ObservedThread => ({
  ...thread,
  latestRun: { ...thread.latestRun!, status, completedAt },
  runtime: null,
});

/** Feeds snapshots through the coordinator's memory and returns the alerts each raised. */
function alertsFor(
  snapshots: ReadonlyArray<ObservedThread | "reconnect">,
): Array<ThreadNotificationKind | null> {
  let memory: ThreadNotificationMemory | undefined;
  const alerts: Array<ThreadNotificationKind | null> = [];
  for (const snapshot of snapshots) {
    if (snapshot === "reconnect") {
      memory = undefined;
      continue;
    }
    const observation = observeThreadForNotification(snapshot, memory);
    memory = observation.memory;
    alerts.push(observation.kind);
  }
  return alerts;
}

describe("thread alerts across user runs and heartbeat checks", () => {
  it("announces the user's completion once and stays quiet through routine checks", () => {
    const userRun = working("run-user");
    const userDone = ended(userRun, "completed", "2026-10-05T10:01:00.000Z");
    const check = working("run-check", "heartbeat");
    const checkDone = ended(check, "completed", "2026-10-05T10:06:00.000Z");
    const nextUserDone = ended(working("run-next"), "completed", "2026-10-05T10:20:00.000Z");
    expect(
      alertsFor([userRun, userDone, userDone, check, checkDone, checkDone, nextUserDone]),
    ).toEqual([null, "completion", null, null, null, null, "completion"]);
  });

  it("still raises a heartbeat check's question and failure", () => {
    const check = working("run-check", "heartbeat");
    const asks = { ...check, hasPendingUserInput: true };
    const failed = ended(working("run-check-2", "heartbeat"), "failed", "2026-10-05T10:11:00.000Z");
    expect(
      alertsFor([
        check,
        asks,
        check,
        ended(check, "completed", "2026-10-05T10:06:00.000Z"),
        failed,
      ]),
    ).toEqual([null, "input", null, null, "input"]);
  });

  it("stays quiet on the first snapshot after a reconnect, even for news it missed", () => {
    const userDone = ended(working("run-user"), "completed", "2026-10-05T10:01:00.000Z");
    const laterDone = ended(working("run-later"), "completed", "2026-10-05T10:30:00.000Z");
    const failed = ended(working("run-failed"), "failed", "2026-10-05T10:40:00.000Z");
    expect(alertsFor([userDone, "reconnect", laterDone, "reconnect", failed, failed])).toEqual([
      null,
      null,
      null,
      null,
    ]);
  });
});
