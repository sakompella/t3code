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
  status: "completed" | "failed" | "interrupted",
  completedAt: string,
): ObservedThread => ({
  ...thread,
  latestRun: { ...thread.latestRun!, status, completedAt },
  runtime: null,
});

/** A thread whose latest run that is not a routine check ended at `taskEndedAt`. */
const afterTask = (thread: ObservedThread, taskEndedAt: string | null): ObservedThread => ({
  ...thread,
  latestTaskRunCompletedAt: taskEndedAt,
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

  describe("when a check follows a user's run within one shell update", () => {
    const userEndedAt = "2026-10-05T10:01:00.000Z";
    const userRun = working("run-user");
    // While a check runs it is the latest run that is not routine, and it has not ended.
    const checkRunning = afterTask(working("run-check", "heartbeat"), null);
    const checkDone = afterTask(
      ended(working("run-check", "heartbeat"), "completed", "2026-10-05T10:01:30.000Z"),
      userEndedAt,
    );
    const nextCheckRunning = afterTask(working("run-check-2", "heartbeat"), null);
    const nextCheckDone = afterTask(
      ended(working("run-check-2", "heartbeat"), "completed", "2026-10-05T10:06:00.000Z"),
      userEndedAt,
    );

    it("announces the user's completion once the check ends, and only once", () => {
      expect(
        alertsFor([userRun, checkRunning, checkDone, checkDone, nextCheckRunning, nextCheckDone]),
      ).toEqual([null, null, "completion", null, null, null]);
    });

    it("announces it when the check also ended in that update", () => {
      expect(alertsFor([userRun, checkDone, nextCheckDone])).toEqual([null, "completion", null]);
    });

    it("does not announce it again after a user's completion already alerted", () => {
      const userDone = afterTask(ended(userRun, "completed", userEndedAt), userEndedAt);
      expect(alertsFor([userRun, userDone, checkRunning, checkDone])).toEqual([
        null,
        "completion",
        null,
        null,
      ]);
    });

    it("stays quiet when the first snapshot after a reconnect is the check", () => {
      expect(alertsFor([checkDone, nextCheckRunning, nextCheckDone])).toEqual([null, null, null]);
    });
  });

  it("never announces a run that failed or was interrupted when a later check ends", () => {
    const interruptedAt = "2026-10-05T10:02:00.000Z";
    const interrupted = afterTask(
      ended(working("run-user"), "interrupted", interruptedAt),
      interruptedAt,
    );
    const failedAt = "2026-10-05T10:12:00.000Z";
    const failedCheck = afterTask(
      ended(working("run-check-1", "heartbeat"), "failed", failedAt),
      failedAt,
    );
    const checkAfter = (runId: string, taskEndedAt: string, completedAt: string) =>
      afterTask(ended(working(runId, "heartbeat"), "completed", completedAt), taskEndedAt);
    expect(
      alertsFor([
        working("run-user"),
        interrupted,
        afterTask(working("run-check", "heartbeat"), null),
        checkAfter("run-check", interruptedAt, "2026-10-05T10:05:00.000Z"),
        failedCheck,
        afterTask(working("run-check-2", "heartbeat"), null),
        checkAfter("run-check-2", failedAt, "2026-10-05T10:15:00.000Z"),
      ]),
    ).toEqual([null, null, null, null, "input", null, null]);
  });
});
