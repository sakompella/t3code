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
  status: "completed" | "failed" | "interrupted" | "cancelled",
  completedAt: string,
): ObservedThread => ({
  ...thread,
  latestRun: { ...thread.latestRun!, status, completedAt },
  runtime: null,
});

type TaskStatus = NonNullable<ObservedThread["latestTaskRunStatus"]>;

/**
 * A thread whose latest run that is not a routine check is in `taskStatus`,
 * ended at `taskEndedAt`, as the shell reports it.
 */
const afterTask = (
  thread: ObservedThread,
  taskEndedAt: string | null,
  taskStatus: TaskStatus,
): ObservedThread => ({
  ...thread,
  latestTaskRunCompletedAt: taskEndedAt,
  latestTaskRunStatus: taskStatus,
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
    const checkRunning = afterTask(working("run-check", "heartbeat"), null, "running");
    const checkDone = (taskStatus: TaskStatus) =>
      afterTask(
        ended(working("run-check", "heartbeat"), "completed", "2026-10-05T10:01:30.000Z"),
        userEndedAt,
        taskStatus,
      );
    const nextCheckRunning = afterTask(working("run-check-2", "heartbeat"), null, "running");
    const nextCheckDone = (taskStatus: TaskStatus) =>
      afterTask(
        ended(working("run-check-2", "heartbeat"), "completed", "2026-10-05T10:06:00.000Z"),
        userEndedAt,
        taskStatus,
      );

    it("announces the user's completion once the check ends, and only once", () => {
      expect(
        alertsFor([
          userRun,
          checkRunning,
          checkDone("completed"),
          checkDone("completed"),
          nextCheckRunning,
          nextCheckDone("completed"),
        ]),
      ).toEqual([null, null, "completion", null, null, null]);
    });

    it("announces it when the check also ended in that update", () => {
      expect(alertsFor([userRun, checkDone("completed"), nextCheckDone("completed")])).toEqual([
        null,
        "completion",
        null,
      ]);
    });

    it.each(["failed", "interrupted", "cancelled"] as const)(
      "never announces a user's run that %s and was never seen ending",
      (taskStatus) => {
        expect(
          alertsFor([
            userRun,
            checkRunning,
            checkDone(taskStatus),
            nextCheckRunning,
            nextCheckDone(taskStatus),
          ]),
        ).toEqual([null, null, null, null, null]);
        expect(alertsFor([userRun, checkDone(taskStatus)])).toEqual([null, null]);
      },
    );

    it("does not take an end as a completion when the server sent no status", () => {
      const { latestTaskRunStatus: _omitted, ...fromOlderServer } = checkDone("completed");
      expect(alertsFor([userRun, checkRunning, fromOlderServer])).toEqual([null, null, null]);
    });

    it("does not announce it again after a user's completion already alerted", () => {
      const userDone = afterTask(
        ended(userRun, "completed", userEndedAt),
        userEndedAt,
        "completed",
      );
      expect(alertsFor([userRun, userDone, checkRunning, checkDone("completed")])).toEqual([
        null,
        "completion",
        null,
        null,
      ]);
    });

    it("stays quiet when the first snapshot after a reconnect is the check", () => {
      expect(
        alertsFor([checkDone("completed"), nextCheckRunning, nextCheckDone("completed")]),
      ).toEqual([null, null, null]);
    });
  });

  it("never announces a failed or interrupted run it saw end when a later check ends", () => {
    const interruptedAt = "2026-10-05T10:02:00.000Z";
    const interrupted = afterTask(
      ended(working("run-user"), "interrupted", interruptedAt),
      interruptedAt,
      "interrupted",
    );
    const failedAt = "2026-10-05T10:12:00.000Z";
    const failedCheck = afterTask(
      ended(working("run-check-1", "heartbeat"), "failed", failedAt),
      failedAt,
      "failed",
    );
    const checkAfter = (
      runId: string,
      taskEndedAt: string,
      taskStatus: TaskStatus,
      completedAt: string,
    ) =>
      afterTask(
        ended(working(runId, "heartbeat"), "completed", completedAt),
        taskEndedAt,
        taskStatus,
      );
    expect(
      alertsFor([
        working("run-user"),
        interrupted,
        afterTask(working("run-check", "heartbeat"), null, "running"),
        checkAfter("run-check", interruptedAt, "interrupted", "2026-10-05T10:05:00.000Z"),
        failedCheck,
        afterTask(working("run-check-2", "heartbeat"), null, "running"),
        checkAfter("run-check-2", failedAt, "failed", "2026-10-05T10:15:00.000Z"),
      ]),
    ).toEqual([null, null, null, null, "input", null, null]);
  });
});
