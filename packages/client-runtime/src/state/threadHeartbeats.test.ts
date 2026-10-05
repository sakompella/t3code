import { EnvironmentId, RunId } from "@t3tools/contracts";
import { derivePendingBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import { presentThreadShell } from "./models.ts";
import { v2ThreadShell } from "./orchestrationV2TestFixtures.ts";
import { deriveRunningTurnBackgroundWork } from "./threadBackgroundWork.ts";
import {
  deriveThreadHeartbeats,
  formatHeartbeatNextRun,
  heartbeatsEqual,
  presentHeartbeats,
  runCompletedTask,
} from "./threadHeartbeats.ts";

const at = (epochMillis: number) => DateTime.toDate(DateTime.makeUnsafe(epochMillis));

const deploy = {
  id: "deploy",
  description: "deploy watch",
  schedule: "every 15m",
  paused: false,
  nextRunAt: "2026-10-03T15:40:00.000Z",
} as const;

const projectionParts = (runStatus: "running" | "completed" = "completed", activeId = "pt-1") => ({
  thread: { activeProviderThreadId: activeId },
  runs: [{ id: "run-1", ordinal: 1, status: runStatus }],
  providerThreads: [
    { id: "pt-1", pendingBackgroundTasks: [], heartbeats: [deploy] },
    {
      id: "pt-old",
      pendingBackgroundTasks: [],
      heartbeats: [{ id: "old", schedule: "every 5m", paused: false }],
    },
  ],
  turnItems: [],
});
const projection = (...args: Parameters<typeof projectionParts>) =>
  projectionParts(...args) as never;

describe("deriveThreadHeartbeats", () => {
  it("lists the active provider thread's heartbeats and no other thread's", () => {
    expect(deriveThreadHeartbeats(projection()).map((heartbeat) => heartbeat.id)).toEqual([
      "deploy",
    ]);
    expect(deriveThreadHeartbeats(projection("completed", "pt-old")).map((h) => h.id)).toEqual([
      "old",
    ]);
    expect(deriveThreadHeartbeats(projection("completed", "pt-none"))).toEqual([]);
  });

  it("is empty when the provider thread has none, including threads saved before heartbeats", () => {
    const legacy = {
      thread: { activeProviderThreadId: "pt-1" },
      providerThreads: [{ id: "pt-1", pendingBackgroundTasks: [] }],
    } as never;
    expect(deriveThreadHeartbeats(legacy)).toEqual([]);
  });

  it("does not count as work, whether or not a turn runs", () => {
    for (const status of ["running", "completed"] as const) {
      const parts = projectionParts(status);
      expect(deriveThreadHeartbeats(projection(status))).toHaveLength(1);
      expect(deriveRunningTurnBackgroundWork(projection(status))).toEqual([]);
      expect(
        derivePendingBackgroundWork({
          latestRun: parts.runs[0] as never,
          providerThreads: parts.providerThreads as never,
          turnItems: parts.turnItems,
          activeProviderThreadId: parts.thread.activeProviderThreadId,
          runs: parts.runs as never,
        }),
      ).toEqual([]);
    }
  });
});

describe("heartbeatsEqual", () => {
  it("compares by content, so an unchanged projection keeps the same value", () => {
    expect(heartbeatsEqual([deploy], [{ ...deploy }])).toBe(true);
    expect(heartbeatsEqual([deploy], [{ ...deploy, nextRunAt: "2026-10-03T15:55:00.000Z" }])).toBe(
      false,
    );
    expect(heartbeatsEqual([deploy], [])).toBe(false);
  });
});

describe("presentHeartbeats", () => {
  const now = at(Date.parse(deploy.nextRunAt) - 60_000);

  it("is null without heartbeats", () => {
    expect(presentHeartbeats([], now)).toBeNull();
  });

  it("names one heartbeat's schedule, purpose, and next run as a timestamp", () => {
    expect(presentHeartbeats([deploy], now)).toEqual({
      title: "Heartbeat every 15m",
      detail: `deploy watch · Next ${formatHeartbeatNextRun(deploy.nextRunAt, now)}`,
    });
    expect(presentHeartbeats([{ id: "x", schedule: "every 5m", paused: false }], now)).toEqual({
      title: "Heartbeat every 5m",
      detail: null,
    });
  });

  it("says a paused heartbeat is paused instead of naming a next run", () => {
    expect(presentHeartbeats([{ id: "x", schedule: "every 5m", paused: true }], now)).toEqual({
      title: "Heartbeat every 5m",
      detail: "Paused",
    });
  });

  it("summarizes several heartbeats on one line", () => {
    const presentation = presentHeartbeats(
      [deploy, { id: "tests", schedule: "every 2h", paused: true }],
      now,
    );
    expect(presentation?.title).toBe("2 heartbeats");
    expect(presentation?.detail).toBe(
      `every 15m, next ${formatHeartbeatNextRun(deploy.nextRunAt, now)}; every 2h, paused`,
    );
  });
});

describe("formatHeartbeatNextRun", () => {
  it("adds the weekday only when the run is on another day", () => {
    const sameDay = formatHeartbeatNextRun(deploy.nextRunAt, at(Date.parse(deploy.nextRunAt)));
    const otherDay = formatHeartbeatNextRun(
      deploy.nextRunAt,
      at(Date.parse(deploy.nextRunAt) - 3 * 24 * 60 * 60 * 1000),
    );
    expect(otherDay).not.toBe(sameDay);
    expect(otherDay.endsWith(sameDay)).toBe(true);
  });
});

describe("runCompletedTask", () => {
  it("counts a finished run someone asked for, never a heartbeat check or an unfinished run", () => {
    const statuses = ["completed", "failed", "interrupted", "cancelled", "running"] as const;
    for (const status of statuses) {
      expect(runCompletedTask({ status })).toBe(status === "completed");
      for (const trigger of [null, "heartbeat"] as const) {
        expect(runCompletedTask({ status, trigger })).toBe(
          status === "completed" && trigger !== "heartbeat",
        );
      }
    }
    expect(runCompletedTask(null)).toBe(false);
  });

  it("reads the trigger the server sent with the thread's latest run", () => {
    const latestRun = (latestRunTrigger?: "heartbeat" | null) =>
      presentThreadShell(EnvironmentId.make("environment"), {
        ...v2ThreadShell,
        latestRunId: RunId.make("run-heartbeat"),
        status: "completed",
        ...(latestRunTrigger === undefined ? {} : { latestRunTrigger }),
      }).latestRun;
    expect(runCompletedTask(latestRun("heartbeat"))).toBe(false);
    expect(runCompletedTask(latestRun(null))).toBe(true);
    // Servers from before the trigger existed never sent one.
    expect(runCompletedTask(latestRun())).toBe(true);
  });
});
