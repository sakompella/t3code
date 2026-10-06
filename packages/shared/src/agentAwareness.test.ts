import { describe, expect, it } from "@effect/vitest";

import type {
  EnvironmentId,
  OrchestrationV2ThreadShell,
  Project,
  ThreadId,
} from "@t3tools/contracts";
import { ProviderInstanceId, RunId, RuntimeRequestId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { projectThreadAwarenessV2 } from "./agentAwareness.ts";

const NOW = "2026-05-22T12:00:00.000Z";

const project = {
  title: "t3code",
} satisfies Pick<Project, "title">;

describe("projectThreadAwarenessV2", () => {
  const updatedAt = DateTime.makeUnsafe(NOW);
  const v2Thread = (
    overrides: Partial<
      Pick<
        OrchestrationV2ThreadShell,
        | "activityRunStatus"
        | "status"
        | "pendingBackgroundTasks"
        | "pendingRuntimeRequest"
        | "lineage"
        | "activeRunId"
        | "latestRunId"
        | "latestRunTrigger"
      >
    > = {},
  ) => ({
    id: "thread-2" as ThreadId,
    lineage: {
      rootThreadId: "thread-2" as ThreadId,
      parentThreadId: null,
      relationshipToParent: null,
    },
    title: "Integrate orchestration",
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
    status: "running" as const,
    pendingRuntimeRequest: null,
    activeRunId: null,
    latestRunId: null,
    latestRunTrigger: null,
    updatedAt,
    ...overrides,
  });

  it("projects V2 run state", () => {
    expect(
      projectThreadAwarenessV2({
        environmentId: "env-1" as EnvironmentId,
        project,
        thread: v2Thread(),
      }),
    ).toMatchObject({ phase: "running", headline: "Agent is working" });
  });

  it.each(["running", "completed", "failed"] as const)(
    "does not publish %s subagent activity",
    (status) => {
      expect(
        projectThreadAwarenessV2({
          environmentId: "env-1" as EnvironmentId,
          project,
          thread: v2Thread({
            status,
            lineage: {
              rootThreadId: "parent" as ThreadId,
              parentThreadId: "parent" as ThreadId,
              relationshipToParent: "subagent",
            },
          }),
        }),
      ).toBeNull();
    },
  );

  it("keeps an older activity run visible over a newer cancelled run", () => {
    expect(
      projectThreadAwarenessV2({
        environmentId: "env-1" as EnvironmentId,
        project,
        thread: v2Thread({ status: "cancelled", activityRunStatus: "running" }),
      }),
    ).toMatchObject({ phase: "running", headline: "Agent is working" });
  });

  it.each([
    ["only a dev server", "completed", [{ taskId: "dev", kind: "command" }]],
    ["a monitor", "running", [{ taskId: "watch", kind: "monitor" }]],
    [
      "a dev server and a subagent",
      "running",
      [
        { taskId: "dev", kind: "command" },
        { taskId: "review", kind: "subagent" },
      ],
    ],
  ] as const)("reports a completed run waiting on %s as %s", (_case, phase, tasks) => {
    expect(
      projectThreadAwarenessV2({
        environmentId: "env-1" as EnvironmentId,
        project,
        thread: v2Thread({ status: "completed", pendingBackgroundTasks: tasks }),
      }),
    ).toMatchObject({ phase });
  });

  it("prioritizes V2 user-input requests", () => {
    expect(
      projectThreadAwarenessV2({
        environmentId: "env-1" as EnvironmentId,
        project,
        thread: v2Thread({
          pendingRuntimeRequest: {
            id: RuntimeRequestId.make("request-1"),
            kind: "user_input",
            createdAt: updatedAt,
          },
        }),
      }),
    ).toMatchObject({ phase: "waiting_for_input", headline: "Waiting for input" });
  });

  it("does not present authentication refreshes as user approvals", () => {
    expect(
      projectThreadAwarenessV2({
        environmentId: "env-1" as EnvironmentId,
        project,
        thread: v2Thread({
          pendingRuntimeRequest: {
            id: RuntimeRequestId.make("request-auth-refresh"),
            kind: "auth_refresh",
            createdAt: updatedAt,
          },
        }),
      }),
    ).toMatchObject({ phase: "running", headline: "Agent is working" });
  });

  describe("a heartbeat run, which is a routine check", () => {
    const heartbeatRun = RunId.make("run-heartbeat");
    const heartbeat = (overrides: Parameters<typeof v2Thread>[0] = {}) =>
      projectThreadAwarenessV2({
        environmentId: "env-1" as EnvironmentId,
        project,
        thread: v2Thread({
          latestRunId: heartbeatRun,
          latestRunTrigger: "heartbeat",
          activeRunId: null,
          ...overrides,
        }),
      });

    it("publishes nothing while it starts, works or finishes", () => {
      for (const activityRunStatus of ["preparing", "starting", "running"] as const) {
        expect(heartbeat({ activityRunStatus, activeRunId: heartbeatRun })).toBeNull();
      }
      expect(heartbeat({ status: "completed" })).toBeNull();
      expect(
        heartbeat({
          status: "completed",
          pendingBackgroundTasks: [{ taskId: "child", kind: "subagent" }],
        }),
      ).toBeNull();
    });

    it("still publishes its failures and requests", () => {
      expect(heartbeat({ status: "failed" })).toMatchObject({ phase: "failed" });
      expect(
        heartbeat({
          status: "running",
          activityRunStatus: "running",
          activeRunId: heartbeatRun,
          pendingRuntimeRequest: {
            id: RuntimeRequestId.make("question"),
            kind: "user_input",
            createdAt: updatedAt,
          },
        }),
      ).toMatchObject({ phase: "waiting_for_input" });
      expect(
        heartbeat({
          status: "running",
          activityRunStatus: "running",
          activeRunId: heartbeatRun,
          pendingRuntimeRequest: {
            id: RuntimeRequestId.make("approval"),
            kind: "command",
            createdAt: updatedAt,
          },
        }),
      ).toMatchObject({ phase: "waiting_for_approval" });
    });

    it("leaves a user's run that is still active ahead of it visible", () => {
      expect(
        heartbeat({
          status: "queued",
          activityRunStatus: "running",
          activeRunId: RunId.make("run-user"),
        }),
      ).toMatchObject({ phase: "running" });
    });

    it("leaves runs someone asked for unchanged", () => {
      expect(heartbeat({ status: "completed", latestRunTrigger: null })).toMatchObject({
        phase: "completed",
      });
    });
  });
});
