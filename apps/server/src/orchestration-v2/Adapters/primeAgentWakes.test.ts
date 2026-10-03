import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { makePrimeAgentBackgroundJobs, piCustomMessageNotification } from "./primeAgentWakes.ts";

const completion = (details: Record<string, unknown>) => ({
  role: "custom",
  customType: "async_bash_completion",
  content: "[bash-done pid:7 exit:0]",
  details,
});

describe("piCustomMessageNotification", () => {
  it("names the finished command and its exit code", () => {
    const notification = piCustomMessageNotification(
      completion({ pid: 7, command: "pnpm test", exitCode: 1 }),
    );
    expect(notification?.summary).toBe("Background command finished");
    expect(notification?.detail).toBe("pnpm test\n\nExit code 1");
  });

  it("adds the handle variable the caller knows for the job", () => {
    const message = completion({ pid: 7, command: "pnpm test", exitCode: 0 });
    expect(piCustomMessageNotification(message, () => "tests")?.detail).toBe(
      "pnpm test\n\nExit code 0\n\nHandle: tests",
    );
  });

  it("truncates a long command", () => {
    const detail = piCustomMessageNotification(
      completion({ pid: 7, command: "x".repeat(5_000), exitCode: 0 }),
    )?.detail;
    expect(detail?.split("\n")[0]).toHaveLength(500);
    expect(detail?.split("\n")[0]?.endsWith("…")).toBe(true);
  });

  it("has no detail when the message does not say which command finished", () => {
    expect(piCustomMessageNotification(completion({}))).not.toHaveProperty("detail");
    expect(piCustomMessageNotification(completion({ command: "  " }))).not.toHaveProperty("detail");
  });

  it("shows what a heartbeat asked for, without its header line", () => {
    const notification = piCustomMessageNotification({
      role: "custom",
      customType: "heartbeat_prompt",
      content: "[heartbeat: every 5m run#3]\n\nCheck the deploy.",
    });
    expect(notification?.detail).toBe("Check the deploy.");
  });

  it("shows why a child failed or exited, when it says", () => {
    expect(
      piCustomMessageNotification({
        role: "custom",
        customType: "rlm_child_failure",
        details: { error: "name taken" },
      })?.detail,
    ).toBe("name taken");
    expect(
      piCustomMessageNotification({
        role: "custom",
        customType: "rlm_child_terminal_notice",
        details: { kind: "completed_without_reply", lastAssistantTextPreview: "all done" },
      })?.detail,
    ).toBe("all done");
  });

  it("leaves a notice with nothing beyond its summary without detail", () => {
    for (const message of [
      { role: "custom", customType: "heartbeat_prompt", content: "[heartbeat: every 5m run#3]" },
      { role: "custom", customType: "rlm_child_failure", details: {} },
      {
        role: "custom",
        customType: "rlm_child_terminal_notice",
        details: { kind: "completed_without_reply", sessionName: "worker" },
      },
      { role: "custom", customType: "agent_message", details: { message: "  " } },
    ]) {
      expect(piCustomMessageNotification(message)).not.toHaveProperty("detail");
    }
  });
});

describe("makePrimeAgentBackgroundJobs", () => {
  it.effect(
    "remembers the handle of the job a completion reports after the job leaves the roster",
    () =>
      Effect.gen(function* () {
        const jobs = makePrimeAgentBackgroundJobs(() => Effect.void);
        yield* jobs.trackBackgroundJobs("build = bash('make build'); print(build.pid)");
        yield* jobs.trackBackgroundJobs("bash('make lint')");
        const finished = completion({ pid: 7, command: "make build", exitCode: 0 });
        const orphan = completion({ pid: 8, command: "make lint", exitCode: 0 });
        const unknown = completion({ pid: 9, command: "other", exitCode: 0 });
        yield* jobs.completeBackgroundJob(finished);
        yield* jobs.completeBackgroundJob(orphan);
        yield* jobs.completeBackgroundJob(unknown);

        expect(jobs.hasPendingJobs()).toBe(false);
        expect(jobs.handleOf(finished)).toBe("build");
        expect(jobs.handleOf(orphan)).toBeNull();
        expect(jobs.handleOf(unknown)).toBeNull();
      }),
  );
});
