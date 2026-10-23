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

  it("names the heartbeat that fired, so its run reads as a routine check", () => {
    const heartbeat = (details: unknown) =>
      piCustomMessageNotification({
        role: "custom",
        customType: "heartbeat_prompt",
        content: "[heartbeat: every 5m run#3]\n\nCheck the deploy.",
        details,
      })?.source;
    expect(heartbeat({ jobId: "deploy", runCount: 3 })).toEqual({
      kind: "heartbeat",
      heartbeatId: "deploy",
    });
    expect(heartbeat(undefined)).toEqual({ kind: "heartbeat" });
    expect(heartbeat({ jobId: " " })).toEqual({ kind: "heartbeat" });
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

  it.effect("counts a completion notice once when a continuation run replays it", () =>
    Effect.gen(function* () {
      const jobs = makePrimeAgentBackgroundJobs(() => Effect.void);
      yield* jobs.trackBackgroundJobs("first = bash('sleep 9')");
      yield* jobs.trackBackgroundJobs("second = bash('sleep 9')");
      const finished = completion({ pid: 7, command: "sleep 9", exitCode: 0 });
      yield* jobs.completeBackgroundJob(finished);
      yield* jobs.completeBackgroundJob(finished);

      expect(jobs.tasks().map((task) => task.description)).toEqual(["sleep 9"]);
      expect(jobs.handleOf(finished)).toBe("first");
    }),
  );

  it.effect("takes a job off the roster when a cell reads it, but keeps tracking it", () =>
    Effect.gen(function* () {
      const rosters: Array<ReadonlyArray<string | undefined>> = [];
      const jobs = makePrimeAgentBackgroundJobs((patch) =>
        Effect.sync(() => {
          rosters.push((patch.pendingBackgroundTasks ?? []).map((task) => task.description));
        }),
      );
      yield* jobs.trackBackgroundJobs("a = bash('make a'); b = bash('make b')");
      yield* jobs.trackBackgroundJobs("print(a.running, a.pid)");
      yield* jobs.trackBackgroundJobs("print(a.output())");
      // Nothing changed for a cell that touches no job.
      yield* jobs.trackBackgroundJobs("print(1)");

      expect(rosters).toEqual([["make a", "make b"], ["make b"]]);
      expect(jobs.hasPendingJobs()).toBe(true);

      // The notice a read job still sends names it, and ends it.
      yield* jobs.completeBackgroundJob(completion({ pid: 7, command: "make a", exitCode: 0 }));
      expect(rosters).toEqual([["make a", "make b"], ["make b"]]);
      yield* jobs.completeBackgroundJob(completion({ pid: 8, command: "make b", exitCode: 0 }));
      expect(rosters.at(-1)).toEqual([]);
      expect(jobs.hasPendingJobs()).toBe(false);
    }),
  );

  it.effect("gives a completion to the job no cell read before one a cell read", () =>
    Effect.gen(function* () {
      const jobs = makePrimeAgentBackgroundJobs(() => Effect.void);
      yield* jobs.trackBackgroundJobs("old = bash('make')");
      yield* jobs.trackBackgroundJobs("print(old.output())");
      yield* jobs.trackBackgroundJobs("new = bash('make')");
      const finished = completion({ pid: 7, command: "make", exitCode: 0 });
      yield* jobs.completeBackgroundJob(finished);

      expect(jobs.handleOf(finished)).toBe("new");
      expect(jobs.hasPendingJobs()).toBe(true);
    }),
  );

  describe("when a cell assigns a handle's name again", () => {
    const descriptions = (jobs: ReturnType<typeof makePrimeAgentBackgroundJobs>) =>
      jobs.tasks().map((task) => task.description);

    it.effect("ends only the job the name holds when it is awaited", () =>
      Effect.gen(function* () {
        const jobs = makePrimeAgentBackgroundJobs(() => Effect.void);
        yield* jobs.trackBackgroundJobs("h = bash('long-A')");
        yield* jobs.trackBackgroundJobs("h = bash('short-B')");
        yield* jobs.trackBackgroundJobs("await h");

        expect(descriptions(jobs)).toEqual(["long-A"]);
        expect(jobs.hasPendingJobs()).toBe(true);

        // The detached job still ends by its own notice, and keeps the name it started with.
        const finished = completion({ pid: 7, command: "long-A", exitCode: 0 });
        yield* jobs.completeBackgroundJob(finished);
        expect(jobs.hasPendingJobs()).toBe(false);
        expect(jobs.handleOf(finished)).toBe("h");
      }),
    );

    it.effect("ends only the job the name holds when it is killed", () =>
      Effect.gen(function* () {
        const jobs = makePrimeAgentBackgroundJobs(() => Effect.void);
        yield* jobs.trackBackgroundJobs("h = bash('long-A')");
        yield* jobs.trackBackgroundJobs("h = bash('long-B')");
        yield* jobs.trackBackgroundJobs("h.kill()");

        expect(descriptions(jobs)).toEqual(["long-A"]);
      }),
    );

    it.effect("takes only the job the name holds off the roster when it is read", () =>
      Effect.gen(function* () {
        const jobs = makePrimeAgentBackgroundJobs(() => Effect.void);
        yield* jobs.trackBackgroundJobs("h = bash('long-A')");
        yield* jobs.trackBackgroundJobs("h = bash('long-B')");
        yield* jobs.trackBackgroundJobs("print(h.output())");

        expect(descriptions(jobs)).toEqual(["long-A"]);
        expect(jobs.hasPendingJobs()).toBe(true);
      }),
    );

    it.effect("keeps a job reachable through the name until a cell assigns it again", () =>
      Effect.gen(function* () {
        const jobs = makePrimeAgentBackgroundJobs(() => Effect.void);
        yield* jobs.trackBackgroundJobs("h = bash('long-A')");
        yield* jobs.trackBackgroundJobs("print(h.output())");
        // Using the old job, then starting another under the same name.
        yield* jobs.trackBackgroundJobs("h.kill()\nh = bash('long-B')");

        expect(descriptions(jobs)).toEqual(["long-B"]);
        yield* jobs.trackBackgroundJobs("await h");
        expect(jobs.hasPendingJobs()).toBe(false);
      }),
    );

    it.effect("does not apply what a cell does with the new value to the old job", () =>
      Effect.gen(function* () {
        const jobs = makePrimeAgentBackgroundJobs(() => Effect.void);
        yield* jobs.trackBackgroundJobs("h = bash('long-A')");
        yield* jobs.trackBackgroundJobs("h = await bash('short-B')\nprint(h.output)");
        yield* jobs.trackBackgroundJobs("h = bash('long-C'); await h");

        expect(descriptions(jobs)).toEqual(["long-A"]);
      }),
    );

    it.effect("separates jobs started under one name within a single cell", () =>
      Effect.gen(function* () {
        const jobs = makePrimeAgentBackgroundJobs(() => Effect.void);
        yield* jobs.trackBackgroundJobs("h = bash('long-A')\nh = bash('long-B')\nawait h");

        expect(descriptions(jobs)).toEqual(["long-A"]);

        yield* jobs.trackBackgroundJobs("h = bash('long-C')\nh = bash('long-D')");
        yield* jobs.trackBackgroundJobs("await h");
        expect(descriptions(jobs)).toEqual(["long-A", "long-C"]);
      }),
    );
  });

  it.effect("ends the jobs of awaited, gathered, and killed handles", () =>
    Effect.gen(function* () {
      const jobs = makePrimeAgentBackgroundJobs(() => Effect.void);
      yield* jobs.trackBackgroundJobs("a = bash('make a')\nb = bash('make b')\nc = bash('make c')");
      yield* jobs.trackBackgroundJobs("await asyncio.gather(a, b)");
      yield* jobs.trackBackgroundJobs("c.kill()");

      expect(jobs.hasPendingJobs()).toBe(false);
    }),
  );
});
