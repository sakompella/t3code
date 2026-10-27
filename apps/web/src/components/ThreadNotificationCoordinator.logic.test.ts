import { WORKING_EXIT_GRACE_MS } from "@t3tools/client-runtime/state/thread-inbox";
import { ProviderInstanceId, RunId } from "@t3tools/contracts";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import {
  createThreadNotificationGate,
  observeThreadForNotification,
  type ThreadNotificationKind,
  type ThreadNotificationObservation,
} from "./ThreadNotificationCoordinator.logic";

const working: ThreadNotificationObservation = {
  attention: null,
  settled: false,
  completedAt: null,
};
const completed = (at: number): ThreadNotificationObservation => ({
  attention: null,
  settled: true,
  completedAt: at,
});
const visible = { muted: false, quiet: false };

function harness() {
  const notify = vi.fn();
  const gate = createThreadNotificationGate<string>(notify);
  let prior: ThreadNotificationObservation | undefined;
  const observe = (next: ThreadNotificationObservation, options = visible) => {
    gate.observe("thread", prior, next, options);
    prior = next;
  };
  return { gate, notify, observe };
}

describe("createThreadNotificationGate", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("drops a completion when the thread resumes within the grace", () => {
    const { notify, observe } = harness();
    observe(working);
    observe(completed(1));
    vi.advanceTimersByTime(WORKING_EXIT_GRACE_MS - 1);
    observe(working);
    vi.advanceTimersByTime(WORKING_EXIT_GRACE_MS * 2);
    expect(notify).not.toHaveBeenCalled();
  });

  it("notifies a real completion once after the grace", () => {
    const { notify, observe } = harness();
    observe(working);
    observe(completed(1));
    vi.advanceTimersByTime(WORKING_EXIT_GRACE_MS - 1);
    expect(notify).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(notify.mock.calls).toEqual([["thread", "completion"]]);
    observe({ ...completed(1) });
    vi.advanceTimersByTime(WORKING_EXIT_GRACE_MS * 2);
    expect(notify).toHaveBeenCalledTimes(1);
  });

  it("notifies attention at once and replaces a pending completion", () => {
    const { notify, observe } = harness();
    observe(working);
    observe(completed(1));
    vi.advanceTimersByTime(500);
    observe({ attention: "run-1:approval", settled: false, completedAt: null });
    expect(notify.mock.calls).toEqual([["thread", "input"]]);
    vi.advanceTimersByTime(60_000);
    // The request resolves and the same run reads as completed again.
    observe(completed(1));
    vi.advanceTimersByTime(WORKING_EXIT_GRACE_MS * 2);
    expect(notify.mock.calls).toEqual([["thread", "input"]]);
  });

  it("notifies one completion across continuation gaps", () => {
    const { notify, observe } = harness();
    observe(working);
    observe(completed(1));
    vi.advanceTimersByTime(300);
    observe(working);
    vi.advanceTimersByTime(5_000);
    observe(completed(2));
    vi.advanceTimersByTime(300);
    observe(working);
    vi.advanceTimersByTime(5_000);
    observe(completed(3));
    vi.advanceTimersByTime(WORKING_EXIT_GRACE_MS);
    expect(notify.mock.calls).toEqual([["thread", "completion"]]);
  });

  it("keeps a completion interrupted by attention after a working gap from firing", () => {
    const { notify, observe } = harness();
    observe(working);
    observe(completed(1));
    vi.advanceTimersByTime(500);
    observe(working);
    observe({ attention: "run-1:approval", settled: false, completedAt: null });
    vi.advanceTimersByTime(60_000);
    observe(completed(1));
    vi.advanceTimersByTime(WORKING_EXIT_GRACE_MS * 2);
    expect(notify.mock.calls).toEqual([["thread", "input"]]);
  });

  it("adds nothing when attention follows a completion that already fired", () => {
    const { notify, observe } = harness();
    observe(working);
    observe(completed(1));
    vi.advanceTimersByTime(WORKING_EXIT_GRACE_MS);
    observe({ attention: "run-1:limited", settled: false, completedAt: null });
    observe(completed(1));
    vi.advanceTimersByTime(WORKING_EXIT_GRACE_MS * 2);
    expect(notify.mock.calls).toEqual([
      ["thread", "completion"],
      ["thread", "input"],
    ]);
  });

  it("still notifies a newer completion after attention", () => {
    const { notify, observe } = harness();
    observe(working);
    observe(completed(1));
    vi.advanceTimersByTime(500);
    observe({ attention: "run-2:approval", settled: false, completedAt: null });
    observe(working);
    observe(completed(2));
    vi.advanceTimersByTime(WORKING_EXIT_GRACE_MS);
    expect(notify.mock.calls).toEqual([
      ["thread", "input"],
      ["thread", "completion"],
    ]);
  });

  it("ignores unrelated updates while a completion waits", () => {
    const { notify, observe } = harness();
    observe(working);
    observe(completed(1));
    vi.advanceTimersByTime(1_500);
    observe({ ...completed(1) });
    vi.advanceTimersByTime(500);
    expect(notify).toHaveBeenCalledTimes(1);
  });

  it("notifies a run that settles again after a brief runtime blip", () => {
    const { notify, observe } = harness();
    observe(working);
    observe(completed(1));
    vi.advanceTimersByTime(500);
    observe(working);
    observe(completed(1));
    vi.advanceTimersByTime(WORKING_EXIT_GRACE_MS);
    expect(notify.mock.calls).toEqual([["thread", "completion"]]);
  });

  it("restarts the grace when a newer completion replaces a pending one", () => {
    const { notify, observe } = harness();
    observe(working);
    observe(completed(1));
    vi.advanceTimersByTime(1_500);
    observe(completed(2));
    vi.advanceTimersByTime(1_500);
    expect(notify).not.toHaveBeenCalled();
    vi.advanceTimersByTime(500);
    expect(notify).toHaveBeenCalledTimes(1);
  });

  it("drops a pending completion on mute, stop request, removal or clear", () => {
    for (const end of [
      (h: ReturnType<typeof harness>) => h.observe(completed(1), { ...visible, muted: true }),
      (h: ReturnType<typeof harness>) =>
        h.observe({ ...completed(1) }, { ...visible, quiet: true }),
      (h: ReturnType<typeof harness>) => h.gate.retain(new Set()),
      (h: ReturnType<typeof harness>) => h.gate.clear(),
    ]) {
      const h = harness();
      h.observe(working);
      h.observe(completed(1));
      end(h);
      vi.advanceTimersByTime(WORKING_EXIT_GRACE_MS * 2);
      expect(h.notify).not.toHaveBeenCalled();
    }
  });

  it("never notifies a stopped run or the first observation", () => {
    const { notify, observe } = harness();
    observe(completed(1));
    observe(working);
    observe(completed(2), { ...visible, quiet: true });
    vi.advanceTimersByTime(WORKING_EXIT_GRACE_MS * 2);
    expect(notify).not.toHaveBeenCalled();
  });
});

type ObservedThread = Parameters<typeof observeThreadForNotification>[0];

const NOW = "2026-04-10T12:00:00.000Z";
const FUTURE_WAKE = "2026-04-10T18:00:00.000Z";
const PAST_WAKE = "2026-04-10T11:00:00.000Z";

const WORKING: ObservedThread = {
  archivedAt: null,
  latestRun: null,
  runtime: {
    status: "running",
    activeRunId: RunId.make("run-1"),
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerName: "Codex",
    lastError: null,
    updatedAt: "2026-04-10T10:00:00.000Z",
  },
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  snoozedUntil: null,
};

const FINISHED: ObservedThread = {
  ...WORKING,
  latestRun: {
    runId: RunId.make("run-1"),
    status: "completed",
    requestedAt: null,
    startedAt: null,
    completedAt: "2026-04-10T11:30:00.000Z",
    assistantMessageId: null,
  },
  runtime: null,
};

const FAILED: ObservedThread = {
  ...FINISHED,
  latestRun: { ...FINISHED.latestRun!, status: "failed" },
};

const ASKS_APPROVAL: ObservedThread = { ...WORKING, hasPendingApprovals: true };
const ASKS_QUESTION: ObservedThread = { ...WORKING, hasPendingUserInput: true };

/** Feeds snapshots through the gate, past its grace each time, and returns the alert each raised. */
function alertsFor(
  snapshots: ReadonlyArray<ObservedThread>,
): Array<ThreadNotificationKind | null> {
  const notify = vi.fn();
  const gate = createThreadNotificationGate<string>(notify);
  let prior: ThreadNotificationObservation | undefined;
  return snapshots.map((snapshot) => {
    const seen = notify.mock.calls.length;
    const { observation, options } = observeThreadForNotification(snapshot, NOW);
    gate.observe("thread", prior, observation, options);
    prior = observation;
    vi.advanceTimersByTime(WORKING_EXIT_GRACE_MS);
    return notify.mock.calls[seen]?.[1] ?? null;
  });
}

describe("observeThreadForNotification", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it.each([
    ["a finished run", FINISHED, "completion"],
    ["a failed run", FAILED, "input"],
    ["an approval request", ASKS_APPROVAL, "input"],
    ["a question", ASKS_QUESTION, "input"],
  ] as const)("alerts for %s on an awake thread", (_case, thread, kind) => {
    expect(alertsFor([WORKING, thread])).toEqual([null, kind]);
  });

  it("remembers a stopped task without announcing its completion", () => {
    expect(alertsFor([WORKING, { ...FINISHED, latestRunStopRequested: true }, FINISHED])).toEqual(
      [null, null, null],
    );
  });

  it("does not use a stopped heartbeat to silence a completed task", () => {
    const heartbeat: ObservedThread = {
      ...FINISHED,
      latestRun: { ...FINISHED.latestRun!, runId: RunId.make("heartbeat"), trigger: "heartbeat" },
      latestRunStopRequested: true,
      latestTaskRunCompletedAt: FINISHED.latestRun!.completedAt,
      latestTaskRunStatus: "completed",
    };
    expect(alertsFor([WORKING, heartbeat, heartbeat])).toEqual([null, "completion", null]);
  });

  it("stays quiet about a thread it sees for the first time", () => {
    expect(alertsFor([FINISHED])).toEqual([null]);
  });

  it.each([
    ["a finished run", FINISHED],
    ["a failed run", FAILED],
    ["an approval request", ASKS_APPROVAL],
    ["a question", ASKS_QUESTION],
  ] as const)("raises no alert for %s on a snoozed thread", (_case, thread) => {
    expect(alertsFor([WORKING, { ...thread, snoozedUntil: FUTURE_WAKE }])).toEqual([null, null]);
  });

  it("does not announce what happened under the snooze once the timer ends", () => {
    expect(
      alertsFor([
        WORKING,
        { ...FINISHED, snoozedUntil: FUTURE_WAKE },
        { ...FINISHED, snoozedUntil: PAST_WAKE },
      ]),
    ).toEqual([null, null, null]);
  });

  it("alerts for new work after the timer ends", () => {
    const later = {
      ...FINISHED,
      snoozedUntil: PAST_WAKE,
      latestRun: { ...FINISHED.latestRun!, completedAt: "2026-04-10T11:59:00.000Z" },
    };
    expect(alertsFor([WORKING, { ...FINISHED, snoozedUntil: FUTURE_WAKE }, later])).toEqual([
      null,
      null,
      "completion",
    ]);
  });

  it("alerts for a different request once the user unsnoozes", () => {
    expect(
      alertsFor([
        WORKING,
        { ...ASKS_APPROVAL, snoozedUntil: FUTURE_WAKE },
        { ...ASKS_APPROVAL, snoozedUntil: null },
        { ...ASKS_QUESTION, snoozedUntil: null },
      ]),
    ).toEqual([null, null, null, "input"]);
  });
});
