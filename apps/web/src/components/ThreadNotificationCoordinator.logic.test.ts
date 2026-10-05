import { RunId } from "@t3tools/contracts";
import { ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  observeThreadForNotification,
  type ThreadNotificationMemory,
} from "./ThreadNotificationCoordinator.logic";

type ObservedThread = Parameters<typeof observeThreadForNotification>[0];

const NOW = "2026-04-10T12:00:00.000Z";
const FUTURE_WAKE = "2026-04-10T18:00:00.000Z";
const PAST_WAKE = "2026-04-10T11:00:00.000Z";

const WORKING: ObservedThread = {
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

const NOTHING_SEEN: ThreadNotificationMemory = { attention: null, completion: null };

function observe(thread: ObservedThread, prior: ThreadNotificationMemory = NOTHING_SEEN) {
  return observeThreadForNotification(thread, prior, NOW);
}

describe("observeThreadForNotification", () => {
  it.each([
    ["a finished run", FINISHED, "completion"],
    ["a failed run", FAILED, "input"],
    ["an approval request", ASKS_APPROVAL, "input"],
    ["a question", ASKS_QUESTION, "input"],
  ] as const)("alerts for %s on an awake thread", (_case, thread, kind) => {
    expect(observe(thread).kind).toBe(kind);
  });

  it("stays quiet about a thread it sees for the first time", () => {
    expect(observeThreadForNotification(FINISHED, undefined, NOW).kind).toBeNull();
  });

  it.each([
    ["a finished run", FINISHED],
    ["a failed run", FAILED],
    ["an approval request", ASKS_APPROVAL],
    ["a question", ASKS_QUESTION],
  ] as const)("raises no alert for %s on a snoozed thread", (_case, thread) => {
    expect(observe({ ...thread, snoozedUntil: FUTURE_WAKE }).kind).toBeNull();
  });

  it("does not announce what happened under the snooze once the timer ends", () => {
    const snoozed = observe({ ...FINISHED, snoozedUntil: FUTURE_WAKE });
    expect(snoozed.kind).toBeNull();
    const afterExpiry = observe({ ...FINISHED, snoozedUntil: PAST_WAKE }, snoozed.memory);
    expect(afterExpiry.kind).toBeNull();
  });

  it("alerts for new work after the timer ends", () => {
    const snoozed = observe({ ...FINISHED, snoozedUntil: FUTURE_WAKE });
    const later = {
      ...FINISHED,
      snoozedUntil: PAST_WAKE,
      latestRun: { ...FINISHED.latestRun!, completedAt: "2026-04-10T11:59:00.000Z" },
    };
    expect(observe(later, snoozed.memory).kind).toBe("completion");
  });

  it("alerts for a different request once the user unsnoozes", () => {
    const snoozed = observe({ ...ASKS_APPROVAL, snoozedUntil: FUTURE_WAKE });
    expect(observe({ ...ASKS_APPROVAL, snoozedUntil: null }, snoozed.memory).kind).toBeNull();
    expect(observe({ ...ASKS_QUESTION, snoozedUntil: null }, snoozed.memory).kind).toBe("input");
  });
});
