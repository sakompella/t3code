import { RunId, ThreadId, TurnItemId, type OrchestrationV2TurnItem } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import {
  noticeExpandedText,
  systemNoticeIsRoutine,
  turnItemIsFinishedProgressNotice,
  turnItemIsWorkspacePreparation,
} from "./turnItemPresentation.ts";

function command(input: string): OrchestrationV2TurnItem {
  const now = DateTime.makeUnsafe("2026-08-03T00:00:00.000Z");
  return {
    id: TurnItemId.make("item-command"),
    threadId: ThreadId.make("thread-1"),
    runId: RunId.make("run-1"),
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "completed",
    title: "Workspace ready",
    startedAt: now,
    completedAt: now,
    updatedAt: now,
    type: "command_execution",
    input,
    output: "Workspace preparation completed.",
    exitCode: 0,
  };
}

describe("turnItemIsWorkspacePreparation", () => {
  it("identifies the synthetic workspace preparation command", () => {
    expect(turnItemIsWorkspacePreparation(command("Preparing workspace"))).toBe(true);
    expect(turnItemIsWorkspacePreparation(command("prepare workspace"))).toBe(false);
  });
});

function systemNotice(
  tone: "warning" | "info" | "progress" | undefined,
  status: "running" | "completed",
  message = "Finishing up…",
): OrchestrationV2TurnItem {
  const now = DateTime.makeUnsafe("2026-08-03T00:00:00.000Z");
  return {
    id: TurnItemId.make("item-notice"),
    threadId: ThreadId.make("thread-1"),
    runId: RunId.make("run-1"),
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status,
    title: "Finishing up…",
    startedAt: now,
    completedAt: status === "completed" ? now : null,
    updatedAt: now,
    type: "system_notice",
    message,
    ...(tone === undefined ? {} : { tone }),
  };
}

describe("system notice tones", () => {
  it("treats only info and progress notices as routine; an absent tone is a warning", () => {
    expect(systemNoticeIsRoutine(systemNotice("info", "completed"))).toBe(true);
    expect(systemNoticeIsRoutine(systemNotice("progress", "running"))).toBe(true);
    expect(systemNoticeIsRoutine(systemNotice("warning", "completed"))).toBe(false);
    expect(systemNoticeIsRoutine(systemNotice(undefined, "completed"))).toBe(false);
    expect(systemNoticeIsRoutine(command("Preparing workspace"))).toBe(false);
  });

  it("drops a progress notice once it completes, and nothing else", () => {
    expect(turnItemIsFinishedProgressNotice(systemNotice("progress", "completed"))).toBe(true);
    expect(turnItemIsFinishedProgressNotice(systemNotice("progress", "running"))).toBe(false);
    expect(turnItemIsFinishedProgressNotice(systemNotice("info", "completed"))).toBe(false);
    expect(turnItemIsFinishedProgressNotice(systemNotice(undefined, "completed"))).toBe(false);
  });
});

function notification(summary: string, detail?: string): OrchestrationV2TurnItem {
  return {
    ...systemNotice(undefined, "completed"),
    type: "notification",
    source: { kind: "command" },
    outcome: "completed",
    summary,
    ...(detail === undefined ? {} : { detail }),
  } as OrchestrationV2TurnItem;
}

const LONG_MESSAGE =
  "Refined its harness: Update local Windows SSH, exit-node, and RustDesk handoff state while preserving the rest";

describe("noticeExpandedText", () => {
  it("keeps a short system notice closed, so it never repeats its label", () => {
    expect(noticeExpandedText(systemNotice("info", "completed", "Finished up"))).toBeNull();
  });

  it("opens a system notice whose message a row can cut off, with the whole message", () => {
    expect(noticeExpandedText(systemNotice("info", "completed", LONG_MESSAGE))).toBe(LONG_MESSAGE);
  });

  it("opens a short system notice with several lines", () => {
    expect(noticeExpandedText(systemNotice("info", "completed", "Failed\nretrying"))).toBe(
      "Failed\nretrying",
    );
  });

  it("keeps a short notification without detail closed", () => {
    expect(noticeExpandedText(notification("Background command finished"))).toBeNull();
    expect(noticeExpandedText(notification("Background command finished", "  \n"))).toBeNull();
  });

  it("opens a notification for its detail without repeating a short summary", () => {
    expect(noticeExpandedText(notification("Background command finished", "Exit code 0"))).toBe(
      "Exit code 0",
    );
  });

  it("opens a notification whose summary can be cut off, even without detail", () => {
    expect(noticeExpandedText(notification(LONG_MESSAGE))).toBe(LONG_MESSAGE);
  });

  it("shows a cut-off summary before the detail", () => {
    expect(noticeExpandedText(notification(LONG_MESSAGE, "Exit code 0"))).toBe(
      `${LONG_MESSAGE}\n\nExit code 0`,
    );
  });

  it("has no text for other items", () => {
    expect(noticeExpandedText(command("Preparing workspace"))).toBeNull();
  });
});
