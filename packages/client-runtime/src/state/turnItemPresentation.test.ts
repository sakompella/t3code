import { RunId, ThreadId, TurnItemId, type OrchestrationV2TurnItem } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import {
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
    message: "Finishing up…",
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
