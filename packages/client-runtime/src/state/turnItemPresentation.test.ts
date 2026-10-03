import {
  ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import {
  systemNoticeIsRoutine,
  turnItemIsFinishedProgressNotice,
  turnItemIsWorkspacePreparation,
  workspacePreparationRetryRunIds,
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

function preparationFailure(
  status: OrchestrationV2TurnItem["status"],
  code: string | null = ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE,
): OrchestrationV2TurnItem {
  const {
    input: _input,
    output: _output,
    exitCode: _exitCode,
    ...base
  } = command("") as Extract<OrchestrationV2TurnItem, { type: "command_execution" }>;
  return {
    ...base,
    id: TurnItemId.make("item-error"),
    status,
    title: "Workspace preparation failed",
    type: "error",
    failure: { class: "validation_error", message: "fetch failed", code, retryable: false },
  };
}

const worktree = { type: "worktree", baseRef: "main" } as const;
function run(
  status: OrchestrationV2Run["status"],
  workspacePreparation?: typeof worktree,
): Pick<OrchestrationV2Run, "id" | "status" | "workspacePreparation"> {
  return {
    id: RunId.make("run-1"),
    status,
    ...(workspacePreparation === undefined ? {} : { workspacePreparation }),
  };
}

describe("turnItemIsWorkspacePreparation", () => {
  it("identifies the synthetic workspace preparation command", () => {
    expect(turnItemIsWorkspacePreparation(command("Preparing workspace"))).toBe(true);
    expect(turnItemIsWorkspacePreparation(command("prepare workspace"))).toBe(false);
  });

  it("hides a preparation failure only once a retry cancelled it", () => {
    expect(turnItemIsWorkspacePreparation(preparationFailure("failed"))).toBe(false);
    expect(turnItemIsWorkspacePreparation(preparationFailure("cancelled"))).toBe(true);
    expect(turnItemIsWorkspacePreparation(preparationFailure("cancelled", null))).toBe(false);
  });
});

describe("workspacePreparationRetryRunIds", () => {
  it("offers a retry while the run still ends in its failed preparation", () => {
    const failure = [preparationFailure("failed")];
    expect([...workspacePreparationRetryRunIds([run("failed", worktree)], failure)]).toEqual([
      RunId.make("run-1"),
    ]);
    // Retried: the run is preparing again and the old failure is cancelled.
    expect(workspacePreparationRetryRunIds([run("preparing", worktree)], failure).size).toBe(0);
    // An older server records no preparation, so there is nothing to repeat.
    expect(workspacePreparationRetryRunIds([run("failed")], failure).size).toBe(0);
    // A provider error on a run that did reach the provider is not a preparation failure.
    expect(
      workspacePreparationRetryRunIds(
        [run("failed", worktree)],
        [preparationFailure("failed", "provider_crashed")],
      ).size,
    ).toBe(0);
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
