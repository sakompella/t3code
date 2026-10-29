import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { describe, expect, it } from "vite-plus/test";

import { BearerConnectionTarget, PrimaryConnectionTarget } from "../connection/model.ts";
import type {
  EnvironmentConnectionPresentation,
  EnvironmentPresentation,
} from "../connection/presentation.ts";
import {
  foldOfflineEnvironmentThreads,
  isOfflineFoldedEnvironment,
} from "./offlineEnvironments.ts";

const primary = EnvironmentId.make("primary");
const sandbox = EnvironmentId.make("lima-sandbox");
const thread = (environmentId: EnvironmentId, id: string) => ({
  environmentId,
  id: ThreadId.make(id),
});
const threads = [
  thread(sandbox, "s1"),
  thread(primary, "p1"),
  thread(sandbox, "s2"),
  thread(primary, "p2"),
];
const offline = new Map([[sandbox, "lima-sandbox"]]);

function presentation(
  target: "primary" | "saved",
  connection: Omit<EnvironmentConnectionPresentation, "traceId">,
): EnvironmentPresentation {
  const environmentId = target === "primary" ? primary : sandbox;
  return {
    entry: {
      target:
        target === "primary"
          ? new PrimaryConnectionTarget({
              environmentId,
              label: "This Mac",
              httpBaseUrl: "http://localhost:3773",
              wsBaseUrl: "ws://localhost:3773",
            })
          : new BearerConnectionTarget({ environmentId, label: "lima-sandbox", connectionId: "c" }),
      profile: Option.none(),
      enabled: true,
    },
    connection: { ...connection, traceId: null },
    serverConfig: null,
  };
}

describe("isOfflineFoldedEnvironment", () => {
  it("folds a saved environment once an attempt to reach it failed", () => {
    expect(
      isOfflineFoldedEnvironment(
        presentation("saved", { phase: "reconnecting", error: "connect ECONNREFUSED" }),
      ),
    ).toBe(true);
    expect(isOfflineFoldedEnvironment(presentation("saved", { phase: "error", error: null }))).toBe(
      true,
    );
    expect(
      isOfflineFoldedEnvironment(presentation("saved", { phase: "offline", error: null })),
    ).toBe(true);
  });

  it("keeps a saved environment that is connected, connecting, or just dropped", () => {
    for (const phase of ["connected", "connecting", "reconnecting", "available"] as const) {
      expect(isOfflineFoldedEnvironment(presentation("saved", { phase, error: null }))).toBe(false);
    }
  });

  it("never folds the primary environment", () => {
    for (const phase of ["offline", "error", "unsupported", "reconnecting"] as const) {
      expect(isOfflineFoldedEnvironment(presentation("primary", { phase, error: "down" }))).toBe(
        false,
      );
    }
  });
});

describe("foldOfflineEnvironmentThreads", () => {
  it("moves an offline environment's threads into one closed fold", () => {
    const result = foldOfflineEnvironmentThreads({
      threads,
      offlineEnvironmentLabels: offline,
      expandedEnvironmentIds: new Set(),
      openThread: null,
    });
    expect(result.threads).toEqual([thread(primary, "p1"), thread(primary, "p2")]);
    expect(result.folds).toEqual([
      {
        environmentId: sandbox,
        label: "lima-sandbox",
        expanded: false,
        shownThreads: [],
        hiddenThreads: [thread(sandbox, "s1"), thread(sandbox, "s2")],
      },
    ]);
  });

  it("shows every folded thread while the fold is open", () => {
    const [fold] = foldOfflineEnvironmentThreads({
      threads,
      offlineEnvironmentLabels: offline,
      expandedEnvironmentIds: new Set([sandbox]),
      openThread: null,
    }).folds;
    expect(fold?.shownThreads).toEqual([thread(sandbox, "s1"), thread(sandbox, "s2")]);
    expect(fold?.hiddenThreads).toEqual([]);
  });

  it("keeps the open thread visible under a closed fold", () => {
    const [fold] = foldOfflineEnvironmentThreads({
      threads,
      offlineEnvironmentLabels: offline,
      expandedEnvironmentIds: new Set(),
      openThread: { environmentId: sandbox, threadId: ThreadId.make("s2") },
    }).folds;
    expect(fold?.shownThreads).toEqual([thread(sandbox, "s2")]);
    expect(fold?.hiddenThreads).toEqual([thread(sandbox, "s1")]);
  });

  it("restores the list unchanged once the environment reconnects", () => {
    const result = foldOfflineEnvironmentThreads({
      threads,
      offlineEnvironmentLabels: new Map(),
      expandedEnvironmentIds: new Set([sandbox]),
      openThread: null,
    });
    expect(result.threads).toBe(threads);
    expect(result.folds).toEqual([]);
  });
});
