import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import { UsageLimitPoolDashboard } from "./providerUsageLimits.ts";

const decodeDashboard = Schema.decodeUnknownSync(UsageLimitPoolDashboard);
const olderHubDashboard = {
  nowMs: 1_793_350_000_000,
  fromMs: 1_793_000_000_000,
  toMs: 1_794_000_000_000,
  window: "five-hour",
  lookbackHours: 3,
  historyStartedAtMs: null,
  pools: [],
};

describe("UsageLimitPoolDashboard events", () => {
  it("decodes a hub from before observed events without them", () => {
    expect(decodeDashboard(olderHubDashboard).events).toBeUndefined();
  });

  it("keeps the events it knows and drops kinds and windows from newer hubs", () => {
    const observed = {
      accountId: "claude-first.json",
      label: "Claude 1",
      provider: "claude",
      window: "five-hour",
      atMs: 1_793_347_200_000,
    };
    const hit = { ...observed, kind: "limit-hit", resetAtMs: 1_793_365_200_000 };
    const reset = { ...observed, kind: "reset", atMs: hit.resetAtMs };
    const { events } = decodeDashboard({
      ...olderHubDashboard,
      events: [
        { ...hit, harness: "prime-agent" },
        { ...hit, kind: "throttle" },
        { ...hit, window: "monthly" },
        reset,
      ],
    });
    expect(events).toEqual([hit, reset]);
  });
});
