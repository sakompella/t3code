import { EnvironmentId, type PeerLinkSummary } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";

import { peerLinkHealth } from "./peerLinks.ts";

const linkedAt = DateTime.makeUnsafe("2026-01-01T00:00:00.000Z");
const link = (expiresAt: DateTime.Utc | null): PeerLinkSummary => ({
  environmentId: EnvironmentId.make("environment-box"),
  label: "Box",
  urls: ["https://box.example"],
  access: "auto",
  linkedAt,
  expiresAt,
  lastReachedAt: linkedAt,
  lastError: null,
  status: "reachable",
});

describe("peerLinkHealth", () => {
  it("never warns about a link that lasts until revoked", () => {
    for (const days of [0, 29, 31, 10_000]) {
      expect(peerLinkHealth(link(null), DateTime.add(linkedAt, { days }))).toEqual({
        kind: "reachable",
        expiresInDays: null,
      });
    }
  });

  it("counts down and then expires a link with an expiry", () => {
    const expiring = link(DateTime.add(linkedAt, { days: 30 }));
    expect(peerLinkHealth(expiring, DateTime.add(linkedAt, { days: 27 }))).toEqual({
      kind: "reachable",
      expiresInDays: 3,
    });
    expect(peerLinkHealth(expiring, DateTime.add(linkedAt, { days: 30 }))).toEqual({
      kind: "expired",
    });
  });
});
