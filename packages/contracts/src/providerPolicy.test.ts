import { describe, expect, it } from "vite-plus/test";

import { offeredRuntimeModes, providerRuntimeMode, RuntimeMode } from "./providerPolicy.ts";

const MODES = RuntimeMode.literals;
// Every non-empty set of modes a provider could offer.
const OFFERS = Array.from({ length: 2 ** MODES.length - 1 }, (_, index) =>
  MODES.filter((_, bit) => ((index + 1) >> bit) & 1),
);

describe("the runtime mode a provider runs", () => {
  it("is always a mode the provider offers, and the stored one when it does", () => {
    for (const offered of OFFERS) {
      for (const stored of MODES) {
        const runs = providerRuntimeMode(stored, offered);
        expect(offered).toContain(runs);
        if (offered.includes(stored)) expect(runs).toBe(stored);
      }
    }
  });

  it("is the most restrictive offered mode when the stored one is not offered", () => {
    for (const offered of OFFERS) {
      for (const stored of MODES.filter((mode) => !offered.includes(mode))) {
        const runs = providerRuntimeMode(stored, offered);
        expect(offered.filter((mode) => MODES.indexOf(mode) < MODES.indexOf(runs))).toEqual([]);
      }
    }
  });

  it("is the stored mode on a provider that states no modes", () => {
    for (const stored of MODES) {
      expect(providerRuntimeMode(stored, undefined)).toBe(stored);
      expect(providerRuntimeMode(stored, [])).toBe(stored);
    }
  });

  it("offers every mode, in order, unless the provider states fewer", () => {
    expect(offeredRuntimeModes(undefined)).toEqual(MODES);
    expect(offeredRuntimeModes([])).toEqual(MODES);
    expect(offeredRuntimeModes(["full-access", "approval-required"])).toEqual([
      "approval-required",
      "full-access",
    ]);
  });
});
