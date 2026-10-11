import { describe, expect, it } from "vite-plus/test";

import {
  newThreadRuntimeMode,
  offeredRuntimeModes,
  providerRuntimeMode,
  RuntimeMode,
  runtimeModesForNewThreads,
} from "./providerPolicy.ts";

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

describe("the runtime modes new threads can run in", () => {
  const codex = { instanceId: "codex", enabled: true };
  const fork = {
    instanceId: "primeAgent",
    enabled: true,
    supportedRuntimeModes: ["full-access"],
  } as const;
  const off = {
    instanceId: "off",
    enabled: false,
    supportedRuntimeModes: ["approval-required"],
  } as const;
  const target = (
    providers: ReadonlyArray<typeof codex | typeof fork | typeof off>,
    instanceId: string | null,
    defaultRuntimeMode: RuntimeMode = "approval-required",
  ) => ({
    providers,
    defaultModelSelection: instanceId === null ? null : { instanceId },
    defaultRuntimeMode,
  });

  it("are the default provider's modes when it is set and enabled", () => {
    expect(runtimeModesForNewThreads([target([codex, fork], "primeAgent")])).toEqual([
      "full-access",
    ]);
    expect(runtimeModesForNewThreads([target([codex, fork], "codex")])).toEqual(MODES);
  });

  it("are what every enabled provider offers when the default is unset or unusable", () => {
    for (const instanceId of [null, "missing", "off"]) {
      expect(runtimeModesForNewThreads([target([fork], instanceId)])).toEqual(["full-access"]);
      expect(runtimeModesForNewThreads([target([codex, fork, off], instanceId)])).toEqual([
        "full-access",
      ]);
      expect(runtimeModesForNewThreads([target([codex, off], instanceId)])).toEqual(MODES);
    }
  });

  it("are only the modes every selected target offers", () => {
    const targets = [target([codex], "codex"), target([codex, fork], "primeAgent")];
    expect(runtimeModesForNewThreads(targets)).toEqual(["full-access"]);
    for (const one of targets) {
      for (const mode of runtimeModesForNewThreads(targets)) {
        expect(runtimeModesForNewThreads([one])).toContain(mode);
      }
    }
  });

  it("show one mode only when every target runs the same one", () => {
    expect(newThreadRuntimeMode([target([codex], "codex"), target([codex], null)])).toBe(
      "approval-required",
    );
    expect(newThreadRuntimeMode([target([fork], "primeAgent")])).toBe("full-access");
    expect(
      newThreadRuntimeMode([target([codex], "codex"), target([fork], "primeAgent")]),
    ).toBeNull();
    expect(
      newThreadRuntimeMode([
        target([codex], "codex", "full-access"),
        target([codex], "codex", "approval-required"),
      ]),
    ).toBeNull();
  });
});
