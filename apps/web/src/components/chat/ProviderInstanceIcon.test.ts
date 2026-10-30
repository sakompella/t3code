import { ProviderDriverKind } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  providerDrawsInitialsGlyph,
  resolveProviderInstanceAcpRegistryIconUrl,
} from "./ProviderInstanceIcon";

describe("resolveProviderInstanceAcpRegistryIconUrl", () => {
  it("uses allowlisted catalog metadata and rejects untrusted overrides", () => {
    expect(
      resolveProviderInstanceAcpRegistryIconUrl({
        driverKind: ProviderDriverKind.make("acpRegistry"),
        agentId: "kilo",
        iconUrl: "https://cdn.agentclientprotocol.com/registry/icons/kilo.svg",
      }),
    ).toBe("https://cdn.agentclientprotocol.com/registry/icons/kilo.svg");
    expect(
      resolveProviderInstanceAcpRegistryIconUrl({
        driverKind: ProviderDriverKind.make("acpRegistry"),
        agentId: "generic-agent",
        iconUrl: "https://example.com/not-official.svg",
      }),
    ).toBe("https://cdn.agentclientprotocol.com/registry/v1/latest/generic-agent.svg");
  });
});

describe("providerDrawsInitialsGlyph", () => {
  it("is true only for providers that have no glyph of their own", () => {
    const drawsInitials = (driverKind: string) =>
      providerDrawsInitialsGlyph(ProviderDriverKind.make(driverKind));
    expect(drawsInitials("primeAgent")).toBe(true);
    for (const withGlyph of ["codex", "claudeAgent", "antigravity", "pi", "acpRegistry"]) {
      expect(drawsInitials(withGlyph), withGlyph).toBe(false);
    }
  });
});
