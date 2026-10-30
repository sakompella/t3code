/**
 * Pi's and Prime Agent's client definitions: settings form, label, and glyph.
 * Browser- and React Native-safe.
 *
 * @module provider-pi/client
 */
import { ProviderDriverKind } from "@t3tools/contracts";
import { defineProviderClient } from "@t3tools/provider-core/client";

import { PiSettings, PrimeAgentSettings } from "./settings.ts";

export const piClient = defineProviderClient({
  driverKind: ProviderDriverKind.make("pi"),
  label: "Pi",
  acpRegistryAgentIds: ["pi-acp"],
  settingsSchema: PiSettings,
  icon: {
    viewBox: "165.29 165.29 469.43 469.43",
    fill: { light: "#0F0F0F", dark: "#F5F5F5" },
    paths: [
      {
        d: "M165.29 165.29H517.36V400H400V517.36H282.65V634.72H165.29ZM282.65 282.65V400H400V282.65Z",
        fillRule: "evenodd",
      },
      { d: "M517.36 400H634.72V634.72H517.36Z" },
    ],
  },
});

/** Prime Agent has no official mark yet, so clients draw its initials. */
export const primeAgentClient = defineProviderClient({
  driverKind: ProviderDriverKind.make("primeAgent"),
  label: "Prime Agent",
  badgeLabel: "Early Access",
  settingsSchema: PrimeAgentSettings,
});
