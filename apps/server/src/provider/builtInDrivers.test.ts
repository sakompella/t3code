import { assert, describe, it } from "@effect/vitest";
import { resolveProviderInstanceEnabled } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { BUILT_IN_DRIVERS } from "./builtInDrivers.ts";

describe("built-in drivers", () => {
  it("resolve the same default enabled state on the client as the server's config decoding", () => {
    for (const { driverKind, configSchema } of BUILT_IN_DRIVERS) {
      const serverEnabled = (Schema.decodeUnknownSync(configSchema)({}) as { enabled: boolean })
        .enabled;
      assert.strictEqual(
        resolveProviderInstanceEnabled({ driver: driverKind, config: {} }),
        serverEnabled,
        `default enabled state differs between client and server for driver "${driverKind}"`,
      );
    }
  });
});
