// @effect-diagnostics nodeBuiltinImport:off
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { assert, describe, it } from "@effect/vitest";

import { PI_FLAVOR, PRIME_AGENT_FLAVOR, resolvePiAgentDir } from "./PiFlavor.ts";

const cases = [
  { flavor: PI_FLAVOR, directory: ".pi", variable: "PI_CODING_AGENT_DIR" },
  { flavor: PRIME_AGENT_FLAVOR, directory: ".prime", variable: "PRIME_AGENT_CODING_AGENT_DIR" },
];

describe("resolvePiAgentDir", () => {
  it.each(cases)(
    "uses the default home for absent or blank $variable",
    ({ flavor, directory, variable }) => {
      const expected = NodePath.join(NodeOS.homedir(), directory, "agent");
      for (const value of [undefined, "", "   "]) {
        assert.equal(
          resolvePiAgentDir({ agentDir: flavor.agentDir, environment: { [variable]: value } }),
          expected,
        );
      }
    },
  );

  it.each(cases)("trims and expands the configured $variable", ({ flavor, variable }) => {
    assert.equal(
      resolvePiAgentDir({
        agentDir: flavor.agentDir,
        environment: { [variable]: "  ~/custom-agent  " },
      }),
      NodePath.join(NodeOS.homedir(), "custom-agent"),
    );
    assert.equal(
      resolvePiAgentDir({
        agentDir: flavor.agentDir,
        environment: { [variable]: " /tmp/custom-agent " },
      }),
      "/tmp/custom-agent",
    );
  });
});
