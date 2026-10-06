import { assert, describe, it } from "@effect/vitest";

import { PI_FLAVOR, PRIME_AGENT_FLAVOR, resolvePiAgentDir } from "./flavor.ts";

const home = "/home/user";
const cases = [
  { flavor: PI_FLAVOR, directory: ".pi", variable: "PI_CODING_AGENT_DIR" },
  { flavor: PRIME_AGENT_FLAVOR, directory: ".prime", variable: "PRIME_AGENT_CODING_AGENT_DIR" },
];

describe("resolvePiAgentDir", () => {
  it.each(cases)(
    "uses the default home for absent or blank $variable",
    ({ flavor, directory, variable }) => {
      for (const value of [undefined, "", "   "]) {
        assert.equal(
          resolvePiAgentDir({ agentDir: flavor.agentDir, environment: { [variable]: value }, home }),
          `${home}/${directory}/agent`,
        );
      }
    },
  );

  it.each(cases)("trims and expands the configured $variable", ({ flavor, variable }) => {
    assert.equal(
      resolvePiAgentDir({
        agentDir: flavor.agentDir,
        environment: { [variable]: "  ~/custom-agent  " },
        home,
      }),
      `${home}/custom-agent`,
    );
    assert.equal(
      resolvePiAgentDir({
        agentDir: flavor.agentDir,
        environment: { [variable]: " /tmp/custom-agent " },
        home,
      }),
      "/tmp/custom-agent",
    );
  });
});
