import { assert, describe, it } from "@effect/vitest";

import { identifyPiCli, PI_FLAVOR, PRIME_AGENT_FLAVOR, resolvePiAgentDir } from "./flavor.ts";
import { PRIME_AGENT_098_HELP, PRIME_AGENT_FORK_HELP } from "./primeAgentHelp.testkit.ts";

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
          resolvePiAgentDir({
            agentDir: flavor.agentDir,
            environment: { [variable]: value },
            home,
          }),
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

const stockHelp = PRIME_AGENT_098_HELP;
const forkHelp = PRIME_AGENT_FORK_HELP;
const STOCK_010_REFUSAL = "stock Prime Agent 0.10 is not supported; install the T3 fork";

describe("identifyPiCli", () => {
  it.each([
    { version: "0.9.8", help: stockHelp, displayName: "Prime Agent" },
    { version: "prime-agent 0.9.6", help: stockHelp, displayName: "Prime Agent" },
    { version: "0.10.0+t3fork.1", help: forkHelp, displayName: "Prime Agent (fork, 0.10)" },
    {
      version: "prime-agent 0.11.2+t3fork.14",
      help: forkHelp,
      displayName: "Prime Agent (fork, 0.10)",
    },
  ])("launches $version as $displayName", ({ version, help, displayName }) => {
    const identity = identifyPiCli(PRIME_AGENT_FLAVOR, { version, help });
    assert.equal(identity._tag, "Supported");
    if (identity._tag === "Supported") assert.equal(identity.flavor.displayName, displayName);
  });

  it.each([
    { name: "stock 0.10", version: "0.10.0", help: forkHelp, reason: STOCK_010_REFUSAL },
    {
      name: "a stock 0.10 beta",
      version: "0.10.0-beta.3",
      help: forkHelp,
      reason: STOCK_010_REFUSAL,
    },
    {
      name: "a stock build with other metadata",
      version: "0.10.1+fork.7",
      help: forkHelp,
      reason: STOCK_010_REFUSAL,
    },
    { name: "a two-part version", version: "0.10", help: forkHelp, reason: "--version" },
    { name: "a nightly label", version: "nightly", help: forkHelp, reason: "--version" },
    {
      name: "the fork marker without rpc mode",
      version: "0.10.0+t3fork.1",
      help: "  --mode <text>\n",
      reason: "--mode rpc",
    },
    {
      name: "a mode-only CLI",
      version: "0.9.8",
      help: "Options:\n  --mode <text>  pick\n",
      reason: "--mode rpc",
    },
    {
      name: "help with no options",
      version: "0.9.8",
      help: "usage: something else\n",
      reason: "--mode rpc",
    },
  ])("refuses $name", ({ version, help, reason }) => {
    const identity = identifyPiCli(PRIME_AGENT_FLAVOR, { version, help });
    assert.equal(identity._tag, "Unsupported");
    if (identity._tag === "Unsupported") assert.include(identity.reason, reason);
  });

  it("reads 0.9.8 help however its option columns are spaced", () => {
    const variants = [
      stockHelp.replaceAll(", --", ",  --"),
      stockHelp.replaceAll(", --", ",\t--"),
      stockHelp.replaceAll("  --mode ", "\t--mode "),
      stockHelp.replaceAll("--mode <", "--mode=<"),
      stockHelp.replaceAll("\n", "\r\n"),
    ];
    for (const help of variants) {
      const identity = identifyPiCli(PRIME_AGENT_FLAVOR, { version: "0.9.8", help });
      assert.equal(identity._tag, "Supported", JSON.stringify(help.slice(0, 400)));
      if (identity._tag === "Supported") assert.isTrue(identity.flavor.extensions);
    }
  });
});
