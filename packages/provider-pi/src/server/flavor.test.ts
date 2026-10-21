import { assert, describe, it } from "@effect/vitest";

import {
  identifyForkPiCli,
  identifyStockPiCli,
  PI_FLAVOR,
  piCliProbeForVersion,
  PRIME_AGENT_FLAVOR,
  PRIME_AGENT_FORK_FLAVOR,
  resolvePiAgentDir,
} from "./flavor.ts";
import {
  forkCapabilitiesWith,
  PRIME_AGENT_098_HELP,
  PRIME_AGENT_FORK_CAPABILITIES,
} from "./primeAgentHelp.testkit.ts";

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

describe("piCliProbeForVersion", () => {
  it.each(["0.9.8", "prime-agent 0.9.6", "v0.9.8-beta.1"])("asks %s for --help", (version) => {
    assert.deepEqual(piCliProbeForVersion(version), { _tag: "Help" });
  });

  it.each(["0.10.0", "0.10.0-beta.3", "0.10.0+t3fork.1", "prime-agent 1.2.3"])(
    "asks %s for --capabilities",
    (version) => {
      assert.deepEqual(piCliProbeForVersion(version), { _tag: "Capabilities" });
    },
  );

  it.each(["0.10", "nightly", ""])("refuses the unreadable version %j", (version) => {
    const probe = piCliProbeForVersion(version);
    assert.equal(probe._tag, "Unsupported");
    if (probe._tag === "Unsupported") assert.include(probe.reason, "--version");
  });
});

describe("identifyStockPiCli", () => {
  it.each([
    { name: "a mode-only CLI", help: "Options:\n  --mode <text>  pick\n" },
    { name: "help with no options", help: "usage: something else\n" },
  ])("refuses $name", ({ help }) => {
    const identity = identifyStockPiCli(PRIME_AGENT_FLAVOR, help);
    assert.equal(identity._tag, "Unsupported");
    if (identity._tag === "Unsupported") assert.include(identity.reason, "--mode rpc");
  });

  it("reads 0.9.8 help however its option columns are spaced", () => {
    const variants = [
      stockHelp,
      stockHelp.replaceAll(", --", ",  --"),
      stockHelp.replaceAll(", --", ",\t--"),
      stockHelp.replaceAll("  --mode ", "\t--mode "),
      stockHelp.replaceAll("--mode <", "--mode=<"),
      stockHelp.replaceAll("\n", "\r\n"),
    ];
    for (const help of variants) {
      const identity = identifyStockPiCli(PRIME_AGENT_FLAVOR, help);
      assert.equal(identity._tag, "Supported", JSON.stringify(help.slice(0, 400)));
      if (identity._tag === "Supported") assert.strictEqual(identity.flavor, PRIME_AGENT_FLAVOR);
    }
  });
});

describe("identifyForkPiCli", () => {
  const advertised: ReadonlyArray<string> = JSON.parse(PRIME_AGENT_FORK_CAPABILITIES).rpc;
  const identify = (stdout: string, exitCode = 0) =>
    identifyForkPiCli(PRIME_AGENT_FLAVOR, { exitCode, stdout });

  it("launches the slice-4 fork, and any fork that advertises more commands", () => {
    for (const stdout of [
      PRIME_AGENT_FORK_CAPABILITIES,
      PRIME_AGENT_FORK_CAPABILITIES.trim(),
      forkCapabilitiesWith({ rpc: [...advertised, "get_goal"], version: "0.11.0" }),
      forkCapabilitiesWith({ rpc: advertised.toReversed(), extra: [1, 2] }),
    ]) {
      const identity = identify(stdout);
      assert.equal(identity._tag, "Supported", stdout);
      if (identity._tag === "Supported")
        assert.strictEqual(identity.flavor, PRIME_AGENT_FORK_FLAVOR);
    }
  });

  it("refuses every contract but 1", () => {
    for (const contract of [-1, 0, 0.5, 1.000001, 2, 3, 10, 2 ** 31]) {
      const identity = identify(forkCapabilitiesWith({ contract }));
      assert.equal(identity._tag, "Unsupported", String(contract));
      if (identity._tag === "Unsupported") assert.include(identity.reason, `contract ${contract}`);
    }
  });

  it("refuses any failed run as stock, whatever it printed", () => {
    for (const exitCode of [1, 2, 127]) {
      const identity = identify(PRIME_AGENT_FORK_CAPABILITIES, exitCode);
      assert.equal(identity._tag, "Unsupported");
      if (identity._tag === "Unsupported") {
        assert.include(identity.reason, "stock Prime Agent 0.10 is not supported");
      }
    }
  });

  it("names every missing command T3 calls", () => {
    const identity = identify(forkCapabilitiesWith({ rpc: ["prompt", "abort", "steer"] }));
    assert.equal(identity._tag, "Unsupported");
    if (identity._tag === "Unsupported") {
      for (const command of ["navigate_tree", "get_fork_messages", "observe"]) {
        assert.include(identity.reason, command);
      }
      assert.notInclude(identity.reason, "steer");
    }
  });
});
