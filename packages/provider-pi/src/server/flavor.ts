/**
 * PiFlavor — what differs between agents that speak Pi's RPC protocol.
 *
 * Prime Agent is a hard fork of Pi (forked around Pi 0.75). Its RPC keeps
 * Pi's commands and events, but it forked before Pi added `agent_settled`
 * and `get_entries`, and it replaced Pi's bash/edit/write tools with a single
 * persistent Python tool. Everything else (sessions, forks, extensions,
 * extension UI, model selection, compaction) is shared, so both run through
 * the same adapter, probe, and text generation with one flavor each.
 */
import { ProviderDriverKind } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";

export interface PiFlavor {
  readonly driverKind: ProviderDriverKind;
  /** Product name used in user-facing status and error text. */
  readonly displayName: string;
  readonly defaultBinary: string;
  readonly agentDir: {
    readonly agentDirEnvVar: string;
    readonly defaultAgentDir: string;
  };
  /** Oldest CLI version whose RPC this adapter was verified against. */
  readonly minimumVersion: string;
  /** Shown when the binary is missing from PATH. */
  readonly installHint: string;
  /** Shown when the CLI reports no usable models. */
  readonly loginHint: string;
  /**
   * How the adapter learns that a turn is over. Pi emits `agent_settled`.
   * Prime Agent has no such event; after `agent_end` the adapter polls
   * `get_state` until no run, compaction, or session action remains.
   */
  readonly settleSignal: "agent_settled" | "idle_probe";
  /**
   * How turn boundaries in the native session tree are found for rollback
   * and fork. Pi lists new entries with `get_entries`; Prime Agent only lists
   * the active branch's user entries with `get_fork_messages`.
   */
  readonly sessionTree: "entries" | "fork_messages";
  /** Pi's built-in bash/edit/write tools, or Prime Agent's single Python tool. */
  readonly tools: "pi" | "ipython";
  /**
   * Whether a wake's continuation run gets a notice naming what woke the
   * agent. Prime Agent wakes itself by design (heartbeats, schedules, finished
   * background commands, agent messages), and the messages it was woken with
   * say which, so the offer waits for its reply. Pi's extension wakes carry no
   * such cause and are offered at once with the generic notice.
   */
  readonly wakeNotices: boolean;
  /**
   * Whether the adapter aborts a pending auto-retry before it replaces the
   * session. Prime Agent disposes a replaced session without cancelling it.
   */
  readonly abortsRetryOnSwitch: boolean;
  /**
   * How rollback rewinds the native conversation. `fork` writes a new session
   * file; `tree` moves the branch head inside the same file through T3's
   * extension command, falling back to `fork` when the extension is absent;
   * `rpc_tree` moves it with the RPC's own `navigate_tree` command.
   */
  readonly rollback: "fork" | "tree" | "rpc_tree";
  /**
   * Whether the CLI loads TypeScript extensions. T3 then loads its own
   * (`--extension`), which holds the only approval hook, and turns
   * extensions and tools off for headless helpers (`--no-extensions
   * --no-tools`). The Prime Agent 0.10 fork rejects all three flags.
   */
  readonly extensions: boolean;
  /**
   * The flavor a CLI of this driver speaks instead when it answers
   * `--capabilities` with the T3 fork's contract (see `identifyForkPiCli`).
   * The adapter reads it when it opens a session.
   */
  readonly successor?: PiFlavor;
  /**
   * Whether the RPC can drop stream events. Prime Agent's RPC rides its
   * daemon socket and resyncs instead of queueing when that socket backs up,
   * so any event can go missing, up to a whole reply or wake. Every update
   * and the final message still carry the whole message so far, and the
   * adapter treats that snapshot as the truth; what never arrived is rebuilt
   * from the stored conversation (see `primeAgentReconciler.ts`). Pi's
   * stream is lossless.
   */
  readonly lossyStream: boolean;
  /**
   * Whether the adapter warns when a request re-bills a prompt the previous
   * one cached (see `primeAgentCacheMiss.ts`). Prime Agent only; plain Pi
   * keeps its behavior. It seeds from the stored conversation that only the
   * reconciler reads, so it needs `lossyStream`.
   */
  readonly cacheMissNotices: boolean;
  /**
   * Whether `rlm_child_update` children get their own T3 thread. The RPC lets
   * a client `observe` a child's session, which streams the same events as the
   * main one. Pi has no child sessions to observe.
   */
  readonly childThreads: boolean;
  /**
   * Set when the agent has its own MCP client in its kernel. T3 then never
   * registers its tools natively. It ships the `t3-code` skill once the user
   * declared the server in the agent's settings file, and shows a setup notice
   * until then. `null` means T3's extension registers the tools natively.
   */
  readonly kernelMcp: PiFlavor["agentDir"] | null;
  /**
   * Whether the RPC lists the session's heartbeats (`list_heartbeats`), the
   * recurring prompts it runs while the client keeps it alive. The adapter
   * publishes them on the provider thread. Its RPC does not forward
   * `heartbeats_changed`, so they are read when the session opens and when a
   * turn settles.
   */
  readonly heartbeats: boolean;
  /**
   * How long T3 waits for the process to exit after SIGTERM before SIGKILL.
   * Prime Agent's SIGTERM handler tells its daemon to stop the session's
   * worker (`complete_owned_session`), which frees the session lease. A kill
   * that lands first leaves the lease held until the daemon's 30 s
   * disconnect timer fires, and a resume in that window is refused.
   */
  readonly terminationGrace: Duration.Duration;
  /**
   * Whether a thread's existing session is opened by launching the process on
   * it (`--resume <file>`) instead of asking the running process to
   * `switch_session`. Prime Agent's daemon hands its message, observe and
   * heartbeat controllers only to the session a process starts on, so a
   * session swapped in later cannot message its subagents. Pi has no such
   * controllers, and its `--resume` opens a picker, so it keeps the swap.
   */
  readonly resumesAtLaunch: boolean;
  /**
   * Whether the adapter retires its process when the daemon reports that it
   * no longer hosts the session the process is bound to. The daemon can lose
   * a session (a crashed worker) while the process stays alive, and every
   * request through it then fails; a fresh process on the session file works.
   */
  readonly restartsOnLostDaemonSession: boolean;
  /**
   * Whether the session keeps the thinking level asked for and `get_state`
   * reports it clamped to the current model, so a switch to a model with more
   * levels gets the requested one back (the 0.10 fork: Extra High reads as
   * High on a model without it). Pi and 0.9.8 store the clamped level, and
   * that is what the next model gets.
   */
  readonly remembersThinkingRequest: boolean;
}

/** The agent's config directory: its environment override, else the default under `home`. */
export function resolvePiAgentDir(input: {
  readonly agentDir: PiFlavor["agentDir"];
  readonly environment: NodeJS.ProcessEnv;
  readonly home: string;
}): string {
  const { agentDirEnvVar, defaultAgentDir } = input.agentDir;
  return expandHomePath(
    input.environment[agentDirEnvVar]?.trim() || `~/${defaultAgentDir}`,
    input.home,
  );
}

const primeAgentDir = {
  agentDirEnvVar: "PRIME_AGENT_CODING_AGENT_DIR",
  defaultAgentDir: ".prime/agent",
};

export const PI_FLAVOR: PiFlavor = {
  driverKind: ProviderDriverKind.make("pi"),
  displayName: "Pi",
  defaultBinary: "pi",
  agentDir: { agentDirEnvVar: "PI_CODING_AGENT_DIR", defaultAgentDir: ".pi/agent" },
  // get_entries arrived in 0.80.3 and agent_settled landed in source at
  // 0.80.4; 0.80.5 was the first published package containing both.
  minimumVersion: "0.80.5",
  installHint:
    "Pi CLI (`pi`) is not installed or not on PATH. Install with `npm install -g --ignore-scripts @earendil-works/pi-coding-agent`.",
  loginHint:
    "Pi has no usable models. Run `pi` in a terminal and use /login, or configure an API key in ~/.pi/agent.",
  settleSignal: "agent_settled",
  sessionTree: "entries",
  tools: "pi",
  wakeNotices: false,
  abortsRetryOnSwitch: false,
  rollback: "fork",
  extensions: true,
  lossyStream: false,
  cacheMissNotices: false,
  childThreads: false,
  kernelMcp: null,
  heartbeats: false,
  terminationGrace: Duration.seconds(1),
  resumesAtLaunch: false,
  restartsOnLostDaemonSession: false,
  remembersThinkingRequest: false,
};

/** Prime Agent 0.9.8, the TypeScript release. */
const primeAgent098 = {
  driverKind: ProviderDriverKind.make("primeAgent"),
  displayName: "Prime Agent",
  defaultBinary: "prime-agent",
  agentDir: primeAgentDir,
  // The RPC surface (get_state sessionActions, fork, get_fork_messages)
  // this adapter relies on was verified against 0.9.6 and 0.9.8.
  minimumVersion: "0.9.6",
  installHint: "Prime Agent CLI (`prime-agent`) is not installed or not on PATH.",
  loginHint:
    "Prime Agent has no usable models. Run `prime-agent` in a terminal and use /login, or configure an API key in ~/.prime/agent.",
  settleSignal: "idle_probe",
  sessionTree: "fork_messages",
  tools: "ipython",
  wakeNotices: true,
  abortsRetryOnSwitch: true,
  rollback: "tree",
  extensions: true,
  lossyStream: true,
  cacheMissNotices: true,
  childThreads: true,
  kernelMcp: primeAgentDir,
  heartbeats: true,
  terminationGrace: Duration.seconds(15),
  resumesAtLaunch: true,
  restartsOnLostDaemonSession: true,
  remembersThinkingRequest: false,
} satisfies PiFlavor;

/**
 * The `sakompella/prime-agent` fork of the Rust Prime Agent 0.10. Its RPC
 * runs in the daemon (RLM children, agent messages, heartbeats, wakes),
 * speaks the 0.9.8 commands and frames, and adds `navigate_tree`. It has no
 * extensions, so T3 cannot gate tools before they run. It reaches T3's tools
 * through `kernelMcp` until its native `t3` module replaces that route.
 */
export const PRIME_AGENT_FORK_FLAVOR: PiFlavor = {
  ...primeAgent098,
  displayName: "Prime Agent (fork, 0.10)",
  minimumVersion: "0.10.0",
  rollback: "rpc_tree",
  extensions: false,
  remembersThinkingRequest: true,
};

export const PRIME_AGENT_FLAVOR: PiFlavor = {
  ...primeAgent098,
  successor: PRIME_AGENT_FORK_FLAVOR,
};

/**
 * What T3 makes of a CLI from its probes: the flavor it speaks, or why T3
 * will not launch it. Launch flags differ between flavors, so a CLI T3
 * cannot identify is never launched with a guessed one.
 */
export type PiCliIdentity =
  | { readonly _tag: "Supported"; readonly flavor: PiFlavor }
  | { readonly _tag: "Unsupported"; readonly reason: string };

const unsupported = (reason: string) => ({ _tag: "Unsupported", reason }) as const;

/**
 * The probe that identifies a CLI after `--version`. Below 0.10 it is 0.9.8's
 * `--help`, which must offer `--mode rpc`. From 0.10 on it is the T3 fork's
 * `--capabilities`. 0.9.8 is never asked for capabilities: it takes an
 * unknown long flag as an extension flag and runs print mode.
 */
export type PiCliProbe =
  | { readonly _tag: "Help" }
  | { readonly _tag: "Capabilities" }
  | Extract<PiCliIdentity, { _tag: "Unsupported" }>;

const CLI_VERSION = /\bv?(\d+)\.(\d+)\.(\d+)/;
/** The first Rust Prime Agent release. Prereleases of it count as 0.10 too. */
const FIRST_RUST_MINOR = { major: 0, minor: 10 };

export function piCliProbeForVersion(version: string): PiCliProbe {
  const parsed = CLI_VERSION.exec(version);
  if (parsed === null) return unsupported("its `--version` printed no version T3 Code can read");
  const [major, minor] = [Number(parsed[1]), Number(parsed[2])];
  const isRust =
    major > FIRST_RUST_MINOR.major ||
    (major === FIRST_RUST_MINOR.major && minor >= FIRST_RUST_MINOR.minor);
  return isRust ? { _tag: "Capabilities" } : { _tag: "Help" };
}

/** Whether an option line of the help offers `--mode` with an `rpc` value. */
function offersRpcMode(help: string): boolean {
  return help.split(/\r?\n/).some((line) => {
    const tokens = line.trim().split(/[\s=]+/);
    if (!tokens[0]?.startsWith("-")) return false;
    const valueIndex = tokens.indexOf("--mode") + 1;
    if (valueIndex === 0) return false;
    const values = (tokens[valueIndex] ?? "").replace(/[<>[\]{}]/g, "").split(/[|,]/);
    return values.includes("rpc");
  });
}

/** Identifies a CLI below 0.10 from its `--help`: `flavor` itself, if it offers `--mode rpc`. */
export function identifyStockPiCli(flavor: PiFlavor, help: string): PiCliIdentity {
  return offersRpcMode(help)
    ? { _tag: "Supported", flavor }
    : unsupported("its `--help` offers no `--mode rpc`");
}

/**
 * What the T3 fork prints for `prime-agent --capabilities`: one JSON object
 * naming the fork, the contract its T3-facing RPC follows, its version and
 * the RPC commands it routes.
 */
const ForkCapabilities = Schema.fromJsonString(
  Schema.Struct({
    fork: Schema.String,
    contract: Schema.Number,
    version: Schema.String,
    rpc: Schema.Array(Schema.String),
  }),
);
const decodeForkCapabilities = Schema.decodeUnknownOption(ForkCapabilities);

const T3_FORK_MARKER = "t3fork";
/**
 * The fork contracts whose commands and frames this adapter was reviewed
 * against. Contract 2 renamed the Python tool (see `isPythonCellTool`).
 */
const SUPPORTED_FORK_CONTRACTS: ReadonlyArray<number> = [1, 2];

/**
 * The Python cell tool's names: `ipython` before contract 2 and in 0.9.8,
 * `python_repl` after upstream's rename. Both show as the same cell.
 */
export const isPythonCellTool = (toolName: string): boolean =>
  toolName === "ipython" || toolName === "python_repl";
/** The RPC commands the adapter sends to the fork (every supported contract). It may advertise more. */
const FORK_COMMANDS_T3_CALLS = [
  "prompt",
  "abort",
  "abort_retry",
  "compact",
  "new_session",
  "switch_session",
  "get_state",
  "get_available_models",
  "set_model",
  "set_thinking_level",
  "set_session_name",
  "get_messages",
  "get_session_stats",
  "get_fork_messages",
  "get_commands",
  "fork",
  "navigate_tree",
  "list_heartbeats",
  "observe",
  "unobserve",
];

/**
 * Identifies a CLI of 0.10 or later from its `--capabilities` run. Only the
 * T3 fork answers it, with exit 0 and a contract T3 supports; stock 0.10
 * exits 1 with "Unknown option".
 */
export function identifyForkPiCli(
  flavor: PiFlavor,
  run: { readonly exitCode: number; readonly stdout: string },
): PiCliIdentity {
  if (flavor.successor === undefined || run.exitCode !== 0) {
    return unsupported(
      "stock Prime Agent 0.10 is not supported; install the T3 fork (`sakompella/prime-agent`), which answers `prime-agent --capabilities`",
    );
  }
  const capabilities = decodeForkCapabilities(run.stdout);
  if (Option.isNone(capabilities)) {
    return unsupported("its `--capabilities` output is not the T3 fork's JSON contract");
  }
  const { fork, contract, rpc } = capabilities.value;
  if (fork !== T3_FORK_MARKER) {
    return unsupported(`its \`--capabilities\` names fork "${fork}", not "${T3_FORK_MARKER}"`);
  }
  if (!SUPPORTED_FORK_CONTRACTS.includes(contract)) {
    return unsupported(
      `its fork contract ${contract} is not supported; T3 Code supports contracts ${SUPPORTED_FORK_CONTRACTS.join(" and ")}`,
    );
  }
  const missing = FORK_COMMANDS_T3_CALLS.filter((command) => !rpc.includes(command));
  return missing.length === 0
    ? { _tag: "Supported", flavor: flavor.successor }
    : unsupported(`its fork lacks RPC commands T3 Code calls: ${missing.join(", ")}`);
}

/**
 * Whether this driver's agents call T3's MCP tools from code in their kernel.
 * Their tool items show that code, never the T3 tool it called.
 */
export const callsT3ToolsFromKernel = (driver: ProviderDriverKind) =>
  [PI_FLAVOR, PRIME_AGENT_FLAVOR].some(
    (flavor) => flavor.driverKind === driver && flavor.kernelMcp !== null,
  );
