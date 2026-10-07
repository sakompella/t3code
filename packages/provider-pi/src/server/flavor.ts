/**
 * PiFlavor — what differs between agents that speak Pi's RPC protocol.
 *
 * Prime Agent is a hard fork of Pi (forked around Pi 0.75). Its RPC keeps
 * Pi's commands and events, but it forked before Pi added `agent_settled`
 * and `get_entries`, and it replaced Pi's bash/edit/write tools with a single
 * persistent `ipython` tool. Everything else (sessions, forks, extensions,
 * extension UI, model selection, compaction) is shared, so both run through
 * the same adapter, probe, and text generation with one flavor each.
 */
import { ProviderDriverKind } from "@t3tools/contracts";
import * as Duration from "effect/Duration";

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
  /** Pi's built-in bash/edit/write tools, or Prime Agent's single `ipython` tool. */
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
   * How rollback rewinds the native conversation. `fork` writes a new session
   * file; `tree` moves the branch head inside the same file through T3's
   * extension command, falling back to `fork` when the extension is absent.
   */
  readonly rollback: "fork" | "tree";
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
  rollback: "fork",
  lossyStream: false,
  cacheMissNotices: false,
  childThreads: false,
  kernelMcp: null,
  heartbeats: false,
  terminationGrace: Duration.seconds(1),
  resumesAtLaunch: false,
};

export const PRIME_AGENT_FLAVOR: PiFlavor = {
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
  rollback: "tree",
  lossyStream: true,
  cacheMissNotices: true,
  childThreads: true,
  kernelMcp: primeAgentDir,
  heartbeats: true,
  terminationGrace: Duration.seconds(15),
  resumesAtLaunch: true,
};
