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

export interface PiFlavor {
  readonly driverKind: ProviderDriverKind;
  /** Product name used in user-facing status and error text. */
  readonly displayName: string;
  readonly defaultBinary: string;
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
   * What happens when the agent starts work with no T3 turn. Pi only does that
   * through a misbehaving extension, so the session stops. Prime Agent wakes
   * itself by design (heartbeats, schedules, finished background commands,
   * agent messages), so the work is buffered and handed to a continuation run.
   */
  readonly selfWakes: "stop" | "continuation";
  /**
   * How rollback rewinds the native conversation. `fork` writes a new session
   * file; `tree` moves the branch head inside the same file through T3's
   * extension command, falling back to `fork` when the extension is absent.
   */
  readonly rollback: "fork" | "tree";
  /**
   * Whether the RPC can drop stream events. Prime Agent's RPC rides its
   * daemon socket and resyncs instead of queueing when that socket backs up,
   * so deltas, `text_end`, and `message_start` can go missing. Every update
   * and the final message still carry the whole message so far, and the
   * adapter treats that snapshot as the truth. Pi's stream is lossless.
   */
  readonly lossyStream: boolean;
  /**
   * Whether `rlm_child_update` children get their own T3 thread. The RPC lets
   * a client `observe` a child's session, which streams the same events as the
   * main one. Pi has no child sessions to observe.
   */
  readonly childThreads: boolean;
  /**
   * Set when the agent has its own MCP client in its kernel. T3 then ships the
   * `t3-code` skill instead of registering its tools natively, provided the
   * user declared the server in the agent's settings file. `null` means T3's
   * extension always registers the tools.
   */
  readonly kernelMcp: {
    /** Environment variable that relocates the agent's config directory. */
    readonly agentDirEnvVar: string;
    /** Config directory under the user's home when that variable is unset. */
    readonly defaultAgentDir: string;
  } | null;
}

export const PI_FLAVOR: PiFlavor = {
  driverKind: ProviderDriverKind.make("pi"),
  displayName: "Pi",
  defaultBinary: "pi",
  // get_entries arrived in 0.80.3 and agent_settled landed in source at
  // 0.80.4; 0.80.5 was the first published package containing both.
  minimumVersion: "0.80.5",
  installHint:
    "Pi CLI (`pi`) is not installed or not on PATH. Install with `npm install -g @earendil-works/pi-coding-agent`.",
  loginHint:
    "Pi has no usable models. Run `pi` in a terminal and use /login, or configure an API key in ~/.pi/agent.",
  settleSignal: "agent_settled",
  sessionTree: "entries",
  tools: "pi",
  selfWakes: "stop",
  rollback: "fork",
  lossyStream: false,
  childThreads: false,
  kernelMcp: null,
};

export const PRIME_AGENT_FLAVOR: PiFlavor = {
  driverKind: ProviderDriverKind.make("primeAgent"),
  displayName: "Prime Agent",
  defaultBinary: "prime-agent",
  // The RPC surface (get_state sessionActions, fork, get_fork_messages)
  // this adapter relies on was verified against 0.9.6 and 0.9.8.
  minimumVersion: "0.9.6",
  installHint: "Prime Agent CLI (`prime-agent`) is not installed or not on PATH.",
  loginHint:
    "Prime Agent has no usable models. Run `prime-agent` in a terminal and use /login, or configure an API key in ~/.prime/agent.",
  settleSignal: "idle_probe",
  sessionTree: "fork_messages",
  tools: "ipython",
  selfWakes: "continuation",
  rollback: "tree",
  lossyStream: true,
  childThreads: true,
  kernelMcp: { agentDirEnvVar: "PRIME_AGENT_CODING_AGENT_DIR", defaultAgentDir: ".prime/agent" },
};
