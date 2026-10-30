/**
 * `prime-agent --help` as each Prime Agent CLI prints it, captured on
 * 2026-10-11: 0.9.8 (TypeScript) on the Mac and the 0.10 fork's slice-2 build
 * on the VM. The fork has no extensions, so its help lists no `--extension`.
 * Below them, what each CLI does with `--capabilities`.
 */
export const PRIME_AGENT_098_HELP = `prime-agent - AI coding assistant with a Python REPL tool

Usage:
  prime-agent [options] [@files...] [message...]
  prime-agent <command> [args...]

Options:
Run options:
  -p, --print                        Print a response and exit
  --mode <text|json|rpc|acp|daemon>  Select the output mode (default: text)
  --cwd <dir>                        Use a specific working directory
  --offline                          Disable startup network operations
  --verbose                          Force verbose startup
  --daemon-socket <path>             Use a specific daemon socket

Model options:
  --provider <name>    Select a model provider
  --model <id>         Select a model
  --api-key <key>      Use an API key for this run
  --models <patterns>  Set comma-separated models for cycling
  --thinking <level>   Set reasoning: off, minimal, low, medium, high, xhigh, max

Session options:
  -c, --continue           Continue the previous session
  -r, --resume [path|id]   Open the agents view, or resume a saved session
  --fork <path|id>         Fork a saved session into a new session
  --session-dir <dir>      Use a custom session directory
  --no-session             Do not save the session
  --goal <objective>       Seed a persistent goal for a new root session
  --goal-token-budget <n>  Set a positive token budget for --goal

Tool and resource options:
  -t, --tools <list>          Allowlist comma-separated tool names
  -nt, --no-tools             Disable all tools by default
  -nbt, --no-builtin-tools    Disable built-in tools by default
  -e, --extension <source>    Load an extension (repeatable)
  -ne, --no-extensions        Disable extension discovery
  --skill <path>              Load a skill (repeatable)
  -ns, --no-skills            Disable skill discovery
  --prompt-template <path>    Load a prompt template (repeatable)
  -np, --no-prompt-templates  Disable prompt template discovery
  --theme <path>              Load a theme (repeatable)
  --no-themes                 Disable theme discovery
  -nc, --no-context-files     Disable AGENTS.md and CLAUDE.md discovery

Prompt options:
  --system-prompt <text>         Replace the default system prompt
  --append-system-prompt <text>  Append to the system prompt (repeatable)
  --                             Treat all following arguments as messages

Autonomous options:
  --autonomous                        Continue until gates pass or a limit is reached
  --autonomous-gate <command>         Run a completion gate (repeatable)
  --autonomous-gate-retries <n>       Set positive retries per failed gate (default: 3)
  --autonomous-gate-timeout-ms <n>    Set positive per-gate timeout in ms (default: 300000)
  --autonomous-max-continuations <n>  Set positive follow-up limit (default: 3)
  --autonomous-max-turns <n>          Set positive assistant-turn limit (default: 12)
  --autonomous-max-tokens <n>         Set positive token limit (default: 80000)
  --autonomous-timeout-ms <n>         Set positive wall-clock limit in ms (default: 1800000)

Help:
  -v, --version  Show version and exit
  -h, --help     Show this help

Commands:
  help      Show command help
  agents    Search and open sessions
  list      List agents
  sessions  Show agent status, activity, and usage
  attach    Attach the interactive UI to an agent
  stop      Stop an agent
  rename    Rename an agent
  send      Send a message to an agent
  schedule  Manage prompts that run later or on a recurring schedule
  status    Show background service status
  doctor    Inspect and safely clean up background services
  incident  Reconstruct a daemon incident from its logs
  shutdown  Stop every agent and background service
  mcp       Manage user MCP servers
  package   Manage capability packages
  update    Update Prime Agent
  model     Inspect available models
  session   Manage saved sessions
  config    Configure package resources

Run "prime-agent help <command>" for command details.
`;

export const PRIME_AGENT_FORK_HELP = `prime-agent - AI coding assistant with a Python REPL tool

Usage:
  prime-agent [options] [@files...] [message...]
  prime-agent <command> [args...]

Options:
Run options:
  -p, --print                        Print a response and exit
  --mode <text|json|rpc|acp|daemon>  Select the output mode (default: text)
  --cwd <dir>                        Use a specific working directory
  --offline                          Disable startup network operations
  --verbose                          Force verbose startup
  --daemon-socket <path>             Use a specific daemon socket

Model options:
  --provider <name>    Select a model provider
  --model <id>         Select a model
  --api-key <key>      Use an API key for this run
  --models <patterns>  Set comma-separated models for cycling
  --thinking <level>   Set reasoning: off, minimal, low, medium, high, xhigh, max

Session options:
  -c, --continue           Continue the previous session
  -r, --resume [path|id]   Open the agents view, or resume a saved session
  --fork <path|id>         Fork a saved session into a new session
  --session-dir <dir>      Use a custom session directory
  --no-session             Do not save the session
  --goal <objective>       Seed a persistent goal for a new root session
  --goal-token-budget <n>  Set a positive token budget for --goal

Resource options:
  --skill <path>              Load a skill (repeatable)
  -ns, --no-skills            Disable skill discovery
  --prompt-template <path>    Load a prompt template (repeatable)
  -np, --no-prompt-templates  Disable prompt template discovery
  --theme <path>              Load a theme (repeatable)
  --no-themes                 Disable theme discovery
  -nc, --no-context-files     Disable AGENTS.md and CLAUDE.md discovery

Prompt options:
  --system-prompt <text>         Replace the default system prompt
  --append-system-prompt <text>  Append to the system prompt (repeatable)
  --                             Treat all following arguments as messages

Autonomous options:
  --autonomous                        Continue until gates pass or a limit is reached
  --autonomous-gate <command>         Run a completion gate (repeatable)
  --autonomous-gate-retries <n>       Set positive retries per failed gate (default: 3)
  --autonomous-gate-timeout-ms <n>    Set positive per-gate timeout in ms (default: 300000)
  --autonomous-max-continuations <n>  Set positive follow-up limit (default: 3)
  --autonomous-max-turns <n>          Set positive assistant-turn limit (default: 12)
  --autonomous-max-tokens <n>         Set positive token limit (default: 80000)
  --autonomous-timeout-ms <n>         Set positive wall-clock limit in ms (default: 1800000)

Help:
  -v, --version  Show version and exit
  -h, --help     Show this help

Commands:
  help       Show command help
  agents     Search and open sessions
  list       List agents
  sessions   Show agent status, activity, and usage
  attach     Attach the interactive UI to an agent
  stop       Stop an agent
  rename     Rename an agent
  send       Send a message to an agent
  schedule   Manage prompts that run later or on a recurring schedule
  status     Show background service status
  telemetry  Show or change usage telemetry
  doctor     Inspect and safely clean up background services
  incident   Reconstruct a daemon incident from its logs
  shutdown   Stop every agent and background service
  mcp        Manage user MCP servers
  package    Manage capability packages
  update     Update to the latest Rust build (uninstalls the TypeScript version)
  model      Inspect available models
  session    Manage saved sessions
  config     Configure package resources
  factory    Share and run factory machines from the machine library
  prompt     Print the assembled system prompt with its layer breakdown

Run "prime-agent help <command>" for command details.
`;

/**
 * `prime-agent --capabilities` from the 0.10 fork at Prime commit `b514fa587`
 * (slice 4), exactly as it prints it: one JSON line, exit 0.
 */
export const PRIME_AGENT_FORK_CAPABILITIES = `{"fork":"t3fork","contract":1,"version":"0.10.0","rpc":["prompt","steer","follow_up","abort","new_session","switch_session","get_state","set_model","cycle_model","get_available_models","set_thinking_level","cycle_thinking_level","set_steering_mode","set_follow_up_mode","compact","refine","set_auto_compaction","set_auto_retry","abort_retry","bash","abort_bash","get_session_stats","export_html","fork","navigate_tree","clone","get_fork_messages","get_last_assistant_text","set_session_name","get_messages","get_commands","get_rlm_children","send_message","agent_messages_status","agent_messages_pause","agent_messages_resume","agent_messages_clear","list_schedules","add_schedule","cancel_schedule","list_heartbeats","get_heartbeat","set_heartbeat","update_heartbeat","manage_heartbeat","observe","unobserve"]}\n`;

/** The slice-4 capabilities line with some fields replaced, for contract checks. */
export const forkCapabilitiesWith = (fields: Record<string, unknown>): string =>
  `${JSON.stringify({ ...JSON.parse(PRIME_AGENT_FORK_CAPABILITIES), ...fields })}\n`;

/** Stock Prime Agent 0.10 and fork builds before slice 4 refuse the flag. */
export const STOCK_PRIME_AGENT_010_CAPABILITIES = {
  stderr: "Error: Unknown option: --capabilities\n",
  exitCode: 1,
};

/**
 * What 0.9.8 does with `--capabilities`, checked on the Mac: it takes an
 * unknown long flag as an extension flag, runs print mode on the empty
 * stdin, and exits 0 without output.
 */
export const PRIME_AGENT_098_CAPABILITIES = { stdout: "", exitCode: 0 };
