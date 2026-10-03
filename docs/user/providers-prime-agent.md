# Prime Agent

T3 Code can use your existing Prime Agent installation while keeping its models, auth, extensions,
skills, context files, and native session history. Prime Agent is a fork of Pi, so it works like
the [Pi provider](./providers-pi.md) except where noted here.

## Set Up Prime Agent

1. Install Prime Agent 0.9.6 or newer on the machine running the T3 Code server.
2. Run `prime-agent` once in a terminal and finish the provider login or API-key setup you
   normally use.
3. Open T3 Code Settings, enable Prime Agent, and refresh the provider.

If `prime-agent` is not on the server's `PATH`, set its binary path to the executable. Launch
arguments work as they do for Pi.

## Let Prime Agent Use T3 Code Tools

Prime Agent can start threads, delegate tasks, and schedule work through T3 Code's MCP server. By
default T3 Code gives it about 70 native tools for this. To give it one `ipython` tool and a short
`t3-code` skill instead, add this to `mcpServers` in `~/.prime/agent/settings.json` (or in
`$PRIME_AGENT_CODING_AGENT_DIR/settings.json`) and start a new session:

```json
"t3-code": {
  "type": "http",
  "url": "http://127.0.0.1:3773/mcp",
  "bearerTokenEnvVar": "T3_MCP_BEARER_TOKEN"
}
```

Use your server's own port in `url`. Until the entry matches, T3 Code keeps the native tools and
shows a notice with the exact entry to add. T3 Code never edits this file.

## How Work Appears

Prime Agent runs every action as a cell in one persistent Python kernel. T3 Code shows a cell that
runs a shell command (a `%%bash` cell, or a lone `bash("...")` call) as a command, and any other
cell as a Python step with its code. Edits made through Prime Agent's edit helper also appear as
file changes with their diffs.

Threads use Prime Agent's native session files in `~/.prime/agent/sessions` for resume, rollback,
and forks, so a thread started in T3 Code can be continued in the Prime Agent terminal UI. Rolling
back rewinds the conversation inside the same session file, so the discarded turns remain as a
branch that Prime Agent's `/tree` view can still show. Rolling back does not reset Prime Agent's
Python kernel, so variables and background jobs from the discarded turns can still exist; Prime
Agent is told this on its next turn.

## Subagents

Child agents that Prime Agent starts with `rlm.spawn` appear as subagent cards with their task,
model, live activity, and result. When Prime Agent ends its turn while children are still working,
the thread shows them above the composer as a tree like Prime Agent's agents view, with each
child's model, activity, and running time, and children it started nested beneath it. A child's
reply starts a new run in the same thread. Commands Prime Agent leaves running in the background appear there too,
until they finish. Stopping background work restarts the Prime Agent session, which ends its
running children and commands. Children run without a T3 Code thread of their own, so they cannot
be opened or resumed from T3 Code.

## Work Prime Agent Starts on Its Own

Prime Agent can resume work without a new message, for example when a background command it
started finishes, a heartbeat or schedule fires, or another agent messages it. T3 Code shows that
work as a new run in the same thread, introduced by a notice of what woke Prime Agent. If you send
a message while that work is still running, your message waits until it finishes.

## Permission Modes

- **Supervised** asks before every kernel cell and extension tool, showing the cell's code.
- **Full access** runs cells without T3 Code approval prompts.

**Auto-accept edits** is not offered. Prime Agent only reports an edit after its cell has run, so
T3 Code cannot tell an edit from a command before approving it.

## Troubleshooting

- If Prime Agent is unavailable, confirm that the configured binary runs on the server machine and
  reports version 0.9.6 or newer, then refresh the provider in Settings.
- If no models appear, open Prime Agent directly and confirm its authentication and model
  configuration.
