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

Prime Agent can start threads, delegate tasks, and schedule work through T3 Code's MCP server. It
keeps its single Python tool and calls T3 Code from the Python kernel, guided by a short
`t3-code` skill. Prime Agent reads MCP servers only from its user settings file, so you must add
this to `mcpServers` in `~/.prime/agent/settings.json` (or in
`$PRIME_AGENT_CODING_AGENT_DIR/settings.json`) and start a new session:

```json
"t3-code": {
  "type": "http",
  "url": "http://127.0.0.1:3773/mcp",
  "bearerTokenEnvVar": "T3_MCP_BEARER_TOKEN"
}
```

Use your server's own port in `url`. Until the entry matches, the session has no T3 Code tools and
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

With the Prime Agent fork (0.10), the **Edit from here** dialog also offers **Summarize the
discarded turns**. Prime Agent then summarizes the turns you rewind past and keeps that summary in
its context. The optional instructions say what the summary should keep. The summary uses the
thread's model, so the rewind takes longer.

## Subagents

Child agents that Prime Agent starts with `rlm.spawn` appear as subagent cards with their task,
model, live activity, and result. When Prime Agent ends its turn while children are still working,
the thread shows them above the composer as a tree like Prime Agent's agents view, with each
child's model, activity, and running time, and children it started nested beneath it. A child's
reply starts a new run in the same thread. Commands Prime Agent leaves running in the background appear there too,
until they finish. Stopping background work restarts the Prime Agent session, which ends its
running children and commands. Each child also gets a read-only thread. Open it from the subagent
card to follow the child's conversation; you cannot send it messages from T3 Code.

## Work Prime Agent Starts on Its Own

Prime Agent can resume work without a new message, for example when a background command it
started finishes, a heartbeat or schedule fires, or another agent messages it. T3 Code shows that
work as a new run in the same thread, introduced by a notice of what woke Prime Agent. If you send
a message while that work is still running, your message joins it as a steer.

A heartbeat run is treated as a routine check: when it finishes, T3 Code sends no "Thread completed"
notification or mobile push, does not mark the thread unread, and does not count it as activity for
auto-settle. If the check fails or Prime Agent asks for approval or input, you are notified as usual.
Open the thread to read what a check reported.

## Permission Modes

- **Supervised** asks before every kernel cell and extension tool, showing the cell's code.
- **Full access** runs cells without T3 Code approval prompts.

**Auto-accept edits** is not offered. Prime Agent only reports an edit after its cell has run, so
T3 Code cannot tell an edit from a command before approving it.

## Prime Agent 0.10 (fork)

T3 Code also runs the T3 fork of Prime Agent 0.10 (`sakompella/prime-agent`). It recognizes the
fork by its answer to `prime-agent --capabilities`, and shows the provider as "Prime Agent (fork,
0.10)". Stock Prime Agent 0.10 and fork builds without that command are not supported: T3 Code
does not launch them, and Settings says to install the fork. Subagents, heartbeats, wakes and edit cards
work as described above. The differences:

- Only **Full access** is offered, in the composer and in the default permissions setting. The
  fork cannot ask before it runs a cell, so its threads run with full access whatever mode they
  stored. The default permissions setting shows the mode new threads run in on the provider they
  start on: the default model's provider, or, with no usable default, the first ready provider.
  Across several environments it offers only the modes all of them run. An agent or MCP client approved
  for narrower permissions cannot send work to fork threads, move a thread onto the fork, or fork
  its own fork thread into a new one.
- Rollback rewinds the session in place, and Prime Agent is told with the next message it
  accepts. T3 Code never repeats the rewind when the session reopens, because a heartbeat or
  message wake may have added conversation since. If T3 Code cannot confirm the rewind on reopen,
  because the fork still lists the discarded message or its branch could not be read, the thread
  warns "Rollback not confirmed" until the fork shows the rewind kept or accepts your next
  message. A heartbeat or message wake that runs before your next message is not told about the
  rollback.
- T3 Code tools may not reach the kernel yet. The fork runs sessions in background workers that do
  not receive T3 Code's per-session credential, so the setup above can still leave a session
  without T3 Code tools.
- Features of T3 Code's Prime Agent extension are not available: `$skill` mentions are not
  expanded, and the output cap for OpenRouter models does not apply. T3 Code leaves extension and
  tool launch arguments (`--extension`, `--no-extensions`, `--tools`, `--no-tools` and their
  short forms) out of fork launches, and warns in Settings and in the thread.
- The fork cannot generate thread titles, commit messages, pull request content or branch names.
  Choose another text generation model in Settings.
- An edit too large to show in full is marked "edit (diff truncated)".

## Troubleshooting

- If Prime Agent is unavailable, confirm that the configured binary runs on the server machine and
  reports version 0.9.6 or newer, then refresh the provider in Settings. T3 Code does not launch a
  binary whose `--version` it cannot read. Below 0.10, `--help` must offer `--mode rpc`. From 0.10
  on, `prime-agent --capabilities` must print the T3 fork's contract 1 or 2 with every command T3 Code
  uses; the Settings message names what is missing.
- If no models appear, open Prime Agent directly and confirm its authentication and model
  configuration.
