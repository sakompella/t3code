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

## How Work Appears

Prime Agent runs every action as a cell in one persistent Python kernel. T3 Code shows a cell that
runs a shell command (a `%%bash` cell, or a lone `bash("...")` call) as a command, and any other
cell as a Python step with its code. Edits made through Prime Agent's edit helper also appear as
file changes with their diffs.

Threads use Prime Agent's native session files in `~/.prime/agent/sessions` for resume, rollback,
and forks, so a thread started in T3 Code can be continued in the Prime Agent terminal UI.

## Subagents

Child agents that Prime Agent starts with `rlm.spawn` appear as subagent cards with their task,
model, live activity, and result. A turn stays open until its children finish, so a parent that
waits for a child's reply finishes in the same turn. Stopping the turn restarts the Prime Agent
session, which also stops its running children. Children run without a T3 Code thread of their
own, so they cannot be opened or resumed from T3 Code.

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
- Heartbeats or schedules that start work in a session T3 Code owns, outside a T3 Code turn, stop
  that session so the work does not run unseen.
