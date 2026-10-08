# This fork

`sakompella/t3code` is a personal fork of [pingdotgg/t3code](https://github.com/pingdotgg/t3code). Its main addition is support for [Prime Agent](https://github.com/PrimeIntellect-ai/prime-agent). Upstream PRs are not a goal. These rules override the upstream guidance in `AGENTS.md` where they conflict.

## Priorities, in order

1. **It works for the owner, and they stay productive.** This comes first, even when a change moves the fork far from upstream.
2. **Maintainability.** Keep code simple. Delete workarounds that a better fix replaces. Don't keep fork code because it already exists.
3. **Closeness to upstream `main`.** A minor concern. Prefer upstream's features and patterns when they do the job, and avoid needless divergence, but never at the cost of 1 or 2.

## Prime Agent

- **Don't change Prime Agent.** No fork, no local Nix patches. The owner uses it for most of their work outside T3. Fix integration problems in T3.
- **A T3-shipped Prime Agent extension is a last resort.** Use one only when it gives a material gain that T3-only code can't. Keep it minimal. T3 already loads one (`apps/server/src/orchestration-v2/Adapters/piT3McpInjection.ts`).
- **Target the version the owner runs now**, installed through `numtide/llm-agents.nix`. As of 2026-10-04 that is 0.9.8 (TypeScript). Don't add compatibility code for versions nobody here runs. Do rely on what Prime Agent's RPC actually offers rather than on a version number, so another machine on another version fails clearly instead of silently. Prime Agent 0.9.9 is a Rust rewrite whose RPC has no daemon attach, observation or subagent list. Expect integration work when it ships.
- **Prime Agent sessions see exactly one tool, `ipython`.** T3 features reach the agent through the kernel and the `t3-code` MCP server, never as native tools.

## Providers

Keep upstream's providers even if unused. Removing them saves little and adds a large diff.

## Fork features the owner wants kept

Don't remove these to reduce divergence:

- The Prime Agent heartbeat status line in the composer.
- Refinement notices, the "Finishing up…" row shown while Prime Agent reviews its harness after the final reply, and expandable long notices.
- Syntax highlighting for Python and shell tool rows (web and mobile), including the exit code shown under highlighted source.

## What this fork changes

Changes against upstream `main`, grouped by feature. Each item says what changed, then why when that isn't obvious. Upstream PRs are not planned, so the reasons live here rather than in PR descriptions. When you change fork behavior, update this list.

### Prime Agent provider

- **Prime Agent is a provider.** It runs through upstream's Pi RPC adapter as a second "flavor" (`PiFlavor.ts`), with its own settings, picker entry and user guide (`docs/user/providers-prime-agent.md`). Prime-Agent-only logic lives in `Adapters/primeAgent*.ts`.
  - Why not a new adapter: Prime Agent is a fork of Pi and speaks the same RPC protocol; reusing the adapter keeps the diff in one place.
- **Prime Agent sessions see one model tool, `ipython`.** T3 features reach the agent through the kernel and the `t3-code` MCP server, using a bundled `t3-code` skill. If `t3-code` isn't declared in Prime Agent's global settings at this server's endpoint, the session still opens kernel-only and shows the exact entry to add. The T3 extension still loads, only for the permission hook.
  - Why: the owner's rule is one tool. Prime Agent 0.9.8 reads MCP servers only from the global settings file, so T3 can't supply the server at launch. Without the permission hook, Supervised mode would run every tool unchecked.
- **Prime Agent opens a thread by launching on its session** (`--resume <session file>`), not by switching sessions after launch.
  - Why: in 0.9.8, replacing a session at runtime (`switch_session`, `new_session`) drops its agent-messaging, `agent_observe` and heartbeat support, so a T3-hosted parent couldn't message its subagents. Plain Pi still switches sessions.
- **Stop and reopen keep the same session.** Prime Agent gets up to 15 s to close cleanly before a forced kill, and a launch or resume refused with "Session is already active" retries the same session for up to 15 s.
  - Why: the old 1 s kill cut off Prime Agent's clean close, so its daemon held the session for 30 s. The next message then failed to resume and started a fresh session with a context handoff, which lost the subagents.
- **Lost events are rebuilt from Prime Agent's stored conversation** (`primeAgentReconciler.ts`). At settle, on reconnect and on a backed-off timer while background work exists, T3 reads `get_messages` between two `get_state` reads. It adds replies, tool results and follow-up turns it never received. It also settles subagent cards from the notices the parent stored. Nothing is added twice. Unknown outcomes stay unknown.
  - A running turn does not wait for settle when its stream moves past a reply or tool whose end never came (a tool starts, the next reply starts, or the turn ends). T3 reads `get_messages` once then and completes the stored reply or tool result in the place it first appeared. If that read does not find it, a cut reply stays open until the settle read and is never shown as final.
  - Why: under load, the Prime Agent daemon drops events for slow clients and sends a catch-up snapshot, which RPC mode discards. On 2026-10-04 seven replies and many "finished" updates never reached T3. One reconciler replaced several piecemeal repairs. On 2026-10-08 a reply lost its end mid-sentence and showed cut off for 5 minutes, until its turn settled.
  - Limit: a reply recovered after its turn ended appears in the run active at that time.
  - Limit: Stop ends a cut reply with an aborted copy that stock stores under a new timestamp. T3 holds the copy until a history read places it. When the stream and the history cannot prove which reply the copy ended, T3 keeps them apart, and the reply shows its cut text. One such miss: a live copy that holds exactly what the reply showed, next to an older unseen copy that also fits. RPC mode drops the daemon's `meta.sequence`, so T3 cannot see the gap between them. Forwarding the sequence would close it.
  - Limit: A failed attempt removed by an unseen retry can be shown as that retry when its aborted copy still holds the text T3 showed. The retry keeps the attempt's row; later runs stay separate. A stored completed/failed retry reply or compaction summary vetoes that match, leaving distinct rows.
- **RPC accepts records up to 64 Mi characters.** A larger record fails every pending request with `PiRpcRecordTooLargeError`, and reading resumes at the next line.
  - Why: upstream silently dropped lines over 8 MiB, so a long session's `get_messages` vanished and the request hung. 64 Mi is about 5 times the largest real history; parsing at the cap blocks for about 0.2 s.
- **A slow `get_state` reply no longer kills a busy session.** Probes allow more time on a busy machine, and a probe that found no commands keeps the known ones.
  - Why: missed status replies under heavy load were read as a dead process, which killed working sessions.
- **Child dialogs reach the parent UI.** A small T3-shipped extension uses stock Prime Agent's public daemon client to attach a real dialog recipient before T3 observes a child. Selection, input, editor and approval requests appear in the parent's thread with the child's session id. Answers return to the child unchanged; unavailable parent UI cancels rather than approving. An extension reload moves the attached daemon clients to the reloaded extension instead of attaching again, so each child question has one parent recipient. After a lost daemon socket the route is not ready. Stock `DaemonClient` auto-reconnect retries for up to 60 s, and the extension then attaches the child again by itself. Parent dialogs open at the loss close, and their child requests get `cancelled: true` once reattached. If recovery fails, the parent shows an error and the route is dropped. Prime Agent itself is not patched.
  - Limit: stock Prime Agent never replays a child dialog sent while the socket was down and has no public list of pending dialogs. Such a request waits for its own timeout, an answer from another UI client, or the child session closing. T3 does not stop the child.
  - Why: stock 0.9.8's child watcher advertises extension UI, but RPC observation drops those requests. Its daemon can also admit a child dialog from another session's aggregate UI flag. A real recipient fixes both paths without disabling parent approvals.
- **Subagents.** They show as background work, with their own read-only child threads. A child's terminal update reaches the parent even after the run that started it has ended. A finished child stays known, so its follow-up replies still reach its thread. Shell jobs and kernel jobs are tracked as background work too.
- **Self-wakes.** Heartbeats, subagent messages and background-job completions continue the thread as a new run with a notice saying why, instead of ending it.
  - **A wake keeps the user's settle.** Only a message the user sends (including their scheduled tasks) un-settles a thread. A settled thread still leaves the settled shelf on web and mobile while a wake needs the user: a pending approval or question, or a run that failed after the settle. Its completion does not move it, but still sends the completion alert. A server restart note (`restartContinuationOfRunId`) is a wake too.
    - Why: every wake used to clear settle, so a background command finishing or a heartbeat put a parked thread back on top of the inbox.
  - **Snooze is strict.** Only its timer or an explicit Unsnooze ends a snooze (Wake in the menus, Wake now in the open thread, dragging the row out of the shelf, Undo, or `unsnooze` in the `t3-code` thread tool). Nothing the thread does or receives ends it early: finished commands, subagent and delegated results, heartbeats, approvals, questions, failures, finished runs, and new messages, including the user's. The agent keeps working, including a usage-limit auto-resume that comes due, and every event and message is still stored; opening the thread shows its status and requests as usual. A snoozed thread also sends no alert: no toast, sound or desktop notification on web and desktop, and no mobile push or Live Activity (the server publishes nothing for it). Auto-settle skips it until the timer ends. Usage-limit recovery arms a snooze only on a thread that is not already snoozed, and only an explicit stop for the same recovery ends one. Pinning does not end a snooze, however it is done, including dragging a snoozed row into Pinned; the pin shows once the snooze ends. Dragging the row into Active is the wake.
    - Why: a snooze that any activity could undo put the thread back on top of the inbox within minutes, because Prime Agent threads keep waking themselves. The old exceptions (approval, failure, finished run) had the same effect.
    - Limit: a request that arrives under a snooze waits, unseen in the inbox, until the timer ends or the user opens the thread. A thread cannot be snoozed while a request is already pending. When the timer ends, nothing announces what happened under it, and a completion or failure that finished meanwhile is never pushed later. The relay dates it by the snooze deadline, which stays on the thread after the timer ends, and by `snoozeEndedAt`, which an unsnooze sets to the earlier of the deadline and now before it clears the deadline (an unsnooze with no snooze to end leaves it alone). Both come from the same thread read, so neither needs publishing to be on at the time or any event ordering. Unsnoozing does not push either; a running thread's Live Activity returns at its next event.
  - **Limit: running `bash()` jobs.** A thread that leaves a `bash()` job running still reads as done when the model stops, as upstream decided for Claude background shells. Prime Agent 0.9.8 gives no sign of whether a job will end or is a dev server that runs for hours, so T3 can't hold the thread as Waiting.
- **Rollback and fork.** Rollback rewinds the session in place instead of forking a session file, and tells the agent its live tool state was not reset. A fork can start from a turn while a later turn still runs.
- **Turn lifecycle repairs.** Tools whose end event never arrived are closed when the turn ends. A turn whose closing events were dropped still settles. A pending retry is cancelled before T3 replaces the session.
- **Display of tool calls.** Python cells show their code, bash cells show as commands, and edits show relative paths.
- **Pages from `html_render` show inline.** Prime Agent calls T3's tools from Python, so its tool row is a `python` cell that only prints the result, and clients never showed the page. When a driver that calls T3 from its kernel (`callsT3ToolsFromKernel`) publishes a page, the handler records it in the caller's run as its own `html_render` tool item (`html_render.record`), the shape Claude's native row has. Clients and thread deletion read it with no change. Once the record starts it can't be cancelled. A record that never committed removes the page and fails the call; one that committed keeps it even if its reply was lost. When T3 can't tell which happened, it keeps the page and fails the call saying so; a stray file beats a timeline item pointing at a missing page. A thread deleted during publication refuses the record, and a caller whose provider is no longer registered fails before publishing. Other providers' own `html_render` rows already carry the page, so they get no second item.
  - Why not read the printed output: it is Python's repr of the result, not a format T3 controls.
  - Limit: pages published before this change stay as printed output.
- **Compatibility policy.** The model manifest marks Prime Agent 0.9.6 and later as supported and recommends 0.9.8 or later.
- **Diagnostics.** Each subagent update is logged with the run that took or dropped it, and a failed launch reports the end of stderr.

### Usage accounting

- **Usage Cost/Tokens includes Pi and Prime Agent session logs**, including Prime Agent child sessions, disabled accounts and custom agent homes. It uses the existing incremental transcript cache and drops duplicate response IDs per server. A late child-usage attribution reparses only its changed file to correct cached parent totals. Compaction and branch summaries use saved costs under `summary (model not recorded)` when the log omits the model. Saved model costs win; zero or missing response costs fall back to model pricing. Web and mobile show separate Pi and Prime Agent series.
  - Why: the hub quota connection supplies limits, not durable request history. Local logs cover agent work without an always-on hub collector. Other hub clients are not included, and copied history on separate machines may still count twice.

### Prime Agent UI (keep)

- **Heartbeat line.** Threads publish the heartbeats their session runs, and web and mobile show them in a quiet line above the composer.
- **A heartbeat run is a routine check, not a finished task.** The wake notice records which heartbeat fired (`heartbeat` notification source), and the thread shell carries the latest run's trigger (`latestRunTrigger`). A heartbeat run that completes is routine (`isOrchestrationV2RoutineRun`). For a routine run:
  - Web and desktop show no "Thread completed" toast, sound or notification. The relay publishes nothing, so mobile gets no push and no Live Activity.
  - It does not mark the thread unread on web or mobile, and Mark unread rewinds to the last run someone asked for. The shell's `latestTaskRunCompletedAt` carries when that run ended, so an unseen completion from before the check stays unread.
  - It is not thread activity for auto-settle, so a thread with a heartbeat still goes stale and still settles when its pull request merges.
  - A heartbeat's prompt never counts as the latest user message, so "Last user message" ordering (project order, the command palette's recent threads, mobile home) ignores it.

  Failures, approvals and questions from a heartbeat run alert, mark unread and count as activity as usual. A routine run neither alerts nor repeats an earlier completion alert. Shell updates are coalesced, so the web coordinator reads a routine run as standing for the latest run someone asked for (`latestTaskRunCompletedAt` and `latestTaskRunStatus`). A user's completion that a check started or ended in the same update still alerts once, when the check ends. A task that failed, was interrupted, or comes from a server that sends no status never alerts as completed.
  - Why: Prime Agent threads with a heartbeat finished a run every few minutes, and each one alerted as if the task were done. Prime Agent gives no typed "nothing new" signal, and reading the reply text would be a guess, so the trigger decides: a check's end is execution finishing, and the user learns of real news through failures and requests or by opening the thread.
  - On the wire, the source is sent as `background_task` with `work: "heartbeat"`, so clients from before it still decode it. Servers from before it send no trigger, which clients read as a run someone asked for.
  - Not done: alerting when a check deletes its own heartbeat (the watch ended). Prime Agent reports heartbeats only as a list read at settle, and T3 clears that list when the session stops or the server restarts. Read later, every earlier check would then look like it ended its watch. Doing it right needs the outcome decided once at settle and stored on the run, and an agent may also delete and recreate a heartbeat to change it.
  - Limits: every successful check is quiet, including one that reports good news or a problem in its reply text. Nothing stored tells a no-change check from a meaningful one, and T3 does not guess from the reply text.
  - Whether a wake un-settles or un-snoozes a thread is the settle and snooze policy's decision, not this one.

- **Background-work banner.** Live subagents show as a tree in the composer banner, including while a turn is running.
- **Notices.** Prime Agent marks routine notices as info or progress, and they render as quiet rows. Finished progress notices disappear. Refinement outcomes show as notices.
  - **"Finishing up…"** is shown while Prime Agent reviews its harness after the final reply. Without it the turn looks stuck, because Prime Agent emits nothing during that review. The row appears only after 1.5 s.
- **Cache-miss notices.** A Prime Agent thread shows a warning notice when a parent reply re-billed most of the prompt the previous reply cached, for example "Cache miss after 10m idle: 45k tokens re-billed (~$0.14)". Misses below 20k tokens and $0.10 stay quiet. The text says only what usage shows: a model switch, or an idle gap past 5 minutes. Plain Pi shows none. The detector is T3's own (`primeAgentCacheMiss.ts`); Prime Agent, the protocol and settings are unchanged, and there is no running total.
  - Why: a cold cache bills the whole prompt again, and nothing in a T3 thread said so.
  - How: the baseline is the previous reply's prompt. It is seeded from `get_messages` when T3 attaches, so the first reply after a reopen is compared with the last stored one, and older misses in that history stay silent. A live reply carries the parent's own usage. A stored reply may have its subagents' usage folded in, so a stored folded reply resets the baseline instead of counting. A reply counts once, by its response id or else its timestamp, so a catch-up read never repeats a live notice; an equal timestamp alone never hides a distinct reply. The notice reads live and stored replies by this identity, before the reconciler's coarser timestamp key can call them already seen. Compaction, branch summaries and rollback reset the baseline; a summary counts once by role and timestamp, so one sharing a timestamp with a counted reply still resets, and a re-read of it never wipes a newer baseline. Prices come from the reply's own costs, and an unknown cache-read price counts as free.
  - Limits: a folded stored reply leaves no baseline, so the reply after it is not compared. A reply that arrives after a newer one was counted is history and does not move the baseline. Replies without a response id and with the same timestamp count as one.
- **Long notices expand.** A long notice can be opened to read all of it, and a notification with no detail no longer opens an empty panel.
- **Syntax highlighting** for shell commands and Python cells in tool rows, on web and mobile. Mobile shows the exit code under the highlighted source.

### General fixes

These aren't Prime Agent-specific.

- **Replies to steers and notifications stay visible** inside a folded run on web and mobile, and each segment between them folds separately. Before, only the run's last reply showed.
  - Why: a single fold for the whole run hid later work above earlier steers, so the collapsed view broke time order. Each segment now has its own "Worked for" row in place, timed and expanded on its own. A run without steers or notifications still has one fold. Segment keys and labels are shared in `packages/client-runtime/src/state/runFold.ts`.
- **A steer sent to a turn that just ended is no longer lost.** If the turn failed, the steer runs as the next run. If it was stopped, the steer goes back to the held queue.
- **`ELECTRON_RUN_AS_NODE` is passed only to children that run Electron as Node.**
  - Why: it leaked into every spawned process. Agents and tools that launch Electron apps then started bare Node instead.
- **Faster quit.** Server shutdown releases provider sessions concurrently. Stopping a Pi session returns as soon as the process exits. The desktop app gives the backend 3 s before a forced kill.
- **Startup.** Requests that arrive after the port is bound, but before the server is ready, get `503` with `Retry-After` instead of hanging.
- **Preview.** Evaluating a void expression, like `click()`, returns `null` instead of failing.
- **Rollback.** A fork no longer brings back turns that were rolled back first.
- **Provider switching.** Agent instructions say to delegate to another provider only for a capability the current one lacks (for example Codex computer use), not to reach a model family. Prime Agent can run GPT models itself.
