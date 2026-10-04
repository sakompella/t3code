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
