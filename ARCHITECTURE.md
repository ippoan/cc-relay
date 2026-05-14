# Architecture

Design decisions for cc-relay. This document is the record of *why*; the
issues on [project #7](https://github.com/orgs/ippoan/projects/7) cover the
*how* per phase.

## Goals

- Multiple Claude Code on Web sessions (each per repo) share state through a
  single WebSocket coordinator hosted on Cloudflare Durable Objects.
- Each agent's file changes are broadcast to every other agent.
- Notifications targeted at a specific agent surface through
  `/tmp/agent-inbox.jsonl`, which is read at `UserPromptSubmit` time so the
  message appears in Claude's context just before the next prompt.
- A shared plan (checklist of tasks) is held in the DO and mutated through
  `claim_task` / `update_task` MCP tools with simple per-task locking.
- Everything ships as a single `x86_64-unknown-linux-musl` static binary, started
  by the `SessionStart` hook.

## Why a separate daemon and MCP process

Claude Code spawns and kills MCP servers around its own lifecycle. Anything
that needs to outlive a single Claude Code restart — the WebSocket
connection to the coordinator, the file watcher — has to live elsewhere.

So:

- `rust-mcp-agent stdio` is a **short-lived relay**: Claude spawns it, it
  speaks MCP over stdio, and it forwards every tool call to the local daemon
  over `http://127.0.0.1:9876`.
- `rust-mcp-agent daemon` is the **long-lived state holder**: it owns the
  WebSocket, the inotify watcher, and the `/tmp/agent-inbox.jsonl`
  read/write loop. The `SessionStart` hook starts it with `nohup setsid`
  so it survives the Claude Code process tree.

This split also means the WebSocket reconnects independently of Claude.

## Why Rust is the source of truth for the wire protocol

The daemon (Rust) and the DO (TypeScript on Cloudflare Workers) need to agree
on every message shape. We pick Rust as the canonical definition and use
[`ts-rs`](https://crates.io/crates/ts-rs) to export TypeScript types into
`coordinator/src/generated/` as a side effect of `cargo test`. CI runs the
test suite and then `git diff --exit-code coordinator/src/generated/`; if the
generated TS drifts from what's committed, CI fails.

Rationale:

- Strong enums + `#[serde(tag = "type")]` give us a discriminated union for
  free in both languages.
- Forgetting to update one side becomes a CI failure, not a runtime mystery.
- The coordinator stays a thin pure-TypeScript Worker — no Rust toolchain
  required to deploy it.

## Protocol versioning

The first message every daemon sends is `Hello { protocol_version: u32, ... }`.
The DO checks the value and closes with code `4001` ("protocol version
mismatch") if it doesn't match. Bumping the version is a deliberate breaking
change; old daemons get a clear close reason instead of cryptic decode errors.

## Coordinator: SessionDO with WebSocket Hibernation

One Durable Object instance per session id (`/session/:id`). Inside the DO:

- `state.acceptWebSocket(ws)` enrolls each agent's socket for hibernation.
  The runtime suspends the DO when there's nothing to do and only wakes it
  on actual messages — ping/pong is handled automatically via
  `setWebSocketAutoResponse()`.
- Storage is sqlite-backed (`new_sqlite_classes` migration). Keys:
  - `agent:<id>` → `AgentMeta` (repo, joined_at)
  - `event:<ts>:<rand>` → recent `WireMessage` ring buffer
  - `plan` → `Task[]`
  - `inbox:<agent_id>:<ts>` → queued notifies for agents that are currently
    offline; flushed on reconnect.

## Distribution

`x86_64-unknown-linux-musl` static binary, attached to GitHub Releases by
the `release.yml` workflow. The `bootstrap-mcp-agent.sh` hook curls the
binary, verifies a hard-coded sha256, and starts the daemon with
`nohup setsid` so it detaches cleanly. macOS arm64 etc. are out of scope
for the MVP.

## Repository layout and worktree usage

```
crates/
  agent-core/      WireProtocol + ts-rs export
  agent-daemon/    runtime (watcher, WS client, HTTP server) — library
  agent-mcp/       stdio MCP server                            — library
  agent-cli/       binary: clap dispatcher into the two above
coordinator/       Cloudflare Worker (TS), Cargo workspace excludes this
hooks/             .claude/hooks scripts
tests/integration/ end-to-end test scripts (P4)
.github/workflows/ ci.yml, release.yml
```

When working on this repo from Claude Code, prefer separate git worktrees per
issue (e.g. `git worktree add ../cc-relay-issue-4 claude/issue-4`) so multiple
sessions can edit independent crates without stepping on each other. Push to
short-lived feature branches; the Epic stays on `main`.

## Non-goals (MVP)

- macOS / non-x86_64 builds.
- Anything other than Claude Code on Web (no Remote Control, no local CLI).
- Routines / Channels integration.
- A web UI (`coordinator/dashboard`) — separate Epic.
