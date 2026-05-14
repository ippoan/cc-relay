# Architecture

Design decisions for cc-relay. This document is the record of *why*; the
issues on [project #7](https://github.com/orgs/ippoan/projects/7) cover the
*how* per phase.

> **Status (2026-05-14):** the original Cloudflare-DO + WebSocket design
> below is **superseded** by ADR-001 at the bottom of this file. The
> historical sections are retained for context; new work should follow the
> *GitHub-as-broker + stdio MCP server* model from ADR-001.

## Goals

- Multiple Claude Code on Web sessions (each per repo) share state through a
  single broker (a GitHub repo for the MVP — see ADR-001).
- Each agent's notifications are routed to the addressed agent and surface
  through the inbox at `UserPromptSubmit` time so the message appears in
  Claude's context just before the next prompt.
- A shared plan (checklist of tasks) is held in the broker and mutated through
  `claim_task` / `update_task` MCP tools with simple per-task locking.
- Everything ships as a single `x86_64-unknown-linux-musl` static binary,
  invoked by Claude Code as a stdio MCP server (no separate long-lived
  daemon — see ADR-001).

## Why Rust as the source of truth for the wire protocol

> **Superseded by ADR-001.** With the coordinator gone, there is no second
> language consuming the protocol; ts-rs export has been removed and Rust is
> the only definition.

Originally: the daemon (Rust) and the DO (TypeScript on Cloudflare Workers)
needed to agree on every message shape, so we picked Rust as the canonical
definition and used [`ts-rs`](https://crates.io/crates/ts-rs) to export
TypeScript types into `coordinator/src/generated/` as a side effect of
`cargo test`. CI ran the test suite and then `git diff --exit-code` against
the generated tree.

## Distribution

`x86_64-unknown-linux-musl` static binary, attached to GitHub Releases by
the `release.yml` workflow. The binary is `rust-mcp-agent` and Claude Code
spawns it as an MCP server via `.mcp.json`. macOS arm64 etc. are out of scope
for the MVP.

## Repository layout and worktree usage

```
crates/
  agent-core/      Wire protocol types (Rust only, no TS export)
  agent-mcp/       stdio MCP server                            — library
  agent-cli/       binary: clap dispatcher into the MCP server
hooks/             .claude/hooks scripts
.github/workflows/ ci.yml, release.yml
```

Crates landing in later phases (per Epic #1):

```
crates/
  agent-broker/    Broker trait + GitHubBroker impl (P4 / #16)
```

When working on this repo from Claude Code, prefer separate git worktrees per
issue (e.g. `git worktree add ../cc-relay-issue-4 claude/issue-4`) so multiple
sessions can edit independent crates without stepping on each other. Push to
short-lived feature branches; the Epic stays on `main`.

## Non-goals (MVP)

- macOS / non-x86_64 builds.
- Anything other than Claude Code on Web (no Remote Control, no local CLI).
- Routines / Channels integration.
- A web UI — separate Epic.

---

## ADR-001: GitHub-as-broker + stdio-only MCP server

**Status:** Accepted (2026-05-14). Supersedes the original *Cloudflare DO +
WebSocket + long-lived daemon* design recorded above (and in #5, #7).

### Context

The MVP target is **Claude Code on Web**. During P2 the daemon and stdio MCP
relay were built and worked locally, but a sandbox audit on 2026-05-14 found
the web environment fundamentally cannot reach the coordinator we planned:

- All outbound HTTPS goes through a proxy that enforces a static host
  allowlist. Hosts outside the list (verified: `auth-staging.ippoan.org`,
  `*.workers.dev`, `pages.dev`, `dash.cloudflare.com`, `api.cloudflare.com`)
  return `403 host_not_allowed` with an `x-deny-reason` header.
- DNS for non-allowlisted hosts does not resolve at all
  (`CLAUDE_CODE_PROXY_RESOLVES_HOSTS=true` is set, so the proxy owns DNS).
- Raw TCP/UDP is blocked: `/dev/tcp/api.github.com/443` and
  `nc stun.cloudflare.com 3478` both return `Terminated`.
- Therefore: **no WebSocket to a Cloudflare Worker is reachable**, full
  stop. The same applies to any custom-hostname coordinator we might run.

What *is* reachable from inside the sandbox (verified by probing):

| Reachable                          | Notes                                  |
| ---------------------------------- | -------------------------------------- |
| `api.github.com`                   | 200, REST + GraphQL                    |
| `*.googleapis.com`                 | Pub/Sub, Firestore, GCS, Run, IAM, …   |
| `s3.*.amazonaws.com` (and friends) | DynamoDB, SQS, …                       |
| `*.r2.cloudflarestorage.com`       | Cloudflare R2 specifically (not Workers) |
| `login.microsoftonline.com`        | Azure login only                       |

Separately, the Anthropic docs confirm that **MCP connector traffic is
routed through the Anthropic backend** and is *not* subject to the sandbox
allowlist — but only for connectors registered via Claude's MCP config,
not for arbitrary outbound from the user's process.

### Decision

1. **Drop the long-lived daemon.** With no socket to hold open, there is
   nothing for the daemon to be long-lived *for*. The agent becomes a pure
   stdio MCP server that Claude Code spawns and tears down on its own
   schedule. Inbox cursor state moves into the MCP process.
2. **Replace the WebSocket coordinator with a `Broker` trait.** The MVP
   implementation is `GitHubBroker`: agent-to-agent messages and shared plan
   ops are encoded as comments / structured bodies on a designated GitHub
   issue (or set of issues) in a configurable repo. Polling drains new
   messages on demand from inside the MCP tool calls. One credential
   (a GitHub token in env) gets a user fully set up; cost risk is zero.
3. **Keep `Broker` open for extension.** Pub/Sub, R2, S3, etc. all sit in
   the allowlisted set above. New backends just add a `Broker` impl behind
   the same trait — no protocol or MCP-tool changes required.
4. **Delete the coordinator.** No Cloudflare Worker, no Durable Object, no
   ts-rs export. The protocol is now Rust-only (`crates/agent-core`).

### Consequences

- The `agent-daemon` crate is removed in this phase (#15). The HTTP
  loopback, file watcher, and WS reconnect logic all go with it. If we ever
  need a watcher again, it gets reintroduced in its own crate.
- `WireMessage` (designed around a multiplexed WS channel) becomes
  unnecessary. We keep the value types (`Priority`, `NotifyTarget`,
  `TaskSpec`, `TaskStatus`, `PlanOp`) and add a single `NotifyMessage`
  struct as the broker payload unit.
- Claude Code restarts no longer drop messages: there is no in-memory queue
  to lose. The broker is the only durable state, and the MCP process is
  stateless apart from a small inbox cursor.
- Latency on `notify_agent` is now bounded by GitHub API round-trip
  (~hundreds of ms) rather than a WebSocket push (~tens of ms). Acceptable
  for the human-in-the-loop coordination this project targets.

### Alternatives considered

- **Cloudflare Worker via custom hostname.** Doesn't help — the proxy
  allowlist is independent of TLS; a custom hostname is still rejected at
  the proxy layer.
- **Google Pub/Sub MVP.** Reachable via `pubsub.googleapis.com`, but
  setup requires a GCP project, billing, and a service-account JSON.
  Higher friction than a single GitHub token. Kept as a future `Broker`
  impl.
- **WebRTC / peer-to-peer.** STUN UDP is blocked at the sandbox; without
  STUN there is no NAT traversal. Dead end.
- **Anthropic MCP connector to a hosted relay.** Tempting because
  connector traffic bypasses the allowlist, but it requires us to operate
  a hosted server *and* register a connector per session. Worse than
  GitHub for the MVP; revisit if/when GitHub's REST limits bite.

### Phase mapping

| Phase   | Issue | Scope                                                        |
| ------- | ----- | ------------------------------------------------------------ |
| **P3**  | #15   | this ADR + workspace cleanup (delete daemon + coordinator)   |
| **P4**  | #16   | `agent-broker` crate: `Broker` trait + `GitHubBroker`        |
| **P5**  | #17   | refactor `agent-mcp` to call the broker (no more daemon URL) |
| **P6**  | #18   | `agent-cli` simplification (`stdio` only, `--broker` flag)   |
| **P7**  | #19   | end-to-end integration test against a real GitHub repo       |
| **P8**  | #20   | README rewrite + `.mcp.json` template                        |
