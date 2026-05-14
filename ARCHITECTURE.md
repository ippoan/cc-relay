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

### Validation (2026-05-14)

A throwaway `.mcp.json` probe (added in 585f85c, removed in the next commit)
confirmed end-to-end on Claude Code on Web that:

- Repo-local `.mcp.json` is read at session start, after the branch is
  checked out.
- `type: "stdio"` entries are honored; the configured `command` is spawned
  with `args` from the repo root.
- The server's `tools/list` results are registered as
  `mcp__<server>__<tool>` in the session's tool namespace.
- The full `initialize` / `notifications/initialized` / `tools/list` /
  `tools/call` round-trip over stdio JSON-RPC completes successfully.

Two additional observations from the probe that constrain the design:

- The filesystem is **not** shared across sessions — each session runs in
  its own container with a distinct `CLAUDE_CODE_CONTAINER_ID`. No
  filesystem-based broker is viable; the GitHub Issue broker assumption in
  the decision above stands.
- `.mcp.json` is read at session start only. Editing it (or the binary it
  points to) mid-session has no effect until the next session starts. The
  `SessionStart` hook (versioned under `hooks/` per #9) is the right place
  to ensure the agent binary exists before Claude Code reads `.mcp.json` —
  i.e. download a pinned release artifact, or `cargo install` from a tag,
  whichever phase #20 lands on.

---

## ADR-002: End-user auth via auth-worker MCP OAuth Provider

**Status:** Accepted (2026-05-14). Issue
[#33](https://github.com/ippoan/cc-relay/issues/33).

### Context

ADR-001 left `GitHubBroker` authenticating with a static GitHub token
supplied by the caller. That suffices for the maintainer / CI path (App
PEM → installation token, see `docs/github-app.md`), but does **not**
work for end-users: shipping a public binary that needs the App private
key, or asking every user to mint a PAT and paste it into env, are both
non-starters.

We needed a flow that:

- Lets an end-user authenticate once on their host with no manual
  copy-paste of credentials.
- Yields a real `api.github.com` OAuth token with Issues r/w + private
  repo access (so the GitHubBroker hot path stays unchanged).
- Survives sandbox restarts (refresh works without re-prompting).
- Requires no new infrastructure on the cc-relay side beyond a CLI
  subcommand.

[`ippoan/auth-worker`](https://github.com/ippoan/auth-worker) already
provides this: an MCP OAuth Provider that wraps GitHub OAuth, supports
RFC 8628 device flow, and exposes `/mcp/introspect` to swap a JWT for
the bound GitHub OAuth token. `github-mcp-server-rs` is its existing
consumer. Auth-worker
[PR #131](https://github.com/ippoan/auth-worker/pull/131) finalised the
integration contract — dynamic scope mapping
(`mcp.write` → `read:user repo`) + a documented sandbox-reachability
workaround.

### Decision

1. **Consume auth-worker; do not build our own.** No new OAuth
   infrastructure on the cc-relay side. `crates/agent-broker/src/{auth,
   token_cache, introspect}.rs` implement the consumer protocol
   (device flow + introspect) by inlining patterns from
   `github-mcp-server-rs`. A future `auth-worker-client-rs` crate can
   replace the inlined code without touching the broker.
2. **Host-side login, read-only mount.** `rust-mcp-agent auth` runs on
   the host (laptop / workstation), writes `~/.cc-relay/token` with
   mode 0600. Claude Code on Web mounts the file read-only into the
   sandbox. The broker process inside the sandbox reads it and
   refreshes via `TokenManager::ensure_fresh` whenever the 5-minute
   skew window trips. This sidesteps the sandbox's allowlist block on
   `auth.ippoan.org` (see `docs/relay-validation.md`).
3. **GitHubBroker hot path stays direct.** The broker uses the raw
   `github_token` extracted from `/mcp/introspect` to call
   `api.github.com` directly — ADR-001's "GitHub-as-broker" design is
   untouched. auth-worker is hit only on refresh, roughly once per
   hour per session.
4. **`INTERNAL_SHARED_SECRET` shared with `github-mcp-server-rs`.** The
   value is distributed out-of-band by the auth-worker maintainer (see
   `docs/credentials.md` §4). cc-relay does not embed it in the binary.
   Long-term, auth-worker
   [#91](https://github.com/ippoan/auth-worker/issues/91) replaces the
   shared secret with a Service Binding.
5. **Static `client_id = "cc-relay"`, scope = `mcp.read mcp.write`.**
   No Dynamic Client Registration. Auth-worker's device flow does not
   validate `client_id` — the real authorization gate is
   `GITHUB_MCP_USER_ALLOWLIST`. JWT `aud` is fixed to
   `"github-mcp-server-rs"` for *every* consumer (auth-worker
   simplification).

### Consequences

- `GitHubBroker` no longer holds a `&str` token directly. It owns an
  `Arc<TokenManager>` and resolves `Authorization` per-request via
  `ensure_fresh` + `bearer`. Tests that previously passed a literal
  string still work — `GitHubBroker::new(.., token: &str)` wraps the
  caller in `TokenManager::static_token` internally.
- One file is the entire on-disk surface: `~/.cc-relay/token` (mode
  0600, plain JSON). No keyring integration, no encryption-at-rest.
  Hardening tracked separately.
- 30-day refresh TTL implies the user must re-run `rust-mcp-agent
  auth` at most once a month. Reachable via `verification_uri_complete`
  in a single browser click.
- Sandbox reachability remains a soft dependency, not a hard one. If
  Anthropic later allowlists `auth.ippoan.org`, the host-side mount
  step can be retired without protocol changes (the CLI just runs
  inside the sandbox instead).

### Alternatives considered (rejected)

- **PAT distribution.** Every user mints a PAT, pastes it into env.
  Rejected as friction + leaked-PAT risk.
- **Self-implemented device flow on a new `auth.ippoan.org`.** Already
  exists upstream; reinventing it would diverge our crypto / KV
  surface from the rest of the org.
- **Streamable HTTP MCP server + Anthropic Custom Integration.** Was
  considered (would bypass the sandbox allowlist via MCP routing). The
  REST surface auth-worker needs for device flow / introspect is *not*
  part of the MCP connector — so this would still require running a
  shim. Higher complexity than host-side mount; can be revisited if
  Phase 7 relay matures.
- **PEM embedded in binary.** Same secrecy problem as PAT, worse
  blast radius.
- **VPS / Cloudflare DO / sandbox-internal token.** None solve "no
  manual copy-paste"; they just move the secret around.

### Validation

- Unit tests (`crates/agent-broker/src/{auth,token_cache,introspect,token_manager}.rs`)
  cover RFC 8628 polling, refresh, introspect happy / 401 / 503 / inactive,
  file mode 0600, refresh-then-introspect.
- Integration test
  `crates/agent-broker/tests/e2e_auth_then_github.rs` runs two
  wiremock servers (auth-worker + api.github.com) and asserts that a
  near-expired cached token triggers refresh + introspect, and the
  subsequent `api.github.com` call carries the new bearer.
- Sandbox reachability probe (`scripts/probe-relay-reachability.sh`)
  and findings (`docs/relay-validation.md`) confirm the host-side
  mount workaround is necessary as of 2026-05-14.
