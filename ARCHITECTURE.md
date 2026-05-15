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

---

## ADR-003: Sandbox auth via Claude Code MCP connector + auth-worker user-less relay

**Status:** Proposed (2026-05-14). Supersedes ADR-002 (host-side mount) for
the Claude Code on Web hot path. ADR-002 is retained as the CI / non-Claude-
Code fallback. Issue [#35](https://github.com/ippoan/cc-relay/issues/35).

### Context

ADR-002 ships an end-user via `rust-mcp-agent auth` on the host laptop, then
read-only mounts `~/.cc-relay/token` into the sandbox. This works but adds a
2-step setup (host login + mount config) before `fresh clone → open in Claude
Code Web` becomes useful.

Investigation of `ippoan/auth-worker` (2026-05-14, `src/`) found that the MCP
OAuth Provider already exposes a complete *MCP relay*:

- `POST https://mcp(-staging).ippoan.org/u/<github_login>/mcp` — HTTP bridge
  (Phase 7 / #119 implemented in `src/durable_objects/mcp-session-do.ts`),
- `GET .../u/<github_login>/connect` — outbound WebSocket from a binary,
- DCR (`/mcp/register`) + Authorization Code + PKCE on `mcp.ippoan.org`
  (Phase 5 / #128 implemented in `src/handlers/mcp-{register,authorize,
  auth-callback}.ts`),
- `WWW-Authenticate: Bearer realm="MCP", resource_metadata=…` on 401 (MCP
  Authorization spec 2025-06-18 compliant).

The crucial property: **Claude Code on Web's MCP client traffic is routed
through the Anthropic backend**, not the sandbox proxy — so `mcp.ippoan.org`
is reachable from a session even though direct `curl` from inside the
sandbox still gets `403 host_not_allowed` (cf. ADR-002 §Validation,
`docs/relay-validation.md`).

The remaining gap is that the existing relay endpoint is **user-scoped**
(`/u/<github_login>/…`) — a `.mcp.json` committed to a shared repo cannot
contain a literal login. A user-less endpoint that resolves the DO from
the JWT's `github_login` claim closes the gap and lets the same
`.mcp.json` work for every collaborator.

### Decision

1. **Auth-worker exposes a user-less MCP endpoint.** New routes on
   `mcp(-staging).ippoan.org`:
   - `POST /mcp` — same body / behavior as `/u/:user/mcp`, but DO id is
     derived from `verifyMcpJwt(jwt).github_login` rather than the URL.
   - `GET /connect` — analogous WS upgrade for the binary side.
   Existing `/u/:user/mcp` routes are retained for `github-mcp-server-rs`
   back-compat. Tracked in a separate auth-worker issue.
2. **Commit `.mcp.json` at the cc-relay repo root.** Single static entry:
   ```json
   {
     "mcpServers": {
       "cc-relay": {
         "type": "http",
         "url": "https://mcp-staging.ippoan.org/mcp"
       }
     }
   }
   ```
   `type: "http"` matches the Streamable HTTP bridge (POST + JSON, no SSE
   upgrade — confirmed via `src/handlers/mcp-relay-bridge.ts` reading).
   Production switches to `mcp.ippoan.org` after staging acceptance.
3. **cc-relay broker becomes the MCP server itself.** Option (b) from #35
   §3: `crates/agent-broker` (or a new `crates/agent-mcp-server`) hosts
   an `rmcp::StreamableHttpService` + outbound WS client to
   `wss://mcp-staging.ippoan.org/connect`. Tools exposed mirror the
   `GitHubBroker` API (notify_agent, claim_task, plan ops) but the
   broker no longer calls `api.github.com` from inside the sandbox —
   the host-side broker process holds the auth-worker JWT and calls
   GitHub directly.
4. **First-session UX is OAuth-driven.** On `tools/list`, Claude Code Web
   POSTs `/mcp` → 401 + `resource_metadata` → user is shown the
   `mcp.ippoan.org/authorize` GitHub consent screen
   (`mcp.write` → GitHub `repo` scope). Approval mints a JWT scoped to
   the user's `github_login`. No `~/.cc-relay/token` step from the user's
   perspective; the host-side broker stores the JWT in memory and
   refreshes via `/mcp/token` (refresh_token grant) before expiry.
5. **`INTERNAL_SHARED_SECRET` is no longer needed.** The broker does not
   call `/mcp/introspect` (it already holds the JWT). ADR-002 §4
   dependency is dropped.
6. **ADR-002 retained as fallback.** CI runs, non-Claude-Code-Web local
   shells, and any environment without MCP-routing-capable client
   continue to use host-side `rust-mcp-agent auth` + token mount.

### Consequences

- **Sandbox auth is one user-facing click**: GitHub consent on first
  session. No host CLI invocation, no file mount step.
- **Broker is a long-running host process** (WS connection to
  `mcp.ippoan.org`). Previous ADR-002 design ran the broker inside the
  sandbox; that changes — the sandbox now only holds the Claude Code
  agent, which reaches the broker indirectly via the Anthropic MCP
  router → auth-worker DO → host-side broker WS.
- **JWT lives in host broker memory only.** A refresh_token may be
  persisted to `~/.cc-relay/refresh_token` (mode 0600) so the host
  broker survives restarts, but the access JWT itself is never written
  to disk.
- **Latency:** Claude Code Web → Anthropic backend → `mcp.ippoan.org`
  DO → WS → host broker → `api.github.com` → reverse. Two extra hops
  vs ADR-002's direct `api.github.com` call. Acceptable for the
  human-in-the-loop coordination this project targets.
- **auth-worker dependency widens.** Previously auth-worker was a
  refresh-time-only dependency (≈ 1/hour). Now every MCP tool call
  passes through `mcp.ippoan.org`. Outage on auth-worker → all tool
  calls fail. Mitigation: ADR-002 fallback path remains operational
  per a documented env-var switch.

### Alternatives considered (rejected)

- **`<github_login>` placeholder via SessionStart hook.** `.mcp.json` is
  read at session start *before* hooks complete (Claude Code hooks
  docs: "SessionStart fires before servers finish connecting"). A
  hook-rewritten `.mcp.json` would only apply to the *next* session,
  defeating the goal.
- **`install.sh` after clone.** Adds a manual step on every fresh
  clone. Negates the "open in Claude Code Web and it just works"
  property the MVP is chasing.
- **broker remains in sandbox + (a) file watcher.** Keeps the ADR-002
  host-side login step. Smaller refactor but does not deliver the
  user-facing simplification motivating #35.

### Validation (to land before flipping to Accepted)

- New auth-worker route `POST /mcp` returns 200 on a valid JWT and 401
  with the right `WWW-Authenticate` on a missing one (mirrors
  `mcp-relay-bridge.test.ts`).
- Host broker starts, opens WS to `wss://mcp-staging.ippoan.org/connect`,
  registers tools, and serves a `tools/call` round-trip from a Claude
  Code Web session opened on a fresh clone of `ippoan/cc-relay`.
- Acceptance from #35: `mcp__cc_relay__*` (or whatever final naming) is
  visible in the session tool namespace, and a sample tool call
  resolves an `api.github.com/repos/ippoan/cc-relay/issues` request
  via the host broker with status 200.
- `docs/credentials.md` §1 rewritten; ADR-002 retained as §2 (CI /
  fallback) — covered separately under #35 acceptance.

### Phase mapping

| Phase | Repo | Scope |
|-------|------|-------|
| **A** | `auth-worker` | new issue: `POST /mcp` + `GET /connect` user-less routes on `mcp.ippoan.org` |
| **B** | `cc-relay`    | ADR-003 (this commit) + `.mcp.json` at repo root pointing at staging |
| **C** | `cc-relay`    | `crates/agent-broker` → `crates/agent-mcp-server`: rmcp StreamableHttp + WS client |
| **D** | `cc-relay`    | `rust-mcp-agent serve` subcommand: host-side broker daemon entrypoint |
| **E** | `cc-relay`    | `docs/credentials.md` §1 rewrite; ADR-002 → §2 fallback |
| **F** | both          | staging acceptance: fresh-clone Claude Code Web round-trip |
| **G** | `cc-relay`    | flip ADR-003 Status to Accepted; switch `.mcp.json` to prod `mcp.ippoan.org` |

---

## ADR-004: GitHub issue activity webhooks via per-issue WebSocket rooms

**Status:** Proposed (2026-05-15)。「みて」手動 polling を廃止する。

### Context

ADR-001/002/003 は据え置き。session は `mcp.ippoan.org` に認証して
broker tool (`notify_agent` / `get_inbox` / ...) を WebSocket relay 経由
で叩ける。**だが GitHub issue 上の state 変化を active session が知る
手段は polling しかない。**

Anthropic が用意する `subscribe_pr_activity` は PR 用に server-side
webhook を `<github-webhook-activity>` 形式で配信し、browser を閉じた
session も wake する。**issue 版は存在しない** — Anthropic MCP toolset
に `subscribe_issue_activity` は無い。

実際の痛み (2026-05-15 session): 「issue 作る → 別 session に作業させる
→ 返信コメントを待つ」のラウンドごとに operator が手で `みて` と打って
`issue_read` を発火させる必要があった。「agent task thread を issue で
回す」発想は成立するが、運用が消耗する。

ここに着地する前に却下した workaround:

- **`rust-mcp-agent` 内 in-process polling。** authenticated polling
  なら rate-limit は問題にならず、container が hot な間は機能する。
  しかし CCoW container は数時間無動作で reclaim される (Claude Code
  on the web 公式 docs)。tokio task は死に、reclaim 中に発生した
  event は再 wake 時に拾えない。**hibernate しやすい container の中
  から wake を解く方法は構造的に存在しない** — 外部からの配信が必須。
- **PR-per-task (`subscribe_pr_activity` 利用)。** インフラ追加ゼロで
  今日動くが、コード変更のない task のために empty PR が貯まる。
  繋ぎとしてはありだが、「GitHub Issue を broker にした agent 間
  メッセージ relay」という cc-relay 本来の形ではない。
- **`McpSession` (per-user) DO + 別 `IssueSubsDO` (per-issue) で
  subscription state を server-side に持つ案** (本 ADR の前稿)。
  server 側に subscription registry を抱える時点で state 管理コストが
  発生し、`pending_event:` buffer の TTL / GC / replay まで設計する
  羽目になった。「subscription を server に登録する」発想自体が
  unnecessary だった。

正解の形: **subscription は WebSocket connection そのもの**。client は
購読したい issue ごとに専用 WS を `wss://mcp.ippoan.org/issues/...` に
open する。auth-worker は issue ごとの `IssueRoomDO` でその WS を受ける
だけ。subscription registry も `pending_event:` buffer も要らない。webhook
が来たら該当 `IssueRoomDO` 内の全 WS に `ws.send()` で broadcast。

### Decision

#### 全体像

```
┌──────────────────────┐   POST /webhooks/github   ┌─────────────────────┐
│ github.com webhook   │ ─────────────────────────▶│   auth-worker       │
│ (issues, comments)   │   X-Hub-Signature-256     │   (Cloudflare       │
└──────────────────────┘                           │    Worker)          │
                                                   │ 1. HMAC verify      │
                                                   │ 2. parse owner/     │
                                                   │    repo/issue#      │
                                                   │ 3. idFromName(      │
                                                   │      `issue:<id>`)  │
                                                   └──────────┬──────────┘
                                                              │ DO RPC
                                                              ▼
                                              ┌───────────────────────────┐
                                              │   IssueRoomDO             │
                                              │   (per owner/repo#N)      │
                                              │   - hibernatable WS pool  │
                                              │   - tag: "subscriber"     │
                                              │   - NO storage            │
                                              │                           │
                                              │   ws.send(event) を       │
                                              │   getWebSockets()         │
                                              │     .forEach で broadcast │
                                              └────────┬──────────────────┘
                                                       │ WS event JSON
                  ┌────────────────────────────────────┼─────────────────────┐
                  │                                    │                     │
                  ▼                                    ▼                     ▼
       ┌─────────────────────┐         ┌─────────────────────┐    ... 他 session
       │  rust-mcp-agent A   │         │  rust-mcp-agent B   │
       │  (CCoW container 1) │         │  (CCoW container 2) │
       │                     │         │                     │
       │  MCP notif 発火     │         │  MCP notif 発火     │
       └─────────┬───────────┘         └─────────┬───────────┘
                 │                               │
                 ▼                               ▼
       ┌──────────────────┐            ┌──────────────────┐
       │  Claude session  │            │  Claude session  │
       └──────────────────┘            └──────────────────┘

[Subscribe flow]
  Claude session: subscribe_issue_activity(owner, repo, N)
    ↓ rust-mcp-agent
    ↓ wss://mcp.ippoan.org/issues/<owner>/<repo>/<N>/connect で新 WS open
    ↓ Authorization: Bearer <JWT>
  auth-worker GET /issues/:owner/:repo/:number/connect:
    ↓ JWT verify
    ↓ idFromName(`issue:<owner>/<repo>#<N>`) → IssueRoomDO stub
    ↓ DO に WS を accept させる
  IssueRoomDO:
    ↓ webSocketAccept(client, tags=["subscriber", <github_login>])
    ↓ WS そのものが subscription。storage は使わない。
  rust-mcp-agent:
    ↓ ~/.cc-relay/watched-issues.txt に "owner/repo#N" を append
    ↓ (復帰時の再 connect 用、subscription 状態の source of truth ではない)
    ↓ ToolResult を Claude session に返す
```

#### 設計の核

**subscription = WS connection 自体**。close した瞬間に unsubscribe
されるので、server 側に subscription state を持たなくていい。
`IssueRoomDO` は storage を一切使わず、hibernatable WS pool のみ。

#### Webhook receiver (`auth-worker`)

```
POST /webhooks/github  (host: mcp.ippoan.org / mcp-staging.ippoan.org)
Headers:
  X-Hub-Signature-256: sha256=<hmac>
  X-GitHub-Event:      issues | issue_comment
  X-GitHub-Delivery:   <uuid>
Body: GitHub event payload (JSON)
```

Handler の処理:

1. `env.GITHUB_WEBHOOK_SECRET` を鍵に HMAC-SHA256 検証。既存
   `lineworks-webhook.ts` の constant-time compare helper 流用。public
   issue でも spam 対策として残す。
2. `X-GitHub-Event` が `{issues, issue_comment}` 以外なら 200 ignore。
3. payload から `owner`, `repo`, `issue_number`, `event_type`,
   `delivery_id` を抽出。
4. `idFromName(`issue:${owner}/${repo}#${number}`)` で `IssueRoomDO`
   stub を取得。
5. `stub.fetch('/__push_event', { body: eventJson })` で push。
6. 200 を返す。**routing は単一 DO への単一 fetch**、fan-out logic 不要。

#### 新 DO: `IssueRoomDO`

`issue:${owner}/${repo}#${number}` を `idFromName` のキーにする。

**Storage は使わない**。hibernatable WebSocket のみ保持。空 instance に
なれば GC される (DO 標準動作)。

`fetch` ハンドラの route:

| Path | Method | 役割 |
|---|---|---|
| `/__connect` | GET (Upgrade: websocket) | client WS を accept、`["subscriber", github_login]` tag で登録 |
| `/__push_event` | POST | body の event JSON を全 WS に broadcast |

push handler の中身 (TypeScript 擬似):

```ts
async fetch(req: Request) {
  if (path === "/__push_event") {
    const event = await req.json();
    const wsList = this.ctx.getWebSockets("subscriber");
    const eventStr = JSON.stringify(event);
    for (const ws of wsList) {
      try { ws.send(eventStr); }
      catch { /* dead ws、無視 */ }
    }
    return new Response(JSON.stringify({ delivered: wsList.length }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }
  // /__connect の方は既存 mcp-relay-connect.ts と同形の Upgrade pattern
}
```

#### auth-worker route 追加

| Path | Method | Handler |
|---|---|---|
| `POST /webhooks/github` | POST | 新 `handlers/github-webhook.ts` |
| `GET /issues/:owner/:repo/:number/connect` | GET (Upgrade) | 新 `handlers/issue-room-connect.ts` |

既存 `dispatchMcpRelay` の host 判定 (`mcp.ippoan.org`) 内に分岐を追加。
JWT verify は既存 `mcp-jwt.ts` の helper を流用。

#### Event JSON (per-issue WS)

per-issue WS は **event delivery 専用**なので、ADR-003 の req/resp
envelope は必要なし。raw event JSON を投げ込む:

```json
{
  "v": 1,
  "event_type": "issue_comment.created",
  "delivery_id": "<github X-GitHub-Delivery>",
  "owner": "ippoan",
  "repo": "cc-relay",
  "issue_number": 42,
  "received_at": "2026-05-15T10:30:00Z",
  "payload": {
    "action": "created",
    "comment": {
      "id": 4458750334,
      "user": { "login": "yhonda-ohishi" },
      "body": "<4 KB で truncate>",
      "html_url": "https://github.com/ippoan/cc-relay/issues/42#issuecomment-..."
    },
    "issue": { "number": 42, "title": "...", "state": "open" }
  }
}
```

`v` は schema version、breaking change で bump。`payload` は **subset**:
notification preview に必要な field のみ。フルデータは
`api.github.com` で再取得可 (binary が GitHub token を持っている)。
frame size 目安: 1 配信 8 KB 未満。超えたら `body` / `labels[]` を
truncate。

#### cc-relay 側 MCP tools

`crates/agent-mcp/src/lib.rs` に追加:

```rust
#[tool(description = "Subscribe to GitHub issue activity. Opens a \
                      dedicated WebSocket to mcp.ippoan.org. Comments \
                      and label changes arrive as <github-webhook-activity> \
                      MCP notifications. Idempotent (再 subscribe で no-op)。")]
async fn subscribe_issue_activity(
    &self,
    owner: String,
    repo: String,
    issue_number: u64,
) -> ToolResult {
    // 1. 既存の watched-issues.txt に entry を append (重複は dedup)
    // 2. このプロセス内の WS pool に未登録なら、新 WS を
    //    wss://mcp.ippoan.org/issues/<owner>/<repo>/<N>/connect に open
    // 3. tokio task spawn: WS から event を読み、
    //    <github-webhook-activity> envelope で MCP notification を発火
}

#[tool(description = "Unsubscribe from GitHub issue activity.")]
async fn unsubscribe_issue_activity(
    &self,
    owner: String,
    repo: String,
    issue_number: u64,
) -> ToolResult {
    // 1. watched-issues.txt から該当 entry を削除
    // 2. WS pool から該当 WS を close
}
```

`~/.cc-relay/watched-issues.txt` の形式:

```
ippoan/cc-relay#42
ippoan/auth-worker#117
```

このファイルは **server-side state の source of truth ではない** —
server には WS 自体しか subscription 情報がない。ファイルは **process
再起動時に再 connect する宛先リスト** として使う。

#### Hibernation / 復帰

CCoW container が reclaim されると tokio task / WS は全部死ぬ。復帰時:

1. rust-mcp-agent が再起動
2. SessionStart hook 経路 (claude-md `session-start-install-hooks.sh`)
   で agent process が立ち上がる
3. agent が `watched-issues.txt` を read
4. 各 entry について:
   a. `wss://mcp.ippoan.org/issues/<owner>/<repo>/<N>/connect` に
      新 WS を open (auto re-subscribe)
   b. 既存 `CursorStore` (`~/.cc-relay/state-<slug>.json`) で
      last seen comment cursor を read
   c. `GitHubBroker` で issue comments を cursor 以降 fetch (catchup)
   d. 新 comment があれば MCP notification として session に発火
5. 以後は live WS event で real-time

server 側に pending event buffer を持たないので **取りこぼしは
CursorStore catchup が保証する**。重複検出は `delivery_id` ベース。

#### GitHub webhook 設定

repo ごとに:

- Settings → Webhooks → Add webhook
- Payload URL: `https://mcp.ippoan.org/webhooks/github`
- Content type: `application/json`
- Secret: `auth-worker` env `GITHUB_WEBHOOK_SECRET` (1 個共有)
- Events: **Issues** + **Issue comments** (個別選択)
- Active: ✓

repo 1 個に対して 1 回。Octokit 自動化スクリプト (`scripts/install-webhook.sh`)
は本 ADR 範囲外、follow-up。

### Consequences

#### 良くなる点

- **server-side subscription state ゼロ**。`IssueRoomDO` は WS pool
  だけ、storage を使わない。
- **subscription registry 不要**。WS connection そのものが
  subscription。close = unsubscribe、idempotent も自然 (open 済なら
  no-op)。
- **`pending_event:` buffer 不要**。disconnect 中の event は server
  側から消えるが、復帰時に CursorStore catchup で回収する設計。
- issue event の real-time push、end-to-end 遅延 1–2 秒目標。
- **CCoW container standby を生き残る**。WS は死ぬが復帰時に再 connect
  + cursor catchup で取りこぼし救出。
- 既存 `lineworks-webhook.ts` の HMAC 検証 helper、`McpSession` の
  hibernatable WS pattern、`CursorStore` を再利用。新規実装は薄い。
- **routing が DO 1 個に対する単一 fetch**。fan-out logic なし、
  per-target error handling 不要。

#### 悪化する点 / 未解決

- 新 DO class (`IssueRoomDO`) が 1 つ増える。ただし storage 0 で、
  既存 `McpSession` より単純。
- **WS connection 数が watched issue 数だけ並列に増える**。5–20 個
  程度なら問題なし、リソース消費は微小。100 を超えると Cloudflare
  Worker の WS quota との整合性を要確認。
- 新 repo の onboarding ごとに GitHub webhook 設定が必要。摩擦は
  `scripts/install-webhook.sh` (follow-up) で吸収予定。
- GitHub webhook retry は 3 回 / 約 30 分。`auth-worker` がそれ以上
  落ちると event 取りこぼし → CursorStore catchup で復帰時に救う。
- ADR-003 の単一 WS relay と並列に **issue 専用 WS** を増やす形に
  なるので、ADR-003 の純粋な「1 session 1 WS」モデルからは外れる。
  ADR-003 とは独立した別 channel として扱う。

### Alternatives considered (rejected)

| Alternative | 却下理由 |
|---|---|
| `rust-mcp-agent` 内 in-process polling | CCoW standby で死ぬ。外部 wake signal なしには解けない。 |
| PR-per-task (`subscribe_pr_activity` 流用) | 動作するが empty PR が貯まる。繋ぎとして OK、long-term の形ではない。 |
| `McpSession` (per-user) DO + 別 `IssueSubsDO` (per-issue) | server-side subscription registry + `pending_event:` buffer + TTL/GC の設計コストが高い。WS 自体を subscription にすれば全部消える (本 ADR の選択)。 |
| KV reverse index `(owner/repo#N) → [github_login...]` | eventual consistency + KV cost。DO `idFromName` が strongly consistent + 無料同等。 |
| 既存 MCP relay WS の上で `kind: "event"` frame を multiplex | 設計可能だが ADR-003 を変更する必要があり、subscription state も結局 server に持つ羽目に。per-issue WS なら ADR-003 を一切触らない。 |

### Validation (Status: Accepted に flip する前に通すこと)

1. `POST /webhooks/github` に valid signature → 200、payload parse、
   該当 `IssueRoomDO` の `/__push_event` が呼ばれる (mock delivery で
   unit test)。
2. `POST /webhooks/github` に invalid signature → 401、DO call なし。
3. `IssueRoomDO`:
   a. `/__connect` で WS accept、hibernatable 状態で保持される。
   b. `/__push_event` 受信で全 attached WS に broadcast。
   c. WS が close → DO が GC 対象になる。
4. 復帰 catchup:
   a. WS open → subscribe → comment 投稿 → event 受信。
   b. WS 強制 close → 5 秒間に comment 投稿 → 再 open + CursorStore
      catchup で missed comment が拾える。
5. **staging end-to-end 受け入れテスト** (`ippoan/cc-relay#<test-issue>`):
   - rust-mcp-agent session が `subscribe_issue_activity` を call →
     `wss://mcp-staging.ippoan.org/issues/.../connect` が確立。
   - operator が browser から comment 投稿。
   - 5 秒以内に binary 側で event JSON 到着。
   - Claude session が `<github-webhook-activity>` notification を受信。
6. hibernation test:
   - subscribe → CCoW session 閉じる → container reclaim 待ち → comment
     投稿 → session 再開 → SessionStart hook 経由で agent 再起動 →
     `watched-issues.txt` から再 connect → CursorStore catchup で
     missed comment が `<github-webhook-activity>` notification として
     surface。

### Phase mapping

| Phase | Repo | Scope |
|-------|------|-------|
| **A** | `cc-relay`    | 本 ADR merge (本 PR)。 |
| **B** | `auth-worker` | `POST /webhooks/github` route + HMAC verify、`IssueRoomDO` class + `/__connect` + `/__push_event`、`GET /issues/:owner/:repo/:number/connect` route。新 repo に webhook 設定して GitHub からの POST と DO の broadcast を end-to-end で確認。 |
| **C** | `cc-relay`    | `subscribe_issue_activity` / `unsubscribe_issue_activity` MCP tool 追加。WS pool 管理、`watched-issues.txt`、tokio task で event → MCP notification 変換、`<github-webhook-activity>` envelope。 |
| **D** | `cc-relay`    | SessionStart hook 経路で `watched-issues.txt` から再 connect + CursorStore catchup を実装。hibernation test pass。 |
| **E** | `ippoan/cc-relay` (config) | repo webhook を設定 (1 回)。検証用 issue を作って §Validation step 5/6 を実施。 |
| **F** | both          | ADR-004 Status を Accepted に flip。`docs/agent-task-protocol.md` cookbook 追加。 |


