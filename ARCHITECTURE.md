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

## ADR-004: GitHub issue activity webhooks via existing MCP relay WS (multiplex)

**Status:** Accepted (2026-05-15)。「みて」手動 polling を廃止する。
Phase A–F まで実機検証済 (#44 / #47 / #138 / #46)。後続の Phase D 強化
(SSE notification back-pipe) は #47 で merged。

> **本 ADR は 2 度書き直されている。最終形は「既存 MCP WS で multiplex
> + client side filter」**。経緯は #Context 末尾と
> #Alternatives considered (rejected) を参照。

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

#### 設計の変遷

本 ADR は 2 回 reject されて 3 度目に着地した。経過を残す:

1. **旧案 1: `IssueSubsDO` (per-issue subscription registry) + `McpSession.push_event`**
   server 側に subscription state を持つ。registry の TTL / GC / replay
   buffer の設計コストが高い。「subscription を server に登録する」発想
   自体が unnecessary と判明、reject。
2. **旧案 2: per-issue WS endpoint (`GET /issues/<owner>/<repo>/<N>/connect`)
   + `IssueRoomDO`**
   subscription = WS connection 自体。server-side state ゼロは達成したが、
   **MCP の慣習 (1 server = 1 endpoint) から外れる**。client は subscribe
   ごとに別 WS を張る形で、auth/接続管理コストが分散。reject。
3. **本案: 既存 MCP relay WS の上で event を multiplex (本 ADR)**
   webhook 到着時、auth-worker は **既存 `McpSession` DO (per github_login)
   の `/__push_event`** に push、attached `client` WS 全部に
   `{kind:"event", ...}` frame を broadcast。binary 側はそれを既存の
   frame dispatcher (`agent-mcp/src/relay.rs`) で受けて
   `~/.cc-relay/watched-issues.txt` で filter。MCP は 1 endpoint のまま、
   server-side subscription registry も要らない。

#### このプロセスで却下した別案

- **`rust-mcp-agent` 内 in-process polling。** authenticated polling
  なら rate-limit は問題にならず、container が hot な間は機能する。
  しかし CCoW container は数時間無動作で reclaim される。tokio task は
  死に、reclaim 中に発生した event は再 wake 時に拾えない。**hibernate
  しやすい container の中から wake を解く方法は構造的に存在しない**
  — 外部からの配信が必須。
- **PR-per-task (`subscribe_pr_activity` 利用)。** インフラ追加ゼロで
  今日動くが、コード変更のない task のために empty PR が貯まる。
  繋ぎとしてはありだが、「GitHub Issue を broker にした agent 間
  メッセージ relay」という cc-relay 本来の形ではない。

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
                                                   │      owner.login)   │
                                                   └──────────┬──────────┘
                                                              │ DO RPC
                                                              ▼
                                              ┌───────────────────────────┐
                                              │   既存 McpSession DO       │
                                              │   (per github_login)      │
                                              │   - hibernatable WS pool  │
                                              │   - tag: "client"         │
                                              │   - req/resp 用 + event 用 │
                                              │     を 1 本で multiplex     │
                                              │                           │
                                              │   /__push_event POST →    │
                                              │   ws.send({kind:"event",  │
                                              │            v:1, ...})     │
                                              │   を attached WS 全部に    │
                                              │   broadcast               │
                                              └────────┬──────────────────┘
                                                       │ 既存 MCP WS frame
                                                       ▼
                                              ┌───────────────────────────┐
                                              │   rust-mcp-agent          │
                                              │   (relay mode)            │
                                              │                           │
                                              │   run() loop で frame の  │
                                              │   kind を peek →           │
                                              │   "event" なら             │
                                              │   handle_event_frame:     │
                                              │     - watched-issues.txt  │
                                              │       で filter            │
                                              │     - 一致なら             │
                                              │       issue-events.jsonl  │
                                              │       に append            │
                                              │   "req" なら既存 JSON-RPC │
                                              │   dispatch                │
                                              └────────┬──────────────────┘
                                                       │ pull (next tool call)
                                                       ▼
                                              ┌───────────────────────────┐
                                              │   Claude session          │
                                              │                           │
                                              │   get_issue_events tool で│
                                              │   buffered events を drain│
                                              └───────────────────────────┘

[Subscribe flow] — server 通信なし、純粋に file 操作
  Claude session: subscribe_issue_activity(owner, repo, N)
    ↓ rust-mcp-agent (relay mode)
    ↓ RelayServer::tool_subscribe_issue
    ↓ WatchedIssuesFile::add(IssueKey) で
    ↓   ~/.cc-relay/watched-issues.txt に "owner/repo#N" を append
    ↓   (dedup 済、既存 entry なら no-op)
    ↓ ToolResult: "subscribed: owner/repo#N" を返す

[Event flow] — 受信側
  GitHub webhook → /webhooks/github (auth-worker)
    → HMAC verify、payload parse
    → McpSession.idFromName(repository.owner.login) で routing
    → /__push_event POST (event JSON)
  McpSession DO:
    → ws.send(`{"kind":"event",...event_body}`) を attached WS 全部に broadcast
  rust-mcp-agent:
    → 既存 WS で受信 → frame の kind を peek → "event" を検出
    → handle_event_frame: watched-issues.txt と照合 → 一致なら
      issue-events.jsonl に append
  Claude session:
    → 次の tool call で get_issue_events を呼ぶ → drain
```

#### 設計の核

- **MCP 1 endpoint を維持**。webhook event は既存 `McpSession` WS で
  multiplex (`kind:"event"` frame 1 種を増やすだけ)。
- **subscription registry は server 側に無し**。`watched-issues.txt` は
  client 側 file。filter も client 側。auth-worker は github_login 単位
  で event を broadcast するだけ、subscriber を知らない。
- **MCP relay の req/resp 経路は変更なし**。`McpSession` DO に 1 route
  (`/__push_event`) と binary 側に 1 match arm (`kind == "event"`) を
  足すだけで実装が完結する。
- **真の wake-up は別途**。本 ADR の流れだけだと event が
  `issue-events.jsonl` に積まれる pull 型(Claude session が
  `get_issue_events` を能動的に呼んで初めて見える)。Anthropic Claude.ai
  / Claude Code Web が auth-worker `/mcp` HTTP bridge 経由で接続する
  現アーキテクチャでは、server-initiated MCP notification を流す
  back-channel が無いため。streamable HTTP / SSE 対応は Phase D 以降。

#### Webhook receiver (`auth-worker`)

```
POST /webhooks/github  (host: mcp.ippoan.org / mcp-staging.ippoan.org)
Headers:
  X-Hub-Signature-256: sha256=<hmac>
  X-GitHub-Event:      issues | issue_comment
  X-GitHub-Delivery:   <uuid>
Body: GitHub event payload (JSON)
```

Handler の処理 (`src/handlers/github-webhook.ts`):

1. `env.GITHUB_WEBHOOK_SECRET` を鍵に HMAC-SHA256 検証。public issue
   前提なので spam 対策、authentication ではない。
2. `X-GitHub-Event` が `{issues, issue_comment}` 以外なら 200 ignore。
3. payload から `owner` (= `repository.owner.login`), `repo`,
   `issue_number`, `event_type`, `delivery_id` を抽出。
4. **`MCP_SESSION_DO.idFromName(owner)` で既存 McpSession DO stub** を
   取得。`payload.repository.owner.login` を routing key として使うので
   個人 repo (`ippoan/cc-relay`) なら自然に解決。organization repo
   対応は後追い (KV / D1 で github_login mapping)。
5. `stub.fetch('/__push_event', { body: eventJson })` で push。
6. 200 を返す。

#### `McpSession` DO への追加

既存の `McpSession` (per `github_login`) に 1 route 追加:

| Path | Method | Body |
|---|---|---|
| `/__push_event` | POST | `{event_type, delivery_id, owner, repo, issue_number, received_at, payload}` (raw event JSON) |

Handler は以下を実行 (`src/durable_objects/mcp-session-do.ts:handlePushEvent`):

1. `getWebSockets("client")` で attached WS を全列挙。
2. body を JSON parse、`{kind:"event", v:1, ...event_body}` で wrap。
3. 各 WS に `ws.send(JSON.stringify(frame))`、失敗時は dead として
   carry on。
4. レスポンス: `{delivered, dead, total}`。

新 DO class は **作らない**。`IssueSubsDO` / `IssueRoomDO` 系の
別 namespace は不要。

#### Event frame schema

既存 `req`/`resp` と同じ envelope。新 `kind:"event"` variant:

```json
{
  "kind": "event",
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
`api.github.com` で再取得可。frame size 目安: 1 配信 8 KB 未満。

#### cc-relay 側 MCP tools

`crates/agent-mcp/src/relay.rs` の `RelayServer` が以下を expose:

| Tool | 動作 | Server 通信 |
|---|---|---|
| `subscribe_issue_activity(owner, repo, issue_number)` | `~/.cc-relay/watched-issues.txt` に append (dedup) | ❌ |
| `unsubscribe_issue_activity(...)` | `~/.cc-relay/watched-issues.txt` から remove | ❌ |
| `get_issue_events()` | `~/.cc-relay/issue-events.jsonl` を drain (read + rename `.read`) | ❌ |
| `list_watched_issues()` | 現在 subscribe 中の set を JSON array で返す | ❌ |

すべて file 操作のみ。auth-worker への RPC は走らせない (subscription
を server に伝える必要なし)。

`~/.cc-relay/watched-issues.txt` の形式:

```
ippoan/cc-relay#42
ippoan/auth-worker#117
```

行頭 `#` のコメント、空行は無視。

#### Event frame の受信側 (`agent-mcp/src/relay.rs`)

既存の WS receive loop で:

1. 受信 text frame を `serde_json::Value` として parse。
2. `kind` field を peek。
3. `"event"` なら `RelayServer::handle_event_frame` を呼ぶ:
   - `owner` / `repo` / `issue_number` を抽出。
   - `WatchedIssuesFile::load()` で watched set を取得。
   - set に含まれるなら `IssueEventsFile::append_event` で
     `~/.cc-relay/issue-events.jsonl` に append。
   - 含まれないなら debug log だけ出して drop。
4. `"req"` (既存) なら従来の JSON-RPC dispatch を実行。
5. `"resp"` / `"hello"` / unknown は無視。

#### Hibernation / 復帰

CCoW container が reclaim されると tokio task / WS は全部死ぬ。復帰時:

1. rust-mcp-agent が再起動 (SessionStart hook 経由)
2. agent が既存の relay WS を再 open (ADR-003 経路、token cache + auth)
3. 既存の `~/.cc-relay/watched-issues.txt` は永続なのでそのまま filter
   に使われる。
4. disconnect 中の event は server-side buffer なし → **取りこぼし**
   が起きる。
5. **取りこぼしの救済**は `~/.cc-relay/state-<slug>.json` (既存
   `CursorStore`) で各 watched issue の last seen comment cursor を
   load、`GitHubBroker` で cursor 以降を fetch して
   `issue-events.jsonl` に流す follow-up task で行う。
6. 以後は live event で動く。

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

- **MCP の慣習 (1 server = 1 endpoint) に準拠**。client は 1 本の WS で
  全てをやりとり、別 endpoint を増やさない。
- **server-side subscription registry ゼロ**。`McpSession` は元から
  存在する DO で、event broadcast のためだけに `/__push_event` 1 route
  を増やすのみ。新 DO class 追加なし。
- **subscription state は client-side file のみ**。`watched-issues.txt`
  が真理。auth-worker は誰が何を subscribe しているか知らない。
- **routing が DO 1 個に対する単一 fetch**。`payload.repository.owner.login`
  → `MCP_SESSION_DO.idFromName(login)` で完結。fan-out logic 不要。
- 既存 `lineworks-webhook.ts` の HMAC 検証 helper を流用。`McpSession`
  の hibernatable WS pattern と frame envelope (`req`/`resp`) もそのまま
  使う。新規実装は薄い (auth-worker 側で `/__push_event` 1 route +
  webhook handler 1 個、binary 側で `kind:"event"` arm 1 個 + tool 4 個)。

#### 悪化する点 / 未解決

- **server-initiated MCP notification は流せない**。Claude Code Web /
  Anthropic Claude.ai は `POST /mcp` HTTP bridge 経由なので、server
  からの push channel が無い。binary が受けた event は
  `~/.cc-relay/issue-events.jsonl` に積むだけで、Claude session が
  `get_issue_events` を能動的に呼ばないと見えない (pull 型)。
  → 真の wake-up には auth-worker `/mcp` を Streamable HTTP / SSE に
    対応させる必要があり、本 ADR の scope 外。Phase D 以降。
- 新 repo の onboarding ごとに GitHub webhook 設定が必要。摩擦は
  `scripts/install-webhook.sh` (follow-up) で吸収予定。
- GitHub webhook retry は 3 回 / 約 30 分。`auth-worker` がそれ以上
  落ちると event 取りこぼし → CursorStore catchup で復帰時に救う
  (follow-up task)。
- routing key が `repository.owner.login` 固定なので、organization repo
  でメンバー全員に届けたいケース等は本 ADR の範囲外。github_login
  mapping を別途持つ後追いが必要。

### Alternatives considered (rejected)

| Alternative | 却下理由 |
|---|---|
| `rust-mcp-agent` 内 in-process polling | CCoW standby で死ぬ。外部 wake signal なしには解けない。 |
| PR-per-task (`subscribe_pr_activity` 流用) | 動作するが empty PR が貯まる。繋ぎとして OK、long-term の形ではない。 |
| **旧案 1**: `McpSession` (per-user) DO + 別 `IssueSubsDO` (per-issue) で server-side subscription registry を持つ | registry の TTL / GC / replay buffer 設計コストが高い。「subscription を server に登録する」発想自体が unnecessary と判明。 |
| **旧案 2**: per-issue WS endpoint (`GET /issues/<owner>/<repo>/<N>/connect`) + `IssueRoomDO` | MCP は通常 1 endpoint。issue ごとに別 WS を張る形は慣習から外れ、auth/接続管理コストが分散。**本 ADR の最終形は既存 MCP relay WS を multiplex する形に再修正**。 |
| KV reverse index `(owner/repo#N) → [github_login...]` | eventual consistency + KV cost。DO `idFromName` が strongly consistent + 無料同等。本 ADR では reverse index 自体が不要 (broadcast + client filter)。 |

### Validation (Status: Accepted に flip する前に通すこと)

1. `POST /webhooks/github` に valid signature → 200、payload parse、
   該当 `McpSession` の `/__push_event` が呼ばれる (mock delivery で
   unit test)。
2. `POST /webhooks/github` に invalid signature → 401、DO call なし。
3. `McpSession.handlePushEvent`:
   a. body が valid JSON → 全 attached `client` WS に
      `{kind:"event", v:1, ...event_body}` frame を broadcast。
   b. attached WS なし → 200 with `delivered: 0`。
   c. WS が dead → broadcast 継続 + dead count を返す。
4. cc-relay binary 側:
   a. `subscribe_issue_activity(o, r, n)` で
      `~/.cc-relay/watched-issues.txt` に entry が append される。
   b. 同じ args で 2 回呼ぶと `"already subscribed"` を返す
      (idempotent)。
   c. `unsubscribe_issue_activity(o, r, n)` で entry が消える。
   d. `kind:"event"` frame 受信 → watched set に含まれれば
      `~/.cc-relay/issue-events.jsonl` に append、含まれなければ drop。
   e. `get_issue_events` で drain (read + rename `.read`)、2 回目は空。
5. **staging end-to-end 受け入れテスト** (`ippoan/cc-relay#<test-issue>`):
   - rust-mcp-agent session が `subscribe_issue_activity` を call。
   - operator が browser から comment 投稿。
   - 5 秒以内に binary 側で `~/.cc-relay/issue-events.jsonl` に entry。
   - Claude session が `get_issue_events` を呼ぶと event が drain
     される。

### Phase mapping (実績)

| Phase | Repo | Scope | 状態 |
|---|---|---|---|
| **A** | `cc-relay` | 本 ADR の amend (multiplex 設計に最終形) | 本 PR |
| **B** | `auth-worker` | `POST /webhooks/github` route + HMAC verify、`McpSession.handlePushEvent` route 追加、`payload.repository.owner.login` routing。 | ✅ #138 merged (#137 の per-issue WS 案を rollback したもの) |
| **C** | `cc-relay` | `subscribe_issue_activity` / `unsubscribe_issue_activity` / `get_issue_events` / `list_watched_issues` MCP tool 追加。`watched-issues.txt` + `issue-events.jsonl` の file ops。frame dispatcher に `kind:"event"` arm。 | ✅ #44 merged |
| **D** | both | (real-time wake-up) auth-worker `/mcp` の Streamable HTTP / SSE 対応、binary 側で event 受信時に MCP `notifications/message` で Claude session へ push。 | ✅ #47 merged |
| **E** | `cc-relay` | SessionStart 復帰 + CursorStore catchup (取りこぼし救済) | ✅ ADR-006 経路 (auth-worker#140) に統合、cc-relay 側は claude-hooks#9 で SessionStart hook が drain instruction を inject |
| **F** | `ippoan/cc-relay` (config) | repo webhook を設定 (1 回)。検証用 issue を作って §Validation step 5 を実施。 | ✅ #46 close 時に staging E2E 確認済 |
| **G** | both | ADR-004 Status を Accepted に flip。`ARCHITECTURE.md` に ADR-005 / ADR-006 / CCoW cookbook 追加 (本コミット #49)。 | ✅ #49 |



---

## ADR-005: Claude Code Channel — stdio MCP server で session に push する出力経路

**Status:** Accepted (2026-05-15)。`crates/agent-mcp/src/channel.rs` で実装、
`rust-mcp-agent channel` subcommand として shipped (PR #48)。

### Context

ADR-004 で webhook event を WS frame として cc-relay binary に届ける経路は
できた。問題は **その先 — binary が Claude session に対して event を
push する手段** がない点だった。

`rust-mcp-agent relay` mode は Streamable HTTP over WS のリクエスト/レスポンス
(`Frame::Req` / `Frame::Resp`) しか喋らないので、server-initiated な
notification を session に届ける口が無い。`get_issue_events` で操作者が
能動的に drain する形は動くが、

- 受動 channel が無いので Claude が「webhook が来た」事実を turn 内で
  知る手段が無い
- 結局 operator が「みて」と打つ polling 形になり、ADR-004 が目指した
  自動化が完成しない

Claude Code は [Channels Reference] で **`notifications/claude/channel`
JSON-RPC notification を stdio MCP server から受けると `<channel source=...>`
envelope として user turn に inject する** 機構を提供している
([code.claude.com/docs/en/channels-reference][Channels Reference])。
これを使えば binary → Claude session の push が成立する。

[Channels Reference]: https://code.claude.com/docs/en/channels-reference

### Decision

`rust-mcp-agent channel` mode を追加 (`crates/agent-mcp/src/channel.rs`):

1. **Transport**: stdio (`spawn` される subprocess、Claude Code 標準)
2. **Capability**: `initialize` response に
   `capabilities.experimental["claude/channel"] = {}` を返し、Claude Code
   側に「このサーバーは channel 経路を持つ」と申告
3. **Inbound (req/resp)**: 通常の MCP `tools/list` / `tools/call` を stdio
   越しに受ける (relay mode と同じ `RelayServer` dispatcher を共有)
4. **Outbound (event push)**:
   - 同時に **auth-worker への outbound WS を 1 本張る** (relay mode と同じ
     wire / 同じ JWT)
   - `kind:"event"` frame を受信したら、`notifications/claude/channel` の
     JSON-RPC notification を 1 行で stdout に書く
   - Claude Code がそれを line-by-line に読み取り、`<channel source="cc-relay"
     issue_number="..." delivery_id="...">payload</channel>` 形式で
     **次の user turn に inject** する
5. **stdout race 防止**: JSON-RPC response (tools/call の返り値) と
   notification が同じ stdout に出るので、`mpsc::UnboundedSender<String>` で
   1 本の writer task に集約する

### Consequences

- ✅ session が hot な間は **operator 操作なしで** webhook event が
  Claude turn に流れる (ADR-004 が目指した自動化の完成)
- ✅ relay mode と server side (auth-worker DO) を共有しているので
  auth-worker 側に追加実装不要
- ⚠️ stdio subprocess は Claude Code が host である必要 — Claude.ai
  (web) や Claude Code on the Web (CCoW) のような connector 経路では
  使えない (`POST /mcp` しか喋らない)。CCoW では ADR-006 経路を使う
- ⚠️ subprocess が落ちると channel が切れる。再起動は Claude Code が
  MCP server 設定に従って自動 spawn する
- ⚠️ 旧 ADR-005 案 (`notifications/claude/channel` を CCoW connector が
  受ける) は **存在しない** — connector はこの notification を session に
  届けない。ADR-005 は **stdio 限定**であることを明記する

### Validation

- `agent-mcp/tests/channel_integration.rs` (or stub) で
  `initialize` 応答に `experimental["claude/channel"]` が乗ること
- mock WS から `event` frame 投げて stdout に
  `notifications/claude/channel` が出ることを文字列マッチで確認
- staging で実機: Claude Code (CLI) から `rust-mcp-agent channel` を
  起動 → GitHub webhook 発火 → 次 user turn の context に `<channel
  source="cc-relay" ...>` が現れること

### Phase mapping

| Phase | Scope | 状態 |
|---|---|---|
| **A** | stdio + outbound WS + channel capability + notification emit | ✅ #48 merged |
| **B** | meta filter (subscription) + de-dup by `delivery_id` | TODO |
| **C** | reconnect / JWT refresh tightening | TODO |


---

## ADR-006: Server-side subscription + CCoW polling drain

**Status:** Accepted (2026-05-15)。`auth-worker/src/durable_objects/mcp-session-do.ts`
で実装 (auth-worker#140)、organization repo mapping は auth-worker#141 で
追加、staging E2E 実機検証済 (cc-relay#46 close 時点)。

### Context

ADR-004 (binary 経路) + ADR-005 (Claude Code stdio channel) は
**binary subprocess が起動できる環境** で動く。だが Claude Code on the Web
(CCoW) は

- subprocess を spawn できない (sandbox)
- MCP は `POST /mcp` (Streamable HTTP) connector 経由のみ
- session container は inactivity で hibernate / 再 spawn される — 動いて
  いない時間が webhook 到着と被ると event を取りこぼす

つまり CCoW で ADR-004 を使いたければ、**server 側に event を貯めて
session が次に turn を回した時に drain する経路**が要る。

### Decision

`McpSession` Durable Object に **subscription set + event queue** を持たせ、
inline stub MCP server に 4 つの tool を追加する:

| Tool | 動作 |
|---|---|
| `subscribe_issue_activity(owner, repo, issue_number)` | DO storage `subs` set に `owner/repo#N` を append (idempotent) |
| `unsubscribe_issue_activity(owner, repo, issue_number)` | `subs` set から remove |
| `list_watched_issues()` | 現在の `subs` set を返す |
| `get_pending_events()` | `events` queue を drain (read + clear)、event の配列を **JSON 文字列** で返す |

webhook handler 側 (`handlePushEvent`) は ADR-004 の WS broadcast と並行して
**subscription filter を通った event を `events` queue に append** する
(`queueEventIfSubscribed`)。queue は FIFO、上限 `MAX_QUEUED_EVENTS = 500`
で drop-oldest policy。

```
POST /webhooks/github
        │
        ▼
[handleGithubWebhook]
        │ X-Hub-Signature-256 verify
        │ owner mapping (gh_org:<owner> → github_login)
        ▼
[McpSession DO #idFromName(github_login)]
        ├─▶ /__push_event → WS broadcast (ADR-004 経路)
        ├─▶ SSE channel push (ADR-004 Phase D)
        └─▶ queueEventIfSubscribed (本 ADR)
                  │ if subs has owner/repo#N:
                  │   events.push(eventBody)
                  │   while events.length > 500: events.shift()
                  └─ DO storage put "events"

CCoW session next turn:
   tools/call get_pending_events → DO returns JSON.stringify(events)
   → DO clears storage.events
   → Claude が JSON.parse して payload を読む
```

#### organization repo の routing

個人 repo は webhook の `payload.repository.owner.login == github_login` で
自然に DO に届く。organization repo は owner = org 名で github_login と
ズレるため、`AUTH_CONFIG` KV に `gh_org:<org> = <github_login>` mapping を
持って resolve する (auth-worker#141):

```bash
# 例: ippoan org のすべての webhook を yhonda-ohishi の DO に送る
wrangler kv key put --remote --binding=AUTH_CONFIG --env staging \
  gh_org:ippoan yhonda-ohishi
```

mapping が無ければ owner をそのまま使う (= 既存挙動)。

### Consequences

- ✅ CCoW でも webhook event が拾える (binary なし)
- ✅ ADR-004 binary 経路と共存 — 同じ event が両経路に流れて Claude 側で
  `delivery_id` de-dup する想定。drop-oldest cap 500 は通常運用で十分
- ⚠️ `get_pending_events` の返り値は **MCP `content` 1 個の JSON 文字列**
  で配列ではない (MCP spec の制約)。消費側は `JSON.parse` してから配列を
  扱う必要がある
- ⚠️ at-most-once delivery — drain 中に DO が落ちると event lost。次 ADR で
  cursor-based replay にする余地あり (現状は drop で割り切り)
- ⚠️ queue 上限 500 を超える長期 hibernation では event 欠落する。
  `delivery_id` の連番 gap で操作者が検知して GitHub Webhook Replay で
  再送する運用
- ⚠️ subscription state は DO 単位 (per github_login)。同じ user が複数
  CCoW session を持つと subscribe/unsubscribe が共有される — 通常は意図通り

### Validation

- `auth-worker/test/durable_objects/mcp-session-do.test.ts` の
  `"McpSession inline stub — ADR-006 server-side tools"` describe block
  が 5 tools (`ping` + 4 ADR-006 tools) を網羅
- `test/handlers/github-webhook.test.ts` の `"ADR-006: routes org-owned
  repo to mapped github_login"` 等で org mapping 動作を確認
- staging 実機 (cc-relay#46 close 時点):
  - CCoW session が `subscribe_issue_activity(ippoan, cc-relay, 50)` 発火
  - browser から #50 にコメント投稿
  - 次 turn で `get_pending_events()` 呼ぶ → 配列に 1 event、`delivery_id`
    も entity body も揃って取得
  - 2 度目の `get_pending_events()` は空配列 (drain after read)

### Phase mapping

| Phase | Repo | Scope | 状態 |
|---|---|---|---|
| **A** | `auth-worker` | DO storage subs/events、4 tools、handlePushEvent から queue 連携、上限 500 drop-oldest | ✅ auth-worker#140 |
| **B** | `auth-worker` | org repo mapping (`gh_org:<owner>` KV lookup) | ✅ auth-worker#141 |
| **C** | docs (this ADR) | ARCHITECTURE.md に文書化、CCoW cookbook 追加 | ✅ #49 (本コミット) |
| **D** | (future) | cursor-based replay (at-least-once)、subscription per-session 化 | TODO |


---

## CCoW cookbook: webhook event を Claude session に流す (ADR-006 経路)

CCoW (Claude Code on the Web) で GitHub webhook event を消費する手順。

binary subprocess が動かない環境で、**ADR-006 server-side queue だけで**
完結する最小レシピ。

### 1. MCP server を attach する

CCoW プロジェクトの `.mcp.json` (or `~/.claude/mcp_servers.json`) で
auth-worker の `/u/<login>/mcp` を `streamable-http` transport で attach:

```jsonc
{
  "mcpServers": {
    "cc-relay": {
      "type": "streamable-http",
      "url": "https://mcp-staging.ippoan.org/u/yhonda-ohishi/mcp",
      "headers": {
        "Authorization": "Bearer <MCP_JWT>"
      }
    }
  }
}
```

JWT は `rust-mcp-agent auth` で 1 度発行 (`~/.cc-relay/token`)。`<login>`
は GitHub login。

### 2. organization repo の場合 — `gh_org` mapping を登録

ippoan/cc-relay のように owner が org の repo を subscribe する場合、
webhook の routing 先 (owner) と JWT 所有者 (github_login) が一致しない。
mapping を 1 度だけ KV に書く:

```bash
wrangler kv key put --remote --binding=AUTH_CONFIG --env staging \
  gh_org:ippoan yhonda-ohishi
```

これで `ippoan/*` の webhook は `yhonda-ohishi` の `McpSession` DO に
routing される。

### 3. subscribe → コメント投稿 → drain

Claude session 内で:

```text
mcp__cc_relay__subscribe_issue_activity(owner="ippoan", repo="cc-relay", issue_number=50)
```

別ブラウザ tab から該当 issue にコメントを投稿。次の turn 開始時に
Claude が:

```text
mcp__cc_relay__get_pending_events()
```

を call すると DO の queue が drain される。返り値は **JSON 文字列の配列**
(MCP `content` 1 個):

```json
"[{\"event_type\":\"issue_comment.created\",\"delivery_id\":\"abc-123\",\"owner\":\"ippoan\",\"repo\":\"cc-relay\",\"issue_number\":50,\"received_at\":\"2026-05-15T16:00:00Z\",\"payload\":{...}}]"
```

→ `JSON.parse` して配列にしてから `event_type` / `payload.comment.body`
等を読む。

### 4. SessionStart hook と組み合わせる (推奨)

毎セッション開始時に自動 drain したい場合は
[yhonda-ohishi/claude-hooks#9](https://github.com/yhonda-ohishi/claude-hooks/pull/9)
の `session-start-cc-relay-wss.sh` + `user-prompt-submit-cc-relay-events.sh`
を使う:

- SessionStart で `additionalContext` に「`list_watched_issues` →
  `get_pending_events` を呼んで」という指示を inject
- `delivery_id` を `~/.cc-relay/seen-deliveries.json` (24h TTL) で記録、
  WS probe 経路と queue drain 経路の重複を排除

### 5. Troubleshooting

| 症状 | 原因 / 対処 |
|---|---|
| `get_pending_events` が常に空配列 | (a) subscription 不在 — `list_watched_issues` で確認 (b) webhook が auth-worker まで届いていない — repo webhook delivery 画面で `200` を確認 (c) org mapping 未登録 — Step 2 |
| 返り値が配列でなく string | 正常。MCP `content` の制約で string 1 個。`JSON.parse` してから扱う |
| event が大量に貯まって古いものが消える | drop-oldest cap 500 を踏んだ。GitHub Webhook Replay で再送 (delivery_id 連番 gap で検出) |
| `delivery_id` 重複 event | binary 経路 (ADR-004) と queue 経路 (本 ADR) で同じ event が届く設計。`delivery_id` で de-dup する |


---

## WSS `/connect` の用途と CCoW から見た制約 (cc-relay#50)

`wss://mcp(-staging).ippoan.org/[u/<login>/]connect` は **binary 専用** の
outbound WebSocket endpoint。CCoW Claude session **自身がこの WS を喋る
ことはない**。

### wire format

`auth-worker/src/durable_objects/mcp-session-do.ts handleConnect`:

- 認証: MCP Bearer JWT (`Authorization` header on Upgrade)
- accept 後の wire は **MCP protocol ではなく cc-relay 独自の Frame v1**:
  `{kind:"req"|"resp"|"event"|"notif"|"hello", v:1, ...}` (base64 body)
- 元々は `rust-mcp-agent` binary が outbound に張る前提。Claude.ai /
  CCoW connector はこの protocol を喋らない (し、binary を CCoW で
  起動する手段も無い)

### CCoW container と WSS

実測 (2026-05-15): CCoW container の egress は **WSS は通す** ことが
判明している:

- `wss://mcp-staging.ippoan.org/u/<owner>/connect` への HTTP/1.1 Upgrade
  試行で `401 Unauthorized` (auth 要求) まで進む → エッジ到達 + worker 応答
- 旧 PoC の「WSS /connect 403」は古い allowlist (現在は閉じてない)

つまり「WS が通る」事と「polling を撤廃できる」事の間には、まだ
**transport の用途** という gap がある。WSS が通ることだけでは Claude
session を wake させる経路にはならない。

### 3 つの path と現状

| Path | Status | 用途 |
|---|---|---|
| **A. Binary outbound WS** (binary `relay` / `channel`) | ✅ Shipped (ADR-004, ADR-005) | binary を起動できる環境 (CLI / 一部 hook) — event を frame で受けて MCP notification に変換 |
| **B. CCoW から WSS probe を立てる** (claude-hooks#9 の `session-start-cc-relay-wss.sh`) | ✅ Shipped — Phase A PoC | CCoW container 内で `rust-mcp-agent probe` を background launch し、frame を JSONL log に append。UserPromptSubmit hook で差分を session に inject。**turn 内**の long-poll を解消するが、**hibernation 中**の event は queue drain (Path C) が必要 |
| **C. ADR-006 polling drain** | ✅ Shipped (本 ADR) | CCoW session resume 時の取りこぼし救済。binary 不要 |

### CCoW から見た制約

1. **Claude session を外部 event で wake する経路は存在しない**。Claude
   session は user message か tool result からのみ turn を開始する。WSS
   が通っても server-initiated push を session に届ける路が無い。
2. CCoW container は hibernate / 再 spawn されるので、長期 WS は保てない。
   Path B の probe も session が止まれば一緒に止まる → Path C で穴埋め。
3. binary は CCoW container 内では token 不在 (CCoW は `~/.cc-relay/token`
   を seed しない) なので、`rust-mcp-agent auth` で 1 度発行する。

### B 案 (将来): WSS /connect を MCP Streamable HTTP の WS variant 化

MCP spec の Streamable HTTP transport には **WebSocket variant** も認め
られている。`/u/<login>/connect` の wire を **MCP JSON-RPC over WebSocket**
に変更/追加すれば、Anthropic Claude.ai connector が将来 WS variant を
喋るようになった時に polling 撤廃の path B 統合が成立する。

ただし 2026-05 時点で:

- Anthropic Claude.ai MCP connector spec は `POST /mcp` 専用 (WS variant
  未対応)
- SEP-1287 (WS push spec) は 2025-12-03 に closed されたので当面棚上げ

→ **B 案は当面棚上げ**、Path A + Path C で運用継続。

### 参考

- ADR-004 (binary 経路 + SSE notification back-pipe)
- ADR-005 (Claude Code Channel — stdio 限定)
- ADR-006 (server-side queue — CCoW)
- yhonda-ohishi/claude-hooks#8 (probe + UserPromptSubmit hook 設計)
- yhonda-ohishi/claude-hooks#9 (probe + UserPromptSubmit hook 実装)
- ippoan/cc-relay#50 (本 section の親 issue)

