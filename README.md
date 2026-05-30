# cc-relay

**Agent coordination for Claude Code on the Web.** Multiple Claude Code
sessions share state — agent presence, agent-to-agent messages, and a
shared task plan — through a single broker. The MVP backend is **GitHub
Issues**; the protocol is open to other backends (Pub/Sub, R2, …) via the
`Broker` trait.

```
┌─────────────────────┐   stdio / WS    ┌────────────────────┐
│  Claude Code (Web)  │ ──────────────▶ │  rust-mcp-agent    │
│  session A          │                 │  (this binary)     │
└─────────────────────┘                 │  tools/list,       │
                                        │  tools/call,       │
┌─────────────────────┐                 │  notifications     │
│  Claude Code (Web)  │ ──────────────▶ └──────────┬─────────┘
│  session B          │                            │
└─────────────────────┘                            │ Broker trait
                                                   ▼
                                        ┌────────────────────┐
                                        │  GitHubBroker      │
                                        │  (per Issue: plan  │
                                        │   in body, msgs in │
                                        │   comments)        │
                                        └────────────────────┘
```

Design rationale: [`ARCHITECTURE.md`](./ARCHITECTURE.md) (ADR-001 = "why
GitHub-as-broker"). Live status: open issues + [project board](https://github.com/orgs/ippoan/projects/7).

## Status

| Phase | Description | State |
|---|---|---|
| **P0–P4** | Scaffold + protocol + Broker trait + `GitHubBroker` impl | ✅ shipped |
| **P5 (#17)** | `agent-mcp` refactor — all tools go through `Broker` | ✅ shipped |
| **P6 (#18)** | `agent-cli` simplification (stdio + 3 transports) | ✅ shipped |
| **P7 (#19)** | End-to-end integration test (real GitHub repo) | 🔄 in flight |
| **P8 (#20)** | Installation docs (this PR) | 🔄 you are here |
| **P9 (#10)** | `v0.1.0` musl static binary release | ⏳ pending P7 |
| **P10 (#11)** | Auth / observability / config polish | ⏳ pending P7 |

Other major design decisions:

- **ADR-003** — Claude Code on Web sandbox authentication via the
  auth-worker MCP OAuth provider (replaces ADR-002 host-mount for the hot
  path)
- **ADR-004** — GitHub issue activity webhooks delivered via the existing
  MCP relay WS (event multiplex, ADR-005 + ADR-006)
- **ADR-005** — Claude Code Channel stdio transport (`channel` subcommand)
- **ADR-006** — server-side event queue + CCoW polling drain for sessions
  that hibernate (`subscribe_issue_activity` / `get_pending_events` tools
  hosted by auth-worker)

## Subcommands

`rust-mcp-agent` is a single binary with five subcommands.

| Subcommand | Transport | Use |
|---|---|---|
| `auth` | HTTPS pair flow (1-click, #145) | One-shot login: writes MCP JWT to `~/.cc-relay/token` |
| `stdio` | stdin/stdout JSON-RPC | Claude Code spawns this as a local MCP server via `.mcp.json` |
| `relay` | outbound WSS | Hosts the MCP server *behind* the auth-worker so Claude.ai connector can reach it from anywhere |
| `channel` | stdio + outbound WSS | Like `stdio`, but also receives GitHub webhook events and pushes them as `notifications/claude/channel` (ADR-005) |
| `probe` | outbound WSS | Smoke probe — connects, logs every frame to JSONL, exits. Diagnostic only (#50) |

For most users **`stdio`** (with the bundled `.mcp.json`) is the path you
want.

## Quickstart — Claude Code on the Web (recommended)

Fresh clone, no laptop required.

1. **Open this repo in Claude Code on the Web.** The bundled
   [`.mcp.json`](./.mcp.json) makes Claude Code pick up the `cc-relay` MCP
   server automatically (routed through `mcp-staging.ippoan.org` per
   ADR-003).
2. **Log in once.** From any session shell:
   ```bash
   rust-mcp-agent auth
   ```
   Open the printed `https://auth.ippoan.org/...` URL in any browser,
   approve the GitHub OAuth, and the binary writes
   `~/.cc-relay/token-staging.json`. No environment variables, no
   `INTERNAL_SHARED_SECRET` to manage (ADR-003).
3. **Start a shared session.** Pick a broker issue:
   ```bash
   # in ippoan/cc-relay (or any repo with the cc-relay-agent App installed)
   gh issue create \
     --title "cc-relay session $(date +%Y-%m-%dT%H-%M-%S)" \
     --body '{"v":1,"agents":[],"plan":[]}' \
     --label cc-relay/active
   ```
   Take note of the issue number (e.g. `42`).
4. **Use the tools.** In any Claude Code turn:
   - `notify_agent(to="alice", message="hi")` — send a message
   - `get_inbox()` — pull messages addressed to you
   - `get_plan()` / `add_task(...)` / `claim_task(...)` / `update_task(...)` — manage shared plan
   - `cc_relay_list_agents()` — see who's joined
   - `subscribe_issue_activity(...)` + `get_issue_events()` — watch GitHub events on this issue (ADR-004)

## Quickstart — host laptop (fallback / CI)

Use this if you cannot run inside Claude Code on the Web (CI job, local
debugging, etc.). This is the ADR-002 path; the public hot path is
ADR-003 above.

```bash
# 1. Fetch the prebuilt static binary
curl -fsSLO https://github.com/ippoan/cc-relay/releases/latest/download/rust-mcp-agent-x86_64-linux-musl
curl -fsSLO https://github.com/ippoan/cc-relay/releases/latest/download/rust-mcp-agent-x86_64-linux-musl.sha256
sha256sum -c rust-mcp-agent-x86_64-linux-musl.sha256
chmod +x rust-mcp-agent-x86_64-linux-musl
sudo mv rust-mcp-agent-x86_64-linux-musl /usr/local/bin/rust-mcp-agent

# 2. Authenticate once (writes ~/.cc-relay/token)
rust-mcp-agent auth

# 3. Launch as an MCP server pointed at a broker issue
rust-mcp-agent stdio \
  --broker-repo ippoan/cc-relay \
  --broker-issue 42 \
  --broker-token "$GITHUB_TOKEN" \
  --agent-id alice
```

Wire that command into Claude Code's MCP config so it's spawned
automatically:

```jsonc
// ~/.config/claude-code/.mcp.json (or per-project .mcp.json)
{
  "mcpServers": {
    "cc-relay": {
      "command": "rust-mcp-agent",
      "args": [
        "stdio",
        "--broker-repo", "ippoan/cc-relay",
        "--broker-issue", "42",
        "--agent-id", "alice"
      ],
      "env": {
        "CC_RELAY_BROKER_TOKEN": "${GITHUB_TOKEN}"
      }
    }
  }
}
```

> The bundled [`.mcp.json`](./.mcp.json) at the repo root uses the
> **HTTP transport via auth-worker** (ADR-003), not local stdio. Use it
> for the Web flow; the JSON above is for the host laptop / CI flow.

## Tool reference

| Tool | Args | Description | Backend |
|---|---|---|---|
| `cc_relay_list_agents` | — | All agents currently joined to the session | `broker.list_agents` |
| `notify_agent` | `to` (string or `"*"`), `message`, `priority?` | Send a message to one agent or broadcast | `broker.send` |
| `get_inbox` | — | Pull messages addressed to this agent since last call | `broker.fetch_since` + cursor persistence (`~/.cc-relay/state-*.json`) |
| `get_plan` | — | Current shared plan as JSON array of `TaskSpec` | `broker.get_plan` |
| `add_task` | `id`, `title`, `status?`, `assignee?`, `notes?` | Append a task to the shared plan | `plan_op(Add)` |
| `claim_task` | `task_id` | Take ownership; assignee = this agent's `agent_id` | `plan_op(Claim)` |
| `update_task` | `task_id`, `status`, `notes?` | Status: `pending` / `in_progress` / `done` / `cancelled` | `plan_op(Update)` |
| `remove_task` | `task_id` | Drop a task entirely | `plan_op(Remove)` |
| `subscribe_issue_activity` | `owner`, `repo`, `issue_number` | Watch a GitHub issue for webhook events (ADR-004) | local file `~/.cc-relay/watched-issues.txt` |
| `unsubscribe_issue_activity` | same | Stop watching | local file |
| `list_watched_issues` | — | What's currently subscribed | local file |
| `get_issue_events` | — | Drain buffered webhook events received since last call | local file (`issue-events.jsonl`) |

When the cc-relay MCP server is talking to **auth-worker** directly (Web
hot path, ADR-006), an additional set of stub tools is exposed by the
auth-worker `McpSession` Durable Object: `cc_relay_ping`,
`subscribe_issue_activity`, `unsubscribe_issue_activity`,
`list_watched_issues`, `get_pending_events`. See ARCHITECTURE.md ADR-006
("CCoW cookbook" section) for usage and the JSON-string return-value
quirk on `get_pending_events`.

## Configuration reference

All flags accept an equivalent `CC_RELAY_*` environment variable so they
can be set via `.mcp.json` `env`.

### `stdio` / `relay` / `channel`

| Flag | Env | Default | Description |
|---|---|---|---|
| `--broker-repo` | `CC_RELAY_BROKER_REPO` | — | `owner/repo` for the broker Issue |
| `--broker-token` | `CC_RELAY_BROKER_TOKEN` | — | GitHub PAT or installation token (`repo` scope) |
| `--broker-issue` | `CC_RELAY_BROKER_ISSUE` | — | Broker Issue number |
| `--agent-id` | — | `stdio-agent` / `host-broker` | This agent's identity |
| `--ws-url` (relay / channel only) | `CC_RELAY_WS_URL` | `wss://mcp-staging.ippoan.org/connect` | Override for prod (`mcp.ippoan.org`) |
| `--token-path` (relay / channel only) | — | `~/.cc-relay/token-staging.json` | Where to read the MCP JWT |
| `--log-level` | `CC_RELAY_LOG` | `info` | tracing-subscriber filter |

### `auth`

| Flag | Env | Default | Description |
|---|---|---|---|
| `--base-url` | `CC_RELAY_AUTH_BASE_URL` | `https://auth.ippoan.org` | Override for staging / wt-quick |
| `--client-id` | `CC_RELAY_CLIENT_ID` | `cc-relay` | Device-flow client_id |
| `--token-path` | — | `~/.cc-relay/token` | Where to write the token |

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `gh API 401` on `notify_agent` | `CC_RELAY_BROKER_TOKEN` is missing or expired. Re-run `gh auth refresh` or generate a fresh PAT |
| `412 Precondition Failed` once in a while | CAS conflict on the broker Issue body. Broker retries; no action needed unless you see it loop indefinitely (file a bug) |
| Tools list empty in Claude Code | Make sure `.mcp.json` is at the repo root *or* in `~/.config/claude-code/`. CCoW reads it on session start (`SessionStart` hook is too late per [hooks docs](https://code.claude.com/docs/en/hooks.md)) |
| `get_inbox` returns the same message twice | Cursor file (`~/.cc-relay/state-*.json`) was wiped or relocated. Re-run the binary so it recreates the cursor at `Cursor::beginning()` |
| `subscribe_issue_activity` accepted but no events | Repo webhook is not configured to point at `auth-worker`. See `docs/relay-validation.md` for the one-time setup |
| stdio binary を更新したのに反映されない | 既定では次セッションで自動反映。走行中セッションへ即反映するなら `kill` + `/mcp` Reconnect（`kill` 単独では stdio は再 spawn しない）。詳細は [`docs/stdio-deployment.md`](./docs/stdio-deployment.md) §4 |

## Architecture index

- [`ARCHITECTURE.md`](./ARCHITECTURE.md) — full design history, indexed
  by ADR. Read **ADR-001** first; everything else builds on it.
- [`docs/github-app.md`](./docs/github-app.md) — broker `cc-relay-agent`
  GitHub App configuration
- [`docs/credentials.md`](./docs/credentials.md) — token flows (end-user
  vs maintainer)
- [`docs/stdio-deployment.md`](./docs/stdio-deployment.md) — CCoW stdio
  variant deployment (Releases binary / OAT auth / `.claude.json` / kill
  reflow). Proposal #72 / ADR-008 候補
- [`docs/sub-agent-workflow.md`](./docs/sub-agent-workflow.md) —
  dogfooding pattern: parallel Claude Code sessions via `Agent` tool
  while broker is being built
- [`docs/relay-validation.md`](./docs/relay-validation.md) — sandbox
  reachability probe results
- [`examples/sub-agent-recipes/`](./examples/sub-agent-recipes/) —
  copy-paste prompt templates for sub-agent workflows
- [`.mcp.json`](./.mcp.json) — Claude Code on Web MCP config (ADR-003
  route via auth-worker)

## Contributing

`main` is protected; branches are short-lived per-issue
(`<issue>-<type>-<short-description>`, see worktree-naming-guard in
`yhonda-ohishi/claude-hooks`). PRs auto-merge once CI is green.

```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

CI runs all three on every PR; `release-build` cross-compiles the musl
static binary on `main` merges and on `v*` tag pushes.

## License

MIT
