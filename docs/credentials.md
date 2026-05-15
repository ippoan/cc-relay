# cc-relay credentials

How end-users and maintainers authenticate cc-relay to GitHub. See
[ADR-003](../ARCHITECTURE.md#adr-003-sandbox-auth-via-claude-code-mcp-connector--auth-worker-user-less-relay)
for the current design (ADR-002 is the host-mount fallback).

> **ADR-003 status update.** §1 has been rewritten to drop the
> `INTERNAL_SHARED_SECRET` requirement on end-user `auth` — the CLI now
> introspects via Bearer JWT (see §4). §2 (host mount) and §3 (host-side
> refresh) describe the ADR-002 fallback path and only apply when the
> binary runs on your laptop instead of the sandbox.

Two flows coexist:

| Audience | Flow | Token | Scope |
|---|---|---|---|
| **End-user** (you, running cc-relay) | auth-worker device flow | OAuth token from `/mcp/introspect` | `read:user repo` |
| **Maintainer / CI** | GitHub App installation token (PEM) | App installation token | Repo App perms (see `docs/github-app.md`) |

You only need the end-user flow. The maintainer flow is documented in
[`docs/github-app.md`](./github-app.md) and is used by CI workflows and
release tooling.

## §1. First-time setup

Under ADR-003 (Claude Code on Web), the binary runs **inside the
sandbox**, not on your laptop. From any session shell:

```
rust-mcp-agent auth
```

No env vars needed for end-users. The CLI calls `/mcp/introspect`
with `Authorization: Bearer <MCP_JWT>` (mode 1 in
`auth-worker/src/handlers/mcp-introspect.ts`); the JWT obtained from
the device flow IS the authentication, so `INTERNAL_SHARED_SECRET`
distribution is no longer required for end-users (see §4).

`rust-mcp-agent` prints a `https://auth.ippoan.org/...` URL and a
per-session user code. Open the URL in any browser, approve the GitHub
consent screen, and the CLI exits with:

```
ok, you are <github_login> (token written to /home/<you>/.cc-relay/token)
```

The file has mode `0600`. Its layout is plain JSON
(`TokenSet { access_token, refresh_token, scope, github_token,
expires_at, acquired_at }`).

### Why a one-time browser approval is enough

`refresh_token` lives 30 days. Inside the broker, `TokenManager`
refreshes automatically when the access token is within 5 minutes of
expiring (default `expires_in = 3600` seconds). So a fresh `auth` run
covers ~30 days of normal use; you only re-run it when the refresh
token itself expires.

## §2. Mount the token into the sandbox

Claude Code on Web spawns each session in a sandbox container. Mount
your host's `~/.cc-relay/` directory **read-only** into the container
at the same path. The exact mechanism depends on how you're launching
the session; the contract is just:

```
host:~/.cc-relay/token  ->  sandbox:~/.cc-relay/token   (read-only)
```

The broker reads the file via `TokenManager::from_cache(...)`. Writes
(refresh) happen on the host between sessions, not inside the sandbox.

## §3. Refresh lifecycle

- **In-session, JWT close to expiry:** `TokenManager::ensure_fresh`
  calls `POST /mcp/token` (refresh_token grant) + `POST /mcp/introspect`,
  rewrites `~/.cc-relay/token` in place, continues. **This requires
  `auth.ippoan.org` to be reachable from the broker process.** Inside
  a Claude Code on Web sandbox, it currently isn't (see
  `docs/relay-validation.md` §4); so the practical pattern is to keep
  the broker happy on the host (the refresh succeeds there) and ship a
  fresh `~/.cc-relay/token` into each sandbox session.

  When Anthropic adds `auth.ippoan.org` to the sandbox allowlist
  (resolution path A in `docs/relay-validation.md`), in-sandbox
  refresh works automatically — no code change needed.

- **`refresh_token` expired (~30 days):** the next refresh fails with
  `BrokerError::Auth("refresh denied: ...")`. Re-run `rust-mcp-agent
  auth` on the host.

- **`GITHUB_MCP_USER_ALLOWLIST` doesn't include your login:** the
  device approval step in `auth` returns `access_denied`. Ask the
  auth-worker maintainer to add your GitHub login to the allowlist
  (see §5).

## §4. `INTERNAL_SHARED_SECRET` (legacy mode)

`POST /mcp/introspect` accepts **two** auth modes (see
`auth-worker/src/handlers/mcp-introspect.ts`):

- **Mode 1 — Bearer JWT** (default for end-user `rust-mcp-agent auth`).
  `Authorization: Bearer <MCP_JWT>`. OAuth has already authenticated the
  user; the JWT itself proves they may read their own `github_token`.
  **No env var required.**
- **Mode 2 — Shared secret** (legacy; `github-mcp-server-rs` and CI).
  `Authorization: <INTERNAL_SHARED_SECRET>` (raw, no `Bearer` prefix).
  Pass via `CC_RELAY_AUTH_INTROSPECT_SECRET` if you need to exercise
  the legacy path (CI, dual-consumer tests).

End-users on Claude Code on Web should never set
`CC_RELAY_AUTH_INTROSPECT_SECRET`. The legacy mode will be removed once
`github-mcp-server-rs` has migrated and auth-worker
[#91](https://github.com/ippoan/auth-worker/issues/91) (Service Binding)
lands.

## §5. Allowlist registration

Auth-worker enforces `GITHUB_MCP_USER_ALLOWLIST` (JSON array of GitHub
logins) at the device-callback step. If your `github_login` is not in
the list, the browser approval step returns `access_denied` and the
device flow fails.

Ask the auth-worker maintainer to run, on their side:

```
wrangler secret put GITHUB_MCP_USER_ALLOWLIST
```

and add your login. The list is fail-closed: malformed JSON / non-array
/ non-string entries deny everyone.

## §6. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `BrokerError::Auth("no cached token at ...")` | `rust-mcp-agent auth` never ran, or the mount path is wrong | §1 + §2 |
| `BrokerError::Auth("refresh denied: ...")` | `refresh_token` expired (>30d) | re-run `rust-mcp-agent auth` |
| `BrokerError::Auth("introspect: invalid INTERNAL_SHARED_SECRET")` | wrong env var value | §4 |
| `auth` ends with `device flow denied: access_denied` | login not in `USER_ALLOWLIST` | §5 |
| broker 403 on `api.github.com/repos/.../issues` | requested `mcp.read` only, no `mcp.write` | re-run `auth` (the CLI requests both by default) |
| sandbox cannot reach `auth.ippoan.org` (403 `host_not_allowed`) | expected — see ADR-002 / §2 host-side workaround | already handled by mount; nothing to do until path (A) lands |

## §7. Maintainer-only path (GitHub App PEM)

The PEM-based installation token flow described in
[`docs/github-app.md`](./github-app.md) is **not** for end-users. It's
used by:

- CI workflows that need to comment / merge PRs on this repo.
- Release automation that publishes artifacts on tag pushes.

The PEM never leaves the maintainer's secret store (1Password etc.) +
GitHub Actions Secrets. `*.pem` is git-ignored as a defense-in-depth
measure.