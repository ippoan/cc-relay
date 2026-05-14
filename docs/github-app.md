# cc-relay-agent (GitHub App)

Public configuration for the GitHub App that cc-relay uses as its broker
credential (per ADR-001) and for general repo automation (PR creation, CI
status reads, eventual webhook delivery).

The App was created on 2026-05-14 via the manifest flow; this document is
the canonical record of its non-secret identifiers.

## Identity

| Field            | Value                                                                  |
| ---------------- | ---------------------------------------------------------------------- |
| App name         | `cc-relay-agent`                                                       |
| App ID           | `3710243`                                                              |
| App slug         | `cc-relay-agent`                                                       |
| Owner            | `ippoan` (organization)                                                |
| Settings page    | https://github.com/organizations/ippoan/settings/apps/cc-relay-agent   |
| Public install   | No — `Only on this account`                                            |
| Installation     | `cc-relay` repo only                                                   |
| Installation ID  | `132248860`                                                            |
| Webhook          | Inactive (broker polls; webhook will be enabled when a receiver exists) |

## Permissions (repository)

| Permission         | Level | Why                                                  |
| ------------------ | ----- | ---------------------------------------------------- |
| Issues             | r/w   | Broker channel (Issues + comments as transport)      |
| Pull requests      | r/w   | PR open / comment / auto-merge enablement            |
| Contents           | r/w   | Branch / file ops via API when not going through git |
| Workflows          | r/w   | Future: agent modifies CI                            |
| Actions            | r     | Read workflow run status                             |
| Checks             | r     | Read check run results                               |
| Commit statuses    | r     | Read CI status                                       |
| Metadata           | r     | Mandatory default                                    |

No organization permissions, no account permissions, no user-to-server OAuth.

## Subscribed events

None at App level (webhook is inactive). When a webhook receiver lands,
these are the events to subscribe to:
`issues`, `issue_comment`, `pull_request`, `pull_request_review`,
`pull_request_review_comment`, `workflow_run`.

## Credentials at runtime

> **Two auth paths coexist.** This page documents the **maintainer /
> CI** path (GitHub App PEM → installation token). End-users hit the
> separate **auth-worker device flow** described in
> [`docs/credentials.md`](./credentials.md) (and in
> [ADR-002](../ARCHITECTURE.md#adr-002-end-user-auth-via-auth-worker-mcp-oauth-provider)).
> Do not distribute the PEM to end-users; they don't need it.

The agent binary reads three environment variables:

| Env var                        | Value                                                    |
| ------------------------------ | -------------------------------------------------------- |
| `CC_RELAY_GH_APP_ID`           | `3710243` (public — this doc)                            |
| `CC_RELAY_GH_INSTALLATION_ID`  | `132248860` (public — this doc)                          |
| `CC_RELAY_GH_PRIVATE_KEY` _or_ | PEM contents (single string with literal `\n`)           |
| `CC_RELAY_GH_PRIVATE_KEY_PATH` | Path to the `.pem` on disk                               |

Provide *one of* `CC_RELAY_GH_PRIVATE_KEY` or `CC_RELAY_GH_PRIVATE_KEY_PATH`,
not both.

**The `.pem` is the only secret.** Stored out-of-repo (1Password etc.);
`.gitignore` excludes `*.pem` defensively. Anyone with the PEM can mint
installation tokens against `ippoan/cc-relay` until the key is revoked.

## Token flow (to be implemented in P4 / #16)

```
1. Binary reads APP_ID + INSTALLATION_ID + PEM at startup.
2. Sign an RS256 JWT:
     header.alg = RS256
     payload    = { iss: app_id, iat: now-60s, exp: now+9m }
3. POST https://api.github.com/app/installations/132248860/access_tokens
     Authorization: Bearer <jwt>
   →  { token: "ghs_...", expires_at: "...+1h" }
4. Cache the installation token in-process.
5. Use the token as Bearer for every Issues / PR API call.
6. When <5 min to expiry (or on 401), repeat step 2–3.
```

## Rotation

- **Private key**: regenerate on the App settings page (multiple keys can
  coexist). Roll out the new key, then delete the old one. Old keys
  immediately stop working.
- **App ID / Installation ID**: stable. No rotation.
- **If the key leaks**: regenerate → delete the compromised key →
  optionally suspend the installation while replacing the secret. No data
  in the broker is sensitive (Issues are repo-visible), but a leaked key
  also lets an attacker open PRs / mutate Issues until revoked.

## Re-creating the App from scratch

`/tmp/cc-relay-app-manifest.json` (in a session at the time of creation)
held the manifest. To reproduce, re-issue a manifest with the same fields
and click through the create flow again; the new App will have a different
App ID and Installation ID, and `docs/github-app.md` must be updated.
