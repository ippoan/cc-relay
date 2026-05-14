# Relay reachability validation

Issue [#33](https://github.com/ippoan/cc-relay/issues/33) proposes that
cc-relay use [`ippoan/auth-worker`](https://github.com/ippoan/auth-worker)'s
MCP OAuth Provider (hosted at `auth.ippoan.org`) for end-user GitHub auth,
following the same pattern as
[`ippoan/github-mcp-server-rs`](https://github.com/ippoan/github-mcp-server-rs).

That design only works if a **Claude Code on Web sandbox** can reach
`auth.ippoan.org`. The sandbox enforces a static proxy allowlist
(see [ARCHITECTURE.md ADR-001](../ARCHITECTURE.md#adr-001-github-as-broker--stdio-only-mcp-server)),
so this assumption must be verified — not asserted.

## How to validate

```
./scripts/probe-relay-reachability.sh
```

Run it from inside whichever environment you want to characterise: a Claude
Code on Web session for the real signal, or a workstation as a positive
control.

The script hits the four endpoints the proposed device-flow client would
use, plus `api.github.com` as a control, and exits non-zero if any
auth-worker target is blocked at the proxy allowlist
(`x-deny-reason: host_not_allowed`) or unreachable via DNS / TCP.

## Findings (2026-05-14, Claude Code on Web sandbox)

| Endpoint | Result |
| --- | --- |
| `https://auth.ippoan.org/.well-known/oauth-authorization-server` | `403 host_not_allowed` |
| `https://auth.ippoan.org/mcp/device_authorization` | `403 host_not_allowed` |
| `https://auth.ippoan.org/mcp/token` | `403 host_not_allowed` |
| `https://auth.ippoan.org/mcp/introspect` | `403 host_not_allowed` |
| `https://mcp.ippoan.org/` | DNS resolution failure |
| `https://api.github.com/` (control) | reachable |

The proxy returns `HTTP/2 403` with header `x-deny-reason: host_not_allowed`
for every `auth.ippoan.org` request. This matches the allowlist behaviour
documented in [ADR-001](../ARCHITECTURE.md#adr-001-github-as-broker--stdio-only-mcp-server)
for non-allowlisted hosts.

**Conclusion:** as of 2026-05-14, the device-flow client described in #33
cannot reach the auth-worker from a Claude Code on Web sandbox.

## Adopted workaround: host-side login + read-only mount

Resolved by auth-worker
[PR #131](https://github.com/ippoan/auth-worker/pull/131) (consumer
integration guide §4). cc-relay follows the same pattern as
`github-mcp-server-rs`:

1. The end-user runs `rust-mcp-agent auth` on the **host** (laptop /
   workstation), where `auth.ippoan.org` is reachable normally.
2. The resulting `~/.cc-relay/token` is mounted read-only into the
   Claude Code on Web sandbox.
3. The broker process inside the sandbox loads the file via
   `TokenManager::from_cache` and refreshes within the 5-minute skew
   window. Refresh traffic also goes through `auth.ippoan.org`, so
   refresh has to happen on the host before mount — but the refresh
   token's 30-day TTL makes this a once-per-month operation in
   practice. See `docs/credentials.md` for the end-user procedure.

This unblocks #33 without requiring any change to the sandbox
allowlist.

## Future paths (not blocking #33)

The probe is kept in-tree because the host-side mount workaround is
not the long-term shape. Re-run it if any of these resolve:

1. **(A) Allowlist `auth.ippoan.org`.** Anthropic adds the host to the
   sandbox proxy allowlist. The CLI would then run inside the sandbox
   directly. Status: TBD (Anthropic side).
2. **(B) Register auth-worker as a Custom Integration.** MCP connector
   traffic bypasses the sandbox allowlist. Would need to cover the REST
   surface (device_authorization / token / introspect), not just MCP.
3. **(C) Phase 7 WS relay (`mcp.ippoan.org`).** Pre-supposes (A) since
   the relay host itself has to be reachable.

```
./scripts/probe-relay-reachability.sh
```

A clean run (exit 0, every row `reachable`) means the host-side mount
workaround can be retired in favour of running the CLI inside the
sandbox.
