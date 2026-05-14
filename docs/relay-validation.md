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
cannot reach the auth-worker from a Claude Code on Web sandbox. The
implementation work in #33 is blocked on resolving this reachability gap.

## Resolution paths (from #33)

1. **(A) Allowlist `auth.ippoan.org`.** Have Claude Code on Web add the
   host to the sandbox proxy allowlist. Lowest-risk if achievable, since
   no architectural change is needed.
2. **(B) Register auth-worker as a Custom Integration.** Anthropic docs
   state MCP connector traffic is routed through the Anthropic backend
   and bypasses the sandbox allowlist. Needs verification that this also
   covers the auth-worker's REST endpoints (device_authorization / token
   / introspect), not just its MCP surface.
3. **(C) Reach auth-worker via the Phase 7 WS relay.** Would force a
   broader redesign — cc-relay is currently stdio-MCP only — so this is
   the fallback if (A) and (B) both fail.

The cross-reference issue
[`ippoan/auth-worker#130`](https://github.com/ippoan/auth-worker/issues/130)
tracks the upstream side of (A)/(B).

## Re-running on future sandbox changes

If Anthropic updates the proxy allowlist (path A) or routing rules
(path B), re-run the probe to confirm before reopening implementation
work on #33:

```
./scripts/probe-relay-reachability.sh
```

A clean run (exit 0, every row `reachable`) unblocks the broker-side
work outlined in #33's "含まれるもの" section.
