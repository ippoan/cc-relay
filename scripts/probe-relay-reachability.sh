#!/usr/bin/env bash
# Probe whether the auth-worker relay endpoints needed by issue #33
# (device-flow + introspect, hosted at auth.ippoan.org) are reachable
# from the current execution environment.
#
# Intended runs:
#   - Inside a Claude Code on Web sandbox, to verify proxy allowlist status.
#   - On a developer workstation, as a positive control.
#
# Output is line-per-endpoint, suitable for piping into a PR comment or
# pasting into docs/relay-validation.md.
#
# Exit status:
#   0  every target endpoint returned a non-403 HTTP status
#   1  at least one target was blocked (HTTP 403 / DNS failure / timeout)
#
# api.github.com is included as a control: if it fails too, the network
# itself is the problem, not the auth-worker allowlist.

set -u

TIMEOUT="${PROBE_TIMEOUT:-8}"

# Target endpoints exercised by the proposed cc-relay <-> auth-worker flow.
# Keep the device_authorization / token / introspect paths first so a
# reader sees the auth-worker results before the control row.
TARGETS=(
    "https://auth.ippoan.org/.well-known/oauth-authorization-server|auth-worker discovery"
    "https://auth.ippoan.org/mcp/device_authorization|device flow start"
    "https://auth.ippoan.org/mcp/token|device flow poll"
    "https://auth.ippoan.org/mcp/introspect|JWT -> github_token"
    "https://api.github.com/|control (must be 200)"
)

fail=0
printf "%-60s  %-25s  %s\n" "endpoint" "result" "note"
printf "%-60s  %-25s  %s\n" "--------" "------" "----"

for row in "${TARGETS[@]}"; do
    url="${row%%|*}"
    note="${row#*|}"

    # -o /dev/null discards body; -w "%{http_code}" prints only the status.
    # --max-time bounds DNS + connect + transfer; on DNS / TCP failure curl
    # emits HTTP "000" which we treat as unreachable.
    code=$(curl -sS -o /dev/null -w "%{http_code}" \
                --max-time "$TIMEOUT" "$url" 2>/dev/null || echo "000")

    # A response with no `x-deny-reason` header — even a 403 from the
    # origin — proves the host is reachable. The Claude Code on Web proxy
    # only sets `x-deny-reason: host_not_allowed` when it blocks at the
    # allowlist layer, so that header is the single signal we trust.
    deny=""
    if [ "$code" != "000" ]; then
        deny=$(curl -sS -D - -o /dev/null --max-time "$TIMEOUT" "$url" \
                    2>/dev/null | awk 'tolower($1)=="x-deny-reason:" {print $2; exit}' \
                    | tr -d '\r')
    fi

    if [ "$code" = "000" ]; then
        result="UNREACHABLE (dns/tcp)"
        fail=1
    elif [ -n "$deny" ]; then
        result="$code blocked: $deny"
        fail=1
    else
        result="$code reachable"
    fi

    printf "%-60s  %-25s  %s\n" "$url" "$result" "$note"
done

echo
if [ "$fail" -eq 0 ]; then
    echo "OK: every auth-worker endpoint reachable from this environment."
else
    echo "BLOCKED: at least one auth-worker endpoint is not reachable."
    echo "See docs/relay-validation.md for known causes (proxy allowlist)."
fi

exit "$fail"
