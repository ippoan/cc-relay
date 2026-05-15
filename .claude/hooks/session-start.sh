#!/usr/bin/env bash
# Session start: bootstrap yhonda-ohishi/claude-hooks if missing, then delegate
# to the centralised cc-relay broker hook.
#
# The actual build + relay-launch logic lives in
# `session-start-cc-relay-broker.sh` over in claude-hooks so other consumers
# (and future smoke-test setups) can reuse it without copy-pasting.
#
# Web-only: returns 0 immediately when `CLAUDE_CODE_REMOTE` is unset.
set -euo pipefail

if [[ "${CLAUDE_CODE_REMOTE:-}" != "true" ]]; then
  exit 0
fi

HOOKS_DIR="${HOME}/.claude/sources/claude-hooks"

# Pick a clone URL. The Claude Code on Web sandbox restricts direct
# `https://github.com/<owner>/<repo>.git` access on non-allow-listed repos
# (yhonda-ohishi/claude-hooks currently returns 404), but exposes an
# anonymous local git proxy on `127.0.0.1:35231` for those same repos.
# Probe the proxy first; fall back to public HTTPS for hosts that don't
# run the proxy (local dev when CLAUDE_CODE_REMOTE=true is forced, smoke
# test environments, etc.).
PUBLIC_URL="https://github.com/yhonda-ohishi/claude-hooks.git"
PROXY_URL="http://local_proxy@127.0.0.1:35231/git/yhonda-ohishi/claude-hooks"
if [[ -n "${CLAUDE_HOOKS_HOOKS_URL:-}" ]]; then
  HOOKS_REPO_URL="${CLAUDE_HOOKS_HOOKS_URL}"
elif git ls-remote "$PROXY_URL" HEAD >/dev/null 2>&1; then
  HOOKS_REPO_URL="$PROXY_URL"
else
  HOOKS_REPO_URL="$PUBLIC_URL"
fi

# Bootstrap: clone claude-hooks if it isn't already there. Network failure is
# logged but does not block the session — the user can fix it manually.
if [[ ! -d "${HOOKS_DIR}/.git" ]]; then
  mkdir -p "$(dirname "$HOOKS_DIR")"
  if ! git clone --depth=1 --quiet "$HOOKS_REPO_URL" "$HOOKS_DIR" 2>&1; then
    echo "[session-start] failed to clone $HOOKS_REPO_URL — relay not started" >&2
    exit 0
  fi
  echo "[session-start] cloned claude-hooks from $HOOKS_REPO_URL into $HOOKS_DIR" >&2
fi

BROKER_HOOK="${HOOKS_DIR}/session-start-cc-relay-broker.sh"
if [[ ! -x "$BROKER_HOOK" ]]; then
  echo "[session-start] $BROKER_HOOK missing or not executable — relay not started" >&2
  exit 0
fi

exec "$BROKER_HOOK"
