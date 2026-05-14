#!/usr/bin/env bash
# .mcp.json probe — confirms whether Claude Code on Web spawns stdio MCP servers
# declared in repo-local .mcp.json.
#
# What this does:
#  1. Appends a one-shot log line to /tmp/cc-relay-mcp-probe.log with timestamp,
#     pid, working directory, args, and any MCP/CLAUDE/ANTHROPIC env vars.
#  2. Speaks just enough of the MCP wire protocol to survive the handshake:
#     responds to `initialize` and `tools/list`, exposes a single no-op tool.
#  3. Logs every stdin line it sees, so we can see what Claude sent.
#  4. Holds open on stdin so Claude treats it as a healthy server until refresh.
#
# After Claude Code on Web reloads the session, inspect
# /tmp/cc-relay-mcp-probe.log. If the file exists with timestamped entries:
#   → .mcp.json IS honored, stdio MCP servers work on Web.
# If the file does not exist:
#   → either .mcp.json is not read, or the type:"stdio" entry is rejected.

set -u
LOG=/tmp/cc-relay-mcp-probe.log

log() {
  printf '%s %s\n' "$(date -Iseconds)" "$*" >> "$LOG"
}

{
  echo "==================== probe spawned ===================="
  echo "ts=$(date -Iseconds) pid=$$ ppid=$PPID"
  echo "pwd=$PWD"
  echo "user=$(id -un 2>/dev/null || echo ?)"
  echo "args=$*"
  echo "--- env (MCP|CLAUDE|ANTHROPIC) ---"
  env | grep -iE 'MCP|CLAUDE|ANTHROPIC' || echo "(none)"
  echo "--- /proc/self/status (first 6 lines) ---"
  head -n 6 /proc/self/status 2>/dev/null || true
  echo "--- caller ---"
  ps -o pid,ppid,comm,args -p $PPID 2>/dev/null || true
  echo "------------------------------------------------"
} >> "$LOG" 2>&1

# Minimal MCP server: respond to `initialize` + `tools/list`, expose one tool.
# We deliberately stay simple (no Content-Length framing) — Claude Code stdio
# transport uses line-delimited JSON-RPC for stdio servers.
while IFS= read -r line; do
  log "STDIN $line"

  # Extract `"id":...` (number or string) up to the next comma or brace.
  id=$(printf '%s' "$line" | grep -oE '"id":[[:space:]]*("[^"]*"|[0-9]+)' | head -n1 | sed 's/^"id":[[:space:]]*//')

  case "$line" in
    *'"method":"initialize"'*)
      reply='{"jsonrpc":"2.0","id":'"${id:-1}"',"result":{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"cc-relay-probe","version":"0.0.1"}}}'
      printf '%s\n' "$reply"
      log "OUT $reply"
      ;;
    *'"method":"notifications/initialized"'*)
      log "ACK notifications/initialized (no reply expected)"
      ;;
    *'"method":"tools/list"'*)
      reply='{"jsonrpc":"2.0","id":'"${id:-2}"',"result":{"tools":[{"name":"probe_ping","description":"cc-relay .mcp.json probe — call to confirm stdio works","inputSchema":{"type":"object","properties":{}}}]}}'
      printf '%s\n' "$reply"
      log "OUT $reply"
      ;;
    *'"method":"tools/call"'*)
      reply='{"jsonrpc":"2.0","id":'"${id:-3}"',"result":{"content":[{"type":"text","text":"probe_ping ok — .mcp.json stdio works on this client"}]}}'
      printf '%s\n' "$reply"
      log "OUT $reply"
      ;;
    *'"method":"ping"'*)
      reply='{"jsonrpc":"2.0","id":'"${id:-0}"',"result":{}}'
      printf '%s\n' "$reply"
      log "OUT $reply"
      ;;
    *)
      log "SKIP (no handler)"
      ;;
  esac
done

log "EOF on stdin, exiting"
