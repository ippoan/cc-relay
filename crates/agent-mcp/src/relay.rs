//! ADR-003 Phase C: cc-relay host-side broker as MCP server over WebSocket relay.
//!
//! Connects an outbound WebSocket to `wss://mcp(-staging).ippoan.org/connect`
//! (auth-worker `McpSession` Durable Object) and acts as the MCP server that
//! the auth-worker DO bridges Claude.ai's `POST /u/<login>/mcp` requests to.
//!
//! Frame protocol mirrors `auth-worker/src/durable_objects/mcp-session-do.ts`:
//!
//! ```json
//! // Sent by us once on connect
//! {"kind":"hello","v":1,"binary_version":"...","proto":1}
//! // Received from DO per inbound HTTP request
//! {"kind":"req","v":1,"id":"<uuid>","method":"POST","path":"/","headers":{...},"body_b64":"..."}
//! // Sent by us per response
//! {"kind":"resp","v":1,"id":"<uuid>","status":200,"headers":{...},"body_b64":"..."}
//! ```
//!
//! `body_b64` of the inbound `req` frame is a JSON-RPC 2.0 message
//! (initialize / tools/list / tools/call / notifications/*). The dispatcher
//! ([`handle_jsonrpc`]) is hand-rolled rather than going through `rmcp`
//! because rmcp's transport layer assumes either stdio or HTTP, neither of
//! which fits the WS-frame request/response shape cleanly. Keeping it
//! hand-rolled also lets us share the exact response shapes with the inline
//! stub in `auth-worker/src/durable_objects/mcp-session-do.ts` (relay mode +
//! stub mode are wire-compatible).
//!
//! Tools exposed:
//!
//! - `cc_relay_list_agents` — calls [`Broker::list_agents`] and returns the
//!   roster as text content. Proof-of-life tool for Phase C; the rest of the
//!   Broker surface (notify_agent, plan ops, ...) lands in follow-ups.

use std::sync::Arc;

use agent_broker::{Broker, Cursor, CursorStore};
use agent_core::{NotifyMessage, NotifyTarget, PlanOp, Priority, TaskSpec, TaskStatus};
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;

use crate::watched_issues::{IssueEventsFile, IssueKey, WatchedIssuesFile};

/// Protocol version published in `initialize` and the `hello` frame. Mirrors
/// the inline stub server's value so the wire view from a connector POV is
/// the same regardless of which side answers.
pub const STUB_PROTOCOL_VERSION: &str = "2025-06-18";

/// Frame schema version (binary side `github-mcp-server-rs#27` and auth-worker
/// `mcp-session-do.ts:FRAME_VERSION` both pin to 1).
pub(crate) const FRAME_VERSION: u32 = 1;

/// Server name advertised in `initialize`. Matches `cc-relay` in the
/// `.mcp.json` server entry to keep tool namespace prefixes intuitive.
const SERVER_NAME: &str = "cc-relay";
pub(crate) const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// ADR-005: instructions added to Claude's system prompt when running as a
/// Channel. Tells Claude how to recognize the `<channel>` tag emitted by
/// `notifications/claude/channel`, what the meta-derived attributes mean,
/// and that events are one-way (no reply tool wired in Phase A).
const CHANNEL_INSTRUCTIONS: &str = "GitHub webhook events arrive as \
    `<channel source=\"cc-relay\" event_type=\"...\" owner=\"...\" repo=\"...\" \
    issue_number=\"...\" delivery_id=\"...\">...</channel>` envelopes. \
    `event_type` is e.g. `issue_comment.created` / `issues.opened`. The body \
    is the raw GitHub event JSON (truncated). The events are one-way: read \
    them and act, no reply expected. Filter is done client-side via the \
    `subscribe_issue_activity` / `unsubscribe_issue_activity` tools.";

/// Outbound `hello` frame (us → DO) sent immediately after WS upgrade.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct HelloFrame {
    pub(crate) kind: &'static str,
    pub(crate) v: u32,
    pub(crate) binary_version: &'static str,
    pub(crate) proto: u32,
}

/// Inbound `req` frame (DO → us) per Claude.ai-originated `POST /mcp`.
#[derive(Debug, Clone, Deserialize)]
struct ReqFrame {
    kind: String,
    #[serde(default)]
    id: String,
    #[serde(default)]
    body_b64: String,
}

/// Outbound `resp` frame (us → DO) for a single completed request.
#[derive(Debug, Clone, Serialize)]
struct RespFrame<'a> {
    kind: &'static str,
    v: u32,
    id: &'a str,
    status: u16,
    headers: serde_json::Map<String, Value>,
    body_b64: String,
}

/// Configuration for [`run`].
#[derive(Debug, Clone)]
pub struct RelayConfig {
    /// Full WebSocket URL (e.g. `wss://mcp-staging.ippoan.org/connect` for
    /// the user-less ADR-003 endpoint, or
    /// `wss://mcp-staging.ippoan.org/u/<login>/connect` for the legacy
    /// user-scoped one).
    pub ws_url: String,
    /// MCP access JWT (from Phase 3 `/mcp/token` device flow). Sent in the
    /// `Authorization: Bearer ...` header on the WS upgrade.
    pub access_token: String,
}

/// Hand-rolled JSON-RPC dispatcher over the [`Broker`] trait. Single instance
/// is shared across every inbound `req` frame.
pub struct RelayServer {
    broker: Arc<dyn Broker>,
    /// ADR-004: subscription state (re-subscribe on restart + event filter).
    watched: WatchedIssuesFile,
    /// ADR-004: event buffer for `get_issue_events` tool drain.
    events: IssueEventsFile,
    /// ADR-004 Phase D: outbound back-pipe to auth-worker. When `Some`, every
    /// successful `handle_event_frame` also sends a `kind:"notif"` frame
    /// wrapping a JSON-RPC `notifications/message` so the auth-worker DO can
    /// fan it out to attached SSE channels. `None` in tests (no real WS).
    notif_tx: Option<mpsc::UnboundedSender<String>>,
    /// ADR-005 Phase A: when set, the server runs as a Claude Code Channel
    /// (`claude/channel` experimental capability) over **stdio** rather than
    /// as a WS frame relay. In this mode:
    ///   - `initialize` advertises `experimental: { "claude/channel": {} }`
    ///     + `instructions` so Claude knows how to read the `<channel>` tag.
    ///   - `handle_event_frame` formats each event as a JSON-RPC
    ///     `notifications/claude/channel` notification (with `content` /
    ///     `meta` params) and pushes it to `notif_tx`. The stdio writer
    ///     task drains the channel and writes raw JSON-RPC lines to stdout.
    channel_mode: bool,
    /// P5 #17 Phase 17.2: in-memory cursor advanced by every `get_inbox`
    /// call. Initialised from `cursor_store` if one is wired; otherwise
    /// `Cursor::beginning()` and lives only in-process.
    cursor: Mutex<Cursor>,
    /// P5 #17 Phase 17.2: file-backed cursor persistence
    /// (`~/.cc-relay/state-<slug>.json`). `None` in tests / when the
    /// caller does not care about cross-session resume.
    cursor_store: Option<Arc<CursorStore>>,
}

impl RelayServer {
    pub fn new(broker: Arc<dyn Broker>) -> Self {
        let watched_path = WatchedIssuesFile::default_path()
            .unwrap_or_else(|_| std::path::PathBuf::from("/tmp/.cc-relay-watched-issues.txt"));
        let events_path = IssueEventsFile::default_path()
            .unwrap_or_else(|_| std::path::PathBuf::from("/tmp/.cc-relay-issue-events.jsonl"));
        Self::with_files(
            broker,
            WatchedIssuesFile::new(watched_path),
            IssueEventsFile::new(events_path),
        )
    }

    /// テスト用 / カスタム file パスを指定する constructor。
    pub fn with_files(
        broker: Arc<dyn Broker>,
        watched: WatchedIssuesFile,
        events: IssueEventsFile,
    ) -> Self {
        Self {
            broker,
            watched,
            events,
            notif_tx: None,
            channel_mode: false,
            cursor: Mutex::new(Cursor::beginning()),
            cursor_store: None,
        }
    }

    /// P5 #17 Phase 17.2: install a file-backed cursor store. Loads the
    /// persisted cursor (best effort — a missing or corrupt file resets
    /// to `Cursor::beginning()`) before returning, so the first
    /// `get_inbox` call after a restart resumes from the right point.
    ///
    /// Async because [`CursorStore::load`] hits the filesystem; callers
    /// (`agent-cli`'s `run_stdio` / `run_relay` / `run_channel_cmd`)
    /// already run inside the tokio runtime so the await is free.
    pub async fn with_persisted_cursor(mut self, store: Arc<CursorStore>) -> Self {
        let loaded = store.load().await;
        *self.cursor.lock().await = loaded;
        self.cursor_store = Some(store);
        self
    }

    /// ADR-004 Phase D: WS 上の back-pipe を設定する。`run` から呼ばれる。
    /// テストではセットしない (no-op になる)。
    pub fn set_notif_sender(&mut self, tx: mpsc::UnboundedSender<String>) {
        self.notif_tx = Some(tx);
    }

    /// ADR-005 Phase A: Claude Code Channel mode に切り替える。
    /// `channel::run` から呼ばれる。tests / relay mode では false のまま。
    pub fn enable_channel_mode(&mut self) {
        self.channel_mode = true;
    }

    /// `kind:"event"` frame の本体 JSON を受け取って:
    /// 1. `owner` / `repo` / `issue_number` を抽出
    /// 2. `watched-issues.txt` の set にあるか filter
    /// 3. ある場合 `issue-events.jsonl` に append
    pub fn handle_event_frame(&self, frame_body: &Value) {
        let owner = frame_body.get("owner").and_then(Value::as_str);
        let repo = frame_body.get("repo").and_then(Value::as_str);
        let number = frame_body.get("issue_number").and_then(Value::as_u64);
        let (Some(owner), Some(repo), Some(number)) = (owner, repo, number) else {
            tracing::warn!(
                frame = ?frame_body,
                "event frame missing owner/repo/issue_number"
            );
            return;
        };
        let key = IssueKey::new(owner, repo, number);
        let watched = match self.watched.load() {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    path = %self.watched.path().display(),
                    "load watched-issues failed; dropping event"
                );
                return;
            }
        };
        if !watched.contains(&key) {
            tracing::debug!(
                issue = %key.as_filekey(),
                "event for unwatched issue, dropping"
            );
            return;
        }
        if let Err(e) = self.events.append_event(frame_body) {
            tracing::warn!(
                error = %e,
                issue = %key.as_filekey(),
                "append event failed (event lost)"
            );
            return;
        }
        tracing::info!(
            issue = %key.as_filekey(),
            "event buffered"
        );

        // notif_tx が None のテストでは silent no-op。
        let Some(tx) = self.notif_tx.as_ref() else {
            return;
        };

        let payload = if self.channel_mode {
            // ADR-005: Claude Code Channel notification — JSON-RPC をそのまま
            // stdio に流す形式。`<channel source="cc-relay" ...>` envelope に
            // 変換されて session context に inject される。
            let preview = serde_json::to_string(frame_body).unwrap_or_else(|_| "{}".into());
            let event_type = frame_body
                .get("event_type")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let delivery_id = frame_body
                .get("delivery_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            let notif = json!({
                "jsonrpc": "2.0",
                "method": "notifications/claude/channel",
                "params": {
                    "content": preview,
                    "meta": {
                        "event_type": event_type,
                        "owner": owner,
                        "repo": repo,
                        "issue_number": number.to_string(),
                        "delivery_id": delivery_id,
                    },
                },
            });
            // `notif` only contains `serde_json::Value` and owned `&str`,
            // whose Serialize impls are infallible.
            serde_json::to_string(&notif).expect("notif JSON serialize is infallible")
        } else {
            // ADR-004 Phase D (legacy): auth-worker McpSession DO に
            // `kind:"notif"` frame で MCP `notifications/message` を back-pipe。
            // SSE channel が attach されていれば fan-out される (現状の
            // Anthropic harness は GET /mcp を開かないので発火しない)。
            let notif_body = json!({
                "jsonrpc": "2.0",
                "method": "notifications/message",
                "params": {
                    "level": "info",
                    "logger": "cc-relay/issue-events",
                    "data": frame_body,
                },
            });
            let frame = json!({
                "kind": "notif",
                "v": FRAME_VERSION,
                "body": notif_body,
            });
            // Same rationale as the channel-mode branch above —
            // `serde_json::Value` serialize is infallible.
            serde_json::to_string(&frame).expect("notif frame JSON serialize is infallible")
        };
        if let Err(e) = tx.send(payload) {
            tracing::warn!(error = %e, "notif send failed (writer dropped)");
        }
    }

    /// Process one JSON-RPC message body and produce the response body.
    ///
    /// Returns `Ok(None)` for notifications (no `id` or `id` is `null`) so the
    /// caller can return `202 Accepted` with no body — matches the inline stub
    /// in `mcp-session-do.ts:handleInlineMcp`.
    pub async fn handle_jsonrpc(&self, body: &[u8]) -> serde_json::Result<Option<Vec<u8>>> {
        let msg: Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => {
                return Ok(Some(serde_json::to_vec(&error_response(
                    Value::Null,
                    -32700,
                    "Parse error",
                ))?));
            }
        };
        if !msg.is_object() {
            return Ok(Some(serde_json::to_vec(&error_response(
                Value::Null,
                -32600,
                "Invalid Request",
            ))?));
        }
        let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
        let id = msg.get("id").cloned().unwrap_or(Value::Null);
        let is_notification = id.is_null();
        if is_notification {
            return Ok(None);
        }
        let response = match method {
            "initialize" => self.handle_initialize(id, msg.get("params")),
            "tools/list" => self.handle_tools_list(id),
            "tools/call" => self.handle_tools_call(id, msg.get("params")).await,
            "ping" => result_response(id, json!({})),
            "prompts/list" => result_response(id, json!({ "prompts": [] })),
            "resources/list" => result_response(id, json!({ "resources": [] })),
            other => error_response(id, -32601, &format!("Method not found: {other}")),
        };
        Ok(Some(serde_json::to_vec(&response)?))
    }

    fn handle_initialize(&self, id: Value, params: Option<&Value>) -> Value {
        let proto = params
            .and_then(|p| p.get("protocolVersion"))
            .and_then(Value::as_str)
            .unwrap_or(STUB_PROTOCOL_VERSION);
        // ADR-005: channel mode は `experimental.claude/channel` 機能と
        // instructions を一緒に advertise する。Claude Code はこの機能を
        // 見て `notifications/claude/channel` listener を登録し、
        // `<channel source="cc-relay" ...>` envelope を session context に
        // inject できるようになる。
        let mut capabilities = json!({ "tools": { "listChanged": false } });
        let mut response = json!({
            "protocolVersion": proto,
            "serverInfo": { "name": SERVER_NAME, "version": SERVER_VERSION },
        });
        if self.channel_mode {
            capabilities["experimental"] = json!({ "claude/channel": {} });
            response["instructions"] = json!(CHANNEL_INSTRUCTIONS);
        }
        response["capabilities"] = capabilities;
        result_response(id, response)
    }

    fn handle_tools_list(&self, id: Value) -> Value {
        result_response(
            id,
            json!({
                "tools": [
                    {
                        "name": "cc_relay_list_agents",
                        "description": "List agents currently joined to the cc-relay session via the configured Broker.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "required": [],
                            "additionalProperties": false,
                        },
                    },
                    {
                        "name": "subscribe_issue_activity",
                        "description": "Subscribe to GitHub issue activity (comments, labels, state changes). Persists the (owner, repo, issue_number) tuple to ~/.cc-relay/watched-issues.txt. Events arriving via webhook are filtered against this set and buffered for get_issue_events. Idempotent.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "owner": { "type": "string" },
                                "repo": { "type": "string" },
                                "issue_number": { "type": "integer", "minimum": 1 }
                            },
                            "required": ["owner", "repo", "issue_number"],
                            "additionalProperties": false,
                        },
                    },
                    {
                        "name": "unsubscribe_issue_activity",
                        "description": "Unsubscribe from GitHub issue activity. Removes the entry from ~/.cc-relay/watched-issues.txt. Future events for this issue are dropped at the filter step. Idempotent.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "owner": { "type": "string" },
                                "repo": { "type": "string" },
                                "issue_number": { "type": "integer", "minimum": 1 }
                            },
                            "required": ["owner", "repo", "issue_number"],
                            "additionalProperties": false,
                        },
                    },
                    {
                        "name": "get_issue_events",
                        "description": "Drain buffered GitHub issue events received since the last call. Returns a JSON array of event objects. Subsequent calls return only newly arrived events (the file is renamed to .read on drain).",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "required": [],
                            "additionalProperties": false,
                        },
                    },
                    {
                        "name": "list_watched_issues",
                        "description": "Return the list of (owner, repo, issue_number) tuples currently subscribed via subscribe_issue_activity.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "required": [],
                            "additionalProperties": false,
                        },
                    },
                    {
                        "name": "notify_agent",
                        "description": "Send a message to another agent in the cc-relay session via the configured Broker. Use `*` for `to` to broadcast to every other agent.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "to": { "type": "string" },
                                "message": { "type": "string" },
                                "priority": {
                                    "type": "string",
                                    "enum": ["low", "normal", "high"]
                                }
                            },
                            "required": ["to", "message"],
                            "additionalProperties": false,
                        },
                    },
                    {
                        "name": "get_inbox",
                        "description": "Pull all messages addressed to this agent since the last `get_inbox` call (or since the agent first joined, on cold start). Returns a JSON array of {from, to, message, priority, timestamp}. Cursor is persisted across restarts when configured.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "required": [],
                            "additionalProperties": false,
                        },
                    },
                    {
                        "name": "get_plan",
                        "description": "Return the current shared plan as a JSON array of TaskSpec ({id, title, status, assignee?, notes?}).",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "required": [],
                            "additionalProperties": false,
                        },
                    },
                    {
                        "name": "add_task",
                        "description": "Add a new task to the shared plan. `id` must be unique within the session. `status` defaults to `pending`.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "title": { "type": "string" },
                                "status": {
                                    "type": "string",
                                    "enum": ["pending", "in_progress", "done", "cancelled"]
                                },
                                "assignee": { "type": "string" },
                                "notes": { "type": "string" }
                            },
                            "required": ["id", "title"],
                            "additionalProperties": false,
                        },
                    },
                    {
                        "name": "claim_task",
                        "description": "Take ownership of a task. Fails if already assigned to a different live agent and not yet Done/Cancelled. Assignee is this binary's `agent_id` (from broker.self_id()).",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "task_id": { "type": "string" }
                            },
                            "required": ["task_id"],
                            "additionalProperties": false,
                        },
                    },
                    {
                        "name": "update_task",
                        "description": "Update a task's status and (optionally) notes.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "task_id": { "type": "string" },
                                "status": {
                                    "type": "string",
                                    "enum": ["pending", "in_progress", "done", "cancelled"]
                                },
                                "notes": { "type": "string" }
                            },
                            "required": ["task_id", "status"],
                            "additionalProperties": false,
                        },
                    },
                    {
                        "name": "remove_task",
                        "description": "Drop a task entirely from the shared plan.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "task_id": { "type": "string" }
                            },
                            "required": ["task_id"],
                            "additionalProperties": false,
                        },
                    }
                ]
            }),
        )
    }

    async fn handle_tools_call(&self, id: Value, params: Option<&Value>) -> Value {
        let name = params
            .and_then(|p| p.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let args = params
            .and_then(|p| p.get("arguments"))
            .cloned()
            .unwrap_or(Value::Null);
        match name {
            "cc_relay_list_agents" => match self.broker.list_agents().await {
                Ok(agents) => {
                    let body = serde_json::to_string(&agents).unwrap_or_else(|_| "[]".into());
                    result_response(
                        id,
                        json!({
                            "content": [{ "type": "text", "text": body }],
                            "isError": false,
                        }),
                    )
                }
                Err(e) => result_response(
                    id,
                    json!({
                        "content": [{ "type": "text", "text": format!("broker error: {e}") }],
                        "isError": true,
                    }),
                ),
            },
            "subscribe_issue_activity" => self.tool_subscribe_issue(id, &args),
            "unsubscribe_issue_activity" => self.tool_unsubscribe_issue(id, &args),
            "get_issue_events" => self.tool_get_issue_events(id),
            "list_watched_issues" => self.tool_list_watched_issues(id),
            "notify_agent" => self.tool_notify_agent(id, &args).await,
            "get_inbox" => self.tool_get_inbox(id).await,
            "get_plan" => self.tool_get_plan(id).await,
            "add_task" => self.tool_add_task(id, &args).await,
            "claim_task" => self.tool_claim_task(id, &args).await,
            "update_task" => self.tool_update_task(id, &args).await,
            "remove_task" => self.tool_remove_task(id, &args).await,
            other => error_response(id, -32602, &format!("Unknown tool: {other}")),
        }
    }

    /// P5 #17 Phase 17.2: `notify_agent` — publish a [`NotifyMessage`] via
    /// the broker. `from` is filled from `broker.self_id()`; `priority`
    /// defaults to `Normal` when absent; `timestamp` is wall-clock ms.
    async fn tool_notify_agent(&self, id: Value, args: &Value) -> Value {
        let to = match args.get("to").and_then(Value::as_str) {
            Some(s) if !s.is_empty() => s,
            _ => return tool_text_error(id, "missing or empty 'to'"),
        };
        let message = match args.get("message").and_then(Value::as_str) {
            Some(s) => s.to_string(),
            None => return tool_text_error(id, "missing or non-string 'message'"),
        };
        let priority = match args.get("priority").and_then(Value::as_str) {
            None => Priority::Normal,
            Some("low") => Priority::Low,
            Some("normal") => Priority::Normal,
            Some("high") => Priority::High,
            Some(other) => {
                return tool_text_error(
                    id,
                    &format!("invalid 'priority' '{other}' (expected low|normal|high)"),
                )
            }
        };
        let target = if to == "*" {
            NotifyTarget::All
        } else {
            NotifyTarget::Agent(to.to_string())
        };
        let from = self.broker.self_id().to_string();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let msg = NotifyMessage {
            from,
            to: target,
            message,
            priority,
            timestamp,
        };
        match self.broker.send(msg).await {
            Ok(()) => tool_text_ok(id, "ok"),
            Err(e) => tool_text_error(id, &format!("broker error: {e}")),
        }
    }

    /// P5 #17 Phase 17.2: `get_inbox` — pull messages addressed to this
    /// agent since the persisted cursor; advance + save the cursor.
    async fn tool_get_inbox(&self, id: Value) -> Value {
        let start_cursor = self.cursor.lock().await.clone();
        let (msgs, new_cursor) = match self.broker.fetch_since(start_cursor).await {
            Ok(t) => t,
            Err(e) => return tool_text_error(id, &format!("broker error: {e}")),
        };
        {
            let mut guard = self.cursor.lock().await;
            *guard = new_cursor.clone();
        }
        if let Some(store) = self.cursor_store.as_ref() {
            if let Err(e) = store.save(&new_cursor).await {
                tracing::warn!(error = %e, "cursor save failed (continuing with in-memory only)");
            }
        }
        let body = serde_json::to_string(&msgs).unwrap_or_else(|_| "[]".into());
        tool_text_ok(id, &body)
    }

    // ────────────────────────────────────────────────────────────────────
    // P5 #17 Phase 17.3: plan ops (get_plan + add/claim/update/remove_task)
    // ────────────────────────────────────────────────────────────────────

    async fn tool_get_plan(&self, id: Value) -> Value {
        match self.broker.get_plan().await {
            Ok(plan) => {
                let body = serde_json::to_string(&plan).unwrap_or_else(|_| "[]".into());
                tool_text_ok(id, &body)
            }
            Err(e) => tool_text_error(id, &format!("broker error: {e}")),
        }
    }

    async fn tool_add_task(&self, id: Value, args: &Value) -> Value {
        let task_id = match args.get("id").and_then(Value::as_str) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => return tool_text_error(id, "missing or empty 'id'"),
        };
        let title = match args.get("title").and_then(Value::as_str) {
            Some(s) => s.to_string(),
            None => return tool_text_error(id, "missing or non-string 'title'"),
        };
        let status = match args.get("status").and_then(Value::as_str) {
            None => TaskStatus::Pending,
            Some(s) => match parse_task_status(s) {
                Some(st) => st,
                None => return tool_text_error(id, &format!("invalid 'status' '{s}'")),
            },
        };
        let assignee = args
            .get("assignee")
            .and_then(Value::as_str)
            .map(str::to_string);
        let notes = args
            .get("notes")
            .and_then(Value::as_str)
            .map(str::to_string);
        let task = TaskSpec {
            id: task_id,
            title,
            status,
            assignee,
            notes,
        };
        match self.broker.plan_op(PlanOp::Add { task }).await {
            Ok(()) => tool_text_ok(id, "ok"),
            Err(e) => tool_text_error(id, &format!("broker error: {e}")),
        }
    }

    async fn tool_claim_task(&self, id: Value, args: &Value) -> Value {
        let task_id = match args.get("task_id").and_then(Value::as_str) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => return tool_text_error(id, "missing or empty 'task_id'"),
        };
        let agent_id = self.broker.self_id().to_string();
        match self
            .broker
            .plan_op(PlanOp::Claim { task_id, agent_id })
            .await
        {
            Ok(()) => tool_text_ok(id, "ok"),
            Err(e) => tool_text_error(id, &format!("broker error: {e}")),
        }
    }

    async fn tool_update_task(&self, id: Value, args: &Value) -> Value {
        let task_id = match args.get("task_id").and_then(Value::as_str) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => return tool_text_error(id, "missing or empty 'task_id'"),
        };
        let status = match args.get("status").and_then(Value::as_str) {
            Some(s) => match parse_task_status(s) {
                Some(st) => st,
                None => return tool_text_error(id, &format!("invalid 'status' '{s}'")),
            },
            None => return tool_text_error(id, "missing or non-string 'status'"),
        };
        let notes = args
            .get("notes")
            .and_then(Value::as_str)
            .map(str::to_string);
        match self
            .broker
            .plan_op(PlanOp::Update {
                task_id,
                status,
                notes,
            })
            .await
        {
            Ok(()) => tool_text_ok(id, "ok"),
            Err(e) => tool_text_error(id, &format!("broker error: {e}")),
        }
    }

    async fn tool_remove_task(&self, id: Value, args: &Value) -> Value {
        let task_id = match args.get("task_id").and_then(Value::as_str) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => return tool_text_error(id, "missing or empty 'task_id'"),
        };
        match self.broker.plan_op(PlanOp::Remove { task_id }).await {
            Ok(()) => tool_text_ok(id, "ok"),
            Err(e) => tool_text_error(id, &format!("broker error: {e}")),
        }
    }

    fn tool_subscribe_issue(&self, id: Value, args: &Value) -> Value {
        let key = match parse_issue_args(args) {
            Ok(k) => k,
            Err(e) => return tool_text_error(id, &e),
        };
        match self.watched.add(&key) {
            Ok(added) => tool_text_ok(
                id,
                &format!(
                    "{} {}",
                    if added {
                        "subscribed:"
                    } else {
                        "already subscribed:"
                    },
                    key.as_filekey()
                ),
            ),
            Err(e) => tool_text_error(id, &format!("subscribe failed: {e}")),
        }
    }

    fn tool_unsubscribe_issue(&self, id: Value, args: &Value) -> Value {
        let key = match parse_issue_args(args) {
            Ok(k) => k,
            Err(e) => return tool_text_error(id, &e),
        };
        match self.watched.remove(&key) {
            Ok(removed) => tool_text_ok(
                id,
                &format!(
                    "{} {}",
                    if removed {
                        "unsubscribed:"
                    } else {
                        "was not subscribed:"
                    },
                    key.as_filekey()
                ),
            ),
            Err(e) => tool_text_error(id, &format!("unsubscribe failed: {e}")),
        }
    }

    fn tool_get_issue_events(&self, id: Value) -> Value {
        match self.events.drain() {
            Ok(entries) => {
                let body = serde_json::to_string(&entries).unwrap_or_else(|_| "[]".into());
                tool_text_ok(id, &body)
            }
            Err(e) => tool_text_error(id, &format!("drain failed: {e}")),
        }
    }

    fn tool_list_watched_issues(&self, id: Value) -> Value {
        match self.watched.load() {
            Ok(set) => {
                let mut list: Vec<String> = set.iter().map(IssueKey::as_filekey).collect();
                list.sort();
                let body = serde_json::to_string(&list).unwrap_or_else(|_| "[]".into());
                tool_text_ok(id, &body)
            }
            Err(e) => tool_text_error(id, &format!("load watched failed: {e}")),
        }
    }
}

/// Parse the `status` argument of `add_task` / `update_task` into the
/// internal [`TaskStatus`] enum. Returns `None` for any string outside
/// the snake_case schema, mirroring the tool's `inputSchema` enum list.
fn parse_task_status(s: &str) -> Option<TaskStatus> {
    match s {
        "pending" => Some(TaskStatus::Pending),
        "in_progress" => Some(TaskStatus::InProgress),
        "done" => Some(TaskStatus::Done),
        "cancelled" => Some(TaskStatus::Cancelled),
        _ => None,
    }
}

fn parse_issue_args(args: &Value) -> std::result::Result<IssueKey, String> {
    let owner = args
        .get("owner")
        .and_then(Value::as_str)
        .ok_or("missing or non-string 'owner'")?;
    let repo = args
        .get("repo")
        .and_then(Value::as_str)
        .ok_or("missing or non-string 'repo'")?;
    let number = args
        .get("issue_number")
        .and_then(Value::as_u64)
        .ok_or("missing or non-integer 'issue_number'")?;
    if owner.is_empty() || repo.is_empty() {
        return Err("owner and repo must not be empty".into());
    }
    if number == 0 {
        return Err("issue_number must be > 0".into());
    }
    Ok(IssueKey::new(owner, repo, number))
}

fn tool_text_ok(id: Value, text: &str) -> Value {
    result_response(
        id,
        json!({
            "content": [{ "type": "text", "text": text }],
            "isError": false,
        }),
    )
}

fn tool_text_error(id: Value, text: &str) -> Value {
    result_response(
        id,
        json!({
            "content": [{ "type": "text", "text": text }],
            "isError": true,
        }),
    )
}

/// Wrap a JSON-RPC `result` payload.
fn result_response(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// Wrap a JSON-RPC `error` payload.
fn error_response(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

/// 受信 loop 本体。real-WS connect prelude は `crate::relay_ws::run` に
/// 切り出し、ここ (`pump_inbound`) は mock-stream で完全にテスト可能。
pub(crate) async fn pump_inbound(
    server: &RelayServer,
    stream: &mut (impl StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
              + Unpin),
    out_tx: &mpsc::UnboundedSender<String>,
) -> Result<()> {
    while let Some(message) = stream.next().await {
        let message = message.context("ws stream error")?;
        let text = match message {
            Message::Text(t) => t,
            Message::Binary(b) => String::from_utf8(b).context("non-utf8 binary frame")?,
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
            Message::Close(_) => {
                tracing::info!("agent-mcp relay: ws closed by peer");
                break;
            }
        };
        // ADR-004: webhook event frames arrive on the same WS via
        // McpSession `/__push_event`. Peek at `kind` before parsing as
        // ReqFrame so we can route to the issue-events handler.
        let frame_value: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "skip malformed inbound frame (json parse)");
                continue;
            }
        };
        let kind_owned = frame_value
            .get("kind")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_default();
        if kind_owned == "event" {
            server.handle_event_frame(&frame_value);
            continue;
        }
        let req: ReqFrame = match serde_json::from_value(frame_value) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, kind = %kind_owned, "skip malformed inbound frame (req parse)");
                continue;
            }
        };
        if req.kind != "req" {
            // hello / resp / unknown — ignore on this side
            continue;
        }
        let body = if req.body_b64.is_empty() {
            Vec::new()
        } else {
            B64.decode(req.body_b64.as_bytes())
                .map_err(|e| anyhow!("body_b64 decode: {e}"))?
        };
        let (status, body_out) = match server.handle_jsonrpc(&body).await? {
            Some(out) => (200u16, out),
            None => (202u16, Vec::new()),
        };
        let mut headers = serde_json::Map::new();
        if status == 200 {
            headers.insert(
                "content-type".into(),
                Value::String("application/json".into()),
            );
        }
        let resp = RespFrame {
            kind: "resp",
            v: FRAME_VERSION,
            id: &req.id,
            status,
            headers,
            body_b64: if body_out.is_empty() {
                String::new()
            } else {
                B64.encode(&body_out)
            },
        };
        if out_tx.send(serde_json::to_string(&resp)?).is_err() {
            tracing::warn!("writer task dropped; aborting pump");
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_broker::{AgentMeta, Cursor, Result as BrokerResult};
    use agent_core::{NotifyMessage, PlanOp, TaskSpec};
    use std::sync::Mutex;

    /// Test-double Broker that lets a test pre-load the
    /// [`Broker::list_agents`] return value and inspects which method was
    /// called. Other methods either return empty success or panic — the
    /// Phase C dispatcher only routes `cc_relay_list_agents`, so the
    /// remaining methods MUST NOT be reached from the JSON-RPC path.
    struct StubBroker {
        agents: Mutex<Vec<AgentMeta>>,
        list_agents_calls: Mutex<u32>,
        force_error: bool,
        /// P5 #17 Phase 17.2: captured `send` invocations.
        sent: Mutex<Vec<NotifyMessage>>,
        /// P5 #17 Phase 17.2: payload the next `fetch_since` will return
        /// (drained on call). The cursor returned bumps `last_comment_id`
        /// by the number of messages returned.
        next_fetch: Mutex<Vec<NotifyMessage>>,
        /// When true, `send` returns Auth error to exercise the error path.
        send_error: bool,
        /// P5 #17 Phase 17.3: captured `plan_op` invocations.
        plan_ops: Mutex<Vec<PlanOp>>,
        /// P5 #17 Phase 17.3: payload `get_plan` returns.
        plan_snapshot: Mutex<Vec<TaskSpec>>,
        /// When true, `plan_op` returns Auth error to exercise the error
        /// path independently of `send_error`.
        plan_op_error: bool,
        /// When true, `fetch_since` returns Auth error.
        fetch_error: bool,
        /// When true, `get_plan` returns Auth error.
        get_plan_error: bool,
    }

    impl StubBroker {
        fn with_agents(agents: Vec<AgentMeta>) -> Arc<Self> {
            Arc::new(Self {
                agents: Mutex::new(agents),
                list_agents_calls: Mutex::new(0),
                force_error: false,
                sent: Mutex::new(vec![]),
                next_fetch: Mutex::new(vec![]),
                send_error: false,
                plan_ops: Mutex::new(vec![]),
                plan_snapshot: Mutex::new(vec![]),
                plan_op_error: false,
                fetch_error: false,
                get_plan_error: false,
            })
        }
        fn err() -> Arc<Self> {
            Arc::new(Self {
                agents: Mutex::new(vec![]),
                list_agents_calls: Mutex::new(0),
                force_error: true,
                sent: Mutex::new(vec![]),
                next_fetch: Mutex::new(vec![]),
                send_error: false,
                plan_ops: Mutex::new(vec![]),
                plan_snapshot: Mutex::new(vec![]),
                plan_op_error: false,
                fetch_error: false,
                get_plan_error: false,
            })
        }
        /// Builder for Phase 17.2 tests: pre-seed messages to be returned
        /// by the next `fetch_since` call.
        fn with_inbox(msgs: Vec<NotifyMessage>) -> Arc<Self> {
            Arc::new(Self {
                agents: Mutex::new(vec![]),
                list_agents_calls: Mutex::new(0),
                force_error: false,
                sent: Mutex::new(vec![]),
                next_fetch: Mutex::new(msgs),
                send_error: false,
                plan_ops: Mutex::new(vec![]),
                plan_snapshot: Mutex::new(vec![]),
                plan_op_error: false,
                fetch_error: false,
                get_plan_error: false,
            })
        }
        /// Builder: `send` will return an error.
        fn with_send_error() -> Arc<Self> {
            Arc::new(Self {
                agents: Mutex::new(vec![]),
                list_agents_calls: Mutex::new(0),
                force_error: false,
                sent: Mutex::new(vec![]),
                next_fetch: Mutex::new(vec![]),
                send_error: true,
                plan_ops: Mutex::new(vec![]),
                plan_snapshot: Mutex::new(vec![]),
                plan_op_error: false,
                fetch_error: false,
                get_plan_error: false,
            })
        }
        /// Builder for Phase 17.3 tests: pre-seed the plan returned by
        /// `get_plan`.
        fn with_plan(plan: Vec<TaskSpec>) -> Arc<Self> {
            Arc::new(Self {
                agents: Mutex::new(vec![]),
                list_agents_calls: Mutex::new(0),
                force_error: false,
                sent: Mutex::new(vec![]),
                next_fetch: Mutex::new(vec![]),
                send_error: false,
                plan_ops: Mutex::new(vec![]),
                plan_snapshot: Mutex::new(plan),
                plan_op_error: false,
                fetch_error: false,
                get_plan_error: false,
            })
        }
        /// Builder: `plan_op` will return an error.
        fn with_plan_op_error() -> Arc<Self> {
            Arc::new(Self {
                agents: Mutex::new(vec![]),
                list_agents_calls: Mutex::new(0),
                force_error: false,
                sent: Mutex::new(vec![]),
                next_fetch: Mutex::new(vec![]),
                send_error: false,
                plan_ops: Mutex::new(vec![]),
                plan_snapshot: Mutex::new(vec![]),
                plan_op_error: true,
                fetch_error: false,
                get_plan_error: false,
            })
        }

        fn with_fetch_error() -> Arc<Self> {
            Arc::new(Self {
                agents: Mutex::new(vec![]),
                list_agents_calls: Mutex::new(0),
                force_error: false,
                sent: Mutex::new(vec![]),
                next_fetch: Mutex::new(vec![]),
                send_error: false,
                plan_ops: Mutex::new(vec![]),
                plan_snapshot: Mutex::new(vec![]),
                plan_op_error: false,
                fetch_error: true,
                get_plan_error: false,
            })
        }

        fn with_get_plan_error() -> Arc<Self> {
            Arc::new(Self {
                agents: Mutex::new(vec![]),
                list_agents_calls: Mutex::new(0),
                force_error: false,
                sent: Mutex::new(vec![]),
                next_fetch: Mutex::new(vec![]),
                send_error: false,
                plan_ops: Mutex::new(vec![]),
                plan_snapshot: Mutex::new(vec![]),
                plan_op_error: false,
                fetch_error: false,
                get_plan_error: true,
            })
        }
    }

    #[async_trait::async_trait]
    impl Broker for StubBroker {
        async fn join(&self, _agent_id: &str) -> BrokerResult<()> {
            Ok(())
        }
        async fn leave(&self, _agent_id: &str) -> BrokerResult<()> {
            Ok(())
        }
        async fn send(&self, msg: NotifyMessage) -> BrokerResult<()> {
            if self.send_error {
                return Err(agent_broker::BrokerError::Auth("send stub".into()));
            }
            self.sent.lock().unwrap().push(msg);
            Ok(())
        }
        async fn fetch_since(&self, c: Cursor) -> BrokerResult<(Vec<NotifyMessage>, Cursor)> {
            if self.fetch_error {
                return Err(agent_broker::BrokerError::Auth("fetch stub".into()));
            }
            let drained: Vec<NotifyMessage> = std::mem::take(&mut *self.next_fetch.lock().unwrap());
            let advanced = Cursor {
                last_comment_id: c.last_comment_id + drained.len() as u64,
                last_etag: c.last_etag,
            };
            Ok((drained, advanced))
        }
        async fn list_agents(&self) -> BrokerResult<Vec<AgentMeta>> {
            *self.list_agents_calls.lock().unwrap() += 1;
            if self.force_error {
                return Err(agent_broker::BrokerError::Auth("stub".into()));
            }
            Ok(self.agents.lock().unwrap().clone())
        }
        async fn get_plan(&self) -> BrokerResult<Vec<TaskSpec>> {
            if self.get_plan_error {
                return Err(agent_broker::BrokerError::Auth("get_plan stub".into()));
            }
            Ok(self.plan_snapshot.lock().unwrap().clone())
        }
        async fn plan_op(&self, op: PlanOp) -> BrokerResult<()> {
            if self.plan_op_error {
                return Err(agent_broker::BrokerError::Auth("plan_op stub".into()));
            }
            self.plan_ops.lock().unwrap().push(op);
            Ok(())
        }
        fn self_id(&self) -> &str {
            "stub-agent"
        }
    }

    fn server() -> RelayServer {
        RelayServer::new(StubBroker::with_agents(vec![]))
    }

    /// テスト用 server: watched/events を tempdir 配下に設定して、
    /// `~/.cc-relay/*` を汚さない + 並列テストで干渉しない。
    fn server_with_tempfiles(broker: Arc<dyn Broker>) -> (RelayServer, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let watched = WatchedIssuesFile::new(dir.path().join("watched.txt"));
        let events = IssueEventsFile::new(dir.path().join("events.jsonl"));
        let srv = RelayServer::with_files(broker, watched, events);
        (srv, dir)
    }

    async fn dispatch(srv: &RelayServer, body: &str) -> Option<Value> {
        srv.handle_jsonrpc(body.as_bytes())
            .await
            .unwrap()
            .map(|b| serde_json::from_slice(&b).unwrap())
    }

    #[tokio::test]
    async fn initialize_returns_protocol_and_server_info() {
        let srv = server();
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["jsonrpc"], "2.0");
        assert_eq!(resp["id"], 1);
        assert_eq!(resp["result"]["protocolVersion"], "2025-06-18");
        assert_eq!(resp["result"]["serverInfo"]["name"], "cc-relay");
        // default mode: no `claude/channel` capability, no instructions
        assert!(resp["result"]["capabilities"]["experimental"].is_null());
        assert!(resp["result"]["instructions"].is_null());
    }

    #[tokio::test]
    async fn initialize_in_channel_mode_advertises_claude_channel_capability() {
        let mut srv = server();
        srv.enable_channel_mode();
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}"#,
        )
        .await
        .unwrap();
        assert!(resp["result"]["capabilities"]["experimental"]["claude/channel"].is_object());
        assert!(resp["result"]["instructions"]
            .as_str()
            .unwrap()
            .contains("<channel"));
    }

    #[tokio::test]
    async fn initialize_without_params_falls_back_to_default_protocol() {
        let srv = server();
        let resp = dispatch(&srv, r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#)
            .await
            .unwrap();
        assert_eq!(resp["result"]["protocolVersion"], STUB_PROTOCOL_VERSION);
    }

    #[tokio::test]
    async fn initialize_with_non_string_protocol_falls_back() {
        let srv = server();
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":42}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["protocolVersion"], STUB_PROTOCOL_VERSION);
    }

    // -----------------------------------------------------------------
    // ADR-004 Phase C: subscribe / unsubscribe / get_issue_events /
    // list_watched_issues + event frame handling
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn subscribe_then_list_watched_returns_added() {
        let (srv, _tmp) = server_with_tempfiles(StubBroker::with_agents(vec![]));
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"subscribe_issue_activity","arguments":{"owner":"ippoan","repo":"cc-relay","issue_number":42}}}"#,
        )
        .await
        .unwrap();
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("subscribed:"), "got: {text}");
        assert!(text.contains("ippoan/cc-relay#42"));

        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"list_watched_issues"}}"#,
        )
        .await
        .unwrap();
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let list: Vec<String> = serde_json::from_str(text).unwrap();
        assert_eq!(list, vec!["ippoan/cc-relay#42"]);
    }

    #[tokio::test]
    async fn subscribe_idempotent() {
        let (srv, _tmp) = server_with_tempfiles(StubBroker::with_agents(vec![]));
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"subscribe_issue_activity","arguments":{"owner":"a","repo":"b","issue_number":1}}}"#;
        let resp1 = dispatch(&srv, body).await.unwrap();
        let resp2 = dispatch(&srv, body).await.unwrap();
        assert!(resp1["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("subscribed:"));
        assert!(resp2["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("already subscribed:"));
    }

    #[tokio::test]
    async fn unsubscribe_removes_entry() {
        let (srv, _tmp) = server_with_tempfiles(StubBroker::with_agents(vec![]));
        let sub = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"subscribe_issue_activity","arguments":{"owner":"a","repo":"b","issue_number":1}}}"#;
        let unsub = r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"unsubscribe_issue_activity","arguments":{"owner":"a","repo":"b","issue_number":1}}}"#;
        dispatch(&srv, sub).await.unwrap();
        let resp = dispatch(&srv, unsub).await.unwrap();
        assert!(resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("unsubscribed:"));
        // 2 回目 unsubscribe は "was not subscribed"
        let resp = dispatch(&srv, unsub).await.unwrap();
        assert!(resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("was not subscribed:"));
    }

    #[tokio::test]
    async fn subscribe_rejects_invalid_args() {
        let (srv, _tmp) = server_with_tempfiles(StubBroker::with_agents(vec![]));
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"subscribe_issue_activity","arguments":{"owner":"","repo":"b","issue_number":1}}}"#;
        let resp = dispatch(&srv, body).await.unwrap();
        assert_eq!(resp["result"]["isError"], true);
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"subscribe_issue_activity","arguments":{"owner":"a","repo":"b","issue_number":0}}}"#;
        let resp = dispatch(&srv, body).await.unwrap();
        assert_eq!(resp["result"]["isError"], true);
    }

    #[tokio::test]
    async fn handle_event_frame_filters_unwatched() {
        let (srv, _tmp) = server_with_tempfiles(StubBroker::with_agents(vec![]));
        // 別 issue を subscribe
        let sub = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"subscribe_issue_activity","arguments":{"owner":"ippoan","repo":"cc-relay","issue_number":42}}}"#;
        dispatch(&srv, sub).await.unwrap();

        // 違う issue の event は drop される
        let frame = serde_json::json!({
            "kind": "event",
            "v": 1,
            "event_type": "issue_comment.created",
            "owner": "other",
            "repo": "repo",
            "issue_number": 99,
            "payload": {},
        });
        srv.handle_event_frame(&frame);

        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_issue_events"}}"#,
        )
        .await
        .unwrap();
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let arr: Vec<serde_json::Value> = serde_json::from_str(text).unwrap();
        assert!(arr.is_empty(), "expected empty, got: {text}");
    }

    #[tokio::test]
    async fn handle_event_frame_buffers_watched_then_drains() {
        crate::test_utils::init_tracing();
        let (srv, _tmp) = server_with_tempfiles(StubBroker::with_agents(vec![]));
        let sub = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"subscribe_issue_activity","arguments":{"owner":"ippoan","repo":"cc-relay","issue_number":42}}}"#;
        dispatch(&srv, sub).await.unwrap();

        let frame = serde_json::json!({
            "kind": "event",
            "v": 1,
            "event_type": "issue_comment.created",
            "owner": "ippoan",
            "repo": "cc-relay",
            "issue_number": 42,
            "payload": {"action": "created"},
        });
        srv.handle_event_frame(&frame);
        srv.handle_event_frame(&frame);

        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_issue_events"}}"#,
        )
        .await
        .unwrap();
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let arr: Vec<serde_json::Value> = serde_json::from_str(text).unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["event_type"], "issue_comment.created");

        // drain は rename するので 2 回目は空
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_issue_events"}}"#,
        )
        .await
        .unwrap();
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let arr: Vec<serde_json::Value> = serde_json::from_str(text).unwrap();
        assert!(arr.is_empty());
    }

    #[tokio::test]
    async fn handle_event_frame_in_channel_mode_pushes_jsonrpc_channel_notification() {
        let (mut srv, _tmp) = server_with_tempfiles(StubBroker::with_agents(vec![]));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        srv.set_notif_sender(tx);
        srv.enable_channel_mode();

        let sub = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"subscribe_issue_activity","arguments":{"owner":"ippoan","repo":"cc-relay","issue_number":42}}}"#;
        dispatch(&srv, sub).await.unwrap();

        let frame = serde_json::json!({
            "kind": "event",
            "v": 1,
            "event_type": "issue_comment.created",
            "delivery_id": "deliv-xyz",
            "owner": "ippoan",
            "repo": "cc-relay",
            "issue_number": 42,
            "payload": {"action": "created", "comment": {"body": "hi"}},
        });
        srv.handle_event_frame(&frame);

        let wire = rx.try_recv().expect("channel notif should be queued");
        let parsed: Value = serde_json::from_str(&wire).unwrap();
        assert_eq!(parsed["jsonrpc"], "2.0");
        assert_eq!(parsed["method"], "notifications/claude/channel");
        // meta must use string-only values per Claude Code Channels spec
        assert_eq!(
            parsed["params"]["meta"]["event_type"],
            "issue_comment.created"
        );
        assert_eq!(parsed["params"]["meta"]["owner"], "ippoan");
        assert_eq!(parsed["params"]["meta"]["repo"], "cc-relay");
        assert_eq!(parsed["params"]["meta"]["issue_number"], "42");
        assert_eq!(parsed["params"]["meta"]["delivery_id"], "deliv-xyz");
        // content carries the raw event JSON so Claude has full context
        let content = parsed["params"]["content"].as_str().unwrap();
        assert!(content.contains("issue_comment.created"));
        assert!(content.contains("ippoan/cc-relay") || content.contains("\"owner\":\"ippoan\""));
    }

    #[tokio::test]
    async fn handle_event_frame_in_relay_mode_still_emits_kind_notif_back_pipe() {
        // Phase D legacy back-pipe is preserved for relay (non-channel) mode.
        let (mut srv, _tmp) = server_with_tempfiles(StubBroker::with_agents(vec![]));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        srv.set_notif_sender(tx);
        // NOTE: channel_mode left at default (false) — this is the relay path.

        let sub = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"subscribe_issue_activity","arguments":{"owner":"ippoan","repo":"cc-relay","issue_number":42}}}"#;
        dispatch(&srv, sub).await.unwrap();

        let frame = serde_json::json!({
            "kind": "event",
            "v": 1,
            "event_type": "issue_comment.created",
            "owner": "ippoan",
            "repo": "cc-relay",
            "issue_number": 42,
            "payload": {"action": "created"},
        });
        srv.handle_event_frame(&frame);

        let wire = rx.try_recv().expect("relay notif should be queued");
        let parsed: Value = serde_json::from_str(&wire).unwrap();
        // Relay mode wraps the JSON-RPC body in a `kind:"notif"` WS frame
        assert_eq!(parsed["kind"], "notif");
        assert_eq!(parsed["body"]["method"], "notifications/message");
    }

    #[tokio::test]
    async fn handle_event_frame_drops_when_required_fields_missing() {
        let (srv, _tmp) = server_with_tempfiles(StubBroker::with_agents(vec![]));
        // owner/repo/issue_number どれか欠落
        let frame = serde_json::json!({
            "kind": "event",
            "v": 1,
            "event_type": "issue_comment.created",
            "owner": "ippoan",
            "issue_number": 42,
            "payload": {},
        });
        srv.handle_event_frame(&frame); // should not panic

        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_issue_events"}}"#,
        )
        .await
        .unwrap();
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let arr: Vec<serde_json::Value> = serde_json::from_str(text).unwrap();
        assert!(arr.is_empty());
    }

    #[tokio::test]
    async fn tools_list_advertises_all_tools() {
        let srv = server();
        let resp = dispatch(&srv, r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#)
            .await
            .unwrap();
        let tools = resp["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
        // ADR-004 Phase C: subscribe/unsubscribe/get_issue_events/list_watched_issues 追加。
        assert!(names.contains(&"cc_relay_list_agents"));
        assert!(names.contains(&"subscribe_issue_activity"));
        assert!(names.contains(&"unsubscribe_issue_activity"));
        assert!(names.contains(&"get_issue_events"));
        assert!(names.contains(&"list_watched_issues"));
        // P5 #17 Phase 17.2: broker-backed agent comms.
        assert!(names.contains(&"notify_agent"));
        assert!(names.contains(&"get_inbox"));
        // P5 #17 Phase 17.3: shared plan ops.
        assert!(names.contains(&"get_plan"));
        assert!(names.contains(&"add_task"));
        assert!(names.contains(&"claim_task"));
        assert!(names.contains(&"update_task"));
        assert!(names.contains(&"remove_task"));
    }

    #[tokio::test]
    async fn tools_call_list_agents_returns_broker_roster_as_json_text() {
        let broker = StubBroker::with_agents(vec![
            AgentMeta {
                agent_id: "alice".into(),
                joined_at: 1_700_000_000_000,
            },
            AgentMeta {
                agent_id: "bob".into(),
                joined_at: 1_700_000_000_001,
            },
        ]);
        let srv = RelayServer::new(broker.clone());
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"cc_relay_list_agents"}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], false);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let parsed: Vec<AgentMeta> = serde_json::from_str(text).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(*broker.list_agents_calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn tools_call_list_agents_propagates_broker_error_as_is_error_true() {
        let srv = RelayServer::new(StubBroker::err());
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"cc_relay_list_agents"}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("broker error"));
    }

    #[tokio::test]
    async fn tools_call_unknown_tool_returns_minus_32602() {
        let srv = server();
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"missing"}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn tools_call_without_name_returns_minus_32602() {
        let srv = server();
        let resp = dispatch(&srv, r#"{"jsonrpc":"2.0","id":5,"method":"tools/call"}"#)
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn ping_returns_empty_result() {
        let srv = server();
        let resp = dispatch(&srv, r#"{"jsonrpc":"2.0","id":6,"method":"ping"}"#)
            .await
            .unwrap();
        assert_eq!(resp["result"], json!({}));
    }

    #[tokio::test]
    async fn prompts_and_resources_list_return_empty_arrays() {
        let srv = server();
        let p = dispatch(&srv, r#"{"jsonrpc":"2.0","id":7,"method":"prompts/list"}"#)
            .await
            .unwrap();
        assert_eq!(p["result"]["prompts"], json!([]));
        let r = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":8,"method":"resources/list"}"#,
        )
        .await
        .unwrap();
        assert_eq!(r["result"]["resources"], json!([]));
    }

    #[tokio::test]
    async fn unknown_method_returns_minus_32601() {
        let srv = server();
        let resp = dispatch(&srv, r#"{"jsonrpc":"2.0","id":9,"method":"unknown/x"}"#)
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], -32601);
    }

    #[tokio::test]
    async fn notification_no_id_returns_none() {
        let srv = server();
        let out = srv
            .handle_jsonrpc(br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
            .await
            .unwrap();
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn notification_id_null_returns_none() {
        let srv = server();
        let out = srv
            .handle_jsonrpc(br#"{"jsonrpc":"2.0","id":null,"method":"notifications/initialized"}"#)
            .await
            .unwrap();
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn malformed_json_returns_minus_32700() {
        let srv = server();
        let resp = dispatch(&srv, "{not json").await.unwrap();
        assert_eq!(resp["error"]["code"], -32700);
        assert!(resp["id"].is_null());
    }

    #[tokio::test]
    async fn non_object_body_returns_minus_32600() {
        let srv = server();
        let resp = dispatch(&srv, "42").await.unwrap();
        assert_eq!(resp["error"]["code"], -32600);
    }

    #[tokio::test]
    async fn missing_method_treated_as_empty_returns_minus_32601() {
        let srv = server();
        let resp = dispatch(&srv, r#"{"jsonrpc":"2.0","id":11}"#)
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], -32601);
    }

    // ────────────────────────────────────────────────────────────────────
    // P5 #17 Phase 17.2: notify_agent + get_inbox + cursor persistence
    // ────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn tools_call_notify_agent_invokes_broker_send_with_self_id_as_from() {
        let broker = StubBroker::with_agents(vec![]);
        let srv = RelayServer::new(broker.clone());
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"notify_agent","arguments":{"to":"alice","message":"hi","priority":"high"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], serde_json::Value::Bool(false));
        let sent = broker.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let m = &sent[0];
        assert_eq!(m.from, "stub-agent");
        assert!(matches!(&m.to, NotifyTarget::Agent(id) if id == "alice"));
        assert_eq!(m.message, "hi");
        assert!(matches!(m.priority, Priority::High));
        assert!(m.timestamp > 0);
    }

    #[tokio::test]
    async fn tools_call_notify_agent_broadcast_maps_star_to_all() {
        let broker = StubBroker::with_agents(vec![]);
        let srv = RelayServer::new(broker.clone());
        let _ = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"notify_agent","arguments":{"to":"*","message":"all hands"}}}"#,
        )
        .await
        .unwrap();
        let sent = broker.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(matches!(sent[0].to, NotifyTarget::All));
        // priority defaults to Normal when omitted
        assert!(matches!(sent[0].priority, Priority::Normal));
    }

    #[tokio::test]
    async fn tools_call_notify_agent_propagates_broker_error_as_is_error_true() {
        let broker = StubBroker::with_send_error();
        let srv = RelayServer::new(broker);
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"notify_agent","arguments":{"to":"alice","message":"hi"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], serde_json::Value::Bool(true));
        let text = resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_default();
        assert!(text.contains("broker error"), "text was: {text}");
    }

    #[tokio::test]
    async fn tools_call_notify_agent_rejects_invalid_priority() {
        let broker = StubBroker::with_agents(vec![]);
        let srv = RelayServer::new(broker.clone());
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"notify_agent","arguments":{"to":"alice","message":"hi","priority":"urgent"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], serde_json::Value::Bool(true));
        // No broker.send call should have happened.
        assert!(broker.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn tools_call_get_inbox_returns_fetch_since_results_as_json_array() {
        let msgs = vec![
            NotifyMessage {
                from: "alice".into(),
                to: NotifyTarget::Agent("stub-agent".into()),
                message: "ping".into(),
                priority: Priority::Normal,
                timestamp: 1_700_000_000_000,
            },
            NotifyMessage {
                from: "bob".into(),
                to: NotifyTarget::All,
                message: "all hands".into(),
                priority: Priority::High,
                timestamp: 1_700_000_000_001,
            },
        ];
        let broker = StubBroker::with_inbox(msgs);
        let srv = RelayServer::new(broker);
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_inbox"}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], serde_json::Value::Bool(false));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let arr: Vec<serde_json::Value> = serde_json::from_str(text).unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["from"], "alice");
        assert_eq!(arr[0]["message"], "ping");
        assert_eq!(arr[1]["from"], "bob");
    }

    #[tokio::test]
    async fn tools_call_get_inbox_advances_in_memory_cursor() {
        let broker = StubBroker::with_inbox(vec![NotifyMessage {
            from: "alice".into(),
            to: NotifyTarget::Agent("stub-agent".into()),
            message: "ping".into(),
            priority: Priority::Normal,
            timestamp: 1,
        }]);
        let srv = RelayServer::new(broker.clone());
        // first call → 1 msg
        let _ = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_inbox"}}"#,
        )
        .await
        .unwrap();
        // StubBroker's next_fetch was drained → second call returns empty
        // *and* the cursor should be at 1 (so the request is still valid).
        let resp2 = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_inbox"}}"#,
        )
        .await
        .unwrap();
        let text = resp2["result"]["content"][0]["text"].as_str().unwrap();
        let arr: Vec<serde_json::Value> = serde_json::from_str(text).unwrap();
        assert_eq!(arr.len(), 0);
        // verify the in-memory cursor advanced to last_comment_id = 1
        let c = srv.cursor.lock().await;
        assert_eq!(c.last_comment_id, 1);
    }

    #[tokio::test]
    async fn tools_call_get_inbox_persists_cursor_to_store() {
        let broker = StubBroker::with_inbox(vec![NotifyMessage {
            from: "alice".into(),
            to: NotifyTarget::Agent("stub-agent".into()),
            message: "ping".into(),
            priority: Priority::Normal,
            timestamp: 1,
        }]);
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(CursorStore::at_path(dir.path().join("cursor.json")));
        let srv = RelayServer::new(broker)
            .with_persisted_cursor(store.clone())
            .await;
        let _ = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_inbox"}}"#,
        )
        .await
        .unwrap();
        // file should now exist and round-trip a cursor at last_comment_id=1
        let loaded = store.load().await;
        assert_eq!(loaded.last_comment_id, 1);
    }

    // ────────────────────────────────────────────────────────────────────
    // P5 #17 Phase 17.3: get_plan + add/claim/update/remove_task
    // ────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn tools_call_get_plan_returns_broker_snapshot_as_json_array() {
        let plan = vec![
            TaskSpec {
                id: "T1".into(),
                title: "Review auth".into(),
                status: TaskStatus::Pending,
                assignee: None,
                notes: None,
            },
            TaskSpec {
                id: "T2".into(),
                title: "Wire broker".into(),
                status: TaskStatus::InProgress,
                assignee: Some("alice".into()),
                notes: Some("WIP".into()),
            },
        ];
        let broker = StubBroker::with_plan(plan);
        let srv = RelayServer::new(broker);
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_plan"}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], serde_json::Value::Bool(false));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let arr: Vec<serde_json::Value> = serde_json::from_str(text).unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["id"], "T1");
        assert_eq!(arr[0]["status"], "pending");
        assert_eq!(arr[1]["assignee"], "alice");
        assert_eq!(arr[1]["status"], "in_progress");
    }

    #[tokio::test]
    async fn tools_call_add_task_invokes_broker_plan_op_add() {
        let broker = StubBroker::with_agents(vec![]);
        let srv = RelayServer::new(broker.clone());
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"add_task","arguments":{"id":"T1","title":"Review","status":"in_progress","assignee":"alice","notes":"WIP"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], serde_json::Value::Bool(false));
        let ops = broker.plan_ops.lock().unwrap();
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            PlanOp::Add { task } => {
                assert_eq!(task.id, "T1");
                assert_eq!(task.title, "Review");
                assert!(matches!(task.status, TaskStatus::InProgress));
                assert_eq!(task.assignee.as_deref(), Some("alice"));
                assert_eq!(task.notes.as_deref(), Some("WIP"));
            }
            other => panic!("expected PlanOp::Add, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tools_call_add_task_defaults_status_to_pending_when_omitted() {
        let broker = StubBroker::with_agents(vec![]);
        let srv = RelayServer::new(broker.clone());
        let _ = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"add_task","arguments":{"id":"T2","title":"Plain task"}}}"#,
        )
        .await
        .unwrap();
        let ops = broker.plan_ops.lock().unwrap();
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], PlanOp::Add { .. }));
        if let PlanOp::Add { task } = &ops[0] {
            assert!(matches!(task.status, TaskStatus::Pending));
            assert!(task.assignee.is_none());
            assert!(task.notes.is_none());
        }
    }

    #[tokio::test]
    async fn tools_call_claim_task_uses_self_id_as_assignee() {
        let broker = StubBroker::with_agents(vec![]);
        let srv = RelayServer::new(broker.clone());
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"claim_task","arguments":{"task_id":"T1"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], serde_json::Value::Bool(false));
        let ops = broker.plan_ops.lock().unwrap();
        match &ops[0] {
            PlanOp::Claim { task_id, agent_id } => {
                assert_eq!(task_id, "T1");
                assert_eq!(agent_id, "stub-agent");
            }
            other => panic!("expected PlanOp::Claim, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tools_call_update_task_passes_status_and_notes() {
        let broker = StubBroker::with_agents(vec![]);
        let srv = RelayServer::new(broker.clone());
        let _ = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"update_task","arguments":{"task_id":"T1","status":"done","notes":"shipped"}}}"#,
        )
        .await
        .unwrap();
        let ops = broker.plan_ops.lock().unwrap();
        match &ops[0] {
            PlanOp::Update {
                task_id,
                status,
                notes,
            } => {
                assert_eq!(task_id, "T1");
                assert!(matches!(status, TaskStatus::Done));
                assert_eq!(notes.as_deref(), Some("shipped"));
            }
            other => panic!("expected PlanOp::Update, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tools_call_update_task_rejects_invalid_status() {
        let broker = StubBroker::with_agents(vec![]);
        let srv = RelayServer::new(broker.clone());
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"update_task","arguments":{"task_id":"T1","status":"reopen"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], serde_json::Value::Bool(true));
        assert!(broker.plan_ops.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn tools_call_remove_task_invokes_broker_plan_op_remove() {
        let broker = StubBroker::with_agents(vec![]);
        let srv = RelayServer::new(broker.clone());
        let _ = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"remove_task","arguments":{"task_id":"T9"}}}"#,
        )
        .await
        .unwrap();
        let ops = broker.plan_ops.lock().unwrap();
        match &ops[0] {
            PlanOp::Remove { task_id } => assert_eq!(task_id, "T9"),
            other => panic!("expected PlanOp::Remove, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tools_call_plan_op_propagates_broker_error_as_is_error_true() {
        let broker = StubBroker::with_plan_op_error();
        let srv = RelayServer::new(broker);
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"claim_task","arguments":{"task_id":"T1"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], serde_json::Value::Bool(true));
        let text = resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_default();
        assert!(text.contains("broker error"), "text was: {text}");
    }

    // ────────────────────────────────────────────────────────────────────
    // Coverage: tool input-validation error branches.
    // ────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn notify_agent_rejects_missing_to() {
        let srv = server();
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"notify_agent","arguments":{"message":"hi"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("'to'"), "got: {text}");
    }

    #[tokio::test]
    async fn notify_agent_rejects_missing_message() {
        let srv = server();
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"notify_agent","arguments":{"to":"alice"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("'message'"), "got: {text}");
    }

    #[tokio::test]
    async fn get_inbox_propagates_broker_error() {
        let srv = RelayServer::new(StubBroker::with_fetch_error());
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_inbox"}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("broker error"), "got: {text}");
    }

    #[tokio::test]
    async fn get_inbox_warns_when_cursor_save_fails() {
        // Persist into a path where the parent is a regular file; CursorStore::save
        // will fail to create the directory. The handler must NOT return error;
        // it logs warn and continues.
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("not_a_dir");
        std::fs::write(&blocker, b"i am a file").unwrap();
        // child path under a file → mkdir will fail
        let store = Arc::new(CursorStore::at_path(
            blocker.join("sub").join("cursor.json"),
        ));
        let broker = StubBroker::with_inbox(vec![NotifyMessage {
            from: "alice".into(),
            to: NotifyTarget::Agent("stub-agent".into()),
            message: "ping".into(),
            priority: Priority::Normal,
            timestamp: 1,
        }]);
        let srv = RelayServer::new(broker).with_persisted_cursor(store).await;
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_inbox"}}"#,
        )
        .await
        .unwrap();
        // We still get an OK response (warn + continue, not error).
        assert_eq!(resp["result"]["isError"], false);
    }

    #[tokio::test]
    async fn get_plan_propagates_broker_error() {
        let srv = RelayServer::new(StubBroker::with_get_plan_error());
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_plan"}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("broker error"));
    }

    #[tokio::test]
    async fn add_task_validation_errors() {
        let srv = server();
        // missing id
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"add_task","arguments":{"title":"x"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        // missing title
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"add_task","arguments":{"id":"T1"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        // invalid status
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"add_task","arguments":{"id":"T1","title":"x","status":"bogus"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
    }

    #[tokio::test]
    async fn add_task_propagates_broker_error() {
        let srv = RelayServer::new(StubBroker::with_plan_op_error());
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"add_task","arguments":{"id":"T1","title":"x"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("broker error"));
    }

    #[tokio::test]
    async fn claim_task_rejects_missing_task_id() {
        let srv = server();
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"claim_task","arguments":{}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
    }

    #[tokio::test]
    async fn update_task_validation_errors() {
        let srv = server();
        // missing task_id
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"update_task","arguments":{"status":"done"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        // missing status
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"update_task","arguments":{"task_id":"T1"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
    }

    #[tokio::test]
    async fn update_task_propagates_broker_error() {
        let srv = RelayServer::new(StubBroker::with_plan_op_error());
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"update_task","arguments":{"task_id":"T1","status":"done"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        assert!(resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("broker error"));
    }

    #[tokio::test]
    async fn remove_task_rejects_missing_task_id() {
        let srv = server();
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"remove_task","arguments":{}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
    }

    #[tokio::test]
    async fn remove_task_propagates_broker_error() {
        let srv = RelayServer::new(StubBroker::with_plan_op_error());
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"remove_task","arguments":{"task_id":"T1"}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
    }

    #[tokio::test]
    async fn subscribe_unsubscribe_propagate_io_errors() {
        // Force load() to fail by making the watched path a *directory*:
        // `read_to_string` then returns an Err that is not NotFound.
        let dir = tempfile::tempdir().unwrap();
        let watched_path = dir.path().join("watched.txt");
        std::fs::create_dir_all(&watched_path).unwrap();
        let watched = WatchedIssuesFile::new(watched_path);
        let events = IssueEventsFile::new(dir.path().join("events.jsonl"));
        let srv = RelayServer::with_files(StubBroker::with_agents(vec![]), watched, events);

        // subscribe path → "subscribe failed:"
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"subscribe_issue_activity","arguments":{"owner":"a","repo":"b","issue_number":1}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("subscribe failed:"), "got: {text}");

        // unsubscribe path → "unsubscribe failed:"
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"unsubscribe_issue_activity","arguments":{"owner":"a","repo":"b","issue_number":1}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        assert!(resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("unsubscribe failed:"));

        // list_watched_issues path → "load watched failed:"
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_watched_issues"}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        assert!(resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("load watched failed:"));
    }

    #[tokio::test]
    async fn get_issue_events_propagates_drain_error() {
        // Make events path a directory → drain()'s read_to_string fails.
        let dir = tempfile::tempdir().unwrap();
        let watched = WatchedIssuesFile::new(dir.path().join("watched.txt"));
        let events_path = dir.path().join("events.jsonl");
        std::fs::create_dir_all(&events_path).unwrap();
        let events = IssueEventsFile::new(events_path);
        let srv = RelayServer::with_files(StubBroker::with_agents(vec![]), watched, events);
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_issue_events"}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        assert!(resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("drain failed:"));
    }

    #[tokio::test]
    async fn subscribe_invalid_args_owner_missing() {
        // parse_issue_args owner missing branch
        let (srv, _tmp) = server_with_tempfiles(StubBroker::with_agents(vec![]));
        let resp = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"unsubscribe_issue_activity","arguments":{"repo":"b","issue_number":1}}}"#,
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
    }

    // ────────────────────────────────────────────────────────────────────
    // handle_event_frame error branches: load() and append_event() both error.
    // ────────────────────────────────────────────────────────────────────

    #[test]
    fn handle_event_frame_load_error_drops_frame() {
        crate::test_utils::init_tracing();
        // Watched path is a directory — load() returns Err.
        let dir = tempfile::tempdir().unwrap();
        let watched_path = dir.path().join("watched.txt");
        std::fs::create_dir_all(&watched_path).unwrap();
        let watched = WatchedIssuesFile::new(watched_path);
        let events = IssueEventsFile::new(dir.path().join("events.jsonl"));
        let srv = RelayServer::with_files(StubBroker::with_agents(vec![]), watched, events);
        let frame = json!({
            "kind": "event",
            "owner": "a",
            "repo": "b",
            "issue_number": 1,
            "event_type": "x",
        });
        srv.handle_event_frame(&frame); // must not panic; logs warn and returns
    }

    #[test]
    fn handle_event_frame_unwatched_logs_debug() {
        crate::test_utils::init_tracing();
        // watched is empty, so frame for unsubscribed issue hits the "unwatched"
        // debug branch (lines 237-241).
        let dir = tempfile::tempdir().unwrap();
        let watched = WatchedIssuesFile::new(dir.path().join("watched.txt"));
        let events = IssueEventsFile::new(dir.path().join("events.jsonl"));
        let srv = RelayServer::with_files(StubBroker::with_agents(vec![]), watched, events);
        let frame = json!({
            "kind": "event",
            "owner": "a",
            "repo": "b",
            "issue_number": 99,
            "event_type": "x",
        });
        srv.handle_event_frame(&frame);
    }

    #[test]
    fn handle_event_frame_append_error_drops_frame() {
        crate::test_utils::init_tracing();
        // Watched contains the key, but events path is a directory → append fails.
        let dir = tempfile::tempdir().unwrap();
        let watched = WatchedIssuesFile::new(dir.path().join("watched.txt"));
        watched.add(&IssueKey::new("a", "b", 1)).unwrap();
        // Make the events file path a directory → OpenOptions::open fails.
        let events_path = dir.path().join("events.jsonl");
        std::fs::create_dir_all(&events_path).unwrap();
        let events = IssueEventsFile::new(events_path);
        let srv = RelayServer::with_files(StubBroker::with_agents(vec![]), watched, events);
        let frame = json!({
            "kind": "event",
            "owner": "a",
            "repo": "b",
            "issue_number": 1,
            "event_type": "x",
        });
        srv.handle_event_frame(&frame);
    }

    #[tokio::test]
    async fn handle_event_frame_warns_when_notif_receiver_dropped() {
        // After the receiver is dropped, the `tx.send(payload)` call on
        // line 322 returns Err and we log warn (line 323).
        let (mut srv, _tmp) = server_with_tempfiles(StubBroker::with_agents(vec![]));
        let (tx, rx) = mpsc::unbounded_channel::<String>();
        srv.set_notif_sender(tx);
        let sub = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"subscribe_issue_activity","arguments":{"owner":"a","repo":"b","issue_number":1}}}"#;
        dispatch(&srv, sub).await.unwrap();
        drop(rx); // close the channel so send() fails
        let frame = json!({
            "kind": "event",
            "owner": "a",
            "repo": "b",
            "issue_number": 1,
            "event_type": "x",
        });
        srv.handle_event_frame(&frame);
    }

    // ────────────────────────────────────────────────────────────────────
    // pump_inbound coverage — exercise every branch with stream::iter.
    // ────────────────────────────────────────────────────────────────────

    type WsItem = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>;

    fn req_frame(id: &str, method: &str) -> Message {
        let inner = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
        })
        .to_string();
        let body_b64 = B64.encode(inner.as_bytes());
        Message::Text(
            json!({
                "kind": "req",
                "v": 1,
                "id": id,
                "body_b64": body_b64,
            })
            .to_string(),
        )
    }

    #[tokio::test]
    async fn pump_inbound_handles_text_req_and_responds() {
        let server = RelayServer::new(StubBroker::with_agents(vec![]));
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let items: Vec<WsItem> = vec![
            // Ping/Pong/Frame are skipped
            Ok(Message::Ping(vec![1, 2, 3])),
            Ok(Message::Pong(vec![4])),
            // A valid request — should produce a resp frame.
            Ok(req_frame("r1", "ping")),
            // A graceful close — pump exits.
            Ok(Message::Close(None)),
        ];
        let mut stream = futures_util::stream::iter(items);
        super::pump_inbound(&server, &mut stream, &tx)
            .await
            .unwrap();
        // exactly one resp frame should have been pushed
        let wire = rx.try_recv().expect("a resp frame should be queued");
        let v: Value = serde_json::from_str(&wire).unwrap();
        assert_eq!(v["kind"], "resp");
        assert_eq!(v["id"], "r1");
        assert_eq!(v["status"], 200);
        // body_b64 decodes to a JSON-RPC ping result
        let body = B64.decode(v["body_b64"].as_str().unwrap()).unwrap();
        let resp: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(resp["id"], "r1");
    }

    #[tokio::test]
    async fn pump_inbound_decodes_binary_text() {
        // Binary frame with valid utf8 body containing a req frame.
        let server = RelayServer::new(StubBroker::with_agents(vec![]));
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let inner = json!({"jsonrpc": "2.0", "id": "r2", "method": "ping"}).to_string();
        let body_b64 = B64.encode(inner.as_bytes());
        let frame = json!({
            "kind": "req", "v": 1, "id": "r2", "body_b64": body_b64,
        })
        .to_string();
        let items: Vec<WsItem> = vec![
            Ok(Message::Binary(frame.into_bytes())),
            Ok(Message::Close(None)),
        ];
        let mut stream = futures_util::stream::iter(items);
        super::pump_inbound(&server, &mut stream, &tx)
            .await
            .unwrap();
        let wire = rx.try_recv().expect("resp expected");
        let v: Value = serde_json::from_str(&wire).unwrap();
        assert_eq!(v["id"], "r2");
    }

    #[tokio::test]
    async fn pump_inbound_routes_event_frame_to_handler() {
        let (mut server, _tmp) = server_with_tempfiles(StubBroker::with_agents(vec![]));
        // make sure the notif tx is wired so the event handler exercises it
        let (notif_tx, _notif_rx) = mpsc::unbounded_channel::<String>();
        server.set_notif_sender(notif_tx);
        // subscribe to the issue
        let sub = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"subscribe_issue_activity","arguments":{"owner":"a","repo":"b","issue_number":1}}}"#;
        dispatch(&server, sub).await.unwrap();

        let (tx, _rx) = mpsc::unbounded_channel::<String>();
        let event_frame = Message::Text(
            json!({
                "kind": "event",
                "v": 1,
                "owner": "a",
                "repo": "b",
                "issue_number": 1,
                "event_type": "issue_comment.created",
                "delivery_id": "deliv-1",
            })
            .to_string(),
        );
        let items: Vec<WsItem> = vec![Ok(event_frame), Ok(Message::Close(None))];
        let mut stream = futures_util::stream::iter(items);
        super::pump_inbound(&server, &mut stream, &tx)
            .await
            .unwrap();
        // Verify the handler buffered the event.
        let resp = dispatch(
            &server,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_issue_events"}}"#,
        )
        .await
        .unwrap();
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let arr: Vec<Value> = serde_json::from_str(text).unwrap();
        assert_eq!(arr.len(), 1);
    }

    #[tokio::test]
    async fn pump_inbound_skips_malformed_json_frame() {
        let server = RelayServer::new(StubBroker::with_agents(vec![]));
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let items: Vec<WsItem> = vec![
            Ok(Message::Text("{not json".into())),
            // Then a valid req so we know pump kept going.
            Ok(req_frame("after", "ping")),
            Ok(Message::Close(None)),
        ];
        let mut stream = futures_util::stream::iter(items);
        super::pump_inbound(&server, &mut stream, &tx)
            .await
            .unwrap();
        let wire = rx.try_recv().expect("resp after malformed");
        assert!(wire.contains("\"id\":\"after\""));
    }

    #[tokio::test]
    async fn pump_inbound_skips_malformed_req_frame_then_continues() {
        let server = RelayServer::new(StubBroker::with_agents(vec![]));
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        // Frame is valid JSON but `id` is a number, not a string → ReqFrame parse fails.
        let bad = Message::Text(json!({"kind": "req", "v": 1, "id": 7}).to_string());
        let items: Vec<WsItem> = vec![
            Ok(bad),
            Ok(req_frame("after", "ping")),
            Ok(Message::Close(None)),
        ];
        let mut stream = futures_util::stream::iter(items);
        super::pump_inbound(&server, &mut stream, &tx)
            .await
            .unwrap();
        let wire = rx.try_recv().expect("resp after bad req");
        assert!(wire.contains("\"id\":\"after\""));
    }

    #[tokio::test]
    async fn pump_inbound_ignores_non_req_frames() {
        let server = RelayServer::new(StubBroker::with_agents(vec![]));
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let items: Vec<WsItem> = vec![
            Ok(Message::Text(
                json!({"kind": "hello", "v": 1, "binary_version": "x", "proto": 1}).to_string(),
            )),
            Ok(Message::Close(None)),
        ];
        let mut stream = futures_util::stream::iter(items);
        super::pump_inbound(&server, &mut stream, &tx)
            .await
            .unwrap();
        assert!(rx.try_recv().is_err(), "no resp expected for hello frame");
    }

    #[tokio::test]
    async fn pump_inbound_returns_202_and_empty_body_for_notification() {
        let server = RelayServer::new(StubBroker::with_agents(vec![]));
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        // Wrap a JSON-RPC notification (no `id`) — handle_jsonrpc returns None → status 202, no body.
        let inner = json!({"jsonrpc": "2.0", "method": "notifications/initialized"}).to_string();
        let body_b64 = B64.encode(inner.as_bytes());
        let frame = Message::Text(
            json!({"kind": "req", "v": 1, "id": "notif1", "body_b64": body_b64}).to_string(),
        );
        let items: Vec<WsItem> = vec![Ok(frame), Ok(Message::Close(None))];
        let mut stream = futures_util::stream::iter(items);
        super::pump_inbound(&server, &mut stream, &tx)
            .await
            .unwrap();
        let wire = rx.try_recv().expect("resp expected");
        let v: Value = serde_json::from_str(&wire).unwrap();
        assert_eq!(v["status"], 202);
        assert_eq!(v["body_b64"], "");
    }

    #[tokio::test]
    async fn pump_inbound_handles_empty_body_b64_as_empty_request() {
        let server = RelayServer::new(StubBroker::with_agents(vec![]));
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        // An empty body_b64 → handle_jsonrpc receives an empty buffer →
        // serde_json::from_slice fails → -32700 Parse error response.
        let frame = Message::Text(
            json!({"kind": "req", "v": 1, "id": "empty", "body_b64": ""}).to_string(),
        );
        let items: Vec<WsItem> = vec![Ok(frame), Ok(Message::Close(None))];
        let mut stream = futures_util::stream::iter(items);
        super::pump_inbound(&server, &mut stream, &tx)
            .await
            .unwrap();
        let wire = rx.try_recv().expect("resp expected");
        let v: Value = serde_json::from_str(&wire).unwrap();
        assert_eq!(v["status"], 200);
        let body = B64.decode(v["body_b64"].as_str().unwrap()).unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["error"]["code"], -32700);
    }

    #[tokio::test]
    async fn pump_inbound_returns_error_on_invalid_body_b64() {
        let server = RelayServer::new(StubBroker::with_agents(vec![]));
        let (tx, _rx) = mpsc::unbounded_channel::<String>();
        let frame = Message::Text(
            json!({"kind": "req", "v": 1, "id": "bad", "body_b64": "!!!not base64!!!"}).to_string(),
        );
        let items: Vec<WsItem> = vec![Ok(frame)];
        let mut stream = futures_util::stream::iter(items);
        let err = super::pump_inbound(&server, &mut stream, &tx)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("body_b64 decode"), "got: {err}");
    }

    #[tokio::test]
    async fn pump_inbound_propagates_stream_error() {
        let server = RelayServer::new(StubBroker::with_agents(vec![]));
        let (tx, _rx) = mpsc::unbounded_channel::<String>();
        let items: Vec<WsItem> = vec![Err(tokio_tungstenite::tungstenite::Error::AlreadyClosed)];
        let mut stream = futures_util::stream::iter(items);
        let err = super::pump_inbound(&server, &mut stream, &tx)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ws stream error"));
    }

    #[tokio::test]
    async fn pump_inbound_errors_on_non_utf8_binary() {
        let server = RelayServer::new(StubBroker::with_agents(vec![]));
        let (tx, _rx) = mpsc::unbounded_channel::<String>();
        let items: Vec<WsItem> = vec![Ok(Message::Binary(vec![0xff, 0xfe, 0xfd]))];
        let mut stream = futures_util::stream::iter(items);
        let err = super::pump_inbound(&server, &mut stream, &tx)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("non-utf8"));
    }

    #[tokio::test]
    async fn pump_inbound_aborts_when_writer_dropped() {
        let server = RelayServer::new(StubBroker::with_agents(vec![]));
        let (tx, rx) = mpsc::unbounded_channel::<String>();
        drop(rx);
        let items: Vec<WsItem> = vec![Ok(req_frame("orphan", "ping"))];
        let mut stream = futures_util::stream::iter(items);
        // The send fails → break and Ok(()) returned.
        super::pump_inbound(&server, &mut stream, &tx)
            .await
            .unwrap();
    }

    // ────────────────────────────────────────────────────────────────────
    // run() — only test the upfront request-construction failure path.
    // The actual ws connect is unreachable from a unit test.
    // ────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn run_returns_error_for_invalid_url() {
        let server = RelayServer::new(StubBroker::with_agents(vec![]));
        let cfg = RelayConfig {
            ws_url: "not a url".into(),
            access_token: "tok".into(),
        };
        let err = crate::relay_ws::run(server, cfg).await.unwrap_err();
        assert!(err.to_string().contains("invalid ws url"), "got: {err}");
    }

    #[tokio::test]
    async fn run_returns_error_for_non_ascii_token() {
        let server = RelayServer::new(StubBroker::with_agents(vec![]));
        let cfg = RelayConfig {
            ws_url: "ws://127.0.0.1:1/".into(),
            // a non-ascii char in the token forces HeaderValue::from_str to fail
            access_token: "tok\u{00e9}".into(),
        };
        let err = crate::relay_ws::run(server, cfg).await.unwrap_err();
        assert!(
            err.to_string().contains("invalid bearer token")
                || err.to_string().contains("ws connect"),
            "got: {err}"
        );
    }

    // ────────────────────────────────────────────────────────────────────
    // StubBroker join/leave coverage — directly call the trait methods.
    // ────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn stub_broker_join_leave_are_ok() {
        let b = StubBroker::with_agents(vec![]);
        b.join("x").await.unwrap();
        b.leave("x").await.unwrap();
    }

    #[tokio::test]
    async fn add_task_panic_else_branch_is_unreachable_for_pending() {
        // The `let PlanOp::Add { task } = &ops[0] else { panic!("expected Add") };`
        // branch in the existing test is unreachable when StubBroker records the op
        // correctly — covered indirectly by add_task_propagates_broker_error +
        // tools_call_add_task_defaults_status_to_pending_when_omitted. This test
        // intentionally exercises both paths once more so the irrefutable bind path
        // shows live coverage.
        let broker = StubBroker::with_agents(vec![]);
        let srv = RelayServer::new(broker.clone());
        let _ = dispatch(
            &srv,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"add_task","arguments":{"id":"X","title":"t"}}}"#,
        )
        .await
        .unwrap();
        let ops = broker.plan_ops.lock().unwrap();
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            PlanOp::Add { task } => assert_eq!(task.id, "X"),
            _ => unreachable!(),
        }
    }
}
