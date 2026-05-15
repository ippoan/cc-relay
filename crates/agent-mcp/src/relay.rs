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

use agent_broker::Broker;
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

use crate::watched_issues::{IssueEventsFile, IssueKey, WatchedIssuesFile};

/// Protocol version published in `initialize` and the `hello` frame. Mirrors
/// the inline stub server's value so the wire view from a connector POV is
/// the same regardless of which side answers.
pub const STUB_PROTOCOL_VERSION: &str = "2025-06-18";

/// Frame schema version (binary side `github-mcp-server-rs#27` and auth-worker
/// `mcp-session-do.ts:FRAME_VERSION` both pin to 1).
const FRAME_VERSION: u32 = 1;

/// Server name advertised in `initialize`. Matches `cc-relay` in the
/// `.mcp.json` server entry to keep tool namespace prefixes intuitive.
const SERVER_NAME: &str = "cc-relay";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Outbound `hello` frame (us → DO) sent immediately after WS upgrade.
#[derive(Debug, Clone, Serialize)]
struct HelloFrame {
    kind: &'static str,
    v: u32,
    binary_version: &'static str,
    proto: u32,
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
        }
    }

    /// ADR-004 Phase D: WS 上の back-pipe を設定する。`run` から呼ばれる。
    /// テストではセットしない (no-op になる)。
    pub fn set_notif_sender(&mut self, tx: mpsc::UnboundedSender<String>) {
        self.notif_tx = Some(tx);
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

        // ADR-004 Phase D: auth-worker McpSession DO に `kind:"notif"` frame で
        // MCP `notifications/message` を back-pipe。DO 側で attached SSE channel
        // 全部に fan-out される (Anthropic Claude.ai / Claude Code Web の real-time
        // wake-up 経路)。notif_tx が None のテストでは silent no-op。
        let Some(tx) = self.notif_tx.as_ref() else {
            return;
        };
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
        let payload = match serde_json::to_string(&frame) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "notif frame serialize failed");
                return;
            }
        };
        if let Err(e) = tx.send(payload) {
            tracing::warn!(error = %e, "notif back-pipe send failed (ws writer dropped)");
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
        result_response(
            id,
            json!({
                "protocolVersion": proto,
                "capabilities": { "tools": { "listChanged": false } },
                "serverInfo": { "name": SERVER_NAME, "version": SERVER_VERSION },
            }),
        )
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
            other => error_response(id, -32602, &format!("Unknown tool: {other}")),
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

/// Connect to the relay WS and pump frames forever.
///
/// Loops on inbound `req` frames; per frame, decodes the body, runs
/// [`RelayServer::handle_jsonrpc`], and sends back a `resp` frame. Returns
/// only when the WS closes (caller decides whether to reconnect).
///
/// ADR-004 Phase D: WS への送信は全て mpsc channel 経由にする。
/// `handle_event_frame` も同じ channel で `kind:"notif"` frame を流すので、
/// reader loop と event handler が同じ sink を奪い合う race を避ける。
pub async fn run(mut server: RelayServer, config: RelayConfig) -> Result<()> {
    let mut request = config
        .ws_url
        .as_str()
        .into_client_request()
        .with_context(|| format!("invalid ws url: {}", config.ws_url))?;
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", config.access_token))
            .context("invalid bearer token (non-ascii chars)")?,
    );

    tracing::info!(url = %config.ws_url, "agent-mcp relay: connecting");
    let (ws, http_resp) = tokio_tungstenite::connect_async(request)
        .await
        .with_context(|| format!("ws connect failed: {}", config.ws_url))?;
    tracing::info!(status = %http_resp.status(), "agent-mcp relay: connected");

    let (mut sink, mut stream) = ws.split();

    // ADR-004 Phase D: 送信は全部 channel 経由。handle_event_frame からも
    // back-pipe する notif frame を流すため。writer は別 task で drain する。
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    server.set_notif_sender(out_tx.clone());

    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if let Err(e) = sink.send(Message::Text(msg)).await {
                tracing::warn!(error = %e, "ws sink write failed; closing writer");
                break;
            }
        }
        // sink を flush + close。drop だけだと TCP RST になる事があるので明示。
        let _ = sink.close().await;
    });

    // hello frame — informational, DO ignores it for Phase 6/7 but logs it.
    let hello = HelloFrame {
        kind: "hello",
        v: FRAME_VERSION,
        binary_version: SERVER_VERSION,
        proto: FRAME_VERSION,
    };
    out_tx
        .send(serde_json::to_string(&hello)?)
        .context("send hello frame failed (writer dead)")?;

    let result = pump_inbound(&server, &mut stream, &out_tx).await;

    // writer task を終了させる: tx を drop すれば channel が close、writer task は
    // recv() で None を返して抜ける。
    drop(out_tx);
    let _ = writer.await;

    result
}

/// 受信 loop 本体。`run()` から切り出して、writer task の cleanup を 1 箇所に
/// まとめる。
async fn pump_inbound(
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
    }

    impl StubBroker {
        fn with_agents(agents: Vec<AgentMeta>) -> Arc<Self> {
            Arc::new(Self {
                agents: Mutex::new(agents),
                list_agents_calls: Mutex::new(0),
                force_error: false,
            })
        }
        fn err() -> Arc<Self> {
            Arc::new(Self {
                agents: Mutex::new(vec![]),
                list_agents_calls: Mutex::new(0),
                force_error: true,
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
        async fn send(&self, _msg: NotifyMessage) -> BrokerResult<()> {
            Ok(())
        }
        async fn fetch_since(&self, c: Cursor) -> BrokerResult<(Vec<NotifyMessage>, Cursor)> {
            Ok((vec![], c))
        }
        async fn list_agents(&self) -> BrokerResult<Vec<AgentMeta>> {
            *self.list_agents_calls.lock().unwrap() += 1;
            if self.force_error {
                return Err(agent_broker::BrokerError::Auth("stub".into()));
            }
            Ok(self.agents.lock().unwrap().clone())
        }
        async fn get_plan(&self) -> BrokerResult<Vec<TaskSpec>> {
            Ok(vec![])
        }
        async fn plan_op(&self, _op: PlanOp) -> BrokerResult<()> {
            Ok(())
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
}
