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
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

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
}

impl RelayServer {
    pub fn new(broker: Arc<dyn Broker>) -> Self {
        Self { broker }
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
            other => error_response(id, -32602, &format!("Unknown tool: {other}")),
        }
    }
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
pub async fn run(server: RelayServer, config: RelayConfig) -> Result<()> {
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

    // hello frame — informational, DO ignores it for Phase 6/7 but logs it.
    let hello = HelloFrame {
        kind: "hello",
        v: FRAME_VERSION,
        binary_version: SERVER_VERSION,
        proto: FRAME_VERSION,
    };
    sink.send(Message::Text(serde_json::to_string(&hello)?))
        .await
        .context("send hello frame failed")?;

    while let Some(message) = stream.next().await {
        let message = message.context("ws stream error")?;
        let text = match message {
            Message::Text(t) => t,
            Message::Binary(b) => String::from_utf8(b).context("non-utf8 binary frame")?,
            Message::Ping(p) => {
                sink.send(Message::Pong(p)).await.ok();
                continue;
            }
            Message::Pong(_) | Message::Frame(_) => continue,
            Message::Close(_) => {
                tracing::info!("agent-mcp relay: ws closed by peer");
                break;
            }
        };
        let req: ReqFrame = match serde_json::from_str(&text) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "skip malformed inbound frame");
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
        sink.send(Message::Text(serde_json::to_string(&resp)?))
            .await
            .context("send resp frame failed")?;
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

    #[tokio::test]
    async fn tools_list_advertises_cc_relay_list_agents() {
        let srv = server();
        let resp = dispatch(&srv, r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#)
            .await
            .unwrap();
        let tools = resp["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["name"], "cc_relay_list_agents");
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
