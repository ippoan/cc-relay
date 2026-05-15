//! cc-relay stdio MCP server.
//!
//! Claude Code spawns this binary as an MCP server over stdio. Tools
//! that need to talk to other agents go through a `Broker` (P4 / #16);
//! the inbox JSONL file is read locally for `get_inbox`.
//!
//! Tools exposed (P3 surface; broker wiring lands in P5):
//! - `notify_agent` — currently posts to a stub daemon URL; swapped for a
//!   broker call in P5 / #17.
//! - `get_inbox` — read `/tmp/agent-inbox.jsonl` and rename it to `.read`.
//! - `get_plan` — placeholder until the broker carries plan state (#17).
//! - `claim_task` / `update_task` — placeholder for #17 plan ops.

pub mod inbox;
pub mod relay;

use std::path::PathBuf;
use std::sync::Arc;

use crate::inbox::{self as inbox_io, InboxEntry};
use agent_core::{NotifyTarget, Priority};
use anyhow::Result;
use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResult, Content, Implementation, ProtocolVersion, ServerCapabilities, ServerInfo,
};
use rmcp::transport::stdio;
use rmcp::{tool, tool_handler, tool_router, ServerHandler, ServiceExt};
use schemars::JsonSchema;
use serde::Deserialize;

/// Configuration for the MCP server. Built by agent-cli.
#[derive(Debug, Clone)]
pub struct McpConfig {
    /// Base URL of the local daemon HTTP server (default
    /// `http://127.0.0.1:9876`).
    pub daemon_url: String,
    /// Path to the JSONL inbox the daemon writes to.
    pub inbox: PathBuf,
}

/// Run the stdio MCP server until stdin is closed.
pub async fn run(config: McpConfig) -> Result<()> {
    tracing::info!(daemon = %config.daemon_url, "agent-mcp starting (stdio)");

    let server = AgentServer::new(config);
    let service = server.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}

#[derive(Clone)]
struct AgentServer {
    inner: Arc<Inner>,
    /// Populated by `#[tool_router]` and consumed by `#[tool_handler]`;
    /// the field looks unused to the standard dead-code lint.
    #[allow(dead_code)]
    tool_router: ToolRouter<Self>,
}

struct Inner {
    daemon_url: String,
    inbox: PathBuf,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct NotifyArgs {
    /// Recipient `agent_id`. Use `*` to broadcast to every agent in the
    /// session.
    to: String,
    /// Message body shown verbatim in the recipient's next Claude prompt.
    message: String,
    /// Priority hint (`low` / `normal` / `high`). Optional.
    #[serde(default)]
    priority: Option<NotifyPriority>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum NotifyPriority {
    Low,
    Normal,
    High,
}

impl From<NotifyPriority> for Priority {
    fn from(p: NotifyPriority) -> Self {
        match p {
            NotifyPriority::Low => Priority::Low,
            NotifyPriority::Normal => Priority::Normal,
            NotifyPriority::High => Priority::High,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
struct EmptyArgs {}

#[derive(Debug, Deserialize, JsonSchema)]
struct ClaimArgs {
    /// Task id from `get_plan`.
    task_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct UpdateArgs {
    /// Task id from `get_plan`.
    task_id: String,
    /// New status: `pending` / `in_progress` / `done` / `cancelled`.
    status: String,
}

#[tool_router]
impl AgentServer {
    fn new(config: McpConfig) -> Self {
        let inner = Arc::new(Inner {
            daemon_url: config.daemon_url,
            inbox: config.inbox,
            http: reqwest::Client::new(),
        });
        Self {
            inner,
            tool_router: Self::tool_router(),
        }
    }

    /// Send a message to another agent (or all agents) via the daemon.
    #[tool(
        description = "Send a message to another agent in the same cc-relay session. Use `*` for `to` to broadcast."
    )]
    async fn notify_agent(
        &self,
        Parameters(args): Parameters<NotifyArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let to = if args.to == "*" {
            NotifyTarget::All
        } else {
            NotifyTarget::Agent(args.to)
        };
        let body = serde_json::json!({
            "type": "notify",
            "to": match to {
                NotifyTarget::All => serde_json::json!({ "kind": "all" }),
                NotifyTarget::Agent(ref id) => serde_json::json!({ "kind": "agent", "value": id }),
            },
            "message": args.message,
            "priority": args.priority.map(Priority::from).unwrap_or_default(),
        });

        let url = format!("{}/event", self.inner.daemon_url);
        match self.inner.http.post(&url).json(&body).send().await {
            Ok(resp) if resp.status().is_success() => {
                Ok(CallToolResult::success(vec![Content::text(format!(
                    "notify sent ({})",
                    resp.status()
                ))]))
            }
            Ok(resp) => Err(rmcp::ErrorData::internal_error(
                format!("daemon returned {}", resp.status()),
                None,
            )),
            Err(e) => Err(rmcp::ErrorData::internal_error(
                format!("daemon unreachable at {url}: {e}"),
                None,
            )),
        }
    }

    /// Drain the inbox JSONL file and return what was there. The daemon
    /// keeps appending; calling this multiple times in a row returns the
    /// newly-arrived lines each time.
    #[tool(description = "Read pending messages addressed to this agent. Returns a JSON array.")]
    async fn get_inbox(
        &self,
        Parameters(_): Parameters<EmptyArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let entries = inbox_io::read_all(&self.inner.inbox)
            .await
            .map_err(|e| rmcp::ErrorData::internal_error(format!("read inbox: {e}"), None))?;

        // Move the file aside so the next call sees only fresh lines.
        // Errors here are non-fatal; we still return the snapshot.
        if !entries.is_empty() {
            let read_path = self.inner.inbox.with_extension("jsonl.read");
            let _ = tokio::fs::rename(&self.inner.inbox, &read_path).await;
        }

        let payload: Vec<serde_json::Value> = entries
            .iter()
            .map(|e: &InboxEntry| {
                serde_json::json!({
                    "from": e.from,
                    "message": e.message,
                    "priority": e.priority,
                    "timestamp": e.timestamp,
                })
            })
            .collect();
        let text = serde_json::to_string(&payload).unwrap_or_else(|_| "[]".into());
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    /// Plan snapshot is held in the coordinator DO (#7). Until that lands
    /// this stub returns an empty list so the tool surface is stable.
    #[tool(description = "Return the current shared plan. Returns an empty array until #7 lands.")]
    async fn get_plan(
        &self,
        Parameters(_): Parameters<EmptyArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        Ok(CallToolResult::success(vec![Content::text("[]")]))
    }

    /// Plan mutation is also DO-side; stub for now.
    #[tool(description = "Claim a task in the shared plan. Returns ok or an error string.")]
    async fn claim_task(
        &self,
        Parameters(args): Parameters<ClaimArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        Ok(CallToolResult::success(vec![Content::text(format!(
            "claim_task({}) — not yet wired; lands with #7",
            args.task_id
        ))]))
    }

    #[tool(description = "Update the status of a task in the shared plan.")]
    async fn update_task(
        &self,
        Parameters(args): Parameters<UpdateArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        Ok(CallToolResult::success(vec![Content::text(format!(
            "update_task({}, {}) — not yet wired; lands with #7",
            args.task_id, args.status
        ))]))
    }
}

#[tool_handler]
impl ServerHandler for AgentServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_protocol_version(ProtocolVersion::V_2024_11_05)
            .with_server_info(Implementation::new(
                "cc-relay-agent-mcp",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Tools for coordinating with other Claude Code agents in the same \
                 cc-relay session. Use `notify_agent` to send a message, `get_inbox` \
                 to read incoming ones, and `get_plan` / `claim_task` / `update_task` \
                 to coordinate work.",
            )
    }
}
