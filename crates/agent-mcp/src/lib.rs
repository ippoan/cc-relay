//! cc-relay agent MCP server.
//!
//! Hosts the [`relay::RelayServer`] JSON-RPC dispatcher (broker-backed,
//! per ADR-001 / P5 / #17) and exposes three transport bindings:
//!
//! - [`stdio::run`] — line-delimited JSON-RPC over stdin / stdout. Used by
//!   `rust-mcp-agent stdio` when Claude Code spawns the binary as a local
//!   stdio MCP server (the ADR-001 default for Claude Code on Web).
//! - [`relay::run`] — outbound WebSocket to the auth-worker `McpSession`
//!   Durable Object (ADR-003 + ADR-004). Frame-based wire (`Frame::Req` /
//!   `Frame::Resp` / `Frame::Event`).
//! - [`channel::run`] — stdio JSON-RPC **plus** an outbound WS used solely
//!   to receive `kind:"event"` frames and re-emit them as JSON-RPC
//!   `notifications/claude/channel` lines on stdout (ADR-005 Phase A).
//!
//! All three share the same broker-backed `RelayServer`; the only
//! difference is how frames arrive and where responses go.
//!
//! Tools exposed (this slice — P5 #17 Phase 17.1):
//! - `cc_relay_list_agents` — `broker.list_agents()`
//! - `subscribe_issue_activity` / `unsubscribe_issue_activity` /
//!   `list_watched_issues` / `get_issue_events` — local file ops backing
//!   ADR-004 webhook event filtering / drain
//!
//! Tools that still need broker wiring (P5 #17 Phase 17.2 / 17.3):
//! `notify_agent`, `get_inbox`, `get_plan`, `add_task`, `claim_task`,
//! `update_task`, `remove_task`.

pub mod channel;
pub mod relay;
pub mod stdio;
pub mod watched_issues;

pub use relay::{RelayConfig, RelayServer};
