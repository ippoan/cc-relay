//! Configuration passed from `agent-cli` into [`crate::run`].
//!
//! Keeping this as a plain struct (no clap derive here) makes the daemon
//! callable from tests and from any future supervisor without dragging in
//! a CLI parser.

use std::net::SocketAddr;
use std::path::PathBuf;

/// Everything the daemon needs to start. Constructed by the agent-cli
/// `daemon` subcommand (#14).
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// Identifier sent in the `Hello` message. Must be unique within a
    /// session; collisions are resolved by the coordinator's "after wins"
    /// rule (see #4 / #7).
    pub agent_id: String,
    /// Repository / worktree root to watch.
    pub worktree: PathBuf,
    /// `wss://...` URL of the coordinator endpoint, typically
    /// `wss://<worker>.workers.dev/session/<id>`.
    pub ws_url: String,
    /// JSONL inbox file. Each inbound `notify_agent` becomes a line here.
    pub inbox: PathBuf,
    /// Loopback address for the HTTP server that hooks (#9) POST to.
    pub http_bind: SocketAddr,
    /// Optional human-readable repo label included in `Hello`.
    pub repo: Option<String>,
}

impl DaemonConfig {
    /// Convenience for the test-only and integration-only paths.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn for_test(agent_id: &str, worktree: PathBuf, ws_url: String) -> Self {
        Self {
            agent_id: agent_id.into(),
            worktree,
            ws_url,
            inbox: std::env::temp_dir().join(format!("cc-relay-inbox-{agent_id}.jsonl")),
            http_bind: "127.0.0.1:0".parse().unwrap(),
            repo: None,
        }
    }
}
