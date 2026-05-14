//! cc-relay daemon runtime.
//!
//! Three independent tasks share state through `tokio::sync::mpsc`
//! channels:
//!
//! ```text
//!  watcher ──────┐
//!  HTTP server ──┴──► outbound ──► ws_client ──► coordinator
//!                                       │
//!                     inbox.jsonl ◀─────┴── inbound (notify_agent)
//! ```
//!
//! The binary entry point is [`crate::run`], called by the agent-cli
//! `daemon` subcommand (#14).

pub mod config;
mod http_server;
mod watcher;
mod ws_client;

use std::time::{SystemTime, UNIX_EPOCH};

use agent_core::inbox::{self, InboxEntry};
use agent_core::protocol::{NotifyTarget, WireMessage};
use anyhow::Result;
use tokio::sync::mpsc;

pub use config::DaemonConfig;

/// Wall-clock millis since Unix epoch. Used as `timestamp` in any
/// `WireMessage` originated locally.
pub(crate) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Run the daemon to completion. Returns `Ok(())` only on a clean exit
/// (SIGTERM or a terminal close code from the coordinator).
pub async fn run(config: DaemonConfig) -> Result<()> {
    tracing::info!(agent_id = %config.agent_id, worktree = %config.worktree.display(), "daemon starting");

    // Capacity 256 is more than enough headroom for the watcher and HTTP
    // server combined; if we ever block here something is very wrong.
    let (outbound_tx, outbound_rx) = mpsc::channel::<WireMessage>(256);
    let (inbound_tx, inbound_rx) = mpsc::channel::<WireMessage>(256);

    // Watcher owns the debouncer handle; dropping the handle stops the
    // watcher thread, so we hold it for the lifetime of the daemon.
    let _debouncer = watcher::spawn(&config.agent_id, &config.worktree, outbound_tx.clone())?;

    // HTTP server.
    let http_handle = tokio::spawn(http_server::run(
        config.http_bind,
        config.agent_id.clone(),
        outbound_tx.clone(),
    ));

    // Inbox writer: any inbound `NotifyAgent` targeted at us (or `all`)
    // gets appended to /tmp/agent-inbox.jsonl for hooks to pick up.
    let inbox_path = config.inbox.clone();
    let me = config.agent_id.clone();
    let inbox_handle = tokio::spawn(async move { inbox_loop(me, inbox_path, inbound_rx).await });

    // WebSocket client. Lives until a terminal close code or all senders
    // are dropped.
    let ws_cfg = ws_client::WsConfig {
        ws_url: config.ws_url.clone(),
        agent_id: config.agent_id.clone(),
        repo: config.repo.clone(),
    };
    let ws_result = ws_client::run(ws_cfg, outbound_rx, inbound_tx.clone()).await;

    // Once the WS exits the other tasks have no purpose; abort and
    // ignore their results (they may be `JoinError`s from abort).
    http_handle.abort();
    inbox_handle.abort();

    ws_result
}

/// Drain `inbound` and append every notify destined for us to the inbox
/// JSONL file. Anything not addressed to us is ignored at this layer —
/// fan-out is the coordinator's job.
async fn inbox_loop(
    me: String,
    inbox_path: std::path::PathBuf,
    mut inbound: mpsc::Receiver<WireMessage>,
) {
    while let Some(msg) = inbound.recv().await {
        let WireMessage::NotifyAgent {
            from,
            to,
            message,
            priority,
            timestamp,
        } = msg
        else {
            continue;
        };

        let for_me = match to {
            NotifyTarget::All => true,
            NotifyTarget::Agent(ref id) => id == &me,
        };
        if !for_me {
            continue;
        }

        let entry = InboxEntry {
            from,
            message,
            priority,
            timestamp,
        };
        if let Err(e) = inbox::append(&inbox_path, &entry).await {
            tracing::warn!(error = %e, "failed to append to inbox");
        }
    }
}
