//! Stdio transport for the cc-relay agent MCP server.
//!
//! Spawned by Claude Code as a subprocess via `.mcp.json`. Reads
//! line-delimited JSON-RPC on stdin, dispatches through
//! [`RelayServer::handle_jsonrpc`], writes the response (or nothing, for
//! notifications) to stdout. No WebSocket, no channel notifications —
//! that is what [`channel::run`](crate::channel::run) is for.
//!
//! Compared to `channel::run`, the only differences are:
//! - no `enable_channel_mode()` (responses are plain MCP, not
//!   `notifications/claude/channel`)
//! - no outbound WS task (event frames are not received in stdio mode;
//!   webhook events go through ADR-006 polling via `get_issue_events` or
//!   the future broker-side `notify_agent` round-trip)
//!
//! The stdout writer task is still kept separate so that future
//! server-initiated notifications (logging, progress) can be slotted in
//! without re-introducing the race-on-stdout problem.

use std::sync::Arc;

use agent_broker::Broker;
use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use crate::relay::RelayServer;

/// Run the stdio MCP server until stdin closes (= Claude Code parent
/// exited or the user terminated the subprocess).
///
/// `broker` must already be configured (auth + repo + agent_id, etc.).
/// `agent-cli`'s `run_stdio` wires this up before calling.
pub async fn run(broker: Arc<dyn Broker>) -> Result<()> {
    let server = Arc::new(RelayServer::new(broker));

    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();

    // Single stdout owner — prevents JSON-RPC response / future
    // notification interleave on the same line.
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(line) = out_rx.recv().await {
            let frame = format!("{line}\n");
            if let Err(e) = stdout.write_all(frame.as_bytes()).await {
                tracing::warn!(error = %e, "stdout write failed; exiting writer");
                break;
            }
            if let Err(e) = stdout.flush().await {
                tracing::warn!(error = %e, "stdout flush failed; exiting writer");
                break;
            }
        }
    });

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(l)) => l,
            Ok(None) => {
                tracing::info!("stdio: stdin EOF, exiting");
                break;
            }
            Err(e) => {
                tracing::warn!(error = %e, "stdin read failed, exiting");
                break;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        match server.handle_jsonrpc(line.as_bytes()).await {
            Ok(Some(resp)) => {
                let s = match std::str::from_utf8(&resp) {
                    Ok(s) => s.to_string(),
                    Err(e) => {
                        tracing::warn!(error = %e, "handle_jsonrpc non-utf8 response");
                        continue;
                    }
                };
                if out_tx.send(s).is_err() {
                    tracing::warn!("writer dropped; exiting stdin loop");
                    break;
                }
            }
            Ok(None) => {} // notification — no response
            Err(e) => {
                tracing::warn!(error = %e, "handle_jsonrpc errored; skipping line");
            }
        }
    }

    drop(out_tx);
    let _ = writer.await;
    Ok(())
}
