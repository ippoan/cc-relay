//! ADR-005 Phase A: cc-relay as a **Claude Code Channel** over stdio.
//!
//! In contrast to [`relay`](crate::relay) which speaks JSON-RPC over a
//! WebSocket frame envelope (auth-worker `McpSession` bridge), this module
//! runs the same [`RelayServer`] dispatcher over **stdio** (the standard MCP
//! transport that Claude Code spawns subprocesses with) AND keeps an
//! **outbound** WebSocket to the auth-worker open to receive
//! `kind:"event"` frames (GitHub webhook events).
//!
//! When an event arrives, [`RelayServer::handle_event_frame`] formats it as
//! a JSON-RPC `notifications/claude/channel` notification (because the
//! server is constructed with `channel_mode = true`) and pushes the wire
//! string to a shared `mpsc::UnboundedSender<String>`. A single stdout
//! writer task drains that channel, so JSON-RPC responses and channel
//! notifications never race on stdout.
//!
//! Wire format (per the [Channels Reference]):
//!
//! - `initialize` response advertises
//!   `capabilities.experimental['claude/channel'] = {}` + `instructions`.
//! - Notifications are JSON-RPC 2.0 of the form
//!   `{"jsonrpc":"2.0","method":"notifications/claude/channel","params":{"content":"...","meta":{...}}}`,
//!   one per line. Claude Code reads stdin line-by-line and routes any
//!   notification with the `notifications/claude/channel` method to its
//!   session-context injector, producing
//!   `<channel source="cc-relay" ...>content</channel>` in the next user
//!   turn.
//!
//! [Channels Reference]: https://code.claude.com/docs/en/channels-reference

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

use crate::relay::{RelayConfig, RelayServer};

/// Run the stdio Channel server.
///
/// The caller passes a freshly-constructed `RelayServer`; this function
/// flips `channel_mode` on and installs the stdout-bound notification
/// sender before wrapping it in an `Arc` for the stdin / WS tasks to
/// share.
///
/// Returns when stdin closes (= Claude Code parent exited).
pub async fn run(mut server: RelayServer, config: RelayConfig) -> Result<()> {
    server.enable_channel_mode();

    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    server.set_notif_sender(out_tx.clone());
    let server = Arc::new(server);

    // Single stdout owner — prevents JSON-RPC response / notification
    // interleave on the same line.
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

    // Outbound WS task — receives `kind:"event"` frames from auth-worker
    // and routes them through `RelayServer::handle_event_frame`, which
    // formats them as channel notifications and pushes through `out_tx`.
    let ws_server = Arc::clone(&server);
    let ws_url = config.ws_url.clone();
    let ws_token = config.access_token.clone();
    let ws_task = tokio::spawn(async move {
        if let Err(e) = pump_ws_events(&ws_server, &ws_url, &ws_token).await {
            tracing::warn!(error = %e, "ws event pump exited");
        }
    });

    // Main loop: line-delimited JSON-RPC on stdin, dispatched through
    // `handle_jsonrpc`. Responses go to `out_tx`; notifications produce
    // no stdout (handle_jsonrpc returns `Ok(None)`).
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(l)) => l,
            Ok(None) => {
                tracing::info!("channel: stdin EOF, exiting");
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
    ws_task.abort();
    Ok(())
}

/// Outbound WS receive loop. Connects, sends a `hello` frame for parity
/// with relay mode (so DO's `handleConnect` log matches), then drops
/// every frame except `kind:"event"` into `RelayServer::handle_event_frame`.
async fn pump_ws_events(server: &Arc<RelayServer>, ws_url: &str, token: &str) -> Result<()> {
    let mut request = ws_url
        .into_client_request()
        .with_context(|| format!("invalid ws url: {ws_url}"))?;
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token}"))
            .context("invalid bearer token (non-ascii chars)")?,
    );
    tracing::info!(url = %ws_url, "channel: connecting auth-worker ws");
    let (ws, http_resp) = tokio_tungstenite::connect_async(request)
        .await
        .with_context(|| format!("ws connect failed: {ws_url}"))?;
    tracing::info!(status = %http_resp.status(), "channel: ws connected");
    let (mut sink, mut stream) = ws.split();
    // Mirror relay mode's hello so DO log shape matches.
    let hello = json!({
        "kind": "hello",
        "v": 1,
        "binary_version": env!("CARGO_PKG_VERSION"),
        "proto": 1,
    });
    sink.send(Message::Text(hello.to_string()))
        .await
        .context("send hello frame")?;

    while let Some(msg) = stream.next().await {
        let msg = msg.context("ws stream error")?;
        let text = match msg {
            Message::Text(t) => t,
            Message::Binary(b) => String::from_utf8(b).context("non-utf8 ws frame")?,
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
            Message::Close(_) => {
                tracing::info!("channel: ws closed by peer");
                break;
            }
        };
        let v: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "skip malformed ws frame");
                continue;
            }
        };
        match v.get("kind").and_then(Value::as_str) {
            Some("event") => server.handle_event_frame(&v),
            // req / resp / hello / unknown — channel mode does not bridge
            // Claude.ai POST requests, so silently drop everything else.
            _ => continue,
        }
    }
    Err(anyhow!("ws stream ended"))
}
