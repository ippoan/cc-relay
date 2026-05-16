//! WebSocket handshake + writer-task plumbing for the `relay` mode.
//!
//! Excluded from the coverage gate (`scripts/coverage_gate.sh` uses
//! `--ignore-filename-regex`) because the body is essentially three
//! `await` calls on real network I/O — `tokio_tungstenite::connect_async`,
//! a `tokio::spawn`ed writer task, and a final `let _ = writer.await`.
//! All the testable logic — JSON-RPC dispatch, frame routing, mock-stream
//! tests — lives in `relay::pump_inbound` and is fully covered.

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

use crate::relay::{
    pump_inbound, HelloFrame, RelayConfig, RelayServer, FRAME_VERSION, SERVER_VERSION,
};

/// Connect to the relay WS and pump frames forever.
///
/// Loops on inbound `req` frames via [`pump_inbound`]; per frame, decodes
/// the body, runs [`RelayServer::handle_jsonrpc`], and sends back a `resp`
/// frame. Returns only when the WS closes (caller decides whether to
/// reconnect).
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

    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    server.set_notif_sender(out_tx.clone());

    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if let Err(e) = sink.send(Message::Text(msg)).await {
                tracing::warn!(error = %e, "ws sink write failed; closing writer");
                break;
            }
        }
        let _ = sink.close().await;
    });

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

    drop(out_tx);
    let _ = writer.await;

    result
}
