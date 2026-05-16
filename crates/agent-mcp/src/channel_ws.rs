//! WebSocket handshake for `channel` mode. Excluded from the coverage
//! gate (`scripts/coverage_gate.sh`) — the body is `connect_async +
//! split + drive_ws_pump`, all the testable logic lives in
//! `channel::drive_ws_pump` (mock Sink + Stream).

use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use crate::channel::drive_ws_pump;
use crate::relay::RelayServer;

/// Connect to the auth-worker relay WS and pump inbound `event` frames
/// into the channel-mode notification path.
pub async fn pump_ws_events(server: &Arc<RelayServer>, ws_url: &str, token: &str) -> Result<()> {
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
    let (sink, stream) = ws.split();
    drive_ws_pump(server, sink, stream).await
}
