//! WebSocket client. Speaks JSON `WireMessage`s, reconnects with
//! exponential backoff, and exits cleanly when the coordinator closes
//! with [`agent_core::protocol::CloseCode::REPLACED`] (4002).

use std::time::Duration;

use agent_core::protocol::{CloseCode, Hello, WireMessage, PROTOCOL_VERSION};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode as WsCloseCode;
use tokio_tungstenite::tungstenite::Message;

/// Outcome of a single connect attempt, used to decide whether to retry
/// or to exit the daemon entirely.
#[derive(Debug)]
enum Outcome {
    /// Coordinator closed with code 4001 / 4002 / 4003; do not retry.
    Stop,
    /// Connection dropped for a transient reason; reconnect after backoff.
    Retry,
}

/// Run the WebSocket loop until [`Outcome::Stop`].
///
/// `outbound` is the receiver fed by the watcher and HTTP server tasks.
/// `inbound` receives messages from the coordinator (used by the inbox
/// fan-in loop in [`crate::lib`]).
pub async fn run(
    config: WsConfig,
    mut outbound: mpsc::Receiver<WireMessage>,
    inbound: mpsc::Sender<WireMessage>,
) -> Result<()> {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);

    loop {
        match connect_once(&config, &mut outbound, &inbound).await {
            Outcome::Stop => return Ok(()),
            Outcome::Retry => {
                tracing::warn!(?backoff, "websocket disconnected, retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }
}

/// Static-ish config for the WS loop. Lives in this module so the
/// watcher / http modules do not have to know its shape.
#[derive(Debug, Clone)]
pub struct WsConfig {
    pub ws_url: String,
    pub agent_id: String,
    pub repo: Option<String>,
}

async fn connect_once(
    config: &WsConfig,
    outbound: &mut mpsc::Receiver<WireMessage>,
    inbound: &mpsc::Sender<WireMessage>,
) -> Outcome {
    let (stream, _resp) = match tokio_tungstenite::connect_async(&config.ws_url).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "websocket connect failed");
            return Outcome::Retry;
        }
    };
    tracing::info!(url = %config.ws_url, "websocket connected");
    let (mut tx, mut rx) = stream.split();

    // First thing on the wire is always the Hello. If the coordinator
    // dislikes the protocol_version it will reply with close 4001.
    let hello = WireMessage::Hello(Hello {
        protocol_version: PROTOCOL_VERSION,
        agent_id: config.agent_id.clone(),
        repo: config.repo.clone(),
        auth_token: None,
    });
    if let Err(e) = send_msg(&mut tx, &hello).await {
        tracing::warn!(error = %e, "failed to send hello");
        return Outcome::Retry;
    }

    loop {
        tokio::select! {
            // Daemon → coordinator. The watcher / HTTP server feed this.
            maybe = outbound.recv() => {
                let Some(msg) = maybe else {
                    // Sender side dropped — daemon is shutting down.
                    let _ = tx.send(Message::Close(None)).await;
                    return Outcome::Stop;
                };
                if let Err(e) = send_msg(&mut tx, &msg).await {
                    tracing::warn!(error = %e, "send failed");
                    return Outcome::Retry;
                }
            }

            // Coordinator → daemon.
            frame = rx.next() => {
                match frame {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<WireMessage>(&text) {
                            Ok(msg) => {
                                if inbound.send(msg).await.is_err() {
                                    return Outcome::Stop;
                                }
                            }
                            Err(e) => tracing::warn!(error = %e, raw = %text, "malformed wire message"),
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {
                        tracing::warn!("unexpected binary frame, ignoring");
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {
                        // tungstenite auto-responds to pings; we just observe.
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let code = frame.as_ref().map(|f| u16::from(f.code));
                        tracing::info!(?code, reason = ?frame.as_ref().map(|f| &f.reason), "websocket closed by peer");
                        return match code {
                            Some(c) if is_terminal(c) => Outcome::Stop,
                            _ => Outcome::Retry,
                        };
                    }
                    Some(Ok(Message::Frame(_))) => { /* low-level; ignore */ }
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "websocket read error");
                        return Outcome::Retry;
                    }
                    None => {
                        tracing::info!("websocket stream ended");
                        return Outcome::Retry;
                    }
                }
            }
        }
    }
}

async fn send_msg<S>(tx: &mut S, msg: &WireMessage) -> Result<()>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let json = serde_json::to_string(msg)?;
    tx.send(Message::Text(json)).await?;
    Ok(())
}

/// Close codes the daemon must not retry on:
/// - 4001 = our protocol version is wrong (binary upgrade needed)
/// - 4002 = a newer daemon took over this `agent_id`
/// - 4003 = bad auth token
fn is_terminal(code: u16) -> bool {
    code == CloseCode::PROTOCOL_VERSION_MISMATCH
        || code == CloseCode::REPLACED
        || code == CloseCode::UNAUTHORIZED
}

// `WsCloseCode` is re-exported so future code that wants to send a close
// frame from the daemon side (e.g. on SIGTERM) does not have to import
// tungstenite directly.
#[allow(dead_code)]
pub(crate) type DaemonCloseCode = WsCloseCode;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_codes_match_protocol_constants() {
        assert!(is_terminal(CloseCode::PROTOCOL_VERSION_MISMATCH));
        assert!(is_terminal(CloseCode::REPLACED));
        assert!(is_terminal(CloseCode::UNAUTHORIZED));
        assert!(!is_terminal(1000)); // normal close → retry
        assert!(!is_terminal(1006)); // abnormal → retry
    }
}
