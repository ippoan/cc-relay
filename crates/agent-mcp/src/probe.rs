//! Issue #50 A 案 PoC: minimal WSS /connect probe.
//!
//! Connects to the auth-worker `/connect` (or `/u/<owner>/connect`) WS
//! endpoint with a Bearer JWT, sends a `hello` frame to mirror what
//! `relay`/`channel` modes do, and writes every received frame as one
//! JSON line to a log file. No broker, no MCP tool surface, no stdio
//! JSON-RPC — the goal is to characterise turn-internal long-poll
//! behaviour from a CCoW container without touching the rest of the
//! stack.
//!
//! Ping/Pong/Close are handled internally; `event` / `req` / `resp` /
//! `notif` / `hello` / unknown frames all get appended verbatim with a
//! `received_at_ms` wall-clock stamp.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

/// Probe configuration. Built by `agent-cli probe`.
#[derive(Debug, Clone)]
pub struct ProbeConfig {
    pub ws_url: String,
    pub access_token: String,
    pub log_path: PathBuf,
    /// Stop after this many frames are appended to the log. `None`
    /// means run until the peer closes or the process is signalled.
    pub max_frames: Option<usize>,
}

/// Run the probe until the peer closes, `max_frames` is hit, or the
/// task is aborted.
///
/// Returns `Ok(count)` with the number of frames written.
pub async fn run(cfg: ProbeConfig) -> Result<usize> {
    let mut request = cfg
        .ws_url
        .as_str()
        .into_client_request()
        .with_context(|| format!("invalid ws url: {}", cfg.ws_url))?;
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", cfg.access_token))
            .context("invalid bearer token (non-ascii chars)")?,
    );

    tracing::info!(url = %cfg.ws_url, log = %cfg.log_path.display(), "probe: connecting");
    let (ws, http_resp) = tokio_tungstenite::connect_async(request)
        .await
        .with_context(|| format!("ws connect failed: {}", cfg.ws_url))?;
    tracing::info!(status = %http_resp.status(), "probe: ws connected");

    let mut log = open_log(&cfg.log_path).await?;
    write_log_line(
        &mut log,
        &json!({
            "received_at_ms": now_ms(),
            "kind": "_probe_connected",
            "http_status": http_resp.status().as_u16(),
        }),
    )
    .await?;

    let (mut sink, mut stream) = ws.split();
    let hello = json!({
        "kind": "hello",
        "v": 1,
        "binary_version": env!("CARGO_PKG_VERSION"),
        "proto": 1,
        "probe": true,
    });
    sink.send(Message::Text(hello.to_string()))
        .await
        .context("send hello frame")?;

    let mut count: usize = 0;
    while let Some(msg) = stream.next().await {
        let msg = msg.context("ws stream error")?;
        let text = match msg {
            Message::Text(t) => t,
            Message::Binary(b) => String::from_utf8(b).context("non-utf8 ws frame")?,
            // tungstenite responds to Ping automatically; we still log
            // it so we can prove keepalive cadence on the wire.
            Message::Ping(p) => {
                write_log_line(
                    &mut log,
                    &json!({
                        "received_at_ms": now_ms(),
                        "kind": "_probe_ping",
                        "len": p.len(),
                    }),
                )
                .await?;
                continue;
            }
            Message::Pong(p) => {
                write_log_line(
                    &mut log,
                    &json!({
                        "received_at_ms": now_ms(),
                        "kind": "_probe_pong",
                        "len": p.len(),
                    }),
                )
                .await?;
                continue;
            }
            Message::Frame(_) => continue,
            Message::Close(reason) => {
                let (code, msg) = reason
                    .map(|r| (Some(u16::from(r.code)), Some(r.reason.into_owned())))
                    .unwrap_or((None, None));
                write_log_line(
                    &mut log,
                    &json!({
                        "received_at_ms": now_ms(),
                        "kind": "_probe_close",
                        "code": code,
                        "reason": msg,
                    }),
                )
                .await?;
                tracing::info!(?code, "probe: ws closed by peer");
                break;
            }
        };

        // Try to JSON-decode for shape clarity, fall back to raw text
        // if the peer ever sends non-JSON.
        let parsed: Value = match serde_json::from_str::<Value>(&text) {
            Ok(v) => v,
            Err(_) => Value::String(text),
        };
        write_log_line(
            &mut log,
            &json!({
                "received_at_ms": now_ms(),
                "frame": parsed,
            }),
        )
        .await?;
        count += 1;

        if let Some(max) = cfg.max_frames {
            if count >= max {
                tracing::info!(count, max, "probe: max-frames reached, exiting");
                break;
            }
        }
    }

    log.flush().await.ok();
    Ok(count)
}

async fn open_log(path: &Path) -> Result<tokio::fs::File> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create log parent dir: {}", parent.display()))?;
        }
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("open log file: {}", path.display()))
}

async fn write_log_line(file: &mut tokio::fs::File, value: &Value) -> Result<()> {
    let mut line = serde_json::to_vec(value).context("encode log line")?;
    line.push(b'\n');
    file.write_all(&line).await.context("write log line")?;
    Ok(())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
