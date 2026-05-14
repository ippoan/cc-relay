//! Local HTTP server. Hooks (#9) POST file/notify events to
//! `http://127.0.0.1:9876/event`; we translate them into `WireMessage`s
//! and hand them off to the WS sender via the `outbound` channel.
//!
//! Bound to loopback only by construction. The CLI is the only place
//! that can change the bind address.

use std::net::SocketAddr;
use std::sync::Arc;

use agent_core::protocol::{FileEventKind, NotifyTarget, Priority, WireMessage};
use anyhow::Result;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::now_millis;

/// Payload accepted at `POST /event`. The discriminant lets a single
/// endpoint serve both file-change events (from the
/// `PostToolUse:Write|Edit` hook) and outbound notifies (from the MCP
/// `notify_agent` tool) without duplicate routes.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HookPayload {
    /// Sent by `notify-change.sh`. We attach our own `agent_id` because
    /// hooks do not know it.
    FileEdited {
        path: String,
        #[serde(default)]
        kind: Option<HookFileEventKind>,
    },
    /// Sent by `agent-mcp` when Claude calls the `notify_agent` tool.
    Notify {
        to: NotifyTarget,
        message: String,
        #[serde(default)]
        priority: Priority,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HookFileEventKind {
    Created,
    Modified,
    Removed,
    Renamed,
}

impl From<HookFileEventKind> for FileEventKind {
    fn from(k: HookFileEventKind) -> Self {
        match k {
            HookFileEventKind::Created => FileEventKind::Created,
            HookFileEventKind::Modified => FileEventKind::Modified,
            HookFileEventKind::Removed => FileEventKind::Removed,
            HookFileEventKind::Renamed => FileEventKind::Renamed,
        }
    }
}

#[derive(Clone)]
struct AppState {
    agent_id: Arc<str>,
    outbound: mpsc::Sender<WireMessage>,
}

/// Run the HTTP server until the listener errors out. The returned
/// future does not resolve under normal circumstances; the caller wraps
/// it in `tokio::spawn` and shuts down by dropping the daemon's runtime.
pub async fn run(
    bind: SocketAddr,
    agent_id: String,
    outbound: mpsc::Sender<WireMessage>,
) -> Result<()> {
    let state = AppState {
        agent_id: Arc::from(agent_id),
        outbound,
    };
    let app: Router = Router::new()
        .route("/event", post(post_event))
        .with_state(state);

    let listener = TcpListener::bind(bind).await?;
    let local = listener.local_addr()?;
    tracing::info!(addr = %local, "http server listening");

    axum::serve(listener, app).await?;
    Ok(())
}

async fn post_event(State(state): State<AppState>, Json(payload): Json<HookPayload>) -> StatusCode {
    let msg = match payload {
        HookPayload::FileEdited { path, kind } => WireMessage::FileEvent {
            agent_id: state.agent_id.to_string(),
            path,
            kind: kind.map(Into::into).unwrap_or(FileEventKind::Modified),
            timestamp: now_millis(),
        },
        HookPayload::Notify {
            to,
            message,
            priority,
        } => WireMessage::NotifyAgent {
            from: state.agent_id.to_string(),
            to,
            message,
            priority,
            timestamp: now_millis(),
        },
    };

    // `try_send` instead of `send().await` so a clogged channel cannot
    // back-pressure the hook script (which runs synchronously inside
    // Claude). Dropping under load is a fine trade-off for these events.
    match state.outbound.try_send(msg) {
        Ok(()) => StatusCode::ACCEPTED,
        Err(e) => {
            tracing::warn!(error = %e, "drop event: outbound channel full or closed");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}
