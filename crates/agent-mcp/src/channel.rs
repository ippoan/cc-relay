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
//! [Channels Reference]: https://code.claude.com/docs/en/channels-reference

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

use crate::relay::{RelayConfig, RelayServer};

/// Outcome of processing one WebSocket [`Message`]. Extracted so unit
/// tests can hit every branch (event vs hello vs binary vs close vs
/// malformed JSON) without standing up a real WS server.
#[derive(Debug, PartialEq)]
pub enum WsFrameOutcome {
    /// Event frame — dispatched via `handle_event_frame`.
    Event,
    /// Non-event frame (`hello` / `resp` / unknown) — silently ignored.
    Ignored,
    /// Ping / Pong / partial frame — skipped before any JSON parsing.
    NonText,
    /// Connection closed by the peer — caller should exit the loop.
    Closed,
    /// JSON parse failed — caller should `continue`.
    BadJson,
}

/// Pure-ish handler for one ws message. Mirrors the body of the
/// pre-existing `while let Some(msg) = stream.next()` loop, but doesn't
/// touch the stream itself so it's trivially testable.
pub fn handle_ws_message(server: &RelayServer, msg: Message) -> WsFrameOutcome {
    let text = match msg {
        Message::Text(t) => t,
        Message::Binary(b) => match String::from_utf8(b) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "non-utf8 ws binary frame");
                return WsFrameOutcome::BadJson;
            }
        },
        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => return WsFrameOutcome::NonText,
        Message::Close(_) => {
            tracing::info!("channel: ws closed by peer");
            return WsFrameOutcome::Closed;
        }
    };
    let v: Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "skip malformed ws frame");
            return WsFrameOutcome::BadJson;
        }
    };
    match v.get("kind").and_then(Value::as_str) {
        Some("event") => {
            server.handle_event_frame(&v);
            WsFrameOutcome::Event
        }
        _ => WsFrameOutcome::Ignored,
    }
}

/// Build a `hello` frame used by both `pump_ws_events` and tests.
pub(crate) fn hello_frame() -> String {
    let hello = json!({
        "kind": "hello",
        "v": 1,
        "binary_version": env!("CARGO_PKG_VERSION"),
        "proto": 1,
    });
    hello.to_string()
}

/// Run the stdio Channel server.
pub async fn run(server: RelayServer, config: RelayConfig) -> Result<()> {
    let reader = BufReader::new(tokio::io::stdin());
    let writer = tokio::io::stdout();
    run_io(server, config, reader, writer).await
}

/// Testable core of [`run`]. Accepts injected reader / writer so unit
/// tests can drive the stdin path without a real subprocess; the
/// outbound WS task is started in the background and aborted on exit.
pub async fn run_io<R, W>(
    mut server: RelayServer,
    config: RelayConfig,
    reader: R,
    mut writer: W,
) -> Result<()>
where
    R: AsyncBufRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    server.enable_channel_mode();

    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    server.set_notif_sender(out_tx.clone());
    let server = Arc::new(server);

    let writer_task = tokio::spawn(async move {
        while let Some(line) = out_rx.recv().await {
            let frame = format!("{line}\n");
            if let Err(e) = writer.write_all(frame.as_bytes()).await {
                tracing::warn!(error = %e, "stdout write failed; exiting writer");
                break;
            }
            if let Err(e) = writer.flush().await {
                tracing::warn!(error = %e, "stdout flush failed; exiting writer");
                break;
            }
        }
    });

    let ws_server = Arc::clone(&server);
    let ws_url = config.ws_url.clone();
    let ws_token = config.access_token.clone();
    let ws_task = tokio::spawn(async move {
        if let Err(e) = pump_ws_events(&ws_server, &ws_url, &ws_token).await {
            tracing::warn!(error = %e, "ws event pump exited");
        }
    });

    let mut lines = reader.lines();
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
        if let crate::stdio::LineOutcome::Response(s) =
            crate::stdio::process_line(&server, &line).await
        {
            if out_tx.send(s).is_err() {
                tracing::warn!("writer dropped; exiting stdin loop");
                break;
            }
        }
    }

    // Drop order matters: writer_task only exits when ALL `out_tx` clones
    // are dropped, and `server.notif_tx` (set above) is one. So we have
    // to abort + await ws_task first (releases its `Arc<RelayServer>`
    // clone) and drop our `server` Arc + local `out_tx` before awaiting
    // writer_task, otherwise the writer's `out_rx.recv().await` never
    // returns None.
    ws_task.abort();
    let _ = ws_task.await;
    drop(out_tx);
    drop(server);
    let _ = writer_task.await;
    Ok(())
}

/// Outbound WS receive loop. Connects, sends hello, then routes every
/// inbound frame through [`handle_ws_message`].
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
    let (sink, stream) = ws.split();
    drive_ws_pump(server, sink, stream).await
}

/// Stream-driven WS pump split out for testability. Generic over Sink /
/// Stream of `Message` so tests can substitute in-memory channels.
pub(crate) async fn drive_ws_pump<S, T, E>(
    server: &Arc<RelayServer>,
    mut sink: S,
    mut stream: T,
) -> Result<()>
where
    S: Sink<Message, Error = E> + Unpin,
    T: Stream<Item = std::result::Result<Message, E>> + Unpin,
    E: std::fmt::Display + Send + Sync + 'static,
{
    sink.send(Message::Text(hello_frame()))
        .await
        .map_err(|e| anyhow!("send hello frame: {e}"))?;

    while let Some(msg) = stream.next().await {
        let msg = msg.map_err(|e| anyhow!("ws stream error: {e}"))?;
        match handle_ws_message(server, msg) {
            WsFrameOutcome::Closed => return Ok(()),
            _ => continue,
        }
    }
    Err(anyhow!("ws stream ended"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay::RelayServer;
    use agent_broker::types::{AgentMeta, BrokerError, Cursor, Result as BrokerResult};
    use agent_broker::Broker;
    use agent_core::{NotifyMessage, PlanOp, TaskSpec};
    use async_trait::async_trait;
    use futures_util::stream;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context as TaskCtx, Poll};

    /// In-memory `Sink<Message>` for tests.
    struct CollectSink(Arc<Mutex<Vec<Message>>>);

    impl futures_util::Sink<Message> for CollectSink {
        type Error = std::io::Error;
        fn poll_ready(
            self: Pin<&mut Self>,
            _: &mut TaskCtx<'_>,
        ) -> Poll<std::result::Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(
            self: Pin<&mut Self>,
            item: Message,
        ) -> std::result::Result<(), std::io::Error> {
            self.0.lock().unwrap().push(item);
            Ok(())
        }
        fn poll_flush(
            self: Pin<&mut Self>,
            _: &mut TaskCtx<'_>,
        ) -> Poll<std::result::Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(
            self: Pin<&mut Self>,
            _: &mut TaskCtx<'_>,
        ) -> Poll<std::result::Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    struct StubBroker;

    #[async_trait]
    impl Broker for StubBroker {
        fn self_id(&self) -> &str {
            "channel-test"
        }
        async fn join(&self, _: &str) -> BrokerResult<()> {
            Ok(())
        }
        async fn leave(&self, _: &str) -> BrokerResult<()> {
            Ok(())
        }
        async fn list_agents(&self) -> BrokerResult<Vec<AgentMeta>> {
            Ok(vec![])
        }
        async fn send(&self, _: NotifyMessage) -> BrokerResult<()> {
            Err(BrokerError::Other(anyhow::anyhow!("nope")))
        }
        async fn fetch_since(&self, c: Cursor) -> BrokerResult<(Vec<NotifyMessage>, Cursor)> {
            Ok((vec![], c))
        }
        async fn get_plan(&self) -> BrokerResult<Vec<TaskSpec>> {
            Ok(vec![])
        }
        async fn plan_op(&self, _: PlanOp) -> BrokerResult<()> {
            Err(BrokerError::Other(anyhow::anyhow!("nope")))
        }
    }

    fn server() -> RelayServer {
        RelayServer::new(Arc::new(StubBroker) as Arc<dyn Broker>)
    }

    #[test]
    fn handle_ws_message_event_dispatches() {
        let s = server();
        let body = json!({
            "kind": "event",
            "owner": "o",
            "repo": "r",
            "issue_number": 1,
            "event_type": "issue_comment",
        });
        let m = Message::Text(body.to_string());
        assert_eq!(handle_ws_message(&s, m), WsFrameOutcome::Event);
    }

    #[test]
    fn handle_ws_message_binary_event_works() {
        let s = server();
        let body = json!({
            "kind": "event",
            "owner": "o",
            "repo": "r",
            "issue_number": 1,
        });
        let m = Message::Binary(body.to_string().into_bytes());
        assert_eq!(handle_ws_message(&s, m), WsFrameOutcome::Event);
    }

    #[test]
    fn handle_ws_message_binary_non_utf8_is_bad_json() {
        let s = server();
        let m = Message::Binary(vec![0xff, 0xfe]);
        assert_eq!(handle_ws_message(&s, m), WsFrameOutcome::BadJson);
    }

    #[test]
    fn handle_ws_message_ping_is_non_text() {
        let s = server();
        assert_eq!(
            handle_ws_message(&s, Message::Ping(Vec::new())),
            WsFrameOutcome::NonText
        );
        assert_eq!(
            handle_ws_message(&s, Message::Pong(Vec::new())),
            WsFrameOutcome::NonText
        );
    }

    #[test]
    fn handle_ws_message_close_is_closed() {
        let s = server();
        assert_eq!(
            handle_ws_message(&s, Message::Close(None)),
            WsFrameOutcome::Closed
        );
    }

    #[test]
    fn handle_ws_message_bad_json_is_bad_json() {
        let s = server();
        assert_eq!(
            handle_ws_message(&s, Message::Text("not json {{".into())),
            WsFrameOutcome::BadJson
        );
    }

    #[test]
    fn handle_ws_message_unknown_kind_is_ignored() {
        let s = server();
        let m = Message::Text(json!({"kind": "hello"}).to_string());
        assert_eq!(handle_ws_message(&s, m), WsFrameOutcome::Ignored);
        let m = Message::Text(json!({}).to_string());
        assert_eq!(handle_ws_message(&s, m), WsFrameOutcome::Ignored);
    }

    #[test]
    fn hello_frame_contains_expected_fields() {
        let s = hello_frame();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["kind"], "hello");
        assert_eq!(v["v"], 1);
        assert_eq!(v["proto"], 1);
        assert!(v["binary_version"].is_string());
    }

    #[tokio::test]
    async fn drive_ws_pump_closes_on_close_frame() {
        let s = Arc::new(server());
        let buf = Arc::new(Mutex::new(Vec::<Message>::new()));
        let sink = CollectSink(buf.clone());
        let stream = stream::iter(vec![
            Ok::<_, std::io::Error>(Message::Text(
                json!({"kind":"event","owner":"o","repo":"r","issue_number":1}).to_string(),
            )),
            Ok(Message::Close(None)),
        ]);
        drive_ws_pump(&s, sink, stream).await.unwrap();
        let sent = buf.lock().unwrap();
        // First message sent into the sink should be the hello frame.
        match &sent[0] {
            Message::Text(t) => assert!(t.contains("\"hello\""), "got {t}"),
            _ => panic!("expected text hello"),
        }
    }

    #[tokio::test]
    async fn drive_ws_pump_errors_when_stream_ends_without_close() {
        let s = Arc::new(server());
        let buf = Arc::new(Mutex::new(Vec::new()));
        let sink = CollectSink(buf);
        let stream = stream::iter(vec![Ok::<_, std::io::Error>(Message::Ping(vec![]))]);
        let e = drive_ws_pump(&s, sink, stream).await.unwrap_err();
        assert!(e.to_string().contains("ws stream ended"));
    }

    #[tokio::test]
    async fn drive_ws_pump_propagates_stream_error() {
        let s = Arc::new(server());
        let buf = Arc::new(Mutex::new(Vec::new()));
        let sink = CollectSink(buf);
        let err = std::io::Error::other("boom");
        let stream = stream::iter(vec![Err::<Message, _>(err)]);
        let e = drive_ws_pump(&s, sink, stream).await.unwrap_err();
        assert!(e.to_string().contains("ws stream error"));
    }

    #[tokio::test]
    async fn run_io_round_trips_responses_via_stdout() {
        let s = server();
        let cfg = RelayConfig {
            // Unreachable URL so pump_ws_events errors out fast and is
            // logged + aborted. We only assert on the stdin/stdout path.
            ws_url: "ws://127.0.0.1:1".into(),
            access_token: "x".into(),
        };
        let input = b"{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"ping\"}\n" as &[u8];
        let reader = tokio::io::BufReader::new(input);
        let (writer, mut sink) = tokio::io::duplex(8192);
        let join = tokio::spawn(async move { run_io(s, cfg, reader, writer).await });
        let mut out = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut sink, &mut out)
            .await
            .unwrap();
        join.await.unwrap().unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("\"id\":42"), "got: {text}");
    }

    #[tokio::test]
    async fn pump_ws_events_invalid_url_errors() {
        // pump_ws_events is private; exercise it through run_io with an
        // invalid url. The ws task fails internally and is logged; run
        // still returns Ok when stdin closes. To exercise the error
        // path of pump_ws_events directly we just call connect with a
        // garbage url here in a tokio task and assert it errors.
        let s = Arc::new(server());
        let err = super::pump_ws_events(&s, "not-a-url", "tok")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("invalid ws url")
                || err.to_string().contains("ws connect failed")
        );
    }

    #[tokio::test]
    async fn pump_ws_events_non_ascii_token_errors() {
        // The bearer token interpolation goes through HeaderValue::from_str
        // which rejects non-ascii. wss:// is fine syntactically.
        let s = Arc::new(server());
        let err = super::pump_ws_events(&s, "wss://example.invalid/x", "héllo")
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("invalid bearer token") || msg.contains("ws connect failed"),
            "got: {msg}"
        );
    }

    /// Forces every read to error so we hit `lines.next_line() Err(_)`.
    struct ErroringReader;
    impl tokio::io::AsyncRead for ErroringReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut TaskCtx<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::other("forced read failure")))
        }
    }

    /// Forces every write to error so we hit the writer-task's
    /// `write_all` Err branch.
    struct ErroringWriter;
    impl tokio::io::AsyncWrite for ErroringWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut TaskCtx<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Err(std::io::Error::other("forced write failure")))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskCtx<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::other("forced flush failure")))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskCtx<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Writes succeed but flush always errors.
    struct FlushErrWriter;
    impl tokio::io::AsyncWrite for FlushErrWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut TaskCtx<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskCtx<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::other("forced flush failure")))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskCtx<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_channel_writers_shutdown_cleanly() {
        // run_io drops without explicit shutdown; touch the no-op
        // shutdown bodies so they're not flagged as dead code.
        use tokio::io::AsyncWriteExt;
        let mut w1 = ErroringWriter;
        w1.shutdown().await.unwrap();
        let mut w2 = FlushErrWriter;
        w2.shutdown().await.unwrap();
    }

    fn cfg() -> RelayConfig {
        RelayConfig {
            ws_url: "ws://127.0.0.1:1".into(),
            access_token: "x".into(),
        }
    }

    #[tokio::test]
    async fn run_io_exits_on_stdin_read_error() {
        let s = server();
        let reader = tokio::io::BufReader::new(ErroringReader);
        let (writer, _sink) = tokio::io::duplex(64);
        run_io(s, cfg(), reader, writer).await.unwrap();
    }

    #[tokio::test]
    async fn run_io_writer_failure_aborts_writer_task() {
        let s = server();
        let input = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n\
                      {\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}\n"
            as &[u8];
        let reader = tokio::io::BufReader::new(input);
        run_io(s, cfg(), reader, ErroringWriter).await.unwrap();
    }

    #[tokio::test]
    async fn run_io_flush_failure_aborts_writer_task() {
        let s = server();
        let input = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n\
                      {\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}\n"
            as &[u8];
        let reader = tokio::io::BufReader::new(input);
        run_io(s, cfg(), reader, FlushErrWriter).await.unwrap();
    }
}
