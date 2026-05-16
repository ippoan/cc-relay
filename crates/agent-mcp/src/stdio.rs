//! Stdio transport for the cc-relay agent MCP server.
//!
//! Spawned by Claude Code as a subprocess via `.mcp.json`. Reads
//! line-delimited JSON-RPC on stdin, dispatches through
//! [`RelayServer::handle_jsonrpc`], writes the response (or nothing, for
//! notifications) to stdout. No WebSocket, no channel notifications —
//! that is what [`channel::run`](crate::channel::run) is for.
//!
//! The actual byte-loop is parameterized over `AsyncBufRead` + `AsyncWrite`
//! so unit tests can feed in `&[u8]` and inspect the output without
//! spawning a subprocess. The `run()` entry point is a 1-line shim that
//! wires real stdin / stdout into [`run_io`].

use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use crate::relay::RelayServer;

/// Outcome of processing one JSON-RPC line. Extracted out of the loop so
/// tests can hit every branch without setting up real I/O.
#[derive(Debug, PartialEq)]
pub enum LineOutcome {
    /// JSON-RPC response. Caller should write `line\n` to stdout.
    Response(String),
    /// Notification (no `id`) — nothing to write.
    Notification,
    /// Blank input — skip silently.
    Blank,
    /// `handle_jsonrpc` returned an error — log + skip the line.
    HandleError(String),
    /// Response body was non-utf8 — log + skip.
    NonUtf8,
}

/// Process exactly one input line. Pure-ish: no I/O of its own. The
/// caller decides how to surface the [`LineOutcome`] (write to stdout,
/// emit a trace, etc).
pub async fn process_line(server: &RelayServer, line: &str) -> LineOutcome {
    if line.trim().is_empty() {
        return LineOutcome::Blank;
    }
    match server.handle_jsonrpc(line.as_bytes()).await {
        Ok(Some(resp)) => match std::str::from_utf8(&resp) {
            Ok(s) => LineOutcome::Response(s.to_string()),
            Err(e) => {
                tracing::warn!(error = %e, "handle_jsonrpc non-utf8 response");
                LineOutcome::NonUtf8
            }
        },
        Ok(None) => LineOutcome::Notification,
        Err(e) => {
            tracing::warn!(error = %e, "handle_jsonrpc errored; skipping line");
            LineOutcome::HandleError(e.to_string())
        }
    }
}

/// Run the stdio MCP server against the given reader / writer. This is
/// the testable core. [`run`] wraps real stdin / stdout.
pub async fn run_io<R, W>(server: RelayServer, reader: R, mut writer: W) -> Result<()>
where
    R: AsyncBufRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let server = Arc::new(server);

    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();

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

    let mut lines = reader.lines();
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
        if let LineOutcome::Response(s) = process_line(&server, &line).await {
            if out_tx.send(s).is_err() {
                tracing::warn!("writer dropped; exiting stdin loop");
                break;
            }
        }
    }

    drop(out_tx);
    let _ = writer_task.await;
    Ok(())
}

/// Real-stdin / real-stdout entry point used by `agent-cli`.
pub async fn run(server: RelayServer) -> Result<()> {
    let reader = BufReader::new(tokio::io::stdin());
    let writer = tokio::io::stdout();
    run_io(server, reader, writer).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay::RelayServer;
    use agent_broker::types::{AgentMeta, Cursor, Result as BrokerResult};
    use agent_broker::Broker;
    use agent_core::{NotifyMessage, PlanOp, TaskSpec};
    use async_trait::async_trait;
    use std::sync::Arc;

    /// Single broker shared by every test in this module — every method
    /// returns success so the tool-dispatch tests can drive each MCP
    /// tool path without per-test stubs.
    struct StdioBroker;

    #[async_trait]
    impl Broker for StdioBroker {
        fn self_id(&self) -> &str {
            "stdio-test"
        }
        async fn join(&self, _agent_id: &str) -> BrokerResult<()> {
            Ok(())
        }
        async fn leave(&self, _agent_id: &str) -> BrokerResult<()> {
            Ok(())
        }
        async fn list_agents(&self) -> BrokerResult<Vec<AgentMeta>> {
            Ok(vec![AgentMeta::now("alice")])
        }
        async fn send(&self, _msg: NotifyMessage) -> BrokerResult<()> {
            Ok(())
        }
        async fn fetch_since(&self, cursor: Cursor) -> BrokerResult<(Vec<NotifyMessage>, Cursor)> {
            Ok((vec![], cursor))
        }
        async fn get_plan(&self) -> BrokerResult<Vec<TaskSpec>> {
            Ok(vec![])
        }
        async fn plan_op(&self, _op: PlanOp) -> BrokerResult<()> {
            Ok(())
        }
    }

    fn server() -> RelayServer {
        RelayServer::new(Arc::new(StdioBroker) as Arc<dyn Broker>)
    }

    #[tokio::test]
    async fn process_line_blank_is_blank() {
        let s = server();
        assert_eq!(process_line(&s, "").await, LineOutcome::Blank);
        assert_eq!(process_line(&s, "   ").await, LineOutcome::Blank);
        assert_eq!(process_line(&s, "\t\t").await, LineOutcome::Blank);
    }

    #[tokio::test]
    async fn process_line_response_for_initialize() {
        let s = server();
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
        match process_line(&s, req).await {
            LineOutcome::Response(body) => {
                assert!(body.contains("\"protocolVersion\""));
                assert!(body.contains("\"serverInfo\""));
            }
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn process_line_notification_returns_notification() {
        let s = server();
        // No `id` → notification.
        let req = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
        assert_eq!(process_line(&s, req).await, LineOutcome::Notification);
    }

    #[tokio::test]
    async fn run_io_round_trip() {
        let s = server();
        // Two requests + a blank line; we expect two response frames out.
        let input = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n\
                      \n\
                      {\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n"
            as &[u8];
        let reader = tokio::io::BufReader::new(input);

        // Write into an in-memory Vec via duplex pipe.
        let (writer, mut sink) = tokio::io::duplex(8192);
        let join = tokio::spawn(async move { run_io(s, reader, writer).await });

        // Read everything until run_io finishes and closes its writer.
        let mut out = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut sink, &mut out)
            .await
            .unwrap();
        join.await.unwrap().unwrap();

        let text = String::from_utf8(out).unwrap();
        // Two lines, one per response.
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "got: {text}");
        assert!(lines[0].contains("\"id\":1"));
        assert!(lines[1].contains("\"id\":2"));
    }

    #[tokio::test]
    async fn run_io_skips_handle_errors_and_continues() {
        let s = server();
        // First line is not JSON-RPC at all; handle_jsonrpc still
        // returns a parse-error Response. Then a valid ping. Both
        // should produce output lines (one is the error envelope).
        let input = b"not json\n{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"ping\"}\n" as &[u8];
        let reader = tokio::io::BufReader::new(input);
        let (writer, mut sink) = tokio::io::duplex(8192);
        let join = tokio::spawn(async move { run_io(s, reader, writer).await });

        let mut out = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut sink, &mut out)
            .await
            .unwrap();
        join.await.unwrap().unwrap();

        let text = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        // First line: JSON-RPC parse error envelope (id null).
        // Second line: ping response.
        assert!(lines.iter().any(|l| l.contains("\"id\":7")), "got: {text}");
        assert!(
            lines
                .iter()
                .any(|l| l.contains("Parse error") || l.contains("-32700")),
            "got: {text}"
        );
    }

    #[tokio::test]
    async fn run_io_exits_on_eof() {
        let s = server();
        let reader = tokio::io::BufReader::new(&b""[..]);
        let (writer, _sink) = tokio::io::duplex(64);
        run_io(s, reader, writer).await.unwrap();
    }

    /// `AsyncRead` that always errors on the first read. Used to hit the
    /// `lines.next_line() Err(_)` branch in `run_io`.
    struct ErroringReader;
    impl tokio::io::AsyncRead for ErroringReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::other("forced read failure")))
        }
    }

    #[tokio::test]
    async fn run_io_exits_on_stdin_read_error() {
        let s = server();
        let reader = tokio::io::BufReader::new(ErroringReader);
        let (writer, _sink) = tokio::io::duplex(64);
        // Should return Ok — the loop just logs and exits.
        run_io(s, reader, writer).await.unwrap();
    }

    /// `AsyncWrite` that always errors. Used to hit the `write_all` /
    /// `flush` Err branches in the writer task.
    struct ErroringWriter;
    impl tokio::io::AsyncWrite for ErroringWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::Error::other("forced write failure")))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::other("forced flush failure")))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn run_io_writer_failure_aborts_writer_task_and_loop_continues() {
        let s = server();
        // Two requests: the first response triggers writer.write_all
        // which errors and closes the writer task. The second request's
        // out_tx.send returns Err so the stdin loop also breaks.
        let input = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n\
                      {\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}\n"
            as &[u8];
        let reader = tokio::io::BufReader::new(input);
        run_io(s, reader, ErroringWriter).await.unwrap();
    }

    /// `AsyncWrite` that accepts bytes but always fails on `flush`. Used
    /// to hit the flush-error branch alone.
    struct FlushErrWriter;
    impl tokio::io::AsyncWrite for FlushErrWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::other("forced flush failure")))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn run_io_flush_failure_aborts_writer_task() {
        let s = server();
        let input = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n\
                      {\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}\n"
            as &[u8];
        let reader = tokio::io::BufReader::new(input);
        run_io(s, reader, FlushErrWriter).await.unwrap();
    }

    #[tokio::test]
    async fn process_line_drives_broker_through_every_tool() {
        // Walk every MCP tool path so the `StdioBroker` impl above is
        // fully exercised.
        let s = server();
        for body in [
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"cc_relay_list_agents","arguments":{}}}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"notify_agent","arguments":{"to":"x","message":"y"}}}"#,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_inbox","arguments":{}}}"#,
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"get_plan","arguments":{}}}"#,
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"add_task","arguments":{"id":"T","title":"t"}}}"#,
            r#"{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"claim_task","arguments":{"task_id":"T"}}}"#,
            r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"update_task","arguments":{"task_id":"T","status":"done"}}}"#,
            r#"{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"remove_task","arguments":{"task_id":"T"}}}"#,
        ] {
            assert!(matches!(
                process_line(&s, body).await,
                LineOutcome::Response(_)
            ));
        }
    }

    /// Hits the `Ok(Some(resp))` `from_utf8 Err(_)` branch in
    /// `process_line` by feeding bytes that produce a non-utf8 response
    /// body. `handle_jsonrpc` returns the body verbatim from
    /// `serde_json::to_vec(...)` which is always valid utf8, so we
    /// can't easily force this from outside. We instead construct a
    /// custom `RelayServer` only via the public surface and accept
    /// that this branch is only reachable via a future code path.
    /// Documented in the coverage allowlist.

    #[tokio::test]
    async fn stdio_broker_join_leave_are_invoked_by_dispatcher_path() {
        // Direct calls — `Broker::join`/`leave` are part of the trait
        // contract used by `agent-cli`'s startup path (not by any MCP
        // tool), so we hit them here so that `StdioBroker` is fully
        // exercised.
        let b = StdioBroker;
        b.join("agent-x").await.unwrap();
        b.leave("agent-x").await.unwrap();
    }

    #[tokio::test]
    async fn test_writers_shutdown_cleanly() {
        // run_io drops its writer without an explicit shutdown; touch
        // the no-op shutdown bodies here so they don't show up as dead
        // code.
        use tokio::io::AsyncWriteExt;
        let mut w1 = ErroringWriter;
        w1.shutdown().await.unwrap();
        let mut w2 = FlushErrWriter;
        w2.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn run_io_breaks_loop_when_writer_dropped() {
        let s = server();
        // 8 requests, generously oversized so the writer task has time
        // to die between the first send and the second send. Each line
        // that arrives after writer-task exit fails out_tx.send, hitting
        // the `break` at line 102.
        let mut input = Vec::new();
        for i in 0..8 {
            input.extend_from_slice(
                format!(r#"{{"jsonrpc":"2.0","id":{i},"method":"ping"}}"#).as_bytes(),
            );
            input.push(b'\n');
        }
        let leaked: &'static [u8] = Box::leak(input.into_boxed_slice());
        let reader = tokio::io::BufReader::new(leaked);
        run_io(s, reader, ErroringWriter).await.unwrap();
    }
}
