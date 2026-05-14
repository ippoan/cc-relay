//! `/tmp/agent-inbox.jsonl` read/write helpers.
//!
//! The inbox is a JSONL file (one [`InboxEntry`] per line). The daemon
//! appends to it as notifications arrive over the WebSocket; the
//! `check-inbox.sh` hook reads it at `UserPromptSubmit` time and renames
//! the file so Claude sees each message exactly once.
//!
//! This module is gated behind the `io` feature. The coordinator does
//! not need it.

use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::protocol::Priority;

/// One line of the inbox JSONL file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InboxEntry {
    pub from: String,
    pub message: String,
    pub priority: Priority,
    /// Millis since epoch, server-stamped at the coordinator.
    pub timestamp: i64,
}

/// Append a single entry to the inbox JSONL file, creating it if needed.
///
/// Concurrent appends from multiple writers are safe at the JSONL level
/// because each call writes exactly one line via a single `write_all`,
/// but ordering between writers is not preserved.
pub async fn append(path: impl AsRef<Path>, entry: &InboxEntry) -> Result<()> {
    let mut line = serde_json::to_string(entry).context("serialize inbox entry")?;
    line.push('\n');

    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path.as_ref())
        .await
        .with_context(|| format!("open inbox {}", path.as_ref().display()))?;
    f.write_all(line.as_bytes())
        .await
        .context("write inbox entry")?;
    f.flush().await.context("flush inbox")?;
    Ok(())
}

/// Read every entry from the inbox. Returns an empty vec if the file does
/// not exist. Malformed lines are skipped with a `tracing::warn!` would go
/// here in P6 once tracing is wired in (#11).
pub async fn read_all(path: impl AsRef<Path>) -> Result<Vec<InboxEntry>> {
    let f = match tokio::fs::File::open(path.as_ref()).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(
                anyhow::Error::new(e).context(format!("open inbox {}", path.as_ref().display()))
            );
        }
    };

    let mut entries = Vec::new();
    let mut lines = BufReader::new(f).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<InboxEntry>(&line) {
            Ok(entry) => entries.push(entry),
            Err(_) => {
                // P6 (#11) will add structured logging here.
            }
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::*;

    #[tokio::test]
    async fn append_then_read_roundtrip() {
        let dir = tempdir();
        let path = dir.join("inbox.jsonl");

        let e1 = InboxEntry {
            from: "alice".into(),
            message: "hello".into(),
            priority: Priority::Normal,
            timestamp: 1_700_000_000_000,
        };
        let e2 = InboxEntry {
            from: "bob".into(),
            message: "ping".into(),
            priority: Priority::High,
            timestamp: 1_700_000_001_000,
        };

        append(&path, &e1).await.unwrap();
        append(&path, &e2).await.unwrap();

        let got = read_all(&path).await.unwrap();
        assert_eq!(got, vec![e1, e2]);
    }

    #[tokio::test]
    async fn read_missing_file_is_empty() {
        let dir = tempdir();
        let path = dir.join("does-not-exist.jsonl");
        let got = read_all(&path).await.unwrap();
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn malformed_lines_are_skipped() {
        let dir = tempdir();
        let path = dir.join("inbox.jsonl");

        // Write a mix of valid and garbage lines without going through `append`.
        let valid = InboxEntry {
            from: "alice".into(),
            message: "ok".into(),
            priority: Priority::Low,
            timestamp: 0,
        };
        let line = serde_json::to_string(&valid).unwrap();
        let content = format!("{line}\nnot-json\n\n{line}\n");

        tokio::fs::write(&path, content).await.unwrap();

        let got = read_all(&path).await.unwrap();
        assert_eq!(got.len(), 2);
    }

    /// Use a unique tmp dir per test so `cargo test` parallelism does not
    /// corrupt the inbox.
    fn tempdir() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "cc-relay-test-{}-{}",
            std::process::id(),
            // nanos-since-epoch is plenty to disambiguate parallel cases.
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
