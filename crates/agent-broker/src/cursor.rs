//! Cursor persistence for [`Broker::fetch_since`](crate::Broker::fetch_since).
//!
//! The cursor itself is tiny (one comment id + one ETag) but
//! persisting it across agent restarts means we don't have to re-read
//! the entire comment history on every cold start — the next conditional
//! GET can short-circuit to a 304.
//!
//! Layout: `~/.cc-relay/state-<slug>.json` where `<slug>` is the broker
//! session id (typically `owner/repo#issue`) with non-`[A-Za-z0-9_-]`
//! chars replaced by `_`.
//!
//! Concurrency: agents do not share this file. Each Claude Code on Web
//! session runs in its own sandbox with its own home directory, so no
//! cross-process locking is needed. Within a single process, writes go
//! through a `write-tmp + rename` so a crash mid-write cannot leave a
//! truncated state file.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::types::Cursor;

const SCHEMA_VERSION: u32 = 1;

/// On-disk envelope. Versioned so a future incompatible schema change
/// can be detected and rejected instead of silently misinterpreted.
#[derive(Debug, Serialize, Deserialize)]
struct PersistedCursor {
    #[serde(default = "default_version")]
    v: u32,
    #[serde(default)]
    last_comment_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_etag: Option<String>,
}

fn default_version() -> u32 {
    SCHEMA_VERSION
}

impl PersistedCursor {
    fn from_cursor(c: &Cursor) -> Self {
        Self {
            v: SCHEMA_VERSION,
            last_comment_id: c.last_comment_id,
            last_etag: c.last_etag.clone(),
        }
    }

    fn into_cursor(self) -> Cursor {
        Cursor {
            last_comment_id: self.last_comment_id,
            last_etag: self.last_etag,
        }
    }
}

/// File-backed [`Cursor`] store under `~/.cc-relay/`.
///
/// Use [`CursorStore::new`] in production (resolves home directory) and
/// [`CursorStore::at_path`] in tests / when you already know exactly
/// where the file should live.
pub struct CursorStore {
    path: PathBuf,
}

impl CursorStore {
    /// Construct a store for the given broker session.
    ///
    /// `session_id` should be a stable string identifying the broker
    /// target — for `GitHubBroker` use `"<owner>/<repo>#<issue>"`. The
    /// state directory (`~/.cc-relay/`) is created if it doesn't exist.
    pub fn new(session_id: &str) -> anyhow::Result<Self> {
        let dir = home_dir()?.join(".cc-relay");
        std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        let path = dir.join(format!("state-{}.json", slug(session_id)));
        Ok(Self { path })
    }

    /// Construct a store at an arbitrary path. Useful for tests and for
    /// callers that already manage their own state directory.
    pub fn at_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Read the persisted cursor.
    ///
    /// Returns [`Cursor::beginning`] on missing file, corrupt JSON, or
    /// unknown schema version (with a `tracing::warn` for the latter
    /// two). Never errors — a broken state file is recoverable by
    /// resyncing from GitHub.
    pub async fn load(&self) -> Cursor {
        let bytes = match fs::read(&self.path).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Cursor::beginning();
            }
            Err(e) => {
                tracing::warn!(error = %e, path = %self.path.display(), "cursor read failed");
                return Cursor::beginning();
            }
        };

        match serde_json::from_slice::<PersistedCursor>(&bytes) {
            Ok(p) if p.v == SCHEMA_VERSION => p.into_cursor(),
            Ok(p) => {
                tracing::warn!(
                    v = p.v,
                    path = %self.path.display(),
                    "cursor file has unknown schema version, ignoring",
                );
                Cursor::beginning()
            }
            Err(e) => {
                tracing::warn!(error = %e, path = %self.path.display(), "cursor file corrupt, ignoring");
                Cursor::beginning()
            }
        }
    }

    /// Atomically persist `cursor` — writes to `<path>.tmp` first, then
    /// renames. A crash before `rename` leaves the previous valid file
    /// intact.
    pub async fn save(&self, cursor: &Cursor) -> anyhow::Result<()> {
        let body = serde_json::to_vec_pretty(&PersistedCursor::from_cursor(cursor))
            .context("serialize cursor")?;

        // `with_extension("json.tmp")` replaces `.json` with `.json.tmp`
        // — exactly the sibling tmp file we want.
        let tmp = self.path.with_extension("json.tmp");

        // Scope the file handle so it's dropped before rename — on some
        // platforms a rename over an open file is unhappy.
        {
            let mut f = fs::File::create(&tmp)
                .await
                .with_context(|| format!("create {}", tmp.display()))?;
            f.write_all(&body)
                .await
                .with_context(|| format!("write {}", tmp.display()))?;
            f.flush().await.context("flush cursor")?;
        }

        if let Err(e) = fs::rename(&tmp, &self.path).await {
            // Best-effort cleanup so a half-written tmp doesn't linger.
            let _ = fs::remove_file(&tmp).await;
            return Err(anyhow::Error::new(e).context(format!(
                "rename {} → {}",
                tmp.display(),
                self.path.display()
            )));
        }

        Ok(())
    }

    /// Path to the underlying state file. Mostly useful for tracing /
    /// diagnostics.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Resolve `$HOME` (POSIX) or `%USERPROFILE%` (Windows) without pulling
/// in the `dirs` / `home` crates.
fn home_dir() -> anyhow::Result<PathBuf> {
    if let Some(p) = std::env::var_os("HOME") {
        if !p.is_empty() {
            return Ok(PathBuf::from(p));
        }
    }
    if let Some(p) = std::env::var_os("USERPROFILE") {
        if !p.is_empty() {
            return Ok(PathBuf::from(p));
        }
    }
    anyhow::bail!("cannot find $HOME or %USERPROFILE% — set one, or use CursorStore::at_path")
}

/// Map an arbitrary session string into a safe filename component:
/// keep `[A-Za-z0-9_-]`, replace everything else with `_`.
fn slug(session_id: &str) -> String {
    let mut s = String::with_capacity(session_id.len());
    for c in session_id.chars() {
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
            s.push(c);
        } else {
            s.push('_');
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unique tmp dir per test so parallel `cargo test` doesn't collide.
    fn tempdir() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "cc-relay-cursor-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[tokio::test]
    async fn load_missing_returns_beginning() {
        let dir = tempdir();
        let store = CursorStore::at_path(dir.join("not-yet.json"));
        let c = store.load().await;
        assert_eq!(c, Cursor::beginning());
    }

    #[tokio::test]
    async fn save_then_load_roundtrip() {
        let dir = tempdir();
        let store = CursorStore::at_path(dir.join("c.json"));
        let cursor = Cursor {
            last_comment_id: 42,
            last_etag: Some("\"abc\"".into()),
        };
        store.save(&cursor).await.unwrap();
        let back = store.load().await;
        assert_eq!(back, cursor);
    }

    #[tokio::test]
    async fn corrupt_file_falls_back_to_beginning() {
        let dir = tempdir();
        let path = dir.join("corrupt.json");
        tokio::fs::write(&path, b"not-json-at-all").await.unwrap();
        let store = CursorStore::at_path(&path);
        let c = store.load().await;
        assert_eq!(c, Cursor::beginning());
    }

    #[tokio::test]
    async fn unknown_version_falls_back_to_beginning() {
        let dir = tempdir();
        let path = dir.join("future.json");
        tokio::fs::write(&path, br#"{"v":99,"last_comment_id":7,"last_etag":"x"}"#)
            .await
            .unwrap();
        let store = CursorStore::at_path(&path);
        let c = store.load().await;
        assert_eq!(c, Cursor::beginning());
    }

    #[tokio::test]
    async fn save_leaves_no_tmp_file_after_success() {
        let dir = tempdir();
        let store = CursorStore::at_path(dir.join("atomic.json"));
        store
            .save(&Cursor {
                last_comment_id: 1,
                last_etag: None,
            })
            .await
            .unwrap();
        let tmp = dir.join("atomic.json.tmp");
        assert!(!tmp.exists(), "tmp file should have been renamed away");
    }

    #[tokio::test]
    async fn load_returns_beginning_when_read_fails_non_notfound() {
        // Reading a directory path returns EISDIR (not NotFound) → exercises
        // the `Err(e)` arm with the tracing::warn.
        let dir = tempdir();
        let store = CursorStore::at_path(&dir);
        let c = store.load().await;
        assert_eq!(c, Cursor::beginning());
    }

    #[tokio::test]
    async fn save_errors_when_rename_target_is_directory() {
        // Create the destination as a non-empty directory so the final
        // rename fails — exercises the cleanup-then-error branch.
        let dir = tempdir();
        let path = dir.join("state.json");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("decoy"), b"x").unwrap();
        let store = CursorStore::at_path(&path);
        let err = store
            .save(&Cursor {
                last_comment_id: 7,
                last_etag: None,
            })
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("rename"), "expected rename in err, got {msg}");
        // The tmp file should have been cleaned up.
        let tmp = path.with_extension("json.tmp");
        assert!(
            !tmp.exists() || tmp.is_dir(),
            "tmp file should have been removed after failed rename"
        );
    }

    #[tokio::test]
    async fn path_accessor_returns_constructed_path() {
        let dir = tempdir();
        let p = dir.join("state.json");
        let store = CursorStore::at_path(&p);
        assert_eq!(store.path(), p.as_path());
    }

    #[test]
    fn default_version_returns_schema_version() {
        // Direct call: also exercised indirectly via the deserialize
        // default-attribute on `v` (see deserialize_missing_v_uses_default).
        assert_eq!(default_version(), SCHEMA_VERSION);
    }

    #[tokio::test]
    async fn load_accepts_file_missing_v_field_via_serde_default() {
        // No `v` in JSON → serde calls `default_version()` to fill it →
        // load returns the cursor (SCHEMA_VERSION matches the default).
        let dir = tempdir();
        let path = dir.join("nov.json");
        std::fs::write(&path, br#"{"last_comment_id":11,"last_etag":"E"}"#).unwrap();
        let store = CursorStore::at_path(&path);
        let c = store.load().await;
        assert_eq!(c.last_comment_id, 11);
        assert_eq!(c.last_etag.as_deref(), Some("E"));
    }

    #[test]
    fn home_dir_returns_path_when_home_set() {
        // HOME is set in any sane test env; this exercises the happy path
        // (lines 171-173). The error / USERPROFILE branches mutate global
        // env which is racy across tests — covered only opportunistically.
        let home = home_dir().expect("HOME should be set in test env");
        assert!(home.is_absolute() || !home.as_os_str().is_empty());
    }

    #[test]
    fn new_creates_state_directory_under_home() {
        // CursorStore::new resolves home_dir() and creates `~/.cc-relay`.
        // We can't easily override HOME without racy global env mutation,
        // so just call new() with a deterministic session id and verify
        // the resulting file path falls under `<home>/.cc-relay/`.
        let store = CursorStore::new("unit/test#new-cov").expect("new should succeed");
        let p = store.path();
        let s = p.to_string_lossy();
        assert!(
            s.contains("/.cc-relay/state-"),
            "unexpected path layout: {s}"
        );
        assert!(s.ends_with(".json"), "expected .json suffix, got {s}");
    }

    #[test]
    fn slug_replaces_path_and_fragment_chars() {
        assert_eq!(slug("ippoan/cc-relay#42"), "ippoan_cc-relay_42");
        assert_eq!(slug("OWNER/REPO#N"), "OWNER_REPO_N");
        // Already-safe chars survive.
        assert_eq!(slug("plain_name-123"), "plain_name-123");
    }
}
