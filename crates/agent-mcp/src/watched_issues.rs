//! ADR-004 Phase C: `~/.cc-relay/watched-issues.txt` の subscription
//! 永続化ヘルパー。
//!
//! 本ファイルは **「次回 binary 起動時に再 subscribe する宛先リスト」**
//! としての役割。`auth-worker` 側に subscription registry は持たない
//! (events は McpSession DO で broadcast → binary 側で filter)。
//!
//! - 形式: 1 行 1 entry、`owner/repo#N`。空行と `#` 始まりはコメント。
//! - 操作: append (dedup あり) / remove (no-op safe) / list / 一致判定。
//! - 同時実行: ファイル全書き換えなので、複数 process が同時に編集する
//!   と race が起き得るが、本 binary は 1 instance per session 想定なので
//!   問題にしない。

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Issue 識別子。`owner/repo#N` の形で永続化する。
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct IssueKey {
    pub owner: String,
    pub repo: String,
    pub number: u64,
}

impl IssueKey {
    pub fn new(owner: impl Into<String>, repo: impl Into<String>, number: u64) -> Self {
        Self {
            owner: owner.into(),
            repo: repo.into(),
            number,
        }
    }

    /// `"owner/repo#N"` 形式に直列化。
    pub fn as_filekey(&self) -> String {
        format!("{}/{}#{}", self.owner, self.repo, self.number)
    }

    /// 行 1 つを parse。`owner/repo#N` 以外の形 (空行、コメント、不正値)
    /// は `None`。
    pub fn parse_line(s: &str) -> Option<Self> {
        let line = s.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let (left, num_part) = line.split_once('#')?;
        let (owner, repo) = left.split_once('/')?;
        if owner.is_empty() || repo.is_empty() {
            return None;
        }
        let number: u64 = num_part.parse().ok()?;
        Some(IssueKey {
            owner: owner.to_string(),
            repo: repo.to_string(),
            number,
        })
    }
}

/// `~/.cc-relay/watched-issues.txt` を sync で読み書きする薄い helper。
///
/// async にしないのは: 1) 操作頻度が低い (subscribe/unsubscribe は人手)、
/// 2) 起動時の restore はブロッキングで十分、3) MCP tool handler から
/// `tokio::task::spawn_blocking` を経由しなくても許容遅延 (file ops は
/// 数 ms オーダー)。
#[derive(Debug, Clone)]
pub struct WatchedIssuesFile {
    path: PathBuf,
}

impl WatchedIssuesFile {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 既定 path (`$HOME/.cc-relay/watched-issues.txt`)。
    pub fn default_path() -> Result<PathBuf> {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .context("$HOME not set")?;
        Ok(home.join(".cc-relay").join("watched-issues.txt"))
    }

    /// 現在の watch set を `HashSet<IssueKey>` で返す。
    /// ファイルが無ければ空セット。parse 失敗行は読み飛ばし。
    pub fn load(&self) -> Result<HashSet<IssueKey>> {
        let s = match std::fs::read_to_string(&self.path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
            Err(e) => {
                return Err(e).with_context(|| format!("read {}", self.path.display()));
            }
        };
        Ok(s.lines().filter_map(IssueKey::parse_line).collect())
    }

    /// `key` を追加。既に含まれている場合は `Ok(false)`。
    pub fn add(&self, key: &IssueKey) -> Result<bool> {
        let mut set = self.load()?;
        if !set.insert(key.clone()) {
            return Ok(false);
        }
        self.write_set(&set)?;
        Ok(true)
    }

    /// `key` を削除。元から無ければ `Ok(false)`。
    pub fn remove(&self, key: &IssueKey) -> Result<bool> {
        let mut set = self.load()?;
        if !set.remove(key) {
            return Ok(false);
        }
        self.write_set(&set)?;
        Ok(true)
    }

    /// `set` の内容で完全に書き換える。並びは `owner/repo#N` の
    /// lexicographic で安定化(diff レビュー時の noise を減らす)。
    fn write_set(&self, set: &HashSet<IssueKey>) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create dir {}", parent.display()))?;
        }
        let mut lines: Vec<String> = set.iter().map(IssueKey::as_filekey).collect();
        lines.sort();
        let mut out = lines.join("\n");
        if !out.is_empty() {
            out.push('\n');
        }
        // atomic write: write to tmp, rename
        let tmp = self.path.with_extension("txt.tmp");
        std::fs::write(&tmp, out).with_context(|| format!("write {}", tmp.display()))?;
        std::fs::rename(&tmp, &self.path)
            .with_context(|| format!("rename {} -> {}", tmp.display(), self.path.display()))?;
        Ok(())
    }
}

/// `~/.cc-relay/issue-events.jsonl` — `kind:"event"` で受信して
/// `watched-issues.txt` の filter を通過した event を JSONL で蓄積する。
/// `get_issue_events` tool が drain (read + rotate to `.read`) する。
///
/// rotate pattern は既存 `inbox::read_all` + `rename(.jsonl, .jsonl.read)`
/// と同じで、tool 呼び出しごとに「未読のみ」を返す。
#[derive(Debug, Clone)]
pub struct IssueEventsFile {
    path: PathBuf,
}

impl IssueEventsFile {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 既定 path (`$HOME/.cc-relay/issue-events.jsonl`)。
    pub fn default_path() -> Result<PathBuf> {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .context("$HOME not set")?;
        Ok(home.join(".cc-relay").join("issue-events.jsonl"))
    }

    /// 受信した event JSON を 1 行追加。fail-open: ファイル書き込み
    /// 失敗時は warn log を出して呼び出し元には Ok を返す方が安全
    /// (event delivery loop が止まらない)。
    pub fn append_event(&self, event: &serde_json::Value) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create dir {}", parent.display()))?;
        }
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("open {} for append", self.path.display()))?;
        let line = serde_json::to_string(event).context("serialize event for jsonl")?;
        writeln!(file, "{line}")
            .with_context(|| format!("write line to {}", self.path.display()))?;
        Ok(())
    }

    /// 未読 event を全部読み、ファイルを `.read` にリネームして次の
    /// `drain` では空になるようにする (inbox と同じ semantics)。
    /// ファイル無し or 空 → 空 `Vec`。
    pub fn drain(&self) -> Result<Vec<serde_json::Value>> {
        let s = match std::fs::read_to_string(&self.path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e).with_context(|| format!("read {}", self.path.display())),
        };
        let entries: Vec<serde_json::Value> = s
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();
        if !entries.is_empty() {
            let read_path = self.path.with_extension("jsonl.read");
            // best-effort rename; tool が成功を返すために fatal にはしない
            if let Err(e) = std::fs::rename(&self.path, &read_path) {
                tracing::warn!(error = %e, path = %self.path.display(), "drain: rename failed; entries may surface again next call");
            }
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn issuekey_parse_and_format_roundtrip() {
        let k = IssueKey::new("ippoan", "cc-relay", 42);
        assert_eq!(k.as_filekey(), "ippoan/cc-relay#42");
        assert_eq!(IssueKey::parse_line("ippoan/cc-relay#42").unwrap(), k);
    }

    #[test]
    fn issuekey_parse_invalid() {
        assert!(IssueKey::parse_line("").is_none());
        assert!(IssueKey::parse_line("# comment").is_none());
        assert!(IssueKey::parse_line("no-hash").is_none());
        assert!(IssueKey::parse_line("no/repo#notnum").is_none());
        assert!(IssueKey::parse_line("/repo#1").is_none());
        assert!(IssueKey::parse_line("owner/#1").is_none());
    }

    #[test]
    fn issuekey_parse_strips_whitespace() {
        let k = IssueKey::parse_line("  ippoan/cc-relay#7  ").unwrap();
        assert_eq!(k, IssueKey::new("ippoan", "cc-relay", 7));
    }

    #[test]
    fn watched_load_missing_file_returns_empty() {
        let dir = tempdir().unwrap();
        let f = WatchedIssuesFile::new(dir.path().join("nonexistent.txt"));
        assert!(f.load().unwrap().is_empty());
    }

    #[test]
    fn watched_add_dedup_and_persistence() {
        let dir = tempdir().unwrap();
        let f = WatchedIssuesFile::new(dir.path().join("watched.txt"));
        let k1 = IssueKey::new("a", "b", 1);
        let k2 = IssueKey::new("x", "y", 2);

        assert!(f.add(&k1).unwrap());
        assert!(f.add(&k2).unwrap());
        assert!(!f.add(&k1).unwrap()); // dup

        let set = f.load().unwrap();
        assert_eq!(set.len(), 2);
        assert!(set.contains(&k1));
        assert!(set.contains(&k2));

        // ファイル内容も確認 (sort 済)
        let content = std::fs::read_to_string(f.path()).unwrap();
        assert_eq!(content, "a/b#1\nx/y#2\n");
    }

    #[test]
    fn watched_remove_idempotent() {
        let dir = tempdir().unwrap();
        let f = WatchedIssuesFile::new(dir.path().join("watched.txt"));
        let k = IssueKey::new("a", "b", 1);
        f.add(&k).unwrap();
        assert!(f.remove(&k).unwrap());
        assert!(!f.remove(&k).unwrap()); // 2 回目は false
        assert!(f.load().unwrap().is_empty());
    }

    #[test]
    fn watched_skips_comments_and_blank_lines() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("watched.txt");
        std::fs::write(
            &path,
            "# header comment\n\nippoan/cc-relay#1\n\nippoan/auth-worker#117\n# trailing\n",
        )
        .unwrap();
        let f = WatchedIssuesFile::new(path);
        let set = f.load().unwrap();
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn events_drain_empty_when_no_file() {
        let dir = tempdir().unwrap();
        let f = IssueEventsFile::new(dir.path().join("events.jsonl"));
        assert!(f.drain().unwrap().is_empty());
    }

    #[test]
    fn events_append_and_drain_then_empty() {
        let dir = tempdir().unwrap();
        let f = IssueEventsFile::new(dir.path().join("events.jsonl"));

        let e1 = serde_json::json!({"event_type": "issues.opened", "issue_number": 1});
        let e2 = serde_json::json!({"event_type": "issue_comment.created", "issue_number": 2});

        f.append_event(&e1).unwrap();
        f.append_event(&e2).unwrap();

        let entries = f.drain().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].get("event_type").unwrap(), "issues.opened");
        assert_eq!(entries[1].get("issue_number").unwrap(), 2);

        // drain は rename するので 2 回目は空
        assert!(f.drain().unwrap().is_empty());
    }
}
