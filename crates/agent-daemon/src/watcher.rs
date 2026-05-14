//! Filesystem watcher. Wraps `notify-debouncer-full` and applies
//! `.gitignore` filtering so we do not flood the WebSocket with churn
//! from `target/`, `node_modules/`, etc.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use agent_core::protocol::{FileEventKind, WireMessage};
use anyhow::{Context, Result};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use notify::{EventKind, RecursiveMode, Watcher};
use notify_debouncer_full::{new_debouncer, DebounceEventResult, DebouncedEvent, FileIdMap};
use tokio::sync::mpsc;

use crate::now_millis;

/// Hardcoded ignore patterns applied on top of `.gitignore`. These are
/// directories we never want notifications from, even if the repo's
/// `.gitignore` is missing or stale.
const ALWAYS_IGNORE: &[&str] = &[".git", "target", "node_modules", ".wrangler", ".venv"];

/// 200ms is a long enough window to collapse "save → toolchain rewrites
/// the same file" bursts (rustfmt, prettier, etc.) without making the
/// UI feel laggy.
const DEBOUNCE: Duration = Duration::from_millis(200);

/// Spawn a blocking watcher thread and forward debounced, gitignore-
/// filtered events to the `out` channel as `WireMessage::FileEvent`s.
///
/// The function returns the [`notify_debouncer_full::Debouncer`] handle
/// so the caller can keep it alive for the lifetime of the daemon; when
/// it is dropped, the watcher thread exits.
pub fn spawn(
    agent_id: &str,
    worktree: &Path,
    out: mpsc::Sender<WireMessage>,
) -> Result<notify_debouncer_full::Debouncer<notify::RecommendedWatcher, FileIdMap>> {
    let worktree = worktree
        .canonicalize()
        .with_context(|| format!("canonicalize worktree {}", worktree.display()))?;
    let agent_id = agent_id.to_owned();

    let gitignore = build_gitignore(&worktree);
    let root = worktree.clone();

    let mut debouncer = new_debouncer(DEBOUNCE, None, move |res: DebounceEventResult| match res {
        Ok(events) => {
            for ev in events {
                if let Some(msg) = translate(&agent_id, &root, &gitignore, ev) {
                    // Block here is intentional: the debouncer thread
                    // is not async, and dropping events silently would
                    // hide real watcher activity from agents.
                    if out.blocking_send(msg).is_err() {
                        return; // receiver gone, daemon shutting down
                    }
                }
            }
        }
        Err(errors) => {
            for e in errors {
                tracing::warn!(error = %e, "watcher error");
            }
        }
    })?;

    debouncer
        .watcher()
        .watch(&worktree, RecursiveMode::Recursive)
        .with_context(|| format!("watch {}", worktree.display()))?;

    Ok(debouncer)
}

fn build_gitignore(worktree: &Path) -> Arc<Gitignore> {
    let mut builder = GitignoreBuilder::new(worktree);
    let _ = builder.add(worktree.join(".gitignore"));
    let gi = builder.build().unwrap_or_else(|_| Gitignore::empty());
    Arc::new(gi)
}

fn translate(
    agent_id: &str,
    worktree: &Path,
    gitignore: &Gitignore,
    ev: DebouncedEvent,
) -> Option<WireMessage> {
    let path = ev.event.paths.into_iter().next()?;
    if is_ignored(&path, worktree, gitignore) {
        return None;
    }

    let kind = match ev.event.kind {
        EventKind::Create(_) => FileEventKind::Created,
        EventKind::Modify(_) => FileEventKind::Modified,
        EventKind::Remove(_) => FileEventKind::Removed,
        // notify-debouncer-full coalesces rename pairs into a Modify or a
        // separate Create+Remove depending on the platform. There is no
        // dedicated Rename variant in our protocol; we expose the raw
        // platform-level event when it surfaces.
        _ => FileEventKind::Modified,
    };

    let relative = path
        .strip_prefix(worktree)
        .map(PathBuf::from)
        .unwrap_or(path);

    Some(WireMessage::FileEvent {
        agent_id: agent_id.to_owned(),
        path: relative.to_string_lossy().into_owned(),
        kind,
        timestamp: now_millis(),
    })
}

fn is_ignored(path: &Path, worktree: &Path, gitignore: &Gitignore) -> bool {
    let rel = match path.strip_prefix(worktree) {
        Ok(r) => r,
        Err(_) => return true, // outside the worktree → not our problem
    };

    for component in rel.components() {
        if let std::path::Component::Normal(name) = component {
            if let Some(name) = name.to_str() {
                if ALWAYS_IGNORE.contains(&name) {
                    return true;
                }
            }
        }
    }

    gitignore
        .matched_path_or_any_parents(rel, path.is_dir())
        .is_ignore()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn always_ignore_blocks_target() {
        let wt = std::env::temp_dir().join("cc-relay-test-watcher-1");
        std::fs::create_dir_all(wt.join("target")).unwrap();
        let path = wt.join("target/foo.rs");
        assert!(is_ignored(&path, &wt, &Gitignore::empty()));
    }

    #[test]
    fn normal_path_is_not_ignored() {
        let wt = std::env::temp_dir().join("cc-relay-test-watcher-2");
        std::fs::create_dir_all(&wt).unwrap();
        let path = wt.join("src/lib.rs");
        assert!(!is_ignored(&path, &wt, &Gitignore::empty()));
    }
}
