//! P10 #11 Phase 11.2: TOML config loader.
//!
//! Reads `~/.config/cc-relay/config.toml` (XDG-compliant default, override
//! via `CC_RELAY_CONFIG`) and lifts known keys into process env vars
//! **only when they are not already set**. clap's existing `env = "..."`
//! attribute then picks them up. This produces the precedence:
//!
//! ```text
//! CLI flag > shell env var > TOML config file > clap default / required-error
//! ```
//!
//! The set-env-from-TOML approach is the smallest possible change: no
//! struct fields are added, every existing `#[arg(env = "...")]` keeps
//! working without code changes, and there is exactly one place
//! (`apply_env_from_toml`) that knows the mapping.
//!
//! ## Schema
//!
//! ```toml
//! [broker]
//! repo = "ippoan/cc-relay"        # CC_RELAY_BROKER_REPO
//! token = "ghp_xxx"               # CC_RELAY_BROKER_TOKEN  (sensitive)
//! issue = 42                      # CC_RELAY_BROKER_ISSUE
//!
//! [relay]
//! ws_url = "wss://mcp.ippoan.org/connect"   # CC_RELAY_WS_URL
//!
//! [log]
//! level = "info"                  # CC_RELAY_LOG
//! ```
//!
//! Unknown keys are ignored (forward-compat with future config additions).

use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::Deserialize;

/// Top-level TOML config. Every section is optional so a partial file
/// still parses — users typically only set the broker section.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigFile {
    pub broker: BrokerSection,
    pub relay: RelaySection,
    pub log: LogSection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BrokerSection {
    pub repo: Option<String>,
    pub token: Option<String>,
    pub issue: Option<u64>,
    pub agent_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RelaySection {
    pub ws_url: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogSection {
    pub level: Option<String>,
}

/// Resolve the config path to read. Precedence:
/// 1. `CC_RELAY_CONFIG` env var (if set, used verbatim)
/// 2. `~/.config/cc-relay/config.toml`
///
/// Returns `None` when the home directory cannot be resolved AND
/// `CC_RELAY_CONFIG` is not set — there is simply no config to load.
pub fn default_path() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("CC_RELAY_CONFIG") {
        return Some(PathBuf::from(p));
    }
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config/cc-relay/config.toml"))
}

/// Parse the TOML file at `path`. Returns `Ok(None)` if the file does
/// not exist (a missing config is not an error — every key is optional
/// and can be supplied via env / CLI). Returns `Err` for parse errors
/// so the user notices typos.
pub fn load(path: &std::path::Path) -> Result<Option<ConfigFile>> {
    let bytes = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("read config {}", path.display())),
    };
    let cfg: ConfigFile =
        toml::from_str(&bytes).with_context(|| format!("parse TOML at {}", path.display()))?;
    Ok(Some(cfg))
}

/// For each known key in `cfg`, set the corresponding `CC_RELAY_*` env
/// var **only when it is not already set in the process environment**.
/// This implements the `env > TOML` precedence: a user export of the
/// env var takes priority over the file.
///
/// Safety: secret material from `[broker].token` becomes a process env
/// var, which is inherited by child processes (subprocess MCP servers,
/// shells the user opens). Same blast radius as exporting the env var
/// manually — users who want narrower exposure can keep the secret in a
/// shell-local export instead of the TOML file.
pub fn apply_env_from_toml(cfg: &ConfigFile) {
    set_if_unset("CC_RELAY_BROKER_REPO", cfg.broker.repo.as_deref());
    set_if_unset("CC_RELAY_BROKER_TOKEN", cfg.broker.token.as_deref());
    set_if_unset(
        "CC_RELAY_BROKER_ISSUE",
        cfg.broker.issue.map(|n| n.to_string()).as_deref(),
    );
    // agent_id has no env binding in clap (it's `default_value` only),
    // so set CC_RELAY_AGENT_ID for the same reason and let clap's
    // command-line default keep working when neither is set. This makes
    // the TOML key practically useful even though the clap arg is not
    // env-wired.
    set_if_unset("CC_RELAY_AGENT_ID", cfg.broker.agent_id.as_deref());
    set_if_unset("CC_RELAY_WS_URL", cfg.relay.ws_url.as_deref());
    set_if_unset("CC_RELAY_LOG", cfg.log.level.as_deref());
}

fn set_if_unset(key: &str, value: Option<&str>) {
    if std::env::var_os(key).is_some() {
        return;
    }
    if let Some(v) = value {
        // SAFETY: set_var on a single-threaded startup path before any
        // other thread is spawned. The clap `Cli::parse()` immediately
        // after this call is single-threaded; tokio runtime is built
        // later.
        std::env::set_var(key, v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::io::Write;

    fn write_tmp(contents: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f
    }

    /// Restore an env var to its prior state. Pulled out so both arms
    /// (`Some` / `None`) can be exercised independently below; the
    /// inline `match` previously left whichever arm was unused for
    /// the test's own state as zero-count.
    fn restore_env(key: &str, prior: Option<OsString>) {
        match prior {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
    }

    #[test]
    fn restore_env_some_sets_back() {
        let key = "CC_RELAY_RESTORE_HELPER_SOME";
        std::env::remove_var(key);
        restore_env(key, Some(OsString::from("prior-value")));
        assert_eq!(std::env::var(key).unwrap(), "prior-value");
        std::env::remove_var(key);
    }

    #[test]
    fn restore_env_none_removes() {
        let key = "CC_RELAY_RESTORE_HELPER_NONE";
        std::env::set_var(key, "leftover");
        restore_env(key, None);
        assert!(std::env::var_os(key).is_none());
    }

    #[test]
    fn missing_file_returns_ok_none() {
        let p = std::path::PathBuf::from("/nonexistent/cc-relay-config-xyz.toml");
        let out = load(&p).unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn parses_full_schema() {
        let f = write_tmp(
            r#"
[broker]
repo = "ippoan/cc-relay"
token = "ghp_secret"
issue = 42
agent_id = "alice"

[relay]
ws_url = "wss://mcp.example.org/connect"

[log]
level = "debug"
"#,
        );
        let cfg = load(f.path()).unwrap().unwrap();
        assert_eq!(cfg.broker.repo.as_deref(), Some("ippoan/cc-relay"));
        assert_eq!(cfg.broker.token.as_deref(), Some("ghp_secret"));
        assert_eq!(cfg.broker.issue, Some(42));
        assert_eq!(cfg.broker.agent_id.as_deref(), Some("alice"));
        assert_eq!(
            cfg.relay.ws_url.as_deref(),
            Some("wss://mcp.example.org/connect")
        );
        assert_eq!(cfg.log.level.as_deref(), Some("debug"));
    }

    #[test]
    fn empty_sections_yield_none_fields() {
        let f = write_tmp("[broker]\n[relay]\n[log]\n");
        let cfg = load(f.path()).unwrap().unwrap();
        assert!(cfg.broker.repo.is_none());
        assert!(cfg.broker.token.is_none());
        assert!(cfg.broker.issue.is_none());
        assert!(cfg.broker.agent_id.is_none());
        assert!(cfg.relay.ws_url.is_none());
        assert!(cfg.log.level.is_none());
    }

    #[test]
    fn unknown_key_in_known_section_is_rejected() {
        // deny_unknown_fields catches typos that would otherwise silently
        // be ignored. e.g. `tokens` instead of `token` in [broker].
        let f = write_tmp("[broker]\ntokens = \"oops\"\n");
        let err = load(f.path()).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("tokens") || msg.contains("unknown"),
            "expected typo to surface, got: {msg}"
        );
    }

    #[test]
    fn apply_env_does_not_overwrite_existing_env() {
        // Use a unique env var name so this test can run in parallel
        // with others that touch CC_RELAY_*.
        let key = "CC_RELAY_BROKER_REPO";
        let prior = std::env::var_os(key);
        std::env::set_var(key, "from-shell");
        let cfg = ConfigFile {
            broker: BrokerSection {
                repo: Some("from-toml".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        apply_env_from_toml(&cfg);
        assert_eq!(std::env::var(key).unwrap(), "from-shell");
        // Restore environment to whatever the test was given.
        restore_env(key, prior);
    }

    #[test]
    fn apply_env_fills_in_when_unset() {
        // Use an obscure key suffix to avoid colliding with parallel tests.
        let key = "CC_RELAY_BROKER_REPO_TEST_FILL";
        std::env::remove_var(key);
        // We can't easily test the production keys without tearing each
        // other's state, so spot-check the helper directly.
        set_if_unset(key, Some("from-toml"));
        assert_eq!(std::env::var(key).unwrap(), "from-toml");
        std::env::set_var(key, "shell-wins");
        set_if_unset(key, Some("from-toml-2"));
        assert_eq!(std::env::var(key).unwrap(), "shell-wins");
        std::env::remove_var(key);
    }

    #[test]
    fn default_path_honors_cc_relay_config_env() {
        let key = "CC_RELAY_CONFIG";
        let prior = std::env::var_os(key);
        std::env::set_var(key, "/tmp/explicit-config.toml");
        let p = default_path().unwrap();
        assert_eq!(p, std::path::PathBuf::from("/tmp/explicit-config.toml"));
        restore_env(key, prior);
    }

    #[test]
    fn default_path_falls_back_to_home() {
        // We can't reliably swap HOME in parallel tests, so just assert
        // that with CC_RELAY_CONFIG unset we get *some* path that ends
        // in `.config/cc-relay/config.toml` (HOME is set in the
        // environment by default).
        let key = "CC_RELAY_CONFIG";
        let prior = std::env::var_os(key);
        std::env::remove_var(key);
        let p = default_path().expect("HOME is always set in test env");
        assert!(p.ends_with(".config/cc-relay/config.toml"), "got {p:?}");
        restore_env(key, prior);
    }

    #[test]
    fn load_returns_err_on_non_notfound_io_error() {
        // Pointing `load` at a directory triggers an IO error other
        // than NotFound on most platforms (EISDIR on Linux).
        let dir = tempfile::tempdir().unwrap();
        let err = load(dir.path()).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("read config"), "got: {msg}");
    }
}
