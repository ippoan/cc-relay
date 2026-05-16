//! `agent-cli` library — clap types + small testable helpers.
//!
//! The runtime "shim" functions that wire arguments into real I/O loops
//! (`stdio::run` reading real stdin, `relay::run` opening a real
//! WebSocket, `auth::start_device_authorization` hitting auth.ippoan.org)
//! live in [`runners`]; that file is excluded from the coverage gate
//! because it has no testable seam — every line is `let x =
//! Y::new(args)` or `loop_fn(server).await`. The interesting logic
//! reachable from the dispatcher (`parse_owner_repo`, `init_tracing`,
//! `dispatch` discrimination) lives here and is fully unit-tested.

use std::path::PathBuf;
use std::process::ExitCode;

use agent_broker::auth::{DEFAULT_BASE_URL, DEFAULT_CLIENT_ID};
use agent_broker::token_cache;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

pub mod config;
pub mod runners;

/// CLI entry point. Subcommands dispatch into `agent-mcp` or the
/// auth-worker client.
#[derive(Debug, Parser)]
#[command(version, about = "cc-relay agent (stdio MCP server)")]
pub struct Cli {
    /// `tracing` filter: `info`, `debug`, `agent_mcp=trace`, etc.
    #[arg(long, global = true, default_value = "info", env = "CC_RELAY_LOG")]
    pub log_level: String,

    /// Path to a TOML config file. Default location:
    /// `~/.config/cc-relay/config.toml` (override via `CC_RELAY_CONFIG`).
    /// Precedence: CLI flag > shell env > TOML > clap default. See
    /// `crates/agent-cli/src/config.rs` for the schema.
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Debug, Subcommand)]
pub enum Cmd {
    /// Run the stdio MCP server.
    Stdio(StdioArgs),
    /// Run the auth-worker device flow and write `~/.cc-relay/token`.
    Auth(AuthArgs),
    /// Outbound WS to the auth-worker MCP relay.
    Relay(RelayArgs),
    /// Claude Code Channel mode (stdio + outbound WS).
    Channel(ChannelArgs),
}

#[derive(Debug, Parser)]
pub struct StdioArgs {
    #[arg(long, env = "CC_RELAY_BROKER_REPO")]
    pub broker_repo: String,
    #[arg(long, env = "CC_RELAY_BROKER_TOKEN", hide_env_values = true)]
    pub broker_token: String,
    #[arg(long, env = "CC_RELAY_BROKER_ISSUE")]
    pub broker_issue: u64,
    #[arg(long, env = "CC_RELAY_AGENT_ID", default_value = "stdio-agent")]
    pub agent_id: String,
}

#[derive(Debug, Parser)]
pub struct RelayArgs {
    #[arg(
        long,
        env = "CC_RELAY_WS_URL",
        default_value = "wss://mcp-staging.ippoan.org/connect"
    )]
    pub ws_url: String,
    #[arg(long)]
    pub token_path: Option<PathBuf>,
    #[arg(long, env = "CC_RELAY_BROKER_REPO")]
    pub broker_repo: String,
    #[arg(long, env = "CC_RELAY_BROKER_TOKEN", hide_env_values = true)]
    pub broker_token: String,
    #[arg(long, env = "CC_RELAY_BROKER_ISSUE")]
    pub broker_issue: u64,
    #[arg(long, env = "CC_RELAY_AGENT_ID", default_value = "host-broker")]
    pub agent_id: String,
}

#[derive(Debug, Parser)]
pub struct ChannelArgs {
    #[arg(
        long,
        env = "CC_RELAY_WS_URL",
        default_value = "wss://mcp-staging.ippoan.org/connect"
    )]
    pub ws_url: String,
    #[arg(long)]
    pub token_path: Option<PathBuf>,
    #[arg(long, env = "CC_RELAY_BROKER_REPO")]
    pub broker_repo: String,
    #[arg(long, env = "CC_RELAY_BROKER_TOKEN", hide_env_values = true)]
    pub broker_token: String,
    #[arg(long, env = "CC_RELAY_BROKER_ISSUE")]
    pub broker_issue: u64,
    #[arg(long, env = "CC_RELAY_AGENT_ID", default_value = "host-broker")]
    pub agent_id: String,
}

#[derive(Debug, Parser)]
pub struct AuthArgs {
    #[arg(long, env = "CC_RELAY_AUTH_BASE_URL", default_value = DEFAULT_BASE_URL)]
    pub base_url: String,
    #[arg(long, env = "CC_RELAY_CLIENT_ID", default_value = DEFAULT_CLIENT_ID)]
    pub client_id: String,
    #[arg(long, env = "CC_RELAY_AUTH_INTROSPECT_SECRET")]
    pub introspect_secret: Option<String>,
    #[arg(long)]
    pub token_path: Option<PathBuf>,
}

/// Parse `owner/repo`. Extracted so the four runners share one impl
/// with one test surface.
pub fn parse_owner_repo(s: &str) -> Result<(String, String)> {
    let (owner, repo) = s
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("--broker-repo must be 'owner/repo'"))?;
    if owner.is_empty() || repo.is_empty() {
        anyhow::bail!("--broker-repo must be 'owner/repo'");
    }
    Ok((owner.to_string(), repo.to_string()))
}

/// Resolve the token path: honor `--token-path` if given, else
/// fall back to `token_cache::default_path()` (≈ `~/.cc-relay/token`).
pub fn resolve_token_path(explicit: Option<PathBuf>) -> Result<PathBuf> {
    match explicit {
        Some(p) => Ok(p),
        None => token_cache::default_path().context("resolve default token path"),
    }
}

/// Build the multi-threaded tokio runtime used by the binary.
pub fn build_runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")
}

/// Install a global tracing subscriber. Called once at process start
/// from [`runners::run`].
pub fn init_tracing(filter: &str) -> Result<()> {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let env = EnvFilter::try_new(filter)
        .with_context(|| format!("invalid --log-level filter: {filter:?}"))?;
    let layer = fmt::layer().with_writer(std::io::stderr).with_ansi(false);
    tracing_subscriber::registry()
        .with(env)
        .with(layer)
        .try_init()
        .map_err(|e| anyhow::anyhow!("init tracing: {e}"))
}

/// Discriminate the parsed `Cmd` and route into the right async
/// runner. The runner bodies live in [`runners`] (excluded from the
/// coverage gate); this match arm itself is what we want to lock in
/// against future subcommand additions.
pub async fn dispatch(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Stdio(args) => runners::run_stdio(args).await,
        Cmd::Auth(args) => runners::run_auth(args).await,
        Cmd::Relay(args) => runners::run_relay(args).await,
        Cmd::Channel(args) => runners::run_channel_cmd(args).await,
    }
}

/// Process entry point used by the thin `main.rs` shim. Forwards to
/// [`runners::run`] so the binary's `main()` stays one line.
pub fn run() -> ExitCode {
    runners::run()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parse_owner_repo_valid() {
        let (o, r) = parse_owner_repo("ippoan/cc-relay").unwrap();
        assert_eq!(o, "ippoan");
        assert_eq!(r, "cc-relay");
    }

    #[test]
    fn parse_owner_repo_rejects_no_slash() {
        let e = parse_owner_repo("missing-slash").unwrap_err();
        assert!(e.to_string().contains("owner/repo"));
    }

    #[test]
    fn parse_owner_repo_rejects_empty_owner() {
        let e = parse_owner_repo("/repo").unwrap_err();
        assert!(e.to_string().contains("owner/repo"));
    }

    #[test]
    fn parse_owner_repo_rejects_empty_repo() {
        let e = parse_owner_repo("owner/").unwrap_err();
        assert!(e.to_string().contains("owner/repo"));
    }

    #[test]
    fn resolve_token_path_explicit_passthrough() {
        let p = PathBuf::from("/tmp/whatever.json");
        let got = resolve_token_path(Some(p.clone())).unwrap();
        assert_eq!(got, p);
    }

    #[test]
    fn resolve_token_path_default_is_some_path() {
        let got = resolve_token_path(None).unwrap();
        assert!(!got.as_os_str().is_empty());
    }

    #[test]
    fn build_runtime_succeeds() {
        let rt = build_runtime().expect("build runtime");
        drop(rt);
    }

    #[test]
    fn cli_parses_stdio_subcommand() {
        let cli = Cli::try_parse_from([
            "rust-mcp-agent",
            "stdio",
            "--broker-repo",
            "o/r",
            "--broker-token",
            "tok",
            "--broker-issue",
            "1",
        ])
        .expect("parse");
        match cli.cmd {
            Cmd::Stdio(a) => {
                assert_eq!(a.broker_repo, "o/r");
                assert_eq!(a.broker_token, "tok");
                assert_eq!(a.broker_issue, 1);
                assert_eq!(a.agent_id, "stdio-agent");
            }
            _ => panic!("expected stdio"),
        }
    }

    #[test]
    fn cli_parses_relay_subcommand() {
        let cli = Cli::try_parse_from([
            "rust-mcp-agent",
            "relay",
            "--broker-repo",
            "o/r",
            "--broker-token",
            "tok",
            "--broker-issue",
            "1",
        ])
        .expect("parse");
        assert!(matches!(cli.cmd, Cmd::Relay(_)));
    }

    #[test]
    fn cli_parses_channel_subcommand() {
        let cli = Cli::try_parse_from([
            "rust-mcp-agent",
            "channel",
            "--broker-repo",
            "o/r",
            "--broker-token",
            "tok",
            "--broker-issue",
            "1",
        ])
        .expect("parse");
        assert!(matches!(cli.cmd, Cmd::Channel(_)));
    }

    #[test]
    fn cli_parses_auth_subcommand() {
        let cli = Cli::try_parse_from(["rust-mcp-agent", "auth"]).expect("parse");
        assert!(matches!(cli.cmd, Cmd::Auth(_)));
    }

    #[test]
    fn init_tracing_rejects_invalid_filter() {
        let e = init_tracing("=== invalid ===").unwrap_err();
        assert!(e.to_string().contains("invalid --log-level filter"));
    }

    #[test]
    fn init_tracing_double_init_errors() {
        // First call wins; second call should hit the `try_init()` error
        // branch ("a global default trace dispatcher has already been set").
        let _ = init_tracing("info");
        let e = init_tracing("info").unwrap_err();
        assert!(e.to_string().contains("init tracing"));
    }

    #[tokio::test]
    async fn dispatch_stdio_fails_for_bad_broker_repo() {
        let args = StdioArgs {
            broker_repo: "no-slash".into(),
            broker_token: "tok".into(),
            broker_issue: 1,
            agent_id: "test".into(),
        };
        let e = dispatch(Cmd::Stdio(args)).await.unwrap_err();
        assert!(e.to_string().contains("owner/repo"));
    }

    #[tokio::test]
    async fn dispatch_relay_fails_for_bad_broker_repo() {
        let dir = std::env::temp_dir().join(format!("ccr-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let token_path = dir.join("never.json");
        let args = RelayArgs {
            ws_url: "ws://127.0.0.1:1".into(),
            token_path: Some(token_path),
            broker_repo: "no-slash".into(),
            broker_token: "tok".into(),
            broker_issue: 1,
            agent_id: "test".into(),
        };
        let e = dispatch(Cmd::Relay(args)).await.unwrap_err();
        let s = e.to_string();
        assert!(s.contains("token") || s.contains("owner/repo"), "got: {s}");
    }

    #[tokio::test]
    async fn dispatch_channel_fails_for_bad_broker_repo() {
        let dir = std::env::temp_dir().join(format!("ccr-test-ch-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let token_path = dir.join("never.json");
        let args = ChannelArgs {
            ws_url: "ws://127.0.0.1:1".into(),
            token_path: Some(token_path),
            broker_repo: "no-slash".into(),
            broker_token: "tok".into(),
            broker_issue: 1,
            agent_id: "test".into(),
        };
        let e = dispatch(Cmd::Channel(args)).await.unwrap_err();
        let s = e.to_string();
        assert!(s.contains("token") || s.contains("owner/repo"), "got: {s}");
    }

    #[tokio::test]
    async fn dispatch_auth_fails_against_unreachable_host() {
        let dir = std::env::temp_dir().join(format!("ccr-test-auth-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let args = AuthArgs {
            base_url: "http://127.0.0.1:1".into(),
            client_id: "x".into(),
            introspect_secret: Some(String::new()),
            token_path: Some(dir.join("auth-out.json")),
        };
        let e = dispatch(Cmd::Auth(args)).await.unwrap_err();
        let s = format!("{e:#}");
        assert!(s.contains("device_authorization") || s.contains("connect"));
    }
}
