//! `rust-mcp-agent` — clap dispatcher.
//!
//! Two subcommands:
//! - `daemon` → calls `agent_daemon::run(config)` (the long-lived state holder)
//! - `stdio`  → calls `agent_mcp::run(config)` (the short-lived MCP relay)
//!
//! Global flags (`--log-level`, `--config`) live on the top-level
//! [`Cli`] struct so they apply to both subcommands.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use agent_daemon::DaemonConfig;
use agent_mcp::McpConfig;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

/// CLI entry point. Subcommands dispatch into `agent-daemon` and
/// `agent-mcp` libraries.
#[derive(Debug, Parser)]
#[command(version, about = "cc-relay agent (daemon + stdio MCP server)")]
struct Cli {
    /// `tracing` filter: `info`, `debug`, `agent_daemon=trace`, etc.
    #[arg(long, global = true, default_value = "info", env = "CC_RELAY_LOG")]
    log_level: String,

    /// Path to a TOML config file. Wired in P6 (#11); currently accepted
    /// and ignored so hooks can pass it through unconditionally.
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Run the long-lived daemon (watcher + WS client + HTTP server).
    Daemon(DaemonArgs),
    /// Run the stdio MCP server. Used by `claude --mcp-config`.
    Stdio(StdioArgs),
}

#[derive(Debug, Parser)]
struct DaemonArgs {
    /// Identifier for this agent within the session.
    #[arg(long, env = "CC_RELAY_AGENT_ID")]
    agent_id: String,

    /// Repository root to watch.
    #[arg(long)]
    worktree: PathBuf,

    /// `wss://<worker>.workers.dev/session/<id>` URL.
    #[arg(long, env = "CC_RELAY_WS_URL")]
    ws_url: String,

    /// JSONL inbox path. Read by `check-inbox.sh` at `UserPromptSubmit`.
    #[arg(long, default_value = "/tmp/agent-inbox.jsonl")]
    inbox: PathBuf,

    /// Loopback bind for the local hook HTTP server.
    #[arg(long, default_value = "127.0.0.1:9876")]
    http_bind: SocketAddr,

    /// Optional human-readable repo label (e.g. `ippoan/cc-relay`).
    #[arg(long)]
    repo: Option<String>,
}

#[derive(Debug, Parser)]
struct StdioArgs {
    /// Base URL of the local daemon's HTTP server.
    #[arg(long, default_value = "http://127.0.0.1:9876")]
    daemon_url: String,

    /// Same inbox path the daemon writes to.
    #[arg(long, default_value = "/tmp/agent-inbox.jsonl")]
    inbox: PathBuf,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    if let Err(e) = init_tracing(&cli.log_level, &cli.cmd) {
        eprintln!("rust-mcp-agent: failed to init logging: {e:#}");
        return ExitCode::FAILURE;
    }

    if let Some(path) = &cli.config {
        // P6 (#11) will load this. For now we just acknowledge it so
        // hooks can pass --config unconditionally without breaking.
        tracing::info!(config = %path.display(), "config file accepted but not yet read (P6/#11)");
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("rust-mcp-agent: failed to build tokio runtime: {e:#}");
            return ExitCode::FAILURE;
        }
    };

    let result: Result<()> = runtime.block_on(async {
        match cli.cmd {
            Cmd::Daemon(args) => run_daemon(args).await,
            Cmd::Stdio(args) => run_stdio(args).await,
        }
    });

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("rust-mcp-agent: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run_daemon(args: DaemonArgs) -> Result<()> {
    let config = DaemonConfig {
        agent_id: args.agent_id,
        worktree: args.worktree,
        ws_url: args.ws_url,
        inbox: args.inbox,
        http_bind: args.http_bind,
        repo: args.repo,
    };
    agent_daemon::run(config).await.context("daemon exited")
}

async fn run_stdio(args: StdioArgs) -> Result<()> {
    let config = McpConfig {
        daemon_url: args.daemon_url,
        inbox: args.inbox,
    };
    agent_mcp::run(config).await.context("stdio mcp exited")
}

/// Set up tracing. For `stdio` we route logs to stderr only — the stdout
/// channel is MCP protocol and must stay clean.
fn init_tracing(filter: &str, cmd: &Cmd) -> Result<()> {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let env = EnvFilter::try_new(filter)
        .with_context(|| format!("invalid --log-level filter: {filter:?}"))?;

    let layer = match cmd {
        Cmd::Stdio(_) => fmt::layer().with_writer(std::io::stderr).with_ansi(false),
        Cmd::Daemon(_) => fmt::layer().with_writer(std::io::stderr),
    };

    tracing_subscriber::registry().with(env).with(layer).init();
    Ok(())
}
