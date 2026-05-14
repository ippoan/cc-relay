//! `rust-mcp-agent` — clap dispatcher.
//!
//! After ADR-001 there is a single subcommand:
//! - `stdio` → calls `agent_mcp::run(config)` (stdio MCP server)
//!
//! The `daemon` subcommand from P2 was deleted along with the
//! `agent-daemon` crate. Broker-specific flags land in P6 / #18.

use std::path::PathBuf;
use std::process::ExitCode;

use agent_mcp::McpConfig;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

/// CLI entry point. The single subcommand dispatches into `agent-mcp`.
#[derive(Debug, Parser)]
#[command(version, about = "cc-relay agent (stdio MCP server)")]
struct Cli {
    /// `tracing` filter: `info`, `debug`, `agent_mcp=trace`, etc.
    #[arg(long, global = true, default_value = "info", env = "CC_RELAY_LOG")]
    log_level: String,

    /// Path to a TOML config file. Wired in #11; currently accepted
    /// and ignored so hooks can pass it through unconditionally.
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Run the stdio MCP server. Used by `claude --mcp-config`.
    Stdio(StdioArgs),
}

#[derive(Debug, Parser)]
struct StdioArgs {
    /// Stub field carried over from P2. P5 / #17 replaces this with a
    /// `--broker` flag and the URL is no longer used.
    #[arg(long, default_value = "http://127.0.0.1:9876")]
    daemon_url: String,

    /// JSONL inbox path. Read by `check-inbox.sh` at `UserPromptSubmit`
    /// and by the `get_inbox` MCP tool.
    #[arg(long, default_value = "/tmp/agent-inbox.jsonl")]
    inbox: PathBuf,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    if let Err(e) = init_tracing(&cli.log_level) {
        eprintln!("rust-mcp-agent: failed to init logging: {e:#}");
        return ExitCode::FAILURE;
    }

    if let Some(path) = &cli.config {
        // #11 will load this. For now we just acknowledge it so hooks
        // can pass --config unconditionally without breaking.
        tracing::info!(config = %path.display(), "config file accepted but not yet read (#11)");
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

async fn run_stdio(args: StdioArgs) -> Result<()> {
    let config = McpConfig {
        daemon_url: args.daemon_url,
        inbox: args.inbox,
    };
    agent_mcp::run(config).await.context("stdio mcp exited")
}

/// Set up tracing. Logs always go to stderr — stdout is reserved for the
/// MCP protocol stream and must stay clean.
fn init_tracing(filter: &str) -> Result<()> {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let env = EnvFilter::try_new(filter)
        .with_context(|| format!("invalid --log-level filter: {filter:?}"))?;

    let layer = fmt::layer().with_writer(std::io::stderr).with_ansi(false);

    tracing_subscriber::registry().with(env).with(layer).init();
    Ok(())
}
