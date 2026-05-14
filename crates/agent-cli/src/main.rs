//! `rust-mcp-agent` — clap dispatcher.
//!
//! Subcommands:
//! - `stdio` → calls `agent_mcp::run(config)` (stdio MCP server)
//! - `auth`  → runs the auth-worker device flow and writes
//!   `~/.cc-relay/token` (see issue #33 / ADR-002)
//!
//! The `daemon` subcommand from P2 was deleted along with the
//! `agent-daemon` crate. Broker-specific flags land in P6 / #18.

use std::path::PathBuf;
use std::process::ExitCode;

use agent_broker::auth::{self, default_scopes, AuthConfig, DEFAULT_BASE_URL, DEFAULT_CLIENT_ID};
use agent_broker::{introspect, token_cache};
use agent_mcp::McpConfig;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

/// CLI entry point. Subcommands dispatch into `agent-mcp` or the
/// auth-worker client.
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

    /// Run the auth-worker device flow and write `~/.cc-relay/token`.
    /// See `docs/credentials.md`.
    Auth(AuthArgs),
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

#[derive(Debug, Parser)]
struct AuthArgs {
    /// Auth-worker base URL. Override for staging / local testing.
    #[arg(long, env = "CC_RELAY_AUTH_BASE_URL", default_value = DEFAULT_BASE_URL)]
    base_url: String,

    /// Static client_id this binary uses. The auth-worker device flow
    /// does not validate this — the real gate is
    /// `GITHUB_MCP_USER_ALLOWLIST` on the auth-worker side.
    #[arg(long, env = "CC_RELAY_CLIENT_ID", default_value = DEFAULT_CLIENT_ID)]
    client_id: String,

    /// `INTERNAL_SHARED_SECRET` for the auth-worker introspect endpoint.
    /// Distributed out-of-band by the auth-worker maintainer; see
    /// `docs/credentials.md` §4. Required.
    #[arg(long, env = "CC_RELAY_AUTH_INTROSPECT_SECRET")]
    introspect_secret: Option<String>,

    /// Where to write the token file. Defaults to `~/.cc-relay/token`.
    #[arg(long)]
    token_path: Option<PathBuf>,
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
            Cmd::Auth(args) => run_auth(args).await,
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

async fn run_auth(args: AuthArgs) -> Result<()> {
    let secret = args
        .introspect_secret
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "--introspect-secret (or CC_RELAY_AUTH_INTROSPECT_SECRET) is required; \
                 obtain it from the auth-worker maintainer (see docs/credentials.md §4)"
            )
        })?;

    let token_path = match args.token_path {
        Some(p) => p,
        None => token_cache::default_path().context("resolve default token path")?,
    };

    let cfg = AuthConfig {
        base_url: args.base_url,
        client_id: args.client_id,
        scopes: default_scopes(),
    };
    let http = reqwest::Client::builder()
        .user_agent(concat!("rust-mcp-agent/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("build reqwest client")?;

    eprintln!(
        "rust-mcp-agent: starting device flow against {} (client_id={})",
        cfg.base_url, cfg.client_id,
    );
    let device = auth::start_device_authorization(&http, &cfg)
        .await
        .context("start device_authorization")?;

    let url = device
        .verification_uri_complete
        .as_deref()
        .unwrap_or(&device.verification_uri);
    eprintln!();
    eprintln!("Open this URL in a browser and approve:");
    eprintln!("    {url}");
    if device.verification_uri_complete.is_none() {
        eprintln!("(enter user code: {})", device.user_code);
    }
    eprintln!();
    eprintln!("Waiting for approval (polling every {}s)…", device.interval);

    let token_set = auth::poll_token(&http, &cfg, &device)
        .await
        .context("poll device token")?;

    let active = introspect::introspect(&http, &cfg, secret, &token_set.access_token)
        .await
        .context("introspect new access token")?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "auth-worker returned active=false immediately after issuing the token; \
                 INTERNAL_SHARED_SECRET may be wrong or the JWT aud mismatched"
            )
        })?;

    let github_login = active.github_login.clone();
    let final_set = token_set.with_github_token(active.github_token);
    token_cache::save(&token_path, &final_set).context("write token cache")?;

    println!(
        "ok, you are {github_login} (token written to {})",
        token_path.display()
    );
    Ok(())
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
