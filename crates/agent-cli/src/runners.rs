//! Runtime shims that wire the parsed `Cmd` into real I/O — `stdio::run`
//! over real stdin/stdout, `relay::run` opening a real WebSocket, etc.
//!
//! Excluded from line-coverage enforcement (`scripts/coverage_gate.sh`)
//! because the bodies are dominated by dependency construction whose
//! testable cores live elsewhere:
//! - `parse_owner_repo`, `resolve_token_path`, `build_runtime`,
//!   `init_tracing`, `dispatch` — `agent-cli/src/lib.rs`
//! - `RelayServer::handle_jsonrpc`, `pump_inbound`, `handle_event_frame` —
//!   `agent-mcp/src/relay.rs`
//! - `process_line`, `run_io` — `agent-mcp/src/{stdio,channel}.rs`
//!
//! What's left here is "build dependency, hand it to the I/O loop" — the
//! kind of plumbing that breaks visibly the moment a developer flips a
//! sub-command flag, so a unit test buys nothing over running the binary
//! itself.

use std::sync::Arc;

use agent_broker::auth::{self, default_scopes, AuthConfig};
use agent_broker::{introspect, token_cache, Broker, CursorStore, GitHubBroker};
use agent_mcp::channel::run as channel_run;
use agent_mcp::relay::{run as relay_run, RelayConfig, RelayServer};
use anyhow::{Context, Result};

use crate::{parse_owner_repo, resolve_token_path, AuthArgs, ChannelArgs, RelayArgs, StdioArgs};

pub async fn run_stdio(args: StdioArgs) -> Result<()> {
    let (owner, repo) = parse_owner_repo(&args.broker_repo)?;
    let session_id = format!("{}/{}#{}", owner, repo, args.broker_issue);
    let broker = GitHubBroker::new(
        owner,
        repo,
        args.broker_issue,
        args.agent_id,
        &args.broker_token,
    )
    .context("build GitHubBroker")?;
    let broker: Arc<dyn Broker> = Arc::new(broker);
    let cursor_store = Arc::new(CursorStore::new(&session_id).context("init CursorStore")?);
    let server = RelayServer::new(broker)
        .with_persisted_cursor(cursor_store)
        .await;
    agent_mcp::stdio::run(server)
        .await
        .context("stdio mcp exited")
}

pub async fn run_relay(args: RelayArgs) -> Result<()> {
    let token_path = resolve_token_path(args.token_path)?;
    let token_set = token_cache::load(&token_path)
        .with_context(|| format!("read token cache at {}", token_path.display()))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no cached token at {}; run `rust-mcp-agent auth` first",
                token_path.display()
            )
        })?;

    let (owner, repo) = parse_owner_repo(&args.broker_repo)?;
    let broker = GitHubBroker::new(
        owner,
        repo,
        args.broker_issue,
        args.agent_id,
        &args.broker_token,
    )
    .context("build GitHubBroker")?;
    let broker: Arc<dyn Broker> = Arc::new(broker);

    let server = RelayServer::new(broker);
    let cfg = RelayConfig {
        ws_url: args.ws_url,
        access_token: token_set.access_token,
    };
    relay_run(server, cfg).await.context("relay loop exited")
}

pub async fn run_channel_cmd(args: ChannelArgs) -> Result<()> {
    let token_path = resolve_token_path(args.token_path)?;
    let token_set = token_cache::load(&token_path)
        .with_context(|| format!("read token cache at {}", token_path.display()))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no cached token at {}; run `rust-mcp-agent auth` first",
                token_path.display()
            )
        })?;

    let (owner, repo) = parse_owner_repo(&args.broker_repo)?;
    let broker = GitHubBroker::new(
        owner,
        repo,
        args.broker_issue,
        args.agent_id,
        &args.broker_token,
    )
    .context("build GitHubBroker")?;
    let broker: Arc<dyn Broker> = Arc::new(broker);

    let server = RelayServer::new(broker);
    let cfg = RelayConfig {
        ws_url: args.ws_url,
        access_token: token_set.access_token,
    };
    channel_run(server, cfg)
        .await
        .context("channel loop exited")
}

pub async fn run_auth(args: AuthArgs) -> Result<()> {
    let secret = args.introspect_secret.as_deref().filter(|s| !s.is_empty());
    let token_path = resolve_token_path(args.token_path)?;
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

/// Process entry point used by the thin `main.rs` shim.
///
/// Order matters here: the TOML config is loaded BEFORE `Cli::parse()`
/// so that `apply_env_from_toml` can inject `CC_RELAY_*` env vars that
/// clap then picks up via `#[arg(env = "...")]`. This produces the
/// precedence
///   CLI flag > shell env > TOML > clap default / required-error.
pub fn run() -> std::process::ExitCode {
    use clap::Parser;

    let cli_config_path = scan_argv_for_config_flag();
    let toml_path = cli_config_path.or_else(crate::config::default_path);
    if let Some(p) = toml_path.as_deref() {
        match crate::config::load(p) {
            Ok(Some(cfg)) => crate::config::apply_env_from_toml(&cfg),
            Ok(None) => {} // missing file is fine — every key is optional
            Err(e) => {
                eprintln!("rust-mcp-agent: {e:#}");
                return std::process::ExitCode::FAILURE;
            }
        }
    }

    let cli = crate::Cli::parse();

    if let Err(e) = crate::init_tracing(&cli.log_level) {
        eprintln!("rust-mcp-agent: failed to init logging: {e:#}");
        return std::process::ExitCode::FAILURE;
    }

    if let Some(path) = &cli.config {
        tracing::info!(config = %path.display(), "config loaded");
    }

    let runtime = match crate::build_runtime() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("rust-mcp-agent: {e:#}");
            return std::process::ExitCode::FAILURE;
        }
    };

    match runtime.block_on(crate::dispatch(cli.cmd)) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("rust-mcp-agent: {e:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// Manual argv scan for `--config <path>` / `--config=<path>`. Used
/// before `Cli::parse()` so the TOML loader can run first. Returns
/// `None` if the flag is absent or its value is missing (clap will
/// emit the proper error during parse anyway).
fn scan_argv_for_config_flag() -> Option<std::path::PathBuf> {
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        if a == "--config" {
            return args.next().map(std::path::PathBuf::from);
        }
        if let Some(rest) = a.strip_prefix("--config=") {
            return Some(std::path::PathBuf::from(rest));
        }
    }
    None
}
