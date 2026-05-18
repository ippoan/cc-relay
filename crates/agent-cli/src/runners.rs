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
use agent_broker::token_cache::TokenSet;
use agent_broker::{introspect, token_cache, Broker, CursorStore, GitHubBroker};
use agent_mcp::channel::run as channel_run;
use agent_mcp::relay::{RelayConfig, RelayServer};
use agent_mcp::relay_run;
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

/// Provision `~/.cc-relay/token` using the auth-worker 1-click pair
/// flow (auth-worker #144 / #145).
///
/// Device Flow has been retired (#145). The pair flow has a much
/// better UX:
///
/// 1. POST /mcp/pair/new — anonymous, returns a `pair_url`.
/// 2. Operator opens the URL in a browser. GitHub OAuth runs once.
/// 3. Auth-worker stores the binding JWT against the pair code, and
///    pushes it down any WS attached with `Authorization: Bearer
///    <pair_code>` (used by `github-mcp-server-rs`).
///
/// cc-relay is *server-side* automation — it does not hold a WS to
/// receive the JWT push. Until auth-worker grows a "fetch the bound
/// JWT for a pair code" endpoint (issue #145 follow-up), the JWT is
/// supplied out-of-band: either via `--jwt <value>` / env var
/// `CC_RELAY_MCP_JWT`, or pasted on stdin when neither is set. The
/// JWT is then introspected to extract the bound `github_token`,
/// which is what `api.github.com` calls use.
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

    // Resolve the JWT, either from explicit args / env, or via the pair
    // flow + stdin handoff.
    let jwt = if let Some(j) = args.jwt.as_deref().filter(|s| !s.is_empty()) {
        j.to_string()
    } else {
        let claim_login = args
            .claim_login
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "either --jwt / CC_RELAY_MCP_JWT or --claim-login is required \
                     (claim-login is the GitHub username the browser session must match)"
                )
            })?;

        eprintln!(
            "rust-mcp-agent: starting pair flow against {} (client_id={}, claim_login={})",
            cfg.base_url, cfg.client_id, claim_login,
        );
        let pair = auth::pair_new(
            &http,
            &cfg,
            claim_login,
            Some(concat!("cc-relay/", env!("CARGO_PKG_VERSION"))),
        )
        .await
        .context("start pair flow")?;

        eprintln!();
        eprintln!("Open this URL in a browser and approve:");
        eprintln!("    {}", pair.pair_url);
        eprintln!();
        eprintln!(
            "(pair code expires in {}s; the browser session must be signed into GitHub as {})",
            pair.expires_in, claim_login,
        );
        eprintln!();
        eprintln!(
            "After clicking 'Paired ✓', paste the binding JWT here (received by the WS-attached \
             binary on `/u/{claim_login}/connect`):"
        );

        let mut input = String::new();
        std::io::stdin()
            .read_line(&mut input)
            .context("read JWT from stdin")?;
        let trimmed = input.trim();
        if trimmed.is_empty() {
            anyhow::bail!("no JWT was pasted on stdin; re-run `rust-mcp-agent auth`");
        }
        trimmed.to_string()
    };

    // Introspect to verify the JWT is good and to extract github_token.
    let active = introspect::introspect(&http, &cfg, secret, &jwt)
        .await
        .context("introspect provisioned JWT")?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "auth-worker reports active=false for the provided JWT; \
                 ensure the pair flow completed and the JWT was not truncated"
            )
        })?;

    let github_login = active.github_login.clone();
    let final_set = TokenSet {
        access_token: jwt,
        refresh_token: None,
        scope: active.scope,
        github_token: Some(active.github_token),
        expires_at: active.exp,
        acquired_at: token_cache::now_secs(),
    };
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
