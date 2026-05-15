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

use std::sync::Arc;

use agent_broker::auth::{self, default_scopes, AuthConfig, DEFAULT_BASE_URL, DEFAULT_CLIENT_ID};
use agent_broker::{introspect, token_cache, Broker, GitHubBroker};
use agent_mcp::channel::run as channel_run;
use agent_mcp::probe::{run as probe_run, ProbeConfig};
use agent_mcp::relay::{run as relay_run, RelayConfig, RelayServer};
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

    /// ADR-003 Phase C: open an outbound WS to the auth-worker MCP relay
    /// (`wss://mcp(-staging).ippoan.org/connect`) and serve as the host-side
    /// MCP server that Claude.ai connector POSTs land on. Long-running.
    Relay(RelayArgs),

    /// ADR-005 Phase A: run as a Claude Code Channel (stdio MCP server with
    /// `experimental: { "claude/channel": {} }` capability). Spawned by
    /// Claude Code as a subprocess; reads JSON-RPC on stdin, writes on
    /// stdout. Also opens an outbound WS to the auth-worker MCP relay to
    /// receive GitHub webhook event frames and emits each one as a
    /// `notifications/claude/channel` JSON-RPC notification, which Claude
    /// Code injects into the session context as `<channel source="cc-relay" ...>`.
    Channel(ChannelArgs),

    /// Issue #50 A 案 PoC: open WSS /connect, send `hello`, append every
    /// received frame to a log file. No broker, no MCP tool surface — the
    /// goal is to characterise turn-internal long-poll behaviour from a
    /// CCoW container before deciding whether a hook can inject events
    /// without polling.
    Probe(ProbeArgs),
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
struct RelayArgs {
    /// Full WebSocket URL of the auth-worker user-less relay endpoint.
    /// Override for prod (`wss://mcp.ippoan.org/connect`) once Phase G lands.
    #[arg(
        long,
        env = "CC_RELAY_WS_URL",
        default_value = "wss://mcp-staging.ippoan.org/connect"
    )]
    ws_url: String,

    /// Path to the cached MCP access token (written by `auth` subcommand).
    /// The `access_token` field is sent verbatim as `Authorization: Bearer ...`
    /// on the WS upgrade. JWT refresh on expiry is a follow-up (token TTL is
    /// 1h; a 1h-bounded session is acceptable for the Phase C smoke test).
    #[arg(long)]
    token_path: Option<PathBuf>,

    /// GitHub broker repo in `owner/repo` form (host of the broker Issue).
    /// `cc_relay_list_agents` is the only tool wired in this Phase C cut, so
    /// the broker only needs read access to its issue body.
    #[arg(long, env = "CC_RELAY_BROKER_REPO")]
    broker_repo: String,

    /// GitHub installation token for the broker. Distinct from the MCP access
    /// JWT (which authenticates the WS upgrade). See `docs/github-app.md`.
    /// Phase D will move this behind a credential-resolver; Phase C accepts
    /// it inline so the binary can be smoke-tested without further setup.
    #[arg(long, env = "CC_RELAY_BROKER_TOKEN")]
    broker_token: String,

    /// Broker Issue number. Used by `GitHubBroker` as the canonical
    /// agents/plan/notify document.
    #[arg(long, env = "CC_RELAY_BROKER_ISSUE")]
    broker_issue: u64,

    /// Agent id this binary advertises in `Broker::join` etc. Defaults to
    /// `host-broker`. The Phase C tool surface (`cc_relay_list_agents`) does
    /// not call `join`, but later tools (`notify_agent`) will.
    #[arg(long, default_value = "host-broker")]
    agent_id: String,
}

#[derive(Debug, Parser)]
struct ChannelArgs {
    /// Full WebSocket URL of the auth-worker relay endpoint that delivers
    /// webhook events. Same default as `relay` mode — staging by default,
    /// override for prod once #46 Phase G lands.
    #[arg(
        long,
        env = "CC_RELAY_WS_URL",
        default_value = "wss://mcp-staging.ippoan.org/connect"
    )]
    ws_url: String,

    /// Path to the cached MCP access token (written by `auth` subcommand).
    #[arg(long)]
    token_path: Option<PathBuf>,

    /// GitHub broker repo (`owner/repo`). Same shape as `relay` mode.
    /// Required for the `cc_relay_list_agents` tool to function; Channel
    /// mode keeps the broker so the tool surface is identical.
    #[arg(long, env = "CC_RELAY_BROKER_REPO")]
    broker_repo: String,

    /// GitHub installation token for the broker. Phase A accepts it inline
    /// like `relay` mode does; credential-resolver work is a follow-up.
    #[arg(long, env = "CC_RELAY_BROKER_TOKEN")]
    broker_token: String,

    /// Broker Issue number. Required for `GitHubBroker::new`.
    #[arg(long, env = "CC_RELAY_BROKER_ISSUE")]
    broker_issue: u64,

    /// Agent id this binary advertises in `Broker::join` etc.
    #[arg(long, default_value = "host-broker")]
    agent_id: String,
}

#[derive(Debug, Parser)]
struct ProbeArgs {
    /// Full WebSocket URL to probe. Issue #50 background notes that the
    /// per-user form `wss://mcp-staging.ippoan.org/u/<owner>/connect` reaches
    /// the edge from a CCoW container; the user-less `…/connect` default is
    /// kept here for parity with `relay`/`channel` modes.
    #[arg(
        long,
        env = "CC_RELAY_WS_URL",
        default_value = "wss://mcp-staging.ippoan.org/connect"
    )]
    ws_url: String,

    /// Path to the cached MCP access token (written by `auth`). Mutually
    /// exclusive with `--access-token`; one of the two MUST be provided.
    #[arg(long, conflicts_with = "access_token")]
    token_path: Option<PathBuf>,

    /// Raw MCP JWT to send as `Authorization: Bearer <token>`. Reads from
    /// `CC_RELAY_PROBE_TOKEN` env if not on the command line.
    #[arg(long, env = "CC_RELAY_PROBE_TOKEN", hide_env_values = true)]
    access_token: Option<String>,

    /// Where to append received frames as JSONL.
    #[arg(long, default_value = "/tmp/cc-relay-probe.jsonl")]
    log: PathBuf,

    /// Stop after N frames (excluding ping/pong logging). Default: run
    /// until peer closes or process is killed.
    #[arg(long)]
    max_frames: Option<usize>,
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

    /// `INTERNAL_SHARED_SECRET` for the auth-worker introspect endpoint
    /// (legacy mode). Optional: when omitted, the CLI calls
    /// `/mcp/introspect` with `Authorization: Bearer <MCP_JWT>` (mode 1
    /// in `auth-worker/src/handlers/mcp-introspect.ts`). End-users do
    /// not need to set this; it remains for CI / `github-mcp-server-rs`
    /// backward-compat.
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
            Cmd::Relay(args) => run_relay(args).await,
            Cmd::Channel(args) => run_channel_cmd(args).await,
            Cmd::Probe(args) => run_probe(args).await,
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

async fn run_relay(args: RelayArgs) -> Result<()> {
    let token_path = match args.token_path {
        Some(p) => p,
        None => token_cache::default_path().context("resolve default token path")?,
    };
    let token_set = token_cache::load(&token_path)
        .with_context(|| format!("read token cache at {}", token_path.display()))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no cached token at {}; run `rust-mcp-agent auth` first",
                token_path.display()
            )
        })?;

    let (owner, repo) = args
        .broker_repo
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("--broker-repo must be 'owner/repo'"))?;
    let broker = GitHubBroker::new(
        owner.to_string(),
        repo.to_string(),
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

/// ADR-005 Phase A: dispatcher for `channel` subcommand. Mirrors `run_relay`
/// (same broker / token resolution) but hands the constructed `RelayServer`
/// to `agent_mcp::channel::run`, which serves stdio + outbound-WS instead of
/// pumping WS frames bidirectionally.
async fn run_channel_cmd(args: ChannelArgs) -> Result<()> {
    let token_path = match args.token_path {
        Some(p) => p,
        None => token_cache::default_path().context("resolve default token path")?,
    };
    let token_set = token_cache::load(&token_path)
        .with_context(|| format!("read token cache at {}", token_path.display()))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no cached token at {}; run `rust-mcp-agent auth` first",
                token_path.display()
            )
        })?;

    let (owner, repo) = args
        .broker_repo
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("--broker-repo must be 'owner/repo'"))?;
    let broker = GitHubBroker::new(
        owner.to_string(),
        repo.to_string(),
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

async fn run_probe(args: ProbeArgs) -> Result<()> {
    let access_token = match (args.access_token, args.token_path) {
        (Some(t), _) => t,
        (None, Some(p)) => {
            let token_set = token_cache::load(&p)
                .with_context(|| format!("read token cache at {}", p.display()))?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no cached token at {}; run `rust-mcp-agent auth` first",
                        p.display()
                    )
                })?;
            token_set.access_token
        }
        (None, None) => {
            anyhow::bail!(
                "probe requires either --access-token / CC_RELAY_PROBE_TOKEN or --token-path"
            );
        }
    };

    let cfg = ProbeConfig {
        ws_url: args.ws_url,
        access_token,
        log_path: args.log,
        max_frames: args.max_frames,
    };
    let count = probe_run(cfg).await.context("probe loop exited")?;
    eprintln!("rust-mcp-agent: probe wrote {count} frame(s)");
    Ok(())
}

async fn run_auth(args: AuthArgs) -> Result<()> {
    // `secret = None` → introspect via Bearer JWT (recommended for
    // end-users; no shared secret distribution). `Some(_)` keeps the
    // legacy shared-secret call path for CI / github-mcp-server-rs.
    let secret = args.introspect_secret.as_deref().filter(|s| !s.is_empty());

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
