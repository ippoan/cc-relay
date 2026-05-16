//! `rust-mcp-agent` — clap dispatcher.
//!
//! Thin shim around [`agent_cli::run`]. All real logic — including
//! TOML config loading, subcommand parsing, tracing init, runtime
//! build, and dispatch — lives in `lib.rs` / `runners.rs` so unit
//! tests can reach it. See the `tests` module in `lib.rs`.

fn main() -> std::process::ExitCode {
    agent_cli::run()
}
