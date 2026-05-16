//! cc-relay broker abstraction.
//!
//! Defines the [`Broker`] trait used by `agent-mcp` for every piece of
//! cross-sandbox state — the live agents list, the shared task plan, and
//! the agent-to-agent notification stream. Concrete impls (`GitHubBroker`
//! in P4b / #16; Pub/Sub, R2, DynamoDB later) translate the trait calls
//! into backend-specific I/O while keeping the in-memory shapes in
//! [`agent_core`] backend-agnostic.
//!
//! See `ARCHITECTURE.md` ADR-001 for why GitHub Issues are the MVP
//! transport and what the trade-offs are versus the original
//! WebSocket-coordinator design.
//!
//! Status:
//!
//! - P4a (#16) — trait + shared value types — landed.
//! - P4b (#16, this slice) — `GitHubBroker` impl + mock-server tests.
//! - P4c (#16) — cursor persistence + rate-limit/5xx polish.

pub mod auth;
pub mod broker;
pub mod cursor;
pub mod github;
pub mod introspect;
pub mod token_cache;
pub mod token_manager;
pub mod types;

pub use auth::{AuthConfig, DeviceAuthorizationResponse};
pub use broker::Broker;
pub use cursor::CursorStore;
pub use github::GitHubBroker;
pub use introspect::IntrospectionActive;
pub use token_cache::TokenSet;
pub use token_manager::TokenManager;
pub use types::{AgentMeta, BrokerError, Cursor, Result};

#[cfg(test)]
pub(crate) mod test_utils {
    /// Install a tracing subscriber for the current process, exactly
    /// once. Called from any test that exercises a `tracing::warn!` /
    /// `info!` / `debug!` line whose lazy formatter args we want
    /// covered (otherwise llvm-cov reports the field-expression line
    /// as zero-count even though the surrounding macro line ran).
    pub(crate) fn init_tracing() {
        use std::sync::OnceLock;
        static INIT: OnceLock<()> = OnceLock::new();
        INIT.get_or_init(|| {
            let _ = tracing_subscriber::fmt()
                .with_test_writer()
                .with_max_level(tracing::Level::TRACE)
                .try_init();
        });
    }
}
