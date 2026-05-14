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

pub mod broker;
pub mod cursor;
pub mod github;
pub mod types;

pub use broker::Broker;
pub use cursor::CursorStore;
pub use github::GitHubBroker;
pub use types::{AgentMeta, BrokerError, Cursor, Result};
