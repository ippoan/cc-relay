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
//! This crate is currently P4a scaffolding: only the trait surface and
//! shared value types ship. The `GitHubBroker` implementation, cursor
//! persistence, and mock-server tests follow in P4b / P4c.

pub mod broker;
pub mod types;

pub use broker::Broker;
pub use types::{AgentMeta, BrokerError, Cursor, Result};
