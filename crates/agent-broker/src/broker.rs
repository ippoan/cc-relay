//! The [`Broker`] trait — abstraction over the cross-sandbox transport.
//!
//! Every method is async and may block on network I/O. Implementations
//! own all retry / backoff / rate-limit / CAS-conflict logic; callers
//! see clean [`Result`] semantics with no transport-level mechanics
//! leaking through. Errors that callers genuinely need to dispatch on
//! (auth refresh, CAS conflict, rate limiting, missing resource) are
//! expressed as [`BrokerError`](crate::BrokerError) variants; everything
//! else collapses into [`BrokerError::Other`](crate::BrokerError::Other).
//!
//! The MVP backend is `GitHubBroker` (lands in P4b / #16). Future
//! backends (Pub/Sub, R2, DynamoDB, …) just implement this trait — no
//! MCP-tool or `agent_core` protocol changes required.

use agent_core::{NotifyMessage, PlanOp, TaskSpec};
use async_trait::async_trait;

use crate::types::{AgentMeta, Cursor, Result};

/// Cross-sandbox state and message transport for cc-relay agents.
///
/// Implementations must be `Send + Sync + 'static` so a single broker
/// instance can be shared across the MCP server's tool handlers behind
/// an `Arc<dyn Broker>`.
#[async_trait]
pub trait Broker: Send + Sync + 'static {
    /// Announce this agent in the session. Appends to the broker's
    /// agents roster (in the GitHub backend, the Issue body's `agents`
    /// array) via CAS, so concurrent joins from different agents do not
    /// clobber each other.
    async fn join(&self, agent_id: &str) -> Result<()>;

    /// Withdraw this agent from the session. Removes from the agents
    /// roster. Backends without explicit leave semantics may rely on a
    /// TTL instead; for those, implementations are free to make this a
    /// no-op.
    async fn leave(&self, agent_id: &str) -> Result<()>;

    /// Publish a [`NotifyMessage`] to the broker. Push-based from the
    /// sender's perspective: `send` returns once the message is durably
    /// visible to other agents that subsequently call
    /// [`fetch_since`](Self::fetch_since).
    async fn send(&self, msg: NotifyMessage) -> Result<()>;

    /// Pull messages addressed to *this* agent that arrived after
    /// `cursor`. Returns the matching messages in arrival order plus the
    /// advanced cursor to thread into the next call.
    ///
    /// "Addressed to this agent" means
    /// `msg.to == NotifyTarget::Agent(me)` or
    /// `msg.to == NotifyTarget::All`. Messages this agent itself sent
    /// are excluded.
    async fn fetch_since(&self, cursor: Cursor) -> Result<(Vec<NotifyMessage>, Cursor)>;

    /// List agents currently considered live. Includes self if it has
    /// joined.
    async fn list_agents(&self) -> Result<Vec<AgentMeta>>;

    /// Read the entire shared plan as a flat list of tasks.
    async fn get_plan(&self) -> Result<Vec<TaskSpec>>;

    /// Apply a single [`PlanOp`]. The broker validates the op against
    /// current plan state (e.g. `Claim` fails if the task is already
    /// held by a different live agent) and either commits it via CAS or
    /// returns [`BrokerError::Conflict`](crate::BrokerError::Conflict)
    /// after exhausting its retry budget.
    async fn plan_op(&self, op: PlanOp) -> Result<()>;

    /// The `agent_id` this broker instance speaks as. Used by the MCP
    /// server to populate
    /// [`NotifyMessage::from`](agent_core::NotifyMessage::from) when a
    /// tool call routes through [`send`](Self::send) and to filter
    /// self-sent messages out of [`fetch_since`](Self::fetch_since).
    fn self_id(&self) -> &str;
}
