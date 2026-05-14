//! Wire protocol value types.
//!
//! Until ADR-001 (see `ARCHITECTURE.md`) this file also defined a
//! [`WireMessage`] discriminated union and used `ts-rs` to export a
//! TypeScript copy of every type into `coordinator/src/generated/`.
//! Both are gone now: there is no second-language consumer (the
//! Cloudflare Worker has been deleted) and broker payloads are flat
//! structs rather than a multiplexed WS envelope.
//!
//! What lives here is the small set of value types shared between the
//! MCP server and (in P4 / #16) the `Broker` trait implementations:
//!
//! - [`Priority`], [`NotifyTarget`], [`NotifyMessage`] — agent-to-agent
//!   notify payloads.
//! - [`TaskSpec`], [`TaskStatus`], [`PlanOp`] — shared-plan state and
//!   mutations.

use serde::{Deserialize, Serialize};

/// Target of a `notify_agent` call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum NotifyTarget {
    /// Direct message to a specific `agent_id`.
    Agent(String),
    /// Broadcast to every other agent in the session.
    All,
}

/// Priority hint for `notify_agent`. The inbox layer uses this only to
/// decide ordering when multiple notifies are flushed in the same batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low,
    #[default]
    Normal,
    High,
}

/// One agent-to-agent notification carried by the broker.
///
/// This is the unit a `Broker::publish` accepts and a `Broker::poll`
/// returns. Concrete broker impls (e.g. `GitHubBroker`, P4 / #16) decide
/// how to serialize it — JSON in an issue comment, a Pub/Sub message
/// attribute, an R2 object, etc. — but the in-memory shape stays the
/// same so the MCP server can be broker-agnostic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotifyMessage {
    /// `agent_id` of the sender.
    pub from: String,
    /// Recipient. `NotifyTarget::All` broadcasts to every other agent.
    pub to: NotifyTarget,
    /// Free-form message body, shown verbatim in the recipient's next
    /// Claude prompt by the inbox hook.
    pub message: String,
    /// Priority hint; controls only flush ordering, not delivery.
    #[serde(default)]
    pub priority: Priority,
    /// Millis since epoch, stamped by the *sender* at publish time.
    /// Brokers that have a server-side clock may overwrite this on the
    /// receiving end; consumers must not rely on monotonicity.
    pub timestamp: i64,
}

/// A single task on the shared plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub id: String,
    pub title: String,
    pub status: TaskStatus,
    /// `agent_id` that currently owns the task, or `None` if unclaimed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    /// Free-form notes. The broker does not parse this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

/// Lifecycle of a [`TaskSpec`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    InProgress,
    Done,
    Cancelled,
}

/// A single mutation of the shared plan. The broker (P4 / #16) is
/// responsible for validating each op against current plan state and
/// either applying it or rejecting it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PlanOp {
    /// Add a new task. `id` must be unique within the session.
    Add { task: TaskSpec },
    /// Take ownership of a task. Fails if already assigned to someone else
    /// and not yet `Done`/`Cancelled`.
    Claim { task_id: String, agent_id: String },
    /// Update status. Optional `notes` overwrites the existing notes.
    Update {
        task_id: String,
        status: TaskStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        notes: Option<String>,
    },
    /// Drop a task entirely.
    Remove { task_id: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `NotifyMessage` carries an internally-tagged `to` so consumers get
    /// a clean `{ kind: "agent", value: "alice" } | { kind: "all" }` union
    /// regardless of the broker backend.
    #[test]
    fn notify_target_shape() {
        let to = NotifyTarget::Agent("alice".into());
        let s = serde_json::to_string(&to).unwrap();
        assert_eq!(s, r#"{"kind":"agent","value":"alice"}"#);

        let to = NotifyTarget::All;
        let s = serde_json::to_string(&to).unwrap();
        assert_eq!(s, r#"{"kind":"all"}"#);
    }

    /// Round-trip a `NotifyMessage` through JSON to lock the shape brokers
    /// commit to on disk / on the wire.
    #[test]
    fn notify_message_roundtrip() {
        let m = NotifyMessage {
            from: "alice".into(),
            to: NotifyTarget::Agent("bob".into()),
            message: "ping".into(),
            priority: Priority::High,
            timestamp: 1_700_000_000_000,
        };
        let s = serde_json::to_string(&m).unwrap();
        let back: NotifyMessage = serde_json::from_str(&s).unwrap();
        assert_eq!(back, m);
    }

    /// `PlanOp` is internally tagged on `op`; the discriminator must stay
    /// snake_case so brokers can pattern-match on raw JSON.
    #[test]
    fn plan_op_tag() {
        let op = PlanOp::Claim {
            task_id: "T-1".into(),
            agent_id: "alice".into(),
        };
        let s = serde_json::to_string(&op).unwrap();
        assert!(s.contains(r#""op":"claim""#));
    }
}
