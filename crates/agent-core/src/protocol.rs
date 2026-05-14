//! WireProtocol — Rust source of truth.
//!
//! Each public type derives `serde::{Serialize, Deserialize}` (JSON on the
//! wire) and `ts_rs::TS` (TypeScript export for the coordinator). The
//! `export_bindings_*` tests at the bottom of this file are what actually
//! write `coordinator/src/generated/*.ts`; CI fails if that tree differs
//! from what is committed.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Current wire protocol version. Bump on any breaking change to a
/// [`WireMessage`] variant. Daemons that send a mismatching version are
/// closed by the coordinator with [`CloseCode::PROTOCOL_VERSION_MISMATCH`].
pub const PROTOCOL_VERSION: u32 = 1;

/// WebSocket close codes used by cc-relay.
///
/// The codes are RFC 6455 private-use range (4000-4999). The coordinator
/// (`SessionDO`) emits them; daemons interpret them to decide whether to
/// reconnect (most codes) or exit (4002 "replaced").
pub struct CloseCode;

impl CloseCode {
    /// Hello carried a `protocol_version` the coordinator does not speak.
    /// The daemon should *not* retry — the binary needs an upgrade.
    pub const PROTOCOL_VERSION_MISMATCH: u16 = 4001;

    /// Another daemon connected with the same `agent_id` and took over.
    /// The daemon should exit cleanly; the new daemon owns the session.
    pub const REPLACED: u16 = 4002;

    /// `auth_token` missing or wrong (P6 / #11).
    pub const UNAUTHORIZED: u16 = 4003;
}

/// Top-level message exchanged over the WebSocket.
///
/// `#[serde(tag = "type", rename_all = "snake_case")]` produces a
/// discriminated union that survives the Rust ↔ TypeScript boundary cleanly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export, export_to = "../../../coordinator/src/generated/")]
pub enum WireMessage {
    /// First message every daemon sends after connecting.
    Hello(Hello),
    /// Coordinator's reply to a successful `Hello`.
    HelloAck {
        /// Echoed back from `Hello.agent_id` for sanity-checking.
        agent_id: String,
        /// Server-side wall clock in millis since epoch.
        #[ts(type = "number")]
        server_time: i64,
        /// Same as [`PROTOCOL_VERSION`] on the coordinator side.
        protocol_version: u32,
    },
    /// A new agent has joined the session. *Not* sent on a grace-window
    /// reconnect (see #7).
    AgentJoined { agent_id: String },
    /// An agent has left (clean close or grace expired).
    AgentLeft { agent_id: String },
    /// A file changed in some agent's worktree.
    FileEvent {
        agent_id: String,
        path: String,
        kind: FileEventKind,
        #[ts(type = "number")]
        timestamp: i64,
    },
    /// Targeted notification from one agent to one or all others.
    NotifyAgent {
        from: String,
        to: NotifyTarget,
        message: String,
        priority: Priority,
        #[ts(type = "number")]
        timestamp: i64,
    },
    /// Mutation of the shared plan.
    PlanOp { op: PlanOp },
    /// Full snapshot of the plan, sent on demand (`get_plan`) and after
    /// significant mutations.
    PlanSnapshot { tasks: Vec<TaskSpec> },
    /// Coordinator-side error that does not warrant closing the socket.
    Error { code: u16, message: String },
}

/// Body of [`WireMessage::Hello`]. Split into its own struct so daemons
/// can construct it once and pass it around.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../../coordinator/src/generated/")]
pub struct Hello {
    /// Must equal [`PROTOCOL_VERSION`]. Mismatch ⇒ close 4001.
    pub protocol_version: u32,
    /// Caller-chosen identifier, scoped to a session. Collisions are
    /// resolved by the coordinator with the "after wins" rule (#4, #7).
    pub agent_id: String,
    /// Optional human-readable repo label (e.g. `ippoan/cc-relay`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    /// Shared-secret authentication. Wired in P6 (#11); ignored for now
    /// if the coordinator has no token configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
}

impl Hello {
    /// Convenience constructor for the common daemon case.
    pub fn new(agent_id: impl Into<String>) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            agent_id: agent_id.into(),
            repo: None,
            auth_token: None,
        }
    }
}

/// Kind of a filesystem change observed by `notify-rs` after debouncing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export, export_to = "../../../coordinator/src/generated/")]
pub enum FileEventKind {
    Created,
    Modified,
    Removed,
    Renamed,
}

/// Target of a `notify_agent` call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
#[ts(export, export_to = "../../../coordinator/src/generated/")]
pub enum NotifyTarget {
    /// Direct message to a specific `agent_id`.
    Agent(String),
    /// Broadcast to every other agent in the session.
    All,
}

/// Priority hint for `notify_agent`. The inbox layer uses this only to
/// decide ordering when multiple notifies are flushed in the same batch.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize, TS,
)]
#[serde(rename_all = "snake_case")]
#[ts(export, export_to = "../../../coordinator/src/generated/")]
pub enum Priority {
    Low,
    #[default]
    Normal,
    High,
}

/// A single task on the shared plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../../coordinator/src/generated/")]
pub struct TaskSpec {
    pub id: String,
    pub title: String,
    pub status: TaskStatus,
    /// `agent_id` that currently owns the task, or `None` if unclaimed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    /// Free-form notes. The coordinator does not parse this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

/// Lifecycle of a [`TaskSpec`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export, export_to = "../../../coordinator/src/generated/")]
pub enum TaskStatus {
    Pending,
    InProgress,
    Done,
    Cancelled,
}

/// A single mutation of the shared plan. The coordinator validates each
/// op against the current plan state and either applies it or replies
/// with a [`WireMessage::Error`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "op", rename_all = "snake_case")]
#[ts(export, export_to = "../../../coordinator/src/generated/")]
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
    /// Drop a task entirely. The coordinator broadcasts the resulting
    /// [`WireMessage::PlanSnapshot`].
    Remove { task_id: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sanity-check that a Hello round-trips through JSON, including
    /// optional fields and the `type` discriminator.
    #[test]
    fn hello_roundtrip() {
        let m = WireMessage::Hello(Hello::new("alice"));
        let s = serde_json::to_string(&m).unwrap();
        assert!(s.contains(r#""type":"hello""#));
        assert!(s.contains(r#""protocol_version":1"#));
        let back: WireMessage = serde_json::from_str(&s).unwrap();
        assert_eq!(back, m);
    }

    /// `NotifyAgent` carries an internally-tagged `to` so TS gets a clean
    /// `{ kind: "agent", value: "alice" } | { kind: "all" }` union.
    #[test]
    fn notify_target_shape() {
        let to = NotifyTarget::Agent("alice".into());
        let s = serde_json::to_string(&to).unwrap();
        assert_eq!(s, r#"{"kind":"agent","value":"alice"}"#);

        let to = NotifyTarget::All;
        let s = serde_json::to_string(&to).unwrap();
        assert_eq!(s, r#"{"kind":"all"}"#);
    }

    /// `PlanOp` is `#[serde(tag = "op")]`, so the `type` field of the
    /// outer `WireMessage` envelope must coexist with the inner `op` tag.
    #[test]
    fn plan_op_envelope() {
        let m = WireMessage::PlanOp {
            op: PlanOp::Claim {
                task_id: "T-1".into(),
                agent_id: "alice".into(),
            },
        };
        let s = serde_json::to_string(&m).unwrap();
        // Both discriminators must show up at the right nesting level.
        assert!(s.contains(r#""type":"plan_op""#));
        assert!(s.contains(r#""op":"claim""#));
    }

    /// Daemons check this exact value; the test fails loudly if anyone
    /// edits the constant without thinking about the protocol bump.
    #[test]
    fn protocol_version_is_one() {
        assert_eq!(PROTOCOL_VERSION, 1);
    }

    /// Close codes live in the RFC 6455 private-use range and must not
    /// drift silently. If you add a new one, add a line here.
    #[test]
    fn close_codes_are_stable() {
        assert_eq!(CloseCode::PROTOCOL_VERSION_MISMATCH, 4001);
        assert_eq!(CloseCode::REPLACED, 4002);
        assert_eq!(CloseCode::UNAUTHORIZED, 4003);
    }
}
