//! Wire protocol value types shared between `agent-mcp` and (in P4 / #16)
//! the `agent-broker` crate. Rust is the only definition; see
//! `ARCHITECTURE.md` ADR-001 for why ts-rs / TypeScript export was
//! removed.

pub mod protocol;

pub use protocol::{NotifyMessage, NotifyTarget, PlanOp, Priority, TaskSpec, TaskStatus};
