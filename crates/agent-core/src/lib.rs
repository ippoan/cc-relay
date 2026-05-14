//! Wire protocol types shared between the Rust daemon and the TypeScript
//! coordinator. Rust is the source of truth; `ts-rs` exports the
//! TypeScript definitions into `coordinator/src/generated/` as a side
//! effect of `cargo test`.

pub mod protocol;

#[cfg(feature = "io")]
pub mod inbox;

pub use protocol::{
    CloseCode, FileEventKind, Hello, NotifyTarget, PlanOp, Priority, TaskSpec, TaskStatus,
    WireMessage, PROTOCOL_VERSION,
};
