//! Fabric state-commit round workflow (`docs/architecture/lifecycles/fabric-state-commit.md`).
//!
//! The commit-state round: each node writes its scratchpad rows into durable storage,
//! and check-ins are collected before transitioning to ready-for-traffic.

pub use crate::fabric_rounds::{FabricRounds, RoundActions};
#[cfg(feature = "file-backend")]
pub use crate::fabric_rounds::StorePublication;
pub use crate::fabric_state::{FabricState, RoundKind, RoundOp};
