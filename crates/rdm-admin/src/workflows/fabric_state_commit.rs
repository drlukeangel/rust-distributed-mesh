//! Fabric state-commit checklist workflow (`docs/architecture/lifecycles/fabric-state-commit.md`).
//!
//! The commit-state checklist: each node writes its scratchpad rows into durable storage,
//! and check-ins are collected before transitioning to ready-for-traffic.

pub use crate::fabric_status::{FabricStates, StateActions};
#[cfg(feature = "file-backend")]
pub use crate::fabric_status::StorePublication;
pub use crate::fabric_state::{FabricState, StateCommand, StateOp};
