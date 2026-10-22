//! Node primary update workflow (`docs/architecture/lifecycles/node-primary-update.md`).
//!
//! Moves the primary (mesh-primary or fabric-primary) to an eligible successor: the primary finishes
//! the writes it has issued, then calls its successor, which installs itself.

pub use crate::election::{elect, Candidate, ElectionLog, Incumbency, Moving};
pub use crate::fabric_handover::{
    update_operation, FinishWrites, HandoverDoor, ProjectView,
    UpdateLocks, UpdateMemo,
};
