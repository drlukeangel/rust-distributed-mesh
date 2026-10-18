//! Node primary update workflow (`docs/architecture/lifecycles/node-primary-update.md`).
//!
//! Transfers the primary seat (mesh-primary or fabric-primary) to an eligible successor,
//! cutting off writes from the former holder and installing the new seat holder.

pub use crate::election::{elect, Candidate, ElectionLog, Incumbency, Moving};
pub use crate::fabric_handover::{
    update_operation, CutOffWrites, HandoverDoor, ProjectView, SourceFailure,
    UpdateLocks, UpdateMemo, WriteCutOff,
};
#[cfg(feature = "file-backend")]
pub use crate::fabric_handover::SeatStores;
