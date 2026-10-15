//! Fabric state-sync round workflow (`docs/architecture/lifecycles/fabric-state-sync.md`).
//!
//! The state-sync round: executes the application's `sync_state` hook across the fabric
//! before committing state.

pub use crate::fabric_rounds::{FabricRounds, RoundActions};
pub use crate::fabric_state::{FabricState, RoundKind, RoundOp};
pub use crate::wiring::{FabricHooks, StateSyncRound, SyncState};
