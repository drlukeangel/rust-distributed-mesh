//! Fabric state-sync checklist workflow (`docs/architecture/lifecycles/fabric-state-sync.md`).
//!
//! The state-sync checklist: executes the application's `sync_state` hook across the fabric
//! before committing state.

pub use crate::fabric_status::{FabricStates, StateActions};
pub use crate::fabric_state::{FabricState, StateCommand, StateOp};
pub use crate::wiring::{FabricHooks, StateSyncAnswer, StateSyncFailed, StateSyncHandoff, SyncState};
