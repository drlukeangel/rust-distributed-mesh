//! Fabric ready-for-traffic round workflow (`docs/architecture/lifecycles/fabric-ready-for-traffic.md`).
//!
//! The final status round of fabric initialization or recovery: every planned member has
//! checked in, the application is notified via the `traffic_opened` hook, and client traffic is opened.

pub use crate::fabric_rounds::{ClimbInputs, FabricRounds, NoScratchpad, RoundActions, StorePublication};
pub use crate::fabric_state::{FabricState, FirstReady, RoundKind, RoundOp};
