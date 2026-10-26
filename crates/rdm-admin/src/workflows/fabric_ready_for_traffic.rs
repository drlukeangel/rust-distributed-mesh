//! Fabric ready-for-traffic workflow (`docs/architecture/lifecycles/fabric-ready-for-traffic.md`).
//!
//! Once every mesh the accepted Build names is ready-for-traffic the fabric-primary sets the fabric
//! ready-for-traffic and calls the application's `on_fabric_ready` notice (Q174, Q175).

pub use crate::fabric_status::{ChangeInputs, FabricStates};
pub use crate::wiring::{FabricHooks, FabricReadyCtx, FabricReadyNotice};
pub use crate::fabric_state::FabricState;
