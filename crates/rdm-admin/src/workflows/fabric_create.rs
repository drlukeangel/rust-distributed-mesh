//! Fabric create workflow (`docs/architecture/lifecycles/fabric-create.md`).
//!
//! Brings a fabric into being with its first mesh. The fabric id is minted with
//! status pending, each mesh's first node-admin is created, the first mesh's primary
//! becomes the fabric-primary, and the fabric changes to ready-for-traffic.

pub use crate::fabric_status::FabricStates;
pub use crate::fabric_state::FabricState;
pub use crate::fabric_storage::{FabricClaim, FabricIdentity, FabricStorage, MemoryFabricStorage};
