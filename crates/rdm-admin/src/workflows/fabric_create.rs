//! Fabric create workflow (`docs/architecture/lifecycles/fabric-create.md`).
//!
//! Brings a fabric into being with its first mesh. The fabric id is minted with
//! status pending, each mesh's first node-admin is created, the first mesh's primary
//! becomes the fabric-primary, and the fabric climbs to ready-for-traffic.

pub use crate::fabric_rounds::{FabricRounds, NoScratchpad, RoundActions};
#[cfg(feature = "file-backend")]
pub use crate::fabric_rounds::StorePublication;
pub use crate::fabric_state::{FabricState, FirstReady};
pub use crate::fabric_storage::{FabricClaim, FabricIdentity, FabricStorage, MemoryFabricStorage};
#[cfg(feature = "file-backend")]
pub use crate::fabric_storage::FileFabricStorage;
