//! Fabric recovery workflow (`docs/architecture/lifecycles/fabric-recovery.md`).
//!
//! Restores the fabric from the fabric-primary's durable topology after all node-admins
//! are lost, whether or not any node is still alive.

pub use crate::admin::PeerRecovery;
pub use crate::investigate::restore_peer_recovery;
pub use crate::topology_read::{get_topology, TopologyRead};
