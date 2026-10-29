//! Fabric recovery workflow (`docs/architecture/lifecycles/fabric-recovery.md`).
//!
//! Restores the fabric from the fabric-primary's durable topology after all node-admins
//! are lost, whether or not any node is still alive.

/// Peer recovery state on a taking fabric-primary (`docs/architecture/lifecycles/fabric-recovery.md`).
pub use crate::admin::PeerRecovery;
/// Restore in-progress peer recoveries on fabric-primary takeover (`docs/architecture/lifecycles/fabric-recovery.md`).
pub use crate::investigate::restore_peer_recovery;
/// Topology read helpers for hydration and recovery (`docs/architecture/lifecycles/node-topology-get.md`).
pub use crate::topology_read::{get_topology, TopologyRead};
