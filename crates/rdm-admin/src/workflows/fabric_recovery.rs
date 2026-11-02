//! Fabric recovery workflow (`docs/architecture/lifecycles/fabric-recovery.md`).
//!
//! Restores the fabric from the fabric-primary's durable topology after all node-admins
//! are lost, whether or not any node is still alive. Each other mesh's lost mesh-primary is reached
//! through node investigation, whose verdict runs node recovery
//! (`docs/architecture/lifecycles/node-recovery.md`).

/// Topology read helpers for hydration and recovery (`docs/architecture/lifecycles/node-topology-get.md`).
pub use crate::topology_read::{get_topology, TopologyRead};
