//! Node topology get primitive (`docs/architecture/lifecycles/node-topology-get.md`).
//!
//! An admitted member requests topology from its mesh-primary on op `0x1E` (`GetTopology`),
//! installing complete snapshots and refreshing rafka-time.

pub use crate::topology_read::{
    get_topology, ReadMesh, ServedTime, StoredMap, StoredMesh, TimeNotAdopted, TopologyDoor,
    TopologyFailure, TopologyRead, TopologySlot,
};
