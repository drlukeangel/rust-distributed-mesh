// Phase 2: topology-node cache now lives in `rafka-node-base` so every node
// type gets it automatically. admin-ui re-exports the shared types and the
// process-global accessor; it no longer maintains its own duplicate cache or
// fill loop.
//
// The fill loop is spawned by NodeRuntime::run() → run_node(), which admin-ui
// already calls (Role::Observer). No separate supervise call needed here.

pub use rafka_node_base::{TopologyNode, topology_nodes};
