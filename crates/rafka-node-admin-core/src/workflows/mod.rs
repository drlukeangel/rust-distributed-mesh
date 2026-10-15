//! The lifecycle workflows: each op is one workflow, a sequence of calls to primitives in its
//! lifecycle page's order, which every node kind runs and plugs into.

pub mod fabric_create;
pub mod fabric_delete;
pub mod fabric_ready_for_traffic;
pub mod fabric_recovery;
pub mod fabric_state_commit;
pub mod fabric_state_sync;
pub mod mesh_create;
pub mod mesh_delete;
pub mod mesh_investigation;
pub mod node_config_update;
pub mod node_connections_delete;
pub mod node_create;
pub mod node_delete;
pub mod node_mesh_join;
pub mod node_mesh_leave;
pub mod node_name_update;
pub mod node_primary_update;
pub mod node_recovery;
pub mod node_restart;
pub mod node_runtime_inspect;
pub use rdm_node::workflows::node_start;
pub mod node_status;
pub mod node_stop;
pub mod node_tickle;
pub mod node_topology_get;
