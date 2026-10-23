//! Generic node-admin core for the Mesh product (i143 PRD §4, §7).
//!
//! Owns the generic Fabric → Mesh → Node model and its topology invariants,
//! Build (intents, planner, state adapters, the fabric projection, the
//! executor), the control routes, deployment providers and pipelines, and
//! lifecycle transitions. It is the only lifecycle authority: the Admin UI
//! and every other front end are clients of its control API
//! (`rafka-node-admin-client`). Nothing here knows a Rafka role.
#![deny(missing_docs)]


pub mod entry;
pub mod join;
pub mod heard_again;
pub mod admin;
pub use rdm_node::before_ready;
pub mod accepted;
pub mod build;
pub mod certs;
pub mod cert_renewal;
pub mod build_claim;
pub mod build_drive;
pub mod build_op;
pub mod build_run;
pub mod build_facts_read;
pub mod fabric_handover;
pub mod fabric_recovery;
pub mod fetch_build;
#[cfg(feature = "file-backend")]
pub mod build_state;
pub mod deployment;
pub mod drift;
pub mod election;
pub mod executor;
pub mod seat_revision;
pub mod fabric_builds;
pub mod fabric_rounds;
pub mod fabric_state;
pub mod fabric_storage;
pub mod control_plane;
/// The control plane module (`control_plane`).
pub use control_plane as control;
pub mod investigate;
pub mod lifecycle;
pub mod mesh_claims;
pub mod mesh_delete;
pub use rdm_core::model;
pub mod node_commands;
pub mod node_investigation;
pub mod node_config;
pub mod node_properties;
pub mod node_name_update;
pub use rdm_node::node_name;
pub mod node_rows;
pub use rdm_node::node_rpc;
pub use rdm_node::node_self;
pub mod stop_gate;
pub mod offline;
pub mod rafka_time;
pub mod readiness;
pub(crate) mod reenter;
pub mod round;
pub mod seat_watch;
pub use rdm_node::blocking;
pub use rdm_node::durable_file;
#[cfg(feature = "file-backend")]
pub mod record_store;
pub mod stopped_gc;
pub(crate) mod status_declare;
pub mod status_rpc;
#[cfg(test)]
pub(crate) mod span_capture;
pub use rdm_node::connection_reconnect;
pub use rdm_node::connections_writer;
pub mod storage;
pub mod storage_backend;
#[cfg(test)]
pub(crate) mod test_support;
pub use rdm_node::storage_error;
#[cfg(feature = "file-backend")]
pub use rdm_node::store_root;
pub mod status_storage;
pub mod topology;
pub use rdm_node::topology_read;
pub mod primitives;
pub mod views;
pub mod wire;
pub mod wiring;
pub mod workflows;
