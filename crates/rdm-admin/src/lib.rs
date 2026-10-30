//! Generic node-admin core for the Mesh product (i143 PRD §4, §7).
//!
//! Owns the generic Fabric → Mesh → Node model and its topology invariants,
//! Build (intents, planner, state adapters, the fabric projection, the
//! executor), the control routes, deployment providers and pipelines, and
//! lifecycle transitions. It is the control plane (`docs/architecture/node-admin-lifecycle.md` §1):
//! the Admin UI and every other front end are clients of its control API
//! (`rdm-client`). Nothing here knows a Rafka role.
//!
//! # Where it sits
//!
//! On `rdm-node`, `rdm-transport` and `rdm-core`: node-admin is itself a node of its mesh. An
//! application's node-admin executable is a `main` that calls [`entry::run_with`] (or
//! [`entry::run_on`] with its own topology storage backend), handing the admin its
//! [`wiring::Wiring`]: its fabric hooks, its own node kinds, ops and declared secrets, and its cert
//! material. Everything else is configured from the environment.
//!
//! # Example
//!
//! ```no_run
//! use rdm_admin::wiring::{FabricHooks, Wiring};
//!
//! const SERVICE: &str = "my-node-admin";
//!
//! #[tokio::main]
//! async fn main() {
//!     rdm_admin::entry::run_with(SERVICE, |cfg| Wiring {
//!         fabric_hooks: FabricHooks::no_app_work(),
//!         certs: rdm_admin::entry::material_or_exit(SERVICE, cfg),
//!         ..Wiring::default()
//!     })
//!     .await
//! }
//! ```
//!
//! # Guide
//!
//! [Node-admin lifecycle](../../guide/architecture/node-admin-lifecycle.html),
//! [Build](../../guide/architecture/build.html),
//! [Deployment provider](../../guide/architecture/deployment-provider.html) and
//! [Fabric and node hooks](../../guide/architecture/fabric-node-hooks.html), in the docs site
//! `scripts/docs-site.sh` builds.
#![deny(missing_docs)]


pub mod entry;
pub mod join;
pub mod heard_again;
pub mod admin;
/// Node readiness hooks (`docs/architecture/fabric-node-hooks.md`).
pub use rdm_node::node_hooks;
pub mod accepted;
pub mod build;
pub mod certs;
pub mod cert_renewal;
pub mod build_claim;
pub mod build_drive;
pub mod build_op;
pub mod build_run;
pub mod build_facts_read;
pub mod read_op;
pub mod fabric_handover;
pub mod fabric_recovery;
pub mod fetch_build;
#[cfg(feature = "file-backend")]
pub mod build_state;
pub mod deployment;
pub mod drift;
pub mod election;
pub mod executor;
pub mod path_investigation;
pub mod atlas;
pub(crate) mod refused_caller;
pub mod fabric_builds;
pub mod fabric_status;
pub mod fabric_state;
pub mod fabric_storage;
pub mod control_plane;
/// The control plane module (`control_plane`).
pub use control_plane as control;
pub mod investigate;
pub mod lifecycle;
pub mod mesh_claims;
pub mod mesh_delete;
/// Fabric, Mesh, and Node domain model types (`docs/architecture/node-identity-and-state.md` §2).
pub use rdm_core::model;
pub mod node_commands;
pub mod node_investigation;
pub mod node_config;
pub mod node_properties;
pub mod node_name_update;
/// Node `path.name` updates (`docs/architecture/lifecycles/node-name-update.md`).
pub use rdm_node::node_name;
pub mod node_rows;
/// Process-wide Node RPC client and live resolver (`docs/architecture/node-rpc.md`).
pub use rdm_node::node_rpc;
/// Self-handled status commands on a running node (`docs/architecture/lifecycles/node-stop.md`, `node-start.md`).
pub use rdm_node::node_self;
pub mod stop_gate;
pub mod offline;
pub mod rafka_time;
pub mod readiness;
pub(crate) mod reenter;
pub mod checklist;
pub mod primary_watch;
/// Blocking pool offload helpers (`rdm_node::blocking`).
pub use rdm_node::blocking;
/// Atomic durable file writes (`rdm_node::durable_file`).
pub use rdm_node::durable_file;
pub mod record_cache;
#[cfg(feature = "file-backend")]
pub mod record_store;
pub mod stopped_gc;
pub(crate) mod status_declare;
pub mod status_rpc;
#[cfg(test)]
pub(crate) mod span_capture;
/// Outbound data-plane connection reconnect helper (`docs/architecture/connections.md` §2.3).
pub use rdm_node::connection_reconnect;
/// Local `connections.storage` writer (`docs/architecture/connections.md` §3.1).
pub use rdm_node::connections_writer;
pub mod storage;
pub mod storage_backend;
pub mod storage_change;
#[cfg(test)]
pub(crate) mod test_support;
/// Storage boundary error types (`docs/architecture/storage.md`).
pub use rdm_node::storage_error;
/// Live topology storage provider move step (`docs/architecture/storage.md` §2).
pub use rdm_node::storage_move;
/// Atomic storage provider handle (`docs/architecture/storage.md` §2).
pub use rdm_node::storage_handle;
#[cfg(feature = "file-backend")]
/// Shared store root JSONL log (`docs/architecture/storage.md` §2).
pub use rdm_node::store_log;
#[cfg(feature = "file-backend")]
/// Shared store root directory (`docs/architecture/storage.md` §2).
pub use rdm_node::store_root;
pub mod topology;
/// Hydration and reconnect topology read (`docs/architecture/lifecycles/node-topology-get.md`).
pub use rdm_node::topology_read;
pub mod primitives;
pub mod views;
pub mod wire;
pub mod wiring;
pub mod workflows;
