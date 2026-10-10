//! The generic RPC proof node and its launch contract. An `rpc_node` binds exactly the endpoint node-admin assigned,
//! serves Node RPC, joins the fabric's membership topic and
//! publishes its digest. It carries no product semantics.
#![deny(missing_docs)]


pub use rafka_mesh_entity::launch;
pub mod node;
pub mod node_rpc;
pub mod declare_probe;
pub mod proof_store;
pub mod resolve_probe;
pub mod faults;
pub mod originate;
pub mod admin_faults;

/// The launcher half of a functional fabric, shared by test crates.
#[cfg(feature = "rig")]
pub mod rig;
