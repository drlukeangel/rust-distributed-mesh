//! The generic RPC proof node and its launch contract (`docs/i143/design.md`
//! §1–§3). An `rpc_node` binds exactly the endpoints node-admin assigned,
//! serves Node RPC on every slot, joins the fabric's membership topic and
//! publishes its digest. It carries no product semantics.

pub use rafka_mesh_entity::launch;
pub mod node;
