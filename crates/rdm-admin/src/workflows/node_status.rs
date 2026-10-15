//! Node status workflow (`docs/architecture/lifecycles/node-status.md`).
//!
//! Handles node status reporting, status declarations, and status RPC queries.

pub use crate::status_rpc::{
    declared_of_member, member_of_declared, ConnectionsDeletedReceipt, Declared, MeshReport,
    Reporter, SeatFold, StatusAuthority,
};
