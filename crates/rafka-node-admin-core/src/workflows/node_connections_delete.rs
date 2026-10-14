//! Node connections delete primitive (`docs/architecture/lifecycles/node-connections-delete.md`).
//!
//! Gracefully cuts connections of a node: all connections (`scope: All`) or a specific
//! list of connection IDs (`scope: Connections(ids)`).

pub use crate::connections_writer::{ConnectionsWriter, HeldConnections, RecoveryAttempt, RowsHeld};
pub use crate::node_self::{Deleted, CONNECTIONS_DELETE};
