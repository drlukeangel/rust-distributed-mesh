//! The lifecycle primitives called by the workflow modules (`docs/plans/proposal/rdm-code-conformance.md` §0.1).
//!
//! Each primitive does one thing: join, the `before_ready` hook, the Build floor,
//! rafka-time, `node.connections.delete`, the transport cut, the exit proof, row claim/put/tombstone,
//! topology reads, and status reports/applies.

pub use crate::admin::{exit_proof_of, nodes_exit_proof};
pub use crate::before_ready;
pub use crate::connections_writer;
pub use crate::fetch_build;
pub use crate::join;
pub use crate::node_rows;
pub use crate::node_self;
pub use crate::offline;
pub use crate::rafka_time;
pub use crate::seat_revision;
pub use crate::seat_watch;
pub use crate::status_rpc;
pub use crate::stop_gate;
pub use crate::topology_read;
