//! The lifecycle primitives called by the workflow modules (`docs/plans/proposal/rdm-code-conformance.md` §0.1).
//!
//! Each primitive does one thing: join, the `pre_node_ready` hook, the Build floor,
//! rafka-time, `node.connection.delete`, the transport cut, the exited verdict, row claim/put/tombstone,
//! topology reads, and status reports/applies.

pub use crate::deployment::provider::{inspect_runtime, Inspected};
pub use crate::node_hooks;
pub use crate::connections_writer;
pub use crate::fetch_build;
pub use crate::join;
pub use crate::node_rows;
pub use crate::node_investigation;
pub use crate::node_self;
pub use crate::offline;
pub use crate::rafka_time;
pub use crate::path_investigation;
pub use crate::primary_watch;
pub use crate::status_rpc;
pub use crate::stop_gate;
pub use crate::topology_read;
