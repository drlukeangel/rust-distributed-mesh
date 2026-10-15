//! Node mesh leave primitive (`docs/architecture/lifecycles/node-mesh-leave.md`).
//!
//! Takes a node off its mesh (and backbone for node-admins) after its connections
//! are deleted.

pub use crate::node_self::CutReport;
pub use crate::stop_gate::StopGate;
