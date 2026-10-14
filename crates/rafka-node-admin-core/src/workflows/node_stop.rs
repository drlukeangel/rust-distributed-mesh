//! Node stop workflow (`docs/architecture/lifecycles/node-stop.md`).
//!
//! Stops a node: executes `node.connections.delete { scope: All }`, cuts transport,
//! and transitions the live process to stopped.

pub use crate::node_self::{CutReport, NodeSelf, StartFailed, TasksParked};
pub use crate::stop_gate::{GateState, StopGate, Turn};
