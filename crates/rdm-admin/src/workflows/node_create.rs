//! Node create workflow (`docs/architecture/lifecycles/node-create.md`).
//!
//! Creates a logical node, prepares its identity and data directory, launches its runtime,
//! and composes the `node.start` workflow with `StartKind::FirstStart`.

pub use crate::app_hydration::StartKind;
pub use crate::deployment::pipeline::{CommandAdmission, CreateStep, READY_PREREQUISITES};
pub use crate::workflows::node_start::{node_start, FirstStart, JoinThrough, JoinTo, Joining, StartNode, Starting};
