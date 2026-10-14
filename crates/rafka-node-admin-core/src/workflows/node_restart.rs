//! Node restart workflow (`docs/architecture/lifecycles/node-restart.md`).
//!
//! Composed restart workflow: publishes `NodeRestarting`, runs the entire `node.stop`
//! sequence on the live process, then runs `node.start` with `StartKind::AfterStop`.

pub use crate::app_hydration::StartKind;
pub use crate::deployment::pipeline::StartStep;
pub use crate::node_self::NodeSelf;
