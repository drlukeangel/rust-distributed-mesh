//! Node config update workflow (`docs/architecture/lifecycles/node-config-update.md`).
//!
//! Updates configuration for a node without restarting it, writing updated rows and
//! notifying the target node.

pub use crate::node_config::{NodeConfigs, NodeConfigsSlot};
