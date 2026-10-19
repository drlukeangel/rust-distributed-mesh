//! Node name update workflow (`docs/architecture/lifecycles/node-name-update.md`).
//!
//! Renames a node within its mesh path, verifying rename preconditions and updating
//! durable node rows and indices.

pub use crate::node_name_update::{update, FirstReadyOnRoot, NameDoors};
