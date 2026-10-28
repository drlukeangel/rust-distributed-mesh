//! Node delete workflow (`docs/architecture/lifecycles/node-delete.md`).
//!
//! Composes `node.stop`, verifies exact provider runtime exit proof, releases node ID and
//! name claims, and retains storage.

pub use crate::deployment::pipeline::{DeleteStep, StorageDisposition};
pub use crate::deployment::provider::{DeploymentHandle, DeploymentProvider, DeploymentStatus, TerminationMode};
pub use crate::admin::AdminRunner;
