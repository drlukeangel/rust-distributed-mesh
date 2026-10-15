//! Node runtime inspect primitive (`docs/architecture/lifecycles/node-runtime-inspect.md`).
//!
//! Queries the deployment provider for the exact runtime handle to verify whether the
//! process/container is running or proven exited.

pub use crate::deployment::provider::{DeploymentHandle, DeploymentProvider, DeploymentStatus, Found};
pub use crate::seat_watch::{inspect_birth, Finding};
