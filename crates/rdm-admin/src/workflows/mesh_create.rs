//! Mesh create workflow (`docs/architecture/lifecycles/mesh-create.md`).
//!
//! The mesh-absent arm of `node.create`, run as a step of the Build whose accepted
//! topology names the new mesh. A mesh comes into being with its first node-admin.

pub use crate::deployment::pipeline::CreateMeshStep;
pub use crate::mesh_claims::{InFlight, Taken};
