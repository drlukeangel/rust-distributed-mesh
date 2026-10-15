//! Mesh delete workflow (`docs/architecture/lifecycles/mesh-delete.md`).
//!
//! The teardown of an entire mesh: members are deleted, the mesh-primary leaves last,
//! and the mesh transitions to deleted.

pub use crate::mesh_delete::*;
pub use crate::admin::MeshDeleteDoor;
