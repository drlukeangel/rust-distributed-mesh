//! Fabric delete workflow (`docs/architecture/lifecycles/fabric-delete.md`).
//!
//! Tears down a fabric by running `mesh.delete` on each mesh leaf-inward, deleting
//! each mesh's final primary, releasing the fabric name, and reporting `fabric.deleted`.

pub use crate::fabric_storage::FabricStorage;
pub use crate::mesh_delete::complete_fabric_delete;
