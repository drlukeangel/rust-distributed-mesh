//! The fabric's one shared store root (R14): `<store_dir>/<fabric_id>/`, one location every node
//! of the fabric reaches on the same backing namespace.
//!
//! Layout: `fabric/` (the fabric-primary's), `meshes/<mesh>` (each mesh's primary's Mesh row),
//! `nodes/<mesh>/` (each mesh's primary's members' Node rows and contacts), `connections/<node>/`
//! (each node its own) and `status/<mesh>/`. The writers are exactly the writer ladder
//! (fabric-node-lifecycle.md §4.3), so no two writers share a key. A node's data dir keeps only its
//! own runtime material. A `store_dir` that is missing or unreadable fails the boot by name; nothing
//! falls back to the data dir and nothing creates the `store_dir` (the provider supplies it).

use crate::model::{FabricId, PathName};
use crate::record_store::StorageError;
use std::path::{Path, PathBuf};

/// The env variable that names a node's `store_dir`.
pub use rafka_mesh_entity::launch::ENV_STORE_DIR;

/// The fabric's store root, `<store_dir>/<fabric_id>/`, checked at boot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreRoot {
    dir: PathBuf,
}

impl StoreRoot {
    /// The root of `fabric_id` under `store_dir`: `store_dir` must be a readable directory, refused by
    /// name (its path and the cause) otherwise and never created here; the fabric's directory under it
    /// is created when absent.
    pub fn open(store_dir: &Path, fabric_id: &FabricId) -> Result<Self, StorageError> {
        let refused = |cause: String| StorageError::Io { file: store_dir.display().to_string(), reason: format!("the store_dir ({ENV_STORE_DIR}) cannot be used: {cause}") };
        let meta = std::fs::metadata(store_dir).map_err(|e| refused(e.to_string()))?;
        if !meta.is_dir() {
            return Err(refused("it is not a directory".into()));
        }
        std::fs::read_dir(store_dir).map_err(|e| refused(format!("it cannot be listed: {e}")))?;
        let dir = store_dir.join(fabric_id.to_string());
        std::fs::create_dir_all(&dir).map_err(|e| StorageError::Io { file: dir.display().to_string(), reason: format!("the fabric's store root could not be created: {e}") })?;
        Ok(Self { dir })
    }

    /// The root at `dir` itself, the fabric's directory already chosen and created by the caller (a
    /// cell's own directory, or a store an embedding app hands in).
    pub fn at(dir: &Path) -> Self {
        Self { dir: dir.to_path_buf() }
    }

    /// `<store_dir>/<fabric_id>/`.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// `nodes/<mesh>/`: the mesh's members' Node rows and contacts.
    pub fn nodes(&self, mesh: &str) -> PathBuf {
        self.dir.join("nodes").join(mesh)
    }

    /// `connections/<node>/`: the node's own connections rows and raw log.
    pub fn connections(&self, node: &PathName) -> PathBuf {
        self.dir.join("connections").join(node.to_string())
    }

    /// `status/<mesh>/`.
    pub fn status(&self, mesh: &str) -> PathBuf {
        self.dir.join("status").join(mesh)
    }
}

/// Whether `provider` may back the store root: the process provider hands every node of an estate
/// one directory on the host's own filesystem. Any other provider qualifies only by a functional
/// cell proving hard-link insertion, atomic rename, file and directory fsync, and coherent reads
/// and listings on its actual backing store, cross-host included (R14 item 6); none exists, so it is
/// refused by name.
pub fn qualify(provider: crate::model::ProviderKind) -> Result<(), String> {
    match provider {
        crate::model::ProviderKind::Process => Ok(()),
        other => Err(format!(
            "the {other:?} provider is not qualified to back the fabric's store root ({ENV_STORE_DIR}): no cell proves hard-link insertion, atomic rename, file and directory fsync, and coherent reads and listings on its backing store (R14 item 6)"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::FabricId;

    /// CONTRACT (R14 items 1 and 8): a node's boot opens its stores under `<store_dir>/<fabric_id>/`
    /// and nothing else: a `store_dir` that is missing, or is not a readable directory, fails the
    /// boot by name, naming the path and the cause, and nothing is created in its place.
    #[test]
    fn a_missing_or_unreadable_store_dir_fails_the_boot_by_name_and_creates_nothing() {
        let d = crate::record_store::tempdir("store-root");
        let fabric = FabricId::mint();
        let missing = d.join("no-such-store");
        let e = StoreRoot::open(&missing, &fabric).unwrap_err().to_string();
        assert!(e.contains(&missing.display().to_string()) && e.contains("store_dir") && e.contains("No such file"), "{e}");
        assert!(!missing.exists(), "the boot created the store_dir it was refused for");
        let file = d.join("a-file");
        std::fs::write(&file, b"x").unwrap();
        let e = StoreRoot::open(&file, &fabric).unwrap_err().to_string();
        assert!(e.contains(&file.display().to_string()) && e.contains("not a directory"), "{e}");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = StoreRoot::open(&store, &fabric).unwrap();
        assert_eq!(root.dir(), store.join(fabric.to_string()));
        assert!(root.dir().is_dir());
        assert_eq!(root.connections(&"mesh1.rpc.1".parse().unwrap()), store.join(fabric.to_string()).join("connections").join("mesh1.rpc.1"));
        assert_eq!(root.status("mesh2"), store.join(fabric.to_string()).join("status").join("mesh2"));
    }
}
