//! The fabric's one shared store root (R14): `<store_dir>/<fabric_id>/`, one location every node
//! of the fabric reaches on the same backing namespace.
//!
//! Layout: `fabric/` (the fabric-primary's), `meshes/<mesh>` (each mesh's primary's Mesh row),
//! `nodes/<mesh>/` (each mesh's primary's members' Node rows and contacts), `connections/<node>/`
//! (each node its own) and `status/<mesh>/` (each mesh's primary's status rows). Beside the
//! per-fabric roots, `<store_dir>/fabrics/<name>` is the fabrics' name index (R-FN1): each fabric's
//! claim of its name, written by its fabric-primary. The writers are exactly the writer ladder
//! (fabric-node-lifecycle.md §4.3), so no two writers share a key. A node's data dir keeps only its
//! own runtime material. A `store_dir` that is missing or unreadable fails the boot by name; nothing
//! falls back to the data dir and nothing creates the `store_dir` (the provider supplies it).
//!
//! The store exists once the fabric's first state-commit published it (states.md, Rounds;
//! node-identity-and-state.md §2.7, The write: "Before then `system.ops.nodes` and
//! `system.ops.meshes` do not exist"). A node that opens a root holding no Fabric row yet holds
//! every row it writes in its [`Scratchpad`] until its commit-state action publishes them; a node
//! that opens a root the fabric-primary already published the Fabric row to writes to it directly.

use crate::model::{FabricId, IncarnationId, PathName};
use crate::record_store::{FileRecords, Scratchpad, StorageError};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The env variable that names a node's `store_dir`.
pub use rafka_mesh_entity::launch::ENV_STORE_DIR;

/// The Fabric row's file under the root: the fabric-primary publishes it at the fabric's first
/// state-commit, so a root that holds it is a store that exists.
const FABRIC_ROW: &str = "fabric/identity.json";

/// The fabrics' name index, `<store_dir>/fabrics/`, beside the per-fabric roots (R-FN1).
pub const FABRIC_NAMES: &str = "fabrics";

/// The fabric's store root, `<store_dir>/<fabric_id>/`, checked at boot.
#[derive(Clone)]
pub struct StoreRoot {
    dir: PathBuf,
    /// Where the fabrics' name index lives, `<this>/fabrics/`: the `store_dir` above the per-fabric
    /// roots for a root opened on one ([`Self::open`]); the root's own directory for a root handed in
    /// whole ([`Self::at`]), whose caller chose the fabric's directory and owns nothing above it.
    store_dir: PathBuf,
    /// What this process holds before the fabric's first state-commit: `Some` when it opened the
    /// root before the Fabric row was published to it.
    scratchpad: Option<Arc<Scratchpad>>,
}

impl std::fmt::Debug for StoreRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreRoot").field("dir", &self.dir).field("writes_to", &self.writes_to()).finish()
    }
}

impl StoreRoot {
    /// The root of `fabric_id` under `store_dir`: `store_dir` must be a readable directory, refused by
    /// name (its path and the cause) otherwise and never created here; the fabric's directory under it
    /// is created when absent. A root that holds no Fabric row yet is opened with a scratchpad: the
    /// stores opened on it hold their rows until it is published.
    pub fn open(store_dir: &Path, fabric_id: &FabricId) -> Result<Self, StorageError> {
        let refused = |cause: String| StorageError::Io { file: store_dir.display().to_string(), reason: format!("the store_dir ({ENV_STORE_DIR}) cannot be used: {cause}") };
        let meta = std::fs::metadata(store_dir).map_err(|e| refused(e.to_string()))?;
        if !meta.is_dir() {
            return Err(refused("it is not a directory".into()));
        }
        std::fs::read_dir(store_dir).map_err(|e| refused(format!("it cannot be listed: {e}")))?;
        let dir = store_dir.join(fabric_id.to_string());
        std::fs::create_dir_all(&dir).map_err(|e| StorageError::Io { file: dir.display().to_string(), reason: format!("the fabric's store root could not be created: {e}") })?;
        let scratchpad = (!dir.join(FABRIC_ROW).exists()).then(Scratchpad::new);
        Ok(Self { dir, store_dir: store_dir.to_path_buf(), scratchpad })
    }

    /// The root at `dir` itself, the fabric's directory already chosen and created by the caller (a
    /// cell's own directory, or a store an embedding app hands in), written directly. Nothing above
    /// `dir` is the root's: the fabrics' name index is `<dir>/fabrics/`.
    pub fn at(dir: &Path) -> Self {
        Self { dir: dir.to_path_buf(), store_dir: dir.to_path_buf(), scratchpad: None }
    }

    /// [`Self::at`], every store opened on it holding its rows in `scratchpad` until it is published.
    pub fn held_at(dir: &Path, scratchpad: Arc<Scratchpad>) -> Self {
        Self { dir: dir.to_path_buf(), store_dir: dir.to_path_buf(), scratchpad: Some(scratchpad) }
    }

    /// What this process holds for the root before the fabric's first state-commit, when it opened
    /// the root before then.
    pub fn scratchpad(&self) -> Option<&Arc<Scratchpad>> {
        self.scratchpad.as_ref()
    }

    /// Where a write to a store opened on this root goes now: `scratchpad` before its publication,
    /// `store-root` after it, or when the root already held the Fabric row at open.
    pub fn writes_to(&self) -> &'static str {
        match &self.scratchpad {
            Some(s) if !s.published() => "scratchpad",
            _ => "store-root",
        }
    }

    /// Open the store `dir` under `base` (a directory of this root), written by the process born as
    /// `writer`: held in this root's scratchpad until it is published.
    pub async fn records(&self, base: &Path, dir: &str, writer: &IncarnationId) -> Result<FileRecords, StorageError> {
        match &self.scratchpad {
            Some(scratchpad) => {
                let store = base.join(dir);
                let store = store.strip_prefix(&self.dir).unwrap_or(&store).display().to_string().trim_end_matches('/').to_string();
                FileRecords::open_held(base, dir, writer, scratchpad, store).await
            }
            None => FileRecords::open(base, dir, writer).await,
        }
    }

    /// `<store_dir>/<fabric_id>/`.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Open `<store_dir>/fabrics/`, the fabrics' name index above the per-fabric roots (R-FN1: "The
    /// fabric's (path, name) claim is an insert-and-fail at the path level, above the per-fabric store
    /// roots"), written by the process born as `writer`: held in this root's scratchpad until it is
    /// published, named `fabrics` in the publication, as every store opened on it.
    pub async fn fabric_names(&self, writer: &IncarnationId) -> Result<FileRecords, StorageError> {
        match &self.scratchpad {
            Some(scratchpad) => FileRecords::open_held(&self.store_dir, FABRIC_NAMES, writer, scratchpad, FABRIC_NAMES.to_string()).await,
            None => FileRecords::open(&self.store_dir, FABRIC_NAMES, writer).await,
        }
    }

    /// `nodes/<mesh>/`: the mesh's members' Node rows and contacts.
    pub fn nodes(&self, mesh: &str) -> PathBuf {
        self.dir.join("nodes").join(mesh)
    }

    /// `meshes/<mesh>/`: the mesh's own rows beside its Mesh row (`meshes/<mesh>/claims/`, its
    /// in-flight attempt: `crate::mesh_claims`).
    pub fn meshes(&self, mesh: &str) -> PathBuf {
        self.dir.join("meshes").join(mesh)
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
