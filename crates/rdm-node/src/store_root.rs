//! The fabric's one shared store root (R14): `<store_dir>/<fabric_id>/`, one location every node
//! of the fabric reaches on the same backing namespace.
//!
//! Layout (R-SR1 to R-SR5, proposal `store-root-one-directory-per-entity.md` §4):
//!
//! ```text
//! <store_dir>/fabrics/<name>          the fabrics' name index (R-FN1): each fabric's claim of its name
//! <store_dir>/<fabric id>/
//!   layout                            the layout marker, r-sr1
//!   fabric.jsonl                      the Fabric record's writes
//!   meshes.jsonl                      every Mesh record's writes
//!   nodes.jsonl                       every node record's writes, every runtime row, every tag change
//!   connections.jsonl                 every connection fact of every source
//!   builds.jsonl                      every Build fact
//!   locks/in-flight/<mesh name>       the one attempt in flight on that mesh name, create-exclusive
//!   locks/name/<name>                 the claim of a mesh name or a node path.name, create-exclusive
//! ```
//!
//! One append-only JSONL file per storage boundary ([`crate::store_log`]): every write is one line
//! carrying the whole record, the latest complete line for a key is the current state, nothing is
//! ever removed, and the indexes are the fold. The conditional operations keep their guarantees in the
//! locks, because an append cannot refuse. The writers are exactly the writer ladder
//! (node-identity-and-state.md §4.3), one writer per key. A node's data dir keeps only its own runtime
//! material. A `store_dir` that is missing or unreadable fails the boot by name; nothing falls back to
//! the data dir and nothing creates the `store_dir` (the provider supplies it).
//!
//! The root holds a layout marker, `layout` (R-SR1), written once by its creator before any other
//! entry of the root: [`StoreRoot::open`] makes a root that is absent or empty with its marker in one
//! step, and refuses by name a root that holds entries and no marker, or a marker of another value,
//! naming its path and the value found; nothing is migrated.
//!
//! Every write reaches the root from its store's open: the fabric-primary's Fabric and Mesh rows, the
//! mesh-primaries' Node and Mesh rows, every node's connection rows, the Build facts and the meshes'
//! attempt claims (Q171).

use crate::model::{FabricId, IncarnationId};
use crate::record_store::FileRecords;
use crate::storage_error::StorageError;
use std::path::{Path, PathBuf};

/// The env variable that names a node's `store_dir`.
pub use rdm_core::launch::ENV_STORE_DIR;

/// The fabric's store file under the root: the Fabric record's writes.
pub const FABRIC_FILE: &str = "fabric.jsonl";
/// The meshes' store file: every Mesh record's writes.
pub const MESHES_FILE: &str = "meshes.jsonl";
/// The nodes' store file: every node record's writes, runtime rows and tag rows.
pub const NODES_FILE: &str = "nodes.jsonl";
/// The connections' store file: every connection fact of every source.
pub const CONNECTIONS_FILE: &str = "connections.jsonl";
/// The builds' store file: every Build fact.
pub const BUILDS_FILE: &str = "builds.jsonl";
/// The locks' directory: the attempt in flight on each mesh name and every name claim.
pub const LOCKS_DIR: &str = "locks";

/// The kind of the Fabric record's lines.
pub const FABRIC_RECORD_KIND: &str = "fabric-record";

/// Whether the root `dir`'s fabric file holds a Fabric record's line: the store exists.
/// The fabrics' name index, `<store_dir>/fabrics/`, beside the per-fabric roots (R-FN1).
pub const FABRIC_NAMES: &str = "fabrics";

/// The layout marker's file in a root (R-SR1): written once by the root's creator, before any other
/// entry of the root exists.
pub const LAYOUT: &str = "layout";

/// The layout this build reads and writes (R-SR1): a root whose marker holds another value, or that
/// holds no marker, is refused by name at open, with no migration. The next layout change bumps it.
pub const LAYOUT_VALUE: &str = "r-sr1";

/// The marker `dir` holds: `Ok(None)` when it holds none.
fn read_layout(dir: &Path) -> Result<Option<String>, StorageError> {
    let file = dir.join(LAYOUT);
    match std::fs::read(&file) {
        Ok(b) => Ok(Some(String::from_utf8_lossy(&b).trim_end_matches('\n').to_string())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StorageError::Io { file: file.display().to_string(), reason: format!("the store root's layout marker cannot be read: {e}") }),
    }
}

/// `dir`'s marker is this build's, refused by name (the root's path and the value found) otherwise.
fn check_layout(dir: &Path) -> Result<(), StorageError> {
    match read_layout(dir)? {
        Some(v) if v == LAYOUT_VALUE => Ok(()),
        Some(v) => Err(StorageError::Unrecognised { file: dir.display().to_string(), reason: format!("its layout marker holds {v:?}, this build reads {LAYOUT_VALUE:?} roots only; a root of another layout is never migrated (R-SR1)") }),
        None => Err(StorageError::Unrecognised { file: dir.display().to_string(), reason: format!("it holds entries and no layout marker ({LAYOUT}): a root of an older layout, never migrated; this build reads {LAYOUT_VALUE:?} roots only (R-SR1)") }),
    }
}

/// Make the root `dir` under `store_dir` with its marker in one step: a directory beside it holding
/// the marker, fsynced, renamed to `dir`. A root another opener made first stands, and this one's
/// directory goes. An empty directory at `dir` is no root and is replaced.
fn create_root(store_dir: &Path, dir: &Path) -> Result<(), StorageError> {
    use std::io::Write as _;
    let io = |file: &Path, e: std::io::Error| StorageError::Io { file: file.display().to_string(), reason: format!("the fabric's store root could not be created: {e}") };
    let name = dir.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    let tmp = store_dir.join(format!(".{name}.{}.{:x}.new", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0)));
    std::fs::create_dir(&tmp).map_err(|e| io(&tmp, e))?;
    let marker = tmp.join(LAYOUT);
    let mut f = std::fs::File::create(&marker).map_err(|e| io(&marker, e))?;
    f.write_all(format!("{LAYOUT_VALUE}\n").as_bytes()).map_err(|e| io(&marker, e))?;
    f.sync_all().map_err(|e| io(&marker, e))?;
    crate::durable_file::sync_dir(&tmp, &marker)?;
    match std::fs::rename(&tmp, dir) {
        Ok(()) => crate::durable_file::sync_dir(store_dir, dir),
        Err(_) if read_layout(dir)?.is_some() => {
            let _ = std::fs::remove_dir_all(&tmp);
            Ok(())
        }
        Err(e) => {
            let _ = std::fs::remove_dir_all(&tmp);
            Err(io(dir, e))
        }
    }
}

/// The fabric's store root, `<store_dir>/<fabric_id>/`, checked at boot.
#[derive(Clone)]
pub struct StoreRoot {
    dir: PathBuf,
    /// Where the fabrics' name index lives, `<this>/fabrics/`: the `store_dir` above the per-fabric
    /// roots for a root opened on one ([`Self::open`]); the root's own directory for a root handed in
    /// whole ([`Self::at`]), whose caller chose the fabric's directory and owns nothing above it.
    store_dir: PathBuf,
}

impl std::fmt::Debug for StoreRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreRoot").field("dir", &self.dir).finish()
    }
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
        let empty = match std::fs::read_dir(&dir) {
            Ok(mut entries) => entries.next().is_none(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
            Err(e) => return Err(StorageError::Io { file: dir.display().to_string(), reason: format!("the fabric's store root cannot be listed: {e}") }),
        };
        if empty {
            create_root(store_dir, &dir)?;
        }
        check_layout(&dir)?;
        Ok(Self { dir, store_dir: store_dir.to_path_buf() })
    }

    /// The root at `dir` itself, the fabric's directory already chosen and created by the caller (a
    /// cell's own directory, or a store an embedding app hands in), written directly. Nothing above
    /// `dir` is the root's: the fabrics' name index is `<dir>/fabrics/`.
    /// The caller is the root's creator: a directory with no layout marker is given this build's.
    pub fn at(dir: &Path) -> Self {
        mark(dir);
        Self { dir: dir.to_path_buf(), store_dir: dir.to_path_buf() }
    }

    /// `layout`: the root's layout marker (R-SR1).
    pub fn layout(&self) -> PathBuf {
        self.dir.join(LAYOUT)
    }

    /// Open the store `dir` under `base` (a directory of this root), written by the process born as
    /// `writer`.
    pub async fn records(&self, base: &Path, dir: &str, writer: &IncarnationId) -> Result<FileRecords, StorageError> {
        FileRecords::open(base, dir, writer).await
    }

    /// Open the store file `file` of this root, reading `formats`, folded on its first read or append
    /// and never at its open ([`crate::store_log::StoreLog::open_on_first_read`], rdm#89, Q227).
    pub async fn log(&self, file: &str, formats: crate::store_log::Formats) -> Result<crate::store_log::StoreLog, StorageError> {
        crate::store_log::StoreLog::open_on_first_read(&self.dir.join(file), formats).await
    }

    /// Open the store file `file` of this root to append to it, reading none of it
    /// ([`crate::store_log::StoreLog::open_append`]).
    pub async fn log_append(&self, file: &str, formats: crate::store_log::Formats) -> Result<crate::store_log::StoreLog, StorageError> {
        crate::store_log::StoreLog::open_append(&self.dir.join(file), formats).await
    }

    /// `fabric.jsonl`.
    pub fn fabric(&self) -> PathBuf {
        self.dir.join(FABRIC_FILE)
    }

    /// `<store_dir>/<fabric_id>/`.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Open `<store_dir>/fabrics/`, the fabrics' name index above the per-fabric roots (R-FN1, Q109: the
    /// fabric's (path, name) claim sits at the path level, above the per-fabric store roots), written by the process born as `writer`.
    pub async fn fabric_names(&self, writer: &IncarnationId) -> Result<FileRecords, StorageError> {
        FileRecords::open(&self.store_dir, FABRIC_NAMES, writer).await
    }

    /// `locks/name/<name>`: the claim of a mesh name or a node path.name (R-SR5).
    pub fn lock(&self, name: &str) -> PathBuf {
        self.dir.join(LOCKS_DIR).join("name").join(name)
    }

}

/// Give the root `dir` a caller chose this build's layout marker when it holds none: created
/// exclusively, so a marker another writer put there first stands.
fn mark(dir: &Path) {
    use std::io::Write as _;
    let _ = std::fs::create_dir_all(dir);
    if let Ok(mut f) = std::fs::OpenOptions::new().write(true).create_new(true).open(dir.join(LAYOUT)) {
        let _ = f.write_all(format!("{LAYOUT_VALUE}\n").as_bytes()).and_then(|()| f.sync_all());
    }
}

/// Providers qualified for the shared store namespace (R14 item 6): the process provider
/// supplies a host directory; the container provider bind-mounts that directory at the same
/// absolute path in every runtime. `container_proof` proves hard-link insertion, atomic
/// replacement, file and directory fsync, and coherent cross-container reads and listings.
pub fn qualify(provider: crate::model::ProviderKind) -> Result<(), String> {
    match provider {
        crate::model::ProviderKind::Process | crate::model::ProviderKind::Container => Ok(()),
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
    }

    /// CONTRACT (R-SR1, proposal §5.1 "An old root is refused by its marker, no migration"): the root's
    /// creator writes `layout` once, before any other entry exists, and the root reopens; a root that
    /// holds entries and no `layout`, and one whose `layout` holds another value, is refused by name at
    /// open, naming the root's path and the value found, and nothing in it is changed. Must NOT happen:
    /// an older root opened as an empty one, or a marker written into it.
    #[test]
    fn a_root_without_this_layout_is_refused_by_name_and_a_new_root_reopens() {
        let store = crate::record_store::tempdir("store-root-layout");
        let fabric = FabricId::mint();
        let root = StoreRoot::open(&store, &fabric).unwrap();
        assert_eq!(std::fs::read_to_string(root.layout()).unwrap(), format!("{LAYOUT_VALUE}\n"), "the creator writes the marker");
        std::fs::write(root.dir().join("nodes.jsonl"), b"").unwrap();
        StoreRoot::open(&store, &fabric).expect("a populated root in this layout reopens");
        let leftovers: Vec<_> = std::fs::read_dir(&store).unwrap().flatten().map(|e| e.file_name().to_string_lossy().into_owned()).filter(|n| n.starts_with('.')).collect();
        assert!(leftovers.is_empty(), "no creation directory is left beside the root: {leftovers:?}");

        let older = FabricId::mint();
        let dir = store.join(older.to_string());
        std::fs::create_dir_all(dir.join("fabric")).unwrap();
        std::fs::write(dir.join("fabric").join("record.json"), b"{}").unwrap();
        let e = StoreRoot::open(&store, &older).unwrap_err().to_string();
        assert!(e.contains(&dir.display().to_string()) && e.contains("no layout marker"), "{e}");
        assert!(!dir.join(LAYOUT).exists(), "the older root is left as it was");

        let other = FabricId::mint();
        let dir = store.join(other.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(LAYOUT), b"r-sr0\n").unwrap();
        let e = StoreRoot::open(&store, &other).unwrap_err().to_string();
        assert!(e.contains(&dir.display().to_string()) && e.contains("\"r-sr0\""), "the value found is named: {e}");
    }

    /// CONTRACT (R-SR1: the root's creator writes `layout` once): many openers of one new root at once
    /// make one root, every one of them opens it, and its marker is this build's. Must NOT happen: an
    /// opener refused because it looked between another's directory and its marker.
    #[test]
    fn openers_of_one_new_root_at_once_all_open_it() {
        let store = crate::record_store::tempdir("store-root-race");
        let fabric = FabricId::mint();
        let opened: Vec<_> = (0..16).map(|_| {
            let (store, fabric) = (store.clone(), fabric.clone());
            std::thread::spawn(move || StoreRoot::open(&store, &fabric).map(|_| ()))
        }).collect();
        for o in opened {
            o.join().unwrap().expect("every opener opens the one root");
        }
        assert_eq!(read_layout(&store.join(fabric.to_string())).unwrap().as_deref(), Some(LAYOUT_VALUE));
    }
}
