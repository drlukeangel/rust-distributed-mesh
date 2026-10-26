//! The fabric's one shared store root (R14): `<store_dir>/<fabric_id>/`, one location every node
//! of the fabric reaches on the same backing namespace.
//!
//! Layout: `fabric/` (the fabric-primary's identity, seats, mesh deletes and `fabric/builds/` Build
//! journal), `meshes/<mesh>` (each mesh's primary's Mesh row),
//! `meshes/<mesh>/claims/` (the mesh's one attempt in flight, inserted by its claimant: the
//! fabric-primary for a topology operation, the mesh-primary for its drift attempt; R-BI7),
//! `meshes/<mesh>/builds/` (the mesh's primary's Build receipts; R-BI6),
//! `nodes/<mesh>/` (each mesh's primary's members' Node rows and contacts), `connections/<node>/`
//! (each node its own) and `status/<mesh>/` (each mesh's primary's status rows). Beside the
//! per-fabric roots, `<store_dir>/fabrics/<name>` is the fabrics' name index (R-FN1): each fabric's
//! claim of its name, written by its fabric-primary. The writers are exactly the writer ladder
//! (node-identity-and-state.md §4.3), so no two writers share a key. A node's data dir keeps only its
//! own runtime material. A `store_dir` that is missing or unreadable fails the boot by name; nothing
//! falls back to the data dir and nothing creates the `store_dir` (the provider supplies it).
//!
//! The root holds a layout marker, `layout` (R-SR1), written once by its creator before any other
//! entry of the root: [`StoreRoot::open`] makes a root that is absent or empty with its marker in one
//! step, and refuses by name a root that holds entries and no marker, or a marker of another value,
//! naming its path and the value found; nothing is migrated.
//!
//! The store exists once the fabric's first state-commit published it (states.md, Setting a state;
//! node-identity-and-state.md §2.7, The write: "Before then `system.ops.nodes` and
//! `system.ops.meshes` do not exist"). A node that opens a root holding no Fabric row yet holds
//! every row it writes in its [`Scratchpad`] until its commit-state action publishes them; a node
//! that opens a root the fabric-primary already published the Fabric row to writes to it directly.
//! Three stores write to the root from their open, before state-commit too, opening their
//! [`FileRecords`] directly rather than through [`StoreRoot::records`]: `status/<mesh>/declared`
//! (an Applied declaration, Q54), the Build lanes and the meshes' attempt claims (a committed Build
//! fact, Q61, build.md §5.2).

use crate::model::{FabricId, IncarnationId, PathName};
use crate::record_store::{FileRecords, Scratchpad};
use crate::storage_error::StorageError;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The env variable that names a node's `store_dir`.
pub use rdm_core::launch::ENV_STORE_DIR;

/// The Fabric row's file under the root: the fabric-primary publishes it at the fabric's first
/// state-commit, so a root that holds it is a store that exists.
const FABRIC_ROW: &str = "fabric/record.json";

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
        let empty = match std::fs::read_dir(&dir) {
            Ok(mut entries) => entries.next().is_none(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
            Err(e) => return Err(StorageError::Io { file: dir.display().to_string(), reason: format!("the fabric's store root cannot be listed: {e}") }),
        };
        if empty {
            create_root(store_dir, &dir)?;
        }
        check_layout(&dir)?;
        let scratchpad = (!dir.join(FABRIC_ROW).exists()).then(Scratchpad::new);
        Ok(Self { dir, store_dir: store_dir.to_path_buf(), scratchpad })
    }

    /// The root at `dir` itself, the fabric's directory already chosen and created by the caller (a
    /// cell's own directory, or a store an embedding app hands in), written directly. Nothing above
    /// `dir` is the root's: the fabrics' name index is `<dir>/fabrics/`.
    /// The caller is the root's creator: a directory with no layout marker is given this build's.
    pub fn at(dir: &Path) -> Self {
        mark(dir);
        Self { dir: dir.to_path_buf(), store_dir: dir.to_path_buf(), scratchpad: None }
    }

    /// [`Self::at`], every store opened on it holding its rows in `scratchpad` until it is published.
    pub fn held_at(dir: &Path, scratchpad: Arc<Scratchpad>) -> Self {
        mark(dir);
        Self { dir: dir.to_path_buf(), store_dir: dir.to_path_buf(), scratchpad: Some(scratchpad) }
    }

    /// `layout`: the root's layout marker (R-SR1).
    pub fn layout(&self) -> PathBuf {
        self.dir.join(LAYOUT)
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

    /// Open `<store_dir>/fabrics/`, the fabrics' name index above the per-fabric roots (R-FN1, Q109: the
    /// fabric's (path, name) claim sits at the path level, above the per-fabric store roots), written by the process born as `writer`: held in this root's scratchpad until it is
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
    /// in-flight attempt: ``mesh_claims``; `meshes/<mesh>/builds/`, its Build receipts: R-BI6).
    pub fn meshes(&self, mesh: &str) -> PathBuf {
        self.dir.join("meshes").join(mesh)
    }

    /// `fabric/builds/`: the fabric-primary's Build journal on the shared store root (R-BI6).
    pub fn fabric_builds(&self) -> PathBuf {
        self.dir.join("fabric").join("builds")
    }

    /// `meshes/<mesh>/builds/`: `mesh`'s primary's Build receipts on the shared store root (R-BI6).
    pub fn mesh_builds(&self, mesh: &str) -> PathBuf {
        self.meshes(mesh).join("builds")
    }

    /// `connections/<node>/`: the node's own connections rows and raw log.
    pub fn connections(&self, node: &PathName) -> PathBuf {
        self.dir.join("connections").join(node.to_string())
    }

    /// `connections-by-destination/<node id>/`: the by-destination index of the connections rows
    /// (connections.md §3.1), one member per (source, destination, alpn, kind) whose destination is
    /// that node, written by each source beside its own row.
    pub fn connections_to(&self, destination: &crate::model::NodeId) -> PathBuf {
        self.dir.join("connections-by-destination").join(destination.as_str())
    }

    /// `status/<mesh>/`.
    pub fn status(&self, mesh: &str) -> PathBuf {
        self.dir.join("status").join(mesh)
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
        assert_eq!(root.connections(&"mesh1.rpc.1".parse().unwrap()), store.join(fabric.to_string()).join("connections").join("mesh1.rpc.1"));
        assert_eq!(root.status("mesh2"), store.join(fabric.to_string()).join("status").join("mesh2"));
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
