//! One record per file in a store directory (the fabric's store root, or a node's data dir): the durable implementation every node-admin
//! storage boundary (`fabric.storage`, `mesh.storage`, `nodes.storage`) is built on.
//!
//! Each write is a temp file, fsync and atomic rename, then a directory fsync. Each record is
//! stored with its format tag; a file this build does not recognise (another format, or not a
//! record at all) is refused by name, never read as a value.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Why a storage read or write failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// The file could not be read or written.
    Io {
        /// The file.
        file: String,
        /// Why it failed.
        reason: String,
    },
    /// A write refused because this store's writer gave up its authority (a seat transfer's
    /// cut-off, R14 item 4): it never reached the disk.
    CutOff {
        /// The file.
        file: String,
        /// The cut-off's reason.
        reason: String,
    },
    /// A write refused because this writer does not hold the seat that writes the row (R14 item 2:
    /// the writers are exactly the writer ladder): it never reached the disk.
    NotSeatHolder {
        /// The file.
        file: String,
        /// The seat and what the committed seat record says of this writer.
        reason: String,
    },
    /// A file this build does not recognise: refused, never read as a value.
    Unrecognised {
        /// The file.
        file: String,
        /// Why it is not recognised.
        reason: String,
    },
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { file, reason } => write!(f, "{file}: {reason}"),
            Self::CutOff { file, reason } => write!(f, "{file}: this store's writes are cut off: {reason}"),
            Self::NotSeatHolder { file, reason } => write!(f, "{file}: this writer does not hold the seat that writes it: {reason}"),
            Self::Unrecognised { file, reason } => write!(f, "{file} is not a record this build recognises: {reason}"),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Stored<T> {
    format: String,
    record: T,
}

/// One row a store holds in its process's scratchpad.
#[derive(Debug, Clone)]
enum HeldRow {
    /// The whole row, as a write puts it.
    Put(Vec<u8>),
    /// A create-once row, as an insert claims it: published only where no row is live at its key.
    Insert(Vec<u8>),
    /// The row removed.
    Removed,
}

/// Whether a store may write what it holds to the store root when its scratchpad is published:
/// `Err` names the seat its writer does not hold (R14 item 2: the writers are exactly the writer
/// ladder), and the store keeps its rows. A store with no gate publishes unless it is cut off.
pub type PublishGate = std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

/// The scratchpad side of one store on the fabric's store root (states.md, Rounds: "Before
/// state-commit, RDM holds its rows in the scratchpad; state-commit publishes the required held
/// rows to the configured stores").
struct Held {
    /// The store's directory under the store root: how a publication names it.
    store: String,
    /// `true` until the scratchpad is published: every write is held. Decided under the store's
    /// write lock, so a write and the publication are ordered.
    holding: std::sync::Mutex<bool>,
    /// The rows held, by key: before publication every write; after it, only the rows the store
    /// kept (it was cut off, or its gate named a seat its writer does not hold). Read first, before
    /// the disk; never written by anyone else.
    rows: std::sync::Mutex<std::collections::BTreeMap<String, HeldRow>>,
    gate: std::sync::OnceLock<PublishGate>,
}

/// The records of one store under `<base>/<dir>/`, one `<key>.json` each.
#[derive(Debug, Clone)]
pub struct FileRecords(std::sync::Arc<Records>);

struct Records {
    dir: PathBuf,
    /// Serializes this store's writers. An async lock: a writer waiting on another's fsync yields
    /// its worker instead of blocking it. Each writer moves its guard into its disk job, so the
    /// lock is held until the disk operation finishes whatever happens to the caller: taking it
    /// once waits for every write this store issued (R14 item 4).
    write: std::sync::Arc<tokio::sync::Mutex<()>>,
    /// Set when this store's writer gave up its authority (a seat transfer's cut-off): every write
    /// granted the lock from then on is refused by name, a write queued before it included.
    cut_off: std::sync::Mutex<Option<String>>,
    /// The incarnation of the process writing through this store: its temp files are named by it,
    /// so two writers' temp files never share a name (R14 item 5).
    writer: String,
    /// `Some` for a store on a store root its process opened before the fabric's first
    /// state-commit: it holds its writes in the scratchpad until the scratchpad is published.
    /// `None` for a store that writes to the disk from its open (a node's data dir, or a store root
    /// that already holds the fabric's row).
    held: Option<Held>,
}

impl std::fmt::Debug for Records {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileRecords").field("dir", &self.dir).field("writer", &self.writer).field("held", &self.held.as_ref().map(|h| h.store.as_str())).finish()
    }
}

impl FileRecords {
    /// Open (creating) the directory `dir` under `base` (the node's data dir, or a directory of the
    /// fabric's store root), written by the process born as `writer`, straight to the disk.
    pub fn open(base: &Path, dir: &str, writer: &crate::model::IncarnationId) -> Result<Self, StorageError> {
        Self::opened(base, dir, writer, None)
    }

    /// [`Self::open`], holding every write in `scratchpad` until it is published; named `store` in
    /// the publication. A scratchpad already published opens the store straight to the disk.
    pub(crate) fn open_held(base: &Path, dir: &str, writer: &crate::model::IncarnationId, scratchpad: &Scratchpad, store: String) -> Result<Self, StorageError> {
        let held = Held { store, holding: std::sync::Mutex::new(true), rows: Default::default(), gate: std::sync::OnceLock::new() };
        let records = Self::opened(base, dir, writer, Some(held))?;
        if !scratchpad.enrol(std::sync::Arc::new(records.clone())) {
            *records.held().expect("opened held").holding.lock().unwrap_or_else(|e| e.into_inner()) = false;
        }
        Ok(records)
    }

    fn opened(base: &Path, dir: &str, writer: &crate::model::IncarnationId, held: Option<Held>) -> Result<Self, StorageError> {
        let dir = base.join(dir);
        std::fs::create_dir_all(&dir).map_err(|e| StorageError::Io { file: dir.display().to_string(), reason: e.to_string() })?;
        Ok(Self(std::sync::Arc::new(Records { dir, write: std::sync::Arc::new(tokio::sync::Mutex::new(())), cut_off: std::sync::Mutex::new(None), writer: writer.0.clone(), held })))
    }

    fn held(&self) -> Option<&Held> {
        self.0.held.as_ref()
    }

    /// The gate this store's publication passes before it writes what it holds (set once).
    pub fn set_publish_gate(&self, gate: PublishGate) {
        if let Some(h) = self.held() {
            let _ = h.gate.set(gate);
        }
    }

    /// Whether this store holds its writes in its scratchpad now.
    pub fn holding(&self) -> bool {
        self.held().is_some_and(|h| *h.holding.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// The row the scratchpad holds at `key`, when it holds one.
    fn held_row(&self, key: &str) -> Option<HeldRow> {
        self.held().and_then(|h| h.rows.lock().unwrap_or_else(|e| e.into_inner()).get(key).cloned())
    }

    /// Hold `row` at `key` when this store is holding its writes (the caller holds the write
    /// lock): `true` when it was held, `false` when the write goes to the disk.
    fn hold(&self, key: &str, row: HeldRow) -> bool {
        let Some(h) = self.held() else { return false };
        if !*h.holding.lock().unwrap_or_else(|e| e.into_inner()) {
            return false;
        }
        h.rows.lock().unwrap_or_else(|e| e.into_inner()).insert(key.to_string(), row);
        true
    }

    fn path(&self, key: &str) -> PathBuf {
        self.0.dir.join(format!("{key}.json"))
    }

    /// `.<key>.<writer incarnation>.json.tmp`: the temp file a write of `key` renames from.
    pub(crate) fn tmp_path(&self, key: &str) -> PathBuf {
        self.0.dir.join(format!(".{key}.{}.json.tmp", self.0.writer))
    }

    fn decode<T: DeserializeOwned>(path: &Path, bytes: &[u8], format: &str) -> Result<T, StorageError> {
        let stored: Stored<T> =
            serde_json::from_slice(bytes).map_err(|e| StorageError::Unrecognised { file: path.display().to_string(), reason: e.to_string() })?;
        if stored.format != format {
            return Err(StorageError::Unrecognised { file: path.display().to_string(), reason: format!("format {:?}, this build reads {format:?}", stored.format) });
        }
        Ok(stored.record)
    }

    fn encode<T: Serialize>(path: &Path, format: &str, record: &T) -> Result<Vec<u8>, StorageError> {
        serde_json::to_vec(&Stored { format: format.to_string(), record }).map_err(|e| StorageError::Io { file: path.display().to_string(), reason: e.to_string() })
    }

    /// The record stored under `key`, or `None` when none was ever written: the scratchpad's row
    /// when it holds one, else a point read of its file on the blocking pool, as every write is, so
    /// no worker thread waits on the disk.
    pub async fn read<T: DeserializeOwned>(&self, key: &str, format: &str) -> Result<Option<T>, StorageError> {
        let path = self.path(key);
        match self.held_row(key) {
            Some(HeldRow::Put(b) | HeldRow::Insert(b)) => return Self::decode(&path, &b, format).map(Some),
            Some(HeldRow::Removed) => return Ok(None),
            None => {}
        }
        let file = path.clone();
        match blocking_io(move || std::fs::read(&file)).await {
            Ok(b) => Self::decode(&path, &b, format).map(Some),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
        }
    }

    /// Every record in the store, in key order: the rows the scratchpad holds over the files on the
    /// disk (a row it holds removed hides the file), the directory and its files read on the
    /// blocking pool, as a point read is, so no worker thread waits on the disk.
    pub async fn list<T: DeserializeOwned + Send + 'static>(&self, format: &str) -> Result<Vec<T>, StorageError> {
        // The held rows are read before the disk: a row the publication moves to the disk meanwhile
        // is in one of the two.
        let held: std::collections::BTreeMap<String, HeldRow> = self.held().map(|h| h.rows.lock().unwrap_or_else(|e| e.into_inner()).clone()).unwrap_or_default();
        let (dir, format) = (self.0.dir.clone(), format.to_string());
        blocking_io(move || {
            let io = |e: std::io::Error| StorageError::Io { file: dir.display().to_string(), reason: e.to_string() };
            let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
                .map_err(io)?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|x| x == "json"))
                .collect();
            paths.sort();
            let mut records: std::collections::BTreeMap<String, T> = std::collections::BTreeMap::new();
            // A file listed and then removed before it is read is a record that is gone: the list
            // answers the records that remain.
            for p in paths {
                let key = p.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
                if held.contains_key(&key) {
                    continue;
                }
                match std::fs::read(&p) {
                    Ok(b) => {
                        records.insert(key, Self::decode(&p, &b, &format)?);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(StorageError::Io { file: p.display().to_string(), reason: e.to_string() }),
                }
            }
            for (key, row) in held {
                if let HeldRow::Put(b) | HeldRow::Insert(b) = row {
                    let p = dir.join(format!("{key}.json"));
                    records.insert(key, Self::decode(&p, &b, &format)?);
                }
            }
            Ok(records.into_values().collect())
        })
        .await
    }

    /// Store `record` under `key`, replacing what was there: held in the scratchpad while the store
    /// holds its writes; otherwise temp file, fsync, rename, directory fsync, on the blocking pool.
    pub async fn write<T: Serialize>(&self, key: &str, format: &str, record: &T) -> Result<(), StorageError> {
        self.write_gated(key, format, record, || Ok(())).await
    }

    /// [`Self::write`] whose `gate` is asked only when the write goes to the disk (a held write is
    /// gated at its publication instead): a refusal never reaches the disk.
    pub async fn write_gated<T: Serialize>(&self, key: &str, format: &str, record: &T, gate: impl FnOnce() -> Result<(), StorageError>) -> Result<(), StorageError> {
        let path = self.path(key);
        let bytes = Self::encode(&path, format, record)?;
        let held = self.writer(&path).await?;
        if self.hold(key, HeldRow::Put(bytes.clone())) {
            return Ok(());
        }
        gate()?;
        let (dir, tmp, this, key) = (self.0.dir.clone(), self.tmp_path(key), self.clone(), key.to_string());
        blocking_io(move || {
            let _held = held;
            write_file(&dir, &tmp, &path, &bytes)?;
            this.forget_kept(&key);
            Ok(())
        })
        .await
    }

    /// Store `record` under `key` only if no record is there: the claim is the filesystem's own
    /// atomic link of the finished temp file to the key, which fails when the key is live (held in
    /// the scratchpad while the store holds its writes, against the rows it holds and the disk).
    /// Returns whether this call created it.
    pub async fn insert<T: Serialize>(&self, key: &str, format: &str, record: &T) -> Result<bool, StorageError> {
        let path = self.path(key);
        let bytes = Self::encode(&path, format, record)?;
        let held = self.writer(&path).await?;
        if self.holding() {
            let live = match self.held_row(key) {
                Some(HeldRow::Put(_) | HeldRow::Insert(_)) => true,
                Some(HeldRow::Removed) => false,
                None => path.exists(),
            };
            return Ok(!live && self.hold(key, HeldRow::Insert(bytes)));
        }
        let (dir, tmp) = (self.0.dir.clone(), self.tmp_path(key));
        blocking_io(move || {
            let _held = held;
            insert_file(&dir, &tmp, &path, &bytes)
        })
        .await
    }

    /// Whether a record under `key` exists, whatever it holds: the scratchpad's row, else its file.
    pub fn holds(&self, key: &str) -> bool {
        match self.held_row(key) {
            Some(HeldRow::Put(_) | HeldRow::Insert(_)) => true,
            Some(HeldRow::Removed) => false,
            None => self.path(key).exists(),
        }
    }

    /// The write lock, owned so the caller moves it into its disk job, and the cut-off checked
    /// once it is granted: a write queued before the cut-off and granted the lock after it is
    /// refused by name.
    async fn writer(&self, file: &Path) -> Result<tokio::sync::OwnedMutexGuard<()>, StorageError> {
        let held = self.0.write.clone().lock_owned().await;
        if let Some(reason) = self.0.cut_off.lock().unwrap_or_else(|e| e.into_inner()).clone() {
            return Err(StorageError::CutOff { file: file.display().to_string(), reason });
        }
        Ok(held)
    }

    /// A row the store kept at its publication is superseded by the write of its key to the disk.
    fn forget_kept(&self, key: &str) {
        if let Some(h) = self.held() {
            h.rows.lock().unwrap_or_else(|e| e.into_inner()).remove(key);
        }
    }

    /// Wait for every write this store issued: the write lock taken once and released. Each
    /// write holds the lock until its disk job finishes, so this returns only once they have.
    pub async fn drain(&self) {
        drop(self.0.write.lock().await);
    }

    /// Refuse every write granted the lock from now on, naming `reason`.
    pub fn cut_off(&self, reason: String) {
        *self.0.cut_off.lock().unwrap_or_else(|e| e.into_inner()) = Some(reason);
    }

    /// Lift the cut-off: the transfer it was set for committed nothing, so this store's writer
    /// keeps its authority, and the rows it kept at a publication made while it was cut off are
    /// written now.
    pub async fn lift_cut_off(&self) -> Result<(), StorageError> {
        *self.0.cut_off.lock().unwrap_or_else(|e| e.into_inner()) = None;
        if self.held().is_some() && !self.holding() {
            self.publish_held().await?;
        }
        Ok(())
    }

    /// Remove the record under `key`, if any: held in the scratchpad while the store holds its
    /// writes.
    pub async fn remove(&self, key: &str) -> Result<(), StorageError> {
        let (path, dir) = (self.path(key), self.0.dir.clone());
        let held = self.writer(&path).await?;
        if self.hold(key, HeldRow::Removed) {
            return Ok(());
        }
        let (this, key) = (self.clone(), key.to_string());
        blocking_io(move || {
            let _held = held;
            let removed = match std::fs::remove_file(&path) {
                // A removal that must survive a crash syncs its directory.
                Ok(()) => sync_dir(&dir, &path),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
            };
            if removed.is_ok() {
                this.forget_kept(&key);
            }
            removed
        })
        .await
    }

    /// Write what this store holds to the disk and write to the disk from now on (the scratchpad's
    /// publication): under the write lock, so no write slips between. A store cut off, or whose gate
    /// refuses, keeps its rows (still read, never written) and says why.
    async fn publish_held(&self) -> Result<StorePublished, StorageError> {
        let Some(h) = self.held() else { return Ok(StorePublished::default()) };
        let guard = self.0.write.clone().lock_owned().await;
        *h.holding.lock().unwrap_or_else(|e| e.into_inner()) = false;
        let rows = h.rows.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let mut done = StorePublished { store: h.store.clone(), ..Default::default() };
        if rows.is_empty() {
            return Ok(done);
        }
        let cut = self.0.cut_off.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let why_kept = match (cut, h.gate.get()) {
            (Some(reason), _) => Some(format!("cut off: {reason}")),
            (None, Some(gate)) => gate().err(),
            (None, None) => None,
        };
        if let Some(why) = why_kept {
            done.kept = rows.len();
            done.why_kept = Some(why);
            return Ok(done);
        }
        let this = self.clone();
        let written = blocking_io(move || {
            let _held = guard;
            let mut written = 0;
            for (key, row) in rows {
                let path = this.path(&key);
                match row {
                    HeldRow::Put(b) => write_file(&this.0.dir, &this.tmp_path(&key), &path, &b)?,
                    // A create-once row that is already live at its key stays what it is.
                    HeldRow::Insert(b) => {
                        insert_file(&this.0.dir, &this.tmp_path(&key), &path, &b)?;
                    }
                    HeldRow::Removed => match std::fs::remove_file(&path) {
                        Ok(()) => sync_dir(&this.0.dir, &path)?,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => return Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
                    },
                }
                this.forget_kept(&key);
                written += 1;
            }
            Ok::<usize, StorageError>(written)
        })
        .await?;
        done.written = written;
        Ok(done)
    }
}

/// What one store did with its rows when its scratchpad was published.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorePublished {
    /// The store's directory under the store root.
    pub store: String,
    /// The rows written to the store root.
    pub written: usize,
    /// The rows kept in the scratchpad instead, not written.
    pub kept: usize,
    /// Why the rows were kept.
    pub why_kept: Option<String>,
}

/// One store a scratchpad holds rows for until its publication.
#[async_trait::async_trait]
pub(crate) trait HeldStore: Send + Sync {
    /// Write what the store holds to the store root, and write there from now on.
    async fn publish(&self) -> Result<StorePublished, StorageError>;
}

#[async_trait::async_trait]
impl HeldStore for FileRecords {
    async fn publish(&self) -> Result<StorePublished, StorageError> {
        self.publish_held().await
    }
}

/// What a process holds before its fabric's first state-commit (states.md, Rounds: "Before
/// state-commit, RDM holds its rows in the scratchpad; state-commit publishes the required held
/// rows to the configured stores"; fabric-state-commit.md step 2): every store the process opens
/// on a store root that holds no Fabric row yet holds its writes here. [`Scratchpad::publish`],
/// the node's commit-state action, writes what each store holds to the store root, and from then
/// on every store writes there directly (node-admin-lifecycle.md §7). Each node holds only the
/// rows it writes, so what it publishes is its own rung of the writer ladder.
#[derive(Default)]
pub struct Scratchpad {
    state: std::sync::Mutex<ScratchState>,
}

#[derive(Default)]
struct ScratchState {
    published: bool,
    stores: Vec<std::sync::Arc<dyn HeldStore>>,
}

/// What a scratchpad's publication did, store by store.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Publication {
    /// Each store that held rows.
    pub stores: Vec<StorePublished>,
}

impl Publication {
    /// The rows written to the store root.
    pub fn written(&self) -> usize {
        self.stores.iter().map(|s| s.written).sum()
    }

    /// The rows kept in the scratchpad.
    pub fn kept(&self) -> usize {
        self.stores.iter().map(|s| s.kept).sum()
    }

    /// `<store>=<written>` for each store that wrote rows, `<store> kept <n>: <why>` for each that
    /// kept them, comma-separated.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        for s in &self.stores {
            if s.written > 0 {
                parts.push(format!("{}={}", s.store, s.written));
            }
            if s.kept > 0 {
                parts.push(format!("{} kept {}: {}", s.store, s.kept, s.why_kept.as_deref().unwrap_or("")));
            }
        }
        parts.join(",")
    }
}

impl Scratchpad {
    /// A scratchpad holding nothing, not published.
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::default())
    }

    /// Hold `store`'s writes until the publication: `false` once published (the store writes to the
    /// store root from its open).
    pub(crate) fn enrol(&self, store: std::sync::Arc<dyn HeldStore>) -> bool {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if s.published {
            return false;
        }
        s.stores.push(store);
        true
    }

    /// Whether the scratchpad was published.
    pub fn published(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).published
    }

    /// Publish: every store enrolled writes what it holds to the store root and writes there from
    /// now on; a store opened from now on writes there from its open. A store that keeps rows (cut
    /// off, or not its writer's seat) keeps them, named; a later publication writes what has since
    /// become its writer's. A write that fails ends the publication with its error, naming the file:
    /// what it had not written is still held.
    pub async fn publish(&self) -> Result<Publication, StorageError> {
        let stores = {
            let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
            s.published = true;
            s.stores.clone()
        };
        let mut out = Publication::default();
        for store in stores {
            let done = store.publish().await?;
            if done.written > 0 || done.kept > 0 {
                out.stores.push(done);
            }
        }
        Ok(out)
    }
}

/// Cut off every store of `stores` with `reason`, then take each one's write lock once: every write
/// already issued has finished when this returns, and every later one is refused by name.
pub async fn cut_off_and_drain(stores: &[&FileRecords], reason: &str) {
    for s in stores {
        s.cut_off(reason.to_string());
    }
    for s in stores {
        s.drain().await;
    }
}

fn write_file(dir: &Path, tmp: &Path, path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    use std::io::Write as _;
    let io = |e: std::io::Error| StorageError::Io { file: path.display().to_string(), reason: e.to_string() };
    let mut f = std::fs::File::create(tmp).map_err(io)?;
    f.write_all(bytes).map_err(io)?;
    f.sync_all().map_err(io)?;
    std::fs::rename(tmp, path).map_err(io)?;
    sync_dir(dir, path)
}

/// Sync `dir` after `file` was renamed, linked or removed in it: a change whose directory sync
/// fails is a failed change, named.
fn sync_dir(dir: &Path, file: &Path) -> Result<(), StorageError> {
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| StorageError::Io { file: file.display().to_string(), reason: format!("the change landed but the directory {} could not be synced: {e}", dir.display()) })
}

fn insert_file(dir: &Path, tmp: &Path, path: &Path, bytes: &[u8]) -> Result<bool, StorageError> {
    use std::io::Write as _;
    let io = |e: std::io::Error| StorageError::Io { file: path.display().to_string(), reason: e.to_string() };
    let mut f = std::fs::File::create(tmp).map_err(io)?;
    f.write_all(bytes).map_err(io)?;
    f.sync_all().map_err(io)?;
    let linked = std::fs::hard_link(tmp, path);
    let _ = std::fs::remove_file(tmp);
    match linked {
        Ok(()) => sync_dir(dir, path).map(|()| true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(io(e)),
    }
}

/// Run `f`, a blocking disk call (a write that ends in an fsync), on tokio's blocking pool. No
/// worker thread waits on the disk, and no lock a worker could block on is held across it: a
/// journal commit that takes seconds delays only the caller awaiting it.
pub(crate) async fn blocking_io<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    match tokio::task::spawn_blocking(f).await {
        Ok(v) => v,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(e) => panic!("a blocking disk call was cancelled: {e}"),
    }
}

#[cfg(test)]
pub(crate) fn tempdir(tag: &str) -> PathBuf {
    static MADE: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());
    extern "C" fn remove_made() {
        for d in MADE.lock().unwrap_or_else(|e| e.into_inner()).drain(..) {
            let _ = std::fs::remove_dir_all(d);
        }
    }
    let d = std::env::temp_dir().join(format!("{tag}-{}-{}", std::process::id(), rand::random::<u64>()));
    std::fs::create_dir_all(&d).unwrap();
    let mut made = MADE.lock().unwrap();
    if made.is_empty() {
        // Every directory a test of this binary made is removed when the binary exits.
        unsafe { libc::atexit(remove_made) };
    }
    made.push(d.clone());
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A current-thread runtime whose blocking pool has one thread: a disk job queued behind a
    /// parked one stays issued and unfinished until the test releases the parked one.
    fn one_blocking_thread() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread().max_blocking_threads(1).enable_all().build().unwrap()
    }

    /// Park the one blocking thread until the returned sender is sent to (or dropped).
    fn park_the_blocking_pool() -> std::sync::mpsc::Sender<()> {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        drop(tokio::task::spawn_blocking(move || {
            let _ = rx.recv();
        }));
        tx
    }

    /// Poll `f` until it has issued its disk job and waits on it (two scheduler rounds), then drop it.
    async fn cancel_after_issue<F: std::future::Future>(f: F) -> bool {
        let mut rounds = 0;
        tokio::pin!(f);
        loop {
            tokio::select! {
                biased;
                _ = &mut f => return true,
                _ = tokio::task::yield_now() => {
                    rounds += 1;
                    if rounds == 2 {
                        return false;
                    }
                }
            }
        }
    }

    /// CONTRACT (R14 item 4): a writer whose caller is cancelled while its disk job is issued keeps
    /// the store's write lock until the disk job finishes, so taking the lock once (the drain) waits
    /// for every issued write and no cancellation releases it early.
    #[test]
    fn a_cancelled_writer_holds_the_store_lock_until_its_disk_job_finishes() {
        one_blocking_thread().block_on(async {
            let d = tempdir("records-cancel");
            let r = FileRecords::open(&d, "things", &crate::model::IncarnationId("test".into())).unwrap();
            let release = park_the_blocking_pool();
            assert!(!cancel_after_issue(r.write("a", "thing/1", &"one".to_string())).await, "the write finished while the blocking pool was parked");
            assert!(r.0.write.try_lock().is_err(), "the cancelled caller released the store's write lock while its disk job was still issued");
            release.send(()).unwrap();
            r.drain().await;
            assert_eq!(r.read::<String>("a", "thing/1").await.unwrap().as_deref(), Some("one"), "the drain returned before the cancelled write landed");
        });
    }

    /// CONTRACT (R14 item 4): a write queued on the lock before the cut-off and granted it after the
    /// cut-off is refused by name; it never reaches the disk.
    #[test]
    fn a_write_queued_before_the_cut_off_and_granted_after_it_is_refused_by_name() {
        one_blocking_thread().block_on(async {
            let d = tempdir("records-cut-off");
            let r = std::sync::Arc::new(FileRecords::open(&d, "things", &crate::model::IncarnationId("test".into())).unwrap());
            let release = park_the_blocking_pool();
            // The first write holds the lock across its (parked) disk job.
            let first = tokio::spawn({
                let r = r.clone();
                async move { r.write("a", "thing/1", &"one".to_string()).await }
            });
            tokio::task::yield_now().await;
            // The second is queued on the lock before the cut-off.
            let queued = tokio::spawn({
                let r = r.clone();
                async move { r.write("b", "thing/1", &"two".to_string()).await }
            });
            tokio::task::yield_now().await;
            assert!(!queued.is_finished(), "the queued write did not wait on the lock");
            r.cut_off("the seats moved to mesh1.admin.2 by take-seats:b1".into());
            release.send(()).unwrap();
            first.await.unwrap().unwrap();
            let refused = queued.await.unwrap();
            assert!(
                matches!(&refused, Err(StorageError::CutOff { file, reason }) if file.ends_with("b.json") && reason.contains("take-seats:b1")),
                "a write queued before the cut-off and granted the lock after it must be refused by name: {refused:?}"
            );
            assert_eq!(r.read::<String>("b", "thing/1").await.unwrap(), None, "the refused write reached the disk");
        });
    }

    /// CONTRACT (R14 item 5): a write, an insert or a delete whose directory fsync fails is a failed
    /// write, named: the store directory is searchable and writable but unreadable, so the file
    /// lands and the directory cannot be opened to sync.
    #[tokio::test]
    async fn a_directory_fsync_failure_fails_the_write_the_insert_and_the_delete_by_name() {
        use std::os::unix::fs::PermissionsExt as _;
        let d = tempdir("records-dir-fsync");
        let r = FileRecords::open(&d, "things", &crate::model::IncarnationId("test".into())).unwrap();
        r.write("gone", "thing/1", &"x".to_string()).await.unwrap();
        let dir = d.join("things");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o300)).unwrap();
        let named = |e: &Result<(), StorageError>| matches!(e, Err(StorageError::Io { reason, .. }) if reason.contains("directory") && reason.contains("could not be synced"));
        let wrote = r.write("a", "thing/1", &"one".to_string()).await;
        let inserted = r.insert("b", "thing/1", &"two".to_string()).await.map(|_| ());
        let removed = r.remove("gone").await;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(named(&wrote), "a write whose directory fsync failed answered {wrote:?}");
        assert!(named(&inserted), "an insert whose directory fsync failed answered {inserted:?}");
        assert!(named(&removed), "a delete whose directory fsync failed answered {removed:?}");
    }

    fn files(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir).map(|d| d.filter_map(|e| e.ok()).map(|e| e.file_name().to_string_lossy().into_owned()).collect()).unwrap_or_default();
        names.sort();
        names
    }

    /// CONTRACT (states.md, Rounds: "Before state-commit, RDM holds its rows in the scratchpad;
    /// state-commit publishes the required held rows to the configured stores"): a store opened on
    /// a scratchpad puts no file on the disk for a write, an insert or a removal; its reads and lists
    /// answer what it holds over what the disk holds. The publication writes what it holds, a held
    /// removal removing the file, and from then on the store writes to the disk directly; a store
    /// opened after the publication writes to the disk from its open.
    #[tokio::test]
    async fn a_held_store_writes_nothing_until_its_scratchpad_is_published_then_writes_to_the_disk() {
        let d = tempdir("records-held");
        let w = crate::model::IncarnationId("held".into());
        // Another writer's row, already on the disk.
        FileRecords::open(&d, "things", &w).unwrap().write("theirs", "thing/1", &"t".to_string()).await.unwrap();
        let pad = Scratchpad::new();
        let r = FileRecords::open_held(&d, "things", &w, &pad, "things".into()).unwrap();
        assert!(r.holding());
        r.write("a", "thing/1", &"one".to_string()).await.unwrap();
        assert!(r.insert("b", "thing/1", &"two".to_string()).await.unwrap(), "a held insert of an absent key creates it");
        assert!(!r.insert("b", "thing/1", &"again".to_string()).await.unwrap(), "a held insert of a key it holds is refused");
        assert!(!r.insert("theirs", "thing/1", &"mine".to_string()).await.unwrap(), "a held insert of a key live on the disk is refused");
        r.write("c", "thing/1", &"three".to_string()).await.unwrap();
        r.remove("c").await.unwrap();
        assert_eq!(files(&d.join("things")), vec!["theirs.json"], "nothing the held store wrote reached the disk");
        assert_eq!(r.read::<String>("a", "thing/1").await.unwrap().as_deref(), Some("one"));
        assert_eq!(r.read::<String>("c", "thing/1").await.unwrap(), None, "a held removal hides the row");
        assert_eq!(r.list::<String>("thing/1").await.unwrap(), vec!["one", "two", "t"], "a list answers the held rows over the disk's, in key order");
        let published = pad.publish().await.unwrap();
        assert_eq!(published.written(), 3, "{published:?}");
        assert_eq!(published.summary(), "things=3");
        assert!(!r.holding());
        assert_eq!(files(&d.join("things")), vec!["a.json", "b.json", "theirs.json"], "the publication wrote what was held, the held removal included");
        r.write("d", "thing/1", &"four".to_string()).await.unwrap();
        assert!(d.join("things").join("d.json").exists(), "after the publication a write goes to the disk");
        let later = FileRecords::open_held(&d, "later", &w, &pad, "later".into()).unwrap();
        assert!(!later.holding(), "a store opened after the publication writes to the disk from its open");
        assert_eq!(pad.publish().await.unwrap().written(), 0, "a second publication finds nothing held");
    }

    /// CONTRACT (R14 items 2 and 4): a store cut off when its scratchpad is published, or whose
    /// gate names a seat its writer does not hold, writes nothing to the disk: it keeps its rows,
    /// still read, and says why. A lifted cut-off writes the rows the store kept; a gated store's
    /// rows stay its own, and a write that reaches the disk after the publication is asked the gate.
    #[tokio::test]
    async fn a_store_cut_off_or_gated_at_publication_keeps_its_rows_and_a_lift_writes_them() {
        let d = tempdir("records-kept");
        let w = crate::model::IncarnationId("kept".into());
        let pad = Scratchpad::new();
        let cut = FileRecords::open_held(&d, "cut", &w, &pad, "cut".into()).unwrap();
        let gated = FileRecords::open_held(&d, "gated", &w, &pad, "gated".into()).unwrap();
        gated.set_publish_gate(std::sync::Arc::new(|| Err("fabric-primary seat: held by mesh2.admin.1".to_string())));
        cut.write("a", "thing/1", &1u32).await.unwrap();
        gated.write("p", "thing/1", &2u32).await.unwrap();
        cut.cut_off("seat transfer drain-node:mesh1.admin.1".into());
        let published = pad.publish().await.unwrap();
        assert_eq!((published.written(), published.kept()), (0, 2), "{published:?}");
        assert_eq!(published.summary(), "cut kept 1: cut off: seat transfer drain-node:mesh1.admin.1,gated kept 1: fabric-primary seat: held by mesh2.admin.1");
        assert!(files(&d.join("cut")).is_empty() && files(&d.join("gated")).is_empty(), "a kept row never reaches the disk");
        assert_eq!(gated.read::<u32>("p", "thing/1").await.unwrap(), Some(2), "a kept row is still read");
        let refused = gated.write_gated("q", "thing/1", &3u32, || Err(StorageError::NotSeatHolder { file: "gated/q.json".into(), reason: "held by mesh2.admin.1".into() })).await;
        assert!(matches!(refused, Err(StorageError::NotSeatHolder { .. })), "after the publication a write that reaches the disk is asked its gate: {refused:?}");
        cut.lift_cut_off().await.unwrap();
        assert_eq!(files(&d.join("cut")), vec!["a.json"], "a lifted cut-off writes the row the store kept");
    }

    /// CONTRACT (R14 item 2: "Keys never overlap across writers"; states.md, Rounds): a store whose
    /// writer does not own its rows when its scratchpad is published writes none of them and holds
    /// none of them: after the publication every read and list answers the disk, where the key's one
    /// writer puts the row. What must NOT happen: a row held at the publication read in place of the
    /// row the key's writer later put on the disk.
    #[tokio::test]
    async fn a_row_held_by_a_writer_that_does_not_own_its_key_is_never_read_in_place_of_the_disk() {
        let d = tempdir("records-shadow");
        let pad = Scratchpad::new();
        let other = FileRecords::open_held(&d, "fabric", &crate::model::IncarnationId("mesh2-admin-1".into()), &pad, "fabric".into()).unwrap();
        other.set_publish_gate(std::sync::Arc::new(|| Err("fabric-primary seat: held by mesh1.admin.1".to_string())));
        other.write("pointer", "thing/1", &"bld-1".to_string()).await.unwrap();
        pad.publish().await.unwrap();
        assert!(!other.holding(), "the publication ended the store's holding");
        // The key's one writer puts its row on the disk after the publication.
        FileRecords::open(&d, "fabric", &crate::model::IncarnationId("mesh1-admin-1".into())).unwrap().write("pointer", "thing/1", &"bld-2".to_string()).await.unwrap();
        assert_eq!(other.read::<String>("pointer", "thing/1").await.unwrap().as_deref(), Some("bld-2"), "a row held at the publication was read in place of the row the key's writer put on the disk");
        assert_eq!(other.list::<String>("thing/1").await.unwrap(), vec!["bld-2"], "a row held at the publication hid the disk's row from a list");
        assert!(other.holds("pointer"), "the disk holds the key");
    }

    /// CONTRACT (R14 item 5): every writer on the shared root names its temp file by the key and its
    /// own incarnation, `.<key>.<incarnation>.json.tmp`: two writers' temp files never share a name.
    /// The cell leaves a write's temp file in place (its rename target is a directory) and reads its name.
    #[tokio::test]
    async fn a_temp_file_is_named_by_its_key_and_its_writers_incarnation() {
        let d = tempdir("record-store-tmp");
        let writer = crate::model::IncarnationId("inc-7".into());
        let r = FileRecords::open(&d, "things", &writer).unwrap();
        std::fs::create_dir_all(d.join("things").join("k.json").join("blocker")).unwrap();
        assert!(r.write("k", "thing/1", &1u32).await.is_err(), "a rename onto a non-empty directory fails");
        assert!(d.join("things").join(".k.inc-7.json.tmp").exists(), "{:?}", std::fs::read_dir(d.join("things")).unwrap().map(|e| e.unwrap().file_name()).collect::<Vec<_>>());
        let other = FileRecords::open(&d, "things", &crate::model::IncarnationId("inc-8".into())).unwrap();
        assert_ne!(r.tmp_path("k"), other.tmp_path("k"));
    }

    #[tokio::test]
    async fn a_record_survives_reopening_and_an_unknown_format_is_refused_by_name() {
        let d = tempdir("records");
        let r = FileRecords::open(&d, "things", &crate::model::IncarnationId("test".into())).unwrap();
        assert_eq!(r.read::<String>("a", "thing/1").await.unwrap(), None);
        r.write("a", "thing/1", &"one".to_string()).await.unwrap();
        r.write("b", "thing/1", &"two".to_string()).await.unwrap();
        let again = FileRecords::open(&d, "things", &crate::model::IncarnationId("test".into())).unwrap();
        assert_eq!(again.read::<String>("a", "thing/1").await.unwrap().as_deref(), Some("one"));
        assert_eq!(again.list::<String>("thing/1").await.unwrap(), ["one", "two"]);
        assert!(matches!(again.read::<String>("a", "thing/2").await, Err(StorageError::Unrecognised { .. })));
        std::fs::write(d.join("things").join("c.json"), b"not json").unwrap();
        assert!(matches!(again.list::<String>("thing/1").await, Err(StorageError::Unrecognised { file, .. }) if file.ends_with("c.json")));
        again.remove("c").await.unwrap();
        again.remove("a").await.unwrap();
        assert_eq!(again.list::<String>("thing/1").await.unwrap(), ["two"]);
    }

    /// CONTRACT: a record removed while another caller lists the store is a record that is gone;
    /// the list answers the records that remain and never fails on the one that vanished.
    #[tokio::test]
    async fn a_list_racing_a_removal_answers_the_records_that_remain() {
        let d = tempdir("records-race");
        let r = std::sync::Arc::new(FileRecords::open(&d, "things", &crate::model::IncarnationId("test".into())).unwrap());
        for round in 0..20 {
            for k in 0..200 {
                r.write(&format!("k{k:03}"), "thing/1", &k).await.unwrap();
            }
            let remover = {
                let r = r.clone();
                tokio::spawn(async move {
                    for k in 0..200 {
                        r.remove(&format!("k{k:03}")).await.unwrap();
                    }
                })
            };
            while !remover.is_finished() {
                let listed = r.list::<u32>("thing/1").await;
                assert!(listed.is_ok(), "round {round}: a list racing a removal failed: {:?}", listed.err());
            }
            remover.await.unwrap();
        }
    }
}
