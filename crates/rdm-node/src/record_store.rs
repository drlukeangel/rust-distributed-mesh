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

/// A held claim's decision, made again at the publication against the row then standing on the
/// disk at its key (its bytes; `None`: no row): `Ok(Ok(true))` the claim is written, `Ok(Ok(false))`
/// the same row stands and nothing is written, `Ok(Err)` the claim is refused, why named (its
/// holder). `Err`: the standing row is not a record this build recognises, refused by name and never
/// decided as no row.
pub type ClaimDecision = std::sync::Arc<dyn Fn(Option<&[u8]>) -> Result<Result<bool, String>, StorageError> + Send + Sync>;

/// One row a store holds in its process's scratchpad.
#[derive(Clone)]
enum HeldRow {
    /// The whole row, as a write puts it.
    Put(Vec<u8>),
    /// A create-once row, as an insert claims it: published only where no row is live at its key.
    Insert(Vec<u8>),
    /// A claim the store's adapter decided against what its writer saw (its own scratchpad over the
    /// disk): the publication decides it again against the row then standing on the disk, by
    /// `decide`, and never writes it blindly over a row another writer published meanwhile. `insert`:
    /// the key is one two writers can claim at once (above a fabric's root), so the publication
    /// inserts by the filesystem's atomic link first.
    Claim { bytes: Vec<u8>, decide: ClaimDecision, insert: bool },
    /// The row removed.
    Removed,
    /// The holder's row released (a withdrawn claim): the publication removes the row standing at
    /// the key only when `decide` says it is still the holder's (`Ok(true)`).
    Released(ClaimDecision),
}

impl HeldRow {
    /// The row's bytes, when it holds a row (`None`: removed or released).
    fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Put(b) | Self::Insert(b) | Self::Claim { bytes: b, .. } => Some(b),
            Self::Removed | Self::Released(_) => None,
        }
    }
}

impl std::fmt::Debug for HeldRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Put(b) => write!(f, "Put({} bytes)", b.len()),
            Self::Insert(b) => write!(f, "Insert({} bytes)", b.len()),
            Self::Claim { bytes, insert, .. } => write!(f, "Claim({} bytes, insert: {insert})", bytes.len()),
            Self::Removed => f.write_str("Removed"),
            Self::Released(_) => f.write_str("Released"),
        }
    }
}

/// What the publication did with one held claim.
enum ClaimPublished {
    /// Written at its key.
    Written,
    /// The same row stands at its key: nothing written.
    Stands,
    /// Refused against the row standing at its key, why named: nothing written.
    Refused(String),
}

/// Whether the writer of a store still owns the rows it held when its scratchpad is published:
/// `Err` names the seat its writer no longer holds (R14 item 2: the writers are exactly the writer
/// ladder), and the rows it held are dropped, named, never written and never read again. A store
/// with no gate is its writer's own.
pub type PublishGate = std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

/// The scratchpad side of one store on the fabric's store root (states.md, Rounds: "Before
/// state-commit, RDM holds its rows in the scratchpad; state-commit publishes the required held
/// rows to the configured stores").
struct Held {
    /// The store's directory under the store root: how a publication names it.
    store: String,
    /// `true` while every write is held: until the scratchpad is published, and past it only while a
    /// seat transfer that cut the store off has neither committed nor been lifted (the rows are
    /// still its writer's until it does). Decided under the store's write lock, so a write and the
    /// publication are ordered.
    holding: std::sync::Mutex<bool>,
    /// The rows held, by key, only while `holding`: this writer's own writes, not yet on the disk,
    /// so its reads answer them over the disk. Empty once the store writes to the disk: from then
    /// on every read is the disk's.
    rows: std::sync::Mutex<std::collections::BTreeMap<String, HeldRow>>,
    /// Set when the scratchpad was published while the store was cut off: a lift of the cut-off
    /// publishes what it holds then.
    owed: std::sync::Mutex<bool>,
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
    /// fabric's store root), written by the process born as `writer`, straight to the disk: the
    /// directory made on the blocking pool, as every read and write is, so no worker thread waits on
    /// the disk.
    pub async fn open(base: &Path, dir: &str, writer: &crate::model::IncarnationId) -> Result<Self, StorageError> {
        Self::opened(base, dir, writer, None).await
    }

    /// [`Self::open`], holding every write in `scratchpad` until it is published; named `store` in
    /// the publication. A scratchpad already published opens the store straight to the disk.
    pub async fn open_held(base: &Path, dir: &str, writer: &crate::model::IncarnationId, scratchpad: &Scratchpad, store: String) -> Result<Self, StorageError> {
        let held = Held { store, holding: std::sync::Mutex::new(true), rows: Default::default(), owed: std::sync::Mutex::new(false), gate: std::sync::OnceLock::new() };
        let records = Self::opened(base, dir, writer, Some(held)).await?;
        if !scratchpad.enrol(std::sync::Arc::new(records.clone())) {
            *records.held().expect("opened held").holding.lock().unwrap_or_else(|e| e.into_inner()) = false;
        }
        Ok(records)
    }

    async fn opened(base: &Path, dir: &str, writer: &crate::model::IncarnationId, held: Option<Held>) -> Result<Self, StorageError> {
        let dir = base.join(dir);
        let made = dir.clone();
        blocking_io(move || std::fs::create_dir_all(&made)).await.map_err(|e| StorageError::Io { file: dir.display().to_string(), reason: e.to_string() })?;
        Ok(Self(std::sync::Arc::new(Records { dir, write: std::sync::Arc::new(tokio::sync::Mutex::new(())), cut_off: std::sync::Mutex::new(None), writer: writer.to_string(), held })))
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

    /// The store's directory under the store root (its publication name), else its directory.
    pub fn store_name(&self) -> String {
        self.held().map(|h| h.store.clone()).unwrap_or_else(|| self.0.dir.display().to_string())
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
    pub fn tmp_path(&self, key: &str) -> PathBuf {
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
        if let Some(row) = self.held_row(key) {
            return row.bytes().map(|b| Self::decode(&path, b, format)).transpose();
        }
        let file = path.clone();
        match blocking_io(move || std::fs::read(&file)).await {
            Ok(b) => Self::decode(&path, &b, format).map(Some),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
        }
    }

    /// [`Self::read`] of a key one of several formats may hold (a lane's fact or its seal): the
    /// format the row was written in and its record, or `None` when none was ever written. A row in a
    /// format outside `formats` is refused by name, never read as no row.
    pub async fn read_tagged(&self, key: &str, formats: &[&str]) -> Result<Option<(String, serde_json::Value)>, StorageError> {
        let path = self.path(key);
        let bytes = match self.held_row(key) {
            Some(row) => match row.bytes() {
                Some(b) => b.to_vec(),
                None => return Ok(None),
            },
            None => {
                let file = path.clone();
                match blocking_io(move || std::fs::read(&file)).await {
                    Ok(b) => b,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(e) => return Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
                }
            }
        };
        let stored: Stored<serde_json::Value> = serde_json::from_slice(&bytes).map_err(|e| StorageError::Unrecognised { file: path.display().to_string(), reason: e.to_string() })?;
        if !formats.contains(&stored.format.as_str()) {
            return Err(StorageError::Unrecognised { file: path.display().to_string(), reason: format!("format {:?}, this build reads {formats:?} here", stored.format) });
        }
        Ok(Some((stored.format, stored.record)))
    }

    /// Every record in the store, in key order: while the store holds its writes, the rows it holds
    /// over the files on the disk (a row it holds removed hides the file); after, the disk's alone.
    /// The directory and its files are read on the blocking pool, as a point read is, so no worker
    /// thread waits on the disk.
    pub async fn list<T: DeserializeOwned + Send + 'static>(&self, format: &str) -> Result<Vec<T>, StorageError> {
        // The held rows are read before the disk: a row the publication moves to the disk meanwhile
        // (it leaves the held rows only once its file is written) is in one of the two.
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
            Self::read_listed(&dir, paths, held, &format)
        })
        .await
    }

    /// The records of the files a directory read `paths` named, under the rows the store holds. A file
    /// listed and then removed before it is read is a record that is gone: the list answers the
    /// records that remain.
    fn read_listed<T: DeserializeOwned>(dir: &Path, paths: Vec<PathBuf>, held: std::collections::BTreeMap<String, HeldRow>, format: &str) -> Result<Vec<T>, StorageError> {
        let mut records: std::collections::BTreeMap<String, T> = std::collections::BTreeMap::new();
        for p in paths {
            let key = p.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
            if held.contains_key(&key) {
                continue;
            }
            match std::fs::read(&p) {
                Ok(b) => {
                    records.insert(key, Self::decode(&p, &b, format)?);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(StorageError::Io { file: p.display().to_string(), reason: e.to_string() }),
            }
        }
        for (key, row) in held {
            if let Some(b) = row.bytes() {
                let p = dir.join(format!("{key}.json"));
                records.insert(key, Self::decode(&p, b, format)?);
            }
        }
        Ok(records.into_values().collect())
    }

    /// Store `record` under `key`, replacing what was there: held in the scratchpad while the store
    /// holds its writes; otherwise temp file, fsync, rename, directory fsync, on the blocking pool.
    pub async fn write<T: Serialize>(&self, key: &str, format: &str, record: &T) -> Result<(), StorageError> {
        self.write_gated(key, format, record, || Ok(())).await
    }

    /// [`Self::write`] whose `gate` is asked under the write lock, before the write is held or reaches
    /// the disk: a refusal is neither held nor written.
    pub async fn write_gated<T: Serialize>(&self, key: &str, format: &str, record: &T, gate: impl FnOnce() -> Result<(), StorageError>) -> Result<(), StorageError> {
        let path = self.path(key);
        let bytes = Self::encode(&path, format, record)?;
        let held = self.writer(&path).await?;
        gate()?;
        if self.hold(key, HeldRow::Put(bytes.clone())) {
            return Ok(());
        }
        let (dir, tmp) = (self.0.dir.clone(), self.tmp_path(key));
        blocking_io(move || {
            let _held = held;
            write_file(&dir, &tmp, &path, &bytes)
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
            let live = self.holds(key).await?;
            return Ok(!live && self.hold(key, HeldRow::Insert(bytes)));
        }
        let (dir, tmp) = (self.0.dir.clone(), self.tmp_path(key));
        blocking_io(move || {
            let _held = held;
            insert_file(&dir, &tmp, &path, &bytes)
        })
        .await
    }

    /// [`Self::write`] of a claim the store's adapter decided under its own lock against the row at
    /// `key` (R14 item 3: the sole writer's conditional write). While the store holds its writes the
    /// claim is held with `decide`, the adapter's own decision of `record` against a row standing at
    /// the key (`Ok(true)` write, `Ok(false)` the same row stands, `Err` refused, naming why), and the
    /// publication makes it again against the row then on the disk: a held claim is never written
    /// blindly over a row another writer published meanwhile.
    pub async fn write_claimed<T: Serialize + DeserializeOwned + 'static>(&self, key: &str, format: &str, record: &T, decide: impl Fn(Option<&T>) -> Result<bool, String> + Send + Sync + 'static) -> Result<(), StorageError> {
        let path = self.path(key);
        let bytes = Self::encode(&path, format, record)?;
        let held = self.writer(&path).await?;
        if self.hold(key, HeldRow::Claim { bytes: bytes.clone(), decide: Self::decision(&path, format, decide), insert: false }) {
            return Ok(());
        }
        let (dir, tmp) = (self.0.dir.clone(), self.tmp_path(key));
        blocking_io(move || {
            let _held = held;
            write_file(&dir, &tmp, &path, &bytes)
        })
        .await
    }

    /// [`Self::insert`] of a claim two writers can make at once (a key above a fabric's root): held,
    /// the claim keeps `decide` (as [`Self::write_claimed`]), and the publication inserts it by the
    /// filesystem's atomic link, deciding against the row it meets there when the key is live.
    pub async fn insert_claimed<T: Serialize + DeserializeOwned + 'static>(&self, key: &str, format: &str, record: &T, decide: impl Fn(Option<&T>) -> Result<bool, String> + Send + Sync + 'static) -> Result<bool, StorageError> {
        let path = self.path(key);
        let bytes = Self::encode(&path, format, record)?;
        let held = self.writer(&path).await?;
        if self.holding() {
            let live = self.holds(key).await?;
            return Ok(!live && self.hold(key, HeldRow::Claim { bytes, decide: Self::decision(&path, format, decide), insert: true }));
        }
        let (dir, tmp) = (self.0.dir.clone(), self.tmp_path(key));
        blocking_io(move || {
            let _held = held;
            insert_file(&dir, &tmp, &path, &bytes)
        })
        .await
    }

    /// [`Self::remove`] of a release the store's adapter decided under its own lock (the holder's
    /// row at `key` goes). While the store holds its writes the release is held with `holder`, which
    /// says whether a row standing at the key is still the holder's, and the publication removes the
    /// row on the disk only when it is: a held release never removes a row another writer published
    /// meanwhile.
    pub async fn remove_released<T: DeserializeOwned + 'static>(&self, key: &str, format: &str, holder: impl Fn(&T) -> bool + Send + Sync + 'static) -> Result<(), StorageError> {
        let path = self.path(key);
        let held = self.writer(&path).await?;
        if self.hold(key, HeldRow::Released(Self::decision(&path, format, move |standing: Option<&T>| Ok(standing.is_some_and(&holder))))) {
            return Ok(());
        }
        let dir = self.0.dir.clone();
        blocking_io(move || {
            let _held = held;
            remove_file(&dir, &path).map(|_| ())
        })
        .await
    }

    /// A read of the record standing on the disk at `key` of this store, for a held claim of another
    /// store whose decision at the publication follows this store's row (the publication runs it on
    /// the blocking pool): `None` when no row is there; a file this build does not recognise is
    /// refused by name, never read as no row.
    pub fn disk_read<T: DeserializeOwned>(&self, key: &str, format: &str) -> impl Fn() -> Result<Option<T>, StorageError> + Send + Sync + 'static {
        let (path, format) = (self.path(key), format.to_string());
        move || read_standing(&path)?.map(|b| Self::decode::<T>(&path, &b, &format)).transpose()
    }

    /// `decide`, over a typed record, as a decision over the bytes standing at the key: a standing
    /// file this build does not recognise is refused by name, never decided as no row.
    fn decision<T: DeserializeOwned + 'static>(path: &Path, format: &str, decide: impl Fn(Option<&T>) -> Result<bool, String> + Send + Sync + 'static) -> ClaimDecision {
        let (path, format) = (path.to_path_buf(), format.to_string());
        std::sync::Arc::new(move |standing: Option<&[u8]>| {
            let standing = standing.map(|b| Self::decode::<T>(&path, b, &format)).transpose()?;
            Ok(decide(standing.as_ref()))
        })
    }

    /// Whether a record under `key` exists, whatever it holds: the scratchpad's row, else a look at
    /// the disk on the blocking pool, as a point read is. A look that cannot be answered (the
    /// directory unreadable) is an error naming the file, never an answer of "not held".
    pub async fn holds(&self, key: &str) -> Result<bool, StorageError> {
        if let Some(row) = self.held_row(key) {
            return Ok(row.bytes().is_some());
        }
        let path = self.path(key);
        let file = path.clone();
        blocking_io(move || file.try_exists()).await.map_err(|e| StorageError::Io { file: path.display().to_string(), reason: e.to_string() })
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
    /// keeps its authority over its keys, and the rows it held through a publication made while it
    /// was cut off are written now.
    pub async fn lift_cut_off(&self) -> Result<(), StorageError> {
        *self.0.cut_off.lock().unwrap_or_else(|e| e.into_inner()) = None;
        let owed = self.held().is_some_and(|h| std::mem::take(&mut *h.owed.lock().unwrap_or_else(|e| e.into_inner())));
        if owed {
            self.publish_held().await?;
        }
        Ok(())
    }

    /// The seat transfer this store was cut off for committed: its writer's authority over the
    /// store's keys ended at the seat row (R14 item 4), so every row it still holds is dropped,
    /// never written and never read again. Returns how many rows were dropped.
    pub async fn drop_held(&self) -> usize {
        let Some(h) = self.held() else { return 0 };
        let _one = self.0.write.lock().await;
        let dropped = std::mem::take(&mut *h.rows.lock().unwrap_or_else(|e| e.into_inner())).len();
        *h.holding.lock().unwrap_or_else(|e| e.into_inner()) = false;
        *h.owed.lock().unwrap_or_else(|e| e.into_inner()) = false;
        dropped
    }

    /// Remove the record under `key`, if any: held in the scratchpad while the store holds its
    /// writes. Returns whether this call removed a row that stood: the filesystem's own unlink decides
    /// it (held: the rows the scratchpad holds over the disk, under the write lock), so of two
    /// removers of one row exactly one is answered `true`.
    pub async fn remove(&self, key: &str) -> Result<bool, StorageError> {
        let (path, dir) = (self.path(key), self.0.dir.clone());
        let held = self.writer(&path).await?;
        if self.holding() {
            let stood = self.holds(key).await?;
            self.hold(key, HeldRow::Removed);
            return Ok(stood);
        }
        blocking_io(move || {
            let _held = held;
            remove_file(&dir, &path)
        })
        .await
    }

    /// Write what this store holds to the disk and write to the disk from now on (the scratchpad's
    /// publication): under the write lock, so no write slips between. A held row leaves the held
    /// rows once its file is written; a write that fails ends the publication by name, the rows it
    /// had not written still held. A store whose gate says its writer no longer owns its rows drops
    /// them, named. A store cut off by a seat transfer that has neither committed nor been lifted
    /// goes on holding its rows, named: the commit drops them, a lift publishes them.
    async fn publish_held(&self) -> Result<StorePublished, StorageError> {
        let Some(h) = self.held() else { return Ok(StorePublished::default()) };
        let guard = self.0.write.clone().lock_owned().await;
        let mut done = StorePublished { store: h.store.clone(), ..Default::default() };
        if !*h.holding.lock().unwrap_or_else(|e| e.into_inner()) {
            return Ok(done);
        }
        let rows = h.rows.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(reason) = self.0.cut_off.lock().unwrap_or_else(|e| e.into_inner()).clone() {
            if !rows.is_empty() {
                *h.owed.lock().unwrap_or_else(|e| e.into_inner()) = true;
                done.held = rows.len();
                done.why_held = Some(format!("cut off: {reason}"));
                return Ok(done);
            }
        }
        if let Some(Err(why)) = h.gate.get().map(|gate| gate()) {
            h.rows.lock().unwrap_or_else(|e| e.into_inner()).clear();
            *h.holding.lock().unwrap_or_else(|e| e.into_inner()) = false;
            done.dropped = rows.len();
            done.why_dropped = Some(why);
            return Ok(done);
        }
        let this = self.clone();
        let (written, stands, refused) = blocking_io(move || {
            let _held = guard;
            let h = this.held().expect("a held store");
            let (mut written, mut stands, mut refused) = (0, 0, Vec::new());
            for (key, row) in rows {
                let (path, tmp) = (this.path(&key), this.tmp_path(&key));
                match row {
                    HeldRow::Put(b) => {
                        write_file(&this.0.dir, &tmp, &path, &b)?;
                        written += 1;
                    }
                    // A create-once row that is already live at its key stays what it is.
                    HeldRow::Insert(b) => {
                        insert_file(&this.0.dir, &tmp, &path, &b)?;
                        written += 1;
                    }
                    // A claim is decided again against the row standing at its key: written, the same
                    // row standing, or refused, named, and nothing written.
                    HeldRow::Claim { bytes, decide, insert } => match publish_claim(&this.0.dir, &tmp, &path, &bytes, &decide, insert)? {
                        ClaimPublished::Written => written += 1,
                        ClaimPublished::Stands => stands += 1,
                        ClaimPublished::Refused(why) => refused.push(format!("{key}: {why}")),
                    },
                    HeldRow::Removed => {
                        remove_file(&this.0.dir, &path)?;
                        written += 1;
                    }
                    // A release removes the row standing at its key only while it is the holder's.
                    HeldRow::Released(holder) => {
                        if let Ok(true) = holder(read_standing(&path)?.as_deref())? {
                            remove_file(&this.0.dir, &path)?;
                            written += 1;
                        }
                    }
                }
                h.rows.lock().unwrap_or_else(|e| e.into_inner()).remove(&key);
            }
            *h.holding.lock().unwrap_or_else(|e| e.into_inner()) = false;
            Ok::<(usize, usize, Vec<String>), StorageError>((written, stands, refused))
        })
        .await?;
        done.written = written;
        done.stands = stands;
        done.refused = refused;
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
    /// The rows dropped, never written: their writer no longer owns their keys.
    pub dropped: usize,
    /// Why the rows were dropped.
    pub why_dropped: Option<String>,
    /// The rows still held: a seat transfer cut the store off and has neither committed nor been
    /// lifted.
    pub held: usize,
    /// Why the rows are still held.
    pub why_held: Option<String>,
    /// The held claims whose row the publication found standing at their key (the same row, put by
    /// another writer of the same identity): nothing written for them.
    pub stands: usize,
    /// The held claims the publication refused against the row standing at their key, each
    /// `<key>: <why>` naming the holder: none of them was written.
    pub refused: Vec<String>,
}

/// One store a scratchpad holds rows for until its publication.
#[async_trait::async_trait]
pub trait HeldStore: Send + Sync {
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

    /// The rows dropped, never written.
    pub fn dropped(&self) -> usize {
        self.stores.iter().map(|s| s.dropped).sum()
    }

    /// The rows still held by a store cut off by a seat transfer not yet decided.
    pub fn held(&self) -> usize {
        self.stores.iter().map(|s| s.held).sum()
    }

    /// The held claims whose row stood at their key already, nothing written for them.
    pub fn stands(&self) -> usize {
        self.stores.iter().map(|s| s.stands).sum()
    }

    /// The held claims refused against the row standing at their key, never written.
    pub fn refused(&self) -> usize {
        self.stores.iter().map(|s| s.refused.len()).sum()
    }

    /// `<store>=<written>` for each store that wrote rows, `<store> dropped <n>: <why>` for each that
    /// dropped them, `<store> held <n>: <why>` for each still holding them, `<store> stands <n>` for
    /// each whose held claims found their row standing and `<store> refused <n>: <key>: <why>; ...` for
    /// each whose held claims were refused, comma-separated.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        for s in &self.stores {
            if s.written > 0 {
                parts.push(format!("{}={}", s.store, s.written));
            }
            if s.dropped > 0 {
                parts.push(format!("{} dropped {}: {}", s.store, s.dropped, s.why_dropped.as_deref().unwrap_or("")));
            }
            if s.held > 0 {
                parts.push(format!("{} held {}: {}", s.store, s.held, s.why_held.as_deref().unwrap_or("")));
            }
            if s.stands > 0 {
                parts.push(format!("{} stands {}", s.store, s.stands));
            }
            if !s.refused.is_empty() {
                parts.push(format!("{} refused {}: {}", s.store, s.refused.len(), s.refused.join("; ")));
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
    pub fn enrol(&self, store: std::sync::Arc<dyn HeldStore>) -> bool {
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
    /// now on; a store opened from now on writes there from its open. A store whose writer no longer
    /// owns its rows drops them, named; one cut off by a seat transfer not yet decided goes on
    /// holding them, named, until the transfer commits (dropped) or is lifted (written). A write
    /// that fails ends the publication with its error, naming the file: what it had not written is
    /// still held, and a later publication writes it.
    pub async fn publish(&self) -> Result<Publication, StorageError> {
        let stores = {
            let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
            s.published = true;
            s.stores.clone()
        };
        let mut out = Publication::default();
        for store in stores {
            let done = store.publish().await?;
            if done.written > 0 || done.dropped > 0 || done.held > 0 || done.stands > 0 || !done.refused.is_empty() {
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

/// Write `bytes` to `tmp`, fsync, rename to `path`, and fsync `dir`.
pub fn write_file(dir: &Path, tmp: &Path, path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
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

/// Remove `path` from `dir`, if it is there; a removal that must survive a crash syncs its directory.
/// `true`: this unlink removed the file; `false`: no file was there.
fn remove_file(dir: &Path, path: &Path) -> Result<bool, StorageError> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_dir(dir, path).map(|()| true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
    }
}

/// The bytes standing at `path`, `None` when no row is there.
fn read_standing(path: &Path) -> Result<Option<Vec<u8>>, StorageError> {
    match std::fs::read(path) {
        Ok(b) => Ok(Some(b)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
    }
}

/// Publish one held claim: decided again against the row standing at `path` (after an atomic link
/// insert, for an `insert` claim), written only when the decision says so.
fn publish_claim(dir: &Path, tmp: &Path, path: &Path, bytes: &[u8], decide: &ClaimDecision, insert: bool) -> Result<ClaimPublished, StorageError> {
    if insert && insert_file(dir, tmp, path, bytes)? {
        return Ok(ClaimPublished::Written);
    }
    match decide(read_standing(path)?.as_deref())? {
        Ok(true) => write_file(dir, tmp, path, bytes).map(|()| ClaimPublished::Written),
        Ok(false) => Ok(ClaimPublished::Stands),
        Err(why) => Ok(ClaimPublished::Refused(why)),
    }
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

/// Run `f`, a blocking disk call (a write that ends in an fsync, a read, a directory made or looked
/// at), on tokio's blocking pool. No
/// worker thread waits on the disk, and no lock a worker could block on is held across it: a
/// journal commit that takes seconds delays only the caller awaiting it. Inside [`io_time`] the call
/// adds what it took to that work's [`IoTime`]: the wait from its issue until a blocking thread began
/// it, and its run on that thread, including while the call is still queued or running if the caller
/// awaiting it is cancelled.
pub async fn blocking_io<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let slot = std::sync::Arc::new(std::sync::Mutex::new(CallTiming {
        issued: std::time::Instant::now(),
        began: None,
        finished: None,
    }));
    let _ = IO_TIME.try_with(|t| t.register(slot.clone()));
    let slot_run = slot.clone();
    let run = tokio::task::spawn_blocking(move || {
        slot_run.lock().unwrap().began = Some(std::time::Instant::now());
        let v = f();
        slot_run.lock().unwrap().finished = Some(std::time::Instant::now());
        v
    });
    match run.await {
        Ok(v) => v,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(e) => panic!("a blocking disk call was cancelled: {e}"),
    }
}

tokio::task_local! {
    /// The blocking time of the work [`io_time`] runs.
    static IO_TIME: IoTime;
}

#[derive(Debug)]
struct CallTiming {
    issued: std::time::Instant,
    began: Option<std::time::Instant>,
    finished: Option<std::time::Instant>,
}

/// What the [`blocking_io`] calls of one piece of work took: how many there were, how long they
/// waited for a blocking thread, and how long they ran on one. Shared with [`io_time_with`] so a
/// step cancelled while a blocking call is still queued or running still records its count and time.
#[derive(Debug, Default, Clone)]
pub struct IoTime {
    calls: std::sync::Arc<std::sync::Mutex<Vec<std::sync::Arc<std::sync::Mutex<CallTiming>>>>>,
}

impl IoTime {
    fn register(&self, slot: std::sync::Arc<std::sync::Mutex<CallTiming>>) {
        self.calls.lock().unwrap().push(slot);
    }

    fn totals(&self) -> (u64, std::time::Duration, std::time::Duration) {
        let now = std::time::Instant::now();
        let list = self.calls.lock().unwrap();
        let mut queued = std::time::Duration::ZERO;
        let mut ran = std::time::Duration::ZERO;
        for slot in list.iter() {
            let s = slot.lock().unwrap();
            match (s.began, s.finished) {
                (None, _) => queued += now.saturating_duration_since(s.issued),
                (Some(b), None) => {
                    queued += b.saturating_duration_since(s.issued);
                    ran += now.saturating_duration_since(b);
                }
                (Some(b), Some(f)) => {
                    queued += b.saturating_duration_since(s.issued);
                    ran += f.saturating_duration_since(b);
                }
            }
        }
        (list.len() as u64, queued, ran)
    }

    /// How many blocking calls the work made (completed or in flight).
    pub fn calls(&self) -> u64 {
        self.calls.lock().unwrap().len() as u64
    }

    /// How long they waited, from issue, for a blocking thread to begin them, in milliseconds.
    pub fn queued_ms(&self) -> u64 {
        self.totals().1.as_millis() as u64
    }

    /// How long they ran on a blocking thread, in milliseconds.
    pub fn ran_ms(&self) -> u64 {
        self.totals().2.as_millis() as u64
    }
}

/// Run `work`, recording its [`blocking_io`] calls into `io`, awaited on this task.
pub async fn io_time_with<F: std::future::Future>(io: IoTime, work: F) -> F::Output {
    IO_TIME.scope(io, work).await
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

/// A future a test cell runs, boxed so a cell can hand one over.
#[cfg(test)]
pub(crate) type CellFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

/// Make `fifo` (its directory too) a FIFO a read of which waits until a writer opens it, and run
/// `op` on a single-thread runtime. The writer is a thread that, once a reader has the FIFO open,
/// waits for word from the runtime that it still runs (up to 5 s), then writes `bytes`. Answers
/// whether that word came while the read waited, and what `op` answered.
#[cfg(test)]
pub(crate) fn runtime_runs_while_a_fifo_is_read<T: Send + 'static>(fifo: PathBuf, bytes: Vec<u8>, op: CellFuture<T>) -> (bool, T) {
    std::fs::create_dir_all(fifo.parent().unwrap()).unwrap();
    let c = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0, "mkfifo {}", fifo.display());
    let (word_tx, word_rx) = std::sync::mpsc::channel::<()>();
    let writer = std::thread::spawn(move || {
        use std::io::Write as _;
        // Blocks until the read has the FIFO open: the read is waiting from here on.
        let mut f = std::fs::OpenOptions::new().write(true).open(&fifo).unwrap();
        let heard = word_rx.recv_timeout(std::time::Duration::from_secs(5)).is_ok();
        f.write_all(&bytes).unwrap();
        heard
    });
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let out = rt.block_on(async move {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        // The read and its start signal are one poll: this task resumes only once that poll returns.
        let read = tokio::spawn(async move {
            let _ = started_tx.send(());
            op.await
        });
        started_rx.await.unwrap();
        let _ = word_tx.send(());
        read.await.unwrap()
    });
    (writer.join().unwrap(), out)
}

/// Run `setup`, then `op` on its answer, on a single-thread runtime whose blocking pool has one
/// thread, held busy while `op` first runs. A disk call `op` makes on the blocking pool queues
/// behind the held thread, so `op` is still waiting after the runtime has turned over; one it makes
/// on the runtime's thread has finished by then. Answers whether `op` waited, and what it answered
/// once the pool's thread was let go. (A FIFO cannot hold a `stat` or a `mkdir`: this is the cell
/// for a disk call that never opens a file.)
#[cfg(test)]
pub(crate) fn op_waits_on_the_blocking_pool<S: Send + 'static, T: Send + 'static>(setup: CellFuture<S>, op: impl FnOnce(S) -> CellFuture<T> + Send + 'static) -> (bool, T) {
    let rt = tokio::runtime::Builder::new_current_thread().max_blocking_threads(1).enable_all().build().unwrap();
    rt.block_on(async move {
        let state = setup.await;
        let (held_tx, held_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = tokio::task::spawn_blocking(move || {
            let _ = held_tx.send(());
            let _ = release_rx.recv();
        });
        held_rx.await.unwrap();
        let task = tokio::spawn(op(state));
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let waited = !task.is_finished();
        release_tx.send(()).unwrap();
        holder.await.unwrap();
        (waited, task.await.unwrap())
    })
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
            let r = FileRecords::open(&d, "things", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap();
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
            let r = std::sync::Arc::new(FileRecords::open(&d, "things", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap());
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
        let r = FileRecords::open(&d, "things", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap();
        r.write("gone", "thing/1", &"x".to_string()).await.unwrap();
        let dir = d.join("things");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o300)).unwrap();
        let named = |e: &Result<(), StorageError>| matches!(e, Err(StorageError::Io { reason, .. }) if reason.contains("directory") && reason.contains("could not be synced"));
        let wrote = r.write("a", "thing/1", &"one".to_string()).await;
        let inserted = r.insert("b", "thing/1", &"two".to_string()).await.map(|_| ());
        let removed = r.remove("gone").await.map(|_| ());
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
        let w = crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("held")).unwrap();
        // Another writer's row, already on the disk.
        FileRecords::open(&d, "things", &w).await.unwrap().write("theirs", "thing/1", &"t".to_string()).await.unwrap();
        let pad = Scratchpad::new();
        let r = FileRecords::open_held(&d, "things", &w, &pad, "things".into()).await.unwrap();
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
        let later = FileRecords::open_held(&d, "later", &w, &pad, "later".into()).await.unwrap();
        assert!(!later.holding(), "a store opened after the publication writes to the disk from its open");
        assert_eq!(pad.publish().await.unwrap().written(), 0, "a second publication finds nothing held");
    }

    /// CONTRACT (`FileRecords::remove`: "of two removers of one row exactly one is answered `true`"):
    /// a removal answers whether this call removed a row that stood, on the disk (the unlink decides)
    /// and held (the scratchpad's rows over the disk decide, under the write lock): of two removals of
    /// one standing row one is `true` and one `false`, and a key never written is `false`. Must NOT
    /// happen: both removals answered `true` (R24's relanding gate: one os-stopped row cleared twice).
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn two_removals_of_one_row_answer_true_once_on_the_disk_and_held() {
        let d = tempdir("records-remove-once");
        let w = crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("remove")).unwrap();
        let once = |a: Result<bool, StorageError>, b: Result<bool, StorageError>| [a.unwrap(), b.unwrap()].iter().filter(|r| **r).count();
        let disk = FileRecords::open(&d, "disk", &w).await.unwrap();
        disk.write("r", "thing/1", &"x".to_string()).await.unwrap();
        let (a, b) = tokio::join!(disk.remove("r"), disk.remove("r"));
        assert_eq!(once(a, b), 1, "two removals of one row on the disk: one removed it");
        assert!(!disk.remove("never").await.unwrap(), "a removal of a key never written removed nothing");
        FileRecords::open(&d, "held", &w).await.unwrap().write("theirs", "thing/1", &"t".to_string()).await.unwrap();
        let pad = Scratchpad::new();
        let held = FileRecords::open_held(&d, "held", &w, &pad, "held".into()).await.unwrap();
        held.write("mine", "thing/1", &"m".to_string()).await.unwrap();
        for key in ["theirs", "mine"] {
            let (a, b) = tokio::join!(held.remove(key), held.remove(key));
            assert_eq!(once(a, b), 1, "two held removals of {key}: one removed it");
        }
        assert!(!held.remove("never").await.unwrap(), "a held removal of a key never written removed nothing");
    }

    /// CONTRACT (this module: "a file this build does not recognise ... is refused by name, never read
    /// as a value"; CLAUDE.md: a reader that does not recognise a value refuses with a named reason):
    /// a held claim, and a held release, whose key holds at the publication a file this build does not
    /// recognise end the publication by name, naming the file: neither is decided as if no row stood
    /// there, and the file stands. Must NOT happen: an unrecognised row overwritten by a claim, or a
    /// release falling through to "not the holder's".
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_held_claim_or_release_over_an_unrecognised_row_ends_the_publication_by_name() {
        let d = tempdir("records-unrecognised-claim");
        let w = crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("held")).unwrap();
        let foreign = br#"{"format":"thing/9","record":"theirs"}"#;
        for (key, release) in [("claimed", false), ("released", true)] {
            let pad = Scratchpad::new();
            let r = FileRecords::open_held(&d, "things", &w, &pad, "things".into()).await.unwrap();
            if release {
                r.remove_released::<String>(key, "thing/1", |_| true).await.unwrap();
            } else {
                r.write_claimed(key, "thing/1", &"mine".to_string(), |standing: Option<&String>| Ok(standing.is_none())).await.unwrap();
            }
            let file = d.join("things").join(format!("{key}.json"));
            std::fs::write(&file, foreign).unwrap();
            match pad.publish().await {
                Err(StorageError::Unrecognised { file: named, reason }) => assert!(named.ends_with(&format!("{key}.json")) && reason.contains("thing/9"), "{key}: {named}: {reason}"),
                other => panic!("{key}: the publication over an unrecognised row answered {other:?}"),
            }
            assert_eq!(std::fs::read(&file).unwrap(), foreign, "{key}: the unrecognised row stands");
        }
    }

    /// CONTRACT (R14 items 2 and 4): at its scratchpad's publication a store whose gate says its
    /// writer no longer owns its rows drops them, named, and writes none; a store cut off by a seat
    /// transfer that has neither committed nor been lifted goes on holding its rows, named, still read
    /// as its writer's own. A lift writes what it held; a commit drops it, named by the count, and
    /// nothing dropped is read again. What must NOT happen: a row whose writer's authority ended kept
    /// to be read in place of the disk, or a row lost while its writer still owns its key.
    #[tokio::test]
    async fn a_moved_writer_drops_its_held_rows_and_a_cut_off_store_holds_them_until_the_transfer_decides() {
        let d = tempdir("records-decided");
        let w = crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("held")).unwrap();
        let pad = Scratchpad::new();
        let gated = FileRecords::open_held(&d, "gated", &w, &pad, "gated".into()).await.unwrap();
        let lifted = FileRecords::open_held(&d, "lifted", &w, &pad, "lifted".into()).await.unwrap();
        let committed = FileRecords::open_held(&d, "committed", &w, &pad, "committed".into()).await.unwrap();
        gated.set_publish_gate(std::sync::Arc::new(|| Err("fabric-primary seat: the holder is mesh2.admin.1".to_string())));
        gated.write("p", "thing/1", &1u32).await.unwrap();
        lifted.write("a", "thing/1", &2u32).await.unwrap();
        committed.write("b", "thing/1", &3u32).await.unwrap();
        lifted.cut_off("take-seats:a".into());
        committed.cut_off("take-seats:b".into());
        let published = pad.publish().await.unwrap();
        assert_eq!((published.written(), published.dropped(), published.held()), (0, 1, 2), "{published:?}");
        assert_eq!(published.summary(), "gated dropped 1: fabric-primary seat: the holder is mesh2.admin.1,lifted held 1: cut off: take-seats:a,committed held 1: cut off: take-seats:b");
        for store in ["gated", "lifted", "committed"] {
            assert!(files(&d.join(store)).is_empty(), "{store}: nothing reached the disk");
        }
        assert!(!gated.holding(), "a store whose rows were dropped writes to the disk from now on");
        assert_eq!(gated.read::<u32>("p", "thing/1").await.unwrap(), None, "a dropped row is never read");
        assert!(lifted.holding() && committed.holding(), "a store cut off by an undecided transfer goes on holding");
        assert_eq!(lifted.read::<u32>("a", "thing/1").await.unwrap(), Some(2), "a row held through an undecided cut-off is still its writer's");
        lifted.lift_cut_off().await.unwrap();
        assert_eq!(files(&d.join("lifted")), vec!["a.json"], "a lifted cut-off writes the row its writer held");
        assert!(!lifted.holding());
        assert_eq!(committed.drop_held().await, 1, "the commit drops the row the store held");
        assert!(files(&d.join("committed")).is_empty() && !committed.holding());
        assert_eq!(committed.read::<u32>("b", "thing/1").await.unwrap(), None, "a row dropped at the commit is never read");
        let refused = committed.write("c", "thing/1", &4u32).await;
        assert!(matches!(&refused, Err(StorageError::CutOff { reason, .. }) if reason == "take-seats:b"), "a write after the commit is refused by the cut-off: {refused:?}");
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
        let other = FileRecords::open_held(&d, "fabric", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("mesh2-admin-1")).unwrap(), &pad, "fabric".into()).await.unwrap();
        other.set_publish_gate(std::sync::Arc::new(|| Err("fabric-primary seat: held by mesh1.admin.1".to_string())));
        other.write("pointer", "thing/1", &"3qcfk1kq47m9".to_string()).await.unwrap();
        pad.publish().await.unwrap();
        assert!(!other.holding(), "the publication ended the store's holding");
        // The key's one writer puts its row on the disk after the publication.
        FileRecords::open(&d, "fabric", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("mesh1-admin-1")).unwrap()).await.unwrap().write("pointer", "thing/1", &"h3t6eck82y1w".to_string()).await.unwrap();
        assert_eq!(other.read::<String>("pointer", "thing/1").await.unwrap().as_deref(), Some("h3t6eck82y1w"), "a row held at the publication was read in place of the row the key's writer put on the disk");
        assert_eq!(other.list::<String>("thing/1").await.unwrap(), vec!["h3t6eck82y1w"], "a row held at the publication hid the disk's row from a list");
        assert!(other.holds("pointer").await.unwrap(), "the disk holds the key");
    }

    /// CONTRACT (R14 item 5): every writer on the shared root names its temp file by the key and its
    /// own incarnation, `.<key>.<incarnation>.json.tmp`: two writers' temp files never share a name.
    /// The cell leaves a write's temp file in place (its rename target is a directory) and reads its name.
    #[tokio::test]
    async fn a_temp_file_is_named_by_its_key_and_its_writers_incarnation() {
        let d = tempdir("record-store-tmp");
        let writer = crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("inc-7")).unwrap();
        let r = FileRecords::open(&d, "things", &writer).await.unwrap();
        std::fs::create_dir_all(d.join("things").join("k.json").join("blocker")).unwrap();
        assert!(r.write("k", "thing/1", &1u32).await.is_err(), "a rename onto a non-empty directory fails");
        assert!(d.join("things").join(format!(".k.{writer}.json.tmp")).exists(), "{:?}", std::fs::read_dir(d.join("things")).unwrap().map(|e| e.unwrap().file_name()).collect::<Vec<_>>());
        let other = FileRecords::open(&d, "things", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("inc-8")).unwrap()).await.unwrap();
        assert_ne!(r.tmp_path("k"), other.tmp_path("k"));
    }

    #[tokio::test]
    async fn a_record_survives_reopening_and_an_unknown_format_is_refused_by_name() {
        let d = tempdir("records");
        let r = FileRecords::open(&d, "things", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap();
        assert_eq!(r.read::<String>("a", "thing/1").await.unwrap(), None);
        r.write("a", "thing/1", &"one".to_string()).await.unwrap();
        r.write("b", "thing/1", &"two".to_string()).await.unwrap();
        let again = FileRecords::open(&d, "things", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap();
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
    /// the list answers the records that remain and never fails on the one that vanished. The race is
    /// made exact: the directory read named a file that is gone by the time its record is read.
    #[tokio::test]
    async fn a_list_racing_a_removal_answers_the_records_that_remain() {
        let d = tempdir("records-race");
        let r = FileRecords::open(&d, "things", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap();
        r.write("k001", "thing/1", &1u32).await.unwrap();
        r.write("k003", "thing/1", &3u32).await.unwrap();
        let dir = d.join("things");
        let listed = vec![dir.join("k001.json"), dir.join("k002.json"), dir.join("k003.json")];
        assert!(!listed[1].exists(), "k002 was listed and is gone before its record is read");
        let records = FileRecords::read_listed::<u32>(&dir, listed, std::collections::BTreeMap::new(), "thing/1");
        assert_eq!(records.unwrap(), [1, 3], "the list answers the records that remain");
    }

    /// CONTRACT: opening a store (its directory made on the disk) never waits on the disk on a
    /// runtime worker: it runs on the blocking pool, as every read and write of the store does
    /// (`blocking_io`). Every node-admin store is opened inside the admin's boot, on the runtime.
    #[test]
    fn opening_a_store_waits_on_the_disk_off_the_runtime_thread() {
        let d = tempdir("records-open");
        let (waited, opened) = op_waits_on_the_blocking_pool(Box::pin(async {}), move |()| Box::pin(async move { FileRecords::open(&d, "things", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.map(|_| ()) }));
        assert!(waited, "the open's mkdir ran on the blocking pool, not the runtime's thread");
        assert_eq!(opened, Ok(()));
    }

    /// CONTRACT: a held insert's look at whether its key is live (the store holding its writes before
    /// the fabric's first state-commit, the key absent from the rows it holds, so the disk answers)
    /// never waits on the disk on a runtime worker: it runs on the blocking pool, as `holds` and a
    /// point read do. Every node-admin store on the store root is held from the admin's boot.
    #[test]
    fn a_held_inserts_look_at_the_disk_waits_off_the_runtime_thread() {
        let d = tempdir("records-held-insert");
        let setup = Box::pin(async move {
            let pad = Scratchpad::new();
            let r = FileRecords::open_held(&d, "things", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &pad, "things".into()).await.unwrap();
            (pad, r)
        });
        let (waited, inserted) = op_waits_on_the_blocking_pool(setup, |(_pad, r)| Box::pin(async move { r.insert("a", "thing/1", &"one".to_string()).await }));
        assert!(waited, "the held insert's look at the disk ran on the blocking pool, not the runtime's thread");
        assert_eq!(inserted, Ok(true));
    }

    /// CONTRACT: `holds`, a look at whether a key's file is on the disk, never waits on the disk on
    /// a runtime worker: it runs on the blocking pool, as a point read does. The fabric store's
    /// open asks it (the retired whole-record file) inside the admin's boot.
    #[test]
    fn a_look_at_whether_a_key_is_held_waits_on_the_disk_off_the_runtime_thread() {
        let d = tempdir("records-holds");
        let setup = Box::pin(async move {
            let r = FileRecords::open(&d, "things", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap();
            r.write("a", "thing/1", &"one".to_string()).await.unwrap();
            r
        });
        let (waited, held) = op_waits_on_the_blocking_pool(setup, |r| Box::pin(async move { (r.holds("a").await, r.holds("b").await) }));
        assert!(waited, "the look ran on the blocking pool, not the runtime's thread");
        assert_eq!(held, (Ok(true), Ok(false)));
    }
}
