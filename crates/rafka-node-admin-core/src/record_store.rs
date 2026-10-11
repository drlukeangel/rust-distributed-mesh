//! One record per file in an admin's data dir: the durable implementation every node-admin
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
            Self::Unrecognised { file, reason } => write!(f, "{file} is not a record this build recognises: {reason}"),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Stored<T> {
    format: String,
    record: T,
}

/// The records of one store under `<data dir>/<dir>/`, one `<key>.json` each.
#[derive(Debug)]
pub struct FileRecords {
    dir: PathBuf,
    /// Serializes this store's writers. An async lock: a writer waiting on another's fsync yields
    /// its worker instead of blocking it. Each writer moves its guard into its disk job, so the
    /// lock is held until the disk operation finishes whatever happens to the caller: taking it
    /// once waits for every write this store issued (R14 item 4).
    write: std::sync::Arc<tokio::sync::Mutex<()>>,
    /// Set when this store's writer gave up its authority (a seat transfer's cut-off): every write
    /// granted the lock from then on is refused by name, a write queued before it included.
    cut_off: std::sync::Mutex<Option<String>>,
}

impl FileRecords {
    /// Open (creating) the directory `dir` under the admin's data directory.
    pub fn open(own_data_dir: &Path, dir: &str) -> Result<Self, StorageError> {
        let dir = own_data_dir.join(dir);
        std::fs::create_dir_all(&dir).map_err(|e| StorageError::Io { file: dir.display().to_string(), reason: e.to_string() })?;
        Ok(Self { dir, write: std::sync::Arc::new(tokio::sync::Mutex::new(())), cut_off: std::sync::Mutex::new(None) })
    }

    fn path(&self, key: &str) -> PathBuf {
        self.dir.join(format!("{key}.json"))
    }

    fn decode<T: DeserializeOwned>(path: &Path, bytes: &[u8], format: &str) -> Result<T, StorageError> {
        let stored: Stored<T> =
            serde_json::from_slice(bytes).map_err(|e| StorageError::Unrecognised { file: path.display().to_string(), reason: e.to_string() })?;
        if stored.format != format {
            return Err(StorageError::Unrecognised { file: path.display().to_string(), reason: format!("format {:?}, this build reads {format:?}", stored.format) });
        }
        Ok(stored.record)
    }

    /// The record stored under `key`, or `None` when none was ever written: a point read of its
    /// file on the blocking pool, as every write is, so no worker thread waits on the disk.
    pub async fn read<T: DeserializeOwned>(&self, key: &str, format: &str) -> Result<Option<T>, StorageError> {
        let path = self.path(key);
        let file = path.clone();
        match blocking_io(move || std::fs::read(&file)).await {
            Ok(b) => Self::decode(&path, &b, format).map(Some),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
        }
    }

    /// Every record in the store, in key order: the directory and its files read on the blocking
    /// pool, as a point read is, so no worker thread waits on the disk.
    pub async fn list<T: DeserializeOwned + Send + 'static>(&self, format: &str) -> Result<Vec<T>, StorageError> {
        let (dir, format) = (self.dir.clone(), format.to_string());
        blocking_io(move || {
            let io = |e: std::io::Error| StorageError::Io { file: dir.display().to_string(), reason: e.to_string() };
            let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
                .map_err(io)?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|x| x == "json"))
                .collect();
            paths.sort();
            // A file listed and then removed before it is read is a record that is gone: the list
            // answers the records that remain.
            let mut records = Vec::with_capacity(paths.len());
            for p in paths {
                match std::fs::read(&p) {
                    Ok(b) => records.push(Self::decode(&p, &b, &format)?),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(StorageError::Io { file: p.display().to_string(), reason: e.to_string() }),
                }
            }
            Ok(records)
        })
        .await
    }

    /// Store `record` under `key`, replacing what was there: temp file, fsync, rename, directory
    /// fsync, on the blocking pool.
    pub async fn write<T: Serialize>(&self, key: &str, format: &str, record: &T) -> Result<(), StorageError> {
        let path = self.path(key);
        let bytes = serde_json::to_vec(&Stored { format: format.to_string(), record }).map_err(|e| StorageError::Io { file: path.display().to_string(), reason: e.to_string() })?;
        let (dir, tmp) = (self.dir.clone(), self.dir.join(format!(".{key}.json.tmp")));
        let held = self.writer(&path).await?;
        blocking_io(move || {
            let _held = held;
            write_file(&dir, &tmp, &path, &bytes)
        })
        .await
    }

    /// Store `record` under `key` only if no record is there: the claim is the filesystem's own
    /// atomic link of the finished temp file to the key, which fails when the key is live. Returns
    /// whether this call created it.
    pub async fn insert<T: Serialize>(&self, key: &str, format: &str, record: &T) -> Result<bool, StorageError> {
        let path = self.path(key);
        let bytes = serde_json::to_vec(&Stored { format: format.to_string(), record }).map_err(|e| StorageError::Io { file: path.display().to_string(), reason: e.to_string() })?;
        let (dir, tmp) = (self.dir.clone(), self.dir.join(format!(".{key}.json.tmp")));
        let held = self.writer(&path).await?;
        blocking_io(move || {
            let _held = held;
            insert_file(&dir, &tmp, &path, &bytes)
        })
        .await
    }

    /// Whether a file under `key` exists, whatever it holds.
    pub fn holds(&self, key: &str) -> bool {
        self.path(key).exists()
    }

    /// The write lock, owned so the caller moves it into its disk job, and the cut-off checked
    /// once it is granted: a write queued before the cut-off and granted the lock after it is
    /// refused by name.
    async fn writer(&self, file: &Path) -> Result<tokio::sync::OwnedMutexGuard<()>, StorageError> {
        let held = self.write.clone().lock_owned().await;
        if let Some(reason) = self.cut_off.lock().unwrap_or_else(|e| e.into_inner()).clone() {
            return Err(StorageError::CutOff { file: file.display().to_string(), reason });
        }
        Ok(held)
    }

    /// Wait for every write this store issued: the write lock taken once and released. Each
    /// write holds the lock until its disk job finishes, so this returns only once they have.
    pub async fn drain(&self) {
        drop(self.write.lock().await);
    }

    /// Refuse every write granted the lock from now on, naming `reason`.
    pub fn cut_off(&self, reason: String) {
        *self.cut_off.lock().unwrap_or_else(|e| e.into_inner()) = Some(reason);
    }

    /// Lift the cut-off: the transfer it was set for committed nothing, so this store's writer
    /// keeps its authority.
    pub fn lift_cut_off(&self) {
        *self.cut_off.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// Remove the record under `key`, if any.
    pub async fn remove(&self, key: &str) -> Result<(), StorageError> {
        let (path, dir) = (self.path(key), self.dir.clone());
        let held = self.writer(&path).await?;
        blocking_io(move || {
            let _held = held;
            match std::fs::remove_file(&path) {
                // A removal that must survive a crash syncs its directory.
                Ok(()) => sync_dir(&dir, &path),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
            }
        })
        .await
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
            let r = FileRecords::open(&d, "things").unwrap();
            let release = park_the_blocking_pool();
            assert!(!cancel_after_issue(r.write("a", "thing/1", &"one".to_string())).await, "the write finished while the blocking pool was parked");
            assert!(r.write.try_lock().is_err(), "the cancelled caller released the store's write lock while its disk job was still issued");
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
            let r = std::sync::Arc::new(FileRecords::open(&d, "things").unwrap());
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
        let r = FileRecords::open(&d, "things").unwrap();
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
        let r = FileRecords::open(&d, "things").unwrap();
        assert_eq!(r.read::<String>("a", "thing/1").await.unwrap(), None);
        r.write("a", "thing/1", &"one".to_string()).await.unwrap();
        r.write("b", "thing/1", &"two".to_string()).await.unwrap();
        let again = FileRecords::open(&d, "things").unwrap();
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
        let r = std::sync::Arc::new(FileRecords::open(&d, "things").unwrap());
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
