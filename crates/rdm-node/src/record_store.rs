//! One record per file in a store directory (the fabric's store root, or a node's data dir): the durable implementation every node-admin
//! storage boundary (`fabric.storage`, `mesh.storage`, `nodes.storage`) is built on.
//!
//! Each write is a temp file, fsync and atomic rename, then a directory fsync. Each record is
//! stored with its format tag; a file this build does not recognise (another format, or not a
//! record at all) is refused by name, never read as a value.

use crate::blocking::blocking_io;
use crate::durable_file::{sync_dir, write_file};
use crate::storage_error::StorageError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize)]
struct Stored<T> {
    format: String,
    record: T,
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
    /// The incarnation of the process writing through this store: its temp files are named by it,
    /// so two writers' temp files never share a name (R14 item 5).
    writer: String,
}

impl std::fmt::Debug for Records {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileRecords").field("dir", &self.dir).field("writer", &self.writer).finish()
    }
}

impl FileRecords {
    /// Open (creating) the directory `dir` under `base` (the node's data dir, or a directory of the
    /// fabric's store root), written by the process born as `writer`, straight to the disk: the
    /// directory made on the blocking pool, as every read and write is, so no worker thread waits on
    /// the disk.
    pub async fn open(base: &Path, dir: &str, writer: &crate::model::IncarnationId) -> Result<Self, StorageError> {
        let dir = base.join(dir);
        let made = dir.clone();
        blocking_io(move || std::fs::create_dir_all(&made)).await.map_err(|e| StorageError::Io { file: dir.display().to_string(), reason: e.to_string() })?;
        Ok(Self(std::sync::Arc::new(Records { dir, write: std::sync::Arc::new(tokio::sync::Mutex::new(())), writer: writer.to_string() })))
    }

    /// The store's directory.
    pub fn store_name(&self) -> String {
        self.0.dir.display().to_string()
    }

    fn path(&self, key: &str) -> PathBuf {
        self.0.dir.join(format!("{key}.json"))
    }

    /// `.<key>.<writer incarnation>.json.tmp`: the temp file a write of `key` renames from.
    pub fn tmp_path(&self, key: &str) -> PathBuf {
        self.0.dir.join(format!(".{key}.{}.json.tmp", self.0.writer))
    }

    pub(crate) fn decode<T: DeserializeOwned>(path: &Path, bytes: &[u8], format: &str) -> Result<T, StorageError> {
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

    /// The record stored under `key`, or `None` when none was ever written: a point read of its file
    /// on the blocking pool, as every write is, so no worker thread waits on the disk.
    pub async fn read<T: DeserializeOwned>(&self, key: &str, format: &str) -> Result<Option<T>, StorageError> {
        let path = self.path(key);
        let file = path.clone();
        match blocking_io(move || std::fs::read(&file)).await {
            Ok(b) => Self::decode(&path, &b, format).map(Some),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
        }
    }

    /// [`Self::read`] of a key one of several formats may hold: the
    /// format the row was written in and its record, or `None` when none was ever written. A row in a
    /// format outside `formats` is refused by name, never read as no row.
    pub async fn read_tagged(&self, key: &str, formats: &[&str]) -> Result<Option<(String, serde_json::Value)>, StorageError> {
        let path = self.path(key);
        let file = path.clone();
        let bytes = match blocking_io(move || std::fs::read(&file)).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
        };
        Self::tagged(&path, &bytes, formats).map(Some)
    }

    /// [`Self::read_tagged`] of `keys`, in order, up to the first key no row was ever written at: one
    /// call on the blocking pool for the whole run, never one per row. Each row's format and record,
    /// in key order.
    pub async fn read_tagged_run(&self, keys: Vec<String>, formats: &[&str]) -> Result<Vec<(String, serde_json::Value)>, StorageError> {
        let run: Vec<PathBuf> = keys.iter().map(|k| self.path(k)).collect();
        let formats: Vec<String> = formats.iter().map(|f| f.to_string()).collect();
        blocking_io(move || {
            let mut rows = Vec::new();
            for path in run {
                let bytes = match std::fs::read(&path) {
                    Ok(bytes) => bytes,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                    Err(e) => return Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
                };
                rows.push(Self::tagged(&path, &bytes, &formats)?);
            }
            Ok(rows)
        })
        .await
    }

    /// The format and record of the row `bytes` at `path`; a format outside `formats` is refused by name.
    fn tagged<F: AsRef<str> + std::fmt::Debug>(path: &Path, bytes: &[u8], formats: &[F]) -> Result<(String, serde_json::Value), StorageError> {
        let stored: Stored<serde_json::Value> = serde_json::from_slice(bytes).map_err(|e| StorageError::Unrecognised { file: path.display().to_string(), reason: e.to_string() })?;
        if !formats.iter().any(|f| f.as_ref() == stored.format) {
            return Err(StorageError::Unrecognised { file: path.display().to_string(), reason: format!("format {:?}, this build reads {formats:?} here", stored.format) });
        }
        Ok((stored.format, stored.record))
    }

    /// Every record in the store, in key order. The directory and its files are read on the blocking pool, as a point read is, so no worker
    /// thread waits on the disk.
    pub async fn list<T: DeserializeOwned + Send + 'static>(&self, format: &str) -> Result<Vec<T>, StorageError> {
        Ok(self.list_keyed(format).await?.into_iter().map(|(_, record)| record).collect())
    }

    /// [`Self::list`], each record with its key.
    pub async fn list_keyed<T: DeserializeOwned + Send + 'static>(&self, format: &str) -> Result<Vec<(String, T)>, StorageError> {
        let (dir, format) = (self.0.dir.clone(), format.to_string());
        blocking_io(move || {
            let io = |e: std::io::Error| StorageError::Io { file: dir.display().to_string(), reason: e.to_string() };
            let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
                .map_err(io)?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|x| x == "json"))
                .collect();
            paths.sort();
            Self::read_listed(paths, &format)
        })
        .await
    }

    /// The records of the files a directory read `paths` named. A file listed and then removed before
    /// it is read is a record that is gone: the list answers the records that remain.
    fn read_listed<T: DeserializeOwned>(paths: Vec<PathBuf>, format: &str) -> Result<Vec<(String, T)>, StorageError> {
        let mut records: std::collections::BTreeMap<String, T> = std::collections::BTreeMap::new();
        for p in paths {
            let key = p.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
            match std::fs::read(&p) {
                Ok(b) => {
                    records.insert(key, Self::decode(&p, &b, format)?);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(StorageError::Io { file: p.display().to_string(), reason: e.to_string() }),
            }
        }
        Ok(records.into_iter().collect())
    }

    /// Store `record` under `key`, replacing what was there: temp file, fsync, rename, directory fsync,
    /// on the blocking pool.
    pub async fn write<T: Serialize>(&self, key: &str, format: &str, record: &T) -> Result<(), StorageError> {
        let path = self.path(key);
        let bytes = Self::encode(&path, format, record)?;
        let held = self.writer(&path).await?;
        let (dir, tmp) = (self.0.dir.clone(), self.tmp_path(key));
        blocking_io(move || {
            let _held = held;
            write_file(&dir, &tmp, &path, &bytes)
        })
        .await
    }

    /// Store `record` under `key` only if no record is there: the claim is the filesystem's own
    /// atomic link of the finished temp file to the key, which fails when the key is live.
    /// Returns whether this call created it.
    pub async fn insert<T: Serialize>(&self, key: &str, format: &str, record: &T) -> Result<bool, StorageError> {
        let path = self.path(key);
        let bytes = Self::encode(&path, format, record)?;
        let held = self.writer(&path).await?;
        let (dir, tmp) = (self.0.dir.clone(), self.tmp_path(key));
        blocking_io(move || {
            let _held = held;
            insert_file(&dir, &tmp, &path, &bytes)
        })
        .await
    }

    /// Whether a record under `key` exists, whatever it holds: a look at the disk on the blocking pool,
    /// as a point read is. A look that cannot be answered (the directory unreadable) is an error
    /// naming the file, never an answer of "not held".
    pub async fn holds(&self, key: &str) -> Result<bool, StorageError> {
        let path = self.path(key);
        let file = path.clone();
        blocking_io(move || file.try_exists()).await.map_err(|e| StorageError::Io { file: path.display().to_string(), reason: e.to_string() })
    }

    /// The write lock, owned so the caller moves it into its disk job.
    async fn writer(&self, _file: &Path) -> Result<tokio::sync::OwnedMutexGuard<()>, StorageError> {
        Ok(self.0.write.clone().lock_owned().await)
    }

    /// Wait for every write this store issued: the write lock taken once and released. Each
    /// write holds the lock until its disk job finishes, so this returns only once they have.
    pub async fn finish_issued_writes(&self) {
        drop(self.0.write.lock().await);
    }

    /// Remove the record under `key`, if any. Returns whether this call removed a row that stood: the
    /// filesystem's own unlink decides it, so of two removers of one row exactly one is answered `true`.
    pub async fn remove(&self, key: &str) -> Result<bool, StorageError> {
        let (path, dir) = (self.path(key), self.0.dir.clone());
        let held = self.writer(&path).await?;
        blocking_io(move || {
            let _held = held;
            remove_file(&dir, &path)
        })
        .await
    }
}

/// Remove `path` from `dir`, if it is there; a removal that must survive a crash syncs its directory.
/// `true`: this unlink removed the file; `false`: no file was there.
pub(crate) fn remove_file(dir: &Path, path: &Path) -> Result<bool, StorageError> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_dir(dir, path).map(|()| true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
    }
}

pub(crate) fn insert_file(dir: &Path, tmp: &Path, path: &Path, bytes: &[u8]) -> Result<bool, StorageError> {
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
    use crate::blocking::{io_time_with, IoTime};

    /// A current-thread runtime whose blocking pool has one thread: a disk job queued behind a
    /// held one stays issued and unfinished until the test releases the held one.
    fn one_blocking_thread() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread().max_blocking_threads(1).enable_all().build().unwrap()
    }

    /// Hold the one blocking thread until the returned sender is sent to (or dropped).
    fn hold_the_blocking_pool() -> std::sync::mpsc::Sender<()> {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        drop(tokio::task::spawn_blocking(move || {
            let _ = rx.recv();
        }));
        tx
    }

    /// Poll `f` until it has issued its disk job and waits on it (two scheduler checklists), then drop it.
    async fn cancel_after_issue<F: std::future::Future>(f: F) -> bool {
        let mut fabric_states = 0;
        tokio::pin!(f);
        loop {
            tokio::select! {
                biased;
                _ = &mut f => return true,
                _ = tokio::task::yield_now() => {
                    fabric_states += 1;
                    if fabric_states == 2 {
                        return false;
                    }
                }
            }
        }
    }

    /// CONTRACT (R14 item 4): a writer whose caller is cancelled while its disk job is issued keeps
    /// the store's write lock until the disk job finishes, so taking the lock once (the flush) waits
    /// for every issued write and no cancellation releases it early.
    #[test]
    fn a_cancelled_writer_holds_the_store_lock_until_its_disk_job_finishes() {
        one_blocking_thread().block_on(async {
            let d = tempdir("records-cancel");
            let r = FileRecords::open(&d, "things", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap();
            let release = hold_the_blocking_pool();
            assert!(!cancel_after_issue(r.write("a", "thing/1", &"one".to_string())).await, "the write finished while the blocking pool was held");
            assert!(r.0.write.try_lock().is_err(), "the cancelled caller released the store's write lock while its disk job was still issued");
            release.send(()).unwrap();
            r.finish_issued_writes().await;
            assert_eq!(r.read::<String>("a", "thing/1").await.unwrap().as_deref(), Some("one"), "the flush returned before the cancelled write landed");
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

    /// CONTRACT (the Build journal's read path; i144 e6, `fabric_primary.rs:204`): a run read answers
    /// the rows at its keys in order, up to the first key no row stands at, in one blocking call; a
    /// format outside those asked is refused by name. Must NOT happen: a row past the first absent key,
    /// or a blocking call per row.
    #[tokio::test]
    async fn a_run_read_answers_the_rows_at_its_keys_to_the_first_absent_one_in_one_blocking_call() {
        let d = tempdir("records-run");
        let w = crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("run")).unwrap();
        let keys = || ["01", "02", "03", "04"].map(String::from).to_vec();
        let values = |run: Vec<(String, serde_json::Value)>| run.into_iter().map(|(_, r)| r.as_str().unwrap_or_default().to_string()).collect::<Vec<_>>();
        // On the disk: 01, 02 and 04; 03 never written.
        let disk = FileRecords::open(&d, "lane", &w).await.unwrap();
        for (k, v) in [("01", "one"), ("02", "two"), ("04", "four")] {
            disk.write(k, "thing/1", &v.to_string()).await.unwrap();
        }
        let io = IoTime::default();
        let run = io_time_with(io.clone(), disk.read_tagged_run(keys(), &["thing/1"])).await.unwrap();
        assert_eq!(values(run), vec!["one", "two"], "the run ends at the first key no row stands at");
        assert_eq!(io.calls(), 1, "one blocking call for the run");
        assert!(matches!(disk.read_tagged_run(keys(), &["other/1"]).await, Err(StorageError::Unrecognised { .. })), "a format outside those asked is refused by name");
    }

    /// CONTRACT (`FileRecords::remove`: "of two removers of one row exactly one is answered `true`"):
    /// a removal answers whether this call removed a row that stood (the unlink decides): of two
    /// removals of one standing row one is `true` and one `false`, and a key never written is `false`. Must NOT
    /// happen: both removals answered `true` (R24's relanding gate: one os-stopped row cleared twice).
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn two_removals_of_one_row_answer_true_once() {
        let d = tempdir("records-remove-once");
        let w = crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("remove")).unwrap();
        let once = |a: Result<bool, StorageError>, b: Result<bool, StorageError>| [a.unwrap(), b.unwrap()].iter().filter(|r| **r).count();
        let disk = FileRecords::open(&d, "disk", &w).await.unwrap();
        disk.write("r", "thing/1", &"x".to_string()).await.unwrap();
        let (a, b) = tokio::join!(disk.remove("r"), disk.remove("r"));
        assert_eq!(once(a, b), 1, "two removals of one row on the disk: one removed it");
        assert!(!disk.remove("never").await.unwrap(), "a removal of a key never written removed nothing");
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
        let records = FileRecords::read_listed::<u32>(listed, "thing/1");
        assert_eq!(records.unwrap().into_iter().map(|(_, r)| r).collect::<Vec<_>>(), [1, 3], "the list answers the records that remain");
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
