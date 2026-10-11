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
    /// its worker instead of blocking it.
    write: tokio::sync::Mutex<()>,
}

impl FileRecords {
    /// Open (creating) the directory `dir` under the admin's data directory: the directory made on
    /// the blocking pool, as every read and write is, so no worker thread waits on the disk.
    pub async fn open(own_data_dir: &Path, dir: &str) -> Result<Self, StorageError> {
        let dir = own_data_dir.join(dir);
        let made = dir.clone();
        blocking_io(move || std::fs::create_dir_all(&made)).await.map_err(|e| StorageError::Io { file: dir.display().to_string(), reason: e.to_string() })?;
        Ok(Self { dir, write: tokio::sync::Mutex::new(()) })
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
        let _g = self.write.lock().await;
        blocking_io(move || write_file(&dir, &tmp, &path, &bytes)).await
    }

    /// Store `record` under `key` only if no record is there: the claim is the filesystem's own
    /// atomic link of the finished temp file to the key, which fails when the key is live. Returns
    /// whether this call created it.
    pub async fn insert<T: Serialize>(&self, key: &str, format: &str, record: &T) -> Result<bool, StorageError> {
        let path = self.path(key);
        let bytes = serde_json::to_vec(&Stored { format: format.to_string(), record }).map_err(|e| StorageError::Io { file: path.display().to_string(), reason: e.to_string() })?;
        let (dir, tmp) = (self.dir.clone(), self.dir.join(format!(".{key}.json.tmp")));
        let _g = self.write.lock().await;
        blocking_io(move || insert_file(&dir, &tmp, &path, &bytes)).await
    }

    /// Whether a file under `key` exists, whatever it holds: a look at the disk on the blocking
    /// pool, as a point read is. A look that cannot be answered (the directory unreadable) is an
    /// error naming the file, never an answer of "not held".
    pub async fn holds(&self, key: &str) -> Result<bool, StorageError> {
        let path = self.path(key);
        let file = path.clone();
        blocking_io(move || file.try_exists()).await.map_err(|e| StorageError::Io { file: path.display().to_string(), reason: e.to_string() })
    }

    /// Remove the record under `key`, if any.
    pub async fn remove(&self, key: &str) -> Result<(), StorageError> {
        let path = self.path(key);
        let _g = self.write.lock().await;
        blocking_io(move || match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
        })
        .await
    }
}

fn write_file(dir: &Path, tmp: &Path, path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    use std::io::Write as _;
    let io = |e: std::io::Error| StorageError::Io { file: path.display().to_string(), reason: e.to_string() };
    let mut f = std::fs::File::create(tmp).map_err(io)?;
    f.write_all(bytes).map_err(io)?;
    f.sync_all().map_err(io)?;
    std::fs::rename(tmp, path).map_err(io)?;
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
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
        Ok(()) => {
            if let Ok(d) = std::fs::File::open(dir) {
                let _ = d.sync_all();
            }
            Ok(true)
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(io(e)),
    }
}

/// Run `f`, a blocking disk call (a write that ends in an fsync, a read, a directory made or looked
/// at), on tokio's blocking pool. No
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

    #[tokio::test]
    async fn a_record_survives_reopening_and_an_unknown_format_is_refused_by_name() {
        let d = tempdir("records");
        let r = FileRecords::open(&d, "things").await.unwrap();
        assert_eq!(r.read::<String>("a", "thing/1").await.unwrap(), None);
        r.write("a", "thing/1", &"one".to_string()).await.unwrap();
        r.write("b", "thing/1", &"two".to_string()).await.unwrap();
        let again = FileRecords::open(&d, "things").await.unwrap();
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
        let r = std::sync::Arc::new(FileRecords::open(&d, "things").await.unwrap());
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

    /// CONTRACT: opening a store (its directory made on the disk) never waits on the disk on a
    /// runtime worker: it runs on the blocking pool, as every read and write of the store does
    /// (`blocking_io`). Every node-admin store is opened inside the admin's boot, on the runtime.
    #[test]
    fn opening_a_store_waits_on_the_disk_off_the_runtime_thread() {
        let d = tempdir("records-open");
        let (waited, opened) = op_waits_on_the_blocking_pool(Box::pin(async {}), move |()| Box::pin(async move { FileRecords::open(&d, "things").await.map(|_| ()) }));
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
            let r = FileRecords::open(&d, "things").await.unwrap();
            r.write("a", "thing/1", &"one".to_string()).await.unwrap();
            r
        });
        let (waited, held) = op_waits_on_the_blocking_pool(setup, |r| Box::pin(async move { (r.holds("a").await, r.holds("b").await) }));
        assert!(waited, "the look ran on the blocking pool, not the runtime's thread");
        assert_eq!(held, (Ok(true), Ok(false)));
    }
}
