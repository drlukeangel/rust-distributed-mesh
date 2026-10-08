//! One record per file in an admin's data dir: the durable implementation every node-admin
//! storage boundary (`fabric.storage`, `mesh.storage`, `nodes.storage`) is built on.
//!
//! Each write is a temp file, fsync and atomic rename, then a directory fsync. Each record is
//! stored with its format tag; a file this build does not recognise (another format, or not a
//! record at all) is refused by name, never read as a value.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Why a storage read or write failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    Io { file: String, reason: String },
    /// A file this build does not recognise: refused, never read as a value.
    Unrecognised { file: String, reason: String },
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
    write: Mutex<()>,
}

impl FileRecords {
    pub fn open(own_data_dir: &Path, dir: &str) -> Result<Self, StorageError> {
        let dir = own_data_dir.join(dir);
        std::fs::create_dir_all(&dir).map_err(|e| StorageError::Io { file: dir.display().to_string(), reason: e.to_string() })?;
        Ok(Self { dir, write: Mutex::new(()) })
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

    /// The record stored under `key`, or `None` when none was ever written.
    pub fn read<T: DeserializeOwned>(&self, key: &str, format: &str) -> Result<Option<T>, StorageError> {
        let path = self.path(key);
        match std::fs::read(&path) {
            Ok(b) => Self::decode(&path, &b, format).map(Some),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
        }
    }

    /// Every record in the store, in key order.
    pub fn list<T: DeserializeOwned>(&self, format: &str) -> Result<Vec<T>, StorageError> {
        let io = |e: std::io::Error| StorageError::Io { file: self.dir.display().to_string(), reason: e.to_string() };
        let mut paths: Vec<PathBuf> = std::fs::read_dir(&self.dir)
            .map_err(io)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "json"))
            .collect();
        paths.sort();
        paths
            .into_iter()
            .map(|p| {
                let b = std::fs::read(&p).map_err(|e| StorageError::Io { file: p.display().to_string(), reason: e.to_string() })?;
                Self::decode(&p, &b, format)
            })
            .collect()
    }

    /// Store `record` under `key`, replacing what was there.
    pub fn write<T: Serialize>(&self, key: &str, format: &str, record: &T) -> Result<(), StorageError> {
        off_the_runtime(|| self.write_now(key, format, record))
    }

    fn write_now<T: Serialize>(&self, key: &str, format: &str, record: &T) -> Result<(), StorageError> {
        use std::io::Write as _;
        let _g = self.write.lock().unwrap();
        let path = self.path(key);
        let tmp = self.dir.join(format!(".{key}.json.tmp"));
        let io = |e: std::io::Error| StorageError::Io { file: path.display().to_string(), reason: e.to_string() };
        let bytes = serde_json::to_vec(&Stored { format: format.to_string(), record }).map_err(|e| StorageError::Io { file: path.display().to_string(), reason: e.to_string() })?;
        let mut f = std::fs::File::create(&tmp).map_err(io)?;
        f.write_all(&bytes).map_err(io)?;
        f.sync_all().map_err(io)?;
        std::fs::rename(&tmp, &path).map_err(io)?;
        if let Ok(d) = std::fs::File::open(&self.dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }

    /// Remove the record under `key`, if any.
    pub fn remove(&self, key: &str) -> Result<(), StorageError> {
        let _g = self.write.lock().unwrap();
        let path = self.path(key);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
        }
    }
}

#[cfg(test)]
pub(crate) fn tempdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("{tag}-{}-{}", std::process::id(), rand::random::<u64>()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_survives_reopening_and_an_unknown_format_is_refused_by_name() {
        let d = tempdir("records");
        let r = FileRecords::open(&d, "things").unwrap();
        assert_eq!(r.read::<String>("a", "thing/1").unwrap(), None);
        r.write("a", "thing/1", &"one".to_string()).unwrap();
        r.write("b", "thing/1", &"two".to_string()).unwrap();
        let again = FileRecords::open(&d, "things").unwrap();
        assert_eq!(again.read::<String>("a", "thing/1").unwrap().as_deref(), Some("one"));
        assert_eq!(again.list::<String>("thing/1").unwrap(), ["one", "two"]);
        assert!(matches!(again.read::<String>("a", "thing/2"), Err(StorageError::Unrecognised { .. })));
        std::fs::write(d.join("things").join("c.json"), b"not json").unwrap();
        assert!(matches!(again.list::<String>("thing/1"), Err(StorageError::Unrecognised { file, .. }) if file.ends_with("c.json")));
        again.remove("c").unwrap();
        again.remove("a").unwrap();
        assert_eq!(again.list::<String>("thing/1").unwrap(), ["two"]);
    }
}

/// Run `f`, a blocking file write that ends in an fsync, without stalling the async runtime: on a
/// multi-thread runtime the worker hands its other tasks to the rest of the runtime first
/// (`block_in_place`), so a journal commit stalls only this caller, never the tasks queued behind
/// it. Outside a runtime, or on a current-thread one, `f` runs as is.
pub(crate) fn off_the_runtime<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(f),
        _ => f(),
    }
}
