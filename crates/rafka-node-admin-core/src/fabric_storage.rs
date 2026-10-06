//! `fabric.storage`: node-admin's Fabric control state behind a storage boundary
//! (fabric-mesh-lifecycle.md §11.1, §13).
//!
//! It holds the Fabric record (id and name) and, once a fabric shutdown begins, the
//! [`FabricShutdown`] record. Every admin holds its own copy: the fabric-primary writes the shutdown
//! when it initiates one, and every admin that learns it persists the same record before it
//! freezes, so losing the fabric-primary and its storage loses nothing. A shutdown is never deleted
//! while its Fabric lives, and an admin that starts on a data dir holding one comes up frozen.
//!
//! Two implementations: [`MemoryFabricStorage`] for runs that need no restart survival, and
//! [`FileFabricStorage`], one record per file in the admin's data dir, each write a temp file,
//! fsync and atomic rename. A file this build does not recognise is refused by name, never read as
//! a value.

use crate::model::FabricId;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// The Fabric record: its immutable identity, and `build_id`, the one accepted topology (the
/// complete Build it names). `None` only before Day 0 accepted the first Build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricRecord {
    pub fabric_id: FabricId,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_id: Option<crate::build::BuildId>,
}

/// A fabric shutdown in progress. `initiated_by` names the fabric-primary that began it;
/// `initiated_at_ms` is a diagnostic and takes no part in authority or ordering. The record holds no
/// drain progress: every admin derives the freeze barrier (every live admin `Draining`) and runtime
/// disappearance from its own view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricShutdown {
    pub initiated_by: String,
    pub initiated_by_node_id: String,
    pub initiated_at_ms: u64,
}

/// Why a storage read or write failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FabricStorageError {
    Io { file: String, reason: String },
    /// A file this build does not recognise: refused, never read as a value.
    Unrecognised { file: String, reason: String },
}

impl std::fmt::Display for FabricStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { file, reason } => write!(f, "fabric.storage {file}: {reason}"),
            Self::Unrecognised { file, reason } => write!(f, "fabric.storage {file} is not a record this build recognises: {reason}"),
        }
    }
}

/// node-admin's Fabric control state.
pub trait FabricStorage: Send + Sync {
    fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError>;
    fn put_fabric(&self, record: &FabricRecord) -> Result<(), FabricStorageError>;
    fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError>;
    /// Hold `shutdown`. The first shutdown held is kept: a later one never replaces it, and it is
    /// never removed. Returns the record now held.
    fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError>;
}

/// What a write decides against the held shutdown: the first one is kept.
fn decide(held: Option<FabricShutdown>, offered: &FabricShutdown) -> (FabricShutdown, bool) {
    match held {
        None => (offered.clone(), true),
        Some(h) => (h, false),
    }
}

#[derive(Debug, Default)]
pub struct MemoryFabricStorage {
    fabric: Mutex<Option<FabricRecord>>,
    shutdown: Mutex<Option<FabricShutdown>>,
}

impl MemoryFabricStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl FabricStorage for MemoryFabricStorage {
    fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError> {
        Ok(self.fabric.lock().unwrap().clone())
    }
    fn put_fabric(&self, record: &FabricRecord) -> Result<(), FabricStorageError> {
        *self.fabric.lock().unwrap() = Some(record.clone());
        Ok(())
    }
    fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError> {
        Ok(self.shutdown.lock().unwrap().clone())
    }
    fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError> {
        let mut g = self.shutdown.lock().unwrap();
        let (now, _) = decide(g.clone(), shutdown);
        *g = Some(now.clone());
        Ok(now)
    }
}

/// The directory inside an admin's data dir.
pub const FABRIC_DIR: &str = "fabric";
const FABRIC_FILE: &str = "fabric.json";
const SHUTDOWN_FILE: &str = "shutdown.json";
const FABRIC_FORMAT: &str = "fabric-record/1";
const SHUTDOWN_FORMAT: &str = "fabric-shutdown/1";

#[derive(Serialize, Deserialize)]
struct Stored<T> {
    format: String,
    record: T,
}

/// One record per file under `<data dir>/fabric/`.
#[derive(Debug)]
pub struct FileFabricStorage {
    dir: PathBuf,
    write: Mutex<()>,
}

impl FileFabricStorage {
    pub fn open(own_data_dir: &Path) -> Result<Self, FabricStorageError> {
        let dir = own_data_dir.join(FABRIC_DIR);
        std::fs::create_dir_all(&dir).map_err(|e| FabricStorageError::Io { file: dir.display().to_string(), reason: e.to_string() })?;
        Ok(Self { dir, write: Mutex::new(()) })
    }

    fn read<T: serde::de::DeserializeOwned>(&self, file: &str, format: &str) -> Result<Option<T>, FabricStorageError> {
        let path = self.dir.join(file);
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(FabricStorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
        };
        let stored: Stored<T> = serde_json::from_slice(&bytes)
            .map_err(|e| FabricStorageError::Unrecognised { file: path.display().to_string(), reason: e.to_string() })?;
        if stored.format != format {
            return Err(FabricStorageError::Unrecognised {
                file: path.display().to_string(),
                reason: format!("format {:?}, this build reads {format:?}", stored.format),
            });
        }
        Ok(Some(stored.record))
    }

    fn write<T: Serialize>(&self, file: &str, format: &str, record: &T) -> Result<(), FabricStorageError> {
        use std::io::Write as _;
        let path = self.dir.join(file);
        let tmp = self.dir.join(format!(".{file}.tmp"));
        let io = |e: std::io::Error| FabricStorageError::Io { file: path.display().to_string(), reason: e.to_string() };
        let bytes = serde_json::to_vec(&Stored { format: format.to_string(), record }).map_err(|e| FabricStorageError::Io {
            file: path.display().to_string(),
            reason: e.to_string(),
        })?;
        let mut f = std::fs::File::create(&tmp).map_err(io)?;
        f.write_all(&bytes).map_err(io)?;
        f.sync_all().map_err(io)?;
        std::fs::rename(&tmp, &path).map_err(io)?;
        if let Ok(d) = std::fs::File::open(&self.dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }
}

impl FabricStorage for FileFabricStorage {
    fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError> {
        self.read(FABRIC_FILE, FABRIC_FORMAT)
    }
    fn put_fabric(&self, record: &FabricRecord) -> Result<(), FabricStorageError> {
        let _g = self.write.lock().unwrap();
        self.write(FABRIC_FILE, FABRIC_FORMAT, record)
    }
    fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError> {
        self.read(SHUTDOWN_FILE, SHUTDOWN_FORMAT)
    }
    fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError> {
        let _g = self.write.lock().unwrap();
        let (now, changed) = decide(self.shutdown()?, shutdown);
        if changed {
            self.write(SHUTDOWN_FILE, SHUTDOWN_FORMAT, &now)?;
        }
        Ok(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shutdown() -> FabricShutdown {
        FabricShutdown { initiated_by: "mesh1.admin.1".into(), initiated_by_node_id: "n1".into(), initiated_at_ms: 10 }
    }

    #[test]
    fn a_held_shutdown_is_never_replaced_or_removed() {
        for s in [Box::new(MemoryFabricStorage::new()) as Box<dyn FabricStorage>, {
            let d = tempdir();
            Box::new(FileFabricStorage::open(&d).unwrap())
        }] {
            assert_eq!(s.shutdown().unwrap(), None);
            assert_eq!(s.put_shutdown(&shutdown()).unwrap(), shutdown());
            let other = FabricShutdown { initiated_by: "mesh2.admin.1".into(), ..shutdown() };
            assert_eq!(s.put_shutdown(&other).unwrap(), shutdown(), "the first initiation is kept");
        }
    }

    #[test]
    fn the_file_storage_reloads_what_it_wrote_and_refuses_an_unknown_format_by_name() {
        let d = tempdir();
        let s = FileFabricStorage::open(&d).unwrap();
        let fabric = FabricRecord { fabric_id: FabricId::mint(), name: "fabric1".into(), build_id: Some(crate::build::BuildId("bld-1".into())) };
        s.put_fabric(&fabric).unwrap();
        s.put_shutdown(&shutdown()).unwrap();
        let reopened = FileFabricStorage::open(&d).unwrap();
        assert_eq!(reopened.fabric().unwrap(), Some(fabric));
        assert_eq!(reopened.shutdown().unwrap(), Some(shutdown()));
        std::fs::write(d.join(FABRIC_DIR).join(SHUTDOWN_FILE), br#"{"format":"fabric-shutdown/9","record":{}}"#).unwrap();
        assert!(matches!(reopened.shutdown(), Err(FabricStorageError::Unrecognised { .. })));
        std::fs::write(d.join(FABRIC_DIR).join(SHUTDOWN_FILE), b"not json").unwrap();
        assert!(matches!(reopened.shutdown(), Err(FabricStorageError::Unrecognised { .. })));
    }

    fn tempdir() -> PathBuf {
        let d = std::env::temp_dir().join(format!("fabric-storage-{}-{}", std::process::id(), rand::random::<u64>()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }
}
