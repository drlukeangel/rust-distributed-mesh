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

use async_trait::async_trait;
use crate::model::FabricId;
use serde::{Deserialize, Serialize};
use std::path::Path;
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

/// Why a `fabric.storage` read or write failed.
pub use crate::record_store::StorageError as FabricStorageError;

/// node-admin's Fabric control state.
#[async_trait]
pub trait FabricStorage: Send + Sync {
    async fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError>;
    async fn put_fabric(&self, record: &FabricRecord) -> Result<(), FabricStorageError>;
    async fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError>;
    /// Hold `shutdown`. The first shutdown held is kept: a later one never replaces it, and it is
    /// never removed. Returns the record now held.
    async fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError>;
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

#[async_trait]
impl FabricStorage for MemoryFabricStorage {
    async fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError> {
        Ok(self.fabric.lock().unwrap().clone())
    }
    async fn put_fabric(&self, record: &FabricRecord) -> Result<(), FabricStorageError> {
        *self.fabric.lock().unwrap() = Some(record.clone());
        Ok(())
    }
    async fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError> {
        Ok(self.shutdown.lock().unwrap().clone())
    }
    async fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError> {
        let mut g = self.shutdown.lock().unwrap();
        let (now, _) = decide(g.clone(), shutdown);
        *g = Some(now.clone());
        Ok(now)
    }
}

/// The directory inside an admin's data dir.
pub const FABRIC_DIR: &str = "fabric";
const FABRIC_KEY: &str = "fabric";
const SHUTDOWN_KEY: &str = "shutdown";
const FABRIC_FORMAT: &str = "fabric-record/1";
const SHUTDOWN_FORMAT: &str = "fabric-shutdown/1";

/// One record per file under `<data dir>/fabric/` (`crate::record_store`).
#[derive(Debug)]
pub struct FileFabricStorage {
    records: crate::record_store::FileRecords,
    /// Serializes shutdown writers (decide, then write); async, so a waiter yields its worker.
    shutdown: tokio::sync::Mutex<()>,
}

impl FileFabricStorage {
    pub fn open(own_data_dir: &Path) -> Result<Self, FabricStorageError> {
        Ok(Self { records: crate::record_store::FileRecords::open(own_data_dir, FABRIC_DIR)?, shutdown: tokio::sync::Mutex::new(()) })
    }
}

#[async_trait]
impl FabricStorage for FileFabricStorage {
    async fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError> {
        self.records.read(FABRIC_KEY, FABRIC_FORMAT)
    }
    async fn put_fabric(&self, record: &FabricRecord) -> Result<(), FabricStorageError> {
        self.records.write(FABRIC_KEY, FABRIC_FORMAT, record).await
    }
    async fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError> {
        self.records.read(SHUTDOWN_KEY, SHUTDOWN_FORMAT)
    }
    async fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError> {
        let _g = self.shutdown.lock().await;
        let (now, changed) = decide(self.records.read(SHUTDOWN_KEY, SHUTDOWN_FORMAT)?, shutdown);
        if changed {
            self.records.write(SHUTDOWN_KEY, SHUTDOWN_FORMAT, &now).await?;
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

    #[tokio::test]
    async fn a_held_shutdown_is_never_replaced_or_removed() {
        for s in [Box::new(MemoryFabricStorage::new()) as Box<dyn FabricStorage>, {
            let d = tempdir();
            Box::new(FileFabricStorage::open(&d).unwrap())
        }] {
            assert_eq!(s.shutdown().await.unwrap(), None);
            assert_eq!(s.put_shutdown(&shutdown()).await.unwrap(), shutdown());
            let other = FabricShutdown { initiated_by: "mesh2.admin.1".into(), ..shutdown() };
            assert_eq!(s.put_shutdown(&other).await.unwrap(), shutdown(), "the first initiation is kept");
        }
    }

    #[tokio::test]
    async fn the_file_storage_reloads_what_it_wrote_and_refuses_an_unknown_format_by_name() {
        let d = tempdir();
        let s = FileFabricStorage::open(&d).unwrap();
        let fabric = FabricRecord { fabric_id: FabricId::mint(), name: "fabric1".into(), build_id: Some(crate::build::BuildId("bld-1".into())) };
        s.put_fabric(&fabric).await.unwrap();
        s.put_shutdown(&shutdown()).await.unwrap();
        let reopened = FileFabricStorage::open(&d).unwrap();
        assert_eq!(reopened.fabric().await.unwrap(), Some(fabric));
        assert_eq!(reopened.shutdown().await.unwrap(), Some(shutdown()));
        std::fs::write(d.join(FABRIC_DIR).join("shutdown.json"), br#"{"format":"fabric-shutdown/9","record":{}}"#).unwrap();
        assert!(matches!(reopened.shutdown().await, Err(FabricStorageError::Unrecognised { .. })));
        std::fs::write(d.join(FABRIC_DIR).join("shutdown.json"), b"not json").unwrap();
        assert!(matches!(reopened.shutdown().await, Err(FabricStorageError::Unrecognised { .. })));
    }

    fn tempdir() -> std::path::PathBuf {
        crate::record_store::tempdir("fabric-storage")
    }
}
