//! `status.storage`: the Mesh statuses and Fabric events a node-admin applied as an authority
//! (`status_rpc`), each fact its own keyed row, beside the node rows `nodes.storage` holds.
//!
//! A status fact is a blind put of its own key: `(mesh_id, state)` for a Mesh, `(fabric_id,
//! event)` for a Fabric event. Nothing is read back to be written again. A reader folds the rows:
//! the greatest state per Mesh, the set of events per Fabric. `Applied` means the row's put was
//! acknowledged before the answer left; a restarted admin folds the rows it wrote into what it
//! has applied.

use crate::model::{FabricId, MeshId};
use crate::record_store::{FileRecords, StorageError};
use async_trait::async_trait;
use rafka_node_rpc_contract::status::MeshState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

/// One Mesh status fact: the Mesh entered `state`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshStatusRow {
    /// The mesh.
    pub mesh_id: MeshId,
    /// The state it entered.
    pub state: MeshState,
}

impl MeshStatusRow {
    /// The row's key: the natural key of the fact.
    pub fn key(&self) -> String {
        format!("{}-{:?}", self.mesh_id, self.state)
    }
}

/// One Fabric event fact: the Fabric event named `event` (its natural key) was applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricEventRow {
    /// The fabric.
    pub fabric_id: FabricId,
    /// The event applied.
    pub event: String,
}

impl FabricEventRow {
    /// The row's key: the fabric id and the hex of the event name.
    pub fn key(&self) -> String {
        let hex: String = self.event.bytes().map(|b| format!("{b:02x}")).collect();
        format!("{}-{hex}", self.fabric_id)
    }
}

/// The durable record of the status facts this admin applied.
#[async_trait]
pub trait StatusStorage: Send + Sync {
    /// A blind put of the fact's own key.
    async fn put_mesh_status(&self, row: &MeshStatusRow) -> Result<(), StorageError>;
    /// The greatest state held per Mesh.
    async fn mesh_statuses(&self) -> Result<HashMap<MeshId, MeshState>, StorageError>;
    /// Keep `row` under its own key.
    async fn put_fabric_event(&self, row: &FabricEventRow) -> Result<(), StorageError>;
    /// The events held, per Fabric, each once.
    async fn fabric_events(&self) -> Result<Vec<FabricEventRow>, StorageError>;
}

fn fold_meshes(rows: impl IntoIterator<Item = MeshStatusRow>) -> HashMap<MeshId, MeshState> {
    let mut out: HashMap<MeshId, MeshState> = HashMap::new();
    for r in rows {
        let e = out.entry(r.mesh_id).or_insert(r.state);
        if r.state > *e {
            *e = r.state;
        }
    }
    out
}

/// A status store held in memory.
#[derive(Debug, Default)]
pub struct MemoryStatusStorage {
    meshes: Mutex<Vec<MeshStatusRow>>,
    events: Mutex<Vec<FabricEventRow>>,
}

#[async_trait]
impl StatusStorage for MemoryStatusStorage {
    async fn put_mesh_status(&self, row: &MeshStatusRow) -> Result<(), StorageError> {
        let mut m = self.meshes.lock().unwrap();
        if !m.contains(row) {
            m.push(row.clone());
        }
        Ok(())
    }
    async fn mesh_statuses(&self) -> Result<HashMap<MeshId, MeshState>, StorageError> {
        Ok(fold_meshes(self.meshes.lock().unwrap().clone()))
    }
    async fn put_fabric_event(&self, row: &FabricEventRow) -> Result<(), StorageError> {
        let mut e = self.events.lock().unwrap();
        if !e.contains(row) {
            e.push(row.clone());
        }
        Ok(())
    }
    async fn fabric_events(&self) -> Result<Vec<FabricEventRow>, StorageError> {
        Ok(self.events.lock().unwrap().clone())
    }
}

const MESH_STATUS_FORMAT: &str = "mesh-status/1";
const FABRIC_EVENT_FORMAT: &str = "fabric-event/1";

/// One row per file: `<data dir>/status/meshes/<mesh_id>-<state>.json` and
/// `<data dir>/status/fabric/<fabric_id>-<hex of the event key>.json`.
#[derive(Debug)]
pub struct FileStatusStorage {
    meshes: FileRecords,
    fabric: FileRecords,
}

impl FileStatusStorage {
    /// Open the store under the admin's own data directory.
    pub fn open(own_data_dir: &Path) -> Result<Self, StorageError> {
        Ok(Self { meshes: FileRecords::open(own_data_dir, "status/meshes")?, fabric: FileRecords::open(own_data_dir, "status/fabric")? })
    }
}

#[async_trait]
impl StatusStorage for FileStatusStorage {
    async fn put_mesh_status(&self, row: &MeshStatusRow) -> Result<(), StorageError> {
        self.meshes.write(&row.key(), MESH_STATUS_FORMAT, row).await
    }
    async fn mesh_statuses(&self) -> Result<HashMap<MeshId, MeshState>, StorageError> {
        Ok(fold_meshes(self.meshes.list::<MeshStatusRow>(MESH_STATUS_FORMAT)?))
    }
    async fn put_fabric_event(&self, row: &FabricEventRow) -> Result<(), StorageError> {
        self.fabric.write(&row.key(), FABRIC_EVENT_FORMAT, row).await
    }
    async fn fabric_events(&self) -> Result<Vec<FabricEventRow>, StorageError> {
        self.fabric.list(FABRIC_EVENT_FORMAT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_reader_folds_the_greatest_state_and_each_event_once_across_a_reopen() {
        let dir = std::env::temp_dir().join(format!("status-storage-{}", MeshId::mint()));
        let (m, f) = (MeshId::mint(), FabricId::mint());
        {
            let s = FileStatusStorage::open(&dir).unwrap();
            // Written out of order: the later fact first, a lagging earlier one after it.
            s.put_mesh_status(&MeshStatusRow { mesh_id: m.clone(), state: MeshState::ReadyForTraffic }).await.unwrap();
            s.put_mesh_status(&MeshStatusRow { mesh_id: m.clone(), state: MeshState::Pending }).await.unwrap();
            s.put_mesh_status(&MeshStatusRow { mesh_id: m.clone(), state: MeshState::Pending }).await.unwrap();
            s.put_fabric_event(&FabricEventRow { fabric_id: f.clone(), event: "shutdown-initiated:mesh1.admin.1".into() }).await.unwrap();
            s.put_fabric_event(&FabricEventRow { fabric_id: f.clone(), event: "shutdown-initiated:mesh1.admin.1".into() }).await.unwrap();
        }
        let s = FileStatusStorage::open(&dir).unwrap();
        assert_eq!(s.mesh_statuses().await.unwrap().get(&m), Some(&MeshState::ReadyForTraffic), "the greatest state wins whatever the write order");
        assert_eq!(s.fabric_events().await.unwrap().len(), 1, "one row per event key");
        std::fs::remove_dir_all(&dir).ok();
    }
}
