//! `status.storage`: the status facts a node-admin holds, each its own keyed row with one writer,
//! beside the contact rows `nodes.storage` holds: the Mesh statuses, Fabric events and node
//! declared states it applied as an authority (`status_rpc`), and the silence status it holds for a
//! member as that member's mesh primary (`offline::put_silence_status`).
//!
//! A status fact is a blind put of its own key: `(mesh_id, state)` for a Mesh, `(fabric_id,
//! event)` for a Fabric event, `(node_id, incarnation, state)` for a node's declared state (a new
//! cycle tombstones the birth's rows),
//! `(node_id, incarnation)` for a birth's silence status. Nothing is read back to be written again.
//! A reader folds the rows: the greatest state per Mesh and per birth, the set of events per Fabric. `Applied` means the row's put was
//! acknowledged before the answer left; a restarted admin folds the rows it wrote into what it
//! has applied.

use crate::model::{FabricId, IncarnationId, MeshId, NodeId, NodeStatus, PathName};
use crate::record_store::{FileRecords, StorageError};
use async_trait::async_trait;
use rafka_node_rpc_contract::status::{MeshState, NodeState};
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

/// One node declared-state fact as its authority applied it: the birth `incarnation_id` entered
/// `state`. Keyed by the fact itself, `(node_id, incarnation, state)`, as a Mesh status row is:
/// two declarations of one birth put two rows and neither overwrites the other, whatever order
/// their puts land in. A reader folds the greatest state per birth. Written only by the authority;
/// a start beginning the birth's next cycle tombstones that birth's rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDeclaredRow {
    /// The node.
    pub node_id: NodeId,
    /// The birth that declared it.
    pub incarnation_id: IncarnationId,
    /// The state applied.
    pub state: NodeState,
}

impl NodeDeclaredRow {
    /// The row's key: the natural key of the fact.
    pub fn key(&self) -> String {
        Self::key_of(&self.node_id, &self.incarnation_id, self.state)
    }

    fn key_of(node_id: &NodeId, incarnation_id: &IncarnationId, state: NodeState) -> String {
        format!("{node_id}--{}--{state:?}", incarnation_id.0)
    }

    /// Every key a birth's declared-state rows can hold: what a cycle begin tombstones, blind.
    pub fn keys_of_birth(node_id: &NodeId, incarnation_id: &IncarnationId) -> Vec<String> {
        // Exhaustive: a new state is a compile error here until its key is tombstoned too.
        let every = |s: NodeState| match s {
            NodeState::Pending | NodeState::ReadyForTraffic | NodeState::Draining | NodeState::Leaving => s,
        };
        [NodeState::Pending, NodeState::ReadyForTraffic, NodeState::Draining, NodeState::Leaving].into_iter().map(every).map(|s| Self::key_of(node_id, incarnation_id, s)).collect()
    }
}

/// The greatest declared state per birth.
pub fn fold_declared(rows: impl IntoIterator<Item = NodeDeclaredRow>) -> HashMap<(NodeId, IncarnationId), NodeState> {
    let mut out = HashMap::new();
    for r in rows {
        let e = out.entry((r.node_id, r.incarnation_id)).or_insert(r.state);
        if r.state > *e {
            *e = r.state;
        }
    }
    out
}

/// The silence status a mesh primary holds for one birth of its own mesh (fabric-node-lifecycle.md
/// §7.3): `PendingReconnect`, then `Dead` once its offline tickle found no path on two rounds a
/// staleness floor apart, `None` once it is heard again. Keyed by the exact birth; written only by
/// the mesh primary's tickle loop. Observer-inferred: never itself a reason to act.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSilenceRow {
    /// The node.
    pub node_id: NodeId,
    /// The node's `path.name`.
    pub name: PathName,
    /// The exact birth.
    pub incarnation_id: IncarnationId,
    /// The status held, `None` while heard.
    pub status: Option<NodeStatus>,
}

impl NodeSilenceRow {
    /// The row's key: the exact birth.
    pub fn key(&self) -> String {
        format!("{}--{}", self.node_id, self.incarnation_id.0)
    }
}

/// The durable record of the status facts this admin holds.
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
    /// A blind put of the declared-state fact's own key.
    async fn put_node_declared(&self, row: &NodeDeclaredRow) -> Result<(), StorageError>;
    /// A blind tombstone of every declared-state key of the birth `(node_id, incarnation_id)`.
    async fn remove_node_declared(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), StorageError>;
    /// Every declared-state row held.
    async fn node_declared(&self) -> Result<Vec<NodeDeclaredRow>, StorageError>;
    /// A blind put of the birth's silence-status row.
    async fn put_node_silence(&self, row: &NodeSilenceRow) -> Result<(), StorageError>;
    /// Every birth's silence-status row.
    async fn node_silences(&self) -> Result<Vec<NodeSilenceRow>, StorageError>;
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
    declared: Mutex<std::collections::BTreeMap<String, NodeDeclaredRow>>,
    silences: Mutex<std::collections::BTreeMap<String, NodeSilenceRow>>,
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
    async fn put_node_declared(&self, row: &NodeDeclaredRow) -> Result<(), StorageError> {
        self.declared.lock().unwrap().insert(row.key(), row.clone());
        Ok(())
    }
    async fn remove_node_declared(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), StorageError> {
        let mut d = self.declared.lock().unwrap();
        for k in NodeDeclaredRow::keys_of_birth(node_id, incarnation_id) {
            d.remove(&k);
        }
        Ok(())
    }
    async fn node_declared(&self) -> Result<Vec<NodeDeclaredRow>, StorageError> {
        Ok(self.declared.lock().unwrap().values().cloned().collect())
    }
    async fn put_node_silence(&self, row: &NodeSilenceRow) -> Result<(), StorageError> {
        self.silences.lock().unwrap().insert(row.key(), row.clone());
        Ok(())
    }
    async fn node_silences(&self) -> Result<Vec<NodeSilenceRow>, StorageError> {
        Ok(self.silences.lock().unwrap().values().cloned().collect())
    }
}

const MESH_STATUS_FORMAT: &str = "mesh-status/1";
const FABRIC_EVENT_FORMAT: &str = "fabric-event/1";
const NODE_DECLARED_FORMAT: &str = "node-declared/2";
const NODE_SILENCE_FORMAT: &str = "node-silence/1";

/// One row per file: `<data dir>/status/meshes/<mesh_id>-<state>.json`,
/// `<data dir>/status/fabric/<fabric_id>-<hex of the event key>.json`,
/// `<data dir>/status/declared/<node_id>--<incarnation>--<state>.json` and
/// `<data dir>/status/silence/<node_id>--<incarnation>.json`.
#[derive(Debug)]
pub struct FileStatusStorage {
    meshes: FileRecords,
    fabric: FileRecords,
    declared: FileRecords,
    silence: FileRecords,
}

impl FileStatusStorage {
    /// Open the store under the admin's own data directory.
    pub fn open(own_data_dir: &Path) -> Result<Self, StorageError> {
        Ok(Self {
            meshes: FileRecords::open(own_data_dir, "status/meshes")?,
            fabric: FileRecords::open(own_data_dir, "status/fabric")?,
            declared: FileRecords::open(own_data_dir, "status/declared")?,
            silence: FileRecords::open(own_data_dir, "status/silence")?,
        })
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
    async fn put_node_declared(&self, row: &NodeDeclaredRow) -> Result<(), StorageError> {
        self.declared.write(&row.key(), NODE_DECLARED_FORMAT, row).await
    }
    async fn remove_node_declared(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), StorageError> {
        for k in NodeDeclaredRow::keys_of_birth(node_id, incarnation_id) {
            self.declared.remove(&k).await?;
        }
        Ok(())
    }
    async fn node_declared(&self) -> Result<Vec<NodeDeclaredRow>, StorageError> {
        self.declared.list(NODE_DECLARED_FORMAT)
    }
    async fn put_node_silence(&self, row: &NodeSilenceRow) -> Result<(), StorageError> {
        self.silence.write(&row.key(), NODE_SILENCE_FORMAT, row).await
    }
    async fn node_silences(&self) -> Result<Vec<NodeSilenceRow>, StorageError> {
        self.silence.list(NODE_SILENCE_FORMAT)
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
