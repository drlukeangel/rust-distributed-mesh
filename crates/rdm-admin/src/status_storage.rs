//! `status.storage`: the status facts a node-admin holds, each its own keyed row with one writer,
//! beside the contact rows `nodes.storage` holds: the Mesh statuses, Fabric events and node
//! declared states it applied as an authority (`status_rpc`), and the silence status it holds for a
//! member as that member's mesh primary (`offline::put_silence_status`).
//!
//! A status fact is a blind put of its own key: `(mesh_id, state)` for a Mesh, `(fabric_id,
//! event)` for a Fabric event, `(node_id, incarnation, state)` for a node's declared state (a new
//! cycle tombstones only those state rows), and `(birth, build_id, attempt, operation)` for an
//! operation's applied effect (Q49: a cycle begin never clears it),
//! `(node_id, incarnation)` for a birth's silence status. Nothing is read back to be written again.
//! A reader folds the rows: the greatest state per Mesh and per birth, the set of events per Fabric. `Applied` means the row's put was
//! acknowledged before the answer left. The rows of a mesh live on the fabric's store root under
//! `status/<mesh>/`, written by the mesh's seat holder; the admin that takes the seat folds them into
//! what it has applied (`status_rpc::SeatFold`).

use crate::model::{FabricId, IncarnationId, MeshId, NodeId, NodeStatus, PathName};
use crate::record_store::{FileRecords, StorageError};
use async_trait::async_trait;
use rdm_core::status::{MeshState, NodeState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    /// The birth's path.name as it declared: what a recovery's new node at that path supersedes it by.
    pub name: crate::model::PathName,
    /// The state applied.
    pub state: NodeState,
}

impl NodeDeclaredRow {
    /// The row's key: the natural key of the fact.
    pub fn key(&self) -> String {
        Self::key_of(&self.node_id, &self.incarnation_id, self.state)
    }

    fn key_of(node_id: &NodeId, incarnation_id: &IncarnationId, state: NodeState) -> String {
        format!("{node_id}--{}--{state:?}", incarnation_id.as_str())
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

/// An operation effect already applied to this exact birth (Q49). Independent of its current cycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeOperationRow {
    /// The birth's node.
    pub node_id: NodeId,
    /// The birth's incarnation.
    pub incarnation_id: IncarnationId,
    /// The path this birth held.
    pub name: PathName,
    /// The operation that produced this effect.
    pub operation: rdm_core::status::DeclaredOperation,
    /// The effect applied.
    pub state: NodeState,
}

impl NodeOperationRow {
    /// The operation identity: exact birth, Build, attempt and operation, never its current state.
    pub fn key(&self) -> String {
        let hex = |v: &str| v.bytes().map(|b| format!("{b:02x}")).collect::<String>();
        format!("{}--{}--{}--{}--{}", self.node_id, self.incarnation_id, hex(&self.operation.build_id), self.operation.attempt, hex(&self.operation.operation))
    }
}

/// The path.name each birth declared at.
pub fn declared_names(rows: &[NodeDeclaredRow]) -> HashMap<(NodeId, IncarnationId), crate::model::PathName> {
    rows.iter().map(|r| ((r.node_id.clone(), r.incarnation_id.clone()), r.name.clone())).collect()
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

/// The silence status a mesh primary holds for one birth of its own mesh (node-identity-and-state.md
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
        format!("{}--{}", self.node_id, self.incarnation_id.as_str())
    }
}

/// The provider's proof that one birth of the mesh primary's own mesh exited (node-recovery.md
/// "Exited is the proof, recorded durably"): the exit code and where it came from, as the mesh
/// primary inspected it. Keyed by the exact birth; written only by the mesh primary, a blind put
/// once its tick finds the runtime exited. The recovery it starts reads this row to publish the
/// exited instance's node.deleted; nothing of it goes on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeExitRow {
    /// The node.
    pub node_id: NodeId,
    /// The node's `path.name`.
    pub name: PathName,
    /// The exact birth that exited.
    pub incarnation_id: IncarnationId,
    /// The runtime's exit code, when the provider holds one.
    pub exit_code: Option<i32>,
    /// Where the proof came from: `provider` (the provider's own terminal status), `exit-record`
    /// (the runtime's exit record in its data dir) or `none` (exited with no code).
    pub proof: String,
    /// When the mesh primary observed it, ms since the Unix epoch.
    pub observed_at_ms: u64,
}

impl NodeExitRow {
    /// The row's key: the exact birth.
    pub fn key(&self) -> String {
        Self::key_of(&self.node_id, &self.incarnation_id)
    }

    /// The key of the birth `(node_id, incarnation_id)`'s exit row.
    pub fn key_of(node_id: &NodeId, incarnation_id: &IncarnationId) -> String {
        format!("{node_id}--{}", incarnation_id.as_str())
    }
}

/// The durable `held-running` fact on a birth's status row (R-FM1): the provider reported its
/// exact runtime `Running` while the birth is silent. Keyed by the exact birth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeHeldRunningRow {
    /// The node.
    pub node_id: NodeId,
    /// The node's `path.name`.
    pub name: PathName,
    /// The exact birth.
    pub incarnation_id: IncarnationId,
    /// When the first `Running` verdict was observed, ms since the Unix epoch.
    pub since: u64,
    /// How many `Running` verdicts have been observed for this birth during the hold.
    pub verdicts: u64,
}

impl NodeHeldRunningRow {
    /// The row's key: the exact birth.
    pub fn key(&self) -> String {
        Self::key_of(&self.node_id, &self.incarnation_id)
    }

    /// The key of the birth `(node_id, incarnation_id)`'s held-running row.
    pub fn key_of(node_id: &NodeId, incarnation_id: &IncarnationId) -> String {
        format!("{node_id}--{}", incarnation_id.as_str())
    }
}

/// The durable `os-stopped` fact on a birth's status row (R-PV1; deployment-provider.md §2.5): the
/// provider inspected the exact runtime and the operating system holds it suspended. An observation,
/// never a hold: written by the mesh-primary, removed when its inspection reads `Running` or
/// `Exited`, and folded by a successor only after its own inspection confirms it. Keyed by the exact
/// birth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeOsStoppedRow {
    /// The node.
    pub node_id: NodeId,
    /// The node's `path.name`.
    pub name: PathName,
    /// The exact birth.
    pub incarnation_id: IncarnationId,
    /// When the first `OsStopped` verdict was observed, ms since the Unix epoch.
    pub since: u64,
    /// How many inspections have read the runtime `OsStopped` since.
    pub inspections: u64,
}

impl NodeOsStoppedRow {
    /// The row's key: the exact birth.
    pub fn key(&self) -> String {
        Self::key_of(&self.node_id, &self.incarnation_id)
    }

    /// The key of the birth `(node_id, incarnation_id)`'s os-stopped row.
    pub fn key_of(node_id: &NodeId, incarnation_id: &IncarnationId) -> String {
        format!("{node_id}--{}", incarnation_id.as_str())
    }
}

/// The durable `data-dir-lost` fact on a birth's status row (R-FM2): the inherited data dir was
/// absent for `RDM_DATA_DIR_GONE_FLOORS` staleness floors while the provider reported the birth
/// `Exited`. Keyed by the exact birth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDataDirLostRow {
    /// The node.
    pub node_id: NodeId,
    /// The node's `path.name`.
    pub name: PathName,
    /// The exact birth.
    pub incarnation_id: IncarnationId,
    /// The absent data dir path.
    pub path: String,
    /// When the absent data dir was first observed, ms since the Unix epoch.
    pub since: u64,
}

impl NodeDataDirLostRow {
    /// The row's key: the exact birth.
    pub fn key(&self) -> String {
        Self::key_of(&self.node_id, &self.incarnation_id)
    }

    /// The key of the birth `(node_id, incarnation_id)`'s data-dir-lost row.
    pub fn key_of(node_id: &NodeId, incarnation_id: &IncarnationId) -> String {
        format!("{node_id}--{}", incarnation_id.as_str())
    }
}

/// The durable `data-dir-unreadable` fact on a birth's status row (R-FM2): the inherited data dir
/// exists on disk, but `node-key` is missing, unreadable, or corrupt. Keyed by the exact birth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDataDirUnreadableRow {
    /// The node.
    pub node_id: NodeId,
    /// The node's `path.name`.
    pub name: PathName,
    /// The exact birth.
    pub incarnation_id: IncarnationId,
    /// The broken data dir path.
    pub path: String,
    /// Why `node-key` could not be loaded from the data dir.
    pub cause: String,
}

impl NodeDataDirUnreadableRow {
    /// The row's key: the exact birth.
    pub fn key(&self) -> String {
        Self::key_of(&self.node_id, &self.incarnation_id)
    }

    /// The key of the birth `(node_id, incarnation_id)`'s data-dir-unreadable row.
    pub fn key_of(node_id: &NodeId, incarnation_id: &IncarnationId) -> String {
        format!("{node_id}--{}", incarnation_id.as_str())
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
    /// The events among the status rows of the mesh named `mesh` (`status/<mesh>/fabric/` of the store
    /// root: this store's own mesh or another's), each once: the fabric-primary's ready-for-traffic row
    /// among its own mesh's rows is read here by an admin of any mesh.
    async fn fabric_events_of(&self, mesh: &str) -> Result<Vec<FabricEventRow>, StorageError>;
    /// A blind put of the declared-state fact's own key.
    async fn put_node_declared(&self, row: &NodeDeclaredRow) -> Result<(), StorageError>;
    /// A blind tombstone of every declared-state key of the birth `(node_id, incarnation_id)`.
    async fn remove_node_declared(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), StorageError>;
    /// Every declared-state row held.
    async fn node_declared(&self) -> Result<Vec<NodeDeclaredRow>, StorageError>;
    /// Blind put of the operation's applied effect; a cycle begin never clears it.
    async fn put_node_operation(&self, row: &NodeOperationRow) -> Result<(), StorageError>;
    /// The operation effects held, including effects from earlier cycles of a live birth.
    async fn node_operations(&self) -> Result<Vec<NodeOperationRow>, StorageError>;
    /// Forget one operation effect only when its exact birth is forgotten.
    async fn remove_node_operation(&self, row: &NodeOperationRow) -> Result<(), StorageError>;
    /// A blind put of the birth's silence-status row.
    async fn put_node_silence(&self, row: &NodeSilenceRow) -> Result<(), StorageError>;
    /// Every birth's silence-status row.
    async fn node_silences(&self) -> Result<Vec<NodeSilenceRow>, StorageError>;
    /// A blind put of the birth's exit-proof row.
    async fn put_node_exit(&self, row: &NodeExitRow) -> Result<(), StorageError>;
    /// The exit-proof row of the birth `(node_id, incarnation_id)`, when one was put.
    async fn node_exit(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeExitRow>, StorageError>;
    /// Whether the status rows of the mesh named `mesh` (`status/<mesh>/` of the store root: this
    /// store's own mesh or another's) hold the Mesh status fact `row`: a point read of its key.
    async fn mesh_status_held_of(&self, mesh: &str, row: &MeshStatusRow) -> Result<bool, StorageError>;
    /// A blind put of the Mesh status fact `row` among the status rows of the mesh named `mesh`: the
    /// fabric-primary as acting primary of a leaving mesh with none left (mesh-delete.md step 7, R-MD1).
    async fn put_mesh_status_of(&self, mesh: &str, row: &MeshStatusRow) -> Result<(), StorageError>;
    /// The exit-proof row of the birth `(node_id, incarnation_id)` among the status rows of the mesh
    /// named `mesh` (this store's own or another's): a point read of its key.
    async fn node_exit_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeExitRow>, StorageError>;
    /// A blind put of the exit-proof row `row` among the status rows of the mesh named `mesh`.
    async fn put_node_exit_of(&self, mesh: &str, row: &NodeExitRow) -> Result<(), StorageError>;
    /// The `held-running` row of the birth `(node_id, incarnation_id)` among the status rows of the
    /// mesh named `mesh` (this store's own or another's): a point read of its key (R-FM1).
    async fn node_held_running_of(&self, _mesh: &str, _node_id: &NodeId, _incarnation_id: &IncarnationId) -> Result<Option<NodeHeldRunningRow>, StorageError> {
        Ok(None)
    }
    /// A blind put of the `held-running` row `row` among the status rows of the mesh named `mesh` (R-FM1).
    async fn put_node_held_running_of(&self, _mesh: &str, _row: &NodeHeldRunningRow) -> Result<(), StorageError> {
        Ok(())
    }
    /// Remove the `held-running` row of the birth `(node_id, incarnation_id)` among the status rows
    /// of the mesh named `mesh` (R-FM1).
    async fn remove_node_held_running_of(&self, _mesh: &str, _node_id: &NodeId, _incarnation_id: &IncarnationId) -> Result<(), StorageError> {
        Ok(())
    }
    /// The `os-stopped` row of the birth `(node_id, incarnation_id)` among the status rows of the
    /// mesh named `mesh` (this store's own or another's): a point read of its key (R-PV1).
    async fn node_os_stopped_of(&self, _mesh: &str, _node_id: &NodeId, _incarnation_id: &IncarnationId) -> Result<Option<NodeOsStoppedRow>, StorageError> {
        Ok(None)
    }
    /// A blind put of the `os-stopped` row `row` among the status rows of the mesh named `mesh` (R-PV1).
    async fn put_node_os_stopped_of(&self, _mesh: &str, _row: &NodeOsStoppedRow) -> Result<(), StorageError> {
        Ok(())
    }
    /// Remove the `os-stopped` row of the birth `(node_id, incarnation_id)` among the status rows of
    /// the mesh named `mesh` (R-PV1): `true` when this call removed a row that stood, so of two
    /// removers of one row exactly one is answered `true`.
    async fn remove_node_os_stopped_of(&self, _mesh: &str, _node_id: &NodeId, _incarnation_id: &IncarnationId) -> Result<bool, StorageError> {
        Ok(false)
    }
    /// The `data-dir-lost` row of the birth `(node_id, incarnation_id)` among the status rows of
    /// the mesh named `mesh` (R-FM2).
    async fn node_data_dir_lost_of(&self, _mesh: &str, _node_id: &NodeId, _incarnation_id: &IncarnationId) -> Result<Option<NodeDataDirLostRow>, StorageError> {
        Ok(None)
    }
    /// The latest `data-dir-lost` row at the `path.name` `name` among the status rows of the mesh named
    /// `mesh`: a point read of the `path.name` index its put writes beside the birth's row, which a
    /// later birth at the same path.name (the one recovered over a fresh data dir) is answered from.
    async fn node_data_dir_lost_at(&self, _mesh: &str, _name: &PathName) -> Result<Option<NodeDataDirLostRow>, StorageError> {
        Ok(None)
    }
    /// A blind put of the `data-dir-lost` row `row` among the status rows of the mesh named `mesh`
    /// (R-FM2), and of its `path.name` index entry.
    async fn put_node_data_dir_lost_of(&self, _mesh: &str, _row: &NodeDataDirLostRow) -> Result<(), StorageError> {
        Ok(())
    }
    /// The `data-dir-unreadable` row of the birth `(node_id, incarnation_id)` among the status rows
    /// of the mesh named `mesh` (R-FM2).
    async fn node_data_dir_unreadable_of(&self, _mesh: &str, _node_id: &NodeId, _incarnation_id: &IncarnationId) -> Result<Option<NodeDataDirUnreadableRow>, StorageError> {
        Ok(None)
    }
    /// The `data-dir-unreadable` row at the `path.name` `name` among the status rows of the mesh named
    /// `mesh`: a point read of the `path.name` index its put writes beside the birth's row, which a
    /// birth the view no longer holds (deleted by its recovery, held unrecovered) is answered from.
    async fn node_data_dir_unreadable_at(&self, _mesh: &str, _name: &PathName) -> Result<Option<NodeDataDirUnreadableRow>, StorageError> {
        Ok(None)
    }
    /// A blind put of the `data-dir-unreadable` row `row` among the status rows of the mesh named
    /// `mesh` (R-FM2), and of its `path.name` index entry.
    async fn put_node_data_dir_unreadable_of(&self, _mesh: &str, _row: &NodeDataDirUnreadableRow) -> Result<(), StorageError> {
        Ok(())
    }
    /// Remove the `data-dir-unreadable` row of the birth `(node_id, incarnation_id)` among the
    /// status rows of the mesh named `mesh` (R-FM2).
    async fn remove_node_data_dir_unreadable_of(&self, _mesh: &str, _node_id: &NodeId, _incarnation_id: &IncarnationId, _name: &PathName) -> Result<(), StorageError> {
        Ok(())
    }
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
    operations: Mutex<std::collections::BTreeMap<String, NodeOperationRow>>,
    silences: Mutex<std::collections::BTreeMap<String, NodeSilenceRow>>,
    exits: Mutex<std::collections::BTreeMap<String, NodeExitRow>>,
    /// The Mesh status rows of each mesh named through the `_of` puts: a store held in memory names
    /// no mesh of its own, so every named mesh keeps its own rows, and a read of a named mesh's rows
    /// also finds this store's own (their keys, a mesh id or an exact birth, are fabric-unique).
    meshes_named: Mutex<std::collections::BTreeMap<String, Vec<MeshStatusRow>>>,
    /// The exit-proof rows of each mesh named through the `_of` reads and puts, by row key.
    exits_named: Mutex<std::collections::BTreeMap<String, std::collections::BTreeMap<String, NodeExitRow>>>,
    /// The held-running rows of each mesh named through the `_of` reads and puts, by row key.
    held_running_named: Mutex<std::collections::BTreeMap<String, std::collections::BTreeMap<String, NodeHeldRunningRow>>>,
    /// The os-stopped rows of each mesh named through the `_of` reads and puts, by row key.
    os_stopped_named: Mutex<std::collections::BTreeMap<String, std::collections::BTreeMap<String, NodeOsStoppedRow>>>,
    /// The data-dir-lost rows of each mesh named through the `_of` reads and puts, by row key.
    data_dir_lost_named: Mutex<std::collections::BTreeMap<String, std::collections::BTreeMap<String, NodeDataDirLostRow>>>,
    /// The data-dir-unreadable rows of each mesh named through the `_of` reads and puts, by row key.
    data_dir_unreadable_named: Mutex<std::collections::BTreeMap<String, std::collections::BTreeMap<String, NodeDataDirUnreadableRow>>>,
    /// The `path.name` index of each mesh's data-dir-lost rows: the latest row at each path.name.
    data_dir_lost_at: Mutex<std::collections::BTreeMap<String, std::collections::BTreeMap<String, NodeDataDirLostRow>>>,
    /// The `path.name` index of each mesh's data-dir-unreadable rows.
    data_dir_unreadable_at: Mutex<std::collections::BTreeMap<String, std::collections::BTreeMap<String, NodeDataDirUnreadableRow>>>,
    /// The Fabric event rows of each mesh named through [`MemoryStatusStorage::put_fabric_event_of`]:
    /// another mesh's rows on the store root, which this store's own `fabric_events` never lists.
    events_named: Mutex<std::collections::BTreeMap<String, Vec<FabricEventRow>>>,
}

impl MemoryStatusStorage {
    /// Keep `row` among the status rows of the mesh named `mesh`, as that mesh's writer puts its own
    /// (the fabric-primary's ready-for-traffic row among its mesh's rows): another mesh's rows on the
    /// store root, read by name, never listed as this store's own.
    pub fn put_fabric_event_of(&self, mesh: &str, row: &FabricEventRow) {
        let mut named = self.events_named.lock().unwrap();
        let rows = named.entry(mesh.to_string()).or_default();
        if !rows.contains(row) {
            rows.push(row.clone());
        }
    }
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
    async fn fabric_events_of(&self, mesh: &str) -> Result<Vec<FabricEventRow>, StorageError> {
        // A store held in memory names no mesh of its own: a named mesh's rows, and this store's own.
        let mut rows = self.events_named.lock().unwrap().get(mesh).cloned().unwrap_or_default();
        for row in self.events.lock().unwrap().iter() {
            if !rows.contains(row) {
                rows.push(row.clone());
            }
        }
        Ok(rows)
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
    async fn put_node_operation(&self, row: &NodeOperationRow) -> Result<(), StorageError> {
        self.operations.lock().unwrap().insert(row.key(), row.clone());
        Ok(())
    }
    async fn node_operations(&self) -> Result<Vec<NodeOperationRow>, StorageError> {
        Ok(self.operations.lock().unwrap().values().cloned().collect())
    }
    async fn remove_node_operation(&self, row: &NodeOperationRow) -> Result<(), StorageError> {
        self.operations.lock().unwrap().remove(&row.key());
        Ok(())
    }
    async fn put_node_silence(&self, row: &NodeSilenceRow) -> Result<(), StorageError> {
        self.silences.lock().unwrap().insert(row.key(), row.clone());
        Ok(())
    }
    async fn node_silences(&self) -> Result<Vec<NodeSilenceRow>, StorageError> {
        Ok(self.silences.lock().unwrap().values().cloned().collect())
    }
    async fn put_node_exit(&self, row: &NodeExitRow) -> Result<(), StorageError> {
        self.exits.lock().unwrap().insert(row.key(), row.clone());
        Ok(())
    }
    async fn node_exit(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeExitRow>, StorageError> {
        Ok(self.exits.lock().unwrap().get(&NodeExitRow::key_of(node_id, incarnation_id)).cloned())
    }
    async fn mesh_status_held_of(&self, mesh: &str, row: &MeshStatusRow) -> Result<bool, StorageError> {
        Ok(self.meshes_named.lock().unwrap().get(mesh).is_some_and(|rows| rows.contains(row)) || self.meshes.lock().unwrap().contains(row))
    }
    async fn put_mesh_status_of(&self, mesh: &str, row: &MeshStatusRow) -> Result<(), StorageError> {
        let mut named = self.meshes_named.lock().unwrap();
        let rows = named.entry(mesh.to_string()).or_default();
        if !rows.contains(row) {
            rows.push(row.clone());
        }
        Ok(())
    }
    async fn node_exit_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeExitRow>, StorageError> {
        let key = NodeExitRow::key_of(node_id, incarnation_id);
        let named = self.exits_named.lock().unwrap().get(mesh).and_then(|rows| rows.get(&key).cloned());
        Ok(named.or_else(|| self.exits.lock().unwrap().get(&key).cloned()))
    }
    async fn put_node_exit_of(&self, mesh: &str, row: &NodeExitRow) -> Result<(), StorageError> {
        self.exits_named.lock().unwrap().entry(mesh.to_string()).or_default().insert(row.key(), row.clone());
        Ok(())
    }
    async fn node_held_running_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeHeldRunningRow>, StorageError> {
        let key = NodeHeldRunningRow::key_of(node_id, incarnation_id);
        Ok(self.held_running_named.lock().unwrap().get(mesh).and_then(|rows| rows.get(&key).cloned()))
    }
    async fn put_node_held_running_of(&self, mesh: &str, row: &NodeHeldRunningRow) -> Result<(), StorageError> {
        self.held_running_named.lock().unwrap().entry(mesh.to_string()).or_default().insert(row.key(), row.clone());
        Ok(())
    }
    async fn remove_node_held_running_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), StorageError> {
        let key = NodeHeldRunningRow::key_of(node_id, incarnation_id);
        if let Some(rows) = self.held_running_named.lock().unwrap().get_mut(mesh) {
            rows.remove(&key);
        }
        Ok(())
    }
    async fn node_os_stopped_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeOsStoppedRow>, StorageError> {
        let key = NodeOsStoppedRow::key_of(node_id, incarnation_id);
        Ok(self.os_stopped_named.lock().unwrap().get(mesh).and_then(|rows| rows.get(&key).cloned()))
    }
    async fn put_node_os_stopped_of(&self, mesh: &str, row: &NodeOsStoppedRow) -> Result<(), StorageError> {
        self.os_stopped_named.lock().unwrap().entry(mesh.to_string()).or_default().insert(row.key(), row.clone());
        Ok(())
    }
    async fn remove_node_os_stopped_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<bool, StorageError> {
        let key = NodeOsStoppedRow::key_of(node_id, incarnation_id);
        Ok(self.os_stopped_named.lock().unwrap().get_mut(mesh).and_then(|rows| rows.remove(&key)).is_some())
    }
    async fn node_data_dir_lost_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeDataDirLostRow>, StorageError> {
        let key = NodeDataDirLostRow::key_of(node_id, incarnation_id);
        Ok(self.data_dir_lost_named.lock().unwrap().get(mesh).and_then(|rows| rows.get(&key).cloned()))
    }
    async fn node_data_dir_lost_at(&self, mesh: &str, name: &PathName) -> Result<Option<NodeDataDirLostRow>, StorageError> {
        Ok(self.data_dir_lost_at.lock().unwrap().get(mesh).and_then(|rows| rows.get(&name.to_string()).cloned()))
    }
    async fn put_node_data_dir_lost_of(&self, mesh: &str, row: &NodeDataDirLostRow) -> Result<(), StorageError> {
        self.data_dir_lost_named.lock().unwrap().entry(mesh.to_string()).or_default().insert(row.key(), row.clone());
        self.data_dir_lost_at.lock().unwrap().entry(mesh.to_string()).or_default().insert(row.name.to_string(), row.clone());
        Ok(())
    }
    async fn node_data_dir_unreadable_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeDataDirUnreadableRow>, StorageError> {
        let key = NodeDataDirUnreadableRow::key_of(node_id, incarnation_id);
        Ok(self.data_dir_unreadable_named.lock().unwrap().get(mesh).and_then(|rows| rows.get(&key).cloned()))
    }
    async fn node_data_dir_unreadable_at(&self, mesh: &str, name: &PathName) -> Result<Option<NodeDataDirUnreadableRow>, StorageError> {
        Ok(self.data_dir_unreadable_at.lock().unwrap().get(mesh).and_then(|rows| rows.get(&name.to_string()).cloned()))
    }
    async fn put_node_data_dir_unreadable_of(&self, mesh: &str, row: &NodeDataDirUnreadableRow) -> Result<(), StorageError> {
        self.data_dir_unreadable_named.lock().unwrap().entry(mesh.to_string()).or_default().insert(row.key(), row.clone());
        self.data_dir_unreadable_at.lock().unwrap().entry(mesh.to_string()).or_default().insert(row.name.to_string(), row.clone());
        Ok(())
    }
    async fn remove_node_data_dir_unreadable_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId, name: &PathName) -> Result<(), StorageError> {
        let key = NodeDataDirUnreadableRow::key_of(node_id, incarnation_id);
        if let Some(rows) = self.data_dir_unreadable_named.lock().unwrap().get_mut(mesh) {
            rows.remove(&key);
        }
        if let Some(rows) = self.data_dir_unreadable_at.lock().unwrap().get_mut(mesh) {
            rows.remove(&name.to_string());
        }
        Ok(())
    }
}

const MESH_STATUS_FORMAT: &str = "mesh-status/1";
const FABRIC_EVENT_FORMAT: &str = "fabric-event/1";
const NODE_DECLARED_FORMAT: &str = "node-declared/3";
const NODE_SILENCE_FORMAT: &str = "node-silence/1";
const NODE_EXIT_FORMAT: &str = "node-exit/1";
const NODE_HELD_RUNNING_FORMAT: &str = "node-held-running/1";
const NODE_OS_STOPPED_FORMAT: &str = "node-os-stopped/1";
const NODE_DATA_DIR_LOST_FORMAT: &str = "node-data-dir-lost/1";
const NODE_DATA_DIR_UNREADABLE_FORMAT: &str = "node-data-dir-unreadable/1";

/// One row per file under `status/<mesh>/` of the fabric's store root (`crate::store_root`):
/// `meshes/<mesh_id>-<state>.json`, `fabric/<fabric_id>-<hex of the event key>.json`,
/// `declared/<node_id>--<incarnation>--<state>.json`, `silence/<node_id>--<incarnation>.json`,
/// `exits/<node_id>--<incarnation>.json`, `held-running/<node_id>--<incarnation>.json`,
/// `os-stopped/<node_id>--<incarnation>.json`, `data-dir-lost/<node_id>--<incarnation>.json` and
/// `data-dir-unreadable/<node_id>--<incarnation>.json`. The
/// mesh's seat holder writes them (node-identity-and-state.md §4.3); every admin of the mesh reads
/// the same rows.
#[derive(Debug)]
pub struct FileStatusStorage {
    meshes: FileRecords,
    fabric: FileRecords,
    declared: FileRecords,
    operations: FileRecords,
    silence: FileRecords,
    exits: FileRecords,
    held_running: FileRecords,
    os_stopped: FileRecords,
    data_dir_lost: FileRecords,
    data_dir_unreadable: FileRecords,
    /// The `path.name` index of the data-dir-lost rows: the latest row at each path.name.
    data_dir_lost_at: FileRecords,
    /// The `path.name` index of the data-dir-unreadable rows.
    data_dir_unreadable_at: FileRecords,
    /// This store's own mesh: its rows are the fields above.
    mesh: String,
    root: crate::store_root::StoreRoot,
    writer: IncarnationId,
    /// Every other mesh's status rows this admin has read or written (`meshes`, `exits`, `fabric`), by mesh
    /// and directory, opened on first use.
    others: Mutex<std::collections::BTreeMap<(String, &'static str), std::sync::Arc<FileRecords>>>,
    /// Set when the fabric-primary seat this admin held moved (a seat transfer's cut-off): every
    /// other mesh's status rows, those opened later included, refuse writes naming it.
    others_cut_off: Mutex<Option<String>>,
}

impl FileStatusStorage {
    /// Open the store under `status/<mesh>/` of the fabric's store root (and another mesh's
    /// `status/<mesh>/`, on first use), written by the process born as `writer`.
    pub async fn open(root: &crate::store_root::StoreRoot, mesh: &str, writer: &IncarnationId) -> Result<Self, StorageError> {
        let base = root.status(mesh);
        Ok(Self {
            meshes: root.records(&base, "meshes", writer).await?,
            fabric: root.records(&base, "fabric", writer).await?,
            declared: root.records(&base, "declared", writer).await?,
            operations: root.records(&base, "operations", writer).await?,
            silence: root.records(&base, "silence", writer).await?,
            exits: root.records(&base, "exits", writer).await?,
            held_running: root.records(&base, "held-running", writer).await?,
            os_stopped: root.records(&base, "os-stopped", writer).await?,
            data_dir_lost: root.records(&base, "data-dir-lost", writer).await?,
            data_dir_unreadable: root.records(&base, "data-dir-unreadable", writer).await?,
            data_dir_lost_at: root.records(&base, "data-dir-lost-at", writer).await?,
            data_dir_unreadable_at: root.records(&base, "data-dir-unreadable-at", writer).await?,
            mesh: mesh.to_string(),
            root: root.clone(),
            writer: writer.clone(),
            others: Mutex::new(std::collections::BTreeMap::new()),
            others_cut_off: Mutex::new(None),
        })
    }

    /// The records the mesh-primary seat writes (every status row of the mesh): what a seat
    /// transfer moving that seat cuts off (R14 item 4).
    pub fn mesh_primary_records(&self) -> [&FileRecords; 12] {
        [
            &self.meshes,
            &self.fabric,
            &self.declared,
            &self.operations,
            &self.silence,
            &self.exits,
            &self.held_running,
            &self.os_stopped,
            &self.data_dir_lost,
            &self.data_dir_unreadable,
            &self.data_dir_lost_at,
            &self.data_dir_unreadable_at,
        ]
    }

    /// The `dir` status rows (`meshes`, `exits`, `fabric`) of another mesh `mesh` on the shared root, opened
    /// outside the lock and kept once under it (the cut-off read under the same lock).
    async fn other(&self, mesh: &str, dir: &'static str) -> Result<std::sync::Arc<FileRecords>, StorageError> {
        let key = (mesh.to_string(), dir);
        if let Some(m) = self.others.lock().unwrap_or_else(|e| e.into_inner()).get(&key) {
            return Ok(m.clone());
        }
        let opened = std::sync::Arc::new(self.root.records(&self.root.status(mesh), dir, &self.writer).await?);
        let mut others = self.others.lock().unwrap_or_else(|e| e.into_inner());
        let m = others.entry(key).or_insert(opened).clone();
        if let Some(reason) = self.others_cut_off.lock().unwrap_or_else(|e| e.into_inner()).clone() {
            m.cut_off(reason);
        }
        Ok(m)
    }

    /// The fabric-primary seat moves (R14 item 4): cut off this store's writes to every other mesh's
    /// status rows, those it opens later included, and wait for every write already issued to them.
    pub async fn cut_off_other_meshes(&self, reason: &str) {
        let opened: Vec<std::sync::Arc<FileRecords>> = {
            let others = self.others.lock().unwrap_or_else(|e| e.into_inner());
            *self.others_cut_off.lock().unwrap_or_else(|e| e.into_inner()) = Some(reason.to_string());
            others.values().cloned().collect()
        };
        let stores: Vec<&FileRecords> = opened.iter().map(|m| &**m).collect();
        crate::record_store::cut_off_and_drain(&stores, reason).await;
    }

    /// Lift [`Self::cut_off_other_meshes`]: the transfer it was set for committed nothing. The
    /// first store whose kept rows could not be written is named; every store is lifted.
    pub async fn lift_other_meshes(&self) -> Result<(), StorageError> {
        let opened: Vec<std::sync::Arc<FileRecords>> = {
            let others = self.others.lock().unwrap_or_else(|e| e.into_inner());
            *self.others_cut_off.lock().unwrap_or_else(|e| e.into_inner()) = None;
            others.values().cloned().collect()
        };
        let mut first = Ok(());
        for s in opened {
            if let (Err(e), true) = (s.lift_cut_off().await, first.is_ok()) {
                first = Err(e);
            }
        }
        first
    }

    /// The seat transfer [`Self::cut_off_other_meshes`] was set for committed: every row the other
    /// meshes' status stores still hold (written as acting primary before the fabric's first
    /// state-commit) is dropped, never written. Returns each store that dropped rows, with how many.
    pub async fn drop_other_meshes(&self) -> Vec<(String, usize)> {
        let opened: Vec<std::sync::Arc<FileRecords>> = self.others.lock().unwrap_or_else(|e| e.into_inner()).values().cloned().collect();
        let mut dropped = Vec::new();
        for s in opened {
            let n = s.drop_held().await;
            if n > 0 {
                dropped.push((s.store_name(), n));
            }
        }
        dropped
    }
}

#[async_trait]
impl StatusStorage for FileStatusStorage {
    async fn put_mesh_status(&self, row: &MeshStatusRow) -> Result<(), StorageError> {
        self.meshes.write(&row.key(), MESH_STATUS_FORMAT, row).await
    }
    async fn mesh_statuses(&self) -> Result<HashMap<MeshId, MeshState>, StorageError> {
        Ok(fold_meshes(self.meshes.list::<MeshStatusRow>(MESH_STATUS_FORMAT).await?))
    }
    async fn put_fabric_event(&self, row: &FabricEventRow) -> Result<(), StorageError> {
        self.fabric.write(&row.key(), FABRIC_EVENT_FORMAT, row).await
    }
    async fn fabric_events(&self) -> Result<Vec<FabricEventRow>, StorageError> {
        self.fabric.list(FABRIC_EVENT_FORMAT).await
    }
    async fn fabric_events_of(&self, mesh: &str) -> Result<Vec<FabricEventRow>, StorageError> {
        if mesh == self.mesh {
            return self.fabric_events().await;
        }
        self.other(mesh, "fabric").await?.list(FABRIC_EVENT_FORMAT).await
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
        self.declared.list(NODE_DECLARED_FORMAT).await
    }
    async fn put_node_operation(&self, row: &NodeOperationRow) -> Result<(), StorageError> {
        self.operations.write(&row.key(), "node-operation/1", row).await
    }
    async fn node_operations(&self) -> Result<Vec<NodeOperationRow>, StorageError> {
        self.operations.list("node-operation/1").await
    }
    async fn remove_node_operation(&self, row: &NodeOperationRow) -> Result<(), StorageError> {
        self.operations.remove(&row.key()).await.map(|_| ())
    }
    async fn put_node_silence(&self, row: &NodeSilenceRow) -> Result<(), StorageError> {
        self.silence.write(&row.key(), NODE_SILENCE_FORMAT, row).await
    }
    async fn node_silences(&self) -> Result<Vec<NodeSilenceRow>, StorageError> {
        self.silence.list(NODE_SILENCE_FORMAT).await
    }
    async fn put_node_exit(&self, row: &NodeExitRow) -> Result<(), StorageError> {
        self.exits.write(&row.key(), NODE_EXIT_FORMAT, row).await
    }
    async fn node_exit(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeExitRow>, StorageError> {
        self.exits.read(&NodeExitRow::key_of(node_id, incarnation_id), NODE_EXIT_FORMAT).await
    }
    async fn mesh_status_held_of(&self, mesh: &str, row: &MeshStatusRow) -> Result<bool, StorageError> {
        if mesh == self.mesh {
            return self.meshes.holds(&row.key()).await;
        }
        self.other(mesh, "meshes").await?.holds(&row.key()).await
    }
    async fn put_mesh_status_of(&self, mesh: &str, row: &MeshStatusRow) -> Result<(), StorageError> {
        if mesh == self.mesh {
            return self.meshes.write(&row.key(), MESH_STATUS_FORMAT, row).await;
        }
        self.other(mesh, "meshes").await?.write(&row.key(), MESH_STATUS_FORMAT, row).await
    }
    async fn node_exit_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeExitRow>, StorageError> {
        if mesh == self.mesh {
            return self.node_exit(node_id, incarnation_id).await;
        }
        self.other(mesh, "exits").await?.read(&NodeExitRow::key_of(node_id, incarnation_id), NODE_EXIT_FORMAT).await
    }
    async fn put_node_exit_of(&self, mesh: &str, row: &NodeExitRow) -> Result<(), StorageError> {
        if mesh == self.mesh {
            return self.put_node_exit(row).await;
        }
        self.other(mesh, "exits").await?.write(&row.key(), NODE_EXIT_FORMAT, row).await
    }
    async fn node_held_running_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeHeldRunningRow>, StorageError> {
        let key = NodeHeldRunningRow::key_of(node_id, incarnation_id);
        if mesh == self.mesh {
            return self.held_running.read(&key, NODE_HELD_RUNNING_FORMAT).await;
        }
        self.other(mesh, "held-running").await?.read(&key, NODE_HELD_RUNNING_FORMAT).await
    }
    async fn put_node_held_running_of(&self, mesh: &str, row: &NodeHeldRunningRow) -> Result<(), StorageError> {
        if mesh == self.mesh {
            return self.held_running.write(&row.key(), NODE_HELD_RUNNING_FORMAT, row).await;
        }
        self.other(mesh, "held-running").await?.write(&row.key(), NODE_HELD_RUNNING_FORMAT, row).await
    }
    async fn remove_node_held_running_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), StorageError> {
        let key = NodeHeldRunningRow::key_of(node_id, incarnation_id);
        if mesh == self.mesh {
            return self.held_running.remove(&key).await.map(|_| ());
        }
        self.other(mesh, "held-running").await?.remove(&key).await.map(|_| ())
    }
    async fn node_os_stopped_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeOsStoppedRow>, StorageError> {
        let key = NodeOsStoppedRow::key_of(node_id, incarnation_id);
        if mesh == self.mesh {
            return self.os_stopped.read(&key, NODE_OS_STOPPED_FORMAT).await;
        }
        self.other(mesh, "os-stopped").await?.read(&key, NODE_OS_STOPPED_FORMAT).await
    }
    async fn put_node_os_stopped_of(&self, mesh: &str, row: &NodeOsStoppedRow) -> Result<(), StorageError> {
        if mesh == self.mesh {
            return self.os_stopped.write(&row.key(), NODE_OS_STOPPED_FORMAT, row).await;
        }
        self.other(mesh, "os-stopped").await?.write(&row.key(), NODE_OS_STOPPED_FORMAT, row).await
    }
    async fn remove_node_os_stopped_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<bool, StorageError> {
        let key = NodeOsStoppedRow::key_of(node_id, incarnation_id);
        if mesh == self.mesh {
            return self.os_stopped.remove(&key).await;
        }
        self.other(mesh, "os-stopped").await?.remove(&key).await
    }
    async fn node_data_dir_lost_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeDataDirLostRow>, StorageError> {
        let key = NodeDataDirLostRow::key_of(node_id, incarnation_id);
        if mesh == self.mesh {
            return self.data_dir_lost.read(&key, NODE_DATA_DIR_LOST_FORMAT).await;
        }
        self.other(mesh, "data-dir-lost").await?.read(&key, NODE_DATA_DIR_LOST_FORMAT).await
    }
    async fn node_data_dir_lost_at(&self, mesh: &str, name: &PathName) -> Result<Option<NodeDataDirLostRow>, StorageError> {
        if mesh == self.mesh {
            return self.data_dir_lost_at.read(&name.to_string(), NODE_DATA_DIR_LOST_FORMAT).await;
        }
        self.other(mesh, "data-dir-lost-at").await?.read(&name.to_string(), NODE_DATA_DIR_LOST_FORMAT).await
    }
    async fn put_node_data_dir_lost_of(&self, mesh: &str, row: &NodeDataDirLostRow) -> Result<(), StorageError> {
        // The birth's row, then its path.name index entry: two keys, each a blind put of its own.
        if mesh == self.mesh {
            self.data_dir_lost.write(&row.key(), NODE_DATA_DIR_LOST_FORMAT, row).await?;
            return self.data_dir_lost_at.write(&row.name.to_string(), NODE_DATA_DIR_LOST_FORMAT, row).await;
        }
        self.other(mesh, "data-dir-lost").await?.write(&row.key(), NODE_DATA_DIR_LOST_FORMAT, row).await?;
        self.other(mesh, "data-dir-lost-at").await?.write(&row.name.to_string(), NODE_DATA_DIR_LOST_FORMAT, row).await
    }
    async fn node_data_dir_unreadable_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeDataDirUnreadableRow>, StorageError> {
        let key = NodeDataDirUnreadableRow::key_of(node_id, incarnation_id);
        if mesh == self.mesh {
            return self.data_dir_unreadable.read(&key, NODE_DATA_DIR_UNREADABLE_FORMAT).await;
        }
        self.other(mesh, "data-dir-unreadable").await?.read(&key, NODE_DATA_DIR_UNREADABLE_FORMAT).await
    }
    async fn node_data_dir_unreadable_at(&self, mesh: &str, name: &PathName) -> Result<Option<NodeDataDirUnreadableRow>, StorageError> {
        if mesh == self.mesh {
            return self.data_dir_unreadable_at.read(&name.to_string(), NODE_DATA_DIR_UNREADABLE_FORMAT).await;
        }
        self.other(mesh, "data-dir-unreadable-at").await?.read(&name.to_string(), NODE_DATA_DIR_UNREADABLE_FORMAT).await
    }
    async fn put_node_data_dir_unreadable_of(&self, mesh: &str, row: &NodeDataDirUnreadableRow) -> Result<(), StorageError> {
        // The birth's row, then its path.name index entry: two keys, each a blind put of its own.
        if mesh == self.mesh {
            self.data_dir_unreadable.write(&row.key(), NODE_DATA_DIR_UNREADABLE_FORMAT, row).await?;
            return self.data_dir_unreadable_at.write(&row.name.to_string(), NODE_DATA_DIR_UNREADABLE_FORMAT, row).await;
        }
        self.other(mesh, "data-dir-unreadable").await?.write(&row.key(), NODE_DATA_DIR_UNREADABLE_FORMAT, row).await?;
        self.other(mesh, "data-dir-unreadable-at").await?.write(&row.name.to_string(), NODE_DATA_DIR_UNREADABLE_FORMAT, row).await
    }
    async fn remove_node_data_dir_unreadable_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId, name: &PathName) -> Result<(), StorageError> {
        // Tombstones of the birth's key and of its path.name index entry, each blind.
        let key = NodeDataDirUnreadableRow::key_of(node_id, incarnation_id);
        if mesh == self.mesh {
            self.data_dir_unreadable.remove(&key).await?;
            return self.data_dir_unreadable_at.remove(&name.to_string()).await.map(|_| ());
        }
        self.other(mesh, "data-dir-unreadable").await?.remove(&key).await?;
        self.other(mesh, "data-dir-unreadable-at").await?.remove(&name.to_string()).await.map(|_| ())
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
            let s = FileStatusStorage::open(&crate::store_root::StoreRoot::at(&dir), "mesh1", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap();
            // Written out of order: the later fact first, a lagging earlier one after it.
            s.put_mesh_status(&MeshStatusRow { mesh_id: m.clone(), state: MeshState::ReadyForTraffic }).await.unwrap();
            s.put_mesh_status(&MeshStatusRow { mesh_id: m.clone(), state: MeshState::Pending }).await.unwrap();
            s.put_mesh_status(&MeshStatusRow { mesh_id: m.clone(), state: MeshState::Pending }).await.unwrap();
            s.put_fabric_event(&FabricEventRow { fabric_id: f.clone(), event: "shutdown-initiated:mesh1.admin.1".into() }).await.unwrap();
            s.put_fabric_event(&FabricEventRow { fabric_id: f.clone(), event: "shutdown-initiated:mesh1.admin.1".into() }).await.unwrap();
        }
        let s = FileStatusStorage::open(&crate::store_root::StoreRoot::at(&dir), "mesh1", &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap();
        assert_eq!(s.mesh_statuses().await.unwrap().get(&m), Some(&MeshState::ReadyForTraffic), "the greatest state wins whatever the write order");
        assert_eq!(s.fabric_events().await.unwrap().len(), 1, "one row per event key");
        std::fs::remove_dir_all(&dir).ok();
    }
}
