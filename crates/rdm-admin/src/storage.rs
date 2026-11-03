//! `mesh.storage`, `nodes.storage` and `connections.storage`: node-admin control state behind
//! storage boundaries, beside `fabric.storage` (`crate::fabric_storage`) and `builds.storage`
//! (`crate::build_state::FileJournal`).
//!
//! Each boundary is a trait with a memory implementation (runs that need no restart survival) and
//! a file implementation in the admin's own data dir (`crate::record_store`: one record per file,
//! temp + fsync + rename, an unrecognised file refused by name). Rafka binds its own production
//! backends to the same traits.
//!
//! Every object is stored whole (storage-whole-records.md): its writer holds it in its cache, applies
//! a change to that copy, writes the whole object, and replaces its cached copy once storage answers.
//! No write reads storage first; storage is read only to fill a cache.
//!
//! - `mesh.storage`: each Mesh record at its id (its identity and its one state) and each
//!   live mesh's record by its name.
//! - `nodes.storage`: each node's record at its id (its row and the facts of its births: declared
//!   states, operation effects, silence, data-dir facts, cert dates, the birth's address and the
//!   primaries recorded for it) and its record at its `(path, name)`, through which the pair resolves (R-SR5); and
//!   the runtimes this admin started, in its own data dir. A birth's address on its record is a hint:
//!   never topology, never death proof. The admin's own row (NodeId, path and name, the incarnation it last
//!   ran, its transport identity), so a restart is the same logical node, is its own seed in its data
//!   dir, read and written by its storage backend (`crate::storage_backend::StorageBackend::own_row`).
//! - `connections.storage`: the latest connection fact per (source, destination, kind).

use async_trait::async_trait;
use crate::model::{FabricId, IncarnationId, MeshId, NodeId, PathName, EndpointId};
pub use rdm_client::NameHeld;
#[cfg(feature = "file-backend")]
use crate::record_store::FileRecords;
use crate::storage_error::StorageError;
use serde::{Deserialize, Serialize};
use rdm_core::status::{MeshState, NodeState};
use std::collections::{BTreeMap, BTreeSet, HashMap};
#[cfg(feature = "file-backend")]
use std::path::Path;
use std::sync::Mutex;

// ---------------------------------------------------------------- mesh.storage

/// A Mesh record (node-identity-and-state.md §2.7): the mesh's identity and its one `state`. Kept at
/// its mesh id and written whole by the mesh's writer (its primary; the fabric-primary while the mesh
/// has none) from the copy it caches (storage-whole-records.md, "The rule"), each move of its state
/// also an event in the events store (`rdm.mesh.<state>`, build.md §5); its name is read from the live records
/// ([`MeshStorage::mesh_named`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeshRecord {
    /// The mesh's minted id: the record's key.
    pub mesh_id: MeshId,
    /// The mesh's name, unique among the live meshes.
    pub name: String,
    /// The mesh's state (states.md, "Who sets each state"): it only moves forward, pending -> ready
    /// -> deleting -> deleted. Deleted is set by the mesh's removal (mesh.deleted at its primary, or
    /// mesh-left at the fabric-primary as acting primary): its name is free (R-MN1 item 2) and its id
    /// is never written live again.
    pub state: MeshState,
}

impl MeshRecord {
    /// A new mesh's record: `pending`.
    pub fn new(mesh_id: MeshId, name: impl Into<String>) -> Self {
        Self { mesh_id, name: name.into(), state: MeshState::Pending }
    }

    /// Whether the mesh is deleted: its name free, its id never written live again.
    pub fn is_deleted(&self) -> bool {
        self.state == MeshState::Deleted
    }
}

/// What a write of a mesh's record did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshWritten {
    /// The record was written: new, or changed (its deleted write).
    Written,
    /// This mesh's record stands exactly as given: nothing is written (a retry, a replaced primary).
    Held,
}

/// Why a write of a mesh's record wrote nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshRefused {
    /// Another live mesh's record holds the name: it is free only once that mesh is deleted.
    NameHeld {
        /// The mesh's name.
        name: String,
        /// The mesh that holds it.
        by: MeshId,
    },
    /// This mesh is deleted: a deleted mesh is never written live again and its id is never reissued
    /// (states.md); only its deleted record is ever written again.
    MeshDeleted {
        /// The mesh's name.
        name: String,
        /// The deleted mesh.
        mesh_id: MeshId,
    },
}

impl MeshRefused {
    /// The span outcome that names the refusal.
    pub fn outcome(&self) -> &'static str {
        match self {
            Self::NameHeld { .. } => "name-held",
            Self::MeshDeleted { .. } => "mesh-deleted",
        }
    }
}

impl std::fmt::Display for MeshRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NameHeld { name, by } => write!(f, "the mesh name {name} is held by mesh {by}: it is free only once that mesh is deleted"),
            Self::MeshDeleted { name, mesh_id } => write!(f, "mesh {mesh_id} at {name} is deleted: a deleted mesh is never written live again"),
        }
    }
}

impl std::error::Error for MeshRefused {}

/// Write `record`, the whole record its writer holds (node-identity-and-state.md §2.7; Q109: the
/// storage decides nothing, the writer does). `held` is the writer's cached copy of this mesh's record,
/// or `None` for a mesh it holds none of. A deleted mesh is never written live again (`MeshDeleted`). A live
/// record's name is checked against the records: another live mesh's record at the name refuses it
/// `NameHeld` naming that mesh, and nothing is written; a deleted record frees its name (R-MN1). The
/// record is then written whole unless it stands exactly as `held`.
pub async fn write_mesh(meshes: &dyn MeshStorage, held: Option<&MeshRecord>, record: &MeshRecord) -> Result<Result<MeshWritten, MeshRefused>, StorageError> {
    if held.is_some_and(|h| h.is_deleted()) && !record.is_deleted() {
        return Ok(Err(MeshRefused::MeshDeleted { name: record.name.clone(), mesh_id: record.mesh_id.clone() }));
    }
    if !record.is_deleted() {
        if let Some(by) = meshes.mesh_named(&record.name).await?.filter(|h| h.mesh_id != record.mesh_id) {
            return Ok(Err(MeshRefused::NameHeld { name: record.name.clone(), by: by.mesh_id }));
        }
    }
    if held == Some(record) {
        return Ok(Ok(MeshWritten::Held));
    }
    meshes.put_mesh(record).await?;
    Ok(Ok(MeshWritten::Written))
}

/// The one live record of `records` named `name`; two are refused by name (`file` the store they were
/// read from).
pub fn mesh_holding(records: impl IntoIterator<Item = MeshRecord>, name: &str, file: &str) -> Result<Option<MeshRecord>, StorageError> {
    let mut holding = records.into_iter().filter(|r| r.name == name && !r.is_deleted());
    let first = holding.next();
    if let (Some(a), Some(b)) = (&first, holding.next()) {
        return Err(StorageError::Unrecognised { file: file.to_string(), reason: format!("two live mesh records hold the name {name}: meshes {} and {}", a.mesh_id, b.mesh_id) });
    }
    Ok(first)
}


/// The durable record of the meshes: each mesh's record at its id, and each mesh's in-flight row.
#[async_trait]
pub trait MeshStorage: Send + Sync {
    /// The record at `mesh_id`: a point read of its key; `None` when none was ever written.
    async fn mesh(&self, mesh_id: &MeshId) -> Result<Option<MeshRecord>, StorageError>;
    /// The live record named `name`, if one is: read from the records ([`mesh_holding`]).
    async fn mesh_named(&self, name: &str) -> Result<Option<MeshRecord>, StorageError>;
    /// Write `record` whole at its id, as given (Q109: storage stores and returns what it is given).
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError>;
    /// Every mesh record held.
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError>;
}

/// A mesh store held in memory: the records at their ids.
#[derive(Debug, Default)]
pub struct MemoryMeshStorage {
    records: Mutex<BTreeMap<String, MeshRecord>>,
}

#[async_trait]
impl MeshStorage for MemoryMeshStorage {
    async fn mesh(&self, mesh_id: &MeshId) -> Result<Option<MeshRecord>, StorageError> {
        Ok(self.records.lock().unwrap().get(mesh_id.as_str()).cloned())
    }
    async fn mesh_named(&self, name: &str) -> Result<Option<MeshRecord>, StorageError> {
        mesh_holding(self.records.lock().unwrap().values().cloned(), name, "mesh.storage (memory)")
    }
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError> {
        self.records.lock().unwrap().insert(record.mesh_id.to_string(), record.clone());
        Ok(())
    }
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
        Ok(self.records.lock().unwrap().values().cloned().collect())
    }
}

#[cfg(feature = "file-backend")]
const MESH_FORMAT: &str = "mesh-record/4";
#[cfg(feature = "file-backend")]
const MESH_KIND: &str = "mesh-record";


#[cfg(feature = "file-backend")]
/// Each write of a Mesh record, whole, one line of the root's `meshes.jsonl` at its mesh id (R-SR1,
/// R-SR2), the latest line the record.
pub struct FileMeshStorage {
    records: crate::store_log::StoreLog,
    log_writer: crate::store_log::LogWriter,
}

#[cfg(feature = "file-backend")]
impl std::fmt::Debug for FileMeshStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileMeshStorage").field("records", &self.records).finish_non_exhaustive()
    }
}

#[cfg(feature = "file-backend")]
impl FileMeshStorage {
    /// Open the store under the fabric's store root, written by `writer`.
    pub async fn open(root: &crate::store_root::StoreRoot, writer: &crate::store_log::LogWriter) -> Result<Self, StorageError> {
        Ok(Self { records: root.log(crate::store_root::MESHES_FILE, crate::store_log::Formats::new().with::<MeshRecord>(MESH_FORMAT)).await?, log_writer: writer.clone() })
    }

    /// Finishes every write of the Mesh records this store has issued
    /// (node-primary-update.md step 3).
    pub async fn finish_issued_writes(&self) {
        self.records.finish_issued_writes().await;
    }

}

#[cfg(feature = "file-backend")]
#[async_trait]
impl MeshStorage for FileMeshStorage {
    async fn mesh(&self, mesh_id: &MeshId) -> Result<Option<MeshRecord>, StorageError> {
        self.records.latest(MESH_KIND, mesh_id.as_str()).await?.map(|l| l.decode(self.records.path())).transpose()
    }
    async fn mesh_named(&self, name: &str) -> Result<Option<MeshRecord>, StorageError> {
        let records = self.meshes().await?;
        mesh_holding(records, name, &self.records.path().display().to_string())
    }
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError> {
        self.records.append(&self.log_writer, MESH_FORMAT, record.mesh_id.as_str(), record).await
    }
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
        self.records.latest_of(MESH_KIND).await?.iter().map(|l| l.decode(self.records.path())).collect()
    }
}

// ---------------------------------------------------------------- facts on a node's record

pub use rdm_node::nodes_storage::{AppTypes, Allocation, BirthFacts, BirthRef, CertDates, DataDirLost, DataDirUnreadable, NodeOperation, NodeRow, RuntimeRow, SigningCertDates, BIRTHS_KEPT};

/// The primaries recorded for `birth` leave its facts: a birth whose exit is proven holds no primary
/// (mesh-delete.md step 7, "Deleted births resolve Gone"). The mesh-primary's write, never a node's: a
/// node records no primary (rdm-node's `node_base_holds_no_comparator`). The birth's addresses are the
/// node's own part of its record (Q333), never the mesh-primary's to clear.
pub fn clear_primaries(birth: &mut BirthFacts) {
    birth.is_primary = None;
    birth.is_fabric_primary = None;
}

// ---------------------------------------------------------------- nodes.storage


/// The admin's own row (`docs/architecture/node-identity-and-state.md` §2.5–§2.8,
/// `docs/architecture/storage.md` §2): its own seed, kept in its data dir and read before any store
/// opens (`crate::storage_backend::StorageBackend::own_row`), so a restarting node-admin is the same
/// logical node in the same Fabric and Mesh. The facts of its births, its address and primaries
/// included, are on its node's record ([`NodeRow`]), written by its mesh-primary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    /// Explicit mesh of this admin birth.
    pub mesh: String,
    /// The node's minted id.
    pub node_id: NodeId,
    /// The Fabric the birth belongs to: a restarting node-admin opens the fabric's store root
    /// (`<store_dir>/<fabric_id>/`) from its own row's, read before any store opens (R-FI1).
    pub fabric_id: FabricId,
    /// The node's path.
    pub path: String,
    /// The name, beside its path.
    pub name: String,
    /// The Mesh the birth belongs to, by its minted id (node-identity-and-state.md §2.7: `Node.mesh_id`):
    /// a restarting node-admin reads its mesh's row by it, never by the name alone, since a mesh
    /// created again under the name is another mesh with another id (R-MN1).
    pub mesh_id: MeshId,
    /// The incarnation this birth last ran (a restart supersedes it).
    pub incarnation_id: IncarnationId,
    /// The node's fabric endpoint id.
    pub endpoint_id: EndpointId,
    /// The one address of the birth's Iroh endpoint.
    pub transport_addr: std::net::SocketAddr,
    /// Non-Iroh listeners (the own row of a node-admin: its `control` API).
    #[serde(default)]
    pub listeners: Vec<(String, std::net::SocketAddr)>,
    /// The revision at which this birth is its node type's primary in its mesh, when it is: the
    /// election's result, recorded by the mesh-primary (node-identity-and-state.md §2.7); the write
    /// records the role and does not grant it. A row written before primaries were recorded carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_primary: Option<u64>,
    /// The revision at which this birth is the fabric-primary, when it is, recorded by the node-admin
    /// on its own row (node-identity-and-state.md §2.8), as [`Self::is_primary`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_fabric_primary: Option<u64>,
    /// The `topology_storage_provider` this birth writes (storage.md §2), kept on the own row in its
    /// data dir and written again at each move: a start with no launcher opens the provider it names
    /// ("a restart with no launcher (the adoption launch) | the node itself | its own row in its data
    /// dir"). A row written before it carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology_storage_provider: Option<String>,
}

impl NodeRecord {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

impl NodeRecord {
    /// Where this birth's gossip was last reachable: its transport address, as the topology view dials.
    pub fn gossip_addr(&self) -> Option<iroh::EndpointAddr> {
        let key = self.endpoint_id.0.parse::<iroh::PublicKey>().ok()?;
        Some(iroh::EndpointAddr::new(key).with_ip_addr(self.transport_addr))
    }
}

/// The live records of every mesh: [`records`] with a `Deleted` record skipped. Every read of the
/// births a store root holds to reach (a restart's seeds, its durable map, the rafka-time peers, the
/// stored topology) reads them here, each from a record's newest birth.
pub async fn live_records(meshes: &dyn MeshStorage, nodes: &dyn NodesStorage, own_mesh: &str) -> Result<Vec<NodeRow>, StorageError> {
    Ok(records(meshes, nodes, own_mesh).await?.into_iter().filter(|r| r.status != crate::model::NodeStatus::Deleted).collect())
}

/// The records of every mesh: the records of each mesh `meshes` holds a record of, and of `own_mesh`
/// (whose record a Day-0 admin's own row precedes), each mesh read once, a `Deleted` record included: a
/// deleted birth's recorded primary stays an election input until a newer revision supersedes it
/// (fabric-node-lifecycle-elections.md §3), as a lost primary's does through fabric recovery. Each
/// mesh's records as the reader's fold reads them: a version a deleted birth wrote is ignored
/// ([`crate::node_rows::fold_records`]).
pub async fn records(meshes: &dyn MeshStorage, nodes: &dyn NodesStorage, own_mesh: &str) -> Result<Vec<NodeRow>, StorageError> {
    let mut names: BTreeSet<String> = meshes.meshes().await?.into_iter().map(|m| m.name).collect();
    names.insert(own_mesh.to_string());
    let mut out = Vec::new();
    for mesh in names {
        out.extend(crate::node_rows::fold_records("mesh-records", nodes.nodes(&mesh).await?));
    }
    Ok(out)
}

/// The greatest declared state of each birth the records keep, and the `(path, name)` it declared at.
pub fn fold_declared<'a>(records: impl IntoIterator<Item = &'a NodeRow>) -> HashMap<(NodeId, IncarnationId), (NodeState, PathName)> {
    let mut out = HashMap::new();
    for r in records {
        for b in &r.births {
            if let Some(state) = b.declared_state() {
                out.insert((r.node_id.clone(), b.incarnation_id.clone()), (state, b.location()));
            }
        }
    }
    out
}

#[cfg(feature = "file-backend")]
/// The format of the admin's own row in its data dir ([`own_in`]).
const NODE_FORMAT: &str = "node-record/4";

#[cfg(feature = "file-backend")]
/// The admin's own row in `own_data_dir`, read before the store opens: a restarting admin's row
/// names the Fabric whose store root it opens and the mesh whose nodes it opens on.
pub async fn own_in(own_data_dir: &Path, writer: &IncarnationId) -> Result<Option<NodeRecord>, StorageError> {
    FileRecords::open(own_data_dir, "nodes", writer).await?.read("self", NODE_FORMAT).await
}

#[cfg(feature = "file-backend")]
/// Keep `record` as the admin's own row in `own_data_dir`, where [`own_in`] reads it, written
/// by the process born as `writer`.
pub async fn put_own_in(own_data_dir: &Path, writer: &IncarnationId, record: &NodeRecord) -> Result<(), StorageError> {
    FileRecords::open(own_data_dir, "nodes", writer).await?.write("self", NODE_FORMAT, record).await
}

pub use rdm_node::nodes_storage::{MemoryNodesStorage, NodesStorage};
#[cfg(feature = "file-backend")]
pub use rdm_node::nodes_storage::FileNodesStorage;

// ---------------------------------------------------------------- connections.storage

pub use rdm_node::storage::{ConnectionsStorage, MemoryConnectionsStorage};
#[cfg(feature = "file-backend")]
pub use rdm_node::storage::FileConnectionsStorage;

#[cfg(all(test, feature = "file-backend"))]
mod tests {
    use super::*;
    use crate::node_rows::WrittenRows;

    /// The same script over any nodes store: the facts of one birth, each written from the record its
    /// writer holds, all stand on the record a fresh reader loads whole (storage-whole-records.md,
    /// cells: "two facts of one birth both stand on the record across a reload"): a late `Pending`
    /// beside `Ready`, two attempts' operation effects, a lost and an unreadable data dir;
    /// the birth's cert dates are its last.
    async fn facts_of_one_birth_all_stand(first: &dyn NodesStorage, reopened: &dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Box<dyn NodesStorage>> + Send>>) {
        let (node_id, incarnation_id, name): (NodeId, IncarnationId, PathName) = (NodeId::mint(), IncarnationId::mint(), rdm_core::PathName::new("mesh1.node", "1").unwrap());
        let written = WrittenRows::default();
        let base = NodeRow::new(name.clone(), "mesh1", "node", node_id.clone(), crate::model::NodeStatus::Ready);
        let write = async |change: Box<dyn FnOnce(&mut BirthFacts) + Send>| {
            written.update(first, &node_id, Some(base.clone()), |r| change(r.birth_mut(&incarnation_id, &name))).await.unwrap();
        };
        write(Box::new(|b| { b.declared.insert(NodeState::Ready); })).await;
        write(Box::new(|b| { b.declared.insert(NodeState::Pending); })).await;
        let op = || rdm_core::status::DeclaredOperation { operation: rdm_core::OperationId::node("node.stop", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap())).in_occurrence(rdm_core::OperationOccurrenceId::mint()) };
        write(Box::new(move |b| b.apply_operation(&op(), NodeState::Stopped))).await;
        write(Box::new(move |b| b.apply_operation(&op(), NodeState::Stopped))).await;
        write(Box::new(|b| b.data_dir_lost = Some(DataDirLost { path: "/d".into(), since: 1 }))).await;
        write(Box::new(|b| b.data_dir_unreadable = Some(DataDirUnreadable { path: "/d".into(), cause: "node-key: not hex".into() }))).await;
        let cert = |issued_at: u64| CertDates { issued_at, expires_at: issued_at + 10, issuer_expires_at: 100, renew_at: issued_at + 6 };
        write(Box::new(move |b| b.cert = Some(cert(1)))).await;
        write(Box::new(move |b| b.cert = Some(cert(20)))).await;

        let store = reopened().await;
        let record = store.node(&node_id).await.unwrap().expect("the record is read whole");
        let b = record.birth(&incarnation_id).expect("the birth's facts");
        assert_eq!(b.declared_state(), Some(NodeState::Ready), "a late Pending removes no Ready: the reader takes the greatest");
        assert_eq!(b.declared.len(), 2, "two states, both kept");
        assert_eq!(b.operations.len(), 2, "two occurrences' effects both stand");
        assert!(b.data_dir_lost.is_some() && b.data_dir_unreadable.is_some(), "a lost and an unreadable data dir both stand");
        assert_eq!(b.cert, Some(cert(20)), "the birth's cert dates are the last written");
        assert_eq!(store.nodes("mesh1").await.unwrap(), vec![record.clone()], "the record is its mesh's");
        assert!(store.nodes("mesh2").await.unwrap().is_empty(), "and no other mesh's");
    }

    /// CONTRACT (storage-whole-records.md, "The objects" and its cell "two facts of one birth both stand
    /// on the record across a reload ... on the memory and file backends"): every fact of a birth is
    /// kept by its full identity inside its node's record, so no later or delayed write erases an
    /// independent fact; the file backend's record survives a reopen by another writer. What must NOT
    /// happen: a late Pending replacing Ready, one attempt's effect erasing another's, a fact
    /// lost on the reload.
    #[tokio::test]
    async fn the_facts_of_one_birth_all_stand_on_its_record_across_a_reload_on_either_backend() {
        let memory = std::sync::Arc::new(MemoryNodesStorage::default());
        let held = memory.clone();
        facts_of_one_birth_all_stand(&*memory, &move || {
            let held = held.clone();
            Box::pin(async move { Box::new(SharedNodes(held)) as Box<dyn NodesStorage> })
        })
        .await;
        let dir = crate::test_support::tempdir("facts-of-one-birth");
        let root = crate::store_root::StoreRoot::at(&dir);
        let data = dir.join("data");
        let first = FileNodesStorage::open(&data, &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap();
        facts_of_one_birth_all_stand(&first, &move || {
            let (root, data) = (root.clone(), data.clone());
            Box::pin(async move { Box::new(FileNodesStorage::open(&data, &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap()) as Box<dyn NodesStorage> })
        })
        .await;
    }

    /// CONTRACT (storage-whole-records.md, "The rule", first cell): a write takes the cached object and
    /// writes it whole: each put carries every fact the copy held, never one fact, and no read precedes
    /// a put of a record the writer holds.
    #[tokio::test]
    async fn each_put_carries_the_whole_record_held_and_no_read_precedes_it() {
        #[derive(Default)]
        struct Recording {
            inner: MemoryNodesStorage,
            log: Mutex<Vec<String>>,
            puts: Mutex<Vec<NodeRow>>,
        }
        #[async_trait]
        impl NodesStorage for Recording {
            async fn put_runtime(&self, row: &RuntimeRow) -> Result<(), StorageError> { self.inner.put_runtime(row).await }
            async fn runtimes(&self) -> Result<Vec<RuntimeRow>, StorageError> { self.inner.runtimes().await }
            async fn runtime(&self, n: &NodeId, i: &IncarnationId) -> Result<Option<RuntimeRow>, StorageError> { self.inner.runtime(n, i).await }
            async fn node(&self, n: &NodeId) -> Result<Option<NodeRow>, StorageError> { self.log.lock().unwrap().push("read".into()); self.inner.node(n).await }
            async fn nodes(&self, m: &str) -> Result<Vec<NodeRow>, StorageError> { self.log.lock().unwrap().push("read".into()); self.inner.nodes(m).await }
            async fn put_properties(&self, record: &rdm_node::node_records::NodeProperties) -> Result<(), StorageError> {
                self.inner.put_properties(record).await
            }
            async fn properties(&self, node_id: &crate::model::NodeId) -> Result<Option<rdm_node::node_records::NodeProperties>, StorageError> {
                self.inner.properties(node_id).await
            }
            async fn put_node(&self, r: &NodeRow) -> Result<(), StorageError> { self.log.lock().unwrap().push("put".into()); self.puts.lock().unwrap().push(r.clone()); self.inner.put_node(r).await }
            async fn records_at(&self, n: &PathName) -> Result<Vec<NodeRow>, StorageError> { self.inner.records_at(n).await }
        }
        let store = Recording::default();
        let written = WrittenRows::default();
        let (name, node_id, inc): (PathName, NodeId, IncarnationId) = (rdm_core::PathName::new("mesh1.node", "1").unwrap(), NodeId::mint(), IncarnationId::mint());
        let mut held = NodeRow::new(name.clone(), "mesh1", "node", node_id.clone(), crate::model::NodeStatus::Ready);
        held.storage_persistence = Some(rdm_core::meta::StoragePersistence::Ephemeral);
        held.executable = Some(rdm_core::binding::Binding { launch_id: "node".into(), executable: "/bin/rpc".into(), sha256: "1".repeat(64), image: None });
        written.hold(&held);
        written.update(&store, &node_id, None, |r| { r.birth_mut(&inc, &name).declared.insert(NodeState::Ready); }).await.unwrap();
        written.update(&store, &node_id, None, |r| r.birth_mut(&inc, &name).silence = Some(crate::model::NodeStatus::PendingReconnect)).await.unwrap();
        written.update(&store, &node_id, None, |r| r.birth_mut(&inc, &name).cert = Some(CertDates { issued_at: 1, expires_at: 9, issuer_expires_at: 99, renew_at: 6 })).await.unwrap();
        assert_eq!(*store.log.lock().unwrap(), ["put", "put", "put"], "three writes, no read before any of them");
        let puts = store.puts.lock().unwrap().clone();
        let last = puts.last().unwrap();
        assert_eq!((last.storage_persistence, last.executable.is_some()), (Some(rdm_core::meta::StoragePersistence::Ephemeral), true), "the record's own fields go with every put");
        let b = last.birth(&inc).unwrap();
        assert!(b.declared.contains(&NodeState::Ready) && b.silence.is_some() && b.cert.is_some(), "the last put carries every fact the copy held: {b:?}");
        assert!(puts[1].birth(&inc).unwrap().declared.contains(&NodeState::Ready), "the second put carries the first's fact too");
    }

    /// CONTRACT (storage-whole-records.md, "The rule": the copy is filled from storage when the writer
    /// takes the object's writing): a record written after the fill listed the mesh, and before the fill
    /// held it, is the copy the fill leaves, and the next write carries it. Must NOT happen: the listing's
    /// older record replacing the newer copy, so the next whole write erases what was written between.
    #[tokio::test]
    async fn a_fill_never_replaces_a_record_written_after_its_listing() {
        struct StaleListing {
            inner: MemoryNodesStorage,
            listed: Mutex<Vec<NodeRow>>,
        }
        #[async_trait]
        impl NodesStorage for StaleListing {
            async fn put_runtime(&self, row: &RuntimeRow) -> Result<(), StorageError> { self.inner.put_runtime(row).await }
            async fn runtimes(&self) -> Result<Vec<RuntimeRow>, StorageError> { self.inner.runtimes().await }
            async fn runtime(&self, n: &NodeId, i: &IncarnationId) -> Result<Option<RuntimeRow>, StorageError> { self.inner.runtime(n, i).await }
            async fn node(&self, n: &NodeId) -> Result<Option<NodeRow>, StorageError> { self.inner.node(n).await }
            // The listing as it stood before the last write landed.
            async fn nodes(&self, _m: &str) -> Result<Vec<NodeRow>, StorageError> { Ok(self.listed.lock().unwrap().clone()) }
            async fn put_properties(&self, record: &rdm_node::node_records::NodeProperties) -> Result<(), StorageError> {
                self.inner.put_properties(record).await
            }
            async fn properties(&self, node_id: &crate::model::NodeId) -> Result<Option<rdm_node::node_records::NodeProperties>, StorageError> {
                self.inner.properties(node_id).await
            }
            async fn put_node(&self, r: &NodeRow) -> Result<(), StorageError> { self.inner.put_node(r).await }
            async fn records_at(&self, n: &PathName) -> Result<Vec<NodeRow>, StorageError> { self.inner.records_at(n).await }
        }
        let store = StaleListing { inner: MemoryNodesStorage::default(), listed: Mutex::new(Vec::new()) };
        let written = WrittenRows::default();
        let (name, node_id, inc): (PathName, NodeId, IncarnationId) = (rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), NodeId::mint(), IncarnationId::mint());
        written.update(&store, &node_id, Some(NodeRow::new(name.clone(), "mesh1", "node-admin", node_id.clone(), crate::model::NodeStatus::Pending)), |_| ()).await.unwrap();
        *store.listed.lock().unwrap() = vec![store.inner.node(&node_id).await.unwrap().unwrap()];
        written.update(&store, &node_id, None, |r| { r.birth_mut(&inc, &name).declared.insert(NodeState::Ready); }).await.unwrap();
        written.fill(&store, "mesh1").await.unwrap();
        written.update(&store, &node_id, None, |r| r.birth_mut(&inc, &name).signing_cert = Some(SigningCertDates { issued_at: 1, expires_at: 9, renew_at: 6 })).await.unwrap();
        let stored = store.inner.node(&node_id).await.unwrap().unwrap();
        let b = stored.birth(&inc).unwrap();
        assert!(b.declared.contains(&NodeState::Ready) && b.signing_cert.is_some(), "the write between the listing and the fill stands beside the next: {b:?}");
    }

    /// The memory store a reopen hands back: the same rows.
    struct SharedNodes(std::sync::Arc<MemoryNodesStorage>);

    #[async_trait]
    impl NodesStorage for SharedNodes {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.0.runtimes().await
        }
        async fn runtime(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.0.runtime(node_id, incarnation).await
        }
        async fn node(&self, node_id: &crate::model::NodeId) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.0.node(node_id).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.0.nodes(mesh).await
        }
        async fn put_properties(&self, record: &rdm_node::node_records::NodeProperties) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_properties(record).await
        }
        async fn properties(&self, node_id: &crate::model::NodeId) -> Result<Option<rdm_node::node_records::NodeProperties>, crate::storage_error::StorageError> {
            self.0.properties(node_id).await
        }
        async fn put_node(&self, row: &crate::storage::NodeRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_node(row).await
        }
        async fn records_at(&self, name: &crate::model::PathName) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.0.records_at(name).await
        }
    }
    use rdm_core::connections::NodeConnection;
    use rdm_core::connections::{ConnectionEnd, ConnectionKind, ConnectionState};

    fn node(name: PathName, mesh: &str, r#type: &str) -> NodeRecord {
        NodeRecord { mesh: mesh.to_owned(),
            node_id: NodeId::mint(),
            fabric_id: FabricId::mint(),
            path: name.path.clone(), name: name.name.clone(),
            mesh_id: MeshId::mint(),
            incarnation_id: IncarnationId::mint(),
            endpoint_id: EndpointId(iroh::SecretKey::generate().public().to_string()),
            transport_addr: "127.0.0.1:41001".parse().unwrap(),
            listeners: vec![],
            is_primary: None,
            is_fabric_primary: None,
            topology_storage_provider: None,
        }
    }

    fn end(name: PathName, mesh: &str, r#type: &str) -> ConnectionEnd {
        // One fixed birth per (path, name): the facts of one connection name the same two ends.
        let fixed = |what: &str| rdm_core::fixture_crockford60(&format!("{what}-{}-{}", name.path, name.name));
        ConnectionEnd { mesh: mesh.to_owned(), r#type: r#type.to_string(), path: name.path.clone(), name: name.name.clone(), node_id: NodeId::parse(&fixed("node")).unwrap(), incarnation: Some(IncarnationId::parse(&fixed("birth")).unwrap()) }
    }

    fn fact(state: ConnectionState) -> NodeConnection {
        NodeConnection {
            id: rdm_core::ConnectionId::mint(),
            source: end(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin"),
            destination: end(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"),
            kind: ConnectionKind::Direct,
            alpn: rdm_core::connections::ALPN.to_vec(),
            state,
            carrier: None,
            recovery: None,
            reason: None,
            logged_at_ms: 1,
        }
    }

    /// Every boundary behaves the same in memory and on disk, and the disk copy survives reopening.
    #[tokio::test]
    async fn mesh_nodes_and_connections_hold_their_records_in_memory_and_across_a_reopen() {
        let d = crate::test_support::tempdir("storage");
        let mesh = MeshRecord::new(MeshId::mint(), "mesh1");
        let (own, peer) = (node(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin"), node(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"));
        let runtime = RuntimeRow { r#type: "node".into(), mesh: peer.mesh.clone(), node_id: peer.node_id.clone(), path: peer.path.clone(), name: peer.name.clone(), incarnation_id: peer.incarnation_id.clone(), runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-1")).unwrap(), data_dir: None };
        let (up, down) = (fact(ConnectionState::Connected), fact(ConnectionState::Disconnected));
        let check = async |m: &dyn MeshStorage, n: &dyn NodesStorage, c: &dyn ConnectionsStorage, fresh: bool| {
            if fresh {
                assert_eq!(m.mesh(&mesh.mesh_id).await.unwrap(), None);
                assert_eq!(crate::storage::write_mesh(&*m, None, &mesh).await.unwrap(), Ok(MeshWritten::Written));
                n.put_runtime(&runtime).await.unwrap();
                c.put_connection(&up).await.unwrap();
                c.put_connection(&down).await.unwrap();
            }
            assert_eq!(m.mesh(&mesh.mesh_id).await.unwrap(), Some(mesh.clone()));
            assert_eq!(m.mesh_named("mesh1").await.unwrap(), Some(mesh.clone()), "the lookup by name names the record");
            assert_eq!(m.meshes().await.unwrap(), [mesh.clone()]);
            assert_eq!(n.runtimes().await.unwrap(), [runtime.clone()]);
            assert_eq!(c.connections().await.unwrap(), [down.clone()], "the latest fact per (source, destination, kind)");
        };
        let mem = (MemoryMeshStorage::default(), MemoryNodesStorage::default(), MemoryConnectionsStorage::default());
        check(&mem.0, &mem.1, &mem.2, true).await;
        let connection_writer = crate::store_log::LogWriter::mint();
        let (root, w, _me): (_, IncarnationId, PathName) = (crate::store_root::StoreRoot::at(&d.join("root")), IncarnationId::mint(), rdm_core::PathName::new("mesh1.node-admin", "1").unwrap());
        check(&FileMeshStorage::open(&root, &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &w)).await.unwrap(), &FileNodesStorage::open(&d, &root, "mesh1", &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &w)).await.unwrap(), &FileConnectionsStorage::open(&root, &connection_writer).await.unwrap(), true).await;
        let (m, n, c) = (FileMeshStorage::open(&root, &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &w)).await.unwrap(), FileNodesStorage::open(&d, &root, "mesh1", &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &w)).await.unwrap(), FileConnectionsStorage::open(&root, &connection_writer).await.unwrap());
        check(&m, &n, &c, false).await;
        c.remove_connection(&down.index()).await.unwrap();
        assert!(c.connections().await.unwrap().is_empty());
        // The own row is the admin's own seed in its data dir, read before any store opens.
        assert_eq!(own_in(&d, &w).await.unwrap(), None);
        put_own_in(&d, &w, &own).await.unwrap();
        assert_eq!(own_in(&d, &IncarnationId::mint()).await.unwrap(), Some(own.clone()), "the own row is read back by the next process");
        assert_eq!(own.gossip_addr().map(|a| a.id.to_string()), Some(own.endpoint_id.0.clone()));
        let op = crate::store_log::Op { node_id: "n".into(), incarnation_id: "i".into(), seq: 1 };
        std::fs::OpenOptions::new().append(true).open(root.dir().join(crate::store_root::MESHES_FILE)).and_then(|mut f| std::io::Write::write_all(&mut f, &crate::store_log::encode_line(&op, "mesh-record/9", mesh.mesh_id.as_str(), b"{}"))).unwrap();
        assert!(matches!(m.mesh(&mesh.mesh_id).await, Err(StorageError::Unrecognised { .. })));
    }

    /// CONTRACT (node-identity-and-state.md §4; states.md "Who sets each state" and the paragraph after
    /// it, Q233): a node's status on its record follows states.md's order through a stop and a start, a
    /// declaration inside node.stop leaves it stopping, a superseded birth moves nothing, and a `Deleted`
    /// record stands. A stop's acknowledgement sets stopping over any status but stopped, a node already
    /// connections-deleted or left included. delete is the mesh-primary's workflow: deleting from its
    /// claim, over a live or a stopped node; the frames of the stop it composes move the row as a stop's
    /// do, stopping through stopped; then deleted. A ready, a pending or a starting moves a deleting row
    /// nowhere.
    // @feature: node-lifecycle
    #[test]
    fn a_node_records_status_follows_the_order_states_md_names() {
        use crate::model::NodeStatus as S;
        let (birth, old) = (IncarnationId::mint(), IncarnationId::mint());
        let mut r = NodeRow::new(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", NodeId::mint(), S::Starting);
        r.birth_mut(&old, &r.location());
        r.birth_mut(&birth, &r.location());
        let walk = |r: &mut NodeRow, steps: &[(S, S)]| for (to, then) in steps { r.set_status(&birth, *to); assert_eq!(r.status, *then, "after {to:?}"); };
        walk(&mut r, &[(S::Pending, S::Pending), (S::Ready, S::Ready), (S::Starting, S::Ready), (S::Pending, S::Pending), (S::Ready, S::Ready)]);
        // A stop (Q205): stopping from its admission, then each primitive's frame, then stopped.
        // The node's Stopped declaration may reach the mesh-primary before the stream's last frames
        // (node-stop.md step 5): a stopped row stands, and no stopping takes it.
        walk(&mut r, &[(S::Stopping, S::Stopping), (S::Ready, S::Stopping), (S::ConnectionsDeleting, S::ConnectionsDeleting), (S::ConnectionsDeleted, S::ConnectionsDeleted), (S::Leaving, S::Leaving), (S::Left, S::Left), (S::ConnectionsDeleting, S::Left), (S::Stopped, S::Stopped), (S::Left, S::Stopped), (S::Stopping, S::Stopped)]);
        walk(&mut r, &[(S::Starting, S::Starting), (S::Pending, S::Pending), (S::Ready, S::Ready)]);
        walk(&mut r, &[(S::ConnectionsDeleting, S::ConnectionsDeleting), (S::ConnectionsDeleted, S::ConnectionsDeleted), (S::ConnectionsDeleting, S::ConnectionsDeleted)]);
        // A stop of a node already connections-deleted: its acknowledgement sets stopping (Q233), and
        // its primitives' frames move the row on to stopped.
        walk(&mut r, &[(S::Stopping, S::Stopping), (S::ConnectionsDeleting, S::ConnectionsDeleting), (S::ConnectionsDeleted, S::ConnectionsDeleted), (S::Leaving, S::Leaving), (S::Left, S::Left), (S::Stopped, S::Stopped)]);
        walk(&mut r, &[(S::Starting, S::Starting), (S::Pending, S::Pending), (S::Ready, S::Ready)]);
        // A stop of a node already left: the same.
        walk(&mut r, &[(S::Leaving, S::Leaving), (S::Left, S::Left), (S::Stopping, S::Stopping), (S::ConnectionsDeleting, S::ConnectionsDeleting), (S::ConnectionsDeleted, S::ConnectionsDeleted), (S::Leaving, S::Leaving), (S::Left, S::Left), (S::Stopped, S::Stopped)]);
        walk(&mut r, &[(S::Starting, S::Starting), (S::Pending, S::Pending), (S::Ready, S::Ready)]);
        // A delete of the live node: deleting from its claim, then the composed stop's frames.
        walk(&mut r, &[(S::Deleting, S::Deleting), (S::Ready, S::Deleting), (S::Pending, S::Deleting), (S::Starting, S::Deleting)]);
        assert!(!r.clone().set_status(&birth, S::Deleting), "deleting again leaves a deleting row unchanged");
        assert!(!r.clone().set_status(&old, S::Stopping), "a superseded birth moves no status");
        walk(&mut r, &[(S::Stopping, S::Stopping), (S::ConnectionsDeleting, S::ConnectionsDeleting), (S::ConnectionsDeleted, S::ConnectionsDeleted), (S::Leaving, S::Leaving), (S::Left, S::Left), (S::Stopped, S::Stopped)]);
        // A delete of a stopped node: deleting from its claim over stopped, then deleted from the receipt.
        assert!(!r.clone().set_status(&old, S::Deleted), "a superseded birth moves no status");
        walk(&mut r, &[(S::Deleting, S::Deleting), (S::Deleted, S::Deleted)]);
        walk(&mut r, &[(S::Pending, S::Deleted), (S::Starting, S::Deleted)]);
    }

    fn row(name: PathName, mesh: &str, r#type: &str) -> NodeRow {
        NodeRow::new(name.clone(), mesh, r#type, NodeId::mint(), crate::model::NodeStatus::Ready)
    }

    /// CONTRACT (node-identity-and-state.md §2: "A current-pair lookup reads the node records by their
    /// path and name; it never uses the last writer at a path"; node-identity-and-state.md: a delete
    /// "writes Deleted lifecycle state, which frees the pair"): a `(path, name)` resolves to the live
    /// record standing there, and `NameHeld { by: node.id }` names another node's live record. A name
    /// the node's own record holds is its own again; a delete frees the pair, so nothing resolves at the
    /// name (the `Deleted` record stands, read by id) until the next node created there writes its
    /// record. In memory and on disk alike.
    #[tokio::test]
    async fn a_nodes_name_is_read_from_the_live_records_and_a_delete_frees_it() {
        let d = crate::test_support::tempdir("node-names");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let file = FileNodesStorage::open(&d.join("admin-1-1-data"), &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap();
        let mem = MemoryNodesStorage::default();
        for n in [&file as &dyn NodesStorage, &mem as &dyn NodesStorage] {
            let w = WrittenRows::default();
            let a = row(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
            let _one = w.lock(&a.node_id).await;
            assert_eq!(crate::node_rows::check_name(n, None, &a.location(), &a.node_id).await.unwrap(), Ok(()), "a free name is the node's");
            w.put(n, &a).await.unwrap();
            drop(_one);
            assert_eq!(crate::node_rows::record_at(n, &a.location()).await.unwrap(), w.row_of(&a.node_id), "the name resolves to the live record, as its writer wrote it");
            assert_eq!(crate::node_rows::check_name(n, None, &a.location(), &a.node_id).await.unwrap(), Ok(()), "the node's own record holds its name");
            let other = row(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
            assert_eq!(crate::node_rows::check_name(n, None, &a.location(), &other.node_id).await.unwrap(), Err(NameHeld { path: a.path.clone(), name: a.name.clone(), by: a.node_id.clone() }), "another node's live record holds the name");
            crate::node_rows::write_deleted(n, &w, &a.location(), &a.node_id).await.ok().expect("the delete's record");
            assert_eq!(crate::node_rows::record_at(n, &a.location()).await.unwrap(), None, "a Deleted record frees its pair: nothing resolves at the name");
            assert_eq!(crate::node_rows::check_name(n, None, &a.location(), &other.node_id).await.unwrap(), Ok(()), "the next node takes the name");
            let _two = w.lock(&other.node_id).await;
            w.put(n, &other).await.unwrap();
            assert_eq!(crate::node_rows::record_at(n, &a.location()).await.unwrap().map(|r| r.node_id), Some(other.node_id.clone()), "the name resolves to the new node");
            assert_eq!(n.node(&a.node_id).await.unwrap().map(|r| r.status), Some(crate::model::NodeStatus::Deleted), "the deleted record stands, read by id");
        }
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (node-name-update.md, "Check and move": "one write of the node's record at its node.id
    /// (Q323) carrying the destination pair and `renamed_from: <old pair>`: the name move is that one
    /// write. A retry, or a successor writer resuming the command, reads the record: one that already
    /// carries the destination pair is the move done"): a rename writes the node's record once, the old
    /// pair then free; its successor writer's retry finds the record at the new pair and writes nothing;
    /// another node's rename to that pair is `NameHeld` naming the first node.
    #[tokio::test]
    async fn a_rename_is_one_record_write_its_retry_writes_nothing_and_another_nodes_rename_to_it_is_held() {
        let d = crate::test_support::tempdir("rename-retry");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let file = FileNodesStorage::open(&d.join("admin-1-1-data"), &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap();
        let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let from = row(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
        let first = WrittenRows::default();
        first.update(&file, &from.node_id, Some(from.clone()), |_| ()).await.unwrap();
        let to: PathName = rdm_core::PathName::new("mesh1.node", "9").unwrap();
        let mut record = crate::model::Node::allocated(from.location(), from.mesh.clone(), &from.r#type);
        record.node_id = from.node_id.clone();
        record.status = from.status;
        let lines = || crate::store_log::read_file(&root.dir().join(crate::store_root::NODES_FILE)).unwrap().iter().filter(|l| l.kind() == "node-record" && l.key == from.node_id.as_str()).count();
        let before = lines();
        let (moved, written) = crate::node_rows::move_row(&file, &first, &me, &record, &from.location(), &to).await.expect("the rename is written");
        assert_eq!((moved, written.map(|r| r.location())), (crate::node_rows::Moved::Renamed, Some(to.clone())));
        assert_eq!(lines(), before + 1, "the rename is one write of the node's record");
        assert_eq!(file.node(&from.node_id).await.unwrap().map(|r| (r.location(), r.renamed_from)), Some((to.clone(), Some(from.name.clone()))), "the record carries the new name and the name it came from");
        assert_eq!(crate::node_rows::record_at(&file, &from.location()).await.unwrap(), None, "the old pair is free");
        // Its successor writer retries the command from what storage holds.
        let successor = WrittenRows::default();
        let (moved, _) = crate::node_rows::move_row(&file, &successor, &me, &record, &from.location(), &to).await.expect("the retry answers");
        assert_eq!(moved, crate::node_rows::Moved::AlreadyMoved, "the record at the new pair is the move done");
        assert_eq!(lines(), before + 1, "the retry writes nothing");
        let other = row(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node");
        let mut theirs = crate::model::Node::allocated(other.location(), other.mesh.clone(), &other.r#type);
        theirs.node_id = other.node_id.clone();
        successor.update(&file, &other.node_id, Some(other.clone()), |_| ()).await.unwrap();
        assert_eq!(crate::node_rows::move_row(&file, &successor, &me, &theirs, &other.location(), &to).await.err(), Some(crate::node_rows::RenameRefused::NameHeld(NameHeld { path: to.path.clone(), name: to.name.clone(), by: from.node_id.clone() })), "another node's rename to the name is NameHeld");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (R14 items 1 and 2; acceptance "two mesh-primaries write their own meshes' rows at
    /// once on one shared root without touching each other's keys"): each mesh-primary writes its
    /// mesh's record and its members' node records under its own mesh's keys, at once, on
    /// one root; neither writes a key under the other's mesh, its own data dir holds none of them, and
    /// each reads the other's records from the shared root.
    #[tokio::test]
    async fn two_mesh_primaries_write_their_own_meshes_rows_at_once_on_one_shared_root_without_touching_each_others_keys() {
        let d = crate::test_support::tempdir("shared-root");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = crate::store_root::StoreRoot::open(&store, &crate::model::FabricId::mint()).unwrap();
        let (data1, data2) = (d.join("admin-1-1-data"), d.join("admin-2-1-data"));
        let (inc1, inc2) = (IncarnationId::mint(), IncarnationId::mint());
        let (n1, n2) = (FileNodesStorage::open(&data1, &root, "mesh1", &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &inc1)).await.unwrap(), FileNodesStorage::open(&data2, &root, "mesh2", &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &inc2)).await.unwrap());
        let (m1, m2) = (FileMeshStorage::open(&root, &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &inc1)).await.unwrap(), FileMeshStorage::open(&root, &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &inc2)).await.unwrap());
        let writes = async |n: &FileNodesStorage, m: &FileMeshStorage, mesh: &str| {
            let w = WrittenRows::default();
            crate::storage::write_mesh(&*m, None, &MeshRecord::new(MeshId::mint(), mesh)).await.unwrap().expect("each mesh writes its record at a name no mesh holds");
            let mut ids = Vec::new();
            for (name, r#type) in [(PathName::new(format!("{mesh}.node-admin"), "1").unwrap(), "node-admin"), (PathName::new(format!("{mesh}.node"), "1").unwrap(), "node"), (PathName::new(format!("{mesh}.node"), "2").unwrap(), "node")] {
                let r = row(name, mesh, r#type);
                w.update(n, &r.node_id, Some(r.clone()), |_| ()).await.unwrap();
                ids.push(r.node_id.to_string());
            }
            ids.sort();
            ids
        };
        let (ids1, ids2) = tokio::join!(writes(&n1, &m1, "mesh1"), writes(&n2, &m2, "mesh2"));
        let records = crate::store_log::latest_in(&root.dir().join(crate::store_root::NODES_FILE)).unwrap();
        for (mesh, ids) in [("mesh1", ids1), ("mesh2", ids2)] {
            let of_mesh: Vec<String> = records.iter().filter(|((kind, _), l)| kind == "node-record" && l.record["mesh"] == mesh).map(|((_, key), _)| key.clone()).collect();
            assert_eq!(of_mesh, ids, "{mesh}'s records, keyed by node id, in the root's nodes.jsonl");
        }
        assert_eq!(crate::store_log::keys_in(&root.dir().join(crate::store_root::MESHES_FILE), "mesh-record").unwrap().len(), 2, "one record per mesh");
        for data in [&data1, &data2] {
            assert!(!data.join(crate::store_root::MESHES_FILE).exists() && !data.join(crate::store_root::NODES_FILE).exists(), "{} holds a shared record", data.display());
        }
        assert!(crate::node_rows::record_at(&n1, &rdm_core::PathName::new("mesh2.node", "1").unwrap()).await.unwrap().is_some(), "mesh1's primary reads mesh2's record from the shared root");
        assert!(crate::node_rows::record_at(&n2, &rdm_core::PathName::new("mesh1.node", "1").unwrap()).await.unwrap().is_some(), "mesh2's primary reads mesh1's record from the shared root");
        assert_eq!(live_records(&m1, &n1, "mesh1").await.unwrap().len(), 6, "a restart reads every mesh's records");
    }

    /// CONTRACT (R-ML2 item 4; §5.4): mesh2 created again under its name, a new mesh with a new id,
    /// takes the mesh's name over the deleted mesh's record, its node-admin's path and name over the
    /// Deleted record there, and its records carry the new MeshId. Must NOT happen: the recreated mesh
    /// refused its name.
    #[tokio::test]
    async fn a_recreated_mesh_takes_its_names_over_the_dead_ones() {
        use crate::node_rows::RecordAt as _;
        let d = crate::test_support::tempdir("recreated-mesh");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let w = crate::store_log::LogWriter::mint();
        let (meshes, nodes) = (FileMeshStorage::open(&root, &w).await.unwrap(), FileNodesStorage::open(&d, &root, "mesh1", &w).await.unwrap());
        let admin: PathName = rdm_core::PathName::new("mesh2.node-admin", "1").unwrap();
        let old = MeshRecord::new(MeshId::mint(), "mesh2");
        write_mesh(&meshes, None, &old).await.unwrap().expect("the first mesh2 writes its record at its name");
        let first = NodeRow { mesh_id: Some(old.mesh_id.clone()), ..NodeRow::new(admin.clone(), "mesh2", "node-admin", NodeId::mint(), crate::model::NodeStatus::Pending) };
        assert_eq!(crate::node_rows::write_record(&nodes, &first).await.unwrap(), Ok(()));
        // mesh2 is removed: its node Deleted, its record deleted.
        nodes.put_node(&NodeRow { status: crate::model::NodeStatus::Deleted, ..first.clone() }).await.unwrap();
        write_mesh(&meshes, Some(&old), &MeshRecord { state: MeshState::Deleted, ..old.clone() }).await.unwrap().expect("the deleted record is written");
        // mesh2 created again.
        let new = MeshRecord::new(MeshId::mint(), "mesh2");
        assert_eq!(write_mesh(&meshes, None, &new).await.unwrap(), Ok(MeshWritten::Written), "the recreated mesh takes its name over the deleted mesh's");
        assert_eq!(meshes.mesh_named("mesh2").await.unwrap().map(|m| m.mesh_id), Some(new.mesh_id.clone()));
        let second = NodeRow { mesh_id: Some(new.mesh_id.clone()), ..NodeRow::new(admin.clone(), "mesh2", "node-admin", NodeId::mint(), crate::model::NodeStatus::Pending) };
        assert_eq!(crate::node_rows::write_record(&nodes, &second).await.unwrap(), Ok(()), "its node-admin's record takes the name over the Deleted record");
        assert_eq!(nodes.record_at(&admin).await.unwrap().and_then(|r| r.mesh_id), Some(new.mesh_id.clone()), "its records carry the new MeshId");
    }

    /// CONTRACT (R-SR2: "a create withdrawn before the node ran appends the record marked Withdrawn";
    /// nothing is ever removed): a create withdrawn before its node ran leaves its record `Withdrawn`,
    /// read at its id and listed by no mesh and at no name, which frees the pair; the next node's record
    /// takes the name over it. On the file backend every write of the record stands as a line. Must
    /// NOT happen: the record removed, or a record over a `Withdrawn` one refused `NameHeld`.
    #[tokio::test]
    async fn a_withdrawn_create_leaves_its_record_withdrawn_and_a_later_record_takes_its_name() {
        let d = crate::test_support::tempdir("withdrawn-create");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let file = FileNodesStorage::open(&d, &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap();
        let memory = MemoryNodesStorage::default();
        for (store, backend) in [(&file as &dyn NodesStorage, "file"), (&memory as &dyn NodesStorage, "memory")] {
            let name: PathName = rdm_core::PathName::new("mesh1.node", "3").unwrap();
            let pending = |n: &PathName| NodeRow::new(n.clone(), "mesh1", "node", NodeId::mint(), crate::model::NodeStatus::Pending);
            let x = pending(&name);
            assert_eq!(crate::node_rows::write_record(store, &x).await.unwrap(), Ok(()), "{backend}: x's record takes the name");
            assert!(crate::node_rows::release_record(store, &WrittenRows::default(), &name, &x.node_id).await.unwrap(), "{backend}: x held the name");
            assert_eq!(store.node(&x.node_id).await.unwrap().map(|r| r.status), Some(crate::model::NodeStatus::Withdrawn), "{backend}: the record stands, Withdrawn");
            assert_eq!(crate::node_rows::record_at(store, &name).await.unwrap(), None, "{backend}: no record at the name");
            assert!(store.nodes("mesh1").await.unwrap().iter().all(|r| r.node_id != x.node_id), "{backend}: no mesh lists it");
            let z = pending(&name);
            assert_eq!(crate::node_rows::write_record(store, &z).await.unwrap(), Ok(()), "{backend}: z's record takes the name over x's Withdrawn record");
            assert_eq!(crate::node_rows::record_at(store, &name).await.unwrap().map(|r| r.node_id), Some(z.node_id.clone()), "{backend}: the name is z's");
            if backend == "file" {
                let lines = crate::store_log::read_file(&root.dir().join(crate::store_root::NODES_FILE)).unwrap();
                assert_eq!(lines.iter().filter(|l| l.kind() == "node-record" && l.key == x.node_id.as_str()).count(), 2, "x's record and its Withdrawn record both stand as lines");
            }
        }
    }

    /// CONTRACT (R14 items 1 and 9, R-SR1; connections.md §3: the raw log and the latest-per-key view
    /// are one file): a node appends its connection facts as lines of the root's `connections.jsonl`,
    /// where a second store opened by another process reads the latest fact per key as the current
    /// entry and every fact as the raw log.
    #[tokio::test]
    async fn an_ordinary_nodes_connection_facts_land_in_connections_jsonl_and_a_second_instance_reads_them() {
        let d = crate::test_support::tempdir("shared-connections");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = crate::store_root::StoreRoot::open(&store, &crate::model::FabricId::mint()).unwrap();
        let connection_writer = crate::store_log::LogWriter::mint();
        let writer = FileConnectionsStorage::open(&root, &connection_writer).await.unwrap();
        let (up, down) = (fact(ConnectionState::Connected), fact(ConnectionState::Disconnected));
        writer.append_history(&up).await.unwrap();
        writer.put_connection(&up).await.unwrap();
        writer.append_history(&down).await.unwrap();
        writer.put_connection(&down).await.unwrap();
        let file = root.dir().join(crate::store_root::CONNECTIONS_FILE);
        assert_eq!(writer.connections().await.unwrap(), [down.clone()], "the writer reads the entries it wrote");
        assert_eq!(writer.history().await.unwrap(), [up.clone(), down.clone()], "the writer reads the facts it wrote");
        assert_eq!(crate::store_log::read_file(&file).unwrap().len(), 2, "one line per fact");
        let reader = FileConnectionsStorage::open(&root, &connection_writer).await.unwrap();
        assert_eq!(reader.connections().await.unwrap(), [down.clone()], "the latest fact per (source, destination, alpn, kind)");
        assert_eq!(reader.history().await.unwrap(), [up, down], "every fact, oldest first");
    }

    /// CONTRACT (connections.md §3.1: "by-destination prefix = hash(destination)", one latest member
    /// per (source, destination, alpn, kind)): the store's by-destination index names every current
    /// row whose destination is one node, whichever source wrote it and in whichever mesh, read by any
    /// process on the root; a removed row leaves it, and a row whose destination is re-born at its
    /// path and name under a new node ID has an independent index until explicit removal. Memory and file
    /// alike; on the file root the index is the fold of `connections.jsonl` keyed by destination.
    #[tokio::test]
    async fn the_by_destination_index_names_every_source_holding_a_row_to_one_node_in_any_mesh() {
        let d = crate::test_support::tempdir("by-destination");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = crate::store_root::StoreRoot::open(&store, &crate::model::FabricId::mint()).unwrap();
        let destination = end(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
        let row = |source: PathName, mesh: &str, destination: &ConnectionEnd| NodeConnection { source: end(source, mesh, "node"), destination: destination.clone(), ..fact(ConnectionState::Connected) };
        let (a, b) = (row(PathName::new("mesh1.node", "2").unwrap(), "mesh1", &destination), row(PathName::new("mesh2.node", "1").unwrap(), "mesh2", &destination));
        let re_born = ConnectionEnd { node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()), ..destination.clone() };
        let again = NodeConnection { id: rdm_core::ConnectionId::mint(), destination: re_born.clone(), ..a.clone() };
        let check = async |sa: &dyn ConnectionsStorage, sb: &dyn ConnectionsStorage, reader: &dyn ConnectionsStorage, backend: &str| {
            sa.put_connection(&a).await.unwrap();
            sb.put_connection(&b).await.unwrap();
            let mut held = reader.connections_to(&destination.node_id).await.unwrap();
            held.sort_by_key(|r| r.source.location());
            assert_eq!(held, [a.clone(), b.clone()], "{backend}: both sources, in two meshes, hold a row to the node");
            assert!(reader.connections_to(&a.source.node_id).await.unwrap().is_empty(), "{backend}: a source is no destination");
            sb.remove_connection(&b.index()).await.unwrap();
            assert_eq!(reader.connections_to(&destination.node_id).await.unwrap(), [a.clone()], "{backend}: a removed row leaves the index");
            sa.put_connection(&again).await.unwrap();
            assert_eq!(reader.connections_to(&destination.node_id).await.unwrap(), [a.clone()], "{backend}: a distinct node ID retains its own indexed facts");
            sa.remove_connection(&a.index()).await.unwrap();
            assert!(reader.connections_to(&destination.node_id).await.unwrap().is_empty(), "{backend}: explicit removal clears the predecessor index");
            assert_eq!(reader.connections_to(&re_born.node_id).await.unwrap(), [again.clone()], "{backend}: the new node's index holds it");
        };
        let mem = MemoryConnectionsStorage::default();
        check(&mem, &mem, &mem, "memory").await;
        let open = async |_name: PathName| FileConnectionsStorage::open(&root, &crate::store_log::LogWriter::mint()).await.unwrap();
        let (fa, fb, reader) = (open(PathName::new("mesh1.node", "2").unwrap()).await, open(PathName::new("mesh2.node", "1").unwrap()).await, open(PathName::new("mesh1.node-admin", "1").unwrap()).await);
        check(&fa, &fb, &reader, "file").await;
    }

    #[tokio::test]
    async fn runtime_rows_are_one_keyed_row_per_birth_and_an_unrecognised_row_is_refused_by_name() {
        let d = std::env::temp_dir().join(format!("rdm-runtime-rows-{}", rand::random::<u32>()));
        let row = |inc: &str| RuntimeRow { r#type: "node-admin".into(), mesh: "mesh1".into(),
            node_id: NodeId::mint(),
            path: "mesh1.node-admin".into(), name: "2".into(),
            incarnation_id: IncarnationId::parse(&rdm_core::fixture_crockford60(inc)).unwrap(),
            runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-1")).unwrap(),
            data_dir: Some("/tmp/x".into()),
        };
        let (a, b) = (row("i1"), row("i2"));
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let n = FileNodesStorage::open(&d, &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap();
        n.put_runtime(&a).await.unwrap();
        n.put_runtime(&b).await.unwrap();
        n.put_runtime(&a).await.unwrap();
        let mut held = FileNodesStorage::open(&d, &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap().runtimes().await.unwrap();
        held.sort_by_key(|r| r.incarnation_id.to_string());
        assert_eq!(held, [a.clone(), b.clone()], "two births, two rows; a repeated put is the same row");
        let op = crate::store_log::Op { node_id: "n".into(), incarnation_id: "i".into(), seq: 1 };
        std::fs::OpenOptions::new().append(true).open(root.dir().join(crate::store_root::NODES_FILE)).and_then(|mut f| std::io::Write::write_all(&mut f, &crate::store_log::encode_line(&op, "node-runtime/9", &a.key(), b"{}"))).unwrap();
        assert!(matches!(n.runtimes().await, Err(StorageError::Unrecognised { .. })));
    }

    /// CONTRACT (node-identity-and-state.md §2.7: the mesh-primary "checks the mesh's name against the mesh records at the key
    /// `(path, name)` ... Held means held by a different MeshId: `NameHeld` names that holder.
    /// The same MeshId writing its own row again ... is idempotent and accepted"; R-MN1: only a delete
    /// releases a name, a mesh's by its removal, the record deleted; states.md: a mesh id is never
    /// reissued, deleted is final; Q109: the storage decides nothing): the record's writer writes a
    /// mesh's record at its name. A free name takes the record; the same mesh writing again with its
    /// record unchanged writes nothing; another mesh's write at the live name is refused naming the
    /// holder and writes nothing; the holder's deleted record is a write of its own record that releases
    /// the name, and a repeat of it writes nothing; the deleted mesh is never written live again; another
    /// mesh then writes its record at the name. The memory store and the file store each run their own check.
    #[tokio::test]
    async fn a_mesh_name_is_held_by_its_writers_record_and_only_a_dead_row_releases_it() {
        let d = crate::test_support::tempdir("storage-mesh-row");
        let check = async |m: &dyn MeshStorage, store: &str| {
            let (first, second) = (MeshId::mint(), MeshId::mint());
            let live = |id: &MeshId| MeshRecord::new(id.clone(), "mesh2");
            let dead = |id: &MeshId| MeshRecord { state: MeshState::Deleted, ..live(id) };
            assert_eq!(crate::storage::write_mesh(&*m, None, &live(&first)).await, Ok(Ok(MeshWritten::Written)), "{store}: a free name takes the record");
            assert_eq!(crate::storage::write_mesh(&*m, Some(&live(&first)), &live(&first)).await, Ok(Ok(MeshWritten::Held)), "{store}: the same mesh writing its own record again writes nothing");
            assert_eq!(crate::storage::write_mesh(&*m, None, &live(&second)).await, Ok(Err(MeshRefused::NameHeld { name: "mesh2".into(), by: first.clone() })), "{store}: another mesh's live row holds mesh2");
            assert_eq!(m.mesh_named("mesh2").await.unwrap(), Some(live(&first)), "{store}: nothing is written by a refused write");
            assert_eq!(crate::storage::write_mesh(&*m, Some(&live(&first)), &dead(&first)).await, Ok(Ok(MeshWritten::Written)), "{store}: the holder's deleted record is a write of its own record");
            assert_eq!(m.mesh_named("mesh2").await.unwrap(), None, "{store}: the deleted record released the name");
            assert_eq!(crate::storage::write_mesh(&*m, Some(&dead(&first)), &dead(&first)).await, Ok(Ok(MeshWritten::Held)), "{store}: a repeat of the deleted record writes nothing");
            assert_eq!(crate::storage::write_mesh(&*m, Some(&dead(&first)), &live(&first)).await, Ok(Err(MeshRefused::MeshDeleted { name: "mesh2".into(), mesh_id: first.clone() })), "{store}: a deleted mesh is never written live again");
            assert_eq!(crate::storage::write_mesh(&*m, None, &live(&second)).await, Ok(Ok(MeshWritten::Written)), "{store}: the released name is written again");
            assert_eq!(m.mesh_named("mesh2").await.unwrap(), Some(live(&second)), "{store}: the name resolves to the new mesh's record");
            assert_eq!(m.mesh(&first).await.unwrap(), Some(dead(&first)), "{store}: the deleted mesh's record stands, read by id");
        };
        check(&MemoryMeshStorage::default(), "memory").await;
        check(&FileMeshStorage::open(&crate::store_root::StoreRoot::at(&d.join("root")), &crate::store_log::LogWriter::mint()).await.unwrap(), "file").await;
        // The name is the records' (node-identity-and-state.md: the fabric-primary checks a mesh's name
        // against its records), never a lock: the file store's root holds no locks/name/.
        let names = d.join("root").join("locks").join("name");
        let held = names.exists();
        let _ = std::fs::remove_dir_all(&d);
        assert!(!held, "the file store keeps no mesh name lock at {}", names.display());
    }

    /// CONTRACT: docs/architecture/storage.md:86
    /// "| a restart with no launcher (the adoption launch) | the node itself | its own row in its data dir |"
    /// CONTRACT: docs/architecture/node-identity-and-state.md:149
    /// "node.delete releases the node's identity and name; its row stays. The node's mesh-primary writes
    /// the row with status deleted, keeping the record and its config; the deleted row releases..."
    #[tokio::test]
    async fn clear_primaries_fold_declared_and_own_in_behave_as_specified() {
        // 1. `clear_primaries` strips the primary revisions.
        let inc = IncarnationId::mint();
        let name: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let mut birth = BirthFacts::new(inc.clone(), name.clone());
        birth.is_primary = Some(3);
        birth.is_fabric_primary = Some(4);
        birth.declared.insert(NodeState::Ready);
        birth.declared.insert(NodeState::Stopped);

        clear_primaries(&mut birth);
        assert_eq!(birth.is_primary, None);
        assert_eq!(birth.is_fabric_primary, None);

        // 2. `fold_declared` extracts the greatest declared state and path name per (node_id, incarnation_id).
        let node_id = NodeId::mint();
        let mut row = NodeRow::new(name.clone(), "mesh1", "node-admin", node_id.clone(), crate::model::NodeStatus::Stopped);
        row.births.push(birth);
        let declared = fold_declared([&row]);
        assert_eq!(declared.get(&(node_id.clone(), inc.clone())), Some(&(NodeState::Stopped, name.clone())));

        // 3. `own_in` and `put_own_in` round-trip the admin's own row in its data dir.
        let d = crate::test_support::tempdir("storage-own-in");
        let writer = IncarnationId::mint();
        assert_eq!(own_in(&d, &writer).await.unwrap(), None);
        let secret = iroh::SecretKey::from_bytes(&[11u8; 32]);
        let own_record = NodeRecord { mesh: "mesh1".to_owned(),
            node_id: node_id.clone(),
            fabric_id: FabricId::mint(),
            path: name.path.clone(), name: name.name.clone(),
            mesh_id: MeshId::mint(),
            incarnation_id: inc,
            endpoint_id: EndpointId(secret.public().to_string()),
            transport_addr: "127.0.0.1:9100".parse().unwrap(),
            listeners: vec![("control".into(), "127.0.0.1:9101".parse().unwrap())],
            is_primary: Some(1),
            is_fabric_primary: Some(1),
            topology_storage_provider: Some("file".into()),
        };
        put_own_in(&d, &writer, &own_record).await.unwrap();
        assert_eq!(own_in(&d, &writer).await.unwrap(), Some(own_record));
        let _ = std::fs::remove_dir_all(&d);
    }
}
