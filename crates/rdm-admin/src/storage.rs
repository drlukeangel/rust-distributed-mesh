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
        self.records.append(&self.log_writer, MESH_KIND, MESH_FORMAT, record.mesh_id.as_str(), record).await
    }
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
        self.records.latest_of(MESH_KIND).await?.iter().map(|l| l.decode(self.records.path())).collect()
    }
}

// ---------------------------------------------------------------- a node's record

pub use rdm_node::nodes_storage::{BirthRef, CertDates, NodeLine, NodeRecord, SigningCertDates};

/// The primaries recorded on `record` leave it: a node whose exit is proven holds no primary
/// (mesh-delete.md step 7, "Deleted births resolve Gone"), written by its mesh-primary with its
/// `Deleted` state, the node being gone.
pub fn clear_primaries(record: &mut NodeRecord) {
    record.is_primary = None;
    record.is_fabric_primary = None;
}

// ---------------------------------------------------------------- nodes.storage


/// The admin's own row in its data dir (`docs/architecture/node-identity-and-state.md` §2.5–§2.8,
/// `docs/architecture/storage.md` §2): its own seed, read before any store opens
/// (`crate::storage_backend::StorageBackend::own_row`), so a restarting node-admin is the same logical
/// node in the same Fabric and Mesh. Its node record ([`NodeRecord`]) is the admin's own, in the nodes
/// store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnRow {
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

impl OwnRow {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

impl OwnRow {
    /// Where this birth's gossip was last reachable: its transport address, as the topology view dials.
    pub fn gossip_addr(&self) -> Option<iroh::EndpointAddr> {
        let key = self.endpoint_id.0.parse::<iroh::PublicKey>().ok()?;
        Some(iroh::EndpointAddr::new(key).with_ip_addr(self.transport_addr))
    }
}

/// The live records of every mesh: [`records`] with a `Deleted` record skipped. Every read of the
/// nodes a store root holds to reach (a restart's seeds, its durable map, the rafka-time peers, the
/// stored topology) reads them here.
pub async fn live_records(meshes: &dyn MeshStorage, nodes: &dyn NodesStorage, own_mesh: &str) -> Result<Vec<NodeRecord>, StorageError> {
    Ok(records(meshes, nodes, own_mesh).await?.into_iter().filter(|r| r.state != crate::model::NodeStatus::Deleted).collect())
}

/// The records of every mesh: the records of each mesh `meshes` holds a record of, and of `own_mesh`
/// (whose record a Day-0 admin's own row precedes), each mesh read once, a `Deleted` record included: a
/// deleted node's recorded primary stays an election input until a newer revision supersedes it
/// (fabric-node-lifecycle-elections.md §3), as a lost primary's does through fabric recovery.
pub async fn records(meshes: &dyn MeshStorage, nodes: &dyn NodesStorage, own_mesh: &str) -> Result<Vec<NodeRecord>, StorageError> {
    let mut names: BTreeSet<String> = meshes.meshes().await?.into_iter().map(|m| m.name).collect();
    names.insert(own_mesh.to_string());
    let mut out = Vec::new();
    for mesh in names {
        out.extend(nodes.nodes(&mesh).await?);
    }
    Ok(out)
}

#[cfg(feature = "file-backend")]
/// The format of the admin's own row in its data dir ([`own_in`]).
const NODE_FORMAT: &str = "node-record/4";

#[cfg(feature = "file-backend")]
/// The admin's own row in `own_data_dir`, read before the store opens: a restarting admin's row
/// names the Fabric whose store root it opens and the mesh whose nodes it opens on.
pub async fn own_in(own_data_dir: &Path, writer: &IncarnationId) -> Result<Option<OwnRow>, StorageError> {
    FileRecords::open(own_data_dir, "nodes", writer).await?.read("self", NODE_FORMAT).await
}

#[cfg(feature = "file-backend")]
/// Keep `record` as the admin's own row in `own_data_dir`, where [`own_in`] reads it, written
/// by the process born as `writer`.
pub async fn put_own_in(own_data_dir: &Path, writer: &IncarnationId, record: &OwnRow) -> Result<(), StorageError> {
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

    /// CONTRACT (build.md §5.3; storage.md §2; Q365): a Mesh record's line in the root's
    /// `meshes.jsonl` is frozen byte for byte: a CloudEvents event whose `source` is its writer, `type`
    /// `mesh-record`, `dataschema` `mesh-record/4`, `subject` the mesh's id and `data` the whole
    /// record; its `id` and `time` the write's. A layout that moves moves this cell with it (`durable
    /// format moved`). Must NOT happen: a line whose attributes or record bytes moved while this cell
    /// still passes.
    #[tokio::test]
    async fn the_meshes_store_line_is_frozen_byte_for_byte() {
        let d = crate::test_support::tempdir("meshes-store-layout");
        let writer = crate::store_log::LogWriter::mint();
        let s = FileMeshStorage::open(&crate::store_root::StoreRoot::at(&d).unwrap(), &writer).await.unwrap();
        let mesh_id = MeshId::parse(&rdm_core::fixture_crockford60("mesh-1")).unwrap();
        s.put_mesh(&MeshRecord::new(mesh_id.clone(), "mesh1")).await.unwrap();
        let file = d.join(crate::store_root::MESHES_FILE);
        let lines = crate::store_log::read_file(&file).unwrap();
        assert_eq!(lines.len(), 1);
        let attributes = crate::store_log::Attributes { id: lines[0].attributes.id.clone(), source: writer.source(), r#type: "mesh-record".into(), subject: Some(mesh_id.to_string()), time: None, dataschema: "mesh-record/4".into(), traceparent: None, tracestate: None };
        let record = format!(r#"{{"mesh_id":"{mesh_id}","name":"mesh1","state":"pending"}}"#);
        assert_eq!(String::from_utf8(std::fs::read(&file).unwrap()).unwrap(), String::from_utf8(crate::store_log::encode_line(&attributes, record.as_bytes())).unwrap());
    }

    use rdm_core::connections::NodeConnection;
    use rdm_core::connections::{ConnectionEnd, ConnectionKind, ConnectionState};

    fn node(name: PathName, mesh: &str, r#type: &str) -> OwnRow {
        OwnRow { mesh: mesh.to_owned(),
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
            reason: None,
            logged_at_ms: 1,
        }
    }

    /// Every boundary behaves the same in memory and on disk, and the disk copy survives reopening: a
    /// Mesh record, a node's record read at its id and listed by its mesh, the latest connection fact.
    #[tokio::test]
    async fn mesh_nodes_and_connections_hold_their_records_in_memory_and_across_a_reopen() {
        let d = crate::test_support::tempdir("storage");
        let mesh = MeshRecord::new(MeshId::mint(), "mesh1");
        let own = node(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin");
        let peer = row(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
        let (up, down) = (fact(ConnectionState::Connected), fact(ConnectionState::Disconnected));
        let check = async |m: &dyn MeshStorage, n: &dyn NodesStorage, c: &dyn ConnectionsStorage, fresh: bool| {
            if fresh {
                assert_eq!(m.mesh(&mesh.mesh_id).await.unwrap(), None);
                assert_eq!(crate::storage::write_mesh(&*m, None, &mesh).await.unwrap(), Ok(MeshWritten::Written));
                n.put_node(&peer).await.unwrap();
                c.put_connection(&up).await.unwrap();
                c.put_connection(&down).await.unwrap();
            }
            assert_eq!(m.mesh(&mesh.mesh_id).await.unwrap(), Some(mesh.clone()));
            assert_eq!(m.mesh_named("mesh1").await.unwrap(), Some(mesh.clone()), "the lookup by name names the record");
            assert_eq!(m.meshes().await.unwrap(), [mesh.clone()]);
            assert_eq!(n.node(&peer.node_id).await.unwrap().map(|l| l.record), Some(peer.clone()), "the node's record at its id");
            assert_eq!(n.nodes("mesh1").await.unwrap(), [peer.clone()], "the record is its mesh's, read from its path");
            assert_eq!(c.connections().await.unwrap(), [down.clone()], "the latest fact per (source, destination, kind)");
        };
        let mem = (MemoryMeshStorage::default(), MemoryNodesStorage::default(), MemoryConnectionsStorage::default());
        check(&mem.0, &mem.1, &mem.2, true).await;
        let connection_writer = crate::store_log::LogWriter::mint();
        let (root, w): (_, IncarnationId) = (crate::store_root::StoreRoot::at(&d.join("root")).unwrap(), IncarnationId::mint());
        let writer = || crate::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &crate::model::NodeId::mint(), &w, &rdm_transport::clock::RafkaTime::unadopted());
        check(&FileMeshStorage::open(&root, &writer()).await.unwrap(), &FileNodesStorage::open(&root, &writer()).await.unwrap(), &FileConnectionsStorage::open(&root, &connection_writer).await.unwrap(), true).await;
        let (m, n, c) = (FileMeshStorage::open(&root, &writer()).await.unwrap(), FileNodesStorage::open(&root, &writer()).await.unwrap(), FileConnectionsStorage::open(&root, &connection_writer).await.unwrap());
        check(&m, &n, &c, false).await;
        c.remove_connection(&down.index()).await.unwrap();
        assert!(c.connections().await.unwrap().is_empty());
        // The own row is the admin's own seed in its data dir, read before any store opens.
        assert_eq!(own_in(&d, &w).await.unwrap(), None);
        put_own_in(&d, &w, &own).await.unwrap();
        assert_eq!(own_in(&d, &IncarnationId::mint()).await.unwrap(), Some(own.clone()), "the own row is read back by the next process");
        assert_eq!(own.gossip_addr().map(|a| a.id.to_string()), Some(own.endpoint_id.0.clone()));
        let writer = crate::store_log::LogWriter::mint();
        std::fs::OpenOptions::new().append(true).open(root.dir().join(crate::store_root::MESHES_FILE)).and_then(|mut f| std::io::Write::write_all(&mut f, &crate::store_log::encode_line(&crate::store_log::Attributes::of(&writer, "mesh-record", "mesh-record/9", mesh.mesh_id.as_str()), b"{}"))).unwrap();
        assert!(matches!(m.mesh(&mesh.mesh_id).await, Err(StorageError::Unrecognised { .. })));
    }

    /// CONTRACT (node-identity-and-state.md §4; states.md "Who sets each state" and the paragraph after
    /// it, Q233): a node's `state` on its record follows states.md's order through a stop and a start,
    /// a declaration inside node.stop leaves it stopping, and a `Deleted` record stands. A stop's
    /// acknowledgement sets stopping over any state but stopped, a node already connections-deleted or
    /// left included. delete is the mesh-primary's workflow: deleting from its admission, over a live or
    /// a stopped node; the frames of the stop it composes move the record as a stop's do, stopping
    /// through stopped; then deleted. A ready, a pending or a starting moves a deleting record nowhere.
    // @feature: node-lifecycle
    #[test]
    fn a_node_records_state_follows_the_order_states_md_names() {
        use crate::model::NodeStatus as S;
        let mut r = row(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
        r.state = S::Starting;
        let walk = |r: &mut NodeRecord, steps: &[(S, S)]| for (to, then) in steps { r.set_state(*to); assert_eq!(r.state, *then, "after {to:?}"); };
        walk(&mut r, &[(S::Pending, S::Pending), (S::Ready, S::Ready), (S::Starting, S::Ready), (S::Pending, S::Pending), (S::Ready, S::Ready)]);
        // A stop (Q205): stopping from its admission, then each primitive's frame, then stopped.
        // The node's Stopped may be written before the stream's last frames (node-stop.md step 5): a
        // stopped record stands, and no stopping takes it.
        walk(&mut r, &[(S::Stopping, S::Stopping), (S::Ready, S::Stopping), (S::ConnectionsDeleting, S::ConnectionsDeleting), (S::ConnectionsDeleted, S::ConnectionsDeleted), (S::Leaving, S::Leaving), (S::Left, S::Left), (S::ConnectionsDeleting, S::Left), (S::Stopped, S::Stopped), (S::Left, S::Stopped), (S::Stopping, S::Stopped)]);
        walk(&mut r, &[(S::Starting, S::Starting), (S::Pending, S::Pending), (S::Ready, S::Ready)]);
        walk(&mut r, &[(S::ConnectionsDeleting, S::ConnectionsDeleting), (S::ConnectionsDeleted, S::ConnectionsDeleted), (S::ConnectionsDeleting, S::ConnectionsDeleted)]);
        // A stop of a node already connections-deleted: its acknowledgement sets stopping (Q233), and
        // its primitives' frames move the record on to stopped.
        walk(&mut r, &[(S::Stopping, S::Stopping), (S::ConnectionsDeleting, S::ConnectionsDeleting), (S::ConnectionsDeleted, S::ConnectionsDeleted), (S::Leaving, S::Leaving), (S::Left, S::Left), (S::Stopped, S::Stopped)]);
        walk(&mut r, &[(S::Starting, S::Starting), (S::Pending, S::Pending), (S::Ready, S::Ready)]);
        // A stop of a node already left: the same.
        walk(&mut r, &[(S::Leaving, S::Leaving), (S::Left, S::Left), (S::Stopping, S::Stopping), (S::ConnectionsDeleting, S::ConnectionsDeleting), (S::ConnectionsDeleted, S::ConnectionsDeleted), (S::Leaving, S::Leaving), (S::Left, S::Left), (S::Stopped, S::Stopped)]);
        walk(&mut r, &[(S::Starting, S::Starting), (S::Pending, S::Pending), (S::Ready, S::Ready)]);
        // A delete of the live node: deleting from its admission, then the composed stop's frames.
        walk(&mut r, &[(S::Deleting, S::Deleting), (S::Ready, S::Deleting), (S::Pending, S::Deleting), (S::Starting, S::Deleting)]);
        assert!(!r.clone().set_state(S::Deleting), "deleting again leaves a deleting record unchanged");
        walk(&mut r, &[(S::Stopping, S::Stopping), (S::ConnectionsDeleting, S::ConnectionsDeleting), (S::ConnectionsDeleted, S::ConnectionsDeleted), (S::Leaving, S::Leaving), (S::Left, S::Left), (S::Stopped, S::Stopped)]);
        // A delete of a stopped node: deleting from its admission over stopped, then deleted.
        walk(&mut r, &[(S::Deleting, S::Deleting), (S::Deleted, S::Deleted)]);
        walk(&mut r, &[(S::Pending, S::Deleted), (S::Starting, S::Deleted)]);
    }

    /// A ready node's record at `name` of `type` in `mesh` (the first location of `name`'s path).
    fn row(name: PathName, mesh: &str, r#type: &str) -> NodeRecord {
        assert_eq!(rdm_core::path::mesh_of(&name.path), mesh, "a node's mesh is its path's first location");
        rdm_node::nodes_storage::fixture_record(&name, r#type, NodeId::mint(), IncarnationId::mint(), crate::model::NodeStatus::Ready)
    }

    /// CONTRACT (node-identity-and-state.md §2: "A current-pair lookup reads the node records by their
    /// path and name; it never uses the last writer at a path"; node-identity-and-state.md: a delete
    /// "writes Deleted lifecycle state, which frees the pair"): a `(path, name)` resolves to the live
    /// record standing there, and `NameHeld { by: node.id }` names another node's live record. A name
    /// the node's own record holds is its own again; a delete frees the pair, so nothing resolves at the
    /// name (the `Deleted` record stands, read by id) until the next node there writes its record. In
    /// memory and on disk alike.
    #[tokio::test]
    async fn a_nodes_name_is_read_from_the_live_records_and_a_delete_frees_it() {
        let d = crate::test_support::tempdir("node-names");
        let root = crate::store_root::StoreRoot::at(&d.join("root")).unwrap();
        let file = FileNodesStorage::open(&root, &crate::store_log::LogWriter::mint()).await.unwrap();
        let mem = MemoryNodesStorage::default();
        for n in [&file as &dyn NodesStorage, &mem as &dyn NodesStorage] {
            let a = row(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
            assert_eq!(crate::node_rows::check_name(n, &a.location(), &a.node_id).await.unwrap(), Ok(()), "a free name is the node's");
            n.put_node(&a).await.unwrap();
            assert_eq!(crate::node_rows::record_at(n, &a.location()).await.unwrap(), Some(a.clone()), "the name resolves to the live record, as its node wrote it");
            assert_eq!(crate::node_rows::check_name(n, &a.location(), &a.node_id).await.unwrap(), Ok(()), "the node's own record holds its name");
            let other = row(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
            assert_eq!(crate::node_rows::check_name(n, &a.location(), &other.node_id).await.unwrap(), Err(NameHeld { path: a.path.clone(), name: a.name.clone(), by: a.node_id.clone() }), "another node's live record holds the name");
            crate::node_rows::write_deleted(n, &a.location(), &a.node_id).await.ok().expect("the delete's record");
            assert_eq!(crate::node_rows::record_at(n, &a.location()).await.unwrap(), None, "a Deleted record frees its pair: nothing resolves at the name");
            assert_eq!(crate::node_rows::check_name(n, &a.location(), &other.node_id).await.unwrap(), Ok(()), "the next node takes the name");
            n.put_node(&other).await.unwrap();
            assert_eq!(crate::node_rows::record_at(n, &a.location()).await.unwrap().map(|r| r.node_id), Some(other.node_id.clone()), "the name resolves to the new node");
            assert_eq!(n.node(&a.node_id).await.unwrap().map(|l| l.record.state), Some(crate::model::NodeStatus::Deleted), "the deleted record stands, read by id");
        }
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (R14 items 1 and 2; node-identity-and-state.md §2.5, Q356: each node writes every field
    /// of its own record): each mesh-primary writes its mesh's record, and each node of both meshes
    /// writes its own record, at once, on one root: every record is keyed by its node id, each line's
    /// `source` names the node that wrote it, no data dir holds a shared record, and each node reads
    /// the other mesh's records from the shared root.
    #[tokio::test]
    async fn each_node_writes_its_own_record_at_once_on_one_shared_root_under_its_own_key() {
        let d = crate::test_support::tempdir("shared-root");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let fabric = crate::model::FabricId::mint();
        let root = crate::store_root::StoreRoot::open(&store, &fabric).unwrap();
        let (data1, data2) = (d.join("admin-1-1-data"), d.join("admin-2-1-data"));
        let writer_of = |r: &NodeRecord| crate::store_log::LogWriter::new(&fabric, &r.node_id, &r.incarnation, &rdm_transport::clock::RafkaTime::unadopted());
        let writes = async |mesh: &str| {
            let admin = crate::store_log::LogWriter::new(&fabric, &NodeId::mint(), &IncarnationId::mint(), &rdm_transport::clock::RafkaTime::unadopted());
            let m = FileMeshStorage::open(&root, &admin).await.unwrap();
            crate::storage::write_mesh(&m, None, &MeshRecord::new(MeshId::mint(), mesh)).await.unwrap().expect("each mesh writes its record at a name no mesh holds");
            let mut written = Vec::new();
            for (name, r#type) in [(PathName::new(format!("{mesh}.node-admin"), "1").unwrap(), "node-admin"), (PathName::new(format!("{mesh}.node"), "1").unwrap(), "node"), (PathName::new(format!("{mesh}.node"), "2").unwrap(), "node")] {
                let r = row(name, mesh, r#type);
                FileNodesStorage::open(&root, &writer_of(&r)).await.unwrap().put_node(&r).await.unwrap();
                written.push((r.node_id.to_string(), writer_of(&r).source()));
            }
            written.sort();
            written
        };
        let (of1, of2) = tokio::join!(writes("mesh1"), writes("mesh2"));
        let records = crate::store_log::latest_in(&root.dir().join(crate::store_root::NODES_FILE)).unwrap();
        for (mesh, written) in [("mesh1", of1), ("mesh2", of2)] {
            let of_mesh: Vec<(String, String)> = records.iter().filter(|((kind, _), l)| kind == "node-record" && rdm_core::path::mesh_of(l.data["path"].as_str().unwrap()) == mesh).map(|((_, key), l)| (key.clone(), l.attributes.source.clone())).collect();
            assert_eq!(of_mesh, written, "{mesh}'s records, keyed by node id, each line's source the node that wrote it");
        }
        assert_eq!(crate::store_log::subjects_in(&root.dir().join(crate::store_root::MESHES_FILE), "mesh-record").unwrap().len(), 2, "one record per mesh");
        for data in [&data1, &data2] {
            assert!(!data.join(crate::store_root::MESHES_FILE).exists() && !data.join(crate::store_root::NODES_FILE).exists(), "{} holds a shared record", data.display());
        }
        let reader = FileNodesStorage::open(&root, &crate::store_log::LogWriter::mint()).await.unwrap();
        assert!(crate::node_rows::record_at(&reader, &rdm_core::PathName::new("mesh2.node", "1").unwrap()).await.unwrap().is_some(), "a node reads mesh2's record from the shared root");
        assert!(crate::node_rows::record_at(&reader, &rdm_core::PathName::new("mesh1.node", "1").unwrap()).await.unwrap().is_some(), "and mesh1's");
        let meshes = FileMeshStorage::open(&root, &crate::store_log::LogWriter::mint()).await.unwrap();
        assert_eq!(live_records(&meshes, &reader, "mesh1").await.unwrap().len(), 6, "a restart reads every mesh's records");
    }

    /// CONTRACT (R-ML2 item 4; §5.4): mesh2 created again under its name, a new mesh with a new id,
    /// takes the mesh's name over the deleted mesh's record, and its node-admin's record takes the path
    /// and name over the Deleted record there. Must NOT happen: the recreated mesh refused its name.
    #[tokio::test]
    async fn a_recreated_mesh_takes_its_names_over_the_dead_ones() {
        use crate::node_rows::RecordAt as _;
        let d = crate::test_support::tempdir("recreated-mesh");
        let root = crate::store_root::StoreRoot::at(&d.join("root")).unwrap();
        let w = crate::store_log::LogWriter::mint();
        let (meshes, nodes) = (FileMeshStorage::open(&root, &w).await.unwrap(), FileNodesStorage::open(&root, &w).await.unwrap());
        let admin: PathName = rdm_core::PathName::new("mesh2.node-admin", "1").unwrap();
        let old = MeshRecord::new(MeshId::mint(), "mesh2");
        write_mesh(&meshes, None, &old).await.unwrap().expect("the first mesh2 writes its record at its name");
        let first = row(admin.clone(), "mesh2", "node-admin");
        nodes.put_node(&first).await.unwrap();
        // mesh2 is removed: its node Deleted, its record deleted.
        nodes.put_node(&NodeRecord { state: crate::model::NodeStatus::Deleted, ..first.clone() }).await.unwrap();
        write_mesh(&meshes, Some(&old), &MeshRecord { state: MeshState::Deleted, ..old.clone() }).await.unwrap().expect("the deleted record is written");
        // mesh2 created again.
        let new = MeshRecord::new(MeshId::mint(), "mesh2");
        assert_eq!(write_mesh(&meshes, None, &new).await.unwrap(), Ok(MeshWritten::Written), "the recreated mesh takes its name over the deleted mesh's");
        assert_eq!(meshes.mesh_named("mesh2").await.unwrap().map(|m| m.mesh_id), Some(new.mesh_id.clone()));
        let second = row(admin.clone(), "mesh2", "node-admin");
        assert_eq!(crate::node_rows::check_name(&nodes, &admin, &second.node_id).await.unwrap(), Ok(()), "its node-admin takes the name over the Deleted record");
        nodes.put_node(&second).await.unwrap();
        assert_eq!(nodes.record_at(&admin).await.unwrap().map(|r| r.node_id), Some(second.node_id.clone()), "the name resolves to the new node-admin's record");
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
        check(&FileMeshStorage::open(&crate::store_root::StoreRoot::at(&d.join("root")).unwrap(), &crate::store_log::LogWriter::mint()).await.unwrap(), "file").await;
        // The name is the records' (node-identity-and-state.md: the fabric-primary checks a mesh's name
        // against its records), never a lock: the file store's root holds no locks/name/.
        let names = d.join("root").join("locks").join("name");
        let held = names.exists();
        let _ = std::fs::remove_dir_all(&d);
        assert!(!held, "the file store keeps no mesh name lock at {}", names.display());
    }

    /// CONTRACT: docs/architecture/storage.md:86
    /// "| a restart with no launcher (the adoption launch) | the node itself | its own row in its data dir |"
    /// `clear_primaries` takes both primary revisions off a record, and `own_in` / `put_own_in`
    /// round-trip the admin's own row in its data dir.
    #[tokio::test]
    async fn clear_primaries_and_own_in_behave_as_specified() {
        // 1. `clear_primaries` strips the primary revisions.
        let name: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let mut record = row(name.clone(), "mesh1", "node-admin");
        record.is_primary = Some(3);
        record.is_fabric_primary = Some(4);
        clear_primaries(&mut record);
        assert_eq!((record.is_primary, record.is_fabric_primary), (None, None));

        // 2. `own_in` and `put_own_in` round-trip the admin's own row in its data dir.
        let d = crate::test_support::tempdir("storage-own-in");
        let writer = IncarnationId::mint();
        assert_eq!(own_in(&d, &writer).await.unwrap(), None);
        let secret = iroh::SecretKey::from_bytes(&[11u8; 32]);
        let own_record = OwnRow { mesh: "mesh1".to_owned(),
            node_id: record.node_id.clone(),
            fabric_id: FabricId::mint(),
            path: name.path.clone(), name: name.name.clone(),
            mesh_id: MeshId::mint(),
            incarnation_id: record.incarnation.clone(),
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
