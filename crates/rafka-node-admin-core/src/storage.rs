//! `mesh.storage`, `nodes.storage` and `connections.storage`: node-admin control state behind
//! storage boundaries, beside `fabric.storage` (`crate::fabric_storage`) and `builds.storage`
//! (`crate::build_state::FileJournal`).
//!
//! Each boundary is a trait with a memory implementation (runs that need no restart survival) and
//! a file implementation in the admin's own data dir (`crate::record_store`: one record per file,
//! temp + fsync + rename, an unrecognised file refused by name). Rafka binds its own production
//! backends to the same traits.
//!
//! - `mesh.storage`: the admin's own Mesh (id and name), so a restarted admin rejoins the same
//!   Mesh channel rather than minting a new one.
//! - `nodes.storage`: the admin's own row (NodeId, path.name, the incarnation it last ran, its
//!   transport identity), so a restart is the same logical node; the last-known
//!   births it heard, as bootstrap contacts (a stored contact is a hint: never topology, never
//!   death proof); and, when this admin is a birth's authority, the lifecycle state that birth
//!   declared and the admin applied (`status_rpc`).
//! - `connections.storage`: the latest connection fact per (source, destination, kind).

use async_trait::async_trait;
use crate::model::{FabricId, IncarnationId, MeshId, NodeId, PathName, EndpointId};
pub use rafka_node_admin_client::NameHeld;
use crate::record_store::{FileRecords, StorageError};
use rafka_mesh_entity::connections::{ConnectionIndex, NodeConnection};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

// ---------------------------------------------------------------- mesh.storage

/// A Mesh's identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshRecord {
    /// The mesh's minted id.
    pub mesh_id: MeshId,
    /// The mesh's name.
    pub name: String,
}

/// The durable record of the meshes this admin knows.
#[async_trait]
pub trait MeshStorage: Send + Sync {
    /// The record of the mesh named `name`.
    async fn mesh(&self, name: &str) -> Result<Option<MeshRecord>, StorageError>;
    /// Keep `record`, keyed by its mesh name.
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError>;
    /// Every mesh record held.
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError>;
}

/// A mesh store held in memory.
#[derive(Debug, Default)]
pub struct MemoryMeshStorage(Mutex<BTreeMap<String, MeshRecord>>);

#[async_trait]
impl MeshStorage for MemoryMeshStorage {
    async fn mesh(&self, name: &str) -> Result<Option<MeshRecord>, StorageError> {
        Ok(self.0.lock().unwrap().get(name).cloned())
    }
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError> {
        self.0.lock().unwrap().insert(record.name.clone(), record.clone());
        Ok(())
    }
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
        Ok(self.0.lock().unwrap().values().cloned().collect())
    }
}

const MESH_FORMAT: &str = "mesh-record/1";

/// `<store root>/meshes/<name>.json`: each mesh's Mesh row, written by that mesh's primary.
#[derive(Debug)]
pub struct FileMeshStorage(FileRecords);

impl FileMeshStorage {
    /// Open the store under the fabric's store root, written by the process born as `writer`.
    pub async fn open(root: &crate::store_root::StoreRoot, writer: &IncarnationId) -> Result<Self, StorageError> {
        Ok(Self(root.records(root.dir(), "meshes", writer).await?))
    }

    /// The records the mesh-primary seat writes (its Mesh row): what a seat transfer cuts off.
    pub fn mesh_primary_records(&self) -> [&FileRecords; 1] {
        [&self.0]
    }
}

#[async_trait]
impl MeshStorage for FileMeshStorage {
    async fn mesh(&self, name: &str) -> Result<Option<MeshRecord>, StorageError> {
        self.0.read(name, MESH_FORMAT).await
    }
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError> {
        self.0.write(&record.name, MESH_FORMAT, record).await
    }
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
        self.0.list(MESH_FORMAT).await
    }
}

// ---------------------------------------------------------------- nodes.storage

/// One birth as last known: the admin's own row, or a contact it heard. Its address facts only:
/// the declared state and the silence status are their own keyed rows in `status.storage`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    /// The node's minted id.
    pub node_id: NodeId,
    /// The Fabric the birth belongs to: a restarting node-admin opens the fabric's store root
    /// (`<store_dir>/<fabric_id>/`) from its own row's, read before any store opens (R-FI1).
    pub fabric_id: FabricId,
    /// The node's `path.name`.
    pub name: PathName,
    /// The incarnation this birth last ran (a restart supersedes it).
    pub incarnation_id: IncarnationId,
    /// The node's fabric endpoint id.
    pub endpoint_id: EndpointId,
    /// The one address of the birth's Iroh endpoint.
    pub transport_addr: std::net::SocketAddr,
    /// Non-Iroh listeners (the own row of a node-admin: its `control` API).
    #[serde(default)]
    pub listeners: Vec<(String, std::net::SocketAddr)>,
}

impl NodeRecord {
    /// Where this birth's gossip was last reachable: its transport address, as membership dials.
    pub fn gossip_addr(&self) -> Option<iroh::EndpointAddr> {
        let key = self.endpoint_id.0.parse::<iroh::PublicKey>().ok()?;
        Some(iroh::EndpointAddr::new(key).with_ip_addr(self.transport_addr))
    }
}

/// One birth's exact runtime as this admin first held it: the durable map a successor fabric
/// primary proves an exit from when its own membership never heard the birth. Its own keyed row
/// (NodeId + IncarnationId), written once by a blind put, never rewritten.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeRow {
    /// The node's minted id.
    pub node_id: NodeId,
    /// The node's `path.name`.
    pub name: PathName,
    /// The incarnation of the birth.
    pub incarnation_id: IncarnationId,
    /// The runtime the birth ran in.
    pub runtime: rafka_mesh_entity::RuntimeFact,
    /// The data dir the birth reported: where its process runtime records an exit code.
    pub data_dir: Option<String>,
}

impl RuntimeRow {
    /// The row's key: the birth it describes.
    pub fn key(&self) -> String {
        format!("{}--{}", self.node_id, self.incarnation_id.0)
    }
}

/// The record of the node at one `path.name`: the node its writer's view holds there, its status
/// and its config. Its mesh's primary node-admin is its only writer (R13; the fabric-primary only as
/// the acting primary of a mesh with none, `crate::node_rows`) and writes it whole, a blind put of
/// the record it holds; the row follows the node's name and never decides who may hold one (the
/// node owns its name). A `Deleted` row keeps the config for the next node created at the same
/// `path.name`. Its store indexes: `path.name`, its key, and `id`, the name of the row each node
/// holds ([`NodesStorage::node_row_name`]), kept by the store with every write of a row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRow {
    /// The node's `path.name`: the row's key.
    pub name: PathName,
    /// The node that held the name when the row was written.
    pub node_id: NodeId,
    /// The node's status: `Deleted` once its runtime exit is proven.
    pub status: crate::model::NodeStatus,
    /// The application's own key/values on the node (capped: [`rafka_mesh_entity::check_config`]).
    pub config: BTreeMap<String, String>,
}

/// The durable record of the births this admin knows: its own row, its contacts and the runtimes it
/// started.
#[async_trait]
pub trait NodesStorage: Send + Sync {
    /// Keep the runtime of one birth (a blind put of its own row).
    async fn put_runtime(&self, row: &RuntimeRow) -> Result<(), StorageError>;
    /// Every birth's runtime row this admin holds; folded by the reader.
    async fn runtimes(&self) -> Result<Vec<RuntimeRow>, StorageError>;
    /// The runtime row of the exact birth `(node_id, incarnation)`: a point read by its key.
    async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<RuntimeRow>, StorageError>;
    /// This admin's own row; `None` before its first start completed.
    async fn own(&self) -> Result<Option<NodeRecord>, StorageError>;
    /// Keep `record` as this admin's own row.
    async fn put_own(&self, record: &NodeRecord) -> Result<(), StorageError>;
    /// The births this admin last heard, by NodeId (bootstrap contacts).
    async fn contacts(&self) -> Result<Vec<NodeRecord>, StorageError>;
    /// Keep `record` as a contact, keyed by its node id.
    async fn put_contact(&self, record: &NodeRecord) -> Result<(), StorageError>;
    /// Forget the contact of the node `node_id` under `mesh` (its key).
    async fn remove_contact(&self, mesh: &str, node_id: &NodeId) -> Result<(), StorageError>;
    /// Save `row` as the record at its `path.name`. The claim of the name is here, inside the
    /// adapter, and nowhere else (R13): insert-and-fail on the `path.name` key, decided by the store
    /// in one step against every other row write. The key is free when no row is there, the row
    /// there is the same node's, or it is another node's `Deleted` row: `row` is put (a blind put of
    /// the whole row). Any other row holds the name: `NameHeld` naming its node, and nothing is written.
    async fn put_node_row(&self, row: &NodeRow) -> Result<Result<(), NameHeld>, StorageError>;
    /// The record at `name`: a point read through the `path.name` key; `None` when none was ever written.
    async fn node_row_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError>;
    /// The `path.name` of the row the node `node_id` of `mesh` holds now: a point read through the
    /// node row's id index (the row's own key is its `path.name`, so a lookup by node id cannot read
    /// it). The index entry is kept by the store in the same step as every write of a row (a put, a
    /// release), so it names exactly the row the node holds; `None` when it holds none.
    async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, StorageError>;
    /// `holder` lets go of the key `name`: its row goes when it is `holder`'s, decided by the store
    /// in one step against every other row write; a row of another node there (the name was
    /// released before and claimed since) is left as it is. Returns whether `holder`'s row went.
    async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, StorageError>;
}

/// What a save of `row` meets in `held`, the row at its key: free, or held by another node.
fn claim_against(held: Option<&NodeRow>, row: &NodeRow) -> Result<(), NameHeld> {
    match held {
        Some(h) if h.node_id != row.node_id && h.status != crate::model::NodeStatus::Deleted => Err(NameHeld { name: row.name.clone(), by: h.node_id.clone() }),
        _ => Ok(()),
    }
}

/// A nodes store held in memory.
#[derive(Debug, Default)]
pub struct MemoryNodesStorage {
    runtimes: Mutex<BTreeMap<String, RuntimeRow>>,
    own: Mutex<Option<NodeRecord>>,
    contacts: Mutex<BTreeMap<NodeId, NodeRecord>>,
    rows: Mutex<BTreeMap<String, NodeRow>>,
    /// The node row's id index: each node's row's `path.name`, kept with `rows` under its lock.
    ids: Mutex<BTreeMap<NodeId, PathName>>,
}

/// The id index after `row` is kept at its key, over `previous`, the row it replaced there: the
/// row's node names the key, and a previous row of another node no longer holds it.
fn index_put(ids: &mut BTreeMap<NodeId, PathName>, previous: Option<&NodeRow>, row: &NodeRow) {
    if let Some(p) = previous.filter(|p| p.node_id != row.node_id) {
        if ids.get(&p.node_id) == Some(&p.name) {
            ids.remove(&p.node_id);
        }
    }
    ids.insert(row.node_id.clone(), row.name.clone());
}

#[async_trait]
impl NodesStorage for MemoryNodesStorage {
    async fn put_runtime(&self, row: &RuntimeRow) -> Result<(), StorageError> {
        self.runtimes.lock().unwrap().insert(row.key(), row.clone());
        Ok(())
    }
    async fn runtimes(&self) -> Result<Vec<RuntimeRow>, StorageError> {
        Ok(self.runtimes.lock().unwrap().values().cloned().collect())
    }
    async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<RuntimeRow>, StorageError> {
        Ok(self.runtimes.lock().unwrap().get(&format!("{}--{}", node_id, incarnation.0)).cloned())
    }
    async fn own(&self) -> Result<Option<NodeRecord>, StorageError> {
        Ok(self.own.lock().unwrap().clone())
    }
    async fn put_own(&self, record: &NodeRecord) -> Result<(), StorageError> {
        *self.own.lock().unwrap() = Some(record.clone());
        Ok(())
    }
    async fn contacts(&self) -> Result<Vec<NodeRecord>, StorageError> {
        Ok(self.contacts.lock().unwrap().values().cloned().collect())
    }
    async fn put_contact(&self, record: &NodeRecord) -> Result<(), StorageError> {
        self.contacts.lock().unwrap().insert(record.node_id.clone(), record.clone());
        Ok(())
    }
    async fn remove_contact(&self, _mesh: &str, node_id: &NodeId) -> Result<(), StorageError> {
        self.contacts.lock().unwrap().remove(node_id);
        Ok(())
    }
    async fn put_node_row(&self, row: &NodeRow) -> Result<Result<(), NameHeld>, StorageError> {
        // The internal index is the map's key: the look and the insert are one step under its lock.
        let mut rows = self.rows.lock().unwrap();
        let claimed = claim_against(rows.get(&row.name.to_string()), row);
        if claimed.is_ok() {
            let previous = rows.insert(row.name.to_string(), row.clone());
            index_put(&mut self.ids.lock().unwrap(), previous.as_ref(), row);
        }
        Ok(claimed)
    }
    async fn node_row_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError> {
        Ok(self.rows.lock().unwrap().get(&name.to_string()).cloned())
    }
    async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, StorageError> {
        Ok(self.ids.lock().unwrap().get(node_id).filter(|n| n.mesh == mesh).cloned())
    }
    async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, StorageError> {
        let mut rows = self.rows.lock().unwrap();
        let key = name.to_string();
        let held = rows.get(&key).is_some_and(|r| &r.node_id == holder);
        if held {
            rows.remove(&key);
            let mut ids = self.ids.lock().unwrap();
            if ids.get(holder) == Some(name) {
                ids.remove(holder);
            }
        }
        Ok(held)
    }
}

const NODE_FORMAT: &str = "node-record/3";
const RUNTIME_FORMAT: &str = "node-runtime/1";
const ROW_FORMAT: &str = "node-row/1";
const ROW_ID_FORMAT: &str = "node-row-id/1";

/// One entry of the node row's id index: the `path.name` of the row its node holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RowId {
    node_id: NodeId,
    name: PathName,
}

/// One mesh's rows on the fabric's store root, `nodes/<mesh>/`: its members' Node rows
/// (`rows/<path.name>.json`), the rows' id index (`ids/<node id>.json`: the `path.name` of the row
/// the node holds) and contacts (`contacts/<node id>.json`), written by that mesh's primary.
#[derive(Debug)]
struct MeshNodes {
    rows: FileRecords,
    ids: FileRecords,
    contacts: FileRecords,
}

impl MeshNodes {
    async fn open(root: &crate::store_root::StoreRoot, mesh: &str, writer: &IncarnationId) -> Result<Self, StorageError> {
        let base = root.nodes(mesh);
        Ok(Self { rows: root.records(&base, "rows", writer).await?, ids: root.records(&base, "ids", writer).await?, contacts: root.records(&base, "contacts", writer).await? })
    }

    /// The id index after `row` was written at its key over `previous`, the row it replaced there
    /// (written after the row, so an entry never names a row not yet written): the row's node names
    /// the key, and a previous row of another node (a `Deleted` row a recovery's new node takes
    /// over) no longer holds it. An entry that already names the key is left as it is: a put of the
    /// row a node holds (a status change, a delete's `Deleted` row) is the one write of its row. An
    /// entry that does not name it yet is written, also when the write that put the row ended
    /// before its entry was written (the row and its entry are two files): the node's next put of
    /// the key names it.
    async fn index_put(&self, previous: Option<&NodeRow>, row: &NodeRow) -> Result<(), StorageError> {
        if let Some(p) = previous.filter(|p| p.node_id != row.node_id) {
            self.index_drop(&p.node_id, &p.name).await?;
        }
        if self.ids.read::<RowId>(row.node_id.as_str(), ROW_ID_FORMAT).await?.is_some_and(|e| e.name == row.name) {
            return Ok(());
        }
        self.ids.write(row.node_id.as_str(), ROW_ID_FORMAT, &RowId { node_id: row.node_id.clone(), name: row.name.clone() }).await
    }

    /// `node_id`'s entry goes when it names `name`, the key whose row the node no longer holds.
    async fn index_drop(&self, node_id: &NodeId, name: &PathName) -> Result<(), StorageError> {
        if self.ids.read::<RowId>(node_id.as_str(), ROW_ID_FORMAT).await?.is_some_and(|e| &e.name == name) {
            self.ids.remove(node_id.as_str()).await?;
        }
        Ok(())
    }
}

/// The admin's own row and the runtimes it started in its data dir (`nodes/self.json`,
/// `nodes/runtimes/`), and every mesh's Node rows and contacts under `nodes/<mesh>/` of the
/// fabric's store root (`crate::store_root`).
#[derive(Debug)]
pub struct FileNodesStorage {
    own: FileRecords,
    runtimes: FileRecords,
    root: crate::store_root::StoreRoot,
    writer: IncarnationId,
    /// This admin's own mesh's rows: what its mesh-primary seat writes.
    mine: (String, std::sync::Arc<MeshNodes>),
    /// Every other mesh's rows this admin has read or written, opened on first use.
    others: Mutex<BTreeMap<String, std::sync::Arc<MeshNodes>>>,
    /// Set when the fabric-primary seat this admin held moved (a seat transfer's cut-off): every
    /// other mesh's rows, those opened later included, refuse writes naming it.
    others_cut_off: Mutex<Option<String>>,
    /// Every write of a row is one at a time, so a save's or a release's look at the key and its
    /// write are one step against every other write of this store.
    row_writes: tokio::sync::Mutex<()>,
}

impl FileNodesStorage {
    /// Open the store: the admin's own row and runtimes in `own_data_dir`, the rows of `own_mesh`
    /// (and of every other mesh, on first use) under the fabric's store `root`, written by the
    /// process born as `writer`.
    pub async fn open(own_data_dir: &Path, root: &crate::store_root::StoreRoot, own_mesh: &str, writer: &IncarnationId) -> Result<Self, StorageError> {
        Ok(Self {
            own: FileRecords::open(own_data_dir, "nodes", writer).await?,
            runtimes: FileRecords::open(own_data_dir, "nodes/runtimes", writer).await?,
            root: root.clone(),
            writer: writer.clone(),
            mine: (own_mesh.to_string(), std::sync::Arc::new(MeshNodes::open(root, own_mesh, writer).await?)),
            others: Mutex::new(BTreeMap::new()),
            others_cut_off: Mutex::new(None),
            row_writes: tokio::sync::Mutex::new(()),
        })
    }

    /// The admin's own row in `own_data_dir`, read before the store opens: a restarting admin's row
    /// names the Fabric whose store root it opens and the mesh whose rows it opens on.
    pub async fn own_in(own_data_dir: &Path, writer: &IncarnationId) -> Result<Option<NodeRecord>, StorageError> {
        FileRecords::open(own_data_dir, "nodes", writer).await?.read("self", NODE_FORMAT).await
    }

    /// Keep `row` among the runtimes of the node whose data dir is `own_data_dir` (a launch handing a
    /// node-admin the runtimes it holds before it runs), written by the process born as `writer`.
    pub async fn put_runtime_in(own_data_dir: &Path, writer: &IncarnationId, row: &RuntimeRow) -> Result<(), StorageError> {
        FileRecords::open(own_data_dir, "nodes/runtimes", writer).await?.write(&row.key(), RUNTIME_FORMAT, row).await
    }

    /// The records the mesh-primary seat writes (its own mesh's members' Node rows and contacts):
    /// what a seat transfer cuts off (R14 item 4).
    pub fn mesh_primary_records(&self) -> [&FileRecords; 3] {
        [&self.mine.1.rows, &self.mine.1.ids, &self.mine.1.contacts]
    }

    /// The rows of `mesh` on the shared root: a mesh this store has not opened yet is opened on the
    /// blocking pool, outside the lock, and kept once under it (the cut-off read under the same lock).
    async fn mesh(&self, mesh: &str) -> Result<std::sync::Arc<MeshNodes>, StorageError> {
        if mesh == self.mine.0 {
            return Ok(self.mine.1.clone());
        }
        if let Some(m) = self.others.lock().unwrap_or_else(|e| e.into_inner()).get(mesh) {
            return Ok(m.clone());
        }
        let opened = std::sync::Arc::new(MeshNodes::open(&self.root, mesh, &self.writer).await?);
        let mut others = self.others.lock().unwrap_or_else(|e| e.into_inner());
        let m = others.entry(mesh.to_string()).or_insert(opened).clone();
        if let Some(reason) = self.others_cut_off.lock().unwrap_or_else(|e| e.into_inner()).clone() {
            m.rows.cut_off(reason.clone());
            m.ids.cut_off(reason.clone());
            m.contacts.cut_off(reason);
        }
        Ok(m)
    }

    /// The fabric-primary seat moves (R14 item 4): cut off this store's writes to every mesh's rows
    /// but its own, those it opens later included, and wait for every write already issued to them.
    pub async fn cut_off_other_meshes(&self, reason: &str) {
        let opened: Vec<std::sync::Arc<MeshNodes>> = {
            let others = self.others.lock().unwrap_or_else(|e| e.into_inner());
            *self.others_cut_off.lock().unwrap_or_else(|e| e.into_inner()) = Some(reason.to_string());
            others.values().cloned().collect()
        };
        let stores: Vec<&FileRecords> = opened.iter().flat_map(|m| [&m.rows, &m.ids, &m.contacts]).collect();
        crate::record_store::cut_off_and_drain(&stores, reason).await;
    }

    /// Lift [`Self::cut_off_other_meshes`]: the transfer it was set for committed nothing. The
    /// first store whose kept rows could not be written is named; every store is lifted.
    pub async fn lift_other_meshes(&self) -> Result<(), StorageError> {
        let opened: Vec<std::sync::Arc<MeshNodes>> = {
            let others = self.others.lock().unwrap_or_else(|e| e.into_inner());
            *self.others_cut_off.lock().unwrap_or_else(|e| e.into_inner()) = None;
            others.values().cloned().collect()
        };
        let mut first = Ok(());
        for m in opened {
            for s in [&m.rows, &m.ids, &m.contacts] {
                if let (Err(e), true) = (s.lift_cut_off().await, first.is_ok()) {
                    first = Err(e);
                }
            }
        }
        first
    }

    /// The seat transfer [`Self::cut_off_other_meshes`] was set for committed: every row the other
    /// meshes' stores still hold is dropped, never written. Returns each store that dropped rows, with
    /// how many.
    pub async fn drop_other_meshes(&self) -> Vec<(String, usize)> {
        let opened: Vec<std::sync::Arc<MeshNodes>> = self.others.lock().unwrap_or_else(|e| e.into_inner()).values().cloned().collect();
        let mut dropped = Vec::new();
        for m in opened {
            for s in [&m.rows, &m.ids, &m.contacts] {
                let n = s.drop_held().await;
                if n > 0 {
                    dropped.push((s.store_name(), n));
                }
            }
        }
        dropped
    }

    /// Every mesh that holds rows on the shared root, by name.
    async fn meshes_on_root(&self) -> Result<Vec<String>, StorageError> {
        let dir = self.root.dir().join("nodes");
        crate::record_store::blocking_io(move || match std::fs::read_dir(&dir) {
            Ok(entries) => {
                let mut names: Vec<String> = entries.filter_map(|e| e.ok()).filter(|e| e.path().is_dir()).map(|e| e.file_name().to_string_lossy().into_owned()).collect();
                names.sort();
                Ok(names)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(StorageError::Io { file: dir.display().to_string(), reason: e.to_string() }),
        })
        .await
    }
}

#[async_trait]
impl NodesStorage for FileNodesStorage {
    async fn put_runtime(&self, row: &RuntimeRow) -> Result<(), StorageError> {
        self.runtimes.write(&row.key(), RUNTIME_FORMAT, row).await
    }
    async fn runtimes(&self) -> Result<Vec<RuntimeRow>, StorageError> {
        self.runtimes.list(RUNTIME_FORMAT).await
    }
    async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<RuntimeRow>, StorageError> {
        self.runtimes.read(&format!("{}--{}", node_id, incarnation.0), RUNTIME_FORMAT).await
    }
    async fn own(&self) -> Result<Option<NodeRecord>, StorageError> {
        self.own.read("self", NODE_FORMAT).await
    }
    async fn put_own(&self, record: &NodeRecord) -> Result<(), StorageError> {
        self.own.write("self", NODE_FORMAT, record).await
    }
    async fn contacts(&self) -> Result<Vec<NodeRecord>, StorageError> {
        let mut all = Vec::new();
        for mesh in self.meshes_on_root().await? {
            all.extend(self.mesh(&mesh).await?.contacts.list::<NodeRecord>(NODE_FORMAT).await?);
        }
        Ok(all)
    }
    async fn put_contact(&self, record: &NodeRecord) -> Result<(), StorageError> {
        self.mesh(&record.name.mesh).await?.contacts.write(record.node_id.as_str(), NODE_FORMAT, record).await
    }
    async fn remove_contact(&self, mesh: &str, node_id: &NodeId) -> Result<(), StorageError> {
        self.mesh(mesh).await?.contacts.remove(node_id.as_str()).await
    }
    async fn put_node_row(&self, row: &NodeRow) -> Result<Result<(), NameHeld>, StorageError> {
        // One row write at a time: the look at the key and the write are one step against every
        // other write of this store.
        let _one = self.row_writes.lock().await;
        let key = row.name.to_string();
        let rows = self.mesh(&row.name.mesh).await?;
        let previous = rows.rows.read::<NodeRow>(&key, ROW_FORMAT).await?;
        let claimed = claim_against(previous.as_ref(), row);
        if claimed.is_ok() {
            rows.rows.write(&key, ROW_FORMAT, row).await?;
            rows.index_put(previous.as_ref(), row).await?;
        }
        Ok(claimed)
    }
    async fn node_row_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError> {
        self.mesh(&name.mesh).await?.rows.read(&name.to_string(), ROW_FORMAT).await
    }
    async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, StorageError> {
        Ok(self.mesh(mesh).await?.ids.read::<RowId>(node_id.as_str(), ROW_ID_FORMAT).await?.map(|e| e.name))
    }
    async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, StorageError> {
        let _one = self.row_writes.lock().await;
        let key = name.to_string();
        let rows = self.mesh(&name.mesh).await?;
        let held = rows.rows.read::<NodeRow>(&key, ROW_FORMAT).await?.is_some_and(|r| &r.node_id == holder);
        if held {
            rows.rows.remove(&key).await?;
            rows.index_drop(holder, name).await?;
        }
        Ok(held)
    }
}

// ---------------------------------------------------------------- connections.storage

/// The durable record of the connection facts a node holds as a source: one resident entry per
/// index and a raw history.
#[async_trait]
pub trait ConnectionsStorage: Send + Sync {
    /// Keep `fact` as the latest for its (source, destination, kind).
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError>;
    /// Every resident connection fact.
    async fn connections(&self) -> Result<Vec<NodeConnection>, StorageError>;
    /// Forget the resident entry at `index`.
    async fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError>;
    /// Append `fact` to the raw connection log (connections.md section 3): the history every
    /// fact joins, while the index above holds one entry per (source, destination, kind).
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), StorageError>;
    /// The raw log, oldest first.
    async fn history(&self) -> Result<Vec<NodeConnection>, StorageError>;
}

/// A connections store held in memory.
#[derive(Debug, Default)]
pub struct MemoryConnectionsStorage(Mutex<BTreeMap<ConnectionIndex, NodeConnection>>, Mutex<Vec<NodeConnection>>);

#[async_trait]
impl ConnectionsStorage for MemoryConnectionsStorage {
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.0.lock().unwrap().insert(fact.index(), fact.clone());
        Ok(())
    }
    async fn connections(&self) -> Result<Vec<NodeConnection>, StorageError> {
        Ok(self.0.lock().unwrap().values().cloned().collect())
    }
    async fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError> {
        self.0.lock().unwrap().remove(index);
        Ok(())
    }
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.1.lock().unwrap().push(fact.clone());
        Ok(())
    }
    async fn history(&self) -> Result<Vec<NodeConnection>, StorageError> {
        Ok(self.1.lock().unwrap().clone())
    }
}

const CONNECTION_FORMAT: &str = "node-connection/1";

fn connection_key(i: &ConnectionIndex) -> String {
    let kind = match i.kind {
        rafka_mesh_entity::connections::ConnectionKind::Direct => "direct",
        rafka_mesh_entity::connections::ConnectionKind::Proxy => "proxy",
    };
    format!("{}--{}--{kind}", i.source, i.destination)
}

/// `<store root>/connections/<node>/<source>--<destination>--<kind>.json` and its raw log
/// `history.jsonl`: the node's own connections rows, written by the node itself.
#[derive(Debug)]
pub struct FileConnectionsStorage(FileRecords, std::sync::Arc<HistoryLog>);

/// A node's raw connection log, `history.jsonl`: appended one line per fact, one appender at a
/// time; held in the scratchpad, like the node's connections rows, until its publication.
#[derive(Debug)]
struct HistoryLog {
    path: std::path::PathBuf,
    /// The store's directory under the store root: how a publication names it.
    store: String,
    append: tokio::sync::Mutex<()>,
    /// `Some` while the log holds its lines in the scratchpad: the lines held, oldest first.
    held: Mutex<Option<Vec<Vec<u8>>>>,
}

impl HistoryLog {
    /// Append `lines` to the file and fsync it, on the blocking pool.
    async fn append_to_file(&self, lines: Vec<Vec<u8>>) -> Result<(), StorageError> {
        use std::io::Write as _;
        let path = self.path.clone();
        crate::record_store::blocking_io(move || {
            let io = |e: std::io::Error| StorageError::Io { file: path.display().to_string(), reason: e.to_string() };
            let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&path).map_err(io)?;
            for line in &lines {
                f.write_all(line).map_err(io)?;
            }
            f.sync_all().map_err(io)
        })
        .await
    }
}

#[async_trait]
impl crate::record_store::HeldStore for HistoryLog {
    async fn publish(&self) -> Result<crate::record_store::StorePublished, StorageError> {
        let _g = self.append.lock().await;
        let lines = self.held.lock().unwrap_or_else(|e| e.into_inner()).clone().unwrap_or_default();
        let written = lines.len();
        if written > 0 {
            self.append_to_file(lines).await?;
        }
        *self.held.lock().unwrap_or_else(|e| e.into_inner()) = None;
        Ok(crate::record_store::StorePublished { store: self.store.clone(), written, ..Default::default() })
    }
}

impl FileConnectionsStorage {
    /// Open the store of the node `node` under `connections/<node>/` of the fabric's store root,
    /// written by the process born as `writer`: held in the root's scratchpad until its publication.
    pub async fn open(root: &crate::store_root::StoreRoot, node: &PathName, writer: &IncarnationId) -> Result<Self, StorageError> {
        let dir = root.connections(node);
        let records = root.records(&dir, "", writer).await?;
        let log = std::sync::Arc::new(HistoryLog {
            path: dir.join("history.jsonl"),
            store: format!("connections/{node}/history"),
            append: tokio::sync::Mutex::new(()),
            held: Mutex::new(Some(Vec::new())),
        });
        if !root.scratchpad().is_some_and(|s| s.enrol(log.clone())) {
            *log.held.lock().unwrap_or_else(|e| e.into_inner()) = None;
        }
        Ok(Self(records, log))
    }

    /// Append `fact` to the raw log, one appender at a time: held while the log holds its lines,
    /// else written and fsynced on the blocking pool.
    async fn append_history_line(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        let path = &self.1.path;
        let mut line = serde_json::to_vec(fact).map_err(|e| StorageError::Io { file: path.display().to_string(), reason: e.to_string() })?;
        line.push(b'\n');
        let _g = self.1.append.lock().await;
        if let Some(held) = self.1.held.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
            held.push(line);
            return Ok(());
        }
        self.1.append_to_file(vec![line]).await
    }
}

#[async_trait]
impl ConnectionsStorage for FileConnectionsStorage {
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.0.write(&connection_key(&fact.index()), CONNECTION_FORMAT, fact).await
    }
    async fn connections(&self) -> Result<Vec<NodeConnection>, StorageError> {
        self.0.list(CONNECTION_FORMAT).await
    }
    async fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError> {
        self.0.remove(&connection_key(index)).await
    }
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.append_history_line(fact).await
    }
    async fn history(&self) -> Result<Vec<NodeConnection>, StorageError> {
        let path = &self.1.path;
        // One reader or appender at a time: a publication moving the held lines to the file never
        // runs between the two reads below.
        let _g = self.1.append.lock().await;
        let held = self.1.held.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let file = path.clone();
        let mut text = match crate::record_store::blocking_io(move || std::fs::read_to_string(&file)).await {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
        };
        if let Some(lines) = held {
            for l in lines {
                text.push_str(&String::from_utf8_lossy(&l));
            }
        }
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).map_err(|e| StorageError::Unrecognised { file: path.display().to_string(), reason: e.to_string() }))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafka_mesh_entity::connections::{ConnectionEnd, ConnectionKind, ConnectionState};

    fn node(name: &str) -> NodeRecord {
        NodeRecord {
            node_id: NodeId::mint(),
            fabric_id: FabricId::mint(),
            name: name.parse().unwrap(),
            incarnation_id: IncarnationId::mint(),
            endpoint_id: EndpointId(iroh::SecretKey::generate().public().to_string()),
            transport_addr: "127.0.0.1:41001".parse().unwrap(),
            listeners: vec![],
        }
    }

    fn end(name: &str) -> ConnectionEnd {
        ConnectionEnd { name: name.parse().unwrap(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) }
    }

    fn fact(state: ConnectionState) -> NodeConnection {
        NodeConnection {
            id: rafka_mesh_entity::ConnectionId::mint(),
            source: end("mesh1.admin.1"),
            destination: end("mesh1.rpc.1"),
            kind: ConnectionKind::Direct,
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
        let d = crate::record_store::tempdir("storage");
        let mesh = MeshRecord { mesh_id: MeshId::mint(), name: "mesh1".into() };
        let (own, peer) = (node("mesh1.admin.1"), node("mesh1.rpc.1"));
        let (up, down) = (fact(ConnectionState::Connected), fact(ConnectionState::Disconnected));
        let check = async |m: &dyn MeshStorage, n: &dyn NodesStorage, c: &dyn ConnectionsStorage, fresh: bool| {
            if fresh {
                assert_eq!(m.mesh("mesh1").await.unwrap(), None);
                assert_eq!(n.own().await.unwrap(), None);
                m.put_mesh(&mesh).await.unwrap();
                n.put_own(&own).await.unwrap();
                n.put_contact(&peer).await.unwrap();
                c.put_connection(&up).await.unwrap();
                c.put_connection(&down).await.unwrap();
            }
            assert_eq!(m.mesh("mesh1").await.unwrap(), Some(mesh.clone()));
            assert_eq!(m.meshes().await.unwrap(), [mesh.clone()]);
            assert_eq!(n.own().await.unwrap(), Some(own.clone()));
            assert_eq!(n.contacts().await.unwrap(), [peer.clone()]);
            assert_eq!(c.connections().await.unwrap(), [down.clone()], "the latest fact per (source, destination, kind)");
        };
        let mem = (MemoryMeshStorage::default(), MemoryNodesStorage::default(), MemoryConnectionsStorage::default());
        check(&mem.0, &mem.1, &mem.2, true).await;
        let (root, w, me): (_, IncarnationId, PathName) = (crate::store_root::StoreRoot::at(&d.join("root")), IncarnationId::mint(), "mesh1.admin.1".parse().unwrap());
        check(&FileMeshStorage::open(&root, &w).await.unwrap(), &FileNodesStorage::open(&d, &root, "mesh1", &w).await.unwrap(), &FileConnectionsStorage::open(&root, &me, &w).await.unwrap(), true).await;
        let (m, n, c) = (FileMeshStorage::open(&root, &w).await.unwrap(), FileNodesStorage::open(&d, &root, "mesh1", &w).await.unwrap(), FileConnectionsStorage::open(&root, &me, &w).await.unwrap());
        check(&m, &n, &c, false).await;
        n.remove_contact(&peer.name.mesh, &peer.node_id).await.unwrap();
        c.remove_connection(&down.index()).await.unwrap();
        assert!(n.contacts().await.unwrap().is_empty() && c.connections().await.unwrap().is_empty());
        assert_eq!(peer.gossip_addr().map(|a| a.id.to_string()), Some(peer.endpoint_id.0.clone()));
        std::fs::write(root.dir().join("meshes").join("mesh1.json"), br#"{"format":"mesh-record/9","record":{}}"#).unwrap();
        assert!(matches!(m.mesh("mesh1").await, Err(StorageError::Unrecognised { .. })));
    }

    fn row(name: &str) -> NodeRow {
        NodeRow { name: name.parse().unwrap(), node_id: NodeId::mint(), status: crate::model::NodeStatus::ReadyForTraffic, config: BTreeMap::new() }
    }

    /// CONTRACT (CLAUDE.md read rule: a lookup by id that cannot read the store's own key reads the
    /// kind's `id` index): the node row's id index names exactly the row each node holds, kept by the
    /// store in the same step as every row write, in memory and on disk alike. A put at a free key
    /// names it; a move (put at the new key, release the old) leaves it naming the new one; a release
    /// drops it; a recovery's new node put over a `Deleted` row of another node leaves that node with
    /// no entry; a put refused by another node's live row writes no entry and leaves the holder's. A
    /// node that never held a row has none; a put of the key the node already holds names it (a
    /// write that ended between the row file and its entry) and, once named, writes no entry again.
    /// What must NOT happen: an entry naming a key whose row another node holds, or a row its node
    /// holds that no entry names.
    #[tokio::test]
    async fn the_node_rows_id_index_names_exactly_the_row_each_node_holds() {
        let d = crate::record_store::tempdir("row-id-index");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let file = FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        let mem = MemoryNodesStorage::default();
        for n in [&file as &dyn NodesStorage, &mem as &dyn NodesStorage] {
            let a = row("mesh1.rpc.1");
            assert_eq!(n.node_row_name("mesh1", &a.node_id).await.unwrap(), None, "a node that never held a row has no entry");
            assert_eq!(n.put_node_row(&a).await.unwrap(), Ok(()));
            assert_eq!(n.node_row_name("mesh1", &a.node_id).await.unwrap(), Some(a.name.clone()));
            let old = NodeRow { name: a.name.renamed(), ..a.clone() };
            assert_eq!(n.put_node_row(&old).await.unwrap(), Ok(()));
            assert!(n.release_node_row(&a.name, &a.node_id).await.unwrap());
            assert_eq!(n.node_row_name("mesh1", &a.node_id).await.unwrap(), Some(old.name.clone()), "a move leaves the entry naming the new key");
            assert!(n.release_node_row(&old.name, &a.node_id).await.unwrap());
            assert_eq!(n.node_row_name("mesh1", &a.node_id).await.unwrap(), None, "a release drops the entry");
            // A recovery: the exited node's Deleted row is put over by the new node at its name.
            let gone = NodeRow { status: crate::model::NodeStatus::Deleted, ..row("mesh1.rpc.2") };
            assert_eq!(n.put_node_row(&gone).await.unwrap(), Ok(()));
            let new = NodeRow { node_id: NodeId::mint(), ..row("mesh1.rpc.2") };
            assert_eq!(n.put_node_row(&new).await.unwrap(), Ok(()));
            assert_eq!((n.node_row_name("mesh1", &gone.node_id).await.unwrap(), n.node_row_name("mesh1", &new.node_id).await.unwrap()), (None, Some(new.name.clone())), "the name taken over is the new node's alone");
            // A put refused by another node's live row at the key.
            let over = NodeRow { node_id: NodeId::mint(), ..new.clone() };
            assert_eq!(n.put_node_row(&over).await.unwrap(), Err(NameHeld { name: new.name.clone(), by: new.node_id.clone() }));
            assert_eq!((n.node_row_name("mesh1", &new.node_id).await.unwrap(), n.node_row_name("mesh1", &over.node_id).await.unwrap()), (Some(new.name.clone()), None), "a refused put names nothing and leaves the holder's entry");
        }
        assert_eq!(keys(&root.nodes("mesh1").join("ids")).len(), 1, "on disk, one entry per node that holds a row");
        // A put of the row a node already holds at its key (a status change: the Deleted row of a
        // delete) writes the row and nothing of the index: its entry already names the key, and a
        // durable write that changes nothing is a second fsync on every such put.
        let held = row("mesh1.rpc.4");
        assert_eq!(file.put_node_row(&held).await.unwrap(), Ok(()));
        let entry = root.nodes("mesh1").join("ids").join(format!("{}.json", held.node_id));
        let inode = |p: &std::path::Path| std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(p).unwrap());
        let before = inode(&entry);
        assert_eq!(file.put_node_row(&NodeRow { status: crate::model::NodeStatus::Deleted, ..held.clone() }).await.unwrap(), Ok(()));
        assert_eq!(inode(&entry), before, "the entry naming the key is not written again");
        assert_eq!(file.node_row_name("mesh1", &held.node_id).await.unwrap(), Some(held.name.clone()));
        // A write that ended between the row and its entry (two files): the node's next put of the
        // key it holds names it.
        let torn = row("mesh1.rpc.3");
        std::fs::write(root.nodes("mesh1").join("rows").join("mesh1.rpc.3.json"), serde_json::to_vec(&serde_json::json!({"format": ROW_FORMAT, "record": torn})).unwrap()).unwrap();
        assert_eq!(file.node_row_name("mesh1", &torn.node_id).await.unwrap(), None);
        assert_eq!(file.put_node_row(&torn).await.unwrap(), Ok(()));
        assert_eq!(file.node_row_name("mesh1", &torn.node_id).await.unwrap(), Some(torn.name.clone()), "the put of a key the node holds names it");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (the id index names exactly the row each node holds; R13: one rename write path,
    /// `node_rows::move_row`): a rename's move whose put of the new row ended between the row file and
    /// its id index entry (two files) is retried, and the retry finds the node's row already at the new
    /// name: it leaves that row as it is, names it in the index and lets go of the old key. What must
    /// NOT happen: the node left holding its new row with no entry, so the view lists a stopped birth
    /// at the name it published before the move.
    #[tokio::test]
    async fn a_move_retried_over_a_row_written_without_its_entry_leaves_the_row_named() {
        let d = crate::record_store::tempdir("row-id-torn-move");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let file = FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        let from = row("mesh1.rpc.1");
        assert_eq!(file.put_node_row(&from).await.unwrap(), Ok(()));
        let to = NodeRow { name: from.name.renamed(), ..from.clone() };
        // The first move's put wrote the row at `to` and ended before its entry.
        std::fs::write(root.nodes("mesh1").join("rows").join(format!("{}.json", to.name)), serde_json::to_vec(&serde_json::json!({"format": ROW_FORMAT, "record": to})).unwrap()).unwrap();
        let mut record = crate::model::Node::allocated(from.name.clone());
        record.node_id = from.node_id.clone();
        record.status = from.status;
        crate::node_rows::move_row(&file, &"mesh1.admin.1".parse().unwrap(), &record, BTreeMap::new(), &from.name, &to.name).await.unwrap();
        assert_eq!(file.node_row_at(&from.name).await.unwrap(), None, "the old key is let go");
        assert_eq!(file.node_row_at(&to.name).await.unwrap(), Some(to.clone()), "the row the node held there is left as it is");
        assert_eq!(file.node_row_name("mesh1", &from.node_id).await.unwrap(), Some(to.name.clone()), "the row the node holds is named in the id index");
        let _ = std::fs::remove_dir_all(&d);
    }

    fn keys(dir: &std::path::Path) -> Vec<String> {
        let mut k: Vec<String> = std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).filter(|n| n.ends_with(".json")).collect();
        k.sort();
        k
    }

    /// CONTRACT (R14 items 1 and 2; acceptance "two mesh-primaries write their own meshes' rows at
    /// once on one shared root without touching each other's keys"): each mesh-primary writes its
    /// Mesh row under `meshes/<its mesh>` and its members' Node rows and contacts under
    /// `nodes/<its mesh>/`, at once, on one root; neither writes a key under the other's mesh, its
    /// own data dir holds none of them, and each reads the other's rows from the shared root.
    #[tokio::test]
    async fn two_mesh_primaries_write_their_own_meshes_rows_at_once_on_one_shared_root_without_touching_each_others_keys() {
        let d = crate::record_store::tempdir("shared-root");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = crate::store_root::StoreRoot::open(&store, &crate::model::FabricId::mint()).unwrap();
        let (data1, data2) = (d.join("mesh1.admin.1"), d.join("mesh2.admin.1"));
        let (inc1, inc2) = (IncarnationId::mint(), IncarnationId::mint());
        let (n1, n2) = (FileNodesStorage::open(&data1, &root, "mesh1", &inc1).await.unwrap(), FileNodesStorage::open(&data2, &root, "mesh2", &inc2).await.unwrap());
        let (m1, m2) = (FileMeshStorage::open(&root, &inc1).await.unwrap(), FileMeshStorage::open(&root, &inc2).await.unwrap());
        // The fabric's first state-commit published the store: what follows writes to it.
        root.scratchpad().expect("a root without the Fabric row opens with a scratchpad").publish().await.unwrap();
        let writes = async |n: &FileNodesStorage, m: &FileMeshStorage, mesh: &str| {
            m.put_mesh(&MeshRecord { mesh_id: MeshId::mint(), name: mesh.into() }).await.unwrap();
            for name in [format!("{mesh}.admin.1"), format!("{mesh}.rpc.1"), format!("{mesh}.rpc.2")] {
                n.put_node_row(&row(&name)).await.unwrap().expect("each row is at a name no node holds");
                n.put_contact(&node(&name)).await.unwrap();
            }
        };
        tokio::join!(writes(&n1, &m1, "mesh1"), writes(&n2, &m2, "mesh2"));
        for (mesh, other) in [("mesh1", "mesh2"), ("mesh2", "mesh1")] {
            let rows = keys(&root.dir().join("nodes").join(mesh).join("rows"));
            assert_eq!(rows, [format!("{mesh}.admin.1.json"), format!("{mesh}.rpc.1.json"), format!("{mesh}.rpc.2.json")], "{mesh}'s rows");
            assert!(!rows.iter().any(|k| k.starts_with(other)), "{mesh}'s rows hold a key of {other}");
            assert_eq!(keys(&root.dir().join("nodes").join(mesh).join("contacts")).len(), 3, "{mesh}'s contacts");
        }
        assert_eq!(keys(&root.dir().join("meshes")), ["mesh1.json", "mesh2.json"]);
        for data in [&data1, &data2] {
            assert!(!data.join("meshes").exists() && !data.join("nodes").join("rows").exists() && !data.join("nodes").join("contacts").exists(), "{} holds a shared row", data.display());
        }
        assert!(n1.node_row_at(&"mesh2.rpc.1".parse().unwrap()).await.unwrap().is_some(), "mesh1's primary reads mesh2's row from the shared root");
        assert!(n2.node_row_at(&"mesh1.rpc.1".parse().unwrap()).await.unwrap().is_some(), "mesh2's primary reads mesh1's row from the shared root");
        assert_eq!(n1.contacts().await.unwrap().len(), 6, "a restart reads every mesh's contacts");
    }

    /// CONTRACT (R14 items 1 and 9; acceptance "an ordinary node's connections rows land under
    /// `connections/<node>/` in the shared root and the successor mesh-primary reads them there";
    /// states.md, Rounds): before the fabric's first state-commit a node holds its connections rows
    /// and raw log in its scratchpad, reads them there, and puts nothing on the root; its publication
    /// writes them under `connections/<its path.name>/`, where a second store opened by another
    /// process (another incarnation) reads every row.
    #[tokio::test]
    async fn an_ordinary_nodes_connections_rows_land_under_connections_node_and_a_second_instance_reads_them() {
        let d = crate::record_store::tempdir("shared-connections");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = crate::store_root::StoreRoot::open(&store, &crate::model::FabricId::mint()).unwrap();
        let node: PathName = "mesh1.admin.1".parse().unwrap();
        let writer = FileConnectionsStorage::open(&root, &node, &IncarnationId::mint()).await.unwrap();
        let (up, down) = (fact(ConnectionState::Connected), fact(ConnectionState::Disconnected));
        writer.put_connection(&up).await.unwrap();
        writer.append_history(&up).await.unwrap();
        writer.put_connection(&down).await.unwrap();
        let dir = store.join(root.dir().file_name().unwrap()).join("connections").join("mesh1.admin.1");
        assert!(keys(&dir).is_empty() && !dir.join("history.jsonl").exists(), "nothing reaches the root before the publication: {}", dir.display());
        assert_eq!(writer.connections().await.unwrap(), [down.clone()], "the writer reads the rows it holds");
        assert_eq!(writer.history().await.unwrap(), [up.clone()], "the writer reads the log lines it holds");
        let published = root.scratchpad().expect("a root without the Fabric row opens with a scratchpad").publish().await.unwrap();
        assert_eq!(published.summary(), "connections/mesh1.admin.1=1,connections/mesh1.admin.1/history=1", "{published:?}");
        assert_eq!(keys(&dir).len(), 1, "one row per (source, destination, kind): {}", dir.display());
        assert!(dir.join("history.jsonl").exists());
        let reader = FileConnectionsStorage::open(&root, &node, &IncarnationId::mint()).await.unwrap();
        assert_eq!(reader.connections().await.unwrap(), [down]);
        assert_eq!(reader.history().await.unwrap(), [up]);
    }

    /// CONTRACT (R14 item 4 on the shared root): the fabric-primary seat also authorizes rows of
    /// meshes other than its own (the fabric-primary writes them; after R13, as acting primary of a
    /// mesh with no primary). When that seat moves, the holder's writes to every other mesh's rows are
    /// cut off and drained with the rest, the rows of a mesh this store first opens after the cut-off
    /// included, and a lift gives them back; its own mesh's rows follow the mesh seat's cut-off.
    #[tokio::test]
    async fn a_moving_fabric_seat_cuts_off_the_holders_writes_to_every_other_meshs_rows() {
        let d = crate::record_store::tempdir("other-meshes-cut-off");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let n = FileNodesStorage::open(&d, &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        // The same node's rows each time: a put of another node's row at a live key is refused
        // NameHeld by the adapter's claim before any write, and this cell is about the cut-off.
        let rows: BTreeMap<&str, NodeRow> = ["mesh2.rpc.1", "mesh3.rpc.1"].into_iter().map(|name| (name, row(name))).collect();
        n.put_node_row(&rows["mesh2.rpc.1"]).await.unwrap().unwrap();
        n.cut_off_other_meshes("seat transfer drain-node:mesh1.admin.1").await;
        for name in ["mesh2.rpc.1", "mesh3.rpc.1"] {
            match n.put_node_row(&rows[name]).await {
                Err(StorageError::CutOff { reason, .. }) => assert!(reason.contains("drain-node:mesh1.admin.1"), "{reason}"),
                other => panic!("{name}: a row write after the fabric seat's cut-off answered {other:?}"),
            }
            assert!(matches!(n.put_contact(&node(name)).await, Err(StorageError::CutOff { .. })), "{name}: a contact write after the cut-off");
        }
        n.put_node_row(&row("mesh1.rpc.1")).await.expect("its own mesh's rows follow the mesh seat's cut-off, not this one").expect("the key is free");
        n.lift_other_meshes().await.unwrap();
        n.put_node_row(&rows["mesh3.rpc.1"]).await.expect("a lifted cut-off gives the writes back").expect("the key is free");
    }

    #[tokio::test]
    async fn runtime_rows_are_one_keyed_row_per_birth_and_an_unrecognised_row_is_refused_by_name() {
        let d = std::env::temp_dir().join(format!("rdm-runtime-rows-{}", rand::random::<u32>()));
        let row = |inc: &str| RuntimeRow {
            node_id: NodeId::mint(),
            name: "mesh1.admin.2".parse().unwrap(),
            incarnation_id: IncarnationId(inc.into()),
            runtime: rafka_mesh_entity::RuntimeFact::of_this_process("dep-1").unwrap(),
            data_dir: Some("/tmp/x".into()),
        };
        let (a, b) = (row("i1"), row("i2"));
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let n = FileNodesStorage::open(&d, &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        n.put_runtime(&a).await.unwrap();
        n.put_runtime(&b).await.unwrap();
        n.put_runtime(&a).await.unwrap();
        let mut held = FileNodesStorage::open(&d, &root, "mesh1", &IncarnationId::mint()).await.unwrap().runtimes().await.unwrap();
        held.sort_by_key(|r| r.incarnation_id.0.clone());
        assert_eq!(held, [a.clone(), b.clone()], "two births, two rows; a repeated put is the same row");
        std::fs::write(d.join("nodes/runtimes").join(format!("{}.json", a.key())), br#"{"format":"node-runtime/9","record":{}}"#).unwrap();
        assert!(matches!(n.runtimes().await, Err(StorageError::Unrecognised { .. })));
    }

    /// A row file at `name` in a fresh file store that is a FIFO: a read of it waits until a writer
    /// opens it. The writer is a thread that, once a reader has the FIFO open, waits for word from
    /// the runtime that it still runs (up to 5 s), then writes `row`. Whether that word came, while
    /// the read was waiting, is what this answers.
    fn runtime_runs_while_a_row_is_read<T: Send + 'static>(row: NodeRow, op: impl FnOnce(std::sync::Arc<FileNodesStorage>) -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>> + Send + 'static) -> (bool, T) {
        let bytes = serde_json::to_vec(&serde_json::json!({ "format": ROW_FORMAT, "record": row })).unwrap();
        runtime_runs_while_a_file_is_read("root/nodes/mesh1/rows", &row.name.to_string(), bytes, op)
    }

    /// The same, for the record file `<key>.json` under the store's `dir`, written `bytes`.
    fn runtime_runs_while_a_file_is_read<T: Send + 'static>(dir: &str, key: &str, bytes: Vec<u8>, op: impl FnOnce(std::sync::Arc<FileNodesStorage>) -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>> + Send + 'static) -> (bool, T) {
        let d = crate::record_store::tempdir("rows-offload");
        let fifo = d.join(dir).join(format!("{key}.json"));
        crate::record_store::runtime_runs_while_a_fifo_is_read(fifo, bytes, Box::pin(async move { op(std::sync::Arc::new(FileNodesStorage::open(&d, &crate::store_root::StoreRoot::at(&d.join("root")), "mesh1", &IncarnationId::mint()).await.unwrap())).await }))
    }

    /// CONTRACT: a point read of a node row never waits on the disk on a runtime worker (the
    /// defect class that froze node-admins on 10-08: a worker blocked on the disk): it runs on the
    /// blocking pool, as the row's write does (`record_store::blocking_io`). The row write runs
    /// inline on every view install, the join door's among them, after this read of the key.
    /// The cell: a row file that is a FIFO holds the read until a writer comes; the runtime's only
    /// thread keeps running meanwhile, and the read answers the row.
    #[test]
    fn a_node_row_read_waits_on_the_disk_off_the_runtime_thread() {
        let row = NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: NodeId::mint(), status: crate::model::NodeStatus::ReadyForTraffic, config: BTreeMap::new() };
        let name = row.name.clone();
        let (ran, read) = runtime_runs_while_a_row_is_read(row.clone(), move |s| Box::pin(async move { s.node_row_at(&name).await }));
        assert!(ran, "the runtime thread ran while the row read waited on the disk");
        assert_eq!(read, Ok(Some(row)));
    }

    /// CONTRACT: a list of a store's records (`FileRecords::list`: the directory read and every
    /// file in it) never waits on the disk on a runtime worker: it runs on the blocking pool, as a
    /// point read and every write do. The cell lists the runtime rows while one row file holds its
    /// read: the runtime's only thread keeps running, and the list answers the row.
    #[test]
    fn a_list_of_records_waits_on_the_disk_off_the_runtime_thread() {
        let row = RuntimeRow { node_id: NodeId::mint(), name: "mesh1.admin.2".parse().unwrap(), incarnation_id: IncarnationId("i1".into()), runtime: rafka_mesh_entity::RuntimeFact::of_this_process("dep-1").unwrap(), data_dir: Some("/tmp/x".into()) };
        let bytes = serde_json::to_vec(&serde_json::json!({ "format": RUNTIME_FORMAT, "record": row })).unwrap();
        let (ran, listed) = runtime_runs_while_a_file_is_read("nodes/runtimes", &row.key(), bytes, move |s| Box::pin(async move { s.runtimes().await }));
        assert!(ran, "the runtime thread ran while the list waited on the disk");
        assert_eq!(listed, Ok(vec![row]));
    }

    /// CONTRACT: a read of the connections raw log (`history.jsonl`) never waits on the disk on a
    /// runtime worker: it runs on the blocking pool, as the log's append does. The cell reads the
    /// log while its file holds the read: the runtime's only thread keeps running, and the read
    /// answers the logged fact.
    #[test]
    fn a_read_of_the_connections_history_waits_on_the_disk_off_the_runtime_thread() {
        let d = crate::record_store::tempdir("history-offload");
        let logged = fact(ConnectionState::Connected);
        let mut line = serde_json::to_vec(&logged).unwrap();
        line.push(b'\n');
        let (root, node): (_, PathName) = (crate::store_root::StoreRoot::at(&d.join("root")), "mesh1.admin.1".parse().unwrap());
        let fifo = root.connections(&node).join("history.jsonl");
        let (ran, history) = crate::record_store::runtime_runs_while_a_fifo_is_read(fifo, line, Box::pin(async move { FileConnectionsStorage::open(&root, &node, &IncarnationId::mint()).await.unwrap().history().await }));
        assert!(ran, "the runtime thread ran while the history read waited on the disk");
        assert_eq!(history, Ok(vec![logged]));
    }

    /// CONTRACT: the nodes store's adapter is where a name is claimed (R13; ops-naming "Node data":
    /// "claims the new name by insert-and-fail inside the store adapter ... `NameHeld { by: node.id }`
    /// refuses a held name, and nothing is written"). Two saves of rows of two different nodes at one
    /// `path.name`: the second is refused and the first node's row stands. A save of the same node's row
    /// there (its status moves, its Deleted row) is a put, and another node's `Deleted` row holds no
    /// name: a save over it is a put. The memory store (its internal index) and the file store each
    /// run their own check.
    #[tokio::test]
    async fn a_second_nodes_row_at_a_held_name_is_refused_by_the_adapter_and_nothing_is_written() {
        let d = crate::record_store::tempdir("storage-claim");
        let name: PathName = "mesh1.rpc.1".parse().unwrap();
        let row = |node_id: &NodeId, status: crate::model::NodeStatus, tier: &str| NodeRow { name: name.clone(), node_id: node_id.clone(), status, config: BTreeMap::from([("tier".to_string(), tier.to_string())]) };
        let check = async |n: &dyn NodesStorage, store: &str| {
            let (first, second) = (NodeId::mint(), NodeId::mint());
            let held = row(&first, crate::model::NodeStatus::ReadyForTraffic, "gold");
            assert_eq!(n.put_node_row(&held).await, Ok(Ok(())), "{store}: a free key takes the row");
            let refused = n.put_node_row(&row(&second, crate::model::NodeStatus::Pending, "silver")).await;
            assert_eq!(refused, Ok(Err(NameHeld { name: name.clone(), by: first.clone() })), "{store}: another node's live row holds {name}");
            assert_eq!(n.node_row_at(&name).await.unwrap(), Some(held.clone()), "{store}: nothing is written by a refused save");
            let deleted = row(&first, crate::model::NodeStatus::Deleted, "gold");
            assert_eq!(n.put_node_row(&deleted).await, Ok(Ok(())), "{store}: the holder's own row is a put");
            let next = row(&second, crate::model::NodeStatus::Pending, "gold");
            assert_eq!(n.put_node_row(&next).await, Ok(Ok(())), "{store}: a Deleted row holds no name");
            assert_eq!(n.node_row_at(&name).await.unwrap(), Some(next), "{store}: the save over a Deleted row is written");
        };
        check(&MemoryNodesStorage::default(), "memory").await;
        check(&FileNodesStorage::open(&d, &crate::store_root::StoreRoot::at(&d.join("root")), "mesh1", &IncarnationId::mint()).await.unwrap(), "file").await;
        let _ = std::fs::remove_dir_all(&d);
    }
}
