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
//!   death proof); each mesh's Node rows with their id index and their tags index (R-TG1); and,
//!   when this admin is a birth's authority, the lifecycle state that birth declared and the admin
//!   applied (`status_rpc`).
//! - `connections.storage`: the latest connection fact per (source, destination, kind).

use async_trait::async_trait;
use crate::model::{FabricId, IncarnationId, MeshId, NodeId, PathName, EndpointId};
pub use rdm_client::NameHeld;
use crate::record_store::FileRecords;
use crate::storage_error::StorageError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Mutex;

// ---------------------------------------------------------------- mesh.storage

/// A Mesh row (node-identity-and-state.md §2.7): the mesh's identity at its name's key, and whether
/// its removal made it Dead. Written whole by the mesh's primary (the fabric-primary only as acting
/// primary of a mesh with none), through the claim of its name ([`MeshStorage::claim_mesh`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshRecord {
    /// The mesh's minted id.
    pub mesh_id: MeshId,
    /// The mesh's name: the row's key.
    pub name: String,
    /// Set by the mesh's removal (mesh.deleted at its primary, or mesh-left at the fabric-primary as
    /// acting primary): the mesh is Dead, its name is released (R-MN1 item 2) and its id is never
    /// written live again.
    pub dead: bool,
}

/// What a claim of a mesh's name wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshClaim {
    /// The row was written at the name's key: the key was free, held by this mesh's own row with
    /// other contents (its Dead write), or held by another mesh's Dead row, which `over` names (its
    /// removal released the name, R-MN1).
    Written {
        /// The Dead mesh whose row the claim took the name over.
        over: Option<MeshId>,
    },
    /// This mesh's own row stands at the key exactly as given: nothing is written (a re-claim at the
    /// fabric's state-commit, a retry, a replaced primary).
    Held,
}

/// Why a claim of a mesh's name wrote nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshClaimRefused {
    /// Another mesh's live row holds the name: it is released only when that mesh's row is Dead.
    NameHeld {
        /// The name claimed.
        name: String,
        /// The mesh whose row holds it.
        by: MeshId,
    },
    /// This mesh's own row at the name is Dead: a dead mesh is never written live again and its id
    /// is never reissued (states.md); only its Dead row is ever written again.
    MeshDead {
        /// The name claimed.
        name: String,
        /// The dead mesh.
        mesh_id: MeshId,
    },
}

impl MeshClaimRefused {
    /// The span outcome that names the refusal.
    pub fn outcome(&self) -> &'static str {
        match self {
            Self::NameHeld { .. } => "name-held",
            Self::MeshDead { .. } => "mesh-dead",
        }
    }
}

impl std::fmt::Display for MeshClaimRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NameHeld { name, by } => write!(f, "the mesh name {name} is held by mesh {by}: it is released only when that mesh's row is Dead"),
            Self::MeshDead { name, mesh_id } => write!(f, "mesh {mesh_id}'s row at {name} is Dead: a dead mesh is never written live again"),
        }
    }
}

impl std::error::Error for MeshClaimRefused {}

/// What a claim of `row` meets in `held`, the row at its name's key: free (none, or another mesh's
/// Dead row), this mesh's own row, or held by another mesh's live row.
fn claim_mesh_against(held: Option<&MeshRecord>, row: &MeshRecord) -> Result<MeshClaim, MeshClaimRefused> {
    match held {
        None => Ok(MeshClaim::Written { over: None }),
        Some(h) if h == row => Ok(MeshClaim::Held),
        Some(h) if h.mesh_id == row.mesh_id && h.dead => Err(MeshClaimRefused::MeshDead { name: row.name.clone(), mesh_id: row.mesh_id.clone() }),
        Some(h) if h.mesh_id == row.mesh_id => Ok(MeshClaim::Written { over: None }),
        Some(h) if h.dead => Ok(MeshClaim::Written { over: Some(h.mesh_id.clone()) }),
        Some(h) => Err(MeshClaimRefused::NameHeld { name: row.name.clone(), by: h.mesh_id.clone() }),
    }
}

/// The durable record of the meshes this admin knows: each mesh's row at its name.
#[async_trait]
pub trait MeshStorage: Send + Sync {
    /// The row at the name `name`: a point read through its key; `None` when none was ever written.
    async fn mesh(&self, name: &str) -> Result<Option<MeshRecord>, StorageError>;
    /// Claim `record.name` for `record.mesh_id` and keep `record` as the Mesh row at that key. The
    /// claim is here, inside the adapter, and nowhere else (node-identity-and-state.md §2.7, R13):
    /// insert-and-fail on the name's key, decided by the store in one step against every other write
    /// of this store. The key is free when no row is there or another mesh's Dead row is (its removal
    /// released the name): `record` is put whole. This mesh's own row there is the same mesh
    /// re-claiming its name: idempotent, written only when `record` differs (its Dead write), and a
    /// Dead row is never written live again (`MeshDead`). Another mesh's live row holds the name:
    /// `NameHeld` naming that mesh, and nothing is written.
    async fn claim_mesh(&self, record: &MeshRecord) -> Result<Result<MeshClaim, MeshClaimRefused>, StorageError>;
    /// Every mesh row held.
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
    async fn claim_mesh(&self, record: &MeshRecord) -> Result<Result<MeshClaim, MeshClaimRefused>, StorageError> {
        // The internal index is the map's key: the look and the insert are one step under its lock.
        let mut rows = self.0.lock().unwrap();
        let claimed = claim_mesh_against(rows.get(&record.name), record);
        if let Ok(MeshClaim::Written { .. }) = claimed {
            rows.insert(record.name.clone(), record.clone());
        }
        Ok(claimed)
    }
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
        Ok(self.0.lock().unwrap().values().cloned().collect())
    }
}

const MESH_FORMAT: &str = "mesh-record/2";

/// `<store root>/meshes/<name>.json`: each mesh's Mesh row, written by that mesh's primary.
#[derive(Debug)]
pub struct FileMeshStorage {
    rows: FileRecords,
    /// Every claim is one at a time, so a claim's look at the key and its write are one step against
    /// every other write of this store.
    claims: tokio::sync::Mutex<()>,
}

impl FileMeshStorage {
    /// Open the store under the fabric's store root, written by the process born as `writer`.
    pub async fn open(root: &crate::store_root::StoreRoot, writer: &IncarnationId) -> Result<Self, StorageError> {
        Ok(Self { rows: root.records(root.dir(), "meshes", writer).await?, claims: tokio::sync::Mutex::new(()) })
    }

    /// The records the mesh-primary seat writes (its Mesh row): what a seat transfer cuts off.
    pub fn mesh_primary_records(&self) -> [&FileRecords; 1] {
        [&self.rows]
    }
}

#[async_trait]
impl MeshStorage for FileMeshStorage {
    async fn mesh(&self, name: &str) -> Result<Option<MeshRecord>, StorageError> {
        self.rows.read(name, MESH_FORMAT).await
    }
    async fn claim_mesh(&self, record: &MeshRecord) -> Result<Result<MeshClaim, MeshClaimRefused>, StorageError> {
        let _one = self.claims.lock().await;
        let claimed = claim_mesh_against(self.rows.read::<MeshRecord>(&record.name, MESH_FORMAT).await?.as_ref(), record);
        if let Ok(MeshClaim::Written { .. }) = claimed {
            // Held before the fabric's first state-commit, the claim is decided again at the
            // publication against the row then standing at the name's key.
            let row = record.clone();
            self.rows.write_claimed(&record.name, MESH_FORMAT, record, move |standing| claim_mesh_against(standing, &row).map(|c| matches!(c, MeshClaim::Written { .. })).map_err(|r| r.to_string())).await?;
        }
        Ok(claimed)
    }
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
        self.rows.list(MESH_FORMAT).await
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
    pub runtime: rdm_core::RuntimeFact,
    /// The data dir the birth reported: where its process runtime records an exit code.
    pub data_dir: Option<String>,
}

impl RuntimeRow {
    /// The row's key: the birth it describes.
    pub fn key(&self) -> String {
        format!("{}--{}", self.node_id, self.incarnation_id.as_str())
    }
}

/// The record of the node at one `path.name`: the node its writer's view holds there, its status
/// and its config. Its mesh's primary node-admin is its only writer (R13; the fabric-primary only as
/// the acting primary of a mesh with none, `crate::node_rows`) and writes it whole, a blind put of
/// the record it holds; the row follows the node's name and never decides who may hold one (the
/// node owns its name). A `Deleted` row keeps the config for the next node created at the same
/// `path.name`. Its store indexes: `path.name`, its key; `id`, the name of the row each node holds
/// ([`NodesStorage::node_row_name`]); and `tags`, the nodes carrying each tag label
/// ([`NodesStorage::tagged`]); both kept by the store with every write of a row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRow {
    /// The node's `path.name`: the row's key.
    pub name: PathName,
    /// The node that held the name when the row was written.
    pub node_id: NodeId,
    /// The node's status: `Deleted` once its runtime exit is proven.
    pub status: crate::model::NodeStatus,
    /// The application's own key/values on the node (capped: [`rdm_core::check_config`]).
    pub config: BTreeMap<String, String>,
    /// The node's tag labels (R-TG1), set through `node.tags.create` / `node.tags.delete`, each one a
    /// row of the tags index while the row is live. A `Deleted` row keeps them, as it keeps the config;
    /// a row written before tags existed carries none.
    #[serde(default)]
    pub tags: BTreeSet<String>,
    /// What the create that claimed the name allocated (lifecycles/node-create.md step 1: the claim
    /// precedes everything prepared or launched for the birth): the claim row only. The claim is the
    /// one write of the create that outlives a lost executor (its Build receipts are in that
    /// executor's own journal, build.md §5.2), so the identity the claim was made with is durable in
    /// it, and a later attempt of the same operation of the same Build that never heard the
    /// `AllocateIdentity` receipt continues under that identity: its claim is the same birth's
    /// (`crate::node_rows::Claimed::HeldByNode`). Every later row of the node is written after its
    /// launch and carries none; a row written before allocations existed carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocated: Option<Allocation>,
    /// The `path.name` a rename moved this row from (proposal node-properties-own-commands.md §2.1
    /// "Claim and move"), written by the put at the new key. While the key it names still holds this
    /// row's node, the release of that key is pending and the writer that meets the row releases it
    /// (`crate::node_rows::finish_release`); once that key no longer holds the node it says nothing,
    /// so it is never cleared. A row no rename wrote carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renamed_from: Option<PathName>,
    /// The successor that took this row's config and tags at a replace's cutover (node-replace.md
    /// step 4; proposal node-properties-own-commands.md §8, replace: the successor "inherits config
    /// and tags from the retained predecessor row at the cutover rename ... and the predecessor's index
    /// entries gone"), written on the retained predecessor's row once the successor's row stands at the
    /// name. The row keeps its tags, as a `Deleted` row does, and the tags index holds none of them:
    /// every label leaves with that write and none arrives with a later write at the same key. A
    /// rename of the row writes none: the node takes a name again (node-replace.md "Rollback" step 2).
    /// A row no cutover marked carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_by: Option<NodeId>,
    /// Immutable application declaration, scoped to the exact incarnation that reported it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_types: Option<AppTypes>,
}

/// The application's declaration held beside its node row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppTypes {
    /// Exact birth that declared the types.
    pub incarnation: IncarnationId,
    /// Immutable declared application types.
    pub types: BTreeSet<String>,
}

/// The identity a create's `AllocateIdentity` decided for the birth its claim names (beside the
/// row's `node_id`), and the operation of the Build that decided it: keyed by `(build_id,
/// operation)`, never by attempt, because every attempt of the operation continues the same birth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Allocation {
    /// The Build whose operation claimed the name.
    pub build_id: crate::build::BuildId,
    /// The operation that claimed it (`node.create:<path>`).
    pub operation: String,
    /// The incarnation minted for the birth.
    pub incarnation: IncarnationId,
    /// The exact deployment minted for the birth: what the provider finds its runtime by, if one
    /// was ever launched.
    pub deployment_id: crate::model::DeploymentId,
}

impl NodeRow {
    /// The node's path (`<mesh>.<kind>`), separate from its name within that path.
    pub fn path(&self) -> String {
        self.name.path()
    }

    /// The node's name within its path (`<base>[.old|.new]`).
    pub fn node_name(&self) -> String {
        self.name.name()
    }

    /// The node's `node_type`.
    pub fn node_type(&self) -> &'static str {
        self.name.node_type()
    }

    /// The labels the tags index holds for this row's node once the row is written over `previous`,
    /// the row it replaces at its key, split into (leaving, arriving): a label the node's previous
    /// row carried and this one does not, or every label of a `Deleted` row or of a row a successor
    /// inherited ([`Self::inherited_by`]), leaves; every label of any other live row arrives. A
    /// previous row of another node (a `Deleted` row a new node takes over) changes nothing: its labels
    /// left when it turned `Deleted`.
    fn tag_moves(&self, previous: Option<&NodeRow>) -> (BTreeSet<String>, BTreeSet<String>) {
        let held: BTreeSet<String> = previous.filter(|p| p.node_id == self.node_id).map(|p| p.tags.clone()).unwrap_or_default();
        if self.status == crate::model::NodeStatus::Deleted || self.inherited_by.is_some() {
            return (held.union(&self.tags).cloned().collect(), BTreeSet::new());
        }
        (held.difference(&self.tags).cloned().collect(), self.tags.clone())
    }
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
    /// Whether the row at `name` is `holder`'s, decided in one step against every other row write.
    /// When it is, the row is left exactly as it is and its id index entry names `name` once this
    /// answers: written when it does not, as when a put ended between the row file and its entry
    /// (two files), so a move retried over the row its node already holds leaves no row unnamed.
    async fn hold_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, StorageError>;
    /// `holder` lets go of the key `name`: its row goes when it is `holder`'s, decided by the store
    /// in one step against every other row write; a row of another node there (the name was
    /// released before and claimed since) is left as it is. Returns whether `holder`'s row went.
    async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, StorageError>;
    /// The nodes carrying `label`, in every mesh: a point read of the label's rows of the tags index
    /// (R-TG1), never a read of any node's row. The index is a lookup structure the store keeps with
    /// every write of a row, as it keeps the id index: one row per (label, node id), a blind put when
    /// the node's row carries the label, the tombstone of its own key when the node's row stops
    /// carrying it, turns `Deleted`, or is the only row the node lets go of. A `label` that is not a
    /// tag label ([`rdm_core::status::tag_refusal`]) is refused by name.
    async fn tagged(&self, label: &str) -> Result<BTreeSet<NodeId>, StorageError>;
}

/// The refusal of a key the tags index cannot hold: `label` is not a tag label.
fn tag_key(label: &str, store: &str) -> Result<(), StorageError> {
    match rdm_core::status::tag_refusal(label) {
        None => Ok(()),
        Some(reason) => Err(StorageError::Unrecognised { file: format!("{store} (tags index)"), reason }),
    }
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
    /// The node row's tags index: per label, the nodes carrying it, kept with `rows` under its lock.
    tags: Mutex<BTreeMap<String, BTreeSet<NodeId>>>,
}

/// The tags index after `node_id`'s labels `leaving` go and `arriving` come.
fn tags_move(tags: &mut BTreeMap<String, BTreeSet<NodeId>>, node_id: &NodeId, leaving: &BTreeSet<String>, arriving: &BTreeSet<String>) {
    for l in leaving {
        if let Some(nodes) = tags.get_mut(l) {
            nodes.remove(node_id);
            if nodes.is_empty() {
                tags.remove(l);
            }
        }
    }
    for l in arriving {
        tags.entry(l.clone()).or_default().insert(node_id.clone());
    }
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
        Ok(self.runtimes.lock().unwrap().get(&format!("{}--{}", node_id, incarnation.as_str())).cloned())
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
            let (leaving, arriving) = row.tag_moves(previous.as_ref());
            tags_move(&mut self.tags.lock().unwrap(), &row.node_id, &leaving, &arriving);
        }
        Ok(claimed)
    }
    async fn node_row_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError> {
        Ok(self.rows.lock().unwrap().get(&name.to_string()).cloned())
    }
    async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, StorageError> {
        Ok(self.ids.lock().unwrap().get(node_id).filter(|n| n.mesh == mesh).cloned())
    }
    async fn hold_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, StorageError> {
        let rows = self.rows.lock().unwrap();
        let Some(row) = rows.get(&name.to_string()).filter(|r| &r.node_id == holder) else { return Ok(false) };
        self.ids.lock().unwrap().insert(holder.clone(), name.clone());
        let (leaving, arriving) = row.tag_moves(Some(row));
        tags_move(&mut self.tags.lock().unwrap(), holder, &leaving, &arriving);
        Ok(true)
    }
    async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, StorageError> {
        let mut rows = self.rows.lock().unwrap();
        let key = name.to_string();
        let held = rows.get(&key).is_some_and(|r| &r.node_id == holder);
        if held {
            let released = rows.remove(&key);
            let mut ids = self.ids.lock().unwrap();
            if ids.get(holder) == Some(name) {
                // The node lets go of the only row it holds: its labels go with it.
                ids.remove(holder);
                if let Some(r) = released {
                    tags_move(&mut self.tags.lock().unwrap(), holder, &r.tags, &BTreeSet::new());
                }
            }
        }
        Ok(held)
    }
    async fn tagged(&self, label: &str) -> Result<BTreeSet<NodeId>, StorageError> {
        tag_key(label, "memory nodes store")?;
        Ok(self.tags.lock().unwrap().get(label).cloned().unwrap_or_default())
    }
}

const NODE_FORMAT: &str = "node-record/4";
const RUNTIME_FORMAT: &str = "node-runtime/1";
const ROW_FORMAT: &str = "node-row/1";
const ROW_ID_FORMAT: &str = "node-row-id/1";
const TAG_FORMAT: &str = "node-tag/1";

/// One row of the node row's tags index: the node carries the label. Its key is the node id, in the
/// label's own directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TagRow {
    label: String,
    node_id: NodeId,
}

/// One entry of the node row's id index: the `path.name` of the row its node holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RowId {
    node_id: NodeId,
    name: PathName,
}

/// One mesh's rows on the fabric's store root, `nodes/<mesh>/`: its members' Node rows
/// (`rows/<path.name>.json`), the rows' id index (`ids/<node id>.json`: the `path.name` of the row
/// the node holds), the rows' tags index (`tags/<label>/<node id>.json`: one directory per label,
/// so a label's rows are read without reading any other's) and contacts (`contacts/<node
/// id>.json`), written by that mesh's primary.
#[derive(Debug)]
struct MeshNodes {
    rows: FileRecords,
    ids: FileRecords,
    contacts: FileRecords,
    root: crate::store_root::StoreRoot,
    base: std::path::PathBuf,
    writer: IncarnationId,
    /// Each label's rows of the tags index this store has read or written, opened on first use.
    labels: Mutex<BTreeMap<String, FileRecords>>,
    /// Set while a seat transfer cuts this mesh's rows off: a label's rows opened meanwhile are cut
    /// off at their open, named.
    labels_cut_off: Mutex<Option<String>>,
}

impl MeshNodes {
    async fn open(root: &crate::store_root::StoreRoot, mesh: &str, writer: &IncarnationId) -> Result<Self, StorageError> {
        let base = root.nodes(mesh);
        Ok(Self {
            rows: root.records(&base, "rows", writer).await?,
            ids: root.records(&base, "ids", writer).await?,
            contacts: root.records(&base, "contacts", writer).await?,
            root: root.clone(),
            base,
            writer: writer.clone(),
            labels: Mutex::new(BTreeMap::new()),
            labels_cut_off: Mutex::new(None),
        })
    }

    /// Every store of this mesh's rows opened so far: rows, ids, contacts and each label's rows.
    fn stores(&self) -> Vec<FileRecords> {
        let mut all = vec![self.rows.clone(), self.ids.clone(), self.contacts.clone()];
        all.extend(self.labels.lock().unwrap_or_else(|e| e.into_inner()).values().cloned());
        all
    }

    /// A label's rows opened from now on are cut off naming `reason` (`None`: they are not).
    fn cut_off_labels(&self, reason: Option<String>) {
        *self.labels_cut_off.lock().unwrap_or_else(|e| e.into_inner()) = reason;
    }

    /// `label`'s rows of the tags index, written: opened on first use (its directory made), cut off
    /// at its open while this mesh's rows are cut off.
    async fn label(&self, label: &str) -> Result<FileRecords, StorageError> {
        tag_key(label, &self.base.display().to_string())?;
        if let Some(l) = self.labels.lock().unwrap_or_else(|e| e.into_inner()).get(label) {
            return Ok(l.clone());
        }
        let opened = self.root.records(&self.base, &format!("tags/{label}"), &self.writer).await?;
        let mut labels = self.labels.lock().unwrap_or_else(|e| e.into_inner());
        let l = labels.entry(label.to_string()).or_insert(opened).clone();
        if let Some(reason) = self.labels_cut_off.lock().unwrap_or_else(|e| e.into_inner()).clone() {
            l.cut_off(reason);
        }
        Ok(l)
    }

    /// The nodes of this mesh carrying `label`: its rows of the tags index, read from the label's
    /// own directory. A label no row of this mesh was ever written for (no directory) is carried by
    /// none, and the read makes no directory.
    async fn tagged(&self, label: &str) -> Result<Vec<NodeId>, StorageError> {
        tag_key(label, &self.base.display().to_string())?;
        let opened = self.labels.lock().unwrap_or_else(|e| e.into_inner()).get(label).cloned();
        let rows = match opened {
            Some(l) => l,
            None => {
                let dir = self.base.join("tags").join(label);
                let look = dir.clone();
                let exists = crate::blocking::blocking_io(move || look.try_exists()).await.map_err(|e| StorageError::Io { file: dir.display().to_string(), reason: e.to_string() })?;
                if !exists {
                    return Ok(Vec::new());
                }
                self.label(label).await?
            }
        };
        Ok(rows.list::<TagRow>(TAG_FORMAT).await?.into_iter().map(|r| r.node_id).collect())
    }

    /// The tags index rows of `node_id`'s labels `leaving`, each the tombstone of its own key:
    /// written before the row that drops them, so a label never outlives the last row write that
    /// carried it, whatever ends the write in between.
    async fn tags_leave(&self, node_id: &NodeId, leaving: &BTreeSet<String>) -> Result<(), StorageError> {
        for l in leaving {
            self.label(l).await?.remove(node_id.as_str()).await?;
        }
        Ok(())
    }

    /// The tags index rows of `node_id`'s labels `arriving`, each a blind put of its own key, written
    /// after the row that carries them (as the id index entry is), so an index row never names a
    /// label no row carries yet. A row that stands is left as it is: a put of a row whose labels did
    /// not change writes no index row; one a write ended before writing is written by the node's next
    /// row write.
    async fn tags_arrive(&self, node_id: &NodeId, arriving: &BTreeSet<String>) -> Result<(), StorageError> {
        for l in arriving {
            let rows = self.label(l).await?;
            if !rows.holds(node_id.as_str()).await? {
                rows.write(node_id.as_str(), TAG_FORMAT, &TagRow { label: l.clone(), node_id: node_id.clone() }).await?;
            }
        }
        Ok(())
    }

    /// The id index after `row` was written at its key over `previous`, the row it replaced there
    /// (written after the row, so an entry never names a row not yet written): the row's node names
    /// the key, and a previous row of another node (a `Deleted` row a recovery's new node takes
    /// over) no longer holds it. An entry that already names the key is left as it is: a put of the
    /// row a node holds (a status change, a delete's `Deleted` row) is the one write of its row. An
    /// entry that does not name it yet is written, also when the write that put the row ended
    /// before its entry was written (the row and its entry are two files): the node's next put of
    /// the key, or a move retried over it ([`NodesStorage::hold_node_row`]), names it.
    async fn index_put(&self, previous: Option<&NodeRow>, row: &NodeRow) -> Result<(), StorageError> {
        if let Some(p) = previous.filter(|p| p.node_id != row.node_id) {
            self.index_drop(&p.node_id, &p.name).await?;
        }
        if self.ids.read::<RowId>(row.node_id.as_str(), ROW_ID_FORMAT).await?.is_some_and(|e| e.name == row.name) {
            return Ok(());
        }
        // The entry follows its row's publication: held before the fabric's first state-commit, it is
        // decided again at the publication against the row then standing at the key it names (this
        // store's rows publish before its ids), and written only while that row is the node's.
        let (standing, node, name) = (self.rows.disk_read::<NodeRow>(&row.name.to_string(), ROW_FORMAT), row.node_id.clone(), row.name.clone());
        self.ids
            .write_claimed(row.node_id.as_str(), ROW_ID_FORMAT, &RowId { node_id: row.node_id.clone(), name: row.name.clone() }, move |_entry: Option<&RowId>| match standing() {
                Ok(Some(r)) if r.node_id == node => Ok(true),
                Ok(Some(r)) => Err(format!("the row at {name} is node {}'s, not {node}'s: the id index entry follows its row and is not written", r.node_id)),
                Ok(None) => Err(format!("no row stands at {name}: the id index entry of {node} follows its row and is not written")),
                Err(e) => Err(format!("the row at {name} could not be read for the id index entry of {node}: {e}")),
            })
            .await
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

    /// The records the mesh-primary seat writes (its own mesh's members' Node rows, their id index,
    /// contacts and the label rows of their tags index opened so far): what a seat transfer cuts off
    /// (R14 item 4).
    pub fn mesh_primary_records(&self) -> Vec<FileRecords> {
        self.mine.1.stores()
    }

    /// While a seat transfer cuts the mesh-primary seat's records off (`Some(reason)`), a label's
    /// rows of this admin's own mesh opened meanwhile are cut off at their open; `None` lifts it.
    pub fn cut_off_own_labels(&self, reason: Option<String>) {
        self.mine.1.cut_off_labels(reason);
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
            m.cut_off_labels(Some(reason.clone()));
            for s in m.stores() {
                s.cut_off(reason.clone());
            }
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
        for m in &opened {
            m.cut_off_labels(Some(reason.to_string()));
        }
        let stores: Vec<FileRecords> = opened.iter().flat_map(|m| m.stores()).collect();
        crate::record_store::cut_off_and_drain(&stores.iter().collect::<Vec<_>>(), reason).await;
    }

    /// Why the other meshes' rows are cut off ([`Self::cut_off_other_meshes`]), while they are.
    pub fn other_meshes_cut_off(&self) -> Option<String> {
        self.others_cut_off.lock().unwrap_or_else(|e| e.into_inner()).clone()
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
            m.cut_off_labels(None);
            for s in m.stores() {
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
            for s in m.stores() {
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
        crate::blocking::blocking_io(move || match std::fs::read_dir(&dir) {
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
        self.runtimes.read(&format!("{}--{}", node_id, incarnation.as_str()), RUNTIME_FORMAT).await
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
        self.mesh(mesh).await?.contacts.remove(node_id.as_str()).await.map(|_| ())
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
            let (leaving, arriving) = row.tag_moves(previous.as_ref());
            rows.tags_leave(&row.node_id, &leaving).await?;
            // Held before the fabric's first state-commit, the claim is decided again at the
            // publication against the row then standing at the key.
            let claim = row.clone();
            rows.rows.write_claimed(&key, ROW_FORMAT, row, move |standing| claim_against(standing, &claim).map(|()| true).map_err(|held| held.to_string())).await?;
            rows.index_put(previous.as_ref(), row).await?;
            rows.tags_arrive(&row.node_id, &arriving).await?;
        }
        Ok(claimed)
    }
    async fn node_row_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError> {
        self.mesh(&name.mesh).await?.rows.read(&name.to_string(), ROW_FORMAT).await
    }
    async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, StorageError> {
        // The entry and the row are two files: the name is answered only while the row at it is this
        // node's.
        let rows = self.mesh(mesh).await?;
        let Some(e) = rows.ids.read::<RowId>(node_id.as_str(), ROW_ID_FORMAT).await? else { return Ok(None) };
        Ok(rows.rows.read::<NodeRow>(&e.name.to_string(), ROW_FORMAT).await?.filter(|r| r.node_id == *node_id).map(|_| e.name))
    }
    async fn hold_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, StorageError> {
        let _one = self.row_writes.lock().await;
        let rows = self.mesh(&name.mesh).await?;
        let Some(row) = rows.rows.read::<NodeRow>(&name.to_string(), ROW_FORMAT).await?.filter(|r| &r.node_id == holder) else { return Ok(false) };
        rows.index_put(None, &row).await?;
        let (leaving, arriving) = row.tag_moves(Some(&row));
        rows.tags_leave(holder, &leaving).await?;
        rows.tags_arrive(holder, &arriving).await?;
        Ok(true)
    }
    async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, StorageError> {
        let _one = self.row_writes.lock().await;
        let key = name.to_string();
        let rows = self.mesh(&name.mesh).await?;
        let released = rows.rows.read::<NodeRow>(&key, ROW_FORMAT).await?.filter(|r| &r.node_id == holder);
        let held = released.is_some();
        if let Some(r) = released {
            // The node lets go of the only row it holds (its id index entry names this key): its
            // labels go with it, before the row does.
            if rows.ids.read::<RowId>(holder.as_str(), ROW_ID_FORMAT).await?.is_some_and(|e| &e.name == name) {
                rows.tags_leave(holder, &r.tags).await?;
            }
            // Held before the fabric's first state-commit, the release removes the row standing at
            // the key at the publication only while it is still the holder's.
            let still = holder.clone();
            rows.rows.remove_released(&key, ROW_FORMAT, move |r: &NodeRow| r.node_id == still).await?;
            rows.index_drop(holder, name).await?;
        }
        Ok(held)
    }
    async fn tagged(&self, label: &str) -> Result<BTreeSet<NodeId>, StorageError> {
        tag_key(label, &self.root.dir().join("nodes").display().to_string())?;
        let mut all = BTreeSet::new();
        for mesh in self.meshes_on_root().await? {
            all.extend(self.mesh(&mesh).await?.tagged(label).await?);
        }
        Ok(all)
    }
}

// ---------------------------------------------------------------- connections.storage

pub use rdm_node::storage::{ConnectionsStorage, FileConnectionsStorage, MemoryConnectionsStorage};

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::connections::NodeConnection;
    use rdm_core::connections::{ConnectionEnd, ConnectionKind, ConnectionState};

    fn node(name: &str) -> NodeRecord {
        NodeRecord {
            node_id: NodeId::mint(),
            fabric_id: FabricId::mint(),
            name: name.parse().unwrap(),
            mesh_id: MeshId::mint(),
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
            id: rdm_core::ConnectionId::mint(),
            source: end("mesh1.admin.1"),
            destination: end("mesh1.rpc.1"),
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
        let d = crate::record_store::tempdir("storage");
        let mesh = MeshRecord { mesh_id: MeshId::mint(), name: "mesh1".into(), dead: false };
        let (own, peer) = (node("mesh1.admin.1"), node("mesh1.rpc.1"));
        let (up, down) = (fact(ConnectionState::Connected), fact(ConnectionState::Disconnected));
        let check = async |m: &dyn MeshStorage, n: &dyn NodesStorage, c: &dyn ConnectionsStorage, fresh: bool| {
            if fresh {
                assert_eq!(m.mesh("mesh1").await.unwrap(), None);
                assert_eq!(n.own().await.unwrap(), None);
                assert_eq!(m.claim_mesh(&mesh).await.unwrap(), Ok(MeshClaim::Written { over: None }));
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
        NodeRow { name: name.parse().unwrap(), node_id: NodeId::mint(), status: crate::model::NodeStatus::ReadyForTraffic, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None }
    }

    /// CONTRACT (CLAUDE.md read rule: a lookup by id that cannot read the store's own key reads the
    /// kind's `id` index): the node row's id index names exactly the row each node holds, kept by the
    /// store in the same step as every row write, in memory and on disk alike. A put at a free key
    /// names it; a move (put at the new key, release the old) leaves it naming the new one; a release
    /// drops it; a recovery's new node put over a `Deleted` row of another node leaves that node with
    /// no entry; a put refused by another node's live row writes no entry and leaves the holder's. A
    /// node that never held a row has none; a hold of the key the node already holds names it (a
    /// move retried after a write that ended between the row file and its entry), and a put of it,
    /// once named, writes no entry again.
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
        // A write that ended between the row and its entry (two files): the move retried over the row
        // the node holds there leaves it as it is and names it, so no row is left unnamed.
        let torn = row("mesh1.rpc.3");
        std::fs::write(root.nodes("mesh1").join("rows").join("mesh1.rpc.3.json"), serde_json::to_vec(&serde_json::json!({"format": ROW_FORMAT, "record": torn})).unwrap()).unwrap();
        assert_eq!(file.node_row_name("mesh1", &torn.node_id).await.unwrap(), None);
        assert!(!file.hold_node_row(&torn.name, &NodeId::mint()).await.unwrap(), "another node holds no row there");
        assert!(file.hold_node_row(&torn.name, &torn.node_id).await.unwrap());
        assert_eq!(file.node_row_name("mesh1", &torn.node_id).await.unwrap(), Some(torn.name.clone()), "the hold of a key the node holds names it");
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
        crate::node_rows::move_row(&file, &"mesh1.admin.1".parse().unwrap(), &record, BTreeMap::new(), Default::default(), None, &from.name, &to.name).await.unwrap();
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
            m.claim_mesh(&MeshRecord { mesh_id: MeshId::mint(), name: mesh.into(), dead: false }).await.unwrap().expect("each mesh claims a name no mesh holds");
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

    /// CONTRACT (R-TG1: "node.get with a tag filter names a label and answers every node carrying it,
    /// through a tags index declared on the node store (a point read per label)"; CLAUDE.md: each
    /// fact is its own keyed row, an index row is a blind put of its own key and a tombstone removes
    /// it): the file store keeps the tags index with every write of a row, one file per (label, node
    /// id) under `nodes/<mesh>/tags/<label>/`. Two meshes' rows carrying `red` are read by one label
    /// read across the root, a second store of another incarnation included; a row put without a
    /// label tombstones that label's file and no other; a rename keeps the node's labels (the index
    /// is keyed by node id); a `Deleted` row tombstones every label its row carries; a release of the
    /// only row a node holds tombstones its labels. A label no row ever carried reads as carried by
    /// none and makes no directory; a key that is not a label is refused by name and nothing is read.
    /// A label opened while the mesh-primary seat's records are cut off is cut off at its open.
    #[tokio::test]
    async fn the_tags_index_is_one_file_per_label_and_node_kept_with_every_row_write_and_read_per_label() {
        let d = crate::record_store::tempdir("tags-index");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = crate::store_root::StoreRoot::open(&store, &crate::model::FabricId::mint()).unwrap();
        let (n1, n2) = (FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap(), FileNodesStorage::open(&d.join("mesh2.admin.1"), &root, "mesh2", &IncarnationId::mint()).await.unwrap());
        root.scratchpad().expect("a root without the Fabric row opens with a scratchpad").publish().await.unwrap();
        let tags = |l: &[&str]| l.iter().map(|t| t.to_string()).collect::<BTreeSet<String>>();
        let (a, b) = (NodeId::mint(), NodeId::mint());
        let row_a = NodeRow { tags: tags(&["rabbit", "red"]), node_id: a.clone(), ..row("mesh1.rpc.1") };
        n1.put_node_row(&row_a).await.unwrap().unwrap();
        let row_b = NodeRow { name: "mesh2.rpc.1".parse().unwrap(), node_id: b.clone(), tags: tags(&["red"]), ..row("mesh2.rpc.1") };
        n2.put_node_row(&row_b).await.unwrap().unwrap();
        let files = |mesh: &str, label: &str| keys(&root.dir().join("nodes").join(mesh).join("tags").join(label));
        assert_eq!((files("mesh1", "rabbit"), files("mesh1", "red"), files("mesh2", "red")), (vec![format!("{a}.json")], vec![format!("{a}.json")], vec![format!("{b}.json")]), "one file per (label, node id), under the node's mesh");
        let other = FileNodesStorage::open(&d.join("mesh1.admin.2"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        for n in [&n1, &n2, &other] {
            assert_eq!(n.tagged("red").await.unwrap(), [a.clone(), b.clone()].into(), "one label read answers every mesh's nodes carrying it");
        }
        // A put without rabbit tombstones rabbit's row only.
        n1.put_node_row(&NodeRow { tags: tags(&["red"]), ..row_a.clone() }).await.unwrap().unwrap();
        assert_eq!((files("mesh1", "rabbit").len(), other.tagged("rabbit").await.unwrap().len(), other.tagged("red").await.unwrap().len()), (0, 0, 2), "the dropped label's row is tombstoned, the kept one stands");
        // A rename keeps the labels: put at the new name, then the old key released.
        let moved = NodeRow { name: "mesh1.rpc.1.old".parse().unwrap(), tags: tags(&["red"]), ..row_a.clone() };
        n1.put_node_row(&moved).await.unwrap().unwrap();
        assert!(n1.release_node_row(&row_a.name, &a).await.unwrap());
        assert_eq!(other.tagged("red").await.unwrap(), [a.clone(), b.clone()].into(), "a renamed node keeps its labels");
        // A Deleted row tombstones every label it carries; a release of a node's only row its labels.
        n1.put_node_row(&NodeRow { status: crate::model::NodeStatus::Deleted, ..moved.clone() }).await.unwrap().unwrap();
        assert!(n2.release_node_row(&row_b.name, &b).await.unwrap());
        assert!(other.tagged("red").await.unwrap().is_empty(), "neither the deleted node nor the released one is named under red");
        assert_eq!(n1.node_row_at(&moved.name).await.unwrap().map(|r| r.tags), Some(tags(&["red"])), "the Deleted row keeps its tags");
        // A label no row carried: none, and no directory made.
        assert!(other.tagged("blue").await.unwrap().is_empty());
        assert!(!root.dir().join("nodes").join("mesh1").join("tags").join("blue").exists(), "a read makes no directory");
        assert!(matches!(other.tagged("../rows").await, Err(StorageError::Unrecognised { reason, .. }) if reason.contains("\"../rows\"")), "a key that is not a label is refused by name");
        // While a seat transfer cuts the mesh-primary seat's records off, a label's rows opened then
        // are cut off at their open: the write of its index row is refused by name, never written.
        n1.cut_off_own_labels(Some("the mesh-primary seat moves".into()));
        let green = NodeRow { name: "mesh1.rpc.2".parse().unwrap(), node_id: NodeId::mint(), tags: tags(&["green"]), ..row("mesh1.rpc.2") };
        assert!(matches!(n1.put_node_row(&green).await, Err(StorageError::CutOff { reason, .. }) if reason == "the mesh-primary seat moves"), "a label opened during the cut-off is cut off");
        assert!(files("mesh1", "green").is_empty(), "nothing of the cut-off label is written");
    }

    /// CONTRACT (proposal node-properties-own-commands.md §8, replace: the successor inherits the
    /// retained predecessor row's tags at the cutover, "and the predecessor's index entries gone"):
    /// between the cutover's put of the successor at the name and its mark, both nodes carry the label;
    /// the retained row put again with `inherited_by` keeps its tags and the tags index names none of
    /// them for it; a later put at the same key carrying the field (a tags or config write) indexes
    /// none again, an added label included, while the successor stays named.
    #[tokio::test]
    async fn a_row_a_successor_inherited_keeps_its_tags_and_holds_none_in_the_tags_index() {
        let d = crate::record_store::tempdir("tags-inherited");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = crate::store_root::StoreRoot::open(&store, &crate::model::FabricId::mint()).unwrap();
        let n1 = FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        root.scratchpad().expect("a root without the Fabric row opens with a scratchpad").publish().await.unwrap();
        let tags = |l: &[&str]| l.iter().map(|t| t.to_string()).collect::<BTreeSet<String>>();
        let (pred, succ) = (NodeId::mint(), NodeId::mint());
        let old = NodeRow { node_id: pred.clone(), tags: tags(&["rabbit"]), ..row("mesh1.rpc.1.old") };
        n1.put_node_row(&old).await.unwrap().unwrap();
        n1.put_node_row(&NodeRow { node_id: succ.clone(), tags: tags(&["rabbit"]), ..row("mesh1.rpc.1") }).await.unwrap().unwrap();
        assert_eq!(n1.tagged("rabbit").await.unwrap(), [pred.clone(), succ.clone()].into(), "between the cutover's put and its mark both carry the label");
        let marked = NodeRow { inherited_by: Some(succ.clone()), ..old.clone() };
        n1.put_node_row(&marked).await.unwrap().unwrap();
        assert_eq!(n1.tagged("rabbit").await.unwrap(), [succ.clone()].into(), "the retained row's label left the index");
        assert_eq!(n1.node_row_at(&old.name).await.unwrap(), Some(marked.clone()), "the retained row keeps its tags and names who inherited it");
        n1.put_node_row(&NodeRow { tags: tags(&["rabbit", "red"]), ..marked.clone() }).await.unwrap().unwrap();
        assert_eq!((n1.tagged("rabbit").await.unwrap(), n1.tagged("red").await.unwrap()), ([succ.clone()].into(), BTreeSet::new()), "a later write of the inherited row indexes none of its labels");
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
        n.cut_off_other_meshes("seat transfer node.connections.delete:mesh1.admin.1").await;
        for name in ["mesh2.rpc.1", "mesh3.rpc.1"] {
            match n.put_node_row(&rows[name]).await {
                Err(StorageError::CutOff { reason, .. }) => assert!(reason.contains("node.connections.delete:mesh1.admin.1"), "{reason}"),
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
            incarnation_id: IncarnationId::parse(&rdm_core::fixture_crockford60(inc)).unwrap(),
            runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-1")).unwrap(),
            data_dir: Some("/tmp/x".into()),
        };
        let (a, b) = (row("i1"), row("i2"));
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let n = FileNodesStorage::open(&d, &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        n.put_runtime(&a).await.unwrap();
        n.put_runtime(&b).await.unwrap();
        n.put_runtime(&a).await.unwrap();
        let mut held = FileNodesStorage::open(&d, &root, "mesh1", &IncarnationId::mint()).await.unwrap().runtimes().await.unwrap();
        held.sort_by_key(|r| r.incarnation_id.to_string());
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
    /// blocking pool, as the row's write does (`blocking::blocking_io`). The row write runs
    /// inline on every view install, the join door's among them, after this read of the key.
    /// The cell: a row file that is a FIFO holds the read until a writer comes; the runtime's only
    /// thread keeps running meanwhile, and the read answers the row.
    #[test]
    fn a_node_row_read_waits_on_the_disk_off_the_runtime_thread() {
        let row = NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: NodeId::mint(), status: crate::model::NodeStatus::ReadyForTraffic, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None };
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
        let row = RuntimeRow { node_id: NodeId::mint(), name: "mesh1.admin.2".parse().unwrap(), incarnation_id: IncarnationId::parse(&rdm_core::fixture_crockford60("i1")).unwrap(), runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-1")).unwrap(), data_dir: Some("/tmp/x".into()) };
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
        let row = |node_id: &NodeId, status: crate::model::NodeStatus, tier: &str| NodeRow { name: name.clone(), node_id: node_id.clone(), status, config: BTreeMap::from([("tier".to_string(), tier.to_string())]), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None };
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

    /// CONTRACT (node-identity-and-state.md §2.7: the mesh's name is claimed "by insert-and-fail on
    /// the key `(path, name)` ... Held means held by a different MeshId: `NameHeld` names that holder.
    /// The same MeshId re-claiming its own name ... is idempotent and accepted"; R-MN1: only a delete
    /// releases a name, a mesh's by its removal, the row Dead; states.md: a mesh id is never reissued,
    /// dead is final): the mesh store's adapter is where a mesh's name is claimed. A free key takes
    /// the row; the same mesh re-claiming finds its own row and writes nothing; another mesh's claim
    /// of the live name is refused naming the holder and writes nothing; the holder's Dead row is a
    /// write of its own row, and a repeat of it writes nothing; the Dead mesh is never written live
    /// again; another mesh claims the name over the Dead row, which the claim names. The memory store
    /// and the file store each run their own check.
    #[tokio::test]
    async fn a_mesh_name_is_claimed_in_the_adapter_and_only_a_dead_row_releases_it() {
        let d = crate::record_store::tempdir("storage-mesh-claim");
        let check = async |m: &dyn MeshStorage, store: &str| {
            let (first, second) = (MeshId::mint(), MeshId::mint());
            let row = |id: &MeshId, dead: bool| MeshRecord { mesh_id: id.clone(), name: "mesh2".into(), dead };
            assert_eq!(m.claim_mesh(&row(&first, false)).await, Ok(Ok(MeshClaim::Written { over: None })), "{store}: a free key takes the row");
            assert_eq!(m.claim_mesh(&row(&first, false)).await, Ok(Ok(MeshClaim::Held)), "{store}: the same mesh re-claiming its name writes nothing");
            assert_eq!(m.claim_mesh(&row(&second, false)).await, Ok(Err(MeshClaimRefused::NameHeld { name: "mesh2".into(), by: first.clone() })), "{store}: another mesh's live row holds mesh2");
            assert_eq!(m.mesh("mesh2").await.unwrap(), Some(row(&first, false)), "{store}: nothing is written by a refused claim");
            assert_eq!(m.claim_mesh(&row(&first, true)).await, Ok(Ok(MeshClaim::Written { over: None })), "{store}: the holder's Dead row is a write of its own row");
            assert_eq!(m.claim_mesh(&row(&first, true)).await, Ok(Ok(MeshClaim::Held)), "{store}: a repeat of the Dead row writes nothing");
            assert_eq!(m.claim_mesh(&row(&first, false)).await, Ok(Err(MeshClaimRefused::MeshDead { name: "mesh2".into(), mesh_id: first.clone() })), "{store}: a dead mesh is never written live again");
            assert_eq!(m.claim_mesh(&row(&second, false)).await, Ok(Ok(MeshClaim::Written { over: Some(first.clone()) })), "{store}: a Dead row holds no name");
            assert_eq!(m.mesh("mesh2").await.unwrap(), Some(row(&second, false)), "{store}: the claim over the Dead row is written");
        };
        check(&MemoryMeshStorage::default(), "memory").await;
        check(&FileMeshStorage::open(&crate::store_root::StoreRoot::at(&d.join("root")), &IncarnationId::mint()).await.unwrap(), "file").await;
        let _ = std::fs::remove_dir_all(&d);
    }
    /// CONTRACT (R14 item 3: "The name claim is the sole writer's conditional write"; item 9: "At
    /// commit-state each writer writes what it holds to the shared root"; CLAUDE.md: a claim is
    /// insert-and-fail and a refused claim writes nothing; node-identity-and-state.md §2.7: "A refused
    /// write stays held, named"): two writers on one shared root, each before the fabric's first
    /// state-commit with its own scratchpad, claim mesh2's name for two different MeshIds and the row
    /// at mesh2.rpc.1 for two different nodes. Each claim wins against what its writer sees (its own
    /// scratchpad over the disk). The first writer's publication writes its claims. The second's
    /// publication re-runs each held claim as a conditional write against the row now standing at its
    /// key: each is refused, naming the holder, and nothing of it is written, so the first writer's
    /// rows stand, and B's id index entry, which follows its row, is not written either; its
    /// commit-state span `rdm.node_admin.fabric.update.via-round-action` counts the refusals and names
    /// each. Must NOT happen: a held claim written blindly over another writer's row at publication.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_held_claim_is_re_run_at_publication_and_a_refused_one_is_named_and_writes_nothing() {
        let d = crate::record_store::tempdir("held-claims");
        let dir = d.join("root");
        let (pad_a, pad_b) = (crate::record_store::Scratchpad::new(), crate::record_store::Scratchpad::new());
        let (root_a, root_b) = (crate::store_root::StoreRoot::held_at(&dir, pad_a.clone()), crate::store_root::StoreRoot::held_at(&dir, pad_b.clone()));
        let (meshes_a, meshes_b) = (FileMeshStorage::open(&root_a, &IncarnationId::mint()).await.unwrap(), FileMeshStorage::open(&root_b, &IncarnationId::mint()).await.unwrap());
        let (nodes_a, nodes_b) = (FileNodesStorage::open(&d.join("a"), &root_a, "mesh2", &IncarnationId::mint()).await.unwrap(), FileNodesStorage::open(&d.join("b"), &root_b, "mesh2", &IncarnationId::mint()).await.unwrap());
        let (mesh_a, mesh_b) = (MeshRecord { mesh_id: MeshId::mint(), name: "mesh2".into(), dead: false }, MeshRecord { mesh_id: MeshId::mint(), name: "mesh2".into(), dead: false });
        let (row_a, row_b) = (row("mesh2.rpc.1"), row("mesh2.rpc.1"));
        assert_eq!(meshes_a.claim_mesh(&mesh_a).await, Ok(Ok(MeshClaim::Written { over: None })), "writer A's claim wins against its scratchpad and the disk");
        assert_eq!(meshes_b.claim_mesh(&mesh_b).await, Ok(Ok(MeshClaim::Written { over: None })), "writer B's claim wins against its scratchpad and the disk");
        assert_eq!(nodes_a.put_node_row(&row_a).await, Ok(Ok(())));
        assert_eq!(nodes_b.put_node_row(&row_b).await, Ok(Ok(())));
        let capture = crate::span_capture::Capture::new("i143-3089-held-claim-refused");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let commit = |pad: std::sync::Arc<crate::record_store::Scratchpad>, node: &str| {
            let node: PathName = node.parse().unwrap();
            async move { crate::fabric_rounds::RoundActions::commit_state(&crate::fabric_rounds::StorePublication::new(Some(pad), None), &node).await }
        };
        commit(pad_a, "mesh1.admin.1").await.expect("writer A publishes its claims");
        commit(pad_b, "mesh2.admin.1").await.expect("writer B's publication names its refused claims and is not a failed write");
        let reader = crate::store_root::StoreRoot::at(&dir);
        assert_eq!(FileMeshStorage::open(&reader, &IncarnationId::mint()).await.unwrap().mesh("mesh2").await.unwrap(), Some(mesh_a.clone()), "B's refused claim wrote nothing: mesh2's row is A's");
        assert_eq!(FileNodesStorage::open(&d.join("r"), &reader, "mesh2", &IncarnationId::mint()).await.unwrap().node_row_at(&row_a.name).await.unwrap(), Some(row_a.clone()), "B's refused row claim wrote nothing: mesh2.rpc.1's row is A's");
        let spans = capture.named("rdm.node_admin.fabric.update.via-round-action");
        let b = spans.iter().find(|s| s.attributes.get("node").map(String::as_str) == Some("mesh2.admin.1")).unwrap_or_else(|| panic!("writer B's commit-state span: {spans:?}"));
        assert_eq!(b.attributes.get("refused").map(String::as_str), Some("3"), "B's commit-state counts its two refused claims and the id index entry that follows the refused row: {b:?}");
        let stores = b.attributes.get("stores").cloned().unwrap_or_default();
        assert!(stores.contains(&format!("meshes refused 1")) && stores.contains(&mesh_a.mesh_id.to_string()), "the refusal of mesh2's claim names its holder: {stores}");
        assert!(stores.contains("nodes/mesh2/rows refused 1") && stores.contains(row_a.node_id.as_str()), "the refusal of mesh2.rpc.1's claim names its holder: {stores}");
        assert!(stores.contains(&format!("nodes/mesh2/ids refused 1: {}", row_b.node_id)), "B's id index entry follows its refused row and is not written, named: {stores}");
        eprintln!("span rdm.node_admin.fabric.update.via-round-action trace_id={} {:?}", b.trace_id, b.attributes);
    }
    /// CONTRACT (node-identity-and-state.md §2.7: "The same MeshId re-claiming its own name, at
    /// state-commit, on a retry or from a replaced primary, is idempotent and accepted"; R13: the
    /// fabric-primary writes as the acting primary of a mesh with no primary; R14 item 9: each writer
    /// publishes what it holds at commit-state): before the fabric's first state-commit the
    /// fabric-primary (writer A, at CreateMesh) and mesh2's primary (writer B, at its seat) each hold
    /// mesh2's claim for the same MeshId. A publishes first and writes the row; B's publication finds
    /// its own mesh's row standing and writes nothing, and names it: its commit-state span counts the
    /// claim that stands (`stands`) and its stores name `meshes stands 1`. Must NOT happen: B's
    /// publication of mesh2's row dropped from its record because the row was already there.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_held_claim_the_standing_row_already_holds_is_named_at_publication() {
        let d = crate::record_store::tempdir("held-claim-stands");
        let dir = d.join("root");
        let (pad_a, pad_b) = (crate::record_store::Scratchpad::new(), crate::record_store::Scratchpad::new());
        let meshes_a = FileMeshStorage::open(&crate::store_root::StoreRoot::held_at(&dir, pad_a.clone()), &IncarnationId::mint()).await.unwrap();
        let meshes_b = FileMeshStorage::open(&crate::store_root::StoreRoot::held_at(&dir, pad_b.clone()), &IncarnationId::mint()).await.unwrap();
        let mesh2 = MeshRecord { mesh_id: MeshId::mint(), name: "mesh2".into(), dead: false };
        assert_eq!(meshes_a.claim_mesh(&mesh2).await, Ok(Ok(MeshClaim::Written { over: None })));
        assert_eq!(meshes_b.claim_mesh(&mesh2).await, Ok(Ok(MeshClaim::Written { over: None })));
        let capture = crate::span_capture::Capture::new("i143-3089-held-claim-stands");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let commit = |pad: std::sync::Arc<crate::record_store::Scratchpad>, node: &str| {
            let node: PathName = node.parse().unwrap();
            async move { crate::fabric_rounds::RoundActions::commit_state(&crate::fabric_rounds::StorePublication::new(Some(pad), None), &node).await }
        };
        commit(pad_a, "mesh1.admin.1").await.unwrap();
        commit(pad_b, "mesh2.admin.1").await.unwrap();
        let spans = capture.named("rdm.node_admin.fabric.update.via-round-action");
        let b = spans.iter().find(|s| s.attributes.get("node").map(String::as_str) == Some("mesh2.admin.1")).unwrap_or_else(|| panic!("writer B's commit-state span: {spans:?}"));
        assert_eq!(b.attributes.get("stands").map(String::as_str), Some("1"), "B's commit-state counts its claim that stands: {b:?}");
        assert_eq!(b.attributes.get("stores").map(String::as_str), Some("meshes stands 1"), "B's stores name mesh2's row standing: {b:?}");
        eprintln!("span rdm.node_admin.fabric.update.via-round-action trace_id={} {:?}", b.trace_id, b.attributes);
    }

    /// CONTRACT (`NodesStorage::release_node_row`: "its row goes when it is `holder`'s ... a row of
    /// another node there (the name was released before and claimed since) is left as it is"; R14 item
    /// 9: each writer publishes what it holds at commit-state): before the fabric's first state-commit
    /// writer A claims mesh2.rpc.1 for its node and withdraws the claim, both held in its scratchpad;
    /// writer B claims mesh2.rpc.1 for another node and publishes first. A's publication removes the row
    /// standing at the key only while it is A's node's: B's row stands. Must NOT happen: a held
    /// release removing at publication a row another writer published meanwhile.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_held_release_removes_at_publication_only_the_holders_own_row() {
        let d = crate::record_store::tempdir("held-release");
        let dir = d.join("root");
        let (pad_a, pad_b) = (crate::record_store::Scratchpad::new(), crate::record_store::Scratchpad::new());
        let nodes_a = FileNodesStorage::open(&d.join("a"), &crate::store_root::StoreRoot::held_at(&dir, pad_a.clone()), "mesh2", &IncarnationId::mint()).await.unwrap();
        let nodes_b = FileNodesStorage::open(&d.join("b"), &crate::store_root::StoreRoot::held_at(&dir, pad_b.clone()), "mesh2", &IncarnationId::mint()).await.unwrap();
        let (row_a, row_b) = (row("mesh2.rpc.1"), row("mesh2.rpc.1"));
        assert_eq!(nodes_a.put_node_row(&row_a).await, Ok(Ok(())));
        assert_eq!(nodes_a.release_node_row(&row_a.name, &row_a.node_id).await, Ok(true), "A withdraws its own claim");
        assert_eq!(nodes_b.put_node_row(&row_b).await, Ok(Ok(())));
        pad_b.publish().await.unwrap();
        pad_a.publish().await.unwrap();
        let reader = FileNodesStorage::open(&d.join("r"), &crate::store_root::StoreRoot::at(&dir), "mesh2", &IncarnationId::mint()).await.unwrap();
        assert_eq!(reader.node_row_at(&row_b.name).await.unwrap(), Some(row_b), "A's held release left B's row standing at mesh2.rpc.1");
    }

    /// CONTRACT (`NodesStorage::node_row_name`: "names exactly the row the node holds"; R14 item 3: the
    /// name claim is the sole writer's conditional write, decided again at publication; CLAUDE.md read
    /// rule: an `id` index entry is a lookup structure over the rows, never a fact of its own): before
    /// the fabric's first state-commit writers A and B each hold a claim of mesh2.rpc.1 for their own
    /// node; B publishes first, so A's publication refuses A's row claim against B's standing row. A's
    /// id index entry follows its row's publication: it is decided again at A's publication against
    /// the row then standing at the key it names, and is not written, so no reader finds A's node at
    /// mesh2.rpc.1. And an entry naming a key whose row another node holds (one a write left between
    /// its two files) answers nothing. Must NOT happen: A's entry published blindly beside its refused
    /// claim, and `node_row_name` naming B's row as A's node's.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_held_id_entry_follows_its_rows_publication_and_names_only_a_row_its_node_holds() {
        let d = crate::record_store::tempdir("held-id-entry");
        let dir = d.join("root");
        let (pad_a, pad_b) = (crate::record_store::Scratchpad::new(), crate::record_store::Scratchpad::new());
        let nodes_a = FileNodesStorage::open(&d.join("a"), &crate::store_root::StoreRoot::held_at(&dir, pad_a.clone()), "mesh2", &IncarnationId::mint()).await.unwrap();
        let nodes_b = FileNodesStorage::open(&d.join("b"), &crate::store_root::StoreRoot::held_at(&dir, pad_b.clone()), "mesh2", &IncarnationId::mint()).await.unwrap();
        let (row_a, row_b) = (row("mesh2.rpc.1"), row("mesh2.rpc.1"));
        assert_eq!(nodes_a.put_node_row(&row_a).await, Ok(Ok(())), "A's claim wins against its scratchpad and the disk");
        assert_eq!(nodes_b.put_node_row(&row_b).await, Ok(Ok(())), "B's claim wins against its scratchpad and the disk");
        pad_b.publish().await.unwrap();
        let published = pad_a.publish().await.unwrap();
        let reader_root = crate::store_root::StoreRoot::at(&dir);
        let reader = FileNodesStorage::open(&d.join("r"), &reader_root, "mesh2", &IncarnationId::mint()).await.unwrap();
        assert_eq!(reader.node_row_at(&row_a.name).await.unwrap(), Some(row_b.clone()), "A's refused claim wrote nothing: mesh2.rpc.1's row is B's");
        let ids = reader_root.nodes("mesh2").join("ids");
        assert!(!ids.join(format!("{}.json", row_a.node_id)).exists(), "A's id entry is not published beside its refused claim: {published:?}");
        assert_eq!(reader.node_row_name("mesh2", &row_a.node_id).await.unwrap(), None, "no reader finds A's node at mesh2.rpc.1");
        assert_eq!(reader.node_row_name("mesh2", &row_b.node_id).await.unwrap(), Some(row_b.name.clone()), "B's entry names B's row");
        // An entry naming a key whose row another node holds: a write that ended between its two files.
        let torn = NodeId::mint();
        std::fs::write(ids.join(format!("{torn}.json")), serde_json::to_vec(&serde_json::json!({ "format": ROW_ID_FORMAT, "record": { "node_id": torn, "name": row_b.name } })).unwrap()).unwrap();
        assert_eq!(reader.node_row_name("mesh2", &torn).await.unwrap(), None, "an entry naming B's row answers nothing for another node");
    }
}
