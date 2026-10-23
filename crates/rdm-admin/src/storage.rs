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
#[cfg(feature = "file-backend")]
use crate::record_store::FileRecords;
use crate::mesh_claims::{InFlight, Taken};
use crate::storage_error::StorageError;
use serde::{Deserialize, Serialize};
use rdm_core::status::{MeshState, NodeState};
use std::collections::{BTreeMap, BTreeSet, HashMap};
#[cfg(feature = "file-backend")]
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

/// Claim `record.name` for `record.mesh_id` and keep `record` as the Mesh row at that key: the row
/// writer reads the row at the name and claims it (node-identity-and-state.md §2.7; Q109: the storage
/// decides nothing). The key is free when no row is there or another mesh's Dead row is (its removal
/// released the name): `record` is put whole. This mesh's own row there is the same mesh re-claiming
/// its name: written only when `record` differs (its Dead write), and a Dead row is never written live
/// again (`MeshDead`). Another mesh's live row holds the name: `NameHeld` naming that mesh, and
/// nothing is written.
pub async fn claim_mesh(meshes: &dyn MeshStorage, record: &MeshRecord) -> Result<Result<MeshClaim, MeshClaimRefused>, StorageError> {
    let claimed = claim_mesh_against(meshes.mesh(&record.name).await?.as_ref(), record);
    if let Ok(MeshClaim::Written { .. }) = claimed {
        meshes.put_mesh(record).await?;
    }
    Ok(claimed)
}

/// The durable record of the meshes this admin knows: each mesh's row at its name.
#[async_trait]
pub trait MeshStorage: Send + Sync {
    /// The row at the name `name`: a point read through its key; `None` when none was ever written.
    async fn mesh(&self, name: &str) -> Result<Option<MeshRecord>, StorageError>;
    /// Write `record` at its name's key, as given (Q109: storage stores and returns what it is given);
    /// the name's claim is the row writer's ([`claim_mesh`]).
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError>;
    /// Every mesh row held.
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError>;
    /// Take `mesh`'s in-flight row for `claim` (R-BI7 caveat 3, [`crate::mesh_claims`]): the insert of
    /// the row's key decides. A row standing for the same attempt is that attempt's own (a repeat take,
    /// by any claimant): `Won`, and the claim of the attempt itself is decided by the Build log. A row
    /// standing for another attempt is `Held`, naming it. A row the insert met and the read no longer
    /// finds was released between the two: refused by name, never answered as either outcome. An
    /// attempt claim is a Build fact, durable when it is taken, before the fabric's first
    /// state-commit too (Q61, build.md §5.2).
    async fn take_in_flight(&self, mesh: &str, claim: &InFlight) -> Result<Taken, StorageError>;
    /// Release `mesh`'s in-flight row held for `attempt`, at that attempt's verdict: the row goes only
    /// while it names that attempt, so a release never removes another attempt's row. Its one writer
    /// is the admin that durably recorded the verdict (the seat that holds the attempt's lane), so no
    /// two releases of one row race.
    async fn release_in_flight(&self, mesh: &str, attempt: &InFlight) -> Result<(), StorageError>;
    /// The attempt holding `mesh`, when one does: a point read of its in-flight row.
    async fn in_flight(&self, mesh: &str) -> Result<Option<InFlight>, StorageError>;
    /// A blind put of the Mesh status fact `row` on the mesh named `mesh` (its own key).
    async fn put_mesh_status(&self, mesh: &str, row: &MeshStatusRow) -> Result<(), StorageError>;
    /// Whether the mesh named `mesh` holds the Mesh status fact `row`: a point read of its key.
    async fn mesh_status_held(&self, mesh: &str, row: &MeshStatusRow) -> Result<bool, StorageError>;
    /// The greatest state held per Mesh, across every mesh.
    async fn mesh_statuses(&self) -> Result<HashMap<MeshId, MeshState>, StorageError>;
}

/// A mesh store held in memory: the Mesh rows at their names, and each mesh's in-flight row.
#[derive(Debug, Default)]
pub struct MemoryMeshStorage {
    rows: Mutex<BTreeMap<String, MeshRecord>>,
    in_flight: Mutex<BTreeMap<String, InFlight>>,
    /// Each mesh's status facts, by mesh name and row key.
    statuses: Mutex<BTreeMap<(String, String), MeshStatusRow>>,
}

#[async_trait]
impl MeshStorage for MemoryMeshStorage {
    async fn mesh(&self, name: &str) -> Result<Option<MeshRecord>, StorageError> {
        Ok(self.rows.lock().unwrap().get(name).cloned())
    }
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError> {
        self.rows.lock().unwrap().insert(record.name.clone(), record.clone());
        Ok(())
    }
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
        Ok(self.rows.lock().unwrap().values().cloned().collect())
    }
    async fn take_in_flight(&self, mesh: &str, claim: &InFlight) -> Result<Taken, StorageError> {
        // The map's key is the row's: the look and the insert are one step under its lock.
        let mut rows = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        Ok(match rows.get(mesh) {
            Some(held) if held.same_attempt(claim) => Taken::Won,
            Some(held) => Taken::Held(held.clone()),
            None => {
                rows.insert(mesh.to_string(), claim.clone());
                Taken::Won
            }
        })
    }
    async fn release_in_flight(&self, mesh: &str, attempt: &InFlight) -> Result<(), StorageError> {
        let mut rows = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        if rows.get(mesh).is_some_and(|held| held.same_attempt(attempt)) {
            rows.remove(mesh);
        }
        Ok(())
    }
    async fn in_flight(&self, mesh: &str) -> Result<Option<InFlight>, StorageError> {
        Ok(self.in_flight.lock().unwrap_or_else(|e| e.into_inner()).get(mesh).cloned())
    }
    async fn put_mesh_status(&self, mesh: &str, row: &MeshStatusRow) -> Result<(), StorageError> {
        self.statuses.lock().unwrap().insert((mesh.to_string(), row.key()), row.clone());
        Ok(())
    }
    async fn mesh_status_held(&self, mesh: &str, row: &MeshStatusRow) -> Result<bool, StorageError> {
        Ok(self.statuses.lock().unwrap().contains_key(&(mesh.to_string(), row.key())))
    }
    async fn mesh_statuses(&self) -> Result<HashMap<MeshId, MeshState>, StorageError> {
        Ok(fold_meshes(self.statuses.lock().unwrap().values().cloned()))
    }
}

#[cfg(feature = "file-backend")]
const MESH_FORMAT: &str = "mesh-record/2";
#[cfg(feature = "file-backend")]
const MESH_STATUS_FORMAT: &str = "mesh-status/1";

#[cfg(feature = "file-backend")]
/// The format of a mesh's in-flight row. A row in the retired `mesh-in-flight/1` (no lane key) is
/// refused by name.
const IN_FLIGHT_FORMAT: &str = "mesh-in-flight/2";

#[cfg(feature = "file-backend")]
/// The key of a mesh's in-flight row under `meshes/<mesh>/claims/`.
const IN_FLIGHT: &str = "in-flight";

#[cfg(feature = "file-backend")]
/// `<store root>/meshes/<name>.json`: each mesh's Mesh row, written by that mesh's primary;
/// `meshes/<mesh>/claims/in-flight.json`, each mesh's in-flight row ([`crate::mesh_claims`]); and
/// `meshes/<mesh>/status/<mesh_id>-<state>.json`, each mesh's status facts, written by each state's
/// setter (states.md, "Who sets each state").
pub struct FileMeshStorage {
    rows: FileRecords,
    root: crate::store_root::StoreRoot,
    writer: IncarnationId,
    /// `meshes/<mesh>/claims/`, one store per mesh, opened on its first use.
    in_flight: tokio::sync::Mutex<BTreeMap<String, FileRecords>>,
    /// `meshes/<mesh>/status/`, one store per mesh, opened on its first use.
    statuses: tokio::sync::Mutex<BTreeMap<String, FileRecords>>,
}

#[cfg(feature = "file-backend")]
impl std::fmt::Debug for FileMeshStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileMeshStorage").field("rows", &self.rows).finish_non_exhaustive()
    }
}

#[cfg(feature = "file-backend")]
impl FileMeshStorage {
    /// Open the store under the fabric's store root, written by the process born as `writer`.
    pub async fn open(root: &crate::store_root::StoreRoot, writer: &IncarnationId) -> Result<Self, StorageError> {
        Ok(Self {
            rows: root.records(root.dir(), "meshes", writer).await?,
            root: root.clone(),
            writer: writer.clone(),
            in_flight: tokio::sync::Mutex::new(BTreeMap::new()),
            statuses: tokio::sync::Mutex::new(BTreeMap::new()),
        })
    }

    /// `mesh`'s status facts, held to the fabric's publication boundary as its Mesh row is.
    async fn statuses_of(&self, mesh: &str) -> Result<FileRecords, StorageError> {
        let mut stores = self.statuses.lock().await;
        if let Some(s) = stores.get(mesh) {
            return Ok(s.clone());
        }
        let s = self.root.records(&self.root.meshes(mesh), "status", &self.writer).await?;
        stores.insert(mesh.to_string(), s.clone());
        Ok(s)
    }

    /// Every mesh that holds a directory under `meshes/` of the shared root, by name.
    async fn meshes_on_root(&self) -> Result<Vec<String>, StorageError> {
        let dir = self.root.dir().join("meshes");
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

    /// `mesh`'s claims store. An attempt claim is a Build fact, durable on the root when it is
    /// taken, before the fabric's first state-commit too (Q61, build.md §5.2): opened directly, never
    /// held in the scratchpad.
    async fn claims_of(&self, mesh: &str) -> Result<FileRecords, StorageError> {
        let mut stores = self.in_flight.lock().await;
        if let Some(s) = stores.get(mesh) {
            return Ok(s.clone());
        }
        let s = FileRecords::open(&self.root.meshes(mesh), "claims", &self.writer).await?;
        stores.insert(mesh.to_string(), s.clone());
        Ok(s)
    }

    /// Finishes every write of the Mesh rows and status facts this store has issued
    /// (node-primary-update.md step 3).
    pub async fn finish_issued_writes(&self) {
        self.rows.finish_issued_writes().await;
        let statuses: Vec<FileRecords> = self.statuses.lock().await.values().cloned().collect();
        for s in statuses {
            s.finish_issued_writes().await;
        }
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl MeshStorage for FileMeshStorage {
    async fn mesh(&self, name: &str) -> Result<Option<MeshRecord>, StorageError> {
        self.rows.read(name, MESH_FORMAT).await
    }
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError> {
        self.rows.write(&record.name, MESH_FORMAT, record).await
    }
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
        self.rows.list(MESH_FORMAT).await
    }
    async fn take_in_flight(&self, mesh: &str, claim: &InFlight) -> Result<Taken, StorageError> {
        let store = self.claims_of(mesh).await?;
        if store.insert(IN_FLIGHT, IN_FLIGHT_FORMAT, claim).await? {
            return Ok(Taken::Won);
        }
        match store.read::<InFlight>(IN_FLIGHT, IN_FLIGHT_FORMAT).await? {
            Some(held) if held.same_attempt(claim) => Ok(Taken::Won),
            Some(held) => Ok(Taken::Held(held)),
            None => Err(StorageError::Io {
                file: format!("{}/{IN_FLIGHT}.json", store.store_name()),
                reason: format!("mesh {mesh}'s in-flight row was live at the insert for attempt {} of {} and gone at the read after it: its holder's verdict released it between the two; nothing is claimed, and the next claim decides on the free row", claim.attempt, claim.build_id),
            }),
        }
    }
    async fn release_in_flight(&self, mesh: &str, attempt: &InFlight) -> Result<(), StorageError> {
        let store = self.claims_of(mesh).await?;
        if !store.read::<InFlight>(IN_FLIGHT, IN_FLIGHT_FORMAT).await?.is_some_and(|held| held.same_attempt(attempt)) {
            return Ok(());
        }
        let attempt = attempt.clone();
        store.remove_released::<InFlight>(IN_FLIGHT, IN_FLIGHT_FORMAT, move |held| held.same_attempt(&attempt)).await
    }
    async fn in_flight(&self, mesh: &str) -> Result<Option<InFlight>, StorageError> {
        self.claims_of(mesh).await?.read(IN_FLIGHT, IN_FLIGHT_FORMAT).await
    }
    async fn put_mesh_status(&self, mesh: &str, row: &MeshStatusRow) -> Result<(), StorageError> {
        self.statuses_of(mesh).await?.write(&row.key(), MESH_STATUS_FORMAT, row).await
    }
    async fn mesh_status_held(&self, mesh: &str, row: &MeshStatusRow) -> Result<bool, StorageError> {
        self.statuses_of(mesh).await?.holds(&row.key()).await
    }
    async fn mesh_statuses(&self) -> Result<HashMap<MeshId, MeshState>, StorageError> {
        let mut rows = Vec::new();
        for mesh in self.meshes_on_root().await? {
            rows.extend(self.statuses_of(&mesh).await?.list::<MeshStatusRow>(MESH_STATUS_FORMAT).await?);
        }
        Ok(fold_meshes(rows))
    }
}

// ---------------------------------------------------------------- facts on a mesh and on its nodes

/// One Mesh status fact on the mesh it describes (`mesh.storage`): the Mesh entered `state`. Keyed by
/// the fact, `(mesh_id, state)`, so two states of one mesh are two rows and neither overwrites the
/// other; a reader folds the greatest. Written by the state's setter (states.md, "Who sets each state").
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

/// The greatest state held per Mesh among `rows`.
pub fn fold_meshes(rows: impl IntoIterator<Item = MeshStatusRow>) -> HashMap<MeshId, MeshState> {
    let mut out: HashMap<MeshId, MeshState> = HashMap::new();
    for r in rows {
        let e = out.entry(r.mesh_id).or_insert(r.state);
        if r.state > *e {
            *e = r.state;
        }
    }
    out
}

/// One node declared-state fact on the birth it describes, as its authority applied it: the birth
/// `incarnation_id` entered `state`. Keyed by the fact itself, `(node_id, incarnation, state)`: two
/// declarations of one birth put two rows and neither overwrites the other, whatever order their puts
/// land in. A reader folds the greatest state per birth. Written only by the node's mesh-primary (the
/// fabric-primary for a mesh with no primary); a start beginning the birth's next cycle tombstones
/// that birth's rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDeclaredRow {
    /// The node.
    pub node_id: NodeId,
    /// The birth that declared it.
    pub incarnation_id: IncarnationId,
    /// The birth's path.name as it declared: what a recovery's new node at that path supersedes it by.
    pub name: PathName,
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
            NodeState::Pending | NodeState::ReadyForTraffic | NodeState::ConnectionsDeleting | NodeState::Stopped => s,
        };
        [NodeState::Pending, NodeState::ReadyForTraffic, NodeState::ConnectionsDeleting, NodeState::Stopped].into_iter().map(every).map(|s| Self::key_of(node_id, incarnation_id, s)).collect()
    }
}

/// An operation effect already applied to this exact birth (Q49), on the birth. Independent of its
/// current cycle.
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
pub fn declared_names(rows: &[NodeDeclaredRow]) -> HashMap<(NodeId, IncarnationId), PathName> {
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
/// §7.3), on the birth: `PendingReconnect`, then `Dead` once its offline tickle found no path on two
/// rounds a staleness floor apart, `None` once it is heard again. Keyed by the exact birth; written
/// only by the mesh primary's tickle loop. Observer-inferred: never itself a reason to act.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSilenceRow {
    /// The node.
    pub node_id: NodeId,
    /// The node's `path.name`.
    pub name: PathName,
    /// The exact birth.
    pub incarnation_id: IncarnationId,
    /// The status held, `None` while heard.
    pub status: Option<crate::model::NodeStatus>,
}

impl NodeSilenceRow {
    /// The row's key: the exact birth.
    pub fn key(&self) -> String {
        birth_key(&self.node_id, &self.incarnation_id)
    }
}

/// The key of the exact birth `(node_id, incarnation_id)`'s row in a family keyed by the birth.
pub fn birth_key(node_id: &NodeId, incarnation_id: &IncarnationId) -> String {
    format!("{node_id}--{}", incarnation_id.as_str())
}

/// The durable `data-dir-lost` fact on a birth (R-FM2): the inherited data dir was absent for
/// `RDM_DATA_DIR_GONE_FLOORS` staleness floors while the provider reported the birth `Exited`. Keyed
/// by the exact birth.
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
        birth_key(&self.node_id, &self.incarnation_id)
    }
}

/// The durable `data-dir-unreadable` fact on a birth (R-FM2): the inherited data dir exists on disk,
/// but `node-key` is missing, unreadable, or corrupt. Keyed by the exact birth.
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
        birth_key(&self.node_id, &self.incarnation_id)
    }
}

/// A birth's member cert dates (fabric-certs.md §Tracking, `node-cert/2`), on the birth: written by
/// the issuer when the birth holds the cert, at its join's admission and on each renewal's
/// `CertRenewed`, from the issue the answer matches. The written expiry is the receipt (Q141); the row
/// is the birth's renewal state. Keyed by the exact birth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCertRow {
    /// The node.
    pub node_id: NodeId,
    /// The node's `path.name`.
    pub name: PathName,
    /// The exact birth.
    pub incarnation_id: IncarnationId,
    /// When the cert was issued, rafka-time ms.
    pub issued_at: u64,
    /// When it expires, rafka-time ms.
    pub expires_at: u64,
    /// When its issuing cert expires, rafka-time ms.
    pub issuer_expires_at: u64,
    /// When its issuer renews it (`rdm_core::certs::renew_at`), rafka-time ms.
    pub renew_at: u64,
}

impl NodeCertRow {
    /// The row's key: the exact birth.
    pub fn key(&self) -> String {
        birth_key(&self.node_id, &self.incarnation_id)
    }
}

/// A node-admin birth's signing cert dates (mesh-issuer-renew.md; `signing-cert/1`), on the birth:
/// the cert it signs its mesh's member certs with stays in its process, like the root; its issue
/// date and expiry are written by the fabric-primary when the admin holds it, at its join's admission
/// and on each `IssuerUpdated`. The written expiry is the receipt (Q141). Keyed by the exact birth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SigningCertRow {
    /// The node-admin.
    pub node_id: NodeId,
    /// Its `path.name`.
    pub name: PathName,
    /// The exact birth.
    pub incarnation_id: IncarnationId,
    /// When the signing cert was minted, rafka-time ms.
    pub issued_at: u64,
    /// When it expires, rafka-time ms.
    pub expires_at: u64,
    /// When the fabric-primary renews it, rafka-time ms.
    pub renew_at: u64,
}

impl SigningCertRow {
    /// The row's key: the exact birth.
    pub fn key(&self) -> String {
        birth_key(&self.node_id, &self.incarnation_id)
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
    /// The revision at which this birth is its node type's primary in its mesh, when it is: the
    /// election's result, recorded by the mesh-primary (node-identity-and-state.md §2.7); the write
    /// records the role and does not grant it. A row written before primaries were recorded carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_primary: Option<u64>,
    /// The revision at which this birth is the fabric-primary, when it is, recorded by the node-admin
    /// on its own row (node-identity-and-state.md §2.8), as [`Self::is_primary`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_fabric_primary: Option<u64>,
}

impl NodeRecord {
    /// Where this birth's gossip was last reachable: its transport address, as membership dials.
    pub fn gossip_addr(&self) -> Option<iroh::EndpointAddr> {
        let key = self.endpoint_id.0.parse::<iroh::PublicKey>().ok()?;
        Some(iroh::EndpointAddr::new(key).with_ip_addr(self.transport_addr))
    }
}

/// One birth's exact runtime as this admin first held it: the durable map a successor fabric
/// primary proves an exit from when its own membership never heard the birth, and the validated
/// report its accepting authority admitted (written first at the join's admission,
/// `crate::join::Joins::keep_admitted`, before the report is released: a re-run of the create after
/// that admin restarted completes WaitForMeshJoin from it, node-create.md step 5). Its own keyed row
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
    /// The successor that took this row's config and tags at a replace (node-replace.md step 3 and
    /// Inherited: the successor's row "inherits config and tags from the predecessor row ... whose
    /// labels leave the active tags index"), written on the retained predecessor's row once the
    /// successor's claim stands at the name. The row keeps its tags, as a `Deleted` row does, and the
    /// tags index holds none of them: every label leaves with that write and none arrives with a later
    /// write at the same key. A rename of the row writes none: the node takes a name again
    /// (node-replace.md "Rollback" step 3). A row no successor marked carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_by: Option<NodeId>,
    /// Immutable application declaration, scoped to the exact incarnation that reported it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_types: Option<AppTypes>,
    /// The executable binding the node runs, beside its config (node-replace.md step 3: "the row
    /// records it beside `config`"): written at the create's claim with the binding that create
    /// launches, kept by every later write of the row, a `Deleted` row's included, and seeded from the
    /// previous row like the config, so a node recovered at the path runs the bin its row records,
    /// never the recovering admin's (node-recovery.md "Retained"). A row written before it was
    /// recorded carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<rdm_core::binding::Binding>,
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

    /// The node's name within its path (`<base>[.old]`).
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
    /// Write `row` at its `path.name`, as given, with its id and tags index entries (Q109: storage
    /// stores and returns what it is given); the name's claim is the row writer's ([`claim_node_row`]).
    async fn put_node_row(&self, row: &NodeRow) -> Result<(), StorageError>;
    /// The record at `name`: a point read through the `path.name` key; `None` when none was ever written.
    async fn node_row_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError>;
    /// The `path.name` of the row the node `node_id` of `mesh` holds now: a point read through the
    /// node row's id index (the row's own key is its `path.name`, so a lookup by node id cannot read
    /// it). The index entry is kept by the store in the same step as every write of a row (a put, a
    /// release), so it names exactly the row the node holds; `None` when it holds none.
    async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, StorageError>;
    /// Remove the row at `name`, with its node's id index entry when it names `name` and that node's
    /// tags index entries.
    async fn remove_node_row(&self, name: &PathName) -> Result<(), StorageError>;
    /// The nodes carrying `label`, in every mesh: a point read of the label's rows of the tags index
    /// (R-TG1), never a read of any node's row. The index is a lookup structure the store keeps with
    /// every write of a row, as it keeps the id index: one row per (label, node id), a blind put when
    /// the node's row carries the label, the tombstone of its own key when the node's row stops
    /// carrying it, turns `Deleted`, or is the only row the node lets go of. A `label` that is not a
    /// tag label ([`rdm_core::status::tag_refusal`]) is refused by name.
    async fn tagged(&self, label: &str) -> Result<BTreeSet<NodeId>, StorageError>;
    /// A blind put of the declared-state fact `row` on its birth in `mesh`, durable before the
    /// answer leaves, before state-commit too (Q54).
    async fn put_node_declared(&self, mesh: &str, row: &NodeDeclaredRow) -> Result<(), StorageError>;
    /// A blind tombstone of every declared-state key of the birth `(node_id, incarnation_id)` in `mesh`.
    async fn remove_node_declared(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), StorageError>;
    /// Every declared-state fact on the births of `mesh`.
    async fn node_declared(&self, mesh: &str) -> Result<Vec<NodeDeclaredRow>, StorageError>;
    /// A blind put of the operation effect `row` on its birth in `mesh`; a cycle begin never clears it.
    async fn put_node_operation(&self, mesh: &str, row: &NodeOperationRow) -> Result<(), StorageError>;
    /// Every operation effect on the births of `mesh`, including effects from earlier cycles.
    async fn node_operations(&self, mesh: &str) -> Result<Vec<NodeOperationRow>, StorageError>;
    /// Forget one operation effect, only when its exact birth is forgotten.
    async fn remove_node_operation(&self, mesh: &str, row: &NodeOperationRow) -> Result<(), StorageError>;
    /// A blind put of the birth's silence fact in `mesh`.
    async fn put_node_silence(&self, mesh: &str, row: &NodeSilenceRow) -> Result<(), StorageError>;
    /// Every birth's silence fact in `mesh`.
    async fn node_silences(&self, mesh: &str) -> Result<Vec<NodeSilenceRow>, StorageError>;
    /// The `data-dir-lost` fact of the birth `(node_id, incarnation_id)` in `mesh` (R-FM2).
    async fn node_data_dir_lost(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeDataDirLostRow>, StorageError>;
    /// The latest `data-dir-lost` fact at the `path.name` `name` in `mesh`: a point read of the
    /// `path.name` index its put writes beside the birth's row, which a later birth at the same
    /// path.name (the one recovered over a fresh data dir) is answered from.
    async fn node_data_dir_lost_at(&self, mesh: &str, name: &PathName) -> Result<Option<NodeDataDirLostRow>, StorageError>;
    /// A blind put of the `data-dir-lost` fact `row` in `mesh` (R-FM2), and of its `path.name` index entry.
    async fn put_node_data_dir_lost(&self, mesh: &str, row: &NodeDataDirLostRow) -> Result<(), StorageError>;
    /// The `data-dir-unreadable` fact of the birth `(node_id, incarnation_id)` in `mesh` (R-FM2).
    async fn node_data_dir_unreadable(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeDataDirUnreadableRow>, StorageError>;
    /// The `data-dir-unreadable` fact at the `path.name` `name` in `mesh`: a point read of the
    /// `path.name` index its put writes beside the birth's row, which a birth the view no longer
    /// holds (deleted by its recovery, held unrecovered) is answered from.
    async fn node_data_dir_unreadable_at(&self, mesh: &str, name: &PathName) -> Result<Option<NodeDataDirUnreadableRow>, StorageError>;
    /// A blind put of the `data-dir-unreadable` fact `row` in `mesh` (R-FM2), and of its `path.name` index entry.
    async fn put_node_data_dir_unreadable(&self, mesh: &str, row: &NodeDataDirUnreadableRow) -> Result<(), StorageError>;
    /// Remove the `data-dir-unreadable` fact of the birth `(node_id, incarnation_id)` in `mesh`, and
    /// its `path.name` index entry at `name`.
    async fn remove_node_data_dir_unreadable(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId, name: &PathName) -> Result<(), StorageError>;
    /// Every birth's member cert dates in `mesh` (fabric-certs.md §Tracking).
    async fn node_certs(&self, mesh: &str) -> Result<Vec<NodeCertRow>, StorageError>;
    /// A blind put of a birth's member cert dates in `mesh`.
    async fn put_node_cert(&self, mesh: &str, row: &NodeCertRow) -> Result<(), StorageError>;
    /// Every node-admin birth's signing cert dates in `mesh`.
    async fn signing_certs(&self, mesh: &str) -> Result<Vec<SigningCertRow>, StorageError>;
    /// A blind put of a node-admin birth's signing cert dates in `mesh`.
    async fn put_signing_cert(&self, mesh: &str, row: &SigningCertRow) -> Result<(), StorageError>;
}

/// The rows of one fact family in memory, by mesh name and row key.
type MemoryFacts<T> = Mutex<BTreeMap<(String, String), T>>;

fn facts_put<T: Clone>(facts: &MemoryFacts<T>, mesh: &str, key: String, row: &T) {
    facts.lock().unwrap().insert((mesh.to_string(), key), row.clone());
}

fn facts_of<T: Clone>(facts: &MemoryFacts<T>, mesh: &str) -> Vec<T> {
    facts.lock().unwrap().iter().filter(|((m, _), _)| m == mesh).map(|(_, r)| r.clone()).collect()
}

fn facts_get<T: Clone>(facts: &MemoryFacts<T>, mesh: &str, key: String) -> Option<T> {
    facts.lock().unwrap().get(&(mesh.to_string(), key)).cloned()
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

/// Save `row` as the record at its `path.name`: the row writer reads the row at the key and claims
/// it (R13; Q109: the storage decides nothing). The key is free when no row is there, the row there is
/// the same node's, or it is another node's `Deleted` row: `row` is put whole. Any other row holds the
/// name: `NameHeld` naming its node, and nothing is written.
pub async fn claim_node_row(nodes: &dyn NodesStorage, row: &NodeRow) -> Result<Result<(), NameHeld>, StorageError> {
    let claimed = claim_against(nodes.node_row_at(&row.name).await?.as_ref(), row);
    if claimed.is_ok() {
        nodes.put_node_row(row).await?;
    }
    Ok(claimed)
}

/// Whether the row at `name` is `holder`'s. When it is, it is put again as it stands, so its id index
/// entry names `name` once this answers, as when a put ended between the row file and its entry (two
/// files): a move retried over the row its node already holds leaves no row unnamed.
pub async fn hold_node_row(nodes: &dyn NodesStorage, name: &PathName, holder: &NodeId) -> Result<bool, StorageError> {
    let Some(row) = nodes.node_row_at(name).await?.filter(|r| &r.node_id == holder) else { return Ok(false) };
    nodes.put_node_row(&row).await?;
    Ok(true)
}

/// `holder` lets go of the key `name`: its row goes when it is `holder`'s; a row of another node there
/// (the name was released before and claimed since) is left as it is. Returns whether `holder`'s row
/// went.
pub async fn release_node_row(nodes: &dyn NodesStorage, name: &PathName, holder: &NodeId) -> Result<bool, StorageError> {
    if !nodes.node_row_at(name).await?.is_some_and(|r| &r.node_id == holder) {
        return Ok(false);
    }
    nodes.remove_node_row(name).await?;
    Ok(true)
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
    declared: MemoryFacts<NodeDeclaredRow>,
    operations: MemoryFacts<NodeOperationRow>,
    silences: MemoryFacts<NodeSilenceRow>,
    data_dir_lost: MemoryFacts<NodeDataDirLostRow>,
    data_dir_lost_at: MemoryFacts<NodeDataDirLostRow>,
    data_dir_unreadable: MemoryFacts<NodeDataDirUnreadableRow>,
    data_dir_unreadable_at: MemoryFacts<NodeDataDirUnreadableRow>,
    certs: MemoryFacts<NodeCertRow>,
    signing_certs: MemoryFacts<SigningCertRow>,
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
    async fn put_node_row(&self, row: &NodeRow) -> Result<(), StorageError> {
        let mut rows = self.rows.lock().unwrap();
        let previous = rows.insert(row.name.to_string(), row.clone());
        index_put(&mut self.ids.lock().unwrap(), previous.as_ref(), row);
        let (leaving, arriving) = row.tag_moves(previous.as_ref());
        tags_move(&mut self.tags.lock().unwrap(), &row.node_id, &leaving, &arriving);
        Ok(())
    }
    async fn node_row_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError> {
        Ok(self.rows.lock().unwrap().get(&name.to_string()).cloned())
    }
    async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, StorageError> {
        Ok(self.ids.lock().unwrap().get(node_id).filter(|n| n.mesh == mesh).cloned())
    }
    async fn remove_node_row(&self, name: &PathName) -> Result<(), StorageError> {
        let mut rows = self.rows.lock().unwrap();
        let Some(removed) = rows.remove(&name.to_string()) else { return Ok(()) };
        let mut ids = self.ids.lock().unwrap();
        if ids.get(&removed.node_id) == Some(name) {
            // The node lets go of the only row it holds: its labels go with it.
            ids.remove(&removed.node_id);
            tags_move(&mut self.tags.lock().unwrap(), &removed.node_id, &removed.tags, &BTreeSet::new());
        }
        Ok(())
    }
    async fn tagged(&self, label: &str) -> Result<BTreeSet<NodeId>, StorageError> {
        tag_key(label, "memory nodes store")?;
        Ok(self.tags.lock().unwrap().get(label).cloned().unwrap_or_default())
    }
    async fn put_node_declared(&self, mesh: &str, row: &NodeDeclaredRow) -> Result<(), StorageError> {
        facts_put(&self.declared, mesh, row.key(), row);
        Ok(())
    }
    async fn remove_node_declared(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), StorageError> {
        let mut d = self.declared.lock().unwrap();
        for k in NodeDeclaredRow::keys_of_birth(node_id, incarnation_id) {
            d.remove(&(mesh.to_string(), k));
        }
        Ok(())
    }
    async fn node_declared(&self, mesh: &str) -> Result<Vec<NodeDeclaredRow>, StorageError> {
        Ok(facts_of(&self.declared, mesh))
    }
    async fn put_node_operation(&self, mesh: &str, row: &NodeOperationRow) -> Result<(), StorageError> {
        facts_put(&self.operations, mesh, row.key(), row);
        Ok(())
    }
    async fn node_operations(&self, mesh: &str) -> Result<Vec<NodeOperationRow>, StorageError> {
        Ok(facts_of(&self.operations, mesh))
    }
    async fn remove_node_operation(&self, mesh: &str, row: &NodeOperationRow) -> Result<(), StorageError> {
        self.operations.lock().unwrap().remove(&(mesh.to_string(), row.key()));
        Ok(())
    }
    async fn put_node_silence(&self, mesh: &str, row: &NodeSilenceRow) -> Result<(), StorageError> {
        facts_put(&self.silences, mesh, row.key(), row);
        Ok(())
    }
    async fn node_silences(&self, mesh: &str) -> Result<Vec<NodeSilenceRow>, StorageError> {
        Ok(facts_of(&self.silences, mesh))
    }
    async fn node_data_dir_lost(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeDataDirLostRow>, StorageError> {
        Ok(facts_get(&self.data_dir_lost, mesh, birth_key(node_id, incarnation_id)))
    }
    async fn node_data_dir_lost_at(&self, mesh: &str, name: &PathName) -> Result<Option<NodeDataDirLostRow>, StorageError> {
        Ok(facts_get(&self.data_dir_lost_at, mesh, name.to_string()))
    }
    async fn put_node_data_dir_lost(&self, mesh: &str, row: &NodeDataDirLostRow) -> Result<(), StorageError> {
        facts_put(&self.data_dir_lost, mesh, row.key(), row);
        facts_put(&self.data_dir_lost_at, mesh, row.name.to_string(), row);
        Ok(())
    }
    async fn node_data_dir_unreadable(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeDataDirUnreadableRow>, StorageError> {
        Ok(facts_get(&self.data_dir_unreadable, mesh, birth_key(node_id, incarnation_id)))
    }
    async fn node_data_dir_unreadable_at(&self, mesh: &str, name: &PathName) -> Result<Option<NodeDataDirUnreadableRow>, StorageError> {
        Ok(facts_get(&self.data_dir_unreadable_at, mesh, name.to_string()))
    }
    async fn put_node_data_dir_unreadable(&self, mesh: &str, row: &NodeDataDirUnreadableRow) -> Result<(), StorageError> {
        facts_put(&self.data_dir_unreadable, mesh, row.key(), row);
        facts_put(&self.data_dir_unreadable_at, mesh, row.name.to_string(), row);
        Ok(())
    }
    async fn remove_node_data_dir_unreadable(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId, name: &PathName) -> Result<(), StorageError> {
        self.data_dir_unreadable.lock().unwrap().remove(&(mesh.to_string(), birth_key(node_id, incarnation_id)));
        self.data_dir_unreadable_at.lock().unwrap().remove(&(mesh.to_string(), name.to_string()));
        Ok(())
    }
    async fn node_certs(&self, mesh: &str) -> Result<Vec<NodeCertRow>, StorageError> {
        Ok(facts_of(&self.certs, mesh))
    }
    async fn put_node_cert(&self, mesh: &str, row: &NodeCertRow) -> Result<(), StorageError> {
        facts_put(&self.certs, mesh, row.key(), row);
        Ok(())
    }
    async fn signing_certs(&self, mesh: &str) -> Result<Vec<SigningCertRow>, StorageError> {
        Ok(facts_of(&self.signing_certs, mesh))
    }
    async fn put_signing_cert(&self, mesh: &str, row: &SigningCertRow) -> Result<(), StorageError> {
        facts_put(&self.signing_certs, mesh, row.key(), row);
        Ok(())
    }
}

#[cfg(feature = "file-backend")]
const NODE_FORMAT: &str = "node-record/4";
#[cfg(feature = "file-backend")]
const RUNTIME_FORMAT: &str = "node-runtime/1";
#[cfg(feature = "file-backend")]
const ROW_FORMAT: &str = "node-row/1";
#[cfg(feature = "file-backend")]
const ROW_ID_FORMAT: &str = "node-row-id/1";
#[cfg(feature = "file-backend")]
const TAG_FORMAT: &str = "node-tag/1";
#[cfg(feature = "file-backend")]
const NODE_DECLARED_FORMAT: &str = "node-declared/3";
#[cfg(feature = "file-backend")]
const NODE_OPERATION_FORMAT: &str = "node-operation/1";
#[cfg(feature = "file-backend")]
const NODE_SILENCE_FORMAT: &str = "node-silence/1";
#[cfg(feature = "file-backend")]
const NODE_DATA_DIR_LOST_FORMAT: &str = "node-data-dir-lost/1";
#[cfg(feature = "file-backend")]
const NODE_DATA_DIR_UNREADABLE_FORMAT: &str = "node-data-dir-unreadable/1";
#[cfg(feature = "file-backend")]
const NODE_CERT_FORMAT: &str = "node-cert/2";
#[cfg(feature = "file-backend")]
const SIGNING_CERT_FORMAT: &str = "signing-cert/1";

#[cfg(feature = "file-backend")]
/// One row of the node row's tags index: the node carries the label. Its key is the node id, in the
/// label's own directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TagRow {
    label: String,
    node_id: NodeId,
}

#[cfg(feature = "file-backend")]
/// One entry of the node row's id index: the `path.name` of the row its node holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RowId {
    node_id: NodeId,
    name: PathName,
}

#[cfg(feature = "file-backend")]
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
    /// The births' declared-state facts, `declared/<node_id>--<incarnation>--<state>.json`: opened
    /// straight to the disk, never held in the scratchpad, so an Applied declaration is durable before
    /// state-commit (Q54).
    declared: FileRecords,
    /// The births' operation effects, `operations/<birth>--<build>--<attempt>--<operation>.json`.
    operations: FileRecords,
    /// The births' silence facts, `silence/<birth>.json`.
    silence: FileRecords,
    /// The births' `data-dir-lost` facts and their `path.name` index (R-FM2).
    data_dir_lost: FileRecords,
    data_dir_lost_at: FileRecords,
    /// The births' `data-dir-unreadable` facts and their `path.name` index (R-FM2).
    data_dir_unreadable: FileRecords,
    data_dir_unreadable_at: FileRecords,
    /// The births' member cert dates, `cert/<birth>.json`.
    certs: FileRecords,
    /// The node-admin births' signing cert dates, `signing-cert/<birth>.json`.
    signing_certs: FileRecords,
    root: crate::store_root::StoreRoot,
    base: std::path::PathBuf,
    writer: IncarnationId,
    /// Each label's rows of the tags index this store has read or written, opened on first use.
    labels: Mutex<BTreeMap<String, FileRecords>>,
}

#[cfg(feature = "file-backend")]
impl MeshNodes {
    async fn open(root: &crate::store_root::StoreRoot, mesh: &str, writer: &IncarnationId) -> Result<Self, StorageError> {
        let base = root.nodes(mesh);
        Ok(Self {
            rows: root.records(&base, "rows", writer).await?,
            ids: root.records(&base, "ids", writer).await?,
            contacts: root.records(&base, "contacts", writer).await?,
            declared: FileRecords::open(&base, "declared", writer).await?,
            operations: root.records(&base, "operations", writer).await?,
            silence: root.records(&base, "silence", writer).await?,
            data_dir_lost: root.records(&base, "data-dir-lost", writer).await?,
            data_dir_lost_at: root.records(&base, "data-dir-lost-at", writer).await?,
            data_dir_unreadable: root.records(&base, "data-dir-unreadable", writer).await?,
            data_dir_unreadable_at: root.records(&base, "data-dir-unreadable-at", writer).await?,
            certs: root.records(&base, "cert", writer).await?,
            signing_certs: root.records(&base, "signing-cert", writer).await?,
            root: root.clone(),
            base,
            writer: writer.clone(),
            labels: Mutex::new(BTreeMap::new()),
        })
    }

    /// Every store of this mesh's rows opened so far: rows, ids, contacts and each label's rows.
    fn stores(&self) -> Vec<FileRecords> {
        let mut all = vec![
            self.rows.clone(),
            self.ids.clone(),
            self.contacts.clone(),
            self.declared.clone(),
            self.operations.clone(),
            self.silence.clone(),
            self.data_dir_lost.clone(),
            self.data_dir_lost_at.clone(),
            self.data_dir_unreadable.clone(),
            self.data_dir_unreadable_at.clone(),
            self.certs.clone(),
            self.signing_certs.clone(),
        ];
        all.extend(self.labels.lock().unwrap_or_else(|e| e.into_inner()).values().cloned());
        all
    }

    /// `label`'s rows of the tags index, written: opened on first use (its directory made).
    async fn label(&self, label: &str) -> Result<FileRecords, StorageError> {
        tag_key(label, &self.base.display().to_string())?;
        if let Some(l) = self.labels.lock().unwrap_or_else(|e| e.into_inner()).get(label) {
            return Ok(l.clone());
        }
        let opened = self.root.records(&self.base, &format!("tags/{label}"), &self.writer).await?;
        let mut labels = self.labels.lock().unwrap_or_else(|e| e.into_inner());
        Ok(labels.entry(label.to_string()).or_insert(opened).clone())
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
    /// the key, or a move retried over it ([`hold_node_row`]), names it.
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

#[cfg(feature = "file-backend")]
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
    /// Every write of a row is one at a time: a row and its index entries are written together.
    row_writes: tokio::sync::Mutex<()>,
}

#[cfg(feature = "file-backend")]
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
            row_writes: tokio::sync::Mutex::new(()),
        })
    }

    /// The admin's own row in `own_data_dir`, read before the store opens: a restarting admin's row
    /// names the Fabric whose store root it opens and the mesh whose rows it opens on.
    pub async fn own_in(own_data_dir: &Path, writer: &IncarnationId) -> Result<Option<NodeRecord>, StorageError> {
        FileRecords::open(own_data_dir, "nodes", writer).await?.read("self", NODE_FORMAT).await
    }

    /// Finishes every write of the Node rows, their indexes and contacts this store has issued, its own
    /// mesh's and every other mesh's it opened (node-primary-update.md step 3).
    pub async fn finish_issued_writes(&self) {
        let opened: Vec<std::sync::Arc<MeshNodes>> = std::iter::once(self.mine.1.clone()).chain(self.others.lock().unwrap_or_else(|e| e.into_inner()).values().cloned()).collect();
        for s in opened.iter().flat_map(|m| m.stores()) {
            s.finish_issued_writes().await;
        }
    }

    /// The rows of `mesh` on the shared root: a mesh this store has not opened yet is opened on the
    /// blocking pool, outside the lock, and kept once under it.
    async fn mesh(&self, mesh: &str) -> Result<std::sync::Arc<MeshNodes>, StorageError> {
        if mesh == self.mine.0 {
            return Ok(self.mine.1.clone());
        }
        if let Some(m) = self.others.lock().unwrap_or_else(|e| e.into_inner()).get(mesh) {
            return Ok(m.clone());
        }
        let opened = std::sync::Arc::new(MeshNodes::open(&self.root, mesh, &self.writer).await?);
        let mut others = self.others.lock().unwrap_or_else(|e| e.into_inner());
        Ok(others.entry(mesh.to_string()).or_insert(opened).clone())
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

#[cfg(feature = "file-backend")]
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
    async fn put_node_row(&self, row: &NodeRow) -> Result<(), StorageError> {
        // One row write at a time: the row and its index entries are written together.
        let _one = self.row_writes.lock().await;
        let key = row.name.to_string();
        let rows = self.mesh(&row.name.mesh).await?;
        let previous = rows.rows.read::<NodeRow>(&key, ROW_FORMAT).await?;
        let (leaving, arriving) = row.tag_moves(previous.as_ref());
        rows.tags_leave(&row.node_id, &leaving).await?;
        rows.rows.write(&key, ROW_FORMAT, row).await?;
        rows.index_put(previous.as_ref(), row).await?;
        rows.tags_arrive(&row.node_id, &arriving).await
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
    async fn remove_node_row(&self, name: &PathName) -> Result<(), StorageError> {
        let _one = self.row_writes.lock().await;
        let key = name.to_string();
        let rows = self.mesh(&name.mesh).await?;
        let Some(r) = rows.rows.read::<NodeRow>(&key, ROW_FORMAT).await? else { return Ok(()) };
        // The node lets go of the only row it holds (its id index entry names this key): its labels
        // go with it, before the row does.
        if rows.ids.read::<RowId>(r.node_id.as_str(), ROW_ID_FORMAT).await?.is_some_and(|e| &e.name == name) {
            rows.tags_leave(&r.node_id, &r.tags).await?;
        }
        rows.rows.remove(&key).await?;
        rows.index_drop(&r.node_id, name).await
    }
    async fn tagged(&self, label: &str) -> Result<BTreeSet<NodeId>, StorageError> {
        tag_key(label, &self.root.dir().join("nodes").display().to_string())?;
        let mut all = BTreeSet::new();
        for mesh in self.meshes_on_root().await? {
            all.extend(self.mesh(&mesh).await?.tagged(label).await?);
        }
        Ok(all)
    }
    async fn put_node_declared(&self, mesh: &str, row: &NodeDeclaredRow) -> Result<(), StorageError> {
        let store = self.mesh(mesh).await?;
        let span = tracing::info_span!("rdm.node_admin.status.update.via-declared-put",
            node_id = %row.node_id, incarnation_id = %row.incarnation_id.as_str(), state = ?row.state,
            writer = %self.writer.as_str(), directory = %store.base.join("declared").display(),
            holding = store.declared.holding(), outcome = tracing::field::Empty);
        let result = tracing::Instrument::instrument(store.declared.write(&row.key(), NODE_DECLARED_FORMAT, row), span.clone()).await;
        span.record("outcome", if result.is_ok() { "acknowledged" } else { "refused" });
        result
    }
    async fn remove_node_declared(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), StorageError> {
        let store = self.mesh(mesh).await?;
        for k in NodeDeclaredRow::keys_of_birth(node_id, incarnation_id) {
            store.declared.remove(&k).await?;
        }
        Ok(())
    }
    async fn node_declared(&self, mesh: &str) -> Result<Vec<NodeDeclaredRow>, StorageError> {
        self.mesh(mesh).await?.declared.list(NODE_DECLARED_FORMAT).await
    }
    async fn put_node_operation(&self, mesh: &str, row: &NodeOperationRow) -> Result<(), StorageError> {
        self.mesh(mesh).await?.operations.write(&row.key(), NODE_OPERATION_FORMAT, row).await
    }
    async fn node_operations(&self, mesh: &str) -> Result<Vec<NodeOperationRow>, StorageError> {
        self.mesh(mesh).await?.operations.list(NODE_OPERATION_FORMAT).await
    }
    async fn remove_node_operation(&self, mesh: &str, row: &NodeOperationRow) -> Result<(), StorageError> {
        self.mesh(mesh).await?.operations.remove(&row.key()).await.map(|_| ())
    }
    async fn put_node_silence(&self, mesh: &str, row: &NodeSilenceRow) -> Result<(), StorageError> {
        self.mesh(mesh).await?.silence.write(&row.key(), NODE_SILENCE_FORMAT, row).await
    }
    async fn node_silences(&self, mesh: &str) -> Result<Vec<NodeSilenceRow>, StorageError> {
        self.mesh(mesh).await?.silence.list(NODE_SILENCE_FORMAT).await
    }
    async fn node_data_dir_lost(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeDataDirLostRow>, StorageError> {
        self.mesh(mesh).await?.data_dir_lost.read(&birth_key(node_id, incarnation_id), NODE_DATA_DIR_LOST_FORMAT).await
    }
    async fn node_data_dir_lost_at(&self, mesh: &str, name: &PathName) -> Result<Option<NodeDataDirLostRow>, StorageError> {
        self.mesh(mesh).await?.data_dir_lost_at.read(&name.to_string(), NODE_DATA_DIR_LOST_FORMAT).await
    }
    async fn put_node_data_dir_lost(&self, mesh: &str, row: &NodeDataDirLostRow) -> Result<(), StorageError> {
        // The birth's row, then its path.name index entry: two keys, each a blind put of its own.
        let store = self.mesh(mesh).await?;
        store.data_dir_lost.write(&row.key(), NODE_DATA_DIR_LOST_FORMAT, row).await?;
        store.data_dir_lost_at.write(&row.name.to_string(), NODE_DATA_DIR_LOST_FORMAT, row).await
    }
    async fn node_data_dir_unreadable(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<NodeDataDirUnreadableRow>, StorageError> {
        self.mesh(mesh).await?.data_dir_unreadable.read(&birth_key(node_id, incarnation_id), NODE_DATA_DIR_UNREADABLE_FORMAT).await
    }
    async fn node_data_dir_unreadable_at(&self, mesh: &str, name: &PathName) -> Result<Option<NodeDataDirUnreadableRow>, StorageError> {
        self.mesh(mesh).await?.data_dir_unreadable_at.read(&name.to_string(), NODE_DATA_DIR_UNREADABLE_FORMAT).await
    }
    async fn put_node_data_dir_unreadable(&self, mesh: &str, row: &NodeDataDirUnreadableRow) -> Result<(), StorageError> {
        // The birth's row, then its path.name index entry: two keys, each a blind put of its own.
        let store = self.mesh(mesh).await?;
        store.data_dir_unreadable.write(&row.key(), NODE_DATA_DIR_UNREADABLE_FORMAT, row).await?;
        store.data_dir_unreadable_at.write(&row.name.to_string(), NODE_DATA_DIR_UNREADABLE_FORMAT, row).await
    }
    async fn remove_node_data_dir_unreadable(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId, name: &PathName) -> Result<(), StorageError> {
        // Tombstones of the birth's key and of its path.name index entry, each blind.
        let store = self.mesh(mesh).await?;
        store.data_dir_unreadable.remove(&birth_key(node_id, incarnation_id)).await?;
        store.data_dir_unreadable_at.remove(&name.to_string()).await.map(|_| ())
    }
    async fn node_certs(&self, mesh: &str) -> Result<Vec<NodeCertRow>, StorageError> {
        self.mesh(mesh).await?.certs.list(NODE_CERT_FORMAT).await
    }
    async fn put_node_cert(&self, mesh: &str, row: &NodeCertRow) -> Result<(), StorageError> {
        self.mesh(mesh).await?.certs.write(&row.key(), NODE_CERT_FORMAT, row).await
    }
    async fn signing_certs(&self, mesh: &str) -> Result<Vec<SigningCertRow>, StorageError> {
        self.mesh(mesh).await?.signing_certs.list(SIGNING_CERT_FORMAT).await
    }
    async fn put_signing_cert(&self, mesh: &str, row: &SigningCertRow) -> Result<(), StorageError> {
        self.mesh(mesh).await?.signing_certs.write(&row.key(), SIGNING_CERT_FORMAT, row).await
    }
}

// ---------------------------------------------------------------- connections.storage

pub use rdm_node::storage::{ConnectionsStorage, MemoryConnectionsStorage};
#[cfg(feature = "file-backend")]
pub use rdm_node::storage::FileConnectionsStorage;

#[cfg(all(test, feature = "file-backend"))]
mod tests {
    use super::*;

    /// The same script over any nodes store: two facts of one birth under different keys both stand
    /// across a reopen, whatever order they land in (storage-by-object.md, "Keys, writers, fold"): a
    /// late `Pending` beside `ReadyForTraffic`, two attempts' operation effects, a lost and an unreadable
    /// data dir; the birth's member cert dates are one row, and a cycle reset removes its declared facts
    /// and keeps its operation effects.
    async fn two_facts_of_one_birth_both_stand(first: &dyn NodesStorage, reopened: &dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Box<dyn NodesStorage>> + Send>>) {
        let (node_id, incarnation_id, name): (NodeId, IncarnationId, PathName) = (NodeId::mint(), IncarnationId::mint(), "mesh1.rpc.1".parse().unwrap());
        let declared = |state| NodeDeclaredRow { node_id: node_id.clone(), incarnation_id: incarnation_id.clone(), name: name.clone(), state };
        first.put_node_declared("mesh1", &declared(NodeState::ReadyForTraffic)).await.unwrap();
        first.put_node_declared("mesh1", &declared(NodeState::Pending)).await.unwrap();
        let effect = |attempt| NodeOperationRow { node_id: node_id.clone(), incarnation_id: incarnation_id.clone(), name: name.clone(), state: NodeState::Stopped, operation: rdm_core::status::DeclaredOperation { build_id: "b".into(), attempt, operation: "node.stop:mesh1.rpc.1".into() } };
        first.put_node_operation("mesh1", &effect(1)).await.unwrap();
        first.put_node_operation("mesh1", &effect(2)).await.unwrap();
        first.put_node_data_dir_lost("mesh1", &NodeDataDirLostRow { node_id: node_id.clone(), name: name.clone(), incarnation_id: incarnation_id.clone(), path: "/d".into(), since: 1 }).await.unwrap();
        first.put_node_data_dir_unreadable("mesh1", &NodeDataDirUnreadableRow { node_id: node_id.clone(), name: name.clone(), incarnation_id: incarnation_id.clone(), path: "/d".into(), cause: "node-key: not hex".into() }).await.unwrap();
        let cert = |issued_at: u64| NodeCertRow { node_id: node_id.clone(), name: name.clone(), incarnation_id: incarnation_id.clone(), issued_at, expires_at: issued_at + 10, issuer_expires_at: 100, renew_at: issued_at + 6 };
        first.put_node_cert("mesh1", &cert(1)).await.unwrap();
        first.put_node_cert("mesh1", &cert(20)).await.unwrap();

        let store = reopened().await;
        let folded = fold_declared(store.node_declared("mesh1").await.unwrap());
        assert_eq!(folded.get(&(node_id.clone(), incarnation_id.clone())), Some(&NodeState::ReadyForTraffic), "a late Pending replaces no ReadyForTraffic: the fold reads both");
        assert_eq!(store.node_declared("mesh1").await.unwrap().len(), 2, "two states, two facts");
        assert_eq!(store.node_operations("mesh1").await.unwrap().len(), 2, "two attempts' effects both stand");
        assert!(store.node_data_dir_lost("mesh1", &node_id, &incarnation_id).await.unwrap().is_some() && store.node_data_dir_unreadable("mesh1", &node_id, &incarnation_id).await.unwrap().is_some(), "a lost and an unreadable data dir both stand");
        assert_eq!(store.node_certs("mesh1").await.unwrap(), vec![cert(20)], "the birth's cert dates are one row: the last written");
        assert!(store.node_declared("mesh2").await.unwrap().is_empty(), "facts are on the birth's own mesh");
        store.remove_node_declared("mesh1", &node_id, &incarnation_id).await.unwrap();
        assert!(store.node_declared("mesh1").await.unwrap().is_empty(), "a cycle reset removes the birth's declared facts");
        assert_eq!(store.node_operations("mesh1").await.unwrap().len(), 2, "and keeps its operation effects");
    }

    /// CONTRACT (storage-by-object.md, "Keys, writers, fold"; Q49, Q141): on the memory and the file
    /// backends alike, every fact of a birth is its own keyed row under the birth's mesh in
    /// `nodes.storage`, so no later or delayed write erases an independent fact; the file backend's
    /// survive a reopen by another writer. What must NOT happen: a late Pending replacing
    /// ReadyForTraffic, one attempt's effect erasing another's, two cert rows for one birth.
    #[tokio::test]
    async fn two_facts_of_one_birth_under_different_keys_both_stand_on_either_backend() {
        let memory = std::sync::Arc::new(MemoryNodesStorage::default());
        let held = memory.clone();
        two_facts_of_one_birth_both_stand(&*memory, &move || {
            let held = held.clone();
            Box::pin(async move { Box::new(SharedNodes(held)) as Box<dyn NodesStorage> })
        })
        .await;
        let dir = crate::test_support::tempdir("facts-of-one-birth");
        let root = crate::store_root::StoreRoot::at(&dir);
        let data = dir.join("data");
        let first = FileNodesStorage::open(&data, &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        two_facts_of_one_birth_both_stand(&first, &move || {
            let (root, data) = (root.clone(), data.clone());
            Box::pin(async move { Box::new(FileNodesStorage::open(&data, &root, "mesh1", &IncarnationId::mint()).await.unwrap()) as Box<dyn NodesStorage> })
        })
        .await;
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
        async fn own(&self) -> Result<Option<crate::storage::NodeRecord>, crate::storage_error::StorageError> {
            self.0.own().await
        }
        async fn put_own(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_own(record).await
        }
        async fn contacts(&self) -> Result<Vec<crate::storage::NodeRecord>, crate::storage_error::StorageError> {
            self.0.contacts().await
        }
        async fn put_contact(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_contact(record).await
        }
        async fn remove_contact(&self, mesh: &str, node_id: &crate::model::NodeId) -> Result<(), crate::storage_error::StorageError> {
            self.0.remove_contact(mesh, node_id).await
        }
        async fn put_node_row(&self, row: &crate::storage::NodeRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_node_row(row).await
        }
        async fn node_row_at(&self, name: &crate::model::PathName) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.0.node_row_at(name).await
        }
        async fn node_row_name(&self, mesh: &str, node_id: &crate::model::NodeId) -> Result<Option<crate::model::PathName>, crate::storage_error::StorageError> {
            self.0.node_row_name(mesh, node_id).await
        }
        async fn remove_node_row(&self, name: &crate::model::PathName) -> Result<(), crate::storage_error::StorageError> {
            self.0.remove_node_row(name).await
        }
        async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<crate::model::NodeId>, crate::storage_error::StorageError> {
            self.0.tagged(label).await
        }
        async fn put_node_declared(&self, mesh: &str, row: &crate::storage::NodeDeclaredRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_node_declared(mesh, row).await
        }
        async fn remove_node_declared(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<(), crate::storage_error::StorageError> {
            self.0.remove_node_declared(mesh, node_id, incarnation_id).await
        }
        async fn node_declared(&self, mesh: &str) -> Result<Vec<crate::storage::NodeDeclaredRow>, crate::storage_error::StorageError> {
            self.0.node_declared(mesh).await
        }
        async fn put_node_operation(&self, mesh: &str, row: &crate::storage::NodeOperationRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_node_operation(mesh, row).await
        }
        async fn node_operations(&self, mesh: &str) -> Result<Vec<crate::storage::NodeOperationRow>, crate::storage_error::StorageError> {
            self.0.node_operations(mesh).await
        }
        async fn remove_node_operation(&self, mesh: &str, row: &crate::storage::NodeOperationRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.remove_node_operation(mesh, row).await
        }
        async fn put_node_silence(&self, mesh: &str, row: &crate::storage::NodeSilenceRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_node_silence(mesh, row).await
        }
        async fn node_silences(&self, mesh: &str) -> Result<Vec<crate::storage::NodeSilenceRow>, crate::storage_error::StorageError> {
            self.0.node_silences(mesh).await
        }
        async fn node_data_dir_lost(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::storage::NodeDataDirLostRow>, crate::storage_error::StorageError> {
            self.0.node_data_dir_lost(mesh, node_id, incarnation_id).await
        }
        async fn node_data_dir_lost_at(&self, mesh: &str, name: &crate::model::PathName) -> Result<Option<crate::storage::NodeDataDirLostRow>, crate::storage_error::StorageError> {
            self.0.node_data_dir_lost_at(mesh, name).await
        }
        async fn put_node_data_dir_lost(&self, mesh: &str, row: &crate::storage::NodeDataDirLostRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_node_data_dir_lost(mesh, row).await
        }
        async fn node_data_dir_unreadable(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::storage::NodeDataDirUnreadableRow>, crate::storage_error::StorageError> {
            self.0.node_data_dir_unreadable(mesh, node_id, incarnation_id).await
        }
        async fn node_data_dir_unreadable_at(&self, mesh: &str, name: &crate::model::PathName) -> Result<Option<crate::storage::NodeDataDirUnreadableRow>, crate::storage_error::StorageError> {
            self.0.node_data_dir_unreadable_at(mesh, name).await
        }
        async fn put_node_data_dir_unreadable(&self, mesh: &str, row: &crate::storage::NodeDataDirUnreadableRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_node_data_dir_unreadable(mesh, row).await
        }
        async fn remove_node_data_dir_unreadable(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId, name: &crate::model::PathName) -> Result<(), crate::storage_error::StorageError> {
            self.0.remove_node_data_dir_unreadable(mesh, node_id, incarnation_id, name).await
        }
        async fn node_certs(&self, mesh: &str) -> Result<Vec<crate::storage::NodeCertRow>, crate::storage_error::StorageError> {
            self.0.node_certs(mesh).await
        }
        async fn put_node_cert(&self, mesh: &str, row: &crate::storage::NodeCertRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_node_cert(mesh, row).await
        }
        async fn signing_certs(&self, mesh: &str) -> Result<Vec<crate::storage::SigningCertRow>, crate::storage_error::StorageError> {
            self.0.signing_certs(mesh).await
        }
        async fn put_signing_cert(&self, mesh: &str, row: &crate::storage::SigningCertRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_signing_cert(mesh, row).await
        }
    }
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
            is_primary: None,
            is_fabric_primary: None,
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
        let d = crate::test_support::tempdir("storage");
        let mesh = MeshRecord { mesh_id: MeshId::mint(), name: "mesh1".into(), dead: false };
        let (own, peer) = (node("mesh1.admin.1"), node("mesh1.rpc.1"));
        let (up, down) = (fact(ConnectionState::Connected), fact(ConnectionState::Disconnected));
        let check = async |m: &dyn MeshStorage, n: &dyn NodesStorage, c: &dyn ConnectionsStorage, fresh: bool| {
            if fresh {
                assert_eq!(m.mesh("mesh1").await.unwrap(), None);
                assert_eq!(n.own().await.unwrap(), None);
                assert_eq!(crate::storage::claim_mesh(&*m, &mesh).await.unwrap(), Ok(MeshClaim::Written { over: None }));
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
        NodeRow { name: name.parse().unwrap(), node_id: NodeId::mint(), status: crate::model::NodeStatus::ReadyForTraffic, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None }
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
        let d = crate::test_support::tempdir("row-id-index");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let file = FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        let mem = MemoryNodesStorage::default();
        for n in [&file as &dyn NodesStorage, &mem as &dyn NodesStorage] {
            let a = row("mesh1.rpc.1");
            assert_eq!(n.node_row_name("mesh1", &a.node_id).await.unwrap(), None, "a node that never held a row has no entry");
            n.put_node_row(&a).await.unwrap();
            assert_eq!(n.node_row_name("mesh1", &a.node_id).await.unwrap(), Some(a.name.clone()));
            let old = NodeRow { name: a.name.renamed(), ..a.clone() };
            n.put_node_row(&old).await.unwrap();
            assert!(crate::storage::release_node_row(&*n, &a.name, &a.node_id).await.unwrap());
            assert_eq!(n.node_row_name("mesh1", &a.node_id).await.unwrap(), Some(old.name.clone()), "a move leaves the entry naming the new key");
            assert!(crate::storage::release_node_row(&*n, &old.name, &a.node_id).await.unwrap());
            assert_eq!(n.node_row_name("mesh1", &a.node_id).await.unwrap(), None, "a release drops the entry");
            // A recovery: the exited node's Deleted row is put over by the new node at its name.
            let gone = NodeRow { status: crate::model::NodeStatus::Deleted, ..row("mesh1.rpc.2") };
            n.put_node_row(&gone).await.unwrap();
            let new = NodeRow { node_id: NodeId::mint(), ..row("mesh1.rpc.2") };
            n.put_node_row(&new).await.unwrap();
            assert_eq!((n.node_row_name("mesh1", &gone.node_id).await.unwrap(), n.node_row_name("mesh1", &new.node_id).await.unwrap()), (None, Some(new.name.clone())), "the name taken over is the new node's alone");
            // A put refused by another node's live row at the key.
            let over = NodeRow { node_id: NodeId::mint(), ..new.clone() };
            assert_eq!(crate::storage::claim_node_row(&*n, &over).await.unwrap(), Err(NameHeld { name: new.name.clone(), by: new.node_id.clone() }));
            assert_eq!((n.node_row_name("mesh1", &new.node_id).await.unwrap(), n.node_row_name("mesh1", &over.node_id).await.unwrap()), (Some(new.name.clone()), None), "a refused put names nothing and leaves the holder's entry");
        }
        assert_eq!(keys(&root.nodes("mesh1").join("ids")).len(), 1, "on disk, one entry per node that holds a row");
        // A put of the row a node already holds at its key (a status change: the Deleted row of a
        // delete) writes the row and nothing of the index: its entry already names the key, and a
        // durable write that changes nothing is a second fsync on every such put.
        let held = row("mesh1.rpc.4");
        file.put_node_row(&held).await.unwrap();
        let entry = root.nodes("mesh1").join("ids").join(format!("{}.json", held.node_id));
        let inode = |p: &std::path::Path| std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(p).unwrap());
        let before = inode(&entry);
        file.put_node_row(&NodeRow { status: crate::model::NodeStatus::Deleted, ..held.clone() }).await.unwrap();
        assert_eq!(inode(&entry), before, "the entry naming the key is not written again");
        assert_eq!(file.node_row_name("mesh1", &held.node_id).await.unwrap(), Some(held.name.clone()));
        // A write that ended between the row and its entry (two files): the move retried over the row
        // the node holds there leaves it as it is and names it, so no row is left unnamed.
        let torn = row("mesh1.rpc.3");
        std::fs::write(root.nodes("mesh1").join("rows").join("mesh1.rpc.3.json"), serde_json::to_vec(&serde_json::json!({"format": ROW_FORMAT, "record": torn})).unwrap()).unwrap();
        assert_eq!(file.node_row_name("mesh1", &torn.node_id).await.unwrap(), None);
        assert!(!crate::storage::hold_node_row(&file, &torn.name, &NodeId::mint()).await.unwrap(), "another node holds no row there");
        assert!(crate::storage::hold_node_row(&file, &torn.name, &torn.node_id).await.unwrap());
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
        let d = crate::test_support::tempdir("row-id-torn-move");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let file = FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        let from = row("mesh1.rpc.1");
        file.put_node_row(&from).await.unwrap();
        let to = NodeRow { name: from.name.renamed(), ..from.clone() };
        // The first move's put wrote the row at `to` and ended before its entry.
        std::fs::write(root.nodes("mesh1").join("rows").join(format!("{}.json", to.name)), serde_json::to_vec(&serde_json::json!({"format": ROW_FORMAT, "record": to})).unwrap()).unwrap();
        let mut record = crate::model::Node::allocated(from.name.clone());
        record.node_id = from.node_id.clone();
        record.status = from.status;
        crate::node_rows::move_row(&file, &"mesh1.admin.1".parse().unwrap(), &record, BTreeMap::new(), Default::default(), None, None, &from.name, &to.name).await.unwrap();
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
        let d = crate::test_support::tempdir("shared-root");
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
            crate::storage::claim_mesh(&*m, &MeshRecord { mesh_id: MeshId::mint(), name: mesh.into(), dead: false }).await.unwrap().expect("each mesh claims a name no mesh holds");
            for name in [format!("{mesh}.admin.1"), format!("{mesh}.rpc.1"), format!("{mesh}.rpc.2")] {
                n.put_node_row(&row(&name)).await.unwrap();
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
    #[tokio::test]
    async fn the_tags_index_is_one_file_per_label_and_node_kept_with_every_row_write_and_read_per_label() {
        let d = crate::test_support::tempdir("tags-index");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = crate::store_root::StoreRoot::open(&store, &crate::model::FabricId::mint()).unwrap();
        let (n1, n2) = (FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap(), FileNodesStorage::open(&d.join("mesh2.admin.1"), &root, "mesh2", &IncarnationId::mint()).await.unwrap());
        root.scratchpad().expect("a root without the Fabric row opens with a scratchpad").publish().await.unwrap();
        let tags = |l: &[&str]| l.iter().map(|t| t.to_string()).collect::<BTreeSet<String>>();
        let (a, b) = (NodeId::mint(), NodeId::mint());
        let row_a = NodeRow { tags: tags(&["rabbit", "red"]), node_id: a.clone(), ..row("mesh1.rpc.1") };
        n1.put_node_row(&row_a).await.unwrap();
        let row_b = NodeRow { name: "mesh2.rpc.1".parse().unwrap(), node_id: b.clone(), tags: tags(&["red"]), ..row("mesh2.rpc.1") };
        n2.put_node_row(&row_b).await.unwrap();
        let files = |mesh: &str, label: &str| keys(&root.dir().join("nodes").join(mesh).join("tags").join(label));
        assert_eq!((files("mesh1", "rabbit"), files("mesh1", "red"), files("mesh2", "red")), (vec![format!("{a}.json")], vec![format!("{a}.json")], vec![format!("{b}.json")]), "one file per (label, node id), under the node's mesh");
        let other = FileNodesStorage::open(&d.join("mesh1.admin.2"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        for n in [&n1, &n2, &other] {
            assert_eq!(n.tagged("red").await.unwrap(), [a.clone(), b.clone()].into(), "one label read answers every mesh's nodes carrying it");
        }
        // A put without rabbit tombstones rabbit's row only.
        n1.put_node_row(&NodeRow { tags: tags(&["red"]), ..row_a.clone() }).await.unwrap();
        assert_eq!((files("mesh1", "rabbit").len(), other.tagged("rabbit").await.unwrap().len(), other.tagged("red").await.unwrap().len()), (0, 0, 2), "the dropped label's row is tombstoned, the kept one stands");
        // A rename keeps the labels: put at the new name, then the old key released.
        let moved = NodeRow { name: "mesh1.rpc.1.old".parse().unwrap(), tags: tags(&["red"]), ..row_a.clone() };
        n1.put_node_row(&moved).await.unwrap();
        assert!(crate::storage::release_node_row(&n1, &row_a.name, &a).await.unwrap());
        assert_eq!(other.tagged("red").await.unwrap(), [a.clone(), b.clone()].into(), "a renamed node keeps its labels");
        // A Deleted row tombstones every label it carries; a release of a node's only row its labels.
        n1.put_node_row(&NodeRow { status: crate::model::NodeStatus::Deleted, ..moved.clone() }).await.unwrap();
        assert!(crate::storage::release_node_row(&n2, &row_b.name, &b).await.unwrap());
        assert!(other.tagged("red").await.unwrap().is_empty(), "neither the deleted node nor the released one is named under red");
        assert_eq!(n1.node_row_at(&moved.name).await.unwrap().map(|r| r.tags), Some(tags(&["red"])), "the Deleted row keeps its tags");
        // A label no row carried: none, and no directory made.
        assert!(other.tagged("blue").await.unwrap().is_empty());
        assert!(!root.dir().join("nodes").join("mesh1").join("tags").join("blue").exists(), "a read makes no directory");
        assert!(matches!(other.tagged("../rows").await, Err(StorageError::Unrecognised { reason, .. }) if reason.contains("\"../rows\"")), "a key that is not a label is refused by name");
    }

    /// CONTRACT (node-replace.md Inherited: the successor inherits the retained predecessor row's tags,
    /// "Retained predecessor labels are removed from the active index, not erased from its row"):
    /// between the successor's claim at the name and the retained row's mark, both nodes carry the label;
    /// the retained row put again with `inherited_by` keeps its tags and the tags index names none of
    /// them for it; a later put at the same key carrying the field (a tags or config write) indexes
    /// none again, an added label included, while the successor stays named.
    #[tokio::test]
    async fn a_row_a_successor_inherited_keeps_its_tags_and_holds_none_in_the_tags_index() {
        let d = crate::test_support::tempdir("tags-inherited");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = crate::store_root::StoreRoot::open(&store, &crate::model::FabricId::mint()).unwrap();
        let n1 = FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        root.scratchpad().expect("a root without the Fabric row opens with a scratchpad").publish().await.unwrap();
        let tags = |l: &[&str]| l.iter().map(|t| t.to_string()).collect::<BTreeSet<String>>();
        let (pred, succ) = (NodeId::mint(), NodeId::mint());
        let old = NodeRow { node_id: pred.clone(), tags: tags(&["rabbit"]), ..row("mesh1.rpc.1.old") };
        n1.put_node_row(&old).await.unwrap();
        n1.put_node_row(&NodeRow { node_id: succ.clone(), tags: tags(&["rabbit"]), ..row("mesh1.rpc.1") }).await.unwrap();
        assert_eq!(n1.tagged("rabbit").await.unwrap(), [pred.clone(), succ.clone()].into(), "between the successor's claim and the mark both carry the label");
        let marked = NodeRow { inherited_by: Some(succ.clone()), ..old.clone() };
        n1.put_node_row(&marked).await.unwrap();
        assert_eq!(n1.tagged("rabbit").await.unwrap(), [succ.clone()].into(), "the retained row's label left the index");
        assert_eq!(n1.node_row_at(&old.name).await.unwrap(), Some(marked.clone()), "the retained row keeps its tags and names who inherited it");
        n1.put_node_row(&NodeRow { tags: tags(&["rabbit", "red"]), ..marked.clone() }).await.unwrap();
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
        let d = crate::test_support::tempdir("shared-connections");
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
        // The by-destination index's members are held and published with the rows (connections.md
        // §3.1): the later row names another birth at the destination's path, so the first birth's
        // index holds its removal and the second's the member.
        let index = |c: &NodeConnection| format!("connections-by-destination/{}=1", c.destination.node_id);
        assert_eq!(published.summary(), format!("connections/mesh1.admin.1=1,connections/mesh1.admin.1/history=1,{},{}", index(&up), index(&down)), "{published:?}");
        assert_eq!(keys(&dir).len(), 1, "one row per (source, destination, kind): {}", dir.display());
        assert!(dir.join("history.jsonl").exists());
        let reader = FileConnectionsStorage::open(&root, &node, &IncarnationId::mint()).await.unwrap();
        assert_eq!(reader.connections().await.unwrap(), [down]);
        assert_eq!(reader.history().await.unwrap(), [up]);
    }

    /// CONTRACT (connections.md §3.1: "by-destination prefix = hash(destination)", one latest member
    /// per (source, destination, alpn, kind)): the store's by-destination index names every current
    /// row whose destination is one node, whichever source wrote it and in whichever mesh, read by any
    /// process on the root; a removed row leaves it, and a row whose destination is re-born at its
    /// path.name under a new node id leaves the old node's index for the new one's. Memory and file
    /// alike; on the file root a read of a node nobody connects to makes no directory.
    #[tokio::test]
    async fn the_by_destination_index_names_every_source_holding_a_row_to_one_node_in_any_mesh() {
        let d = crate::test_support::tempdir("by-destination");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = crate::store_root::StoreRoot::open(&store, &crate::model::FabricId::mint()).unwrap();
        root.scratchpad().expect("a root without the Fabric row opens with a scratchpad").publish().await.unwrap();
        let destination = end("mesh1.rpc.1");
        let row = |source: &str, destination: &ConnectionEnd| NodeConnection { source: end(source), destination: destination.clone(), ..fact(ConnectionState::Connected) };
        let (a, b) = (row("mesh1.rpc.2", &destination), row("mesh2.rpc.1", &destination));
        let reborn = ConnectionEnd { node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()), ..destination.clone() };
        let again = NodeConnection { id: rdm_core::ConnectionId::mint(), destination: reborn.clone(), ..a.clone() };
        let check = async |sa: &dyn ConnectionsStorage, sb: &dyn ConnectionsStorage, reader: &dyn ConnectionsStorage, backend: &str| {
            sa.put_connection(&a).await.unwrap();
            sb.put_connection(&b).await.unwrap();
            let mut held = reader.connections_to(&destination.node_id).await.unwrap();
            held.sort_by_key(|r| r.source.name.to_string());
            assert_eq!(held, [a.clone(), b.clone()], "{backend}: both sources, in two meshes, hold a row to the node");
            assert!(reader.connections_to(&a.source.node_id).await.unwrap().is_empty(), "{backend}: a source is no destination");
            sb.remove_connection(&b.index()).await.unwrap();
            assert_eq!(reader.connections_to(&destination.node_id).await.unwrap(), [a.clone()], "{backend}: a removed row leaves the index");
            sa.put_connection(&again).await.unwrap();
            assert!(reader.connections_to(&destination.node_id).await.unwrap().is_empty(), "{backend}: the row now names the birth re-born at the path: the old node's index holds no member of it");
            assert_eq!(reader.connections_to(&reborn.node_id).await.unwrap(), [again.clone()], "{backend}: the new node's index holds it");
        };
        let mem = MemoryConnectionsStorage::default();
        check(&mem, &mem, &mem, "memory").await;
        let open = async |name: &str| FileConnectionsStorage::open(&root, &name.parse().unwrap(), &IncarnationId::mint()).await.unwrap();
        let (fa, fb, reader) = (open("mesh1.rpc.2").await, open("mesh2.rpc.1").await, open("mesh1.admin.1").await);
        check(&fa, &fb, &reader, "file").await;
        assert!(!root.connections_to(&a.source.node_id).exists(), "a read of a node nobody connects to makes no directory on the root");
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
        let d = crate::test_support::tempdir("rows-offload");
        let fifo = d.join(dir).join(format!("{key}.json"));
        crate::test_support::runtime_runs_while_a_fifo_is_read(fifo, bytes, Box::pin(async move { op(std::sync::Arc::new(FileNodesStorage::open(&d, &crate::store_root::StoreRoot::at(&d.join("root")), "mesh1", &IncarnationId::mint()).await.unwrap())).await }))
    }

    /// CONTRACT: a point read of a node row never waits on the disk on a runtime worker (the
    /// defect class that froze node-admins on 10-08: a worker blocked on the disk): it runs on the
    /// blocking pool, as the row's write does (`blocking::blocking_io`). The row write runs
    /// inline on every view install, the join door's among them, after this read of the key.
    /// The cell: a row file that is a FIFO holds the read until a writer comes; the runtime's only
    /// thread keeps running meanwhile, and the read answers the row.
    #[test]
    fn a_node_row_read_waits_on_the_disk_off_the_runtime_thread() {
        let row = NodeRow { name: "mesh1.rpc.1".parse().unwrap(), node_id: NodeId::mint(), status: crate::model::NodeStatus::ReadyForTraffic, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None };
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
        let d = crate::test_support::tempdir("history-offload");
        let logged = fact(ConnectionState::Connected);
        let mut line = serde_json::to_vec(&logged).unwrap();
        line.push(b'\n');
        let (root, node): (_, PathName) = (crate::store_root::StoreRoot::at(&d.join("root")), "mesh1.admin.1".parse().unwrap());
        let fifo = root.connections(&node).join("history.jsonl");
        let (ran, history) = crate::test_support::runtime_runs_while_a_fifo_is_read(fifo, line, Box::pin(async move { FileConnectionsStorage::open(&root, &node, &IncarnationId::mint()).await.unwrap().history().await }));
        assert!(ran, "the runtime thread ran while the history read waited on the disk");
        assert_eq!(history, Ok(vec![logged]));
    }

    /// CONTRACT: a node row's writer claims its name (R13; lifecycles/node-create.md step 1: "claims
    /// the name, reading the row at it; `NameHeld { by: node.id }` refuses a held name"; Q109: the
    /// storage decides nothing). Two saves of rows of two different nodes at one
    /// `path.name`: the second is refused and the first node's row stands. A save of the same node's row
    /// there (its status moves, its Deleted row) is a put, and another node's `Deleted` row holds no
    /// name: a save over it is a put. The memory store (its internal index) and the file store each
    /// run their own check.
    #[tokio::test]
    async fn a_second_nodes_row_at_a_held_name_is_refused_by_its_writer_and_nothing_is_written() {
        let d = crate::test_support::tempdir("storage-claim");
        let name: PathName = "mesh1.rpc.1".parse().unwrap();
        let row = |node_id: &NodeId, status: crate::model::NodeStatus, tier: &str| NodeRow { name: name.clone(), node_id: node_id.clone(), status, config: BTreeMap::from([("tier".to_string(), tier.to_string())]), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None };
        let check = async |n: &dyn NodesStorage, store: &str| {
            let (first, second) = (NodeId::mint(), NodeId::mint());
            let held = row(&first, crate::model::NodeStatus::ReadyForTraffic, "gold");
            assert_eq!(claim_node_row(n, &held).await, Ok(Ok(())), "{store}: a free key takes the row");
            let refused = claim_node_row(n, &row(&second, crate::model::NodeStatus::Pending, "silver")).await;
            assert_eq!(refused, Ok(Err(NameHeld { name: name.clone(), by: first.clone() })), "{store}: another node's live row holds {name}");
            assert_eq!(n.node_row_at(&name).await.unwrap(), Some(held.clone()), "{store}: nothing is written by a refused save");
            let deleted = row(&first, crate::model::NodeStatus::Deleted, "gold");
            assert_eq!(claim_node_row(n, &deleted).await, Ok(Ok(())), "{store}: the holder's own row is a put");
            let next = row(&second, crate::model::NodeStatus::Pending, "gold");
            assert_eq!(claim_node_row(n, &next).await, Ok(Ok(())), "{store}: a Deleted row holds no name");
            assert_eq!(n.node_row_at(&name).await.unwrap(), Some(next), "{store}: the save over a Deleted row is written");
        };
        check(&MemoryNodesStorage::default(), "memory").await;
        check(&FileNodesStorage::open(&d, &crate::store_root::StoreRoot::at(&d.join("root")), "mesh1", &IncarnationId::mint()).await.unwrap(), "file").await;
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (node-identity-and-state.md §2.7: the mesh-primary "claims the mesh's name at the key
    /// `(path, name)` ... Held means held by a different MeshId: `NameHeld` names that holder.
    /// The same MeshId re-claiming its own name ... is idempotent and accepted"; R-MN1: only a delete
    /// releases a name, a mesh's by its removal, the row Dead; states.md: a mesh id is never reissued,
    /// dead is final; Q109: the storage decides nothing): the row writer claims a mesh's name. A free key takes
    /// the row; the same mesh re-claiming finds its own row and writes nothing; another mesh's claim
    /// of the live name is refused naming the holder and writes nothing; the holder's Dead row is a
    /// write of its own row, and a repeat of it writes nothing; the Dead mesh is never written live
    /// again; another mesh claims the name over the Dead row, which the claim names. The memory store
    /// and the file store each run their own check.
    #[tokio::test]
    async fn a_mesh_name_is_claimed_by_its_writer_and_only_a_dead_row_releases_it() {
        let d = crate::test_support::tempdir("storage-mesh-claim");
        let check = async |m: &dyn MeshStorage, store: &str| {
            let (first, second) = (MeshId::mint(), MeshId::mint());
            let row = |id: &MeshId, dead: bool| MeshRecord { mesh_id: id.clone(), name: "mesh2".into(), dead };
            assert_eq!(crate::storage::claim_mesh(&*m, &row(&first, false)).await, Ok(Ok(MeshClaim::Written { over: None })), "{store}: a free key takes the row");
            assert_eq!(crate::storage::claim_mesh(&*m, &row(&first, false)).await, Ok(Ok(MeshClaim::Held)), "{store}: the same mesh re-claiming its name writes nothing");
            assert_eq!(crate::storage::claim_mesh(&*m, &row(&second, false)).await, Ok(Err(MeshClaimRefused::NameHeld { name: "mesh2".into(), by: first.clone() })), "{store}: another mesh's live row holds mesh2");
            assert_eq!(m.mesh("mesh2").await.unwrap(), Some(row(&first, false)), "{store}: nothing is written by a refused claim");
            assert_eq!(crate::storage::claim_mesh(&*m, &row(&first, true)).await, Ok(Ok(MeshClaim::Written { over: None })), "{store}: the holder's Dead row is a write of its own row");
            assert_eq!(crate::storage::claim_mesh(&*m, &row(&first, true)).await, Ok(Ok(MeshClaim::Held)), "{store}: a repeat of the Dead row writes nothing");
            assert_eq!(crate::storage::claim_mesh(&*m, &row(&first, false)).await, Ok(Err(MeshClaimRefused::MeshDead { name: "mesh2".into(), mesh_id: first.clone() })), "{store}: a dead mesh is never written live again");
            assert_eq!(crate::storage::claim_mesh(&*m, &row(&second, false)).await, Ok(Ok(MeshClaim::Written { over: Some(first.clone()) })), "{store}: a Dead row holds no name");
            assert_eq!(m.mesh("mesh2").await.unwrap(), Some(row(&second, false)), "{store}: the claim over the Dead row is written");
        };
        check(&MemoryMeshStorage::default(), "memory").await;
        check(&FileMeshStorage::open(&crate::store_root::StoreRoot::at(&d.join("root")), &IncarnationId::mint()).await.unwrap(), "file").await;
        let _ = std::fs::remove_dir_all(&d);
    }
}
