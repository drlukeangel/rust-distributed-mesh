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
//! - `mesh.storage`: each Mesh record at its id (its identity, Dead, the states it entered) and each
//!   live mesh's name claim.
//! - `nodes.storage`: each node's record at its id (its row and the facts of its births: declared
//!   states, operation effects, silence, data-dir facts, cert dates, the birth's address and the
//!   primaries recorded for it), its name claim, the `path.name` index and the tags index (R-TG1); and
//!   the runtimes this admin started, in its own data dir. A birth's address on its record is a hint:
//!   never topology, never death proof. The admin's own row (NodeId, path.name, the incarnation it last
//!   ran, its transport identity), so a restart is the same logical node, is its own seed in its data
//!   dir, read and written by its storage backend (`crate::storage_backend::StorageBackend::own_row`).
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

/// A Mesh record (node-identity-and-state.md §2.7): the mesh's identity, whether its removal made it
/// Dead, and the states it entered. Kept at its mesh id and written whole by the mesh's writer (its
/// primary; the fabric-primary while the mesh has none) from the copy it caches
/// (storage-whole-records.md, "The rule"); its name is a claim beside it
/// ([`MeshStorage::claim_mesh_name`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshRecord {
    /// The mesh's minted id: the record's key.
    pub mesh_id: MeshId,
    /// The mesh's name, claimed beside the record.
    pub name: String,
    /// Set by the mesh's removal (mesh.deleted at its primary, or mesh-left at the fabric-primary as
    /// acting primary): the mesh is Dead, its name is released (R-MN1 item 2) and its id is never
    /// written live again.
    pub dead: bool,
    /// The states the mesh entered, each once (states.md, "Who sets each state"): a late state never
    /// removes a greater one, and a reader takes the greatest.
    #[serde(default)]
    pub states: BTreeSet<MeshState>,
}

impl MeshRecord {
    /// A live mesh's record that holds no state yet.
    pub fn new(mesh_id: MeshId, name: impl Into<String>) -> Self {
        Self { mesh_id, name: name.into(), dead: false, states: BTreeSet::new() }
    }

    /// The greatest state the mesh entered, if any.
    pub fn state(&self) -> Option<MeshState> {
        self.states.iter().next_back().copied()
    }
}

/// What a claim of a mesh's name wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshClaim {
    /// The record was written: new, changed (its Dead write), or over another mesh's name that mesh's
    /// removal released, which `over` names (R-MN1).
    Written {
        /// The Dead mesh whose name the claim took over.
        over: Option<MeshId>,
    },
    /// This mesh holds its name and its record stands exactly as given: nothing is written (a
    /// re-claim at the fabric's state-commit, a retry, a replaced primary).
    Held,
}

/// Why a claim of a mesh's name wrote nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshClaimRefused {
    /// Another live mesh holds the name: it is released only when that mesh is Dead.
    NameHeld {
        /// The name claimed.
        name: String,
        /// The mesh that holds it.
        by: MeshId,
    },
    /// This mesh is Dead: a dead mesh is never written live again and its id is never reissued
    /// (states.md); only its Dead record is ever written again.
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
            Self::NameHeld { name, by } => write!(f, "the mesh name {name} is held by mesh {by}: it is released only when that mesh is Dead"),
            Self::MeshDead { name, mesh_id } => write!(f, "mesh {mesh_id} at {name} is Dead: a dead mesh is never written live again"),
        }
    }
}

impl std::error::Error for MeshClaimRefused {}

/// Keep `record`, the whole record its writer holds, under its name's claim (node-identity-and-state.md
/// §2.7; Q109: the storage decides nothing, the writer does). `held` is the writer's cached copy of
/// this mesh's record, or `None` for a mesh it holds none of. A Dead mesh is never written live again
/// (`MeshDead`). The name is claimed for the mesh; a claim another mesh holds is taken over only when
/// that mesh's record is Dead (its removal released the name, R-MN1), else `NameHeld` naming it and
/// nothing is written. The record is then written whole unless it stands exactly as `held`; a Dead
/// record releases the name.
pub async fn claim_mesh(meshes: &dyn MeshStorage, held: Option<&MeshRecord>, record: &MeshRecord) -> Result<Result<MeshClaim, MeshClaimRefused>, StorageError> {
    if held.is_some_and(|h| h.dead) && !record.dead {
        return Ok(Err(MeshClaimRefused::MeshDead { name: record.name.clone(), mesh_id: record.mesh_id.clone() }));
    }
    let mut over = None;
    if !record.dead {
        if let Err(by) = meshes.claim_mesh_name(&record.name, &record.mesh_id).await? {
            if !meshes.mesh(&by).await?.is_some_and(|h| h.dead) {
                return Ok(Err(MeshClaimRefused::NameHeld { name: record.name.clone(), by }));
            }
            meshes.release_mesh_name(&record.name, &by).await?;
            if let Err(by) = meshes.claim_mesh_name(&record.name, &record.mesh_id).await? {
                return Ok(Err(MeshClaimRefused::NameHeld { name: record.name.clone(), by }));
            }
            over = Some(by);
        }
    }
    if over.is_none() && held == Some(record) {
        return Ok(Ok(MeshClaim::Held));
    }
    meshes.put_mesh(record).await?;
    if record.dead {
        meshes.release_mesh_name(&record.name, &record.mesh_id).await?;
    }
    Ok(Ok(MeshClaim::Written { over }))
}

/// The durable record of the meshes: each mesh's record at its id, and each live mesh's name claim.
#[async_trait]
pub trait MeshStorage: Send + Sync {
    /// The record at `mesh_id`: a point read of its key; `None` when none was ever written.
    async fn mesh(&self, mesh_id: &MeshId) -> Result<Option<MeshRecord>, StorageError>;
    /// The record of the mesh whose claim holds `name`, if one does: the claim's lookup, then the
    /// record at its id.
    async fn mesh_named(&self, name: &str) -> Result<Option<MeshRecord>, StorageError>;
    /// Write `record` whole at its id, as given (Q109: storage stores and returns what it is given).
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError>;
    /// Every mesh record held.
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError>;
    /// Claim `name` for `mesh_id`: an insert of the claim's key. A claim this mesh already holds
    /// answers `Ok`; another mesh's answers `Err` naming that mesh, and nothing is written.
    async fn claim_mesh_name(&self, name: &str, mesh_id: &MeshId) -> Result<Result<(), MeshId>, StorageError>;
    /// Release `name`'s claim while `mesh_id` holds it; another mesh's claim there is left as it is.
    async fn release_mesh_name(&self, name: &str, mesh_id: &MeshId) -> Result<(), StorageError>;
    /// Take `mesh`'s in-flight row for `claim` (R-BI7 caveat 3, [`crate::mesh_claims`]): the insert of
    /// the row's key decides. A row standing for the same attempt by the same claimant is a repeat take:
    /// `Won`. A row standing for another attempt, or for the same attempt by another claimant, is
    /// `Held`, naming it: the second claimant of one attempt loses and runs nothing (build.md §5.2, Q142). A row the insert met and the read no longer
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
}

/// A mesh store held in memory: the records at their ids, the name claims and each mesh's in-flight row.
#[derive(Debug, Default)]
pub struct MemoryMeshStorage {
    records: Mutex<BTreeMap<String, MeshRecord>>,
    names: Mutex<BTreeMap<String, MeshId>>,
    in_flight: Mutex<BTreeMap<String, InFlight>>,
}

#[async_trait]
impl MeshStorage for MemoryMeshStorage {
    async fn mesh(&self, mesh_id: &MeshId) -> Result<Option<MeshRecord>, StorageError> {
        Ok(self.records.lock().unwrap().get(mesh_id.as_str()).cloned())
    }
    async fn mesh_named(&self, name: &str) -> Result<Option<MeshRecord>, StorageError> {
        let Some(id) = self.names.lock().unwrap().get(name).cloned() else { return Ok(None) };
        Ok(self.records.lock().unwrap().get(id.as_str()).cloned())
    }
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError> {
        self.records.lock().unwrap().insert(record.mesh_id.to_string(), record.clone());
        Ok(())
    }
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
        Ok(self.records.lock().unwrap().values().cloned().collect())
    }
    async fn claim_mesh_name(&self, name: &str, mesh_id: &MeshId) -> Result<Result<(), MeshId>, StorageError> {
        let mut names = self.names.lock().unwrap();
        Ok(match names.get(name) {
            Some(by) if by != mesh_id => Err(by.clone()),
            Some(_) => Ok(()),
            None => {
                names.insert(name.to_string(), mesh_id.clone());
                Ok(())
            }
        })
    }
    async fn release_mesh_name(&self, name: &str, mesh_id: &MeshId) -> Result<(), StorageError> {
        let mut names = self.names.lock().unwrap();
        if names.get(name) == Some(mesh_id) {
            names.remove(name);
        }
        Ok(())
    }
    async fn take_in_flight(&self, mesh: &str, claim: &InFlight) -> Result<Taken, StorageError> {
        // The map's key is the row's: the look and the insert are one step under its lock.
        let mut rows = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        Ok(match rows.get(mesh) {
            Some(held) if held.same_claim(claim) => Taken::Won,
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
}

#[cfg(feature = "file-backend")]
const MESH_FORMAT: &str = "mesh-record/3";
#[cfg(feature = "file-backend")]
const MESH_NAME_FORMAT: &str = "mesh-name/1";

#[cfg(feature = "file-backend")]
/// The format of a mesh's in-flight row. A row in the retired `mesh-in-flight/1` (no lane key) is
/// refused by name.
const IN_FLIGHT_FORMAT: &str = "mesh-in-flight/2";

#[cfg(feature = "file-backend")]
/// The key of a mesh's in-flight row under `meshes/<mesh>/claims/`.
const IN_FLIGHT: &str = "in-flight";

#[cfg(feature = "file-backend")]
/// A mesh name's claim: the mesh that holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MeshNameClaim {
    name: String,
    mesh_id: MeshId,
}

#[cfg(feature = "file-backend")]
/// `<store root>/mesh-records/<mesh id>.json`: each mesh's record, written whole by its writer;
/// `mesh-names/<name>.json`, each live mesh's name claim; and `meshes/<mesh>/claims/in-flight.json`,
/// each mesh's in-flight row ([`crate::mesh_claims`]).
pub struct FileMeshStorage {
    records: FileRecords,
    names: FileRecords,
    root: crate::store_root::StoreRoot,
    writer: IncarnationId,
    /// `meshes/<mesh>/claims/`, one store per mesh, opened on its first use.
    in_flight: tokio::sync::Mutex<BTreeMap<String, FileRecords>>,
}

#[cfg(feature = "file-backend")]
impl std::fmt::Debug for FileMeshStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileMeshStorage").field("records", &self.records).finish_non_exhaustive()
    }
}

#[cfg(feature = "file-backend")]
impl FileMeshStorage {
    /// Open the store under the fabric's store root, written by the process born as `writer`.
    pub async fn open(root: &crate::store_root::StoreRoot, writer: &IncarnationId) -> Result<Self, StorageError> {
        Ok(Self {
            records: root.records(root.dir(), "mesh-records", writer).await?,
            names: root.records(root.dir(), "mesh-names", writer).await?,
            root: root.clone(),
            writer: writer.clone(),
            in_flight: tokio::sync::Mutex::new(BTreeMap::new()),
        })
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

    /// Finishes every write of the Mesh records and name claims this store has issued
    /// (node-primary-update.md step 3).
    pub async fn finish_issued_writes(&self) {
        self.records.finish_issued_writes().await;
        self.names.finish_issued_writes().await;
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl MeshStorage for FileMeshStorage {
    async fn mesh(&self, mesh_id: &MeshId) -> Result<Option<MeshRecord>, StorageError> {
        self.records.read(mesh_id.as_str(), MESH_FORMAT).await
    }
    async fn mesh_named(&self, name: &str) -> Result<Option<MeshRecord>, StorageError> {
        let Some(claim) = self.names.read::<MeshNameClaim>(name, MESH_NAME_FORMAT).await? else { return Ok(None) };
        self.mesh(&claim.mesh_id).await
    }
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError> {
        self.records.write(record.mesh_id.as_str(), MESH_FORMAT, record).await
    }
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
        self.records.list(MESH_FORMAT).await
    }
    async fn claim_mesh_name(&self, name: &str, mesh_id: &MeshId) -> Result<Result<(), MeshId>, StorageError> {
        if self.names.insert(name, MESH_NAME_FORMAT, &MeshNameClaim { name: name.to_string(), mesh_id: mesh_id.clone() }).await? {
            return Ok(Ok(()));
        }
        match self.names.read::<MeshNameClaim>(name, MESH_NAME_FORMAT).await? {
            Some(held) if held.mesh_id == *mesh_id => Ok(Ok(())),
            Some(held) => Ok(Err(held.mesh_id)),
            None => Err(StorageError::Io {
                file: format!("{}/{name}.json", self.names.store_name()),
                reason: format!("the claim of the mesh name {name} was held at the insert for mesh {mesh_id} and gone at the read after it: its holder released it between the two; nothing is claimed, and the next claim decides on the free name"),
            }),
        }
    }
    async fn release_mesh_name(&self, name: &str, mesh_id: &MeshId) -> Result<(), StorageError> {
        if !self.names.read::<MeshNameClaim>(name, MESH_NAME_FORMAT).await?.is_some_and(|held| held.mesh_id == *mesh_id) {
            return Ok(());
        }
        let mesh_id = mesh_id.clone();
        self.names.remove_released::<MeshNameClaim>(name, MESH_NAME_FORMAT, move |held| held.mesh_id == mesh_id).await
    }
    async fn take_in_flight(&self, mesh: &str, claim: &InFlight) -> Result<Taken, StorageError> {
        let store = self.claims_of(mesh).await?;
        if store.insert(IN_FLIGHT, IN_FLIGHT_FORMAT, claim).await? {
            return Ok(Taken::Won);
        }
        match store.read::<InFlight>(IN_FLIGHT, IN_FLIGHT_FORMAT).await? {
            Some(held) if held.same_claim(claim) => Ok(Taken::Won),
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
}

// ---------------------------------------------------------------- facts on a node's record

/// An operation effect applied to one birth (Q49), on its node's record: independent of the birth's
/// current cycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeOperation {
    /// The operation that produced this effect.
    pub operation: rdm_core::status::DeclaredOperation,
    /// The effect applied.
    pub state: NodeState,
}

/// The `data-dir-lost` fact on a birth (R-FM2): the inherited data dir was absent for
/// `RDM_DATA_DIR_GONE_FLOORS` staleness floors while the provider reported the birth `Exited`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataDirLost {
    /// The absent data dir path.
    pub path: String,
    /// When the absent data dir was first observed, ms since the Unix epoch.
    pub since: u64,
}

/// The `data-dir-unreadable` fact on a birth (R-FM2): the inherited data dir exists on disk, but
/// `node-key` is missing, unreadable, or corrupt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataDirUnreadable {
    /// The broken data dir path.
    pub path: String,
    /// Why `node-key` could not be loaded from the data dir.
    pub cause: String,
}

/// A birth's member cert dates (fabric-certs.md §Tracking), written by the node's mesh-primary when
/// the birth holds the cert. The written expiry is the receipt (Q141); the dates are the birth's
/// renewal state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertDates {
    /// When the cert was issued, rafka-time ms.
    pub issued_at: u64,
    /// When it expires, rafka-time ms.
    pub expires_at: u64,
    /// When its issuing cert expires, rafka-time ms.
    pub issuer_expires_at: u64,
    /// When its issuer renews it (`rdm_core::certs::renew_at`), rafka-time ms.
    pub renew_at: u64,
}

/// A node-admin birth's signing cert dates (mesh-issuer-renew.md): the cert it signs its mesh's member
/// certs with stays in its process, like the root; its dates are written by the admin's mesh-primary
/// when the admin holds it. The written expiry is the receipt (Q141).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SigningCertDates {
    /// When the signing cert was minted, rafka-time ms.
    pub issued_at: u64,
    /// When it expires, rafka-time ms.
    pub expires_at: u64,
    /// When the fabric-primary renews it, rafka-time ms.
    pub renew_at: u64,
}

/// One birth's facts on its node's record (storage-whole-records.md, "The objects"), each kept by its
/// full identity so no fact erases another: one declared entry per state (a late Pending never removes
/// ReadyForTraffic; the reader takes the greatest), one effect per (build_id, attempt, operation).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BirthFacts {
    /// The birth.
    pub incarnation_id: IncarnationId,
    /// The `path.name` the birth held when its first fact was written.
    pub name: PathName,
    /// The states the birth declared and its authority applied in its current cycle.
    #[serde(default)]
    pub declared: BTreeSet<NodeState>,
    /// The operation effects applied to the birth, of every cycle.
    #[serde(default)]
    pub operations: Vec<NodeOperation>,
    /// The silence status its mesh-primary holds (node-identity-and-state.md §7.3): `PendingReconnect`,
    /// then `Dead`; `None` while heard. Observer-inferred: never itself a reason to act.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silence: Option<crate::model::NodeStatus>,
    /// `data-dir-lost` (R-FM2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir_lost: Option<DataDirLost>,
    /// `data-dir-unreadable` (R-FM2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir_unreadable: Option<DataDirUnreadable>,
    /// The member cert dates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert: Option<CertDates>,
    /// A node-admin birth's signing cert dates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signing_cert: Option<SigningCertDates>,
    /// The birth's fabric endpoint id, as its mesh's writer's book holds it (the mesh rows writer,
    /// `crate::admin::rows_writer`): with [`Self::transport_addr`], where the birth is reached. A birth
    /// its writer has not written it for carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_id: Option<EndpointId>,
    /// The one address of the birth's Iroh endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_addr: Option<std::net::SocketAddr>,
    /// Non-Iroh listeners.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listeners: Vec<(String, std::net::SocketAddr)>,
    /// The revision at which this birth is its node type's primary in its mesh, when it is: the
    /// election's result, recorded by the mesh-primary (node-identity-and-state.md §2.7); the write
    /// records the role and does not grant it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_primary: Option<u64>,
    /// The revision at which this birth is the fabric-primary, when it is, recorded on the node-admin's
    /// own row by its mesh-primary (node-identity-and-state.md §2.8; node-primary-update.md: for a
    /// fabric-primary that is its own mesh's primary, itself), as [`Self::is_primary`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_fabric_primary: Option<u64>,
}

impl BirthFacts {
    /// A birth that holds no fact yet.
    pub fn new(incarnation_id: IncarnationId, name: PathName) -> Self {
        Self { incarnation_id, name, declared: BTreeSet::new(), operations: Vec::new(), silence: None, data_dir_lost: None, data_dir_unreadable: None, cert: None, signing_cert: None, endpoint_id: None, transport_addr: None, listeners: Vec::new(), is_primary: None, is_fabric_primary: None }
    }

    /// The birth's address and the primaries recorded for it leave its facts: a birth whose exit is
    /// proven is no node to reach and holds no primary (mesh-delete.md step 7, "Departed births resolve
    /// Gone").
    pub fn clear_address_and_primaries(&mut self) {
        self.endpoint_id = None;
        self.transport_addr = None;
        self.listeners.clear();
        self.is_primary = None;
        self.is_fabric_primary = None;
    }

    /// The greatest state declared in the birth's current cycle.
    pub fn declared_state(&self) -> Option<NodeState> {
        self.declared.iter().next_back().copied()
    }

    /// Keep the effect `state` of `operation`, once per (build_id, attempt, operation): an effect
    /// already held for that identity stands.
    pub fn apply_operation(&mut self, operation: &rdm_core::status::DeclaredOperation, state: NodeState) {
        if !self.operations.iter().any(|o| o.operation == *operation) {
            self.operations.push(NodeOperation { operation: operation.clone(), state });
        }
    }
}

/// One exact birth, by node and incarnation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BirthRef {
    /// The node.
    pub node_id: NodeId,
    /// The birth's incarnation.
    pub incarnation_id: IncarnationId,
}

/// How many births a record keeps the facts of: its current incarnation and the one a restart replaced
/// (storage-whole-records.md, "The objects"); the Build journal keeps the history.
pub const BIRTHS_KEPT: usize = 2;

// ---------------------------------------------------------------- nodes.storage


/// The admin's own row: its own seed, kept in its data dir and read before any store opens
/// (`crate::storage_backend::StorageBackend::own_row`), so a restarting node-admin is the same logical
/// node in the same Fabric and Mesh. The facts of its births, its address and primaries included, are
/// on its node's record ([`NodeRow`]), written by its mesh-primary.
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

/// The record of one node (storage-whole-records.md, "The objects"): kept at its minted node id and
/// written whole by its writer, the node's mesh-primary (the fabric-primary only as the acting primary
/// of a mesh with none, `crate::node_rows`), from the copy it caches: its `path.name`, status, config,
/// tags and executable, and the facts of its current birth and of the one a restart replaced. A
/// `Deleted` record stands and keeps the config for the next node created at the same `path.name`.
/// Beside it: the node's name claim ([`NodesStorage::claim_node_name`]), the `path.name` index
/// ([`NodesStorage::node_at`]) and the tags index ([`NodesStorage::tagged`]), whose entries RDM writes
/// from the moves it computes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRow {
    /// The node's `path.name`.
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
    /// The facts of the node's births, at most [`BIRTHS_KEPT`], the newest last.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub births: Vec<BirthFacts>,
    /// The Mesh the node belongs to, by its minted id (node-identity-and-state.md §2.7: `Node.mesh_id`),
    /// written by its mesh's writer with its births' addresses (`crate::admin::rows_writer`): a mesh
    /// created again under the name is another mesh with another id (R-MN1). A record no writer has
    /// written it on carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mesh_id: Option<MeshId>,
    /// The exact birth this node replaced, written at its create from the recovery attempt that named
    /// it: what a birth recovered over a fresh data dir reads its predecessor's `data-dir-lost` through.
    /// `inherited_by` points the other way and is not followed backwards.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predecessor: Option<BirthRef>,
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
    /// A record of `node_id` at `name` with `status`, nothing else on it yet.
    pub fn new(name: PathName, node_id: NodeId, status: crate::model::NodeStatus) -> Self {
        Self { name, node_id, status, config: BTreeMap::new(), tags: BTreeSet::new(), allocated: None, renamed_from: None, inherited_by: None, app_types: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None }
    }

    /// The facts of the node's newest birth, if the record keeps any.
    pub fn newest_birth(&self) -> Option<&BirthFacts> {
        self.births.last()
    }

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

    /// The facts of the birth `incarnation_id`, if the record keeps them.
    pub fn birth(&self, incarnation_id: &IncarnationId) -> Option<&BirthFacts> {
        self.births.iter().find(|b| b.incarnation_id == *incarnation_id)
    }

    /// The facts of the birth `incarnation_id`, made when the record keeps none, at `name`: a birth
    /// new to the record is the newest, and the oldest is let go past [`BIRTHS_KEPT`].
    pub fn birth_mut(&mut self, incarnation_id: &IncarnationId, name: &PathName) -> &mut BirthFacts {
        let at = match self.births.iter().position(|b| b.incarnation_id == *incarnation_id) {
            Some(at) => at,
            None => {
                self.births.push(BirthFacts::new(incarnation_id.clone(), name.clone()));
                while self.births.len() > BIRTHS_KEPT {
                    self.births.remove(0);
                }
                self.births.len() - 1
            }
        };
        &mut self.births[at]
    }

    /// The labels the tags index holds for this node once this record is written over `previous`,
    /// the node's record as its writer held it before, split into (leaving, arriving): a label the
    /// previous record carried and this one does not, or every label of a `Deleted` record or of a
    /// record a successor inherited ([`Self::inherited_by`]), leaves; every label of any other live
    /// record arrives.
    pub fn tag_moves(&self, previous: Option<&NodeRow>) -> (BTreeSet<String>, BTreeSet<String>) {
        let held: BTreeSet<String> = previous.filter(|p| p.node_id == self.node_id).map(|p| p.tags.clone()).unwrap_or_default();
        if self.status == crate::model::NodeStatus::Deleted || self.inherited_by.is_some() {
            return (held.union(&self.tags).cloned().collect(), BTreeSet::new());
        }
        (held.difference(&self.tags).cloned().collect(), self.tags.clone())
    }
}

/// The live records of every mesh: [`records`] with a `Deleted` record skipped. Every read of the
/// births a store root holds to reach (a restart's seeds, its durable map, the rafka-time peers, the
/// stored topology) reads them here, each from a record's newest birth.
pub async fn live_records(meshes: &dyn MeshStorage, nodes: &dyn NodesStorage, own_mesh: &str) -> Result<Vec<NodeRow>, StorageError> {
    Ok(records(meshes, nodes, own_mesh).await?.into_iter().filter(|r| r.status != crate::model::NodeStatus::Deleted).collect())
}

/// The records of every mesh: the records of each mesh `meshes` holds a record of, and of `own_mesh`
/// (whose record a Day-0 admin's claim precedes), each mesh read once, a `Deleted` record included: a
/// deleted birth's recorded primary stays an election input until a newer revision supersedes it
/// (fabric-node-lifecycle-elections.md §3), as a lost primary's does through fabric recovery.
pub async fn records(meshes: &dyn MeshStorage, nodes: &dyn NodesStorage, own_mesh: &str) -> Result<Vec<NodeRow>, StorageError> {
    let mut names: BTreeSet<String> = meshes.meshes().await?.into_iter().map(|m| m.name).collect();
    names.insert(own_mesh.to_string());
    let mut out = Vec::new();
    for mesh in names {
        out.extend(nodes.nodes(&mesh).await?);
    }
    Ok(out)
}

/// The greatest declared state of each birth the records keep, and the `path.name` it declared at.
pub fn fold_declared<'a>(records: impl IntoIterator<Item = &'a NodeRow>) -> HashMap<(NodeId, IncarnationId), (NodeState, PathName)> {
    let mut out = HashMap::new();
    for r in records {
        for b in &r.births {
            if let Some(state) = b.declared_state() {
                out.insert((r.node_id.clone(), b.incarnation_id.clone()), (state, b.name.clone()));
            }
        }
    }
    out
}

/// The index moves of one record write still owed to the indexes: the record was written and an index
/// write after it was not (storage-whole-records.md: "keeps the pending index moves on the writer until
/// the index acknowledges them; the retry never recomputes an empty difference from the
/// already-updated cache").
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexMoves {
    /// The `path.name` the record is at, when its index entry is owed.
    pub path: Option<PathName>,
    /// The labels leaving the tags index.
    pub leaving: BTreeSet<String>,
    /// The labels arriving in it.
    pub arriving: BTreeSet<String>,
}

impl IndexMoves {
    /// The moves of writing `row` over `previous`, its writer's held copy.
    pub fn of(row: &NodeRow, previous: Option<&NodeRow>) -> Self {
        let (leaving, arriving) = row.tag_moves(previous);
        let held: BTreeSet<String> = previous.filter(|p| p.node_id == row.node_id && p.status != crate::model::NodeStatus::Deleted && p.inherited_by.is_none()).map(|p| p.tags.clone()).unwrap_or_default();
        let arriving = arriving.difference(&held).cloned().collect();
        let path = (previous.map(|p| &p.name) != Some(&row.name)).then(|| row.name.clone());
        Self { path, leaving, arriving }
    }

    /// Whether nothing is owed.
    pub fn is_empty(&self) -> bool {
        self.path.is_none() && self.leaving.is_empty() && self.arriving.is_empty()
    }

    /// These moves, then `later` over them: what both owe together.
    pub fn then(mut self, later: IndexMoves) -> Self {
        for l in &later.arriving {
            self.leaving.remove(l);
        }
        for l in &later.leaving {
            self.arriving.remove(l);
        }
        self.leaving.extend(later.leaving);
        self.arriving.extend(later.arriving);
        if later.path.is_some() {
            self.path = later.path;
        }
        self
    }
}

/// Write `moves` of `node_id` to the indexes: the `path.name` entry and the tags index rows, each a
/// blind write of its own key.
pub async fn write_index_moves(nodes: &dyn NodesStorage, mesh: &str, node_id: &NodeId, moves: &IndexMoves) -> Result<(), StorageError> {
    if let Some(name) = &moves.path {
        nodes.index_path(name, node_id).await?;
    }
    if !moves.leaving.is_empty() || !moves.arriving.is_empty() {
        nodes.index_tags(mesh, node_id, &moves.leaving, &moves.arriving).await?;
    }
    Ok(())
}

/// The durable record of the nodes: each node's record at its id with its name claim, its `path.name`
/// index entry and its tags index rows; and, in this admin's own data dir, the runtimes it started.
#[async_trait]
pub trait NodesStorage: Send + Sync {
    /// Keep the runtime of one birth (a blind put of its own row).
    async fn put_runtime(&self, row: &RuntimeRow) -> Result<(), StorageError>;
    /// Every birth's runtime row this admin holds; folded by the reader.
    async fn runtimes(&self) -> Result<Vec<RuntimeRow>, StorageError>;
    /// The runtime row of the exact birth `(node_id, incarnation)`: a point read by its key.
    async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<RuntimeRow>, StorageError>;
    /// The record of `node_id`: a point read of its key; `None` when none was ever written.
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, StorageError>;
    /// The record the `path.name` index names at `name`: the node whose record was last written at it,
    /// a `Deleted` one included; `None` when no record was ever written there.
    async fn node_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError>;
    /// Every node record of `mesh`.
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRow>, StorageError>;
    /// Write `row` whole at its node id, as given (Q109: storage stores and returns what it is given).
    /// Durable before it answers, before the fabric's first state-commit too (Q54).
    async fn put_node(&self, row: &NodeRow) -> Result<(), StorageError>;
    /// Remove the record of `node_id`: only a create's withdrawn claim, whose node never ran.
    async fn remove_node(&self, mesh: &str, node_id: &NodeId) -> Result<(), StorageError>;
    /// The `path.name` index entry at `name` names `node_id`: a blind write of its key.
    async fn index_path(&self, name: &PathName, node_id: &NodeId) -> Result<(), StorageError>;
    /// The tags index rows of `node_id` of `mesh`: each of `leaving` tombstoned, each of `arriving`
    /// put, blind writes of their own keys (R-TG1).
    async fn index_tags(&self, mesh: &str, node_id: &NodeId, leaving: &BTreeSet<String>, arriving: &BTreeSet<String>) -> Result<(), StorageError>;
    /// The nodes carrying `label`, in every mesh: a point read of the label's rows of the tags index
    /// (R-TG1), never a read of any node's record. A `label` that is not a tag label
    /// ([`rdm_core::status::tag_refusal`]) is refused by name.
    async fn tagged(&self, label: &str) -> Result<BTreeSet<NodeId>, StorageError>;
    /// The node whose name claim holds `name`, if one does.
    async fn name_claim(&self, name: &PathName) -> Result<Option<NodeId>, StorageError>;
    /// Claim `name` for `node_id`: an insert of the claim's key. A claim this node already holds
    /// answers `Ok`; another node's answers `NameHeld` naming it, and nothing is written.
    async fn claim_node_name(&self, name: &PathName, node_id: &NodeId) -> Result<Result<(), NameHeld>, StorageError>;
    /// Release `name`'s claim while `node_id` holds it; another node's claim there is left as it is.
    async fn release_node_name(&self, name: &PathName, node_id: &NodeId) -> Result<(), StorageError>;
}

/// The refusal of a key the tags index cannot hold: `label` is not a tag label.
fn tag_key(label: &str, store: &str) -> Result<(), StorageError> {
    match rdm_core::status::tag_refusal(label) {
        None => Ok(()),
        Some(reason) => Err(StorageError::Unrecognised { file: format!("{store} (tags index)"), reason }),
    }
}

/// A nodes store held in memory.
#[derive(Debug, Default)]
pub struct MemoryNodesStorage {
    runtimes: Mutex<BTreeMap<String, RuntimeRow>>,
    records: Mutex<BTreeMap<NodeId, NodeRow>>,
    /// The `path.name` index: each name's last record.
    paths: Mutex<BTreeMap<String, NodeId>>,
    /// The name claims.
    claims: Mutex<BTreeMap<String, NodeId>>,
    /// The tags index: per label, the nodes carrying it.
    tags: Mutex<BTreeMap<String, BTreeSet<NodeId>>>,
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
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, StorageError> {
        Ok(self.records.lock().unwrap().get(node_id).cloned())
    }
    async fn node_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError> {
        let Some(id) = self.paths.lock().unwrap().get(&name.to_string()).cloned() else { return Ok(None) };
        Ok(self.records.lock().unwrap().get(&id).cloned())
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRow>, StorageError> {
        Ok(self.records.lock().unwrap().values().filter(|r| r.name.mesh == mesh).cloned().collect())
    }
    async fn put_node(&self, row: &NodeRow) -> Result<(), StorageError> {
        self.records.lock().unwrap().insert(row.node_id.clone(), row.clone());
        Ok(())
    }
    async fn remove_node(&self, _mesh: &str, node_id: &NodeId) -> Result<(), StorageError> {
        self.records.lock().unwrap().remove(node_id);
        Ok(())
    }
    async fn index_path(&self, name: &PathName, node_id: &NodeId) -> Result<(), StorageError> {
        self.paths.lock().unwrap().insert(name.to_string(), node_id.clone());
        Ok(())
    }
    async fn index_tags(&self, _mesh: &str, node_id: &NodeId, leaving: &BTreeSet<String>, arriving: &BTreeSet<String>) -> Result<(), StorageError> {
        let mut tags = self.tags.lock().unwrap();
        for l in leaving {
            if let Some(nodes) = tags.get_mut(l) {
                nodes.remove(node_id);
                if nodes.is_empty() {
                    tags.remove(l);
                }
            }
        }
        for l in arriving {
            tag_key(l, "memory nodes store")?;
            tags.entry(l.clone()).or_default().insert(node_id.clone());
        }
        Ok(())
    }
    async fn tagged(&self, label: &str) -> Result<BTreeSet<NodeId>, StorageError> {
        tag_key(label, "memory nodes store")?;
        Ok(self.tags.lock().unwrap().get(label).cloned().unwrap_or_default())
    }
    async fn name_claim(&self, name: &PathName) -> Result<Option<NodeId>, StorageError> {
        Ok(self.claims.lock().unwrap().get(&name.to_string()).cloned())
    }
    async fn claim_node_name(&self, name: &PathName, node_id: &NodeId) -> Result<Result<(), NameHeld>, StorageError> {
        let mut claims = self.claims.lock().unwrap();
        Ok(match claims.get(&name.to_string()) {
            Some(by) if by != node_id => Err(NameHeld { name: name.clone(), by: by.clone() }),
            Some(_) => Ok(()),
            None => {
                claims.insert(name.to_string(), node_id.clone());
                Ok(())
            }
        })
    }
    async fn release_node_name(&self, name: &PathName, node_id: &NodeId) -> Result<(), StorageError> {
        let mut claims = self.claims.lock().unwrap();
        if claims.get(&name.to_string()) == Some(node_id) {
            claims.remove(&name.to_string());
        }
        Ok(())
    }
}

#[cfg(feature = "file-backend")]
const NODE_FORMAT: &str = "node-record/4";
#[cfg(feature = "file-backend")]
const RUNTIME_FORMAT: &str = "node-runtime/1";
#[cfg(feature = "file-backend")]
const RECORD_FORMAT: &str = "node-row/3";
#[cfg(feature = "file-backend")]
const PATH_FORMAT: &str = "node-path/1";
#[cfg(feature = "file-backend")]
const NAME_CLAIM_FORMAT: &str = "node-name/1";
#[cfg(feature = "file-backend")]
const TAG_FORMAT: &str = "node-tag/1";

#[cfg(feature = "file-backend")]
/// One row of the tags index: the node carries the label. Its key is the node id, in the label's own
/// directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TagRow {
    label: String,
    node_id: NodeId,
}

#[cfg(feature = "file-backend")]
/// A `path.name` index entry, or a name claim: the node it names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct NameEntry {
    name: PathName,
    node_id: NodeId,
}

#[cfg(feature = "file-backend")]
/// One mesh's nodes on the fabric's store root, `nodes/<mesh>/`: its members' records
/// (`records/<node id>.json`), the `path.name` index (`paths/<path.name>.json`), the name claims
/// (`names/<path.name>.json`) and the tags index (`tags/<label>/<node id>.json`: one directory per
/// label, so a label's rows are read without reading any other's), written by that mesh's primary. The records, their indexes and the claims are opened straight to
/// the disk, never held in the scratchpad: a Node record carries the declared states, durable before
/// the answer, before state-commit too (Q54).
#[derive(Debug)]
struct MeshNodes {
    records: FileRecords,
    paths: FileRecords,
    names: FileRecords,
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
            records: FileRecords::open(&base, "records", writer).await?,
            paths: FileRecords::open(&base, "paths", writer).await?,
            names: FileRecords::open(&base, "names", writer).await?,
            base,
            writer: writer.clone(),
            labels: Mutex::new(BTreeMap::new()),
        })
    }

    /// Every store of this mesh opened so far.
    fn stores(&self) -> Vec<FileRecords> {
        let mut all = vec![self.records.clone(), self.paths.clone(), self.names.clone()];
        all.extend(self.labels.lock().unwrap_or_else(|e| e.into_inner()).values().cloned());
        all
    }

    /// `label`'s rows of the tags index, written: opened on first use (its directory made).
    async fn label(&self, label: &str) -> Result<FileRecords, StorageError> {
        tag_key(label, &self.base.display().to_string())?;
        if let Some(l) = self.labels.lock().unwrap_or_else(|e| e.into_inner()).get(label) {
            return Ok(l.clone());
        }
        let opened = FileRecords::open(&self.base, &format!("tags/{label}"), &self.writer).await?;
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
}

#[cfg(feature = "file-backend")]
/// The runtimes the admin started in its data dir (`nodes/runtimes/`), beside its own row
/// (`nodes/self.json`, [`Self::own_in`]), and every mesh's node records, their indexes and claims under
/// `nodes/<mesh>/` of the fabric's store root (`crate::store_root`).
#[derive(Debug)]
pub struct FileNodesStorage {
    runtimes: FileRecords,
    root: crate::store_root::StoreRoot,
    writer: IncarnationId,
    /// This admin's own mesh's nodes.
    mine: (String, std::sync::Arc<MeshNodes>),
    /// Every other mesh's nodes this admin has read or written, opened on first use.
    others: Mutex<BTreeMap<String, std::sync::Arc<MeshNodes>>>,
}

#[cfg(feature = "file-backend")]
impl FileNodesStorage {
    /// Open the store: the admin's runtimes in `own_data_dir`, the nodes of `own_mesh`
    /// (and of every other mesh, on first use) under the fabric's store `root`, written by the
    /// process born as `writer`.
    pub async fn open(own_data_dir: &Path, root: &crate::store_root::StoreRoot, own_mesh: &str, writer: &IncarnationId) -> Result<Self, StorageError> {
        Ok(Self {
            runtimes: FileRecords::open(own_data_dir, "nodes/runtimes", writer).await?,
            root: root.clone(),
            writer: writer.clone(),
            mine: (own_mesh.to_string(), std::sync::Arc::new(MeshNodes::open(root, own_mesh, writer).await?)),
            others: Mutex::new(BTreeMap::new()),
        })
    }

    /// The admin's own row in `own_data_dir`, read before the store opens: a restarting admin's row
    /// names the Fabric whose store root it opens and the mesh whose nodes it opens on.
    pub async fn own_in(own_data_dir: &Path, writer: &IncarnationId) -> Result<Option<NodeRecord>, StorageError> {
        FileRecords::open(own_data_dir, "nodes", writer).await?.read("self", NODE_FORMAT).await
    }

    /// Keep `record` as the admin's own row in `own_data_dir`, where [`Self::own_in`] reads it, written
    /// by the process born as `writer`.
    pub async fn put_own_in(own_data_dir: &Path, writer: &IncarnationId, record: &NodeRecord) -> Result<(), StorageError> {
        FileRecords::open(own_data_dir, "nodes", writer).await?.write("self", NODE_FORMAT, record).await
    }

    /// Finishes every write of the node records and their indexes this store has issued, its
    /// own mesh's and every other mesh's it opened (node-primary-update.md step 3).
    pub async fn finish_issued_writes(&self) {
        let opened: Vec<std::sync::Arc<MeshNodes>> = std::iter::once(self.mine.1.clone()).chain(self.others.lock().unwrap_or_else(|e| e.into_inner()).values().cloned()).collect();
        for s in opened.iter().flat_map(|m| m.stores()) {
            s.finish_issued_writes().await;
        }
    }

    /// The nodes of `mesh` on the shared root: a mesh this store has not opened yet is opened outside
    /// the lock and kept once under it.
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

    /// Every mesh that holds nodes on the shared root, by name.
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
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, StorageError> {
        if let Some(r) = self.mine.1.records.read(node_id.as_str(), RECORD_FORMAT).await? {
            return Ok(Some(r));
        }
        for mesh in self.meshes_on_root().await?.into_iter().filter(|m| *m != self.mine.0) {
            if let Some(r) = self.mesh(&mesh).await?.records.read(node_id.as_str(), RECORD_FORMAT).await? {
                return Ok(Some(r));
            }
        }
        Ok(None)
    }
    async fn node_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError> {
        let nodes = self.mesh(&name.mesh).await?;
        let Some(entry) = nodes.paths.read::<NameEntry>(&name.to_string(), PATH_FORMAT).await? else { return Ok(None) };
        nodes.records.read(entry.node_id.as_str(), RECORD_FORMAT).await
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRow>, StorageError> {
        self.mesh(mesh).await?.records.list(RECORD_FORMAT).await
    }
    async fn put_node(&self, row: &NodeRow) -> Result<(), StorageError> {
        self.mesh(&row.name.mesh).await?.records.write(row.node_id.as_str(), RECORD_FORMAT, row).await
    }
    async fn remove_node(&self, mesh: &str, node_id: &NodeId) -> Result<(), StorageError> {
        self.mesh(mesh).await?.records.remove(node_id.as_str()).await.map(|_| ())
    }
    async fn index_path(&self, name: &PathName, node_id: &NodeId) -> Result<(), StorageError> {
        self.mesh(&name.mesh).await?.paths.write(&name.to_string(), PATH_FORMAT, &NameEntry { name: name.clone(), node_id: node_id.clone() }).await
    }
    async fn index_tags(&self, mesh: &str, node_id: &NodeId, leaving: &BTreeSet<String>, arriving: &BTreeSet<String>) -> Result<(), StorageError> {
        let nodes = self.mesh(mesh).await?;
        for l in leaving {
            nodes.label(l).await?.remove(node_id.as_str()).await?;
        }
        for l in arriving {
            nodes.label(l).await?.write(node_id.as_str(), TAG_FORMAT, &TagRow { label: l.clone(), node_id: node_id.clone() }).await?;
        }
        Ok(())
    }
    async fn tagged(&self, label: &str) -> Result<BTreeSet<NodeId>, StorageError> {
        tag_key(label, &self.root.dir().join("nodes").display().to_string())?;
        let mut all = BTreeSet::new();
        for mesh in self.meshes_on_root().await? {
            all.extend(self.mesh(&mesh).await?.tagged(label).await?);
        }
        Ok(all)
    }
    async fn name_claim(&self, name: &PathName) -> Result<Option<NodeId>, StorageError> {
        Ok(self.mesh(&name.mesh).await?.names.read::<NameEntry>(&name.to_string(), NAME_CLAIM_FORMAT).await?.map(|e| e.node_id))
    }
    async fn claim_node_name(&self, name: &PathName, node_id: &NodeId) -> Result<Result<(), NameHeld>, StorageError> {
        let names = self.mesh(&name.mesh).await?.names.clone();
        let key = name.to_string();
        if names.insert(&key, NAME_CLAIM_FORMAT, &NameEntry { name: name.clone(), node_id: node_id.clone() }).await? {
            return Ok(Ok(()));
        }
        match names.read::<NameEntry>(&key, NAME_CLAIM_FORMAT).await? {
            Some(held) if held.node_id == *node_id => Ok(Ok(())),
            Some(held) => Ok(Err(NameHeld { name: name.clone(), by: held.node_id })),
            None => Err(StorageError::Io {
                file: format!("{}/{key}.json", names.store_name()),
                reason: format!("the claim of {name} was held at the insert for {node_id} and gone at the read after it: its holder released it between the two; nothing is claimed, and the next claim decides on the free name"),
            }),
        }
    }
    async fn release_node_name(&self, name: &PathName, node_id: &NodeId) -> Result<(), StorageError> {
        let names = self.mesh(&name.mesh).await?.names.clone();
        let key = name.to_string();
        if !names.read::<NameEntry>(&key, NAME_CLAIM_FORMAT).await?.is_some_and(|held| held.node_id == *node_id) {
            return Ok(());
        }
        let node_id = node_id.clone();
        names.remove_released::<NameEntry>(&key, NAME_CLAIM_FORMAT, move |held| held.node_id == node_id).await
    }
}

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
    /// beside `ReadyForTraffic`, two attempts' operation effects, a lost and an unreadable data dir;
    /// the birth's cert dates are its last.
    async fn facts_of_one_birth_all_stand(first: &dyn NodesStorage, reopened: &dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Box<dyn NodesStorage>> + Send>>) {
        let (node_id, incarnation_id, name): (NodeId, IncarnationId, PathName) = (NodeId::mint(), IncarnationId::mint(), "mesh1.rpc.1".parse().unwrap());
        let written = WrittenRows::default();
        let base = NodeRow::new(name.clone(), node_id.clone(), crate::model::NodeStatus::ReadyForTraffic);
        let write = async |change: Box<dyn FnOnce(&mut BirthFacts) + Send>| {
            written.update(first, &node_id, Some(base.clone()), |r| change(r.birth_mut(&incarnation_id, &name))).await.unwrap();
        };
        write(Box::new(|b| { b.declared.insert(NodeState::ReadyForTraffic); })).await;
        write(Box::new(|b| { b.declared.insert(NodeState::Pending); })).await;
        let op = |attempt| rdm_core::status::DeclaredOperation { build_id: "b".into(), attempt, operation: "node.stop:mesh1.rpc.1".into() };
        write(Box::new(move |b| b.apply_operation(&op(1), NodeState::Stopped))).await;
        write(Box::new(move |b| b.apply_operation(&op(2), NodeState::Stopped))).await;
        write(Box::new(|b| b.data_dir_lost = Some(DataDirLost { path: "/d".into(), since: 1 }))).await;
        write(Box::new(|b| b.data_dir_unreadable = Some(DataDirUnreadable { path: "/d".into(), cause: "node-key: not hex".into() }))).await;
        let cert = |issued_at: u64| CertDates { issued_at, expires_at: issued_at + 10, issuer_expires_at: 100, renew_at: issued_at + 6 };
        write(Box::new(move |b| b.cert = Some(cert(1)))).await;
        write(Box::new(move |b| b.cert = Some(cert(20)))).await;

        let store = reopened().await;
        let record = store.node(&node_id).await.unwrap().expect("the record is read whole");
        let b = record.birth(&incarnation_id).expect("the birth's facts");
        assert_eq!(b.declared_state(), Some(NodeState::ReadyForTraffic), "a late Pending removes no ReadyForTraffic: the reader takes the greatest");
        assert_eq!(b.declared.len(), 2, "two states, both kept");
        assert_eq!(b.operations.len(), 2, "two attempts' effects both stand");
        assert!(b.data_dir_lost.is_some() && b.data_dir_unreadable.is_some(), "a lost and an unreadable data dir both stand");
        assert_eq!(b.cert, Some(cert(20)), "the birth's cert dates are the last written");
        assert_eq!(store.nodes("mesh1").await.unwrap(), vec![record.clone()], "the record is its mesh's");
        assert!(store.nodes("mesh2").await.unwrap().is_empty(), "and no other mesh's");
    }

    /// CONTRACT (storage-whole-records.md, "The objects" and its cell "two facts of one birth both stand
    /// on the record across a reload ... on the memory and file backends"): every fact of a birth is
    /// kept by its full identity inside its node's record, so no later or delayed write erases an
    /// independent fact; the file backend's record survives a reopen by another writer. What must NOT
    /// happen: a late Pending replacing ReadyForTraffic, one attempt's effect erasing another's, a fact
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
        let first = FileNodesStorage::open(&data, &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        facts_of_one_birth_all_stand(&first, &move || {
            let (root, data) = (root.clone(), data.clone());
            Box::pin(async move { Box::new(FileNodesStorage::open(&data, &root, "mesh1", &IncarnationId::mint()).await.unwrap()) as Box<dyn NodesStorage> })
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
            async fn node_at(&self, n: &PathName) -> Result<Option<NodeRow>, StorageError> { self.log.lock().unwrap().push("read".into()); self.inner.node_at(n).await }
            async fn nodes(&self, m: &str) -> Result<Vec<NodeRow>, StorageError> { self.log.lock().unwrap().push("read".into()); self.inner.nodes(m).await }
            async fn put_node(&self, r: &NodeRow) -> Result<(), StorageError> { self.log.lock().unwrap().push("put".into()); self.puts.lock().unwrap().push(r.clone()); self.inner.put_node(r).await }
            async fn remove_node(&self, m: &str, n: &NodeId) -> Result<(), StorageError> { self.inner.remove_node(m, n).await }
            async fn index_path(&self, n: &PathName, id: &NodeId) -> Result<(), StorageError> { self.inner.index_path(n, id).await }
            async fn index_tags(&self, m: &str, id: &NodeId, l: &BTreeSet<String>, a: &BTreeSet<String>) -> Result<(), StorageError> { self.inner.index_tags(m, id, l, a).await }
            async fn tagged(&self, l: &str) -> Result<BTreeSet<NodeId>, StorageError> { self.inner.tagged(l).await }
            async fn name_claim(&self, n: &PathName) -> Result<Option<NodeId>, StorageError> { self.inner.name_claim(n).await }
            async fn claim_node_name(&self, n: &PathName, id: &NodeId) -> Result<Result<(), NameHeld>, StorageError> { self.inner.claim_node_name(n, id).await }
            async fn release_node_name(&self, n: &PathName, id: &NodeId) -> Result<(), StorageError> { self.inner.release_node_name(n, id).await }
        }
        let store = Recording::default();
        let written = WrittenRows::default();
        let (name, node_id, inc): (PathName, NodeId, IncarnationId) = ("mesh1.rpc.1".parse().unwrap(), NodeId::mint(), IncarnationId::mint());
        let mut held = NodeRow::new(name.clone(), node_id.clone(), crate::model::NodeStatus::ReadyForTraffic);
        held.config.insert("tier".into(), "gold".into());
        held.tags.insert("red".into());
        written.hold(&held);
        written.update(&store, &node_id, None, |r| { r.birth_mut(&inc, &name).declared.insert(NodeState::ReadyForTraffic); }).await.unwrap();
        written.update(&store, &node_id, None, |r| r.birth_mut(&inc, &name).silence = Some(crate::model::NodeStatus::PendingReconnect)).await.unwrap();
        written.update(&store, &node_id, None, |r| r.birth_mut(&inc, &name).cert = Some(CertDates { issued_at: 1, expires_at: 9, issuer_expires_at: 99, renew_at: 6 })).await.unwrap();
        assert_eq!(*store.log.lock().unwrap(), ["put", "put", "put"], "three writes, no read before any of them");
        let puts = store.puts.lock().unwrap().clone();
        let last = puts.last().unwrap();
        assert_eq!((last.config.get("tier").map(String::as_str), last.tags == ["red".to_string()].into()), (Some("gold"), true), "the record's own fields go with every put");
        let b = last.birth(&inc).unwrap();
        assert!(b.declared.contains(&NodeState::ReadyForTraffic) && b.silence.is_some() && b.cert.is_some(), "the last put carries every fact the copy held: {b:?}");
        assert!(puts[1].birth(&inc).unwrap().declared.contains(&NodeState::ReadyForTraffic), "the second put carries the first's fact too");
    }

    /// CONTRACT (storage-whole-records.md cells: "a record write that succeeds while a tag-index write
    /// fails: the retry writes the pending index moves, and the index matches the record afterwards"):
    /// the moves of a write whose index write failed stay owed on the writer and go with the node's
    /// next write, never recomputed from the already-updated copy.
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
            async fn node_at(&self, n: &PathName) -> Result<Option<NodeRow>, StorageError> { self.inner.node_at(n).await }
            // The listing as it stood before the last write landed.
            async fn nodes(&self, _m: &str) -> Result<Vec<NodeRow>, StorageError> { Ok(self.listed.lock().unwrap().clone()) }
            async fn put_node(&self, r: &NodeRow) -> Result<(), StorageError> { self.inner.put_node(r).await }
            async fn remove_node(&self, m: &str, n: &NodeId) -> Result<(), StorageError> { self.inner.remove_node(m, n).await }
            async fn index_path(&self, n: &PathName, id: &NodeId) -> Result<(), StorageError> { self.inner.index_path(n, id).await }
            async fn index_tags(&self, m: &str, id: &NodeId, l: &BTreeSet<String>, a: &BTreeSet<String>) -> Result<(), StorageError> { self.inner.index_tags(m, id, l, a).await }
            async fn tagged(&self, l: &str) -> Result<BTreeSet<NodeId>, StorageError> { self.inner.tagged(l).await }
            async fn name_claim(&self, n: &PathName) -> Result<Option<NodeId>, StorageError> { self.inner.name_claim(n).await }
            async fn claim_node_name(&self, n: &PathName, id: &NodeId) -> Result<Result<(), NameHeld>, StorageError> { self.inner.claim_node_name(n, id).await }
            async fn release_node_name(&self, n: &PathName, id: &NodeId) -> Result<(), StorageError> { self.inner.release_node_name(n, id).await }
        }
        let store = StaleListing { inner: MemoryNodesStorage::default(), listed: Mutex::new(Vec::new()) };
        let written = WrittenRows::default();
        let (name, node_id, inc): (PathName, NodeId, IncarnationId) = ("mesh1.admin.1".parse().unwrap(), NodeId::mint(), IncarnationId::mint());
        written.update(&store, &node_id, Some(NodeRow::new(name.clone(), node_id.clone(), crate::model::NodeStatus::Pending)), |_| ()).await.unwrap();
        *store.listed.lock().unwrap() = vec![store.inner.node(&node_id).await.unwrap().unwrap()];
        written.update(&store, &node_id, None, |r| { r.birth_mut(&inc, &name).declared.insert(NodeState::ReadyForTraffic); }).await.unwrap();
        written.fill(&store, "mesh1").await.unwrap();
        written.update(&store, &node_id, None, |r| r.birth_mut(&inc, &name).signing_cert = Some(SigningCertDates { issued_at: 1, expires_at: 9, renew_at: 6 })).await.unwrap();
        let stored = store.inner.node(&node_id).await.unwrap().unwrap();
        let b = stored.birth(&inc).unwrap();
        assert!(b.declared.contains(&NodeState::ReadyForTraffic) && b.signing_cert.is_some(), "the write between the listing and the fill stands beside the next: {b:?}");
    }

    #[tokio::test]
    async fn a_tag_index_write_that_fails_after_its_record_is_written_by_the_next_write() {
        #[derive(Default)]
        struct IndexRefusing {
            inner: MemoryNodesStorage,
            refusing: std::sync::atomic::AtomicBool,
        }
        #[async_trait]
        impl NodesStorage for IndexRefusing {
            async fn put_runtime(&self, row: &RuntimeRow) -> Result<(), StorageError> { self.inner.put_runtime(row).await }
            async fn runtimes(&self) -> Result<Vec<RuntimeRow>, StorageError> { self.inner.runtimes().await }
            async fn runtime(&self, n: &NodeId, i: &IncarnationId) -> Result<Option<RuntimeRow>, StorageError> { self.inner.runtime(n, i).await }
            async fn node(&self, n: &NodeId) -> Result<Option<NodeRow>, StorageError> { self.inner.node(n).await }
            async fn node_at(&self, n: &PathName) -> Result<Option<NodeRow>, StorageError> { self.inner.node_at(n).await }
            async fn nodes(&self, m: &str) -> Result<Vec<NodeRow>, StorageError> { self.inner.nodes(m).await }
            async fn put_node(&self, r: &NodeRow) -> Result<(), StorageError> { self.inner.put_node(r).await }
            async fn remove_node(&self, m: &str, n: &NodeId) -> Result<(), StorageError> { self.inner.remove_node(m, n).await }
            async fn index_path(&self, n: &PathName, id: &NodeId) -> Result<(), StorageError> { self.inner.index_path(n, id).await }
            async fn index_tags(&self, m: &str, id: &NodeId, l: &BTreeSet<String>, a: &BTreeSet<String>) -> Result<(), StorageError> {
                if self.refusing.load(std::sync::atomic::Ordering::SeqCst) {
                    return Err(StorageError::Io { file: format!("nodes/{m}/tags"), reason: "No space left on device (os error 28)".into() });
                }
                self.inner.index_tags(m, id, l, a).await
            }
            async fn tagged(&self, l: &str) -> Result<BTreeSet<NodeId>, StorageError> { self.inner.tagged(l).await }
            async fn name_claim(&self, n: &PathName) -> Result<Option<NodeId>, StorageError> { self.inner.name_claim(n).await }
            async fn claim_node_name(&self, n: &PathName, id: &NodeId) -> Result<Result<(), NameHeld>, StorageError> { self.inner.claim_node_name(n, id).await }
            async fn release_node_name(&self, n: &PathName, id: &NodeId) -> Result<(), StorageError> { self.inner.release_node_name(n, id).await }
        }
        let store = IndexRefusing::default();
        let written = WrittenRows::default();
        let (name, node_id): (PathName, NodeId) = ("mesh1.rpc.1".parse().unwrap(), NodeId::mint());
        written.update(&store, &node_id, Some(NodeRow::new(name.clone(), node_id.clone(), crate::model::NodeStatus::ReadyForTraffic)), |_| ()).await.unwrap();
        store.refusing.store(true, std::sync::atomic::Ordering::SeqCst);
        let refused = written.update(&store, &node_id, None, |r| { r.tags.insert("red".into()); }).await;
        assert!(refused.is_err(), "the index write's refusal is named");
        assert!(store.inner.node(&node_id).await.unwrap().unwrap().tags == ["red".to_string()].into(), "the record stands with the label");
        assert!(store.inner.tagged("red").await.unwrap().is_empty(), "the index does not name it yet");
        store.refusing.store(false, std::sync::atomic::Ordering::SeqCst);
        // The next write of the node changes nothing of its tags: the owed move goes with it.
        written.update(&store, &node_id, None, |r| { r.config.insert("tier".into(), "gold".into()); }).await.unwrap();
        assert_eq!(store.inner.tagged("red").await.unwrap(), [node_id.clone()].into(), "the index matches the record afterwards");
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
        async fn node_at(&self, name: &crate::model::PathName) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.0.node_at(name).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.0.nodes(mesh).await
        }
        async fn put_node(&self, row: &crate::storage::NodeRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_node(row).await
        }
        async fn remove_node(&self, mesh: &str, node_id: &crate::model::NodeId) -> Result<(), crate::storage_error::StorageError> {
            self.0.remove_node(mesh, node_id).await
        }
        async fn index_path(&self, name: &crate::model::PathName, node_id: &crate::model::NodeId) -> Result<(), crate::storage_error::StorageError> {
            self.0.index_path(name, node_id).await
        }
        async fn index_tags(&self, mesh: &str, node_id: &crate::model::NodeId, leaving: &std::collections::BTreeSet<String>, arriving: &std::collections::BTreeSet<String>) -> Result<(), crate::storage_error::StorageError> {
            self.0.index_tags(mesh, node_id, leaving, arriving).await
        }
        async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<crate::model::NodeId>, crate::storage_error::StorageError> {
            self.0.tagged(label).await
        }
        async fn name_claim(&self, name: &crate::model::PathName) -> Result<Option<crate::model::NodeId>, crate::storage_error::StorageError> {
            self.0.name_claim(name).await
        }
        async fn claim_node_name(&self, name: &crate::model::PathName, node_id: &crate::model::NodeId) -> Result<Result<(), crate::storage::NameHeld>, crate::storage_error::StorageError> {
            self.0.claim_node_name(name, node_id).await
        }
        async fn release_node_name(&self, name: &crate::model::PathName, node_id: &crate::model::NodeId) -> Result<(), crate::storage_error::StorageError> {
            self.0.release_node_name(name, node_id).await
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
        let mesh = MeshRecord::new(MeshId::mint(), "mesh1");
        let (own, peer) = (node("mesh1.admin.1"), node("mesh1.rpc.1"));
        let runtime = RuntimeRow { node_id: peer.node_id.clone(), name: peer.name.clone(), incarnation_id: peer.incarnation_id.clone(), runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-1")).unwrap(), data_dir: None };
        let (up, down) = (fact(ConnectionState::Connected), fact(ConnectionState::Disconnected));
        let check = async |m: &dyn MeshStorage, n: &dyn NodesStorage, c: &dyn ConnectionsStorage, fresh: bool| {
            if fresh {
                assert_eq!(m.mesh(&mesh.mesh_id).await.unwrap(), None);
                assert_eq!(crate::storage::claim_mesh(&*m, None, &mesh).await.unwrap(), Ok(MeshClaim::Written { over: None }));
                n.put_runtime(&runtime).await.unwrap();
                c.put_connection(&up).await.unwrap();
                c.put_connection(&down).await.unwrap();
            }
            assert_eq!(m.mesh(&mesh.mesh_id).await.unwrap(), Some(mesh.clone()));
            assert_eq!(m.mesh_named("mesh1").await.unwrap(), Some(mesh.clone()), "the name claim's lookup names the record");
            assert_eq!(m.meshes().await.unwrap(), [mesh.clone()]);
            assert_eq!(n.runtimes().await.unwrap(), [runtime.clone()]);
            assert_eq!(c.connections().await.unwrap(), [down.clone()], "the latest fact per (source, destination, kind)");
        };
        let mem = (MemoryMeshStorage::default(), MemoryNodesStorage::default(), MemoryConnectionsStorage::default());
        check(&mem.0, &mem.1, &mem.2, true).await;
        let (root, w, me): (_, IncarnationId, PathName) = (crate::store_root::StoreRoot::at(&d.join("root")), IncarnationId::mint(), "mesh1.admin.1".parse().unwrap());
        check(&FileMeshStorage::open(&root, &w).await.unwrap(), &FileNodesStorage::open(&d, &root, "mesh1", &w).await.unwrap(), &FileConnectionsStorage::open(&root, &me, &w).await.unwrap(), true).await;
        let (m, n, c) = (FileMeshStorage::open(&root, &w).await.unwrap(), FileNodesStorage::open(&d, &root, "mesh1", &w).await.unwrap(), FileConnectionsStorage::open(&root, &me, &w).await.unwrap());
        check(&m, &n, &c, false).await;
        c.remove_connection(&down.index()).await.unwrap();
        assert!(c.connections().await.unwrap().is_empty());
        // The own row is the admin's own seed in its data dir, read before any store opens.
        assert_eq!(FileNodesStorage::own_in(&d, &w).await.unwrap(), None);
        FileNodesStorage::put_own_in(&d, &w, &own).await.unwrap();
        assert_eq!(FileNodesStorage::own_in(&d, &IncarnationId::mint()).await.unwrap(), Some(own.clone()), "the own row is read back by the next process");
        assert_eq!(own.gossip_addr().map(|a| a.id.to_string()), Some(own.endpoint_id.0.clone()));
        std::fs::write(root.dir().join("mesh-records").join(format!("{}.json", mesh.mesh_id)), br#"{"format":"mesh-record/9","record":{}}"#).unwrap();
        assert!(matches!(m.mesh(&mesh.mesh_id).await, Err(StorageError::Unrecognised { .. })));
    }

    fn row(name: &str) -> NodeRow {
        NodeRow::new(name.parse().unwrap(), NodeId::mint(), crate::model::NodeStatus::ReadyForTraffic)
    }

    /// CONTRACT (node-name-update.md, "Claim and move"; node-delete.md: a delete releases the node's
    /// name): each node's name is a claim beside its record, `NameHeld { by: node.id }` naming another
    /// node's live claim, and the `path.name` index names the record last written at a name, a
    /// `Deleted` one included. A claim the node holds is its own again; a delete's record keeps the
    /// index entry and releases the claim, so the next node created at the name takes it and the index
    /// then names that node. In memory and on disk alike.
    #[tokio::test]
    async fn a_nodes_name_is_a_claim_beside_its_record_and_the_path_index_names_the_record_at_it() {
        let d = crate::test_support::tempdir("node-claims");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let file = FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        let mem = MemoryNodesStorage::default();
        for n in [&file as &dyn NodesStorage, &mem as &dyn NodesStorage] {
            let w = WrittenRows::default();
            let a = row("mesh1.rpc.1");
            let _one = w.lock(&a.node_id).await;
            assert_eq!(crate::node_rows::take_name(n, &a.name, &a.node_id).await.unwrap(), Ok(()), "a free name is claimed");
            w.put(n, &a).await.unwrap();
            drop(_one);
            assert_eq!(n.node_at(&a.name).await.unwrap(), Some(a.clone()), "the index names the record at the name");
            assert_eq!(crate::node_rows::take_name(n, &a.name, &a.node_id).await.unwrap(), Ok(()), "the node's own claim is its own again");
            let other = row("mesh1.rpc.1");
            assert_eq!(crate::node_rows::take_name(n, &a.name, &other.node_id).await.unwrap(), Err(NameHeld { name: a.name.clone(), by: a.node_id.clone() }), "another node's live claim holds the name");
            crate::node_rows::write_deleted(n, &w, &a.name, &a.node_id, None).await.ok().expect("the delete's record");
            assert_eq!(n.name_claim(&a.name).await.unwrap(), None, "a delete releases the name");
            assert_eq!(n.node_at(&a.name).await.unwrap().map(|r| r.status), Some(crate::model::NodeStatus::Deleted), "the Deleted record stands at the name for the next create's seed");
            assert_eq!(crate::node_rows::take_name(n, &a.name, &other.node_id).await.unwrap(), Ok(()), "the next node takes the name");
            let _two = w.lock(&other.node_id).await;
            w.put(n, &other).await.unwrap();
            assert_eq!(n.node_at(&a.name).await.unwrap().map(|r| r.node_id), Some(other.node_id.clone()), "the index names the new node");
            assert_eq!(n.node(&a.node_id).await.unwrap().map(|r| r.status), Some(crate::model::NodeStatus::Deleted), "the deleted record stands, read by id");
        }
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (node-name-update.md, "Claim and move": "a retry, or a successor writer resuming it,
    /// finds the new name claimed by this node and continues from the record write; a claim held by
    /// this node whose record does not carry the name is that unfinished step, never `NameHeld` against
    /// itself"; storage-whole-records.md cell: "a rename's writer killed after the new name claim and
    /// before the record write: the resumed step finds its own claim and completes the rename; a second
    /// rename to that name by another node is `NameHeld`").
    #[tokio::test]
    async fn a_rename_resumed_after_its_claim_and_before_its_record_write_completes_and_another_nodes_rename_to_it_is_held() {
        let d = crate::test_support::tempdir("rename-resumed");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let file = FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let from = row("mesh1.rpc.1");
        let first = WrittenRows::default();
        first.update(&file, &from.node_id, Some(from.clone()), |_| ()).await.unwrap();
        let to: PathName = "mesh1.rpc.9".parse().unwrap();
        // The first writer claimed the new name and was killed before the record write.
        assert_eq!(file.claim_node_name(&to, &from.node_id).await.unwrap(), Ok(()));
        // Its successor resumes the step from what storage holds.
        let successor = WrittenRows::default();
        let mut record = crate::model::Node::allocated(from.name.clone());
        record.node_id = from.node_id.clone();
        record.status = from.status;
        let (moved, written) = crate::node_rows::move_row(&file, &successor, &me, &record, &from.name, &to).await.expect("the resumed rename completes");
        assert_eq!(moved, crate::node_rows::Moved::Renamed);
        assert_eq!(written.map(|r| r.name), Some(to.clone()));
        assert_eq!(file.node(&from.node_id).await.unwrap().map(|r| (r.name, r.renamed_from)), Some((to.clone(), Some(from.name.clone()))), "the record carries the new name and the name it came from");
        assert_eq!(file.name_claim(&from.name).await.unwrap(), None, "the old name's claim is released");
        let other = row("mesh1.rpc.2");
        let mut theirs = crate::model::Node::allocated(other.name.clone());
        theirs.node_id = other.node_id.clone();
        successor.update(&file, &other.node_id, Some(other.clone()), |_| ()).await.unwrap();
        assert_eq!(crate::node_rows::move_row(&file, &successor, &me, &theirs, &other.name, &to).await.err(), Some(crate::node_rows::RenameRefused::NameHeld(NameHeld { name: to.clone(), by: from.node_id.clone() })), "another node's rename to the name is NameHeld");
        let _ = std::fs::remove_dir_all(&d);
    }

    fn keys(dir: &std::path::Path) -> Vec<String> {
        let mut k: Vec<String> = std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).filter(|n| n.ends_with(".json")).collect();
        k.sort();
        k
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
        let (data1, data2) = (d.join("mesh1.admin.1"), d.join("mesh2.admin.1"));
        let (inc1, inc2) = (IncarnationId::mint(), IncarnationId::mint());
        let (n1, n2) = (FileNodesStorage::open(&data1, &root, "mesh1", &inc1).await.unwrap(), FileNodesStorage::open(&data2, &root, "mesh2", &inc2).await.unwrap());
        let (m1, m2) = (FileMeshStorage::open(&root, &inc1).await.unwrap(), FileMeshStorage::open(&root, &inc2).await.unwrap());
        // The fabric's first state-commit published the store: what follows writes to it.
        root.scratchpad().expect("a root without the Fabric row opens with a scratchpad").publish().await.unwrap();
        let writes = async |n: &FileNodesStorage, m: &FileMeshStorage, mesh: &str| {
            let w = WrittenRows::default();
            crate::storage::claim_mesh(&*m, None, &MeshRecord::new(MeshId::mint(), mesh)).await.unwrap().expect("each mesh claims a name no mesh holds");
            let mut ids = Vec::new();
            for name in [format!("{mesh}.admin.1"), format!("{mesh}.rpc.1"), format!("{mesh}.rpc.2")] {
                let r = row(&name);
                w.update(n, &r.node_id, Some(r.clone()), |_| ()).await.unwrap();
                ids.push(format!("{}.json", r.node_id));
            }
            ids.sort();
            ids
        };
        let (ids1, ids2) = tokio::join!(writes(&n1, &m1, "mesh1"), writes(&n2, &m2, "mesh2"));
        for (mesh, ids) in [("mesh1", ids1), ("mesh2", ids2)] {
            assert_eq!(keys(&root.dir().join("nodes").join(mesh).join("records")), ids, "{mesh}'s records, keyed by node id, under its own mesh");
        }
        assert_eq!(keys(&root.dir().join("mesh-records")).len(), 2, "one record per mesh");
        for data in [&data1, &data2] {
            assert!(!data.join("mesh-records").exists() && !data.join("nodes").join("records").exists(), "{} holds a shared record", data.display());
        }
        assert!(n1.node_at(&"mesh2.rpc.1".parse().unwrap()).await.unwrap().is_some(), "mesh1's primary reads mesh2's record from the shared root");
        assert!(n2.node_at(&"mesh1.rpc.1".parse().unwrap()).await.unwrap().is_some(), "mesh2's primary reads mesh1's record from the shared root");
        assert_eq!(live_records(&m1, &n1, "mesh1").await.unwrap().len(), 6, "a restart reads every mesh's records");
    }

    /// CONTRACT (R-TG1: "node.get with a tag filter names a label and answers every node carrying it,
    /// through a tags index declared on the node store (a point read per label)"; storage-whole-records.md:
    /// "RDM computes the index moves from its cached record before the write and the store writes the
    /// index entries blindly"): one file per (label, node id) under `nodes/<mesh>/tags/<label>/`. Two
    /// meshes' records carrying `red` are read by one label read across the root, a second store of
    /// another incarnation included; a write without a label tombstones that label's file and no
    /// other; a rename keeps the node's labels (the index is keyed by node id); a `Deleted` record
    /// tombstones every label it carries. A label no record ever carried reads as carried by none and
    /// makes no directory; a key that is not a label is refused by name and nothing is read.
    #[tokio::test]
    async fn the_tags_index_is_one_file_per_label_and_node_kept_with_every_row_write_and_read_per_label() {
        let d = crate::test_support::tempdir("tags-index");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = crate::store_root::StoreRoot::open(&store, &crate::model::FabricId::mint()).unwrap();
        let (n1, n2) = (FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap(), FileNodesStorage::open(&d.join("mesh2.admin.1"), &root, "mesh2", &IncarnationId::mint()).await.unwrap());
        root.scratchpad().expect("a root without the Fabric row opens with a scratchpad").publish().await.unwrap();
        let (w1, w2) = (WrittenRows::default(), WrittenRows::default());
        let tags = |l: &[&str]| l.iter().map(|t| t.to_string()).collect::<BTreeSet<String>>();
        let row_a = NodeRow { tags: tags(&["rabbit", "red"]), ..row("mesh1.rpc.1") };
        let a = row_a.node_id.clone();
        w1.update(&n1, &a, Some(row_a.clone()), |_| ()).await.unwrap();
        let row_b = NodeRow { tags: tags(&["red"]), ..row("mesh2.rpc.1") };
        let b = row_b.node_id.clone();
        w2.update(&n2, &b, Some(row_b.clone()), |_| ()).await.unwrap();
        let files = |mesh: &str, label: &str| keys(&root.dir().join("nodes").join(mesh).join("tags").join(label));
        assert_eq!((files("mesh1", "rabbit"), files("mesh1", "red"), files("mesh2", "red")), (vec![format!("{a}.json")], vec![format!("{a}.json")], vec![format!("{b}.json")]), "one file per (label, node id), under the node's mesh");
        let other = FileNodesStorage::open(&d.join("mesh1.admin.2"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        for n in [&n1, &n2, &other] {
            assert_eq!(n.tagged("red").await.unwrap(), [a.clone(), b.clone()].into(), "one label read answers every mesh's nodes carrying it");
        }
        // A write without rabbit tombstones rabbit's row only.
        w1.update(&n1, &a, None, |r| { r.tags.remove("rabbit"); }).await.unwrap();
        assert_eq!((files("mesh1", "rabbit").len(), other.tagged("rabbit").await.unwrap().len(), other.tagged("red").await.unwrap().len()), (0, 0, 2), "the dropped label's row is tombstoned, the kept one stands");
        // A rename keeps the labels: the record moves to its new name.
        w1.update(&n1, &a, None, |r| r.name = "mesh1.rpc.1.old".parse().unwrap()).await.unwrap();
        assert_eq!(other.tagged("red").await.unwrap(), [a.clone(), b.clone()].into(), "a renamed node keeps its labels");
        // A Deleted record tombstones every label it carries, and keeps them on the record.
        w1.update(&n1, &a, None, |r| r.status = crate::model::NodeStatus::Deleted).await.unwrap();
        w2.update(&n2, &b, None, |r| r.status = crate::model::NodeStatus::Deleted).await.unwrap();
        assert!(other.tagged("red").await.unwrap().is_empty(), "neither deleted node is named under red");
        assert_eq!(n1.node(&a).await.unwrap().map(|r| r.tags), Some(tags(&["red"])), "the Deleted record keeps its tags");
        // A label no record carried: none, and no directory made.
        assert!(other.tagged("blue").await.unwrap().is_empty());
        assert!(!root.dir().join("nodes").join("mesh1").join("tags").join("blue").exists(), "a read makes no directory");
        assert!(matches!(other.tagged("../rows").await, Err(StorageError::Unrecognised { reason, .. }) if reason.contains("\"../rows\"")), "a key that is not a label is refused by name");
    }

    /// CONTRACT (node-replace.md Inherited: the successor inherits the retained predecessor's tags,
    /// "Retained predecessor labels are removed from the active index, not erased from its row"):
    /// between the successor's claim at the name and the retained record's mark, both nodes carry the
    /// label; the retained record written again with `inherited_by` keeps its tags and the tags index
    /// names none of them for it; a later write of it (a tags or config write) indexes none again, an
    /// added label included, while the successor stays named.
    #[tokio::test]
    async fn a_row_a_successor_inherited_keeps_its_tags_and_holds_none_in_the_tags_index() {
        let d = crate::test_support::tempdir("tags-inherited");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = crate::store_root::StoreRoot::open(&store, &crate::model::FabricId::mint()).unwrap();
        let n1 = FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        root.scratchpad().expect("a root without the Fabric row opens with a scratchpad").publish().await.unwrap();
        let w = WrittenRows::default();
        let tags = |l: &[&str]| l.iter().map(|t| t.to_string()).collect::<BTreeSet<String>>();
        let old = NodeRow { tags: tags(&["rabbit"]), ..row("mesh1.rpc.1.old") };
        let succ_row = NodeRow { tags: tags(&["rabbit"]), ..row("mesh1.rpc.1") };
        let (pred, succ) = (old.node_id.clone(), succ_row.node_id.clone());
        w.update(&n1, &pred, Some(old.clone()), |_| ()).await.unwrap();
        w.update(&n1, &succ, Some(succ_row.clone()), |_| ()).await.unwrap();
        assert_eq!(n1.tagged("rabbit").await.unwrap(), [pred.clone(), succ.clone()].into(), "between the successor's claim and the mark both carry the label");
        w.update(&n1, &pred, None, |r| r.inherited_by = Some(succ.clone())).await.unwrap();
        assert_eq!(n1.tagged("rabbit").await.unwrap(), [succ.clone()].into(), "the retained record's label left the index");
        assert_eq!(n1.node(&pred).await.unwrap().map(|r| (r.tags, r.inherited_by)), Some((tags(&["rabbit"]), Some(succ.clone()))), "the retained record keeps its tags and names who inherited it");
        w.update(&n1, &pred, None, |r| { r.tags.insert("red".into()); }).await.unwrap();
        assert_eq!((n1.tagged("rabbit").await.unwrap(), n1.tagged("red").await.unwrap()), ([succ.clone()].into(), BTreeSet::new()), "a later write of the inherited record indexes none of its labels");
    }

    /// CONTRACT (R14 items 1 and 9; acceptance "an ordinary node's connections rows land under
    /// `connections/<node>/` in the shared root and the successor mesh-primary reads them there";
    /// states.md, Setting a state): before the fabric's first state-commit a node holds its connections rows
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

    /// A record file of a fresh file store that is a FIFO: a read of it waits until a writer opens it.
    /// The writer is a thread that, once a reader has the FIFO open, waits for word from the runtime
    /// that it still runs (up to 5 s), then writes `row`. Whether that word came, while the read was
    /// waiting, is what this answers.
    fn runtime_runs_while_a_row_is_read<T: Send + 'static>(row: NodeRow, op: impl FnOnce(std::sync::Arc<FileNodesStorage>) -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>> + Send + 'static) -> (bool, T) {
        let bytes = serde_json::to_vec(&serde_json::json!({ "format": RECORD_FORMAT, "record": row })).unwrap();
        runtime_runs_while_a_file_is_read("root/nodes/mesh1/records", row.node_id.as_str(), bytes, op)
    }

    /// The same, for the record file `<key>.json` under the store's `dir`, written `bytes`.
    fn runtime_runs_while_a_file_is_read<T: Send + 'static>(dir: &str, key: &str, bytes: Vec<u8>, op: impl FnOnce(std::sync::Arc<FileNodesStorage>) -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>> + Send + 'static) -> (bool, T) {
        let d = crate::test_support::tempdir("rows-offload");
        let fifo = d.join(dir).join(format!("{key}.json"));
        crate::test_support::runtime_runs_while_a_fifo_is_read(fifo, bytes, Box::pin(async move { op(std::sync::Arc::new(FileNodesStorage::open(&d, &crate::store_root::StoreRoot::at(&d.join("root")), "mesh1", &IncarnationId::mint()).await.unwrap())).await }))
    }

    /// CONTRACT: a point read of a node record never waits on the disk on a runtime worker (the
    /// defect class that froze node-admins on 10-08: a worker blocked on the disk): it runs on the
    /// blocking pool, as the record's write does (`blocking::blocking_io`). The cell: a record file that
    /// is a FIFO holds the read until a writer comes; the runtime's only thread keeps running meanwhile,
    /// and the read answers the record.
    #[test]
    fn a_node_row_read_waits_on_the_disk_off_the_runtime_thread() {
        let row = row("mesh1.rpc.1");
        let node_id = row.node_id.clone();
        let (ran, read) = runtime_runs_while_a_row_is_read(row.clone(), move |s| Box::pin(async move { s.node(&node_id).await }));
        assert!(ran, "the runtime thread ran while the record read waited on the disk");
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

    /// CONTRACT (node-identity-and-state.md §2.7: the mesh-primary "claims the mesh's name at the key
    /// `(path, name)` ... Held means held by a different MeshId: `NameHeld` names that holder.
    /// The same MeshId re-claiming its own name ... is idempotent and accepted"; R-MN1: only a delete
    /// releases a name, a mesh's by its removal, the record Dead; states.md: a mesh id is never
    /// reissued, dead is final; Q109: the storage decides nothing): the record's writer claims a
    /// mesh's name. A free name takes the claim and the record; the same mesh re-claiming with its
    /// record unchanged writes nothing; another mesh's claim of the live name is refused naming the
    /// holder and writes nothing; the holder's Dead record is a write of its own record that releases
    /// the name, and a repeat of it writes nothing; the Dead mesh is never written live again; another
    /// mesh then claims the name. The memory store and the file store each run their own check.
    #[tokio::test]
    async fn a_mesh_name_is_claimed_by_its_writer_and_only_a_dead_row_releases_it() {
        let d = crate::test_support::tempdir("storage-mesh-claim");
        let check = async |m: &dyn MeshStorage, store: &str| {
            let (first, second) = (MeshId::mint(), MeshId::mint());
            let live = |id: &MeshId| MeshRecord::new(id.clone(), "mesh2");
            let dead = |id: &MeshId| MeshRecord { dead: true, ..live(id) };
            assert_eq!(crate::storage::claim_mesh(&*m, None, &live(&first)).await, Ok(Ok(MeshClaim::Written { over: None })), "{store}: a free name takes the claim and the record");
            assert_eq!(crate::storage::claim_mesh(&*m, Some(&live(&first)), &live(&first)).await, Ok(Ok(MeshClaim::Held)), "{store}: the same mesh re-claiming its name writes nothing");
            assert_eq!(crate::storage::claim_mesh(&*m, None, &live(&second)).await, Ok(Err(MeshClaimRefused::NameHeld { name: "mesh2".into(), by: first.clone() })), "{store}: another mesh's live claim holds mesh2");
            assert_eq!(m.mesh_named("mesh2").await.unwrap(), Some(live(&first)), "{store}: nothing is written by a refused claim");
            assert_eq!(crate::storage::claim_mesh(&*m, Some(&live(&first)), &dead(&first)).await, Ok(Ok(MeshClaim::Written { over: None })), "{store}: the holder's Dead record is a write of its own record");
            assert_eq!(m.mesh_named("mesh2").await.unwrap(), None, "{store}: the Dead record released the name");
            assert_eq!(crate::storage::claim_mesh(&*m, Some(&dead(&first)), &dead(&first)).await, Ok(Ok(MeshClaim::Held)), "{store}: a repeat of the Dead record writes nothing");
            assert_eq!(crate::storage::claim_mesh(&*m, Some(&dead(&first)), &live(&first)).await, Ok(Err(MeshClaimRefused::MeshDead { name: "mesh2".into(), mesh_id: first.clone() })), "{store}: a dead mesh is never written live again");
            assert_eq!(crate::storage::claim_mesh(&*m, None, &live(&second)).await, Ok(Ok(MeshClaim::Written { over: None })), "{store}: the released name is claimed again");
            assert_eq!(m.mesh_named("mesh2").await.unwrap(), Some(live(&second)), "{store}: the claim names the new mesh's record");
            assert_eq!(m.mesh(&first).await.unwrap(), Some(dead(&first)), "{store}: the dead mesh's record stands, read by id");
        };
        check(&MemoryMeshStorage::default(), "memory").await;
        check(&FileMeshStorage::open(&crate::store_root::StoreRoot::at(&d.join("root")), &IncarnationId::mint()).await.unwrap(), "file").await;
        let _ = std::fs::remove_dir_all(&d);
    }
}
