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
    /// retry, a replaced primary).
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
    /// attempt claim is a Build fact, durable when it is taken (Q61, build.md §5.2).
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
const MESH_KIND: &str = "mesh-record";
#[cfg(feature = "file-backend")]
const MESH_NAME_FORMAT: &str = "mesh-name/1";

#[cfg(feature = "file-backend")]
/// The format of a mesh's in-flight lock's claim.
const IN_FLIGHT_FORMAT: &str = "mesh-in-flight/2";

#[cfg(feature = "file-backend")]
/// A mesh name's claim: the mesh that holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MeshNameClaim {
    name: String,
    mesh_id: MeshId,
}

#[cfg(feature = "file-backend")]
/// A lock's content (R-SR5): the claim, in its format, and the birth that took it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LockClaim<T> {
    format: String,
    record: T,
    /// The birth that took the lock: its node id and incarnation.
    by: (String, String),
}

#[cfg(feature = "file-backend")]
/// `claim` as the content of a lock taken by `by`.
fn lock_content<T: Serialize>(format: &str, claim: &T, by: &crate::store_log::LogWriter) -> Vec<u8> {
    serde_json::to_vec(&LockClaim { format: format.to_string(), record: claim, by: (by.node_id.to_string(), by.incarnation_id.to_string()) }).expect("a lock's claim encodes")
}

#[cfg(feature = "file-backend")]
/// The claim a lock's content holds, refused by name when it is not a `format` claim.
fn lock_claim<T: serde::de::DeserializeOwned>(file: &Path, bytes: &[u8], format: &str) -> Result<T, StorageError> {
    let held: LockClaim<T> = serde_json::from_slice(bytes).map_err(|e| StorageError::Unrecognised { file: file.display().to_string(), reason: e.to_string() })?;
    if held.format != format {
        return Err(StorageError::Unrecognised { file: file.display().to_string(), reason: format!("a lock in format {:?}, this build reads {format:?} here", held.format) });
    }
    Ok(held.record)
}

#[cfg(feature = "file-backend")]
/// The mesh name locks under `locks/name/`.
struct MeshNames {
    locks: crate::store_log::StoreLocks,
    by: crate::store_log::LogWriter,
}

#[cfg(feature = "file-backend")]
impl MeshNames {
    fn path(&self, name: &str) -> std::path::PathBuf {
        self.locks.path(crate::store_log::LockKind::Name, name)
    }

    async fn read(&self, name: &str) -> Result<Option<MeshNameClaim>, StorageError> {
        match self.locks.read(crate::store_log::LockKind::Name, name).await? {
            Some(bytes) => lock_claim(&self.path(name), &bytes, MESH_NAME_FORMAT).map(Some),
            None => Ok(None),
        }
    }

    async fn take(&self, claim: &MeshNameClaim) -> Result<Result<(), MeshId>, StorageError> {
        match self.locks.take(crate::store_log::LockKind::Name, &claim.name, &lock_content(MESH_NAME_FORMAT, claim, &self.by)).await? {
            crate::store_log::LockTaken::Won => Ok(Ok(())),
            crate::store_log::LockTaken::Held(bytes) => {
                let standing: MeshNameClaim = lock_claim(&self.path(&claim.name), &bytes, MESH_NAME_FORMAT)?;
                Ok(if standing.mesh_id == claim.mesh_id { Ok(()) } else { Err(standing.mesh_id) })
            }
        }
    }

    async fn release(&self, name: &str, mesh_id: &MeshId) -> Result<(), StorageError> {
        let (file, mesh_id) = (self.path(name), mesh_id.clone());
        self.locks.release(crate::store_log::LockKind::Name, name, move |b| lock_claim::<MeshNameClaim>(&file, b, MESH_NAME_FORMAT).is_ok_and(|c| c.mesh_id == mesh_id)).await.map(|_| ())
    }
}

#[cfg(feature = "file-backend")]
/// Each write of a Mesh record, whole, one line of the root's `meshes.jsonl` at its mesh id (R-SR1,
/// R-SR2), the latest line the record; each live mesh's name claim, the lock `locks/name/<name>`; and
/// each mesh name's attempt in flight, the lock `locks/in-flight/<mesh name>` ([`crate::mesh_claims`];
/// R-SR5).
pub struct FileMeshStorage {
    records: crate::store_log::StoreLog,
    names: std::sync::Arc<MeshNames>,
    locks: crate::store_log::StoreLocks,
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
    /// Open the store under the fabric's store root, written by `writer`: its name locks and its
    /// in-flight locks taken at once (an attempt claim is a Build fact, durable when it is taken: Q61,
    /// build.md §5.2).
    pub async fn open(root: &crate::store_root::StoreRoot, writer: &crate::store_log::LogWriter) -> Result<Self, StorageError> {
        let locks = crate::store_log::StoreLocks::at(root.dir(), writer);
        let names = std::sync::Arc::new(MeshNames { locks: locks.clone(), by: writer.clone() });
        Ok(Self { records: root.log(crate::store_root::MESHES_FILE, crate::store_log::Formats::new().with::<MeshRecord>(MESH_FORMAT)).await?, names, locks, log_writer: writer.clone() })
    }

    /// Finishes every write of the Mesh records and name claims this store has issued
    /// (node-primary-update.md step 3).
    pub async fn finish_issued_writes(&self) {
        self.records.finish_issued_writes().await;
    }

    fn in_flight_path(&self, mesh: &str) -> std::path::PathBuf {
        self.locks.path(crate::store_log::LockKind::InFlight, mesh)
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl MeshStorage for FileMeshStorage {
    async fn mesh(&self, mesh_id: &MeshId) -> Result<Option<MeshRecord>, StorageError> {
        self.records.latest(MESH_KIND, mesh_id.as_str()).await?.map(|l| l.decode(self.records.path())).transpose()
    }
    async fn mesh_named(&self, name: &str) -> Result<Option<MeshRecord>, StorageError> {
        let Some(claim) = self.names.read(name).await? else { return Ok(None) };
        self.mesh(&claim.mesh_id).await
    }
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError> {
        self.records.append(&self.log_writer, MESH_FORMAT, record.mesh_id.as_str(), record).await
    }
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
        self.records.latest_of(MESH_KIND).await?.iter().map(|l| l.decode(self.records.path())).collect()
    }
    async fn claim_mesh_name(&self, name: &str, mesh_id: &MeshId) -> Result<Result<(), MeshId>, StorageError> {
        self.names.take(&MeshNameClaim { name: name.to_string(), mesh_id: mesh_id.clone() }).await
    }
    async fn release_mesh_name(&self, name: &str, mesh_id: &MeshId) -> Result<(), StorageError> {
        self.names.release(name, mesh_id).await
    }
    async fn take_in_flight(&self, mesh: &str, claim: &InFlight) -> Result<Taken, StorageError> {
        match self.locks.take(crate::store_log::LockKind::InFlight, mesh, &lock_content(IN_FLIGHT_FORMAT, claim, &self.log_writer)).await? {
            crate::store_log::LockTaken::Won => Ok(Taken::Won),
            crate::store_log::LockTaken::Held(bytes) => {
                let held: InFlight = lock_claim(&self.in_flight_path(mesh), &bytes, IN_FLIGHT_FORMAT)?;
                Ok(if held.same_claim(claim) { Taken::Won } else { Taken::Held(held) })
            }
        }
    }
    async fn release_in_flight(&self, mesh: &str, attempt: &InFlight) -> Result<(), StorageError> {
        let (file, attempt) = (self.in_flight_path(mesh), attempt.clone());
        self.locks.release(crate::store_log::LockKind::InFlight, mesh, move |b| lock_claim::<InFlight>(&file, b, IN_FLIGHT_FORMAT).is_ok_and(|held| held.same_attempt(&attempt))).await.map(|_| ())
    }
    async fn in_flight(&self, mesh: &str) -> Result<Option<InFlight>, StorageError> {
        match self.locks.read(crate::store_log::LockKind::InFlight, mesh).await? {
            Some(bytes) => lock_claim(&self.in_flight_path(mesh), &bytes, IN_FLIGHT_FORMAT).map(Some),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------- facts on a node's record

pub use rdm_node::nodes_storage::{AppTypes, Allocation, BirthFacts, BirthRef, CertDates, DataDirLost, DataDirUnreadable, NodeOperation, NodeRow, RuntimeRow, SigningCertDates, BIRTHS_KEPT};

/// The address of `birth` and the primaries recorded for it leave its facts: a birth whose exit is
/// proven is no node to reach and holds no primary (mesh-delete.md step 7, "Deleted births resolve
/// Gone"). The mesh-primary's write, never a node's: a node records no primary (rdm-node's
/// `node_base_holds_no_comparator`).
pub fn clear_address_and_primaries(birth: &mut BirthFacts) {
    birth.endpoint_id = None;
    birth.transport_addr = None;
    birth.listeners.clear();
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
    /// The `topology_storage_provider` this birth writes (storage.md §2), kept on the own row in its
    /// data dir and written again at each move: a start with no launcher opens the provider it names
    /// ("a restart with no launcher (the adoption launch) | the node itself | its own row in its data
    /// dir"). A row written before it carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology_storage_provider: Option<String>,
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
/// (whose record a Day-0 admin's claim precedes), each mesh read once, a `Deleted` record included: a
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
/// index entry and its tags index rows, and every birth's runtime row. Nothing is removed (R-SR2): a
/// create withdrawn before its node ran leaves its record `Withdrawn`.
#[async_trait]
pub trait NodesStorage: Send + Sync {
    /// Keep the runtime of one birth (a blind put of its own row).
    async fn put_runtime(&self, row: &RuntimeRow) -> Result<(), StorageError>;
    /// Every birth's runtime row the store holds; folded by the reader.
    async fn runtimes(&self) -> Result<Vec<RuntimeRow>, StorageError>;
    /// The runtime row of the exact birth `(node_id, incarnation)`: a point read by its key.
    async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<RuntimeRow>, StorageError>;
    /// The record of `node_id`: a point read of its key, a `Withdrawn` one included; `None` when none
    /// was ever written.
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, StorageError>;
    /// The record the `path.name` index names at `name`: the node whose record was last written at it,
    /// a `Deleted` one included; `None` when no record was ever written there, or when that record is
    /// `Withdrawn`.
    async fn node_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError>;
    /// Every node record of `mesh`, a `Withdrawn` one skipped.
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRow>, StorageError>;
    /// Write `row` whole at its node id, as given (Q109: storage stores and returns what it is given).
    /// Durable before it answers (Q54).
    async fn put_node(&self, row: &NodeRow) -> Result<(), StorageError>;
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
        Ok(self.records.lock().unwrap().get(&id).filter(|r| r.status != crate::model::NodeStatus::Withdrawn).cloned())
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRow>, StorageError> {
        Ok(self.records.lock().unwrap().values().filter(|r| r.name.mesh == mesh && r.status != crate::model::NodeStatus::Withdrawn).cloned().collect())
    }
    async fn put_node(&self, row: &NodeRow) -> Result<(), StorageError> {
        self.records.lock().unwrap().insert(row.node_id.clone(), row.clone());
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
/// The format of the admin's own row in its data dir ([`FileNodesStorage::own_in`]).
const NODE_FORMAT: &str = "node-record/4";
#[cfg(feature = "file-backend")]
use rdm_node::nodes_storage::{TagRow, RECORD_KIND, RECORD_FORMAT, RUNTIME_FORMAT, RUNTIME_KIND, TAG_FORMAT, TAG_KIND};
#[cfg(feature = "file-backend")]
const NAME_CLAIM_FORMAT: &str = "node-name/1";

#[cfg(feature = "file-backend")]
/// A name claim: the node it names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct NameEntry {
    name: PathName,
    node_id: NodeId,
}

#[cfg(feature = "file-backend")]
/// The nodes' records, their runtime rows and the tags index's changes, each write one line of the
/// root's `nodes.jsonl` (R-SR1, R-SR2), appended straight to the file: a record at its node id, written by its mesh's writer, durable before the answer
/// (Q54); a runtime row at `<node id>--<incarnation>`, written once by its birth's launching admin, the
/// durable map a successor proves exits from, as durable as when it stood in the launcher's data dir;
/// and a tag change at `<node id>/<label>`. The `path.name` index and the tags index are the fold. Each name claim is the
/// lock `locks/name/<path.name>` of the root (R-SR5). The admin's own row stays its own seed in its
/// data dir, `nodes/self.json` ([`Self::own_in`]).
#[derive(Debug)]
pub struct FileNodesStorage {
    /// The records, the runtime rows and the tags index's changes, and their point reads.
    rows: rdm_node::nodes_storage::FileNodeRows,
    /// The rows' file, appended to.
    records: crate::store_log::StoreLog,
    log_writer: crate::store_log::LogWriter,
    /// The name claims: `locks/name/<path.name>`.
    locks: crate::store_log::StoreLocks,
}

#[cfg(feature = "file-backend")]
impl FileNodesStorage {
    /// Open the store on the fabric's store `root`, written by `writer`. `own_data_dir` and `own_mesh`
    /// are this admin's (its own row is read from the one, [`Self::own_in`]); the store is the
    /// fabric's, every mesh's nodes in one file.
    pub async fn open(own_data_dir: &Path, root: &crate::store_root::StoreRoot, own_mesh: &str, writer: &crate::store_log::LogWriter) -> Result<Self, StorageError> {
        let _ = (own_data_dir, own_mesh);
        let rows = rdm_node::nodes_storage::FileNodeRows::open(root).await?;
        let records = rows.log().clone();
        Ok(Self { rows, records, log_writer: writer.clone(), locks: crate::store_log::StoreLocks::at(root.dir(), writer) })
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

    /// Finishes every write of the node records, the runtime rows and the tags index this store has
    /// issued (node-primary-update.md step 3); a name lock is taken or removed before its call answers.
    pub async fn finish_issued_writes(&self) {
        self.records.finish_issued_writes().await;
    }

    fn name_lock(&self, name: &PathName) -> std::path::PathBuf {
        self.locks.path(crate::store_log::LockKind::Name, &name.to_string())
    }

}

#[cfg(feature = "file-backend")]
#[async_trait]
impl NodesStorage for FileNodesStorage {
    /// A blind put of the birth's own row: one append, nothing read first (its one writer is the admin
    /// that launched the birth, build.md §5.3).
    async fn put_runtime(&self, row: &RuntimeRow) -> Result<(), StorageError> {
        self.records.append(&self.log_writer, RUNTIME_FORMAT, &row.key(), row).await
    }
    async fn runtimes(&self) -> Result<Vec<RuntimeRow>, StorageError> {
        self.records.latest_of(RUNTIME_KIND).await?.iter().map(|l| l.decode(self.records.path())).collect()
    }
    async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<RuntimeRow>, StorageError> {
        self.records.latest(RUNTIME_KIND, &format!("{}--{}", node_id, incarnation.as_str())).await?.map(|l| l.decode(self.records.path())).transpose()
    }
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, StorageError> {
        rdm_node::nodes_storage::NodeRowsStorage::node(&self.rows, node_id).await
    }
    async fn node_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError> {
        rdm_node::nodes_storage::NodeRowsStorage::node_at(&self.rows, name).await
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRow>, StorageError> {
        let mut out = Vec::new();
        for l in self.records.latest_of(RECORD_KIND).await? {
            let row: NodeRow = l.decode(self.records.path())?;
            if row.name.mesh == mesh && row.status != crate::model::NodeStatus::Withdrawn {
                out.push(row);
            }
        }
        Ok(out)
    }
    async fn put_node(&self, row: &NodeRow) -> Result<(), StorageError> {
        self.records.append(&self.log_writer, RECORD_FORMAT, row.node_id.as_str(), row).await
    }
    /// The `path.name` index is the fold of the record lines: a record's own line is its entry.
    async fn index_path(&self, _name: &PathName, _node_id: &NodeId) -> Result<(), StorageError> {
        Ok(())
    }
    async fn index_tags(&self, _mesh: &str, node_id: &NodeId, leaving: &BTreeSet<String>, arriving: &BTreeSet<String>) -> Result<(), StorageError> {
        for (labels, carried) in [(leaving, false), (arriving, true)] {
            for l in labels {
                tag_key(l, &self.records.path().display().to_string())?;
                self.records.append(&self.log_writer, TAG_FORMAT, &format!("{node_id}/{l}"), &TagRow { label: l.clone(), node_id: node_id.clone(), carried }).await?;
            }
        }
        Ok(())
    }
    async fn tagged(&self, label: &str) -> Result<BTreeSet<NodeId>, StorageError> {
        tag_key(label, &self.records.path().display().to_string())?;
        let mut out = BTreeSet::new();
        for l in self.records.latest_of(TAG_KIND).await? {
            let row: TagRow = l.decode(self.records.path())?;
            if row.label == label && row.carried {
                out.insert(row.node_id);
            }
        }
        Ok(out)
    }
    async fn name_claim(&self, name: &PathName) -> Result<Option<NodeId>, StorageError> {
        match self.locks.read(crate::store_log::LockKind::Name, &name.to_string()).await? {
            Some(bytes) => Ok(Some(lock_claim::<NameEntry>(&self.name_lock(name), &bytes, NAME_CLAIM_FORMAT)?.node_id)),
            None => Ok(None),
        }
    }
    /// The claim is the lock `locks/name/<name>`, taken create-exclusive (R-SR5): a lock this node
    /// holds answers `Ok`, another node's `NameHeld` naming it.
    async fn claim_node_name(&self, name: &PathName, node_id: &NodeId) -> Result<Result<(), NameHeld>, StorageError> {
        let claim = NameEntry { name: name.clone(), node_id: node_id.clone() };
        match self.locks.take(crate::store_log::LockKind::Name, &name.to_string(), &lock_content(NAME_CLAIM_FORMAT, &claim, &self.log_writer)).await? {
            crate::store_log::LockTaken::Won => Ok(Ok(())),
            crate::store_log::LockTaken::Held(bytes) => {
                let held: NameEntry = lock_claim(&self.name_lock(name), &bytes, NAME_CLAIM_FORMAT)?;
                Ok(if held.node_id == *node_id { Ok(()) } else { Err(NameHeld { name: name.clone(), by: held.node_id }) })
            }
        }
    }
    /// The lock goes under the name's reclamation mutex, only while it still names `node_id` (R-SR5).
    async fn release_node_name(&self, name: &PathName, node_id: &NodeId) -> Result<(), StorageError> {
        let (file, node_id) = (self.name_lock(name), node_id.clone());
        self.locks.release(crate::store_log::LockKind::Name, &name.to_string(), move |b| lock_claim::<NameEntry>(&file, b, NAME_CLAIM_FORMAT).is_ok_and(|held| held.node_id == node_id)).await.map(|_| ())
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
    /// beside `Ready`, two attempts' operation effects, a lost and an unreadable data dir;
    /// the birth's cert dates are its last.
    async fn facts_of_one_birth_all_stand(first: &dyn NodesStorage, reopened: &dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Box<dyn NodesStorage>> + Send>>) {
        let (node_id, incarnation_id, name): (NodeId, IncarnationId, PathName) = (NodeId::mint(), IncarnationId::mint(), "mesh1.rpc.1".parse().unwrap());
        let written = WrittenRows::default();
        let base = NodeRow::new(name.clone(), node_id.clone(), crate::model::NodeStatus::Ready);
        let write = async |change: Box<dyn FnOnce(&mut BirthFacts) + Send>| {
            written.update(first, &node_id, Some(base.clone()), |r| change(r.birth_mut(&incarnation_id, &name))).await.unwrap();
        };
        write(Box::new(|b| { b.declared.insert(NodeState::Ready); })).await;
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
        assert_eq!(b.declared_state(), Some(NodeState::Ready), "a late Pending removes no Ready: the reader takes the greatest");
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
            async fn node_at(&self, n: &PathName) -> Result<Option<NodeRow>, StorageError> { self.log.lock().unwrap().push("read".into()); self.inner.node_at(n).await }
            async fn nodes(&self, m: &str) -> Result<Vec<NodeRow>, StorageError> { self.log.lock().unwrap().push("read".into()); self.inner.nodes(m).await }
            async fn put_node(&self, r: &NodeRow) -> Result<(), StorageError> { self.log.lock().unwrap().push("put".into()); self.puts.lock().unwrap().push(r.clone()); self.inner.put_node(r).await }
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
        let mut held = NodeRow::new(name.clone(), node_id.clone(), crate::model::NodeStatus::Ready);
        held.config.insert("tier".into(), "gold".into());
        held.tags.insert("red".into());
        written.hold(&held);
        written.update(&store, &node_id, None, |r| { r.birth_mut(&inc, &name).declared.insert(NodeState::Ready); }).await.unwrap();
        written.update(&store, &node_id, None, |r| r.birth_mut(&inc, &name).silence = Some(crate::model::NodeStatus::PendingReconnect)).await.unwrap();
        written.update(&store, &node_id, None, |r| r.birth_mut(&inc, &name).cert = Some(CertDates { issued_at: 1, expires_at: 9, issuer_expires_at: 99, renew_at: 6 })).await.unwrap();
        assert_eq!(*store.log.lock().unwrap(), ["put", "put", "put"], "three writes, no read before any of them");
        let puts = store.puts.lock().unwrap().clone();
        let last = puts.last().unwrap();
        assert_eq!((last.config.get("tier").map(String::as_str), last.tags == ["red".to_string()].into()), (Some("gold"), true), "the record's own fields go with every put");
        let b = last.birth(&inc).unwrap();
        assert!(b.declared.contains(&NodeState::Ready) && b.silence.is_some() && b.cert.is_some(), "the last put carries every fact the copy held: {b:?}");
        assert!(puts[1].birth(&inc).unwrap().declared.contains(&NodeState::Ready), "the second put carries the first's fact too");
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
        written.update(&store, &node_id, None, |r| { r.birth_mut(&inc, &name).declared.insert(NodeState::Ready); }).await.unwrap();
        written.fill(&store, "mesh1").await.unwrap();
        written.update(&store, &node_id, None, |r| r.birth_mut(&inc, &name).signing_cert = Some(SigningCertDates { issued_at: 1, expires_at: 9, renew_at: 6 })).await.unwrap();
        let stored = store.inner.node(&node_id).await.unwrap().unwrap();
        let b = stored.birth(&inc).unwrap();
        assert!(b.declared.contains(&NodeState::Ready) && b.signing_cert.is_some(), "the write between the listing and the fill stands beside the next: {b:?}");
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
        written.update(&store, &node_id, Some(NodeRow::new(name.clone(), node_id.clone(), crate::model::NodeStatus::Ready)), |_| ()).await.unwrap();
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
            topology_storage_provider: None,
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
        check(&FileMeshStorage::open(&root, &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &w)).await.unwrap(), &FileNodesStorage::open(&d, &root, "mesh1", &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &w)).await.unwrap(), &FileConnectionsStorage::open(&root, &me, &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &w)).await.unwrap(), true).await;
        let (m, n, c) = (FileMeshStorage::open(&root, &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &w)).await.unwrap(), FileNodesStorage::open(&d, &root, "mesh1", &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &w)).await.unwrap(), FileConnectionsStorage::open(&root, &me, &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &w)).await.unwrap());
        check(&m, &n, &c, false).await;
        c.remove_connection(&down.index()).await.unwrap();
        assert!(c.connections().await.unwrap().is_empty());
        // The own row is the admin's own seed in its data dir, read before any store opens.
        assert_eq!(FileNodesStorage::own_in(&d, &w).await.unwrap(), None);
        FileNodesStorage::put_own_in(&d, &w, &own).await.unwrap();
        assert_eq!(FileNodesStorage::own_in(&d, &IncarnationId::mint()).await.unwrap(), Some(own.clone()), "the own row is read back by the next process");
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
        let mut r = NodeRow::new("mesh1.rpc.1".parse().unwrap(), NodeId::mint(), S::Starting);
        r.birth_mut(&old, &r.name.clone());
        r.birth_mut(&birth, &r.name.clone());
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

    fn row(name: &str) -> NodeRow {
        NodeRow::new(name.parse().unwrap(), NodeId::mint(), crate::model::NodeStatus::Ready)
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
        let file = FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap();
        let mem = MemoryNodesStorage::default();
        for n in [&file as &dyn NodesStorage, &mem as &dyn NodesStorage] {
            let w = WrittenRows::default();
            let a = row("mesh1.rpc.1");
            let _one = w.lock(&a.node_id).await;
            assert_eq!(crate::node_rows::take_name(n, &a.name, &a.node_id).await.unwrap(), Ok(()), "a free name is claimed");
            w.put(n, &a).await.unwrap();
            drop(_one);
            assert_eq!(n.node_at(&a.name).await.unwrap(), w.row_of(&a.node_id), "the index names the record at the name, as its writer wrote it");
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
        let file = FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap();
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
        let (n1, n2) = (FileNodesStorage::open(&data1, &root, "mesh1", &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &inc1)).await.unwrap(), FileNodesStorage::open(&data2, &root, "mesh2", &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &inc2)).await.unwrap());
        let (m1, m2) = (FileMeshStorage::open(&root, &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &inc1)).await.unwrap(), FileMeshStorage::open(&root, &crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &inc2)).await.unwrap());
        let writes = async |n: &FileNodesStorage, m: &FileMeshStorage, mesh: &str| {
            let w = WrittenRows::default();
            crate::storage::claim_mesh(&*m, None, &MeshRecord::new(MeshId::mint(), mesh)).await.unwrap().expect("each mesh claims a name no mesh holds");
            let mut ids = Vec::new();
            for name in [format!("{mesh}.admin.1"), format!("{mesh}.rpc.1"), format!("{mesh}.rpc.2")] {
                let r = row(&name);
                w.update(n, &r.node_id, Some(r.clone()), |_| ()).await.unwrap();
                ids.push(r.node_id.to_string());
            }
            ids.sort();
            ids
        };
        let (ids1, ids2) = tokio::join!(writes(&n1, &m1, "mesh1"), writes(&n2, &m2, "mesh2"));
        let records = crate::store_log::latest_in(&root.dir().join(crate::store_root::NODES_FILE)).unwrap();
        for (mesh, ids) in [("mesh1", ids1), ("mesh2", ids2)] {
            let of_mesh: Vec<String> = records.iter().filter(|((kind, _), l)| kind == "node-record" && l.record["name"].as_str().is_some_and(|n| n.starts_with(&format!("{mesh}.")))).map(|((_, key), _)| key.clone()).collect();
            assert_eq!(of_mesh, ids, "{mesh}'s records, keyed by node id, in the root's nodes.jsonl");
        }
        assert_eq!(crate::store_log::keys_in(&root.dir().join(crate::store_root::MESHES_FILE), "mesh-record").unwrap().len(), 2, "one record per mesh");
        for data in [&data1, &data2] {
            assert!(!data.join(crate::store_root::MESHES_FILE).exists() && !data.join(crate::store_root::NODES_FILE).exists(), "{} holds a shared record", data.display());
        }
        assert!(n1.node_at(&"mesh2.rpc.1".parse().unwrap()).await.unwrap().is_some(), "mesh1's primary reads mesh2's record from the shared root");
        assert!(n2.node_at(&"mesh1.rpc.1".parse().unwrap()).await.unwrap().is_some(), "mesh2's primary reads mesh1's record from the shared root");
        assert_eq!(live_records(&m1, &n1, "mesh1").await.unwrap().len(), 6, "a restart reads every mesh's records");
    }

    /// CONTRACT (R-SR5: "one winner stays one winner"; the claim is the lock `locks/name/<name>`): two
    /// processes claim one path.name at once: one takes the lock, the other is refused `NameHeld`
    /// naming it. The crash boundaries of a name: a lock taken by a birth that died before any record
    /// line is adopted by the claim resuming its attempt (the same node) and refused to any other
    /// claimant; a holder whose record line is Deleted and whose release never ran leaves a lock the
    /// next claimant takes over; a lock naming a live claimant is never removed by another. Must NOT
    /// happen: two winners, a live claimant's lock removed, or a released holder's lock blocking the
    /// name for ever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_name_lock_has_one_winner_and_its_crash_boundaries_resolve_by_the_claimants_record() {
        let d = crate::test_support::tempdir("name-locks");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let open = || async { FileNodesStorage::open(&d, &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap() };
        let (a, b) = (open().await, open().await);
        let name: PathName = "mesh1.rpc.4".parse().unwrap();
        let (x, y) = (NodeId::mint(), NodeId::mint());
        let (ra, rb) = tokio::join!(a.claim_node_name(&name, &x), b.claim_node_name(&name, &y));
        let (ra, rb) = (ra.unwrap(), rb.unwrap());
        assert!(ra.is_ok() != rb.is_ok(), "one winner: {ra:?} {rb:?}");
        let (winner, loser, loser_store) = if ra.is_ok() { (x.clone(), y.clone(), &b) } else { (y.clone(), x.clone(), &a) };
        assert_eq!(loser_store.claim_node_name(&name, &loser).await.unwrap(), Err(NameHeld { name: name.clone(), by: winner.clone() }), "the other is refused, naming the winner");
        assert!(root.lock(&name.to_string()).is_file(), "the claim is the lock locks/name/{name}");
        // The winner's birth died before any record line: its resumed claim adopts the lock; any other
        // claimant is refused, and the lock stands.
        assert_eq!(a.claim_node_name(&name, &winner).await.unwrap(), Ok(()), "the resumed claim of the same node adopts its lock");
        assert_eq!(crate::node_rows::take_name(&b, &name, &loser).await.unwrap(), Err(NameHeld { name: name.clone(), by: winner.clone() }), "another claimant waits: no record says the name was released");
        // A live claimant's lock is never removed by another.
        a.put_node(&NodeRow::new(name.clone(), winner.clone(), crate::model::NodeStatus::Pending)).await.unwrap();
        assert_eq!(crate::node_rows::take_name(&b, &name, &loser).await.unwrap(), Err(NameHeld { name: name.clone(), by: winner.clone() }), "a live claimant's lock stands");
        b.release_node_name(&name, &loser).await.unwrap();
        assert_eq!(a.name_claim(&name).await.unwrap(), Some(winner.clone()), "a release naming another claimant removes nothing");
        // The winner's record is Deleted and its release never ran: the next claimant takes the name.
        a.put_node(&NodeRow::new(name.clone(), winner.clone(), crate::model::NodeStatus::Deleted)).await.unwrap();
        assert_eq!(crate::node_rows::take_name(&b, &name, &loser).await.unwrap(), Ok(()), "the claim over a Deleted holder's lock wins");
        assert_eq!(a.name_claim(&name).await.unwrap(), Some(loser.clone()));
        a.release_node_name(&name, &winner).await.unwrap();
        assert_eq!(a.name_claim(&name).await.unwrap(), Some(loser), "the old holder's delayed release finds another claimant's lock and leaves it");
    }

    /// CONTRACT (R-ML2 item 4 as amended: "the recreated mesh shares the name locks under locks/name/ and
    /// the in-flight lock under its name, not a directory"; §5.4): mesh2 created again under its name, a
    /// new mesh with a new id, takes the mesh name's lock over the Dead mesh's, its node-admin's
    /// path.name over the Deleted record there, and its records carry the new MeshId; a delayed release
    /// of an attempt on the old mesh against the name's in-flight lock finds it naming the recreated
    /// mesh's attempt and releases nothing. Must NOT happen: the recreated mesh refused its name, or the
    /// old attempt's release freeing the new attempt's lock.
    #[tokio::test]
    async fn a_recreated_mesh_takes_its_names_over_the_dead_ones_and_an_old_attempts_release_frees_nothing() {
        use crate::node_rows::RecordAt as _;
        let d = crate::test_support::tempdir("recreated-mesh");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let w = crate::store_log::LogWriter::mint();
        let (meshes, nodes) = (FileMeshStorage::open(&root, &w).await.unwrap(), FileNodesStorage::open(&d, &root, "mesh1", &w).await.unwrap());
        let admin: PathName = "mesh2.admin.1".parse().unwrap();
        let old = MeshRecord::new(MeshId::mint(), "mesh2");
        claim_mesh(&meshes, None, &old).await.unwrap().expect("the first mesh2 claims its name");
        let first = NodeRow { mesh_id: Some(old.mesh_id.clone()), ..NodeRow::new(admin.clone(), NodeId::mint(), crate::model::NodeStatus::Pending) };
        assert_eq!(crate::node_rows::write_record(&nodes, &first).await.unwrap(), Ok(()));
        let build = crate::build::BuildId::mint();
        let old_attempt = InFlight { build_id: build.clone(), lane: Some("mesh2".into()), attempt: 1, claimant: "mesh2.admin.1".into(), claimant_node: None };
        assert_eq!(meshes.take_in_flight("mesh2", &old_attempt).await.unwrap(), Taken::Won);
        // mesh2 is removed: its node Deleted, its record Dead, its attempt's verdict released.
        nodes.put_node(&NodeRow { status: crate::model::NodeStatus::Deleted, ..first.clone() }).await.unwrap();
        claim_mesh(&meshes, Some(&old), &MeshRecord { dead: true, ..old.clone() }).await.unwrap().expect("the Dead record is written");
        meshes.release_in_flight("mesh2", &old_attempt).await.unwrap();
        // mesh2 created again.
        let new = MeshRecord::new(MeshId::mint(), "mesh2");
        assert!(matches!(claim_mesh(&meshes, None, &new).await.unwrap(), Ok(MeshClaim::Written { .. })), "the recreated mesh takes its name over the Dead mesh's");
        assert_eq!(meshes.mesh_named("mesh2").await.unwrap().map(|m| m.mesh_id), Some(new.mesh_id.clone()));
        let second = NodeRow { mesh_id: Some(new.mesh_id.clone()), ..NodeRow::new(admin.clone(), NodeId::mint(), crate::model::NodeStatus::Pending) };
        assert_eq!(crate::node_rows::write_record(&nodes, &second).await.unwrap(), Ok(()), "its node-admin's claim wins over the Deleted record");
        assert_eq!(nodes.record_at(&admin).await.unwrap().and_then(|r| r.mesh_id), Some(new.mesh_id.clone()), "its records carry the new MeshId");
        let new_attempt = InFlight { build_id: crate::build::BuildId::mint(), ..old_attempt.clone() };
        assert_eq!(meshes.take_in_flight("mesh2", &new_attempt).await.unwrap(), Taken::Won);
        meshes.release_in_flight("mesh2", &old_attempt).await.unwrap();
        assert_eq!(meshes.in_flight("mesh2").await.unwrap(), Some(new_attempt), "the old attempt's delayed release frees nothing");
    }

    /// CONTRACT (R-SR2: "a create withdrawn before the node ran appends the record marked Withdrawn,
    /// over which a name claim wins as over a Deleted one"; nothing is ever removed): a create's claim
    /// withdrawn before its node ran leaves its record `Withdrawn`, read at its id and listed by no
    /// mesh and at no name, its name released; a holder that appended its `Withdrawn` record and died
    /// before releasing the name leaves a claim the next node's claim takes over. On the file backend
    /// every write of the record stands as a line. Must NOT happen: the record removed, or a claim over
    /// a `Withdrawn` record refused `NameHeld`.
    #[tokio::test]
    async fn a_withdrawn_create_leaves_its_record_withdrawn_and_a_later_claim_wins_over_it() {
        let d = crate::test_support::tempdir("withdrawn-create");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let file = FileNodesStorage::open(&d, &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap();
        let memory = MemoryNodesStorage::default();
        for (store, backend) in [(&file as &dyn NodesStorage, "file"), (&memory as &dyn NodesStorage, "memory")] {
            let name: PathName = "mesh1.rpc.3".parse().unwrap();
            let pending = |n: &PathName| NodeRow::new(n.clone(), NodeId::mint(), crate::model::NodeStatus::Pending);
            let x = pending(&name);
            assert_eq!(crate::node_rows::write_record(store, &x).await.unwrap(), Ok(()), "{backend}: x claims the name");
            assert!(crate::node_rows::release_record(store, &WrittenRows::default(), &name, &x.node_id).await.unwrap(), "{backend}: x held the name");
            assert_eq!(store.node(&x.node_id).await.unwrap().map(|r| r.status), Some(crate::model::NodeStatus::Withdrawn), "{backend}: the record stands, Withdrawn");
            assert_eq!(store.node_at(&name).await.unwrap(), None, "{backend}: no record at the name");
            assert!(store.nodes("mesh1").await.unwrap().iter().all(|r| r.node_id != x.node_id), "{backend}: no mesh lists it");
            assert_eq!(store.name_claim(&name).await.unwrap(), None, "{backend}: the name is released");
            // y appended its Withdrawn record and died before releasing the name.
            let y = pending(&name);
            assert_eq!(crate::node_rows::write_record(store, &y).await.unwrap(), Ok(()), "{backend}: y claims the released name");
            store.put_node(&NodeRow { status: crate::model::NodeStatus::Withdrawn, ..y.clone() }).await.unwrap();
            let z = pending(&name);
            assert_eq!(crate::node_rows::write_record(store, &z).await.unwrap(), Ok(()), "{backend}: z's claim wins over y's Withdrawn record");
            assert_eq!(store.name_claim(&name).await.unwrap(), Some(z.node_id.clone()), "{backend}: the name is z's");
            if backend == "file" {
                let lines = crate::store_log::read_file(&root.dir().join(crate::store_root::NODES_FILE)).unwrap();
                assert_eq!(lines.iter().filter(|l| l.kind() == "node-record" && l.key == x.node_id.as_str()).count(), 2, "x's claim and its Withdrawn record both stand as lines");
            }
        }
    }

    /// CONTRACT (R-TG1: "node.get with a tag filter names a label and answers every node carrying it,
    /// through a tags index declared on the node store (a point read per label)"; storage-whole-records.md:
    /// "RDM computes the index moves from its cached record before the write and the store writes the
    /// index entries blindly"; R-SR1, R-SR2): each change of a (node id, label) row is one `node-tag` line of
    /// the root's `nodes.jsonl`, and the index is the fold of them. Two meshes' records carrying `red`
    /// are read by one label read across the root, a second store of another writer included; a write
    /// without a label appends that label's row not carried and no other; a rename keeps the node's
    /// labels (the index is keyed by node id); a `Deleted` record leaves every label it carries. A label
    /// no record ever carried reads as carried by none; a key that is not a label is refused by name and
    /// nothing is read.
    #[tokio::test]
    async fn the_tags_index_is_one_line_per_label_change_kept_with_every_row_write_and_read_per_label() {
        let d = crate::test_support::tempdir("tags-index");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = crate::store_root::StoreRoot::open(&store, &crate::model::FabricId::mint()).unwrap();
        let (n1, n2) = (FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap(), FileNodesStorage::open(&d.join("mesh2.admin.1"), &root, "mesh2", &crate::store_log::LogWriter::mint()).await.unwrap());
        let (w1, w2) = (WrittenRows::default(), WrittenRows::default());
        let tags = |l: &[&str]| l.iter().map(|t| t.to_string()).collect::<BTreeSet<String>>();
        let row_a = NodeRow { tags: tags(&["rabbit", "red"]), ..row("mesh1.rpc.1") };
        let a = row_a.node_id.clone();
        w1.update(&n1, &a, Some(row_a.clone()), |_| ()).await.unwrap();
        let row_b = NodeRow { tags: tags(&["red"]), ..row("mesh2.rpc.1") };
        let b = row_b.node_id.clone();
        w2.update(&n2, &b, Some(row_b.clone()), |_| ()).await.unwrap();
        let carried = |node: &NodeId, label: &str| crate::store_log::read_record(&root.dir().join(crate::store_root::NODES_FILE), "node-tag", Some(&format!("{node}/{label}"))).map(|l| l["record"]["carried"] == true);
        assert_eq!((carried(&a, "rabbit"), carried(&a, "red"), carried(&b, "red"), carried(&b, "rabbit")), (Some(true), Some(true), Some(true), None), "one row per (label, node id), each a line");
        let other = FileNodesStorage::open(&d.join("mesh1.admin.2"), &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap();
        for n in [&n1, &n2, &other] {
            assert_eq!(n.tagged("red").await.unwrap(), [a.clone(), b.clone()].into(), "one label read answers every mesh's nodes carrying it");
        }
        // A write without rabbit tombstones rabbit's row only.
        w1.update(&n1, &a, None, |r| { r.tags.remove("rabbit"); }).await.unwrap();
        assert_eq!((carried(&a, "rabbit"), other.tagged("rabbit").await.unwrap().len(), other.tagged("red").await.unwrap().len()), (Some(false), 0, 2), "the dropped label's row is appended not carried, the kept one stands");
        // A rename keeps the labels: the record moves to its new name.
        w1.update(&n1, &a, None, |r| r.name = "mesh1.rpc.1.old".parse().unwrap()).await.unwrap();
        assert_eq!(other.tagged("red").await.unwrap(), [a.clone(), b.clone()].into(), "a renamed node keeps its labels");
        // A Deleted record tombstones every label it carries, and keeps them on the record.
        w1.update(&n1, &a, None, |r| r.status = crate::model::NodeStatus::Deleted).await.unwrap();
        w2.update(&n2, &b, None, |r| r.status = crate::model::NodeStatus::Deleted).await.unwrap();
        assert!(other.tagged("red").await.unwrap().is_empty(), "neither deleted node is named under red");
        assert_eq!(n1.node(&a).await.unwrap().map(|r| r.tags), Some(tags(&["red"])), "the Deleted record keeps its tags");
        // A label no record carried: none.
        assert!(other.tagged("blue").await.unwrap().is_empty());
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
        let n1 = FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap();
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
        let node: PathName = "mesh1.admin.1".parse().unwrap();
        let writer = FileConnectionsStorage::open(&root, &node, &crate::store_log::LogWriter::mint()).await.unwrap();
        let (up, down) = (fact(ConnectionState::Connected), fact(ConnectionState::Disconnected));
        writer.append_history(&up).await.unwrap();
        writer.put_connection(&up).await.unwrap();
        writer.append_history(&down).await.unwrap();
        writer.put_connection(&down).await.unwrap();
        let file = root.dir().join(crate::store_root::CONNECTIONS_FILE);
        assert_eq!(writer.connections().await.unwrap(), [down.clone()], "the writer reads the entries it wrote");
        assert_eq!(writer.history().await.unwrap(), [up.clone(), down.clone()], "the writer reads the facts it wrote");
        assert_eq!(crate::store_log::read_file(&file).unwrap().len(), 2, "one line per fact");
        let reader = FileConnectionsStorage::open(&root, &node, &crate::store_log::LogWriter::mint()).await.unwrap();
        assert_eq!(reader.connections().await.unwrap(), [down.clone()], "the latest fact per (source, destination, alpn, kind)");
        assert_eq!(reader.history().await.unwrap(), [up, down], "every fact, oldest first");
    }

    /// CONTRACT (connections.md §3.1: "by-destination prefix = hash(destination)", one latest member
    /// per (source, destination, alpn, kind)): the store's by-destination index names every current
    /// row whose destination is one node, whichever source wrote it and in whichever mesh, read by any
    /// process on the root; a removed row leaves it, and a row whose destination is re-born at its
    /// path.name under a new node id leaves the old node's index for the new one's. Memory and file
    /// alike; on the file root the index is the fold of `connections.jsonl` keyed by destination.
    #[tokio::test]
    async fn the_by_destination_index_names_every_source_holding_a_row_to_one_node_in_any_mesh() {
        let d = crate::test_support::tempdir("by-destination");
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let root = crate::store_root::StoreRoot::open(&store, &crate::model::FabricId::mint()).unwrap();
        let destination = end("mesh1.rpc.1");
        let row = |source: &str, destination: &ConnectionEnd| NodeConnection { source: end(source), destination: destination.clone(), ..fact(ConnectionState::Connected) };
        let (a, b) = (row("mesh1.rpc.2", &destination), row("mesh2.rpc.1", &destination));
        let re_born = ConnectionEnd { node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()), ..destination.clone() };
        let again = NodeConnection { id: rdm_core::ConnectionId::mint(), destination: re_born.clone(), ..a.clone() };
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
            assert_eq!(reader.connections_to(&re_born.node_id).await.unwrap(), [again.clone()], "{backend}: the new node's index holds it");
        };
        let mem = MemoryConnectionsStorage::default();
        check(&mem, &mem, &mem, "memory").await;
        let open = async |name: &str| FileConnectionsStorage::open(&root, &name.parse().unwrap(), &crate::store_log::LogWriter::mint()).await.unwrap();
        let (fa, fb, reader) = (open("mesh1.rpc.2").await, open("mesh2.rpc.1").await, open("mesh1.admin.1").await);
        check(&fa, &fb, &reader, "file").await;
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
        check(&FileMeshStorage::open(&crate::store_root::StoreRoot::at(&d.join("root")), &crate::store_log::LogWriter::mint()).await.unwrap(), "file").await;
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT: docs/architecture/storage.md:86
    /// "| a restart with no launcher (the adoption launch) | the node itself | its own row in its data dir |"
    /// CONTRACT: docs/architecture/node-identity-and-state.md:149
    /// "node.delete releases the node's identity and name; its row stays. The node's mesh-primary writes
    /// the row with status deleted, keeping the record and its config; the deleted row releases..."
    #[tokio::test]
    async fn clear_address_and_primaries_fold_declared_write_index_moves_and_own_in_behave_as_specified() {
        // 1. `clear_address_and_primaries` strips endpoint_id, transport_addr, listeners, and primary revisions.
        let inc = IncarnationId::mint();
        let name: PathName = "mesh1.admin.1".parse().unwrap();
        let mut birth = BirthFacts::new(inc.clone(), name.clone());
        birth.endpoint_id = Some(EndpointId("ep-1".into()));
        birth.transport_addr = Some("127.0.0.1:9000".parse().unwrap());
        birth.listeners.push(("control".into(), "127.0.0.1:9001".parse().unwrap()));
        birth.is_primary = Some(3);
        birth.is_fabric_primary = Some(4);
        birth.declared.insert(NodeState::Ready);
        birth.declared.insert(NodeState::Stopped);

        clear_address_and_primaries(&mut birth);
        assert_eq!(birth.endpoint_id, None);
        assert_eq!(birth.transport_addr, None);
        assert!(birth.listeners.is_empty());
        assert_eq!(birth.is_primary, None);
        assert_eq!(birth.is_fabric_primary, None);

        // 2. `fold_declared` extracts the greatest declared state and path name per (node_id, incarnation_id).
        let node_id = NodeId::mint();
        let mut row = NodeRow::new(name.clone(), node_id.clone(), crate::model::NodeStatus::Stopped);
        row.births.push(birth);
        let declared = fold_declared([&row]);
        assert_eq!(declared.get(&(node_id.clone(), inc.clone())), Some(&(NodeState::Stopped, name.clone())));

        // 3. `write_index_moves` applies path and tag moves to `NodesStorage`.
        let store = MemoryNodesStorage::default();
        store.put_node(&row).await.unwrap();
        let mut row_tagged = row.clone();
        row_tagged.tags.insert("tier-edge".into());
        row_tagged.tags.insert("zone-a".into());
        let moves1 = IndexMoves::of(&row_tagged, None);
        write_index_moves(&store, "mesh1", &node_id, &moves1).await.unwrap();
        assert_eq!(store.node_at(&name).await.unwrap().map(|r| r.node_id), Some(node_id.clone()));
        assert!(store.tagged("tier-edge").await.unwrap().contains(&node_id));
        assert!(store.tagged("zone-a").await.unwrap().contains(&node_id));

        let mut row_retagged = row_tagged.clone();
        row_retagged.tags.remove("zone-a");
        let moves2 = IndexMoves::of(&row_retagged, Some(&row_tagged));
        write_index_moves(&store, "mesh1", &node_id, &moves2).await.unwrap();
        assert!(store.tagged("tier-edge").await.unwrap().contains(&node_id));
        assert!(!store.tagged("zone-a").await.unwrap().contains(&node_id));

        // 4. `FileNodesStorage::own_in` and `put_own_in` round-trip the admin's own row in its data dir.
        let d = crate::test_support::tempdir("storage-own-in");
        let writer = IncarnationId::mint();
        assert_eq!(FileNodesStorage::own_in(&d, &writer).await.unwrap(), None);
        let secret = iroh::SecretKey::from_bytes(&[11u8; 32]);
        let own_record = NodeRecord {
            node_id: node_id.clone(),
            fabric_id: FabricId::mint(),
            name: name.clone(),
            mesh_id: MeshId::mint(),
            incarnation_id: inc,
            endpoint_id: EndpointId(secret.public().to_string()),
            transport_addr: "127.0.0.1:9100".parse().unwrap(),
            listeners: vec![("control".into(), "127.0.0.1:9101".parse().unwrap())],
            is_primary: Some(1),
            is_fabric_primary: Some(1),
            topology_storage_provider: Some("file".into()),
        };
        FileNodesStorage::put_own_in(&d, &writer, &own_record).await.unwrap();
        assert_eq!(FileNodesStorage::own_in(&d, &writer).await.unwrap(), Some(own_record));
        let _ = std::fs::remove_dir_all(&d);
    }
}
