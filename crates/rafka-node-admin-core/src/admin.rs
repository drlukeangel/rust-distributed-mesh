//! The node-admin runtime (`rafka-node-admin`).
//!
//! A node-admin is a fabric member that serves the control API. The first
//! one bootstraps the fabric (its `MESH_SPAWN_TYPE` becomes fabric policy);
//! every other one is launched by an admin's deployment pipeline and takes
//! the policy, with its first view, from the entry pull it makes to that
//! admin over QUIC (`rafka_mesh_transport::entry`). No node calls an admin's
//! HTTP API: that is for tests and people.
//!
//! Every admin holds the same two projections over iroh-gossip: fabric
//! membership (each member's digest) and the fabric's Build facts. From
//! membership it derives the observed topology it publishes on its views,
//! with every seat resolved by `election` (the lowest ready NodeId of each
//! cohort; the lowest-NodeId mesh primary holds the fabric). Admins execute Builds (`executor`): the fabric primary
//! and each mesh's admin primary claim the attempts whose next operation is
//! theirs and run what is left through the deployment pipeline (create, restart,
//! retire) and the lifecycle pipeline (a node's `Pending -> ReadyForTraffic`).

use crate::build::BuildOperation;
use crate::build_state::{BuildStateAdapter, LocalBuildLog as _};
use crate::deployment::endpoint::spec_for;
use crate::accepted::AcceptedStore;
use crate::deployment::pipeline::{adoption_missing, CurrentRuntimeAdoption, Publication, 
    CreateRequest, DeploymentPipeline, LaunchTemplate, NodeObserver, RetireRequest, Timeouts, TopologySink,
};
use crate::deployment::provider::{DeploymentHandle, DeploymentProvider, FabricPolicy, TerminationMode};
use crate::executor::{BuildExecutor, OperationRunner};
use crate::election::ElectionLog;
use crate::fabric_builds::FabricBuildStateAdapter;
use crate::control_plane::ControlPlane;
use crate::lifecycle::{
    HookRegistry, LifecycleScope, LifecycleState, LifecycleTransitionPipeline, MemoryReceiptLog, ShapeFacts, Transition,
    TransitionKey, TransitionResult,
};
use crate::model::*;
use crate::topology::Topology;
use iroh::protocol::Router as IrohRouter;
use iroh::{Endpoint, EndpointAddr, SecretKey};
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::{MemberStatus, MeshDigest, MeshNode};
use rafka_mesh_transport::membership::{announce_leaving, gossip_interval, leave_linger_from_env, Backbone, DigestBook, Membership, LEAVE_EVERY};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use tracing::Instrument;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

/// The digest key carrying a node-admin's mesh id.

/// Everything a node-admin reads from its environment.
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// The Fabric's name (its label) and its identity.
    pub fabric: String,
    /// The fabric's minted id.
    pub fabric_id: FabricId,
    /// The name of the mesh this admin belongs to.
    pub mesh: String,
    /// The mesh's minted id, when it is known.
    pub mesh_id: Option<MeshId>,
    /// `RDM_MESH_PRIMARY`: this admin must recover its mesh (node-admin-lifecycle.md §2).
    pub mesh_primary: bool,
    /// `RDM_FABRIC_PRIMARY`: this admin must recover the fabric. Implies `mesh_primary`.
    pub fabric_primary: bool,
    /// `RDM_SEEDS` of a person-started admin: live nodes it dials at once (a launched admin takes
    /// its seeds from its launch).
    pub seeds: Vec<(String, SocketAddr)>,
    /// `RDM_FABRIC_ID` was given: the Fabric is named, not new.
    pub fabric_id_named: bool,
    /// The admin's data directory: its own runtime material only.
    pub data_dir: PathBuf,
    /// The fabric's shared store root (`RDM_STORE_DIR`, or its launch's): its stores open under
    /// `<store_dir>/<fabric_id>/` (R14, `crate::store_root`).
    pub store_dir: PathBuf,
    /// The directory the node executables are looked up in.
    pub bin_dir: PathBuf,
    /// `MESH_SPAWN_TYPE` as given (normalised by the fabric policy).
    pub spawn_type: Option<String>,
    /// The HTTP bind of a bootstrap admin (`RDM_NODE_ADMIN_API_BIND`).
    pub api_bind: SocketAddr,
    /// Set when a deployment pipeline launched this admin.
    pub launch: Option<Launch>,
    /// Passed on to every runtime this admin launches.
    pub passthrough: BTreeMap<String, String>,
    /// The explicit executable bindings this admin launches from (`RDM_EXECUTABLE_BINDINGS`),
    /// validated before it opens a provider. `None` is the built-in mode: executables come from
    /// `bin_dir`. With bindings, nothing is launched from `bin_dir`.
    pub bindings: Option<rafka_mesh_entity::binding::Validated>,
    /// The two variables that named them, handed on to every node-admin this admin launches.
    pub bindings_env: BTreeMap<String, String>,
    /// `RDM_STOPPED_NODE_TTL` (fabric-node-lifecycle.md §6.9): a node stopped longer than it is
    /// collected by a node.delete Build the fabric-primary submits on a sweep every ttl. Read by the
    /// fabric-primary only, held by every admin (a successor sweeps with it). `None`: blank, forever.
    pub stopped_node_ttl: Option<Duration>,
    /// The bound of a `node.connections.delete { All }` (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`, its
    /// launch's when launched): this admin's own, every birth it launches carries it in its launch,
    /// and a command this admin sends that runs one is answered within it.
    pub connections_delete_deadline: Duration,
}

impl AdminConfig {
    /// The admin's configuration from the environment `get` reads, refused by name when a variable
    /// is malformed.
    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let launch = if get(rafka_mesh_entity::launch::ENV_NODE_ID).is_some() { Some(Launch::from_env(&get)?) } else { None };
        let fabric = launch.as_ref().map(|l| l.fabric.clone()).or_else(|| get("RDM_FABRIC")).unwrap_or_else(|| "fabric1".into());
        // A launched node is handed its Fabric's id; the bootstrap admin takes
        // `RDM_FABRIC_ID` when given, else its Fabric is new and it mints one.
        let fabric_id = match &launch {
            Some(l) => l.fabric_id.clone(),
            None => match get(rafka_mesh_entity::launch::ENV_FABRIC_ID).filter(|v| !v.trim().is_empty()) {
                Some(v) => FabricId::parse(&v).map_err(|e| format!("{}: {e}", rafka_mesh_entity::launch::ENV_FABRIC_ID))?,
                None => FabricId::mint(),
            },
        };
        let mesh = launch.as_ref().map(|l| l.name.mesh.clone()).or_else(|| get("RDM_MESH")).unwrap_or_else(|| "mesh1".into());
        if !is_valid_mesh_name(&mesh) {
            return Err(format!("RDM_MESH `{mesh}` is not a valid mesh name"));
        }
        let data_dir = launch
            .as_ref()
            .map(|l| l.data_dir.clone())
            .or_else(|| get("RDM_DATA_DIR").map(PathBuf::from))
            .unwrap_or_else(|| std::env::temp_dir().join(format!("rafka-node-admin-{}", rand::random::<u32>())));
        // The fabric's store root: required, never defaulted, never the data dir (R14 items 1, 8).
        let store_dir = match &launch {
            Some(l) => l.store_dir.clone(),
            None => get(rafka_mesh_entity::launch::ENV_STORE_DIR).filter(|v| !v.trim().is_empty()).map(PathBuf::from).ok_or_else(|| {
                format!("{} is required: the fabric's shared store root every node of the fabric reaches; nothing falls back to the data dir", rafka_mesh_entity::launch::ENV_STORE_DIR)
            })?,
        };
        let bin_dir = get("RDM_BIN_DIR").map(PathBuf::from).unwrap_or_else(|| {
            std::env::current_exe().ok().and_then(|e| e.parent().map(Path::to_path_buf)).unwrap_or_else(|| PathBuf::from("."))
        });
        let api_bind = get("RDM_NODE_ADMIN_API_BIND")
            .map(|b| b.parse().map_err(|e| format!("RDM_NODE_ADMIN_API_BIND `{b}`: {e}")))
            .transpose()?
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0)));
        let bindings_env: BTreeMap<String, String> = [rafka_mesh_entity::binding::ENV_EXECUTABLE_BINDINGS, rafka_mesh_entity::binding::ENV_EXECUTABLE_CANDIDATE]
            .iter()
            .filter_map(|k| get(k).filter(|v| !v.trim().is_empty()).map(|v| (k.to_string(), v)))
            .collect();
        let bindings = match get(rafka_mesh_entity::binding::ENV_EXECUTABLE_BINDINGS).filter(|v| !v.trim().is_empty()) {
            None => None,
            Some(file) => {
                let candidate = get(rafka_mesh_entity::binding::ENV_EXECUTABLE_CANDIDATE)
                    .filter(|v| !v.trim().is_empty())
                    .ok_or_else(|| format!("{} names a binding file but {} (the candidate sha this run is of) is not set", rafka_mesh_entity::binding::ENV_EXECUTABLE_BINDINGS, rafka_mesh_entity::binding::ENV_EXECUTABLE_CANDIDATE))?;
                let set = rafka_mesh_entity::binding::BindingSet::load(Path::new(&file)).map_err(|e| e.to_string())?;
                let expect = rafka_mesh_entity::binding::Expect { candidate_sha: &candidate, required: &[], provider_image: rafka_mesh_entity::binding::ProviderImage::Unchecked };
                // The bootstrap admin hashes every bound file before any provider opens; an admin
                // another admin launched checks all but the hashes (its launch re-hashed its own
                // executable; each later launch re-hashes the one it runs).
                let validated = if launch.is_some() { set.validate_launched(&expect) } else { set.validate(&expect) };
                Some(validated.map_err(|e| format!("refusing to start: explicit executable bindings: {e}"))?)
            }
        };
        let passthrough = ["RDM_EVIDENCE_DIR", "RUST_LOG", "OTEL_EXPORTER_OTLP_ENDPOINT", "RDM_CONTAINER_SUBNET_POOL", "RDM_STALENESS_MS", "RDM_GOSSIP_INTERVAL_MS", "RDM_BACKBONE_INTERVAL_MS", "RDM_LEAVE_LINGER_MS", crate::stopped_gc::ENV]
            .iter()
            .filter_map(|k| get(k).map(|v| (k.to_string(), v)))
            .collect();
        let (mesh_primary, fabric_primary) = match &launch {
            Some(_) => (false, false),
            None => (
                rafka_mesh_entity::launch::decode_flag(rafka_mesh_entity::launch::ENV_MESH_PRIMARY, get(rafka_mesh_entity::launch::ENV_MESH_PRIMARY))?,
                rafka_mesh_entity::launch::decode_flag(rafka_mesh_entity::launch::ENV_FABRIC_PRIMARY, get(rafka_mesh_entity::launch::ENV_FABRIC_PRIMARY))?,
            ),
        };
        if fabric_primary && !mesh_primary {
            return Err(format!(
                "{} is set without {}: a fabric recovery starts the primary node-admin of its own mesh as well (node-admin-lifecycle.md §3)",
                rafka_mesh_entity::launch::ENV_FABRIC_PRIMARY,
                rafka_mesh_entity::launch::ENV_MESH_PRIMARY
            ));
        }
        let stopped_node_ttl = crate::stopped_gc::ttl_from_env(get(crate::stopped_gc::ENV).as_deref())?;
        let connections_delete_deadline = match &launch {
            Some(l) => l.connections_delete_deadline,
            None => rafka_mesh_entity::launch::decode_connections_delete_deadline(get(rafka_mesh_entity::launch::ENV_CONNECTIONS_DELETE_DEADLINE).as_deref())?,
        };
        Ok(Self {
            stopped_node_ttl,
            connections_delete_deadline,
            fabric,
            fabric_id,
            mesh_primary,
            fabric_primary,
            seeds: match &launch {
                Some(_) => Vec::new(),
                None => rafka_mesh_entity::launch::decode_seeds(&get(rafka_mesh_entity::launch::ENV_SEEDS).unwrap_or_default())?,
            },
            fabric_id_named: launch.is_some() || get(rafka_mesh_entity::launch::ENV_FABRIC_ID).is_some_and(|v| !v.trim().is_empty()),
            mesh,
            mesh_id: launch
                .as_ref()
                .and_then(|l| l.mesh_id.clone())
                .map(Ok)
                .or_else(|| get("RDM_MESH_ID").filter(|v| !v.trim().is_empty()).map(|v| MeshId::parse(&v).map_err(|e| format!("RDM_MESH_ID: {e}"))))
                .transpose()?,
            data_dir,
            store_dir,
            bin_dir,
            spawn_type: get(crate::deployment::provider::SPAWN_TYPE_ENV),
            api_bind,
            launch,
            passthrough,
            bindings,
            bindings_env,
        })
    }
}

/// The node's transport key in its data dir (minted on first use).
/// The node key in `data_dir`, minted and written there when none is: read and written on the
/// blocking pool (`record_store::blocking_io`), so no worker thread waits on the disk.
pub(crate) async fn load_or_mint_key(data_dir: &Path) -> Result<SecretKey, String> {
    let data_dir = data_dir.to_path_buf();
    crate::record_store::blocking_io(move || {
        std::fs::create_dir_all(&data_dir).map_err(|e| format!("{}: {e}", data_dir.display()))?;
        let path = data_dir.join("node-key");
        // A key never written is minted; one that cannot be read is an error, never replaced.
        match std::fs::read_to_string(&path) {
            Ok(h) => {
                let bytes: [u8; 32] = hex::decode(h.trim())
                    .map_err(|e| format!("{}: {e}", path.display()))?
                    .try_into()
                    .map_err(|_| format!("{}: not 32 bytes", path.display()))?;
                return Ok(SecretKey::from_bytes(&bytes));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(format!("{}: {e}", path.display())),
        }
        let k = SecretKey::generate();
        std::fs::write(&path, hex::encode(k.to_bytes())).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(k)
    })
    .await
}

/// The local OS clock, for a read that measures time elapsed since a local event (a tickle
/// round's spacing): never a stamp another node reads, which is rafka-time.
fn local_now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

pub(crate) fn node_status(s: MemberStatus) -> NodeStatus {
    match s {
        MemberStatus::Pending => NodeStatus::Pending,
        MemberStatus::ReadyForTraffic => NodeStatus::ReadyForTraffic,
        MemberStatus::Draining => NodeStatus::Draining,
        MemberStatus::Leaving => NodeStatus::Leaving,
    }
}

/// What this admin's pipelines published and removed, and the meshes its
/// Builds created: merged into the membership projection.
#[derive(Default)]
pub struct Records {
    nodes: Mutex<BTreeMap<PathName, Node>>,
    removed: Mutex<std::collections::HashSet<(PathName, Option<IncarnationId>)>>,
    meshes: Mutex<BTreeMap<String, MeshId>>,
    seats: Mutex<Seats>,
    /// nodes.storage on the fabric's shared store root: the durable runtime rows a birth this admin
    /// never heard is adopted from, and the node rows the view reads a birth's name from when the
    /// birth does not publish it ([`RowNames`]).
    contacts: std::sync::OnceLock<Arc<dyn crate::storage::NodesStorage>>,
    /// The lifecycle states this admin applied as an authority (`status_rpc`): the view's
    /// `declared`, never its `status`.
    pub declared: Arc<Mutex<crate::status_rpc::Declared>>,
    /// The exact births the mesh primary's offline tickle holds TRUE OFFLINE (`offline`): shown
    /// `Dead` while they stay silent.
    pub offline: Mutex<std::collections::HashSet<(NodeId, IncarnationId)>>,
    /// The peer mesh the fabric primary's investigation decided to rebirth: the fabric is
    /// `degraded` from the decision until the mesh has a ready primary of a later birth.
    peer_recovery: Mutex<Option<PeerRecovery>>,
    /// The fabric-primary role was taken while the fabric was `degraded`: the new holder adopted
    /// the status and keeps it until every mesh primary reports for its round (`crate::round`).
    adopted_degraded: std::sync::atomic::AtomicBool,
    /// This admin, as its mesh's primary, may declare its Mesh ready: it holds no adopted status
    /// waiting for its round (`crate::round`). Withdrawn when the role ends.
    declare_gate: std::sync::atomic::AtomicBool,
    /// What wakes the declarer (`crate::status_declare`): poked by the topology, this admin's own
    /// digest status, the declare gate, the meshes this admin holds and a down op's receipt.
    pub wake: Arc<crate::status_declare::DeclareWake>,
    /// Poked each time the installed view changes: the hierarchy loop wakes on it to act on a
    /// seat taken or a status that routing reads at once, not on its next round.
    pub view_changed: tokio::sync::Notify,
    /// This admin does not hold the fabric-primary role and the last fabric status published is
    /// `degraded`: it serves that status, not a ready its own (absent) authority records imply.
    published_degraded: std::sync::atomic::AtomicBool,
    /// The exact births whose exit-proof row (`status/<mesh>/exits/`, written by whichever admin
    /// proved the exit from the provider) the installing projection read: each birth the projection
    /// holds and each seat holder the book records, a point read of its key at every install, never
    /// fed by a prover. Shown `Dead` whatever their last digest said, and the proof an election takes
    /// that a seat's holder is gone (fabric-node-lifecycle-elections.md §4.1).
    exited: Mutex<std::collections::HashSet<(NodeId, IncarnationId)>>,
    /// The membership book whose seat records name holders the exit read covers beside the view's
    /// births (a holder the view no longer lists). Set once at start.
    book: std::sync::OnceLock<DigestBook>,
    /// This admin is entering an existing mesh and has not yet been told who holds the seats: it
    /// computes none until its entry read has (ruling R-A2).
    entering: std::sync::atomic::AtomicBool,
    /// The fabric state this admin projects: authoritative on the fabric-primary, the last
    /// published state on every other admin (`crate::fabric_rounds`).
    fabric_state: Mutex<crate::fabric_state::FabricState>,
    /// Whether the fabric this admin projects has first reached ready-for-traffic: set by the first
    /// `set_fabric_state(ReadyForTraffic)` and never cleared (states.md, "The initial fabric before
    /// ready-for-traffic"). The control plane and the status authority read this one handle.
    pub first_ready: crate::fabric_state::FirstReady,
    /// What keeps the fabric in its state, named (`None`: nothing, or not the fabric-primary).
    fabric_blocker: Mutex<Option<String>>,
    /// The fabric state the fabric-primary last put in a FabricStatus frame. The climb enters the
    /// next state only after the current one went out, so every state is its own frame.
    published_fabric_state: Mutex<Option<crate::fabric_state::FabricState>>,
    /// The accepted Build's planned births per mesh: the nodes a mesh's readiness is counted over.
    /// Empty until an accepted Build is held, when the members the view holds are counted.
    planned: Mutex<BTreeMap<String, BTreeSet<PathName>>>,
    /// The meshes whose last heard MeshStatus is ready-for-traffic. A mesh's lifecycle is pending,
    /// ready-for-traffic, leaving, dead (states.md): a ready mesh that loses a node is not pending
    /// again, so its projection holds ready while it has a primary, and the primary that takes over
    /// adopts that state and runs its round (R-S3).
    heard_ready: Mutex<BTreeSet<String>>,
    /// Each MeshStatus Dead held, by its fact (mesh name, publisher, changed_at), bound to the mesh id
    /// it is about: the id the publisher's own digest names when the fact is first held. Dead is a
    /// mesh id's, never a name's (states.md: an id is never reissued; a name is, R-MN1). After
    /// mesh.deleted the last admin, still its primary, publishes MeshStatus Dead (R-NS1 item 1).
    heard_dead: Mutex<BTreeMap<(String, String, u64), Option<MeshId>>>,
    /// The meshes this admin, as fabric-primary, accepted mesh.deleted for: a validated `MeshLeave`,
    /// which the mesh-primary calls only after it committed the mesh row Dead (node-rpc-envelope.md,
    /// mesh delete command pair). Dead in this admin's projection from that acceptance on, without
    /// waiting for the MeshStatus Dead gossip; dropped with the mesh's id at mesh-left.
    deleted_meshes: Mutex<std::collections::HashSet<MeshId>>,
    /// This admin's own logical node, set once at start: the projection knows the mesh it authors
    /// the status of (a mesh primary restates no heard status as its own, gossip.md MeshStatus).
    me: std::sync::OnceLock<NodeId>,
    /// Moves each time the installed view changed: what a round waits on to see a target reachable again.
    view_tick: tokio::sync::watch::Sender<u64>,
    /// The writer of the node rows of each mesh this admin writes for (`crate::node_rows`): the
    /// executor's claims, moves and Deleted rows, and a birth its view holds for the first time
    /// that no create of it claimed. Set once at start.
    rows: std::sync::OnceLock<Arc<crate::node_rows::NodeRows>>,
    /// Where this admin's mesh's status rows live, read when this admin takes its mesh's seat
    /// (`crate::status_rpc::SeatFold`). Set once at start.
    status: std::sync::OnceLock<crate::status_rpc::SeatFold>,
    /// This admin's Node RPC door, opened by an installed view ([`ServingDoor`]). Set once at start.
    door: std::sync::OnceLock<ServingDoor>,
    /// This admin's place on the backbone: it holds the meshes the installed view names, each deleted
    /// or not, which its publication of a deleted mesh reads ([`Backbone::publishes_deleted_mesh_of`]).
    /// Set once at start.
    backbone: std::sync::OnceLock<Backbone>,
}

/// An admin's Node RPC readiness gate (node-rpc-envelope.md, "Node RPC readiness gate": "The router
/// refuses every op, Ping included, with typed NotReady until the Node RPC door is ready. A node-admin
/// waits until its Status authority is filled and its view holds it"): it opens once the authority is
/// filled and a view this admin installs holds its exact birth, under whatever name it holds now (its
/// join answer hands it its row's name, R-JN1), never by the name it booted under.
pub struct ServingDoor {
    /// The router's gate.
    ready: Arc<std::sync::atomic::AtomicBool>,
    /// This admin's exact birth.
    birth: (NodeId, IncarnationId),
    /// Whether the Status authority is filled.
    authority_filled: Box<dyn Fn() -> bool + Send + Sync>,
    /// The name this admin booted under, for the span.
    boot_name: PathName,
}

impl ServingDoor {
    /// The door over the router's gate `ready` for the birth `birth`, booted as `boot_name`.
    pub fn new(ready: Arc<std::sync::atomic::AtomicBool>, birth: (NodeId, IncarnationId), authority_filled: Box<dyn Fn() -> bool + Send + Sync>, boot_name: PathName) -> Self {
        Self { ready, birth, authority_filled, boot_name }
    }

    /// Open the door if it is closed, the authority is filled and `view` holds this admin's exact birth.
    fn open_if_held(&self, view: &Topology) {
        if self.ready.load(std::sync::atomic::Ordering::SeqCst) || !(self.authority_filled)() {
            return;
        }
        if let Some(own) = view.births().find(|n| n.node_id == self.birth.0 && n.incarnation_id.as_ref() == Some(&self.birth.1)) {
            self.ready.store(true, std::sync::atomic::Ordering::SeqCst);
            tracing::info_span!("rdm.node_admin.node.serve.via-view-holds-birth", node = %own.name, node_id = %self.birth.0, incarnation_id = %self.birth.1.as_str(), held_name = %own.name, boot_name = %self.boot_name)
                .in_scope(|| tracing::info!("the Status authority is filled and this admin's view holds its exact birth: the Node RPC router serves"));
        }
    }
}

/// A peer mesh whose rebirth the fabric primary decided (`crate::investigate`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRecovery {
    /// The mesh's name.
    pub mesh: String,
    /// The Rafka-time of the decision.
    pub verdict_rafka_ms: u64,
    /// When this admin took the decision: a report counts as the mesh's proof only if it arrived after.
    pub opened_at: std::time::Instant,
    /// The node-admin births of the mesh the decision found.
    pub lost: Vec<IncarnationId>,
}

/// The seats (`is_primary`, `is_fabric_primary`) by holder. A fabric shutdown runs no election
/// because admins go `Draining`: when this admin learns one it holds the seats of its last view
/// in which no admin was `Draining`, and keeps each seat while its holder is present. A seat goes
/// only with its holder; nothing re-elects into it. An admin that learns the shutdown before it
/// has any view (one joining during the shutdown) holds no seats: it drains nothing.
#[derive(Default)]
pub struct Seats {
    /// The seats of the last view with no `Draining` admin.
    calm: BTreeMap<NodeId, (bool, bool)>,
    /// The seats held for the shutdown, once one is learned.
    held: Option<BTreeMap<NodeId, (bool, bool)>>,
}

/// The meshes `view` names, each with whether it holds that mesh deleted: its row deleted at
/// mesh.deleted (lifecycles/states.md, "mesh deleted").
fn view_meshes(view: &Topology) -> BTreeMap<String, bool> {
    view.meshes.iter().map(|m| (m.name.clone(), m.status == ScopeStatus::Dead)).collect()
}

impl Records {
    /// Hold the accepted Build's planned births per mesh; whether they changed.
    pub fn set_planned(&self, topology: &crate::accepted::FabricTopology) -> bool {
        let planned: BTreeMap<String, BTreeSet<PathName>> = topology.meshes.iter().map(|(m, t)| (m.clone(), t.nodes.clone())).collect();
        let changed = std::mem::replace(&mut *self.planned.lock().unwrap(), planned.clone()) != planned;
        if changed {
            self.wake.poke();
        }
        changed
    }

    /// The writer of node rows the installed views feed (set once, at start).
    pub fn set_rows(&self, rows: Arc<crate::node_rows::NodeRows>) {
        let _ = self.rows.set(rows);
    }

    /// This admin's Node RPC door (set once, at start): every view installed from now on opens it
    /// once it holds the birth, and `installed`, the view in place when it is set, is checked at once.
    pub fn set_door(&self, door: ServingDoor, installed: &Topology) {
        door.open_if_held(installed);
        let _ = self.door.set(door);
    }

    /// This admin's place on the backbone (set once, at start): it holds the meshes `installed`, the
    /// view in place when it is set, names, each deleted or not.
    pub fn set_backbone(&self, backbone: Backbone, installed: &Topology) {
        backbone.set_view_meshes(view_meshes(installed));
        let _ = self.backbone.set(backbone);
    }

    /// The status rows of `mesh` this admin (`name`, born as `me`) folds when it takes `mesh`'s seat
    /// (set once, at start).
    pub fn set_status(&self, storage: Arc<dyn crate::status_storage::StatusStorage>, mesh: String, name: PathName, me: NodeId) {
        let _ = self.status.set(crate::status_rpc::SeatFold::new(storage, mesh, name, me));
    }

    /// The fabric-primary's own ready-for-traffic row (states.md, "Who sets each state": "The
    /// fabric-primary declares fabric state through the completed rounds"): put among this admin's mesh's
    /// status rows on the store root as the first act of entering ready-for-traffic, before the fabric
    /// state latches and the door admits anything, so the fact is durable before anything acts on it.
    /// Committed as an `ApplyFabricEvent` at this admin is (`crate::status_rpc::commit_fabric_event`):
    /// the declarer's later one for this mesh answers `AlreadyApplied`. `Err` names what refused it, and
    /// nothing is held.
    pub async fn put_ready_row(&self, fabric_id: &FabricId) -> Result<rafka_node_rpc_contract::status::StatusReply, String> {
        let Some(fold) = self.status.get() else {
            return Err(format!("no status store is held to put the ready-for-traffic row of fabric {fabric_id}"));
        };
        let ready = crate::status_rpc::event_key(&rafka_node_rpc_contract::status::FabricEvent::ReadyForTraffic);
        match crate::status_rpc::commit_fabric_event(&**fold.storage(), &self.declared, fabric_id, &ready).await {
            r @ (rafka_node_rpc_contract::status::StatusReply::Applied | rafka_node_rpc_contract::status::StatusReply::AlreadyApplied) => Ok(r),
            rafka_node_rpc_contract::status::StatusReply::NotReady { reason } => Err(format!("the status rows of mesh {} on the store root: {reason}", fold.mesh())),
            other => Err(format!("the status rows of mesh {} on the store root answered {}", fold.mesh(), other.name())),
        }
    }

    /// Whether the initial fabric `fabric_id` has first reached ready-for-traffic, decided on the durable
    /// fact (states.md, "The initial fabric before ready-for-traffic"): what this admin already knows
    /// ([`crate::fabric_state::FirstReady`], or `now`, its view's fabric status), else a point read of the
    /// ready-for-traffic row among its mesh's status rows on the store root, else among those of the
    /// fabric-primary's mesh `fabric_primary_mesh` (the row it put as it entered ready-for-traffic), while
    /// the fabric state this admin hears by gossip, and its own mesh's row, can still lag it. A read that
    /// fails is named.
    pub async fn first_ready_on_root(&self, fabric_id: &FabricId, now: ScopeStatus, fabric_primary_mesh: Option<&str>) -> Result<bool, String> {
        if self.first_ready.reached(now) {
            return Ok(true);
        }
        let Some(fold) = self.status.get() else {
            return Err(format!("no status store is held to read whether fabric {fabric_id} first reached ready-for-traffic"));
        };
        self.first_ready.reached_on_root(&**fold.storage(), fabric_id, now, fabric_primary_mesh).await.map_err(|e| {
            let fabric_primary = fabric_primary_mesh.map(|m| format!(" and of the fabric-primary's mesh {m}")).unwrap_or_default();
            format!("the status rows of mesh {}{fabric_primary} on the store root, read for the ready-for-traffic row of fabric {fabric_id}: {e}", fold.mesh())
        })
    }

    /// The tick a waiter arms on to see the view change.
    pub fn subscribe_view(&self) -> tokio::sync::watch::Receiver<u64> {
        self.view_tick.subscribe()
    }

    /// Hold which meshes were last heard ready-for-traffic.
    pub fn set_heard_ready(&self, meshes: BTreeSet<String>) {
        *self.heard_ready.lock().unwrap() = meshes;
    }

    /// Whether `mesh` was last heard ready-for-traffic.
    pub fn heard_ready(&self, mesh: &str) -> bool {
        self.heard_ready.lock().unwrap().contains(mesh)
    }

    /// This admin's own logical node, set once at start.
    pub fn set_me(&self, me: NodeId) {
        let _ = self.me.set(me);
    }

    /// Hold the MeshStatus Dead facts heard now, each `(mesh, publisher, changed_at)`. A fact held for
    /// the first time is bound to the mesh id `id_of(mesh, publisher)` answers (the publisher's live
    /// digest); a fact bound before keeps its id; a fact no longer held is dropped (a newer status of
    /// the mesh, or its MeshLeft).
    pub fn set_heard_dead(&self, facts: Vec<(String, String, u64)>, id_of: impl Fn(&str, &str) -> Option<MeshId>) {
        let mut held = self.heard_dead.lock().unwrap();
        held.retain(|k, _| facts.contains(k));
        for f in facts {
            let bound = held.entry(f.clone()).or_insert(None);
            if bound.is_none() {
                *bound = id_of(&f.0, &f.1);
            }
        }
    }

    /// Hold that this fabric-primary accepted mesh.deleted for the mesh `id` (a validated `MeshLeave`).
    pub fn hold_mesh_deleted(&self, id: &MeshId) {
        self.deleted_meshes.lock().unwrap().insert(id.clone());
    }

    /// Drop the mesh `id` from the meshes this fabric-primary accepted mesh.deleted for (its mesh-left).
    pub fn deleted_meshes_forget(&self, id: &MeshId) {
        self.deleted_meshes.lock().unwrap().remove(id);
    }

    /// Whether this fabric-primary accepted mesh.deleted for the mesh `id`.
    pub fn mesh_deleted(&self, id: &MeshId) -> bool {
        self.deleted_meshes.lock().unwrap().contains(id)
    }

    /// Whether a MeshStatus Dead bound to the mesh `id` is held, unless this admin authors that mesh's
    /// status (`authored_here`: it is the mesh's primary, which states the row it committed and
    /// restates no heard status as its own). The mesh row's own Dead is read by the projection.
    pub fn heard_dead(&self, id: Option<&MeshId>, authored_here: bool) -> bool {
        let Some(id) = id else { return false };
        !authored_here && self.heard_dead.lock().unwrap().values().any(|bound| bound.as_ref() == Some(id))
    }

    /// The planned births per mesh of the accepted Build held (empty: none held).
    pub fn planned(&self) -> BTreeMap<String, BTreeSet<PathName>> {
        self.planned.lock().unwrap().clone()
    }

    /// The fabric state this admin projects.
    pub fn fabric_state(&self) -> crate::fabric_state::FabricState {
        *self.fabric_state.lock().unwrap()
    }

    /// Hold the fabric state; the view follows at its next projection.
    pub fn set_fabric_state(&self, state: crate::fabric_state::FabricState) {
        if state == crate::fabric_state::FabricState::ReadyForTraffic {
            self.first_ready.reach("ready-for-traffic");
        }
        let changed = std::mem::replace(&mut *self.fabric_state.lock().unwrap(), state) != state;
        if changed {
            self.wake.poke();
        }
    }

    /// The fabric state last put in a FabricStatus frame by this admin as the fabric-primary.
    pub fn published_fabric_state(&self) -> Option<crate::fabric_state::FabricState> {
        *self.published_fabric_state.lock().unwrap()
    }

    /// Record the fabric state a FabricStatus frame carried (or `None`: this admin publishes none);
    /// whether the record changed.
    pub fn set_published_fabric_state(&self, state: Option<crate::fabric_state::FabricState>) -> bool {
        std::mem::replace(&mut *self.published_fabric_state.lock().unwrap(), state) != state
    }

    /// What keeps the fabric-primary's fabric in its state, named.
    pub fn fabric_blocker(&self) -> Option<String> {
        self.fabric_blocker.lock().unwrap().clone()
    }

    /// Name (or clear) what keeps the fabric in its state.
    pub fn set_fabric_blocker(&self, blocker: Option<String>) {
        *self.fabric_blocker.lock().unwrap() = blocker;
    }

    /// The peer mesh being reborn after the investigation's decision (`None`: no recovery is open).
    pub fn peer_recovery(&self) -> Option<PeerRecovery> {
        self.peer_recovery.lock().unwrap().clone()
    }

    /// Hold (or clear) the peer mesh being reborn.
    pub fn set_peer_recovery(&self, recovery: Option<PeerRecovery>) {
        *self.peer_recovery.lock().unwrap() = recovery;
    }

    /// The fabric is `degraded` because this admin took the fabric-primary role while it was.
    pub fn adopted_degraded(&self) -> bool {
        self.adopted_degraded.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Hold (or clear) the adopted `degraded`; whether the value changed.
    pub fn set_adopted_degraded(&self, on: bool) -> bool {
        self.adopted_degraded.swap(on, std::sync::atomic::Ordering::SeqCst) != on
    }

    /// Record the provider's proof that `node`'s exact birth exited (node-recovery.md: "Exited is the
    /// proof, recorded durably"): its exit-proof row, a blind put of its `(node_id, incarnation)` key
    /// among the status rows of its own mesh (`status/<mesh>/exits/`), whichever admin proved it. This
    /// is the fact every projection reads as the birth's proven loss ([`Self::read_exits`]); nothing
    /// is held in memory for it. `proof` names where the exit came from (`provider`, `exit-record`,
    /// `none`, `terminate-runtime`).
    pub async fn put_exit(&self, node: &Node, exit_code: Option<i32>, proof: &str) -> Result<crate::status_storage::NodeExitRow, String> {
        let incarnation_id = node.incarnation_id.clone().ok_or_else(|| format!("{} ({}) holds no incarnation: no birth to key its exit-proof row by", node.name, node.node_id))?;
        let store = self.status.get().map(|f| f.storage().clone()).ok_or_else(|| format!("no status store is held to record the exit of {} ({}) at {}", node.name, node.node_id, incarnation_id.as_str()))?;
        let row = crate::status_storage::NodeExitRow { node_id: node.node_id.clone(), name: node.name.clone(), incarnation_id, exit_code, proof: proof.to_string(), observed_at_ms: local_now_ms() };
        store.put_node_exit_of(&node.mesh, &row).await.map_err(|e| format!("status.storage put of {} ({}) exit-proof row among {}'s status rows: {e}", node.name, node.node_id, node.mesh))?;
        Ok(row)
    }

    /// The exit-proof row of `node`'s exact birth among its own mesh's status rows, a point read
    /// (`None`: no exit of that birth is recorded). `Err` names the birth, the mesh and the cause.
    pub async fn exit_row_of(&self, node: &Node) -> Result<Option<crate::status_storage::NodeExitRow>, String> {
        let Some(incarnation) = node.incarnation_id.as_ref() else { return Ok(None) };
        self.exit_row_of_birth(&node.mesh, &node.node_id, incarnation, &node.name.to_string()).await
    }

    /// The exit-proof row of the exact birth `(node_id, incarnation)` of mesh `mesh` among that mesh's
    /// status rows, a point read (`None`: no exit of that birth is recorded); `named` names the birth
    /// in an error. `Err` names the birth, the mesh and the cause.
    pub async fn exit_row_of_birth(&self, mesh: &str, node_id: &NodeId, incarnation: &IncarnationId, named: &str) -> Result<Option<crate::status_storage::NodeExitRow>, String> {
        let store = self.status.get().map(|f| f.storage().clone()).ok_or_else(|| format!("no status store is held to read the exit of {named} ({node_id}) at {}", incarnation.as_str()))?;
        store.node_exit_of(mesh, node_id, incarnation).await.map_err(|e| format!("status.storage read of {named} ({node_id}) exit-proof row at {} among {mesh}'s status rows: {e}", incarnation.as_str()))
    }

    /// [`Self::put_exit`] of the birth an exact Exited receipt names (a TerminateRuntime's, or the
    /// lost final primary's provider proof a mesh leave recorded).
    pub async fn put_exit_of_receipt(&self, t: &crate::deployment::pipeline::TerminalReceipt) -> Result<crate::status_storage::NodeExitRow, String> {
        let name: PathName = t.node.parse().map_err(|e| format!("the Exited receipt of {} {} names no path.name: {e:?}", t.node, t.incarnation_id))?;
        let mut node = Node::allocated(name);
        node.node_id = NodeId::parse(&t.node_id).map_err(|e| format!("the Exited receipt of {} {} names no node id ({:?}): {e}", t.node, t.incarnation_id, t.node_id))?;
        node.incarnation_id = Some(IncarnationId::parse(&t.incarnation_id).map_err(|e| format!("the Exited receipt of {} {} names no incarnation: {e}", t.node, t.incarnation_id))?);
        self.put_exit(&node, t.exit_code, "terminal-receipt").await
    }

    /// The membership book whose seat records the exit read covers (set once, at start).
    pub fn set_book(&self, book: DigestBook) {
        let _ = self.book.set(book);
    }

    /// The read of the exit-proof rows, one part of the view's read phase (taken before the view's
    /// write lock, [`ViewReads::read`], [`Self::install_projected`]): the row of every birth a projection draws from `book`
    /// and these records (each digest held, each stopped birth with or without a digest, each launch
    /// record) and of every seat holder the book records, a point read among its own mesh's status
    /// rows, and the proven exits held. The store root is the fabric's own (R14), so a birth of
    /// another fabric names no row there. An exit-proof row is never withdrawn (the status store
    /// puts and point-reads it, nothing removes it), so the fold is monotone for every birth this
    /// pass names: what this pass read, plus every exit already held for a birth it names (a read
    /// older than one already stored, or a read that failed, takes nothing away). A birth this pass
    /// no longer names drops out. A row that cannot be read proves nothing new and is named on one
    /// `rdm.node_admin.node.reject.via-exit-row-unreadable` span.
    pub async fn read_exits(&self, book: &DigestBook) {
        let Some(storage) = self.status.get().map(|f| f.storage().clone()) else { return };
        let mut births: HashMap<(NodeId, IncarnationId), (String, String)> = HashMap::new();
        for d in book.all().into_iter().chain(book.parked()) {
            births.entry((d.node.node_id.clone(), d.node.incarnation.clone())).or_insert((d.node.name.mesh.clone(), d.node.name.to_string()));
        }
        for op in book.parked_unheard() {
            births.entry((op.node_id.clone(), op.incarnation.clone())).or_insert((op.name.mesh.clone(), op.name.to_string()));
        }
        for n in self.nodes.lock().unwrap().values() {
            if let Some(inc) = &n.incarnation_id {
                births.entry((n.node_id.clone(), inc.clone())).or_insert((n.mesh.clone(), n.name.to_string()));
            }
        }
        for h in book.seats.fabric().into_iter().chain(book.seats.meshes().into_values()) {
            births.entry((h.node_id.clone(), h.incarnation.clone())).or_insert((h.mesh.clone(), format!("the {} seat holder", h.mesh)));
        }
        let named: std::collections::HashSet<(NodeId, IncarnationId)> = births.keys().cloned().collect();
        let mut read: HashMap<(NodeId, IncarnationId), (String, crate::status_storage::NodeExitRow)> = HashMap::new();
        let mut unreadable = Vec::new();
        for ((node_id, inc), (mesh, named)) in births {
            match storage.node_exit_of(&mesh, &node_id, &inc).await {
                Ok(Some(row)) => {
                    read.insert((node_id, inc), (mesh, row));
                }
                Ok(None) => {}
                Err(e) => unreadable.push(format!("{named} ({node_id} at {}) among {mesh}'s status rows: {e}", inc.as_str())),
            }
        }
        if !unreadable.is_empty() {
            tracing::info_span!("rdm.node_admin.node.reject.via-exit-row-unreadable", rows = unreadable.len(), detail = %unreadable.join("; "))
                .in_scope(|| tracing::info!("an exit-proof row could not be read: this pass proves nothing new of that birth"));
        }
        // The fold, against what is held now (another install may have stored since this pass began).
        let entered: Vec<(String, crate::status_storage::NodeExitRow)> = {
            let mut exited = self.exited.lock().unwrap();
            let entered = read.iter().filter(|(k, _)| !exited.contains(*k)).map(|(_, v)| v.clone()).collect();
            let kept: std::collections::HashSet<(NodeId, IncarnationId)> = exited.iter().filter(|k| named.contains(*k)).cloned().collect();
            *exited = kept.into_iter().chain(read.into_keys()).collect();
            entered
        };
        for (mesh, row) in entered {
            tracing::info_span!("rdm.node_admin.node.resolve.via-exit-proof-row", node = %row.name, node_id = %row.node_id, incarnation_id = %row.incarnation_id.as_str(), mesh = %mesh, exit_code = row.exit_code.map(|c| c.to_string()).unwrap_or_default(), proof = %row.proof)
                .in_scope(|| tracing::info!("the projection reads the birth's exit-proof row: it is held exited, Dead and gone from every seat"));
        }
    }

    /// Whether the birth's exit-proof row was read by the installed projection.
    pub fn is_exited(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        self.exited.lock().unwrap().contains(&(node_id.clone(), incarnation.clone()))
    }

    /// The admin has been told (or needs no telling of) who holds the seats: elections run.
    pub fn set_entering(&self, entering: bool) {
        self.entering.store(entering, std::sync::atomic::Ordering::SeqCst);
    }

    /// Whether this admin has not yet been told who holds the seats (its entry read has not answered).
    pub fn is_entering(&self) -> bool {
        self.entering.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The incarnations whose exit-proof row the installed projection read.
    fn exited_incarnations(&self) -> std::collections::HashSet<IncarnationId> {
        self.exited.lock().unwrap().iter().map(|(_, i)| i.clone()).collect()
    }

    /// A non-holder of the fabric-primary role serves the `degraded` status the holder published.
    pub fn published_degraded(&self) -> bool {
        self.published_degraded.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Hold (or clear) the published `degraded`.
    pub fn set_published_degraded(&self, on: bool) {
        self.published_degraded.store(on, std::sync::atomic::Ordering::SeqCst);
    }

    /// May this admin declare its Mesh ready (see the field)?
    pub fn declare_gate(&self) -> bool {
        self.declare_gate.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Open or close the gate on declaring this admin's Mesh ready.
    pub fn set_declare_gate(&self, open: bool) {
        if self.declare_gate.swap(open, std::sync::atomic::Ordering::SeqCst) != open {
            self.wake.poke();
        }
    }

    /// Install the projection `project` yields as the view, and return it. The projection is taken
    /// while the view lock is held, so of two installers the one that holds the lock last installs
    /// the later state: a projection taken before waiting could carry a removal's absence away
    /// again. The declarer wakes when the view changed; a write of the same view wakes nothing.
    ///
    /// The view's read phase runs first, before its write lock is taken: no disk read holds a
    /// reader of the view. It reads the exit-proof rows of the births the book this admin's
    /// membership holds and these records name ([`Self::read_exits`]); the projection taken under
    /// the lock reads the exits held. A birth that enters the book between the two is read at the
    /// next rebuild.
    pub async fn install_projected(&self, topology: &RwLock<Topology>, project: impl FnOnce() -> Topology) -> Topology {
        if let Some(book) = self.book.get() {
            self.read_exits(book).await;
        }
        let held = topology.write().await;
        let t = project();
        self.install(held, t).await
    }

    /// [`Self::install_projected`] of the admin `me`'s projection from what the stores hold: the
    /// view's one read phase, [`ViewReads::read`], runs first over `book`'s births of `fabric_id`, and
    /// only then is the view's write lock taken and `project` handed the reads: no disk read holds a
    /// reader of the view. A read older than the view it replaces is replaced at the next rebuild. A
    /// birth the view lists at a moved row's name for the first time is one span.
    pub async fn install_read(&self, me: &PathName, topology: &RwLock<Topology>, book: &DigestBook, fabric_id: &FabricId, project: impl FnOnce(&ViewReads) -> Topology) -> Topology {
        let reads = ViewReads::read(me, book, self, fabric_id).await;
        let held = topology.write().await;
        let t = project(&reads);
        for (node_id, incarnation, published, name) in reads.rows.moved() {
            let listed = |v: &Topology| v.births().any(|n| &n.node_id == node_id && n.incarnation_id.as_ref() == Some(incarnation) && &n.name == name);
            if listed(&t) && !listed(&held) {
                tracing::info_span!("rdm.node_admin.topology.update.via-row-name", node = %me, subject = %name, published = %published, node_id = %node_id, incarnation_id = %incarnation.as_str())
                    .in_scope(|| tracing::info!("the view lists a birth that does not publish at the name its row holds"));
            }
        }
        self.install(held, t).await
    }

    /// Install `t` over the view `held` (its write lock held). The door opens here, before the lock is
    /// released, so no reader of the installed view sees this admin's birth while its door refuses.
    async fn install(&self, mut held: tokio::sync::RwLockWriteGuard<'_, Topology>, t: Topology) -> Topology {
        if let Some(door) = self.door.get() {
            door.open_if_held(&t);
        }
        if *held != t {
            // The seat of this admin's mesh taken with this view: its status rows are folded in
            // before the view is installed, so no decision reads the view against an empty record.
            if let Some(fold) = self.status.get() {
                fold.on_install(&t, &self.declared, self.book.get()).await;
            }
            // A replaced predecessor this view now names `<path>.old` (its row moved there; its digest
            // still says `<path>`): one span as it enters, with the member that took the path.
            for old in t.predecessors().filter(|o| !held.predecessors().any(|h| h.node_id == o.node_id && h.incarnation_id == o.incarnation_id)) {
                tracing::info_span!("rdm.node_admin.topology.update.via-predecessor-held", node = %old.name, node_id = %old.node_id, status = ?old.status)
                    .in_scope(|| tracing::info!("the view names a replaced predecessor under its .old path"));
            }
            // The births the fabric-primary's view holds for the first time: their rows are written
            // from this held record once the view is installed (`crate::node_rows`).
            let arrivals = self.rows.get().map(|rows| rows.arrivals(&held, &t)).unwrap_or_default();
            // The backbone holds the meshes this view names, each deleted or not, from this install on
            // and in the order views are installed: the fabric-primary publishes a deleted mesh's
            // aggregate from its last admin's stop on (mesh-delete.md step 7), and never a live
            // mesh's (R-RX4). Its own mesh.deleted receipt is one such install
            // (`admin::leave::record_mesh_deleted`), so a stop that follows it reads the mesh deleted.
            if let Some(backbone) = self.backbone.get() {
                backbone.set_view_meshes(view_meshes(&t));
            }
            *held = t.clone();
            drop(held);
            self.view_tick.send_modify(|v| *v += 1);
            self.wake.poke();
            self.view_changed.notify_one();
            if let (Some(rows), false) = (self.rows.get(), arrivals.is_empty()) {
                rows.write_arrivals(&t, arrivals).await;
            }
        }
        t
    }

    /// A fabric shutdown is held: keep the seats as they were before any admin drained.
    pub fn hold_seats(&self) {
        let mut seats = self.seats.lock().unwrap();
        if seats.held.is_none() {
            seats.held = Some(seats.calm.clone());
        }
    }

    /// The seats held, by node, as `(mesh primary, fabric primary)`, when held.
    pub fn held_seats(&self) -> Option<BTreeMap<NodeId, (bool, bool)>> {
        self.seats.lock().unwrap().held.clone()
    }
}

impl TopologySink for Records {
    fn publish(&self, node: Node) {
        // A publication writes no contact: a mesh's contacts are its primary's alone
        // (`MeshRowsWriter`, the writer ladder).
        self.removed.lock().unwrap().remove(&(node.name.clone(), node.incarnation_id.clone()));
        self.nodes.lock().unwrap().insert(node.name.clone(), node);
    }
    fn remove(&self, name: &PathName) {
        let gone = self.nodes.lock().unwrap().remove(name);
        self.removed.lock().unwrap().insert((name.clone(), gone.and_then(|n| n.incarnation_id)));
    }
    fn remove_birth(&self, name: &PathName, node_id: &NodeId, incarnation: &IncarnationId) {
        // Only the exact birth leaves the records; another birth held at the path (the new node a
        // recovery created there) stays. The exited birth is held removed whether or not the
        // records held it, so the view does not take it back from the book.
        let mut nodes = self.nodes.lock().unwrap();
        if nodes.get(name).is_some_and(|n| n.node_id == *node_id && n.incarnation_id.as_ref() == Some(incarnation)) {
            nodes.remove(name);
        }
        self.removed.lock().unwrap().insert((name.clone(), Some(incarnation.clone())));
    }
    fn rename_predecessor(&self, name: &PathName, _node_id: &NodeId, incarnation: &IncarnationId) {
        // The predecessor's launch record no longer stands at the path.name its successor takes; the
        // view names it by its row (`ViewReads`), which its mesh's row writer moves to `<path>.old`.
        let mut nodes = self.nodes.lock().unwrap();
        if nodes.get(name).is_some_and(|n| n.incarnation_id.as_ref() == Some(incarnation)) {
            nodes.remove(name);
        }
    }
    fn node_rows(&self) -> Option<Arc<crate::node_rows::NodeRows>> {
        self.rows.get().cloned()
    }
}

/// What an election takes beside the nodes: the seat records this admin holds, and the births
/// proven gone. A birth is proven gone when its runtime was found exited, when its departure was
/// heard, or when a later birth announced that it replaced it (`supersedes`). Silence, an
/// unreachable path and the observer-inferred `Dead` prove nothing.
pub(crate) fn incumbency_of(book: &DigestBook, records: &Records) -> crate::election::Incumbency {
    let mut lost = records.exited_incarnations();
    lost.extend(book.departed().into_iter().map(|op| op.incarnation));
    lost.extend(book.all().into_iter().filter_map(|d| d.node.supersedes));
    lost.extend(book.seats.gone());
    crate::election::Incumbency { fabric: book.seats.fabric(), meshes: book.seats.meshes(), lost }
}

/// Where this birth's Build log reads its authority: the committed fabric seat record held in
/// `book`, judged by [`crate::seat_watch::fabric_seat_record`] against the births proven gone (the
/// lost set every election takes, [`incumbency_of`]) and named from the digests `book` holds.
pub(crate) fn fabric_seat_source(book: &DigestBook, records: &Arc<Records>, me: (NodeId, IncarnationId), me_name: &PathName) -> crate::fabric_builds::SeatSource {
    let changed = Some(book.seats.subscribe());
    let (book, records, me_name) = (book.clone(), records.clone(), me_name.to_string());
    let read = Arc::new(move || {
        let incumbency = incumbency_of(&book, &records);
        let name_of = |id: &NodeId| if *id == me.0 { Some(me_name.clone()) } else { book.get(id.as_str()).map(|(d, _)| d.node.name.to_string()) };
        crate::seat_watch::fabric_seat_record(incumbency.fabric.as_ref(), &me, &incumbency.lost, name_of)
    });
    crate::fabric_builds::SeatSource { read, changed }
}

/// Where this birth's drift-lane opens and claims read their seat (R-BI7): the committed record
/// of its own mesh's primary seat held in `book`, judged as [`fabric_seat_record`] judges the fabric
/// seat's.
pub(crate) fn mesh_seat_source(book: &DigestBook, records: &Arc<Records>, me: (NodeId, IncarnationId), me_name: &PathName, mesh: &str) -> crate::fabric_builds::SeatSource {
    let changed = Some(book.seats.subscribe());
    let (book, records, me_name, mesh) = (book.clone(), records.clone(), me_name.to_string(), mesh.to_string());
    let read = Arc::new(move || {
        let incumbency = incumbency_of(&book, &records);
        let name_of = |id: &NodeId| if *id == me.0 { Some(me_name.clone()) } else { book.get(id.as_str()).map(|(d, _)| d.node.name.to_string()) };
        crate::seat_watch::mesh_seat_record(&mesh, incumbency.meshes.get(&mesh), &me, &incumbency.lost, name_of)
    });
    crate::fabric_builds::SeatSource { read, changed }
}

/// The seats this admin announces from one decision of its hierarchy loop: each seat the projection
/// `t` gives it (`live`: a cut-off view authorizes nothing) whose record names another birth, at the
/// next epoch over that record. The record is the one `t` was decided against (`decided`): a seat
/// record held after the decision (a committed handover landing while the loop awaits) is the next
/// decision's input, and is never superseded by this one (fabric-node-lifecycle-elections.md §4.4).
/// The record this admin already holds for itself needs no announcement.
pub(crate) fn seat_announcements(t: &Topology, live: bool, mesh: &str, me: (&NodeId, &IncarnationId), decided: &crate::election::Incumbency) -> Vec<(rafka_mesh_entity::Seat, rafka_mesh_entity::SeatHolder)> {
    let mesh_primary = live && t.holds_mesh_seat(mesh, me.0);
    let fabric_primary = live && t.holds_fabric_seat(me.0);
    let mut said = Vec::new();
    for (seat, holds) in [(rafka_mesh_entity::Seat::MeshPrimary, mesh_primary), (rafka_mesh_entity::Seat::FabricPrimary, fabric_primary)] {
        if !holds {
            continue;
        }
        let held = match seat {
            rafka_mesh_entity::Seat::MeshPrimary => decided.meshes.get(mesh).cloned(),
            rafka_mesh_entity::Seat::FabricPrimary => decided.fabric.clone(),
        };
        if held.as_ref().is_some_and(|h| h.is_birth(me.0, me.1)) {
            continue;
        }
        said.push((seat, rafka_mesh_entity::SeatHolder { mesh: mesh.to_string(), node_id: me.0.clone(), incarnation: me.1.clone(), epoch: held.map(|h| h.epoch + 1).unwrap_or(1) }));
    }
    said
}

/// The observed topology from membership digests and this admin's records,
/// with the settled primary rule (module docs). No node row is read: each birth is listed at the name
/// it published ([`project_with`] names the births that do not publish by their rows).
pub fn project(fabric: &str, fabric_id: &FabricId, provider: ProviderKind, book: &DigestBook, records: &Records) -> Topology {
    project_at(fabric, fabric_id, provider, book, records, std::time::Instant::now())
}

/// [`project`] as of the instant `reads` were taken at, from what they read: each birth that does not
/// publish its own name listed at the name its row holds.
pub fn project_with(fabric: &str, fabric_id: &FabricId, provider: ProviderKind, book: &DigestBook, records: &Records, reads: &ViewReads) -> Topology {
    project_decided_at(fabric, fabric_id, provider, book, records, &incumbency_of(book, records), reads)
}

/// What a view rebuild reads from the stores, taken in one read phase before the view's write lock
/// ([`Records::install_read`], `view_now`) and handed to the projection under it. Every store read a
/// projection needs is read in [`ViewReads::read`]: the exit-proof rows, folded into the records
/// ([`Records::read_exits`]), and the node rows, a field here; nothing is read under the lock.
pub struct ViewReads {
    /// The instant the births were judged at: the projection is taken as of it, so the births it lists
    /// from these reads are exactly the ones the reads covered.
    at: std::time::Instant,
    /// The names the node rows hold for the births that do not publish their own.
    rows: RowNames,
}

impl ViewReads {
    /// Nothing read: every birth is listed at the name it published, as of `at`.
    pub fn none_at(at: std::time::Instant) -> Self {
        Self { at, rows: RowNames::default() }
    }

    /// The read phase of the admin `me`'s rebuild over `book`'s births of `fabric_id`, as of now: first
    /// the exit-proof rows of the births `book` and `records` name ([`Records::read_exits`]), then the
    /// row of every birth that does not publish its own name, from `records`' nodes store (no store:
    /// nothing is read). The exits are read first: a birth proven exited does not publish its name, so
    /// its row is read with the rest.
    pub async fn read(me: &PathName, book: &DigestBook, records: &Records, fabric_id: &FabricId) -> Self {
        records.read_exits(book).await;
        let at = std::time::Instant::now();
        Self { at, rows: RowNames::read(me, book, records, fabric_id, at).await }
    }
}

/// The names the node rows hold for the births a projection lists that do not publish their own
/// (CLAUDE.md read rule: the node-row kind declares no projection, so each is a point read through a
/// store index, read for one projection and held by nothing after it; the caller holds the birth's
/// node id, so it is the row's id index, `NodesStorage::node_row_name`: one read per birth). A birth
/// that publishes names itself: its digest carries its row's name (R12: it publishes only the row's
/// name, a live rename reaching it as `ApplyNodeName`). One that does not (stopped, silent past the
/// staleness floor, proven exited, held true offline) may have had its row moved while it said
/// nothing: a stopped node's `node.name.update` (R13: it is told nothing and reads the name at its next
/// join) or a replace's `RenamePredecessor`.
#[derive(Default)]
struct RowNames {
    /// Each birth read: the name it published, and where its row stands.
    names: HashMap<(NodeId, IncarnationId), (PathName, RowName)>,
}

/// Where the row of one birth stands.
enum RowName {
    /// The name of the row the birth holds (the name it published, when it holds no row).
    At(PathName),
    /// Its id index entry could not be read: the birth's name is not known, and the view does not
    /// list it.
    Unreadable,
}

impl RowNames {
    /// The row of every birth of `fabric_id` in `book` that does not publish its own name as of `at`,
    /// read by the admin `me` from `records`' nodes store (no store: nothing is read).
    async fn read(me: &PathName, book: &DigestBook, records: &Records, fabric_id: &FabricId, at: std::time::Instant) -> Self {
        let mut rows = Self::default();
        let Some(store) = records.contacts.get().cloned() else { return rows };
        let mut quiet: Vec<(NodeId, IncarnationId, PathName)> = Vec::new();
        for d in book.all() {
            let Some((_, age)) = book.get_at(d.node.node_id.as_str(), at) else { continue };
            if &d.fabric_id == fabric_id && !publishes(book, records, &d.node.node_id, &d.node.incarnation, age) {
                quiet.push((d.node.node_id.clone(), d.node.incarnation.clone(), d.node.name.clone()));
            }
        }
        quiet.extend(book.parked().into_iter().filter(|d| &d.fabric_id == fabric_id).map(|d| (d.node.node_id, d.node.incarnation, d.node.name)));
        quiet.extend(book.parked_unheard().into_iter().map(|op| (op.node_id, op.incarnation, op.name)));
        for (node_id, incarnation, published) in quiet {
            let name = row_name(me, &*store, &node_id, &incarnation, &published).await;
            rows.names.insert((node_id, incarnation), (published, name));
        }
        rows
    }

    /// Each birth read whose row stands at another name than the one it published:
    /// `(node id, incarnation, published, row's name)`.
    fn moved(&self) -> impl Iterator<Item = (&NodeId, &IncarnationId, &PathName, &PathName)> {
        self.names.iter().filter_map(|((id, inc), (published, row))| match row {
            RowName::At(name) if name != published => Some((id, inc, published, name)),
            _ => None,
        })
    }

    /// The name the projection lists the birth `(node_id, incarnation)` at, which published
    /// `published`: its row's when this read covered it, else the published one. `None`: its row
    /// could not be read, and the birth is not listed.
    fn name_of(&self, node_id: &NodeId, incarnation: &IncarnationId, published: &PathName) -> Option<PathName> {
        match self.names.get(&(node_id.clone(), incarnation.clone())) {
            Some((_, RowName::At(n))) => Some(n.clone()),
            Some((_, RowName::Unreadable)) => None,
            None => Some(published.clone()),
        }
    }
}

/// The birth speaks for its own name: it published within the staleness floor and is neither proven
/// exited nor held true offline.
fn publishes(book: &DigestBook, records: &Records, node_id: &NodeId, incarnation: &IncarnationId, age: Duration) -> bool {
    age <= book.staleness_floor() && !records.is_exited(node_id, incarnation) && !records.offline.lock().unwrap().contains(&(node_id.clone(), incarnation.clone()))
}

/// Where the row of the birth `(node_id, incarnation)`, which last published `published`, stands, as
/// the admin `me` reads it: one point read through the row's id index in the birth's mesh (a rename
/// never changes the mesh). An index entry that cannot be read is one span on every projection that
/// reads it, until it can be read.
async fn row_name(me: &PathName, store: &dyn crate::storage::NodesStorage, node_id: &NodeId, incarnation: &IncarnationId, published: &PathName) -> RowName {
    match store.node_row_name(&published.mesh, node_id).await {
        Ok(Some(name)) => RowName::At(name),
        // The node holds no row: no rename moved it, and the name it published stands.
        Ok(None) => RowName::At(published.clone()),
        Err(e) => {
            tracing::info_span!("rdm.node_admin.topology.reject.via-row-unreadable", node = %me, subject = %published, node_id = %node_id, incarnation_id = %incarnation.as_str(), index = "id", detail = %e)
                .in_scope(|| tracing::info!("the birth's row could not be read through its id index: its name is not known and the view does not list it"));
            RowName::Unreadable
        }
    }
}

/// Projects the book into the view now: what the join door runs once it installed an admitted
/// birth's address, so the view the status authority resolves senders from holds that birth.
pub fn view_projector(me: PathName, topology: Arc<RwLock<Topology>>, fabric: String, fabric_id: FabricId, provider: ProviderKind, book: DigestBook, records: Arc<Records>) -> Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync> {
    Arc::new(move || {
        let (me, topology, fabric, fabric_id, book, records) = (me.clone(), topology.clone(), fabric.clone(), fabric_id.clone(), book.clone(), records.clone());
        Box::pin(async move {
            records.install_read(&me, &topology, &book, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &book, &records, reads)).await;
        })
    })
}

/// Why [`project`] lacks `name` right now, from the same inputs: every digest the book holds for
/// the name (incarnation, status, unheard age, predecessor, forwarded, departed, routable), the
/// removed set's entries for it, and the records' entry. The text of an unknown-node refusal from
/// the live view, so the exclusion is named where it happens.
pub fn describe_absence(book: &DigestBook, records: &Records, name: &PathName) -> String {
    let now = std::time::Instant::now();
    let digests: Vec<String> = book
        .all()
        .into_iter()
        .filter(|d| &d.node.name == name)
        .map(|d| {
            let age = book.get_at(d.node.node_id.as_str(), now).map(|(_, a)| a.as_millis() as u64).unwrap_or(0);
            format!(
                "digest{{incarnation {}, status {:?}, unheard {} ms, supersedes {}, departed {}, routable {}, floor {} ms}}",
                d.node.incarnation.as_str(),
                d.status,
                age,
                d.node.supersedes.as_ref().map(|s| s.to_string()).unwrap_or_else(|| "none".into()),
                book.is_departed(d.node.node_id.as_str()),
                book.routable(d.node.node_id.as_str()),
                book.staleness_floor().as_millis()
            )
        })
        .collect();
    let removed: Vec<String> = records
        .removed
        .lock()
        .unwrap()
        .iter()
        .filter(|(n, _)| n == name)
        .map(|(_, inc)| inc.as_ref().map(|i| i.to_string()).unwrap_or_else(|| "none".into()))
        .collect();
    let recorded = records.nodes.lock().unwrap().get(name).map(|n| format!("incarnation {}, status {:?}", n.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_else(|| "none".into()), n.status));
    format!(
        "book: [{}]; removed: [{}]; records: {}",
        if digests.is_empty() { "no digest for the name".to_string() } else { digests.join(", ") },
        removed.join(", "),
        recorded.unwrap_or_else(|| "none".into())
    )
}

/// [`project`] as of `now`.
pub fn project_at(fabric: &str, fabric_id: &FabricId, provider: ProviderKind, book: &DigestBook, records: &Records, now: std::time::Instant) -> Topology {
    project_decided_at(fabric, fabric_id, provider, book, records, &incumbency_of(book, records), &ViewReads::none_at(now))
}

/// [`project_with`] with its seats decided against `decided`, the seat records and proven-gone births
/// read once: a caller that acts on the projection's seats (the hierarchy loop announcing one)
/// acts against the very records the projection was decided from, never a later read of them.
pub(crate) fn project_decided(fabric: &str, fabric_id: &FabricId, provider: ProviderKind, book: &DigestBook, records: &Records, decided: &crate::election::Incumbency, reads: &ViewReads) -> Topology {
    project_decided_at(fabric, fabric_id, provider, book, records, decided, reads)
}

/// The launch record this admin holds of the exact birth `incarnation`, whatever name the view lists
/// the birth at.
fn launched(recorded: &BTreeMap<PathName, Node>, incarnation: &Option<IncarnationId>) -> Option<Node> {
    incarnation.as_ref().and_then(|inc| recorded.values().find(|r| r.incarnation_id.as_ref() == Some(inc)).cloned())
}

fn project_decided_at(fabric: &str, fabric_id: &FabricId, provider: ProviderKind, book: &DigestBook, records: &Records, decided: &crate::election::Incumbency, reads: &ViewReads) -> Topology {
    let (now, rows) = (reads.at, &reads.rows);
    let removed = records.removed.lock().unwrap().clone();
    let recorded = records.nodes.lock().unwrap().clone();
    let mut nodes: BTreeMap<PathName, Node> = BTreeMap::new();
    // Every birth membership has spoken for, whatever became of it.
    let mut heard: HashSet<IncarnationId> = HashSet::new();
    // Each birth's election claim, by incarnation.
    let mut mesh_ids: BTreeMap<String, MeshId> = records.meshes.lock().unwrap().clone();
    for d in book.all() {
        if &d.fabric_id != fabric_id {
            continue;
        }
        let Some((_, age)) = book.get_at(d.node.node_id.as_str(), now) else { continue };
        heard.insert(d.node.incarnation.clone());
        // A birth that publishes names itself (R12); one that does not is listed at the name its row
        // holds (a predecessor a replace renamed `<path>.old`), one whose row could not be read not at all.
        let name = if publishes(book, records, &d.node.node_id, &d.node.incarnation, age) {
            d.node.name.clone()
        } else {
            match rows.name_of(&d.node.node_id, &d.node.incarnation, &d.node.name) {
                Some(n) => n,
                None => continue,
            }
        };
        // A birth under an open restart is held through everything below until its later birth is
        // heard: the restart's own retire (its Leaving, its removal from these records) is not a
        // departure (fabric-node-lifecycle.md: Restarting is commanded silence).
        let restarting = book.restart_of(d.node.node_id.as_str(), &d.node.incarnation).is_some();
        if !restarting && removed.contains(&(name.clone(), Some(d.node.incarnation.clone()))) {
            continue;
        }
        let silent = age > book.staleness_floor();
        if let Some(id) = d.mesh_id.clone() {
            mesh_ids.entry(name.mesh.clone()).or_insert(id);
        }
        let mut n = Node::allocated(name.clone());
        n.node_id = d.node.node_id.clone();
        n.endpoint_id = Some(d.node.endpoint_id.clone());
        n.incarnation_id = Some(d.node.incarnation.clone());
        n.provider = Some(provider);
        // Silent past the staleness floor: this node's own pruner marks it PendingReconnect in place;
        // `Dead` only while the mesh primary's offline tickle holds this exact birth true offline.
        n.status = if restarting {
            NodeStatus::Restarting
        } else if records.is_exited(&d.node.node_id, &d.node.incarnation) {
            NodeStatus::Dead
        } else if !silent {
            node_status(d.status)
        } else if records.offline.lock().unwrap().contains(&(d.node.node_id.clone(), d.node.incarnation.clone())) {
            NodeStatus::Dead
        } else {
            NodeStatus::PendingReconnect
        };
        // Application routing (states.md "Routing"): the birth listed ready-for-traffic, and the book
        // holds it and its mesh ready-for-traffic. Anything else is a routing stop.
        n.routable = n.status == NodeStatus::ReadyForTraffic && book.routable(d.node.node_id.as_str());
        n.declared = n.incarnation_id.as_ref().and_then(|inc| records.declared.lock().unwrap().node(&n.node_id, inc)).map(|s| format!("{s:?}"));
        n.admin_api_base = d.admin_api_base.clone();
        n.config = d.config.clone();
        n.transport_addr = Some(d.node.transport_addr);
        n.data_dir = d.data_dir.clone();
        // The birth's own published runtime fact names its deployment, whichever admin launched it.
        n.deployment_id = d.node.runtime.as_ref().and_then(|f| f.deployment().ok());
        if let Some(r) = launched(&recorded, &n.incarnation_id) {
            n.deployment_id = r.deployment_id.clone().or(n.deployment_id.take());
            n.data_dir = r.data_dir.clone();
        }
        // One birth per path: the newest digest wins over a superseded one.
        match nodes.get(&name) {
            Some(prev) if prev.status.is_live() && !n.status.is_live() => {}
            _ => {
                nodes.insert(name, n);
            }
        }
    }
    // A parked birth (stopped, its process alive): the book dropped it, and this view keeps it,
    // `Leaving` and `parked`, until it rejoins as itself or departs.
    for d in book.parked() {
        if &d.fabric_id != fabric_id || book.is_departed(d.node.node_id.as_str()) {
            continue;
        }
        heard.insert(d.node.incarnation.clone());
        // A stopped node is told nothing of a rename (R13): it is listed at the name its row holds.
        let Some(name) = rows.name_of(&d.node.node_id, &d.node.incarnation, &d.node.name) else { continue };
        if removed.contains(&(name.clone(), Some(d.node.incarnation.clone()))) {
            continue;
        }
        // Its held digest states its mesh's id as a heard one does: a mesh whose births are all
        // stopped keeps its id, and with it its state (states.md: a mesh's deleted is final).
        if let Some(id) = d.mesh_id.clone() {
            mesh_ids.entry(name.mesh.clone()).or_insert(id);
        }
        let mut n = Node::allocated(name.clone());
        n.node_id = d.node.node_id.clone();
        n.endpoint_id = Some(d.node.endpoint_id.clone());
        n.incarnation_id = Some(d.node.incarnation.clone());
        n.provider = Some(provider);
        n.status = if records.is_exited(&d.node.node_id, &d.node.incarnation) { NodeStatus::Dead } else { NodeStatus::Leaving };
        n.parked = n.status == NodeStatus::Leaving;
        n.routable = false;
        n.declared = n.incarnation_id.as_ref().and_then(|inc| records.declared.lock().unwrap().node(&n.node_id, inc)).map(|s| format!("{s:?}"));
        n.admin_api_base = d.admin_api_base.clone();
        n.config = d.config.clone();
        n.transport_addr = Some(d.node.transport_addr);
        n.data_dir = d.data_dir.clone();
        n.deployment_id = d.node.runtime.as_ref().and_then(|f| f.deployment().ok());
        if let Some(r) = launched(&recorded, &n.incarnation_id) {
            n.deployment_id = r.deployment_id.clone().or(n.deployment_id.take());
            n.data_dir = r.data_dir.clone();
        }
        nodes.entry(name).or_insert(n);
    }
    // A parked birth this admin never held a digest of (it joined after the stop): the stop
    // overlay the mesh topology carried names it, `Leaving` and `parked`, until it rejoins or departs.
    for op in book.parked_unheard() {
        if book.is_departed(op.node_id.as_str()) {
            continue;
        }
        heard.insert(op.incarnation.clone());
        let Some(name) = rows.name_of(&op.node_id, &op.incarnation, &op.name) else { continue };
        if removed.contains(&(name.clone(), Some(op.incarnation.clone()))) {
            continue;
        }
        let mut n = Node::allocated(name.clone());
        n.node_id = op.node_id.clone();
        n.incarnation_id = Some(op.incarnation.clone());
        n.provider = Some(provider);
        n.status = if records.is_exited(&op.node_id, &op.incarnation) { NodeStatus::Dead } else { NodeStatus::Leaving };
        n.parked = n.status == NodeStatus::Leaving;
        n.routable = false;
        n.declared = n.incarnation_id.as_ref().and_then(|inc| records.declared.lock().unwrap().node(&n.node_id, inc)).map(|s| format!("{s:?}"));
        if let Some(r) = launched(&recorded, &n.incarnation_id) {
            n.deployment_id = r.deployment_id.clone();
            n.data_dir = r.data_dir.clone();
        }
        nodes.entry(name).or_insert(n);
    }
    // An own-mesh birth this admin holds only from its mesh-primary's projection (gossip.md §3.1
    // `Members`: what every node-admin holds of every Mesh; the member's own Digest never reached this
    // admin): listed as the projection carried it, never routable, its status the projection's while
    // it is within the staleness floor of this admin first holding it, and past the floor marked
    // `PendingReconnect` by this admin's own pruner (node-recovery.md "Detection and proof" step 1),
    // so the window it keeps runs from that first hold, whichever admin takes the seat.
    for (d, age) in book.projected_unheard_at(now) {
        if &d.fabric_id != fabric_id {
            continue;
        }
        heard.insert(d.node.incarnation.clone());
        let Some(name) = rows.name_of(&d.node.node_id, &d.node.incarnation, &d.node.name) else { continue };
        if removed.contains(&(name.clone(), Some(d.node.incarnation.clone()))) {
            continue;
        }
        if let Some(id) = d.mesh_id.clone() {
            mesh_ids.entry(name.mesh.clone()).or_insert(id);
        }
        let mut n = Node::allocated(name.clone());
        n.node_id = d.node.node_id.clone();
        n.endpoint_id = Some(d.node.endpoint_id.clone());
        n.incarnation_id = Some(d.node.incarnation.clone());
        n.provider = Some(provider);
        n.status = if records.is_exited(&d.node.node_id, &d.node.incarnation) {
            NodeStatus::Dead
        } else if age <= book.staleness_floor() {
            node_status(d.status)
        } else if records.offline.lock().unwrap().contains(&(d.node.node_id.clone(), d.node.incarnation.clone())) {
            NodeStatus::Dead
        } else {
            NodeStatus::PendingReconnect
        };
        n.routable = false;
        n.declared = n.incarnation_id.as_ref().and_then(|inc| records.declared.lock().unwrap().node(&n.node_id, inc)).map(|s| format!("{s:?}"));
        n.admin_api_base = d.admin_api_base.clone();
        n.config = d.config.clone();
        n.transport_addr = Some(d.node.transport_addr);
        n.data_dir = d.data_dir.clone();
        n.deployment_id = d.node.runtime.as_ref().and_then(|f| f.deployment().ok());
        if let Some(r) = launched(&recorded, &n.incarnation_id) {
            n.deployment_id = r.deployment_id.clone().or(n.deployment_id.take());
            n.data_dir = r.data_dir.clone();
        }
        nodes.entry(name).or_insert(n);
    }
    // Births this admin started that have not reported yet. A launch record
    // stands in for a birth only until membership speaks for it: once heard,
    // it never revives the birth (another admin may have retired it since). A
    // departed birth was heard and has left: its record never stands in either.
    for op in book.departed() {
        heard.insert(op.incarnation.clone());
    }
    for (name, mut r) in recorded {
        if r.incarnation_id.as_ref().is_some_and(|i| heard.contains(i)) || book.is_departed(r.node_id.as_str()) {
            continue;
        }
        // Its exit-proof row read: the birth is Dead, whatever its record last held.
        if r.incarnation_id.as_ref().is_some_and(|i| records.is_exited(&r.node_id, i)) {
            r.status = NodeStatus::Dead;
            r.routable = false;
        }
        nodes.entry(name).or_insert(r);
    }
    // Each birth whose exit-proof row was read carries the proof, beside the `Dead` it shows: a
    // decision that needs the proof never reads it from the status, which the true-offline mark sets too.
    for n in nodes.values_mut() {
        n.exit_proven = n.incarnation_id.as_ref().is_some_and(|i| records.is_exited(&n.node_id, i));
    }
    let mut topology = Topology {
        fabric: Fabric { id: fabric_id.clone(), name: fabric.into(), status: ScopeStatus::Pending, provider },
        meshes: Vec::new(),
        nodes: nodes.into_values().collect(),
    };
    let planned = records.planned();
    // A mesh the accepted Build names, or one that still has members: a mesh that left (the Build
    // does not name it and it has no member) is not in the topology. A planned mesh that does not
    // exist yet is not in the view either: the view is what is observed, and the Build's plan is
    // `topology − observed`.
    let mesh_names: BTreeSet<String> = mesh_ids.keys().filter(|m| book.accepted_has_mesh(m) != Some(false)).cloned().chain(topology.members().map(|n| n.mesh.clone())).collect();
    // Every seat, by the one election function (`election`); through a fabric shutdown, the
    // seats held when it was learned (`Seats`).
    if records.is_entering() {
        // Not told yet who holds the seats: this view elects nobody.
        for n in topology.nodes.iter_mut() {
            n.is_primary = false;
            n.is_fabric_primary = false;
        }
    } else {
        // The election takes members: a replaced predecessor holds no seat.
        let (mut members, predecessors): (Vec<Node>, Vec<Node>) = std::mem::take(&mut topology.nodes).into_iter().partition(|n| !n.name.old);
        crate::election::resolve_with(&mut members, decided);
        topology.nodes = members.into_iter().chain(predecessors).collect();
    }
    {
        let mut seats = records.seats.lock().unwrap();
        match &seats.held {
            Some(held) => {
                for n in topology.nodes.iter_mut() {
                    let (p, f) = held.get(&n.node_id).copied().unwrap_or((false, false));
                    n.is_primary = p;
                    n.is_fabric_primary = f;
                }
            }
            None => {
                if !topology.members().any(|n| n.kind == NodeKind::NodeAdmin && n.status == NodeStatus::Draining) {
                    seats.calm = topology
                        .members()
                        .filter(|n| n.is_primary || n.is_fabric_primary)
                        .map(|n| (n.node_id.clone(), (n.is_primary, n.is_fabric_primary)))
                        .collect();
                }
            }
        }
    }
    if topology.fabric_primary().is_some() {
        // The fabric primary authors `degraded` from the investigation's rebirth decision until the
        // reborn mesh's primary reports for its round (`crate::investigate`), and a successor holder
        // keeps an adopted `degraded` until every mesh primary reports (`crate::round`). An admin
        // that holds no seat serves the `degraded` the holder published.
        let held = records.fabric_state();
        let degraded = records.peer_recovery.lock().unwrap().is_some() || records.adopted_degraded() || records.published_degraded();
        topology.fabric.status = if held == crate::fabric_state::FabricState::ReadyForTraffic && degraded { ScopeStatus::Degraded } else { held.scope() };
    }
    let declared_meshes = records.declared.lock().unwrap().meshes.clone();
    for m in mesh_names {
        // A mesh becomes ready-for-traffic when every node of the mesh is (states.md); once heard
        // ready it stays ready while it has a primary, because a lost node is a degraded fabric and
        // a round for the primary that takes over, never a mesh gone back to pending.
        let ready = topology.mesh_unready(&m, planned.get(&m)).is_empty() || (records.heard_ready(&m) && topology.cohort_primary(&m, NodeKind::NodeAdmin).is_some());
        let id = mesh_ids.get(&m).cloned();
        // A mesh past ready-for-traffic is the state its row holds (states.md: pending ->
        // ready-for-traffic -> leaving -> dead): the leaving mesh's primary commits Leaving at
        // leave-mesh, and this projection is what that primary announces on its MeshStatus send.
        // Dead is final: after mesh.deleted the mesh row is Dead and its last admin, still its
        // primary, publishes MeshStatus Dead (R-NS1 item 1); an admin that does not author the
        // mesh's status also states the Dead it heard, bound to this mesh id (never to a name).
        let row = id.as_ref().and_then(|id| declared_meshes.get(id).copied());
        let authored_here = records.me.get().is_some_and(|me| topology.cohort_primary(&m, NodeKind::NodeAdmin).is_some_and(|p| &p.node_id == me));
        let status = match row {
            Some(rafka_node_rpc_contract::status::MeshState::Dead) => ScopeStatus::Dead,
            _ if id.as_ref().is_some_and(|id| records.mesh_deleted(id)) => ScopeStatus::Dead,
            _ if records.heard_dead(id.as_ref(), authored_here) => ScopeStatus::Dead,
            Some(rafka_node_rpc_contract::status::MeshState::Leaving) => ScopeStatus::Leaving,
            _ if ready => ScopeStatus::ReadyForTraffic,
            _ => ScopeStatus::Pending,
        };
        topology.meshes.push(Mesh { id, name: m, status });
    }
    topology
}

/// How often the view is rebuilt when no routing fact moved: the projection also reads the clock
/// (a member silent past the staleness floor).
const VIEW_POLL: Duration = Duration::from_millis(200);

/// The view's next rebuild is due: at once when a routing fact the book holds moved since the last
/// rebuild read it (a birth's status, an overlay, a departure, a mesh status: states.md "Routing",
/// applied on receipt), else after `poll`; and only while the admin's gate is open. A parked admin's
/// rebuild is held here, holding nothing, and is due the moment the gate opens. Answers the
/// rebuild's turn: the caller ends it before it waits here again.
pub(crate) async fn view_rebuild_due(facts: &mut rafka_mesh_transport::membership::RoutingWatch, poll: Duration, gate: &crate::park_gate::ParkGate) -> crate::park_gate::Turn {
    tokio::select! {
        _ = tokio::time::sleep(poll) => {}
        _ = facts.moved() => {}
    }
    gate.turn("view-rebuild").await
}

/// Every birth of `fabric` this admin's book holds besides itself, heard a moment ago or only
/// carried in an installed topology read. These are the contacts a restarted admin can try, and the
/// births a maker serves a joining admin for a mesh it holds no snapshot of.
fn held_births(book: &DigestBook, fabric: &FabricId, me: &NodeId) -> Vec<MeshDigest> {
    book.all().into_iter().filter(|d| &d.fabric_id == fabric && &d.node.node_id != me).collect()
}

/// The contacts this admin drops from nodes.storage, of `written`, the contacts it holds, given
/// `held`, the births its book holds and `departed`, whether the book holds a node departed: a
/// contact whose node departed (node.delete's NodeDeleted, or mesh-left, which departs every birth
/// of the removed mesh: mesh-leave.md step 7, "Departed births resolve Gone"), and a contact whose
/// birth the book no longer holds, at a path another logical node now holds. A contact is a
/// bootstrap hint for a birth that may still answer; a departed one never does, and a maker's
/// stored map never hands it to a mesh created again under the same name (R-MN1).
fn dropped_contacts(written: &HashMap<NodeId, crate::storage::NodeRecord>, held: &[MeshDigest], departed: impl Fn(&NodeId) -> bool) -> Vec<NodeId> {
    written
        .values()
        .filter(|w| departed(&w.node_id) || (!held.iter().any(|d| d.node.node_id == w.node_id) && held.iter().any(|d| d.node.name == w.name)))
        .map(|w| w.node_id.clone())
        .collect()
}

/// The durable runtime row of the exact birth `(node_id, incarnation)` of `node`, read by its key
/// (`NodesStorage::runtime`, the point read; never a list of every row searched for it), with
/// `reader` naming the admin that reads it. A row never written (`Ok(None)`) and a store that could
/// not be read (`Err`, naming the store's cause) are two facts, each on its own span.
pub(crate) async fn birth_runtime_row(nodes: &dyn crate::storage::NodesStorage, reader: &PathName, node: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, String> {
    match nodes.runtime(node_id, incarnation).await {
        Ok(Some(row)) => Ok(Some(row)),
        Ok(None) => {
            tracing::info_span!("rdm.node_admin.node.reject.via-no-runtime-row", reader = %reader, node = %node, node_id = %node_id, incarnation_id = %incarnation.as_str(), store = "nodes.storage")
                .in_scope(|| tracing::info!("no durable runtime row of this birth was ever written here"));
            Ok(None)
        }
        Err(e) => {
            tracing::info_span!("rdm.node_admin.node.reject.via-runtime-row-unreadable", reader = %reader, node = %node, node_id = %node_id, incarnation_id = %incarnation.as_str(), store = "nodes.storage", error = %e)
                .in_scope(|| tracing::info!("the durable runtime row of this birth could not be read"));
            Err(format!("the durable runtime row of {node} ({node_id}) at {} could not be read from {reader}'s nodes.storage: {e}", incarnation.as_str()))
        }
    }
}

/// A mesh's contacts and its Mesh row on the fabric's shared store root, written by that mesh's
/// primary alone, for its own members alone (R14 item 2; the writer ladder,
/// fabric-node-lifecycle.md §4.3: a mesh row and its members' rows are that mesh's primary's). A
/// round that does not hold the seat writes nothing. A round that takes it first claims the mesh's
/// name by the mesh store's insert-and-fail and writes the Mesh row (node-identity-and-state.md
/// §2.7): nothing else of the mesh is written until the claim wins, and a name another mesh's live
/// row holds leaves the mesh's rows unwritten for the tenure, named. Then it reads what the shared
/// root holds for the mesh, writes each member whose held birth differs and drops a contact whose
/// node departed or whose path.name another node now holds (`dropped_contacts`). A contact is a
/// restart's hint, never topology.
pub(crate) struct MeshRowsWriter {
    /// The admin's `path.name`, the writer its spans name.
    me: String,
    fabric_id: FabricId,
    mesh: String,
    mesh_id: MeshId,
    nodes: Arc<dyn crate::storage::NodesStorage>,
    meshes: Arc<dyn crate::storage::MeshStorage>,
    /// What this seat tenure knows the shared root holds for the mesh's contacts; `None` outside a tenure.
    written: Option<HashMap<NodeId, crate::storage::NodeRecord>>,
    /// The claim of the mesh's name this seat tenure decided: `None` outside a tenure or before the
    /// store answered it; `Some(Ok)` once it won; `Some(Err)` when it was refused (another mesh's
    /// live row holds the name, or this mesh is Dead): the mesh's rows stay unwritten for the tenure.
    claim: Option<Result<(), crate::storage::MeshClaimRefused>>,
}

impl MeshRowsWriter {
    pub(crate) fn new(me: &str, fabric_id: FabricId, mesh: &str, mesh_id: MeshId, nodes: Arc<dyn crate::storage::NodesStorage>, meshes: Arc<dyn crate::storage::MeshStorage>) -> Self {
        Self { me: me.to_string(), fabric_id, mesh: mesh.to_string(), mesh_id, nodes, meshes, written: None, claim: None }
    }

    /// One round over the births `held` (every birth of the fabric this admin holds, itself
    /// included), `seated` when this admin holds its mesh's primary seat now, `departed` whether the
    /// book holds a node departed.
    pub(crate) async fn round(&mut self, held: &[MeshDigest], seated: bool, departed: impl Fn(&NodeId) -> bool) {
        if !seated {
            self.written = None;
            self.claim = None;
            return;
        }
        if self.claim.is_none() {
            // A store that did not answer decides nothing: the next round asks again.
            self.claim = claim_mesh_name(&*self.meshes, &self.me, &self.mesh, &self.mesh_id).await.ok().map(|c| c.map(|_| ()));
        }
        if !matches!(self.claim, Some(Ok(()))) {
            return;
        }
        if self.written.is_none() {
            match self.nodes.contacts().await {
                Ok(rows) => self.written = Some(rows.into_iter().filter(|c| c.name.mesh == self.mesh).map(|c| (c.node_id.clone(), c)).collect()),
                Err(e) => {
                    tracing::info!(mesh = %self.mesh, error = %e, "the mesh's contacts could not be read; none is written this round");
                    return;
                }
            }
        }
        let written = self.written.as_mut().expect("read above");
        // A departed birth is never written: its contact is dropped below and stays dropped.
        let members: Vec<MeshDigest> = held.iter().filter(|d| d.node.name.mesh == self.mesh && !departed(&d.node.node_id)).cloned().collect();
        for d in &members {
            let r = crate::storage::NodeRecord {
                node_id: d.node.node_id.clone(),
                fabric_id: self.fabric_id.clone(),
                name: d.node.name.clone(),
                mesh_id: self.mesh_id.clone(),
                incarnation_id: d.node.incarnation.clone(),
                endpoint_id: d.node.endpoint_id.clone(),
                transport_addr: d.node.transport_addr,
                listeners: Vec::new(),
            };
            // The contact is what is held, put whole; the declared state and the silence status
            // are their own rows in status.storage, never carried on it.
            if written.get(&r.node_id) != Some(&r) && self.nodes.put_contact(&r).await.is_ok() {
                written.insert(r.node_id.clone(), r);
            }
        }
        let dropped: Vec<crate::storage::NodeRecord> = dropped_contacts(written, &members, &departed).into_iter().filter_map(|id| written.get(&id).cloned()).collect();
        for r in dropped {
            if self.nodes.remove_contact(&r.name.mesh, &r.node_id).await.is_ok() {
                written.remove(&r.node_id);
            }
        }
    }
}

/// The Mesh row of the mesh a restarting node-admin's own row names, read by that mesh's id (R-MN1: a
/// mesh created again under the same name is a new mesh with a new id; R-ML2: a dead mesh is never
/// restarted): the point read at the mesh's key, the name, is taken as the admin's mesh only when it
/// is the row of the mesh id its own row holds, and live. A row of another MeshId at the name means
/// the admin's mesh was removed and a mesh created again under the name; refused by name, naming
/// both ids, as are a Dead row and no row.
pub(crate) async fn own_mesh_row(meshes: &dyn crate::storage::MeshStorage, own: &crate::storage::NodeRecord) -> Result<crate::storage::MeshRecord, String> {
    let (mesh, mesh_id) = (&own.name.mesh, &own.mesh_id);
    match meshes.mesh(mesh).await.map_err(|e| format!("this admin ({}) belongs to mesh {mesh_id} ({mesh}), and mesh.storage could not read the row of {mesh}: {e}", own.name))? {
        None => Err(format!("this admin ({}) belongs to mesh {mesh_id} ({mesh}), and mesh.storage holds no row of {mesh}", own.name)),
        Some(row) if row.mesh_id != *mesh_id => Err(format!(
            "this admin ({}) belongs to mesh {mesh_id} ({mesh}), which was removed: the name {mesh} is held by mesh {}, a mesh created again under it, and a removed mesh is never rejoined",
            own.name, row.mesh_id
        )),
        Some(row) if row.dead => Err(format!("this admin ({}) belongs to mesh {mesh_id} ({mesh}), whose row is Dead: a dead mesh is never restarted", own.name)),
        Some(row) => Ok(row),
    }
}

/// mesh.create's claim of the mesh's name at its key (node-identity-and-state.md §2.7), by the mesh
/// store's insert-and-fail ([`crate::storage::MeshStorage::claim_mesh`]), made by `me`: the
/// fabric-primary at CreateMesh, as the writer of a mesh with no primary (R13), the mesh's first
/// durable write; and the mesh's primary when its seat tenure starts, which finds the mesh's own row
/// (the same MeshId re-claiming, idempotent). The Mesh row written, or this mesh's own row found
/// standing, or refused by name. One `rdm.node_admin.mesh.create.via-mesh-row` span, `moment`
/// `claim`. `Err`: the store did not answer, and nothing is decided.
pub(crate) async fn claim_mesh_name(meshes: &dyn crate::storage::MeshStorage, me: &str, mesh: &str, mesh_id: &MeshId) -> Result<Result<crate::storage::MeshClaim, crate::storage::MeshClaimRefused>, String> {
    let span = tracing::info_span!(
        "rdm.node_admin.mesh.create.via-mesh-row",
        node = %me,
        mesh = %mesh,
        mesh_id = %mesh_id,
        moment = "claim",
        outcome = tracing::field::Empty,
        holder = tracing::field::Empty,
        over = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let record = crate::storage::MeshRecord { mesh_id: mesh_id.clone(), name: mesh.to_string(), dead: false };
    let claimed = meshes.claim_mesh(&record).await.map_err(|e| format!("{me}: mesh.storage did not answer the claim of {mesh} for mesh {mesh_id}: {e}"));
    match &claimed {
        Ok(Ok(crate::storage::MeshClaim::Written { over })) => {
            span.record("outcome", "row-written");
            if let Some(over) = over {
                span.record("over", tracing::field::display(over));
            }
        }
        Ok(Ok(crate::storage::MeshClaim::Held)) => {
            span.record("outcome", "row-held-by-mesh");
        }
        Ok(Err(refused)) => {
            span.record("outcome", refused.outcome()).record("detail", refused.to_string().as_str());
            if let crate::storage::MeshClaimRefused::NameHeld { by, .. } = refused {
                span.record("holder", tracing::field::display(by));
            }
        }
        Err(e) => {
            span.record("outcome", "row-refused").record("detail", e.as_str());
        }
    }
    span.in_scope(|| tracing::info!("mesh.create: the mesh's name is claimed by insert-and-fail at its key; nothing else of the mesh is written until the claim wins"));
    claimed
}

/// Why `create-mesh:<mesh>` did not claim the mesh's name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CreateMeshError {
    /// The mesh store answered and refused the claim by name: nothing of the mesh is written.
    Refused(String),
    /// The mesh store did not answer: nothing is decided, and nothing of the mesh is written.
    Unanswered(String),
}

impl std::fmt::Display for CreateMeshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(why) | Self::Unanswered(why) => f.write_str(why),
        }
    }
}

/// The mesh id the operation carries, held for what every launch of the mesh carries.
fn hold_mesh_id(records: &Records, mesh: &str, mesh_id: &MeshId) {
    records.meshes.lock().unwrap().insert(mesh.to_string(), mesh_id.clone());
    records.wake.poke();
}

/// The claim `create-mesh:<mesh>` makes at the fabric-primary `me` (lifecycles/node-create.md "Mesh absent
/// ... Until the mesh has a primary, the fabric-primary writes its rows"; R13; CLAUDE.md: the claim is
/// the first durable write of a create): `mesh_id`, the id the operation carries (minted once, when the
/// mesh entered the accepted topology: node-identity-and-state.md §2.7, "Who mints it"), is claimed at the
/// mesh's name before anything else of the mesh is written, and held in `records` (what every launch of
/// the mesh carries) only once the claim won. Every run of the operation claims with that id, so a run
/// whose predecessor claimed and was lost finds the mesh's own row: the same MeshId re-claiming is
/// idempotent. A refused claim fails the operation by name, holding nothing of the mesh: the Build runs
/// no create of its nodes, so no node row of the mesh is claimed.
pub(crate) async fn create_mesh(records: &Records, meshes: &dyn crate::storage::MeshStorage, me: &str, mesh: &str, mesh_id: &MeshId) -> Result<MeshId, CreateMeshError> {
    match claim_mesh_name(meshes, me, mesh, mesh_id).await {
        Ok(Ok(_)) => {
            // The won claim's id is the mesh's: it replaces any id held for the name before (a
            // removed mesh's), so no launch of this mesh carries another.
            hold_mesh_id(records, mesh, mesh_id);
            Ok(mesh_id.clone())
        }
        Ok(Err(refused)) => Err(CreateMeshError::Refused(format!("{me}: create-mesh:{mesh}: the claim of the mesh's name for mesh {mesh_id} was refused, and nothing of the mesh is written: {refused}"))),
        Err(e) => Err(CreateMeshError::Unanswered(format!("{e}; nothing of the mesh is written"))),
    }
}

/// A step's receipt, its durable commit: the Build state takes it, or the step is not committed and the
/// refusal is named with the step, its operation and its attempt.
async fn commit_receipt(builds: &dyn BuildStateAdapter, me: &str, receipt: crate::build_state::BuildStepReceipt) -> Result<(), (&'static str, String)> {
    builds
        .append_step_receipt(&receipt)
        .await
        .map_err(|e| ("receipt-refused", format!("{me}: the {} receipt of {} (Build {}, attempt {}) was not written: {e}", receipt.step, receipt.operation, receipt.build_id, receipt.attempt)))
}

/// `create-mesh:<mesh>` as the Build runs it (node-rpc-envelope.md, Build op 0x20: "completed step
/// receipts are read and replayed, never executed again"; Build carrier: "Completed steps are not
/// rerun. Settle an idempotent uncertain step by its natural key"). The operation's one step is the
/// claim of the mesh's name ([`create_mesh`]), and its `Complete` receipt, written by `executor`, is the
/// step's durable commit.
///
/// - A `Complete` receipt of the operation in the Build's current run (from the attempt a reopen began,
///   [`crate::build_state::BuildProjection::opened`], to this attempt) is the step done: it is replayed, which holds the operation's mesh id for this admin's launches and claims
///   nothing and writes nothing.
/// - No receipt is an uncertain step, run by its natural key: the claim finds the mesh free (written),
///   or its own row standing (`row-held-by-mesh`: an earlier run claimed and was lost before it
///   receipted), and the `Complete` receipt follows. A claim refused by name leaves a `Failed` receipt of
///   `ClaimMeshName` naming the holder, and the step fails.
/// - A mesh store that does not answer decides nothing and receipts nothing; a receipt the Build state
///   does not take leaves the step uncommitted. Each fails the step by its own name.
///
/// One `rdm.node_admin.mesh.create.via-receipt` span per run; its `outcome` is `replayed`, `complete`,
/// `refused`, `store-unanswered`, `receipt-refused` or `build-unreadable`, and the claim's own span is its
/// child.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_mesh_step(records: &Records, meshes: &dyn crate::storage::MeshStorage, builds: &dyn BuildStateAdapter, executor: &str, me: &str, build_id: &crate::build::BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String> {
    use crate::build_state::{BuildStepReceipt, StepOutcome};
    use crate::deployment::pipeline::CreateMeshStep;
    let BuildOperation::CreateMesh { mesh, mesh_id } = op else { return Err(format!("{me}: {} is not a create-mesh operation", op.key())) };
    let operation = op.key();
    let span = tracing::info_span!(
        "rdm.node_admin.mesh.create.via-receipt",
        node = %me,
        mesh = %mesh,
        mesh_id = %mesh_id,
        build_id = %build_id,
        attempt,
        operation = %operation,
        outcome = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let receipt = |step: CreateMeshStep, outcome: StepOutcome| BuildStepReceipt { mesh: None, build_id: build_id.clone(), attempt, operation: operation.clone(), step: step.name().into(), outcome, output: None, executor: Some(executor.to_string()) };
    let run = async {
        let held = builds.read_build(build_id).await.map_err(|e| ("build-unreadable", format!("{me}: {operation}: the Build {build_id} could not be read for its receipts: {e}")))?;
        if held.steps.iter().any(|r| r.operation == operation && r.step == CreateMeshStep::Complete.name() && r.outcome == StepOutcome::Complete && r.attempt >= held.opened && r.attempt <= attempt) {
            hold_mesh_id(records, mesh, mesh_id);
            return Ok("replayed");
        }
        match create_mesh(records, meshes, me, mesh, mesh_id).await {
            Ok(_) => {
                commit_receipt(builds, me, receipt(CreateMeshStep::Complete, StepOutcome::Complete)).await?;
                Ok("complete")
            }
            Err(CreateMeshError::Refused(reason)) => {
                commit_receipt(builds, me, receipt(CreateMeshStep::ClaimMeshName, StepOutcome::Failed { reason: reason.clone() })).await?;
                Err(("refused", reason))
            }
            Err(CreateMeshError::Unanswered(reason)) => Err(("store-unanswered", reason)),
        }
    };
    let r = tracing::Instrument::instrument(run, span.clone()).await;
    match &r {
        Ok(outcome) => span.record("outcome", *outcome),
        Err((outcome, detail)) => span.record("outcome", *outcome).record("detail", detail.as_str()),
    };
    span.in_scope(|| tracing::info!("create-mesh: the operation's step is receipted, or replayed from its receipt"));
    r.map(|_| ()).map_err(|(_, detail)| detail)
}

/// fabric.create's claim of the fabric's name (node-identity-and-state.md §2.8), by the fabric store's
/// insert-and-fail ([`crate::fabric_storage::FabricStorage::claim_identity`]): Day 0's first write of
/// the fabric, and the fabric-primary's row writer's before it writes anything else to `fabric/`. One
/// `rdm.node_admin.fabric.create.via-fabric-row` span, `moment` `claim`, names the outcome
/// (`row-written`, `row-held-by-fabric`, `name-held` with the holder, `row-refused`).
pub(crate) async fn claim_fabric_name(storage: &dyn crate::fabric_storage::FabricStorage, identity: &crate::fabric_storage::FabricIdentity, node_id: &NodeId) -> Result<Result<crate::fabric_storage::FabricClaim, crate::fabric_storage::FabricNameHeld>, String> {
    let span = tracing::info_span!(
        "rdm.node_admin.fabric.create.via-fabric-row",
        node_id = %node_id,
        fabric = %identity.name,
        fabric_id = %identity.fabric_id,
        moment = "claim",
        outcome = tracing::field::Empty,
        holder = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let claimed = storage.claim_identity(identity).await.map_err(|e| format!("fabric.storage did not answer the claim of the fabric name {} for Fabric {}: {e}", identity.name, identity.fabric_id));
    match &claimed {
        Ok(Ok(crate::fabric_storage::FabricClaim::Written)) => span.record("outcome", "row-written"),
        Ok(Ok(crate::fabric_storage::FabricClaim::Held)) => span.record("outcome", "row-held-by-fabric"),
        Ok(Err(held)) => span.record("outcome", "name-held").record("holder", tracing::field::display(&held.by)).record("detail", held.to_string().as_str()),
        Err(e) => span.record("outcome", "row-refused").record("detail", e.as_str()),
    };
    span.in_scope(|| tracing::info!("fabric.create: the fabric's name is claimed by insert-and-fail at its key; nothing else of the fabric is written until the claim wins"));
    claimed
}

/// `fabric/` on the fabric's store root, written by the fabric-primary alone (R14 item 2): while this
/// birth holds the fabric seat it writes the Fabric identity it is configured for, so a seat that
/// came to it before the fabric's first state-commit, from a holder whose held rows were dropped,
/// still leaves the Fabric row on the store root. Seats are no `fabric/` rows: the leadership view is
/// folded from the seat-transfer rows (proposal node-primary-update-fabric-mesh-and-node-type.md
/// Part C2: "The per-seat rows under `fabric/seats` go"). The Fabric row carries no pointer to the
/// newest Build (R-BI3). The claim of the
/// fabric's name comes first: nothing else is written to `fabric/` until it wins, and a name another
/// Fabric's identity holds leaves `fabric/` unwritten by this writer, named once. While it does not
/// hold the seat it writes nothing to `fabric/`.
pub(crate) struct FabricRowsWriter {
    /// This admin's exact birth: it writes only while the fabric seat names it.
    me: (NodeId, IncarnationId),
    identity: crate::fabric_storage::FabricIdentity,
    storage: Arc<dyn crate::fabric_storage::FabricStorage>,
    /// The claim of the fabric's name: `None` before the store answered it; `Some(Ok)` once it won;
    /// `Some(Err)` when another Fabric's identity holds the key (a held identity is never rewritten).
    claim: Option<Result<(), crate::fabric_storage::FabricNameHeld>>,
}

impl FabricRowsWriter {
    pub(crate) fn new(me: (NodeId, IncarnationId), identity: crate::fabric_storage::FabricIdentity, storage: Arc<dyn crate::fabric_storage::FabricStorage>) -> Self {
        Self { me, identity, storage, claim: None }
    }

    /// One round on the seat records `seats` holds: while the fabric seat names this birth, the claim
    /// of the fabric's name (which writes the identity row) until the store answers it.
    pub(crate) async fn round(&mut self, seats: &rafka_mesh_transport::membership::SeatBook) {
        if !seats.fabric().is_some_and(|h| h.is_birth(&self.me.0, &self.me.1)) {
            return;
        }
        let (storage, identity) = (&self.storage, &self.identity);
        if self.claim.is_none() {
            // A store that did not answer decides nothing: the next round asks again.
            self.claim = claim_fabric_name(&**storage, identity, &self.me.0).await.ok().map(|c| c.map(|_| ()));
        }
    }
}

/// The provider's proof that the exact birth a digest or a durable row names exited: the runtime
/// fact is the digest's (when it is of this birth) or the row's, adopted in this provider's control
/// domain and inspected, and only the runtime's own terminal status counts. `Ok` carries the exit
/// code and where it came from; `Err` names why nothing is proven.
pub async fn exit_proof_of(
    provider: &dyn crate::deployment::provider::DeploymentProvider,
    held_row: &Option<(MeshDigest, Duration)>,
    row: Option<&crate::storage::RuntimeRow>,
) -> Result<(Option<i32>, &'static str), String> {
    let (fact, data_dir, birth) = match (held_row, row) {
        (Some((dg, _)), _) if dg.node.runtime.is_some() => (dg.node.runtime.clone().expect("checked"), dg.data_dir.clone(), dg.node.incarnation.clone()),
        (_, Some(r)) => (r.runtime.clone(), r.data_dir.clone(), r.incarnation_id.clone()),
        _ => return Err(format!("no runtime fact: the book holds {} for it and no durable row names it", if held_row.is_some() { "a digest without a runtime" } else { "no digest of this birth" })),
    };
    // Proof is the exact runtime's own terminal status, in this provider's control domain; anything
    // else proves nothing.
    let handle = crate::deployment::provider::adopt(provider, &fact).map_err(|e| format!("the provider refused to adopt its runtime: {e}"))?;
    let inspected = provider.inspect(&handle).await;
    let status = crate::deployment::provider::exit_proof(inspected.clone(), &fact, data_dir.as_deref().map(std::path::Path::new), &birth.as_str()).await.map_err(|e| format!("its exit record cannot be read: {e}"))?;
    match status {
        crate::deployment::provider::DeploymentStatus::Exited { code } => {
            let source = match (&inspected, code) {
                (_, None) => "none",
                (crate::deployment::provider::DeploymentStatus::Exited { code: Some(_) }, _) => "provider",
                _ => "exit-record",
            };
            Ok((code, source))
        }
        other => Err(format!("its runtime is not proven exited: {other:?}")),
    }
}

/// The workflow an exited member's removal was announced by ([`RecoverySubmitter::removal_announced`]):
/// which announcement, and the operation, Build and attempt it names (a mesh.delete announced by its
/// mesh's status names its operation and no Build).
struct Announced {
    announced: &'static str,
    operation: String,
    build_id: String,
    attempt: u32,
}

/// What the mesh primary's recovery of one TRUE OFFLINE member reads and calls
/// (node-recovery.md "Detection and proof" step 3, sequence step 1).
#[derive(Clone)]
pub(crate) struct RecoverySubmitter {
    /// This mesh primary's `path.name`: the submitter.
    pub me: PathName,
    pub topology: Arc<RwLock<Topology>>,
    pub book: DigestBook,
    pub provider: Arc<dyn DeploymentProvider>,
    pub nodes_storage: Arc<dyn crate::storage::NodesStorage>,
    pub status_storage: Arc<dyn crate::status_storage::StatusStorage>,
    /// This admin's Build door: its own control plane, where it opens and claims its mesh's drift
    /// attempts, and its own executor, which runs them.
    pub door: crate::build_op::BuildSlot,
    /// This admin's Build state: the fabric's Build facts as its own log holds them. The tick reads
    /// from it which exited births of its mesh are owed a submit ([`RecoverySubmitter::owed_submits`]).
    pub builds: Arc<dyn crate::build_state::BuildStateAdapter>,
}

/// What a loop that proves exits last recorded per birth `(node_id, incarnation)`: the mesh primary's
/// tick ([`RecoverySubmitter::record_verdict`]) and the fabric primary's drift pass
/// ([`reconcile_drift`]) each hold one, and record through [`record_verdict_once`].
pub type ExitVerdicts = Mutex<HashMap<(NodeId, IncarnationId), Inspected>>;

/// The verdict rule of a loop that proves exits (node-recovery.md "Exited is the proof, recorded
/// durably"): `outcome` of `birth` is recorded when it differs from what `verdicts` holds for that
/// birth, by `record` (its span, and for an exit the birth's exit-proof row, a blind put of its own
/// key); a verdict that stands is neither named nor put again by a later pass. A record that fails
/// (the store refused the put) leaves `verdicts` as it was, so the next pass that proves the exit
/// records it. The answer is where an exit's row stands: `put` by this call, `recorded` by an earlier
/// one, or the refusal.
pub(crate) async fn record_verdict_once<F>(verdicts: &ExitVerdicts, birth: (NodeId, IncarnationId), outcome: &Inspected, record: impl FnOnce() -> F) -> Result<&'static str, String>
where
    F: std::future::Future<Output = Result<(), String>>,
{
    if verdicts.lock().unwrap().get(&birth) == Some(outcome) {
        return Ok("recorded");
    }
    record().await?;
    verdicts.lock().unwrap().insert(birth, outcome.clone());
    Ok("put")
}

/// What the provider's inspection of a TRUE OFFLINE member proved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inspected {
    /// Exited: the exit code and where it came from.
    Exited(Option<i32>, &'static str),
    /// Not proven exited (still running, or no runtime fact): why.
    NotExited(String),
}

impl RecoverySubmitter {
    /// The provider's exact-exit proof of `node`'s birth: the runtime fact the book's digest of that
    /// birth carries, else the birth's own durable runtime row (a point read by its key).
    pub(crate) async fn inspect(&self, node: &Node) -> Inspected {
        let Some(incarnation) = node.incarnation_id.clone() else { return Inspected::NotExited(format!("{} holds no incarnation to inspect", node.name)) };
        // The birth's own digest, else the birth as its mesh-primary's projection carried it to this
        // admin (gossip.md §3: the projection preserves the RuntimeFact an authority-bearing admin
        // needs to manage the birth).
        let held_row = self.book.get(node.node_id.as_str()).or_else(|| self.book.projected_at(node.node_id.as_str(), std::time::Instant::now())).filter(|(dg, _)| dg.node.incarnation == incarnation);
        let row = if held_row.as_ref().is_some_and(|(dg, _)| dg.node.runtime.is_some()) {
            None
        } else {
            match self.nodes_storage.runtime(&node.node_id, &incarnation).await {
                Ok(r) => r,
                Err(e) => return Inspected::NotExited(format!("the durable runtime row of {} at {} could not be read: {e}", node.node_id, incarnation.as_str())),
            }
        };
        match exit_proof_of(&*self.provider, &held_row, row.as_ref()).await {
            Ok((code, source)) => Inspected::Exited(code, source),
            Err(why) => Inspected::NotExited(why),
        }
    }

    /// The durable runtime rows the tick watches births from, read once per tick. A refused read
    /// leaves the tick watching the view's births only.
    pub(crate) async fn durable_rows(&self) -> Vec<crate::storage::RuntimeRow> {
        match self.nodes_storage.runtimes().await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::info_span!("rdm.node_admin.node.reject.via-runtime-rows-unreadable", reader = %self.me, store = "nodes.storage", error = %e)
                    .in_scope(|| tracing::info!("the durable runtime rows could not be read: this tick watches the view's births only"));
                Vec::new()
            }
        }
    }

    /// One TRUE OFFLINE birth on the mesh primary's tick (node-recovery.md "Detection and proof"
    /// step 3, sequence step 1): the provider's inspection, its verdict recorded when it changes for
    /// the birth ([`Self::record_verdict`]: named, and an exit put as the birth's exit-proof row once,
    /// on the tick that proved it), and the recovery submitted only on an exit proof of a member that
    /// exited outside any workflow. Any other verdict ends here, and so does the exit of a birth whose
    /// removal this admin's book holds announced, read after the inspection (the tick's view predates
    /// it).
    pub(crate) async fn recover_if_proven(&self, n: &Node, verdicts: &ExitVerdicts) -> Inspected {
        let outcome = self.inspect(n).await;
        let row = self.record_verdict(n, &outcome, verdicts).await;
        if let Inspected::Exited(code, source) = &outcome {
            if let Some(Announced { announced, operation, build_id, attempt }) = self.removal_announced(n).await {
                let exit_proof_row = match &row {
                    Ok(stands) => stands.to_string(),
                    Err(e) => e.clone(),
                };
                let incarnation = n.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default();
                tracing::info_span!(
                    "rdm.node_admin.build.reject.via-exit-inside-workflow",
                    node = %n.name,
                    node_id = %n.node_id,
                    incarnation_id = %incarnation,
                    submitter = %self.me,
                    announced,
                    operation = %operation,
                    build_id = %build_id,
                    attempt,
                    exit_code = code.map(|c| c.to_string()).unwrap_or_default(),
                    exit_proof = *source,
                    exit_proof_row = %exit_proof_row,
                )
                .in_scope(|| tracing::info!("the birth's exit belongs to the workflow that announced its removal: it is no drift, and no recovery is submitted"));
            } else if let Err(e) = self.submit(n, *code, source, row).await {
                tracing::info!(node = %n.name, error = %e, "the recovery submit ended without the recovery; the next tick submits again");
            }
        }
        outcome
    }

    /// The verdict `outcome` of `n`'s birth, recorded by the tick once per change: the inspection's
    /// span, and for an exit the birth's exit-proof row in status.storage (node-recovery.md "Exited
    /// is the proof, recorded durably"; [`crate::offline::put_exit_proof`], a blind put of the birth's
    /// own key). `verdicts` holds what this loop last recorded per birth, so a verdict that stands is
    /// neither named nor put again by a later tick; a put the store refused records nothing, so the
    /// next tick that proves the exit puts the row. For an exit, the answer is where its row stands:
    /// `put` by this tick, `recorded` by an earlier one, or the store's refusal of this tick's put.
    async fn record_verdict(&self, n: &Node, outcome: &Inspected, verdicts: &ExitVerdicts) -> Result<&'static str, String> {
        let incarnation = n.incarnation_id.clone().ok_or_else(|| format!("{} ({}) holds no incarnation: no birth to key its verdict by", n.name, n.node_id))?;
        record_verdict_once(verdicts, (n.node_id.clone(), incarnation), outcome, || async {
            self.span_inspection(n, outcome);
            if let Inspected::Exited(code, source) = outcome {
                crate::offline::put_exit_proof(&*self.status_storage, n, *code, source, local_now_ms()).await?;
            }
            Ok(())
        })
        .await
    }

    /// The workflow whose announced removal `n`'s exact birth is under, as this admin's book holds it
    /// now (node-recovery.md: recovery is drift reconciliation for a member that exits outside any
    /// workflow; "Failure and receipts": an exit during a workflow belongs to that workflow): its
    /// node's departure (`node-deleted`), else an open `NodeDeleting` overlay naming the exact birth
    /// (`node-deleting`), else the mesh.delete of its mesh (mesh-delete.md: from mesh.deleting, the
    /// mesh's status deleting, to mesh.deleted, every member's removal is the mesh.delete's;
    /// `mesh-deleting`, `mesh-deleted`), the status the book holds for the mesh. A recovery's own
    /// node.delete of the exited birth (`retire-node:<path>`) is such a workflow too: its overlay
    /// closes when its attempt's verdict is terminal (`DigestBook::deletions_from_facts`), and a
    /// recovery whose Build ended Failed is submitted again by the next tick: from the view while it
    /// holds the birth, else from the Failed attempt the accepted Build holds once its delete departed
    /// the birth ([`Self::owed_submits`]).
    async fn removal_announced(&self, n: &Node) -> Option<Announced> {
        let incarnation = n.incarnation_id.as_ref()?;
        let of = |announced: &'static str, op: rafka_mesh_entity::LifecycleOp| Announced { announced, operation: op.operation, build_id: op.build_id, attempt: op.attempt };
        if let Some(op) = self.book.departed().into_iter().find(|op| op.node_id == n.node_id) {
            return Some(of("node-deleted", op));
        }
        if let Some(op) = self.book.in_flight().into_iter().find(|op| op.node_id == n.node_id && &op.incarnation == incarnation && !op.is_restart() && !op.is_stop()) {
            return Some(of("node-deleting", op));
        }
        let announced = match self.book.statuses.mesh(&n.mesh)?.status.as_str() {
            "leaving" => "mesh-deleting",
            "dead" => "mesh-deleted",
            _ => return None,
        };
        let operation = self.topology.read().await.meshes.iter().find(|m| m.name == n.mesh).and_then(|m| m.id.clone()).map(|id| crate::mesh_leave::operation(&id)).unwrap_or_else(|| format!("mesh.delete of {}", n.mesh));
        Some(Announced { announced, operation, build_id: String::new(), attempt: 0 })
    }

    /// The inspection's span, named when its verdict for the birth changes (a hung member is
    /// inspected every tick and named once).
    pub(crate) fn span_inspection(&self, node: &Node, outcome: &Inspected) {
        let (verdict, exit_code, exit_proof, reason) = match outcome {
            Inspected::Exited(code, source) => ("exited", code.map(|c| c.to_string()).unwrap_or_default(), *source, String::new()),
            Inspected::NotExited(why) => ("not-exited", String::new(), "none", why.clone()),
        };
        let incarnation = node.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default();
        tracing::info_span!("rdm.node_admin.node.resolve.via-provider-inspection", node = %node.name, node_id = %node.node_id, incarnation_id = %incarnation, inspector = %self.me, verdict, exit_code, exit_proof, reason = %reason)
            .in_scope(|| tracing::info!("the mesh primary asked the provider about the TRUE OFFLINE member's exact runtime"));
    }

    /// The recovery of `node`'s exited birth on its exit-proof row, `row` as the tick recorded it
    /// ([`Self::record_verdict`]): a row the store refused ends the submit here, named on its own
    /// span; on a row that stands this mesh primary opens and runs the recovery itself, keyed on the
    /// exited birth ([`Self::submit_in`], R-BI7). The run is followed to its end, so the caller holds
    /// the birth as submitted until then. A repeat answers AlreadyApplied or BuildInProgress.
    pub(crate) async fn submit(&self, node: &Node, code: Option<i32>, source: &'static str, row: Result<&'static str, String>) -> Result<(), String> {
        let incarnation = node.incarnation_id.clone().ok_or_else(|| format!("{} holds no incarnation: no exited birth to key a recovery on", node.name))?;
        let span = submit_span(&self.me, node, &incarnation, code, source);
        let end = match row {
            Ok(_) => self.submit_in(&span, node, &incarnation).instrument(span.clone()).await,
            Err(e) => Err(e),
        };
        settled(&span, end)
    }

    /// The exited births the tick submits again (node-recovery.md "Failure and receipts": "the
    /// mesh-primary is the submitter that resubmits by `build_id` ... the mesh-primary's next tick
    /// finds the member still proven exited and submits"): none unless this admin holds its mesh's
    /// primary seat and no fabric shutdown froze recovery (as [`recovery_due`]); else every exited
    /// birth of its own mesh whose node workflow in the accepted Build stopped at a Failed attempt and
    /// is not done, read from the durable facts on every tick (the current Build, then the
    /// Build log's index by that Build, `BuildStateAdapter::exits_owed`), whether or not the view still
    /// holds the birth and whichever admin submitted it before. Nothing of it is held between ticks: an
    /// admin that takes the seat, or restarts, reads the same facts. A refused read is named on its
    /// own span and owes nothing this tick.
    pub(crate) async fn owed_submits(&self, holds_seat: bool, frozen: bool, mesh: &str) -> Vec<crate::build_state::OwedExit> {
        if !holds_seat || frozen {
            return Vec::new();
        }
        let unreadable = |store: &str, build_id: &str, error: String| {
            tracing::info_span!("rdm.node_admin.build.reject.via-owed-exits-unreadable", reader = %self.me, mesh, store, build_id, error = %error)
                .in_scope(|| tracing::info!("the durable facts of the exits owed a submit could not be read: none is submitted this tick"));
        };
        let build_id = match self.builds.current_build().await {
            Ok(current) => match current.into_build_id() {
                Some(id) => id,
                // No current Build is held: no attempt can have been opened on it.
                None => return Vec::new(),
            },
            Err(e) => {
                unreadable("builds.storage", "", e.to_string());
                return Vec::new();
            }
        };
        match self.builds.exits_owed(&build_id, mesh).await {
            Ok(owed) => owed,
            Err(e) => {
                unreadable("builds.storage", &build_id.as_str(), e.to_string());
                Vec::new()
            }
        }
    }

    /// The submit of an exited birth whose node workflow stopped at a Failed attempt (node-recovery.md
    /// "Failure and receipts"), on the tick: the birth's exit-proof row is point-read (the provider's
    /// proof the first submit recorded; nothing is inspected or written again), then the same submit,
    /// keyed on the exited birth. A missing or unreadable row is no proof: named on its own span, and
    /// nothing is submitted. The fabric-primary answers the submit as any repeat: AlreadyApplied once a
    /// workflow completed for the birth, BuildInProgress while an attempt runs, else the next attempt
    /// of the Build with this birth's action, whose plan runs only what the receipts leave (a delete
    /// that completed is never run again).
    pub(crate) async fn submit_owed(&self, owed: &crate::build_state::OwedExit) -> Result<(), String> {
        let refused = |reason: &str, error: String| {
            tracing::info_span!(
                "rdm.node_admin.node.reject.via-exit-proof-unread",
                node = %owed.path,
                node_id = %owed.node_id,
                incarnation_id = %owed.incarnation.as_str(),
                submitter = %self.me,
                failed_attempt = owed.attempt,
                reason,
                error = %error,
            )
            .in_scope(|| tracing::info!("an exit owed a submit has no readable exit-proof row: nothing is submitted"));
            format!("{}: no exit-proof row of {} at {} to submit on ({reason}{})", self.me, owed.node_id, owed.incarnation.as_str(), if error.is_empty() { String::new() } else { format!(": {error}") })
        };
        let row = match self.status_storage.node_exit(&owed.node_id, &owed.incarnation).await {
            Ok(Some(row)) => row,
            Ok(None) => return Err(refused("missing", String::new())),
            Err(e) => return Err(refused("unreadable", e.to_string())),
        };
        let mut node = Node::allocated(owed.path.clone());
        node.node_id = owed.node_id.clone();
        node.incarnation_id = Some(owed.incarnation.clone());
        let span = submit_span(&self.me, &node, &owed.incarnation, row.exit_code, &row.proof);
        span.record("after_failed_attempt", owed.attempt);
        let end = self.submit_in(&span, &node, &owed.incarnation).instrument(span.clone()).await;
        settled(&span, end)
    }

    /// The recovery of `node`'s exited birth at `incarnation`, run by this admin as its mesh's primary
    /// (R-BI7; node-recovery.md as R-BI7 moves it: "the mesh-primary opens the recovery attempt of the
    /// current Build itself: claim on its mesh's rows, then run; no call to the fabric-primary"):
    /// opened and claimed at its own door, run through its own executor in its mesh's lane and
    /// followed to the terminal ([`crate::build_run::run_recovery`]).
    async fn submit_in(&self, span: &tracing::Span, node: &Node, incarnation: &IncarnationId) -> Result<RecoveryEnd, String> {
        use crate::build_run::{Recovered, RecoveryNotRun};
        use rafka_node_rpc_contract::build::BuildReply;
        let door = self.door.get().cloned().ok_or_else(|| format!("{} holds its mesh's primary seat but its Build door is not built yet", self.me))?;
        let opened = |o: &crate::control_plane::Opened, disposition: &str| {
            span.record("build_id", o.build_id.as_str());
            span.record("mesh", o.mesh.as_deref().unwrap_or(""));
            span.record("attempt", o.attempt);
            span.record("disposition", disposition);
        };
        match crate::build_run::run_recovery(&door.control, &door.run, "node-recovery", node.name.clone(), node.node_id.clone(), incarnation.clone()).await {
            Ok(Recovered::Ran { opened: o, terminal }) => {
                opened(&o, "created");
                match terminal {
                    BuildReply::Complete { attempt, .. } => Ok(RecoveryEnd::Complete { attempt }),
                    BuildReply::Failed { attempt, operation, step, reason, .. } => Ok(RecoveryEnd::Failed { attempt, operation, step, reason }),
                    other => Err(format!("attempt {} of {} in mesh {}'s lane ended {}: {other:?}", o.attempt, o.build_id, node.mesh, other.name())),
                }
            }
            Ok(Recovered::AlreadyApplied(o)) => {
                opened(&o, "already-applied");
                Ok(RecoveryEnd::Complete { attempt: o.attempt })
            }
            Err(RecoveryNotRun::Refused(r)) => {
                let reply = crate::build_op::refusal_reply(r);
                Err(format!("{}: {reply:?}", reply.name()))
            }
            Err(RecoveryNotRun::RunRefused { opened: o, reply }) => {
                opened(&o, "created");
                Err(format!("attempt {} of {} in mesh {}'s lane was opened and its run refused: {}: {reply:?}", o.attempt, o.build_id, node.mesh, reply.name()))
            }
            Err(RecoveryNotRun::Broke { opened: o, why }) => {
                opened(&o, "created");
                Err(why)
            }
        }
    }
}

/// How a submit ended, on its span: the outcome named, and the submit's result.
fn settled(span: &tracing::Span, end: Result<RecoveryEnd, String>) -> Result<(), String> {
    let (outcome, r) = settle(end);
    span.record("outcome", outcome.as_str());
    span.in_scope(|| tracing::info!("the mesh primary opens and runs the recovery of its proven-exited member itself"));
    r
}

/// The span of one submit of the exited birth `node` at `incarnation` by the mesh primary `me`
/// (node-recovery.md "Observability contract": the submit has a span on the mesh-primary).
/// `after_failed_attempt` is recorded on a submit that follows a Failed one of the same birth.
fn submit_span(me: &PathName, node: &Node, incarnation: &IncarnationId, code: Option<i32>, source: &str) -> tracing::Span {
    tracing::info_span!(
        "rdm.node_admin.build.update.via-node-recovery",
        node = %node.name,
        node_id = %node.node_id,
        incarnation_id = %incarnation.as_str(),
        submitter = %me,
        exit_code = code.map(|c| c.to_string()).unwrap_or_default(),
        exit_proof = source,
        mesh = tracing::field::Empty,
        build_id = tracing::field::Empty,
        attempt = tracing::field::Empty,
        disposition = tracing::field::Empty,
        after_failed_attempt = tracing::field::Empty,
        outcome = tracing::field::Empty,
    )
}

/// The silence-status memo of the mesh primary's tick: what it last put per node.
type WrittenSilence = HashMap<NodeId, (Option<IncarnationId>, Option<NodeStatus>)>;

/// The births the durable runtime rows name at a path `view` holds nothing at: a birth this admin
/// never heard and never launched, at a path the accepted Build names (`planned`), whose departure
/// is not held (a departed birth's row is history). Each stands in the map as `PendingReconnect`,
/// to be inspected like any unheard birth: only an exited runtime counts. One birth per path.
pub fn durable_births(view: &Topology, durable: &[crate::storage::RuntimeRow], planned: &dyn Fn(&PathName) -> bool, departed: &dyn Fn(&str) -> bool) -> Vec<Node> {
    let mut births: Vec<Node> = Vec::new();
    for row in durable {
        if view.node(&row.name).is_none() && !births.iter().any(|n| n.name == row.name) && planned(&row.name) && !departed(row.node_id.as_str()) {
            let mut n = Node::allocated(row.name.clone());
            n.node_id = row.node_id.clone();
            n.incarnation_id = Some(row.incarnation_id.clone());
            n.status = NodeStatus::PendingReconnect;
            births.push(n);
        }
    }
    births
}

/// What the mesh primary's tick watches, by node id: every silent (`PendingReconnect` or `Dead`)
/// member of its own mesh in its view other than itself (the birth `me_id`: never by path.name,
/// which a recovery keeps under a new node id); and every birth of its own mesh only a durable
/// runtime row names (`durable_births`: never heard, never launched by this admin), which is node
/// recovery driven by this mesh-primary like any other member's (mesh-recovery.md:8-9). The tickle
/// ladder is the mesh-primary's for its own members (node-recovery.md); a peer mesh's node-admins
/// are reached by the fabric-primary's investigation alone (`crate::investigate`).
pub fn watched_births(view: &Topology, me_id: &NodeId, mesh: &str, durable: &[Node]) -> BTreeMap<String, Node> {
    view.members()
        .filter(|n| n.node_id != *me_id && n.mesh == mesh && matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead))
        .chain(durable.iter().filter(|n| n.mesh == mesh && n.node_id != *me_id))
        .map(|n| (n.node_id.to_string(), n.clone()))
        .collect()
}

/// The watched births the tick inspects for recovery (node-recovery.md): none unless this admin
/// holds its mesh's primary seat; else every watched birth of its own mesh (its secondary
/// node-admins included, mesh-recovery.md:8-9) held TRUE OFFLINE at its exact incarnation.
/// None while this admin holds a fabric shutdown (`frozen`): the shutdown froze drift recovery, and
/// the mesh-primary that drains its mesh recreates nothing (fabric-mesh-lifecycle.md §11.1).
pub fn recovery_due(holds_seat: bool, frozen: bool, mesh: &str, watched: &BTreeMap<String, Node>, offline: &HashSet<(NodeId, IncarnationId)>) -> Vec<Node> {
    if !holds_seat || frozen {
        return Vec::new();
    }
    watched.values().filter(|n| n.mesh == mesh && n.incarnation_id.as_ref().is_some_and(|i| offline.contains(&(n.node_id.clone(), i.clone())))).cloned().collect()
}

/// The mesh primary's per-birth bookkeeping speaks only for the births it still watches: a birth
/// whose node left the view (deleted, or recovered under a new node.id at its path.name) or whose
/// node the view now holds at another incarnation (superseded) leaves the TRUE OFFLINE set; a
/// verdict lives only while its birth is TRUE OFFLINE; the silence memo only while its node is
/// watched. Run every tick, after the tick's writes, so nothing of a gone birth outlives one tick.
fn retire_unwatched(watched: &BTreeMap<String, Node>, offline: &mut HashSet<(NodeId, IncarnationId)>, verdicts: &mut HashMap<(NodeId, IncarnationId), Inspected>, written: &mut WrittenSilence) {
    offline.retain(|(node_id, incarnation)| watched.get(node_id.as_str()).is_some_and(|n| n.incarnation_id.as_ref() == Some(incarnation)));
    verdicts.retain(|birth, _| offline.contains(birth));
    written.retain(|node_id, _| watched.contains_key(node_id.as_str()));
}

/// How a followed recovery Build ended: its terminal frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecoveryEnd {
    /// The Build converged at this attempt.
    Complete { attempt: u32 },
    /// A step of the attempt failed (node-recovery.md "Failure and receipts": `Failed { step, reason }`).
    Failed { attempt: u32, operation: String, step: String, reason: String },
}

/// The submit span's `outcome` and the submit's result, from how the followed recovery ended.
/// A Failed recovery ends the submit in an error: the Build holds the Failed attempt, and the next
/// tick reads it and submits the birth again (node-recovery.md "Failure and receipts",
/// `RecoverySubmitter::owed_submits`).
fn settle(end: Result<RecoveryEnd, String>) -> (String, Result<(), String>) {
    match end {
        Ok(RecoveryEnd::Complete { attempt }) => (format!("complete: attempt {attempt}"), Ok(())),
        Ok(RecoveryEnd::Failed { attempt, operation, step, reason }) => {
            let failed = format!("failed: attempt {attempt} {operation} step {step}: {reason}");
            (failed.clone(), Err(failed))
        }
        Err(e) => (e.clone(), Err(e)),
    }
}

/// The gates of the fabric-primary's drive: the membership's cut-off rule and the Build log's
/// authority, read from the committed fabric seat record.
struct SeatGate {
    membership: Membership,
    builds: Arc<FabricBuildStateAdapter>,
    shutdown: Arc<crate::shutdown::ShutdownControl>,
    /// The control plane whose peer-mesh reader the investigation installs once its ladder runs
    /// (after the drive is built): read when asked, as the Build door reads it.
    control: Arc<ControlPlane>,
    /// The 0x21 door node.primary.update is commanded and run through, which holds what each update
    /// was answered and said (`HandoverDoor::memo`).
    handover: Arc<crate::fabric_handover::HandoverDoor>,
}

#[async_trait::async_trait]
impl crate::build_drive::Gate for SeatGate {
    fn authorizes(&self) -> bool {
        self.membership.authorizes()
    }
    async fn fence(&self) -> Result<Option<String>, crate::build_state::BuildStateError> {
        self.builds.fence_check().await
    }
    async fn fence_set(&self) {
        self.builds.fence_set().await
    }
    fn frozen(&self) -> Option<String> {
        self.shutdown.held().map(|_| "a fabric shutdown froze reconciliation: no claim and no dispatch from here on".to_string())
    }
    fn peer_mesh(&self, mesh: &str) -> Option<crate::investigate::PeerMesh> {
        self.control.peer_mesh.get().and_then(|f| f(mesh))
    }
    /// node.primary.update before an operation on a seat holder (the approved proposal
    /// node-primary-update-fabric-mesh-and-node-type.md, Part C1 step 1: "The holder is commanded from
    /// above"): a mesh-primary's seat is commanded from here (the fabric-primary commands a
    /// mesh-primary); the fabric-primary's own seats it moves itself, having nothing above it, both in
    /// ONE row when it holds both (Q1: "the one row of C1 commits both seats to NA2 at their next
    /// epochs").
    async fn move_seats(&self, m: &crate::executor::SeatMove, build_id: &crate::build::BuildId, attempt: u32, operation: &str) -> Result<(), crate::build_drive::SeatMoveEnd> {
        use crate::build_drive::SeatMoveEnd;
        use crate::election::Moving;
        use crate::fabric_handover::update_operation;
        use rafka_mesh_entity::Seat;
        use rafka_node_rpc_contract::handover::NodePrimaryUpdateReply as Reply;
        let door = &self.handover;
        // An update left incomplete after its row, or every refusal before it (C1 failure contract).
        let ended = |r: Reply| match r {
            Reply::Incomplete { .. } => SeatMoveEnd::Incomplete(format!("{}: {r:?}", r.name())),
            other => SeatMoveEnd::Refused(format!("{}: {other:?}", other.name())),
        };
        if m.holder.node_id != door.node_id {
            if m.mesh_seat {
                let update = update_operation(Seat::MeshPrimary, build_id.as_str(), operation);
                door.command_update_primary(&m.holder, build_id.as_str(), attempt, &update).await?;
            }
            return Ok(());
        }
        // The fabric seat alone moves out of this admin's mesh (C2 search step 2: a mesh retire of its
        // mesh, mesh-delete.md step 1), by node.fabric-primary.update; with the mesh seat, both move in
        // node.primary.update's one row (Q1).
        let (moving, seat) = match (m.mesh_seat, m.fabric_seat) {
            (true, true) => (Moving::BothSeats, Seat::MeshPrimary),
            (true, false) => (Moving::MeshSeat, Seat::MeshPrimary),
            (false, true) => (Moving::FabricSeatOutside, Seat::FabricPrimary),
            (false, false) => return Ok(()),
        };
        let update = update_operation(seat, build_id.as_str(), operation);
        door.update_primary(moving, None, build_id.as_str(), attempt, &update).await.map_err(ended)?;
        Ok(())
    }

    async fn seats_acknowledged(&self, updates: &[crate::build_drive::PrecedingUpdate], build_id: &crate::build::BuildId, attempt: u32) -> Result<(), String> {
        for u in updates {
            self.handover.row_acknowledged(&u.mesh, &u.operation, u.holder.as_ref(), build_id.as_str(), attempt).await?;
        }
        Ok(())
    }
}

/// Proof that an executor's birth departed: the provider inspects the exact runtime the digest book
/// or the durable runtime rows name for it, and only an exited runtime counts.
struct ProviderDeparture {
    /// The admin proving the departure: the reader its refusals name.
    me: PathName,
    topology: Arc<RwLock<Topology>>,
    /// Projects this admin's view now and installs it, so a birth just proven exited holds no seat
    /// in the view the next attempt is planned from.
    refresh: Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>,
    book: DigestBook,
    provider: Arc<dyn DeploymentProvider>,
    nodes: Arc<dyn crate::storage::NodesStorage>,
    records: Arc<Records>,
}

#[async_trait::async_trait]
impl crate::build_drive::DepartureProof for ProviderDeparture {
    async fn proven(&self, executor: &PathName) -> Result<(), String> {
        let Some(n) = self.topology.read().await.node(executor).cloned() else {
            return Err(format!("{executor} is not a node of this admin's view, so no exact birth is named to inspect"));
        };
        let Some(incarnation) = n.incarnation_id.clone() else {
            return Err(format!("{executor} holds no incarnation id to inspect"));
        };
        let held_row = self.book.get(n.node_id.as_str()).filter(|(dg, _)| dg.node.incarnation == incarnation);
        // The birth's digest names its runtime; else its own durable runtime row does (a point read).
        let row = if held_row.as_ref().is_some_and(|(dg, _)| dg.node.runtime.is_some()) {
            None
        } else {
            Some(birth_runtime_row(&*self.nodes, &self.me, &n.name, &n.node_id, &incarnation).await?.ok_or_else(|| {
                format!("the book holds no runtime for {executor} ({}) at {} and no durable runtime row of that birth was ever written at {}", n.node_id, incarnation.as_str(), self.me)
            })?)
        };
        let (code, source) = crate::admin::exit_proof_of(&*self.provider, &held_row, row.as_ref()).await?;
        let recorded = self.records.put_exit(&n, code, source).await;
        tracing::info_span!("rdm.node_admin.build.update.via-executor-exit-proof", node = %n.name, node_id = %n.node_id, incarnation_id = %incarnation.as_str(), exit_code = code.map(|c| c.to_string()).unwrap_or_default(), exit_proof = source, exit_proof_row = %recorded.as_ref().map(|_| "put".to_string()).unwrap_or_else(|e| e.clone()))
            .in_scope(|| tracing::info!("the executor's exact runtime is inspected and exited"));
        recorded.map_err(|e| format!("{executor}'s exit is proven but not recorded, so no projection holds it gone: {e}"))?;
        (self.refresh)().await;
        Ok(())
    }

    /// Whether the exit of the exact birth the view names for `executor` is recorded: a point read
    /// of its exit-proof row (node-recovery.md: "Exited is the proof, recorded durably"). A row that
    /// cannot be read is named and answers not recorded: the exit is proven again, and its row put
    /// again, a blind put of the same key.
    async fn recorded(&self, executor: &PathName) -> bool {
        let Some(n) = self.topology.read().await.node(executor).cloned() else {
            return false;
        };
        match self.records.exit_row_of(&n).await {
            Ok(row) => row.is_some(),
            Err(e) => {
                tracing::info_span!("rdm.node_admin.node.reject.via-exit-row-unreadable", node = %self.me, subject = %n.name, node_id = %n.node_id, rows = 1, detail = %e)
                    .in_scope(|| tracing::info!("the executor's exit-proof row could not be read: its exit is not taken as recorded"));
                false
            }
        }
    }
}

/// What a drift opening decides on, as one comparable string: for a repair, the exited birth and
/// the completed receipts under its `replace-node:<path>`; for surplus, each live birth by
/// incarnation.
fn drift_inputs(t: &Topology, steps: &[crate::build_state::BuildStepReceipt], repair: Option<(&PathName, &IncarnationId)>, surplus: &[String]) -> String {
    let birth = |n: &Node| format!("{}:{}", n.name, n.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or("-"));
    if let Some((path, incarnation)) = repair {
        let op = format!("replace-node:{path}");
        let member = format!("{op}/");
        let complete = steps.iter().filter(|s| s.outcome == crate::build_state::StepOutcome::Complete && (s.operation == op || s.operation.starts_with(&member))).count();
        return format!("repair {path}:{} receipts={complete}", incarnation.as_str());
    }
    surplus.iter().map(|item| t.members().find(|n| n.name.to_string() == *item).map(birth).unwrap_or_else(|| item.clone())).collect::<Vec<_>>().join("; ")
}

/// The fabric authority's drift check (`crate::drift`): when this admin is the fabric primary,
/// the accepted Build is settled, and a cohort the Build's topology names has fewer births than
/// it should, at least one of its node-admins with its exact runtime inspected and found exited, it
/// opens the next attempt of that same Build (the current Build unchanged: the topology did not
/// change). Only a mesh that lost its mesh-primary is repaired here (mesh recovery), and surplus is
/// retired. A member of a mesh with a live mesh-primary, a node-admin included, is not repaired here:
/// its mesh-primary proves its exit and submits its recovery (node-recovery.md, mesh-recovery.md:8-9).
/// `started` holds, per opening, the inputs it was opened on (`drift_inputs`), so an attempt that
/// fails is opened again only once one of them moves, never once per pass; the adapter's
/// insert-and-fail on the attempt keeps two authorities from opening it twice.
/// `verdicts` holds the exits this loop recorded ([`record_verdict_once`]), so a proven exit's row is
/// put once, by the pass that proved it.
pub async fn reconcile_drift(
    time: &rafka_mesh_transport::clock::RafkaTime,
    me: &PathName,
    me_id: &NodeId,
    t: &Topology,
    book: &DigestBook,
    provider: &dyn crate::deployment::provider::DeploymentProvider,
    builds: &dyn BuildStateAdapter,
    contexts: &crate::build_claim::AttemptContexts,
    admission: &tokio::sync::Mutex<()>,
    durable: &[crate::storage::RuntimeRow],
    started: &mut HashSet<(crate::build::BuildId, u32, Vec<String>)>,
    held: &mut HashSet<(NodeId, IncarnationId)>,
    verdicts: &ExitVerdicts,
    hold: &(dyn Fn(&str) -> Option<u64> + Sync),
    defers: &(dyn Fn(&str) -> bool + Sync),
    decided: &(dyn Fn(&str) -> bool + Sync),
    exits: &Records,
) -> Option<(crate::build::BuildId, u32)> {
    let authority = t.fabric_primary().filter(|n| n.node_id == *me_id)?;
    let current = match builds.current().await {
        Ok(current) => current?,
        Err(e) => {
            tracing::info_span!("rdm.node_admin.build.reject.via-build-store-unreadable", route = "proven-drift", node = %me, error = %e)
                .in_scope(|| tracing::info!("the current Build could not be read: no drift attempt is opened on a Build this pass cannot read"));
            return None;
        }
    };
    if matches!(current.state, crate::build_state::BuildState::Pending | crate::build_state::BuildState::Running) {
        return None;
    }
    // Every unheard birth whose exact runtime the provider proves exited, with the exit code the
    // proof carries (the provider's own, or the process runtime's record in its data dir).
    let mut exited = HashSet::new();
    let mut proven: std::collections::BTreeMap<PathName, crate::drift::ExitedBirth> = Default::default();
    // The map this authority proves from: its view, plus every birth the durable runtime rows name
    // at a path its view holds nothing at (a sibling this admin never heard). Such a birth is not
    // reached, not dead: it is inspected like any unheard birth, and only an exited runtime counts.
    let mut map = t.clone();
    map.nodes.extend(durable_births(t, durable, &|p| current.topology.contains(p), &|id| book.is_departed(id)));
    let t = &map;
    // The births this authority may have to prove: the ones it no longer hears, and the node-admins
    // of a peer mesh whose rebirth the ladder decided (`crate::investigate`). A mesh this authority
    // never heard on the backbone is held `ready` in its view from topology alone (topology is not
    // liveness), so the ladder's decision, not a status, names its node-admins.
    let mut candidates = crate::drift::unheard(t);
    for n in t.members() {
        if n.kind == NodeKind::NodeAdmin && n.mesh != me.mesh && decided(&n.mesh) && !candidates.iter().any(|c| c.name == n.name) {
            candidates.push(n);
        }
    }
    let mut held_now: HashSet<(NodeId, IncarnationId)> = HashSet::new();
    let mut proven_now: HashSet<(NodeId, IncarnationId)> = HashSet::new();
    // Where each proven exit's row stands after this pass (`<path.name>=put|recorded|<refusal>`), named
    // on the pass's attempt span or its unchanged refusal.
    let mut exit_proof_rows: Vec<String> = Vec::new();
    for n in candidates {
        // A birth the Build stopped on request is not drift: its `stop-node:<path>` operation completed
        // for this exact birth (node-stop.md: no bind, no JoinNode, no replacement), and the exit it
        // proves is the one that was asked for.
        // The stop's `Complete` is that birth's when its attempt carried the stop fenced to it.
        let operation = format!("stop-node:{}", n.name);
        let stop = n.incarnation_id.clone().map(|from_incarnation| crate::accepted::AttemptAction::Stop { path: n.name.clone(), from_incarnation });
        if stop.as_ref().is_some_and(|stop| current.steps.iter().any(|s| s.operation == operation && s.step == "Complete" && s.outcome == crate::build_state::StepOutcome::Complete && current.carried(s.attempt, stop))) {
            let key = (current.build_id.clone(), 0u32, vec![n.name.to_string(), n.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default(), "stopped-on-request".to_string()]);
            if started.insert(key) {
                tracing::info_span!("rdm.node_admin.build.reject.via-stopped-on-request", node = %me, birth = %n.name, node_id = %n.node_id, operation = %operation)
                    .in_scope(|| tracing::info!("the birth's exit was asked for by a completed stop-node: no repair attempt"));
            }
            continue;
        }
        // A peer mesh with no live node-admin is reborn on the investigation's decision
        // (`crate::investigate`), not on the first exit proof: every one of its births waits for it
        // (R-D1: provider evidence is not authority, and a node this authority replaces while the
        // mesh's first admin is still to be reborn is a node that admin then replaces again from
        // its own map). A node of a mesh that still has a live node-admin is proven here as ever.
        // An ordinary birth of such a mesh is held as evidence once the ladder tracks the mesh as
        // unheard (`hold`); until then it waits with the admins.
        if n.mesh != me.mesh && defers(&n.mesh) && t.cohort(&n.mesh, NodeKind::NodeAdmin).all(|a| !a.status.is_live()) && (n.kind == NodeKind::NodeAdmin || hold(&n.mesh).is_none()) {
            // The hold is named once per mesh and Build: an unnamed deferral is a rebirth nobody is
            // waiting on.
            let unheard = book.mesh_unheard(&n.mesh, std::time::Instant::now());
            let key = (current.build_id.clone(), 0u32, vec!["deferred".to_string(), n.mesh.clone()]);
            if started.insert(key) {
                tracing::info_span!("rdm.node_admin.build.reject.via-drift-deferred", node = %me, mesh = %n.mesh, birth = %n.name, unheard_ms = unheard.map(|d| d.as_millis().to_string()).unwrap_or_else(|| "none".into()))
                    .in_scope(|| tracing::info!("the peer mesh's node-admins wait for the investigation's rebirth decision"));
            }
            continue;
        }
        // A peer mesh held unheard is held as a mesh (R-D1): none of its nodes is restarted, replaced or
        // deleted from here, whatever the provider proves of its runtime. The one exception is the
        // ladder's own decision, for the mesh's first recovering node-admin; once that admin stands, the
        // mesh reconciles its ordinary nodes itself.
        // Which node-admin of the mesh the decision already released is a fact of the accepted Build: the
        // one an attempt held by an admin outside the mesh carried (`crate::build_drive::crossed_admin`).
        let first_recovering_admin = n.kind == NodeKind::NodeAdmin && decided(&n.mesh) && crate::build_drive::crossed_admin(&current, &n.mesh).is_none_or(|crossed| crossed == n.name);
        let unheard_ms = if n.mesh != me.mesh && !first_recovering_admin { hold(&n.mesh) } else { None };
        // An ordinary member is recovered by its own mesh-primary (node-recovery.md: "The
        // mesh-primary is the reconciler for its own members"), never restarted or replaced from
        // here; only a held mesh's ordinary birth is still inspected, as evidence (R-D1).
        if n.kind != NodeKind::NodeAdmin && unheard_ms.is_none() {
            continue;
        }
        // So is a node-admin of a mesh with a live mesh-primary (mesh-recovery.md:8-9, "A lost member
        // of a mesh with a live mesh-primary is node recovery, driven by that mesh-primary"); a mesh
        // primary's own loss by the admin its mesh seats. What stays here is the mesh that lost its
        // mesh-primary (mesh recovery), the ladder's decided mesh included.
        if n.kind == NodeKind::NodeAdmin && unheard_ms.is_none() && !decided(&n.mesh) && crate::executor::node_admin_recoverer(&n.name, t).is_some() {
            continue;
        }
        let held_row = book.get(n.node_id.as_str()).filter(|(dg, _)| Some(&dg.node.incarnation) == n.incarnation_id.as_ref());
        let row = durable.iter().find(|r| r.node_id == n.node_id && Some(&r.incarnation_id) == n.incarnation_id.as_ref());
        // A silent birth this authority cannot prove is named once, with what it lacks: it is not
        // drift until its exact runtime is inspected, and an authority that holds no fact for it
        // can never inspect it.
        let mut unprovable = |why: String| {
            let key = (current.build_id.clone(), 0u32, vec![n.name.to_string(), n.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default(), why.clone()]);
            if started.insert(key) {
                tracing::info_span!("rdm.node_admin.build.reject.via-drift-unprovable", node = %me, birth = %n.name, node_id = %n.node_id, status = ?n.status, reason = %why)
                    .in_scope(|| tracing::info!("a silent birth the accepted topology names cannot be proven exited by this authority"));
            }
        };
        let (code, source) = match exit_proof_of(provider, &held_row, row).await {
            Ok(p) => p,
            Err(why) => {
                unprovable(why);
                continue;
            }
        };
        {
            let incarnation = n.incarnation_id.clone().expect("filtered on it");
            if let Some(unheard_ms) = unheard_ms {
                // The evidence is kept and named once per birth for as long as the hold lasts; the exit
                // authorizes nothing and is not recorded as the view's departure.
                held_now.insert((n.node_id.clone(), incarnation.clone()));
                if held.insert((n.node_id.clone(), incarnation.clone())) {
                    tracing::info_span!(
                        "rdm.node_admin.build.reject.via-unheard-mesh",
                        node = %n.name,
                        node_id = %n.node_id,
                        incarnation_id = %incarnation.as_str(),
                        mesh = %n.mesh,
                        unheard_ms,
                        authority = %authority.name,
                        exit_code = code.map(|c| c.to_string()).unwrap_or_default(),
                        exit_proof = source,
                    )
                    .in_scope(|| tracing::info!("the provider proves the birth exited, but its mesh is unheard: no attempt, no NodeDeleted, no birth from here"));
                }
                continue;
            }
            // The proof is recorded durably, once (`record_verdict_once`): the birth's exit-proof row is
            // what every projection reads as its proven loss, put by the pass that proved the exit and
            // not again by a later pass that finds it standing. A refused put leaves the birth
            // unproven to every reader, and the next pass that proves it puts the row.
            let birth = (n.node_id.clone(), incarnation.clone());
            proven_now.insert(birth.clone());
            match record_verdict_once(verdicts, birth, &Inspected::Exited(code, source), || async { exits.put_exit(n, code, source).await.map(|_| ()) }).await {
                Ok(stands) => exit_proof_rows.push(format!("{}={stands}", n.name)),
                Err(e) => {
                    exit_proof_rows.push(format!("{}={e}", n.name));
                    tracing::info_span!("rdm.node_admin.node.reject.via-exit-row-unwritten", node = %n.name, node_id = %n.node_id, incarnation_id = %incarnation.as_str(), authority = %authority.name, error = %e)
                        .in_scope(|| tracing::info!("the provider proves the birth exited, but its exit-proof row is not written: no projection holds it exited; the repair is decided from the proof in hand"));
                }
            }
            exited.insert(incarnation.clone());
            proven.insert(n.name.clone(), crate::drift::ExitedBirth { node_id: n.node_id.clone(), incarnation, code, source });
        }
    }
    // A hold ends with the silence: a mesh heard again holds no birth, and the next silence is a new hold.
    held.retain(|k| held_now.contains(k));
    // A recorded exit is held while this pass still proves it: a birth no longer proven here is
    // recorded again by the next pass that proves it.
    verdicts.lock().unwrap().retain(|k, _| proven_now.contains(k));
    let short = crate::drift::shortfall(&current.topology, t, &exited);
    // One exited birth per attempt, in path.name order; the next pass takes the next remaining one.
    let first_exited: Option<(&PathName, &crate::drift::ExitedBirth)> =
        proven.iter().find(|(p, _)| short.iter().any(|s| s.exited.iter().any(|e| *e == p.to_string())));
    // Surplus: a live birth of a kept Mesh the accepted topology does not name (a removal the
    // executing view had not heard yet when the Build closed). Its retirement is its own attempt,
    // taken only once no exited birth is left to repair.
    let mut surplus: Vec<String> = t
        .members()
        .filter(|n| n.status.is_live() && current.topology.meshes.contains_key(&n.mesh) && !current.topology.contains(&n.name))
        .filter(|n| n.mesh == me.mesh || hold(&n.mesh).is_none())
        // A birth whose retirement the Build already receipted (`NodeDeleting` of this exact birth) is
        // not surplus: that operation retires it, and this view is only late in hearing it go.
        .filter(|n| {
            let (node_id, incarnation) = (serde_json::to_value(&n.node_id).unwrap_or_default(), serde_json::to_value(&n.incarnation_id).unwrap_or_default());
            !current.steps.iter().any(|st| st.step == "NodeDeleting" && st.output.as_ref().is_some_and(|o| o.get("node_id") == Some(&node_id) && o.get("incarnation") == Some(&incarnation)))
        })
        .map(|n| n.name.to_string())
        .collect();
    surplus.sort();
    if first_exited.is_none() && surplus.is_empty() {
        return None;
    }
    let attempt = current.attempt + 1;
    let (key, scope, action) = match first_exited {
        Some((path, birth)) => {
            // Only a node-admin of a mesh that lost its mesh-primary reaches here (every other member is
            // its mesh-primary's to recover), and a proven-exited birth has no process to restart
            // (node-restart.md): it is replaced.
            let action = crate::accepted::AttemptAction::Replace { path: path.clone(), from_incarnation: birth.incarnation.clone() };
            let scope = short.iter().filter(|s| s.exited.iter().any(|e| *e == path.to_string())).map(ToString::to_string).collect::<Vec<_>>().join("; ");
            (vec![path.to_string()], scope, Some(action))
        }
        None => (surplus.clone(), format!("surplus: {}", surplus.join(" ")), None),
    };
    // An opening is keyed on what the attempt decides on, never on its number: a failed attempt
    // whose inputs are all as they were when it opened gets no next attempt from here, and one
    // input moving opens exactly one (node-status-change.md: "a failed Build ... gets its next
    // attempt", once per failure; R-S2: event-driven, never per pass). A key carrying the attempt
    // number never matches the next pass (every failed attempt raises it), which reopened a resume
    // that failed fast on every pass.
    let inputs = drift_inputs(t, &current.steps, first_exited.map(|(p, b)| (p, &b.incarnation)), &surplus);
    let ident = key.join(" ");
    let opened_at = (current.build_id.clone(), 0u32, vec!["opened".to_string(), ident.clone(), inputs.clone()]);
    if started.contains(&opened_at) {
        let unchanged = (current.build_id.clone(), 0u32, vec!["unchanged".to_string(), ident.clone(), inputs.clone(), current.attempt.to_string()]);
        if started.insert(unchanged) {
            tracing::info_span!("rdm.node_admin.build.reject.via-drift-unchanged", node = %me, build_id = %current.build_id, failed_attempt = current.attempt, scope = %scope, inputs = %inputs, exit_proof_rows = %exit_proof_rows.join(", "))
                .in_scope(|| tracing::info!("the last attempt opened for these inputs ended and none of them moved since: no next attempt until one does"));
        }
        return None;
    }
    let opening = (current.build_id.clone(), attempt, key);
    // The same admission a submitted change takes, held to the attempt's durable write: the attempt
    // opens on the Build that is still the accepted one, still settled, at the same attempt, or
    // nothing opens, nothing of it is written and nothing is spent.
    let _admitted = admission.lock().await;
    match builds.current().await {
        Ok(Some(now)) if now.build_id == current.build_id && now.attempt == current.attempt && !matches!(now.state, crate::build_state::BuildState::Pending | crate::build_state::BuildState::Running) => {}
        Ok(other) => {
            tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route = "proven-drift", build_id = %current.build_id, attempt, scope = %scope, accepted_now = %other.map(|b| format!("{} {:?}", b.build_id, b.state)).unwrap_or_default())
                .in_scope(|| tracing::info!("another Build or attempt was admitted first: no drift attempt"));
            return None;
        }
        Err(e) => {
            tracing::info_span!("rdm.node_admin.build.reject.via-build-store-unreadable", route = "proven-drift", node = %me, build_id = %current.build_id, attempt, scope = %scope, error = %e)
                .in_scope(|| tracing::info!("the current Build could not be read again under the admission: no drift attempt"));
            return None;
        }
    }
    started.retain(|k| !(k.0 == current.build_id && k.1 == 0 && k.2.len() == 3 && k.2[0] == "opened" && k.2[1] == ident));
    started.insert(opened_at);
    started.insert(opening);
    let span = tracing::info_span!(
        "rdm.node_admin.build.update.via-proven-drift",
        build_id = %current.build_id,
        attempt,
        scope = %scope,
        authority = %authority.name,
        authority_node_id = %authority.node_id,
        reason = "proven-drift",
        action = action.as_ref().map(|a| match a { crate::accepted::AttemptAction::Restart { .. } => "restart", crate::accepted::AttemptAction::Replace { .. } => "replace", crate::accepted::AttemptAction::DeleteConnections { .. } => "drain", crate::accepted::AttemptAction::Stop { .. } => "stop", crate::accepted::AttemptAction::Start { .. } => "start", crate::accepted::AttemptAction::Recover { .. } => "recover", crate::accepted::AttemptAction::Delete { .. } => "delete" }).unwrap_or("none"),
        exit_code = first_exited.and_then(|(_, b)| b.code).map(|c| c.to_string()).unwrap_or_default(),
        exit_proof = first_exited.map(|(_, b)| b.source).unwrap_or("none"),
        exit_proof_rows = %exit_proof_rows.join(", "),
    );
    let opened = crate::build_state::AttemptOpened {
        mesh: None,
        build_id: current.build_id.clone(),
        attempt,
        reason: crate::build_state::AttemptReason::ProvenDrift,
        action,
        opened_by: me.to_string(),
        opened_at_ms: time.now_ms(),
    };
    // The attempt's context is this span, on this fabric-primary, before the attempt is open to be claimed.
    if let Err(e) = contexts.put(&current.build_id, attempt, &span.in_scope(crate::build_claim::current_context)).await {
        span.in_scope(|| tracing::info!(error = %e, "the attempt's context could not be recorded; the attempt is not opened"));
        return None;
    }
    match builds.open_attempt(&opened).await {
        Ok(()) => {
            span.in_scope(|| tracing::info!("a birth the accepted topology names is proven gone: the next attempt of the same Build is open"));
            Some((current.build_id.clone(), attempt))
        }
        Err(e @ crate::build_state::BuildStateError::AttemptTaken { .. }) => {
            // Another action holds the number: nothing was opened, and there is no attempt to execute.
            tracing::info_span!("rdm.node_admin.build.reject.via-attempt-taken", route = "proven-drift", build_id = %current.build_id, attempt, scope = %scope, detail = %e)
                .in_scope(|| tracing::info!(detail = %e, "attempt refused: another action holds the number"));
            None
        }
        Err(e) => {
            span.in_scope(|| tracing::info!(error = %e, "the attempt could not be opened"));
            None
        }
    }
}

/// What an entry answer hands this admin of Fabric control: the Fabric identity and the answering
/// admin's current Build with its attempt, the floor this admin's copy of that Build must reach. The
/// first entry and every repeat of it take them through here.
async fn take_entry_control(answer: &crate::wire::JoinAnswer, accepted: &AcceptedStore, entry_floor: &Mutex<Option<(crate::build::BuildId, u32)>>) {
    if let Some(r) = answer.control.fabric.clone() {
        accepted.learn(r, &answer.served_by).await;
    }
    if let Some(floor) = answer.control.build.clone() {
        *entry_floor.lock().unwrap() = Some((floor.build_id, floor.attempt));
    }
}

/// Never Ready around missing control hydration: an admin that holds no current Build (R-BI3: no
/// Build in its builds store is in progress or completed) names itself blocked. Only Day 0 accepts
/// the first Build itself; every other admin hydrates the Build facts from an existing authority
/// (its entry pull, or the fabric control topic).
///
/// Ready also needs the facts of the Build the entry named, the answering admin's current Build: it
/// must be readable from this admin's own log, and its folded attempt must reach the attempt that
/// admin held of it (`floor`). An admin that was not served an entry (Day 0) owes no floor. The
/// reason is typed ([`crate::hydrate::HydrationBlocker`]): the Ready check recovers each one with
/// the request it names.
pub async fn hydration_blocker(me: &PathName, builds: &dyn BuildStateAdapter, floor: Option<(crate::build::BuildId, u32)>) -> Option<crate::hydrate::HydrationBlocker> {
    use crate::hydrate::HydrationBlocker as B;
    if let Some((id, attempt)) = floor {
        match builds.read_build(&id).await {
            Ok(held) if held.attempt < attempt => return Some(B::BehindFloor { me: me.clone(), build_id: id, held: held.attempt, floor: attempt }),
            Ok(_) => {}
            Err(crate::build_state::BuildStateError::UnknownBuild(_)) => return Some(B::NoCurrentBuild { me: me.clone(), wanted: Some(id) }),
            Err(e) => return Some(B::UnreadableBuild { me: me.clone(), build_id: id, error: e.to_string() }),
        }
    }
    match builds.current_build().await {
        Ok(crate::build_state::CurrentBuild::None) => Some(B::NoCurrentBuild { me: me.clone(), wanted: None }),
        Ok(_) => None,
        Err(e) => Some(B::BuildStoreUnreadable { me: me.clone(), error: e.to_string() }),
    }
}

/// A person-started admin that is not a restart (no row of its own in nodes.storage): what it
/// is must be stated by its parameters, and contradictory inputs are refused by name. Day 0 is
/// the start with no mesh, no seed and no flag on a data dir that holds nothing; a recovery start
/// names its Fabric, its mesh, a seed and both primary flags. `holds` names what the data dir
/// already holds, if anything.
pub(crate) fn refuse_contradictory_start(cfg: &AdminConfig, holds: Option<&str>) -> Result<(), String> {
    if cfg.launch.is_some() {
        return Ok(());
    }
    let dir = cfg.data_dir.display();
    let recovering = cfg.mesh_primary || cfg.fabric_primary;
    if let Some(what) = holds {
        return Err(format!(
            "{dir} holds {what} but no row for this admin in nodes.storage: it is neither a restart nor a fresh start, and this start will not take Day 0 or mint a Fabric, Build or MeshId over it"
        ));
    }
    if recovering {
        if !cfg.fabric_primary {
            return Err(format!(
                "RDM_MESH_PRIMARY without RDM_FABRIC_PRIMARY on {dir}: a mesh recovery is started by the fabric primary (it launches the admin and hands it its mesh's Pending); a person starts a fabric recovery with both flags"
            ));
        }
        let mut missing = Vec::new();
        if cfg.mesh_id.is_none() {
            missing.push(rafka_mesh_entity::launch::ENV_MESH_ID);
        }
        if !cfg.fabric_id_named {
            missing.push(rafka_mesh_entity::launch::ENV_FABRIC_ID);
        }
        if cfg.seeds.is_empty() {
            missing.push(rafka_mesh_entity::launch::ENV_SEEDS);
        }
        if !missing.is_empty() {
            return Err(format!("a fabric recovery on {dir} names the Fabric, the mesh and a live node to dial; missing {}", missing.join(", ")));
        }
    } else if cfg.mesh_id.is_some() || !cfg.seeds.is_empty() {
        return Err(format!(
            "{dir} is started with {} but no recovery flag: Day 0 names no mesh and no seed, and a recovery sets RDM_MESH_PRIMARY and RDM_FABRIC_PRIMARY",
            [(cfg.mesh_id.is_some(), rafka_mesh_entity::launch::ENV_MESH_ID), (!cfg.seeds.is_empty(), rafka_mesh_entity::launch::ENV_SEEDS)]
                .iter()
                .filter(|(set, _)| *set)
                .map(|(_, k)| *k)
                .collect::<Vec<_>>()
                .join(" and ")
        ));
    }
    Ok(())
}

/// How a node-admin's own mesh's Pending holds its Ready (e4.s11; i143 export gate: normal
/// joining admins never self-Ready; only the Day-0 root, which has no upstream authority, applies
/// its own mesh's Pending).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PendingGate {
    /// Pending is applied here, or an admin joining a mesh whose admin cohort is live owes nothing.
    Clear,
    /// The Day-0 root applies its own mesh's Pending to itself.
    SelfApply,
    /// A mesh's first admin that is not the Day-0 root waits for the fabric primary to apply it.
    Blocked(String),
}

pub(crate) fn pending_gate(me: &PathName, mesh: &str, day0: bool, pending_applied: bool, cohort_live: bool) -> PendingGate {
    if pending_applied {
        PendingGate::Clear
    } else if day0 {
        PendingGate::SelfApply
    } else if cohort_live {
        PendingGate::Clear
    } else {
        PendingGate::Blocked(format!("{me}: the first admin of {mesh}: its mesh's Pending has not been applied here by the fabric primary"))
    }
}

/// Why this admin cannot yet take authority over `held` (every live birth it
/// hears but itself): each birth that publishes no runtime fact, or one this
/// admin's provider cannot adopt in its own control domain. Empty: it is
/// authority-capable, and may commit Ready.
pub fn authority_blockers(held: &[MeshDigest], provider: &dyn crate::deployment::provider::DeploymentProvider) -> Vec<String> {
    held.iter()
        .filter_map(|d| match &d.node.runtime {
            None => Some(format!("{}: publishes no runtime fact", d.node.name)),
            Some(f) => crate::deployment::provider::adopt(provider, f).err().map(|r| format!("{}: {r}", d.node.name)),
        })
        .collect()
}

/// Readiness, drain and admission seen through each member's own digest:
/// a node publishes `ReadyForTraffic` once it serves, and `Draining` right
/// after it starts refusing new work with a typed `Draining` (node-rpc §35).
pub struct MembershipObserver {
    /// The digest book the observer reads.
    pub book: DigestBook,
    /// The process's one Node RPC client (ruling X), over which the typed drain reaches the
    /// exact birth; `None` only in a cell that observes without a transport.
    pub client: Option<Arc<rafka_node_rpc::NodeRpcClient>>,
    /// The commands this admin sent and awaits a completion call for.
    pub commands: Arc<crate::node_commands::CommandBook>,
    /// This admin's status authority, once it holds a view: a start begins a new cycle of the birth's
    /// declared states.
    pub authority: crate::status_rpc::AuthoritySlot,
    /// This admin's 0x21 door, once it holds a view: node.primary.update goes through it.
    pub handover: crate::fabric_handover::HandoverSlot,
}

impl MembershipObserver {
    fn digest_of(&self, node: &Node) -> Option<MeshDigest> {
        self.book.get(node.node_id.as_str()).map(|(d, _)| d).filter(|d| Some(&d.node.incarnation) == node.incarnation_id.as_ref())
    }
}

/// The blocker an exact birth names for its Ready (node-start.md step 3): only while `book` holds the
/// birth Pending (the status its mesh-primary set, never its heartbeat's), one `ProbeNodeState` to
/// that birth over `client`, and the hook's blocker of
/// a Status `Blocked` answer, named with the birth and the event it runs again on and no attempt
/// counter. Every other answer names no blocker and frames nothing: a `NotReady` is the subject's
/// door or handler not serving yet (node-rpc-envelope.md, "NotReady ... is never a hook blocker"),
/// and the step waits on; so does a birth that answers its state (`Current`, `CurrentParked`), a
/// stale incarnation, or a call that ends unanswered: why it is not Ready is the Ready check's to
/// report.
pub async fn probed_ready_blocker(client: &rafka_node_rpc::NodeRpcClient, book: &DigestBook, node: &Node) -> Option<String> {
    let incarnation = node.incarnation_id.clone()?;
    let (digest, _) = book.get(node.node_id.as_str()).filter(|(d, _)| d.node.incarnation == incarnation)?;
    if digest.status != MemberStatus::Pending {
        return None;
    }
    let birth = rafka_node_admin_client::ExactBirth { target: rafka_node_rpc::NodeTarget::ExactNode(node.node_id.clone()), node_id: node.node_id.clone(), incarnation: incarnation.clone() };
    match rafka_node_admin_client::NodeRpc::new(client).get(&birth, &rafka_node_rpc::CallOptions::default()).await {
        Ok(rafka_node_rpc_contract::status::StatusReply::Blocked { node_id, incarnation: answered, reason, retry_on }) if node_id == node.node_id && answered == incarnation => {
            Some(format!("{}: hydrate_before_ready is blocked: {reason}; it runs again on {}", node.name, retry_on.name()))
        }
        _ => None,
    }
}

#[async_trait::async_trait]
impl NodeObserver for MembershipObserver {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<Publication> {
        self.book
            .get(node_id.as_str())
            .filter(|(d, _)| &d.node.incarnation == incarnation)
            .map(|(d, _)| Publication { runtime: d.node.runtime, data_dir: d.data_dir })
    }

    /// Ready once this admin's book holds the birth ready-for-traffic: the status its mesh-primary
    /// set from the birth's node.started (this admin, as that primary, through its status authority;
    /// otherwise the primary's projection), never a heartbeat's. A refusal names the status the
    /// book's digest carries, the member status the book holds for the birth, and whether this
    /// admin took the birth's node.started (the state it holds the birth declared).
    async fn ready(&self, node: &Node) -> Result<(), String> {
        let Some(d) = self.digest_of(node) else {
            return Err(format!("no digest from {} for this birth", node.name));
        };
        if d.status == MemberStatus::ReadyForTraffic {
            return Ok(());
        }
        let member = match self.book.member_status(&node.node_id, &d.node.incarnation) {
            Some(s) => format!("the member status held for the birth is {s:?}"),
            None => "no member status is held for the birth (a birth of another mesh, or no node.joined taken here)".to_string(),
        };
        let started = match self.authority.get().map(|a| a.declared.lock().unwrap().node(&node.node_id, &d.node.incarnation)) {
            None => "node.started is not known here: this admin holds no declared states yet".to_string(),
            Some(None) => "this admin holds no declared state of the birth: it took no node.started".to_string(),
            Some(Some(rafka_node_rpc_contract::status::NodeState::Pending)) => "this admin holds the birth declared Pending: it took no node.started".to_string(),
            Some(Some(rafka_node_rpc_contract::status::NodeState::ReadyForTraffic)) => "this admin holds the birth declared ReadyForTraffic: it took node.started".to_string(),
            Some(Some(state)) => format!("this admin holds the birth declared {state:?}"),
        };
        Err(format!("{} is held {:?}, not ready-for-traffic: {member}; {started}", node.name, d.status))
    }

    async fn ready_blocker(&self, node: &Node) -> Option<String> {
        probed_ready_blocker(self.client.as_deref()?, &self.book, node).await
    }

    async fn send_command(&self, node: &Node, cmd: crate::node_commands::NodeCommand, ctx: &crate::deployment::pipeline::CommandContext) -> crate::deployment::pipeline::CommandAdmission {
        // A start begins the birth's next cycle: the states it declares from here run forward from
        // `Pending` again, not from the `Leaving` its stop declared.
        if cmd == crate::node_commands::NodeCommand::Start {
            if let (Some(authority), Some(birth)) = (self.authority.get(), node.incarnation_id.as_ref()) {
                authority.begin_cycle(node, birth).await;
            }
        }
        // node.connections.delete { All } goes through this admin's own status authority, the birth's
        // mesh-primary: it commands the birth and holds its node.connections.deleted for a rename.
        if cmd == crate::node_commands::NodeCommand::DeleteConnections {
            let (Some(authority), Some(incarnation)) = (self.authority.get(), node.incarnation_id.clone()) else {
                return crate::deployment::pipeline::CommandAdmission::NotSent { reason: format!("this admin holds no status authority yet to carry node.connections.delete to {}", node.name) };
            };
            let req = rafka_node_rpc_contract::status::StatusRequest::DeleteConnections { node_id: node.node_id.clone(), incarnation, scope: rafka_node_rpc_contract::status::ConnectionScope::All };
            let reply = authority.apply(None, &req).await;
            return match reply {
                rafka_node_rpc_contract::status::StatusReply::ConnectionsDeleted { receipt: Some(receipt), .. } => crate::deployment::pipeline::CommandAdmission::ConnectionsDeleted { receipt },
                other => crate::deployment::pipeline::CommandAdmission::Refused { reply: format!("{}: {other:?}", other.name()) },
            };
        }
        let Some(client) = self.client.as_ref() else {
            return crate::deployment::pipeline::CommandAdmission::NotSent { reason: "this admin holds no Node RPC client".into() };
        };
        crate::node_commands::send_command(client, &self.commands, &rafka_node_rpc::NodeTarget::ExactNode(node.node_id.clone()), node, cmd, ctx).await
    }

    async fn await_completion(&self, node: &Node, cmd: crate::node_commands::NodeCommand, ctx: &crate::deployment::pipeline::CommandContext, within: Duration) -> crate::deployment::pipeline::Completion {
        crate::node_commands::await_completion(&self.commands, node, cmd, ctx, within).await
    }

}

/// How a node-admin leaves its mesh when parked and joins it again when started
/// (`node_self::Session`): its tasks are held at their gate, its gossip topics are left and its
/// connections are cut; a start joins the commanding admin again, takes the topology and runs its hook.
struct AdminSession {
    gate: crate::park_gate::ParkGate,
    node_id: NodeId,
    incarnation: IncarnationId,
    fabric_id: FabricId,
    mesh_id: MeshId,
    membership: Membership,
    backbone: Backbone,
    client: Arc<rafka_node_rpc::NodeRpcClient>,
    server: rafka_node_rpc::NodeRpcServer,
    digest: Arc<Mutex<MeshDigest>>,
    rafka_time: rafka_mesh_transport::clock::RafkaTime,
    accepted: Arc<AcceptedStore>,
    builds: Arc<dyn BuildStateAdapter>,
    entry_floor: Arc<Mutex<Option<(crate::build::BuildId, u32)>>>,
    shutdown: Arc<crate::shutdown::ShutdownControl>,
    hydration: crate::app_hydration::Hydration,
    addressed: Arc<crate::app_hydration::Addressed>,
    hydration_cell: Arc<Mutex<Option<crate::app_hydration::HydrationHandle>>>,
    hydrating: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    wake: Arc<Records>,
    /// This admin's own Build log: a start's Build-floor step absorbs the facts it reads into it.
    local: Arc<dyn crate::build_state::LocalBuildLog>,
    /// The node-admins a start's Build-facts read asks, in order.
    responders: crate::hydrate::Responders,
    /// What this admin owes its authorities: a start begins its next own-state cycle there.
    declarer: Arc<crate::status_declare::Declarer>,
}

#[async_trait::async_trait]
impl crate::node_self::Session for AdminSession {
    async fn park_tasks(&self) -> crate::node_self::TasksParked {
        // The gate parks, so no turn begins; the turns already past their wait point end on their
        // own completion, and only then does the stop go on to its cut.
        let at = self.gate.quiesce().await;
        crate::node_self::TasksParked { turns_in_progress: at.in_progress(), waited_for: at.named() }
    }

    async fn cut(&self, spare: &[iroh::PublicKey]) -> crate::node_self::CutReport {
        // The stop held this admin's tasks before it (`park_tasks`): nothing it was doing goes out on
        // the connections cut below.
        let turns_in_progress = self.gate.state().borrow().in_progress();
        if let Some(h) = self.hydrating.lock().unwrap().take() {
            h.abort();
        }
        self.hydration.gate().close();
        let neighbours = self.membership.neighbours();
        self.membership.park().await;
        self.backbone.park().await;
        let pooled = self.client.close_pooled("node.connections.delete");
        let accepted = self.server.close_connections_except(spare, "node.connections.delete");
        crate::node_self::CutReport { neighbours, pooled, accepted, turns_in_progress }
    }

    async fn rejoin(&self, retire: rafka_node_rpc::CancelToken, commander: tokio::sync::watch::Receiver<iroh::PublicKey>, rows: Option<crate::connections_writer::RowsHeld>) -> Result<(), crate::node_self::StartFailed> {
        // The name this admin holds (its row's, as its last join or apply handed it): what its join
        // presents and what it is named by until the join answer hands it the row's (R12).
        let held = self.digest.lock().unwrap().node.name.clone();
        let root = tracing::info_span!(parent: None, "rdm.mesh.node.update.via-start", node = %held, node_id = %self.node_id, incarnation_id = %self.incarnation.as_str());
        let mut start = AdminAfterStop { session: self, root, commander, retire: Some(retire), rows };
        crate::workflows::node_start::node_start(&mut start, crate::app_hydration::StartKind::AfterStop).await
    }
}

/// A node-admin's start after its stop, as it plugs into `node.start`: it rejoins the admin that
/// commanded the start, takes the control state the join answers and runs its tasks again from
/// where they were held.
struct AdminAfterStop<'a> {
    session: &'a AdminSession,
    root: tracing::Span,
    /// The endpoint key of the call that commanded the start last: the join goes back over that
    /// call, and the admin that answers is the hook's accepting authority.
    commander: tokio::sync::watch::Receiver<iroh::PublicKey>,
    /// The new cycle's token: the hook runs under it.
    retire: Option<rafka_node_rpc::CancelToken>,
    /// The start's hold on this birth's connections rows, dropped once the handed name is held.
    rows: Option<crate::connections_writer::RowsHeld>,
}

#[async_trait::async_trait]
impl crate::workflows::node_start::StartNode for AdminAfterStop<'_> {
    type Error = crate::node_self::StartFailed;

    fn starting(&self) -> crate::workflows::node_start::Starting {
        let s = self.session;
        let held = s.digest.lock().unwrap().node.name.clone();
        crate::workflows::node_start::Starting { root: self.root.clone(), name: held.to_string(), path: held, node_id: s.node_id.clone(), client: s.client.clone(), rafka_time: s.rafka_time.clone() }
    }

    fn joining(&self) -> crate::workflows::node_start::Joining {
        crate::workflows::node_start::Joining::Authority { label: self.commander.borrow().fmt_short().to_string() }
    }

    async fn resolve_authority(&mut self) -> Result<crate::workflows::node_start::JoinTo, crate::node_self::StartFailed> {
        Ok(crate::workflows::node_start::JoinTo::Commander(self.commander.clone()))
    }

    fn join_digest(&self) -> MeshDigest {
        self.session.digest.lock().unwrap().clone()
    }

    async fn enter_channel(&mut self, step: tracing::Span, joined: Option<&crate::wire::JoinAnswer>) -> Result<Membership, crate::node_self::StartFailed> {
        let s = self.session;
        step.record("mesh_id", tracing::field::display(&s.mesh_id));
        if let Some(joined) = joined {
            // The row's name and config the join handed are held on this admin's digest and by its
            // membership before it re-enters its topic or says anything (R-JN1).
            {
                let mut d = s.digest.lock().unwrap();
                if let Some(n) = &joined.control.name {
                    s.membership.set_name(n.clone());
                    d.node.name = n.clone();
                }
                if let Some(c) = &joined.control.config {
                    s.membership.set_config(c.clone());
                    d.config = c.clone();
                }
            }
            // Only now are its connections recorded: the join's names the handed name.
            drop(self.rows.take());
        }
        s.membership.unpark().await;
        s.backbone.unpark().await;
        drop(step);
        Ok(s.membership.clone())
    }

    async fn joined(&mut self, membership: &Membership, through: Option<&crate::workflows::node_start::JoinThrough>, joined: Option<&crate::wire::JoinAnswer>) -> Result<crate::workflows::node_start::HookParts, crate::node_self::StartFailed> {
        let s = self.session;
        // From here on it is named by the name it installed from its join answer (`enter_channel`).
        let me = s.digest.lock().unwrap().node.name.clone();
        // The Fabric record and the Build floor the join answered, recorded as at a first start, and a
        // fabric shutdown in force.
        if let Some(joined) = joined {
            take_entry_control(joined, &s.accepted, &s.entry_floor).await;
            if let Some(sd) = joined.control.shutdown.clone() {
                let _ = s.shutdown.learn(sd, "start", &joined.served_by).await;
            }
        }
        // node.joined begins this birth's next cycle: the node.started it reports is owed afresh to its
        // mesh's primary, before its declarer runs again.
        s.declarer.begin_own_cycle(&me, &s.incarnation.as_str());
        // Back on the topic means a neighbour: only then does this admin say it is back, once, as
        // Pending, so every node that held it parked or restarting hears the rejoin.
        membership.wait_for_neighbour().await;
        let pending = s.digest.lock().unwrap().clone();
        let _ = membership.publish(&pending).await;
        // Its tasks run again, from where they were held.
        s.gate.resume();
        // The control entry this start repeats when nothing names the Build it is missing: the join it
        // made, to the admin that commanded the start, with the digest it presented.
        let entry = through.map(|t| {
            let mut digest = pending.clone();
            digest.status = MemberStatus::Pending;
            entry_repeat(s.client.clone(), t.target.clone(), t.anchor.clone(), digest, s.accepted.clone(), s.entry_floor.clone(), s.shutdown.clone(), s.rafka_time.clone())
        });
        let after = AdminAfterStopTail {
            name: me.clone(),
            node_id: s.node_id.clone(),
            fabric_id: s.fabric_id.clone(),
            digest: s.digest.clone(),
            wake: s.wake.clone(),
            root: self.root.clone(),
            floor: AdminFloor {
                me: me.clone(),
                builds: s.builds.clone(),
                floor: s.entry_floor.clone(),
                hydrator: crate::hydrate::Hydrator::new(me.clone(), s.client.clone(), s.local.clone(), s.responders.clone(), entry),
                progress: None,
            },
        };
        Ok(crate::workflows::node_start::HookParts {
            birth: crate::app_hydration::Birth { name: me.clone(), node_id: s.node_id.clone(), incarnation: s.incarnation.clone(), fabric_id: s.fabric_id.clone(), mesh_id: s.mesh_id.clone(), kind: NodeKind::NodeAdmin },
            hydration: s.hydration.clone(),
            events: crate::app_hydration::Events::new(membership.book.clone(), s.addressed.clone()),
            cancel: self.retire.take().expect("the hook's token is handed once"),
            state: s.hydration_cell.clone(),
            runs: crate::workflows::node_start::HookRuns::Inline { blocked: s.hydrating.clone() },
            after: Box::new(after),
        })
    }
}

/// A node-admin's steps after its hook at a start after its stop.
struct AdminAfterStopTail {
    /// The name the admin holds: its join answer's (R-JN1).
    name: PathName,
    /// Its node id: the topology's members it reaches are every other node of its mesh.
    node_id: NodeId,
    fabric_id: FabricId,
    digest: Arc<Mutex<MeshDigest>>,
    wake: Arc<Records>,
    root: tracing::Span,
    floor: AdminFloor,
}

#[async_trait::async_trait]
impl crate::workflows::node_start::AfterHook for AdminAfterStopTail {
    async fn topology_installed(&mut self, membership: &Membership, read: &crate::topology_read::TopologyRead) {
        let mesh_peers: Vec<EndpointAddr> = read
            .installed
            .iter()
            .flat_map(|m| m.members.iter())
            .filter(|d| d.fabric_id == self.fabric_id && d.node.node_id != self.node_id && d.node.name.mesh == self.name.mesh)
            .filter_map(rafka_mesh_transport::membership::gossip_addr)
            .collect();
        let _ = membership.join_peers(mesh_peers).await;
    }

    async fn floor(&mut self, step: &tracing::Span) {
        self.floor.until_held(step).await;
    }

    async fn report(self: Box<Self>) {
        let mut d = self.digest.lock().unwrap();
        if d.status == MemberStatus::Pending {
            d.status = MemberStatus::ReadyForTraffic;
            drop(d);
            self.wake.wake.poke();
            tracing::info_span!(parent: &self.root, "rdm.mesh.node.update.via-ready", node = %self.name, source = "self-published-membership").in_scope(|| tracing::info!("ready for traffic"));
        }
    }
}

/// How often a start below its Build floor checks the floor again and hands its blocker to the
/// hydrator: the cadence the hydrator's one-request-at-a-time rule and backoff are set against
/// (`crate::hydrate`).
const FLOOR_CHECK: Duration = Duration::from_millis(100);

/// A node-admin's Build floor at a start (node-start.md step 4): "Fetch Build facts only when the node
/// is below the authority's floor, after the hook and before node.started". The floor itself is the
/// attempt of the pointed Build the admin that served the join held, recorded where the join's answer
/// is taken ([`take_entry_control`]); this step reads the facts with `FetchBuildFacts` while the
/// admin's own holdings are below it, and completes when the check holds them, never on a received
/// frame alone.
struct AdminFloor {
    me: PathName,
    builds: Arc<dyn BuildStateAdapter>,
    floor: Arc<Mutex<Option<(crate::build::BuildId, u32)>>>,
    hydrator: crate::hydrate::Hydrator,
    /// Where the Ready check of a first start reads what the start waits on.
    progress: Option<StartProgress>,
}

impl AdminFloor {
    /// Resolves once this admin holds the Build facts its floor requires. Runs inside the floor step's
    /// span, so each read it starts is a child of it.
    async fn until_held(&mut self, step: &tracing::Span) {
        let owed = self.floor.lock().unwrap().clone();
        step.record("owed", owed.as_ref().map_or_else(|| "no floor named by the join".to_string(), |(b, a)| format!("{b} attempt {a}")).as_str());
        let mut seen: Vec<&'static str> = Vec::new();
        loop {
            let floor = self.floor.lock().unwrap().clone();
            let blocker = hydration_blocker(&self.me, &*self.builds, floor).await;
            self.hydrator.tick(blocker.as_ref()).await;
            let Some(blocker) = blocker else { break };
            if !seen.contains(&blocker.kind()) {
                seen.push(blocker.kind());
            }
            if let Some(p) = &self.progress {
                p.at(format!("node.start is at its Build floor: {blocker}"));
            }
            tokio::time::sleep(FLOOR_CHECK).await;
        }
        // Nothing is left outstanding at node.started: a read still in flight is let end.
        self.hydrator.settle().await;
        step.record("blockers", seen.join(",").as_str());
        step.record("outcome", if seen.is_empty() { "held" } else { "fetched" });
    }
}

/// What a node-admin's first start has yet to do once its hook runs, as its Ready check names it.
#[derive(Clone)]
struct StartProgress(Arc<Mutex<Option<String>>>);

impl StartProgress {
    fn begun(me: &PathName) -> Self {
        Self(Arc::new(Mutex::new(Some(format!("{me}: node.start has not run its topology, Build-floor and rafka-time steps")))))
    }

    fn at(&self, what: String) {
        *self.0.lock().unwrap() = Some(what);
    }

    fn started(&self) {
        *self.0.lock().unwrap() = None;
    }

    fn blocker(&self) -> Option<String> {
        self.0.lock().unwrap().clone()
    }
}

/// A node-admin's steps after its hook at its first start (node.create).
struct AdminFirstStart {
    me: PathName,
    me_id: NodeId,
    fabric_id: FabricId,
    mesh: String,
    backbone: Backbone,
    records: Arc<Records>,
    provider: ProviderKind,
    gate: crate::park_gate::ParkGate,
    entry: crate::reenter::EntryCtx,
    entering: Arc<std::sync::atomic::AtomicBool>,
    sweep: Arc<std::sync::OnceLock<crate::reenter::SweepReport>>,
    start_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    floor: AdminFloor,
    progress: StartProgress,
    read_seam: Option<Arc<dyn crate::wiring::ReadSeam>>,
}

#[async_trait::async_trait]
impl crate::workflows::node_start::AfterHook for AdminFirstStart {
    async fn before_topology_read(&mut self, authority: &PathName) {
        if let Some(seam) = &self.read_seam {
            seam.before_topology_read(&self.me.to_string(), &authority.to_string()).await;
        }
    }

    async fn topology_installed(&mut self, membership: &Membership, read: &crate::topology_read::TopologyRead) {
        self.progress.at(format!("{}: node.start is at its topology step", self.me));
        let mut mesh_peers = Vec::new();
        let mut admins = Vec::new();
        for d in read.installed.iter().flat_map(|m| m.members.iter()).filter(|d| d.fabric_id == self.fabric_id && d.node.name != self.me) {
            if let Some(a) = rafka_mesh_transport::membership::gossip_addr(d) {
                if d.node.name.mesh == self.mesh {
                    mesh_peers.push(a.clone());
                }
                if d.node.name.kind == NodeKind::NodeAdmin {
                    admins.push(a);
                }
            }
        }
        let _ = membership.join_peers(mesh_peers).await;
        self.backbone.join_admins(admins).await;
        // The maker's topology: when the own mesh already has nodes, this admin is entering an existing
        // mesh (recovery); a mesh's first birth finds none. A maker that holds no snapshot of this mesh
        // answers from its stored map: births to reach a local node with, never installed.
        let mut map = crate::reenter::map_of_read(read, &self.fabric_id);
        map.extend(crate::reenter::map_of_stored(read));
        // A birth starts from its maker's topology: every settled node the maker's projection holds is
        // in this admin's view as the maker knew it, until membership or the sweep says more. A fabric
        // primary this admin becomes then never plans from an empty view.
        for n in map.iter().filter(|n| n.node_id != self.me_id && n.transition.is_none()) {
            self.records.publish(n.as_node(if n.ready { NodeStatus::ReadyForTraffic } else { NodeStatus::PendingReconnect }, self.provider));
        }
        // The maker's stored ids are its meshes' rows on the shared root, written by each mesh's primary:
        // held in memory here, never written again by this admin. A mesh keeps its id through a
        // recovery, and an admin that deploys that mesh's births launches them with it.
        for m in &read.stored {
            if let Some(id) = &m.mesh_id {
                self.records.meshes.lock().unwrap().entry(m.mesh.clone()).or_insert_with(|| id.clone());
            }
        }
        if map.iter().any(|n| n.name.mesh == self.mesh && n.node_id != self.me_id) {
            self.entering.store(true, std::sync::atomic::Ordering::SeqCst);
            let sweep = spawn_entry_sweep(&self.gate, self.entry.clone(), "maker", map, self.sweep.clone(), self.records.clone(), self.provider);
            self.start_tasks.lock().unwrap().push(sweep);
        }
    }

    async fn floor(&mut self, step: &tracing::Span) {
        self.floor.until_held(step).await;
        self.progress.at(format!("{}: node.start is at its rafka-time step", self.me));
    }

    async fn report(self: Box<Self>) {
        // node.started: the Ready check, which waits on every other blocker too, makes the report.
        self.progress.started();
    }

    async fn start_failed(self: Box<Self>, failed: &crate::node_self::StartFailed, _: bool) {
        // The Ready check names the failed step as this admin's blocker: it is not started.
        self.progress.at(format!("{}: node.start failed at {}: {}", self.me, failed.step, failed.reason));
    }
}

/// Enter an existing mesh: connect to a local node, read its topology and sweep the own mesh once with
/// a Ping (mesh-recovery.md step 6). A reply proves a live exact birth; every node that did not answer
/// is unheard, and node recovery's to prove. `slot` holds the report the Ready check waits on.
fn spawn_entry_sweep(
    gate: &crate::park_gate::ParkGate,
    ctx: crate::reenter::EntryCtx,
    source: &'static str,
    map: Vec<crate::reenter::MapNode>,
    slot: Arc<std::sync::OnceLock<crate::reenter::SweepReport>>,
    records: Arc<Records>,
    provider_kind: ProviderKind,
) -> tokio::task::JoinHandle<()> {
    gate.spawn(async move {
        let report = crate::reenter::enter_existing_mesh(&ctx, source, map).await;
        // The entry read has told this admin what the mesh knows of its seats (or no one could).
        records.set_entering(false);
        // A reply proves a live exact birth: the view holds it (until membership speaks for
        // it), settled as ReadyForTraffic, in transition as its source held it (a Leaving
        // member that answers is alive and still leaving). A settled node that did not answer
        // stays in the map as not yet reached; one in transition is left to its own last digest.
        for n in &report.reached {
            records.publish(n.as_node(n.transition.unwrap_or(NodeStatus::ReadyForTraffic), provider_kind));
        }
        for (n, _) in report.not_reached.iter().filter(|(n, _)| n.transition.is_none()) {
            records.publish(n.as_node(NodeStatus::PendingReconnect, provider_kind));
        }
        // A node not reached is unheard, so the mesh-primary's tick watches it: its tickle, then
        // the provider's exact exit proof, then node recovery only on proof (mesh-recovery.md step 6).
        let _ = slot.set(report);
    })
}

/// The control entry retrieved again ([`crate::hydrate::EntryRepeat`]): the join a start made, to the
/// same authority with the same digest, its answer taken as the start took the first one.
#[allow(clippy::too_many_arguments)]
fn entry_repeat(
    client: Arc<rafka_node_rpc::NodeRpcClient>,
    target: rafka_node_rpc::NodeTarget,
    anchor: String,
    digest: MeshDigest,
    accepted: Arc<AcceptedStore>,
    floor: Arc<Mutex<Option<(crate::build::BuildId, u32)>>>,
    shutdown: Arc<crate::shutdown::ShutdownControl>,
    rafka_time: rafka_mesh_transport::clock::RafkaTime,
) -> crate::hydrate::EntryRepeat {
    Arc::new(move || {
        let (client, target, anchor, digest, accepted, floor, shutdown, rafka_time) = (client.clone(), target.clone(), anchor.clone(), digest.clone(), accepted.clone(), floor.clone(), shutdown.clone(), rafka_time.clone());
        Box::pin(async move {
            let answer = crate::join::call_join(&client, &target, &anchor, &digest, 1).await.map_err(|e| e.to_string())?;
            crate::rafka_time::adopt_join_answer(&tracing::Span::current(), &rafka_time, &digest.node.name.to_string(), &answer);
            take_entry_control(&answer, &accepted, &floor).await;
            if let Some(sd) = answer.control.shutdown.clone() {
                shutdown.learn(sd, "hydration", &answer.served_by).await.map_err(|e| e.to_string())?;
            }
            Ok(())
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    })
}

/// What this admin answers an entry pull with.
struct EntryState {
    name: PathName,
    provider: ProviderKind,
    accepted: Arc<AcceptedStore>,
    builds: Arc<dyn BuildStateAdapter>,
    shutdown: Arc<crate::shutdown::ShutdownControl>,
    membership: Membership,
    /// The rafka-time this admin adopted: a join is answered with it, never with the OS clock.
    time: rafka_mesh_transport::clock::RafkaTime,
}

impl EntryState {
    /// What this admin holds now: the answer to a join (its control state and statuses; the
    /// topology is read with `GetTopology`).
    async fn answer(&self) -> Result<crate::wire::JoinAnswer, String> {
        let e = self;
        // An admin that holds no rafka-time has none to hand a joiner: the join is not completed,
        // and the refusal says why.
        let rafka_time_ms = e.time.try_now_ms().ok_or_else(|| format!("{}: this admin holds no rafka-time to answer a join with", e.name))?;
        // The fabric control state a new admin hydrates before it may be Ready: the answering admin's
        // current Build (R-BI3) and the attempt it holds of it, the floor a joiner's own copy of that
        // Build's attempt facts must reach before it is Ready.
        // A store that cannot be read is no answer: the join is refused naming it, never answered as
        // an admin that holds no Build or no Fabric.
        let build = e.builds.current().await.map_err(|err| format!("{}: its builds store cannot be read for the current Build: {err}", e.name))?.map(|b| crate::wire::BuildFloor { build_id: b.build_id, attempt: b.attempt });
        let fabric = e.accepted.record().await.map_err(|err| format!("{}: its fabric.storage cannot be read for the Fabric identity: {err}", e.name))?;
        let control = crate::wire::JoinControl { provider: e.provider, fabric, shutdown: e.shutdown.held(), build, rafka_time_ms, member_cert: Vec::new(), name: None, config: None };
        Ok(crate::wire::JoinAnswer { served_by: e.name.to_string(), control, statuses: e.membership.status_frames(&e.name.to_string()) })
    }
}

mod leave;
pub use leave::LeaveDoor;

use crate::fence::FenceOutcome;

/// Realises Build operations through the deployment and lifecycle pipelines.
pub struct AdminRunner {
    /// The provider that realises runtimes.
    pub provider: Arc<dyn DeploymentProvider>,
    /// The births this admin deployed and awaits a `JoinNode` from.
    pub joins: Arc<crate::join::Joins>,
    /// The observer of births' membership.
    pub observer: Arc<MembershipObserver>,
    /// The records this admin holds.
    pub records: Arc<Records>,
    /// The Build state.
    pub builds: Arc<dyn BuildStateAdapter>,
    /// This admin's own Build log, the one `builds` reads: facts read from another admin are
    /// absorbed into it without authoring Build gossip.
    pub local: Arc<dyn crate::build_state::LocalBuildLog>,
    /// The facts every launch carries.
    pub template: LaunchTemplate,
    /// The environment passed to every node-admin launch.
    pub admin_env: BTreeMap<String, String>,
    /// The directory the node executables are looked up in.
    pub bin_dir: PathBuf,
    /// Explicit executable bindings: when set, every launch runs its bound executable and
    /// `bin_dir` is never consulted.
    pub bindings: Option<rafka_mesh_entity::binding::Validated>,
    /// The lifecycle transitions of the births this admin executes.
    pub lifecycle: LifecycleTransitionPipeline,
    /// The runtimes this admin started, by node.
    pub handles: Mutex<HashMap<PathName, (Node, DeploymentHandle)>>,
    /// The observed topology.
    pub topology: Arc<RwLock<Topology>>,
    /// What the views are projected from.
    pub fabric: String,
    /// The fabric's minted id.
    pub fabric_id: FabricId,
    /// The provider that runs the fabric's nodes.
    pub fabric_provider: ProviderKind,
    /// The digest book.
    pub book: DigestBook,
    /// This admin's own path.
    pub me: PathName,
    /// This admin's node id (the adopter on every runtime it adopts).
    pub node_id: NodeId,
    /// This admin's Iroh endpoint: it asks a node directly whether it answers.
    pub endpoint: Option<Endpoint>,
    /// This process's one Node RPC client (and its live resolver): every running admin holds it.
    pub node_rpc: crate::node_rpc::ProcessNodeRpc,
    /// Where the retire pipeline's lifecycle events go: this admin's membership and backbone.
    pub lifecycle_events: Arc<dyn crate::deployment::pipeline::LifecycleEvents>,
    /// The mesh leaves this admin, as the fabric-primary, has admitted and awaits a handoff for.
    pub leaves: Arc<crate::mesh_leave::LeaveBook>,
    /// The mesh leaves this admin, as a leaving mesh's primary, is running or has finished.
    pub leave_runs: crate::mesh_leave::LeaveRuns,
    /// The fabric-primary handover: this admin's initiator, doors and recorded operations.
    pub handover: Arc<crate::fabric_handover::HandoverDoor>,
    /// The signer this admin issues a new mesh's first admin's issuing material with.
    pub issuer: Arc<crate::certs::CertIssuer>,
    /// This admin's status.storage: as a mesh primary, the exit-proof rows of its members' exited
    /// births, which a recovery publishes node.deleted from.
    pub status_storage: Arc<dyn crate::status_storage::StatusStorage>,
    /// This admin's mesh.storage: as a mesh primary, its mesh's row Dead at mesh.deleted, and as
    /// fabric-primary at mesh-left, a leaving mesh's row Dead when no primary survived (R-MD1).
    pub mesh_storage: Arc<dyn crate::storage::MeshStorage>,
}

/// The lifecycle events of an operation this admin executes, on its own mesh channel only. The
/// backbone carries them as the open overlays and retained departures of this mesh's aggregate,
/// which its mesh primary publishes; a peer mesh's primary forwards them onto its own channel.
pub struct GossipLifecycle {
    /// This admin's mesh channel.
    pub membership: Membership,
    /// This admin's place on the backbone: the `node-draining` and `node-leaving` command hooks
    /// publish there as well.
    pub backbone: Backbone,
    /// This admin's own path: the publisher of the mesh frames it authors.
    pub me: PathName,
    /// This admin's mesh.delete door, once its runner exists: every birth this executor publishes
    /// deleted is handed to it (mesh-left follows the final primary's exit proof, R-MD1 item 2).
    pub leaver: crate::mesh_leave::LeaverSlot,
}

#[async_trait::async_trait]
impl crate::deployment::pipeline::LifecycleEvents for GossipLifecycle {
    fn now_rafka_ms(&self) -> u64 {
        self.membership.clock().now_rafka_ms()
    }
    async fn deleting(&self, op: &rafka_mesh_entity::LifecycleOp) {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-node-deleting", node = %op.name, node_id = %op.node_id, build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        async {
            self.backbone.publish_status_event(rafka_mesh_transport::membership::StatusEvent::Deleting, op).await;
            tracing::info!("the node is being removed: every mesh hears it is not routable");
        }
        .instrument(span)
        .await
    }
    async fn restarting(&self, op: &rafka_mesh_entity::LifecycleOp) {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-node-restarting", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        async {
            self.backbone.publish_status_event(rafka_mesh_transport::membership::StatusEvent::Restarting, op).await;
            tracing::info!("the node is being restarted: every mesh holds it through its Leaving");
        }
        .instrument(span)
        .await
    }
    async fn stopped(&self, op: &rafka_mesh_entity::LifecycleOp) {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-node-stopped", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        async {
            // This admin executed the stop and holds its matching stopped reply: the event carries the
            // version at which its stop changed the birth. The stop of a deleted mesh's last admin
            // makes this fabric-primary that mesh's publisher of record (gossip.md "Mesh delete").
            self.backbone.publish_status_event(rafka_mesh_transport::membership::StatusEvent::Stopped, op).await;
            tracing::info!("the node is stopped: every mesh drops it from its book; no heartbeat of it ends the stop, only this mesh-primary's node.joined");
        }
        .instrument(span)
        .await
    }
    async fn stop_completed(&self, op: &rafka_mesh_entity::LifecycleOp) {
        // A former seat holder's stop completed: its own "primary updated" was best-effort and its
        // gossip is cut now, so this mesh's primary, its next writer, says the committed update
        // (R-NS2; proposal Part C1 failure contract), before the stop's own status event.
        self.backbone.announce_update_of_stopped_holder(op).await;
    }
    async fn joined(&self, op: &rafka_mesh_entity::LifecycleOp) {
        // The executor's own book ends the stop and holds the birth Pending; its next publication
        // round carries both in this mesh's projection (gossip.md §3.1 `Members`): no frame here.
        self.membership.book.joined(op);
    }
    async fn connections_deleting(&self, op: &rafka_mesh_entity::LifecycleOp) {
        let span = tracing::info_span!("rdm.node_admin.node.update.via-connections-deleting", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        // connections-deleting is this mesh-primary's to set at the admission of
        // node.connections.delete { All } (states.md "Who sets each state"); its projection carries
        // it. No frame is published for it (gossip.md §3.1: NodeDraining is a reserved position).
        span.in_scope(|| {
            self.membership.book.admitted(op, MemberStatus::Draining);
            tracing::info!("node.connections.delete {{ All }} was admitted: the birth is connections-deleting in this mesh-primary's projection");
        });
    }
    async fn leaving(&self, op: &rafka_mesh_entity::LifecycleOp) {
        let f = rafka_mesh_transport::membership::Frame::NodeLeaving { op: op.clone(), forwarded_by: None };
        let span = tracing::info_span!("rdm.node_admin.node.update.via-node-leaving", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation, channels = "mesh,backbone");
        async {
            // stopping is this mesh-primary's to set at the stop's admission (states.md "Who sets each
            // state"); its projection carries it.
            self.membership.book.admitted(op, MemberStatus::Leaving);
            if let Err(e) = self.backbone.publish_command_hook(&f).await {
                tracing::info!(error = %e, "node-leaving not sent on every channel");
            }
            tracing::info!("the stop command was accepted: node-leaving published on the mesh channel and the backbone");
        }
        .instrument(span)
        .await
    }
    async fn mesh_leaving(&self, e: &crate::deployment::pipeline::MeshLeaveEvent) {
        use rafka_mesh_transport::membership::Frame;
        let f = Frame::MeshLeaving { mesh_id: e.mesh_id.clone(), build_id: e.build_id.clone(), attempt: e.attempt, operation: e.operation.clone(), publisher: self.me.to_string(), event_at_rafka_ms: self.membership.clock().now_rafka_ms(), forwarded_by: None };
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-mesh-leaving", mesh_id = %e.mesh_id, build_id = %e.build_id, attempt = e.attempt, operation = %e.operation, publisher = %self.me, channels = "mesh,backbone");
        async {
            if let Err(e) = self.backbone.publish_command_hook(&f).await {
                tracing::info!(error = %e, "mesh-leaving not sent on every channel");
            }
            tracing::info!("the leave-mesh command was accepted: mesh-leaving published on the backbone and this mesh's channel");
        }
        .instrument(span)
        .await
    }
    async fn mesh_leave(&self, e: &crate::deployment::pipeline::MeshLeaveEvent) {
        use rafka_mesh_transport::membership::Frame;
        let Some((final_node_id, final_incarnation)) = e.final_primary.clone() else { return };
        let f = Frame::MeshLeave {
            mesh_id: e.mesh_id.clone(),
            build_id: e.build_id.clone(),
            attempt: e.attempt,
            operation: e.operation.clone(),
            final_node_id,
            final_incarnation,
            receipt_manifest: e.receipt_manifest.clone().unwrap_or_default(),
            publisher: self.me.to_string(),
            event_at_rafka_ms: self.membership.clock().now_rafka_ms(),
            forwarded_by: None,
        };
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-mesh-leave", mesh_id = %e.mesh_id, build_id = %e.build_id, attempt = e.attempt, operation = %e.operation, publisher = %self.me, receipt_manifest = e.receipt_manifest.as_deref().unwrap_or(""), channels = "mesh,backbone");
        async {
            if let Err(e) = self.backbone.publish_command_hook(&f).await {
                tracing::info!(error = %e, "mesh-leave not sent on every channel");
            }
            tracing::info!("the mesh-leave handoff was called: mesh-leave published on the backbone and this mesh's channel");
        }
        .instrument(span)
        .await
    }
    async fn mesh_left(&self, e: &crate::deployment::pipeline::MeshLeaveEvent) {
        use rafka_mesh_transport::membership::Frame;
        let f = Frame::MeshLeft { mesh_id: e.mesh_id.clone(), build_id: e.build_id.clone(), attempt: e.attempt, operation: e.operation.clone(), receipt_manifest: e.receipt_manifest.clone().unwrap_or_default(), publisher: self.me.to_string(), event_at_rafka_ms: self.membership.clock().now_rafka_ms(), forwarded_by: None };
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-mesh-left", mesh_id = %e.mesh_id, build_id = %e.build_id, attempt = e.attempt, operation = %e.operation, publisher = %self.me, receipt_manifest = e.receipt_manifest.as_deref().unwrap_or(""), channels = "mesh,backbone");
        async {
            if let Err(e) = self.backbone.publish_command_hook(&f).await {
                tracing::info!(error = %e, "mesh-left not sent on every channel");
            }
            tracing::info!("every exact exit is proven: mesh-left published on the backbone and this mesh's channel");
        }
        .instrument(span)
        .await
    }
    async fn deleted(&self, op: &rafka_mesh_entity::LifecycleOp) {
        let f = rafka_mesh_transport::membership::Frame::NodeDeleted { op: op.clone(), forwarded_by: None };
        let span = tracing::info_span!("rdm.node_admin.node.delete.via-node-deleted", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        async {
            // The departure the publication takes into this admin's own book: a deleted mesh's
            // aggregate, which the fabric-primary publishes from what its book holds, moves the birth
            // to `deleted` (mesh-delete.md step 7, gossip.md "Mesh delete").
            if let Err(e) = self.membership.publish_lifecycle(&f).await {
                tracing::info!(error = %e, "NodeDeleted not sent on the mesh channel");
            }
            tracing::info!("the exact birth is proven terminal: it has left");
        }
        .instrument(span)
        .await;
        // The mesh.delete door observes the deleted birth: the final primary of a deleted mesh is
        // its mesh-left (R-MD1 item 2); any other birth is nothing to it.
        if let Some(leaver) = self.leaver.get().cloned() {
            leaver.birth_deleted(op).await;
        }
    }
}

impl AdminRunner {
    /// Publish the view as it is now, so a Build that completes after this
    /// operation is never read back against an older view.
    async fn refresh_view(&self) {
        self.records.install_read(&self.me, &self.topology, &self.book, &self.fabric_id, |reads| project_with(&self.fabric, &self.fabric_id, self.fabric_provider, &self.book, &self.records, reads)).await;
    }

    /// A launch of `kind` in `mesh`. Every node carries its mesh's identity (it
    /// names the mesh's channel): the one this admin created, else the one the
    /// fabric knows (a recovered mesh is reconstructed as itself, never anew).



    async fn template_for(&self, kind: NodeKind, mesh: &str) -> Result<LaunchTemplate, String> {
        let known = self.topology.read().await.meshes.iter().find(|m| m.name == mesh).and_then(|m| m.id.clone());
        let mut t = self.template.clone();
        t.executable = match &self.bindings {
            // Explicit mode: the bound executable, re-hashed now; an unbound kind is refused, never
            // launched from the built-in set.
            Some(b) => {
                let r = b.resolve(kind).map_err(|e| format!("{kind:?} in {mesh}: {e}", kind = kind.name()))?;
                tracing::info!(launch_id = %r.launch_id, executable = %r.executable.display(), sha256 = %r.sha256, candidate = %r.candidate.sha, "launching from the explicit executable binding");
                r.executable
            }
            // The built-in set: the node-admin, and the one node executable every other node_type
            // runs, taking its node_type from the launch (rafka-node-base `run_launched`). The rpc
            // node is the testkit's proof node: the proof protocols on top of the node runtime.
            None => {
                let name = match kind {
                    NodeKind::NodeAdmin => "rafka-node-admin",
                    NodeKind::RpcNode => "rafka-rpc-node",
                    _ => "rafka-node",
                };
                self.bin_dir.join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
            }
        };
        if kind == NodeKind::NodeAdmin {
            t.env.extend(self.admin_env.clone());
        }
        // Every node of a mesh carries its id: it names the mesh's channel.
        if let Some(id) = self.records.meshes.lock().unwrap().get(mesh).cloned().or(known) {
            t.env.insert(rafka_mesh_entity::launch::ENV_MESH_ID.into(), id.to_string());
        }
        Ok(t)
    }

    /// `template` for the launch of a mesh's FIRST node-admin: it carries the mesh's issuing
    /// material, issued by the app's signer on this admin's rafka-time. A refusal fails the create
    /// by name: the admin is never launched without it.
    fn with_mesh_issuer(&self, mut t: LaunchTemplate, mesh: &str) -> Result<LaunchTemplate, String> {
        let issuer = self.issuer.mesh_issuer(mesh).map_err(|e| match e {
            crate::certs::IssueFailure::Refused(r) => format!("{mesh}: the cert signer refused to issue the mesh's issuing material: {r}"),
            crate::certs::IssueFailure::NoRafkaTime(why) => format!("{mesh}: {why}"),
        })?;
        t.mesh_issuer = Some(issuer).filter(|b| !b.is_empty());
        Ok(t)
    }

    fn pipeline<'a>(&'a self, template: &'a LaunchTemplate) -> DeploymentPipeline<'a> {
        self.pipeline_with(template, &*self.builds)
    }

    /// [`Self::pipeline`] writing its receipts to and reading its Build from `builds` (an attempt's
    /// lane, [`crate::build_run::LaneBuilds`]).
    fn pipeline_with<'a>(&'a self, template: &'a LaunchTemplate, builds: &'a dyn BuildStateAdapter) -> DeploymentPipeline<'a> {
        DeploymentPipeline {
            provider: &*self.provider,
            joins: &self.joins,
            observer: &*self.observer,
            sink: &*self.records,
            lifecycle: &*self.lifecycle_events,
            builds,
            template,
            timeouts: Timeouts::default(),
        }
    }

    /// The runtime of `node`'s birth: this admin's own handle, or the exact
    /// runtime the birth publishes with its membership (`MeshNode::runtime`),
    /// adopted when its provider and control domain are this admin's. No
    /// Build history is read: whoever launched it, and however long ago, the
    /// birth itself says where its runtime is. A fact from another provider
    /// or control domain is refused by name, never acted on.
    pub(crate) async fn handle_for(&self, node: &Node) -> Result<(Node, DeploymentHandle), String> {
        if let Some((rec, h)) = self.handles.lock().unwrap().get(&node.name).cloned() {
            if node.incarnation_id.is_none() || rec.incarnation_id == node.incarnation_id {
                return Ok((rec, h));
            }
        }
        let incarnation = node.incarnation_id.clone().ok_or_else(|| format!("{} has no known birth", node.name))?;
        // The birth says where its runtime is: its digest, or (a birth this admin never heard) the
        // durable runtime row its maker handed over.
        let held = self.book.get(node.node_id.as_str()).map(|(d, _)| d).or_else(|| self.book.parked().into_iter().find(|d| d.node.node_id == node.node_id)).filter(|d| d.node.incarnation == incarnation);
        let row = match (&held, self.records.contacts.get()) {
            (None, Some(store)) => birth_runtime_row(&**store, &self.me, &node.name, &node.node_id, &incarnation).await?,
            _ => None,
        };
        let (published, data_dir) = match (&held, &row) {
            (Some(d), _) => (d.node.runtime.clone(), d.data_dir.clone()),
            (None, Some(r)) => (Some(r.runtime.clone()), r.data_dir.clone()),
            (None, None) => return Err(format!("{} (birth {}) is not held in this admin's membership or its durable runtime rows; its runtime is unknown", node.name, incarnation.as_str())),
        };
        let refuse = |reason: &str, detail: String| {
            let span = match reason {
                "unpublished" => tracing::info_span!("rdm.node_admin.runtime.reject.via-unpublished", node = %node.name, incarnation_id = %incarnation.as_str(), adopter = %self.me, detail = %detail),
                "other-provider" => tracing::info_span!("rdm.node_admin.runtime.reject.via-other-provider", node = %node.name, incarnation_id = %incarnation.as_str(), adopter = %self.me, detail = %detail),
                "foreign-control-domain" => tracing::info_span!("rdm.node_admin.runtime.reject.via-foreign-control-domain", node = %node.name, incarnation_id = %incarnation.as_str(), adopter = %self.me, detail = %detail),
                _ => tracing::info_span!("rdm.node_admin.runtime.reject.via-invalid-fact", node = %node.name, incarnation_id = %incarnation.as_str(), adopter = %self.me, detail = %detail),
            };
            span.in_scope(|| tracing::info!("runtime not adopted"));
            format!("{} (birth {}): {detail}", node.name, incarnation.as_str())
        };
        let fact = published.ok_or_else(|| refuse("unpublished", "the birth publishes no runtime fact; it cannot be adopted".into()))?;
        let handle = crate::deployment::provider::adopt(&*self.provider, &fact).map_err(|r| refuse(r.reason(), r.to_string()))?;
        let mut record = node.clone();
        record.deployment_id = Some(handle.deployment_id.clone());
        if record.data_dir.is_none() {
            record.data_dir = data_dir;
        }
        tracing::info_span!(
            "rdm.node_admin.runtime.update.via-adopt",
            node = %node.name,
            node_id = %node.node_id,
            incarnation_id = %incarnation.as_str(),
            deployment_id = %fact.deployment_id,
            provider = fact.provider.as_str(),
            provider_control_domain_fingerprint = %fact.domain_fingerprint(),
            runtime_locator_kind = fact.locator.kind(),
            runtime_locator_fingerprint = %fact.locator_fingerprint(),
            source = if held.is_some() { "self-published-membership" } else { "durable-runtime-row" },
            adopter = %self.me,
            adopter_node_id = %self.node_id,
            execution_node_id = %self.node_id,
        )
        .in_scope(|| tracing::info!("adopted a runtime this admin did not launch"));
        self.handles.lock().unwrap().insert(node.name.clone(), (record.clone(), handle.clone()));
        Ok((record, handle))
    }

    /// Fence `path` before a new birth (`crate::fence`): this admin answers the fence's questions.
    async fn fence_predecessor(&self, path: &PathName) -> FenceOutcome {
        let prev = self.topology.read().await.node(path).cloned();
        let out = crate::fence::fence(path, prev, self).await;
        if matches!(out, FenceOutcome::Clear { .. }) {
            self.handles.lock().unwrap().remove(path);
        }
        out
    }

    /// Does `node`'s runtime answer when asked directly (not through gossip)?
    /// A Node RPC `Ping`, two seconds; a frozen or gone runtime does not answer.
    async fn answers(&self, node: &Node) -> bool {
        const WITHIN: Duration = Duration::from_secs(2);
        // Every kind, a node-admin included, serves Ping on its one endpoint.
        let node_rpc = &self.node_rpc;
        let opts = rafka_node_rpc::CallOptions { budget: rafka_node_rpc::Budget::Overall(WITHIN), ..Default::default() };
        let req = rafka_node_rpc_contract::ping::PingRequest::Ping { payload: b"fence".to_vec() };
        let (out, _) = node_rpc.client.call::<rafka_node_rpc_contract::ping::Ping>(&rafka_node_rpc::NodeTarget::ExactNode(node.node_id.clone()), &req, &opts).await;
        out.reply().is_some()
    }

    /// `Node: Pending -> ReadyForTraffic`, committed once the node reports ready.
    async fn bring_into_traffic(&self, node: &Node) -> Result<(), String> {
        let key = TransitionKey { scope: LifecycleScope::Node, from: LifecycleState::Pending, to: LifecycleState::ReadyForTraffic };
        let shape = ShapeFacts { desired_meshes: self.topology.read().await.meshes.len() as u32 };
        let ready = self.observer.ready(node).await;
        let birth = node.incarnation_id.as_ref().ok_or_else(|| format!("{} has no known birth", node.name))?;
        let t = Transition { transition_id: Transition::id_for_birth(key, &node.name.to_string(), &birth.as_str()), target: node.name.to_string(), key, shape: &shape };
        match self.lifecycle.transition(t, || ready, || {}).await {
            TransitionResult::Committed => Ok(()),
            other => Err(format!("{}: {other:?}", node.name)),
        }
    }

    /// The births' runtimes this admin holds, durable rows and the digests it hears now: the map
    /// a node-admin it launches starts from (a successor fabric primary proves exits from it).
    async fn held_runtimes(&self) -> Vec<crate::storage::RuntimeRow> {
        let mut rows: BTreeMap<String, crate::storage::RuntimeRow> = BTreeMap::new();
        if let Some(store) = self.records.contacts.get() {
            for r in store.runtimes().await.unwrap_or_default() {
                rows.insert(r.key(), r);
            }
        }
        for d in self.book.all() {
            if let Some(runtime) = d.node.runtime.clone() {
                let r = crate::storage::RuntimeRow { node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime, data_dir: d.data_dir.clone() };
                rows.insert(r.key(), r);
            }
        }
        rows.into_values().collect()
    }

    async fn create(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, before_ready: Option<crate::deployment::pipeline::BeforeReady>) -> Result<(), String> {
        match self.fence_predecessor(node).await {
            // The member is alive (it answers) or held (its exact runtime still runs):
            // nothing to create. Silence never authorizes a replacement.
            FenceOutcome::Alive | FenceOutcome::Held => return Ok(()),
            // Nothing proves the path free: the create fails, naming what could not be read.
            FenceOutcome::RecordsUnreadable { reason } => return Err(format!("{node}: the runtimes recorded at this path cannot be read, so no birth is created over them: {reason}")),
            // A birth the provider proves terminal is replaced at its path: the old identity is
            // deleted (its terminal proof, NodeDeleted, its Deleted row), then the successor is
            // created (node-recovery.md "Steps").
            FenceOutcome::Clear { gone: Some(prev) } => match prev.incarnation_id {
                Some(birth) => return self.replace(build_id, attempt, node, &birth).await,
                None => {}
            },
            FenceOutcome::Clear { gone: None } => {}
        }
        let mut template = self.template_for(node.kind, &node.mesh).await?;
        if node.kind == NodeKind::NodeAdmin && before_ready.is_some() {
            template = self.with_mesh_issuer(template, &node.mesh)?;
        }
        let held_runtimes = if node.kind == NodeKind::NodeAdmin { self.held_runtimes().await } else { Vec::new() };
        let req = CreateRequest { build_id: build_id.clone(), attempt, node: node.clone(), spec: spec_for(node.kind), data_dir: None, held_runtimes };
        let created = self.pipeline(&template).create_with(&req, before_ready, None).await.map_err(|e| e.to_string())?;
        self.bring_into_traffic(&created.node).await?;
        self.handles.lock().unwrap().insert(node.clone(), (created.node, created.handle));
        Ok(())
    }

    /// The fabric-primary -> mesh state hand-off at a mesh's first admin, as the create pipeline's
    /// `ApplyMeshPending` step at the exact birth just joined at `admin`: `Pending` for a new mesh's
    /// bootstrap admin (e4.s11), the mesh's current state for the admin created for an existing
    /// mesh. A bounded reachability wait (Ping, at most 20 s, no Status), then ONE
    /// `ApplyMeshState(state)` call. `Applied` or
    /// `AlreadyApplied` succeed; any other outcome fails the step typed (a stale-birth answer, a
    /// backward move, an unreachable target, a lost reply), and the rectifier is the retry, so
    /// `Build(shape)` never starts under an assumed state.
    fn mesh_state_hook(&self, admin: &PathName, state: rafka_node_rpc_contract::status::MeshState) -> Result<crate::deployment::pipeline::BeforeReady, String> {
        let node_rpc = &self.node_rpc;
        let (client, resolver, me, me_id, topology, mesh, records) = (node_rpc.client.clone(), node_rpc.resolver.clone(), self.me.clone(), self.node_id.clone(), self.topology.clone(), admin.mesh.clone(), self.records.clone());
        Ok(Arc::new(move |target: Node| {
            let (client, resolver, me, me_id, topology, mesh, records) = (client.clone(), resolver.clone(), me.clone(), me_id.clone(), topology.clone(), mesh.clone(), records.clone());
            Box::pin(async move {
                // The mesh's exact identity, as the launch took it: what this admin created, or else
                // what its view holds from the mesh's members (a recovery never mints one).
                let known = topology.read().await.meshes.iter().find(|m| m.name == mesh).and_then(|m| m.id.clone());
                let Some(mesh_id) = records.meshes.lock().unwrap().get(&mesh).cloned().or(known).map(|m| m.to_string()) else {
                    return Err(format!("{}: neither this admin's records nor its view hold a mesh id for {mesh}, so no {state:?} can be applied", target.name));
                };
                apply_mesh_state(&client, &resolver, &me, &me_id, &topology, &mesh, &mesh_id, state, &target).await.map(|()| state)
            })
        }))
    }

    /// `replace-node:<path>`: the exact birth `from` is replaced by a new node at its path
    /// (`DeploymentPipeline::replace`). The successor is created here, never through `create`: that
    /// door fences against a birth at the path, and the predecessor is the one being replaced.
    async fn replace(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &IncarnationId) -> Result<(), String> {
        let (predecessor, handle) = self.predecessor_of(build_id, node, from).await?;
        // A mesh's only admin replaced leaves the mesh without a live admin: its successor is that
        // mesh's first admin and receives the fabric primary's Pending hand-off (e4.s11).
        let first_admin = node.kind == NodeKind::NodeAdmin && self.topology.read().await.cohort(&node.mesh, NodeKind::NodeAdmin).filter(|n| n.name != *node).all(|n| !n.status.is_live());
        let before_ready = if first_admin { Some(self.mesh_state_hook(node, rafka_node_rpc_contract::status::MeshState::Pending)?) } else { None };
        let mut template = self.template_for(node.kind, &node.mesh).await?;
        if node.kind == NodeKind::NodeAdmin && first_admin {
            template = self.with_mesh_issuer(template, &node.mesh)?;
        }
        let held_runtimes = if node.kind == NodeKind::NodeAdmin { self.held_runtimes().await } else { Vec::new() };
        let req = crate::deployment::pipeline::ReplaceRequest { build_id: build_id.clone(), attempt, predecessor, handle, spec: spec_for(node.kind), held_runtimes };
        let created = self.pipeline(&template).replace(&req, before_ready, |ready| async move { self.bring_into_traffic(&ready).await }).await.map_err(|e| e.to_string())?;
        // A recovered predecessor's exit is proven; a replaced one is parked at `<path>.old`, its
        // process alive: nothing is retired about its paths or its birth.
        if matches!(self.provider.inspect(&req.handle).await, crate::deployment::provider::DeploymentStatus::Exited { .. }) {
            self.after_exit(&req.predecessor);
        }
        self.handles.lock().unwrap().insert(node.clone(), (created.node, created.handle));
        Ok(())
    }

    /// The exact exited birth `from` at `node` a node recovery acts for (node-recovery.md), and its
    /// exact runtime: the view's record while it holds that birth, else a record of the exact birth
    /// resolved from the membership book or the durable runtime row this admin wrote when it first
    /// held the birth (a departed birth has left the book). `op` names the operation in a refusal.
    async fn exited_birth(&self, op: &str, node: &PathName, from: &crate::build::ExitedBirth) -> Result<(Node, DeploymentHandle), String> {
        let seen = self.topology.read().await.node(node).cloned().filter(|n| n.node_id == from.node_id && n.incarnation_id.as_ref() == Some(&from.incarnation));
        let exact = seen.unwrap_or_else(|| {
            let mut n = Node::allocated(node.clone());
            n.node_id = from.node_id.clone();
            n.incarnation_id = Some(from.incarnation.clone());
            n
        });
        let (exited, handle) = self.handle_for(&exact).await.map_err(|e| format!("{op}:{node}: the exited birth's runtime is not resolved: {e}"))?;
        if exited.node_id != from.node_id || exited.incarnation_id.as_ref() != Some(&from.incarnation) {
            return Err(format!("{op}:{node}: resolved birth {} at {:?}, the recovery is of {} at {}", exited.node_id, exited.incarnation_id.as_ref().map(|i| i.as_str()), from.node_id, from.incarnation.as_str()));
        }
        Ok((exited, handle))
    }

    /// Node recovery's node.delete (node-recovery.md sequence step 3; node-delete.md "Crashed
    /// target"): the exact exited birth `from` at `node`, deleted through the delete workflow on this
    /// admin's recorded exit proof, nothing sent to the dead process. A birth the provider sees running
    /// is not exited and is not deleted. The fence of the old birth is the create's
    /// ([`Self::create_over`]), done right before the new birth with its key is launched.
    async fn delete_exited(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &crate::build::ExitedBirth) -> Result<(), String> {
        let exit = self
            .status_storage
            .node_exit(&from.node_id, &from.incarnation)
            .await
            .map_err(|e| format!("retire-node:{node}: the exit-proof row of birth {} at {} could not be read from {}'s status.storage: {e}", from.node_id, from.incarnation.as_str(), self.me))?
            .ok_or_else(|| format!("retire-node:{node}: {} holds no exit-proof row for birth {} at {}: only a proven exit is deleted as a crashed target", self.me, from.node_id, from.incarnation.as_str()))?;
        let (exited, handle) = self.exited_birth("retire-node", node, from).await?;
        if self.provider.inspect(&handle).await == crate::deployment::provider::DeploymentStatus::Running {
            return Err(format!("retire-node:{node}: birth {} at {} still runs: it is not exited and is not deleted as a crashed target", from.node_id, from.incarnation.as_str()));
        }
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: exited, handle, exit: Some(exit) };
        self.pipeline_with(&template, builds).retire(&req).await.map_err(|e| e.to_string())?;
        let held = self.handles.lock().unwrap().get(node).is_some_and(|(n, _)| n.node_id == from.node_id && n.incarnation_id.as_ref() == Some(&from.incarnation));
        if held {
            self.handles.lock().unwrap().remove(node);
        }
        Ok(())
    }

    /// Node recovery's node.create (node-recovery.md sequence steps 4-5): a new node at `node` over
    /// the exited birth `from`'s data dir, through the create workflow: it loads the endpoint key
    /// that dir holds and binds port 0, with a new node.id and incarnation, no supersedes. Not through
    /// the create door's fence: the birth at the path is the exited one this recovery deleted, and
    /// the fence would replace it.
    ///
    /// The exited birth is fenced here, by the create (node-recovery.md "Delete fences the old
    /// instance before the new join"): right before the new runtime with the same key is launched,
    /// inside its `DeployRuntime` step ([`crate::deployment::pipeline::BeforeLaunch`]). Whichever
    /// executor runs the create fences, so an executor lost after the delete's Complete leaves
    /// nothing unfenced; a rerun that reuses a runtime an earlier attempt launched (it was fenced
    /// before that launch) cuts none of that runtime's paths.
    async fn create_over(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &crate::build::ExitedBirth) -> Result<(), String> {
        let (exited, _) = self.exited_birth("create-node", node, from).await?;
        // The launch is the kind's template, as every create's: the new node's config is seeded
        // from the exited birth's Deleted row by the create's name claim (`NodeRows::claim`) and told
        // to it when its row is written at its arrival (`NodeRows::write_arrivals`).
        let template = self.template_for(node.kind, &node.mesh).await?;
        let pipeline = self.pipeline_with(&template, builds);
        let data_dir = pipeline.birth_dir(&exited);
        // The key the fence names is the one the kept data dir holds (a record resolved from the
        // durable runtime rows carries none): the node created over that dir loads exactly it.
        let mut fenced = exited;
        if fenced.endpoint_id.is_none() {
            fenced.endpoint_id = crate::deployment::pipeline::load_transport_key(&data_dir).ok();
        }
        // A node-admin's recovery is a node-admin birth whose maker is this mesh-primary (the
        // template's seed): it is handed the durable runtime rows before it runs, as every admin
        // birth is.
        let held_runtimes = if node.kind == NodeKind::NodeAdmin { self.held_runtimes().await } else { Vec::new() };
        let req = CreateRequest { build_id: build_id.clone(), attempt, node: node.clone(), spec: spec_for(node.kind), data_dir: Some(data_dir), held_runtimes };
        let created = pipeline.create_with(&req, None, Some(Box::pin(self.fence_exited(&fenced)))).await.map_err(|e| e.to_string())?;
        self.bring_into_traffic(&created.node).await?;
        self.handles.lock().unwrap().insert(node.clone(), (created.node, created.handle));
        Ok(())
    }

    /// node-recovery.md "Delete fences the old instance before the new join": the exited birth's
    /// resolver entry is retired and its direct paths dropped, awaited, right before the new birth
    /// with the same endpoint key is launched (the create over its data dir, [`Self::create_over`]).
    ///
    /// The await is bounded, and it is awaited (not spawned as `after_exit` does) because the new
    /// birth carries the SAME key: a retire landing after the new birth's paths exist would retire
    /// those. Bounded, in the pinned iroh (rev 921afe53): `Socket::replace_direct_addrs`
    /// (iroh/src/socket.rs:1408-1420) returns at once when no actor runs for the key, else sends one
    /// message into the actor's 64-slot inbox (socket/remote_map/remote_state.rs:246) and awaits its
    /// oneshot. The actor's loop (remote_state.rs:300-360) runs only synchronous arm bodies and
    /// drains one inbox message per turn through the synchronous `handle_message` (:387); the
    /// `ReplaceDirectAddrs` arm (:403-405) retires the paths in the synchronous
    /// `handle_msg_replace_direct_addrs` (:529) and answers in the same turn. An actor that ends drops
    /// its inbox (and the queued message's oneshot, :372), so the send or the reply resolves with an
    /// error and this returns. The wait is at most the actor's turns over the messages queued ahead
    /// of this one, none of which awaits the exited peer.
    async fn fence_exited(&self, record: &Node) {
        if let Some(incarnation) = record.incarnation_id.as_ref() {
            self.node_rpc.resolver.retire_birth(&record.node_id, incarnation);
        }
        if let (Some(ep), Some(key)) = (&self.endpoint, record.endpoint_id.as_ref().and_then(|k| k.0.parse::<iroh::PublicKey>().ok())) {
            ep.replace_direct_addrs(key, []).await;
            tracing::info_span!("rdm.node_admin.node.update.via-exit-paths-retired", node = %record.name, node_id = %record.node_id, endpoint = %key.fmt_short(), kind = "recovery", reason = "proven-exit")
                .in_scope(|| tracing::info!("the exited birth's direct paths are retired before the new birth with its key is launched"));
        }
    }

    /// The birth `from` at `node` to replace, and its exact runtime: the view's, else (a birth the
    /// view no longer holds, or a replacement an earlier attempt began) the durable records the
    /// Build and the data root hold.
    async fn predecessor_of(&self, build_id: &crate::build::BuildId, node: &PathName, from: &IncarnationId) -> Result<(Node, DeploymentHandle), String> {
        let seen = self.topology.read().await.node(node).cloned().filter(|n| n.incarnation_id.as_ref() == Some(from));
        match seen {
            Some(n) => self.handle_for(&n).await,
            None => self.rebuild_absent_birth(build_id, node).await,
        }
    }

    /// What follows a birth's proven exit, for a removal and a replacement alike: the resolver names
    /// no current birth for it, and its direct paths are retired. A later birth that keeps the key
    /// binds a port the operating system assigns, so this endpoint holds no path to the key until
    /// that birth reports one (R-I1's replacement, with no address left).
    fn after_exit(&self, record: &Node) {
        if let Some(incarnation) = record.incarnation_id.as_ref() {
            let rpc = &self.node_rpc;
            // The exit is proven: the resolver names no current birth for the node until its
            // successor is applied, so no dial, probe or install aims at the dead birth's socket.
            rpc.resolver.retire_birth(&record.node_id, incarnation);
        }
        // The exit is proven: the exited birth's direct paths are retired. Until now they stayed
        // open for the typed drain.
        if let (Some(ep), Some(key)) = (&self.endpoint, record.endpoint_id.as_ref().and_then(|k| k.0.parse::<iroh::PublicKey>().ok())) {
            // Not awaited: the endpoint's actor for this key answers when it is free, and the
            // retire does not wait for a dead peer's actor (the same call membership spawns).
            let ep = ep.clone();
            tokio::spawn(async move { ep.replace_direct_addrs(key, []).await });
            tracing::info_span!("rdm.node_admin.node.update.via-exit-paths-retired", node = %record.name, node_id = %record.node_id, endpoint = %key.fmt_short(), kind = "removal", reason = "proven-exit")
                .in_scope(|| tracing::info!("the exited birth's direct paths are retired: nothing aims at its old socket"));
        }
    }

    /// `node.connections.delete:<path>` / `stop-node:<path>` (node-connections-delete.md, node-stop.md):
    /// the exact birth `from` at `node`, targeted by incarnation. A node.connections.delete leaves the
    /// process running; a stop leaves it stopped, its process alive (node-stop.md: no runtime Exited
    /// receipt), and publishes no departure.
    async fn standalone(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &IncarnationId, stop: bool) -> Result<(), String> {
        let op = if stop { "stop-node" } else { "node.connections.delete" };
        let seen = self.topology.read().await.node(node).cloned();
        let (record, handle) = match seen {
            Some(n) => {
                if n.incarnation_id.as_ref() != Some(from) {
                    return Err(format!("{op}:{node} targets birth {}, the view holds {} at that path", from.as_str(), n.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or("none")));
                }
                self.handle_for(&n).await?
            }
            None => self.handles.lock().unwrap().get(node).cloned().filter(|(rec, _)| rec.incarnation_id.as_ref() == Some(from)).ok_or_else(|| format!("{op}:{node}: birth {} is not in this admin's view and no runtime handle of it is held", from.as_str()))?,
        };
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: record.clone(), handle, exit: None };
        if stop {
            // The process is parked, not exited: nothing is retired about its paths or its birth.
            self.pipeline(&template).stop(&req).await.map_err(|e| e.to_string())?;
        } else {
            self.pipeline(&template).delete_connections(&req).await.map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// `delete-node:<path>.old` (node-decommission.md): the parked birth `from` a replacement left at
    /// `node`, fenced by incarnation, ended with the provider's exact Exited proof and deleted
    /// (`DeploymentPipeline::delete_parked`); what follows its proven exit follows.
    async fn delete_parked(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &IncarnationId) -> Result<(), String> {
        let seen = self.topology.read().await.node(node).cloned();
        let (record, handle) = match seen {
            Some(n) if n.incarnation_id.as_ref() == Some(from) => self.handle_for(&n).await?,
            Some(n) => return Err(format!("delete-node:{node} is fenced to birth {}, the view holds {} at that path", from.as_str(), n.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or("none"))),
            None => {
                let launched = self.handles.lock().unwrap().get(node).cloned().filter(|(rec, _)| rec.incarnation_id.as_ref() == Some(from));
                match launched {
                    Some(h) => h,
                    None => {
                        let birth = self.parked_birth_by_incarnation(node, from).await?;
                        self.handle_for(&birth).await?
                    }
                }
            }
        };
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: record.clone(), handle, exit: None };
        self.pipeline(&template).delete_parked(&req).await.map_err(|e| e.to_string())?;
        self.after_exit(&record);
        self.handles.lock().unwrap().remove(node);
        Ok(())
    }

    /// The exact parked birth `from` that a delete of `node` (`<path>.old`) is fenced to, for an admin
    /// whose view does not name `node` and which did not run the replace (the mesh's primary changed
    /// since): the birth as its membership holds it parked, or else its durable runtime row (every
    /// admin keeps one per birth it heard, `runtime_rows_task`). The record is named `node`; its
    /// runtime is then adopted from the same sources by [`Self::handle_for`].
    async fn parked_birth_by_incarnation(&self, node: &PathName, from: &IncarnationId) -> Result<Node, String> {
        let mut birth = Node::allocated(node.clone());
        birth.incarnation_id = Some(from.clone());
        birth.parked = true;
        birth.status = NodeStatus::Leaving;
        if let Some(d) = self.book.parked().into_iter().find(|d| &d.node.incarnation == from) {
            birth.node_id = d.node.node_id.clone();
            birth.endpoint_id = Some(d.node.endpoint_id.clone());
            birth.transport_addr = Some(d.node.transport_addr);
            birth.data_dir = d.data_dir.clone();
            return Ok(birth);
        }
        let Some(store) = self.records.contacts.get() else {
            return Err(format!("delete-node:{node}: birth {} is not in this admin's view or membership, and this admin has no nodes store to read its runtime row from", from.as_str()));
        };
        let rows = store.runtimes().await.map_err(|e| format!("delete-node:{node}: birth {} is not in this admin's view or membership, and its runtime rows could not be read: {e}", from.as_str()))?;
        let row = rows.into_iter().find(|r| &r.incarnation_id == from).ok_or_else(|| format!("delete-node:{node}: birth {} is not in this admin's view or membership, and this admin holds no runtime row of it", from.as_str()))?;
        birth.node_id = row.node_id;
        birth.data_dir = row.data_dir;
        Ok(birth)
    }

    /// What this admin holds for the start of the parked `record`: the deployment its `JoinNode` is
    /// verified against, as the birth was admitted.
    fn deployed_for(&self, record: &Node, handle: &DeploymentHandle) -> Result<crate::join::Deployed, String> {
        let incarnation = record.incarnation_id.clone().ok_or_else(|| format!("{} has no known birth", record.name))?;
        let admitted = self.book.parked().into_iter().find(|d| d.node.node_id == record.node_id && d.node.incarnation == incarnation);
        Ok(crate::join::Deployed {
            name: record.name.clone(),
            node_id: record.node_id.clone(),
            incarnation,
            supersedes: admitted.as_ref().and_then(|d| d.node.supersedes.clone()),
            endpoint_id: record.endpoint_id.clone().ok_or_else(|| format!("{} has no known endpoint id", record.name))?,
            runtime: handle.fact().ok_or_else(|| format!("{}: the handle names no exact runtime to verify its rejoin against", record.name))?,
            data_dir: record.data_dir.clone().ok_or_else(|| format!("{} has no known data dir", record.name))?,
            // The name this authority last accepted for this exact birth: its held stopped digest's (R-JN1).
            last_accepted_name: admitted.as_ref().map(|d| d.node.name.clone()),
            held: true,
        })
    }

    /// The parked birth `from` at `node`: its record and exact runtime, and what the rejoin is
    /// verified against.
    async fn parked_birth(&self, op: &str, node: &PathName, from: Option<&IncarnationId>) -> Result<(Node, DeploymentHandle, crate::join::Deployed), String> {
        let seen = self.topology.read().await.node(node).cloned().ok_or_else(|| format!("{op}:{node} is not in this admin's view"))?;
        if let Some(from) = from {
            if seen.incarnation_id.as_ref() != Some(from) {
                return Err(format!("{op}:{node} is fenced to birth {}, the view holds {} at that path", from.as_str(), seen.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or("none")));
            }
        }
        let (record, handle) = self.handle_for(&seen).await?;
        // The record the pipeline starts is the view's: it carries the endpoint, the address and the data dir the birth was admitted at.
        let record = Node { status: seen.status, parked: seen.parked, ..record };
        let record = Node { endpoint_id: seen.endpoint_id.clone().or(record.endpoint_id), transport_addr: seen.transport_addr.or(record.transport_addr), data_dir: seen.data_dir.clone().or(record.data_dir), ..record };
        let deployed = self.deployed_for(&record, &handle)?;
        Ok((record, handle, deployed))
    }

    /// `restart-node:<path>` (node-restart.md): stop, then start, on the same process: the same node id,
    /// incarnation, endpoint key and port. The process never exits and no runtime is deployed.
    async fn restart_in_place(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName) -> Result<(), String> {
        let (record, handle, deployed) = self.parked_birth_or_live("restart-node", node).await?;
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = crate::deployment::pipeline::StartRequest { build_id: build_id.clone(), attempt, node: record, handle, deployed };
        self.pipeline(&template).restart(&req).await.map_err(|e| e.to_string())
    }

    /// The live birth at `node` before its stop, for a restart: the same record shape as a parked one.
    async fn parked_birth_or_live(&self, op: &str, node: &PathName) -> Result<(Node, DeploymentHandle, crate::join::Deployed), String> {
        let seen = self.topology.read().await.node(node).cloned();
        let (record, handle) = match seen {
            Some(n) => self.handle_for(&n).await?,
            None => self.handles.lock().unwrap().get(node).cloned().ok_or_else(|| format!("{op}:{node} is not in this admin's view and no runtime handle of it is held"))?,
        };
        let seen = self.topology.read().await.node(node).cloned().unwrap_or_else(|| record.clone());
        let record = Node { endpoint_id: seen.endpoint_id.clone().or(record.endpoint_id), transport_addr: seen.transport_addr.or(record.transport_addr), data_dir: seen.data_dir.clone().or(record.data_dir), status: seen.status, ..record };
        let deployed = self.deployed_for_live(&record, &handle)?;
        Ok((record, handle, deployed))
    }

    /// [`Self::deployed_for`] for a birth still in the book: its admitted digest is the book's.
    fn deployed_for_live(&self, record: &Node, handle: &DeploymentHandle) -> Result<crate::join::Deployed, String> {
        let incarnation = record.incarnation_id.clone().ok_or_else(|| format!("{} has no known birth", record.name))?;
        let admitted = self.book.get(record.node_id.as_str()).map(|(d, _)| d).filter(|d| d.node.incarnation == incarnation);
        Ok(crate::join::Deployed {
            name: record.name.clone(),
            node_id: record.node_id.clone(),
            incarnation,
            supersedes: admitted.as_ref().and_then(|d| d.node.supersedes.clone()),
            endpoint_id: record.endpoint_id.clone().ok_or_else(|| format!("{} has no known endpoint id", record.name))?,
            runtime: handle.fact().ok_or_else(|| format!("{}: the handle names no exact runtime to verify its rejoin against", record.name))?,
            data_dir: record.data_dir.clone().or_else(|| admitted.as_ref().and_then(|d| d.data_dir.clone())).ok_or_else(|| format!("{} has no known data dir", record.name))?,
            last_accepted_name: admitted.as_ref().map(|d| d.node.name.clone()),
            held: true,
        })
    }

    /// `start-node:<path>` (node-start.md): the parked birth `from` rejoins as itself.
    async fn start_parked(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, from: &IncarnationId) -> Result<(), String> {
        let (record, handle, deployed) = self.parked_birth("start-node", node, Some(from)).await?;
        let template = self.template_for(node.kind, &node.mesh).await?;
        let req = crate::deployment::pipeline::StartRequest { build_id: build_id.clone(), attempt, node: record, handle, deployed };
        self.pipeline(&template).start(&req).await.map_err(|e| e.to_string())
    }

    /// The target of a retire the view cannot name: the accepted Build's `Replace` action names the
    /// exact birth, and the one data dir recorded for the path under this provider's data root
    /// names its node and its exact runtime. Refused by name when no record names that birth; a
    /// stored contact of the node, when this admin holds one, must agree on the incarnation.
    async fn rebuild_absent_birth(&self, build_id: &crate::build::BuildId, path: &PathName) -> Result<(Node, DeploymentHandle), String> {
        use tracing::Instrument;
        let span = tracing::info_span!("rdm.node_admin.node.update.via-rebuilt-birth", build_id = %build_id, node = %path, outcome = tracing::field::Empty);
        self.rebuild_absent_birth_in(build_id, path).instrument(span).await
    }

    async fn rebuild_absent_birth_in(&self, build_id: &crate::build::BuildId, path: &PathName) -> Result<(Node, DeploymentHandle), String> {
        let span = tracing::Span::current();
        let refuse = |why: String| {
            span.record("outcome", "refused");
            format!("{path} is not in this admin's view and no durable record names the birth to retire: {why}")
        };
        let build = self.builds.read_build(build_id).await.map_err(|e| refuse(format!("the Build {build_id} could not be read: {e}")))?;
        let from = match &build.action {
            Some(crate::accepted::AttemptAction::Replace { path: p, from_incarnation }) if p == path => from_incarnation.clone(),
            other => return Err(refuse(format!("the Build's action is {other:?}, not a replace of this path"))),
        };
        // A replacement an earlier attempt began receipted the exact birth it replaces (a recovery's
        // NodeDeleting, the RenamePredecessor of either): its node id names the directory.
        let operation = format!("replace-node:{path}");
        let named: Option<NodeId> = build
            .steps
            .iter()
            .filter(|s| s.operation == operation && matches!(s.step.as_str(), "NodeDeleting" | "RenamePredecessor") && s.outcome == crate::build_state::StepOutcome::Complete)
            .filter_map(|s| s.output.clone())
            .filter_map(|v| serde_json::from_value::<rafka_mesh_entity::LifecycleOp>(v).ok())
            .find(|op| op.incarnation == from)
            .map(|op| op.node_id);
        let (dir, node_id) = match named {
            Some(id) => {
                let dir = self.template.data_root.join(format!("{path}-{id}"));
                let look = dir.clone();
                let exists = crate::record_store::blocking_io(move || look.try_exists()).await.map_err(|e| refuse(format!("{}: {e}", dir.display())))?;
                if !exists {
                    return Err(refuse(format!("{} does not exist", dir.display())));
                }
                (dir, id)
            }
            None => {
                let prefix = format!("{path}-");
                let dirs = crate::fence::recorded_dirs(&self.template.data_root, path).await.map_err(refuse)?;
                let dir = match dirs.as_slice() {
                    [one] => one.clone(),
                    [] => return Err(refuse(format!("no data dir {prefix}* under {}", self.template.data_root.display()))),
                    many => return Err(refuse(format!("{} data dirs match {prefix}*: {many:?}", many.len()))),
                };
                let node_id = dir
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.strip_prefix(&prefix))
                    .and_then(|id| NodeId::parse(id).ok())
                    .ok_or_else(|| refuse(format!("{} does not end in a node id", dir.display())))?;
                (dir, node_id)
            }
        };
        let fact = match crate::fence::runtime_record(&dir).await.map_err(refuse)? {
            Some(f) => f,
            None => return Err(refuse(format!("{} holds no runtime record", dir.display()))),
        };
        if let Some(store) = self.records.contacts.get() {
            if let Some(c) = store.contacts().await.ok().and_then(|rows| rows.into_iter().find(|c| c.node_id == node_id)) {
                if c.incarnation_id != from {
                    return Err(refuse(format!("this admin's stored contact holds incarnation {}, the Build names {}", c.incarnation_id.as_str(), from.as_str())));
                }
            }
        }
        let handle = crate::deployment::provider::adopt(&*self.provider, &fact).map_err(|r| refuse(format!("the runtime record is not adoptable: {r}")))?;
        let mut record = Node::allocated(path.clone());
        record.node_id = node_id;
        record.incarnation_id = Some(from.clone());
        record.deployment_id = Some(handle.deployment_id.clone());
        record.data_dir = Some(dir.display().to_string());
        span.record("outcome", "rebuilt");
        tracing::info!(node = %path, incarnation_id = %from.as_str(), deployment_id = %fact.deployment_id, "the birth to retire is rebuilt from the Build's replace action and its exact runtime record");
        Ok((record, handle))
    }

    async fn retire(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName) -> Result<Option<Node>, String> {
        let seen = self.topology.read().await.node(node).cloned();
        let (record, handle) = match &seen {
            Some(n) => self.handle_for(n).await?,
            None => {
                let held = self.handles.lock().unwrap().get(node).cloned();
                match held {
                    Some(h) => h,
                    // Silent past every window the view keeps: the Build names the birth, the
                    // durable runtime record names its exact runtime (node-admin-lifecycle.md 4.2).
                    None => self.rebuild_absent_birth(build_id, node).await?,
                }
            }
        };
        // The record handed back with the runtime is the one this admin deployed; whether the birth
        // is stopped (parked) is the view's, as `parked_birth` takes it: a stopped birth's delete
        // goes straight to its exact runtime's exit proof.
        let record = match &seen {
            Some(n) => Node { parked: n.parked, ..record },
            None => record,
        };
        self.delete_birth(build_id, attempt, record.clone(), handle).await?;
        Ok(Some(record))
    }

    /// node.delete of the live or stopped exact birth `record` (node-delete.md): the ordinary
    /// workflow (`DeploymentPipeline::retire`: node.stop for a live process, the provider's exact
    /// exit proof, its row Deleted, NodeDeleted) over its exact runtime `handle`; then its paths are
    /// retired and this admin's handle of it dropped. The one composition a removal, a mesh
    /// delete's member and a deleted mesh's last admin all call. Answers the terminal receipt.
    pub(crate) async fn delete_birth(&self, build_id: &crate::build::BuildId, attempt: u32, record: Node, handle: DeploymentHandle) -> Result<crate::deployment::pipeline::TerminalReceipt, String> {
        let template = self.template_for(record.kind, &record.mesh).await?;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: record.clone(), handle, exit: None };
        let terminal = self.pipeline(&template).retire(&req).await.map_err(|e| e.to_string())?;
        self.after_exit(&record);
        self.handles.lock().unwrap().remove(&record.name);
        Ok(terminal)
    }

    /// Fabric shutdown: stop every runtime this admin started, all at once;
    /// the fabric primary also stops every other live node of the fabric
    /// (adopting the ones another admin launched).
    pub async fn stop_all(&self) {
        let mut all: Vec<(String, DeploymentHandle)> = self.handles.lock().unwrap().iter().map(|(k, (_, h))| (k.to_string(), h.clone())).collect();
        let view = self.topology.read().await.clone();
        if view.holds_fabric_seat(&self.node_id) {
            for n in view.members().filter(|n| n.node_id != self.node_id && n.status.is_live()) {
                if all.iter().any(|(k, _)| *k == n.name.to_string()) {
                    continue;
                }
                match self.handle_for(n).await {
                    Ok((_, h)) => all.push((n.name.to_string(), h)),
                    Err(e) => tracing::info!(node = %n.name, error = %e, "a live node of the fabric cannot be stopped from here"),
                }
            }
        }
        // And whatever the provider started that no create recorded (a create
        // that failed after DeployRuntime).
        for h in self.provider.launched() {
            if !all.iter().any(|(_, k)| k.pid == h.pid && k.container == h.container) {
                all.push((format!("unrecorded deployment {}", h.deployment_id), h));
            }
        }
        let stops = all.into_iter().map(|(name, h)| async move {
            if let Err(e) = self.provider.terminate(&h, TerminationMode::Graceful { grace: Duration::from_secs(8) }).await {
                tracing::info!(node = %name, error = %e, "stopping a runtime at shutdown failed");
            }
        });
        futures_util::future::join_all(stops).await;
    }
}

#[async_trait::async_trait]
impl crate::fence::PathProbe for AdminRunner {
    async fn answers(&self, node: &Node) -> bool {
        AdminRunner::answers(self, node).await
    }
    async fn inspect(&self, node: &Node) -> Result<crate::deployment::provider::DeploymentStatus, String> {
        let (_, h) = self.handle_for(node).await?;
        Ok(self.provider.inspect(&h).await)
    }
    async fn recorded(&self, path: &PathName) -> Result<Vec<(String, crate::deployment::provider::DeploymentStatus)>, String> {
        let mut out = Vec::new();
        for dir in crate::fence::recorded_dirs(&self.template.data_root, path).await? {
            // A data dir that holds no runtime record holds no runtime.
            let Some(fact) = crate::fence::runtime_record(&dir).await? else { continue };
            // A record of another provider or control domain names no runtime this provider runs.
            let Ok(h) = crate::deployment::provider::adopt(&*self.provider, &fact) else { continue };
            out.push((dir.display().to_string(), self.provider.inspect(&h).await));
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl OperationRunner for AdminRunner {
    async fn run(&self, build_id: &crate::build::BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), crate::executor::RunEnd> {
        let r = self.run_op(build_id, lane, attempt, op).await;
        self.refresh_view().await;
        r
    }
}

impl AdminRunner {
    async fn run_op(&self, build_id: &crate::build::BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), crate::executor::RunEnd> {
        use tracing::Instrument;
        // The workflows write the attempt's receipts and read its Build through the lane's Build
        // state (R-BI7): a drift attempt's in its mesh's lane, a fabric attempt's in the fabric lane.
        let builds = crate::build_run::LaneBuilds::of(&self.builds, lane);
        if let Some(mesh) = lane {
            // A mesh's drift lane carries a node recovery's attempts only (its open is
            // `ControlPlane::open_recovery`'s): any other operation is not one this lane runs.
            if !matches!(op, BuildOperation::RetireNode { exited: Some(_), .. } | BuildOperation::CreateNode { over: Some(_), .. }) {
                return Err(crate::executor::RunEnd::Failed(format!("{}: {} is not an operation of a node recovery, the only work mesh {mesh}'s drift lane runs (attempt {attempt} of {build_id})", self.me, op.key())));
            }
        }
        let ended: Result<(), String> = match op {
            BuildOperation::CreateMesh { .. } => create_mesh_step(&self.records, &*self.mesh_storage, &*self.builds, &self.template.executor(), &self.me.to_string(), build_id, attempt, op).await,
            BuildOperation::CreateNode { node, over: Some(from) } => {
                // node-recovery.md names the create's span `node.create.via-build`.
                let span = tracing::info_span!("rdm.node_admin.node.create.via-build", build_id = %build_id, node = %node, attempt, node_id = %from.node_id, incarnation_id = %from.incarnation.as_str());
                self.create_over(&*builds, build_id, attempt, node, from).instrument(span).await
            }
            BuildOperation::CreateNode { node, over: None } => {
                let span = tracing::info_span!("rdm.node_admin.node.create.via-build", build_id = %build_id, node = %node, attempt);
                async {
                    // A mesh's first admin (a new mesh, or a mesh whose admins were all lost): once it
                    // has joined its mesh, the fabric primary applies MeshStatus::Pending at that exact
                    // birth, before it is asked to be Ready; only with that certainty does the shape
                    // proceed (e4.s11).
                    let first_admin = node.kind == NodeKind::NodeAdmin && self.topology.read().await.cohort(&node.mesh, NodeKind::NodeAdmin).all(|n| !n.status.is_live());
                    let before_ready = if first_admin { Some(self.mesh_state_hook(node, rafka_node_rpc_contract::status::MeshState::Pending)?) } else { None };
                    self.create(build_id, attempt, node, before_ready).await
                }
                .instrument(span)
                .await
            }
            BuildOperation::RestartNode { node } => {
                // node-restart.md: the live process parks and rejoins; there is no relaunch. A member
                // proven exited is node recovery's (node-recovery.md), a node-admin the drift's replace.
                let span = tracing::info_span!("rdm.node_admin.node.update.via-build", build_id = %build_id, node = %node, attempt);
                self.restart_in_place(build_id, attempt, node).instrument(span).await
            }
            BuildOperation::StartNode { node, from_incarnation } => {
                let span = tracing::info_span!("rdm.node_admin.node.update.via-build", build_id = %build_id, node = %node, attempt);
                self.start_parked(build_id, attempt, node, from_incarnation).instrument(span).await
            }
            BuildOperation::ReplaceNode { node, from_incarnation } => {
                // node-replace.md names the operation's span `node.update.via-build`.
                let span = tracing::info_span!("rdm.node_admin.node.update.via-build", build_id = %build_id, node = %node, attempt);
                self.replace(build_id, attempt, node, from_incarnation).instrument(span).await
            }
            BuildOperation::DeleteConnections { node, from_incarnation } => {
                let span = tracing::info_span!("rdm.node_admin.node.update.via-build", build_id = %build_id, node = %node, attempt);
                self.standalone(build_id, attempt, node, from_incarnation, false).instrument(span).await
            }
            BuildOperation::StopNode { node, from_incarnation } => {
                let span = tracing::info_span!("rdm.node_admin.node.update.via-build", build_id = %build_id, node = %node, attempt);
                self.standalone(build_id, attempt, node, from_incarnation, true).instrument(span).await
            }
            BuildOperation::RetireNode { node, exited: Some(from) } => {
                let span = tracing::info_span!("rdm.node_admin.node.delete.via-build", build_id = %build_id, node = %node, attempt, node_id = %from.node_id, incarnation_id = %from.incarnation.as_str());
                self.delete_exited(&*builds, build_id, attempt, node, from).instrument(span).await
            }
            BuildOperation::DeleteNode { node, from_incarnation } => {
                let span = tracing::info_span!("rdm.node_admin.node.delete.via-build", build_id = %build_id, node = %node, attempt);
                self.delete_parked(build_id, attempt, node, from_incarnation).instrument(span).await
            }
            BuildOperation::RetireNode { node, exited: None } => {
                let span = tracing::info_span!("rdm.node_admin.node.delete.via-build", build_id = %build_id, node = %node, attempt);
                self.retire(build_id, attempt, node).instrument(span).await.map(|_| ())
            }
            BuildOperation::ShutdownMesh { mesh, mesh_id } => return self.shutdown_mesh(build_id, attempt, mesh, mesh_id).await,
        };
        ended.map_err(crate::executor::RunEnd::Failed)
    }
}

/// `ApplyMeshState(Pending)` at `target` (a mesh's first admin, just joined), from `me` (born as `me_id`).
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
pub async fn apply_mesh_state(client: &rafka_node_rpc::NodeRpcClient, resolver: &rafka_node_rpc::LiveNodeResolver, me: &PathName, me_id: &NodeId, topology: &Arc<RwLock<Topology>>, mesh: &str, mesh_id: &str, state: rafka_node_rpc_contract::status::MeshState, target: &Node) -> Result<(), String> {
    use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
    use rafka_node_rpc_contract::status::{MeshState, Status, StatusReply, StatusRequest};
    // The target is not heard yet (a peer mesh's admin reaches the backbone only as its mesh's
    // primary): this admin launched it and holds its birth exactly, so its resolver takes the
    // birth from what it launched, never from a digest it has not heard.
    let (Some(endpoint_id), Some(transport_addr), Some(incarnation)) = (target.endpoint_id.as_ref(), target.transport_addr, target.incarnation_id.clone()) else {
        return Err(format!("{}: the birth just launched has no endpoint id, transport address or incarnation to reach it by", target.name));
    };
    let endpoint_id = endpoint_id.0.parse::<iroh::PublicKey>().map_err(|e| format!("{}: its endpoint id is not an iroh key: {e}", target.name))?;
    let fed = resolver.apply(rafka_node_rpc::ResolvedNode { node_id: target.node_id.clone(), name: target.name.clone(), endpoint_id, transport_addr, incarnation }, None);
    tracing::info!(target = %target.name, applied = ?fed, "the resolver holds the launched birth for the mesh-state hand-off");
    let (me_id, me_inc) = {
        let v = topology.read().await;
        (me_id.to_string(), v.own(me_id).and_then(|n| n.incarnation_id.clone()).map(|i| i.to_string()).unwrap_or_default())
    };
    let request = StatusRequest::ApplyMeshState {
        mesh_id: crate::model::MeshId::parse(mesh_id).map_err(|e| format!("{mesh}: its mesh id {mesh_id:?} is not a MeshId: {e}"))?,
        mesh_name: mesh.to_string(),
        state,
    };
    let key = format!("mesh:{mesh_id}:{state:?}");
    // A bounded reachability wait on the exact launched birth, a Ping and no Status: the target
    // answers `Pong` once its router serves with its authority filled (until then every op,
    // ping included, is a typed NotReady). Then exactly one Status call.
    let until = std::time::Instant::now() + Duration::from_secs(20);
    let started = std::time::Instant::now();
    let mut pings: u32 = 0;
    let reach = loop {
        pings += 1;
        let (out, _) = client
            .call::<Ping>(&rafka_node_rpc::NodeTarget::ExactNode(target.node_id.clone()), &PingRequest::Ping { payload: Vec::new() }, &rafka_node_rpc::CallOptions::default())
            .await;
        let answered = matches!(&out, rafka_node_rpc_contract::outcome::RpcOutcome::Reply(r) if matches!(r.value(), PingReply::Pong { .. }));
        let seen = match &out {
            rafka_node_rpc_contract::outcome::RpcOutcome::Reply(r) => format!("{:?}", r.value()),
            other => format!("{}: {other:?}", other.name()),
        };
        if answered {
            break Ok(seen);
        }
        if std::time::Instant::now() >= until {
            break Err(seen);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    tracing::info_span!(
        "rdm.node_admin.mesh.update.via-pending-reachability",
        node = %me,
        target = %target.name,
        target_node_id = %target.node_id,
        mesh = %mesh,
        pings,
        waited_ms = started.elapsed().as_millis() as u64,
        reachable = reach.is_ok(),
        last = %match &reach { Ok(s) | Err(s) => s.as_str() },
    )
    .in_scope(|| tracing::info!("the launched birth was waited for before the mesh-state hand-off"));
    if let Err(last) = reach {
        return Err(format!("{}: unreachable for 20 s before the {state:?} hand-off ({pings} pings, last: {last}); nothing was sent, and Build(shape) does not start under an assumed state", target.name));
    }
    // The one call. Applied and AlreadyApplied succeed; every other outcome fails the step typed,
    // and the rectifier is the retry.
    let (out, _) = client.call::<Status>(&rafka_node_rpc::NodeTarget::ExactNode(target.node_id.clone()), &request, &rafka_node_rpc::CallOptions::default()).await;
    let (outcome, verdict) = match &out {
        rafka_node_rpc_contract::outcome::RpcOutcome::Reply(r) => match r.value() {
            StatusReply::Applied => ("applied".to_string(), Ok(())),
            StatusReply::AlreadyApplied => ("already-applied".to_string(), Ok(())),
            StatusReply::RejectedStaleMesh { held } => (format!("rejected-stale-mesh: the target holds mesh id {held}"), Err(format!("{} holds mesh id {held}, not {mesh_id}: no replacement id is minted; the {state:?} hand-off is refused", target.name))),
            StatusReply::RejectedInvalidMeshTransition { current } => (format!("rejected-invalid-mesh-transition: {current:?}"), Err(format!("{} already holds its Mesh at {current:?}: {state:?} is a backward move", target.name))),
            other => (format!("{other:?}"), Err(format!("{}: the {state:?} hand-off was answered {other:?}; {state:?} is not certain and Build(shape) does not start under an assumed state", target.name))),
        },
        other => (format!("{}: {other:?}", other.name()), Err(format!("{}: the {state:?} hand-off call ended {}: {state:?} is not certain and Build(shape) does not start under an assumed state ({other:?})", target.name, other.name()))),
    };
    // A new mesh's bootstrap admin is handed Pending; the admin created for an existing mesh is
    // handed the mesh's current state (states.md: a mesh's state only moves forward).
    macro_rules! handoff_span {
        ($name:literal) => {
            tracing::info_span!(
                $name,
                node = %me,
                node_id = %me_id,
                incarnation = %me_inc,
                target = %target.name,
                target_node_id = %target.node_id,
                target_incarnation = %target.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default(),
                mesh = %mesh,
                mesh_id = %mesh_id,
                key = %key,
                state = ?state,
                attempt = 1u32,
                outcome = %outcome,
            )
        };
    }
    let span = if state == MeshState::Pending { handoff_span!("rdm.node_admin.mesh.update.via-pending-handoff") } else { handoff_span!("rdm.node_admin.mesh.update.via-state-handoff") };
    span.in_scope(|| tracing::info!("the fabric primary applies its mesh's state at the mesh's first admin"));
    verdict
}

/// The drain's view of this admin: its projection, and its provider through each runtime's
/// RuntimeFact.
struct AdminStopper {
    runner: Arc<AdminRunner>,
    topology: Arc<tokio::sync::RwLock<Topology>>,
}

#[async_trait::async_trait]
impl crate::shutdown::Stopper for AdminStopper {
    async fn view(&self) -> Topology {
        self.topology.read().await.clone()
    }
    async fn stop(&self, node: &Node) -> Result<(), String> {
        let (_, handle) = self.runner.handle_for(node).await?;
        self.runner
            .provider
            .terminate(&handle, TerminationMode::Graceful { grace: Duration::from_secs(8) })
            .await
            .map_err(|e| format!("terminating {}: {e}", node.name))
    }
}

/// A running node-admin: what `run` needs to keep alive and shut down.
pub struct Running {
    /// Rafka-time: the app's way to read it. The one reader every gossip stamp, lifecycle event and
    /// fleet judgement of this admin reads, already adopted when the admin has started. Cloneable;
    /// every clone reads the same instance. `now_ms()` is `max(reference + monotonic elapsed, last
    /// returned)`; an app that stamps its records with rafka-time (the OrderKey's
    /// `writer_timestamp_ms`) reads it here and never from the OS clock.
    pub rafka_time: rafka_mesh_transport::clock::RafkaTime,
    /// The base of this admin's control API.
    pub api_base: String,
    /// The control plane the API serves.
    pub control: Arc<ControlPlane>,
    /// The runner that realises Build operations.
    pub runner: Arc<AdminRunner>,
    /// The admin's membership.
    pub membership: Membership,
    /// The admin's own digest.
    pub digest: Arc<Mutex<MeshDigest>>,
    /// This process's one Node RPC client and live resolver: every Node RPC
    /// caller in the process takes it by clone.
    pub node_rpc: crate::node_rpc::ProcessNodeRpc,
    /// This admin's place on the backbone: while it leaves, its aggregate publication carries its
    /// own `Leaving` out of its mesh ([`Running::leave`]).
    backbone: Backbone,
    router: IrohRouter,
    /// This admin's Node RPC server: the leave waits for the replies it owes before it closes the transport.
    rpc_server: rafka_node_rpc::NodeRpcServer,
    /// The digest cadence, the Build executor, the hierarchy and the anti-entropy:
    /// stopped first on leave.
    publisher: tokio::task::JoinHandle<()>,
    executor: tokio::task::JoinHandle<()>,
    hierarchy: tokio::task::JoinHandle<()>,
    /// The Members anti-entropy: this admin's mesh aggregate on the backbone, on its own timer.
    anti_entropy: tokio::task::JoinHandle<()>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    /// The tasks node.start's steps after the hook started (the own-mesh sweep of a launched admin).
    start_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    /// The gate a stop parks (`node_self`'s `Session::cut`) and a start opens.
    gate: crate::park_gate::ParkGate,
    /// This admin as the subject of the drain, stop and start its mesh-admin commands.
    node_self: Arc<crate::node_self::NodeSelf>,
    /// Asked before each leave announcement publish (a testkit executable's wiring; none in the product).
    leave_seam: Option<Arc<dyn crate::wiring::LeaveSeam>>,
    /// This admin's Build log: fenced before the first `Leaving` (ruling R-A2).
    builds: Arc<FabricBuildStateAdapter>,
    /// Cancelled when this admin is retired (drain-node, stop-node, leaving): its hook and any pull
    /// it has in flight end.
    hydrate_cancel: rafka_node_rpc::Retirement,
    hydration: Arc<Mutex<Option<crate::app_hydration::HydrationHandle>>>,
    /// The member cert the launching admin's signer issued for this birth, as the opaque bytes the
    /// join that admitted it carried (empty when that admin is configured with no certs). `None`
    /// for a birth that made no JoinNode: the Day-0 root and a recovery start.
    pub member_cert: Option<Vec<u8>>,
}

impl Running {
    /// The birth this admin runs under: its digest's node, the birth its Node RPC door opens for.
    pub fn birth(&self) -> MeshNode {
        self.digest.lock().unwrap().node.clone()
    }

    /// Print what the process that started this admin reads on its stdout: the birth it runs under
    /// (`rafka_node_admin_client::BIRTH_LINE`), by which that process reaches it over Node RPC holding
    /// nothing else, then its control base.
    pub fn announce(&self) {
        println!("{}{}", rafka_node_admin_client::BIRTH_LINE, serde_json::to_string(&self.birth()).expect("a birth serializes"));
        println!("RDM_NODE_ADMIN_API_BASE={}", self.api_base);
    }

    /// The gate a stop parks and a start opens: every task this admin runs is held at its own wait
    /// while it is parked.
    pub fn gate(&self) -> &crate::park_gate::ParkGate {
        &self.gate
    }

    /// Whether this admin publishes its mesh's aggregate on the backbone: the role its hierarchy
    /// last decided (its mesh's primary, its view authorizing).
    pub fn publishes_its_mesh(&self) -> bool {
        self.backbone.is_mesh_primary()
    }

    /// This admin as the subject of `drain-node`, `stop-node` and `start-node`: what such a command
    /// naming it reaches once its Status authority has decided it (`status_rpc`'s `serve_command`).
    pub fn node_self(&self) -> Arc<crate::node_self::NodeSelf> {
        self.node_self.clone()
    }

    /// Where this admin's `hydrate_before_ready` stands; `None` when the app registered no hook.
    pub fn hydration(&self) -> Option<crate::app_hydration::HydrationHandle> {
        self.hydration.lock().unwrap().clone()
    }

    /// Resolves with the reason once this admin's hook failed; never resolves otherwise. The
    /// process selects on it beside its stop signal and ends by name.
    pub async fn hydration_failed(&self) -> String {
        let handle = self.hydration.lock().unwrap().clone();
        match handle {
            Some(h) => h.failed().await,
            None => std::future::pending().await,
        }
    }

    /// Stop executing and reconciling: no Build attempt and no drift
    /// recovery starts after this. A fabric shutdown does this first, so the
    /// runtimes it stops are not recovered as proven drift.
    pub fn stop_reconciling(&self) {
        self.executor.abort();
    }

    /// Leave the fabric the way every node does (node-rpc §35): stop taking
    /// Build work and say `Draining`, then keep saying `Leaving` for the
    /// leave linger (`RDM_LEAVE_LINGER_MS`), then close. iroh-gossip
    /// acknowledges nothing and closing drops what is unsent, so one
    /// announcement can be lost; an attempt the executor was running is
    /// continued by the Build's next attempt.
    pub async fn leave(self) {
        self.hydrate_cancel.cancel();
        self.leave_with(false).await
    }

    /// End a parked birth (a `stop-node` completed): it says nothing and closes. Its stop already
    /// cut its gossip neighbours and left its topics, and a stopped node gossips nothing
    /// (node-stop.md step 4): every node holds it Leaving and parked from the `NodeStopped` its
    /// stopped reply produced.
    pub async fn leave_after_stop(self) {
        self.hydrate_cancel.cancel();
        self.leave_with(true).await
    }

    async fn leave_with(self, parked: bool) {
        // A leave hands nothing on and yields no seat (R-SB1): a seat moves only by node.primary.update
        // or node.fabric-primary.update, whose committed row the seat watch hands the Build facts on at,
        // or by the provider's exit proof; a fabric shutdown freezes the drive.
        self.executor.abort();
        self.hierarchy.abort();
        self.anti_entropy.abort();
        self.publisher.abort();
        let say = |status: MemberStatus| {
            let mut d = self.digest.lock().unwrap().clone();
            d.status = status;
            *self.digest.lock().unwrap() = d.clone();
            d
        };
        // Every leg of the leave is its own span, so a stalled leave names the leg that stalled
        // (rafka-v2 #2942): the Draining say, each Leaving announcement on each channel, the
        // transport shutdown.
        let me = self.digest.lock().unwrap().node.name.to_string();
        if !parked {
            let started = std::time::Instant::now();
            let span = tracing::info_span!("rdm.mesh.node.update.via-leave-draining", node = %me, channel = "mesh", elapsed_ms = tracing::field::Empty, outcome = tracing::field::Empty);
            let r = self.membership.publish(&say(MemberStatus::Draining)).instrument(span.clone()).await;
            span.record("elapsed_ms", started.elapsed().as_millis() as u64);
            span.record("outcome", if r.is_ok() { "sent" } else { "refused" });
            span.in_scope(|| tracing::info!("said Draining on the mesh channel"));
        }
        // The hierarchy loop and the anti-entropy are stopped above, so the backbone roles stay as
        // they were when this admin began to leave: an admin primary keeps publishing its mesh's
        // members on the backbone through the linger, its own `Leaving` among them, so its self-authored departure
        // leaves the mesh by the path every member's does (Luke 2026-10-05). No other admin is a
        // publisher of this admin's membership.
        if !parked {
            let announcement = std::sync::atomic::AtomicU32::new(0);
            let (linger_started, waited) = (std::time::Instant::now(), rafka_mesh_transport::membership::runqueue_wait());
            let said = announce_leaving(leave_linger_from_env(), LEAVE_EVERY, || {
                let d = say(MemberStatus::Leaving);
                let n = announcement.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                let (m, bb, me, seam) = (self.membership.clone(), self.backbone.clone(), me.clone(), self.leave_seam.clone());
                async move {
                    let withheld = |channel: &'static str| seam.as_ref().is_some_and(|s| s.withholds(&me, n, channel));
                    let started = std::time::Instant::now();
                    let mesh_span = tracing::info_span!("rdm.mesh.node.update.via-leave-announcement", node = %me, announcement = n, channel = "mesh", elapsed_ms = tracing::field::Empty, outcome = tracing::field::Empty);
                    let outcome = if withheld("mesh") {
                        "withheld"
                    } else if m.publish(&d).instrument(mesh_span.clone()).await.is_ok() {
                        "sent"
                    } else {
                        "refused"
                    };
                    mesh_span.record("elapsed_ms", started.elapsed().as_millis() as u64);
                    mesh_span.record("outcome", outcome);
                    mesh_span.in_scope(|| tracing::info!("said Leaving on the mesh channel"));
                    let mesh = d.node.name.mesh.clone();
                    let started = std::time::Instant::now();
                    let view_span = tracing::info_span!("rdm.mesh.node.update.via-leave-announcement", node = %me, announcement = n, channel = "view", elapsed_ms = tracing::field::Empty, outcome = tracing::field::Empty);
                    let mine: Vec<MeshDigest> = view_span.in_scope(|| m.book.current(m.book.staleness_floor()).into_iter().filter(|x| x.node.name.mesh == mesh).collect());
                    view_span.record("elapsed_ms", started.elapsed().as_millis() as u64);
                    view_span.record("outcome", "read");
                    view_span.in_scope(|| tracing::info!(members = mine.len(), "read this mesh's members for the backbone frame"));
                    let started = std::time::Instant::now();
                    let bb_span = tracing::info_span!("rdm.mesh.node.update.via-leave-announcement", node = %me, announcement = n, channel = "backbone", publishing = bb.is_mesh_primary(), members = mine.len(), elapsed_ms = tracing::field::Empty, outcome = tracing::field::Empty);
                    let outcome = if withheld("backbone") {
                        "withheld"
                    } else if bb.publish(&m, mine).instrument(bb_span.clone()).await {
                        "sent"
                    } else {
                        "not-mesh-primary"
                    };
                    bb_span.record("elapsed_ms", started.elapsed().as_millis() as u64);
                    bb_span.record("outcome", outcome);
                    bb_span.in_scope(|| tracing::info!("said Leaving among this mesh's members on the backbone"));
                }
            })
            .await;
            tracing::info!(
                announcements = said,
                linger_ms = linger_started.elapsed().as_millis() as u64,
                runqueue_wait_ms = rafka_mesh_transport::membership::runqueue_wait_since_ms(waited),
                "said Leaving for the linger"
            );
        }
        let started = std::time::Instant::now();
        let of_start = self.start_tasks.lock().unwrap().drain(..).collect::<Vec<_>>();
        let aborted = self.tasks.len() + of_start.len();
        for t in self.tasks.into_iter().chain(of_start) {
            t.abort();
        }
        tracing::info_span!("rdm.mesh.node.update.via-leave-tasks", node = %me, aborted, elapsed_ms = started.elapsed().as_millis() as u64, outcome = "aborted").in_scope(|| tracing::info!("the node's tasks are aborted"));
        let started = std::time::Instant::now();
        let span = tracing::info_span!("rdm.mesh.node.update.via-leave-shutdown", node = %me, elapsed_ms = tracing::field::Empty, outcome = tracing::field::Empty);
        // Every call this node answered is settled with its caller before the transport closes:
        // a close takes an unacknowledged reply down with the connection.
        self.rpc_server.settled().instrument(span.clone()).await;
        let r = self.router.shutdown().instrument(span.clone()).await;
        span.record("elapsed_ms", started.elapsed().as_millis() as u64);
        span.record("outcome", if r.is_ok() { "closed" } else { "refused" });
        span.in_scope(|| tracing::info!("the transport closed"));
    }
}

/// What the provider runs a node in, for explicit executable bindings.
fn provider_image(p: ProviderKind) -> rafka_mesh_entity::binding::ProviderImage<'static> {
    match p {
        ProviderKind::Process => rafka_mesh_entity::binding::ProviderImage::Process,
        ProviderKind::Container => rafka_mesh_entity::binding::ProviderImage::Container(crate::deployment::container::RUNTIME_IMAGE),
    }
}

/// The Node RPC server a node-admin serves, before it seals: core (Ping, Forward), then the families the
/// admin owns as an authority (status, build claim, join).
pub fn rpc_server(
    connections: Arc<crate::connections_writer::ConnectionsWriter>,
    client: Arc<rafka_node_rpc::NodeRpcClient>,
    authority: crate::status_rpc::AuthoritySlot,
    claim: crate::build_claim::ClaimSlot,
    join: crate::join::JoinSlot,
) -> rafka_node_rpc::ServerBuilder {
    let core = rafka_node_rpc::ServerBuilder::new().serve_core(client, Some(connections));
    // Status is forwardable: a peer may carry an authority's status call through this admin.
    crate::join::serve(crate::build_claim::serve(crate::status_rpc::serve(core, authority), claim), join).carry::<rafka_node_rpc_contract::status::Status>()
}

/// The unreadable pre-event steps already named: each is named once, not on every fact.
type NamedSteps = HashSet<(String, u32, String, String)>;

/// Apply the lifecycle overlays `facts` name open to `book`: the restarts as the facts' truth
/// (`DigestBook::restarts_from_facts`: open while named open, closed only by its own `Complete`
/// receipt, its attempt's `Converged` verdict or its Build's forgetting), every other open operation
/// as a hold that its attempt's terminal verdict or its Build's forgetting closes
/// (`DigestBook::deletions_from_facts`), and every pre-event step this build cannot read refused by
/// name, once. Run inside the
/// log's append as each fact enters it ([`crate::build_state::LocalBuildLog::derive_on_every_fact`]).
fn overlays_from_facts(facts: &[crate::build_state::BuildFact], book: &DigestBook, named: &mut NamedSteps) {
    let builds = crate::build_state::fold(facts);
    let in_flight = crate::build_state::in_flight_ops(facts, &builds);
    book.restarts_from_facts(&in_flight.ops, &in_flight.restarts_completed, &in_flight.attempts_converged, &crate::build_state::forgotten_builds(facts, &builds));
    for op in in_flight.ops.into_iter().filter(|o| !o.is_restart()) {
        book.deleting(op);
    }
    book.deletions_from_facts(&in_flight.removals_ended, &crate::build_state::forgotten_builds(facts, &builds));
    for u in in_flight.unrecognised {
        if named.insert((u.build_id.to_string(), u.attempt, u.operation.clone(), u.step.clone())) {
            tracing::info_span!("rdm.node_admin.build.reject.via-unrecognised-step", build_id = %u.build_id, attempt = u.attempt, operation = %u.operation, step = %u.step, reason = %u.reason)
                .in_scope(|| tracing::warn!("a lifecycle pre-event step this build cannot read opens no overlay"));
        }
    }
}

/// Bring a node-admin up: identity, policy, membership, Build state, the
/// control API, the projection and the executor.
pub async fn start(cfg: AdminConfig) -> Result<Running, String> {
    // `start` carries no hook and no wrapper, so it is an embedding with no application work in state-sync.
    start_with(cfg, crate::wiring::Wiring { fabric_hooks: crate::wiring::FabricHooks::no_app_work(), ..crate::wiring::Wiring::no_certs() }).await
}

/// [`start`], with the decorators and hooks of `wiring` applied to the parts the admin is built from.
pub async fn start_with(cfg: AdminConfig, wiring: crate::wiring::Wiring) -> Result<Running, String> {
    // One boot trace per birth: this root is never entered (iroh's tasks must not inherit it), each
    // step is a child that lives exactly as long as the step, and `via-ready` closes it.
    let boot = tracing::info_span!(parent: None, "rdm.mesh.node.create.via-boot", node = tracing::field::Empty, node_id = tracing::field::Empty, incarnation_id = tracing::field::Empty, kind = "node-admin");
    let started = start_booted(cfg, wiring, boot.clone()).await;
    if let Err(e) = &started {
        boot.in_scope(|| tracing::error!(error = %e, "node-admin failed to come up"));
    }
    started
}

async fn start_booted(mut cfg: AdminConfig, mut wiring: crate::wiring::Wiring, boot: tracing::Span) -> Result<Running, String> {
    // Every task this admin runs is spawned behind this gate: a stop parks it, and a start opens it again.
    let park_gate = crate::park_gate::ParkGate::new();
    // The `sync_state` choice is made on purpose: an embedding with no application work says so
    // (`SyncState::NoAppWork`); one that registered nothing would skip its day 0 and seed unnoticed.
    if wiring.fabric_hooks.sync_state.is_none() {
        return Err("refusing to start: no `sync_state` choice was made; pass SyncState::NoAppWork for an embedding with no application work in state-sync, or the application's sync_state hook".into());
    }
    let fabric_hooks = wiring.fabric_hooks.clone();
    // The embedding's own round action, if it binds one: run after this admin publishes its
    // scratchpad at commit-state (bound once the store root is open, below).
    let embedding_round_actions = wiring.round_actions.take();
    // The Rafka-time this process composes: held by no one until the join (or the Day-0 root's
    // boot) adopts it. Every gossip stamp, lifecycle event and fleet judgement this admin makes
    // reads this one reader.
    let rafka_time = rafka_mesh_transport::clock::RafkaTime::unadopted();
    let clock: rafka_mesh_transport::clock::SharedClock = Arc::new(rafka_time.clone());
    // The app's signing capability, chosen explicitly: a node-admin started with no choice is
    // refused here, by name, before it touches anything.
    let signer = std::mem::take(&mut wiring.certs).resolve().map_err(|e| format!("{}: {e}", cfg.launch.as_ref().map(|l| l.name.to_string()).unwrap_or_else(|| "node-admin".into())))?;
    let issuer = Arc::new(crate::certs::CertIssuer::new(signer, rafka_time.clone()));
    let key_file = cfg.data_dir.join("node-key");
    let key_held = crate::record_store::blocking_io(move || key_file.try_exists()).await.map_err(|e| format!("{}: {e}", cfg.data_dir.join("node-key").display()))?;
    let step = if key_held {
        tracing::info_span!(parent: &boot, "rdm.mesh.node.resolve.via-identity-loaded")
    } else {
        tracing::info_span!(parent: &boot, "rdm.mesh.node.create.via-identity-minted")
    };
    let key = load_or_mint_key(&cfg.data_dir).await?;
    drop(step);
    let storage_step = tracing::info_span!(parent: &boot, "rdm.mesh.node.resolve.via-storage-opened", data_dir = %cfg.data_dir.display(), store_dir = %cfg.store_dir.display(), fabric_id = tracing::field::Empty, restart = tracing::field::Empty, writes_to = tracing::field::Empty, mesh_id = tracing::field::Empty, error = tracing::field::Empty);
    // The node-admin storage boundaries: its own row, the runtimes it started, its Build journal and
    // attempt contexts in its own data dir; the fabric, mesh, nodes, connections and status stores
    // under the fabric's shared store root (R14). An admin that finds its own row in nodes.storage
    // was here before: it restarts as the same logical node, in the same Mesh and Fabric, holding the
    // same Builds. Every store's temp files carry this process's incarnation, decided here, before
    // any store opens: the launch's, else a new one (a restart supersedes the one it last ran).
    let storage_err = |e: crate::record_store::StorageError| e.to_string();
    let incarnation = cfg.launch.as_ref().map(|l| l.incarnation.clone()).unwrap_or_else(IncarnationId::mint);
    if let (None, Ok(p)) = (&cfg.launch, FabricPolicy::bootstrap(cfg.spawn_type.as_deref())) {
        crate::store_root::qualify(p.provider).map_err(|e| format!("refusing to start: {e}"))?;
    }
    // The own row (data dir), read before the store root opens: a restarting admin's row names its
    // Fabric, whose root it opens, and its mesh, whose rows it opens on (R-FI1). A restart is never
    // handed its fabric id: a different one named beside the row is refused by name.
    let own_row = crate::storage::FileNodesStorage::own_in(&cfg.data_dir, &incarnation).await.map_err(storage_err)?;
    if let (None, Some(own)) = (&cfg.launch, &own_row) {
        if cfg.fabric_id_named && cfg.fabric_id != own.fabric_id {
            return Err(format!(
                "refusing to start: {}: this admin's own row ({}) belongs to Fabric {}, and {} names Fabric {}: a restart opens the Fabric of its own row",
                cfg.data_dir.display(),
                own.name,
                own.fabric_id,
                rafka_mesh_entity::launch::ENV_FABRIC_ID,
                cfg.fabric_id
            ));
        }
        cfg.fabric_id = own.fabric_id.clone();
    }
    storage_step.record("fabric_id", tracing::field::display(&cfg.fabric_id));
    storage_step.record("restart", cfg.launch.is_none() && own_row.is_some());
    let store_root = crate::store_root::StoreRoot::open(&cfg.store_dir, &cfg.fabric_id).map_err(|e| format!("refusing to start: {e}"))?;
    // Before the fabric's first state-commit every row this admin writes is held in its scratchpad;
    // its commit-state action publishes them (states.md, Rounds).
    storage_step.record("writes_to", store_root.writes_to());
    let round_actions: Arc<dyn crate::fabric_rounds::RoundActions> = Arc::new(crate::fabric_rounds::StorePublication::new(store_root.scratchpad().cloned(), embedding_round_actions));
    let fabric_files = Arc::new(crate::fabric_storage::FileFabricStorage::open(&store_root, &incarnation).await.map_err(storage_err)?);
    let fabric_storage: Arc<dyn crate::fabric_storage::FabricStorage> = crate::wiring::apply(wiring.fabric_storage.take(), fabric_files.clone());
    let mesh_files = Arc::new(crate::storage::FileMeshStorage::open(&store_root, &incarnation).await.map_err(storage_err)?);
    let mesh_storage: Arc<dyn crate::storage::MeshStorage> = mesh_files.clone();
    let own_mesh = match (&cfg.launch, &own_row) {
        (None, Some(own)) => own.name.mesh.clone(),
        _ => cfg.mesh.clone(),
    };
    let nodes_files = Arc::new(crate::storage::FileNodesStorage::open(&cfg.data_dir, &store_root, &own_mesh, &incarnation).await.map_err(storage_err)?);
    let nodes_storage: Arc<dyn crate::storage::NodesStorage> = nodes_files.clone();
    let journal = Arc::new(crate::build_state::FileJournal::open(&cfg.data_dir).await.map_err(|e| e.to_string())?);
    let attempt_contexts = Arc::new(crate::build_claim::AttemptContexts::open(&cfg.data_dir, &incarnation).await.map_err(storage_err)?);
    // Each mesh's in-flight row on the fabric's store root (R-BI7 caveat 3): the one set of rows the
    // claim door, the drive's release and the recovery door all take and release.
    let mesh_claims = Arc::new(crate::mesh_claims::MeshClaims::on_root(store_root.clone(), incarnation.clone()));
    let restart = match (&cfg.launch, own_row) {
        (None, Some(own)) => {
            let fabric = fabric_storage.fabric().await.map_err(storage_err)?.ok_or_else(|| {
                format!("{}: nodes.storage holds this admin ({}) but fabric.storage holds no Fabric record", cfg.data_dir.display(), own.name)
            })?;
            storage_step.record("mesh_id", tracing::field::display(&own.mesh_id));
            let mesh = match own_mesh_row(&*mesh_storage, &own).await {
                Ok(row) => row,
                Err(e) => {
                    let e = format!("refusing to start: {}: {e}", cfg.data_dir.display());
                    storage_step.record("error", e.as_str());
                    return Err(e);
                }
            };
            cfg.fabric = fabric.name.clone();
            cfg.fabric_id = fabric.fabric_id.clone();
            cfg.mesh = mesh.name.clone();
            cfg.mesh_id = Some(mesh.mesh_id.clone());
            Some(own)
        }
        _ => None,
    };
    if restart.is_none() {
        let holds = if fabric_storage.fabric().await.map_err(storage_err)?.is_some() {
            Some("a Fabric record")
        } else if !mesh_storage.meshes().await.map_err(storage_err)?.is_empty() {
            Some("a mesh record")
        } else if !nodes_storage.contacts().await.map_err(storage_err)?.is_empty() || !nodes_storage.runtimes().await.map_err(storage_err)?.is_empty() {
            Some("a durable topology")
        } else {
            None
        };
        // A node-admin a launcher starts is handed its rows before it runs (runtimes): a launch
        // owes no clean data dir.
        refuse_contradictory_start(&cfg, holds.filter(|_| cfg.launch.is_none())).map_err(|e| format!("refusing to start: {e}"))?;
    }
    drop(storage_step);
    // Identity and the two addresses: assigned by the launching pipeline, the bootstrap admin's own,
    // or (a restart) its own row, under a new incarnation that supersedes the one it last ran.
    // Its bootstrap contacts are the births it last heard, its own Mesh first: hints, any one of
    // which answering is enough.
    let (name, node_id, incarnation, supersedes, mesh_addr, control_addr, seeds) = match (&cfg.launch, &restart) {
        (Some(l), _) => {
            let control = cfg.api_bind;
            (l.name.clone(), l.node_id.clone(), incarnation.clone(), l.supersedes.clone(), l.bind_addr, control, l.seeds.clone())
        }
        (None, Some(own)) => {
            let addr_of = |s: &str| own.listeners.iter().find(|(n, _)| n == s).map(|(_, a)| *a);
            let mut contacts = nodes_storage.contacts().await.map_err(storage_err)?;
            contacts.retain(|c| c.node_id != own.node_id);
            contacts.sort_by_key(|c| (c.name.mesh != own.name.mesh, c.name.to_string()));
            let seeds = contacts.iter().filter_map(|c| c.gossip_addr().and_then(|a| a.ip_addrs().next().map(|ip| (a.id.to_string(), *ip)))).collect();
            tracing::info_span!(
                parent: &boot,
                "rdm.node_admin.node.update.via-restart",
                node = %own.name,
                node_id = %own.node_id,
                supersedes = %own.incarnation_id,
                fabric_id = %cfg.fabric_id,
                contacts = contacts.len(),
            )
            .in_scope(|| tracing::info!("restarting as the same logical node from nodes.storage"));
            (
                own.name.clone(),
                own.node_id.clone(),
                incarnation.clone(),
                Some(own.incarnation_id.clone()),
                // A restart reuses identity and data dir and binds a fresh port (the operating
                // system assigns it), never the one it last held: its peers learn the new address
                // from its digest.
                SocketAddr::new(own.transport_addr.ip(), 0),
                addr_of("control").unwrap_or(cfg.api_bind),
                seeds,
            )
        }
        (None, None) => (
            PathName::new(&cfg.mesh, NodeKind::NodeAdmin, 1),
            NodeId::mint(),
            incarnation.clone(),
            None,
            SocketAddr::new(cfg.api_bind.ip(), 0),
            cfg.api_bind,
            cfg.seeds.clone(),
        ),
    };

    // This node's own connections rows: `connections/<its path.name>/` of the store root (R14).
    let connections_storage: Arc<dyn crate::storage::ConnectionsStorage> =
        Arc::new(crate::storage::FileConnectionsStorage::open(&store_root, &name, &incarnation).await.map_err(storage_err)?);
    boot.record("node", tracing::field::display(&name));
    boot.record("node_id", tracing::field::display(&node_id));
    boot.record("incarnation_id", tracing::field::display(&incarnation.as_str()));
    // A container fabric's node-admin is a host process its containers reach only through the
    // fabric network's gateway (a host address, reachable from host processes too): the provider is
    // prepared before the mesh endpoint binds, and Day 0 binds it on the gateway. A launched admin
    // learns the policy from its launcher's entry answer, after it binds.
    // Explicit bindings are held to the provider's image before the provider does anything.
    if let (Some(b), None, Ok(p)) = (&cfg.bindings, &cfg.launch, FabricPolicy::bootstrap(cfg.spawn_type.as_deref())) {
        b.check_image(provider_image(p.provider)).map_err(|e| format!("refusing to start: explicit executable bindings: {e}"))?;
    }
    let early = match (&cfg.launch, FabricPolicy::bootstrap(cfg.spawn_type.as_deref())) {
        (None, Ok(p)) if p.provider == ProviderKind::Container => Some(crate::deployment::prepare(p, &cfg.fabric_id.to_string()).await.map_err(|e| e.to_string())?),
        _ => None,
    };
    // A host-process admin binds on the gateway; an admin that itself runs in a container of the
    // fabric binds on its own address there (its control address), the gateway not being local.
    let mesh_addr = match (&early, &cfg.launch, &restart) {
        (Some(p), None, None) if rafka_mesh_entity::runtime::this_container_id().is_none() => SocketAddr::new(p.admin_ip, 0),
        _ => mesh_addr,
    };

    // The mesh endpoint: gossip for membership and Build facts.
    // The transport is the one every node kind binds with (`rafka_node_rpc::endpoint::transport_config`).
    let alpns = vec![iroh_gossip::ALPN.to_vec(), rafka_node_rpc::ALPN.to_vec()];
    let step = tracing::info_span!(parent: &boot, "rdm.mesh.node.create.via-endpoint-bound", node = %name, requested = %mesh_addr, bound = tracing::field::Empty);
    let endpoint = rafka_node_rpc::endpoint::bind_exact(key.clone(), mesh_addr, alpns.clone(), rafka_node_rpc::endpoint::transport_config())
        .await
        .map_err(|e| format!("mesh address {mesh_addr}: {e}"))?;
    let mesh_addr = endpoint.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap_or(mesh_addr);
    step.record("bound", tracing::field::display(mesh_addr));
    drop(step);
    let step = tracing::info_span!(parent: &boot, "rdm.mesh.node.create.via-gossip-started", node = %name);
    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
    drop(step);
    // Entry: what this admin holds, for a node it launched (filled once the
    // admin's own digest exists; until then a join is told it is not ready).
    let entry: Arc<std::sync::OnceLock<EntryState>> = Arc::new(std::sync::OnceLock::new());
    // Node RPC on the admin's one endpoint: the status declarations it applies as an authority
    // (`status_rpc`). The authority is filled once this admin holds a view; until then NotReady.
    // This admin's own connections: hydrated from its storage, then kept by what its client
    // observes of its pooled connections (connections.md sections 4.2, 9 and 10).
    let connections = Arc::new(crate::connections_writer::ConnectionsWriter::new(
        rafka_mesh_entity::connections::ConnectionEnd { name: name.clone(), node_id: node_id.clone(), incarnation: Some(incarnation.clone()) },
        connections_storage.clone(),
        Arc::new(std::sync::Mutex::new(rafka_mesh_entity::connections::ConnectionsHeld::new())),
    ));
    match connections.hydrate().await {
        Ok(n) => tracing::info!(node = %name, rows = n, "connections hydrated from storage"),
        Err(e) => return Err(format!("connections storage: {e}")),
    }
    // An owed Proxy retirement whose write was refused is attempted again while owed
    // (connections.md §10); the task ends with the process.
    let _retirements = connections.spawn_retirement_reconciler(crate::connections_writer::RETIREMENT_RETRY);
    // The process's one live-node resolver: the client's and the server's (an accepted
    // connection's peer is named by it).
    let node_rpc_resolver = Arc::new(rafka_node_rpc::LiveNodeResolver::default());
    connections.held().lock().unwrap().set_membership(node_rpc_resolver.clone());
    let authority: crate::status_rpc::AuthoritySlot = Arc::new(std::sync::OnceLock::new());
    let claim_slot: crate::build_claim::ClaimSlot = Arc::new(std::sync::OnceLock::new());
    // The births this admin deployed and awaits a `JoinNode` from, and the door that answers it.
    let joins = Arc::new(crate::join::Joins::default());
    let join_slot: crate::join::JoinSlot = Arc::new(std::sync::OnceLock::new());
    let topology_slot: crate::topology_read::TopologySlot = Arc::new(std::sync::OnceLock::new());
    let build_facts_slot: crate::build_facts_read::BuildFactsSlot = Arc::new(std::sync::OnceLock::new());
    let build_slot: crate::build_op::BuildSlot = Arc::new(std::sync::OnceLock::new());
    let handover_slot: crate::fabric_handover::HandoverSlot = Arc::new(std::sync::OnceLock::new());
    // The process's one Node RPC client is made before the server: core Forward is one direct inner
    // call through it, and every other caller in the process takes it by clone.
    let node_rpc = crate::node_rpc::ProcessNodeRpc::new(node_rpc_resolver.clone(), endpoint.clone());
    // Closed until the Status authority is filled and its view holds this admin (it decides from
    // that view): the router refuses every op, ping included, with a typed NotReady before then.
    let rpc_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let rpc_builder = crate::build_op::serve(crate::fabric_handover::serve(crate::build_facts_read::serve(crate::topology_read::serve(rpc_server(connections.clone(), node_rpc.client.clone(), authority.clone(), claim_slot.clone(), join_slot.clone()), topology_slot.clone()), build_facts_slot.clone()), handover_slot.clone()), build_slot.clone());
    // The app's own ops (each behind the gate of its hook), before the catalog seals.
    let rpc_builder = match wiring.serve_app.take() {
        Some(serve_app) => serve_app(rpc_builder),
        None => rpc_builder,
    };
    let rpc_server = rpc_builder
        .with_ready_gate(rpc_ready.clone())
        .seal(rafka_node_rpc::ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .map_err(|e| format!("the admin's protocol catalog refused to seal: {e:?}"))?;
    let step = tracing::info_span!(parent: &boot, "rdm.mesh.node.add.via-alpn-registered", node = %name, alpns = "node-rpc,gossip");
    let iroh_router = IrohRouter::builder(endpoint.clone())
        .accept(iroh_gossip::ALPN, gossip.clone())
        .accept(rafka_node_rpc::ALPN, rpc_server.clone())
        .spawn();
    drop(step);
    tracing::info_span!(parent: &boot, "rdm.mesh.node.create.via-accept-loop-started", node = %name).in_scope(|| tracing::info!("the router accepts node-rpc and gossip connections"));
    let seed_addrs: Vec<EndpointAddr> = seeds
        .iter()
        .filter_map(|(k, a)| k.parse::<iroh::PublicKey>().ok().map(|pk| EndpointAddr::new(pk).with_ip_addr(*a)))
        .collect();
    if cfg.mesh_primary || cfg.fabric_primary {
        tracing::info_span!(
            "rdm.node_admin.node.update.via-recovery-start",
            node = %name,
            mesh = %cfg.mesh,
            mesh_primary = cfg.mesh_primary,
            fabric_primary = cfg.fabric_primary,
            seeds = seed_addrs.len(),
        )
        .in_scope(|| tracing::info!("started to recover: Ready is held until the topology and the current Build are held"));
    }
    // No crate of RDM serves a route or opens an HTTP listener (rdm-crate-and-test-layout.md §2): the
    // admin's application contact (`admin_api_base`, config, never checked) names this admin's node id.
    let _ = control_addr;
    let api_base = format!("rdm://{node_id}");
    // A launched admin takes the runtime record its launcher's pipeline made available; it is
    // part of the digest it reports at its join.
    let launched_runtime = match &cfg.launch {
        Some(_) => {
            let dir = cfg.data_dir.clone();
            Some(crate::node_self::wait_for_own_record(&dir, Duration::from_secs(10)).await?)
        }
        None => None,
    };
    // The mesh's id names its membership channel: the launch names it (the
    // launching admin always does), else the join answer's projection knows
    // it, else this admin is the mesh's first and mints it.
    // The process's one Node RPC client, built before the join: the join is its first call, and
    // the membership feed starts once the process holds a book.
    let launched_anchor = if restart.is_some() { None } else { cfg.launch.as_ref().and(seed_addrs.first().cloned()) };
    let launcher_of_launch = cfg.launch.as_ref().and_then(|l| l.launcher.clone());
    // The join's authority: the admin that deployed this one. A birth that makes no `JoinNode` (the
    // Day-0 root, a fabric-primary reborn from its durable map, a recovery start) has none, and says why.
    let joins_through = match (&launched_anchor, &launcher_of_launch, &launched_runtime) {
        (Some(anchor), Some(launcher), Some(runtime)) => Some((anchor.clone(), launcher.clone(), runtime.clone())),
        _ => None,
    };
    // This admin's first call after it binds: `JoinNode`, carrying its full digest with the
    // addresses it really bound, to the admin that deployed it.
    let join_digest: Option<MeshDigest> = joins_through.as_ref().map(|(_, _, runtime)| MeshDigest {
        fabric_id: cfg.fabric_id.clone(),
        node: MeshNode {
            node_id: node_id.clone(),
            name: name.clone(),
            endpoint_id: EndpointId(key.public().to_string()),
            transport_addr: mesh_addr,
            incarnation: incarnation.clone(),
            supersedes: supersedes.clone(),
            runtime: Some(runtime.clone()),
        },
        status: MemberStatus::Pending,
        node_state: Default::default(),
        admin_api_base: Some(api_base.clone()),
        emitted_at_rafka_ms: 0,
        digest_seq: 0,
        mesh_id: cfg.mesh_id.clone(),
        in_flight: None,
        config: BTreeMap::new(),
        load: None,
        gossip: None,
        data_dir: Some(cfg.data_dir.display().to_string()),
    });
    let joining = match &joins_through {
        Some((_, launcher, _)) => crate::workflows::node_start::Joining::Authority { label: launcher.name.to_string() },
        None if restart.is_some() => crate::workflows::node_start::Joining::None(crate::app_hydration::NoAuthority::FabricPrimaryReborn),
        None if cfg.mesh_primary || cfg.fabric_primary => crate::workflows::node_start::Joining::None(crate::app_hydration::NoAuthority::Recovery),
        None => crate::workflows::node_start::Joining::None(crate::app_hydration::NoAuthority::Day0Root),
    };
    let resolve: Box<dyn FnOnce() -> Result<crate::workflows::node_start::JoinTo, crate::node_self::StartFailed> + Send> = {
        let (resolver, through) = (node_rpc_resolver.clone(), joins_through.clone());
        Box::new(move || {
            let (anchor, launcher, _) = through.expect("a start that joins has its launching admin");
            resolver.apply(
                rafka_node_rpc::ResolvedNode {
                    node_id: launcher.node_id.clone(),
                    name: launcher.name.clone(),
                    endpoint_id: anchor.id,
                    transport_addr: anchor.ip_addrs().next().copied().ok_or_else(|| crate::node_self::StartFailed { step: "node.join", reason: format!("the launching admin {} has no address in this admin's seeds", launcher.name) })?,
                    incarnation: launcher.incarnation.clone(),
                },
                None,
            );
            Ok(crate::workflows::node_start::JoinTo::Launcher { target: rafka_node_rpc::NodeTarget::ExactNode(launcher.node_id.clone()), endpoint: anchor.id, name: launcher.name.clone() })
        })
    };
    // What a repeat of the control entry calls: the join to the launching admin, as first made.
    let entry_retrieval: Option<(rafka_node_rpc::NodeTarget, String, MeshDigest)> = match (&joins_through, &join_digest) {
        (Some((anchor, launcher, _)), Some(digest)) => Some((rafka_node_rpc::NodeTarget::ExactNode(launcher.node_id.clone()), anchor.id.fmt_short().to_string(), digest.clone())),
        _ => None,
    };
    // What the start brought up, once it did: handed to the caller.
    let out: Arc<Mutex<Option<Running>>> = Arc::default();
    let starting = crate::workflows::node_start::Starting { root: boot.clone(), name: name.to_string(), path: name.clone(), node_id: node_id.clone(), client: node_rpc.client.clone(), rafka_time: rafka_time.clone() };
    // The admin plugs into `node.start` by continuations: each step runs in the scope the previous
    // one left. Its channel step also hydrates its stores; its installed-topology step takes its
    // entry and builds what it serves and elects with; after its hook its Ready check reports.
    let enter: crate::workflows::node_start::EnterSeam<String> = {
        let out = out.clone();
        Box::new(move |step: tracing::Span, joined: Option<crate::wire::JoinAnswer>| -> crate::workflows::node_start::BoxFut<Result<(Membership, crate::workflows::node_start::JoinedSeam<String>), String>> {
            Box::pin(async move {
                // The member cert the launching admin's signer issued for this birth, carried by the join that
                // admitted it; none for a birth that made no JoinNode (the Day-0 root, a recovery start).
                let member_cert: Option<Vec<u8>> = joined.as_ref().map(|a| a.control.member_cert.clone());
                // The fabric's Day-0 root has no authority above it: it adopts its own OS clock as rafka-time,
                // once, here. A recovery start (a flag or a restart) takes it from the nodes it reaches below.
                if cfg.launch.is_none() && restart.is_none() && !cfg.mesh_primary && !cfg.fabric_primary {
                    crate::rafka_time::adopt_own_clock(&boot, &rafka_time, &name.to_string(), crate::rafka_time::OWN_CLOCK_DAY0_ROOT);
                }
                let mesh_id = match (&cfg.mesh_id, &joined) {
                    (Some(id), _) => id.clone(),
                    (None, Some(answer)) => return Err(format!("{name}: a launched admin takes its mesh's id from its launch, and this launch names none (joined {})", answer.served_by)),
                    (None, None) => MeshId::mint(),
                };
                // Subscribe first, pull second: what changes during the pull arrives by
                // gossip, and the book keeps the newer copy.
                step.record("mesh_id", tracing::field::display(&mesh_id));
                step.record("seeds", seed_addrs.len());
                let membership = Membership::join(&gossip, &endpoint, &cfg.fabric_id, &cfg.mesh, &mesh_id, &name.to_string(), clock.clone(), seed_addrs.clone())
                    .await
                    .map_err(|e| format!("membership: {e}"))?;
                // The name this admin holds is its membership's (R12): its connections rows name it as their
                // source from here on, whatever name the join hands it or an `ApplyNodeName` gives it.
                {
                    let (held, birth) = (membership.clone(), name.clone());
                    connections.follow_held_name(move || held.held_name(&birth))?;
                }
                // The row's name and config a launched admin's join handed: held before it publishes anything
                // (R-JN1); at a first start the name is its create spec's, verified by the join.
                let handed: Option<(Option<PathName>, Option<BTreeMap<String, String>>)> = joined.as_ref().map(|a| (a.control.name.clone(), a.control.config.clone()));
                if let Some((n, c)) = &handed {
                    if let Some(n) = n {
                        membership.set_name(n.clone());
                    }
                    if let Some(c) = c {
                        membership.set_config(c.clone());
                    }
                }
                drop(step);
                // An admin that made no `JoinNode` and is no Day-0 root (a restart, or a recovery started with
                // a seed) holds no rafka-time yet: it reads it from the first node it reaches, before it
                // publishes anything or judges any lease.
                if !rafka_time.is_adopted() {
                    // The admin that holds the fabric seat in its own seat records is the fabric's root: it
                    // adopts its own clock at once (fabric-node-lifecycle.md 2.5). Every other admin takes
                    // rafka-time from the first node it reaches that holds one.
                    let held = rafka_mesh_transport::membership::SeatBook::default();
                    crate::fabric_handover::hydrate_seats(&*fabric_storage, &held).await?;
                    let holds_fabric_seat = held.fabric().is_some_and(|h| h.node_id == node_id);
                    let mut contacts = Vec::new();
                    for r in nodes_storage.contacts().await.map_err(storage_err)?.into_iter().filter(|r| r.node_id != node_id) {
                        contacts.push(crate::reenter::MapNode { node_id: r.node_id, name: r.name, endpoint_id: r.endpoint_id, transport_addr: r.transport_addr, incarnation: r.incarnation_id, transition: None, ready: false, data_dir: None, admin_api_base: None });
                    }
                    if holds_fabric_seat {
                        crate::rafka_time::adopt_own_clock(&boot, &rafka_time, &name.to_string(), crate::rafka_time::OWN_CLOCK_FABRIC_SEAT);
                    } else if contacts.is_empty() && seed_addrs.is_empty() {
                        // The store names no node but this admin and it was handed no seed: there is no node to
                        // reach, so nothing is alive by the store's own evidence (fabric-recovery.md "Nothing
                        // alive"). It is the fabric's root and adopts its own clock at once (R-SB1).
                        crate::rafka_time::adopt_own_clock(&boot, &rafka_time, &name.to_string(), crate::rafka_time::OWN_CLOCK_NOTHING_ALIVE);
                    } else {
                        crate::rafka_time::adopt_from_reachable(&rafka_time, &name, &node_rpc.client, &node_rpc_resolver, &membership, contacts, crate::rafka_time::RECOVERY_PULL_WITHIN, &boot)
                            .await
                            .map_err(|e| format!("refusing to start: {e}"))?;
                    }
                }
                let backbone = Backbone::join(&gossip, &endpoint, &membership, &cfg.mesh, &name.to_string(), incarnation.clone(), seed_addrs.clone())
                    .await
                    .map_err(|e| format!("backbone: {e}"))?;
                if let Some(seam) = wiring.announcement_seam.take() {
                    let me = name.to_string();
                    backbone.withhold_announcements(Arc::new(move |announcement, channel| seam.withholds(&me, announcement, channel)));
                }
                // fabric.storage: `fabric/` on the store root, whose one writer is the fabric-primary (R14 item
                // 2): Day 0 writes the Fabric identity (below), and every other admin reads it there. A store
                // root holding another Fabric's identity is refused. A shutdown already held there (a restart or
                // a join during one) is in force from the start.
                let fabric_identity = crate::fabric_storage::FabricIdentity { fabric_id: cfg.fabric_id.clone(), name: cfg.fabric.clone() };
                if let Some(held) = fabric_storage.fabric().await.map_err(|e| e.to_string())? {
                    if held.fabric_id != cfg.fabric_id {
                        return Err(format!("fabric.storage holds the identity of Fabric {} ({}), this admin is configured for Fabric {}", held.fabric_id, held.name, cfg.fabric_id));
                    }
                }
                let shutdown_control = Arc::new(crate::shutdown::ShutdownControl::open(fabric_storage.clone(), name.to_string()).await.map_err(|e| e.to_string())?);
                let mut tasks_early: Vec<tokio::task::JoinHandle<()>> = Vec::new();
                // When the fabric seat moves to another holder, this admin, if it is a mesh primary, pulls
                // rafka-time once from the new fabric-primary.
                tasks_early.push(park_gate.spawn(crate::rafka_time::run_seat_move_pull(rafka_time.clone(), name.clone(), node_id.clone(), node_rpc.client.clone(), membership.clone())));
                // A delta that does not follow what this admin holds desynchronizes that source of its mesh
                // channel; it tops up from its Mesh's own primary (gossip.md §3.3) with `GetTopology` of each
                // such mesh, as every node does. Held behind the gate: a parked admin tops up nothing, and a
                // gap signalled while it was parked is topped up once it runs again.
                tasks_early.push(park_gate.spawn(membership.top_ups(crate::topology_read::top_up_fetch(node_rpc.client.clone()))));
                // The seat-transfer rows on the store root: this admin enters knowing who holds every seat a
                // committed node.primary.update moved, its own included (proposal Part C1 failure contract:
                // a holder that died after its write cut-off reads here whether its row committed).
                crate::fabric_handover::enter_seats(&*fabric_storage, &membership, &backbone, &name, &node_id).await?;
                // The current Build: Day 0 accepts the first Build itself; every other admin hydrates it from
                // its entry pull or the fabric control topic.
                let accepted = Arc::new(AcceptedStore::new(fabric_storage.clone(), name.to_string()));
                let records = Arc::new(Records::default());
                records.set_me(node_id.clone());
                // Only the fabric seat holder writes the Fabric rows, and only its held `fabric/` rows are published: the
                // file store reads the committed fabric seat record, as the Build gate does.
                fabric_files.set_seat_source(fabric_seat_source(&membership.book, &records, (node_id.clone(), incarnation.clone()), &name));
                // `fabric/` has one writer, the fabric-primary (R14 item 2): `FabricRowsWriter`, a round on every
                // seat change.
                {
                    let seats = membership.seats().clone();
                    let mut writer = FabricRowsWriter::new((node_id.clone(), incarnation.clone()), fabric_identity.clone(), fabric_storage.clone());
                    let mut changed = seats.subscribe();
                    let park = park_gate.clone();
                    tasks_early.push(tokio::spawn(async move {
                        loop {
                            // Held here while the admin is parked: its own wait, outside every lock.
                            let turn = park.turn("seat-rows").await;
                            writer.round(&seats).await;
                            drop(turn);
                            if changed.changed().await.is_err() {
                                return;
                            }
                        }
                    }));
                }
                // The status rows of this admin's mesh on the fabric's store root (`status/<mesh>/`): written by
                // the mesh's seat holder, folded into what this admin applied when it takes the seat.
                let status_files = Arc::new(crate::status_storage::FileStatusStorage::open(&store_root, &own_mesh, &incarnation).await.map_err(storage_err)?);
                let status_storage: Arc<dyn crate::status_storage::StatusStorage> = status_files.clone();
                records.set_status(status_storage.clone(), own_mesh.clone(), name.clone(), node_id.clone());
                // The ready-for-traffic event's row among this mesh's status rows is the durable fact that the fabric
                // first reached ready-for-traffic (states.md, "The initial fabric before ready-for-traffic"): an
                // admin booting on a root that holds it knows it whatever status it hears next (a resize puts the
                // fabric back to pending). The Fabric row is published at the first state-commit, before it.
                records.first_ready.read_from(&*status_storage, &cfg.fabric_id, None).await.map_err(storage_err)?;
                records.set_book(membership.book.clone());
                let builds = Arc::new(
                    FabricBuildStateAdapter::join(&gossip, &membership.location_book(), &cfg.fabric_id, seed_addrs.clone(), journal.clone(), accepted.clone(), shutdown_control.clone(), name.to_string(), fabric_seat_source(&membership.book, &records, (node_id.clone(), incarnation.clone()), &name), wiring.catch_up_seam.take())
                        .await
                        .map_err(|e| e.to_string())?
                        .with_mesh_seat(mesh_seat_source(&membership.book, &records, (node_id.clone(), incarnation.clone()), &name, &own_mesh)),
                );
                // The Build state every consumer after hydration shares: the control routes, the executor, the
                // pipelines and the drift check.
                let runs = Arc::new(crate::build_run::AttemptRuns::default());
                let builds_dyn: Arc<dyn BuildStateAdapter> = Arc::new(crate::build_run::FramedBuilds::new(crate::wiring::apply(wiring.builds.take(), builds.clone()), runs.clone()));
                {
                    let b = builds.clone();
                    shutdown_control.set_publish(Arc::new(move |sd| {
                        let b = b.clone();
                        tokio::spawn(async move {
                            if let Err(e) = b.publish_shutdown(&sd).await {
                                tracing::info!(error = %e, "broadcasting the fabric shutdown failed");
                            }
                        });
                    }));
                }

                // Fabric policy and the first view. The bootstrap admin takes its policy
                // from MESH_SPAWN_TYPE. A launched admin takes its entry from the admin
                // that launched it (its first seed) over QUIC: that admin's topology
                // projection and the membership it is projected from, recorded as heard,
                // so this admin's first view is its launcher's; the fabric record carries
                // the policy. Without that answer it cannot know the policy, and it
                // refuses to start by name.
                let entry_floor: Arc<Mutex<Option<(crate::build::BuildId, u32)>>> = Arc::default();
                let _ = build_facts_slot.set(Arc::new(crate::build_facts_read::BuildFactsDoor { me: name.clone(), builds: builds_dyn.clone(), floor: entry_floor.clone() }));
                // The topology this admin enters its mesh from without an authority (its durable map), and
                // where it came from; a launched admin's comes from node.start's topology step, after its hook.
                let mut entry_map: Option<(&'static str, Vec<crate::reenter::MapNode>)> = None;
                let channel = membership.clone();
                let joined_seam: crate::workflows::node_start::JoinedSeam<String> = Box::new(
                    move |_: Option<crate::workflows::node_start::JoinThrough>| -> crate::workflows::node_start::BoxFut<Result<(crate::workflows::node_start::HookParts, Option<crate::workflows::node_start::HookStarted<String>>), String>> {
                        Box::pin(async move {
                            let policy = match joined {
                                Some(answer) => {
                                    // Hydrate the current Build from the launching admin, and record the attempt floor its
                                    // answer names; without them this admin is not started. The Build it names arrives on
                                    // the Build topic, or is fetched from a node-admin that holds it (`FetchBuildFacts`,
                                    // node.start's Build-floor step after the hook).
                                    take_entry_control(&answer, &accepted, &entry_floor).await;
                                    // A fabric shutdown in force: this admin comes up frozen.
                                    if let Some(sd) = answer.control.shutdown.clone() {
                                        shutdown_control.learn(sd, "hydration", &answer.served_by).await.map_err(|e| e.to_string())?;
                                    }
                                    tracing::info_span!("rdm.node_admin.fabric.update.via-join", node = %name, joined = %answer.served_by)
                                        .in_scope(|| tracing::info!("entry pulled; eligible to execute Builds"));
                                    FabricPolicy { provider: answer.control.provider }
                                }
                                None => {
                                    // Day 0: no Fabric authority exists before this admin. It accepts the first Build, its
                                    // own mesh with one node-admin; it is the current Build once written.
                                    if restart.is_some() {
                                        let build_id = builds.current_build().await.map_err(|e| format!("refusing to start: the builds store cannot be read for the current Build: {e}"))?.into_build_id().map(|b| b.to_string()).unwrap_or_default();
                                        tracing::info_span!("rdm.node_admin.fabric.update.via-restart", node = %name, build_id = %build_id)
                                            .in_scope(|| tracing::info!("the current Build reloaded from this admin's own builds store"));
                                    } else if cfg.fabric_primary {
                                        tracing::info_span!("rdm.node_admin.fabric.update.via-recovery", node = %name, fabric_id = %cfg.fabric_id, mesh_id = %mesh_id)
                                            .in_scope(|| tracing::info!("a fabric recovery accepts no Build of its own: the current Build is taken from the nodes it reaches"));
                                    } else if matches!(builds.current_build().await, Ok(crate::build_state::CurrentBuild::None)) {
                                        let b0 = crate::build_state::BuildAccepted {
                                            build_id: crate::build::BuildId::mint(),
                                            topology: crate::accepted::FabricTopology::root(&cfg.fabric, &cfg.mesh, mesh_id.clone()),
                                            submitted_change: None,
                                            submitted_at_ms: rafka_time.now_ms(),
                                        };
                                        // The fabric-primary claims the Fabric's identity at Day 0, into its scratchpad until
                                        // the fabric's first state-commit (node-identity-and-state.md §2.8; fabric-create.md).
                                        if let Err(held) = claim_fabric_name(&*fabric_storage, &fabric_identity, &node_id).await? {
                                            return Err(format!("refusing to start: this admin is configured for Fabric {}: {held}", cfg.fabric_id));
                                        }
                                        attempt_contexts.put(&b0.build_id, 1, &crate::build_claim::current_context()).await.map_err(|e| format!("attempt-context: {e}"))?;
                                        crate::build_state::accept(&*builds, &b0, None, &name.to_string(), "day-0").await.map_err(|e| e.to_string())?;
                                    }
                                    FabricPolicy::bootstrap(cfg.spawn_type.as_deref()).map_err(|e| e.to_string())?
                                }
                            };

                            if restart.is_some() && cfg.launch.is_none() {
                                // A fabric primary reborn into its mesh has no maker: its durable map is the topology.
                                let mut rows = nodes_storage.contacts().await.map_err(storage_err)?;
                                rows.retain(|r| r.node_id != node_id);
                                if let Ok(map) = crate::reenter::get_topology(crate::reenter::TopologySource::DurableMap(&rows)).await {
                                    entry_map = Some(("durable-map", map));
                                }
                            }
                            let entering_existing_mesh = entry_map.as_ref().is_some_and(|(_, m)| m.iter().any(|n| n.name.mesh == cfg.mesh && n.node_id != node_id));
                            // Whether this admin enters an existing mesh: from its durable map here, or from the maker's
                            // topology at node.start's topology step. The Ready check waits for that entry's sweep.
                            let entering = Arc::new(std::sync::atomic::AtomicBool::new(entering_existing_mesh));

                            if let Some(b) = &cfg.bindings {
                                b.check_image(provider_image(policy.provider)).map_err(|e| format!("refusing to start: explicit executable bindings: {e}"))?;
                            }

                            // A restart that enters its mesh from its durable map elects nobody until its entry read has
                            // told it who holds the seats.
                            records.set_entering(entering_existing_mesh && cfg.launch.is_none());
                            let _ = records.contacts.set(nodes_storage.clone());
                            // A fabric primary reborn from its data dir holds its durable topology as its map (rule 5):
                            // every birth the accepted Build names that nodes.storage heard is in its view as not yet
                            // reached, until membership or the sweep says more. Its view is never an empty one that plans
                            // a create for every member.
                            if restart.is_some() && cfg.launch.is_none() {
                                let desired = builds_dyn.current().await.map_err(|e| format!("refusing to start: the builds store cannot be read for the current Build: {e}"))?.map(|b| b.topology);
                                for r in nodes_storage.contacts().await.map_err(storage_err)? {
                                    if r.node_id != node_id && desired.as_ref().is_some_and(|t| t.contains(&r.name)) {
                                        let m = crate::reenter::MapNode { node_id: r.node_id, name: r.name, endpoint_id: r.endpoint_id, transport_addr: r.transport_addr, incarnation: r.incarnation_id, transition: None, ready: false, data_dir: None, admin_api_base: None };
                                        records.publish(m.as_node(NodeStatus::PendingReconnect, policy.provider));
                                    }
                                }
                            }
                            // The meshes the accepted Build names keep the ids this admin stored for them.
                            if let Some(named) = builds_dyn.current().await.map_err(|e| format!("refusing to start: the builds store cannot be read for the current Build: {e}"))?.map(|b| b.topology) {
                                for m in mesh_storage.meshes().await.map_err(storage_err)?.into_iter().filter(|m| !m.dead && named.meshes.contains_key(&m.name)) {
                                    records.meshes.lock().unwrap().insert(m.name, m.mesh_id);
                                }
                            }
                            records.meshes.lock().unwrap().insert(cfg.mesh.clone(), mesh_id.clone());
                            let book = membership.book.clone();
                            let node_rpc_feed = node_rpc.feed(&book, &name.to_string());
                            let control = Arc::new(
                                ControlPlane::new(builds_dyn.clone(), accepted.clone(), name.clone(), project(&cfg.fabric, &cfg.fabric_id, policy.provider, &book, &records), rafka_time.clone()).with_contexts(attempt_contexts.clone()).with_first_ready(records.first_ready.clone(), status_storage.clone()).with_claims(mesh_claims.clone()),
                            );
                            // The claim door: this admin decides a fabric-lane claim only while it holds the fabric-primary
                            // seat, and its own mesh's drift-lane claim only while it holds its mesh's primary seat (R-BI7).
                            let claim_door = Arc::new(crate::build_claim::ClaimDoor { me: name.clone(), node_id: node_id.clone(), topology: control.topology.clone(), builds: builds_dyn.clone(), contexts: attempt_contexts.clone(), claims: mesh_claims.clone() });
                            let _ = claim_slot.set(claim_door.clone());
                            {
                                let (book, records) = (book.clone(), records.clone());
                                let _ = control.absence.set(Arc::new(move |name: &PathName| describe_absence(&book, &records, name)));
                            }
                            {
                                let (me, book, records, fabric, fabric_id, provider) = (name.clone(), book.clone(), records.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider);
                                let _ = control.view_now.set(Arc::new(move || {
                                    let (me, book, records, fabric, fabric_id) = (me.clone(), book.clone(), records.clone(), fabric.clone(), fabric_id.clone());
                                    Box::pin(async move {
                                        let reads = ViewReads::read(&me, &book, &records, &fabric_id).await;
                                        project_with(&fabric, &fabric_id, provider, &book, &records, &reads)
                                    })
                                }));
                            }
                            let _ = control.connections.set(connections.held());
                            {
                                let book = book.clone();
                                let _ = control.loads.set(Arc::new(move || book.current(book.staleness_floor()).into_iter().filter(|d| d.load.is_some() || d.gossip.is_some()).map(|d| (d.node.node_id.to_string(), (d.load, d.gossip))).collect()));
                            }
                            // This admin's re-publish of its presence, for a node-admin's status kick: filled once its
                            // digest exists, below.
                            let republish: crate::status_rpc::Republish = Arc::new(std::sync::OnceLock::new());
                            // Where this admin's own `hydrate_before_ready` stands: written by its start (and a rejoin's),
                            // read by the Ready check and by a probe naming this admin.
                            let hydration_cell: crate::status_rpc::Hydrating = Arc::default();
                            // This admin holding the config it is told (`ApplyNodeConfig`): filled with the republish, below.
                            let configure: crate::status_rpc::Configure = Arc::new(std::sync::OnceLock::new());
                            // This admin taking the name its row gives it (`ApplyNodeName`): filled with the republish, below.
                            let rename: crate::status_rpc::Rename = Arc::new(std::sync::OnceLock::new());
                            let commands = Arc::new(crate::node_commands::CommandBook::new(cfg.connections_delete_deadline));
                            let own_commands: Arc<std::sync::OnceLock<Arc<crate::node_self::NodeSelf>>> = Arc::new(std::sync::OnceLock::new());
                            let leaver: crate::mesh_leave::LeaverSlot = Arc::new(std::sync::OnceLock::new());
                            let rounds_slot: Arc<std::sync::OnceLock<Arc<crate::fabric_rounds::FabricRounds>>> = Arc::new(std::sync::OnceLock::new());
                            let configs_slot: crate::node_config::NodeConfigsSlot = Arc::new(std::sync::OnceLock::new());
                            // The record the writer of this admin's mesh rows holds, shared with its config door: a config
                            // put through the door is what the node's next row carries (`crate::node_rows::WrittenRows`).
                            let written = Arc::new(crate::node_rows::WrittenRows::default());
                            let _ = configs_slot.set(Arc::new(crate::node_config::NodeConfigs::new(node_rpc.client.clone(), configure.clone(), written.clone())));
                            {
                                let records_for_ids = records.clone();
                                let _ = authority.set(Arc::new(crate::status_rpc::StatusAuthority {
                                    status_storage: status_storage.clone(),
                                    nodes_storage: nodes_storage.clone(),
                                    me: name.clone(),
                                    node_id: node_id.clone(),
                                    fabric_id: cfg.fabric_id.clone(),
                                    topology: control.topology.clone(),
                                    declared: records.declared.clone(),
                                    mesh_ids: Arc::new(move || records_for_ids.meshes.lock().unwrap().clone()),
                                    republish: republish.clone(),
                                    commands: commands.clone(),
                                    own: own_commands.clone(),
                                    leaver: leaver.clone(),
                                    hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                                    wake: records.wake.clone(),
                                    rounds: rounds_slot.clone(),
                                    configs: configs_slot.clone(),
                                    configure: configure.clone(),
                                    hydration: hydration_cell.clone(),
                                    rename: rename.clone(),
                                    row_moved: {
                                        // The view is projected at once: it reads the moved row, as every admin's next
                                        // projection does (`ViewReads`), so it holds nothing of the move itself.
                                        let moved: crate::status_rpc::RowMoved = Arc::new(std::sync::OnceLock::new());
                                        let project = view_projector(name.clone(), control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, book.clone(), records.clone());
                                        let _ = moved.set(Arc::new(move |_from: PathName, _node_id: NodeId, _incarnation: IncarnationId, _to: PathName| project()));
                                        moved
                                    },
                                    loads: control.loads.get().cloned(),
                                    book: Some(book.clone()),
                                    first_ready: records.first_ready.clone(),
                                    builds: control.builds.clone(),
                                }));
                            }
                            // The Node RPC door: opened by the first view installed that holds this admin's exact birth
                            // once the authority above is filled, whichever task installs it.
                            {
                                let authority = authority.clone();
                                let door = ServingDoor::new(rpc_ready.clone(), (node_id.clone(), incarnation.clone()), Box::new(move || authority.get().is_some()), name.clone());
                                records.set_door(door, &*control.topology.read().await);
                            }
                            // The backbone holds the meshes each installed view names: the fabric-primary's
                            // publication of a deleted mesh reads them (mesh-delete.md step 7).
                            records.set_backbone(backbone.clone(), &*control.topology.read().await);
                            // What this admin owes its authorities, shared with the rounds: a mesh primary's completion of
                            // a fabric round is owed through it.
                            let declarer = crate::status_declare::Declarer::with_first_ready(records.first_ready.clone());
                            let _ = rounds_slot.set(crate::fabric_rounds::FabricRounds::new(crate::fabric_rounds::RoundEnv {
                                me: name.clone(),
                                node_id: node_id.clone(),
                                incarnation: incarnation.clone(),
                                fabric_id: cfg.fabric_id.clone(),
                                topology: control.topology.clone(),
                                client: node_rpc.client.clone(),
                                builds: builds_dyn.clone(),
                                contexts: attempt_contexts.clone(),
                                declarer: declarer.clone(),
                                wake: records.wake.clone(),
                                records: records.clone(),
                                hooks: fabric_hooks,
                                actions: round_actions.clone(),
                            }));
                            rounds_slot.get().expect("set above").set_gossip(backbone.clone(), membership.clone());
                            // A Build this admin accepts as the fabric-primary reaches its climb inside the acceptance:
                            // a resize puts the fabric back to pending, and the view the door's caller reads next holds it.
                            {
                                let (rounds, me, book, records, fabric, fabric_id, provider, topology) = (rounds_slot.get().expect("set above").clone(), name.clone(), book.clone(), records.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, control.topology.clone());
                                let _ = control.resize.set(Arc::new(move || {
                                    let (rounds, me, book, records, fabric, fabric_id, topology) = (rounds.clone(), me.clone(), book.clone(), records.clone(), fabric.clone(), fabric_id.clone(), topology.clone());
                                    Box::pin(async move {
                                        if rounds.accepted().await {
                                            records.install_read(&me, &topology, &book, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &book, &records, reads)).await;
                                        }
                                    })
                                }));
                            }

                            // The deployment hand (used while this admin is fabric primary). A provider's host-wide
                            // resources (a container fabric's network and labels) are keyed by the Fabric's id: a Fabric
                            // name is not unique on a host.
                            let prepared = match early {
                                Some(p) => p,
                                None => crate::deployment::prepare(policy, &cfg.fabric_id.to_string()).await.map_err(|e| e.to_string())?,
                            };
                            let mut admin_env = BTreeMap::new();
                            admin_env.insert("RDM_BIN_DIR".to_string(), cfg.bin_dir.display().to_string());
                            // A node-admin this admin launches honors the same explicit bindings: its own launches and
                            // restarts run the bound executables too.
                            admin_env.extend(cfg.bindings_env.clone());
                            let data_root = cfg.data_dir.parent().map(Path::to_path_buf).unwrap_or_else(|| cfg.data_dir.clone());
                            let template = LaunchTemplate {
                                fabric: cfg.fabric.clone(),
                                fabric_id: cfg.fabric_id.clone(),
                                executable: PathBuf::new(),
                                // The address this admin's mesh endpoint is bound on: where every birth it launches reaches
                                // it (loopback for a process fabric, the gateway or its own container address for a
                                // container fabric).
                                seeds: vec![(key.public().to_string(), mesh_addr)],
                                launcher: rafka_mesh_entity::launch::Launcher { name: name.clone(), node_id: node_id.clone(), incarnation: incarnation.clone() },
                                env: cfg.passthrough.clone(),
                                data_root,
                                store_dir: cfg.store_dir.clone(),
                                mesh_issuer: None,
                                connections_delete_deadline: cfg.connections_delete_deadline,
                            };
                            let mut registry = HookRegistry::new();
                            for (spec, hook) in wiring.hooks.drain(..) {
                                registry = registry.register(spec, hook);
                            }
                            let hooks = registry.seal().map_err(|e| format!("{e:?}"))?;
                            let provider_dyn: Arc<dyn crate::deployment::provider::DeploymentProvider> = crate::wiring::apply(wiring.provider.take(), prepared.provider.clone());
                            let handover = Arc::new(crate::fabric_handover::HandoverDoor {
                                fabric: cfg.fabric.clone(),
                                me: name.clone(),
                                node_id: node_id.clone(),
                                incarnation: incarnation.clone(),
                                topology: control.topology.clone(),
                                membership: membership.clone(),
                                backbone: backbone.clone(),
                                storage: fabric_storage.clone(),
                                client: node_rpc.client.clone(),
                                book: book.clone(),
                                records: records.clone(),
                                locks: Default::default(),
                                memo: Default::default(),
                                decisions: builds.decisions(),
                                builds: builds_dyn.clone(),
                                project_view: view_projector(name.clone(), control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, book.clone(), records.clone()),
                                cut_off_writes: Arc::new(crate::fabric_handover::SeatStores { mesh: mesh_files.clone(), nodes: nodes_files.clone(), fabric: fabric_files.clone(), status: status_files.clone() }).cut_off_writes(),
                                looker: crate::seat_watch::Looker { me: name.clone(), me_id: node_id.clone(), membership: membership.clone(), client: node_rpc.client.clone(), provider: provider_dyn.clone(), nodes: Some(nodes_storage.clone()) },
                            });
                            let _ = handover_slot.set(handover.clone());
                            let runner = Arc::new(AdminRunner {
                                status_storage: status_storage.clone(),
                                mesh_storage: mesh_storage.clone(),
                                provider: provider_dyn.clone(),
                                joins: joins.clone(),
                                observer: Arc::new(MembershipObserver { book: book.clone(), client: Some(node_rpc.client.clone()), commands: commands.clone(), authority: authority.clone(), handover: handover_slot.clone() }),
                                records: records.clone(),
                                builds: builds_dyn.clone(),
                                local: journal.clone(),
                                template,
                                admin_env,
                                bin_dir: cfg.bin_dir.clone(),
                                bindings: cfg.bindings.clone(),
                                lifecycle: LifecycleTransitionPipeline::new(hooks, Arc::new(MemoryReceiptLog::default())),
                                handles: Mutex::new(HashMap::new()),
                                topology: control.topology.clone(),
                                fabric: cfg.fabric.clone(),
                                fabric_id: cfg.fabric_id.clone(),
                                fabric_provider: policy.provider,
                                book: book.clone(),
                                me: name.clone(),
                                node_id: node_id.clone(),
                                endpoint: Some(endpoint.clone()),
                                node_rpc: node_rpc.clone(),
                                lifecycle_events: crate::wiring::apply(wiring.lifecycle_events.take(), Arc::new(GossipLifecycle { membership: membership.clone(), backbone: backbone.clone(), me: name.clone(), leaver: leaver.clone() })),
                                leaves: Arc::new(crate::mesh_leave::LeaveBook::default()),
                                leave_runs: crate::mesh_leave::LeaveRuns::default(),
                                handover: handover.clone(),
                                issuer: issuer.clone(),
                            });
                            let _ = leaver.set(Arc::new(LeaveDoor { runner: runner.clone() }));
                            // The Build family on `0x20`: the executor's door for `build.attempt.run` and the fabric-primary's
                            // drive of every Build. The same admin holds both; which one a call reaches depends on the seat.
                            let drives = Arc::new(crate::build_drive::Drives::default());
                            let drive_env = {
                                let exec = Arc::new(BuildExecutor { executor: name.to_string(), builds: builds_dyn.clone(), topology: control.topology.clone(), runner: runner.clone() });
                                let run_door = Arc::new(crate::build_run::RunDoor {
                                    me: name.clone(),
                                    builds: builds_dyn.clone(),
                                    exec,
                                    runs: runs.clone(),
                                    local: journal.clone(),
                                    gate: park_gate.clone(),
                                });
                                let env = Arc::new(crate::build_drive::DriveEnv {
                                    me: name.clone(),
                                    topology: control.topology.clone(),
                                    builds: builds_dyn.clone(),
                                    door: claim_door.clone(),
                                    dispatcher: Arc::new(crate::build_op::RpcDispatcher { me: name.clone(), local: run_door.clone(), client: node_rpc.client.clone(), topology: control.topology.clone() }),
                                    gate: Arc::new(SeatGate { membership: membership.clone(), builds: builds.clone(), shutdown: shutdown_control.clone(), control: control.clone(), handover: handover.clone() }),
                                    departure: Arc::new(ProviderDeparture {
                                        me: name.clone(),
                                        topology: control.topology.clone(),
                                        refresh: {
                                            let (me, topology, records, book, fabric, fabric_id, provider) = (name.clone(), control.topology.clone(), records.clone(), book.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider);
                                            Arc::new(move || {
                                                let (me, topology, records, book, fabric, fabric_id) = (me.clone(), topology.clone(), records.clone(), book.clone(), fabric.clone(), fabric_id.clone());
                                                Box::pin(async move {
                                                    records.install_read(&me, &topology, &book, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &book, &records, reads)).await;
                                                })
                                            })
                                        },
                                        book: book.clone(), provider: runner.provider.clone(), nodes: nodes_storage.clone(), records: records.clone() }),
                                    verdicts: Arc::new(crate::build_op::LocalVerdicts { local: journal.clone() }),
                                    rows: {
                                        let rows = Arc::new(crate::node_rows::NodeRows::new(name.clone(), node_id.clone(), control.topology.clone(), nodes_storage.clone()).with_configs(configs_slot.clone()).with_written(written.clone()));
                                        records.set_rows(rows.clone());
                                        let held = rows.clone();
                                        let _ = control.parked_old.set(Arc::new(move |old: &PathName| held.parked_at(old)));
                                        rows
                                    },
                                    turns: park_gate.clone(),
                                });
                                let _ = build_slot.set(Arc::new(crate::build_op::BuildDoor { me: name.clone(), control: control.clone(), drives: drives.clone(), env: env.clone(), run: run_door, seats: membership.seats().clone(), incarnation: incarnation.clone(), incumbent: handover.clone() }));
                                env
                            };

                            // This birth's exact runtime. A launched admin takes the record its
                            // launcher's pipeline made available; the bootstrap admin, which nobody
                            // launched, adopts its own process (Day 0, one receipt per step). Either
                            // way it is published with the birth.
                            let mut adoption = None;
                            let runtime = match launched_runtime {
                                Some(r) => r,
                                None => {
                                    // In a container of this fabric's container runtime, its runtime is that container;
                                    // a host process stays a process (it then cannot be failed over by a container admin,
                                    // which refuses Ready by name).
                                    let in_container = (prepared.provider.kind() == ProviderKind::Container && rafka_mesh_entity::runtime::this_container_id().is_some()).then(|| prepared.provider.control_domain());
                                    let a = CurrentRuntimeAdoption::begin_in(&name, &cfg.data_dir, &crate::model::DeploymentId::mint(), in_container).await.map_err(|e| e.to_string())?;
                                    let f = a.fact().clone();
                                    adoption = Some(a);
                                    f
                                }
                            };
                            // The identity this process records its intentional exit under (a transport that stopped).
                            rafka_mesh_entity::runtime::set_own_exit(rafka_mesh_entity::runtime::OwnExit {
                                data_dir: cfg.data_dir.clone(),
                                deployment_id: runtime.deployment_id.clone(),
                                incarnation: incarnation.to_string(),
                            });
                            // Own digest, published on the membership cadence.
                            let digest = Arc::new(Mutex::new(MeshDigest {
                                fabric_id: cfg.fabric_id.clone(),
                                node: MeshNode {
                                    node_id: node_id.clone(),
                                    name: handed.as_ref().and_then(|(n, _)| n.clone()).unwrap_or_else(|| name.clone()),
                                    endpoint_id: EndpointId(key.public().to_string()),
                                    // The mesh endpoint as bound (a rebind may have moved it).
                                    transport_addr: mesh_addr,
                                    incarnation: incarnation.clone(),
                                    supersedes,
                                    // Day 0: PublishRuntimeFactAndCurrentRuntimeMetadata sets them.
                                    runtime: adoption.is_none().then(|| runtime.clone()),
                                },
                                // Pending until it can take authority at once (the Ready gate below).
                                status: MemberStatus::Pending,
                                node_state: Default::default(),
                                admin_api_base: Some(api_base.clone()),
                                // Stamped by `Membership::publish`: this birth's own sequence and the composed clock.
                                emitted_at_rafka_ms: 0,
                                digest_seq: 0,
                                mesh_id: Some(mesh_id.clone()),
                                in_flight: None,
                                config: handed.as_ref().and_then(|(_, c)| c.clone()).unwrap_or_default(),
                                load: None,
                                gossip: None,
                                data_dir: adoption.is_none().then(|| cfg.data_dir.display().to_string()),
                            }));
                            // nodes.storage and mesh.storage: this birth and its Mesh, so a restart on this data dir is
                            // the same logical node in the same Mesh.
                            {
                                let d = digest.lock().unwrap().clone();
                                nodes_storage
                                    .put_own(&crate::storage::NodeRecord {
                                        node_id: node_id.clone(),
                                        fabric_id: cfg.fabric_id.clone(),
                                        name: name.clone(),
                                        mesh_id: mesh_id.clone(),
                                        incarnation_id: incarnation.clone(),
                                        endpoint_id: d.node.endpoint_id.clone(),
                                        transport_addr: d.node.transport_addr,
                                        listeners: Vec::new(),
                                    })
                                    .await.map_err(storage_err)?;
                            }
                            // nodes.storage contacts and mesh.storage: this admin's mesh's members and its Mesh row on the
                            // fabric's shared root, written while this admin holds its mesh's primary seat (the writer
                            // ladder; `MeshRowsWriter`), as bootstrap hints for a later restart of any node of the fabric.
                            // A birth is written the moment the book first holds it or it changes (the book's own birth
                            // watch, as the runtime rows are), and every member at once when the seat comes to this admin
                            // (the view install's seat watch): a hint that waits on a timer is lost to a restart that comes
                            // first. A contact whose node departed, or whose birth the book no longer holds at a path
                            // another logical node now holds, is dropped (`dropped_contacts`). Nothing here is topology.
                            let (own_seat_tx, mut own_seat) = tokio::sync::watch::channel(false);
                            let contacts_task = {
                                let (book, fabric_id) = (membership.book.clone(), cfg.fabric_id.clone());
                                let mut writer = MeshRowsWriter::new(&name.to_string(), cfg.fabric_id.clone(), &cfg.mesh, mesh_id.clone(), nodes_storage.clone(), mesh_storage.clone());
                                let park = park_gate.clone();
                                tokio::spawn(async move {
                                    let mut births = book.birth_changes();
                                    loop {
                                        // Held here while the admin is parked: its own wait, outside every lock.
                                        let turn = park.turn("mesh-rows").await;
                                        births.borrow_and_update();
                                        let seated = *own_seat.borrow_and_update();
                                        let held: Vec<MeshDigest> = book.all().into_iter().filter(|d| d.fabric_id == fabric_id).collect();
                                        writer.round(&held, seated, |id| book.is_departed(id.as_str())).await;
                                        drop(turn);
                                        tokio::select! {
                                            changed = births.changed() => if changed.is_err() { return },
                                            changed = own_seat.changed() => if changed.is_err() { return },
                                        }
                                    }
                                })
                            };
                            if let Some(a) = adoption {
                                let digest = digest.clone();
                                a.publish(move |f, dir| {
                                    let mut d = digest.lock().unwrap();
                                    d.node.runtime = Some(f.clone());
                                    d.data_dir = Some(dir);
                                })
                                .await
                                .map_err(|e| e.to_string())?;
                            }
                            let _ = entry.set(EntryState {
                                name: name.clone(),
                                provider: policy.provider,
                                accepted: accepted.clone(),
                                builds: builds_dyn.clone(),
                                shutdown: shutdown_control.clone(),
                                membership: membership.clone(),
                                time: rafka_time.clone(),
                            });
                            // The join door: this admin verifies a node's digest against what it deployed, installs the
                            // address for its key and answers what it holds.
                            {
                                let (st, resolver, m, topo) = (entry.clone(), node_rpc_resolver.clone(), membership.clone(), membership.clone());
                                let door = Arc::new(crate::join::JoinDoor {
                                    me: name.clone(),
                                    joins: joins.clone(),
                                    answer: Arc::new(move || {
                                        let st = st.clone();
                                        Box::pin(async move {
                                            match st.get() {
                                                Some(e) => e.answer().await,
                                                None => Err("this admin holds no view yet".to_string()),
                                            }
                                        })
                                    }),
                                    install: Arc::new(move |d: &MeshDigest| {
                                        // The same effect hearing the digest on gossip has: where the key is, and the
                                        // live resolver's birth (which cancels every dial aimed at the key's old address).
                                        m.learn(d.clone(), "join");
                                        if let Ok(endpoint_id) = d.node.endpoint_id.0.parse::<iroh::PublicKey>() {
                                            let applied = resolver.apply(
                                                rafka_node_rpc::ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id, transport_addr: d.node.transport_addr, incarnation: d.node.incarnation.clone() },
                                                d.node.supersedes.as_ref(),
                                            );
                                            tracing::info!(node = %d.node.name, applied = ?applied, "the resolver holds the joined birth");
                                        }
                                    }),
                                    known: view_projector(name.clone(), control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, book.clone(), records.clone()),
                                    primary: Arc::new(move || topo.mesh_primary().ok().map(|p| p.node.name.to_string())),
                                    issuer: issuer.clone(),
                                    row: Arc::new({
                                        let rows = nodes_storage.clone();
                                        move |name: PathName| {
                                            let rows = rows.clone();
                                            Box::pin(async move { rows.node_row_at(&name).await.map_err(|e| e.to_string()) })
                                        }
                                    }),
                                });
                                let _ = join_slot.set(door);
                                // Every node serves the topology it holds (`GetTopology`, op `0x1E`).
                                let own = digest.clone();
                                // For a mesh it holds no snapshot of, this admin answers from the births it stores.
                                // The births it stores and the births its book holds now: what this admin knows of a mesh
                                // does not wait on the contacts being written.
                                let (contacts, meshes, held_book, held_fabric, held_me) = (nodes_storage.clone(), mesh_storage.clone(), book.clone(), cfg.fabric_id.clone(), node_id.clone());
                                let stored: crate::topology_read::StoredSource = Arc::new(move || {
                                    let (contacts, meshes, held_book, held_fabric, held_me) = (contacts.clone(), meshes.clone(), held_book.clone(), held_fabric.clone(), held_me.clone());
                                    Box::pin(async move {
                                        let rows = contacts.contacts().await.map_err(|e| e.to_string())?;
                                        let ids = meshes.meshes().await.map_err(|e| e.to_string())?;
                                        let mut nodes: BTreeMap<NodeId, rafka_node_rpc_contract::topology::StoredNode> = rows
                                            .into_iter()
                                            .map(|r| (r.node_id.clone(), rafka_node_rpc_contract::topology::StoredNode { node_id: r.node_id, name: r.name.to_string(), endpoint_id: r.endpoint_id, incarnation: r.incarnation_id, transport_addr: r.transport_addr }))
                                            .collect();
                                        for d in held_births(&held_book, &held_fabric, &held_me) {
                                            nodes.insert(
                                                d.node.node_id.clone(),
                                                rafka_node_rpc_contract::topology::StoredNode { node_id: d.node.node_id, name: d.node.name.to_string(), endpoint_id: d.node.endpoint_id, incarnation: d.node.incarnation, transport_addr: d.node.transport_addr },
                                            );
                                        }
                                        Ok(crate::topology_read::StoredMap {
                                            nodes: nodes.into_values().collect(),
                                            mesh_ids: ids.into_iter().filter(|m| !m.dead).map(|m| (m.name, m.mesh_id)).collect(),
                                        })
                                    })
                                });
                                let gone: crate::topology_read::GoneSource = {
                                    let (book, records) = (book.clone(), records.clone());
                                    Arc::new(move |h: &rafka_mesh_entity::SeatHolder| incumbency_of(&book, &records).lost.contains(&h.incarnation))
                                };
                                let _ = topology_slot.set(Arc::new(crate::topology_read::TopologyDoor::new(membership.clone(), Arc::new(move || own.lock().unwrap().clone()), rafka_time.clone()).with_stored(stored).with_gone(gone)));
                            }
                            // Each birth's exact runtime, the moment this admin first holds the birth: its own keyed row,
                            // a blind put (a successor fabric primary proves an exit from it).
                            let runtime_rows_task = {
                                let (book, nodes_storage, me, fabric_id) = (membership.book.clone(), nodes_storage.clone(), node_id.clone(), cfg.fabric_id.clone());
                                let park = park_gate.clone();
                                tokio::spawn(async move {
                                    let turn = park.turn("runtime-rows").await;
                                    let mut written: HashSet<(NodeId, IncarnationId)> = nodes_storage.runtimes().await.unwrap_or_default().into_iter().map(|r| (r.node_id, r.incarnation_id)).collect();
                                    drop(turn);
                                    let mut births = book.birth_changes();
                                    loop {
                                        // Held here while the admin is parked: its own wait, outside every lock.
                                        let turn = park.turn("runtime-rows").await;
                                        births.borrow_and_update();
                                        for d in book.all().into_iter().filter(|d| d.fabric_id == fabric_id && d.node.node_id != me) {
                                            let key = (d.node.node_id.clone(), d.node.incarnation.clone());
                                            let Some(runtime) = d.node.runtime.clone() else { continue };
                                            if written.contains(&key) {
                                                continue;
                                            }
                                            let row = crate::storage::RuntimeRow { node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime, data_dir: d.data_dir.clone() };
                                            match nodes_storage.put_runtime(&row).await {
                                                Ok(()) => {
                                                    written.insert(key);
                                                }
                                                Err(e) => tracing::info!(node = %d.node.name, error = %e, "a heard birth's runtime could not be stored"),
                                            }
                                        }
                                        drop(turn);
                                        if births.changed().await.is_err() {
                                            return;
                                        }
                                    }
                                })
                            };
                            // A birth the book shows superseded (a later incarnation of its node held, or its node
                            // departed): the declared-state rows this admin put for it as an authority are tombstoned.
                            let superseded_rows_task = {
                                let (book, authority) = (membership.book.clone(), authority.clone());
                                let park = park_gate.clone();
                                tokio::spawn(async move {
                                    let mut births = book.birth_changes();
                                    loop {
                                        // Held here while the admin is parked: its own wait, outside every lock.
                                        let turn = park.turn("superseded-rows").await;
                                        births.borrow_and_update();
                                        if let Some(a) = authority.get() {
                                            a.retire_superseded(&book).await;
                                        }
                                        drop(turn);
                                        if births.changed().await.is_err() {
                                            return;
                                        }
                                    }
                                })
                            };
                            let mut tasks = vec![contacts_task, node_rpc_feed, runtime_rows_task, superseded_rows_task];
                            tasks.extend(tasks_early);
                            // Entering an existing mesh (a recovering mesh's first admin after its maker's join; a fabric
                            // primary reborn into its mesh from its durable map): connect to a local node, read its
                            // topology and sweep the own mesh once with a Ping. Ready waits for the sweep; every node that
                            // did not answer is unheard, and node recovery's to prove (mesh-recovery.md step 6).
                            let sweep_slot: Arc<std::sync::OnceLock<crate::reenter::SweepReport>> = Arc::new(std::sync::OnceLock::new());
                            let entry_ctx = crate::reenter::EntryCtx { me: name.clone(), me_id: node_id.clone(), client: node_rpc.client.clone(), resolver: node_rpc_resolver.clone(), membership: membership.clone(), time: rafka_time.clone() };
                            if let (true, Some((source, map))) = (entering_existing_mesh, entry_map.take()) {
                                tasks.push(spawn_entry_sweep(&park_gate, entry_ctx.clone(), source, map, sweep_slot.clone(), records.clone(), policy.provider));
                            }
                            // Authority-capable Ready: this admin commits `ReadyForTraffic` (and so
                            // becomes eligible for every seat) only once it can manage every birth it
                            // holds: each live member publishes a runtime fact this admin's provider
                            // adopts in its own control domain, and the Day-0 admin holds a Complete
                            // receipt for every step of its own adoption. Until then it publishes
                            // Pending.
                            // `hydrate_before_ready` (the app's hook): `node.start` runs it once the join is accepted and the
                            // mesh channel joined, before any topology is read, in a task behind this admin's gate, and then
                            // its topology, Build-floor and rafka-time steps in the same task; the Ready check below names
                            // the hook's blocker and the step the start is at. A birth with no launcher has no authority to
                            // pull from, and says why.
                            let hydrate_cancel = rafka_node_rpc::Retirement::new();
                            // The node-admins a Build-facts read asks, in order: the fabric-primary, every other live
                            // node-admin, then the launcher. Read at each request, so the order follows the leadership
                            // projection; every start of this admin asks them.
                            let responders: crate::hydrate::Responders = {
                                let topology = control.topology.clone();
                                let launcher = cfg.launch.as_ref().and_then(|l| l.launcher.clone());
                                Arc::new(move || {
                                    let (topology, launcher) = (topology.clone(), launcher.clone());
                                    Box::pin(async move {
                                        let t = topology.read().await;
                                        let mut out: Vec<crate::hydrate::Responder> = t.fabric_primary().map(|n| crate::hydrate::Responder { name: n.name.clone(), node_id: n.node_id.clone() }).into_iter().collect();
                                        let mut admins: Vec<&crate::model::Node> = t.members().filter(|n| n.kind == NodeKind::NodeAdmin && n.status.is_live()).collect();
                                        admins.sort_by(|a, b| a.name.cmp(&b.name));
                                        for n in admins {
                                            if !out.iter().any(|r| r.name == n.name) {
                                                out.push(crate::hydrate::Responder { name: n.name.clone(), node_id: n.node_id.clone() });
                                            }
                                        }
                                        if let Some(l) = launcher {
                                            if !out.iter().any(|r| r.name == l.name) {
                                                out.push(crate::hydrate::Responder { name: l.name.clone(), node_id: l.node_id.clone() });
                                            }
                                        }
                                        out
                                    })
                                })
                            };
                            // What node.start has yet to do once the hook runs: the Ready check names it until the start's
                            // last step before node.started has ended.
                            let progress = StartProgress::begun(&name);
                            // The tasks node.start's topology step starts (the own-mesh sweep of a launched admin
                            // entering an existing mesh): ended with this admin's other tasks.
                            let start_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::default();
                            let after = AdminFirstStart {
                                me: name.clone(),
                                me_id: node_id.clone(),
                                fabric_id: cfg.fabric_id.clone(),
                                mesh: cfg.mesh.clone(),
                                backbone: backbone.clone(),
                                records: records.clone(),
                                provider: policy.provider,
                                gate: park_gate.clone(),
                                entry: entry_ctx,
                                entering: entering.clone(),
                                sweep: sweep_slot.clone(),
                                start_tasks: start_tasks.clone(),
                                floor: AdminFloor {
                                    me: name.clone(),
                                    builds: builds_dyn.clone(),
                                    floor: entry_floor.clone(),
                                    hydrator: crate::hydrate::Hydrator::new(
                                        name.clone(),
                                        node_rpc.client.clone(),
                                        journal.clone(),
                                        responders.clone(),
                                        entry_retrieval.map(|(target, anchor, digest)| entry_repeat(node_rpc.client.clone(), target, anchor, digest, accepted.clone(), entry_floor.clone(), shutdown_control.clone(), rafka_time.clone())),
                                    ),
                                    progress: Some(progress.clone()),
                                },
                                progress: progress.clone(),
                                read_seam: wiring.read_seam.clone(),
                            };
                            let parts = crate::workflows::node_start::HookParts {
                                birth: crate::app_hydration::Birth { name: name.clone(), node_id: node_id.clone(), incarnation: incarnation.clone(), fabric_id: cfg.fabric_id.clone(), mesh_id: mesh_id.clone(), kind: NodeKind::NodeAdmin },
                                hydration: wiring.hydration.clone(),
                                events: crate::app_hydration::Events::new(book.clone(), records.wake.hydration.clone()),
                                cancel: hydrate_cancel.token(),
                                state: hydration_cell.clone(),
                                runs: crate::workflows::node_start::HookRuns::Spawned {
                                    spawn: {
                                        let park = park_gate.clone();
                                        Box::new(move |hook: crate::workflows::node_start::BoxFut<()>| park.spawn(hook))
                                    },
                                },
                                after: Box::new(after),
                            };
                            let started: crate::workflows::node_start::HookStarted<String> = Box::new(move |hook: tokio::task::JoinHandle<()>| -> crate::workflows::node_start::BoxFut<Result<(), String>> {
                                Box::pin(async move {
                                    tasks.push(hook);
                                    {
                                        let hydration_state = hydration_cell.clone();
                                        let (digest, book, provider, me, membership) = (digest.clone(), book.clone(), provider_dyn.clone(), name.clone(), membership.clone());
                                        let day0 = cfg.launch.is_none().then(|| cfg.data_dir.clone());
                                        let (hydrated, start_progress, entering_gate) = (builds_dyn.clone(), progress.clone(), entering.clone());
                                        let sweep_slot_gate = sweep_slot.clone();
                                        let (incarnation, node_id, mesh_id, fabric_id) = (incarnation.clone(), node_id.clone(), mesh_id.clone(), cfg.fabric_id.clone());
                                        let runtime = runtime.clone();
                                        let boot = boot.clone();
                                        let (records, topology, authority, mesh_name) = (records.clone(), control.topology.clone(), authority.clone(), cfg.mesh.clone());
                                        let park = park_gate.clone();
                                        tasks.push(tokio::spawn(async move {
                                            let mut reported = None;
                                            loop {
                                                // Held here while the admin is parked: its own wait, outside every lock.
                                                let turn = park.turn("ready").await;
                                                let held: Vec<MeshDigest> = book
                                                    .current(book.staleness_floor())
                                                    .into_iter()
                                                    .filter(|d| d.node.name != me && d.status != MemberStatus::Leaving)
                                                    .collect();
                                                let mut blocked = authority_blockers(&held, &*provider);
                                                if let Some(dir) = &day0 {
                                                    match adoption_missing(dir).await {
                                                        Ok(missing) => blocked.extend(missing.into_iter().map(|s| format!("{me}: no Complete receipt for its own {s}"))),
                                                        Err(e) => blocked.push(format!("{me}: its own adoption receipts cannot be read: {e}")),
                                                    }
                                                }
                                                if entering_gate.load(std::sync::atomic::Ordering::SeqCst) && sweep_slot_gate.get().is_none() {
                                                    blocked.push(format!("{me}: the own-mesh sweep on entering the mesh has not finished"));
                                                }
                                                // The app's hook: Ready waits on it, and its blocker is named here with the others.
                                                blocked.extend(hydration_state.lock().unwrap().as_ref().and_then(|h| h.state().blocker(&me)));
                                                // node.start's steps after the hook (topology, Build floor, rafka-time): Ready waits on
                                                // them, and the step it is at is named here with the others.
                                                blocked.extend(start_progress.blocker());
                                                // A mesh is Pending until its authority says so (e4.s11): a mesh's first admin holds
                                                // its own Ready until the fabric primary applied MeshStatus::Pending at it; the Day-0
                                                // root, with no upstream authority, applies its own. An admin joining a mesh whose
                                                // admin cohort is already live owes nothing here.
                                                let pending_applied = records.declared.lock().unwrap().meshes.contains_key(&mesh_id);
                                                let cohort_live = topology.read().await.cohort(&mesh_name, NodeKind::NodeAdmin).any(|n| n.name != me && n.status.is_live());
                                                match pending_gate(&me, &mesh_name, day0.is_some(), pending_applied, cohort_live) {
                                                    PendingGate::Clear => {}
                                                    PendingGate::Blocked(why) => blocked.push(why),
                                                    PendingGate::SelfApply => match authority.get() {
                                                        Some(auth) => {
                                                            let reply = auth.self_apply_mesh_pending(&mesh_id).await;
                                                            if !matches!(reply, rafka_node_rpc_contract::status::StatusReply::Applied | rafka_node_rpc_contract::status::StatusReply::AlreadyApplied) {
                                                                blocked.push(format!("{me}: its mesh's Pending is not applied (self-apply answered {reply:?})"));
                                                            }
                                                        }
                                                        None => blocked.push(format!("{me}: no view yet to apply its mesh's Pending to itself")),
                                                    },
                                                }
                                                if blocked.is_empty() {
                                                    // The current Build the hydration gate above found held: the Ready span names it,
                                                    // so "it held the current Build before Ready" is read off the Ready itself.
                                                    let held_build = match hydrated.current_build().await {
                                                        Ok(c) => c.into_build_id().map(|b| b.to_string()).unwrap_or_default(),
                                                        Err(e) => format!("unreadable: {e}"),
                                                    };
                                                    let mut d = digest.lock().unwrap();
                                                    if d.status != MemberStatus::Pending {
                                                        break;
                                                    }
                                                    d.status = MemberStatus::ReadyForTraffic;
                                                    drop(d);
                                                    records.wake.poke();
                                                    tracing::info_span!(
                                                        parent: &boot,
                                                        "rdm.mesh.node.update.via-ready",
                                                        node = %me,
                                                        incarnation_id = %incarnation.as_str(),
                                                        meshes = membership.meshes_held(),
                                                        node_id = %node_id,
                                                        mesh_id = %mesh_id,
                                                        fabric_id = %fabric_id,
                                                        id_format = rafka_mesh_entity::ID_FORMAT,
                                                        deployment_id = %runtime.deployment_id,
                                                        provider = runtime.provider.as_str(),
                                                        provider_control_domain_fingerprint = %runtime.domain_fingerprint(),
                                                        runtime_locator_kind = runtime.locator.kind(),
                                                        runtime_locator_fingerprint = %runtime.locator_fingerprint(),
                                                        source = "self-published-membership",
                                                        runtime_facts_held = held.len(),
                                                        build_id = %held_build,
                                                    )
                                                    .in_scope(|| tracing::info!("ready for traffic: every held birth's runtime is adoptable"));
                                                    break;
                                                }
                                                if reported.as_ref() != Some(&blocked) {
                                                    tracing::info_span!("rdm.node_admin.runtime.reject.via-not-authority-capable", node = %me, blocked = blocked.len(), detail = %blocked.join("; "))
                                                        .in_scope(|| tracing::info!("not ready: a held birth's runtime cannot be managed from here"));
                                                    reported = Some(blocked);
                                                }
                                                drop(turn);
                                                tokio::time::sleep(Duration::from_millis(100)).await;
                                            }
                                        }));
                                    }
                                    {
                                        let (membership, digest) = (membership.clone(), digest.clone());
                                        let _ = republish.set(Arc::new(move || {
                                            let (membership, digest) = (membership.clone(), digest.clone());
                                            Box::pin(async move {
                                                let peers: Vec<EndpointAddr> = membership.book.all().iter().filter_map(rafka_mesh_transport::membership::gossip_addr).collect();
                                                let _ = membership.join_peers(peers).await;
                                                let d = digest.lock().unwrap().clone();
                                                let _ = membership.publish(&d).await;
                                            })
                                        }));
                                    }
                                    {
                                        // The config is held on this admin's own digest and by its membership, so every publish and
                                        // every read of its own digest carries it.
                                        let (membership, digest) = (membership.clone(), digest.clone());
                                        let _ = configure.set(Arc::new(move |config: BTreeMap<String, String>| {
                                            let (membership, digest) = (membership.clone(), digest.clone());
                                            Box::pin(async move {
                                                membership.set_config(config.clone());
                                                let d = {
                                                    let mut d = digest.lock().unwrap();
                                                    d.config = config;
                                                    d.clone()
                                                };
                                                let _ = membership.publish(&d).await;
                                            })
                                        }));
                                    }
                                    {
                                        // The name is held on this admin's own digest and by its membership, taken only while it
                                        // holds no connection (R-CR1); a stopped admin holds it and publishes nothing.
                                        let (membership, digest, connections) = (membership.clone(), digest.clone(), connections.clone());
                                        let _ = rename.set(Arc::new(move |asked: PathName| {
                                            let (membership, digest, connections) = (membership.clone(), digest.clone(), connections.clone());
                                            Box::pin(async move {
                                                let held = digest.lock().unwrap().node.name.clone();
                                                let routes = connections.held_routes().await;
                                                let decision = crate::node_name::decide(&held, &asked, &routes);
                                                if decision == crate::node_name::NameApply::Applied {
                                                    membership.set_name(asked.clone());
                                                    let d = {
                                                        let mut d = digest.lock().unwrap();
                                                        d.node.name = asked;
                                                        d.clone()
                                                    };
                                                    if !crate::node_self::parking().is_parked() {
                                                        let _ = membership.publish(&d).await;
                                                    }
                                                }
                                                (held, decision)
                                            })
                                        }));
                                    }
                                    // This admin as the subject of drain-node / stop-node from its own mesh-admin.
                                    {
                                        let digest_for_status = digest.clone();
                                        let digest_now = digest.clone();
                                        let set_status: Arc<dyn Fn(MemberStatus) -> MeshDigest + Send + Sync> = Arc::new(move |st| {
                                            let mut d = digest_for_status.lock().unwrap();
                                            d.status = st;
                                            d.clone()
                                        });
                                        let status: Arc<dyn Fn() -> MemberStatus + Send + Sync> = Arc::new(move || digest_now.lock().unwrap().status);
                                        let own = Arc::new(crate::node_self::NodeSelf::new(node_id.clone(), incarnation.clone(), name.clone(), rpc_server.clone(), node_rpc.client.clone(), node_rpc.resolver.clone(), membership.clone(), set_status, status, None, cfg.connections_delete_deadline).with_retire(hydrate_cancel.clone()).with_round_actions(round_actions.clone()).with_backbone(backbone.clone()));
                                        own.set_connections(connections.clone());
                                        own.set_session(Arc::new(AdminSession {
                                            gate: park_gate.clone(),
                                            node_id: node_id.clone(),
                                            incarnation: incarnation.clone(),
                                            fabric_id: cfg.fabric_id.clone(),
                                            mesh_id: mesh_id.clone(),
                                            membership: membership.clone(),
                                            backbone: backbone.clone(),
                                            client: node_rpc.client.clone(),
                                            server: rpc_server.clone(),
                                            digest: digest.clone(),
                                            rafka_time: rafka_time.clone(),
                                            accepted: accepted.clone(),
                                            builds: builds_dyn.clone(),
                                            entry_floor: entry_floor.clone(),
                                            shutdown: shutdown_control.clone(),
                                            hydration: wiring.hydration.clone(),
                                            addressed: records.wake.hydration.clone(),
                                            hydration_cell: hydration_cell.clone(),
                                            hydrating: Arc::default(),
                                            wake: records.clone(),
                                            local: journal.clone(),
                                            responders: responders.clone(),
                                            declarer: declarer.clone(),
                                        }));
                                        let _ = own_commands.set(own);
                                    }
                                    // A node's own `node-drained` / `node-left`, heard on this mesh's channel: this admin carries
                                    // it onto the backbone when it holds the command (gossip.md, node-drained, node-left).
                                    {
                                        let (commands, backbone, rounds) = (commands.clone(), backbone.clone(), rounds_slot.get().cloned().expect("the rounds are built before the carrier"));
                                        membership.set_completion_carrier(Arc::new(move |f: &rafka_mesh_transport::membership::Frame| {
                                            use rafka_mesh_transport::membership::Frame;
                                            // A fabric round check-in by an ordinary node, carried by the mesh primary holding the round.
                                            if let Frame::StateCommitted { hook, .. } | Frame::TrafficOpened { hook, .. } = f {
                                                let kind = if matches!(f, Frame::StateCommitted { .. }) { crate::fabric_state::RoundKind::StateCommit } else { crate::fabric_state::RoundKind::OpenTraffic };
                                                let key = crate::fabric_state::RoundKey { kind, fabric_id: hook.fabric_id.clone(), build_id: hook.build_id.clone(), attempt: hook.attempt };
                                                // A node-admin author publishes on the backbone itself; only an ordinary node's check-in is carried.
                                                let ordinary = hook.publisher.parse::<PathName>().is_ok_and(|p| p.kind != NodeKind::NodeAdmin);
                                                if ordinary && rounds.mesh_book.is_open(&key) {
                                                    let (backbone, f) = (backbone.clone(), f.clone());
                                                    tokio::spawn(async move { backbone.carry_completion(f).await });
                                                }
                                                return;
                                            }
                                            let (Frame::NodeLeft { op, .. } | Frame::NodeDrained { op, .. }) = f else { return };
                                            let key = crate::node_commands::CommandKey { build_id: op.build_id.clone(), attempt: op.attempt, operation: op.operation.clone(), node_id: op.node_id.clone(), incarnation: op.incarnation.clone() };
                                            if commands.is_open(&key) {
                                                let (backbone, f) = (backbone.clone(), f.clone());
                                                tokio::spawn(async move { backbone.carry_completion(f).await });
                                            }
                                        }));
                                    }
                                    let d = digest.clone();
                                    let publisher = membership.publish_every(gossip_interval(), move || {
                                        d.lock().unwrap().clone()
                                    });
                                    // The declarer (i143.e4.s11, R-S2): what this admin owes its authorities over `Status`, sent
                                    // when owed and again only on an eligible event, woken by its inputs; nothing it does gates
                                    // gossip or readiness.
                                    {
                                        let declarer = declarer.clone();
                                        let (digest, topology, records, me_id, incarnation, client, authority, status_storage) =
                                            (digest.clone(), control.topology.clone(), records.clone(), node_id.clone(), incarnation.clone(), node_rpc.client.clone(), authority.clone(), status_storage.clone());
                                        let rounds = rounds_slot.clone();
                                        let park = park_gate.clone();
                                        tasks.push(tokio::spawn(async move {
                                            loop {
                                                // Held here while the admin is parked: its own wait, outside every lock.
                                                let turn = park.turn("declarer").await;
                                                let addressed = records.wake.take_addressed();
                                                let view = topology.read().await.clone();
                                                // Named by the name it holds now: a start installs the name its join
                                                // answer hands (R12, R-JN1), and its declarations say so.
                                                let (me, own) = {
                                                    let d = digest.lock().unwrap();
                                                    (d.node.name.clone(), d.status)
                                                };
                                                let mesh_ids: BTreeMap<String, String> = records.meshes.lock().unwrap().iter().map(|(k, v)| (k.clone(), v.to_string())).collect();
                                                let own_mesh_id = records.meshes.lock().unwrap().get(&me.mesh).cloned();
                                                let own_mesh_held = own_mesh_id.and_then(|id| records.declared.lock().unwrap().meshes.get(&id).copied());
                                                // The fabric-primary owes the ready-for-traffic event from the durable fact: its mesh's
                                                // status rows on the store root, read here, latch the handle the derivation reads.
                                                if view.holds_fabric_seat(&me_id) {
                                                    if let Err(e) = declarer.read_first_ready(&*status_storage, &view).await {
                                                        tracing::info_span!("rdm.node_admin.fabric.reject.via-ready-row-unreadable", node = %me, fabric_id = %view.fabric.id, status = view.fabric.status.name(), detail = %e)
                                                            .in_scope(|| tracing::info!("the ready-for-traffic row of the fabric could not be read from this mesh's status rows: this round owes from the latch and the view"));
                                                    }
                                                }
                                                crate::status_declare::owed_from_view(&declarer, &me, &me_id, &incarnation, own, records.declare_gate(), own_mesh_held, &view, &mesh_ids);
                                                // A completion owed for a round the accepted Build moved past is withdrawn before anything is sent.
                                                if let Some(rounds) = rounds.get() {
                                                    rounds.withdraw_superseded().await;
                                                }
                                                declarer.round(&me, &me_id, &view, &client, authority.get().map(|a| a.as_ref()), &addressed).await;
                                                drop(turn);
                                                // One coalesced wake from the inputs, never a timer: a poke during the round is
                                                // the stored permit, so the next round runs at once; a burst runs one.
                                                records.wake.woken().await;
                                            }
                                        }));
                                    }

                                    // The offline tickle (node-recovery.md "Detection and proof"): every admin keeps the silence
                                    // bookkeeping; the one holding its mesh's primary seat tickles each node of its own mesh the
                                    // topology shows PendingReconnect, half a floor after the mark: a ping (op 1), and when it
                                    // answers the status kick (op 27); when it does not, the same kick through a live node of its
                                    // mesh on the ViaPeer route. The node's status lands on its row in nodes.storage and in the view.
                                    {
                                        let (topology, records, status_storage, runner, me, me_id, mesh) = (control.topology.clone(), records.clone(), status_storage.clone(), runner.clone(), name.clone(), node_id.clone(), cfg.mesh.clone());
                                        let frozen = shutdown_control.clone();
                                        // The recovery of a TRUE OFFLINE member of this mesh (node-recovery.md): the provider's
                                        // exact-exit proof, the exit-proof row, the submit to the fabric-primary.
                                        let recovery = RecoverySubmitter {
                                            me: name.clone(),
                                            topology: control.topology.clone(),
                                            book: book.clone(),
                                            provider: runner.provider.clone(),
                                            nodes_storage: nodes_storage.clone(),
                                            status_storage: status_storage.clone(),
                                            door: build_slot.clone(),
                                            builds: control.builds.clone(),
                                        };
                                        let park = park_gate.clone();
                                        let mut cadence = crate::offline::Cadence::every(gossip_interval())?;
                                        tasks.push(tokio::spawn(async move {
                                            // The births whose inspection or submitted recovery is in flight, and the last verdict
                                            // named per birth. The tick never waits on either.
                                            let in_flight: Arc<Mutex<HashSet<(NodeId, IncarnationId)>>> = Arc::default();
                                            let verdicts: Arc<Mutex<HashMap<(NodeId, IncarnationId), Inspected>>> = Arc::default();
                                            let mut tickle = crate::offline::OfflineTickle::observed_by(me.to_string(), park.clone());
                                            let floor = rafka_mesh_transport::membership::staleness_floor();
                                            let mut written: WrittenSilence = HashMap::new();
                                            loop {
                                                // Held here while the admin is parked: its own wait, outside every lock.
                                                let turn = park.turn("offline-tickle").await;
                                                let view = topology.read().await.clone();
                                                let holds_seat = view.holds_mesh_seat(&mesh, &me_id);
                                                // Every admin watches the same set, the seat holder or not, so a successor tickles on
                                                // the window already running; the durable rows are read each tick (one point in time).
                                                let durable = {
                                                    let rows = recovery.durable_rows().await;
                                                    let planned = records.planned();
                                                    durable_births(&view, &rows, &|p| planned.get(&p.mesh).is_some_and(|m| m.contains(p)), &|id| recovery.book.is_departed(id))
                                                };
                                                let watched = watched_births(&view, &me_id, &mesh, &durable);
                                                // One reading of the clocks for the tick: each birth's mark is read from the book at it.
                                                let (now, now_ms) = (std::time::Instant::now(), local_now_ms());
                                                let silent = crate::offline::silent_set(watched.values(), &recovery.book, now, now_ms, floor);
                                                let client = Some(runner.node_rpc.client.clone());
                                                // The tickle: ask the exact birth to reassert itself.
                                                // Protocol name: ProbeNodeState.
                                                let kick_of = |n: &Node, incarnation: &crate::model::IncarnationId| rafka_node_rpc_contract::status::StatusRequest::ProbeNodeState { node_id: n.node_id.clone(), incarnation: incarnation.clone() };
                                                let own_id = me_id.clone();
                                                let in_view: HashSet<String> = view.members().map(|n| n.node_id.to_string()).collect();
                                                let report = tickle
                                                    .tick(
                                                        now_ms,
                                                        holds_seat,
                                                        silent,
                                                        |id: &str| in_view.contains(id),
                                                        floor.as_millis() as u64 / 2,
                                                        floor.as_millis() as u64,
                                                        |id| {
                                                            let (client, node) = (client.clone(), watched.get(&id).cloned());
                                                            async move {
                                                                let Some(node) = node else { return Err(format!("{id} is not in the topology")) };
                                                                let Some(client) = client else { return Err("this admin has no Node RPC client".to_string()) };
                                                                let target = rafka_node_rpc::NodeTarget::ExactNode(node.node_id.clone());
                                                                let ping = rafka_node_rpc_contract::ping::PingRequest::Ping { payload: b"tickle".to_vec() };
                                                                let (out, _) = client.call::<rafka_node_rpc_contract::ping::Ping>(&target, &ping, &rafka_node_rpc::CallOptions::default()).await;
                                                                if out.reply().is_none() {
                                                                    return Err(format!("{} did not answer the ping: {}", node.name, out.name()));
                                                                }
                                                                // The ping answered: the status kick. The node re-publishes its presence
                                                                // and answers its status.
                                                                let Some(incarnation) = node.incarnation_id.clone() else { return Err(format!("{} ({}) holds no recorded birth: there is no exact birth to tickle", node.name, node.node_id)) };
                                                                let birth = rafka_node_admin_client::ExactBirth { target: target.clone(), node_id: node.node_id.clone(), incarnation };
                                                                let kick = match rafka_node_admin_client::NodeRpc::new(&client).get(&birth, &rafka_node_rpc::CallOptions::default()).await {
                                                                    Ok(reply) => reply.name(),
                                                                    Err(end) => end.rpc_name(),
                                                                };
                                                                tracing::info!(node = %node.name, kick, "status kick after an answered ping");
                                                                Ok(())
                                                            }
                                                        },
                                                        |id| {
                                                            let (client, node) = (client.clone(), watched.get(&id).cloned());
                                                            // Carriers from the topology: every ready member of the target's mesh
                                                            // other than the target and this admin, whatever its kind (every node
                                                            // serves the core Forward op).
                                                            let carriers: Vec<Node> = view
                                                                .members()
                                                                .filter(|c| node.as_ref().is_some_and(|n| c.mesh == n.mesh && c.node_id != n.node_id) && c.node_id != own_id && c.status == NodeStatus::ReadyForTraffic)
                                                                .take(crate::offline::VIA_PEER_TICKLE_FANOUT)
                                                                .cloned()
                                                                .collect();
                                                            async move {
                                                                let (Some(node), Some(client)) = (node, client) else { return crate::offline::ViaPeerVerdict::CandidatesUnreadable(format!("{id} is not in the topology, or this admin has no Node RPC client")) };
                                                                let Some(incarnation) = node.incarnation_id.clone() else {
                                                                    return crate::offline::ViaPeerVerdict::CandidatesUnreadable(format!("{} ({}) holds no recorded birth: there is no exact birth to probe", node.name, node.node_id));
                                                                };
                                                                // Every probe names the same exact birth: one kick, by node id and incarnation.
                                                                let kick = kick_of(&node, &incarnation);
                                                                let (node, client, kick) = (&node, &client, &kick);
                                                                let peers: Vec<_> = carriers
                                                                    .iter()
                                                                    .map(|c| {
                                                                        let route = rafka_node_rpc::RouteChoice::ViaPeer { carrier: c.node_id.clone(), path: c.name.clone() };
                                                                        let probe = async move {
                                                                            let (out, _, _) = client.call_routed::<rafka_node_rpc_contract::status::Status>(&node.node_id, &route, kick, &rafka_node_rpc::CallOptions::default()).await;
                                                                            out.reply().map(|r| r.value().clone())
                                                                        };
                                                                        (c.name.to_string(), probe)
                                                                    })
                                                                    .collect();
                                                                crate::offline::via_peer_verdict(&node.node_id, &incarnation, peers).await
                                                            }
                                                        },
                                                        |id| {
                                                            // One read of iroh's local view of the node, recorded beside the event.
                                                            let (endpoint, key) = (runner.endpoint.clone(), watched.get(&id).and_then(|n| n.endpoint_id.as_ref()).and_then(|k| k.0.parse::<iroh::PublicKey>().ok()));
                                                            async move {
                                                                match endpoint {
                                                                    Some(ep) => rafka_mesh_transport::iroh_obs::observe_remote(&ep, key).await,
                                                                    None => rafka_mesh_transport::iroh_obs::IrohObservation::unavailable(),
                                                                }
                                                            }
                                                        },
                                                    )
                                                    .await;
                                                // The birth's own silence row is put blind, keyed by the exact birth this admin
                                                // holds now; no other row is read or touched. What this loop last put is held here,
                                                // so an unchanged status is not put again.
                                                let write = async |id: &str, status: Option<NodeStatus>, written: &mut HashMap<NodeId, (Option<crate::model::IncarnationId>, Option<NodeStatus>)>| {
                                                    let held = topology.read().await.births().find(|n| n.node_id.as_str() == id).cloned();
                                                    let Some(node) = held.or_else(|| watched.get(id).cloned()) else { return };
                                                    let fact = (node.incarnation_id.clone(), status);
                                                    if written.get(&node.node_id) == Some(&fact) {
                                                        return;
                                                    }
                                                    // A refusal is spanned where it happened (`offline::put_silence_status`); the memo
                                                    // keeps the last status that stands, and nothing below waits on this row.
                                                    if crate::offline::put_silence_status(&*status_storage, &node, status).await.is_ok() {
                                                        written.insert(node.node_id.clone(), fact);
                                                    }
                                                };
                                                if holds_seat {
                                                    for (id, n) in &watched {
                                                        if n.status == NodeStatus::PendingReconnect {
                                                            write(id, Some(NodeStatus::PendingReconnect), &mut written).await;
                                                        }
                                                    }
                                                }
                                                for id in &report.offline {
                                                    if let Some(n) = watched.get(id) {
                                                        if let Some(inc) = n.incarnation_id.clone() {
                                                            records.offline.lock().unwrap().insert((n.node_id.clone(), inc));
                                                        }
                                                    }
                                                    write(id, Some(NodeStatus::Dead), &mut written).await;
                                                }
                                                for id in &report.returned {
                                                    write(id, None, &mut written).await;
                                                }
                                                retire_unwatched(&watched, &mut records.offline.lock().unwrap(), &mut verdicts.lock().unwrap(), &mut written);
                                                // node-recovery.md: on TRUE OFFLINE the mesh primary asks the provider about the exact
                                                // runtime of each member of its own mesh, its secondary node-admins included
                                                // (mesh-recovery.md:8-9); Exited is the proof, and the
                                                // recovery is submitted keyed on the exited birth. Re-evaluated every tick while the
                                                // member stays TRUE OFFLINE, so a recovery that failed is submitted again.
                                                {
                                                    let offline = records.offline.lock().unwrap().clone();
                                                    for n in recovery_due(holds_seat, frozen.held().is_some(), &mesh, &watched, &offline) {
                                                        let Some(incarnation) = n.incarnation_id.clone() else { continue };
                                                        let birth = (n.node_id.clone(), incarnation);
                                                        if !in_flight.lock().unwrap().insert(birth.clone()) {
                                                            continue;
                                                        }
                                                        let (recovery, n, verdicts) = (recovery.clone(), n.clone(), verdicts.clone());
                                                        crate::offline::spawn_recovery_follower(&park, in_flight.clone(), birth, async move {
                                                            recovery.recover_if_proven(&n, &verdicts).await;
                                                        });
                                                    }
                                                    // node-recovery.md "Failure and receipts": an exited birth whose workflow in the
                                                    // accepted Build stopped at a Failed attempt is submitted again, read from the
                                                    // durable facts each tick, whether or not the view still holds it (a workflow
                                                    // whose delete completed departed it) and whoever submitted it before. One submit
                                                    // per birth in flight, whichever way it is due.
                                                    for owed in recovery.owed_submits(holds_seat, frozen.held().is_some(), &mesh).await {
                                                        let birth = (owed.node_id.clone(), owed.incarnation.clone());
                                                        if !in_flight.lock().unwrap().insert(birth.clone()) {
                                                            continue;
                                                        }
                                                        let recovery = recovery.clone();
                                                        crate::offline::spawn_recovery_follower(&park, in_flight.clone(), birth, async move {
                                                            if let Err(e) = recovery.submit_owed(&owed).await {
                                                                tracing::info!(node = %owed.path, error = %e, "the submit of an owed exit ended without the recovery");
                                                            }
                                                        });
                                                    }
                                                }
                                                drop(turn);
                                                cadence.next().await;
                                            }
                                        }));
                                    }

                                    // The seat watch (ruling R-A2): a silent seat holder is looked at, never replaced for being
                                    // silent. A mesh primary says the fabric primary's exact birth is silent (a Concern); the birth
                                    // answers it; an admin of its mesh looks at it, down to its exact runtime.
                                    {
                                        let watch = crate::seat_watch::Watch {
                                            gate: park_gate.clone(),
                                            me: name.clone(),
                                            me_id: node_id.clone(),
                                            incarnation: incarnation.clone(),
                                            membership: membership.clone(),
                                            backbone: backbone.clone(),
                                            topology: control.topology.clone(),
                                            records: records.clone(),
                                            looker: crate::seat_watch::Looker { me: name.clone(), me_id: node_id.clone(), membership: membership.clone(), client: node_rpc.client.clone(), provider: runner.provider.clone(), nodes: Some(nodes_storage.clone()) },
                                            republish: republish.clone(),
                                            builds: builds.clone(),
                                        };
                                        // Not behind the gate: it reads the view; it is held at its own wait (`Watch::gate`).
                                        tasks.push(tokio::spawn(crate::seat_watch::run(watch)));
                                    }

                                    // The open lifecycle overlays, derived from the Build facts inside each append to this admin's
                                    // log (its own appends and the facts it absorbs from the fabric alike), before the append
                                    // returns: a hold opens with its pre-event's receipt and closes with its operation's `Complete`
                                    // or its attempt's `Converged`, and no reader of the log sees that fact while the book still
                                    // holds the hold. A successor primary publishes the same overlays from the same facts. Derived
                                    // once over the facts held before the hierarchy's first round, then on every fact.
                                    {
                                        let (book, named) = (membership.book.clone(), Mutex::new(NamedSteps::new()));
                                        journal.derive_on_every_fact(Arc::new(move |facts| overlays_from_facts(facts, &book, &mut named.lock().unwrap()))).await;
                                    }

                                    let hierarchy;
                                    let anti_entropy;
                                    // What the hierarchy hands the Members anti-entropy: a role it took is held for
                                    // writing until its seat is announced (`roles`), the anti-entropy is told once it
                                    // is (`roles_moved`).
                                    let roles = Arc::new(tokio::sync::RwLock::new(()));
                                    let roles_moved = Arc::new(tokio::sync::Notify::new());
                                    // The hierarchy decides this admin's roles: its mesh's primary forwards the other
                                    // meshes onto its mesh channel; the fabric primary publishes the fabric's status and
                                    // climbs the fabric states. It joins the backbone through every admin it knows and
                                    // its mesh channel through every member of its mesh it knows. It publishes no
                                    // Members aggregate: that is the anti-entropy's timer, below.
                                    {
                                        let (roles, roles_moved) = (roles.clone(), roles_moved.clone());
                                        let (backbone, membership, topology, me, mesh, builds) = (backbone.clone(), membership.clone(), control.topology.clone(), name.clone(), cfg.mesh.clone(), builds.clone());
                                        let (view_fabric, view_fabric_id, view_provider, view_book) = (cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, book.clone());
                                        let (seat_node_id, seat_incarnation) = (node_id.clone(), incarnation.clone());
                                        let adapter = runner.builds.clone();
                                        let (records, rpc_client) = (records.clone(), node_rpc.client.clone());
                                        let fabric_rounds = rounds_slot.get().cloned().expect("the rounds are built before the hierarchy runs");
                                        let park = park_gate.clone();
                                        hierarchy = tokio::spawn(async move {
                                            // The round a new primary runs before it republishes the status it adopted (`crate::round`).
                                            let mut rounds = crate::round::RoundDriver::new(me.clone(), seat_node_id.clone());
                                            let mut was_fabric_primary = false;
                                            let mut was_mesh_primary = false;
                                            let mut unreadable_named: Option<String> = None;
                                            loop {
                                                // Held here while the admin is parked: its own wait, outside every lock.
                                                let turn = park.turn("hierarchy").await;
                                                // The current Build, read once per turn. A builds store that cannot be read is
                                                // named once per distinct error and decides nothing this turn: no planned births,
                                                // accepted meshes or round step are taken from a read that failed.
                                                let current = match adapter.current().await {
                                                    Ok(c) => {
                                                        unreadable_named = None;
                                                        Some(c)
                                                    }
                                                    Err(e) => {
                                                        let text = e.to_string();
                                                        if unreadable_named.as_deref() != Some(text.as_str()) {
                                                            tracing::info_span!("rdm.node_admin.build.reject.via-build-store-unreadable", route = "hierarchy", node = %me, error = %text)
                                                                .in_scope(|| tracing::info!("the current Build could not be read: this turn takes no planned births, accepted meshes or round step from it"));
                                                            unreadable_named = Some(text);
                                                        }
                                                        None
                                                    }
                                                };
                                                // The current Build's planned births are what a mesh's readiness is counted over.
                                                if let Some(Some(b)) = &current {
                                                    records.set_planned(&b.topology);
                                                }
                                                records.set_heard_ready(records.planned().keys().chain(std::iter::once(&mesh)).filter(|m| membership.mesh_status(m).is_some_and(|h| h.status == rafka_mesh_transport::membership::MESH_READY_FOR_TRAFFIC)).cloned().collect());
                                                // A MeshStatus Dead is bound to the mesh id its publisher's own live digest names.
                                                records.set_heard_dead(membership.mesh_statuses().into_iter().filter(|(_, h)| h.status == "dead").map(|(m, h)| (m, h.publisher, h.changed_at_rafka_ms)).collect(), |mesh, publisher| {
                                                    membership.book.all().into_iter().find(|d| d.node.name.mesh == mesh && d.node.name.to_string() == publisher).and_then(|d| d.mesh_id)
                                                });
                                                // Decide on the view as it is now: a cached view can predate the seat record this admin holds
                                                // (a hand-over just taken), and a seat is announced from the decision, never from a stale one.
                                                // The seat records are read once per decision: the projection elects from them and
                                                // the announcement below supersedes only them (`seat_announcements`).
                                                // Read under the view's write lock, as the projection always was: the installed
                                                // view is never older than the records it replaces.
                                                let mut decided = crate::election::Incumbency::default();
                                                let t = records
                                                    .install_read(&me, &topology, &view_book, &view_fabric_id, |reads| {
                                                        decided = incumbency_of(&view_book, &records);
                                                        project_decided(&view_fabric, &view_fabric_id, view_provider, &view_book, &records, &decided, reads)
                                                    })
                                                    .await;
                                                // The accepted Build is the authority on which meshes the fabric has: the book
                                                // takes no old aggregate of a mesh it does not name, and a mesh it does not name
                                                // that has no member left keeps no recorded id (a name added again is a new mesh).
                                                if let Some(Some(b)) = &current {
                                                    membership.book.set_accepted_meshes(b.topology.meshes.keys().cloned().collect());
                                                    records.meshes.lock().unwrap().retain(|name, _| b.topology.meshes.contains_key(name) || t.members().any(|n| &n.mesh == name));
                                                }
                                                // Cut off, or within one silence window of healing, its view
                                                // authorizes nothing: it publishes as no primary.
                                                let live = membership.authorizes();
                                                let mesh_primary = live && t.holds_mesh_seat(&mesh, &seat_node_id);
                                                // The mesh's rows on the shared root follow this seat (`MeshRowsWriter`).
                                                own_seat_tx.send_if_modified(|held| std::mem::replace(held, mesh_primary) != mesh_primary);
                                                let fabric_primary = live && t.holds_fabric_seat(&seat_node_id);
                                                // A role taken or given up this round is held from the anti-entropy until its
                                                // seat is announced below: a role's first aggregate follows its seat record.
                                                let announcing = if (mesh_primary, fabric_primary) != (was_mesh_primary, was_fabric_primary) { Some(roles.write().await) } else { None };
                                                was_mesh_primary = mesh_primary;
                                                backbone.set_mesh_primary(mesh_primary);
                                                backbone.set_fabric_primary(fabric_primary);
                                                // Taking the fabric-primary role while the fabric is degraded adopts degraded: the
                                                // new holder authors no ready on a topology reading.
                                                if fabric_primary && !was_fabric_primary {
                                                    let adopted = membership.fabric_status().is_some_and(|s| s.status == "degraded");
                                                    records.set_adopted_degraded(adopted);
                                                }
                                                if fabric_primary {
                                                    records.set_published_degraded(false);
                                                } else {
                                                    // The seat is released in one place and one order: the published `degraded` is
                                                    // held before the authority records it replaces are cleared, so no projection
                                                    // between the two answers ready for a fabric the holder published degraded.
                                                    records.set_published_degraded(membership.fabric_status().is_some_and(|s| s.status == "degraded"));
                                                    records.set_peer_recovery(None);
                                                    records.set_adopted_degraded(false);
                                                }
                                                was_fabric_primary = fabric_primary;
                                                // The fabric-primary's climb through the fabric states, one non-blocking turn per round
                                                // of this loop; every other admin serves the state the holder published.
                                                fabric_rounds.climb_step(crate::fabric_rounds::ClimbInputs { is_fabric_primary: fabric_primary, heard: membership.fabric_status().as_ref().map(|f| f.status.as_str()), view: &t }).await;
                                                // A seat taken is announced once, when it changes hands: the mesh primary first, then
                                                // the fabric primary (R-A2).
                                                for (seat, holder) in seat_announcements(&t, live, &mesh, (&seat_node_id, &seat_incarnation), &decided) {
                                                    backbone.announce_seat(seat, holder).await;
                                                }
                                                if let Some(announced) = announcing {
                                                    drop(announced);
                                                    roles_moved.notify_one();
                                                }
                                                let heard = membership.book.current(membership.book.staleness_floor());
                                                let mine: Vec<MeshDigest> = backbone.own_members(&membership);
                                                let status_of = |s: crate::model::ScopeStatus| serde_json::to_value(s).ok().and_then(|v| v.as_str().map(String::from)).unwrap_or_default();
                                                // A status is sent when it changed, never per round (gossip.md §3.2).
                                                let mesh_status = t.meshes.iter().find(|m| m.name == mesh).map(|m| status_of(m.status)).unwrap_or_default();
                                                // The fabric's status as the live records state it: a round that just cleared the adopted
                                                // degraded must not be answered by the older projection `t`.
                                                // The fabric-primary publishes the state it holds now, not the projection's last refresh:
                                                // every state the climb enters is its own frame.
                                                let held_state = records.fabric_state();
                                                let fabric_status = |records: &Records| {
                                                    let basis = if fabric_primary { held_state.scope() } else { t.fabric.status };
                                                    match basis {
                                                        crate::model::ScopeStatus::Degraded | crate::model::ScopeStatus::ReadyForTraffic => {
                                                            status_of(if records.adopted_degraded() || records.peer_recovery().is_some() { crate::model::ScopeStatus::Degraded } else { crate::model::ScopeStatus::ReadyForTraffic })
                                                        }
                                                        other => status_of(other),
                                                    }
                                                };
                                                let mut announced = backbone.announce_statuses(&mesh_status, &fabric_status(&records), false, false).await;
                                                // A status adopted from the previous publisher is republished only when its round is
                                                // complete: the planned births checked in, every mesh primary reported.
                                                if announced.mesh_awaiting_round || announced.fabric_awaiting_round {
                                                    // An unreadable builds store steps no round: the adopted status waits, named above.
                                                    if let Some(current) = &current {
                                                        let planned = current.as_ref().map(|b| crate::round::planned(&b.topology, &mesh));
                                                        let done = rounds.step(announced, crate::round::Inputs { view: &t, planned, book: &membership.book, client: Some(&rpc_client), records: &records }).await;
                                                        if done.mesh || done.fabric {
                                                            announced = backbone.announce_statuses(&mesh_status, &fabric_status(&records), done.mesh, done.fabric).await;
                                                        }
                                                    }
                                                } else {
                                                    rounds.end();
                                                }
                                                // The state just announced is the one the climb may move on from.
                                                if records.set_published_fabric_state((fabric_primary && !announced.fabric_awaiting_round).then_some(held_state)) && fabric_primary {
                                                    fabric_rounds.poke_climb();
                                                }
                                                records.set_declare_gate(mesh_primary && !announced.mesh_awaiting_round);
                                                // R-S2: a mesh primary the fabric-primary's view gained or changed is sent the down op.
                                                rounds.address_changed_mesh_primaries(&t, fabric_primary, Some(&rpc_client));
                                                let addr = rafka_mesh_transport::membership::gossip_addr;
                                                let admins: Vec<_> = heard.iter().filter(|d| d.node.name.kind == NodeKind::NodeAdmin && d.node.name != me).filter_map(addr).collect();
                                                builds.join_admins(admins.clone()).await;
                                                backbone.join_admins(admins).await;
                                                let _ = membership.join_peers(mine.iter().filter(|d| d.node.name != me).filter_map(addr).collect()).await;
                                                drop(turn);
                                                // The next round is due after one gossip interval, or at once when a checklist the
                                                // fabric-primary waits on, or the application's answer, moved, or when the view changes
                                                // a role or a status it announces: this admin takes or loses a seat, or its mesh's
                                                // status moves (gossip.md §3.2, a status change is sent at once).
                                                let round_wake = tokio::time::sleep_until(tokio::time::Instant::now() + gossip_interval());
                                                tokio::pin!(round_wake);
                                                loop {
                                                    tokio::select! {
                                                        _ = &mut round_wake => break,
                                                        _ = fabric_rounds.climb_due() => break,
                                                        _ = records.view_changed.notified() => {
                                                            let now = topology.read().await.clone();
                                                            let live = membership.authorizes();
                                                            let (mp, fp) = (live && now.holds_mesh_seat(&mesh, &seat_node_id), live && now.holds_fabric_seat(&seat_node_id));
                                                            let status = now.meshes.iter().find(|m| m.name == mesh).map(|m| status_of(m.status)).unwrap_or_default();
                                                            if (mp, fp) != (mesh_primary, fabric_primary) || status != mesh_status {
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        });
                                    }
                                    // The Members anti-entropy (gossip.md §3.2, "PERPETUAL TIMERS … Members anti-entropy —
                                    // the mesh primary on the backbone; also sent at once on a material change"): its own
                                    // timer, never a step of the hierarchy's round. It reads the book and the roles the
                                    // hierarchy decided, nothing else, so no store read or write of the round (the
                                    // current Build, the exit rows, the view's installation, the climb) delays
                                    // it: a peer's t of this mesh (mesh-investigation.md) is the publication's own
                                    // interval, never the store's. While this admin is its mesh's primary it publishes
                                    // its mesh's members on the backbone (and puts the same source into its own mesh);
                                    // no admin publishes a live mesh on its behalf (R-RX4). While it is the fabric
                                    // primary it publishes each deleted mesh whose last admin it stopped or deleted
                                    // (mesh-delete.md step 7). Held at its own wait while the admin is parked.
                                    {
                                        let (backbone, membership, park) = (backbone.clone(), membership.clone(), park_gate.clone());
                                        anti_entropy = tokio::spawn(async move {
                                            loop {
                                                let turn = park.turn("anti-entropy").await;
                                                {
                                                    let _announced = roles.read().await;
                                                    backbone.publish(&membership, backbone.own_members(&membership)).await;
                                                    backbone.publish_deleted_meshes(&membership).await;
                                                }
                                                drop(turn);
                                                tokio::select! {
                                                    _ = backbone.round_wake(&membership, tokio::time::Instant::now() + rafka_mesh_transport::membership::backbone_gossip_interval()) => {}
                                                    _ = roles_moved.notified() => {}
                                                }
                                            }
                                        });
                                    }
                                    // The projection, refreshed into the control plane's topology.
                                    {
                                        let (topology, fabric, fabric_id, provider, book, records) = (control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, book.clone(), records.clone());
                                        let elections = ElectionLog::new(name.clone());
                                        let me = name.clone();
                                        // Not spawned behind the gate: the rebuild takes the view's lock, which every handler of a
                                        // parked admin reads (its start among them). It is held at its own wait instead
                                        // (`view_rebuild_due`), where it holds nothing.
                                        let rebuild_gate = park_gate.clone();
                                        tasks.push(tokio::spawn(async move {
                                            let mut held: BTreeSet<PathName> = BTreeSet::new();
                                            let mut facts = book.routing_watch();
                                            let mut turn = rebuild_gate.turn("view-rebuild").await;
                                            loop {
                                                facts.seen();
                                                let t = records.install_read(&me, &topology, &book, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &book, &records, reads)).await;
                                                elections.observe(&t);
                                                // A name this view held and now lacks: the exclusion is named from the same
                                                // inputs at the moment it happens, not reconstructed later.
                                                let now: BTreeSet<PathName> = t.members().map(|n| n.name.clone()).collect();
                                                for gone in held.difference(&now) {
                                                    tracing::info_span!("rdm.node_admin.topology.update.via-vanished-from-view", node = %me, name = %gone, why = %describe_absence(&book, &records, gone))
                                                        .in_scope(|| tracing::info!("a name this admin's view held is excluded by the projection now"));
                                                }
                                                held = now;
                                                drop(turn);
                                                turn = view_rebuild_due(&mut facts, VIEW_POLL, &rebuild_gate).await;
                                            }
                                        }));
                                    }
                                    // The peer-mesh investigation (`crate::investigate`): while this admin is the fabric primary,
                                    // a peer mesh it stops hearing on the backbone is probed through its own members and reborn
                                    // only when one of them cannot reach its own node-admin. Its decision is what releases the
                                    // drift check below to replace that mesh's node-admins.
                                    let ladder = Arc::new(Mutex::new(crate::investigate::Ladder::new(crate::investigate::Rungs::configured())));
                                    {
                                        // The R-D1 fence as the Build door and the drive read it: t from this admin's last direct
                                        // backbone receipt of the mesh, whether or not the ladder has stepped the mesh yet.
                                        let (ladder, book) = (ladder.clone(), book.clone());
                                        let _ = control.peer_mesh.set(Arc::new(move |mesh: &str| ladder.lock().unwrap().fence(mesh, book.mesh_unheard(mesh, std::time::Instant::now()))));
                                    }
                                    {
                                        let held = connections.held();
                                        let watch = crate::investigate::Watch {
                                            gate: park_gate.clone(),
                                            me: name.clone(),
                                            me_id: node_id.clone(),
                                            round: rafka_mesh_transport::membership::backbone_gossip_interval(),
                                            membership: membership.clone(),
                                            topology: control.topology.clone(),
                                            records: records.clone(),
                                            settled: { let builds = builds_dyn.clone(); Arc::new(move || { let builds = builds.clone(); Box::pin(async move { builds.current().await.map(|c| c.is_some_and(|b| !matches!(b.state, crate::build_state::BuildState::Pending | crate::build_state::BuildState::Running))).map_err(|e| e.to_string()) }) }) },
                                            client: node_rpc.client.clone(),
                                            connected: Arc::new(move |id: &NodeId| held.lock().unwrap().own_latest_directs().iter().any(|d| &d.destination.node_id == id && d.state == rafka_mesh_entity::connections::ConnectionState::Connected)),
                                            ladder: ladder.clone(),
                                            carriers: Mutex::new(BTreeMap::new()),
                                        };
                                        // Not behind the gate: it reads the view; it is held at its own wait (`Watch::gate`).
                                        tasks.push(tokio::spawn(crate::investigate::run(watch)));
                                    }
                                    let executor;
                                    // The reconcile loop: the drift check, and the fabric-primary's drive of every active Build
                                    // (`build_drive`). Only the fabric-primary drives; an executor runs what it is called with
                                    // (`build_run`) and picks nothing up from its projection. A launched admin starts it only after
                                    // its entry pull, so its first view is its launcher's.
                                    {
                                        let (topology, submitted) = (control.topology.clone(), control.build_submitted.clone());
                                        let (book, records, fabric, fabric_id, provider, cut_off_view) = (book.clone(), records.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, membership.clone());
                                        let (me, me_id, deployer, drift_builds, drift_contexts) = (name.clone(), node_id.clone(), runner.provider.clone(), builds_dyn.clone(), attempt_contexts.clone());
                                        let drift_admission = control.admission.clone();
                                        let (drives, drive_env) = (drives.clone(), drive_env.clone());
                                        let frozen = shutdown_control.clone();
                                        let drift_time = rafka_time.clone();
                                        let drift_nodes = nodes_storage.clone();
                                        let ladder = ladder.clone();
                                        let park = park_gate.clone();
                                        executor = tokio::spawn(async move {
                                            let mut started = HashSet::new();
                                            let mut held = HashSet::new();
                                            let exit_verdicts = ExitVerdicts::default();
                                            loop {
                                                // Held here while the admin is parked: its own wait, outside every lock.
                                                let turn = park.turn("executor").await;
                                                // A fabric shutdown freezes reconciliation: no Build attempt, no drift recovery, no
                                                // rebirth from here on (fabric-mesh-lifecycle.md §11.1).
                                                if frozen.held().is_some() {
                                                    return;
                                                }
                                                // Decide on, and plan from, the view as it is now: a cached view
                                                // can predate the members that make another admin primary.
                                                let now = records.install_read(&me, &topology, &book, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &book, &records, reads)).await;
                                                // Every Build whose next operation this admin executes; none
                                                // while it is cut off or within one silence window of healing
                                                // (its view then authorizes nothing). The fabric authority first
                                                // starts a reconciliation Build for proven drift.
                                                if cut_off_view.authorizes() {
                                                    let durable_rows = if now.holds_fabric_seat(&me_id) { drift_nodes.runtimes().await.unwrap_or_default() } else { Vec::new() };
                                let opened =                     reconcile_drift(&drift_time, &me, &me_id, &now, &book, &*deployer, &*drift_builds, &drift_contexts, &drift_admission, &durable_rows, &mut started, &mut held, &exit_verdicts, &|mesh| ladder.lock().unwrap().fence(mesh, book.mesh_unheard(mesh, std::time::Instant::now())).and_then(|p| p.unheard_ms), &|mesh| book.backbone_meshes().contains(mesh) && !ladder.lock().unwrap().rebirth_decided(mesh), &|mesh| ladder.lock().unwrap().rebirth_decided(mesh), &records).await;
                                                    // The proofs the pass just took are in the view the attempt is planned from: a birth
                                                    // proven exited is not live there, and holds no seat.
                                                    if opened.is_some() {
                                                        records.install_read(&me, &topology, &book, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &book, &records, reads)).await;
                                                    }
                                                    if now.holds_fabric_seat(&me_id) {
                                                        crate::build_op::ensure_active(&drives, &drive_env).await;
                                                    }
                                                }
                                                drop(turn);
                                                tokio::select! {
                                                    _ = submitted.notified() => {}
                                                    _ = tokio::time::sleep(Duration::from_millis(300)) => {}
                                                }
                                            }
                                        });
                                    }
                                    // The stopped-node garbage collector (fabric-node-lifecycle.md §6.9): a sweep every ttl, by the
                                    // fabric-primary only, while its view authorizes and no fabric shutdown froze reconciliation.
                                    // A blank ttl runs no sweep. The sweep submits one ordinary node.delete Build and deletes nothing.
                                    if let Some(ttl) = cfg.stopped_node_ttl {
                                        let (control, frozen, cut_off_view) = (control.clone(), shutdown_control.clone(), membership.clone());
                                        let park = park_gate.clone();
                                        tasks.push(tokio::spawn(async move {
                                            loop {
                                                tokio::time::sleep(ttl).await;
                                                // Held here while the admin is parked: its own wait, outside every lock.
                                                let turn = park.turn("stopped-sweep").await;
                                                if frozen.held().is_some() {
                                                    return;
                                                }
                                                if cut_off_view.authorizes() {
                                                    let swept = control.sweep_stopped(Some(ttl)).await;
                                                    tracing::debug!(?swept, "stopped-node sweep");
                                                }
                                                drop(turn);
                                            }
                                        }));
                                    }
                                    // A fabric shutdown: freeze (the executor stops, this admin says Draining), then this admin's
                                    // part of the drain. The fabric-primary stops itself last by waking the shutdown route.
                                    {
                                        let (abort, digest_w, membership_w, me, me_id, records_w, runs_w) = (executor.abort_handle(), digest.clone(), membership.clone(), name.clone(), node_id.clone(), records.clone(), runs.clone());
                                        let (seen, drain_control) = (shutdown_control.subscribe(), shutdown_control.clone());
                                        let stopper: Arc<dyn crate::shutdown::Stopper> = Arc::new(AdminStopper { runner: runner.clone(), topology: control.topology.clone() });
                                        let done = control.shutdown.clone();
                                        let _ = control.fabric_shutdown.set(Arc::new(crate::control_plane::ShutdownSeat {
                                            control: shutdown_control.clone(),
                                            me: name.clone(),
                                            node_id: node_id.clone(),
                                        }));
                                        let park = park_gate.clone();
                                        tasks.push(tokio::spawn(async move {
                                            let mut seen = seen;
                                            while seen.borrow_and_update().is_none() {
                                                if seen.changed().await.is_err() {
                                                    return;
                                                }
                                            }
                                            // Held here while the admin is parked: its own wait, outside every lock.
                                            let turn = park.turn("shutdown-drain").await;
                                            // The seats stay as they were before any admin drained (FML-26): held before this
                                            // admin says Draining, so no view of its own moves them.
                                            records_w.hold_seats();
                                            abort.abort();
                                            runs_w.abort_all();
                                            let d = {
                                                let mut d = digest_w.lock().unwrap();
                                                d.status = MemberStatus::Draining;
                                                d.clone()
                                            };
                                            let _ = membership_w.publish(&d).await;
                                            // Its Draining reaches the other admins only as its mesh-primary sets it from
                                            // its report (its heartbeat carries no status): the declarer owes the report.
                                            records_w.wake.poke();
                                            tracing::info_span!("rdm.node_admin.fabric.update.via-shutdown-frozen", node = %me)
                                                .in_scope(|| tracing::info!("reconciliation frozen; Draining, reported to this admin's mesh-primary"));
                                            drop(turn);
                                            if crate::shutdown::drain(me, me_id, drain_control, stopper).await {
                                                done.notify_waiters();
                                            }
                                        }));
                                    }
                                    *out.lock().unwrap() = Some(Running { rafka_time: rafka_time.clone(), api_base, control, runner, membership, digest, node_rpc, backbone: backbone.clone(), router: iroh_router, rpc_server: rpc_server.clone(), publisher, executor, hierarchy, anti_entropy, tasks, start_tasks, leave_seam: wiring.leave_seam.take(), builds: builds.clone(), hydrate_cancel, hydration: hydration_cell.clone(), member_cert, gate: park_gate.clone(), node_self: own_commands.get().cloned().expect("this admin's command surface is set before it runs") });
                                    Ok::<_, String>(())
                                })
                            });
                            Ok::<_, String>((parts, Some(started)))
                        })
                    },
                );
                Ok::<_, String>((channel, joined_seam))
            })
        })
    };
    let mut start = crate::workflows::node_start::FirstStart {
        starting: Some(starting),
        joining: Some(joining),
        resolve: Some(resolve),
        digest: join_digest,
        enter: Some(enter),
        joined: None,
        hook_started: None,
    };
    crate::workflows::node_start::node_start(&mut start, crate::app_hydration::StartKind::FirstStart).await?;
    let running = out.lock().unwrap().take();
    running.ok_or_else(|| "the start ended without the node-admin it brought up".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT (node-recovery.md "Failure and receipts"): a recovery Build that ends `Failed { step,
    /// reason }` is recorded as that failure on the submit span's outcome, and the submit ends in an
    /// error so the mesh-primary's next tick finds the member still proven exited and submits again.
    /// A Complete terminal is recorded as complete, and a stream that ends with no terminal is named.
    /// CONTRACT: the mesh primary's per-birth bookkeeping (the TRUE OFFLINE set, the inspection
    /// verdicts, the silence-status memo) holds only births its view still watches at that exact
    /// incarnation. A birth deleted by its recovery (a new node.id at its path.name) and a birth
    /// superseded by a new incarnation of its node are retired; a watched TRUE OFFLINE birth keeps
    /// all three. Must NOT happen: entries of gone births accumulating, one per recovered birth.
    #[test]
    fn the_mesh_primarys_offline_bookkeeping_retires_deleted_and_superseded_births() {
        let watched_node = |path: &str| {
            let mut n = Node::allocated(path.parse().unwrap());
            n.incarnation_id = Some(IncarnationId::mint());
            n.status = NodeStatus::Dead;
            n
        };
        let kept = watched_node("mesh2.rpc.1");
        let restarted = watched_node("mesh2.rpc.2");
        let superseded = (restarted.node_id.clone(), IncarnationId::mint());
        let deleted = (NodeId::mint(), IncarnationId::mint());
        let kept_birth = (kept.node_id.clone(), kept.incarnation_id.clone().unwrap());
        let watched: BTreeMap<String, Node> = [&kept, &restarted].into_iter().map(|n| (n.node_id.to_string(), n.clone())).collect();
        let mut offline: HashSet<_> = [kept_birth.clone(), superseded.clone(), deleted.clone()].into();
        let exited = Inspected::Exited(Some(4), "provider");
        let mut verdicts: HashMap<_, _> = [(kept_birth.clone(), exited.clone()), (superseded.clone(), exited.clone()), (deleted.clone(), exited.clone())].into();
        let mut written: WrittenSilence = [(kept.node_id.clone(), (kept.incarnation_id.clone(), Some(NodeStatus::Dead))), (deleted.0.clone(), (Some(deleted.1.clone()), Some(NodeStatus::Dead)))].into();
        retire_unwatched(&watched, &mut offline, &mut verdicts, &mut written);
        assert_eq!(offline, [kept_birth.clone()].into(), "only the watched TRUE OFFLINE birth stays offline");
        assert_eq!(verdicts.keys().cloned().collect::<HashSet<_>>(), [kept_birth.clone()].into(), "a verdict lives as long as its birth is TRUE OFFLINE");
        assert_eq!(written.keys().cloned().collect::<HashSet<_>>(), [kept.node_id.clone()].into(), "the silence memo of a deleted node is retired");
    }

    #[tokio::test]
    async fn a_recovery_that_ends_failed_is_recorded_as_its_failed_step_never_as_submitted() {
        let failed = RecoveryEnd::Failed { attempt: 3, operation: "retire-node:mesh2.rpc.1".into(), step: "NodeDeleted".into(), reason: "the exit-proof row is missing".into() };
        let (outcome, r) = settle(Ok(failed));
        assert_eq!(outcome, "failed: attempt 3 retire-node:mesh2.rpc.1 step NodeDeleted: the exit-proof row is missing");
        assert_eq!(r, Err(outcome.clone()), "a failed recovery is not a submitted one: the next tick submits again");
        let (outcome, r) = settle(Ok(RecoveryEnd::Complete { attempt: 3 }));
        assert_eq!((outcome.as_str(), r), ("complete: attempt 3", Ok(())));
        let (outcome, r) = settle(Err("the run of attempt 3 ended without its terminal".into()));
        assert_eq!(outcome, "the run of attempt 3 ended without its terminal");
        assert!(r.is_err());
    }

    /// CONTRACT: loading the node key from the data dir never waits on the disk on a runtime
    /// worker: it runs on the blocking pool (`record_store::blocking_io`), as every store read
    /// does. The admin loads it inside its boot, on the runtime. The cell loads a key whose file
    /// holds the read: the runtime's only thread keeps running, and the load answers that key.
    #[test]
    fn loading_the_node_key_waits_on_the_disk_off_the_runtime_thread() {
        let d = crate::record_store::tempdir("node-key-offload");
        let key = SecretKey::generate();
        let fifo = d.join("node-key");
        let dir = d.clone();
        let (ran, loaded) = crate::record_store::runtime_runs_while_a_fifo_is_read(fifo, hex::encode(key.to_bytes()).into_bytes(), Box::pin(async move { load_or_mint_key(&dir).await.map(|k| k.to_bytes()) }));
        assert!(ran, "the runtime thread ran while the key's read waited on the disk");
        assert_eq!(loaded, Ok(key.to_bytes()));
    }

    fn fabric1() -> FabricId {
        FabricId::parse("fab000000001").unwrap()
    }

    /// A fixed canonical id per test mesh (`mesh2` -> `meshid000002`).
    fn mesh_id(mesh: &str) -> MeshId {
        MeshId::parse(&format!("meshd{:0>7}", mesh.trim_start_matches("mesh"))).unwrap()
    }

    fn digest(name: &str, status: MemberStatus) -> MeshDigest {
        let name: PathName = name.parse().unwrap();
        MeshDigest {
            fabric_id: fabric1(),
            node: MeshNode {
                node_id: NodeId::mint(),
                name: name.clone(),
                endpoint_id: EndpointId(format!("key-{name}")),
                transport_addr: "127.0.0.1:41000".parse().unwrap(),
                incarnation: IncarnationId::mint(),
                supersedes: None,
                runtime: None,
            },
            status,
            node_state: Default::default(),
            admin_api_base: (name.kind == NodeKind::NodeAdmin).then(|| format!("http://admin-{name}")),
            emitted_at_rafka_ms: 0,
            digest_seq: 1,
            mesh_id: Some(mesh_id(&name.mesh)),
            in_flight: None,
            config: BTreeMap::new(),
            load: None,
            gossip: None,
            data_dir: None,
        }
    }

    /// CONTRACT (lifecycles/states.md: "Mesh: `pending` → `ready-for-traffic` → `deleting` →
    /// `deleted` ... deleted is final"; mesh-delete.md step 7: the fabric-primary stops the deleted
    /// mesh's last admin, "from that admin's stop on it publishes the deleted mesh's aggregate"): the
    /// fabric-primary accepted mesh.deleted for mesh1 and holds mesh1's last admin, mesh1.admin.1, from
    /// that admin's own digest. The admin is stopped: the book holds it parked. The view still states
    /// mesh1 deleted, under its id, with the parked admin its one node. Must NOT happen: the parked
    /// birth carrying no mesh id into the view, so mesh1 loses its id and is projected
    /// ready-for-traffic again (handover stem: the fabric-primary re-sent mesh1 the down op for the id
    /// its view lost, 410 ms after the stop, and its view named mesh1 live).
    #[test]
    fn a_deleted_mesh_whose_last_admin_is_stopped_stays_deleted_under_its_id() {
        let book = DigestBook::default();
        let records = Records::default();
        let last = digest("mesh1.admin.1", MemberStatus::ReadyForTraffic);
        assert!(book.record(last.clone()));
        let id = last.mesh_id.clone().expect("a node-admin's digest states its mesh id");
        records.hold_mesh_deleted(&id);
        let mesh1 = |t: &Topology| t.meshes.iter().find(|m| m.name == "mesh1").map(|m| (m.id.clone(), m.status));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(mesh1(&t), Some((Some(id.clone()), ScopeStatus::Dead)), "mesh.deleted accepted: mesh1 is deleted");
        assert!(book.stopped(rafka_mesh_entity::LifecycleOp {
            build_id: "b7".into(),
            attempt: 1,
            operation: "stop-node:mesh1.admin.1".into(),
            node_id: last.node.node_id.clone(),
            incarnation: last.node.incarnation.clone(),
            name: last.node.name.clone(),
            event_at_rafka_ms: 1,
        }));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        let n = t.node(&last.node.name).expect("the stopped admin is listed");
        assert!(n.parked, "listed parked: {n:?}");
        assert_eq!(mesh1(&t), Some((Some(id), ScopeStatus::Dead)), "a deleted mesh whose last admin is stopped stays deleted, under its id");
    }

    /// CONTRACT (mesh-leave.md step 7: "Departed births resolve Gone"; R-MN1: "a mesh created again
    /// is a new mesh"): a contact is a bootstrap hint for a birth this admin holds, never a record of
    /// one that is gone. A birth the book holds departed (node.delete's NodeDeleted, or mesh-left,
    /// which departs every birth of the removed mesh) is dropped from the contacts, so a maker's
    /// stored map never hands a removed mesh's births to the first admin of a mesh created again
    /// under the same name. A contact replaced at its path by another logical node is dropped as
    /// before; a contact of a birth that is simply not held now (silent, not departed) is kept.
    #[test]
    fn a_departed_births_contact_is_dropped_and_a_silent_ones_is_kept() {
        let record = |d: &MeshDigest| crate::storage::NodeRecord { node_id: d.node.node_id.clone(), fabric_id: d.fabric_id.clone(), name: d.node.name.clone(), mesh_id: mesh_id(&d.node.name.mesh), incarnation_id: d.node.incarnation.clone(), endpoint_id: d.node.endpoint_id.clone(), transport_addr: d.node.transport_addr, listeners: Vec::new() };
        let (departed, silent, replaced, successor) = (digest("mesh2.rpc.1", MemberStatus::Leaving), digest("mesh2.rpc.2", MemberStatus::ReadyForTraffic), digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic), digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic));
        let written: HashMap<NodeId, crate::storage::NodeRecord> = [&departed, &silent, &replaced].into_iter().map(|d| (d.node.node_id.clone(), record(d))).collect();
        let mut dropped = dropped_contacts(&written, std::slice::from_ref(&successor), |id| id == &departed.node.node_id);
        dropped.sort();
        let mut want = vec![departed.node.node_id.clone(), replaced.node.node_id.clone()];
        want.sort();
        assert_eq!(dropped, want, "the departed birth's and the replaced birth's contacts go; the silent one's stays");
    }

    /// CONTRACT (R-OW1: "`admin_api_base` is config, never checked"; node-update.md: "Config on the
    /// row, `admin_api_base` included, is stored and gossiped as given; nothing checks it"): a
    /// node-admin birth this admin's book holds ready-for-traffic is Ready, its digest carrying no
    /// `admin_api_base`. What must NOT happen: a node-admin refused Ready for a config value RDM
    /// never reads.
    #[tokio::test]
    async fn a_node_admin_with_no_admin_api_base_is_ready_once_its_book_holds_it_ready_for_traffic() {
        let book = DigestBook::default();
        let mut d = digest("mesh1.admin.2", MemberStatus::ReadyForTraffic);
        d.admin_api_base = None;
        assert!(book.record(d.clone()), "the book takes the node-admin's digest");
        assert_eq!(book.get(d.node.node_id.as_str()).map(|(held, _)| held.status), Some(MemberStatus::ReadyForTraffic), "the book holds the birth ready-for-traffic");
        let mut node = Node::allocated(d.node.name.clone());
        node.node_id = d.node.node_id.clone();
        node.incarnation_id = Some(d.node.incarnation.clone());
        let observer = MembershipObserver { book, client: None, commands: Arc::default(), authority: Arc::default(), handover: Arc::default() };
        assert_eq!(crate::deployment::pipeline::NodeObserver::ready(&observer, &node).await, Ok(()));
    }

    /// A nodes store that counts the contact writes made through it.
    struct CountingNodes(Arc<dyn crate::storage::NodesStorage>, std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for CountingNodes {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::record_store::StorageError> {
            self.0.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            self.0.runtimes().await
        }
        async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            self.0.runtime(node_id, incarnation).await
        }
        async fn own(&self) -> Result<Option<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.0.own().await
        }
        async fn put_own(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.0.put_own(record).await
        }
        async fn contacts(&self) -> Result<Vec<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.0.contacts().await
        }
        async fn put_contact(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.0.put_contact(record).await
        }
        async fn remove_contact(&self, mesh: &str, node_id: &NodeId) -> Result<(), crate::record_store::StorageError> {
            self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.0.remove_contact(mesh, node_id).await
        }
        async fn put_node_row(&self, row: &crate::storage::NodeRow) -> Result<Result<(), crate::storage::NameHeld>, crate::record_store::StorageError> {
            self.0.put_node_row(row).await
        }
        async fn node_row_at(&self, name: &PathName) -> Result<Option<crate::storage::NodeRow>, crate::record_store::StorageError> {
            self.0.node_row_at(name).await
        }
        async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, crate::record_store::StorageError> {
            self.0.node_row_name(mesh, node_id).await
        }
        async fn hold_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.0.hold_node_row(name, holder).await
        }
        async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.0.release_node_row(name, holder).await
        }
        async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<NodeId>, crate::record_store::StorageError> {
            self.0.tagged(label).await
        }
    }

    fn contact_names(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir).map(|d| d.filter_map(|e| e.ok()).filter(|e| e.file_name().to_string_lossy().ends_with(".json")).count()).unwrap_or(0)
    }

    /// CONTRACT (R14 item 2, the writer ladder fabric-node-lifecycle.md §4.3; #3070 "the contacts
    /// family must follow the ladder"): on one shared root, a mesh's contacts and its Mesh row are
    /// written by that mesh's primary alone, for its own members alone. A seated and an unseated
    /// admin of mesh1 run over the same held births at once: the unseated one writes nothing, the
    /// seated one writes mesh1's members (itself included) and mesh1's Mesh row, never mesh2's. The
    /// seat moves: the new holder writes the replaced member's new contact and drops the old one,
    /// and the old holder writes nothing more. A launched birth's publication writes no contact.
    #[tokio::test]
    async fn a_meshs_contacts_and_mesh_row_are_written_by_its_primary_alone_on_the_shared_root() {
        let d = crate::record_store::tempdir("contacts-ladder");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let store = |data: &str| {
            let (data, root) = (d.join(data), root.clone());
            async move { Arc::new(CountingNodes(Arc::new(crate::storage::FileNodesStorage::open(&data, &root, "mesh1", &IncarnationId::mint()).await.unwrap()), Default::default())) }
        };
        let (p_nodes, q_nodes) = (store("mesh1.admin.1").await, store("mesh1.admin.2").await);
        let (p_meshes, q_meshes): (Arc<dyn crate::storage::MeshStorage>, Arc<dyn crate::storage::MeshStorage>) =
            (Arc::new(crate::storage::FileMeshStorage::open(&root, &IncarnationId::mint()).await.unwrap()), Arc::new(crate::storage::FileMeshStorage::open(&root, &IncarnationId::mint()).await.unwrap()));
        let (p, q, rpc, other) = (digest("mesh1.admin.1", MemberStatus::ReadyForTraffic), digest("mesh1.admin.2", MemberStatus::ReadyForTraffic), digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic), digest("mesh2.admin.1", MemberStatus::ReadyForTraffic));
        let held = vec![p.clone(), q.clone(), rpc.clone(), other.clone()];
        let mut pw = MeshRowsWriter::new("mesh1.admin.1", fabric1(), "mesh1", mesh_id("mesh1"), p_nodes.clone(), p_meshes);
        let mut qw = MeshRowsWriter::new("mesh1.admin.2", fabric1(), "mesh1", mesh_id("mesh1"), q_nodes.clone(), q_meshes);
        tokio::join!(pw.round(&held, true, |_| false), qw.round(&held, false, |_| false));
        let contacts = root.dir().join("nodes").join("mesh1").join("contacts");
        assert_eq!(contact_names(&contacts), 3, "mesh1's primary writes its three members");
        assert_eq!(contact_names(&root.dir().join("nodes").join("mesh2").join("contacts")), 0, "mesh1's primary writes no mesh2 contact");
        assert_eq!(q_nodes.1.load(std::sync::atomic::Ordering::SeqCst), 0, "an unseated admin writes no contact");
        let mesh_rows: Vec<String> = std::fs::read_dir(root.dir().join("meshes")).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).filter(|n| n.ends_with(".json")).collect();
        assert_eq!(mesh_rows, ["mesh1.json"], "mesh1's primary writes mesh1's row alone");
        // The seat moves to Q; mesh1.rpc.1 is replaced by a new node at its path.name.
        let reborn = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        let held = vec![p.clone(), q.clone(), reborn.clone(), other.clone()];
        let before = p_nodes.1.load(std::sync::atomic::Ordering::SeqCst);
        tokio::join!(pw.round(&held, false, |_| false), qw.round(&held, true, |_| false));
        assert_eq!(p_nodes.1.load(std::sync::atomic::Ordering::SeqCst), before, "the old holder writes nothing once the seat moved");
        let now = crate::storage::NodesStorage::contacts(&*q_nodes).await.unwrap();
        assert_eq!(now.len(), 3, "{now:?}");
        assert!(now.iter().any(|c| c.node_id == reborn.node.node_id) && !now.iter().any(|c| c.node_id == rpc.node.node_id), "the new holder replaced the member's contact");
        // A launched birth's publication writes no contact.
        let records = Records::default();
        let _ = records.contacts.set(q_nodes.clone());
        let writes = q_nodes.1.load(std::sync::atomic::Ordering::SeqCst);
        records.publish(crate::reenter::MapNode { node_id: NodeId::mint(), name: "mesh1.rpc.2".parse().unwrap(), endpoint_id: EndpointId("key-x".into()), transport_addr: "127.0.0.1:41000".parse().unwrap(), incarnation: IncarnationId::mint(), transition: None, ready: false, data_dir: None, admin_api_base: None }.as_node(NodeStatus::Pending, ProviderKind::Process));
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(q_nodes.1.load(std::sync::atomic::Ordering::SeqCst), writes, "a publication wrote a contact");
    }

    /// CONTRACT (node-identity-and-state.md §2.7: the mesh-primary "claims the mesh's name by
    /// insert-and-fail on the key `(path, name)` ... Held means held by a different MeshId:
    /// `NameHeld` names that holder"; "A mesh whose name another mesh's row already holds stays held
    /// as `name-taken`, naming the holder; it is never renamed, and the id never moves"; CLAUDE.md:
    /// the claim is the first durable write of a create, nothing is written for the entity until it
    /// wins): on one shared root, mesh A's primary claims `mesh2` and writes its row and its member's
    /// contact. A second mesh under the same name (another MeshId) has its primary's rounds refused by
    /// name: the row still names A, the second primary writes no contact, and it claims once per seat
    /// tenure: one `rdm.node_admin.mesh.create.via-mesh-row` span `name-held` naming A, however many
    /// rounds run. Must NOT happen: the second mesh's put overwriting A's row, its contacts written
    /// beside a refused claim, or a refused claim tried again on every birth change.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_second_mesh_of_a_live_name_is_refused_by_name_and_writes_nothing() {
        let d = crate::record_store::tempdir("mesh-claim-live");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let store = |data: &str| {
            let (data, root) = (d.join(data), root.clone());
            async move { Arc::new(CountingNodes(Arc::new(crate::storage::FileNodesStorage::open(&data, &root, "mesh2", &IncarnationId::mint()).await.unwrap()), Default::default())) }
        };
        let (a_nodes, b_nodes) = (store("a.admin").await, store("b.admin").await);
        let (a_meshes, b_meshes): (Arc<dyn crate::storage::MeshStorage>, Arc<dyn crate::storage::MeshStorage>) =
            (Arc::new(crate::storage::FileMeshStorage::open(&root, &IncarnationId::mint()).await.unwrap()), Arc::new(crate::storage::FileMeshStorage::open(&root, &IncarnationId::mint()).await.unwrap()));
        let (first, second) = (mesh_id("mesh2"), MeshId::mint());
        let (a_admin, b_admin) = (digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), digest("mesh2.admin.1", MemberStatus::ReadyForTraffic));
        let capture = crate::span_capture::Capture::new("i143-3089-mesh-name-held");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let mut aw = MeshRowsWriter::new("mesh2.admin.1", fabric1(), "mesh2", first.clone(), a_nodes.clone(), a_meshes.clone());
        aw.round(std::slice::from_ref(&a_admin), true, |_| false).await;
        assert_eq!(crate::storage::MeshStorage::mesh(&*a_meshes, "mesh2").await.unwrap().map(|m| m.mesh_id), Some(first.clone()), "mesh A's primary claims mesh2 and writes its row");
        assert_eq!(a_nodes.1.load(std::sync::atomic::Ordering::SeqCst), 1, "mesh A's primary writes its member's contact once its claim won");
        let mut bw = MeshRowsWriter::new("mesh2.admin.1", fabric1(), "mesh2", second.clone(), b_nodes.clone(), b_meshes.clone());
        for _round in 0..2 {
            bw.round(std::slice::from_ref(&b_admin), true, |_| false).await;
        }
        assert_eq!(crate::storage::MeshStorage::mesh(&*b_meshes, "mesh2").await.unwrap().map(|m| m.mesh_id), Some(first.clone()), "the second mesh's claim of a live name writes no row: mesh2's row still names mesh A");
        assert_eq!(b_nodes.1.load(std::sync::atomic::Ordering::SeqCst), 0, "the second mesh's primary writes no contact beside its refused claim");
        let spans = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("mesh_id").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("holder").cloned().unwrap_or_default())).collect();
        assert_eq!(
            said,
            vec![(first.to_string(), "row-written".to_string(), String::new()), (second.to_string(), "name-held".to_string(), first.to_string())],
            "one claim per seat tenure, the refusal naming the holder: {spans:?}"
        );
        eprintln!("span rdm.node_admin.mesh.create.via-mesh-row trace_id={} {:?}", spans[1].trace_id, spans[1].attributes);
    }

    /// CONTRACT (R13: the fabric-primary writes as the acting primary of a mesh with no primary;
    /// lifecycles/node-create.md "Mesh absent ... Until the mesh has a primary, the fabric-primary writes
    /// its rows"; CLAUDE.md: the claim is the first durable write of a create, nothing is written for the
    /// entity until it wins; node-identity-and-state.md §2.7: `NameHeld` names the holder, the same MeshId
    /// re-claiming is idempotent): `create-mesh:mesh2` at the fabric-primary claims mesh2's name first.
    /// On a free key it writes the Mesh row under the id the operation carries and holds that id for the
    /// mesh's launches; a re-run of the operation claims the same id and finds its own row. Where another mesh's
    /// live row holds mesh3, the operation fails naming the holder: mesh3's row stands and no id is held
    /// for mesh3, so no launch can carry one and the Build runs no create of mesh3's nodes. One
    /// `rdm.node_admin.mesh.create.via-mesh-row` span per claim, naming the fabric-primary. Must NOT
    /// happen: a mesh id held, or anything of the mesh written, beside a refused claim.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn create_mesh_claims_the_mesh_name_first_and_a_refused_claim_holds_nothing_of_the_mesh() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let records = Records::default();
        let holder = MeshId::mint();
        let theirs = crate::storage::MeshRecord { mesh_id: holder.clone(), name: "mesh3".into(), dead: false };
        assert_eq!(crate::storage::MeshStorage::claim_mesh(&meshes, &theirs).await, Ok(Ok(crate::storage::MeshClaim::Written { over: None })));
        // The ids the two operations carry, minted when each mesh entered the accepted topology.
        let (carried, mesh3_carried) = (MeshId::mint(), MeshId::mint());
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-claim");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let minted = create_mesh(&records, &meshes, "mesh1.admin.1", "mesh2", &carried).await.expect("a free name is claimed");
        assert_eq!(minted, carried, "the claim is made for the id the operation carries");
        assert_eq!(crate::storage::MeshStorage::mesh(&meshes, "mesh2").await.unwrap(), Some(crate::storage::MeshRecord { mesh_id: minted.clone(), name: "mesh2".into(), dead: false }), "the claim wrote mesh2's row under the carried id");
        assert_eq!(records.meshes.lock().unwrap().get("mesh2"), Some(&minted), "the won claim's id is what the mesh's launches carry");
        assert_eq!(create_mesh(&records, &meshes, "mesh1.admin.1", "mesh2", &carried).await, Ok(minted.clone()), "a re-run claims the same id");
        let refused = create_mesh(&records, &meshes, "mesh1.admin.1", "mesh3", &mesh3_carried).await.expect_err("mesh3's name is held by another mesh's live row");
        let refused = refused.to_string();
        assert!(refused.contains("create-mesh:mesh3") && refused.contains(&holder.to_string()), "the refusal names the operation and the holder: {refused}");
        assert_eq!(crate::storage::MeshStorage::mesh(&meshes, "mesh3").await.unwrap(), Some(theirs), "the refused claim wrote nothing: mesh3's row is the holder's");
        assert_eq!(records.meshes.lock().unwrap().get("mesh3"), None, "no id is held for a mesh whose claim was refused: no launch of it can carry one");
        let spans = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let said: Vec<(String, String, String, String)> = spans
            .iter()
            .map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("mesh").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("holder").cloned().unwrap_or_default()))
            .collect();
        let row = |mesh: &str, outcome: &str, holder: &str| ("mesh1.admin.1".to_string(), mesh.to_string(), outcome.to_string(), holder.to_string());
        assert_eq!(said, vec![row("mesh2", "row-written", ""), row("mesh2", "row-held-by-mesh", ""), row("mesh3", "name-held", &holder.to_string())], "{spans:?}");
        eprintln!("span rdm.node_admin.mesh.create.via-mesh-row trace_id={} {:?}", spans[2].trace_id, spans[2].attributes);
    }

    /// CONTRACT (node-identity-and-state.md §2.7: "Who mints it. The Build that creates a mesh's first
    /// node-admin mints the mesh id, never reissued"; "The same MeshId re-claiming its own name, at
    /// state-commit, on a retry or from a replaced primary, is idempotent and accepted"): the accepted
    /// Build that creates mesh2 is planned by its fabric-primary and, after that admin is lost, by its
    /// successor, which holds nothing of its predecessor's memory. Each runs the plan's
    /// `create-mesh:mesh2` over the one mesh store: the first claim writes mesh2's row, and the
    /// successor's re-run claims with the same MeshId and finds its own row (`row-held-by-mesh`), so
    /// the id its launches carry is the row's. Must NOT happen: the successor minting another MeshId
    /// and being refused `name-held` by its own mesh's row.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_successor_re_running_create_mesh_of_the_same_build_claims_with_its_mesh_id_and_finds_its_own_row() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let observed = view(&[digest("mesh1.admin.1", MemberStatus::ReadyForTraffic)]);
        let current = crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap());
        let change = crate::accepted::TopologyChange::CreateMesh { desired: crate::build::MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)]) };
        let accepted = crate::accepted::compile(&current, &change, &observed).expect("mesh2 is accepted");
        let create_mesh_of = |plan: crate::build::BuildPlan| plan.operations.into_iter().find(|op| matches!(op, BuildOperation::CreateMesh { .. })).expect("the plan creates mesh2");
        let (first_op, successor_op) = (create_mesh_of(crate::accepted::plan(&accepted, &observed, None)), create_mesh_of(crate::accepted::plan(&accepted, &observed, None)));
        let (BuildOperation::CreateMesh { mesh: first_mesh, mesh_id: first_id }, BuildOperation::CreateMesh { mesh: successor_mesh, mesh_id: successor_id }) = (&first_op, &successor_op) else { unreachable!() };
        let minted = &accepted.mesh("mesh2").expect("the accepted topology holds mesh2").mesh_id;
        assert_eq!((first_id, successor_id), (minted, minted), "every plan of the Build carries the MeshId minted when mesh2 was accepted");
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-rerun");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let (first, successor) = (Records::default(), Records::default());
        let claimed = create_mesh(&first, &meshes, "mesh1.admin.1", first_mesh, first_id).await.expect("the first run claims mesh2's free name");
        assert_eq!(&claimed, minted, "the first run claims with the Build's MeshId");
        let rerun = create_mesh(&successor, &meshes, "mesh1.admin.2", successor_mesh, successor_id).await;
        assert_eq!(rerun, Ok(claimed.clone()), "the successor's re-run of the same Build's create-mesh claims with the same MeshId and finds its own row");
        assert_eq!(successor.meshes.lock().unwrap().get("mesh2"), Some(&claimed), "the successor's launches of mesh2 carry the id mesh2's row holds");
        assert_eq!(crate::storage::MeshStorage::mesh(&meshes, "mesh2").await.unwrap().map(|m| m.mesh_id), Some(claimed.clone()), "mesh2's row is the first claim's");
        let spans = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("mesh_id").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(said, vec![("mesh1.admin.1".to_string(), claimed.to_string(), "row-written".to_string()), ("mesh1.admin.2".to_string(), claimed.to_string(), "row-held-by-mesh".to_string())], "{spans:?}");
        eprintln!("span rdm.node_admin.mesh.create.via-mesh-row trace_id={} {:?}", spans[1].trace_id, spans[1].attributes);
    }

    /// The accepted Build that creates mesh2 beside mesh1, held by a Build log, and its `create-mesh:mesh2`
    /// operation as the fabric-primary plans it.
    async fn build_creating_mesh2(builds: &crate::build_state::MemoryBuildStateAdapter) -> (crate::build::BuildId, BuildOperation) {
        let observed = view(&[digest("mesh1.admin.1", MemberStatus::ReadyForTraffic)]);
        let current = crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap());
        let change = crate::accepted::TopologyChange::CreateMesh { desired: crate::build::MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)]) };
        let accepted = crate::accepted::compile(&current, &change, &observed).expect("mesh2 is accepted");
        let op = crate::accepted::plan(&accepted, &observed, None).operations.into_iter().find(|op| matches!(op, BuildOperation::CreateMesh { .. })).expect("the plan creates mesh2");
        let build_id = crate::build::BuildId::parse("jrr142w4840j").unwrap();
        builds.publish_accepted(&crate::build_state::BuildAccepted { build_id: build_id.clone(), topology: accepted, submitted_change: Some(change), submitted_at_ms: 0 }).await.expect("the Build is accepted");
        (build_id, op)
    }

    /// The receipts of `operation` the Build holds: `(attempt, step, outcome, executor)`.
    async fn receipts_of(builds: &crate::build_state::MemoryBuildStateAdapter, build_id: &crate::build::BuildId, operation: &str) -> Vec<(u32, String, crate::build_state::StepOutcome, Option<String>)> {
        builds.read_build(build_id).await.expect("the Build is held").steps.into_iter().filter(|r| r.operation == operation).map(|r| (r.attempt, r.step, r.outcome, r.executor)).collect()
    }

    /// CONTRACT (node-rpc-envelope.md, Build op 0x20: "completed step receipts are read and replayed, never
    /// executed again"; Build carrier: "Completed steps are not rerun"): `create-mesh:mesh2` runs at the
    /// fabric-primary, claims mesh2's name and commits its `Complete` receipt. A successor fabric-primary
    /// that runs the same Build's `create-mesh:mesh2` reads that receipt: it makes no second claim, writes
    /// no second receipt, and holds the MeshId the Build carries for its launches of mesh2. One
    /// `rdm.node_admin.mesh.create.via-mesh-row` span (the first run's `row-written`), and a
    /// `rdm.node_admin.mesh.create.via-receipt` span per run: `complete`, then `replayed`. Must NOT happen:
    /// the successor claiming again (`row-held-by-mesh`), or a second `Complete` receipt of the step.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_completed_create_mesh_is_receipted_and_a_successor_replays_the_receipt_instead_of_claiming_again() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let (build_id, op) = build_creating_mesh2(&builds).await;
        let BuildOperation::CreateMesh { mesh_id, .. } = &op else { unreachable!() };
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-receipt");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let (first, successor) = (Records::default(), Records::default());
        create_mesh_step(&first, &meshes, &builds, "key1@127.0.0.1:1", "mesh1.admin.1", &build_id, 1, &op).await.expect("the first run claims mesh2's free name");
        create_mesh_step(&successor, &meshes, &builds, "key2@127.0.0.1:2", "mesh1.admin.2", &build_id, 2, &op).await.expect("the successor's run of the same step");
        assert_eq!(
            receipts_of(&builds, &build_id, "create-mesh:mesh2").await,
            vec![(1, "Complete".to_string(), crate::build_state::StepOutcome::Complete, Some("key1@127.0.0.1:1".to_string()))],
            "the step is receipted once, by the run that executed it"
        );
        assert_eq!(successor.meshes.lock().unwrap().get("mesh2"), Some(mesh_id), "the successor's launches of mesh2 carry the id the Build carries");
        let claims = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let claimed: Vec<(String, String)> = claims.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(claimed, vec![("mesh1.admin.1".to_string(), "row-written".to_string())], "the successor makes no second claim: {claims:?}");
        let steps = capture.named("rdm.node_admin.mesh.create.via-receipt");
        let said: Vec<(String, String)> = steps.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(said, vec![("mesh1.admin.1".to_string(), "complete".to_string()), ("mesh1.admin.2".to_string(), "replayed".to_string())], "{steps:?}");
        eprintln!("span rdm.node_admin.mesh.create.via-receipt trace_id={} {:?}", steps[1].trace_id, steps[1].attributes);
    }

    /// CONTRACT (node-rpc-envelope.md, Build carrier: "Settle an idempotent uncertain step by its natural
    /// key"; node-identity-and-state.md §2.7: the same MeshId re-claiming its name is idempotent): the
    /// fabric-primary claimed mesh2's name and was lost before it receipted the step, so the Build holds no
    /// receipt of it. The successor's run is uncertain: it claims by the step's natural key, the mesh's
    /// own row answers `row-held-by-mesh`, and the step is receipted `Complete` at the successor's
    /// attempt. Must NOT happen: a refused claim, or a step that ends with no receipt.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_create_mesh_whose_receipt_was_never_written_settles_by_its_natural_key_and_is_receipted() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let (build_id, op) = build_creating_mesh2(&builds).await;
        let BuildOperation::CreateMesh { mesh, mesh_id } = &op else { unreachable!() };
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-uncertain");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        // The lost fabric-primary: its claim landed, and its receipt did not.
        create_mesh(&Records::default(), &meshes, "mesh1.admin.1", mesh, mesh_id).await.expect("the lost fabric-primary's claim");
        assert_eq!(receipts_of(&builds, &build_id, "create-mesh:mesh2").await, vec![], "the lost run left no receipt");
        let successor = Records::default();
        create_mesh_step(&successor, &meshes, &builds, "key2@127.0.0.1:2", "mesh1.admin.2", &build_id, 2, &op).await.expect("the uncertain step settles by its natural key");
        assert_eq!(receipts_of(&builds, &build_id, "create-mesh:mesh2").await, vec![(2, "Complete".to_string(), crate::build_state::StepOutcome::Complete, Some("key2@127.0.0.1:2".to_string()))]);
        assert_eq!(successor.meshes.lock().unwrap().get("mesh2"), Some(mesh_id));
        let claims = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let claimed: Vec<(String, String)> = claims.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(claimed, vec![("mesh1.admin.1".to_string(), "row-written".to_string()), ("mesh1.admin.2".to_string(), "row-held-by-mesh".to_string())], "{claims:?}");
        let steps = capture.named("rdm.node_admin.mesh.create.via-receipt");
        let said: Vec<(String, String)> = steps.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(said, vec![("mesh1.admin.2".to_string(), "complete".to_string())], "{steps:?}");
        eprintln!("span rdm.node_admin.mesh.create.via-receipt trace_id={} {:?}", steps[0].trace_id, steps[0].attributes);
    }

    /// CONTRACT (node-rpc-envelope.md, Build op 0x20: a receipt is read and replayed by the run it belongs
    /// to; `BuildProjection::opened`: a step receipt from the attempt a reopen opened on is the current
    /// run's): `create-mesh:mesh2` completed in the Build's first run, which ended `Converged`; the Build
    /// is reopened (attempt 2, a drift repair) and its plan names `create-mesh:mesh2` again because the
    /// mesh is not yet in the executor's view. The earlier run's receipt is not this run's: the step runs
    /// by its natural key (the mesh's own row answers `row-held-by-mesh`) and is receipted at attempt 2.
    /// Must NOT happen: a receipt of a finished run replayed into the run a reopen began.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_reopened_builds_create_mesh_runs_by_its_natural_key_and_is_receipted_in_the_new_run() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let (build_id, op) = build_creating_mesh2(&builds).await;
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-reopened");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let claim = |attempt: u32, executor: &str| crate::build_state::BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id.clone(), attempt, executor: executor.into() };
        assert_eq!(builds.claim_attempt(&claim(1, "key1@127.0.0.1:1")).await.unwrap(), crate::build_state::ClaimOutcome::Won);
        create_mesh_step(&Records::default(), &meshes, &builds, "key1@127.0.0.1:1", "mesh1.admin.1", &build_id, 1, &op).await.expect("the first run");
        builds.append_attempt_receipt(&crate::build_state::BuildAttemptReceipt { mesh: None, build_id: build_id.clone(), attempt: 1, outcome: crate::build_state::AttemptOutcome::Converged }).await.unwrap();
        builds
            .open_attempt(&crate::build_state::AttemptOpened { mesh: None, build_id: build_id.clone(), attempt: 2, reason: crate::build_state::AttemptReason::ProvenDrift, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 1 })
            .await
            .expect("the Build is reopened");
        assert_eq!(builds.read_build(&build_id).await.unwrap().opened, 2, "attempt 2 begins the Build's current run");
        assert_eq!(builds.claim_attempt(&claim(2, "key2@127.0.0.1:2")).await.unwrap(), crate::build_state::ClaimOutcome::Won);
        create_mesh_step(&Records::default(), &meshes, &builds, "key2@127.0.0.1:2", "mesh1.admin.2", &build_id, 2, &op).await.expect("the reopened run");
        let receipts = receipts_of(&builds, &build_id, "create-mesh:mesh2").await;
        assert_eq!(receipts.iter().map(|r| (r.0, r.1.as_str())).collect::<Vec<_>>(), vec![(1, "Complete"), (2, "Complete")], "each run receipts its own step: {receipts:?}");
        let claims: Vec<String> = capture.named("rdm.node_admin.mesh.create.via-mesh-row").iter().map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        assert_eq!(claims, ["row-written", "row-held-by-mesh"], "the reopened run claims by the natural key");
        let steps: Vec<String> = capture.named("rdm.node_admin.mesh.create.via-receipt").iter().map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        assert_eq!(steps, ["complete", "complete"]);
    }

    /// CONTRACT (node-rpc-envelope.md, Build op 0x20: "Durable step outcomes remain Complete and Failed"):
    /// mesh2's name is held by another mesh's live row, so `create-mesh:mesh2` is refused by name. The
    /// step `ClaimMeshName` is receipted `Failed` with the refusal's reason (the holder named), holds no id
    /// for mesh2, and the operation is never receipted `Complete`. Must NOT happen: a `Complete` receipt beside a refused claim.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_create_mesh_refused_by_name_is_receipted_failed_naming_the_holder() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let (build_id, op) = build_creating_mesh2(&builds).await;
        let holder = MeshId::mint();
        crate::storage::MeshStorage::claim_mesh(&meshes, &crate::storage::MeshRecord { mesh_id: holder.clone(), name: "mesh2".into(), dead: false }).await.unwrap().unwrap();
        let records = Records::default();
        let refused = create_mesh_step(&records, &meshes, &builds, "key1@127.0.0.1:1", "mesh1.admin.1", &build_id, 1, &op).await.expect_err("mesh2's name is held by another mesh");
        assert!(refused.contains("create-mesh:mesh2") && refused.contains(&holder.to_string()), "{refused}");
        let receipts = receipts_of(&builds, &build_id, "create-mesh:mesh2").await;
        assert_eq!(receipts.len(), 1, "{receipts:?}");
        let (attempt, step, outcome, executor) = &receipts[0];
        assert_eq!((*attempt, step.as_str(), executor.as_deref()), (1, "ClaimMeshName", Some("key1@127.0.0.1:1")));
        assert!(matches!(outcome, crate::build_state::StepOutcome::Failed { reason } if reason.contains(&holder.to_string())), "the Failed receipt names the holder: {outcome:?}");
        assert_eq!(records.meshes.lock().unwrap().get("mesh2"), None, "no id is held for a mesh whose claim was refused");
    }

    /// A Build state over `0` whose step receipts are refused: nothing else differs.
    struct RefusesReceipts(Arc<crate::build_state::MemoryBuildStateAdapter>);

    #[async_trait::async_trait]
    impl BuildStateAdapter for RefusesReceipts {
        async fn publish_accepted(&self, a: &crate::build_state::BuildAccepted) -> Result<(), crate::build_state::BuildStateError> {
            self.0.publish_accepted(a).await
        }
        async fn open_attempt(&self, o: &crate::build_state::AttemptOpened) -> Result<(), crate::build_state::BuildStateError> {
            self.0.open_attempt(o).await
        }
        async fn read_build(&self, id: &crate::build::BuildId) -> Result<crate::build_state::BuildProjection, crate::build_state::BuildStateError> {
            self.0.read_build(id).await
        }
        async fn current_build(&self) -> Result<crate::build_state::CurrentBuild, crate::build_state::BuildStateError> {
            self.0.current_build().await
        }
        async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, crate::build_state::BuildStateError> {
            self.0.newest_accepted_at_ms().await
        }
        async fn list_active(&self) -> Result<Vec<crate::build_state::BuildProjection>, crate::build_state::BuildStateError> {
            self.0.list_active().await
        }
        async fn claim_attempt(&self, c: &crate::build_state::BuildAttemptClaim) -> Result<crate::build_state::ClaimOutcome, crate::build_state::BuildStateError> {
            self.0.claim_attempt(c).await
        }
        async fn adopt_claim(&self, c: &crate::build_state::BuildAttemptClaim) -> Result<(), crate::build_state::BuildStateError> {
            self.0.adopt_claim(c).await
        }
        async fn append_step_receipt(&self, r: &crate::build_state::BuildStepReceipt) -> Result<(), crate::build_state::BuildStateError> {
            Err(crate::build_state::BuildStateError::Io(format!("the journal refused the {} receipt of {}", r.step, r.operation)))
        }
        async fn append_attempt_receipt(&self, r: &crate::build_state::BuildAttemptReceipt) -> Result<(), crate::build_state::BuildStateError> {
            self.0.append_attempt_receipt(r).await
        }
        async fn facts(&self) -> Result<Vec<crate::build_state::BuildFact>, crate::build_state::BuildStateError> {
            self.0.facts().await
        }
        async fn forget(&self, id: &crate::build::BuildId) -> Result<(), crate::build_state::BuildStateError> {
            self.0.forget(id).await
        }
        async fn recovery_of(&self, p: &crate::model::PathName, n: &NodeId, i: &IncarnationId) -> Result<Option<(crate::build::BuildId, Option<String>, u32, String)>, crate::build_state::BuildStateError> {
            self.0.recovery_of(p, n, i).await
        }
        async fn mesh_deleted_of(&self, n: &NodeId, i: &IncarnationId) -> Result<Option<(crate::build::BuildId, u32, String)>, crate::build_state::BuildStateError> {
            self.0.mesh_deleted_of(n, i).await
        }
        async fn exits_owed(&self, id: &crate::build::BuildId, mesh: &str) -> Result<Vec<crate::build_state::OwedExit>, crate::build_state::BuildStateError> {
            self.0.exits_owed(id, mesh).await
        }
    }

    /// A mesh store that does not answer.
    struct UnansweredMeshes;

    #[async_trait::async_trait]
    impl crate::storage::MeshStorage for UnansweredMeshes {
        async fn mesh(&self, _: &str) -> Result<Option<crate::storage::MeshRecord>, crate::record_store::StorageError> {
            Err(crate::record_store::StorageError::Io { file: "meshes".into(), reason: "the disk did not answer".into() })
        }
        async fn claim_mesh(&self, _: &crate::storage::MeshRecord) -> Result<Result<crate::storage::MeshClaim, crate::storage::MeshClaimRefused>, crate::record_store::StorageError> {
            Err(crate::record_store::StorageError::Io { file: "meshes/mesh2.json".into(), reason: "the disk did not answer".into() })
        }
        async fn meshes(&self) -> Result<Vec<crate::storage::MeshRecord>, crate::record_store::StorageError> {
            Err(crate::record_store::StorageError::Io { file: "meshes".into(), reason: "the disk did not answer".into() })
        }
    }

    /// CONTRACT (node-rpc-envelope.md, Build op 0x20: a step is committed by its receipt; CLAUDE.md: an
    /// error state names what failed and where, distinct facts distinct arms): `create-mesh:mesh2` fails
    /// by its own name when (1) the Build cannot be read for its receipts, so it claims nothing, (2) the
    /// mesh store does not answer the claim, so it receipts nothing, and (3) the Build state refuses the
    /// `Complete` receipt after the claim won: the step is uncommitted, and the next run settles it by its
    /// natural key (`row-held-by-mesh`) and receipts it. One
    /// `rdm.node_admin.mesh.create.via-receipt` span per run, `outcome` `build-unreadable`,
    /// `store-unanswered`, `receipt-refused`, then `complete`. Must NOT happen: a `Complete` receipt of a
    /// step that was not decided, or a step reported done whose receipt the Build state refused.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_create_mesh_that_cannot_read_decide_or_commit_fails_by_name_and_the_next_run_settles_it() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let memory = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        let (build_id, op) = build_creating_mesh2(&memory).await;
        let BuildOperation::CreateMesh { mesh_id, .. } = &op else { unreachable!() };
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-cannot");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        // (1) A Build the log never accepted: nothing is claimed.
        let unknown = crate::build::BuildId::parse("kpnnk1nrvac1").unwrap();
        let unread = create_mesh_step(&Records::default(), &meshes, &*memory, "key1@127.0.0.1:1", "mesh1.admin.1", &unknown, 1, &op).await.expect_err("the Build is not held");
        assert!(unread.contains("could not be read for its receipts") && unread.contains("kpnnk1nrvac1"), "{unread}");
        assert_eq!(crate::storage::MeshStorage::mesh(&meshes, "mesh2").await.unwrap(), None, "nothing was claimed beside a Build that could not be read");
        // (2) The mesh store does not answer: nothing is receipted.
        let unanswered = create_mesh_step(&Records::default(), &UnansweredMeshes, &*memory, "key1@127.0.0.1:1", "mesh1.admin.1", &build_id, 1, &op).await.expect_err("the mesh store does not answer");
        assert!(unanswered.contains("did not answer") && unanswered.contains("mesh2"), "{unanswered}");
        assert_eq!(receipts_of(&memory, &build_id, "create-mesh:mesh2").await, vec![], "an undecided step is not receipted");
        // (3) The claim won and the receipt is refused: the step is uncommitted.
        let refused = create_mesh_step(&Records::default(), &meshes, &RefusesReceipts(memory.clone()), "key1@127.0.0.1:1", "mesh1.admin.1", &build_id, 1, &op).await.expect_err("the Build state refuses the receipt");
        assert!(refused.contains("Complete receipt of create-mesh:mesh2") && refused.contains("was not written") && refused.contains("journal refused"), "{refused}");
        assert_eq!(crate::storage::MeshStorage::mesh(&meshes, "mesh2").await.unwrap().map(|m| m.mesh_id), Some(mesh_id.clone()), "the claim stands");
        assert_eq!(receipts_of(&memory, &build_id, "create-mesh:mesh2").await, vec![], "the refused receipt is not in the log");
        // The next run settles the uncertain step by its natural key.
        create_mesh_step(&Records::default(), &meshes, &*memory, "key2@127.0.0.1:2", "mesh1.admin.2", &build_id, 2, &op).await.expect("the next run settles the step");
        assert_eq!(receipts_of(&memory, &build_id, "create-mesh:mesh2").await.len(), 1);
        let outcomes: Vec<String> = capture.named("rdm.node_admin.mesh.create.via-receipt").iter().map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        assert_eq!(outcomes, ["build-unreadable", "store-unanswered", "receipt-refused", "complete"]);
        let claims: Vec<String> = capture.named("rdm.node_admin.mesh.create.via-mesh-row").iter().map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        assert_eq!(claims, ["row-refused", "row-written", "row-held-by-mesh"], "the claim made beside the unanswered store is refused by name; the settled run finds the mesh's own row");
    }

    /// CONTRACT (node-identity-and-state.md §2.8: "The fabric-primary claims the fabric's name by
    /// insert-and-fail on its `(path, name)` ... idempotent for the same FabricId and `NameHeld` naming a
    /// different one"; CLAUDE.md: the claim is the first durable write of a create, nothing is written
    /// for the entity until it wins): `fabric/` holds another Fabric's identity (a root an embedding app
    /// handed in). This fabric-primary's rounds claim the fabric's name, are refused naming the holder,
    /// and write nothing of the fabric: the held identity stands, and the claim is made once, one
    /// `rdm.node_admin.fabric.create.via-fabric-row` span `name-held` naming the holder. Must NOT happen:
    /// a row written into another Fabric's `fabric/` beside a refused claim.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_fabric_primarys_refused_fabric_claim_writes_nothing_of_the_fabric() {
        let storage: Arc<dyn crate::fabric_storage::FabricStorage> = Arc::new(crate::fabric_storage::MemoryFabricStorage::new());
        let (holder, ours) = (crate::fabric_storage::FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() }, crate::fabric_storage::FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() });
        let birth = |epoch: u64| rafka_mesh_entity::SeatHolder { mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), epoch };
        let (h_birth, o_birth) = (birth(1), birth(2));
        let capture = crate::span_capture::Capture::new("i143-3089-fabric-name-held");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let h_seats = rafka_mesh_transport::membership::SeatBook::default();
        h_seats.take(rafka_mesh_entity::Seat::FabricPrimary, &h_birth);
        let mut hw = FabricRowsWriter::new((h_birth.node_id.clone(), h_birth.incarnation.clone()), holder.clone(), storage.clone());
        hw.round(&h_seats).await;
        assert_eq!(storage.fabric().await.unwrap(), Some(holder.clone()), "the holder's fabric-primary writes its identity once its claim won");
        let o_seats = rafka_mesh_transport::membership::SeatBook::default();
        o_seats.take(rafka_mesh_entity::Seat::FabricPrimary, &o_birth);
        let mut ow = FabricRowsWriter::new((o_birth.node_id.clone(), o_birth.incarnation.clone()), ours.clone(), storage.clone());
        for _round in 0..2 {
            ow.round(&o_seats).await;
        }
        assert_eq!(storage.fabric().await.unwrap().map(|f| f.fabric_id), Some(holder.fabric_id.clone()), "the held identity stands");
        let spans = capture.named("rdm.node_admin.fabric.create.via-fabric-row");
        let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("fabric_id").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("holder").cloned().unwrap_or_default())).collect();
        assert_eq!(
            said,
            vec![(holder.fabric_id.to_string(), "row-written".to_string(), String::new()), (ours.fabric_id.to_string(), "name-held".to_string(), holder.fabric_id.to_string())],
            "one claim per writer, the refusal naming the holder: {spans:?}"
        );
        eprintln!("span rdm.node_admin.fabric.create.via-fabric-row trace_id={} {:?}", spans[1].trace_id, spans[1].attributes);
    }

    /// CONTRACT (R-MN1: a mesh's name is released only by its removal, the row Dead, and "a mesh created
    /// again under the same name is a new mesh with a new id"; change 4: "the mesh-name claim treats a
    /// Dead mesh row as released"; states.md: dead is final, a mesh id is never reissued): mesh A's
    /// primary claims `mesh2`; the removal writes A's row Dead (`dead-written`, a repeat
    /// `already-dead`); mesh2 created again under a new MeshId has its primary claim the name over the
    /// Dead row, the span naming the Dead mesh it took it over, and its contact is written. A's own
    /// later tenure claims nothing live again (`mesh-dead`). Must NOT happen: the recreated mesh
    /// refused by the removed mesh's row, or the removed mesh's id written live again.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_mesh_removed_and_created_again_claims_its_name_over_the_dead_row() {
        let d = crate::record_store::tempdir("mesh-claim-dead");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let store = |data: &str| {
            let (data, root) = (d.join(data), root.clone());
            async move { Arc::new(CountingNodes(Arc::new(crate::storage::FileNodesStorage::open(&data, &root, "mesh2", &IncarnationId::mint()).await.unwrap()), Default::default())) }
        };
        let (a_nodes, b_nodes) = (store("a.admin").await, store("b.admin").await);
        let meshes: Arc<dyn crate::storage::MeshStorage> = Arc::new(crate::storage::FileMeshStorage::open(&root, &IncarnationId::mint()).await.unwrap());
        let (removed, recreated) = (mesh_id("mesh2"), MeshId::mint());
        let (a_admin, b_admin) = (digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), digest("mesh2.admin.1", MemberStatus::ReadyForTraffic));
        let capture = crate::span_capture::Capture::new("i143-3089-mesh-name-over-dead");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let mut aw = MeshRowsWriter::new("mesh2.admin.1", fabric1(), "mesh2", removed.clone(), a_nodes.clone(), meshes.clone());
        aw.round(std::slice::from_ref(&a_admin), true, |_| false).await;
        assert_eq!(crate::mesh_leave::mesh_row_dead(&*meshes, "mesh2", &removed).await, Ok("dead-written"), "the removal writes mesh A's row Dead");
        assert_eq!(crate::mesh_leave::mesh_row_dead(&*meshes, "mesh2", &removed).await, Ok("already-dead"), "a repeat of the removal writes nothing");
        // Mesh A's primary takes a new tenure: its id is Dead and is never written live again.
        let contacts_before = a_nodes.1.load(std::sync::atomic::Ordering::SeqCst);
        aw.round(std::slice::from_ref(&a_admin), false, |_| false).await;
        aw.round(std::slice::from_ref(&a_admin), true, |_| false).await;
        assert_eq!(crate::storage::MeshStorage::mesh(&*meshes, "mesh2").await.unwrap(), Some(crate::storage::MeshRecord { mesh_id: removed.clone(), name: "mesh2".into(), dead: true }), "the removed mesh's later tenure writes nothing live");
        assert_eq!(a_nodes.1.load(std::sync::atomic::Ordering::SeqCst), contacts_before, "nor any contact");
        let mut bw = MeshRowsWriter::new("mesh2.admin.1", fabric1(), "mesh2", recreated.clone(), b_nodes.clone(), meshes.clone());
        bw.round(std::slice::from_ref(&b_admin), true, |_| false).await;
        let row = crate::storage::MeshStorage::mesh(&*meshes, "mesh2").await.unwrap();
        assert_eq!(row, Some(crate::storage::MeshRecord { mesh_id: recreated.clone(), name: "mesh2".into(), dead: false }), "the recreated mesh's row holds mesh2 under its new MeshId");
        let contacts: Vec<NodeId> = crate::storage::NodesStorage::contacts(&*b_nodes).await.unwrap().into_iter().map(|c| c.node_id).collect();
        assert_eq!(contacts, vec![b_admin.node.node_id.clone()], "the recreated mesh's primary writes its member's contact once its claim won, and drops the removed mesh's at the path.name it now holds");
        let spans = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("mesh_id").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("over").cloned().unwrap_or_default())).collect();
        assert_eq!(
            said,
            vec![
                (removed.to_string(), "row-written".to_string(), String::new()),
                (removed.to_string(), "mesh-dead".to_string(), String::new()),
                (recreated.to_string(), "row-written".to_string(), removed.to_string()),
            ],
            "the dead mesh claims nothing again, and the recreated mesh claims over the Dead row: {spans:?}"
        );
        eprintln!("span rdm.node_admin.mesh.create.via-mesh-row trace_id={} {:?}", spans[2].trace_id, spans[2].attributes);
    }

    fn view(digests: &[MeshDigest]) -> Topology {
        let book = DigestBook::default();
        for d in digests {
            book.record(d.clone());
        }
        project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default())
    }

    fn with_id(mut d: MeshDigest, id: &str) -> MeshDigest {
        d.node.node_id = NodeId::parse(id).unwrap();
        d
    }

    /// A provider whose every inspection answers `status`, in the control domain of the runtime it
    /// is asked about; it counts the inspections.
    struct Answers {
        domain: String,
        status: crate::deployment::provider::DeploymentStatus,
        inspected: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for Answers {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            self.domain.clone()
        }
        async fn spawn(&self, spec: &crate::deployment::provider::ResolvedNodeLaunch) -> Result<crate::deployment::provider::DeploymentHandle, crate::deployment::provider::DeployError> {
            Err(crate::deployment::provider::DeployError::Spawn { node: spec.node.to_string(), reason: "this provider only answers inspections".into() })
        }
        async fn terminate(&self, _: &crate::deployment::provider::DeploymentHandle, _: crate::deployment::provider::TerminationMode) -> Result<(), crate::deployment::provider::DeployError> {
            Ok(())
        }
        async fn inspect(&self, _: &crate::deployment::provider::DeploymentHandle) -> crate::deployment::provider::DeploymentStatus {
            self.inspected.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.status.clone()
        }
        async fn signal_stop(&self, _: &crate::deployment::provider::DeploymentHandle) -> Result<(), crate::deployment::provider::DeployError> {
            Ok(())
        }
        async fn find(&self, _: &crate::deployment::provider::ResolvedNodeLaunch) -> Result<crate::deployment::provider::Found, crate::deployment::provider::DeployError> {
            Ok(crate::deployment::provider::Found::NeverLaunched)
        }
    }

    /// A nodes store whose reads of runtime rows (the point read and the whole set) are refused; every
    /// other row is the memory store's.
    #[derive(Default)]
    struct RuntimeRowUnreadable(crate::storage::MemoryNodesStorage);

    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for RuntimeRowUnreadable {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::record_store::StorageError> {
            self.0.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            Err(crate::record_store::StorageError::Io { file: "runtimes".into(), reason: "Input/output error (os error 5)".into() })
        }
        async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            Err(crate::record_store::StorageError::Unrecognised { file: format!("runtimes/{node_id}--{}.json", incarnation.as_str()), reason: "format runtime/9 is not one this build reads".into() })
        }
        async fn own(&self) -> Result<Option<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.0.own().await
        }
        async fn put_own(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.0.put_own(record).await
        }
        async fn contacts(&self) -> Result<Vec<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.0.contacts().await
        }
        async fn put_contact(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.0.put_contact(record).await
        }
        async fn remove_contact(&self, mesh: &str, node_id: &NodeId) -> Result<(), crate::record_store::StorageError> {
            self.0.remove_contact(mesh, node_id).await
        }
        async fn put_node_row(&self, row: &crate::storage::NodeRow) -> Result<Result<(), crate::storage::NameHeld>, crate::record_store::StorageError> {
            self.0.put_node_row(row).await
        }
        async fn node_row_at(&self, name: &PathName) -> Result<Option<crate::storage::NodeRow>, crate::record_store::StorageError> {
            self.0.node_row_at(name).await
        }
        async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, crate::record_store::StorageError> {
            self.0.node_row_name(mesh, node_id).await
        }
        async fn hold_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.0.hold_node_row(name, holder).await
        }
        async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.0.release_node_row(name, holder).await
        }
        async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<NodeId>, crate::record_store::StorageError> {
            self.0.tagged(label).await
        }
    }

    /// A nodes.storage over memory that counts every list of the runtime rows and answers the point
    /// read of one birth's row as `point` says: the row held (`None`), or the store's refusal.
    #[derive(Default)]
    struct CountsRuntimeLists {
        inner: crate::storage::MemoryNodesStorage,
        lists: std::sync::atomic::AtomicUsize,
        point_refused: bool,
    }

    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for CountsRuntimeLists {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            self.lists.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.runtimes().await
        }
        async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            if self.point_refused {
                return Err(crate::record_store::StorageError::Io { file: format!("runtimes/{node_id}--{}.json", incarnation.as_str()), reason: "Input/output error (os error 5)".into() });
            }
            self.inner.runtime(node_id, incarnation).await
        }
        async fn own(&self) -> Result<Option<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.inner.own().await
        }
        async fn put_own(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_own(record).await
        }
        async fn contacts(&self) -> Result<Vec<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.inner.contacts().await
        }
        async fn put_contact(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_contact(record).await
        }
        async fn remove_contact(&self, mesh: &str, node_id: &NodeId) -> Result<(), crate::record_store::StorageError> {
            self.inner.remove_contact(mesh, node_id).await
        }
        async fn put_node_row(&self, row: &crate::storage::NodeRow) -> Result<Result<(), crate::storage::NameHeld>, crate::record_store::StorageError> {
            self.inner.put_node_row(row).await
        }
        async fn node_row_at(&self, name: &PathName) -> Result<Option<crate::storage::NodeRow>, crate::record_store::StorageError> {
            self.inner.node_row_at(name).await
        }
        async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, crate::record_store::StorageError> {
            self.inner.node_row_name(mesh, node_id).await
        }
        async fn hold_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.inner.hold_node_row(name, holder).await
        }
        async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.inner.release_node_row(name, holder).await
        }
        async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<NodeId>, crate::record_store::StorageError> {
            self.inner.tagged(label).await
        }
    }

    /// CONTRACT (CLAUDE.md read rule: "Every lookup is a point read through a declared index"; error
    /// rule: "never written" and "could not be read" are different errors, distinct arms and spans):
    /// the fabric-primary proves a lost executor `mesh2.admin.1` departed whose digest carries no
    /// runtime, so the proof reads that exact birth's durable runtime row by its key. Held and
    /// exited: the departure is proven. Never written: refused on
    /// `rdm.node_admin.node.reject.via-no-runtime-row`. Unreadable: refused on
    /// `rdm.node_admin.node.reject.via-runtime-row-unreadable` naming the store's cause. Must NOT
    /// happen: a list of every runtime row searched for the one birth, or the two refusals as one.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn the_executor_exit_proof_point_reads_its_births_runtime_row_and_names_unwritten_apart_from_unreadable() {
        use crate::build_drive::DepartureProof as _;
        use crate::storage::NodesStorage as _;
        let fact = rafka_mesh_entity::RuntimeFact::of_this_process(&rafka_mesh_entity::fixture_crockford60("dep-executor")).unwrap();
        let executor = digest("mesh2.admin.1", MemberStatus::ReadyForTraffic);
        let book = DigestBook::default();
        for d in [executor.clone(), digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic)] {
            book.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let row = crate::storage::RuntimeRow { node_id: executor.node.node_id.clone(), name: executor.node.name.clone(), incarnation_id: executor.node.incarnation.clone(), runtime: fact.clone(), data_dir: None };
        let departure = |nodes: Arc<CountsRuntimeLists>| ProviderDeparture {
            me: "mesh1.admin.1".parse().unwrap(),
            topology: Arc::new(RwLock::new(view.clone())),
            refresh: Arc::new(|| Box::pin(async {})),
            book: book.clone(),
            provider: Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(1) }, inspected: Default::default() }),
            nodes,
            records: {
                // The status store the proven exit is recorded in.
                let r = Records::default();
                r.set_status(Arc::new(crate::status_storage::MemoryStatusStorage::default()), "mesh1".into(), "mesh1.admin.1".parse().unwrap(), NodeId::mint());
                Arc::new(r)
            },
        };
        let executor_name: PathName = "mesh2.admin.1".parse().unwrap();
        let birth = (executor.node.node_id.to_string(), executor.node.incarnation.to_string());
        let capture = crate::span_capture::Capture::new("i143-3051-executor-exit-proof-point-read");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);

        let held = Arc::new(CountsRuntimeLists::default());
        held.put_runtime(&row).await.unwrap();
        departure(held.clone()).proven(&executor_name).await.expect("the held row's runtime is proven exited");
        let unwritten = Arc::new(CountsRuntimeLists::default());
        let never = departure(unwritten.clone()).proven(&executor_name).await.unwrap_err();
        let refused = Arc::new(CountsRuntimeLists { point_refused: true, ..Default::default() });
        let unread = departure(refused.clone()).proven(&executor_name).await.unwrap_err();

        let lists = |s: &CountsRuntimeLists| s.lists.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!((lists(&held), lists(&unwritten), lists(&refused)), (0, 0, 0), "the proof reads one row by its key, never the list of every row");
        assert_eq!(capture.named("rdm.node_admin.build.update.via-executor-exit-proof").len(), 1, "the held row proves the exit once");
        assert!(never.contains("no durable runtime row") && !never.contains("could not be read"), "never written is its own refusal: {never}");
        assert!(unread.contains("could not be read") && unread.contains("Input/output error (os error 5)"), "unreadable names the store's cause: {unread}");
        let attrs = |name: &str| {
            let spans = capture.named(name);
            assert_eq!(spans.len(), 1, "{name} once: {spans:?}");
            eprintln!("span {name} trace_id={} {:?}", spans[0].trace_id, spans[0].attributes);
            spans[0].attributes.clone()
        };
        let attr = |a: &std::collections::BTreeMap<String, String>, k: &str| a.get(k).cloned().unwrap_or_default();
        let no_row = attrs("rdm.node_admin.node.reject.via-no-runtime-row");
        assert_eq!((attr(&no_row, "reader"), attr(&no_row, "node"), attr(&no_row, "node_id"), attr(&no_row, "incarnation_id"), attr(&no_row, "store")), ("mesh1.admin.1".to_string(), "mesh2.admin.1".to_string(), birth.0.clone(), birth.1.clone(), "nodes.storage".to_string()), "{no_row:?}");
        let unreadable = attrs("rdm.node_admin.node.reject.via-runtime-row-unreadable");
        assert_eq!((attr(&unreadable, "reader"), attr(&unreadable, "node"), attr(&unreadable, "node_id"), attr(&unreadable, "incarnation_id"), attr(&unreadable, "store")), ("mesh1.admin.1".to_string(), "mesh2.admin.1".to_string(), birth.0, birth.1, "nodes.storage".to_string()), "{unreadable:?}");
        assert!(attr(&unreadable, "error").contains("Input/output error (os error 5)"), "{unreadable:?}");
    }

    /// CONTRACT (fabric-node-lifecycle: a re-run never calls the lost executor or proves its exit
    /// again): the fabric-primary's departure proof answers `recorded` from the exit-proof row of
    /// the EXACT birth the view names for the executor. Must NOT happen: `recorded` false
    /// for a birth whose exit is recorded (the drive would call the lost executor again and a
    /// second via-executor-exit-proof would follow), or true for an earlier birth of the same node.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_departure_recorded_for_the_executors_exact_birth_is_answered_without_a_new_proof() {
        use crate::build_drive::DepartureProof as _;
        let fact = rafka_mesh_entity::RuntimeFact::of_this_process(&rafka_mesh_entity::fixture_crockford60("dep-recorded")).unwrap();
        let executor = digest("mesh2.admin.1", MemberStatus::ReadyForTraffic);
        let book = DigestBook::default();
        book.record(executor.clone());
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let records = Arc::new(Records::default());
        records.set_status(Arc::new(crate::status_storage::MemoryStatusStorage::default()), "mesh1".into(), "mesh1.admin.1".parse().unwrap(), NodeId::mint());
        let departure = ProviderDeparture {
            me: "mesh1.admin.1".parse().unwrap(),
            topology: Arc::new(RwLock::new(view)),
            refresh: Arc::new(|| Box::pin(async {})),
            book,
            provider: Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(1) }, inspected: Default::default() }),
            nodes: Arc::new(CountsRuntimeLists::default()),
            records: records.clone(),
        };
        let name: PathName = "mesh2.admin.1".parse().unwrap();
        assert!(!departure.recorded(&name).await, "nothing is recorded yet");
        let mut birth = Node::allocated(name.clone());
        birth.node_id = executor.node.node_id.clone();
        birth.incarnation_id = Some(IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("an-earlier-birth")).unwrap());
        records.put_exit(&birth, Some(1), "provider").await.unwrap();
        assert!(!departure.recorded(&name).await, "an earlier birth's exit is not this birth's");
        birth.incarnation_id = Some(executor.node.incarnation.clone());
        records.put_exit(&birth, Some(1), "provider").await.unwrap();
        assert!(departure.recorded(&name).await, "the exact birth's exit is recorded, so the drive takes over without a new proof");
        assert!(!departure.recorded(&"mesh9.admin.1".parse().unwrap()).await, "a node outside the view names no birth");
    }

    /// The mesh primary `mesh2.admin.1`'s recovery of its own mesh's births, over `book`, `provider`
    /// and `nodes_storage`, and the TRUE OFFLINE member `mesh2.rpc.1` of `view`.
    fn recovery_of(view: &Topology, book: DigestBook, provider: Arc<dyn DeploymentProvider>, nodes_storage: Arc<dyn crate::storage::NodesStorage>, status: Arc<dyn crate::status_storage::StatusStorage>) -> (RecoverySubmitter, Arc<dyn crate::status_storage::StatusStorage>, Node) {
        let member = view.node(&"mesh2.rpc.1".parse().unwrap()).cloned().expect("the member is in the view");
        let recovery = RecoverySubmitter {
            me: "mesh2.admin.1".parse().unwrap(),
            topology: Arc::new(RwLock::new(view.clone())),
            book,
            provider,
            nodes_storage,
            status_storage: status.clone(),
            door: Default::default(),
            builds: Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        };
        (recovery, status, member)
    }

    /// What the mesh primary's two ticks over one TRUE OFFLINE birth left: the verdict, the
    /// inspection spans, the submit spans, and the birth's exit-proof row.
    async fn two_ticks(recovery: &RecoverySubmitter, status: &dyn crate::status_storage::StatusStorage, member: &Node, cell: &str) -> (Inspected, Vec<crate::span_capture::Captured>, usize, Option<crate::status_storage::NodeExitRow>) {
        let capture = crate::span_capture::Capture::new(cell);
        let verdicts = Mutex::new(HashMap::new());
        let outcome = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            let first = recovery.recover_if_proven(member, &verdicts).await;
            let second = recovery.recover_if_proven(member, &verdicts).await;
            assert_eq!(first, second, "the same birth, the same verdict on the next tick");
            first
        };
        let inspections = capture.named("rdm.node_admin.node.resolve.via-provider-inspection");
        let submits = capture.named("rdm.node_admin.build.update.via-node-recovery").len();
        let proof = status.node_exit(&member.node_id, member.incarnation_id.as_ref().unwrap()).await.unwrap();
        (outcome, inspections, submits, proof)
    }

    /// CONTRACT (node-recovery.md "Detection and proof" step 3: "Exited is the proof, recorded
    /// durably. Still running is no proof"; operation summary "Trigger: Exact provider proof only"):
    /// the mesh primary asks the provider about a TRUE OFFLINE member's exact runtime and the
    /// provider cannot tell (`Unknown`). That is no proof: the verdict is not-exited with the
    /// provider's answer as its reason, named once over two ticks, and nothing is recorded or
    /// submitted. Must NOT happen: an exit-proof row, a recovery submit, a verdict spanned every tick.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_true_offline_member_the_provider_cannot_tell_about_is_not_proven_exited_and_never_recovered() {
        let fact = rafka_mesh_entity::RuntimeFact::of_this_process(&rafka_mesh_entity::fixture_crockford60("dep-unknown")).unwrap();
        let mut member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
        member.node.runtime = Some(fact.clone());
        let book = DigestBook::default();
        for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
            book.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Unknown, inspected: Default::default() });
        let (recovery, status, node) = recovery_of(&view, book, provider.clone(), Arc::new(crate::storage::MemoryNodesStorage::default()), Arc::new(crate::status_storage::MemoryStatusStorage::default()));
        let (outcome, inspections, submits, proof) = two_ticks(&recovery, &*status, &node, "i143-3051-provider-unknown").await;
        assert_eq!(outcome, Inspected::NotExited("its runtime is not proven exited: Unknown".into()), "Unknown is no proof");
        assert_eq!(provider.inspected.load(std::sync::atomic::Ordering::SeqCst), 2, "the provider was asked about the exact runtime on each tick");
        assert_eq!(inspections.len(), 1, "the verdict is named once: {inspections:?}");
        let a = &inspections[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("node"), attr("node_id"), attr("incarnation_id")), ("mesh2.rpc.1".to_string(), node.node_id.to_string(), member.node.incarnation.to_string()), "the exact birth: {a:?}");
        assert_eq!((attr("verdict"), attr("exit_proof"), attr("reason")), ("not-exited".to_string(), "none".to_string(), "its runtime is not proven exited: Unknown".to_string()), "{a:?}");
        assert_eq!(attr("inspector"), "mesh2.admin.1", "{a:?}");
        assert_eq!((submits, proof), (0, None), "no exit-proof row and no recovery submit without an exit proof");
        eprintln!("span rdm.node_admin.node.resolve.via-provider-inspection trace_id={} {a:?}", inspections[0].trace_id);
    }

    /// CONTRACT (node-recovery.md "Detection and proof" step 3: "Exited is the proof, recorded
    /// durably"; CLAUDE.md: an error state names what failed and where): the provider proves a TRUE
    /// OFFLINE member exited and status.storage refuses the birth's exit-proof row. The submit ends
    /// there, on its own `via-node-recovery` span, whose outcome names the row and the store's own
    /// cause; nothing reaches the fabric-primary (no Build named) and no row stands. Must NOT happen:
    /// a refused exit-proof write with no span (an info line only), or a submit without its row.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_refused_exit_proof_write_ends_the_recovery_submit_on_its_span_naming_the_stores_cause() {
        let fact = rafka_mesh_entity::RuntimeFact::of_this_process(&rafka_mesh_entity::fixture_crockford60("dep-exit-refused")).unwrap();
        let mut member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
        member.node.runtime = Some(fact.clone());
        let book = DigestBook::default();
        for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
            book.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(1) }, inspected: Default::default() });
        let refusing = crate::offline::tests::SilenceRefusing { exit: true, ..Default::default() };
        let (recovery, status, node) = recovery_of(&view, book, provider, Arc::new(crate::storage::MemoryNodesStorage::default()), Arc::new(refusing));
        let capture = crate::span_capture::Capture::new("i143-3051-exit-proof-refused");
        let outcome = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            recovery.recover_if_proven(&node, &Mutex::new(HashMap::new())).await
        };
        assert_eq!(outcome, Inspected::Exited(Some(1), "provider"), "the provider proved the exit");
        assert_eq!(status.node_exit(&node.node_id, node.incarnation_id.as_ref().unwrap()).await.unwrap(), None, "no exit-proof row stands");
        let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
        assert_eq!(submits.len(), 1, "the submit that ended on the refused row is spanned: {submits:?}");
        let a = &submits[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("node"), attr("node_id"), attr("incarnation_id")), ("mesh2.rpc.1".to_string(), node.node_id.to_string(), member.node.incarnation.to_string()), "the exact birth: {a:?}");
        assert!(attr("outcome").contains("exit-proof row") && attr("outcome").contains("No space left on device (os error 28)"), "the outcome names the row and the store's cause: {a:?}");
        assert_eq!(attr("build_id"), "", "nothing reached the fabric-primary: {a:?}");
        eprintln!("span rdm.node_admin.build.update.via-node-recovery trace_id={} {a:?}", submits[0].trace_id);
    }

    /// CONTRACT (node-recovery.md "Failure and receipts": "the mesh-primary is the submitter that
    /// resubmits by `build_id` ... the mesh-primary's next tick finds the member still proven exited
    /// and submits"; CLAUDE.md read rule: node-local state that should be the durable answer is
    /// replaced by the read): a submitter that holds nothing in memory (an admin that just took the
    /// seat, or restarted) is owed the exited birth mesh2.rpc.1 by the durable facts alone: the
    /// current Build holds that birth's run, whose attempt ended Failed after its
    /// delete, and the birth is in no view. Nothing is owed while this admin does not hold the seat or
    /// a fabric shutdown froze recovery, nor to another mesh. With no exit-proof row the submit is
    /// refused on `rdm.node_admin.node.reject.via-exit-proof-unread` (reason `missing`) and nothing is
    /// submitted; once status.storage holds the row, the submit span carries the row's exit code and
    /// proof and the Failed attempt it follows. Must NOT happen: an owed exit found only in a previous
    /// submitter's memory, or a submit with no exit proof.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_submitter_that_holds_nothing_in_memory_reads_the_failed_exit_from_the_durable_facts() {
        let book = DigestBook::default();
        book.record(digest("mesh2.admin.1", MemberStatus::ReadyForTraffic));
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let (path, node_id, incarnation): (PathName, _, _) = ("mesh2.rpc.1".parse().unwrap(), NodeId::mint(), IncarnationId::mint());
        assert!(view.node(&path).is_none(), "the exited birth departed: no view holds it");
        let b1 = crate::build::BuildId::parse(&rafka_mesh_entity::fixture_crockford60("b1")).unwrap();
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        let deleted = |step: &str| crate::build_state::BuildStepReceipt {
            mesh: None,
            build_id: b1.clone(),
            attempt: 2,
            operation: format!("retire-node:{path}"),
            step: step.into(),
            outcome: crate::build_state::StepOutcome::Complete,
            output: (step == "NodeDeleted").then(|| {
                serde_json::to_value(rafka_mesh_entity::LifecycleOp { build_id: "b1".into(), attempt: 2, operation: format!("retire-node:{path}"), node_id: node_id.clone(), incarnation: incarnation.clone(), name: path.clone(), event_at_rafka_ms: 1 }).unwrap()
            }),
            executor: Some("mesh2.admin.2".into()),
        };
        builds.absorb(&[
            crate::build_state::BuildFact::Accepted(crate::build_state::BuildAccepted { build_id: b1.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 1 }),
            crate::build_state::BuildFact::Opened(crate::build_state::AttemptOpened {
                mesh: None,
                build_id: b1.clone(),
                attempt: 2,
                reason: crate::build_state::AttemptReason::Recovery,
                action: Some(crate::accepted::AttemptAction::Recover { path: path.clone(), node_id: node_id.clone(), from_incarnation: incarnation.clone() }),
                opened_by: "mesh1.admin.1".into(),
                opened_at_ms: 1,
            }),
            crate::build_state::BuildFact::Claim(crate::build_state::BuildAttemptClaim { claimant: None, mesh: None, build_id: b1.clone(), attempt: 2, executor: "mesh2.admin.2".into() }),
            crate::build_state::BuildFact::Step(deleted("NodeDeleted")),
            crate::build_state::BuildFact::Step(deleted("Complete")),
            crate::build_state::BuildFact::Attempt(crate::build_state::BuildAttemptReceipt { mesh: None, build_id: b1.clone(), attempt: 2, outcome: crate::build_state::AttemptOutcome::Failed { reason: "create-node: PrepareStorage".into() } }),
        ]).await;
        let fabric = Arc::new(crate::fabric_storage::MemoryFabricStorage::new());
        crate::fabric_storage::FabricStorage::claim_identity(&*fabric, &crate::fabric_storage::FabricIdentity { fabric_id: fabric1(), name: "fabric1".into() }).await.unwrap().expect("the fabric's name is claimed");
        let status = Arc::new(crate::status_storage::MemoryStatusStorage::default());
        let recovery = RecoverySubmitter {
            me: "mesh2.admin.1".parse().unwrap(),
            topology: Arc::new(RwLock::new(view)),
            book,
            provider: Arc::new(Answers { domain: String::new(), status: crate::deployment::provider::DeploymentStatus::Unknown, inspected: Default::default() }),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            status_storage: status.clone(),
            door: Default::default(),
            builds,
        };
        assert_eq!(recovery.owed_submits(false, false, "mesh2").await, vec![], "nothing is owed by an admin that does not hold the seat");
        assert_eq!(recovery.owed_submits(true, true, "mesh2").await, vec![], "nothing is owed while a fabric shutdown froze recovery");
        assert_eq!(recovery.owed_submits(true, false, "mesh1").await, vec![], "another mesh's primary is owed none of mesh2's exits");
        let owed = recovery.owed_submits(true, false, "mesh2").await;
        assert_eq!(owed, vec![crate::build_state::OwedExit { path: path.clone(), node_id: node_id.clone(), incarnation: incarnation.clone(), attempt: 2, reason: "create-node: PrepareStorage".into() }], "read from the durable facts alone");

        let capture = crate::span_capture::Capture::new("i143-3088-owed-exit-from-durable-facts");
        let (unproven, submitted) = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            let unproven = recovery.submit_owed(&owed[0]).await;
            crate::offline::put_exit_proof(&*status, &{
                let mut n = Node::allocated(path.clone());
                n.node_id = node_id.clone();
                n.incarnation_id = Some(incarnation.clone());
                n
            }, Some(137), "provider", 1)
            .await
            .unwrap();
            (unproven, recovery.submit_owed(&owed[0]).await)
        };
        let refusals = capture.named("rdm.node_admin.node.reject.via-exit-proof-unread");
        assert_eq!(refusals.len(), 1, "the submit with no exit-proof row is refused on its span: {refusals:?}");
        let a = &refusals[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("node"), attr("node_id"), attr("incarnation_id"), attr("submitter"), attr("failed_attempt"), attr("reason")), (path.to_string(), node_id.to_string(), incarnation.to_string(), "mesh2.admin.1".to_string(), "2".to_string(), "missing".to_string()), "{a:?}");
        assert!(unproven.as_ref().is_err_and(|e| e.contains("no exit-proof row") && e.contains("missing")), "{unproven:?}");
        let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
        assert_eq!(submits.len(), 1, "only the submit with its exit proof reached the submit span: {submits:?}");
        let s = &submits[0].attributes;
        let sattr = |k: &str| s.get(k).cloned().unwrap_or_default();
        assert_eq!((sattr("node"), sattr("node_id"), sattr("incarnation_id"), sattr("submitter")), (path.to_string(), node_id.to_string(), incarnation.to_string(), "mesh2.admin.1".to_string()), "keyed on the exact exited birth: {s:?}");
        assert_eq!((sattr("exit_code"), sattr("exit_proof"), sattr("after_failed_attempt")), ("137".to_string(), "provider".to_string(), "2".to_string()), "the row's proof and the Failed attempt it follows: {s:?}");
        assert!(submitted.is_err() && !sattr("outcome").is_empty(), "this cell has no fabric-primary door: the submit ends named: {s:?}");
        eprintln!("span rdm.node_admin.node.reject.via-exit-proof-unread trace_id={} {a:?}", refusals[0].trace_id);
        eprintln!("span rdm.node_admin.build.update.via-node-recovery trace_id={} {s:?}", submits[0].trace_id);
    }

    /// CONTRACT (node-recovery.md: "Node recovery is RDM's drift reconciliation for a member that
    /// exits outside any workflow"; "Failure and receipts": "An exit during a workflow belongs to
    /// that workflow"): a birth whose removal the mesh primary's book holds announced (a replace's
    /// `NodeDeleting` overlay naming the exact birth, or its `NodeDeleted` departure) is not a
    /// member to recover, however exited its exact runtime is proven: its exit recorded as its
    /// exit-proof row, no recovery submit, one `rdm.node_admin.build.reject.via-exit-inside-workflow`
    /// span naming the workflow. A recovery's own node.delete of the exited birth (`retire-node:<path>`) is such a
    /// workflow while its overlay stands. Must NOT happen: the
    /// recover-node of the predecessor a replace retired (trace 3b983b72926d8d5e3629d7e3d1f264cf:
    /// the tick's ladder round 2 at 12:38:13.028 over the view it read before the replace's
    /// NodeDeleting at 12.808, inspection exited at 13.049 by the replace's own TerminateRuntime,
    /// submit at 13.059, recover-node failed NodeDeleting "the inherited data dir ... does not
    /// exist": HandoffStorage had given it to the successor).
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_proven_exit_of_a_birth_whose_removal_was_announced_submits_no_recovery() {
        let fact = rafka_mesh_entity::RuntimeFact::of_this_process(&rafka_mesh_entity::fixture_crockford60("dep-removal-announced")).unwrap();
        for (case, announced, workflow) in [("replace-deleting", "node-deleting", "replace-node"), ("replace-deleted", "node-deleted", "replace-node"), ("recovery-delete-deleting", "node-deleting", "retire-node")] {
            let mut member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
            member.node.runtime = Some(fact.clone());
            let book = DigestBook::default();
            for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
                book.record(d);
            }
            // The tick's view, read before the announcement arrived.
            let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
            let op = rafka_mesh_entity::LifecycleOp {
                build_id: "dbs8z9896j0f".into(),
                attempt: 2,
                operation: format!("{workflow}:mesh2.rpc.1"),
                node_id: member.node.node_id.clone(),
                incarnation: member.node.incarnation.clone(),
                name: member.node.name.clone(),
                event_at_rafka_ms: 1,
            };
            assert!(book.deleting(op.clone()), "{case}: the overlay is taken");
            if announced == "node-deleted" {
                assert!(book.depart(op.clone()), "{case}: the departure is taken");
            }
            // The birth's durable runtime row (every admin's blind put on first holding it): the
            // inspection proves the exit from it once the departure dropped the digest.
            let nodes = Arc::new(crate::storage::MemoryNodesStorage::default());
            crate::storage::NodesStorage::put_runtime(&*nodes, &crate::storage::RuntimeRow { node_id: member.node.node_id.clone(), name: member.node.name.clone(), incarnation_id: member.node.incarnation.clone(), runtime: fact.clone(), data_dir: None }).await.unwrap();
            let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(137) }, inspected: Default::default() });
            let (recovery, status, node) = recovery_of(&view, book, provider, nodes, Arc::new(crate::status_storage::MemoryStatusStorage::default()));
            let capture = crate::span_capture::Capture::new(&format!("i143-3068-removal-announced-{case}"));
            let outcome = {
                let _g = tracing::dispatcher::set_default(&capture.dispatch);
                recovery.recover_if_proven(&node, &Mutex::new(HashMap::new())).await
            };
            assert_eq!(outcome, Inspected::Exited(Some(137), "provider"), "{case}: the provider proved the exact runtime exited");
            let proof = status.node_exit(&node.node_id, node.incarnation_id.as_ref().unwrap()).await.unwrap();
            let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
            let rejects = capture.named("rdm.node_admin.build.reject.via-exit-inside-workflow");
            assert_eq!((submits.len(), proof.is_some()), (0, true), "{case}: no recovery submit for a birth whose removal was announced, and its proven exit recorded as its exit-proof row (the proven loss every projection reads): {submits:?}");
            assert_eq!(rejects.len(), 1, "{case}: the refusal is spanned once: {rejects:?}");
            let a = &rejects[0].attributes;
            let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
            assert_eq!((attr("node"), attr("node_id"), attr("incarnation_id")), ("mesh2.rpc.1".to_string(), node.node_id.to_string(), member.node.incarnation.to_string()), "{case}: the exact birth: {a:?}");
            assert_eq!((attr("announced"), attr("operation"), attr("build_id"), attr("attempt")), (announced.to_string(), format!("{workflow}:mesh2.rpc.1"), "dbs8z9896j0f".to_string(), "2".to_string()), "{case}: the workflow the exit belongs to: {a:?}");
            assert_eq!(attr("submitter"), "mesh2.admin.1", "{case}: {a:?}");
            eprintln!("span rdm.node_admin.build.reject.via-exit-inside-workflow trace_id={} {a:?}", rejects[0].trace_id);
        }
    }

    /// CONTRACT (node-recovery.md "Failure and receipts": "An exit during a workflow belongs to that
    /// workflow"; mesh-delete.md: from mesh.deleting, the mesh's status deleting, to mesh.deleted, every
    /// member's removal is the mesh.delete's): an exited member of a mesh whose status the mesh primary
    /// holds deleting or deleted is not a member to recover, a primary lost mid-delete included: its
    /// exit is recorded as its exit-proof row, no recovery is submitted, and one
    /// `rdm.node_admin.build.reject.via-exit-inside-workflow` span names the mesh.delete. Must NOT
    /// happen: mesh_lifecycle__mesh_leave_resume on f8001275, where mesh2's surviving admin, seated
    /// after its primary was killed mid-delete, submitted the recovery of that primary 220 times
    /// (trace 8b3d152a6a51c90fc06a4031cd055d37, refused while the Build reconciled).
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_proven_exit_of_a_member_of_a_deleting_or_deleted_mesh_submits_no_recovery() {
        let fact = rafka_mesh_entity::RuntimeFact::of_this_process(&rafka_mesh_entity::fixture_crockford60("dep-mesh-deleting")).unwrap();
        for (status, announced) in [("leaving", "mesh-deleting"), ("dead", "mesh-deleted")] {
            let mut member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
            member.node.runtime = Some(fact.clone());
            let book = DigestBook::default();
            for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
                book.record(d);
            }
            book.statuses.take(&rafka_mesh_transport::membership::Frame::MeshStatus { mesh: "mesh2".into(), status: status.into(), publisher: "mesh2.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10 }, &fabric1());
            let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
            let nodes = Arc::new(crate::storage::MemoryNodesStorage::default());
            crate::storage::NodesStorage::put_runtime(&*nodes, &crate::storage::RuntimeRow { node_id: member.node.node_id.clone(), name: member.node.name.clone(), incarnation_id: member.node.incarnation.clone(), runtime: fact.clone(), data_dir: None }).await.unwrap();
            let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(137) }, inspected: Default::default() });
            let (recovery, status_rows, node) = recovery_of(&view, book, provider, nodes, Arc::new(crate::status_storage::MemoryStatusStorage::default()));
            let capture = crate::span_capture::Capture::new(&format!("i143-3078-mesh-{status}"));
            let outcome = {
                let _g = tracing::dispatcher::set_default(&capture.dispatch);
                recovery.recover_if_proven(&node, &Mutex::new(HashMap::new())).await
            };
            assert_eq!(outcome, Inspected::Exited(Some(137), "provider"), "{status}: the provider proved the exact runtime exited");
            let proof = status_rows.node_exit(&node.node_id, node.incarnation_id.as_ref().unwrap()).await.unwrap();
            let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
            let rejects = capture.named("rdm.node_admin.build.reject.via-exit-inside-workflow");
            assert_eq!((submits.len(), proof.is_some()), (0, true), "{status}: no recovery of a member of a mesh being deleted, its exit recorded: {submits:?}");
            assert_eq!(rejects.len(), 1, "{status}: the refusal is spanned once: {rejects:?}");
            let a = &rejects[0].attributes;
            assert_eq!(a.get("announced").map(String::as_str), Some(announced), "{status}: {a:?}");
            assert!(a.get("operation").is_some_and(|o| o.contains("mesh2") || o.starts_with("shutdown-mesh:")), "{status}: the mesh.delete is named: {a:?}");
            eprintln!("span rdm.node_admin.build.reject.via-exit-inside-workflow trace_id={} {a:?}", rejects[0].trace_id);
        }
    }

    /// A status store counting every exit-proof row it is offered (`put_node_exit`); every row is
    /// the memory store's.
    #[derive(Default)]
    struct CountsExitPuts {
        inner: crate::status_storage::MemoryStatusStorage,
        exit_puts: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::status_storage::StatusStorage for CountsExitPuts {
        async fn put_mesh_status(&self, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_mesh_status(row).await
        }
        async fn mesh_statuses(&self) -> Result<HashMap<crate::model::MeshId, rafka_node_rpc_contract::status::MeshState>, crate::record_store::StorageError> {
            self.inner.mesh_statuses().await
        }
        async fn put_fabric_event(&self, row: &crate::status_storage::FabricEventRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_fabric_event(row).await
        }
        async fn fabric_events(&self) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::record_store::StorageError> {
            self.inner.fabric_events().await
        }
        async fn fabric_events_of(&self, mesh: &str) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::record_store::StorageError> {
            self.inner.fabric_events_of(mesh).await
        }
        async fn put_node_declared(&self, row: &crate::status_storage::NodeDeclaredRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_declared(row).await
        }
        async fn remove_node_declared(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), crate::record_store::StorageError> {
            self.inner.remove_node_declared(node_id, incarnation_id).await
        }
        async fn node_declared(&self) -> Result<Vec<crate::status_storage::NodeDeclaredRow>, crate::record_store::StorageError> {
            self.inner.node_declared().await
        }
        async fn put_node_silence(&self, row: &crate::status_storage::NodeSilenceRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_silence(row).await
        }
        async fn node_silences(&self) -> Result<Vec<crate::status_storage::NodeSilenceRow>, crate::record_store::StorageError> {
            self.inner.node_silences().await
        }
        async fn put_node_exit(&self, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::record_store::StorageError> {
            self.exit_puts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.put_node_exit(row).await
        }
        async fn node_exit(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::record_store::StorageError> {
            self.inner.node_exit(node_id, incarnation_id).await
        }
        async fn mesh_status_held_of(&self, mesh: &str, row: &crate::status_storage::MeshStatusRow) -> Result<bool, crate::record_store::StorageError> {
            self.inner.mesh_status_held_of(mesh, row).await
        }
        async fn put_mesh_status_of(&self, mesh: &str, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_mesh_status_of(mesh, row).await
        }
        async fn node_exit_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::record_store::StorageError> {
            self.inner.node_exit_of(mesh, node_id, incarnation_id).await
        }
        async fn put_node_exit_of(&self, mesh: &str, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::record_store::StorageError> {
            self.exit_puts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.put_node_exit_of(mesh, row).await
        }
    }

    /// CONTRACT (node-recovery.md "Detection and proof" step 3: "Exited is the proof, recorded
    /// durably"; the exit-proof row is a blind put of the birth's own key, offline.rs
    /// `put_exit_proof`): the mesh primary's tick finds the same exited birth TRUE OFFLINE on three
    /// ticks, and the birth's exit-proof row is put once, on the tick that proved the exit. Both arms
    /// of the tick hold it: a birth whose removal was announced (its `retire-node` NodeDeleting
    /// overlay; `via-exit-inside-workflow` names the row `put` on the first tick and `recorded`
    /// after), and a member outside any workflow whose recovery submit ends without the recovery
    /// (here its Build door is not built), submitted again on each tick (node-recovery.md "Failure
    /// and receipts"). Must NOT happen: the row put again on every tick for an exit already recorded
    /// (admin.rs `recover_if_proven`, s60 part 15's finding: the refusal arm puts it per tick).
    // @feature: node-lifecycle
    #[tokio::test]
    async fn the_mesh_primary_records_a_proven_exit_once_over_every_tick_that_finds_it() {
        let fact = rafka_mesh_entity::RuntimeFact::of_this_process(&rafka_mesh_entity::fixture_crockford60("dep-exit-once")).unwrap();
        for case in ["announced", "submitted"] {
            let mut member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
            member.node.runtime = Some(fact.clone());
            let book = DigestBook::default();
            for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
                book.record(d);
            }
            let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
            if case == "announced" {
                let op = rafka_mesh_entity::LifecycleOp {
                    build_id: "dbs8z9896j0f".into(),
                    attempt: 2,
                    operation: "retire-node:mesh2.rpc.1".into(),
                    node_id: member.node.node_id.clone(),
                    incarnation: member.node.incarnation.clone(),
                    name: member.node.name.clone(),
                    event_at_rafka_ms: 1,
                };
                assert!(book.deleting(op), "{case}: the overlay is taken");
            }
            let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(137) }, inspected: Default::default() });
            let counting = Arc::new(CountsExitPuts::default());
            let (recovery, status, node) = recovery_of(&view, book, provider.clone(), Arc::new(crate::storage::MemoryNodesStorage::default()), counting.clone());
            let capture = crate::span_capture::Capture::new(&format!("i143-s78-exit-recorded-once-{case}"));
            let verdicts = Mutex::new(HashMap::new());
            {
                let _g = tracing::dispatcher::set_default(&capture.dispatch);
                for tick in 1..=3 {
                    assert_eq!(recovery.recover_if_proven(&node, &verdicts).await, Inspected::Exited(Some(137), "provider"), "{case}: tick {tick} proves the exit");
                }
            }
            assert_eq!(provider.inspected.load(std::sync::atomic::Ordering::SeqCst), 3, "{case}: the provider is asked on every tick");
            assert!(status.node_exit(&node.node_id, node.incarnation_id.as_ref().unwrap()).await.unwrap().is_some_and(|r| r.exit_code == Some(137) && r.proof == "provider"), "{case}: the birth's exit-proof row stands");
            assert_eq!(counting.exit_puts.load(std::sync::atomic::Ordering::SeqCst), 1, "{case}: the exit-proof row is put once, on the tick that proved the exit, never per tick");
            let inspections = capture.named("rdm.node_admin.node.resolve.via-provider-inspection");
            assert_eq!(inspections.len(), 1, "{case}: the verdict is named once: {inspections:?}");
            if case == "announced" {
                let rejects = capture.named("rdm.node_admin.build.reject.via-exit-inside-workflow");
                let rows: Vec<String> = rejects.iter().map(|r| r.attributes.get("exit_proof_row").cloned().unwrap_or_default()).collect();
                assert_eq!(rows, ["put", "recorded", "recorded"], "{case}: each tick's refusal names the row: put when that tick put it, recorded after: {rejects:?}");
                eprintln!("span rdm.node_admin.build.reject.via-exit-inside-workflow trace_id={} {:?}", rejects[0].trace_id, rejects[0].attributes);
            } else {
                let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
                assert_eq!(submits.len(), 3, "{case}: the recovery is submitted on each tick it is still owed: {submits:?}");
                eprintln!("span rdm.node_admin.build.update.via-node-recovery trace_id={} {:?}", submits[0].trace_id, submits[0].attributes);
            }
        }
    }

    /// CONTRACT (gossip.md `NodeDeleting`: keyed `(kind, build_id, attempt, operation)`, "the fact
    /// rides `Members.in_flight` until the operation completes", and `Members.in_flight` is "while
    /// the operation is claimed and has no completed receipt (derived from Build facts each round)";
    /// node-recovery.md "Failure and receipts": "An exit during a workflow belongs to that workflow:
    /// its Build ends Failed, and the mesh-primary's next tick finds the member still proven exited
    /// and submits the recovery"): a removal's `NodeDeleting` overlay, opened from its attempt's
    /// receipt, closes when that attempt's verdict is `Failed` (one
    /// `rdm.mesh.membership.update.via-deleting-closed-by-facts` span naming the attempt), and the
    /// mesh primary's next tick then submits the recovery of the member whose exact runtime the
    /// failed workflow left exited: the exit-proof row is put and one `via-node-recovery` submit is
    /// made. Must NOT happen: the overlay held after its attempt failed, so the tick refuses the
    /// member `via-exit-inside-workflow` for a workflow that is over and the member is never put back.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_failed_removal_attempts_target_is_recovered_by_the_next_tick() {
        use crate::build_state::{AttemptOutcome, BuildAccepted, BuildAttemptReceipt, BuildFact, BuildStepReceipt, StepOutcome};
        let fact = rafka_mesh_entity::RuntimeFact::of_this_process(&rafka_mesh_entity::fixture_crockford60("dep-failed-removal")).unwrap();
        let mut member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
        member.node.runtime = Some(fact.clone());
        let book = DigestBook::default();
        for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
            book.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let op = rafka_mesh_entity::LifecycleOp {
            build_id: "9r7cpewghdkk".into(),
            attempt: 2,
            operation: "replace-node:mesh2.rpc.1".into(),
            node_id: member.node.node_id.clone(),
            incarnation: member.node.incarnation.clone(),
            name: member.node.name.clone(),
            event_at_rafka_ms: 1,
        };
        let build = crate::build::BuildId::parse(&op.build_id).unwrap();
        let mut facts = vec![
            BuildFact::Accepted(BuildAccepted { build_id: build.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh2", crate::model::MeshId::parse("meshd0000002").unwrap()), submitted_change: None, submitted_at_ms: 1 }),
            BuildFact::Step(BuildStepReceipt { mesh: None, build_id: build.clone(), attempt: 2, operation: op.operation.clone(), step: "NodeDeleting".into(), outcome: StepOutcome::Complete, output: Some(serde_json::to_value(&op).unwrap()), executor: None }),
        ];
        let capture = crate::span_capture::Capture::new("i143-3068-failed-removal-recovered");
        let mut named = NamedSteps::new();
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            overlays_from_facts(&facts, &book, &mut named);
        }
        assert_eq!(book.in_flight(), vec![op.clone()], "the removal's NodeDeleting receipt opens its overlay");
        // The attempt's verdict: the workflow failed (the member's runtime exited under it).
        facts.push(BuildFact::Attempt(BuildAttemptReceipt { mesh: None, build_id: build.clone(), attempt: 2, outcome: AttemptOutcome::Failed { reason: "TerminateRuntime: the member exited under the replace".into() } }));
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            overlays_from_facts(&facts, &book, &mut named);
        }
        assert_eq!(book.in_flight(), Vec::<rafka_mesh_entity::LifecycleOp>::new(), "the failed attempt's overlay leaves Members.in_flight: its operation is no longer claimed by that attempt");
        let closed = capture.named("rdm.mesh.membership.update.via-deleting-closed-by-facts");
        assert_eq!(closed.len(), 1, "the close is spanned once: {closed:?}");
        let a = &closed[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("operation"), attr("build_id"), attr("attempt"), attr("by")), ("replace-node:mesh2.rpc.1".to_string(), "9r7cpewghdkk".to_string(), "2".to_string(), "attempt-failed".to_string()), "the attempt the facts ended: {a:?}");
        eprintln!("span rdm.mesh.membership.update.via-deleting-closed-by-facts trace_id={} {a:?}", closed[0].trace_id);
        // A late copy of the closed attempt's overlay (a frame, an aggregate) is applied once, not again.
        assert!(!book.deleting(op.clone()), "the closed overlay is not opened again by a copy of it");
        // The mesh primary's next tick: the member's exact runtime is proven exited.
        let nodes = Arc::new(crate::storage::MemoryNodesStorage::default());
        crate::storage::NodesStorage::put_runtime(&*nodes, &crate::storage::RuntimeRow { node_id: member.node.node_id.clone(), name: member.node.name.clone(), incarnation_id: member.node.incarnation.clone(), runtime: fact.clone(), data_dir: None }).await.unwrap();
        let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(137) }, inspected: Default::default() });
        let (recovery, status, node) = recovery_of(&view, book, provider, nodes, Arc::new(crate::status_storage::MemoryStatusStorage::default()));
        let outcome = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            recovery.recover_if_proven(&node, &Mutex::new(HashMap::new())).await
        };
        assert_eq!(outcome, Inspected::Exited(Some(137), "provider"), "the provider proved the exact runtime exited");
        let rejects = capture.named("rdm.node_admin.build.reject.via-exit-inside-workflow");
        assert!(rejects.is_empty(), "no workflow holds the member: its failed attempt is over: {rejects:?}");
        assert!(status.node_exit(&node.node_id, node.incarnation_id.as_ref().unwrap()).await.unwrap().is_some(), "the exit-proof row is put");
        let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
        assert_eq!(submits.len(), 1, "the recovery is submitted: {submits:?}");
        eprintln!("span rdm.node_admin.build.update.via-node-recovery trace_id={} {:?}", submits[0].trace_id, submits[0].attributes);
    }

    /// CONTRACT (node-recovery.md "Failure and receipts": "An exit during a workflow belongs to that
    /// workflow: its Build ends Failed, and the mesh-primary's next tick finds the member still proven
    /// exited and submits the recovery"; R-BI7 caveat 2: a mesh's drift attempts are numbered in its
    /// own lane, so an attempt number names one attempt per lane): a recovery's `NodeDeleting` hold,
    /// opened from attempt 1 of mesh2's lane, closes when that attempt's verdict in mesh2's lane is
    /// `Failed` (one `rdm.mesh.membership.update.via-deleting-closed-by-facts` span, by
    /// `attempt-failed`). In another Build, the fabric lane's attempt 1 failing closes nothing of
    /// mesh2's attempt 1, which still runs: its hold stays open. Must NOT happen: a drift attempt's
    /// hold held after its attempt failed (the tick then refuses the member
    /// `via-exit-inside-workflow` for good), or one lane's verdict closing another lane's hold.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_drift_lanes_removal_hold_closes_on_its_own_attempts_verdict_and_on_no_other_lanes() {
        use crate::build_state::{AttemptOutcome, AttemptReason, AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStepReceipt, StepOutcome};
        let member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
        let book = DigestBook::default();
        for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
            book.record(d);
        }
        let mesh = || Some("mesh2".to_string());
        let recovery = |build: &str| rafka_mesh_entity::LifecycleOp {
            build_id: build.into(),
            attempt: 1,
            operation: "retire-node:mesh2.rpc.1".into(),
            node_id: member.node.node_id.clone(),
            incarnation: member.node.incarnation.clone(),
            name: member.node.name.clone(),
            event_at_rafka_ms: 1,
        };
        // mesh2's lane runs attempt 1, the member's recovery; its node.delete published NodeDeleting.
        let lane_attempt = |build: &crate::build::BuildId, op: &rafka_mesh_entity::LifecycleOp| {
            vec![
                BuildFact::Accepted(BuildAccepted { build_id: build.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh2", crate::model::MeshId::parse("meshd0000002").unwrap()), submitted_change: None, submitted_at_ms: 1 }),
                BuildFact::Opened(AttemptOpened { mesh: mesh(), build_id: build.clone(), attempt: 1, reason: AttemptReason::Recovery, action: None, opened_by: "mesh2.admin.1".into(), opened_at_ms: 1 }),
                BuildFact::Claim(BuildAttemptClaim { mesh: mesh(), build_id: build.clone(), attempt: 1, executor: "mesh2.admin.1".into(), claimant: Some("mesh2.admin.1".into()) }),
                BuildFact::Step(BuildStepReceipt { mesh: mesh(), build_id: build.clone(), attempt: 1, operation: op.operation.clone(), step: "NodeDeleting".into(), outcome: StepOutcome::Complete, output: Some(serde_json::to_value(op).unwrap()), executor: None }),
            ]
        };
        let capture = crate::span_capture::Capture::new("i143-s78-drift-lane-hold");
        let mut named = NamedSteps::new();
        let derive = |facts: &[BuildFact], named: &mut NamedSteps| {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            overlays_from_facts(facts, &book, named);
        };

        let op = recovery("9r7cpewghdkk");
        let build = crate::build::BuildId::parse(&op.build_id).unwrap();
        let mut facts = lane_attempt(&build, &op);
        derive(&facts, &mut named);
        assert_eq!(book.in_flight(), vec![op.clone()], "mesh2's attempt 1 holds the member");
        // The attempt's verdict in mesh2's lane: Failed (its claimant departed with it unfinished).
        facts.push(BuildFact::Attempt(BuildAttemptReceipt { mesh: mesh(), build_id: build.clone(), attempt: 1, outcome: AttemptOutcome::Failed { reason: "mesh2.admin.1 departed with attempt 1 of mesh2's lane unfinished".into() } }));
        derive(&facts, &mut named);
        assert_eq!(book.in_flight(), Vec::<rafka_mesh_entity::LifecycleOp>::new(), "the failed drift attempt's hold closes: the member's exit belongs to no workflow now");
        let closed = capture.named("rdm.mesh.membership.update.via-deleting-closed-by-facts");
        assert_eq!(closed.len(), 1, "the close is spanned once: {closed:?}");
        let a = &closed[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("operation"), attr("attempt"), attr("by")), ("retire-node:mesh2.rpc.1".to_string(), "1".to_string(), "attempt-failed".to_string()), "{a:?}");
        eprintln!("span rdm.mesh.membership.update.via-deleting-closed-by-facts trace_id={} {a:?}", closed[0].trace_id);

        // Another Build: the fabric lane's attempt 1 failed; mesh2's attempt 1 still runs.
        let op = recovery("7k2qm4xw9bdc");
        let build = crate::build::BuildId::parse(&op.build_id).unwrap();
        let mut facts = lane_attempt(&build, &op);
        facts.insert(1, BuildFact::Attempt(BuildAttemptReceipt { mesh: None, build_id: build.clone(), attempt: 1, outcome: AttemptOutcome::Failed { reason: "a topology operation failed".into() } }));
        derive(&facts, &mut named);
        assert_eq!(book.in_flight(), vec![op], "the fabric lane's verdict closes nothing of mesh2's running attempt");
        assert_eq!(capture.named("rdm.mesh.membership.update.via-deleting-closed-by-facts").len(), 1, "no second close");
    }

    /// CONTRACT (node-recovery.md "Detection and proof": the mesh primary watches its own mesh's
    /// births, those known only by a durable runtime row included; CLAUDE.md: an error state names
    /// what failed and where): when the tick's read of the durable runtime rows is refused, the
    /// refusal is one `rdm.node_admin.node.reject.via-runtime-rows-unreadable` span naming the
    /// reader, the store and its cause, and the tick watches the view's births only. Must NOT happen:
    /// the narrowed watch with no span (an info line only).
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_refused_read_of_the_durable_runtime_rows_is_spanned_and_the_tick_watches_the_views_births() {
        let book = DigestBook::default();
        for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic)] {
            book.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let provider = Arc::new(Answers { domain: String::new(), status: crate::deployment::provider::DeploymentStatus::Unknown, inspected: Default::default() });
        let (recovery, _, _) = recovery_of(&view, book, provider, Arc::new(RuntimeRowUnreadable::default()), Arc::new(crate::status_storage::MemoryStatusStorage::default()));
        let capture = crate::span_capture::Capture::new("i143-3051-runtime-rows-unreadable");
        let rows = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            recovery.durable_rows().await
        };
        assert!(rows.is_empty(), "no row is read from a refused store: the view's births only");
        let spans = capture.named("rdm.node_admin.node.reject.via-runtime-rows-unreadable");
        assert_eq!(spans.len(), 1, "the refused read is spanned: {spans:?}");
        let a = &spans[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("reader"), attr("store"), attr("error")), ("mesh2.admin.1".to_string(), "nodes.storage".to_string(), "runtimes: Input/output error (os error 5)".to_string()), "{a:?}");
        eprintln!("span rdm.node_admin.node.reject.via-runtime-rows-unreadable trace_id={} {a:?}", spans[0].trace_id);
    }

    /// CONTRACT (node-recovery.md "Detection and proof" step 3: the provider is asked "about the
    /// exact runtime recorded at launch"): a TRUE OFFLINE member whose digest carries no runtime is
    /// inspected through its durable runtime row, and that point read is refused. With no runtime
    /// fact nothing can be proven, whatever the provider would answer: the verdict is not-exited,
    /// naming the row and the store's own cause, the provider is not asked, and nothing is recorded
    /// or submitted. Must NOT happen: the refusal read as "no row", an inspection of a guessed
    /// runtime, an exit-proof row, a recovery submit.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_true_offline_member_whose_runtime_row_cannot_be_read_is_not_proven_exited_and_never_recovered() {
        let fact = rafka_mesh_entity::RuntimeFact::of_this_process(&rafka_mesh_entity::fixture_crockford60("dep-unreadable")).unwrap();
        let member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
        let book = DigestBook::default();
        for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
            book.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        // The provider would prove an exit: it must never be asked without the birth's runtime fact.
        let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(1) }, inspected: Default::default() });
        let (recovery, status, node) = recovery_of(&view, book, provider.clone(), Arc::new(RuntimeRowUnreadable::default()), Arc::new(crate::status_storage::MemoryStatusStorage::default()));
        let (outcome, inspections, submits, proof) = two_ticks(&recovery, &*status, &node, "i143-3051-runtime-row-unreadable").await;
        let why = format!("the durable runtime row of {} at {} could not be read: runtimes/{}--{}.json is not a record this build recognises: format runtime/9 is not one this build reads", node.node_id, member.node.incarnation.as_str(), node.node_id, member.node.incarnation.as_str());
        assert_eq!(outcome, Inspected::NotExited(why.clone()), "an unreadable row is no proof, and is named as unreadable");
        assert_eq!(provider.inspected.load(std::sync::atomic::Ordering::SeqCst), 0, "with no runtime fact the provider is not asked");
        assert_eq!(inspections.len(), 1, "the verdict is named once: {inspections:?}");
        let a = &inspections[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("verdict"), attr("exit_proof"), attr("reason")), ("not-exited".to_string(), "none".to_string(), why), "{a:?}");
        assert_eq!((submits, proof), (0, None), "no exit-proof row and no recovery submit without an exit proof");
        eprintln!("span rdm.node_admin.node.resolve.via-provider-inspection trace_id={} {a:?}", inspections[0].trace_id);
    }

    /// CONTRACT (states.md): a mesh is ready-for-traffic when every node of the mesh is. A mesh with a
    /// primary and a member not yet ready-for-traffic projects `pending`, and the mesh whose nodes
    /// are all ready projects `ready-for-traffic`. Must NOT happen: a mesh ready because it has a
    /// primary.
    #[test]
    fn a_mesh_with_a_node_not_ready_for_traffic_projects_pending() {
        use MemberStatus::*;
        let t = view(&[
            with_id(digest("mesh1.admin.1", ReadyForTraffic), "200000000000"),
            with_id(digest("mesh1.rpc.1", Pending), "300000000000"),
            with_id(digest("mesh2.admin.1", ReadyForTraffic), "400000000000"),
            with_id(digest("mesh2.rpc.1", ReadyForTraffic), "500000000000"),
        ]);
        assert!(t.cohort_primary("mesh1", NodeKind::NodeAdmin).is_some(), "mesh1 has a primary");
        let status = |m: &str| t.mesh_view(m).unwrap().status;
        assert_eq!(status("mesh1"), ScopeStatus::Pending, "a node of mesh1 is not ready: the mesh is not");
        assert_eq!(status("mesh2"), ScopeStatus::ReadyForTraffic, "every node of mesh2 is ready");
    }

    /// CONTRACT (states.md, "Resize and roll"): the nodes a mesh is counted over are the accepted
    /// Build's planned births. A planned birth the view has not heard keeps its mesh `pending`, and a
    /// mesh planned and not yet created is not listed. Must NOT happen: a mesh ready because
    /// the births that happen to be heard are ready.
    #[test]
    fn a_mesh_with_a_planned_birth_not_yet_heard_projects_pending() {
        use MemberStatus::*;
        let book = DigestBook::default();
        for d in [with_id(digest("mesh1.admin.1", ReadyForTraffic), "200000000000"), with_id(digest("mesh1.rpc.1", ReadyForTraffic), "300000000000")] {
            book.record(d);
        }
        let desired = |mesh: &str, rpc: u32| crate::build::MeshDesired::of(mesh, [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, rpc)]);
        let records = Records::default();
        let mut plan = crate::accepted::FabricTopology { fabric: "fabric1".into(), meshes: BTreeMap::new() };
        plan.meshes.insert("mesh1".into(), crate::accepted::MeshTopology::of(&desired("mesh1", 1)));
        records.set_planned(&plan);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.mesh_view("mesh1").unwrap().status, ScopeStatus::ReadyForTraffic, "every planned birth of mesh1 is heard and ready");
        plan.meshes.insert("mesh1".into(), crate::accepted::MeshTopology::of(&desired("mesh1", 2)));
        plan.meshes.insert("mesh2".into(), crate::accepted::MeshTopology::of(&desired("mesh2", 1)));
        records.set_planned(&plan);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.mesh_view("mesh1").unwrap().status, ScopeStatus::Pending, "mesh1.rpc.2 is planned and not heard");
        assert!(t.mesh_view("mesh2").is_none(), "mesh2 is planned and does not exist yet: the view lists what is observed, so the Build still plans its creation");
        let why = t.mesh_unready("mesh1", records.planned().get("mesh1")).iter().map(|m| m.to_string()).collect::<Vec<_>>();
        assert_eq!(why, ["mesh1.rpc.2(no-birth-heard)"], "the node behind a pending mesh is named");
    }

    /// CONTRACT (R-S3, states.md): a mesh's lifecycle is pending, ready-for-traffic, leaving, dead. A mesh
    /// heard ready-for-traffic that loses a node projects ready while it has a primary, so the primary that
    /// takes over adopts the state and runs its round. A mesh never heard ready projects pending while a
    /// node is not ready. Must NOT happen: a ready mesh projected pending because one of its nodes is gone,
    /// which gives the takeover a new status and no round.
    #[test]
    fn a_mesh_heard_ready_stays_ready_when_a_node_is_lost() {
        use MemberStatus::*;
        let book = DigestBook::default();
        for d in [with_id(digest("mesh1.admin.1", ReadyForTraffic), "200000000000"), with_id(digest("mesh1.rpc.1", ReadyForTraffic), "300000000000"), with_id(digest("mesh1.rpc.2", Pending), "400000000000")] {
            book.record(d);
        }
        let records = Records::default();
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.mesh_view("mesh1").unwrap().status, ScopeStatus::Pending, "never heard ready, and mesh1.rpc.2 is not ready");
        records.set_heard_ready(BTreeSet::from(["mesh1".to_string()]));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.mesh_view("mesh1").unwrap().status, ScopeStatus::ReadyForTraffic, "heard ready: a node not ready does not take the mesh back to pending");
    }

    /// CONTRACT: an admin that does not hold the fabric-primary role (it lost the seat to a reborn
    /// mesh's admin while the fabric was degraded) answers `GET /api/fabric` with the status the holder
    /// published, `degraded`, until the holder publishes ready. Must NOT happen: `ready-for-traffic`
    /// because its own authority records (cleared with the seat) are empty.
    #[test]
    fn a_non_holder_projects_the_degraded_status_the_holder_published() {
        use MemberStatus::*;
        let book = DigestBook::default();
        for d in [with_id(digest("mesh1.admin.1", ReadyForTraffic), "200000000000"), with_id(digest("mesh2.admin.1", ReadyForTraffic), "300000000000")] {
            book.record(d);
        }
        let records = Records::default();
        records.set_fabric_state(crate::fabric_state::FabricState::ReadyForTraffic);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.fabric.status, ScopeStatus::ReadyForTraffic, "nothing published degraded: ready");
        records.set_published_degraded(true);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.fabric.status, ScopeStatus::Degraded, "the holder published degraded and has not published ready");
    }

    /// CONTRACT: a birth the fabric authority proved exited is `Dead` in the view whatever its last
    /// digest said, so a mesh whose admins were learned from topology alone and are all proven gone
    /// has no primary admin. Must NOT happen: a dead admin holds its mesh's seat because its digest
    /// says ready.
    #[tokio::test]
    async fn a_proven_exited_birth_is_dead_whatever_its_digest_says() {
        use MemberStatus::*;
        let book = DigestBook::default();
        let a = with_id(digest("mesh1.admin.1", ReadyForTraffic), "200000000000");
        let b = with_id(digest("mesh2.admin.1", ReadyForTraffic), "300000000000");
        book.record(a.clone());
        book.record(b.clone());
        let records = Records::default();
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert!(t.cohort_primary("mesh2", NodeKind::NodeAdmin).is_some());
        let t = prove_exit(&book, &records, &b).await;
        assert_eq!(t.node(&"mesh2.admin.1".parse().unwrap()).unwrap().status, NodeStatus::Dead);
        assert!(t.cohort_primary("mesh2", NodeKind::NodeAdmin).is_none(), "a proven-gone admin holds no seat");
    }

    /// CONTRACT (node-recovery.md "The mesh-primary is the reconciler for its own members";
    /// mesh-investigation.md: a peer mesh unheard on the backbone is the fabric-primary's
    /// investigation, counted from t since it last heard that mesh): a mesh-primary's offline tickle
    /// watches the silent members of its own mesh and nothing of a peer mesh, its node-admins
    /// included. A peer mesh's node-admins are reached by the investigation's probes alone. Must NOT
    /// happen: a mesh-primary tickling, holding TRUE OFFLINE or writing a silence status for a peer
    /// mesh's node-admin on a per-member clock beside the investigation.
    #[test]
    fn a_mesh_primary_watches_only_its_own_meshs_silent_members() {
        use MemberStatus::*;
        let book = DigestBook::default();
        let me = with_id(digest("mesh1.admin.1", ReadyForTraffic), "200000000000");
        let own = with_id(digest("mesh1.rpc.1", ReadyForTraffic), "300000000000");
        let peer_admin = with_id(digest("mesh2.admin.1", ReadyForTraffic), "400000000000");
        let peer_rpc = with_id(digest("mesh2.rpc.1", ReadyForTraffic), "500000000000");
        for d in [&me, &own, &peer_admin, &peer_rpc] {
            book.record((*d).clone());
        }
        let mut t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        for n in t.nodes.iter_mut().filter(|n| n.node_id != me.node.node_id) {
            n.status = NodeStatus::PendingReconnect;
        }
        let watched: Vec<String> = watched_births(&t, &me.node.node_id, "mesh1", &[]).values().map(|n| n.name.to_string()).collect();
        assert_eq!(watched, vec!["mesh1.rpc.1".to_string()], "only its own mesh's silent member is watched");
    }

    /// CONTRACT (node-recovery.md, Detection: "The mesh-primary's tick (every gossip interval) reads
    /// the members its view marks `PendingReconnect` (unheard past the staleness floor)"; "Detection
    /// and proof" step 1: "Every node-admin keeps the silence bookkeeping (`first_seen`), so a
    /// node-admin that takes the seat mid-window continues the window already running"): an admin of
    /// mesh2 whose book holds mesh2.rpc.1 only from its mesh-primary's projection (the member's own
    /// digest never reached it) lists the birth in its view, never routable: with the projected
    /// status within one staleness floor of first holding it, and `PendingReconnect` past it. Its
    /// tick watches the birth, and the silence mark is the instant it first held it plus the floor,
    /// so the window it keeps began before any seat change. Must NOT happen: the view holding no birth
    /// of the member, so an heir's tick watches nothing and the member is never recovered.
    #[test]
    fn an_admin_lists_and_watches_an_own_mesh_birth_held_only_from_its_primarys_projection() {
        use MemberStatus::*;
        let book = DigestBook::default();
        let me = with_id(digest("mesh2.admin.2", ReadyForTraffic), "200000000000");
        assert!(book.record(me.clone()));
        let member = with_id(digest("mesh2.rpc.1", ReadyForTraffic), "300000000000");
        let t0 = std::time::Instant::now();
        book.hold_projected_at(&member, t0);
        let floor = book.staleness_floor();
        let records = Records::default();
        let path: PathName = "mesh2.rpc.1".parse().unwrap();
        let early = project_at("fabric1", &fabric1(), ProviderKind::Process, &book, &records, t0 + floor / 2);
        let n = early.node(&path).unwrap_or_else(|| panic!("the view holds the birth its primary's projection lists: {:?}", early.nodes));
        assert_eq!((n.node_id.clone(), n.status, n.routable), (member.node.node_id.clone(), NodeStatus::ReadyForTraffic, false), "within the floor: the projected status, never routable (no liveness heard)");
        let late_at = t0 + floor + Duration::from_millis(500);
        let late = project_at("fabric1", &fabric1(), ProviderKind::Process, &book, &records, late_at);
        assert_eq!(late.node(&path).map(|n| n.status), Some(NodeStatus::PendingReconnect), "unheard past the floor since it was first held");
        let watched = watched_births(&late, &me.node.node_id, "mesh2", &[]);
        assert_eq!(watched.keys().cloned().collect::<Vec<_>>(), vec![member.node.node_id.to_string()], "the tick watches it");
        let now_ms = 1_000_000u64;
        let silent = crate::offline::silent_set(watched.values(), &book, late_at, now_ms, floor);
        assert_eq!(silent.iter().map(|s| s.marked_at_ms).collect::<Vec<_>>(), vec![Some(now_ms - 500)], "the mark: first held plus the floor");
    }

    /// CONTRACT (node-rpc-envelope.md, "Node RPC readiness gate": "A node-admin waits until its Status
    /// authority is filled and its view holds it"): the admin's Node RPC door opens with the first view
    /// installed that holds its exact birth once the authority is filled, whichever task installs that
    /// view, so nothing that reads the installed view (the declarer that tells its primary it is ready
    /// among them) sees its own birth while its door still refuses. A view without the birth, one
    /// holding another birth of its name, and any view while the authority is not filled leave the
    /// door closed. Must NOT happen: the door opened only by the view-rebuild task, which starts after
    /// the declarer and a Build-log catch-up, so the admin declares itself ready to its primary while
    /// its router answers every round command NotReady (gate red, tree b24a4303,
    /// fabric_states__rounds trace 95adb727: mesh1.admin.2 declared ReadyForTraffic at 788604, its door
    /// opened at 788637, and mesh1.admin.1's three commit-state sends had all come back NotReady).
    #[tokio::test]
    async fn an_admins_door_opens_with_the_first_installed_view_that_holds_its_exact_birth() {
        let me = digest("mesh1.admin.2", MemberStatus::ReadyForTraffic);
        let primary = digest("mesh1.admin.1", MemberStatus::ReadyForTraffic);
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let filled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let records = Records::default();
        let authority = filled.clone();
        let topology = RwLock::new(view(&[primary.clone()]));
        records.set_door(ServingDoor::new(ready.clone(), (me.node.node_id.clone(), me.node.incarnation.clone()), Box::new(move || authority.load(std::sync::atomic::Ordering::SeqCst)), me.node.name.clone()), &*topology.read().await);
        let mut other = me.clone();
        other.node.incarnation = IncarnationId::mint();
        let opened = || ready.load(std::sync::atomic::Ordering::SeqCst);
        records.install_projected(&topology, || view(&[primary.clone(), me.clone()])).await;
        assert!(!opened(), "the authority is not filled: the door stays closed");
        filled.store(true, std::sync::atomic::Ordering::SeqCst);
        records.install_projected(&topology, || view(&[primary.clone()])).await;
        assert!(!opened(), "a view without this admin's birth leaves the door closed");
        records.install_projected(&topology, || view(&[primary.clone(), other.clone()])).await;
        assert!(!opened(), "another birth of its name is not this admin's birth");
        records.install_projected(&topology, || view(&[primary.clone(), me.clone()])).await;
        assert!(opened(), "the first installed view holding its exact birth, the authority filled, opens the door");
    }

    /// CONTRACT (R-EL1, "a seat moves only by explicit yield": the election ignores a holder's
    /// status; seat_watch, R-A2: only a runtime found exited is a verified loss; fabric-node-lifecycle.md
    /// §7.3: every admin keeps the silence window, only the seat holder tickles): a mesh primary that
    /// goes silent keeps its seat in its successor's view until its exact runtime is proven exited
    /// (its exit-proof row, what seat_watch puts, read by the next install). Until then the successor keeps the window but
    /// tickles nothing, holds nothing TRUE OFFLINE and has no recovery due, however long the silence
    /// lasts. Once the exit is proven the seat moves to the successor, which tickles on the window
    /// already running (round 1 at once) and, one staleness floor later, holds the old primary TRUE
    /// OFFLINE and due for its recovery. Must NOT happen: a recovery due at the successor while the
    /// silent primary still holds the seat.
    #[tokio::test(start_paused = true)]
    async fn a_successor_has_no_recovery_due_for_a_silent_mesh_primary_until_the_seat_moves() {
        use crate::offline::{OfflineTickle, SilentNode, ViaPeerVerdict};
        use rafka_mesh_entity::Seat::MeshPrimary;
        use MemberStatus::*;
        let book = DigestBook::default();
        let holder = with_id(digest("mesh2.admin.1", ReadyForTraffic), "200000000000");
        let successor = with_id(digest("mesh2.admin.2", ReadyForTraffic), "300000000000");
        let rpc = with_id(digest("mesh2.rpc.1", ReadyForTraffic), "400000000000");
        for d in [&holder, &successor, &rpc] {
            book.record((*d).clone());
        }
        book.seats.take(MeshPrimary, &holder_of(&holder, 1));
        let records = Records::default();
        let me: PathName = "mesh2.admin.2".parse().unwrap();
        // The successor's view: the holder silent past the floor (its pruner's PendingReconnect, or
        // Dead once proven exited), the seats by the one election over the holder record.
        let view_now = || {
            let mut t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
            for n in t.nodes.iter_mut().filter(|n| n.node_id == holder.node.node_id && n.status.is_live()) {
                n.status = NodeStatus::PendingReconnect;
            }
            crate::election::resolve_with(&mut t.nodes, &incumbency_of(&book, &records));
            t
        };
        let floor = book.staleness_floor().as_millis() as u64;
        let mut tickle = OfflineTickle::observed_by(me.to_string(), crate::park_gate::ParkGate::new());
        let mut offline: HashSet<(NodeId, IncarnationId)> = HashSet::new();
        let pass = async |now: u64, tickle: &mut OfflineTickle, offline: &mut HashSet<(NodeId, IncarnationId)>| {
            // The rounds the last pass started run beside the loop until this one (each probe here
            // answers at once).
            tokio::task::yield_now().await;
            let view = view_now();
            let holds_seat = view.holds_mesh_seat("mesh2", &successor.node.node_id);
            let watched = watched_births(&view, &successor.node.node_id, "mesh2", &[]);
            let silent = watched.values().map(|n| SilentNode { node_id: n.node_id.to_string(), path: n.name.to_string(), incarnation: n.incarnation_id.clone(), marked_at_ms: None }).collect();
            let in_view: HashSet<String> = view.members().map(|n| n.node_id.to_string()).collect();
            let report = tickle
                .tick(
                    now,
                    holds_seat,
                    silent,
                    |id: &str| in_view.contains(id),
                    floor / 2,
                    floor,
                    |id| async move { Err::<(), String>(format!("{id} did not answer the ping")) },
                    |_| async { ViaPeerVerdict::NoPath { asked: Vec::new() } },
                    |_| async { rafka_mesh_transport::iroh_obs::IrohObservation::unavailable() },
                )
                .await;
            for id in &report.offline {
                if let Some(n) = watched.get(id) {
                    offline.insert((n.node_id.clone(), n.incarnation_id.clone().unwrap()));
                }
            }
            let due: Vec<String> = recovery_due(holds_seat, false, "mesh2", &watched, offline).iter().map(|n| n.name.to_string()).collect();
            (holds_seat, report, due)
        };
        let t0 = 1_000_000u64;
        // The holder silent for six floors, its exit not proven: the seat stays, nothing is due.
        for k in 0..6 {
            let (holds_seat, report, due) = pass(t0 + k * floor, &mut tickle, &mut offline).await;
            assert!(!holds_seat, "floor {k}: the silent holder keeps the seat");
            assert!(report.tickled.is_empty() && report.offline.is_empty(), "floor {k}: no tickle, nothing TRUE OFFLINE: {report:?}");
            assert!(due.is_empty(), "floor {k}: no recovery due before the seat moves: {due:?}");
        }
        // seat_watch proves the exact runtime exited: the seat moves to the successor.
        prove_exit(&book, &records, &holder).await;
        let t1 = t0 + 6 * floor;
        let (holds_seat, report, due) = pass(t1, &mut tickle, &mut offline).await;
        assert!(holds_seat, "the proven exit moves the seat to the successor");
        assert_eq!(report.tickled, vec![holder.node.node_id.to_string()], "round 1 at once, on the window kept since the silence began");
        assert!(report.offline.is_empty() && due.is_empty(), "round 1 opens the hold-down only: {report:?} {due:?}");
        let (_, report, due) = pass(t1 + floor, &mut tickle, &mut offline).await;
        assert_eq!(report.tickled, vec![holder.node.node_id.to_string()], "round 2, one floor later");
        assert!(report.offline.is_empty() && due.is_empty(), "round 2 runs beside the loop: nothing is decided before a tick folds it: {report:?} {due:?}");
        let (_, report, due) = pass(t1 + floor + 500, &mut tickle, &mut offline).await;
        assert_eq!(report.offline, vec![holder.node.node_id.to_string()], "the next tick folds round 2: TRUE OFFLINE");
        assert_eq!(due, vec!["mesh2.admin.1".to_string()], "the old primary is due for recovery at the successor, after the seat moved");
    }

    /// CONTRACT (fabric-mesh-lifecycle.md §11.1: every admin that holds the Fabric shutdown "freezes
    /// Build reconciliation/drift recovery/rebirth"; "the surviving primary exists to retry/finish
    /// stops, observe exact disappearance, and report incomplete runtimes, not to recreate nodes"):
    /// a member the shutdown's drain stopped goes silent and the mesh-primary's tickle may hold it
    /// TRUE OFFLINE, but while the shutdown is held nothing is due for recovery, so no recovery Build
    /// is submitted for it. Must NOT happen: a stopped member inspected and submitted for recovery
    /// during the shutdown (the handover cell's tail submitted mesh2.rpc.1 and mesh2.rpc.2 every
    /// tick after the drain stopped them).
    #[test]
    fn no_member_is_due_for_recovery_while_a_fabric_shutdown_is_held() {
        let mut n = Node::allocated("mesh2.rpc.1".parse().unwrap());
        n.node_id = NodeId::mint();
        n.incarnation_id = Some(IncarnationId::mint());
        n.status = NodeStatus::Dead;
        let watched: BTreeMap<String, Node> = [(n.node_id.to_string(), n.clone())].into();
        let offline: HashSet<(NodeId, IncarnationId)> = [(n.node_id.clone(), n.incarnation_id.clone().unwrap())].into();
        let names = |due: Vec<Node>| due.iter().map(|n| n.name.to_string()).collect::<Vec<_>>();
        assert_eq!(names(recovery_due(true, false, "mesh2", &watched, &offline)), vec!["mesh2.rpc.1".to_string()], "TRUE OFFLINE at the seat holder: due");
        assert!(recovery_due(true, true, "mesh2", &watched, &offline).is_empty(), "a held fabric shutdown froze drift recovery: nothing is due");
    }

    fn holder_of(d: &MeshDigest, epoch: u64) -> rafka_mesh_entity::SeatHolder {
        rafka_mesh_entity::SeatHolder { mesh: d.node.name.mesh.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), epoch }
    }

    /// CONTRACT (R-A2, the dw33-1 shape): the seat records an admin holds decide its projection. A
    /// ready admin with a lower NodeId that appears, in any mesh, changes no seat; a view that has
    /// not been told who holds the seats (an entering admin) elects nobody; the holder proven
    /// exited, or replaced by a later birth that says so, hands the seat to its own mesh.
    #[tokio::test]
    async fn the_projection_keeps_a_recorded_holder_and_elects_nobody_before_it_is_told() {
        use MemberStatus::*;
        use rafka_mesh_entity::Seat::{FabricPrimary, MeshPrimary};
        let book = DigestBook::default();
        let holder = with_id(digest("mesh1.admin.1", ReadyForTraffic), "500000000000");
        let sibling = with_id(digest("mesh1.admin.2", ReadyForTraffic), "800000000000");
        for d in [&holder, &sibling] {
            book.record((*d).clone());
        }
        book.seats.take(MeshPrimary, &holder_of(&holder, 1));
        book.seats.take(FabricPrimary, &holder_of(&holder, 1));
        let records = Records::default();
        let seat_names = |t: &Topology| (t.fabric_primary().map(|n| n.name.to_string()), t.cohort_primary("mesh1", NodeKind::NodeAdmin).map(|n| n.name.to_string()));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(seat_names(&t), (Some("mesh1.admin.1".into()), Some("mesh1.admin.1".into())));

        // A lower NodeId appears in another mesh and one in the incumbent mesh, both ready: nothing moves.
        book.record(with_id(digest("mesh3.admin.1", ReadyForTraffic), "100000000000"));
        book.record(with_id(digest("mesh1.admin.3", ReadyForTraffic), "200000000000"));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(seat_names(&t), (Some("mesh1.admin.1".into()), Some("mesh1.admin.1".into())), "a lower NodeId never displaces a living holder");
        assert_eq!(t.cohort_primary("mesh3", NodeKind::NodeAdmin).map(|n| n.name.to_string()).as_deref(), Some("mesh3.admin.1"));

        // An entering admin has not been told who holds the seats: it computes none.
        records.set_entering(true);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert!(t.nodes.iter().all(|n| !n.is_primary && !n.is_fabric_primary), "no seat before the entry read");
        records.set_entering(false);

        // Proven exited: the lowest Ready NodeId of ITS mesh fills both seats, not mesh3's lower id.
        let t = prove_exit(&book, &records, &holder).await;
        assert_eq!(seat_names(&t), (Some("mesh1.admin.3".into()), Some("mesh1.admin.3".into())));
    }

    /// CONTRACT (R-A2): a later birth that names the recorded holder's incarnation as the one it
    /// replaced proves that birth gone: the seat goes to the lowest Ready admin of its mesh.
    #[test]
    fn a_later_birth_that_supersedes_the_holder_proves_it_gone() {
        use MemberStatus::*;
        use rafka_mesh_entity::Seat::{FabricPrimary, MeshPrimary};
        let book = DigestBook::default();
        let holder = with_id(digest("mesh1.admin.1", ReadyForTraffic), "500000000000");
        book.record(holder.clone());
        book.seats.take(MeshPrimary, &holder_of(&holder, 1));
        book.seats.take(FabricPrimary, &holder_of(&holder, 1));
        let mut reborn = with_id(digest("mesh1.admin.1", Pending), "900000000000");
        reborn.node.supersedes = Some(holder.node.incarnation.clone());
        book.record(reborn.clone());
        let ready = with_id(digest("mesh1.admin.2", ReadyForTraffic), "800000000000");
        book.record(ready);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        assert_eq!(t.fabric_primary().map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.2"));
    }

    /// The fabric-primary `mesh1.admin.1` (both seats of mesh1) and `mesh2`'s births `mesh2`, the
    /// first seated as mesh2's primary at epoch 1: the fabric-primary's book, its records holding
    /// its node-rows writer over `storage`, and its view projected from both.
    async fn fabric_primary_over(mesh2: &[MeshDigest], storage: Arc<crate::storage::MemoryNodesStorage>) -> (MeshDigest, DigestBook, Arc<Records>, Arc<RwLock<Topology>>, Arc<crate::node_rows::NodeRows>) {
        use rafka_mesh_entity::Seat::{FabricPrimary, MeshPrimary};
        let fp = with_id(digest("mesh1.admin.1", MemberStatus::ReadyForTraffic), "100000000000");
        let book = DigestBook::default();
        book.record(fp.clone());
        for d in mesh2 {
            book.record(d.clone());
        }
        book.seats.take(FabricPrimary, &holder_of(&fp, 1));
        book.seats.take(MeshPrimary, &holder_of(&fp, 1));
        book.seats.take(MeshPrimary, &holder_of(&mesh2[0], 1));
        let records = Arc::new(Records::default());
        records.set_book(book.clone());
        let view = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)));
        let rows = Arc::new(crate::node_rows::NodeRows::new(fp.node.name.clone(), fp.node.node_id.clone(), view.clone(), storage));
        records.set_rows(rows.clone());
        (fp, book, records, view, rows)
    }

    /// The provider's exit proof of `d`'s birth recorded as the product records it
    /// (`Records::put_exit`), in the status store `records` holds (one in memory, set on first use
    /// under a mesh no view names), then read by an install of the view, as every projection reads
    /// it; the installed view.
    async fn prove_exit(book: &DigestBook, records: &Records, d: &MeshDigest) -> Topology {
        if records.status.get().is_none() {
            records.set_status(Arc::new(crate::status_storage::MemoryStatusStorage::default()), "nomesh".into(), "nomesh.admin.1".parse().unwrap(), NodeId::mint());
        }
        records.set_book(book.clone());
        let mut n = Node::allocated(d.node.name.clone());
        n.node_id = d.node.node_id.clone();
        n.incarnation_id = Some(d.node.incarnation.clone());
        records.put_exit(&n, Some(0), "provider").await.unwrap();
        let view = RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, book, records));
        records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, book, records)).await
    }

    /// The exact Exited receipt of `d`'s birth, as a leave records it.
    fn exited_receipt(d: &MeshDigest) -> crate::deployment::pipeline::TerminalReceipt {
        crate::deployment::pipeline::TerminalReceipt { node: d.node.name.to_string(), node_id: d.node.node_id.to_string(), incarnation_id: d.node.incarnation.to_string(), runtime: None, exit_code: Some(0) }
    }

    /// CONTRACT (mesh-delete.md step 7: the final primary's own node.delete, the fabric-primary its
    /// executor, "as acting primary of a mesh that has no primary, writes its row deleted"; R-EL1: a
    /// seat moves only by explicit yield or proven loss): the final primary's exact Exited is that
    /// proven loss. Recorded as its exit-proof row and read by the install before the view the row is
    /// written from is projected, the exited holder is no longer seated, mesh2 has no primary in the
    /// fabric-primary's view, and the fabric-primary writes the final primary's row Deleted (the write
    /// node.delete's NodeDeleted step makes). Must NOT happen: the refusal of
    /// mesh_identity::canonical_ids on 8803f84e ("mesh1.admin.1 (40ynvqbyvdhh) is not the writer of
    /// mesh2's node rows ...: mesh2's primary node-admin mesh2.admin.1 (kz0m1x9y3wcn) writes them"):
    /// the exit proven, never read as a loss, so the election kept the exited holder.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn the_final_primarys_exit_held_as_a_proven_loss_leaves_the_fabric_primary_writing_its_row_deleted() {
        let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let last = with_id(digest("mesh2.admin.1", MemberStatus::Leaving), "200000000000");
        let (_fp, book, records, view, _rows) = fabric_primary_over(std::slice::from_ref(&last), storage.clone()).await;
        crate::storage::NodesStorage::put_node_row(&*storage, &crate::storage::NodeRow { name: last.node.name.clone(), node_id: last.node.node_id.clone(), status: NodeStatus::ReadyForTraffic, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None }).await.unwrap().unwrap();
        let capture = crate::span_capture::Capture::new("i143-3068-mesh-left-final-exit-held");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let final_primary = crate::mesh_leave::RosterBirth { node: last.node.name.to_string(), node_id: last.node.node_id.to_string(), incarnation_id: last.node.incarnation.to_string() };
        let write = || async {
            let nodes: Vec<Node> = view.read().await.nodes.iter().filter(|n| n.mesh == "mesh2").cloned().collect();
            crate::mesh_leave::birth_row_deleted(records.node_rows().as_deref(), "mesh1.admin.1", "retire-node:mesh2.admin.1", &nodes, &final_primary).await
        };
        // The exit proven and not held: the election keeps the exited holder seated (R-EL1).
        let refused = write().await;
        assert!(refused.as_ref().is_err_and(|e| e.contains("is not the writer of mesh2's node rows")), "unheld, the exited holder still writes mesh2's rows: {refused:?}");
        records.set_status(Arc::new(crate::status_storage::MemoryStatusStorage::default()), "mesh1".into(), "mesh1.admin.1".parse().unwrap(), NodeId::mint());
        records.put_exit_of_receipt(&exited_receipt(&last)).await.unwrap();
        records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
        assert_eq!(view.read().await.cohort_primary("mesh2", NodeKind::NodeAdmin).map(|n| n.name.to_string()), None, "held, the exited holder holds no seat: mesh2 has no primary");
        write().await.unwrap_or_else(|e| panic!("the fabric-primary writes the final primary's row Deleted: {e}"));
        let row = crate::storage::NodesStorage::node_row_at(&*storage, &last.node.name).await.unwrap().expect("the row stays at its path.name");
        assert_eq!((row.status, &row.node_id), (NodeStatus::Deleted, &last.node.node_id), "{row:?}");
        let outcomes: Vec<String> = capture.named("rdm.node_admin.node.delete.via-node-row").iter().map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        assert_eq!(outcomes, ["not-mesh-primary", "row-deleted"], "{outcomes:?}");
        let deleted = capture.named("rdm.node_admin.node.delete.via-node-row").pop().unwrap();
        eprintln!("span rdm.node_admin.node.delete.via-node-row trace_id={} {:?}", deleted.trace_id, deleted.attributes);
    }

    /// CONTRACT (fabric-node-lifecycle-elections.md §4.1: "Only the provider's exit proof vacates a
    /// seat"; node-recovery.md: "Exited is the proof, recorded durably"; CLAUDE.md read rule: a read
    /// is a point read through the store index, and node-local state that should be the durable
    /// answer is replaced by that read, never fed from it): the election's lost set is a read of the
    /// durable exit-proof rows. A fresh fabric-primary (its records hold nothing proven) whose store
    /// holds the exit-proof row of mesh2's seated final primary, among mesh2's status rows, resolves
    /// mesh2 with no primary at its first install: the exited birth is shown Dead, holds no seat, and
    /// the fabric-primary is the writer of mesh2's node rows (mesh-leave.md step 7: acting primary of
    /// a mesh with none left). Must NOT happen: the exited holder kept seated because no proof was
    /// fed into this admin's memory (the proof exists only as a durable row: written by another
    /// admin, or by this one before it restarted).
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_fresh_admin_whose_store_holds_the_final_primarys_exit_proof_resolves_its_mesh_with_no_primary() {
        use crate::status_storage::StatusStorage;
        let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let last = with_id(digest("mesh2.admin.1", MemberStatus::Leaving), "200000000000");
        let (fp, book, records, view, _rows) = fabric_primary_over(std::slice::from_ref(&last), storage).await;
        assert_eq!(view.read().await.cohort_primary("mesh2", NodeKind::NodeAdmin).map(|n| n.name.to_string()).as_deref(), Some("mesh2.admin.1"), "seated before any proof is read");
        // The proof as the store holds it: mesh2's status rows, written by whoever proved the exit.
        let status = Arc::new(crate::status_storage::MemoryStatusStorage::default());
        let proof = crate::status_storage::NodeExitRow { node_id: last.node.node_id.clone(), name: last.node.name.clone(), incarnation_id: last.node.incarnation.clone(), exit_code: Some(0), proof: "provider".into(), observed_at_ms: 1 };
        status.put_node_exit_of("mesh2", &proof).await.unwrap();
        records.set_status(status.clone(), "mesh1".into(), fp.node.name.clone(), fp.node.node_id.clone());
        let capture = crate::span_capture::Capture::new("i143-3068-fresh-admin-reads-exit-row");
        let installed = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await
        };
        let read = capture.named("rdm.node_admin.node.resolve.via-exit-proof-row");
        assert_eq!(read.len(), 1, "the install reads the final primary's exit-proof row once: {read:?}");
        let a = &read[0].attributes;
        assert_eq!((a.get("node").cloned(), a.get("node_id").cloned(), a.get("mesh").cloned()), (Some("mesh2.admin.1".to_string()), Some(last.node.node_id.to_string()), Some("mesh2".to_string())), "{a:?}");
        eprintln!("span rdm.node_admin.node.resolve.via-exit-proof-row trace_id={} {a:?}", read[0].trace_id);
        let held = installed.node(&last.node.name).map(|n| (n.status, n.is_primary));
        assert_eq!(held, Some((NodeStatus::Dead, false)), "the proven-exited holder is Dead and holds no seat");
        assert_eq!(installed.cohort_primary("mesh2", NodeKind::NodeAdmin).map(|n| n.name.to_string()), None, "mesh2 has no primary");
        assert!(crate::node_rows::writes_for(&installed, &fp.node.node_id, "mesh2"), "the fabric-primary writes mesh2's rows as acting primary");
    }

    /// CONTRACT (R-MD1 = A, i143-rulings-2026-10-11-dev.md: "The mesh-primary commits the mesh row
    /// Dead at mesh.deleted ... At mesh-left the fabric-primary writes the final primary's row
    /// Deleted, unchanged. Its ensure-Dead operation returns AlreadyApplied if the mesh-primary wrote
    /// Dead; only when no primary survives to make that write does the fabric-primary write Dead as
    /// acting primary ... Retries cause no second transition"; status_storage.rs: a mesh's status
    /// rows live under `status/<mesh>/`): with mesh2's row Dead already among mesh2's status rows
    /// (its primary's write), mesh-left's Dead write answers AlreadyApplied, writes nothing, and its
    /// `rdm.node_admin.mesh.update.via-dead-row` span says `already-applied` by `mesh-primary`; with
    /// none (no primary survived), the fabric-primary puts the row Dead among mesh2's status rows as
    /// acting primary (`applied`, `acting-primary`), and its replay answers AlreadyApplied. The Mesh
    /// row on mesh.storage follows the same rule (R-MN1 item 2, the name released): `already-dead`
    /// over the mesh-primary's, else `dead-written` once and `already-dead` on the replay. No Dead
    /// row of mesh2 is ever put among the fabric-primary's own mesh's rows. Must NOT happen: a second
    /// transition over the mesh-primary's Dead (the fabric-primary deciding from what it applied
    /// itself, which holds nothing of mesh2), or the row written under the fabric-primary's mesh.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn mesh_lefts_dead_write_is_already_applied_over_the_mesh_primarys_dead_and_the_acting_primarys_when_none_survived() {
        use crate::status_storage::{MeshStatusRow, StatusStorage};
        let dead = |m: &str| MeshStatusRow { mesh_id: mesh_id(m), state: rafka_node_rpc_contract::status::MeshState::Dead };
        let key = crate::mesh_leave::LeaveKey { mesh_id: mesh_id("mesh2"), build_id: crate::build::BuildId::parse("6n8kaqdncqwv").unwrap(), attempt: 1, operation: format!("shutdown-mesh:{}", mesh_id("mesh2")) };
        for case in ["surviving-primary-wrote-dead", "no-survivor"] {
            let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
            let last = with_id(digest("mesh2.admin.1", MemberStatus::Leaving), "200000000000");
            let (_fp, book, records, view, _rows) = fabric_primary_over(std::slice::from_ref(&last), storage.clone()).await;
            crate::storage::NodesStorage::put_node_row(&*storage, &crate::storage::NodeRow { name: last.node.name.clone(), node_id: last.node.node_id.clone(), status: NodeStatus::ReadyForTraffic, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None }).await.unwrap().unwrap();
            prove_exit(&book, &records, &last).await;
            records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
            let status = crate::status_storage::MemoryStatusStorage::default();
            let meshes = crate::storage::MemoryMeshStorage::default();
            if case == "surviving-primary-wrote-dead" {
                // mesh2's primary committed Dead at mesh.deleted, among mesh2's status rows (R-MD1 item 1).
                status.put_mesh_status_of("mesh2", &dead("mesh2")).await.unwrap();
                // ... and its Mesh row Dead on mesh.storage with it (admin/leave.rs, mesh.deleted).
                crate::storage::MeshStorage::claim_mesh(&meshes, &crate::storage::MeshRecord { mesh_id: mesh_id("mesh2"), name: "mesh2".into(), dead: true }).await.unwrap().unwrap();
            }
            let capture = crate::span_capture::Capture::new(&format!("i143-3068-r-md1-{case}"));
            {
                let _g = tracing::dispatcher::set_default(&capture.dispatch);
                for _replay in 0..2 {
                    crate::mesh_leave::mesh_left_rows(records.node_rows().as_deref(), &status, &meshes, &records.declared, "mesh1.admin.1", &key, "mesh2", &[exited_receipt(&last)]).await.unwrap_or_else(|e| panic!("{case}: mesh-left's rows: {e}"));
                }
            }
            assert!(status.mesh_status_held_of("mesh2", &dead("mesh2")).await.unwrap(), "{case}: mesh2's row Dead stands among mesh2's status rows");
            assert_eq!(status.mesh_statuses().await.unwrap().get(&mesh_id("mesh2")), None, "{case}: no Dead row of mesh2 among the fabric-primary's own mesh's rows");
            let spans = capture.named("rdm.node_admin.mesh.update.via-dead-row");
            let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("writer").cloned().unwrap_or_default(), s.attributes.get("mesh_row").cloned().unwrap_or_default())).collect();
            let expected: Vec<(String, String, String)> = match case {
                "surviving-primary-wrote-dead" => vec![("already-applied".into(), "mesh-primary".into(), "already-dead".into()); 2],
                _ => vec![("applied".into(), "acting-primary".into(), "dead-written".into()), ("already-applied".into(), "acting-primary".into(), "already-dead".into())],
            };
            assert_eq!(said, expected, "{case}: one transition at most, and the span names who wrote the row: {spans:?}");
            eprintln!("{case}: span rdm.node_admin.mesh.update.via-dead-row trace_id={} {:?}", spans[0].trace_id, spans[0].attributes);
        }
    }

    /// CONTRACT (R-MN1: a mesh removal deletes every roster birth, and a mesh created again under
    /// the name is a new mesh; mesh-leave.md step 7: at mesh-left the fabric-primary is the acting
    /// primary of a mesh that has none left, and departed births resolve Gone; R14 item 2: a mesh's
    /// contacts are its primary's rows; `dropped_contacts`: a departed birth's contact is dropped, so
    /// a maker's stored map never hands it to a mesh created again under the same name): mesh-left
    /// drops the contact of every birth its proof set names from the removed mesh's contacts, so
    /// the stored map the fabric-primary answers a recreated mesh's first admin with names none of
    /// them. Must NOT happen: mesh_identity::canonical_ids at :208 (trace
    /// a6f24e5914ac3658d302079ddb0c28f0): the recreated mesh2.admin.1's entry sweep pings the removed
    /// mesh's births (via-entry-sweep-no-reply), handed by the maker's stored map from the contacts
    /// nobody dropped (rdm.mesh.topology.serve.via-read stored 1, stored_nodes 5).
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn mesh_left_drops_the_removed_meshs_contacts_of_every_birth_its_proof_set_names() {
        use crate::storage::NodesStorage;
        let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let last = with_id(digest("mesh2.admin.1", MemberStatus::Leaving), "200000000000");
        let member = with_id(digest("mesh2.rpc.1", MemberStatus::Leaving), "210000000000");
        let (fp, book, records, view, _rows) = fabric_primary_over(std::slice::from_ref(&last), storage.clone()).await;
        storage.put_node_row(&crate::storage::NodeRow { name: last.node.name.clone(), node_id: last.node.node_id.clone(), status: NodeStatus::ReadyForTraffic, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None }).await.unwrap().unwrap();
        let contact = |d: &MeshDigest| crate::storage::NodeRecord { node_id: d.node.node_id.clone(), fabric_id: fabric1(), name: d.node.name.clone(), mesh_id: mesh_id(&d.node.name.mesh), incarnation_id: d.node.incarnation.clone(), endpoint_id: d.node.endpoint_id.clone(), transport_addr: d.node.transport_addr, listeners: Vec::new() };
        // mesh2's primary wrote its members' contacts while it held the seat; mesh1's own stays.
        for d in [&last, &member, &fp] {
            storage.put_contact(&contact(d)).await.unwrap();
        }
        prove_exit(&book, &records, &last).await;
        records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
        let key = crate::mesh_leave::LeaveKey { mesh_id: mesh_id("mesh2"), build_id: crate::build::BuildId::parse("6n8kaqdncqwv").unwrap(), attempt: 1, operation: format!("shutdown-mesh:{}", mesh_id("mesh2")) };
        let status = crate::status_storage::MemoryStatusStorage::default();
        let capture = crate::span_capture::Capture::new("i143-3068-mesh-left-contacts");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            crate::mesh_leave::mesh_left_rows(records.node_rows().as_deref(), &status, &crate::storage::MemoryMeshStorage::default(), &records.declared, "mesh1.admin.1", &key, "mesh2", &[exited_receipt(&last), exited_receipt(&member)]).await.unwrap_or_else(|e| panic!("mesh-left's rows: {e}"));
        }
        let held: Vec<String> = storage.contacts().await.unwrap().into_iter().map(|c| c.name.to_string()).collect();
        assert_eq!(held, vec!["mesh1.admin.1".to_string()], "no contact of the removed mesh's births stands; another mesh's is untouched");
        let spans = capture.named("rdm.node_admin.node.remove.via-mesh-left-contact");
        assert_eq!(spans.len(), 1, "{spans:?}");
        let dropped = spans[0].attributes.get("dropped").cloned().unwrap_or_default();
        assert!(dropped.contains("mesh2.admin.1") && dropped.contains("mesh2.rpc.1"), "the span names each birth whose contact is dropped: {dropped}");
        eprintln!("span rdm.node_admin.node.remove.via-mesh-left-contact trace_id={} {:?}", spans[0].trace_id, spans[0].attributes);
    }

    /// CONTRACT (mesh-leave.md, "Resume after the primary's loss", steps 1-2: the lost final
    /// primary's exact Exited is proven and recorded first, then the fabric-primary creates the one
    /// node-admin of a mesh with no live admin; R11: record predecessor Exited first): the proven
    /// exit, recorded as its exit-proof row and read by the install `admin_for_leaving_mesh` runs
    /// before the create, vacates the lost holder's seat, so the fabric-primary is the writer of the mesh's
    /// rows and its claim of the created admin's name is written. Must NOT happen: the refusal of
    /// mesh_lifecycle::mesh_leave_no_survivor on 8803f84e ("AllocateIdentity failed: the name claim
    /// refused the birth ...: mesh1.admin.1 is not the writer of mesh2's node rows").
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn the_lost_only_admins_exit_held_admits_the_fabric_primarys_claim_for_the_admin_it_creates() {
        let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let lost = with_id(digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), "200000000000");
        let (_fp, book, records, view, rows) = fabric_primary_over(std::slice::from_ref(&lost), storage.clone()).await;
        let capture = crate::span_capture::Capture::new("i143-3068-leaving-mesh-admin-claim");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let created: PathName = "mesh2.admin.2".parse().unwrap();
        let refused = rows.claim(&created, &NodeId::mint(), "create-node:mesh2.admin.2", None, crate::node_rows::ClaimFor::Create).await;
        assert!(matches!(&refused, Err(crate::node_rows::RowRefused::NotMeshPrimary(_))), "unheld, the lost holder still writes mesh2's rows: {refused:?}");
        prove_exit(&book, &records, &lost).await;
        records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
        let born = NodeId::mint();
        match rows.claim(&created, &born, "create-node:mesh2.admin.2", None, crate::node_rows::ClaimFor::Create).await {
            Ok(crate::node_rows::Claimed::Written { row, .. }) => assert_eq!((row.node_id, row.status), (born, NodeStatus::Pending)),
            other => panic!("the fabric-primary claims the created admin's name once the lost holder's exit is held: {other:?}"),
        }
        let mut outcomes: Vec<String> = capture.named("rdm.node_admin.node.create.via-node-row").iter().filter(|s| s.attributes.get("moment").map(String::as_str) == Some("claim")).map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        outcomes.dedup();
        assert_eq!(outcomes, ["not-mesh-primary", "row-written"], "{outcomes:?}");
        let written = capture.named("rdm.node_admin.node.create.via-node-row").pop().unwrap();
        eprintln!("span rdm.node_admin.node.create.via-node-row trace_id={} {:?}", written.trace_id, written.attributes);
    }

    /// CONTRACT (R-EL1: a seat moves only by explicit yield or proven loss; seat_watch.rs: "Only a
    /// runtime found exited is a verified loss"; mesh-leave.md, "Resume after the primary's loss":
    /// the workflow goes to the admin seated): mesh2's primary `mesh2.admin.1` is lost and
    /// `mesh2.admin.2` is live. Once the lost holder's exact Exited is proven and its exit-proof row
    /// put, the seat is `mesh2.admin.2`'s in the fabric-primary's view (the leave a later attempt
    /// resumes hands it the workflow) and in its own, which reads the same row and announces the
    /// seat at the next epoch. Must NOT happen: the no_survivor attempts refused "mesh2.admin.2 is live and
    /// holds no seat yet" after mesh2.admin.1's exit was proven (trace 5bc960a3e759a77cfb42ae6e9bc687d0).
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn the_lost_primarys_exit_held_seats_the_live_admin_in_the_fabric_primarys_view_and_its_own() {
        use rafka_mesh_entity::Seat::MeshPrimary;
        let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let lost = with_id(digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), "200000000000");
        let live = with_id(digest("mesh2.admin.2", MemberStatus::ReadyForTraffic), "300000000000");
        let (_fp, book, records, view, _rows) = fabric_primary_over(&[lost.clone(), live.clone()], storage).await;
        let primary = |t: &Topology| t.cohort_primary("mesh2", NodeKind::NodeAdmin).map(|n| n.name.to_string());
        assert_eq!(primary(&*view.read().await).as_deref(), Some("mesh2.admin.1"), "unheld, the lost holder keeps its seat (R-EL1)");
        prove_exit(&book, &records, &lost).await;
        records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
        assert_eq!(primary(&*view.read().await).as_deref(), Some("mesh2.admin.2"), "held, the fabric-primary reads the live admin seated");
        // mesh2.admin.2's own view: its investigation's exit proof, read from the same row.
        let own = Records::default();
        own.set_status(records.status.get().unwrap().storage().clone(), "mesh2".into(), live.node.name.clone(), live.node.node_id.clone());
        own.set_book(book.clone());
        let view2 = RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &book, &own));
        own.install_projected(&view2, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &own)).await;
        let decided = incumbency_of(&book, &own);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &book, &own, &decided, &ViewReads::none_at(std::time::Instant::now()));
        assert_eq!(primary(&t).as_deref(), Some("mesh2.admin.2"), "mesh2.admin.2 reads itself seated");
        let said = seat_announcements(&t, true, "mesh2", (&live.node.node_id, &live.node.incarnation), &decided);
        assert!(said.iter().any(|(seat, h)| *seat == MeshPrimary && h.mesh == "mesh2" && h.is_birth(&live.node.node_id, &live.node.incarnation) && h.epoch == 2), "mesh2.admin.2 announces mesh2's seat at the next epoch: {said:?}");
    }

    /// CONTRACT (R-EL1, fabric-node-lifecycle-elections.md §5 "a living holder keeps its seat";
    /// proposal node-primary-update Part C3 edit 6: the committed seat row is the authority, "primary
    /// updated" only announces it): the view's seat readers name the holder the
    /// seat records give, whatever its status. A holder that is Leaving (drained and stopped by its
    /// Build) whose yield this observer has not heard yet keeps both seats; once the yield is held
    /// (PrimaryUpdated: both records at the next epoch naming the successor), the readers name the
    /// successor. Must NOT happen: an observer reading a Leaving holder's seats as vacant (the
    /// sticky_seat hand-over cell's via-fabric-recompute with winner "" while the record names P).
    #[test]
    fn a_leaving_holder_keeps_both_seats_in_the_readers_until_its_yield_is_held() {
        use MemberStatus::*;
        use rafka_mesh_entity::Seat::{FabricPrimary, MeshPrimary};
        let book = DigestBook::default();
        let records = Records::default();
        let holder = with_id(digest("mesh1.admin.1", Leaving), "500000000000");
        let successor = with_id(digest("mesh1.admin.2", ReadyForTraffic), "800000000000");
        let other = with_id(digest("mesh2.admin.1", ReadyForTraffic), "300000000000");
        for d in [&holder, &successor, &other] {
            book.record((*d).clone());
        }
        book.seats.take(MeshPrimary, &holder_of(&holder, 1));
        book.seats.take(FabricPrimary, &holder_of(&holder, 1));
        book.seats.take(MeshPrimary, &holder_of(&other, 1));
        let seat_names = |t: &Topology| (t.fabric_primary().map(|n| n.name.to_string()), t.cohort_primary("mesh1", NodeKind::NodeAdmin).map(|n| n.name.to_string()));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        let flags = t.nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.1").map(|n| (n.status, n.is_primary, n.is_fabric_primary));
        assert_eq!(flags, Some((NodeStatus::Leaving, true, true)), "the election keeps the Leaving holder seated (R-EL1)");
        assert_eq!(seat_names(&t), (Some("mesh1.admin.1".into()), Some("mesh1.admin.1".into())), "the readers name the Leaving holder until its yield is held");
        assert!(t.holds_fabric_seat(&holder.node.node_id) && t.holds_mesh_seat("mesh1", &holder.node.node_id));
        assert_eq!(t.mesh_view("mesh1").and_then(|m| m.primary_admin).map(|p| p.to_string()).as_deref(), Some("mesh1.admin.1"));

        // The yield is held: PrimaryUpdated's records name the successor at the next epochs.
        book.seats.take(MeshPrimary, &holder_of(&successor, 2));
        book.seats.take(FabricPrimary, &holder_of(&successor, 2));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(seat_names(&t), (Some("mesh1.admin.2".into()), Some("mesh1.admin.2".into())), "the readers name the successor once the yield is held");
        assert!(!t.holds_fabric_seat(&holder.node.node_id) && !t.holds_mesh_seat("mesh1", &holder.node.node_id));
    }

    /// CONTRACT (fabric-node-lifecycle-elections.md §4.4, fabric-primary-handover.md step 3): the
    /// committed next-epoch seat record is the resolver's input and the incumbent yields fabric
    /// authority. A hierarchy-loop decision is announced against the seat record it was decided
    /// from: a committed handover that lands while the loop awaits (its round climb) is not
    /// answered by the incumbent taking the seat back at the next epoch. Must NOT happen: the
    /// yielded incumbent announcing itself over the committed successor (the e3..e12 seat flap of
    /// the fabric-primary handover stem on f06c8197).
    #[tokio::test]
    async fn a_handover_committed_while_the_loop_awaits_is_not_taken_back_by_the_incumbent() {
        use MemberStatus::*;
        use rafka_mesh_entity::Seat::{FabricPrimary, MeshPrimary};
        let book = DigestBook::default();
        let records = Records::default();
        let me = with_id(digest("mesh1.admin.1", ReadyForTraffic), "500000000000");
        let sibling = with_id(digest("mesh1.admin.2", ReadyForTraffic), "800000000000");
        let successor = with_id(digest("mesh2.admin.1", ReadyForTraffic), "300000000000");
        for d in [&me, &sibling, &successor] {
            book.record((*d).clone());
        }
        book.seats.take(MeshPrimary, &holder_of(&me, 1));
        book.seats.take(MeshPrimary, &holder_of(&successor, 1));
        book.seats.take(FabricPrimary, &holder_of(&me, 1));
        let me_birth = (&me.node.node_id, &me.node.incarnation);
        // The loop decides: this admin holds both of its seats, and their records already name it.
        let decided = incumbency_of(&book, &records);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &book, &records, &decided, &ViewReads::none_at(std::time::Instant::now()));
        assert_eq!(t.fabric_primary().map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.1"));
        // While the loop awaits its climb, the committed transfer lands: mesh2.admin.1 at epoch 2.
        book.seats.take(FabricPrimary, &holder_of(&successor, 2));
        let said = seat_announcements(&t, true, "mesh1", me_birth, &decided);
        assert!(said.is_empty(), "the yielded incumbent announces nothing over the committed successor: {said:?} (decided against {:?})", decided.fabric);

        // A legitimate succession is still announced: the recorded holder's birth proven exited,
        // the election seats its own mesh's next admin, at the next epoch over that record.
        let book = DigestBook::default();
        let records = Records::default();
        let gone = with_id(digest("mesh1.admin.2", ReadyForTraffic), "100000000000");
        for d in [&me, &gone] {
            book.record((*d).clone());
        }
        book.seats.take(MeshPrimary, &holder_of(&gone, 5));
        book.seats.take(FabricPrimary, &holder_of(&gone, 5));
        prove_exit(&book, &records, &gone).await;
        let decided = incumbency_of(&book, &records);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &book, &records, &decided, &ViewReads::none_at(std::time::Instant::now()));
        let said = seat_announcements(&t, true, "mesh1", me_birth, &decided);
        let epochs: Vec<_> = said.iter().map(|(s, h)| (*s, h.node_id.clone(), h.epoch)).collect();
        assert_eq!(epochs, vec![(MeshPrimary, me.node.node_id.clone(), 6), (FabricPrimary, me.node.node_id.clone(), 6)], "the successor of a proven-gone holder takes both seats at the next epoch (decided against {:?})", decided.fabric);
    }

    #[test]
    fn the_projection_elects_the_lowest_ready_node_id_per_cohort_and_across_mesh_primaries() {
        use MemberStatus::*;
        let t = view(&[
            with_id(digest("mesh2.admin.1", ReadyForTraffic), "200000000000"),
            with_id(digest("mesh2.rpc.2", ReadyForTraffic), "700000000000"),
            with_id(digest("mesh2.rpc.1", Pending), "000000000001"),
            with_id(digest("mesh1.admin.2", ReadyForTraffic), "300000000000"),
            with_id(digest("mesh1.admin.1", ReadyForTraffic), "400000000000"),
            with_id(digest("mesh1.rpc.3", ReadyForTraffic), "500000000000"),
        ]);
        let mut primaries: Vec<String> = t.nodes.iter().filter(|n| n.is_primary).map(|n| n.name.to_string()).collect();
        primaries.sort();
        assert_eq!(primaries, ["mesh1.admin.2", "mesh1.rpc.3", "mesh2.admin.1", "mesh2.rpc.2"], "a pending member is never primary");
        assert_eq!(t.fabric_primary().map(|n| n.name.to_string()).as_deref(), Some("mesh2.admin.1"), "not the lowest-named mesh");
        assert!(t.violations().is_empty(), "{:?}", t.violations());
        assert_eq!(t.mesh_view("mesh2").unwrap().id, Some(mesh_id("mesh2")));
        assert_eq!(t.fabric.status, ScopeStatus::Pending, "a fabric-primary existing does not make the fabric ready: the view carries the held fabric state");
    }

    /// CONTRACT (fabric-mesh-lifecycle.md §11.1, FML-26): a fabric shutdown runs no election
    /// because admins go Draining. The seats held when the shutdown was learned stay with their
    /// holders through the drain and go only with them. Must NOT happen: a Draining primary's seat
    /// recomputed away, or re-elected into, while the shutdown is held.
    #[test]
    fn a_fabric_shutdown_keeps_every_seat_with_its_holder_and_re_elects_nothing() {
        use MemberStatus::*;
        let records = Records::default();
        let book = DigestBook::default();
        let admins = [
            with_id(digest("mesh1.admin.1", ReadyForTraffic), "900000000000"),
            with_id(digest("mesh1.admin.2", ReadyForTraffic), "100000000000"),
            with_id(digest("mesh2.admin.1", ReadyForTraffic), "500000000000"),
            with_id(digest("mesh1.rpc.1", ReadyForTraffic), "700000000000"),
        ];
        for d in &admins {
            book.record(d.clone());
        }
        let calm = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        let seat = |t: &Topology, name: &str| t.nodes.iter().find(|n| n.name.to_string() == name).map(|n| (n.is_primary, n.is_fabric_primary)).unwrap();
        assert_eq!(seat(&calm, "mesh1.admin.2"), (true, true), "lowest admin id holds mesh1 and the fabric");
        assert_eq!(seat(&calm, "mesh2.admin.1"), (true, false));

        // Outside a shutdown a Draining primary loses its seat: the ordinary rule.
        let mut draining = admins[1].clone();
        draining.status = Draining;
        draining.digest_seq += 1;
        book.record(draining.clone());
        let moved = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(seat(&moved, "mesh1.admin.2"), (false, false));
        assert_eq!(seat(&moved, "mesh1.admin.1"), (true, false), "re-elected outside a shutdown");

        // The shutdown is learned: the seats of the last all-ready view are held, whatever the
        // statuses say now.
        records.hold_seats();
        let held = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(seat(&held, "mesh1.admin.2"), (true, true), "the Draining primary keeps mesh1 and the fabric");
        assert_eq!(seat(&held, "mesh1.admin.1"), (false, false), "nothing is re-elected into a held seat");
        assert_eq!(seat(&held, "mesh2.admin.1"), (true, false));
        for d in admins.iter().skip(2) {
            let mut d = d.clone();
            d.status = Draining;
            d.digest_seq += 1;
            book.record(d);
        }
        let all_draining = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(seat(&all_draining, "mesh2.admin.1"), (true, false), "every admin Draining moves no seat");
        assert_eq!(all_draining.fabric_primary().map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.2"));

        // A holder that disappears takes its seat with it: nothing fills it.
        let book2 = DigestBook::default();
        for d in [&admins[0], &admins[2], &admins[3]] {
            book2.record(d.clone());
        }
        let after = project("fabric1", &fabric1(), ProviderKind::Process, &book2, &records);
        assert!(after.nodes.iter().all(|n| n.name.to_string() != "mesh1.admin.2"), "the holder has left the view");
        assert!(after.fabric_primary().is_none(), "the fabric seat went with its holder");
        assert!(after.cohort_primary("mesh1", NodeKind::NodeAdmin).is_none(), "mesh1's seat went with its holder");
        assert_eq!(seat(&after, "mesh2.admin.1"), (true, false), "the other mesh's seat is untouched");
    }

    #[test]
    fn ready_since_and_ordinal_never_change_a_winner() {
        let at = |name: &str, id: &str, since: u64| {
            let mut d = with_id(digest(name, MemberStatus::ReadyForTraffic), id);
            // A digest still carrying the retired readiness claim: a fact nothing reads.
            d.config.insert("ready_since_ms".into(), since.to_string());
            d
        };
        // rpc.3 has the highest ordinal and the latest ready claim, and the lowest id.
        let a = view(&[at("mesh1.admin.1", "100000000000", 10), at("mesh1.rpc.1", "900000000000", 1), at("mesh1.rpc.2", "800000000000", 2), at("mesh1.rpc.3", "200000000000", 900)]);
        let b = view(&[at("mesh1.admin.1", "100000000000", 999), at("mesh1.rpc.1", "900000000000", 900), at("mesh1.rpc.2", "800000000000", 800), at("mesh1.rpc.3", "200000000000", 1)]);
        for t in [a, b] {
            let mut primaries: Vec<String> = t.nodes.iter().filter(|n| n.is_primary).map(|n| n.name.to_string()).collect();
            primaries.sort();
            assert_eq!(primaries, ["mesh1.admin.1", "mesh1.rpc.3"]);
        }
    }

    #[test]
    fn an_admin_is_authority_capable_only_when_every_held_birth_is_adoptable_here() {
        use crate::deployment::process::ProcessDeploymentProvider;
        use crate::deployment::provider::DeploymentProvider;
        use rafka_mesh_entity::{RuntimeLocator, RuntimeProvider};
        let p = ProcessDeploymentProvider::new();
        let with = |name: &str, domain: Option<String>| {
            let mut d = digest(name, MemberStatus::ReadyForTraffic);
            d.node.runtime = domain.map(|control_domain| rafka_mesh_entity::RuntimeFact {
                deployment_id: rafka_mesh_entity::fixture_crockford60("dep"),
                provider: RuntimeProvider::Process,
                control_domain,
                locator: RuntimeLocator::Process { pid: 4242, start: 17 },
            });
            d
        };
        let here = p.control_domain();
        assert!(authority_blockers(&[with("mesh1.rpc.1", Some(here.clone())), with("mesh1.admin.1", Some(here.clone()))], &p).is_empty());
        let blocked = authority_blockers(
            &[with("mesh1.rpc.1", Some(here.clone())), with("mesh1.rpc.2", None), with("mesh1.rpc.3", Some("process:another-host:1".into()))],
            &p,
        );
        assert_eq!(blocked.len(), 2, "{blocked:?}");
        assert!(blocked[0].starts_with("mesh1.rpc.2: publishes no runtime fact"), "{blocked:?}");
        assert!(blocked[1].starts_with("mesh1.rpc.3:") && blocked[1].contains("control domain"), "{blocked:?}");
    }

    /// CONTRACT (i143 export gate: normal joining or restarted admins cannot self-Ready; only the
    /// Day-0 root may apply its own mesh's Pending): a mesh's first admin that is not Day 0 is held
    /// by name until the fabric primary applies Pending; an admin joining a live cohort owes
    /// nothing; Day 0 alone self-applies; an applied Pending holds no one.
    #[test]
    fn only_the_day0_root_applies_its_own_meshs_pending() {
        let me: PathName = "mesh2.admin.1".parse().unwrap();
        assert_eq!(pending_gate(&me, "mesh2", true, false, false), PendingGate::SelfApply, "Day 0 has no upstream authority");
        match pending_gate(&me, "mesh2", false, false, false) {
            PendingGate::Blocked(why) => assert!(why.contains("not been applied here by the fabric primary"), "{why}"),
            other => panic!("a first admin that is not Day 0 never self-applies: {other:?}"),
        }
        assert_eq!(pending_gate(&me, "mesh2", false, false, true), PendingGate::Clear, "joining a live cohort owes no Pending");
        assert_eq!(pending_gate(&me, "mesh2", false, true, false), PendingGate::Clear);
        assert_eq!(pending_gate(&me, "mesh2", true, true, false), PendingGate::Clear, "Day 0 applies it once");
    }

    /// CONTRACT (R-BI3: the current Build is read from the Build entities; node-start.md step 4, the
    /// Build floor): an admin that holds no current Build, or not the facts of the Build its entry
    /// named at the attempt the answering admin held, is blocked from Ready, each reason named with
    /// the request that recovers it. Day 0 (no entry) owes no floor.
    #[tokio::test]
    async fn an_admin_without_a_current_build_is_never_authority_capable() {
        use crate::build_state::{AttemptOpened, AttemptReason, BuildAccepted, BuildAttemptClaim, BuildStateAdapter as _, MemoryBuildStateAdapter};
        let me: PathName = "mesh1.admin.2".parse().unwrap();
        let builds = MemoryBuildStateAdapter::new();
        let blocked = hydration_blocker(&me, &builds, None).await.expect("blocked while unhydrated");
        assert_eq!(blocked.kind(), "no-current-build");
        assert!(blocked.to_string().starts_with("mesh1.admin.2: holds no current Build"), "{blocked}");
        let id = crate::build::BuildId::parse("7wy02yhavgde").unwrap();
        let blocked = hydration_blocker(&me, &builds, Some((id.clone(), 2))).await.expect("blocked while the entry's Build is not held");
        assert_eq!(blocked.need(), crate::hydrate::Need::BuildFacts(id.clone()), "{blocked}");
        assert!(blocked.to_string().contains(&format!("holds no facts of Build {id}")), "{blocked}");
        builds.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
        assert_eq!(hydration_blocker(&me, &builds, None).await, None, "an admin served no entry owes no attempt floor");
        let blocked = hydration_blocker(&me, &builds, Some((id.clone(), 2))).await.expect("blocked below the entry's attempt").to_string();
        assert!(blocked.contains(&format!("holds attempt 0 of Build {id}")) && blocked.contains("held attempt 2"), "{blocked}");
        for attempt in 1..=2 {
            if attempt > 1 {
                builds.open_attempt(&AttemptOpened { mesh: None, build_id: id.clone(), attempt, reason: AttemptReason::Restart, action: None, opened_by: "x".into(), opened_at_ms: 0 }).await.unwrap();
            }
            builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
        }
        assert_eq!(hydration_blocker(&me, &builds, Some((id.clone(), 2))).await, None, "its attempt facts reached the entry's");
        let other = crate::build::BuildId::parse("qsmy968wj1zt").unwrap();
        let blocked = hydration_blocker(&me, &builds, Some((other.clone(), 9))).await.expect("a floor naming a Build not held blocks until its facts are held");
        assert_eq!(blocked.need(), crate::hydrate::Need::BuildFacts(other), "{blocked}");
    }

    /// CONTRACT (R-S2): the declarer is woken by a view that changed and by the declare gate
    /// opening or closing; writing the same view, or setting the gate to the value it holds,
    /// wakes nothing (the 200 ms projection write is not a timer in disguise).
    #[tokio::test]
    async fn a_changed_view_or_gate_wakes_the_declarer_and_a_repeat_does_not() {
        let records = Records::default();
        let quiet = |records: &Records| {
            let wake = records.wake.clone();
            async move { tokio::time::timeout(Duration::from_millis(150), wake.woken()).await.is_err() }
        };
        let topology = RwLock::new(view(&[digest("mesh1.admin.1", MemberStatus::ReadyForTraffic)]));
        let same = topology.read().await.clone();
        records.install_projected(&topology, || same.clone()).await;
        assert!(quiet(&records).await, "the same view wakes nothing");
        let changed = view(&[digest("mesh1.admin.1", MemberStatus::ReadyForTraffic), digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic)]);
        records.install_projected(&topology, || changed.clone()).await;
        assert!(!quiet(&records).await, "a changed view wakes the declarer");
        records.set_declare_gate(true);
        assert!(!quiet(&records).await, "the gate opening wakes it");
        records.set_declare_gate(true);
        assert!(quiet(&records).await, "the gate set to the value it holds wakes nothing");
        records.set_declare_gate(false);
        assert!(!quiet(&records).await, "the gate closing wakes it");
    }

    /// CONTRACT (R-J1 step 5): a node-admin the fabric-primary admitted over `JoinNode` is held by
    /// the fabric-primary's status authority when the join is answered, before any gossip carries its
    /// digest. Before the join its declaration names a subject the authority does not hold
    /// (`SubjectUnknown`); after it, its first declaration (`ReadyForTraffic`) is `Applied`. Nothing
    /// checks who is calling (node-rpc.md §14): the same declaration from an endpoint that never
    /// joined is decided for the subject it names.
    #[tokio::test]
    async fn an_admitted_node_admin_is_a_held_subject_before_its_digest_is_gossiped() {
        use crate::join::{Deployed, JoinDoor, Joins};
        use crate::status_rpc::{Declared, StatusAuthority};
        use rafka_node_rpc_contract::status::{NodeState, NotAuthority, StatusReply, StatusRequest};
        use rafka_node_rpc_contract::join::{JoinReply, JoinRequest};
        let book = DigestBook::default();
        let records = Arc::new(Records::default());
        book.record(with_id(digest("mesh1.admin.1", MemberStatus::ReadyForTraffic), "100000000000"));
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)));
        let me = topology.read().await.fabric_primary().expect("admin.1 holds the fabric seat").clone();
        let authority = StatusAuthority {
            me: me.name.clone(),
            node_id: me.node_id.clone(),
            fabric_id: fabric1(),
            topology: topology.clone(),
            declared: records.declared.clone(),
            status_storage: Arc::new(crate::status_storage::MemoryStatusStorage::default()),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            mesh_ids: Arc::new(BTreeMap::new),
            republish: Arc::new(std::sync::OnceLock::new()),
            commands: Arc::new(crate::node_commands::CommandBook::default()),
            own: Arc::new(std::sync::OnceLock::new()),
            leaver: Arc::new(std::sync::OnceLock::new()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: records.wake.clone(),
            rounds: Default::default(),
            configs: Default::default(),
            configure: Default::default(),
            hydration: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: None,
            book: None,
            first_ready: Default::default(),
            builds: std::sync::Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        };
        let _ = Declared::default();

        // The birth this admin deployed, and the digest it reports on its first call.
        let runtime = rafka_mesh_entity::RuntimeFact::of_this_process(&rafka_mesh_entity::fixture_crockford60("dep-admitted")).unwrap();
        let mut reported = with_id(digest("mesh1.admin.2", MemberStatus::Pending), "200000000000");
        reported.node.runtime = Some(runtime.clone());
        reported.data_dir = Some("/data/mesh1.admin.2".into());
        let deployed = Deployed {
            name: reported.node.name.clone(),
            node_id: reported.node.node_id.clone(),
            incarnation: reported.node.incarnation.clone(),
            supersedes: None,
            endpoint_id: reported.node.endpoint_id.clone(),
            runtime,
            data_dir: "/data/mesh1.admin.2".into(),
            last_accepted_name: None,
            held: false,
        };
        let joins = Arc::new(Joins::default());
        let _wait = joins.expect(deployed);
        let installed = book.clone();
        let door = JoinDoor {
            me: me.name.clone(),
            joins,
            answer: Arc::new(|| {
                Box::pin(async {
                    Ok(crate::wire::JoinAnswer {
                        served_by: "mesh1.admin.1".into(),
                        control: crate::wire::JoinControl { provider: ProviderKind::Process, fabric: None, shutdown: None, build: None, rafka_time_ms: 1_000, member_cert: Vec::new(), name: None, config: None },
                        statuses: vec![],
                    })
                })
            }),
            install: Arc::new(move |d: &MeshDigest| {
                installed.record(d.clone());
            }),
            known: view_projector("mesh1.admin.1".parse().unwrap(), topology.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, book.clone(), records.clone()),
            primary: Arc::new(|| None),
            row: Arc::new(|_| Box::pin(async { Ok(None) })),
            issuer: Arc::new(crate::certs::CertIssuer::no_certs({
                let t = rafka_mesh_transport::clock::RafkaTime::unadopted();
                t.adopt(1_000);
                t
            })),
        };
        let endpoint = reported.node.endpoint_id.clone();
        assert!(authority.sender_of(&endpoint).await.is_none(), "before the join the authority's view does not hold the birth");
        let declare = StatusRequest::DeclareNodeState { node_id: reported.node.node_id.clone(), incarnation: reported.node.incarnation.clone(), state: NodeState::ReadyForTraffic };
        assert_eq!(authority.apply(None, &declare).await, StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown }, "before the join the subject is not held");
        let joined = door.serve(endpoint.clone(), JoinRequest::JoinNode { digest: (&reported).into() }).await;
        assert!(matches!(joined, JoinReply::Joined { .. }), "{joined:?}");

        // Acceptance made the birth known; no projection tick ran since.
        let sender = authority.sender_of(&endpoint).await.expect("the admitted birth is known when its join is answered");
        assert_eq!(authority.apply(Some(sender), &declare).await, StatusReply::Applied);

        // An endpoint that never joined sends the same declaration: decided for the subject it names.
        assert!(authority.sender_of(&EndpointId("key-never-joined".into())).await.is_none());
        assert_eq!(authority.apply(None, &declare).await, StatusReply::AlreadyApplied);
    }

    #[test]
    fn a_mesh_without_a_ready_admin_cedes_the_fabric_and_stays_pending() {
        use MemberStatus::*;
        let t = view(&[digest("mesh1.admin.1", Draining), digest("mesh1.rpc.1", ReadyForTraffic), digest("mesh2.admin.1", ReadyForTraffic)]);
        assert_eq!(t.fabric_primary().map(|n| n.name.to_string()).as_deref(), Some("mesh2.admin.1"));
        assert_eq!(t.meshes.iter().find(|m| m.name == "mesh1").unwrap().status, ScopeStatus::Pending);
        let empty = view(&[]);
        assert!(empty.fabric_primary().is_none());
        assert_eq!(empty.fabric.status, ScopeStatus::Pending);
    }

    /// CONTRACT (CLAUDE.md read rule: a kind with no projection is a point read through its store
    /// index; R12/R13: the mesh-primary writes the row, a stopped node is told nothing): `mesh1.rpc.1`
    /// is stopped and its mesh-primary moves its row to `mesh1.rpc.1.old` on the fabric's shared store
    /// root. Another admin, which performed no rename and holds nothing of it, lists the parked birth
    /// at the row's name: its view reads the name through the row's `path.name` key. What must NOT
    /// happen: that admin listing the birth at `mesh1.rpc.1`, the name its digest last published, so a
    /// start or a `node.get` there names a row that no longer holds it.
    #[tokio::test]
    async fn a_parked_birth_whose_row_another_admin_moved_is_listed_at_the_rows_name() {
        let d = crate::record_store::tempdir("row-name-view");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let primary = crate::storage::FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        let mine: Arc<dyn crate::storage::NodesStorage> = Arc::new(crate::storage::FileNodesStorage::open(&d.join("mesh1.admin.2"), &root, "mesh1", &IncarnationId::mint()).await.unwrap());
        let book = DigestBook::default();
        let held = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(held.clone());
        assert!(book.stopped(rafka_mesh_entity::LifecycleOp {
            build_id: "b1".into(),
            attempt: 1,
            operation: "stop-node:mesh1.rpc.1".into(),
            node_id: held.node.node_id.clone(),
            incarnation: held.node.incarnation.clone(),
            name: held.node.name.clone(),
            event_at_rafka_ms: 1,
        }));
        // The mesh-primary's rename on the shared root, through the one writer of a row's move.
        let to: PathName = "mesh1.rpc.1.old".parse().unwrap();
        let mut record = Node::allocated(held.node.name.clone());
        record.node_id = held.node.node_id.clone();
        record.incarnation_id = Some(held.node.incarnation.clone());
        record.status = NodeStatus::Leaving;
        let row = crate::storage::NodeRow { name: held.node.name.clone(), node_id: held.node.node_id.clone(), status: NodeStatus::Leaving, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None };
        assert_eq!(crate::storage::NodesStorage::put_node_row(&primary, &row).await.unwrap(), Ok(()));
        crate::node_rows::move_row(&primary, &"mesh1.admin.1".parse().unwrap(), &record, BTreeMap::new(), Default::default(), &held.node.name, &to).await.unwrap();
        // This admin: the same book, its own store on the same root, no rename of its own.
        let records = Arc::new(Records::default());
        let _ = records.contacts.set(mine);
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &DigestBook::default(), &Records::default())));
        view_projector("mesh1.admin.1".parse().unwrap(), topology.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, book.clone(), records.clone())().await;
        let t = topology.read().await.clone();
        let names: Vec<String> = t.nodes.iter().map(|n| n.name.to_string()).collect();
        let n = t.node(&to).unwrap_or_else(|| panic!("the parked birth is listed at its row's name: {names:?}"));
        assert_eq!((n.node_id.clone(), n.incarnation_id.clone(), n.status, n.parked), (held.node.node_id.clone(), Some(held.node.incarnation.clone()), NodeStatus::Leaving, true));
        assert!(t.node(&held.node.name).is_none(), "nothing is listed at the name the row moved from: {names:?}");
        // The book's held stopped digest keeps the name the node last published (R-JN1's last accepted
        // name): only the view names the birth by its row.
        assert_eq!(book.parked().iter().map(|d| d.node.name.clone()).collect::<Vec<_>>(), vec![held.node.name.clone()]);
        // The row moved back (node-replace.md Rollback): the next projection reads it there.
        crate::node_rows::move_row(&primary, &"mesh1.admin.1".parse().unwrap(), &record, BTreeMap::new(), Default::default(), &to, &held.node.name).await.unwrap();
        view_projector("mesh1.admin.1".parse().unwrap(), topology.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, book.clone(), records.clone())().await;
        let t = topology.read().await.clone();
        assert!(t.node(&held.node.name).is_some_and(|n| n.node_id == held.node.node_id && n.parked) && t.node(&to).is_none(), "{:?}", t.nodes.iter().map(|n| n.name.to_string()).collect::<Vec<_>>());
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (CLAUDE.md error rule: "never written" and "could not be read" are different facts):
    /// a parked birth whose row cannot be read (its entry in the row's id index is not a record this
    /// build reads) is not listed by the view, its name being unknown, and the refused read is one
    /// span naming the reading admin, the birth, the index and the store's own cause. What must NOT
    /// happen: the birth listed at the name its digest last published as if no row had moved it, or
    /// the refusal silent.
    #[tokio::test]
    async fn a_parked_birth_whose_row_cannot_be_read_is_not_listed_and_the_refusal_is_spanned() {
        let d = crate::record_store::tempdir("row-name-unreadable");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let mine: Arc<dyn crate::storage::NodesStorage> = Arc::new(crate::storage::FileNodesStorage::open(&d.join("mesh1.admin.2"), &root, "mesh1", &IncarnationId::mint()).await.unwrap());
        let book = DigestBook::default();
        let held = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(held.clone());
        assert!(book.stopped(rafka_mesh_entity::LifecycleOp {
            build_id: "b1".into(),
            attempt: 1,
            operation: "stop-node:mesh1.rpc.1".into(),
            node_id: held.node.node_id.clone(),
            incarnation: held.node.incarnation.clone(),
            name: held.node.name.clone(),
            event_at_rafka_ms: 1,
        }));
        let file = root.nodes("mesh1").join("ids").join(format!("{}.json", held.node.node_id));
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"not an index entry").unwrap();
        let records = Arc::new(Records::default());
        let _ = records.contacts.set(mine);
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &DigestBook::default(), &Records::default())));
        let capture = crate::span_capture::Capture::new("row-name-unreadable");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            view_projector("mesh1.admin.2".parse().unwrap(), topology.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, book.clone(), records.clone())().await;
        }
        let t = topology.read().await.clone();
        assert!(t.births().all(|n| n.node_id != held.node.node_id), "a birth whose row cannot be read is not listed: {:?}", t.nodes.iter().map(|n| n.name.to_string()).collect::<Vec<_>>());
        let spans = capture.named("rdm.node_admin.topology.reject.via-row-unreadable");
        assert_eq!(spans.len(), 1, "{spans:?}");
        let a = &spans[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("node"), attr("subject"), attr("index"), attr("node_id")), ("mesh1.admin.2".to_string(), "mesh1.rpc.1".to_string(), "id".to_string(), held.node.node_id.to_string()), "{a:?}");
        assert!(attr("detail").contains(&file.display().to_string()), "the store's cause names the file: {a:?}");
        eprintln!("span rdm.node_admin.topology.reject.via-row-unreadable trace_id={} {a:?}", spans[0].trace_id);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (states.md "Routing"): the view's `routable` is application routing's answer: a
    /// birth is routable only while it is ready-for-traffic and its mesh is ready-for-traffic, as this
    /// admin holds them. A `Pending` birth of a ready mesh, and a ready birth of a mesh this admin
    /// holds `pending` or `leaving` (or holds no status of), are listed and not routable; a ready
    /// birth of a ready mesh is routable; a birth listed only from this admin's launch record is
    /// Pending and not routable. Must NOT happen: a listed status other than
    /// ready-for-traffic, or a mesh not held ready-for-traffic, answering routable.
    #[test]
    fn the_view_routes_a_birth_only_while_it_and_its_mesh_are_ready_for_traffic() {
        use rafka_mesh_transport::membership::Frame;
        let book = DigestBook::default();
        let held = |mesh: &str, status: &str| {
            let f = Frame::MeshStatus { mesh: mesh.into(), status: status.into(), publisher: format!("{mesh}.admin.1"), forwarded_by: None, changed_at_rafka_ms: 10 };
            assert!(book.statuses.take(&f, &fabric1()), "{f:?}");
        };
        held("mesh1", "ready-for-traffic");
        held("mesh2", "pending");
        held("mesh3", "leaving");
        let (ready, pending) = (digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic), digest("mesh1.rpc.2", MemberStatus::Pending));
        let (of_pending_mesh, of_leaving_mesh, of_unheard_mesh) = (digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic), digest("mesh3.rpc.1", MemberStatus::ReadyForTraffic), digest("mesh4.rpc.1", MemberStatus::ReadyForTraffic));
        for d in [&ready, &pending, &of_pending_mesh, &of_leaving_mesh, &of_unheard_mesh] {
            assert!(book.record(d.clone()));
        }
        // A birth this admin launched that membership has not spoken for yet: listed from its
        // launch record, Pending, nothing realised.
        let records = Records::default();
        let launched: PathName = "mesh1.rpc.3".parse().unwrap();
        records.nodes.lock().unwrap().insert(launched.clone(), crate::model::Node::allocated(launched.clone()));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        let routable = |d: &MeshDigest| t.node(&d.node.name).unwrap_or_else(|| panic!("{} is listed", d.node.name)).routable;
        let unheard = t.node(&launched).expect("the launch record lists the unheard birth");
        assert_eq!(unheard.status, NodeStatus::Pending);
        assert!(!unheard.routable, "a launched birth membership has not spoken for is Pending: a routing stop");
        assert!(routable(&ready), "a ready-for-traffic birth of a ready-for-traffic mesh is routable");
        assert!(!routable(&pending), "a Pending birth is a routing stop");
        assert!(!routable(&of_pending_mesh), "a birth of a pending mesh is a routing stop");
        assert!(!routable(&of_leaving_mesh), "a birth of a leaving mesh is a routing stop");
        assert!(!routable(&of_unheard_mesh), "a birth of a mesh this admin holds no status of is not held ready-for-traffic");
    }

    /// CONTRACT (gossip.md §6.1: a stopped status by itself and silence are not departure proof;
    /// node-identity-and-state.md §4.1: `Leaving` is a status the routing rule reads; §6: the
    /// observer's inference evicts at the staleness floor): a member the book holds `Leaving` is
    /// listed `Leaving` with the state it declared, whatever the age of its last heartbeat within the
    /// staleness floor: a node declared Leaving that still heartbeats is a member of its mesh until it
    /// is stopped or deleted. Past the floor it is the silent member any silent member is. Must NOT
    /// happen: a `Leaving` member one gossip interval and a millisecond after its heartbeat absent from
    /// the view, which is what makes `declared` unreadable at the authority that applied it.
    #[test]
    fn a_member_held_leaving_is_listed_leaving_with_its_declared_state_until_it_is_silent_past_the_staleness_floor() {
        use rafka_node_rpc_contract::status::NodeState;
        let book = DigestBook::default();
        let records = Records::default();
        let d = digest("mesh1.rpc.1", MemberStatus::Leaving);
        let t0 = std::time::Instant::now();
        assert!(book.record_at(d.clone(), t0));
        records.declared.lock().unwrap().nodes.insert((d.node.node_id.clone(), d.node.incarnation.clone()), NodeState::Leaving);
        let listed = |at: std::time::Instant| project_at("fabric1", &fabric1(), ProviderKind::Process, &book, &records, at).node(&d.node.name).cloned();

        let within = listed(t0 + gossip_interval() + Duration::from_millis(1)).expect("a Leaving member one interval after its heartbeat is a member of the view");
        assert_eq!((within.status, within.declared.as_deref(), within.routable, within.parked), (NodeStatus::Leaving, Some("Leaving"), false, false));
        let silent = listed(t0 + book.staleness_floor() + Duration::from_millis(1)).expect("silent past the floor it is listed, not departed");
        assert_eq!((silent.status, silent.declared.as_deref()), (NodeStatus::PendingReconnect, Some("Leaving")), "silent past the staleness floor it is the silent member any silent member is");
    }

    /// CONTRACT (states.md: a mesh is pending -> ready-for-traffic -> leaving -> dead; "Routing": a
    /// mesh Leaving or Dead is a routing stop): the mesh status this admin projects is the state its
    /// mesh row holds once that is past ready-for-traffic. The projection is what the mesh primary
    /// announces on the existing MeshStatus send (`Backbone::announce_statuses`, fed from
    /// `t.meshes`), so a leaving mesh is announced leaving by its own primary. Must NOT happen: a
    /// mesh whose row holds Leaving (or Dead) projected, and so announced, ready-for-traffic.
    #[test]
    fn a_mesh_whose_row_holds_leaving_is_projected_leaving_so_its_primary_announces_leaving() {
        use rafka_node_rpc_contract::status::MeshState;
        let book = DigestBook::default();
        let records = Records::default();
        for (mesh, state) in [("mesh1", MeshState::Leaving), ("mesh2", MeshState::Dead), ("mesh3", MeshState::ReadyForTraffic)] {
            assert!(book.record(digest(&format!("{mesh}.admin.1"), MemberStatus::ReadyForTraffic)));
            records.meshes.lock().unwrap().insert(mesh.into(), mesh_id(mesh));
            records.declared.lock().unwrap().meshes.insert(mesh_id(mesh), state);
        }
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        let status = |m: &str| t.meshes.iter().find(|x| x.name == m).unwrap_or_else(|| panic!("{m} is in the view: {:?}", t.meshes)).status;
        assert_eq!(status("mesh1"), ScopeStatus::Leaving, "a mesh whose row holds Leaving is projected leaving, so its primary announces leaving");
        assert_eq!(status("mesh2"), ScopeStatus::Dead, "a mesh whose row holds Dead is projected dead");
        assert_eq!(status("mesh3"), ScopeStatus::ReadyForTraffic, "a mesh whose row holds ready-for-traffic is projected from its members, as before");
    }

    /// CONTRACT (states.md "Routing": a stop is applied by a peer on receipt of the fact): the view
    /// application routing reads is rebuilt at once when a routing fact is taken, never on the
    /// poll. A mesh status taken makes the rebuild due though the poll is an hour away; so does a
    /// birth saying another status, and a lifecycle overlay opening. Must NOT happen: the view
    /// waiting for its poll to apply a fact the book already holds.
    #[tokio::test(start_paused = true)]
    async fn the_view_rebuild_is_due_on_receipt_of_a_routing_fact_not_on_the_poll() {
        use futures_util::FutureExt;
        use rafka_mesh_transport::membership::Frame;
        let book = DigestBook::default();
        let rpc = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        assert!(book.record(rpc.clone()));
        let hour = Duration::from_secs(3600);
        let mut facts = book.routing_watch();
        let gate = crate::park_gate::ParkGate::new();

        facts.seen();
        let f = Frame::MeshStatus { mesh: "mesh1".into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10 };
        assert!(book.statuses.take(&f, &fabric1()));
        assert!(view_rebuild_due(&mut facts, hour, &gate).now_or_never().is_some(), "a mesh status taken makes the rebuild due at once");

        facts.seen();
        assert!(view_rebuild_due(&mut facts, hour, &gate).now_or_never().is_none(), "nothing moved: the rebuild waits for its poll");
        let mut draining = rpc.clone();
        draining.status = MemberStatus::Draining;
        draining.digest_seq += 1;
        assert!(book.record(draining));
        assert!(view_rebuild_due(&mut facts, hour, &gate).now_or_never().is_some(), "a birth saying Draining makes the rebuild due at once");

        facts.seen();
        let stop = rafka_mesh_entity::LifecycleOp { build_id: "b1".into(), attempt: 1, operation: "stop-node:mesh1.rpc.1".into(), node_id: rpc.node.node_id.clone(), incarnation: rpc.node.incarnation.clone(), name: rpc.node.name.clone(), event_at_rafka_ms: 1 };
        assert!(book.stopped(stop));
        assert!(view_rebuild_due(&mut facts, hour, &gate).now_or_never().is_some(), "a NodeStopped overlay makes the rebuild due at once");
    }

    /// CONTRACT (node.stop: a parked admin's tasks are held; states.md "Routing"): the view rebuild
    /// is held at its own wait, never inside the view's lock. While the admin's gate is parked a
    /// routing fact makes no rebuild due; the gate opening does, at once. Must NOT happen: a parked
    /// admin rebuilding its view, or a rebuild held where it waits on the view's lock (a gated task
    /// stops where it stands, and one stopped queued on the lock holds it from every reader: the
    /// parked admin then never serves the start that would open it).
    #[tokio::test(start_paused = true)]
    async fn a_parked_admins_view_rebuild_is_held_at_its_own_wait_and_resumes_when_the_gate_opens() {
        use futures_util::FutureExt;
        use rafka_mesh_transport::membership::Frame;
        let book = DigestBook::default();
        let mut facts = book.routing_watch();
        let gate = crate::park_gate::ParkGate::new();
        let hour = Duration::from_secs(3600);
        facts.seen();
        gate.park();
        let f = Frame::MeshStatus { mesh: "mesh1".into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10 };
        assert!(book.statuses.take(&f, &fabric1()));
        let due = view_rebuild_due(&mut facts, hour, &gate);
        tokio::pin!(due);
        assert!((&mut due).now_or_never().is_none(), "parked: a routing fact makes no rebuild due");
        gate.resume();
        assert!((&mut due).now_or_never().is_some(), "the gate opening makes the held rebuild due at once");
    }

    /// A nodes store over `inner` that counts the node-row reads made through it, by `path.name` key
    /// and by node id, and how many of them ran while `view`'s write lock was held.
    struct ProbedRows {
        inner: Arc<dyn crate::storage::NodesStorage>,
        view: Arc<RwLock<Topology>>,
        key_reads: std::sync::atomic::AtomicUsize,
        id_reads: std::sync::atomic::AtomicUsize,
        under_write_lock: std::sync::atomic::AtomicUsize,
    }

    impl ProbedRows {
        fn read(&self, by: &std::sync::atomic::AtomicUsize) {
            by.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.view.try_read().is_err() {
                self.under_write_lock.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        fn count(by: &std::sync::atomic::AtomicUsize) -> usize {
            by.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for ProbedRows {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            self.inner.runtimes().await
        }
        async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            self.inner.runtime(node_id, incarnation).await
        }
        async fn own(&self) -> Result<Option<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.inner.own().await
        }
        async fn put_own(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_own(record).await
        }
        async fn contacts(&self) -> Result<Vec<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.inner.contacts().await
        }
        async fn put_contact(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_contact(record).await
        }
        async fn remove_contact(&self, mesh: &str, node_id: &NodeId) -> Result<(), crate::record_store::StorageError> {
            self.inner.remove_contact(mesh, node_id).await
        }
        async fn put_node_row(&self, row: &crate::storage::NodeRow) -> Result<Result<(), crate::storage::NameHeld>, crate::record_store::StorageError> {
            self.inner.put_node_row(row).await
        }
        async fn node_row_at(&self, name: &PathName) -> Result<Option<crate::storage::NodeRow>, crate::record_store::StorageError> {
            self.read(&self.key_reads);
            self.inner.node_row_at(name).await
        }
        async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, crate::record_store::StorageError> {
            self.read(&self.id_reads);
            self.inner.node_row_name(mesh, node_id).await
        }
        async fn hold_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.inner.hold_node_row(name, holder).await
        }
        async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.inner.release_node_row(name, holder).await
        }
        async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<NodeId>, crate::record_store::StorageError> {
            self.inner.tagged(label).await
        }
    }

    /// `mesh1.rpc.1`, born `mesh1.rpc.1.new` and stopped there, its row moved twice by its mesh-primary
    /// on the fabric's shared store root (to `mesh1.rpc.1`, then to `mesh1.rpc.1.old`: each move one
    /// suffix toggled, the stopped node told nothing), and another admin's view over the same root,
    /// whose row reads are probed.
    async fn renamed_twice(cell: &str) -> (std::path::PathBuf, MeshDigest, Arc<ProbedRows>, Arc<RwLock<Topology>>, Arc<Records>, DigestBook) {
        let d = crate::record_store::tempdir(cell);
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let primary = crate::storage::FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        let book = DigestBook::default();
        let held = digest("mesh1.rpc.1.new", MemberStatus::ReadyForTraffic);
        book.record(held.clone());
        assert!(book.stopped(rafka_mesh_entity::LifecycleOp {
            build_id: "b1".into(),
            attempt: 1,
            operation: "stop-node:mesh1.rpc.1.new".into(),
            node_id: held.node.node_id.clone(),
            incarnation: held.node.incarnation.clone(),
            name: held.node.name.clone(),
            event_at_rafka_ms: 1,
        }));
        let mut record = Node::allocated(held.node.name.clone());
        record.node_id = held.node.node_id.clone();
        record.incarnation_id = Some(held.node.incarnation.clone());
        record.status = NodeStatus::Leaving;
        let row = crate::storage::NodeRow { name: held.node.name.clone(), node_id: held.node.node_id.clone(), status: NodeStatus::Leaving, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None };
        assert_eq!(crate::storage::NodesStorage::put_node_row(&primary, &row).await.unwrap(), Ok(()));
        let (me, base) = ("mesh1.admin.1".parse::<PathName>().unwrap(), held.node.name.base());
        crate::node_rows::move_row(&primary, &me, &record, BTreeMap::new(), Default::default(), &held.node.name, &base).await.unwrap();
        crate::node_rows::move_row(&primary, &me, &record, BTreeMap::new(), Default::default(), &base, &base.renamed()).await.unwrap();
        let view = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &DigestBook::default(), &Records::default())));
        let probed = Arc::new(ProbedRows {
            inner: Arc::new(crate::storage::FileNodesStorage::open(&d.join("mesh1.admin.2"), &root, "mesh1", &IncarnationId::mint()).await.unwrap()),
            view: view.clone(),
            key_reads: Default::default(),
            id_reads: Default::default(),
            under_write_lock: Default::default(),
        });
        let records = Arc::new(Records::default());
        let _ = records.contacts.set(probed.clone());
        (d, held, probed, view, records, book)
    }

    /// CONTRACT (CLAUDE.md read rule: "id: only a lookup by id that cannot read the store's own key";
    /// "add the index, never the scan"): a parked birth whose row was moved twice while it was stopped
    /// (`mesh1.rpc.1.new` to `mesh1.rpc.1`, then to `mesh1.rpc.1.old`) is listed by another admin's
    /// view at the row's current name, `mesh1.rpc.1.old`, read by the birth's node id through the
    /// node row's id index: one read, no `path.name` key guessed. What must NOT happen: the birth
    /// listed at a name it no longer holds, or its name found by trying the keys a move could reach.
    #[tokio::test]
    async fn a_parked_birth_renamed_twice_by_its_row_is_listed_at_the_rows_current_name_read_by_its_node_id() {
        let (d, held, probed, view, records, book) = renamed_twice("row-name-twice").await;
        view_projector("mesh1.admin.2".parse().unwrap(), view.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, book.clone(), records.clone())().await;
        let t = view.read().await.clone();
        let names: Vec<String> = t.nodes.iter().map(|n| n.name.to_string()).collect();
        let now = held.node.name.base().renamed();
        assert!(t.node(&now).is_some_and(|n| n.node_id == held.node.node_id && n.parked), "the parked birth is listed at its row's current name: {names:?}");
        assert!(t.node(&held.node.name).is_none() && t.node(&held.node.name.base()).is_none(), "nothing is listed at a name the row moved from: {names:?}");
        assert_eq!((ProbedRows::count(&probed.key_reads), ProbedRows::count(&probed.id_reads)), (0, 1), "the birth's row is read once, by its node id, and no path.name key is tried");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (no disk read under the view's write lock): the projection reads the rows of the
    /// births that do not publish first, then takes the view's write lock and installs. Every row
    /// read finds the view's lock free. What must NOT happen: a row read made while the view's write
    /// lock is held, which holds every reader of the view (a parked admin's handlers, the router)
    /// behind a disk read.
    #[tokio::test]
    async fn the_projection_reads_its_rows_before_it_takes_the_views_write_lock() {
        let (d, _held, probed, view, records, book) = renamed_twice("row-name-lock").await;
        view_projector("mesh1.admin.2".parse().unwrap(), view.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, book.clone(), records.clone())().await;
        let reads = ProbedRows::count(&probed.key_reads) + ProbedRows::count(&probed.id_reads);
        assert!(reads >= 1, "the projection read the parked birth's row");
        assert_eq!(ProbedRows::count(&probed.under_write_lock), 0, "none of the {reads} row reads ran under the view's write lock");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (node.stop, a successor admin): an admin that joined after the stop never held the
    /// node's digest, only the stop overlay the mesh topology carries (`Members.in_flight`, read
    /// by `GetTopology`). Its view lists the node `Leaving` and parked from that overlay.
    #[test]
    fn a_successor_admin_lists_a_parked_node_from_the_stop_overlay_it_read() {
        let book = DigestBook::default();
        let stopped = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        let stop = rafka_mesh_entity::LifecycleOp {
            build_id: "b1".into(),
            attempt: 1,
            operation: "stop-node:mesh1.rpc.1".into(),
            node_id: stopped.node.node_id.clone(),
            incarnation: stopped.node.incarnation.clone(),
            name: stopped.node.name.clone(),
            event_at_rafka_ms: 1,
        };
        assert!(book.stopped(stop));
        assert!(book.get(stopped.node.node_id.as_str()).is_none(), "this admin never held the node's digest");
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let n = t.node(&stopped.node.name).expect("the stop overlay lists the parked node");
        assert_eq!((n.status, n.parked, n.routable), (NodeStatus::Leaving, true, false));
        assert_eq!(n.node_id, stopped.node.node_id);
        assert_eq!(n.incarnation_id.as_ref(), Some(&stopped.node.incarnation));
    }

    #[test]
    fn a_launch_record_never_revives_a_birth_that_another_admin_retired() {
        // This admin launched mesh1.rpc.1; another admin (the primary) retired
        // it: its last word is Leaving, then silence. Membership speaks for the
        // birth (silent past the floor), so this admin's own launch record must
        // not bring it back as ready.
        let book = DigestBook::default();
        let left = digest("mesh1.rpc.1", MemberStatus::Leaving);
        book.record(left.clone());
        let records = Records::default();
        let mut n = Node::allocated(left.node.name.clone());
        n.incarnation_id = Some(left.node.incarnation.clone());
        n.status = NodeStatus::ReadyForTraffic;
        records.publish(n.clone());
        let past_floor = std::time::Instant::now() + book.staleness_floor() + Duration::from_millis(1);
        let t = project_at("fabric1", &fabric1(), ProviderKind::Process, &book, &records, past_floor);
        assert_eq!(t.node(&left.node.name).map(|n| n.status), Some(NodeStatus::PendingReconnect), "the book speaks for the birth, silent past the floor; the launch record's ready never stands in: {:?}", t.node(&left.node.name));
        // A birth it launched that membership has not spoken for yet is shown.
        let fresh = Records::default();
        let mut pending = Node::allocated("mesh1.rpc.2".parse().unwrap());
        pending.incarnation_id = Some(IncarnationId::mint());
        fresh.publish(pending.clone());
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &fresh);
        assert!(t.node(&pending.name).is_some());
        // A birth it launched whose departure another admin proved and published: the book no
        // longer holds the digest, and the launch record never stands in for it.
        let gone = digest("mesh1.rpc.3", MemberStatus::ReadyForTraffic);
        book.record(gone.clone());
        let launched = Records::default();
        let mut n = Node::allocated(gone.node.name.clone());
        n.node_id = gone.node.node_id.clone();
        n.incarnation_id = Some(gone.node.incarnation.clone());
        n.status = NodeStatus::ReadyForTraffic;
        launched.publish(n);
        assert!(project("fabric1", &fabric1(), ProviderKind::Process, &book, &launched).node(&gone.node.name).is_some());
        book.depart(rafka_mesh_entity::LifecycleOp {
            build_id: "b1".into(),
            attempt: 1,
            operation: "retire-node:mesh1.rpc.3".into(),
            node_id: gone.node.node_id.clone(),
            incarnation: gone.node.incarnation.clone(),
            name: gone.node.name.clone(),
            event_at_rafka_ms: 1,
        });
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &launched);
        assert!(t.node(&gone.node.name).is_none(), "a departed birth's launch record is not a node: {:?}", t.node(&gone.node.name));
    }

    #[test]
    fn removed_births_leave_the_view_and_a_newer_birth_at_the_path_does_not() {
        let book = DigestBook::default();
        let old = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(old.clone());
        let records = Records::default();
        let mut n = Node::allocated(old.node.name.clone());
        n.incarnation_id = Some(old.node.incarnation.clone());
        records.publish(n);
        records.remove(&old.node.name);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert!(t.node(&old.node.name).is_none(), "the removed birth is gone");
        let mut newer = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        newer.node.node_id = NodeId::mint();
        book.record(newer.clone());
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.node(&old.node.name).unwrap().incarnation_id.as_ref(), Some(&newer.node.incarnation));
    }

    /// CONTRACT (node-recovery.md, sequence step 3): the recovery's delete removes exactly the exited
    /// birth. A rerun of the recovery after the new node already holds the path removes nothing of the
    /// new node, and the exited birth stays out of the view even when the records never held it.
    /// Must NOT happen: the new node's record removed, or its birth filtered from the view.
    #[test]
    fn a_recovery_removes_exactly_the_exited_birth_and_never_the_new_node_at_its_path() {
        let book = DigestBook::default();
        let old = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(old.clone());
        let mut new = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        new.node.node_id = NodeId::mint();
        book.record(new.clone());
        let records = Records::default();
        let mut n = Node::allocated(new.node.name.clone());
        n.node_id = new.node.node_id.clone();
        n.incarnation_id = Some(new.node.incarnation.clone());
        records.publish(n);
        // The rerun: the new node holds the path, the exited birth is removed again.
        records.remove_birth(&old.node.name, &old.node.node_id, &old.node.incarnation);
        assert_eq!(records.nodes.lock().unwrap().get(&new.node.name).map(|n| n.node_id.clone()), Some(new.node.node_id.clone()), "the new node's record stays");
        let removed = records.removed.lock().unwrap().clone();
        assert!(removed.contains(&(old.node.name.clone(), Some(old.node.incarnation.clone()))), "the exited birth is the one removed: {removed:?}");
        assert!(!removed.contains(&(new.node.name.clone(), Some(new.node.incarnation.clone()))), "the new birth is not removed: {removed:?}");
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.node(&new.node.name).map(|n| n.node_id.clone()), Some(new.node.node_id.clone()), "the view holds the new node at the path");
    }

    /// CONTRACT: a view installed by a writer that had to wait for the view lock reflects the
    /// records as they are when it holds the lock, so a removal that lands while it waits is not
    /// undone by the older projection.
    #[tokio::test]
    async fn a_projection_that_waits_for_the_view_lock_installs_the_state_at_the_lock() {
        let book = Arc::new(DigestBook::default());
        let old = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(old.clone());
        let records = Arc::new(Records::default());
        let mut n = Node::allocated(old.node.name.clone());
        n.incarnation_id = Some(old.node.incarnation.clone());
        records.publish(n);
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)));
        assert!(topology.read().await.node(&old.node.name).is_some(), "the node is in the view before its removal");
        // Another installer holds the view while this one is asked to install.
        let held = topology.write().await;
        let waiting = {
            let (records, topology, book) = (records.clone(), topology.clone(), book.clone());
            tokio::spawn(async move { records.install_projected(&topology, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await })
        };
        tokio::task::yield_now().await;
        records.remove(&old.node.name);
        drop(held);
        waiting.await.unwrap();
        assert!(topology.read().await.node(&old.node.name).is_none(), "the view holds the removed node: a projection taken before the lock was installed after the removal");
    }

    /// A status store whose exit-proof row read, while `latch` is set, takes its answer and then
    /// waits on a latch: it says it is inside the read, then holds until released. While `refuse` is
    /// set the read fails. Every other row is the memory store's.
    struct LatchedExits {
        inner: crate::status_storage::MemoryStatusStorage,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        latch: std::sync::atomic::AtomicBool,
        refuse: std::sync::atomic::AtomicBool,
    }

    impl LatchedExits {
        fn new(latch: bool) -> Self {
            Self { inner: Default::default(), entered: tokio::sync::Notify::new(), release: tokio::sync::Notify::new(), latch: latch.into(), refuse: false.into() }
        }
    }

    #[async_trait::async_trait]
    impl crate::status_storage::StatusStorage for LatchedExits {
        async fn put_mesh_status(&self, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_mesh_status(row).await
        }
        async fn mesh_statuses(&self) -> Result<HashMap<MeshId, rafka_node_rpc_contract::status::MeshState>, crate::record_store::StorageError> {
            self.inner.mesh_statuses().await
        }
        async fn put_fabric_event(&self, row: &crate::status_storage::FabricEventRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_fabric_event(row).await
        }
        async fn fabric_events(&self) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::record_store::StorageError> {
            self.inner.fabric_events().await
        }
        async fn fabric_events_of(&self, mesh: &str) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::record_store::StorageError> {
            self.inner.fabric_events_of(mesh).await
        }
        async fn put_node_declared(&self, row: &crate::status_storage::NodeDeclaredRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_declared(row).await
        }
        async fn remove_node_declared(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), crate::record_store::StorageError> {
            self.inner.remove_node_declared(node_id, incarnation_id).await
        }
        async fn node_declared(&self) -> Result<Vec<crate::status_storage::NodeDeclaredRow>, crate::record_store::StorageError> {
            self.inner.node_declared().await
        }
        async fn put_node_silence(&self, row: &crate::status_storage::NodeSilenceRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_silence(row).await
        }
        async fn node_silences(&self) -> Result<Vec<crate::status_storage::NodeSilenceRow>, crate::record_store::StorageError> {
            self.inner.node_silences().await
        }
        async fn put_node_exit(&self, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_exit(row).await
        }
        async fn node_exit(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::record_store::StorageError> {
            self.inner.node_exit(node_id, incarnation_id).await
        }
        async fn mesh_status_held_of(&self, mesh: &str, row: &crate::status_storage::MeshStatusRow) -> Result<bool, crate::record_store::StorageError> {
            self.inner.mesh_status_held_of(mesh, row).await
        }
        async fn put_mesh_status_of(&self, mesh: &str, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_mesh_status_of(mesh, row).await
        }
        async fn node_exit_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::record_store::StorageError> {
            let answer = self.inner.node_exit_of(mesh, node_id, incarnation_id).await;
            if self.latch.load(std::sync::atomic::Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            if self.refuse.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(crate::record_store::StorageError::Io { file: format!("status/{mesh}/exits"), reason: "the cell refuses the read".into() });
            }
            answer
        }
        async fn put_node_exit_of(&self, mesh: &str, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_exit_of(mesh, row).await
        }
    }

    /// CONTRACT (s41 land 4: no disk read under the view's write lock): an install reads the
    /// exit-proof rows of the births it projects before it takes the view's write lock, so a reader of
    /// the view (a handler, the status authority) is served while an exit-row read is slow, and the
    /// projection installed under the lock holds what the read found. Must NOT happen: the view's
    /// write lock held across a point read of the store.
    #[tokio::test]
    async fn a_reader_of_the_view_is_served_while_an_installs_exit_row_read_is_slow() {
        use crate::status_storage::StatusStorage as _;
        let book = Arc::new(DigestBook::default());
        let lost = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(lost.clone());
        let store = Arc::new(LatchedExits::new(true));
        let row = crate::status_storage::NodeExitRow { node_id: lost.node.node_id.clone(), name: lost.node.name.clone(), incarnation_id: lost.node.incarnation.clone(), exit_code: Some(9), proof: "provider".into(), observed_at_ms: 1 };
        store.inner.put_node_exit_of("mesh1", &row).await.unwrap();
        let records = Arc::new(Records::default());
        records.set_status(store.clone(), "mesh1".into(), "mesh1.admin.1".parse().unwrap(), NodeId::mint());
        records.set_book((*book).clone());
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &DigestBook::default(), &Records::default())));
        let capture = crate::span_capture::Capture::new("i143-3068-exit-rows-read-before-the-view-lock");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let install = {
            let (records, topology, book) = (records.clone(), topology.clone(), book.clone());
            tokio::spawn(async move { records.install_projected(&topology, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await })
        };
        store.entered.notified().await;
        let served = topology.try_read().map(|t| t.nodes.len());
        store.release.notify_one();
        let installed = install.await.unwrap();
        assert!(served.is_ok(), "the view's write lock is held while the install waits on an exit-proof row read");
        let listed = installed.node(&lost.node.name).map(|n| n.status);
        assert_eq!(listed, Some(NodeStatus::Dead), "the projection installed under the lock holds the exit the read found");
        assert_eq!(topology.read().await.node(&lost.node.name).map(|n| n.status), Some(NodeStatus::Dead));
        let read = capture.named("rdm.node_admin.node.resolve.via-exit-proof-row");
        assert_eq!(read.len(), 1, "the read phase reads the birth's exit-proof row once: {read:?}");
        eprintln!("span rdm.node_admin.node.resolve.via-exit-proof-row trace_id={} {:?}", read[0].trace_id, read[0].attributes);
    }

    /// CONTRACT (s41 land 4; node-recovery.md: "Exited is the proof, recorded durably"; the status
    /// store puts and point-reads an exit-proof row and nothing removes it): with the reads outside
    /// the view's lock two installs interleave, and a proof once read stays held while its birth is
    /// named: an install whose read was taken before the row was put, and stored after a later
    /// install read the row, takes nothing away; nor does a read that fails. Must NOT happen: a
    /// proven-exited birth read live again (and so seated again) by a stale or failed read.
    #[tokio::test]
    async fn an_exit_proof_once_read_is_never_withdrawn_by_an_older_or_failed_read() {
        use crate::status_storage::StatusStorage as _;
        use std::sync::atomic::Ordering::SeqCst;
        let book = Arc::new(DigestBook::default());
        let lost = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(lost.clone());
        let store = Arc::new(LatchedExits::new(true));
        let records = Arc::new(Records::default());
        records.set_status(store.clone(), "mesh1".into(), "mesh1.admin.1".parse().unwrap(), NodeId::mint());
        records.set_book((*book).clone());
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)));
        let status = |t: &Topology| t.node(&lost.node.name).map(|n| n.status);
        let capture = crate::span_capture::Capture::new("i143-3068-exit-proof-never-withdrawn");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        // The older install: its read finds no row, and it is held before it stores.
        let older = {
            let (records, topology, book) = (records.clone(), topology.clone(), book.clone());
            tokio::spawn(async move { records.install_projected(&topology, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await })
        };
        store.entered.notified().await;
        store.latch.store(false, SeqCst);
        // The exit is proven and its row put; a later install reads it.
        let row = crate::status_storage::NodeExitRow { node_id: lost.node.node_id.clone(), name: lost.node.name.clone(), incarnation_id: lost.node.incarnation.clone(), exit_code: Some(9), proof: "provider".into(), observed_at_ms: 1 };
        store.inner.put_node_exit_of("mesh1", &row).await.unwrap();
        let later = records.install_projected(&topology, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
        assert_eq!(status(&later), Some(NodeStatus::Dead), "the later install reads the proof");
        store.release.notify_one();
        let after_older = older.await.unwrap();
        assert_eq!(status(&after_older), Some(NodeStatus::Dead), "the older read, stored after the later one, withdrew the proof");
        // A read that fails proves nothing new and takes nothing away.
        store.refuse.store(true, SeqCst);
        let failed = records.install_projected(&topology, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
        assert_eq!(status(&failed), Some(NodeStatus::Dead), "a failed read withdrew the proof");
        assert_eq!(status(&*topology.read().await), Some(NodeStatus::Dead));
        let (read, unreadable) = (capture.named("rdm.node_admin.node.resolve.via-exit-proof-row"), capture.named("rdm.node_admin.node.reject.via-exit-row-unreadable"));
        assert_eq!((read.len(), unreadable.len()), (1, 1), "the proof enters once, and the failed read is named once: {read:?} {unreadable:?}");
        eprintln!("span rdm.node_admin.node.resolve.via-exit-proof-row trace_id={} {:?}", read[0].trace_id, read[0].attributes);
        eprintln!("span rdm.node_admin.node.reject.via-exit-row-unreadable trace_id={} {:?}", unreadable[0].trace_id, unreadable[0].attributes);
    }

    /// CONTRACT (node-recovery.md: "Exited is the proof, recorded durably"; the projection reads
    /// every birth's exit-proof row): a birth this admin's book never held, listed from the record
    /// this admin published of it (a mesh leave's member run from the accepted roster, a launch not
    /// yet heard), is listed `Dead` once its exit-proof row is read, as a birth listed from a digest
    /// or a stop is. Must NOT happen: a proven-exited birth listed in the state its record last held.
    #[tokio::test]
    async fn a_birth_listed_from_its_record_alone_is_dead_once_its_exit_proof_is_read() {
        let book = DigestBook::default();
        let records = Records::default();
        records.set_status(Arc::new(crate::status_storage::MemoryStatusStorage::default()), "mesh2".into(), "mesh2.admin.2".parse().unwrap(), NodeId::mint());
        records.set_book(book.clone());
        let mut n = Node::allocated("mesh2.rpc.1".parse().unwrap());
        n.incarnation_id = Some(IncarnationId::mint());
        n.status = NodeStatus::Leaving;
        records.publish(n.clone());
        let view = RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &book, &records));
        assert_eq!(view.read().await.node(&n.name).map(|v| v.status), Some(NodeStatus::Leaving), "listed from its record");
        records.put_exit(&n, Some(0), "terminal-receipt").await.unwrap();
        let t = records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
        assert_eq!(t.node(&n.name).map(|v| v.status), Some(NodeStatus::Dead), "the proven exit of a birth listed from its record alone");
    }

    const STORE_DIR: &str = "/tmp/rdm-admin-cells/store";

    fn cfg(env: &[(&str, &str)]) -> AdminConfig {
        let mut env: std::collections::HashMap<String, String> = env.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        env.entry("RDM_STORE_DIR".into()).or_insert_with(|| STORE_DIR.into());
        AdminConfig::from_env(|k| env.get(k).cloned()).expect("a readable environment")
    }

    /// CONTRACT (R14 items 1 and 8; acceptance "a missing store_dir is refused by name at boot"):
    /// a node-admin is given the fabric's store root (`RDM_STORE_DIR`, or its launch's); with none it
    /// is refused by name and nothing falls back to its data dir.
    #[test]
    fn a_start_without_a_store_dir_is_refused_by_name() {
        let env: std::collections::HashMap<String, String> = [("RDM_DATA_DIR", "/tmp/rdm-no-store/mesh1.admin.1")].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let e = AdminConfig::from_env(|k| env.get(k).cloned()).err().expect("a start with no store_dir was accepted");
        assert!(e.contains("RDM_STORE_DIR") && e.contains("required"), "{e}");
        assert_eq!(cfg(&[]).store_dir, std::path::PathBuf::from(STORE_DIR));
    }

    /// CONTRACT (node-connections-delete.md step 5): a person-started admin reads its
    /// connections-delete bound once, from `RDM_CONNECTIONS_DELETE_DEADLINE_MS` in its config, 5000 ms
    /// when it names none; a launched admin takes its launch's. A value that is no whole number of
    /// milliseconds is refused by name at boot, never read as the default.
    #[test]
    fn an_admin_takes_its_connections_delete_bound_from_its_config_once() {
        assert_eq!(cfg(&[]).connections_delete_deadline, rafka_mesh_entity::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE);
        assert_eq!(cfg(&[("RDM_CONNECTIONS_DELETE_DEADLINE_MS", "750")]).connections_delete_deadline, Duration::from_millis(750));
        let env: std::collections::HashMap<String, String> = [("RDM_STORE_DIR", STORE_DIR), ("RDM_CONNECTIONS_DELETE_DEADLINE_MS", "soon")].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let e = AdminConfig::from_env(|k| env.get(k).cloned()).err().expect("a bound that is no number was accepted");
        assert!(e.starts_with("RDM_CONNECTIONS_DELETE_DEADLINE_MS `soon`"), "{e}");
        let launch = rafka_mesh_entity::launch::Launch {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            name: "mesh1.admin.2".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
            launcher: None,
            seeds: vec![],
            data_dir: "/tmp/rdm-launched/mesh1.admin.2".into(),
            store_dir: STORE_DIR.into(),
            mesh_id: None,
            mesh_issuer: None,
            connections_delete_deadline: Duration::from_millis(1250),
        };
        let launched = launch.to_env();
        assert_eq!(AdminConfig::from_env(|k| launched.get(k).cloned()).unwrap().connections_delete_deadline, Duration::from_millis(1250));
    }

    const MESH_ID: &str = "4f14h1k7m3p9";
    const SEED: &str = "5d6b3f0e1a2c4b7d8e9f00112233445566778899aabbccddeeff001122334455@127.0.0.1:41000";

    /// A person-started admin that is not a restart is exactly one of: Day 0 (no mesh, no seed, no
    /// flag, an empty data dir) or a fabric recovery (Fabric, mesh, seed and both flags). Anything
    /// else is refused by name, and nothing is minted over durable records.
    #[test]
    fn a_start_that_is_neither_day_zero_nor_a_named_recovery_is_refused_by_name() {
        let ok = |env: &[(&str, &str)]| refuse_contradictory_start(&cfg(env), None);
        let fabric = FabricId::mint().to_string();
        assert_eq!(ok(&[]), Ok(()), "Day 0");
        let recovery = [("RDM_FABRIC_ID", fabric.as_str()), ("RDM_MESH_ID", MESH_ID), ("RDM_SEEDS", SEED), ("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")];
        assert_eq!(ok(&recovery), Ok(()), "a fabric recovery names everything it needs");
        // Durable records without this admin's own row: never Day 0, never a mint.
        for what in ["a Fabric record", "a mesh record", "a durable topology"] {
            let e = refuse_contradictory_start(&cfg(&[]), Some(what)).unwrap_err();
            assert!(e.contains(what) && e.contains("neither a restart nor a fresh start"), "{e}");
            assert!(refuse_contradictory_start(&cfg(&recovery), Some(what)).is_err(), "a recovery start does not override durable records either");
        }
        // A mesh or a seed without a flag is not Day 0.
        let e = ok(&[("RDM_MESH_ID", MESH_ID)]).unwrap_err();
        assert!(e.contains("RDM_MESH_ID") && e.contains("no recovery flag"), "{e}");
        assert!(ok(&[("RDM_SEEDS", SEED)]).unwrap_err().contains("RDM_SEEDS"));
        // A recovery names what it recovers: each missing input is named.
        let missing = ok(&[("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")]).unwrap_err();
        for k in ["RDM_MESH_ID", "RDM_FABRIC_ID", "RDM_SEEDS"] {
            assert!(missing.contains(k), "{missing}");
        }
        // A mesh recovery is the fabric primary's launch, not a person's.
        assert!(ok(&[("RDM_MESH_PRIMARY", "1"), ("RDM_MESH_ID", MESH_ID), ("RDM_SEEDS", SEED)]).unwrap_err().contains("started by the fabric primary"));
        // The fabric flag implies the mesh flag.
        assert!(AdminConfig::from_env(|k| match k {
            "RDM_FABRIC_PRIMARY" => Some("1".to_string()),
            "RDM_STORE_DIR" => Some(STORE_DIR.to_string()),
            _ => None,
        })
        .unwrap_err()
        .contains("RDM_FABRIC_PRIMARY is set without RDM_MESH_PRIMARY"));
    }
}
