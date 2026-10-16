//! Admin configuration, durable records state, serving door, and peer recovery.

use super::*;

/// The digest key carrying a node-admin's mesh id.

/// The environment variable configuring the hold duration (in staleness floors) before a
/// `held-running` member or mesh-primary is terminated and recovered (`R-FM1`, default 10).
pub const ENV_HUNG_HOLD_FLOORS: &str = "RDM_HUNG_HOLD_FLOORS";

/// Default hold duration in staleness floors (`R-FM1`: 10 staleness floors = 5 min at 30 s).
pub const DEFAULT_HUNG_HOLD_FLOORS: u64 = 10;

/// Configured hold floors for `held-running` (`RDM_HUNG_HOLD_FLOORS`, default 10).
pub fn hung_hold_floors() -> u64 {
    std::env::var(ENV_HUNG_HOLD_FLOORS)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(DEFAULT_HUNG_HOLD_FLOORS)
}

/// Configured hold duration in milliseconds (`hung_hold_floors() * staleness_floor()`).
pub fn hung_hold_ms() -> u64 {
    hung_hold_floors().saturating_mul(rdm_core::cadence::staleness_floor().as_millis() as u64)
}

/// The environment variable configuring the hold duration (in staleness floors) before an absent
/// inherited `data_dir` on a proven-exited member is declared lost and recovered over a newly
/// allocated `data_dir` (`R-FM2`, default 2).
pub const ENV_DATA_DIR_GONE_FLOORS: &str = "RDM_DATA_DIR_GONE_FLOORS";

/// Default hold duration in staleness floors (`R-FM2`: 2 staleness floors = 60 s at 30 s).
pub const DEFAULT_DATA_DIR_GONE_FLOORS: u64 = 2;

/// Configured hold floors for an absent inherited `data_dir` (`RDM_DATA_DIR_GONE_FLOORS`, default 2).
pub fn data_dir_gone_floors() -> u64 {
    std::env::var(ENV_DATA_DIR_GONE_FLOORS)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(DEFAULT_DATA_DIR_GONE_FLOORS)
}

/// Configured hold duration in milliseconds (`data_dir_gone_floors() * staleness_floor()`).
pub fn data_dir_gone_hold_ms() -> u64 {
    data_dir_gone_floors().saturating_mul(rdm_core::cadence::staleness_floor().as_millis() as u64)
}

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
    pub bindings: Option<rdm_core::binding::Validated>,
    /// The two variables that named them, handed on to every node-admin this admin launches.
    pub bindings_env: BTreeMap<String, String>,
    /// `RDM_STOPPED_NODE_TTL` (node-identity-and-state.md §6.9): a node stopped longer than it is
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
        let launch = if get(rdm_core::launch::ENV_NODE_ID).is_some() { Some(Launch::from_env(&get)?) } else { None };
        let fabric = launch.as_ref().map(|l| l.fabric.clone()).or_else(|| get("RDM_FABRIC")).unwrap_or_else(|| "fabric1".into());
        // A launched node is handed its Fabric's id; the bootstrap admin takes
        // `RDM_FABRIC_ID` when given, else its Fabric is new and it mints one.
        let fabric_id = match &launch {
            Some(l) => l.fabric_id.clone(),
            None => match get(rdm_core::launch::ENV_FABRIC_ID).filter(|v| !v.trim().is_empty()) {
                Some(v) => FabricId::parse(&v).map_err(|e| format!("{}: {e}", rdm_core::launch::ENV_FABRIC_ID))?,
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
            None => get(rdm_core::launch::ENV_STORE_DIR).filter(|v| !v.trim().is_empty()).map(PathBuf::from).ok_or_else(|| {
                format!("{} is required: the fabric's shared store root every node of the fabric reaches; nothing falls back to the data dir", rdm_core::launch::ENV_STORE_DIR)
            })?,
        };
        let bin_dir = get("RDM_BIN_DIR").map(PathBuf::from).unwrap_or_else(|| {
            std::env::current_exe().ok().and_then(|e| e.parent().map(Path::to_path_buf)).unwrap_or_else(|| PathBuf::from("."))
        });
        let api_bind = get("RDM_NODE_ADMIN_API_BIND")
            .map(|b| b.parse().map_err(|e| format!("RDM_NODE_ADMIN_API_BIND `{b}`: {e}")))
            .transpose()?
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0)));
        let bindings_env: BTreeMap<String, String> = [rdm_core::binding::ENV_EXECUTABLE_BINDINGS, rdm_core::binding::ENV_EXECUTABLE_CANDIDATE]
            .iter()
            .filter_map(|k| get(k).filter(|v| !v.trim().is_empty()).map(|v| (k.to_string(), v)))
            .collect();
        let bindings = match get(rdm_core::binding::ENV_EXECUTABLE_BINDINGS).filter(|v| !v.trim().is_empty()) {
            None => None,
            Some(file) => {
                let candidate = get(rdm_core::binding::ENV_EXECUTABLE_CANDIDATE)
                    .filter(|v| !v.trim().is_empty())
                    .ok_or_else(|| format!("{} names a binding file but {} (the candidate sha this run is of) is not set", rdm_core::binding::ENV_EXECUTABLE_BINDINGS, rdm_core::binding::ENV_EXECUTABLE_CANDIDATE))?;
                let set = rdm_core::binding::BindingSet::load(Path::new(&file)).map_err(|e| e.to_string())?;
                let expect = rdm_core::binding::Expect { candidate_sha: &candidate, required: &[], provider_image: rdm_core::binding::ProviderImage::Unchecked };
                // The bootstrap admin hashes every bound file before any provider opens; an admin
                // another admin launched checks all but the hashes (its launch re-hashed its own
                // executable; each later launch re-hashes the one it runs).
                let validated = if launch.is_some() { set.validate_launched(&expect) } else { set.validate(&expect) };
                Some(validated.map_err(|e| format!("refusing to start: explicit executable bindings: {e}"))?)
            }
        };
        let passthrough = ["RDM_EVIDENCE_DIR", "RUST_LOG", "OTEL_EXPORTER_OTLP_ENDPOINT", "RDM_CONTAINER_SUBNET_POOL", "RDM_STALENESS_MS", "RDM_GOSSIP_INTERVAL_MS", "RDM_BACKBONE_INTERVAL_MS", crate::stopped_gc::ENV, ENV_HUNG_HOLD_FLOORS, ENV_DATA_DIR_GONE_FLOORS]
            .iter()
            .filter_map(|k| get(k).map(|v| (k.to_string(), v)))
            .collect();
        let (mesh_primary, fabric_primary) = match &launch {
            Some(_) => (false, false),
            None => (
                rdm_core::launch::decode_flag(rdm_core::launch::ENV_MESH_PRIMARY, get(rdm_core::launch::ENV_MESH_PRIMARY))?,
                rdm_core::launch::decode_flag(rdm_core::launch::ENV_FABRIC_PRIMARY, get(rdm_core::launch::ENV_FABRIC_PRIMARY))?,
            ),
        };
        if fabric_primary && !mesh_primary {
            return Err(format!(
                "{} is set without {}: a fabric recovery starts the primary node-admin of its own mesh as well (node-admin-lifecycle.md §3)",
                rdm_core::launch::ENV_FABRIC_PRIMARY,
                rdm_core::launch::ENV_MESH_PRIMARY
            ));
        }
        let stopped_node_ttl = crate::stopped_gc::ttl_from_env(get(crate::stopped_gc::ENV).as_deref())?;
        let connections_delete_deadline = match &launch {
            Some(l) => l.connections_delete_deadline,
            None => rdm_core::launch::decode_connections_delete_deadline(get(rdm_core::launch::ENV_CONNECTIONS_DELETE_DEADLINE).as_deref())?,
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
                None => rdm_core::launch::decode_seeds(&get(rdm_core::launch::ENV_SEEDS).unwrap_or_default())?,
            },
            fabric_id_named: launch.is_some() || get(rdm_core::launch::ENV_FABRIC_ID).is_some_and(|v| !v.trim().is_empty()),
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
pub(crate) fn local_now_ms() -> u64 {
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
    pub(crate) nodes: Mutex<BTreeMap<PathName, Node>>,
    pub(crate) removed: Mutex<std::collections::HashSet<(PathName, Option<IncarnationId>)>>,
    pub(crate) meshes: Mutex<BTreeMap<String, MeshId>>,
    /// nodes.storage on the fabric's shared store root: the durable runtime rows a birth this admin
    /// never heard is adopted from, and the node rows the view reads a birth's name from when the
    /// birth does not publish it ([`RowNames`]).
    pub(crate) contacts: std::sync::OnceLock<Arc<dyn crate::storage::NodesStorage>>,
    /// The lifecycle states this admin applied as an authority (`status_rpc`): the view's
    /// `declared`, never its `status`.
    pub declared: Arc<Mutex<crate::status_rpc::Declared>>,
    /// The exact births the mesh primary's offline tickle holds TRUE OFFLINE (`offline`): shown
    /// `Dead` while they stay silent.
    pub offline: Mutex<std::collections::HashSet<(NodeId, IncarnationId)>>,
    /// The births a command of this admin's executor never reached for a reachability reason, marked
    /// `PendingReconnect` at once (node-tickle.md step 1): read by the mesh primary's tick.
    pub unreachable: crate::offline::UnreachableMarks,
    /// Per peer mesh the fabric primary gave a new node-admin (the investigation's carrier-edge-lost
    /// verdict, or the fully crashed mesh's proof): the fabric is `degraded` from that moment until
    /// each such mesh's new primary has reported its round.
    pub(crate) peer_recovery: Mutex<BTreeMap<String, PeerRecovery>>,
    /// The fabric-primary role was taken while the fabric was `degraded`: the new holder adopted
    /// the status and keeps it until every mesh primary reports for its round (`crate::round`).
    pub(crate) adopted_degraded: std::sync::atomic::AtomicBool,
    /// This admin, as its mesh's primary, may declare its Mesh ready: it holds no adopted status
    /// waiting for its round (`crate::round`). Withdrawn when the role ends.
    pub(crate) declare_gate: std::sync::atomic::AtomicBool,
    /// What wakes the declarer (`crate::status_declare`): poked by the topology, this admin's own
    /// digest status, the declare gate, the meshes this admin holds and a down op's receipt.
    pub wake: Arc<crate::status_declare::DeclareWake>,
    /// Poked each time the installed view changes: the hierarchy loop wakes on it to act on a
    /// seat taken or a status that routing reads at once, not on its next round.
    pub view_changed: tokio::sync::Notify,
    /// This admin does not hold the fabric-primary role and the last fabric status published is
    /// `degraded`: it serves that status, not a ready its own (absent) authority records imply.
    pub(crate) published_degraded: std::sync::atomic::AtomicBool,
    /// The exact births whose exit-proof row (`status/<mesh>/exits/`, written by whichever admin
    /// proved the exit from the provider) the installing projection read: each birth the projection
    /// holds and each seat holder the book records, a point read of its key at every install, never
    /// fed by a prover. Shown `Dead` whatever their last digest said, and the proof an election takes
    /// that a seat's holder is gone (fabric-node-lifecycle-elections.md §4.1).
    pub(crate) exited: Mutex<std::collections::HashSet<(NodeId, IncarnationId)>>,
    /// The membership book whose seat records name holders the exit read covers beside the view's
    /// births (a holder the view no longer lists). Set once at start.
    pub(crate) book: std::sync::OnceLock<DigestBook>,
    /// This admin is entering an existing mesh and has not yet been told who holds the seats: it
    /// computes none until its entry read has (ruling R-A2).
    pub(crate) entering: std::sync::atomic::AtomicBool,
    /// The fabric state this admin projects: authoritative on the fabric-primary, the last
    /// published state on every other admin (`crate::fabric_rounds`).
    pub(crate) fabric_state: Mutex<crate::fabric_state::FabricState>,
    /// Whether the fabric this admin projects has first reached ready-for-traffic: set by the first
    /// `set_fabric_state(ReadyForTraffic)` and never cleared (states.md, "The initial fabric before
    /// ready-for-traffic"). The control plane and the status authority read this one handle.
    pub first_ready: crate::fabric_state::FirstReady,
    /// What keeps the fabric in its state, named (`None`: nothing, or not the fabric-primary).
    pub(crate) fabric_blocker: Mutex<Option<String>>,
    /// The fabric state the fabric-primary last put in a FabricStatus frame. The climb enters the
    /// next state only after the current one went out, so every state is its own frame.
    pub(crate) published_fabric_state: Mutex<Option<crate::fabric_state::FabricState>>,
    /// The accepted Build's planned births per mesh: the nodes a mesh's readiness is counted over.
    /// Empty until an accepted Build is held, when the members the view holds are counted.
    pub(crate) planned: Mutex<BTreeMap<String, BTreeSet<PathName>>>,
    /// The meshes whose last heard MeshStatus is ready-for-traffic. A mesh's lifecycle is pending,
    /// ready-for-traffic, leaving, dead (states.md): a ready mesh that loses a node is not pending
    /// again, so its projection holds ready while it has a primary, and the primary that takes over
    /// adopts that state and runs its round (R-S3).
    pub(crate) heard_ready: Mutex<BTreeSet<String>>,
    /// Each MeshStatus Dead held, by its fact (mesh name, publisher, changed_at), bound to the mesh id
    /// it is about: the id the publisher's own digest names when the fact is first held. Dead is a
    /// mesh id's, never a name's (states.md: an id is never reissued; a name is, R-MN1). After
    /// mesh.deleted the last admin, still its primary, publishes MeshStatus Dead (R-NS1 item 1).
    pub(crate) heard_dead: Mutex<BTreeMap<(String, String, u64), Option<MeshId>>>,
    /// The meshes this admin, as fabric-primary, accepted mesh.deleted for: a validated `MeshDeleted`,
    /// which the mesh-primary calls only after it committed the mesh row Dead (node-rpc-envelope.md,
    /// mesh delete command pair). Dead in this admin's projection from that acceptance on, without
    /// waiting for the MeshStatus Dead gossip; dropped with the mesh's id at mesh-left.
    pub(crate) deleted_meshes: Mutex<std::collections::HashSet<MeshId>>,
    /// This admin's own logical node, set once at start: the projection knows the mesh it authors
    /// the status of (a mesh primary restates no heard status as its own, gossip.md MeshStatus).
    pub(crate) me: std::sync::OnceLock<NodeId>,
    /// Moves each time the installed view changed: what a round waits on to see a target reachable again.
    pub(crate) view_tick: tokio::sync::watch::Sender<u64>,
    /// The writer of the node rows of each mesh this admin writes for (`crate::node_rows`): the
    /// executor's claims, moves and Deleted rows, and a birth its view holds for the first time
    /// that no create of it claimed. Set once at start.
    pub(crate) rows: std::sync::OnceLock<Arc<crate::node_rows::NodeRows>>,
    /// What the provider holds recorded at a path (`crate::seat_revision::PathProbe`), asked for a node the
    /// accepted topology names that no view and no durable runtime row holds a birth of. Set once at start.
    pub(crate) path_probe: std::sync::OnceLock<Arc<dyn crate::seat_revision::PathProbe>>,
    /// Where this admin's mesh's status rows live, read when this admin takes its mesh's seat
    /// (`crate::status_rpc::SeatFold`). Set once at start.
    pub(crate) status: std::sync::OnceLock<crate::status_rpc::SeatFold>,
    /// This admin's Node RPC door, opened by an installed view ([`ServingDoor`]). Set once at start.
    pub(crate) door: std::sync::OnceLock<ServingDoor>,
    /// This admin's place on the backbone: it holds the meshes the installed view names, each deleted
    /// or not, which its publication of a deleted mesh reads ([`Backbone::publishes_deleted_mesh_of`]).
    /// Set once at start.
    pub(crate) backbone: std::sync::OnceLock<Backbone>,
}

/// An admin's Node RPC readiness gate (node-rpc-envelope.md, "Node RPC readiness gate": "The router
/// refuses every op, Ping included, with typed NotReady until the Node RPC door is ready. A node-admin
/// waits until its Status authority is filled and its view holds it"): it opens once the authority is
/// filled and a view this admin installs holds its exact birth, under whatever name it holds now (its
/// join answer hands it its row's name, R-JN1), never by the name it booted under.
pub struct ServingDoor {
    /// The router's gate.
    pub(crate) ready: Arc<std::sync::atomic::AtomicBool>,
    /// This admin's exact birth.
    pub(crate) birth: (NodeId, IncarnationId),
    /// Whether the Status authority is filled.
    pub(crate) authority_filled: Box<dyn Fn() -> bool + Send + Sync>,
    /// The name this admin booted under, for the span.
    pub(crate) boot_name: PathName,
}

impl ServingDoor {
    /// The door over the router's gate `ready` for the birth `birth`, booted as `boot_name`.
    pub fn new(ready: Arc<std::sync::atomic::AtomicBool>, birth: (NodeId, IncarnationId), authority_filled: Box<dyn Fn() -> bool + Send + Sync>, boot_name: PathName) -> Self {
        Self { ready, birth, authority_filled, boot_name }
    }

    /// Open the door if it is closed, the authority is filled and `view` holds this admin's exact birth.
    pub(crate) fn open_if_held(&self, view: &Topology) {
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

/// A peer mesh the fabric primary gave a new node-admin: its carrier-edge-lost verdict's recovery
/// (`crate::investigate`), or a fully crashed mesh's create on the provider's proof of every node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRecovery {
    /// The mesh's name.
    pub mesh: String,
    /// The Rafka-time of the ladder's decision; `None` for a fully crashed mesh, which has no ladder
    /// verdict (its create is admitted on the provider's proof).
    pub verdict_rafka_ms: Option<u64>,
    /// When this admin took the decision: a report counts as the mesh's proof only if it arrived after.
    pub opened_at: std::time::Instant,
    /// The node-admin births of the mesh the decision found.
    pub lost: Vec<IncarnationId>,
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

    /// The probe of what the provider holds recorded at a path (set once, at start).
    pub fn set_path_probe(&self, probe: Arc<dyn crate::seat_revision::PathProbe>) {
        let _ = self.path_probe.set(probe);
    }

    /// The probe of what the provider holds recorded at a path, once set.
    pub fn path_probe(&self) -> Option<Arc<dyn crate::seat_revision::PathProbe>> {
        self.path_probe.get().cloned()
    }

    /// This admin's Node RPC door (set once, at start): every view installed from now on opens it
    /// once it holds the birth, and `installed`, the view in place when it is set, is checked at once.
    pub fn set_door(&self, door: ServingDoor, installed: &Topology) {
        door.open_if_held(installed);
        let _ = self.door.set(door);
    }

    /// This admin's place on the backbone (set once, at start): it holds the meshes `installed`, the
    /// view in place when it is set, names, each deleted or not.
    pub fn set_backbone(&self, backbone: Backbone, _installed: &Topology) {
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
    pub async fn put_ready_row(&self, fabric_id: &FabricId) -> Result<rdm_core::status::StatusReply, String> {
        let Some(fold) = self.status.get() else {
            return Err(format!("no status store is held to put the ready-for-traffic row of fabric {fabric_id}"));
        };
        let ready = crate::status_rpc::event_key(&rdm_core::status::FabricEvent::ReadyForTraffic);
        match crate::status_rpc::commit_fabric_event(&**fold.storage(), &self.declared, fabric_id, &ready).await {
            r @ (rdm_core::status::StatusReply::Applied | rdm_core::status::StatusReply::AlreadyApplied) => {
                self.first_ready.reach("ready-for-traffic");
                Ok(r)
            }
            rdm_core::status::StatusReply::NotReady { reason } => Err(format!("the status rows of mesh {} on the store root: {reason}", fold.mesh())),
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

    /// Hold that this fabric-primary accepted mesh.deleted for the mesh `id` (a validated `MeshDeleted`).
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

    /// Every peer mesh the fabric primary gave a new node-admin that has not yet reported its round.
    pub fn peer_recoveries(&self) -> Vec<PeerRecovery> {
        self.peer_recovery.lock().unwrap().values().cloned().collect()
    }

    /// The recovery open for `mesh`, if one is.
    pub fn peer_recovery_of(&self, mesh: &str) -> Option<PeerRecovery> {
        self.peer_recovery.lock().unwrap().get(mesh).cloned()
    }

    /// Whether any peer mesh is being recovered: the fabric is `degraded` while one is.
    pub fn peer_recovering(&self) -> bool {
        !self.peer_recovery.lock().unwrap().is_empty()
    }

    /// Open the recovery of `recovery.mesh`: the fabric primary gave it a new node-admin. A recovery
    /// already open for that mesh is replaced by this one; another mesh's is left as it is.
    pub fn open_peer_recovery(&self, recovery: PeerRecovery) {
        self.peer_recovery.lock().unwrap().insert(recovery.mesh.clone(), recovery);
    }

    /// The fabric primary `me` gives `recovery.mesh` a new node-admin and, at that same moment, authors
    /// the fabric `degraded` (mesh-investigation.md step 6; R-DG1: on either arm), until that mesh's new
    /// primary reports its round (step 7: [`crate::investigate::restore_peer_recovery`]). The one place
    /// the recovery is opened and the one span that says so, `verdict` naming the arm:
    /// `carrier-edge-lost` (the ladder's verdict, `replace`: a new decision replaces an earlier one's
    /// recovery of the mesh) or `fully-crashed` (the create admitted on the provider's proof, which keeps
    /// the recovery already open for the mesh: a later claim must not move `opened_at` past the report
    /// it waits for). Answers whether it opened one.
    pub fn open_mesh_recovery(&self, me: &PathName, recovery: PeerRecovery, verdict: &'static str, replace: bool) -> bool {
        let (mesh, verdict_rafka_ms) = (recovery.mesh.clone(), recovery.verdict_rafka_ms);
        {
            let mut open = self.peer_recovery.lock().unwrap();
            if !replace && open.contains_key(&mesh) {
                return false;
            }
            open.insert(mesh.clone(), recovery);
        }
        let at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
        tracing::info_span!("rdm.node_admin.mesh.create.via-recovery", node = %me, mesh = %mesh, at, fabric_status = "degraded", verdict_rafka_ms = ?verdict_rafka_ms, verdict)
            .in_scope(|| tracing::info!("the fabric primary gives the mesh a new node-admin and authors degraded"));
        true
    }

    /// Close the recovery of `mesh`: its new primary reported its round.
    pub fn close_peer_recovery(&self, mesh: &str) {
        self.peer_recovery.lock().unwrap().remove(mesh);
    }

    /// Close every recovery: this admin no longer holds the fabric-primary role.
    pub fn clear_peer_recoveries(&self) {
        self.peer_recovery.lock().unwrap().clear();
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
        self.wake.poke();
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

    /// Record a `held-running` verdict for `node` (`R-FM1`): on first verdict sets `since = now_ms`
    /// and `verdicts = 1`; on subsequent verdicts preserves `since` and increments `verdicts`.
    pub async fn record_held_running(&self, node: &Node, now_ms: u64, recorder: &str) -> Option<crate::status_storage::NodeHeldRunningRow> {
        let storage = self.status.get()?.storage().clone();
        let inc = node.incarnation_id.clone()?;
        let prev = storage.node_held_running_of(&node.mesh, &node.node_id, &inc).await.ok().flatten();
        let (since, verdicts) = match prev {
            Some(r) => (r.since, r.verdicts.saturating_add(1)),
            None => (now_ms, 1),
        };
        let row = crate::status_storage::NodeHeldRunningRow { node_id: node.node_id.clone(), name: node.name.clone(), incarnation_id: inc, since, verdicts };
        let _ = storage.put_node_held_running_of(&node.mesh, &row).await;
        tracing::info_span!(
            "rdm.node_admin.node.update.via-held-running",
            node = %node.name,
            node_id = %node.node_id,
            incarnation_id = %row.incarnation_id.as_str(),
            recorder,
            since = row.since as i64,
            verdicts = row.verdicts as i64,
            outcome = "recorded",
            "otel.kind" = "internal",
        )
        .in_scope(|| tracing::info!("recorded held-running verdict"));
        Some(row)
    }

    /// Clear any `held-running` row for `node` (`R-FM1`).
    pub async fn clear_held_running(&self, node: &Node) {
        let Some(storage) = self.status.get().map(|f| f.storage().clone()) else { return };
        let Some(inc) = node.incarnation_id.as_ref() else { return };
        let _ = storage.remove_node_held_running_of(&node.mesh, &node.node_id, inc).await;
    }

    /// Read the durable `held-running` row of `node` among its mesh's status rows (`R-FM1`).
    pub async fn held_running_row_of(&self, node: &Node) -> Result<Option<crate::status_storage::NodeHeldRunningRow>, String> {
        let Some(inc) = node.incarnation_id.as_ref() else { return Ok(None) };
        let Some(status) = self.status.get() else { return Ok(None) };
        status.storage().node_held_running_of(&node.mesh, &node.node_id, inc).await.map_err(|e| e.to_string())
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
    pub(crate) fn exited_incarnations(&self) -> std::collections::HashSet<IncarnationId> {
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
        let me = book.held_path_name().unwrap_or_else(|| me.clone());
        let reads = ViewReads::read(&me, book, self, fabric_id).await;
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
    pub(crate) async fn install(&self, mut held: tokio::sync::RwLockWriteGuard<'_, Topology>, t: Topology) -> Topology {
        if let Some(door) = self.door.get() {
            door.open_if_held(&t);
        }
        if *held != t {
            // The seat of this admin's mesh taken with this view: its status rows are folded in
            // before the view is installed, so no decision reads the view against an empty record.
            if let Some(fold) = self.status.get() {
                fold.on_install(&t, &self.declared, self.book.get()).await;
            }
            // A renamed predecessor this view now names `<path>.old` (its row's name; its digest still
            // says `<path>`): one span as it enters.
            for old in t.predecessors().filter(|o| !held.predecessors().any(|h| h.node_id == o.node_id && h.incarnation_id == o.incarnation_id)) {
                tracing::info_span!("rdm.node_admin.topology.update.via-predecessor-held", node = %old.name, node_id = %old.node_id, status = ?old.status)
                    .in_scope(|| tracing::info!("the view names a renamed predecessor under its .old path"));
            }
            // The births the fabric-primary's view holds for the first time: their rows are written
            // from this held record once the view is installed (`crate::node_rows`).
            let arrivals = self.rows.get().map(|rows| rows.arrivals(&held, &t)).unwrap_or_default();
            // The backbone holds the meshes this view names, each deleted or not, from this install on
            // and in the order views are installed: the fabric-primary publishes a deleted mesh's
            // aggregate from its last admin's stop on (mesh-delete.md step 7), and never a live
            // mesh's (R-RX4). Its own mesh.deleted receipt is one such install
            // (`admin::mesh_delete::record_mesh_deleted`), so a stop that follows it reads the mesh deleted.
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
    fn node_rows(&self) -> Option<Arc<crate::node_rows::NodeRows>> {
        self.rows.get().cloned()
    }
}
