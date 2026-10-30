//! Admin configuration, durable records state, serving door, and peer recovery.

use super::*;

/// The digest key carrying a node-admin's mesh id.

/// The environment variable configuring the hold duration (in staleness floors) before an absent
/// inherited `data_dir` on a proven-exited node is declared lost and recovered over a newly
/// allocated `data_dir` (`docs/architecture/lifecycles/node-recovery.md` "Lost data dir (`R-FM2`)",
/// default 2).
pub const ENV_DATA_DIR_GONE_FLOORS: &str = "RDM_DATA_DIR_GONE_FLOORS";

/// Default hold duration in staleness floors (`docs/architecture/lifecycles/node-recovery.md`
/// "Lost data dir (`R-FM2`)": 2 staleness floors = 60 s at 30 s).
pub const DEFAULT_DATA_DIR_GONE_FLOORS: u64 = 2;

/// Configured hold floors for an absent inherited `data_dir` (`RDM_DATA_DIR_GONE_FLOORS`, default 2,
/// `docs/architecture/lifecycles/node-recovery.md` "Lost data dir (`R-FM2`)").
pub fn data_dir_gone_floors() -> u64 {
    std::env::var(ENV_DATA_DIR_GONE_FLOORS)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(DEFAULT_DATA_DIR_GONE_FLOORS)
}

/// Configured hold duration in milliseconds (`data_dir_gone_floors() * staleness_floor()`,
/// `docs/architecture/lifecycles/node-recovery.md` "Lost data dir (`R-FM2`)").
pub fn data_dir_gone_hold_ms() -> u64 {
    data_dir_gone_floors().saturating_mul(rdm_core::cadence::staleness_floor().as_millis() as u64)
}

/// RDM's fixed pass-through list: each, when set on a node-admin, is passed on to every runtime it
/// launches ([`AdminConfig::passthrough`]). A declared secret is never one of them (Q150).
pub const PASSTHROUGH: [&str; 10] = ["RDM_EVIDENCE_DIR", "RUST_LOG", "RUST_BACKTRACE", "OTEL_EXPORTER_OTLP_ENDPOINT", "RDM_CONTAINER_SUBNET_POOL", "RDM_STALENESS_MS", "RDM_GOSSIP_INTERVAL_MS", "RDM_BACKBONE_INTERVAL_MS", crate::stopped_gc::ENV, ENV_DATA_DIR_GONE_FLOORS];

/// Everything a node-admin reads from its environment (`docs/architecture/node-admin-lifecycle.md` §2).
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// The Fabric's name (its label) and its identity.
    pub fabric: String,
    /// The fabric's minted id.
    pub fabric_id: FabricId,
    /// This admin's node id (Q208): a launched admin's `RDM_NODE_ID`; with no launcher, minted here, the
    /// bootstrap's first step, so an app reads it before it builds its telemetry and every span the
    /// process exports carries it. The start runs as this id and mints no other: a Day-0 admin's row
    /// carries it, and a start over an inherited data dir or an adoption launch is a new node with it
    /// (fabric-recovery.md).
    pub node_id: NodeId,
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
    /// The adoption launch (Q156; fabric-recovery.md "Nothing alive"): `RDM_NODE_NAME` with no launcher
    /// (no `RDM_NODE_ID`, no `RDM_LAUNCHER`), the lost fabric-primary's path.name. It adopts the fabric
    /// `RDM_FABRIC_ID` names on the store root, its mesh `RDM_MESH`, and mints no fabric or mesh id; it is
    /// a new birth with its own node.id. `None` for every other start.
    pub adopt: Option<PathName>,
    /// The admin's data directory: its own runtime material only.
    pub data_dir: PathBuf,
    /// The fabric's shared store root (`RDM_STORE_DIR`, or its launch's): its stores open under
    /// `<store_dir>/<fabric_id>/` (R14, `crate::store_root`).
    pub store_dir: PathBuf,
    /// `RDM_BIN_DIR`: where `rdm-node-admin` itself is found; never a launch source (every created
    /// node's executable is the Build's, build-deployment.md §2).
    pub bin_dir: PathBuf,
    /// `MESH_SPAWN_TYPE` as given (normalised by the fabric policy).
    pub spawn_type: Option<String>,
    /// `RDM_NODE_ADMIN_API_BIND` of a bootstrap admin: its address is the one the admin's transport
    /// binds, with port 0 (a containerised Day-0 admin binds its container's address). RDM serves no
    /// HTTP (rdm-crate-and-test-layout.md §2).
    pub api_bind: SocketAddr,
    /// Set when a deployment pipeline launched this admin.
    pub launch: Option<Launch>,
    /// Passed on to every runtime this admin launches.
    pub passthrough: BTreeMap<String, String>,
    /// `RDM_EXECUTABLE_DIRS` (proposal executable-bindings-at-create.md §2): the directories a
    /// containerized admin has mounted read-only at their host paths. The bootstrap admin reads it from
    /// its environment and a launched admin from the one its launcher wrote; this admin writes it into
    /// every node-admin it launches, and as the fabric-primary of a container fabric refuses at
    /// acceptance an executable outside them. Each entry absolute and a directory, else the start is
    /// refused by name.
    pub executable_dirs: Vec<PathBuf>,
    /// `RDM_STOPPED_NODE_TTL` (node-identity-and-state.md §6.9): a node stopped longer than it is
    /// collected by a node.delete Build the fabric-primary submits on a sweep every ttl. Read by the
    /// fabric-primary only, held by every admin (a successor sweeps with it). `None`: blank, forever.
    pub stopped_node_ttl: Option<Duration>,
    /// The bound of a `node.connection.delete { All }` (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`, its
    /// launch's when launched): this admin's own, every birth it launches carries it in its launch,
    /// and a command this admin sends that runs one is answered within it.
    pub connections_delete_deadline: Duration,
    /// The fabric's cert policy (fabric-certs.md §Tracking: `RDM_MEMBER_CERT_TTL_SECS`,
    /// `RDM_ISSUING_CERT_TTL_SECS`, `RDM_CA_CERT_TTL_SECS`, `RDM_CERT_RENEW_AT`), read once by the
    /// bootstrap admin. A launched admin's is the defaults here: it takes the fabric's from its entry
    /// pull, never from its environment.
    pub cert_policy: rdm_core::certs::CertPolicy,
    /// The `topology_storage_provider` this admin writes (storage.md §2): the registered name of the
    /// backend its five record stores open on. The bootstrap admin's from `RDM_TOPOLOGY_STORAGE_PROVIDER`,
    /// `file` when unset, and written on the Fabric row on Day 0; a launched admin's from its launch.
    pub topology_storage_provider: String,
}

impl AdminConfig {
    /// The admin's configuration from the environment `get` reads, refused by name when a variable
    /// is malformed.
    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let launch = if get(rdm_core::launch::ENV_NODE_ID).is_some() { Some(Launch::from_env(&get)?) } else { None };
        let adopt = match &launch {
            Some(_) => None,
            None => adoption_from_env(&get)?,
        };
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
        // The node id this birth runs as (Q208): a launched node's given one; with no launcher (Day 0, a
        // start over an inherited data dir, an adoption launch) a new one, minted before anything else
        // of the boot runs, its telemetry included.
        let node_id = launch.as_ref().map(|l| l.node_id.clone()).unwrap_or_else(NodeId::mint);
        let mesh = launch.as_ref().map(|l| l.name.mesh.clone()).or_else(|| get("RDM_MESH")).unwrap_or_else(|| "mesh1".into());
        if !is_valid_mesh_name(&mesh) {
            return Err(format!("RDM_MESH `{mesh}` is not a valid mesh name"));
        }
        let data_dir = launch
            .as_ref()
            .map(|l| l.data_dir.clone())
            .or_else(|| get("RDM_DATA_DIR").map(PathBuf::from))
            .unwrap_or_else(|| std::env::temp_dir().join(format!("rdm-node-admin-{}", rand::random::<u32>())));
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
        let executable_dirs = rdm_core::launch::decode_executable_dirs(&get(rdm_core::launch::ENV_EXECUTABLE_DIRS).unwrap_or_default())?;
        if let Some(d) = executable_dirs.iter().find(|d| !d.is_dir()) {
            return Err(format!("refusing to start: {} names {}, which is not a directory here", rdm_core::launch::ENV_EXECUTABLE_DIRS, d.display()));
        }
        let passthrough = PASSTHROUGH
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
        let cert_policy = match &launch {
            Some(_) => rdm_core::certs::CertPolicy::default(),
            None => rdm_core::certs::CertPolicy::from_env(&get)?,
        };
        // A launched node's is its launch's; with no launcher, the environment's (Day 0's, `file` when
        // unset), which the boot replaces with the provider its own row in its data dir names when one
        // does (storage.md §2 "The property"; `boot::own_row_provider`).
        let topology_storage_provider = match &launch {
            Some(l) => l.topology_storage_provider.clone(),
            None => rdm_core::launch::decode_topology_storage_provider(get(rdm_core::launch::ENV_TOPOLOGY_STORAGE_PROVIDER))?,
        };
        Ok(Self {
            topology_storage_provider,
            stopped_node_ttl,
            connections_delete_deadline,
            cert_policy,
            fabric,
            fabric_id,
            node_id,
            mesh_primary,
            fabric_primary,
            seeds: match &launch {
                Some(_) => Vec::new(),
                None => rdm_core::launch::decode_seeds(&get(rdm_core::launch::ENV_SEEDS).unwrap_or_default())?,
            },
            fabric_id_named: launch.is_some() || get(rdm_core::launch::ENV_FABRIC_ID).is_some_and(|v| !v.trim().is_empty()),
            adopt,
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
            executable_dirs,
        })
    }
}

/// The adoption launch `get` describes (Q156): `RDM_NODE_NAME` with no launch, which is told from
/// Day 0 by that intent alone, never by what the store holds. It names a node-admin's path.name, the
/// fabric it adopts (`RDM_FABRIC_ID`) and that path's mesh (`RDM_MESH`); a launcher, a recovery flag or
/// a missing input is refused by name. `None` when `RDM_NODE_NAME` is not given: Day 0, or a restart on
/// its own data dir.
fn adoption_from_env(get: &impl Fn(&str) -> Option<String>) -> Result<Option<PathName>, String> {
    use rdm_core::launch::{ENV_FABRIC_ID, ENV_FABRIC_PRIMARY, ENV_LAUNCHER, ENV_MESH_PRIMARY, ENV_NODE_NAME};
    let given = |k: &str| get(k).filter(|v| !v.trim().is_empty());
    let Some(name) = given(ENV_NODE_NAME) else { return Ok(None) };
    let adoption = "an adoption launch (RDM_NODE_NAME with no launcher, fabric-recovery.md \"Nothing alive\")";
    if let Some(launcher) = given(ENV_LAUNCHER) {
        return Err(format!("{ENV_LAUNCHER}={launcher} is set without RDM_NODE_ID: a launched node is handed its node id by its launcher, and {adoption} has no launcher"));
    }
    let name: PathName = name.trim().parse().map_err(|e| format!("{ENV_NODE_NAME}: {e}"))?;
    if name.kind != NodeKind::NodeAdmin {
        return Err(format!("{ENV_NODE_NAME}={name} names a {}: {adoption} is a node-admin's, at the lost fabric-primary's path.name", name.node_type()));
    }
    if given(ENV_FABRIC_ID).is_none() {
        return Err(format!("{ENV_FABRIC_ID} is required: {adoption} at {name} names the fabric it adopts, whose store root holds it; it mints none"));
    }
    match given("RDM_MESH") {
        None => return Err(format!("RDM_MESH is required: {adoption} at {name} names its mesh, {}", name.mesh)),
        Some(mesh) if mesh.trim() != name.mesh => return Err(format!("RDM_MESH={} and {ENV_NODE_NAME}={name}: {adoption} is of its path.name's mesh, {}", mesh.trim(), name.mesh)),
        Some(_) => {}
    }
    for flag in [ENV_MESH_PRIMARY, ENV_FABRIC_PRIMARY] {
        if rdm_core::launch::decode_flag(flag, get(flag))? {
            return Err(format!("{flag} is set with {ENV_NODE_NAME}={name}: {adoption} is no recovery start on a data dir; it takes its role from the recorded primaries and the restore, never from a flag"));
        }
    }
    Ok(Some(name))
}

/// The node's transport key in its data dir (minted on first use).
/// The node key in `data_dir`, minted and written there when none is: read and written on the
/// blocking pool (`blocking::blocking_io`), so no worker thread waits on the disk.
pub(crate) async fn load_or_mint_key(data_dir: &Path) -> Result<SecretKey, String> {
    let data_dir = data_dir.to_path_buf();
    crate::blocking::blocking_io(move || {
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
/// checklist's spacing): never a stamp another node reads, which is rafka-time.
pub(crate) fn local_now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

pub(crate) fn node_status(s: MemberStatus) -> NodeStatus {
    match s {
        MemberStatus::Pending => NodeStatus::Pending,
        MemberStatus::Ready => NodeStatus::Ready,
        MemberStatus::ConnectionsDeleting => NodeStatus::ConnectionsDeleting,
        MemberStatus::Leaving => NodeStatus::Leaving,
    }
}

/// What this admin's pipelines published and removed, and the meshes its
/// Builds created: merged into the topology projection.
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
    /// The records this admin writes, as it holds them (`crate::record_cache`): the one copy its
    /// status authority, the writer of its mesh's rows, its config door, its offline tick and its cert
    /// renewal start every write of a node, mesh or Fabric record from.
    pub caches: crate::record_cache::Caches,
    /// The exact births the mesh primary's offline tickle holds TRUE OFFLINE (`offline`): shown
    /// `Dead` while they stay silent. Shared with the tick's recovery followers, whose verdict rows
    /// stand only for a birth still in it (`RecoverySubmitter::offline`).
    pub offline: Arc<Mutex<std::collections::HashSet<(NodeId, IncarnationId)>>>,
    /// The births the offline tick watches through durable rows alone (`durable_births`), as its last
    /// pass held them: none unless this admin is its mesh's primary (`TenureRows`). Shared with the
    /// status door, which decides their declarations (Q11).
    pub durable: Arc<Mutex<Vec<Node>>>,
    /// The births a command of this admin's executor never reached for a reachability reason, marked
    /// `PendingReconnect` at once (node-tickle.md step 1): read by the mesh primary's tick.
    pub unreachable: crate::offline::UnreachableMarks,
    /// Per peer mesh the fabric primary gave a new node-admin (the investigation's carrier-edge-lost
    /// verdict, or the fully crashed mesh's proof): the fabric is `degraded` from that moment until
    /// each such mesh's new primary has reported its checklist.
    pub(crate) peer_recovery: Mutex<BTreeMap<String, PeerRecovery>>,
    /// The fabric-primary role was taken while the fabric was `degraded`: the new primary adopted
    /// the status and keeps it until every mesh primary reports for its checklist (`crate::checklist`).
    pub(crate) adopted_degraded: std::sync::atomic::AtomicBool,
    /// This admin, as its mesh's primary, may declare its Mesh ready: it holds no adopted status
    /// waiting for its checklist (`crate::checklist`). Withdrawn when the role ends.
    pub(crate) declare_gate: std::sync::atomic::AtomicBool,
    /// What wakes the declarer (`crate::status_declare`): poked by the topology, this admin's own
    /// digest status, the declare gate, the meshes this admin holds and a down op's receipt.
    pub wake: Arc<crate::status_declare::DeclareWake>,
    /// Poked each time the installed view changes: the hierarchy loop wakes on it to act on a
    /// role taken or a status that routing reads at once, not on its next checklist.
    pub view_changed: tokio::sync::Notify,
    /// This admin does not hold the fabric-primary role and the last fabric status published is
    /// `degraded`: it serves that status, not a ready its own (absent) authority records imply.
    pub(crate) published_degraded: std::sync::atomic::AtomicBool,
    /// The topology view whose deleted births leave these records. Set once at start.
    pub(crate) digest_view: std::sync::OnceLock<DigestView>,
    /// This admin is entering an existing mesh and has not yet been told who holds the roles: it
    /// computes none until its entry read has (ruling R-A2).
    pub(crate) entering: std::sync::atomic::AtomicBool,
    /// The fabric state this admin projects: the fabric-primary's own, the last
    /// published state on every other admin (`crate::fabric_status`).
    pub(crate) fabric_state: Mutex<crate::fabric_state::FabricState>,
    /// Moves each time the fabric state this admin projects changes ([`Self::set_fabric_state`]): what
    /// the reconcile loop waits on (its pass reads whether the fabric holds ready), apart from the
    /// declarer's wake.
    pub(crate) fabric_state_watch: tokio::sync::watch::Sender<crate::fabric_state::FabricState>,
    /// What keeps the fabric in its state, named (`None`: nothing, or not the fabric-primary).
    pub(crate) fabric_blocker: Mutex<Option<String>>,
    /// The fabric state the fabric-primary last put in a FabricStatus frame. The change enters the
    /// next state only after the current one went out, so every state is its own frame.
    pub(crate) published_fabric_state: Mutex<Option<crate::fabric_state::FabricState>>,
    /// The accepted Build's planned births per mesh: the nodes a mesh's readiness is counted over.
    /// Empty until an accepted Build is held, when the members the view holds are counted.
    pub(crate) planned: Mutex<BTreeMap<String, BTreeSet<PathName>>>,
    /// The meshes whose last heard MeshStatus is ready. A mesh's lifecycle is pending,
    /// ready, leaving, dead (states.md): a ready mesh that loses a node is not pending
    /// again, so its projection holds ready while it has a primary, and the primary that takes over
    /// adopts that state and runs its checklist (R-S3).
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
    /// Moves each time the installed view changed: what a checklist waits on to see a target reachable again.
    pub(crate) view_tick: tokio::sync::watch::Sender<u64>,
    /// Moves each time a record the projection reads changes here, at its setter (Q254: each cause
    /// raises its own event at its source): a launch record published or removed, a mesh id held, a
    /// birth held true offline or no longer, the planned births, the meshes heard ready or dead, a
    /// mesh deleted, the fabric state, a peer mesh's recovery, the adopted or published `degraded`,
    /// the entry read answered. What the view rebuild waits on beside the digest view's routing watch
    /// and the primary records the election reads ([`crate::admin::ViewWake`]); nothing is sent.
    pub(crate) inputs: tokio::sync::watch::Sender<u64>,
    /// The writer of the node rows of each mesh this admin writes for (`crate::node_rows`): the
    /// executor's claims, moves and Deleted rows, and a birth its view holds for the first time
    /// that no create of it claimed. Set once at start.
    pub(crate) rows: std::sync::OnceLock<Arc<crate::node_rows::NodeRows>>,
    /// App declarations taken at a Join while this admin wrote no rows yet (a Day-0 admin before it
    /// is Ready holds its mesh's role by no view: Q59 = x), written with its first write after it
    /// takes the role.
    pub(crate) held_declarations: Mutex<Vec<(rdm_core::MeshDigest, std::collections::BTreeSet<String>)>>,
    /// What the provider holds recorded at a path (`crate::path_investigation::PathProbe`), asked for a node the
    /// accepted topology names that no view and no durable runtime row holds a birth of. Set once at start.
    pub(crate) path_probe: std::sync::OnceLock<Arc<dyn crate::path_investigation::PathProbe>>,
    /// Where this admin's mesh's status rows live, read when this admin takes its mesh's role
    /// (`crate::status_rpc::RoleFold`). Set once at start.
    pub(crate) status: std::sync::OnceLock<crate::status_rpc::RoleFold>,
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
    /// Whether the Status door is filled.
    pub(crate) status_door_filled: Box<dyn Fn() -> bool + Send + Sync>,
    /// The name this admin booted under, for the span.
    pub(crate) boot_name: PathName,
}

impl ServingDoor {
    /// The door over the router's gate `ready` for the birth `birth`, booted as `boot_name`.
    pub fn new(ready: Arc<std::sync::atomic::AtomicBool>, birth: (NodeId, IncarnationId), status_door_filled: Box<dyn Fn() -> bool + Send + Sync>, boot_name: PathName) -> Self {
        Self { ready, birth, status_door_filled, boot_name }
    }

    /// Open the door if it is closed, the authority is filled and `view` holds this admin's exact birth.
    pub(crate) fn open_if_held(&self, view: &Topology) {
        if self.ready.load(std::sync::atomic::Ordering::SeqCst) || !(self.status_door_filled)() {
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
            self.inputs_moved();
        }
        changed
    }

    /// The writer of node rows the installed views feed (set once, at start).
    pub fn set_rows(&self, rows: Arc<crate::node_rows::NodeRows>) {
        let _ = self.rows.set(rows);
    }

    /// Record the immutable declaration through this mesh's existing node-row writer.
    pub async fn declare_app_types(&self, d: &rdm_core::MeshDigest, types: std::collections::BTreeSet<String>) -> Result<(), String> {
        let rows = self.rows.get().ok_or_else(|| "this admin holds no node-row writer".to_string())?;
        rows.declare_app_types(d, types).await
    }

    /// Record the declaration now, or hold it until this admin writes its mesh's rows (gossip.md §8,
    /// Q59 = x): a launched birth's declaration reaching a Day-0 admin before it is Ready has no row
    /// writer yet, and the Join is answered on the held declaration. `Ok(true)` written, `Ok(false)` held.
    pub async fn declare_app_types_or_hold(&self, d: &rdm_core::MeshDigest, types: std::collections::BTreeSet<String>) -> Result<bool, String> {
        let rows = self.rows.get().ok_or_else(|| "this admin holds no node-row writer".to_string())?;
        if rows.writes_now(&d.node.name.mesh, &d.node.name).await {
            rows.declare_app_types(d, types).await.map(|_| true)
        } else {
            tracing::info_span!("rdm.node_admin.node.update.via-declaration-held", node = %rows.me(), subject = %d.node.name, node_id = %d.node.node_id, types = types.len())
                .in_scope(|| tracing::info!("no writer of the mesh's rows yet: the declaration is held until this admin takes the role"));
            self.held_declarations.lock().unwrap().push((d.clone(), types));
            Ok(false)
        }
    }

    /// Write every declaration held for the role this admin now holds; one still refused stays held.
    pub async fn write_held_declarations(&self) {
        let held: Vec<_> = std::mem::take(&mut *self.held_declarations.lock().unwrap());
        for (d, types) in held {
            match self.declare_app_types(&d, types.clone()).await {
                Ok(()) => tracing::info_span!("rdm.node_admin.node.update.via-declaration-written", subject = %d.node.name, node_id = %d.node.node_id, types = types.len())
                    .in_scope(|| tracing::info!("a declaration held before the role is written beside the row")),
                Err(why) => {
                    tracing::info_span!("rdm.node_admin.node.reject.via-declaration-held", subject = %d.node.name, node_id = %d.node.node_id, detail = %why)
                        .in_scope(|| tracing::info!("the held declaration is still refused: kept"));
                    self.held_declarations.lock().unwrap().push((d, types));
                }
            }
        }
    }

    /// The probe of what the provider holds recorded at a path (set once, at start).
    pub fn set_path_probe(&self, probe: Arc<dyn crate::path_investigation::PathProbe>) {
        let _ = self.path_probe.set(probe);
    }

    /// The probe of what the provider holds recorded at a path, once set.
    pub fn path_probe(&self) -> Option<Arc<dyn crate::path_investigation::PathProbe>> {
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

    /// The facts on `mesh`'s births, on every mesh and on the fabric, which this admin (`name`, born as
    /// `me`) folds when it becomes `mesh`'s primary (set once, at start).
    pub fn set_status(&self, nodes: Arc<dyn crate::storage::NodesStorage>, meshes: Arc<dyn crate::storage::MeshStorage>, fabric: Arc<dyn crate::fabric_storage::FabricStorage>, mesh: String, name: PathName, me: NodeId) {
        let _ = self.status.set(crate::status_rpc::RoleFold::new(self.caches.clone(), nodes, meshes, fabric, mesh, name, me));
    }

    /// The tick a waiter arms on to see the view change.
    pub fn subscribe_view(&self) -> tokio::sync::watch::Receiver<u64> {
        self.view_tick.subscribe()
    }

    /// A record the projection reads changed here: the view's rebuild is due (`Self::inputs`).
    pub fn inputs_moved(&self) {
        self.inputs.send_modify(|v| *v += 1);
    }

    /// The watch a view rebuild arms on to see a record the projection reads change (`Self::inputs`).
    pub fn subscribe_inputs(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inputs.subscribe()
    }

    /// The fabric state as it changes: a waiter arms on it to see the state change.
    pub fn subscribe_fabric_state(&self) -> tokio::sync::watch::Receiver<crate::fabric_state::FabricState> {
        self.fabric_state_watch.subscribe()
    }

    /// Hold which meshes were last heard ready.
    pub fn set_heard_ready(&self, meshes: BTreeSet<String>) {
        if std::mem::replace(&mut *self.heard_ready.lock().unwrap(), meshes.clone()) != meshes {
            self.inputs_moved();
        }
    }

    /// Whether `mesh` was last heard ready.
    pub fn heard_ready(&self, mesh: &str) -> bool {
        self.heard_ready.lock().unwrap().contains(mesh)
    }

    /// This admin's own logical node, set once at start.
    pub fn set_me(&self, me: NodeId) {
        if self.me.set(me).is_ok() {
            self.inputs_moved();
        }
    }

    /// Hold the MeshStatus Dead facts heard now, each `(mesh, publisher, changed_at)`. A fact held for
    /// the first time is bound to the mesh id `id_of(mesh, publisher)` answers (the publisher's live
    /// digest); a fact bound before keeps its id; a fact no longer held is dropped (a newer status of
    /// the mesh, or its MeshLeft).
    pub fn set_heard_dead(&self, facts: Vec<(String, String, u64)>, id_of: impl Fn(&str, &str) -> Option<MeshId>) {
        let moved = {
            let mut held = self.heard_dead.lock().unwrap();
            let before = held.clone();
            held.retain(|k, _| facts.contains(k));
            for f in facts {
                let bound = held.entry(f.clone()).or_insert(None);
                if bound.is_none() {
                    *bound = id_of(&f.0, &f.1);
                }
            }
            *held != before
        };
        if moved {
            self.inputs_moved();
        }
    }

    /// Hold that this fabric-primary accepted mesh.deleted for the mesh `id` (a validated `MeshDeleted`),
    /// or read its Mesh record Dead (a fabric-primary that took over after the mesh.delete).
    pub fn hold_mesh_deleted(&self, id: &MeshId) {
        if self.deleted_meshes.lock().unwrap().insert(id.clone()) {
            self.inputs_moved();
        }
    }

    /// Drop the mesh `id` from the meshes this fabric-primary accepted mesh.deleted for (its mesh-left).
    pub fn deleted_meshes_forget(&self, id: &MeshId) {
        if self.deleted_meshes.lock().unwrap().remove(id) {
            self.inputs_moved();
        }
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

    /// Hold the fabric state; the view follows at its next projection. A change wakes the declarer and
    /// moves the watch the reconcile loop waits on.
    pub fn set_fabric_state(&self, state: crate::fabric_state::FabricState) {
        let changed = std::mem::replace(&mut *self.fabric_state.lock().unwrap(), state) != state;
        if changed {
            self.wake.poke();
            self.fabric_state_watch.send_replace(state);
            self.inputs_moved();
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

    /// Every peer mesh the fabric primary gave a new node-admin that has not yet reported its checklist.
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
        self.inputs_moved();
    }

    /// The fabric primary `me` gives `recovery.mesh` a new node-admin and, at that same moment, authors
    /// the fabric `degraded` (mesh-investigation.md step 6; R-DG1: on either arm), until that mesh's new
    /// primary reports its checklist (step 7: [`crate::investigate::restore_peer_recovery`]). The one place
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
        self.inputs_moved();
        let at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
        tracing::info_span!("rdm.node_admin.mesh.create.via-recovery", node = %me, mesh = %mesh, at, fabric_status = "degraded", verdict_rafka_ms = ?verdict_rafka_ms, verdict)
            .in_scope(|| tracing::info!("the fabric primary gives the mesh a new node-admin and authors degraded"));
        true
    }

    /// Close the recovery of `mesh`: its new primary reported its checklist.
    pub fn close_peer_recovery(&self, mesh: &str) {
        if self.peer_recovery.lock().unwrap().remove(mesh).is_some() {
            self.inputs_moved();
        }
    }

    /// Close every recovery: this admin no longer holds the fabric-primary role.
    pub fn clear_peer_recoveries(&self) {
        let held = std::mem::take(&mut *self.peer_recovery.lock().unwrap());
        if !held.is_empty() {
            self.inputs_moved();
        }
    }

    /// The fabric is `degraded` because this admin took the fabric-primary role while it was.
    pub fn adopted_degraded(&self) -> bool {
        self.adopted_degraded.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Hold (or clear) the adopted `degraded`; whether the value changed.
    pub fn set_adopted_degraded(&self, on: bool) -> bool {
        let changed = self.adopted_degraded.swap(on, std::sync::atomic::Ordering::SeqCst) != on;
        if changed {
            self.inputs_moved();
        }
        changed
    }

    /// The topology view whose deleted births leave these records (set once, at start).
    pub fn set_digest_view(&self, digest_view: DigestView) {
        let _ = self.digest_view.set(digest_view);
    }

    /// Every birth `digest_view` holds deleted leaves these records, one part of the view's read phase
    /// ([`ViewReads::read`], [`Self::install_projected`]) as of the resolver's feed
    /// (`rdm_node::node_rpc::feed_once`): gossip.md §3.1 `NodeDeleted`, "the birth is removed". A launch
    /// record or an entry sweep's record of a deleted birth never stands in for it again, after the
    /// view forgets the departure too.
    pub fn remove_deleted(&self, digest_view: &DigestView) {
        let deleted = digest_view.deleted();
        if deleted.is_empty() {
            return;
        }
        let holder = digest_view.observer_name().unwrap_or_default();
        let mut nodes = self.nodes.lock().unwrap();
        for op in deleted {
            let before = nodes.len();
            nodes.retain(|_, n| !(n.node_id == op.node_id && n.incarnation_id.as_ref() == Some(&op.incarnation)));
            if nodes.len() < before {
                tracing::info_span!("rdm.node_admin.topology.remove.via-node-deleted", node = %holder, subject = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, operation = %op.operation)
                    .in_scope(|| tracing::info!("the deleted birth's record leaves this admin's records"));
            }
        }
    }

    /// The admin has been told (or needs no telling of) who holds the roles: elections run.
    pub fn set_entering(&self, entering: bool) {
        if self.entering.swap(entering, std::sync::atomic::Ordering::SeqCst) != entering {
            self.inputs_moved();
        }
    }

    /// Whether this admin has not yet been told who holds the roles (its entry read has not answered).
    pub fn is_entering(&self) -> bool {
        self.entering.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// A admin that is not the fabric-primary serves the `degraded` status the fabric-primary published.
    pub fn published_degraded(&self) -> bool {
        self.published_degraded.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Hold (or clear) the published `degraded`.
    pub fn set_published_degraded(&self, on: bool) {
        if self.published_degraded.swap(on, std::sync::atomic::Ordering::SeqCst) != on {
            self.inputs_moved();
        }
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
    /// reader of the view. Each birth the digest view holds deleted leaves these records
    /// ([`Self::remove_deleted`]).
    pub async fn install_projected(&self, topology: &RwLock<Topology>, project: impl FnOnce() -> Topology) -> Topology {
        if let Some(digest_view) = self.digest_view.get() {
            self.remove_deleted(digest_view);
        }
        let held = topology.write().await;
        let t = project();
        self.install(held, t).await
    }

    /// [`Self::install_projected`] of the admin `me`'s projection from what the stores hold: the
    /// view's one read phase, [`ViewReads::read`], runs first over `digest_view`'s births of `fabric_id`, and
    /// only then is the view's write lock taken and `project` handed the reads: no disk read holds a
    /// reader of the view. A read older than the view it replaces is replaced at the next rebuild. A
    /// birth the view lists at a moved row's name for the first time is one span.
    pub async fn install_read(&self, me: &PathName, topology: &RwLock<Topology>, digest_view: &DigestView, fabric_id: &FabricId, project: impl FnOnce(&ViewReads) -> Topology) -> Topology {
        let me = digest_view.held_path_name().unwrap_or_else(|| me.clone());
        let reads = ViewReads::read(&me, digest_view, self, fabric_id).await;
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
            // The role of this admin's mesh taken with this view: its status rows are folded in
            // before the view is installed, so no decision reads the view against an empty record.
            if let Some(fold) = self.status.get() {
                fold.on_install(&t, &self.declared, self.digest_view.get()).await;
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
            // mesh topology from its last admin's stop on (mesh-delete.md step 7), and never a live
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
        // A publication writes no record: a mesh's members' addresses are its primary's to write
        // (`MeshRowsWriter`, the writer ladder).
        self.removed.lock().unwrap().remove(&(node.name.clone(), node.incarnation_id.clone()));
        self.nodes.lock().unwrap().insert(node.name.clone(), node);
        self.inputs_moved();
    }
    fn remove(&self, name: &PathName) {
        let gone = self.nodes.lock().unwrap().remove(name);
        self.removed.lock().unwrap().insert((name.clone(), gone.and_then(|n| n.incarnation_id)));
        self.inputs_moved();
    }
    fn remove_birth(&self, name: &PathName, node_id: &NodeId, incarnation: &IncarnationId) {
        // Only the exact birth leaves the records; another birth held at the path (the new node a
        // recovery created there) stays. The exited birth is held removed whether or not the
        // records held it, so the view does not take it back from the digest view.
        let mut nodes = self.nodes.lock().unwrap();
        if nodes.get(name).is_some_and(|n| n.node_id == *node_id && n.incarnation_id.as_ref() == Some(incarnation)) {
            nodes.remove(name);
        }
        drop(nodes);
        self.removed.lock().unwrap().insert((name.clone(), Some(incarnation.clone())));
        self.inputs_moved();
    }
    fn node_rows(&self) -> Option<Arc<crate::node_rows::NodeRows>> {
        self.rows.get().cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: docs/architecture/lifecycles/fabric-recovery.md:172 "No node-admin is alive, the fabric's other nodes alive or not. Nothing inside the fabric brings an admin back: a person or a script launches a node-admin at the lost fabric-primary's path.name, with no launcher, given the fabric id, its mesh and the store root, which must hold that fabric or the start is refused by name. It adopts what the store holds and mints no fabric or mesh id; it is a new birth with its own node.id."
    #[test]
    fn adoption_launch_from_env_parses_lost_admin_path_and_refuses_invalid_inputs() {
        let env_of = |pairs: &[(&str, &str)]| {
            let map: BTreeMap<String, String> = pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect();
            move |k: &str| map.get(k).cloned()
        };

        // Day 0 (no RDM_NODE_NAME): adopt is None, fabric_id and node_id are minted.
        let day0 = AdminConfig::from_env(env_of(&[("RDM_STORE_DIR", "/tmp/rdm-store")]))
            .expect("Day-0 config succeeds");
        assert!(day0.adopt.is_none());
        assert!(!day0.fabric_id_named);

        // Valid adoption launch at mesh1.admin.1: adopts RDM_FABRIC_ID and RDM_MESH, mints no mesh_id, mints its own node_id.
        let adopted = AdminConfig::from_env(env_of(&[
            ("RDM_NODE_NAME", "mesh1.admin.1"),
            ("RDM_FABRIC_ID", "fab000000001"),
            ("RDM_MESH", "mesh1"),
            ("RDM_STORE_DIR", "/tmp/rdm-store"),
        ]))
        .expect("valid adoption launch config succeeds");
        assert_eq!(adopted.adopt, Some("mesh1.admin.1".parse().unwrap()));
        assert_eq!(adopted.fabric_id, FabricId::parse("fab000000001").unwrap());
        assert!(adopted.fabric_id_named);
        assert_eq!(adopted.mesh, "mesh1");
        assert!(adopted.mesh_id.is_none());
        assert_ne!(adopted.node_id, day0.node_id);

        // Refused when RDM_LAUNCHER is set without RDM_NODE_ID.
        let err = AdminConfig::from_env(env_of(&[
            ("RDM_NODE_NAME", "mesh1.admin.1"),
            ("RDM_LAUNCHER", "mesh1.admin.2"),
            ("RDM_FABRIC_ID", "fab000000001"),
            ("RDM_MESH", "mesh1"),
            ("RDM_STORE_DIR", "/tmp/rdm-store"),
        ]))
        .unwrap_err();
        assert!(err.contains("RDM_LAUNCHER=mesh1.admin.2 is set without RDM_NODE_ID"), "{err}");

        // Refused when RDM_NODE_NAME is not a node-admin.
        let err = AdminConfig::from_env(env_of(&[
            ("RDM_NODE_NAME", "mesh1.rpc.1"),
            ("RDM_FABRIC_ID", "fab000000001"),
            ("RDM_MESH", "mesh1"),
            ("RDM_STORE_DIR", "/tmp/rdm-store"),
        ]))
        .unwrap_err();
        assert!(err.contains("names a rpc-node"), "{err}");

        // Refused when RDM_FABRIC_ID is missing.
        let err = AdminConfig::from_env(env_of(&[
            ("RDM_NODE_NAME", "mesh1.admin.1"),
            ("RDM_MESH", "mesh1"),
            ("RDM_STORE_DIR", "/tmp/rdm-store"),
        ]))
        .unwrap_err();
        assert!(err.contains("RDM_FABRIC_ID is required"), "{err}");

        // Refused when RDM_MESH is missing or differs from RDM_NODE_NAME's mesh.
        let err = AdminConfig::from_env(env_of(&[
            ("RDM_NODE_NAME", "mesh1.admin.1"),
            ("RDM_FABRIC_ID", "fab000000001"),
            ("RDM_MESH", "mesh2"),
            ("RDM_STORE_DIR", "/tmp/rdm-store"),
        ]))
        .unwrap_err();
        assert!(err.contains("RDM_MESH=mesh2 and RDM_NODE_NAME=mesh1.admin.1"), "{err}");

        // Refused when RDM_MESH_PRIMARY or RDM_FABRIC_PRIMARY is set with RDM_NODE_NAME.
        for flag in ["RDM_MESH_PRIMARY", "RDM_FABRIC_PRIMARY"] {
            let err = AdminConfig::from_env(env_of(&[
                ("RDM_NODE_NAME", "mesh1.admin.1"),
                ("RDM_FABRIC_ID", "fab000000001"),
                ("RDM_MESH", "mesh1"),
                ("RDM_STORE_DIR", "/tmp/rdm-store"),
                (flag, "1"),
            ]))
            .unwrap_err();
            assert!(err.contains(&format!("{flag} is set with RDM_NODE_NAME=mesh1.admin.1")), "{err}");
        }
    }
}


