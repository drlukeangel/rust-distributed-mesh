//! The node-admin runtime (`rafka-node-admin`; `docs/i143/design.md` §3–§4).
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
use crate::build_state::BuildStateAdapter;
use crate::deployment::endpoint::{spec_for, EndpointAllocator};
use crate::accepted::AcceptedStore;
use crate::deployment::pipeline::{adoption_missing, CurrentRuntimeAdoption, Publication, 
    CreateRequest, DeploymentPipeline, LaunchTemplate, NodeObserver, RetireRequest, Timeouts, TopologySink,
};
use crate::deployment::provider::{DeploymentHandle, DeploymentProvider, FabricPolicy, TerminationMode};
use crate::executor::{BuildExecutor, OperationRunner};
use crate::election::ElectionLog;
use crate::fabric_builds::FabricBuildStateAdapter;
use crate::http::{router, ControlPlane};
use crate::lifecycle::{
    HookRegistry, LifecycleScope, LifecycleState, LifecycleTransitionPipeline, MemoryReceiptLog, ShapeFacts, Transition,
    TransitionKey, TransitionResult,
};
use crate::model::*;
use crate::topology::Topology;
use iroh::protocol::Router as IrohRouter;
use iroh::{Endpoint, EndpointAddr, SecretKey};
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::{EndpointSet, MemberStatus, MeshDigest, MeshNode};
use rafka_mesh_transport::entry::{EntryAnswer, EntryServer, ENTRY_ALPN};
use rafka_mesh_transport::membership::{announce_leaving, leave_linger_from_env, Backbone, DigestBook, Membership, LEAVE_EVERY, SILENT_AFTER};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

/// The digest key carrying a node-admin's mesh id.
pub const MESH_ID: &str = "mesh_id";

/// Everything a node-admin reads from its environment.
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// The Fabric's name (its label) and its identity.
    pub fabric: String,
    pub fabric_id: FabricId,
    pub mesh: String,
    pub mesh_id: Option<MeshId>,
    pub data_dir: PathBuf,
    pub bin_dir: PathBuf,
    /// `MESH_SPAWN_TYPE` as given (normalised by the fabric policy).
    pub spawn_type: Option<String>,
    /// The HTTP bind of a bootstrap admin (`RAFKA_NODE_ADMIN_API_BIND`).
    pub api_bind: SocketAddr,
    /// Set when a deployment pipeline launched this admin.
    pub launch: Option<Launch>,
    /// Passed on to every runtime this admin launches.
    pub passthrough: BTreeMap<String, String>,
}

impl AdminConfig {
    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let launch = if get(rafka_mesh_entity::launch::ENV_NODE_ID).is_some() { Some(Launch::from_env(&get)?) } else { None };
        let fabric = launch.as_ref().map(|l| l.fabric.clone()).or_else(|| get("RAFKA_FABRIC")).unwrap_or_else(|| "fabric1".into());
        // A launched node is handed its Fabric's id; the bootstrap admin takes
        // `RAFKA_FABRIC_ID` when given, else its Fabric is new and it mints one.
        let fabric_id = match &launch {
            Some(l) => l.fabric_id.clone(),
            None => match get(rafka_mesh_entity::launch::ENV_FABRIC_ID).filter(|v| !v.trim().is_empty()) {
                Some(v) => FabricId::parse(&v).map_err(|e| format!("{}: {e}", rafka_mesh_entity::launch::ENV_FABRIC_ID))?,
                None => FabricId::mint(),
            },
        };
        let mesh = launch.as_ref().map(|l| l.name.mesh.clone()).or_else(|| get("RAFKA_MESH")).unwrap_or_else(|| "mesh1".into());
        if !is_valid_mesh_name(&mesh) {
            return Err(format!("RAFKA_MESH `{mesh}` is not a valid mesh name"));
        }
        let data_dir = launch
            .as_ref()
            .map(|l| l.data_dir.clone())
            .or_else(|| get("RAFKA_DATA_DIR").map(PathBuf::from))
            .unwrap_or_else(|| std::env::temp_dir().join(format!("rafka-node-admin-{}", rand::random::<u32>())));
        let bin_dir = get("RAFKA_BIN_DIR").map(PathBuf::from).unwrap_or_else(|| {
            std::env::current_exe().ok().and_then(|e| e.parent().map(Path::to_path_buf)).unwrap_or_else(|| PathBuf::from("."))
        });
        let api_bind = get("RAFKA_NODE_ADMIN_API_BIND")
            .map(|b| b.parse().map_err(|e| format!("RAFKA_NODE_ADMIN_API_BIND `{b}`: {e}")))
            .transpose()?
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0)));
        let passthrough = ["RAFKA_EVIDENCE_DIR", "RUST_LOG", "OTEL_EXPORTER_OTLP_ENDPOINT", "RAFKA_ENDPOINT_PORT_RANGE", "RAFKA_CONTAINER_SUBNET_POOL"]
            .iter()
            .filter_map(|k| get(k).map(|v| (k.to_string(), v)))
            .collect();
        Ok(Self {
            fabric,
            fabric_id,
            mesh,
            mesh_id: launch
                .as_ref()
                .and_then(|l| l.mesh_id.clone())
                .map(Ok)
                .or_else(|| get("RAFKA_MESH_ID").filter(|v| !v.trim().is_empty()).map(|v| MeshId::parse(&v).map_err(|e| format!("RAFKA_MESH_ID: {e}"))))
                .transpose()?,
            data_dir,
            bin_dir,
            spawn_type: get(crate::deployment::provider::SPAWN_TYPE_ENV),
            api_bind,
            launch,
            passthrough,
        })
    }
}

/// The node's transport key in its data dir (minted on first use).
fn load_or_mint_key(data_dir: &Path) -> Result<SecretKey, String> {
    std::fs::create_dir_all(data_dir).map_err(|e| format!("{}: {e}", data_dir.display()))?;
    let path = data_dir.join("node-key");
    if let Ok(h) = std::fs::read_to_string(&path) {
        let bytes: [u8; 32] = hex::decode(h.trim())
            .map_err(|e| format!("{}: {e}", path.display()))?
            .try_into()
            .map_err(|_| format!("{}: not 32 bytes", path.display()))?;
        return Ok(SecretKey::from_bytes(&bytes));
    }
    let k = SecretKey::generate();
    std::fs::write(&path, hex::encode(k.to_bytes())).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(k)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn node_status(s: MemberStatus) -> NodeStatus {
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
    /// nodes.storage: a birth this admin launches is a contact from the moment it is published,
    /// not only once gossip carries it.
    contacts: std::sync::OnceLock<Arc<dyn crate::storage::NodesStorage>>,
    /// The lifecycle states this admin applied as an authority (`status_rpc`): the view's
    /// `declared`, never its `status`.
    pub declared: Arc<Mutex<crate::status_rpc::Declared>>,
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

impl Records {
    /// A fabric shutdown is held: keep the seats as they were before any admin drained.
    pub fn hold_seats(&self) {
        let mut seats = self.seats.lock().unwrap();
        if seats.held.is_none() {
            seats.held = Some(seats.calm.clone());
        }
    }

    pub fn held_seats(&self) -> Option<BTreeMap<NodeId, (bool, bool)>> {
        self.seats.lock().unwrap().held.clone()
    }
}

impl TopologySink for Records {
    fn publish(&self, node: Node) {
        if let (Some(store), Some(incarnation_id), Some(transport_id), Some(transport_addr)) = (self.contacts.get(), node.incarnation_id.clone(), node.transport_id.clone(), node.transport_addr) {
            let r = crate::storage::NodeRecord {
                node_id: node.node_id.clone(),
                name: node.name.clone(),
                incarnation_id,
                transport_id,
                transport_addr,
                endpoints: node.endpoints.clone(),
                listeners: node.listeners.clone(),
                declared: None,
            };
            if let Err(e) = store.put_contact(&r) {
                tracing::info!(node = %node.name, error = %e, "a launched birth could not be stored as a contact");
            }
        }
        self.removed.lock().unwrap().remove(&(node.name.clone(), node.incarnation_id.clone()));
        self.nodes.lock().unwrap().insert(node.name.clone(), node);
    }
    fn remove(&self, name: &PathName) {
        let gone = self.nodes.lock().unwrap().remove(name);
        self.removed.lock().unwrap().insert((name.clone(), gone.and_then(|n| n.incarnation_id)));
    }
}

/// The observed topology from membership digests and this admin's records,
/// with the settled primary rule (module docs).
pub fn project(fabric: &str, fabric_id: &FabricId, provider: ProviderKind, book: &DigestBook, records: &Records) -> Topology {
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
        let Some((_, age)) = book.get(d.node.node_id.as_str()) else { continue };
        heard.insert(d.node.incarnation.clone());
        let name = d.node.name.clone();
        if removed.contains(&(name.clone(), Some(d.node.incarnation.clone()))) {
            continue;
        }
        let silent = age > SILENT_AFTER;
        if silent && d.status == MemberStatus::Leaving {
            continue;
        }
        if let Some(id) = d.extra.get(MESH_ID).and_then(|v| MeshId::parse(v).ok()) {
            mesh_ids.entry(name.mesh.clone()).or_insert(id);
        }
        let mut n = Node::allocated(name.clone());
        n.node_id = d.node.node_id.clone();
        n.transport_id = Some(d.node.transport_id.clone());
        n.incarnation_id = Some(d.node.incarnation.clone());
        n.provider = Some(provider);
        n.status = if silent { NodeStatus::Dead } else { node_status(d.status) };
        n.routable = n.status.is_live() && book.routable(d.node.node_id.as_str());
        n.declared = records.declared.lock().unwrap().node(&n.node_id).filter(|(inc, _)| Some(inc) == n.incarnation_id.as_ref()).map(|(_, s)| format!("{s:?}"));
        n.admin_api_base = d.admin_api_base.clone();
        n.transport_addr = Some(d.node.transport_addr);
        n.endpoints = d.node.endpoints.0.clone();
        n.data_dir = d.data_dir.clone();
        if let Some(r) = recorded.get(&name).filter(|r| r.incarnation_id == n.incarnation_id) {
            n.deployment_id = r.deployment_id.clone();
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
    // Births this admin started that have not reported yet. A launch record
    // stands in for a birth only until membership speaks for it: once heard,
    // it never revives the birth (another admin may have retired it since). A
    // departed birth was heard and has left: its record never stands in either.
    for op in book.departed() {
        heard.insert(op.incarnation.clone());
    }
    for (name, r) in recorded {
        if r.incarnation_id.as_ref().is_some_and(|i| heard.contains(i)) || book.is_departed(r.node_id.as_str()) {
            continue;
        }
        nodes.entry(name).or_insert(r);
    }
    let mut topology = Topology {
        fabric: Fabric { id: fabric_id.clone(), name: fabric.into(), status: ScopeStatus::Pending, provider },
        meshes: Vec::new(),
        nodes: nodes.into_values().collect(),
    };
    let mesh_names: BTreeSet<String> = mesh_ids.keys().cloned().chain(topology.nodes.iter().map(|n| n.mesh.clone())).collect();
    // Every seat, by the one election function (`election`); through a fabric shutdown, the
    // seats held when it was learned (`Seats`).
    crate::election::resolve(&mut topology.nodes);
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
                if !topology.nodes.iter().any(|n| n.kind == NodeKind::NodeAdmin && n.status == NodeStatus::Draining) {
                    seats.calm = topology
                        .nodes
                        .iter()
                        .filter(|n| n.is_primary || n.is_fabric_primary)
                        .map(|n| (n.node_id.clone(), (n.is_primary, n.is_fabric_primary)))
                        .collect();
                }
            }
        }
    }
    if topology.fabric_primary().is_some() {
        topology.fabric.status = ScopeStatus::ReadyForTraffic;
    }
    for m in mesh_names {
        let ready = topology.cohort_primary(&m, NodeKind::NodeAdmin).is_some();
        let id = mesh_ids.get(&m).cloned();
        topology.meshes.push(Mesh { id, name: m, status: if ready { ScopeStatus::ReadyForTraffic } else { ScopeStatus::Pending } });
    }
    topology
}

/// The fabric authority's drift check (`crate::drift`): when this admin is the fabric primary,
/// the accepted Build is settled, and a cohort the Build's topology names has fewer births than
/// it should, at least one of them with its exact runtime inspected and found exited, it opens
/// the next attempt of that same Build (`Fabric.build_id` unchanged: the topology did not change).
/// `started` holds the (build, attempt, exited births) it already opened, so a repair that fails
/// is not opened again and again; the adapter's insert-and-fail on the attempt keeps two
/// authorities from opening it twice.
pub async fn reconcile_drift(
    me: &PathName,
    t: &Topology,
    accepted: &AcceptedStore,
    book: &DigestBook,
    provider: &dyn crate::deployment::provider::DeploymentProvider,
    builds: &dyn BuildStateAdapter,
    started: &mut HashSet<(crate::build::BuildId, u32, Vec<String>)>,
) -> Option<(crate::build::BuildId, u32)> {
    let authority = t.fabric_primary().filter(|n| &n.name == me)?;
    let current = accepted.current(builds).await?;
    if matches!(current.state, crate::build_state::BuildState::Pending | crate::build_state::BuildState::Running) {
        return None;
    }
    let mut exited = HashSet::new();
    for n in crate::drift::unheard(t) {
        let Some(fact) = book.get(n.node_id.as_str()).map(|(dg, _)| dg).filter(|dg| Some(&dg.node.incarnation) == n.incarnation_id.as_ref()).and_then(|dg| dg.node.runtime) else {
            continue;
        };
        // Proof is the exact runtime's own terminal status, in this
        // provider's control domain; anything else proves nothing.
        let Ok(handle) = crate::deployment::provider::adopt(provider, &fact) else { continue };
        if matches!(provider.inspect(&handle).await, crate::deployment::provider::DeploymentStatus::Exited { .. }) {
            exited.insert(n.incarnation_id.clone().expect("filtered on it"));
        }
    }
    let short = crate::drift::shortfall(&current.topology, t, &exited);
    // Surplus: a live birth of a kept Mesh the accepted topology does not name (a removal the
    // executing view had not heard yet when the Build closed).
    let mut surplus: Vec<String> = t
        .nodes
        .iter()
        .filter(|n| n.status.is_live() && current.topology.meshes.contains_key(&n.mesh) && !current.topology.contains(&n.name))
        .map(|n| n.name.to_string())
        .collect();
    surplus.sort();
    if short.is_empty() && surplus.is_empty() {
        return None;
    }
    let attempt = current.attempt + 1;
    let mut key: Vec<String> = short.iter().flat_map(|s| s.exited.iter().cloned()).chain(surplus.iter().cloned()).collect();
    key.sort();
    if !started.insert((current.build_id.clone(), attempt, key)) {
        return None;
    }
    let mut scope: Vec<String> = short.iter().map(ToString::to_string).collect();
    if !surplus.is_empty() {
        scope.push(format!("surplus: {}", surplus.join(" ")));
    }
    let scope = scope.join("; ");
    let span = tracing::info_span!(
        "rafka.node_admin.build.update.via-proven-drift",
        build_id = %current.build_id,
        attempt,
        scope = %scope,
        authority = %authority.name,
        authority_node_id = %authority.node_id,
        reason = "proven-drift",
    );
    let opened = crate::build_state::AttemptOpened {
        build_id: current.build_id.clone(),
        attempt,
        reason: crate::build_state::AttemptReason::ProvenDrift,
        action: None,
        opened_by: me.to_string(),
        opened_at_ms: now_ms(),
    };
    match builds.open_attempt(&opened).await {
        Ok(()) => {
            span.in_scope(|| tracing::info!("a birth the accepted topology names is proven gone: the next attempt of the same Build is open"));
            Some((current.build_id.clone(), attempt))
        }
        Err(e) => {
            span.in_scope(|| tracing::info!(error = %e, "the attempt could not be opened"));
            None
        }
    }
}

/// Never Ready around missing control hydration: an admin that holds no `Fabric.build_id` names
/// itself blocked. Only Day 0 accepts the first Build itself; every other admin hydrates the
/// pointer from an existing authority (its entry pull, or the fabric control topic).
pub fn hydration_blocker(me: &PathName, accepted: &AcceptedStore) -> Option<String> {
    accepted.build_id().is_none().then(|| format!("{me}: holds no Fabric.build_id (not hydrated from an existing authority)"))
}

/// The order a whole-mesh retire takes `members`: every ordinary member, then the admin cohort,
/// then `last_admin` (the mesh's admin primary) last, so a live admin of the mesh forwards every
/// departure before the last one goes. Within a group, path order.
pub fn retire_mesh_order(members: Vec<PathName>, last_admin: Option<&PathName>) -> Vec<PathName> {
    let mut ordinary: Vec<PathName> = members.iter().filter(|n| n.kind != NodeKind::NodeAdmin).cloned().collect();
    let mut admins: Vec<PathName> = members.iter().filter(|n| n.kind == NodeKind::NodeAdmin && Some(*n) != last_admin).cloned().collect();
    ordinary.sort();
    admins.sort();
    ordinary.extend(admins);
    if let Some(r) = last_admin.filter(|r| members.contains(r)) {
        ordinary.push(r.clone());
    }
    ordinary
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
    pub book: DigestBook,
}

impl MembershipObserver {
    fn digest_of(&self, node: &Node) -> Option<MeshDigest> {
        self.book.get(node.node_id.as_str()).map(|(d, _)| d).filter(|d| Some(&d.node.incarnation) == node.incarnation_id.as_ref())
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

    async fn ready(&self, node: &Node) -> Result<(), String> {
        match self.digest_of(node) {
            Some(d) if d.status == MemberStatus::ReadyForTraffic => {
                if node.kind == NodeKind::NodeAdmin && d.admin_api_base.is_none() {
                    return Err(format!("{} serves no control API yet", node.name));
                }
                Ok(())
            }
            Some(d) => Err(format!("{} reports {:?}", node.name, d.status)),
            None => Err(format!("no digest from {} for this birth", node.name)),
        }
    }

    async fn drained(&self, node: &Node) -> bool {
        self.digest_of(node).is_some_and(|d| {
            d.status == MemberStatus::Leaving
                || (d.status == MemberStatus::Draining && d.extra.get("in_flight").is_none_or(|v| v == "0"))
        })
    }

    async fn admission_closed(&self, node: &Node) -> Result<(), String> {
        match self.digest_of(node) {
            Some(d) if matches!(d.status, MemberStatus::Draining | MemberStatus::Leaving) => Ok(()),
            None => Ok(()), // gone from the fabric
            Some(d) => Err(format!("{} still reports {:?}", node.name, d.status)),
        }
    }

    async fn departed(&self, node: &Node) -> bool {
        self.digest_of(node).is_some_and(|d| d.status == MemberStatus::Leaving)
    }
}

/// What this admin answers an entry pull with.
struct EntryState {
    name: PathName,
    fabric: String,
    fabric_id: FabricId,
    provider: ProviderKind,
    records: Arc<Records>,
    book: DigestBook,
    digest: Arc<Mutex<MeshDigest>>,
    accepted: Arc<AcceptedStore>,
    shutdown: Arc<crate::shutdown::ShutdownControl>,
}

/// What fencing a path's previous birth found.
#[derive(Debug, PartialEq, Eq)]
enum FenceOutcome {
    /// Nothing runs there any more: a new birth may take the path. `gone` is the previous
    /// birth when the provider inspected its exact runtime as not running: that inspection is
    /// the proof its departure is published from. `None` when there was no birth, or this
    /// admin holds no runtime for it (no proof, so no departure is published).
    Clear { gone: Option<Node> },
    /// The previous birth answers directly: it is alive and keeps the path.
    Alive,
    /// The previous birth is unheard and does not answer, but the provider inspects its exact
    /// runtime as running: silence is never death proof, so it is held and keeps the path
    /// (Luke 2026-10-05). Only an explicit retire or replace may terminate a running runtime.
    Held,
}

/// Realises Build operations through the deployment and lifecycle pipelines.
pub struct AdminRunner {
    pub provider: Arc<dyn DeploymentProvider>,
    pub allocator: Mutex<EndpointAllocator>,
    pub observer: Arc<MembershipObserver>,
    pub records: Arc<Records>,
    pub builds: Arc<dyn BuildStateAdapter>,
    pub template: LaunchTemplate,
    pub admin_env: BTreeMap<String, String>,
    pub bin_dir: PathBuf,
    pub lifecycle: LifecycleTransitionPipeline,
    pub handles: Mutex<HashMap<PathName, (Node, DeploymentHandle)>>,
    pub topology: Arc<RwLock<Topology>>,
    /// What the views are projected from.
    pub fabric: String,
    pub fabric_id: FabricId,
    pub fabric_provider: ProviderKind,
    pub book: DigestBook,
    /// This admin's own path.
    pub me: PathName,
    /// This admin's node id (the adopter on every runtime it adopts).
    pub node_id: NodeId,
    /// This admin's Iroh endpoint: it asks a node directly whether it answers.
    pub endpoint: Option<Endpoint>,
    /// This process's one Node RPC client (and its live resolver).
    pub node_rpc: Option<crate::node_rpc::ProcessNodeRpc>,
    /// Where the retire pipeline's lifecycle events go: this admin's membership and backbone.
    pub lifecycle_events: Arc<dyn crate::deployment::pipeline::LifecycleEvents>,
}

/// The lifecycle events of a retirement this admin executes, on its own mesh channel and the
/// backbone; peer primaries forward them onto their meshes.
pub struct GossipLifecycle {
    pub membership: Membership,
    pub backbone: Backbone,
}

#[async_trait::async_trait]
impl crate::deployment::pipeline::LifecycleEvents for GossipLifecycle {
    async fn deleting(&self, op: &rafka_mesh_entity::LifecycleOp) {
        let f = rafka_mesh_transport::membership::Frame::NodeDeleting { op: op.clone(), forwarded_by: None };
        let span = tracing::info_span!("rafka.node_admin.node.update.via-node-deleting", node = %op.name, node_id = %op.node_id, build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        let _g = span.enter();
        if let Err(e) = self.membership.publish_lifecycle(&f).await {
            tracing::info!(error = %e, "NodeDeleting not sent on the mesh channel");
        }
        if let Err(e) = self.backbone.publish_lifecycle(&f).await {
            tracing::info!(error = %e, "NodeDeleting not sent on the backbone");
        }
        tracing::info!("the node is being removed: every mesh hears it is not routable");
    }
    async fn deleted(&self, op: &rafka_mesh_entity::LifecycleOp) {
        let f = rafka_mesh_transport::membership::Frame::NodeDeleted { op: op.clone(), forwarded_by: None };
        let span = tracing::info_span!("rafka.node_admin.node.delete.via-node-deleted", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.0, build_id = %op.build_id, attempt = op.attempt, operation = %op.operation);
        let _g = span.enter();
        if let Err(e) = self.membership.publish_lifecycle(&f).await {
            tracing::info!(error = %e, "NodeDeleted not sent on the mesh channel");
        }
        if let Err(e) = self.backbone.publish_lifecycle(&f).await {
            tracing::info!(error = %e, "NodeDeleted not sent on the backbone");
        }
        tracing::info!("the exact birth is proven terminal: it has left");
    }
}

impl AdminRunner {
    /// Publish the view as it is now, so a Build that completes after this
    /// operation is never read back against an older view.
    async fn refresh_view(&self) {
        let t = project(&self.fabric, &self.fabric_id, self.fabric_provider, &self.book, &self.records);
        *self.topology.write().await = t;
    }

    /// A launch of `kind` in `mesh`. Every node carries its mesh's identity (it
    /// names the mesh's channel): the one this admin created, else the one the
    /// fabric knows (a recovered mesh is reconstructed as itself, never anew).



    async fn template_for(&self, kind: NodeKind, mesh: &str) -> LaunchTemplate {
        let known = self.topology.read().await.meshes.iter().find(|m| m.name == mesh).and_then(|m| m.id.clone());
        let mut t = self.template.clone();
        match kind {
            NodeKind::RpcNode => t.executable = self.bin_dir.join(format!("rafka-rpc-node{}", std::env::consts::EXE_SUFFIX)),
            NodeKind::NodeAdmin => {
                t.executable = self.bin_dir.join(format!("rafka-node-admin{}", std::env::consts::EXE_SUFFIX));
                t.env.extend(self.admin_env.clone());
            }
        }
        // Every node of a mesh carries its id: it names the mesh's channel.
        if let Some(id) = self.records.meshes.lock().unwrap().get(mesh).cloned().or(known) {
            t.env.insert(rafka_mesh_entity::launch::ENV_MESH_ID.into(), id.to_string());
        }
        t
    }

    fn pipeline<'a>(&'a self, template: &'a LaunchTemplate) -> DeploymentPipeline<'a> {
        DeploymentPipeline {
            provider: &*self.provider,
            allocator: &self.allocator,
            observer: &*self.observer,
            sink: &*self.records,
            lifecycle: &*self.lifecycle_events,
            builds: &*self.builds,
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
        let digest = self
            .book
            .get(node.node_id.as_str())
            .map(|(d, _)| d)
            .filter(|d| d.node.incarnation == incarnation)
            .ok_or_else(|| format!("{} (birth {}) is not held in this admin's membership; its runtime is unknown", node.name, incarnation.0))?;
        let refuse = |reason: &str, detail: String| {
            let span = match reason {
                "unpublished" => tracing::info_span!("rafka.node_admin.runtime.reject.via-unpublished", node = %node.name, incarnation_id = %incarnation.0, adopter = %self.me, detail = %detail),
                "other-provider" => tracing::info_span!("rafka.node_admin.runtime.reject.via-other-provider", node = %node.name, incarnation_id = %incarnation.0, adopter = %self.me, detail = %detail),
                "foreign-control-domain" => tracing::info_span!("rafka.node_admin.runtime.reject.via-foreign-control-domain", node = %node.name, incarnation_id = %incarnation.0, adopter = %self.me, detail = %detail),
                _ => tracing::info_span!("rafka.node_admin.runtime.reject.via-invalid-fact", node = %node.name, incarnation_id = %incarnation.0, adopter = %self.me, detail = %detail),
            };
            span.in_scope(|| tracing::info!("runtime not adopted"));
            format!("{} (birth {}): {detail}", node.name, incarnation.0)
        };
        let fact = digest.node.runtime.clone().ok_or_else(|| refuse("unpublished", "the birth publishes no runtime fact; it cannot be adopted".into()))?;
        let handle = crate::deployment::provider::adopt(&*self.provider, &fact).map_err(|r| refuse(r.reason(), r.to_string()))?;
        let mut record = node.clone();
        record.deployment_id = Some(handle.deployment_id.clone());
        if record.data_dir.is_none() {
            record.data_dir = digest.data_dir.clone();
        }
        tracing::info_span!(
            "rafka.node_admin.runtime.update.via-adopt",
            node = %node.name,
            node_id = %node.node_id,
            incarnation_id = %incarnation.0,
            deployment_id = %fact.deployment_id,
            provider = fact.provider.as_str(),
            provider_control_domain_fingerprint = %fact.domain_fingerprint(),
            runtime_locator_kind = fact.locator.kind(),
            runtime_locator_fingerprint = %fact.locator_fingerprint(),
            source = "self-published-membership",
            adopter = %self.me,
            adopter_node_id = %self.node_id,
            execution_node_id = %self.node_id,
        )
        .in_scope(|| tracing::info!("adopted a runtime this admin did not launch"));
        self.handles.lock().unwrap().insert(node.name.clone(), (record.clone(), handle.clone()));
        Ok((record, handle))
    }

    /// A new birth at a path whose previous birth this view holds as not live (killed, or only
    /// unheard). Recovery replaces only a birth proven terminal: a predecessor whose exact runtime
    /// the provider inspects as running is held, never terminated here, so a partition or a
    /// frozen process is never turned into a replacement (Luke 2026-10-05, silence never
    /// authorizes replacement).
    async fn fence_predecessor(&self, path: &PathName) -> FenceOutcome {
        let Some(prev) = self.topology.read().await.node(path).cloned() else { return FenceOutcome::Clear { gone: None } };
        if prev.status.is_live() || prev.incarnation_id.is_none() {
            return FenceOutcome::Clear { gone: None };
        }
        let incarnation = prev.incarnation_id.clone().map(|i| i.0).unwrap_or_default();
        // Unheard is not dead: losing many peers costs gossip a window (the
        // dead peers' connections time out) in which a healthy member's
        // digests do not arrive. A predecessor that answers directly is alive.
        if self.answers(&prev).await {
            tracing::info_span!("rafka.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = "answers")
                .in_scope(|| tracing::info!("the previous birth is unheard but answers directly: it stays"));
            return FenceOutcome::Alive;
        }
        let (outcome, proven) = match self.handle_for(&prev).await {
            Err(e) => (format!("not-found: {e}"), false),
            Ok((_, h)) => match self.provider.inspect(&h).await {
                crate::deployment::provider::DeploymentStatus::Running => {
                    tracing::info_span!("rafka.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = "held-running")
                        .in_scope(|| tracing::info!("the previous birth is unheard but its exact runtime still runs: held, never replaced on silence"));
                    return FenceOutcome::Held;
                }
                // Only an inspection that says the runtime exited is proof; `Unknown` fences the
                // path (nothing answers there) but publishes no departure.
                other => (format!("not-running: {other:?}"), matches!(other, crate::deployment::provider::DeploymentStatus::Exited { .. })),
            },
        };
        self.handles.lock().unwrap().remove(path);
        tracing::info_span!("rafka.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = %outcome, proven_gone = proven)
            .in_scope(|| tracing::info!("the previous birth at this path is fenced before a new one"));
        FenceOutcome::Clear { gone: proven.then_some(prev) }
    }

    /// Unplanned loss: the fence above inspected the previous birth's exact runtime as not
    /// running. That inspection is the proof; the replacement operation publishes the old
    /// identity's departure from it before the new birth, and records it on the Build.
    async fn publish_proven_departure(&self, build_id: &crate::build::BuildId, attempt: u32, path: &PathName, prev: &Node) {
        let Some(incarnation) = prev.incarnation_id.clone() else { return };
        let op = rafka_mesh_entity::LifecycleOp {
            build_id: build_id.to_string(),
            attempt,
            operation: format!("create-node:{path}"),
            node_id: prev.node_id.clone(),
            incarnation,
            name: path.clone(),
            event_at_ms: now_ms(),
        };
        self.lifecycle_events.deleted(&op).await;
        let receipt = crate::build_state::BuildStepReceipt {
            build_id: build_id.clone(),
            attempt,
            operation: op.operation.clone(),
            step: "NodeDeleted".into(),
            outcome: crate::build_state::StepOutcome::Complete,
            output: serde_json::to_value(&op).ok(),
        };
        if let Err(e) = self.builds.append_step_receipt(&receipt).await {
            tracing::info!(node = %path, error = %e, "the proven departure was published but not recorded on the Build");
        }
    }

    /// Does `node`'s runtime answer when asked directly (not through gossip)?
    /// An rpc node: a Node RPC `Echo` on its first slot. A node-admin: an
    /// entry pull on its mesh endpoint (QUIC; no node ever calls an admin's
    /// HTTP API). Two seconds each; a frozen or gone runtime does not.
    async fn answers(&self, node: &Node) -> bool {
        const WITHIN: Duration = Duration::from_secs(2);
        match node.kind {
            NodeKind::NodeAdmin => {
                let (Some(ep), Some(transport_id), Some(addr)) = (&self.endpoint, &node.transport_id, node.transport_addr) else { return false };
                let Ok(peer) = transport_id.0.parse::<iroh::PublicKey>() else { return false };
                let anchor = EndpointAddr::new(peer).with_ip_addr(addr);
                rafka_mesh_transport::entry::pull_once(ep, anchor, &self.me.to_string(), WITHIN).await.is_ok()
            }
            NodeKind::RpcNode => {
                let Some(node_rpc) = &self.node_rpc else { return false };
                let opts = rafka_node_rpc::CallOptions { budget: rafka_node_rpc::Budget::Overall(WITHIN), ..Default::default() };
                let req = rafka_node_rpc_contract::echo::EchoRequest::Echo { payload: b"fence".to_vec() };
                let (out, _) = node_rpc.client.call::<rafka_node_rpc_contract::echo::Echo>(&rafka_node_rpc::NodeTarget::ExactNode(node.node_id.clone()), &req, &opts).await;
                out.reply().is_some()
            }
        }
    }

    /// `Node: Pending -> ReadyForTraffic`, committed once the node reports ready.
    async fn bring_into_traffic(&self, node: &Node) -> Result<(), String> {
        let key = TransitionKey { scope: LifecycleScope::Node, from: LifecycleState::Pending, to: LifecycleState::ReadyForTraffic };
        let shape = ShapeFacts { desired_meshes: self.topology.read().await.meshes.len() as u32 };
        let ready = self.observer.ready(node).await;
        let t = Transition { transition_id: Transition::id_for(key, &node.name.to_string()), target: node.name.to_string(), key, shape: &shape };
        match self.lifecycle.transition(t, || ready, || {}).await {
            TransitionResult::Committed => Ok(()),
            other => Err(format!("{}: {other:?}", node.name)),
        }
    }

    async fn create(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, restart_of: Option<Node>) -> Result<(), String> {
        if restart_of.is_none() {
            match self.fence_predecessor(node).await {
                // The member is alive (it answers) or held (its exact runtime still runs):
                // nothing to create. Silence never authorizes a replacement.
                FenceOutcome::Alive | FenceOutcome::Held => return Ok(()),
                FenceOutcome::Clear { gone: Some(prev) } => self.publish_proven_departure(build_id, attempt, node, &prev).await,
                FenceOutcome::Clear { gone: None } => {}
            }
        }
        let template = self.template_for(node.kind, &node.mesh).await;
        let req = CreateRequest { build_id: build_id.clone(), attempt, node: node.clone(), spec: spec_for(node.kind), restart_of };
        let created = self.pipeline(&template).create(&req).await.map_err(|e| e.to_string())?;
        self.bring_into_traffic(&created.node).await?;
        self.handles.lock().unwrap().insert(node.clone(), (created.node, created.handle));
        Ok(())
    }

    async fn retire(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, permanent: bool) -> Result<Option<Node>, String> {
        self.retire_with(build_id, attempt, node, permanent, false).await
    }

    async fn retire_with(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, permanent: bool, observe_departure: bool) -> Result<Option<Node>, String> {
        let seen = self.topology.read().await.node(node).cloned();
        let (record, handle) = match seen {
            Some(n) => self.handle_for(&n).await?,
            None => self.handles.lock().unwrap().get(node).cloned().ok_or_else(|| format!("{node} is not in this admin's view"))?,
        };
        let template = self.template_for(node.kind, &node.mesh).await;
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: record.clone(), handle, permanent, observe_departure };
        self.pipeline(&template).retire(&req).await.map_err(|e| e.to_string())?;
        self.handles.lock().unwrap().remove(node);
        Ok(Some(record))
    }

    /// Fabric shutdown: stop every runtime this admin started, all at once;
    /// the fabric primary also stops every other live node of the fabric
    /// (adopting the ones another admin launched).
    pub async fn stop_all(&self) {
        let mut all: Vec<(String, DeploymentHandle)> = self.handles.lock().unwrap().iter().map(|(k, (_, h))| (k.to_string(), h.clone())).collect();
        let view = self.topology.read().await.clone();
        if view.fabric_primary().is_some_and(|p| p.name == self.me) {
            for n in view.nodes.iter().filter(|n| n.name != self.me && n.status.is_live()) {
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
impl OperationRunner for AdminRunner {
    async fn run(&self, build_id: &crate::build::BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String> {
        let r = self.run_op(build_id, attempt, op).await;
        self.refresh_view().await;
        r
    }

}

impl AdminRunner {
    async fn run_op(&self, build_id: &crate::build::BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String> {
        use tracing::Instrument;
        match op {
            BuildOperation::CreateMesh { mesh } => {
                self.records.meshes.lock().unwrap().entry(mesh.clone()).or_insert_with(MeshId::mint);
                Ok(())
            }
            BuildOperation::CreateNode { node } => {
                let span = tracing::info_span!("rafka.node_admin.node.create.via-build", build_id = %build_id, node = %node, attempt);
                self.create(build_id, attempt, node, None).instrument(span).await
            }
            BuildOperation::RestartNode { node } => {
                let span = tracing::info_span!("rafka.node_admin.node.update.via-build", build_id = %build_id, node = %node, attempt);
                async {
                    let prior = self.retire(build_id, attempt, node, false).await?;
                    self.create(build_id, attempt, node, prior).await
                }
                .instrument(span)
                .await
            }
            BuildOperation::RetireNode { node, permanent } => {
                let span = tracing::info_span!("rafka.node_admin.node.delete.via-build", build_id = %build_id, node = %node, attempt);
                self.retire(build_id, attempt, node, *permanent).instrument(span).await.map(|_| ())
            }
            BuildOperation::RetireMesh { mesh } => {
                let (members, last_admin) = {
                    let view = self.topology.read().await;
                    let mut m: BTreeSet<PathName> = view.nodes.iter().filter(|n| n.mesh == *mesh && n.status.is_live()).map(|n| n.name.clone()).collect();
                    m.extend(self.handles.lock().unwrap().keys().filter(|n| n.mesh == *mesh).cloned());
                    let last_admin = view.cohort_primary(mesh, NodeKind::NodeAdmin).map(|n| n.name.clone());
                    (m.into_iter().collect::<Vec<_>>(), last_admin)
                };
                // Members first, the admin cohort last, the mesh's admin primary very
                // last: every departure leaves the mesh through a live admin (Luke 2026-10-05).
                // Each retire holds its local cleanup until this admin has heard the birth's own
                // `Leaving`.
                for node in retire_mesh_order(members, last_admin.as_ref()) {
                    let span = tracing::info_span!("rafka.node_admin.node.delete.via-build", build_id = %build_id, node = %node, attempt);
                    self.retire_with(build_id, attempt, &node, true, true).instrument(span).await?;
                }
                self.records.meshes.lock().unwrap().remove(mesh);
                Ok(())
            }
        }
    }
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
    pub api_base: String,
    pub control: Arc<ControlPlane>,
    pub runner: Arc<AdminRunner>,
    pub membership: Membership,
    pub digest: Arc<Mutex<MeshDigest>>,
    /// This process's one Node RPC client and live resolver: every Node RPC
    /// caller in the process takes it by clone.
    pub node_rpc: crate::node_rpc::ProcessNodeRpc,
    /// This admin's place on the backbone: while it leaves, its aggregate publication carries its
    /// own `Leaving` out of its mesh ([`Running::leave`]).
    backbone: Backbone,
    router: IrohRouter,
    /// The digest cadence, the Build executor and the hierarchy publication:
    /// stopped first on leave.
    publisher: tokio::task::JoinHandle<()>,
    executor: tokio::task::JoinHandle<()>,
    hierarchy: tokio::task::JoinHandle<()>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Running {
    /// Leave the fabric the way every node does (node-rpc §35): stop taking
    /// Build work and say `Draining`, then keep saying `Leaving` for the
    /// leave linger (`RAFKA_LEAVE_LINGER_MS`), then close. iroh-gossip
    /// acknowledges nothing and closing drops what is unsent, so one
    /// announcement can be lost; an attempt the executor was running is
    /// continued by the Build's next attempt.
    /// Stop executing and reconciling: no Build attempt and no drift
    /// recovery starts after this. A fabric shutdown does this first, so the
    /// runtimes it stops are not recovered as proven drift.
    pub fn stop_reconciling(&self) {
        self.executor.abort();
    }

    pub async fn leave(self) {
        self.executor.abort();
        self.hierarchy.abort();
        self.publisher.abort();
        let say = |status: MemberStatus| {
            let mut d = self.digest.lock().unwrap().clone();
            d.status = status;
            d.emitted_unix_ms = now_ms();
            *self.digest.lock().unwrap() = d.clone();
            d
        };
        let _ = self.membership.publish(&say(MemberStatus::Draining)).await;
        // The hierarchy loop is stopped above, so the backbone roles stay as they were when this
        // admin began to leave: an admin primary keeps publishing its mesh's members on the
        // backbone through the linger, its own `Leaving` among them, so its self-authored departure
        // leaves the mesh by the path every member's does (Luke 2026-10-05). No other admin is a
        // publisher of this admin's membership.
        let said = announce_leaving(leave_linger_from_env(), LEAVE_EVERY, || {
            let d = say(MemberStatus::Leaving);
            let (m, bb, control) = (self.membership.clone(), self.backbone.clone(), self.control.clone());
            async move {
                let _ = m.publish(&d).await;
                let mesh = d.node.name.mesh.clone();
                let mine: Vec<MeshDigest> = m.book.current(SILENT_AFTER).into_iter().filter(|x| x.node.name.mesh == mesh).collect();
                let status = serde_json::to_value(control.topology.read().await.fabric.status).ok().and_then(|v| v.as_str().map(String::from)).unwrap_or_default();
                bb.publish(&m, mine, &status).await;
            }
        })
        .await;
        tracing::info!(announcements = said, "said Leaving for the linger");
        for t in self.tasks {
            t.abort();
        }
        let _ = self.router.shutdown().await;
    }
}

/// Bring a node-admin up: identity, policy, membership, Build state, the
/// control API, the projection and the executor.
pub async fn start(mut cfg: AdminConfig) -> Result<Running, String> {
    let key = load_or_mint_key(&cfg.data_dir)?;
    // The node-admin storage boundaries, in its own data dir. An admin that finds its own row in
    // nodes.storage was here before: it restarts as the same logical node, in the same Mesh and
    // Fabric, holding the same Builds.
    let storage_err = |e: crate::record_store::StorageError| e.to_string();
    let fabric_storage: Arc<dyn crate::fabric_storage::FabricStorage> =
        Arc::new(crate::fabric_storage::FileFabricStorage::open(&cfg.data_dir).map_err(storage_err)?);
    let mesh_storage: Arc<dyn crate::storage::MeshStorage> = Arc::new(crate::storage::FileMeshStorage::open(&cfg.data_dir).map_err(storage_err)?);
    let nodes_storage: Arc<dyn crate::storage::NodesStorage> = Arc::new(crate::storage::FileNodesStorage::open(&cfg.data_dir).map_err(storage_err)?);
    let _connections_storage: Arc<dyn crate::storage::ConnectionsStorage> =
        Arc::new(crate::storage::FileConnectionsStorage::open(&cfg.data_dir).map_err(storage_err)?);
    let journal = Arc::new(crate::build_state::FileJournal::open(&cfg.data_dir).map_err(|e| e.to_string())?);
    let restart = match (&cfg.launch, nodes_storage.own().map_err(storage_err)?) {
        (None, Some(own)) => {
            let fabric = fabric_storage.fabric().map_err(storage_err)?.ok_or_else(|| {
                format!("{}: nodes.storage holds this admin ({}) but fabric.storage holds no Fabric record", cfg.data_dir.display(), own.name)
            })?;
            let mesh = mesh_storage.mesh(&own.name.mesh).map_err(storage_err)?.ok_or_else(|| {
                format!("{}: nodes.storage holds this admin ({}) but mesh.storage holds no {} record", cfg.data_dir.display(), own.name, own.name.mesh)
            })?;
            cfg.fabric = fabric.name.clone();
            cfg.fabric_id = fabric.fabric_id.clone();
            cfg.mesh = mesh.name.clone();
            cfg.mesh_id = Some(mesh.mesh_id.clone());
            Some(own)
        }
        _ => None,
    };
    // Identity and the two slots: assigned by the launching pipeline, the bootstrap admin's own,
    // or (a restart) its own row, under a new incarnation that supersedes the one it last ran.
    // Its bootstrap contacts are the births it last heard, its own Mesh first: hints, any one of
    // which answering is enough.
    let (name, node_id, incarnation, supersedes, mesh_addr, control_addr, seeds, slots) = match (&cfg.launch, &restart) {
        (Some(l), _) => {
            let control = l.listeners.iter().find(|(n, _)| n == "control").map(|(_, a)| *a).ok_or_else(|| "launch assigns no `control` listener".to_string())?;
            (l.name.clone(), l.node_id.clone(), l.incarnation.clone(), l.supersedes.clone(), l.transport_addr, control, l.seeds.clone(), l.endpoints.clone())
        }
        (None, Some(own)) => {
            let slot = |s: &str| match s {
                "mesh" => Some(own.transport_addr),
                _ => own.listeners.iter().find(|(n, _)| n == s).map(|(_, a)| *a),
            };
            let mut contacts = nodes_storage.contacts().map_err(storage_err)?;
            contacts.retain(|c| c.node_id != own.node_id);
            contacts.sort_by_key(|c| (c.name.mesh != own.name.mesh, c.name.to_string()));
            let seeds = contacts.iter().filter_map(|c| c.gossip_addr().and_then(|a| a.ip_addrs().next().map(|ip| (a.id.to_string(), *ip)))).collect();
            tracing::info_span!(
                "rafka.node_admin.node.update.via-restart",
                node = %own.name,
                node_id = %own.node_id,
                supersedes = %own.incarnation_id,
                fabric_id = %cfg.fabric_id,
                contacts = contacts.len(),
            )
            .in_scope(|| tracing::info!("restarting as the same logical node from nodes.storage"));
            // The stable slot keeps its token across the restart: the e6.s8 token rule for admins.
            let kept = if own.endpoints.is_empty() { vec![EndpointSlot::fresh("rpc-0")] } else { own.endpoints.clone() };
            (
                own.name.clone(),
                own.node_id.clone(),
                IncarnationId::mint(),
                Some(own.incarnation_id.clone()),
                slot("mesh").unwrap_or_else(|| SocketAddr::new(cfg.api_bind.ip(), 0)),
                slot("control").unwrap_or(cfg.api_bind),
                seeds,
                kept,
            )
        }
        (None, None) => (
            PathName::new(&cfg.mesh, NodeKind::NodeAdmin, 1),
            NodeId::mint(),
            IncarnationId::mint(),
            None,
            SocketAddr::new(cfg.api_bind.ip(), 0),
            cfg.api_bind,
            Vec::new(),
            vec![EndpointSlot::fresh("rpc-0")],
        ),
    };

    // The mesh endpoint: gossip for membership and Build facts.
    // A dead peer's connection closes within the membership silence window:
    // the gossip actor waits on a dead peer's full send queue until then.
    let transport = iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(Duration::from_secs(1))
        .max_idle_timeout(Some(Duration::from_secs(3).try_into().map_err(|e| format!("idle timeout: {e:?}"))?))
        .build();
    let alpns = vec![iroh_gossip::ALPN.to_vec(), rafka_node_rpc::ALPN.to_vec()];
    let endpoint = match rafka_node_rpc::endpoint::bind_exact(key.clone(), mesh_addr, alpns.clone(), transport.clone()).await {
        Ok(ep) => ep,
        // A restart rebinds the address it last held so its peers' contacts stay good; another
        // process may hold it now, and then a fresh port serves (peers learn it by gossip).
        Err(e) if restart.is_some() => {
            // A reserved port from the host-wide allocator, never an OS-chosen one: the ephemeral
            // range overlaps the allocator's, and a port a birth was assigned but has not bound
            // yet would be taken from under it.
            let fresh = crate::deployment::endpoint::EndpointAllocator::from_env().take_transport(&name).map_err(|e| format!("mesh slot {mesh_addr} is taken and no reserved port is free: {e}"))?;
            tracing::info!(addr = %mesh_addr, fresh = %fresh, error = %e, "the mesh slot this admin last held is taken; binding a reserved fresh port");
            rafka_node_rpc::endpoint::bind_exact(key.clone(), fresh, alpns.clone(), transport)
                .await
                .map_err(|e| format!("mesh slot {fresh}: {e}"))?
        }
        Err(e) => return Err(format!("mesh slot {mesh_addr}: {e}")),
    };
    let mesh_addr = endpoint.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap_or(mesh_addr);
    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
    // Entry: what this admin holds, for a node it launched (filled once the
    // admin's own digest exists; until then a pull is told it is not ready).
    let entry: Arc<std::sync::OnceLock<EntryState>> = Arc::new(std::sync::OnceLock::new());
    let served = entry.clone();
    let entry_server = EntryServer::new(move |_req| match served.get() {
        None => EntryAnswer::default(),
        Some(e) => {
            let mut members = e.book.current(SILENT_AFTER);
            members.push(e.digest.lock().unwrap().clone());
            let topology = project(&e.fabric, &e.fabric_id, e.provider, &e.book, &e.records);
            // The fabric control state a new admin hydrates before it may be Ready.
            let control = serde_json::json!({ "fabric": e.accepted.record().ok().flatten(), "shutdown": e.shutdown.held() });
            EntryAnswer { served_by: e.name.to_string(), topology: serde_json::to_value(&topology).unwrap_or_default(), members, control }
        }
    });
    // Node RPC on the admin's one endpoint: the status declarations it applies as an authority
    // (`status_rpc`). The authority is filled once this admin holds a view; until then NotReady.
    let authority: crate::status_rpc::AuthoritySlot = Arc::new(std::sync::OnceLock::new());
    let rpc_server = crate::status_rpc::serve(rafka_node_rpc::ServerBuilder::new(), authority.clone())
        .seal(rafka_node_rpc::ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() }, slots.iter().cloned())
        .map_err(|e| format!("the admin's protocol catalog refused to seal: {e:?}"))?;
    let iroh_router = IrohRouter::builder(endpoint.clone())
        .accept(iroh_gossip::ALPN, gossip.clone())
        .accept(ENTRY_ALPN, entry_server)
        .accept(rafka_node_rpc::ALPN, rpc_server)
        .spawn();
    let seed_addrs: Vec<EndpointAddr> = seeds
        .iter()
        .filter_map(|(k, a)| k.parse::<iroh::PublicKey>().ok().map(|pk| EndpointAddr::new(pk).with_ip_addr(*a)))
        .collect();
    // The mesh's id names its membership channel: the launch names it (the
    // launching admin always does), else the entry pull's projection knows
    // it, else this admin is the mesh's first and mints it.
    let launched_anchor = if restart.is_some() { None } else { cfg.launch.as_ref().and(seed_addrs.first().cloned()) };
    let mut pulled: Option<rafka_mesh_transport::entry::EntryAnswer> = None;
    let mesh_id = match (&cfg.mesh_id, &launched_anchor) {
        (Some(id), _) => id.clone(),
        (None, Some(anchor)) => {
            let answer = rafka_mesh_transport::entry::pull(&endpoint, anchor.clone(), &name.to_string(), 5)
                .await
                .map_err(|e| format!("entry pull from the launching admin failed: {e}"))?;
            let known = serde_json::from_value::<Topology>(answer.topology.clone())
                .ok()
                .and_then(|t| t.meshes.into_iter().find(|m| m.name == cfg.mesh))
                .and_then(|m| m.id);
            pulled = Some(answer);
            known.unwrap_or_else(MeshId::mint)
        }
        (None, None) => MeshId::mint(),
    };
    // Subscribe first, pull second: what changes during the pull arrives by
    // gossip, and the book keeps the newer copy.
    let membership = Membership::join(&gossip, &endpoint, &cfg.fabric_id, &cfg.mesh, &mesh_id, &name.to_string(), seed_addrs.clone())
        .await
        .map_err(|e| format!("membership: {e}"))?;
    let backbone = Backbone::join(&gossip, &endpoint, &membership, &cfg.mesh, &name.to_string(), seed_addrs.clone())
        .await
        .map_err(|e| format!("backbone: {e}"))?;
    // fabric.storage: this admin's Fabric control state. A shutdown already held there (a restart
    // or a join during one) is in force from the start (fabric-mesh-lifecycle.md §11.1).
    if fabric_storage.fabric().map_err(|e| e.to_string())?.is_none() {
        fabric_storage
            .put_fabric(&crate::fabric_storage::FabricRecord { fabric_id: cfg.fabric_id.clone(), name: cfg.fabric.clone(), build_id: None })
            .map_err(|e| e.to_string())?;
    }
    let shutdown_control = Arc::new(crate::shutdown::ShutdownControl::open(fabric_storage.clone(), name.to_string()).map_err(|e| e.to_string())?);
    // `Fabric.build_id`: Day 0 accepts the first Build itself; every other admin hydrates it from
    // its entry pull or the fabric control topic.
    let accepted = Arc::new(AcceptedStore::new(fabric_storage.clone(), name.to_string()));
    let builds = Arc::new(
        FabricBuildStateAdapter::join(&gossip, &endpoint, &cfg.fabric_id, seed_addrs.clone(), journal.clone(), accepted.clone(), shutdown_control.clone(), name.to_string())
            .await
            .map_err(|e| e.to_string())?,
    );
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
    let policy = match &launched_anchor {
        Some(anchor) => {
            let answer = match pulled.take() {
                Some(a) => a,
                None => rafka_mesh_transport::entry::pull(&endpoint, anchor.clone(), &name.to_string(), 5)
                    .await
                    .map_err(|e| format!("entry pull from the launching admin failed: {e}"))?,
            };
            let topology: Topology =
                serde_json::from_value(answer.topology.clone()).map_err(|e| format!("the entry answer from {} holds no topology: {e}", answer.served_by))?;
            let mut mesh_peers = Vec::new();
            let mut admins = Vec::new();
            for d in answer.members.into_iter().filter(|d| d.fabric_id == cfg.fabric_id && d.node.name != name) {
                if let Some(a) = rafka_mesh_transport::membership::gossip_addr(&d) {
                    if d.node.name.mesh == cfg.mesh {
                        mesh_peers.push(a.clone());
                    }
                    if d.node.name.kind == NodeKind::NodeAdmin {
                        admins.push(a);
                    }
                }
                membership.learn(d, "entry");
            }
            let _ = membership.join_peers(mesh_peers).await;
            backbone.join_admins(admins).await;
            // Hydrate `Fabric.build_id` from the launching admin; without it this admin stays
            // Pending (the Ready gate below). The Build it names arrives on the Build topic.
            match answer.control.get("fabric").filter(|v| !v.is_null()).cloned().map(serde_json::from_value::<crate::fabric_storage::FabricRecord>) {
                Some(Ok(r)) => accepted.learn(r, &*builds, &answer.served_by).await,
                Some(Err(e)) => tracing::info!(served_by = %answer.served_by, error = %e, "the entry answer's Fabric record does not decode"),
                None => {}
            }
            // A fabric shutdown in force: this admin comes up frozen.
            if let Some(v) = answer.control.get("shutdown").filter(|v| !v.is_null()).cloned() {
                match serde_json::from_value::<crate::fabric_storage::FabricShutdown>(v) {
                    Ok(sd) => {
                        shutdown_control.learn(sd, "hydration", &answer.served_by).map_err(|e| e.to_string())?;
                    }
                    Err(e) => tracing::info!(served_by = %answer.served_by, error = %e, "the entry answer's fabric shutdown does not decode"),
                }
            }
            tracing::info_span!("rafka.node_admin.fabric.update.via-join", node = %name, joined = %answer.served_by)
                .in_scope(|| tracing::info!("entry pulled; eligible to execute Builds"));
            FabricPolicy { provider: topology.fabric.provider }
        }
        None => {
            // Day 0: no Fabric authority exists before this admin. It accepts the first Build, its
            // own mesh with one node-admin, and points `Fabric.build_id` at it (Build first).
            if restart.is_some() {
                tracing::info_span!("rafka.node_admin.fabric.update.via-restart", node = %name, build_id = %accepted.build_id().map(|b| b.0).unwrap_or_default())
                    .in_scope(|| tracing::info!("Fabric.build_id and its Build reloaded from this admin's own storage"));
            } else if accepted.build_id().is_none() {
                let b0 = crate::build_state::BuildAccepted {
                    build_id: crate::build::BuildId::mint(),
                    topology: crate::accepted::FabricTopology::root(&cfg.fabric, &cfg.mesh),
                    submitted_change: None,
                    traceparent: rafka_mesh_telemetry::current_traceparent(),
                    submitted_at_ms: now_ms(),
                };
                builds.publish_accepted(&b0).await.map_err(|e| e.to_string())?;
                accepted.point(&b0.build_id, "day-0").map_err(|e| e.to_string())?;
            }
            FabricPolicy::bootstrap(cfg.spawn_type.as_deref()).map_err(|e| e.to_string())?
        }
    };

    // The control API.
    let listener = match tokio::net::TcpListener::bind(control_addr).await {
        Ok(l) => l,
        // As the mesh slot: a restart's last control address may be held by another process now.
        Err(e) if restart.is_some() => {
            tracing::info!(addr = %control_addr, error = %e, "the control slot this admin last held is taken; binding a fresh port");
            tokio::net::TcpListener::bind(SocketAddr::new(control_addr.ip(), 0)).await.map_err(|e| format!("control slot {control_addr}: {e}"))?
        }
        Err(e) => return Err(format!("control slot {control_addr}: {e}")),
    };
    let api_base = format!("http://{}", listener.local_addr().map_err(|e| e.to_string())?);
    let records = Arc::new(Records::default());
    let _ = records.contacts.set(nodes_storage.clone());
    records.meshes.lock().unwrap().insert(cfg.mesh.clone(), mesh_id.clone());
    let book = membership.book.clone();
    let (node_rpc, node_rpc_feed) = crate::node_rpc::ProcessNodeRpc::start(endpoint.clone(), &book, &name.to_string());
    let control = Arc::new(ControlPlane::new(builds.clone(), accepted.clone(), name.clone(), project(&cfg.fabric, &cfg.fabric_id, policy.provider, &book, &records)));
    {
        let records_for_ids = records.clone();
        let _ = authority.set(Arc::new(crate::status_rpc::StatusAuthority {
            me: name.clone(),
            fabric_id: cfg.fabric_id.to_string(),
            topology: control.topology.clone(),
            declared: records.declared.clone(),
            nodes_storage: nodes_storage.clone(),
            mesh_ids: Arc::new(move || records_for_ids.meshes.lock().unwrap().iter().map(|(k, v)| (k.clone(), v.to_string())).collect()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }));
    }

    // The deployment hand (used while this admin is fabric primary).
    let prepared = crate::deployment::prepare(policy, &cfg.fabric).await.map_err(|e| e.to_string())?;
    let mut admin_env = BTreeMap::new();
    admin_env.insert("RAFKA_BIN_DIR".to_string(), cfg.bin_dir.display().to_string());
    let data_root = cfg.data_dir.parent().map(Path::to_path_buf).unwrap_or_else(|| cfg.data_dir.clone());
    let template = LaunchTemplate {
        fabric: cfg.fabric.clone(),
        fabric_id: cfg.fabric_id.clone(),
        executable: PathBuf::new(),
        seeds: vec![(key.public().to_string(), SocketAddr::new(prepared.admin_ip, mesh_addr.port()))],
        env: cfg.passthrough.clone(),
        data_root,
    };
    let hooks = HookRegistry::new().seal().map_err(|e| format!("{e:?}"))?;
    let runner = Arc::new(AdminRunner {
        provider: prepared.provider.clone(),
        allocator: Mutex::new(prepared.allocator),
        observer: Arc::new(MembershipObserver { book: book.clone() }),
        records: records.clone(),
        builds: builds.clone(),
        template,
        admin_env,
        bin_dir: cfg.bin_dir.clone(),
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
        node_rpc: Some(node_rpc.clone()),
        lifecycle_events: Arc::new(GossipLifecycle { membership: membership.clone(), backbone: backbone.clone() }),
    });

    // This birth's exact runtime. A launched admin takes the record its
    // launcher's pipeline made available; the bootstrap admin, which nobody
    // launched, adopts its own process (Day 0, one receipt per step). Either
    // way it is published with the birth.
    let mut adoption = None;
    let runtime = match &cfg.launch {
        Some(_) => {
            let dir = cfg.data_dir.clone();
            tokio::task::spawn_blocking(move || rafka_mesh_entity::runtime::await_own_record(&dir, Duration::from_secs(10)))
                .await
                .map_err(|e| format!("reading the runtime record: {e}"))??
        }
        None => {
            let a = CurrentRuntimeAdoption::begin(&name, &cfg.data_dir, &crate::model::DeploymentId::mint()).map_err(|e| e.to_string())?;
            let f = a.fact().clone();
            adoption = Some(a);
            f
        }
    };
    // Own digest, published on the membership cadence.
    let mut extra = BTreeMap::new();
    extra.insert(MESH_ID.to_string(), mesh_id.to_string());
    let digest = Arc::new(Mutex::new(MeshDigest {
        fabric_id: cfg.fabric_id.clone(),
        node: MeshNode {
            node_id: node_id.clone(),
            name: name.clone(),
            transport_id: TransportId(key.public().to_string()),
            // The mesh endpoint as bound (a rebind may have moved it), and the one stable Node RPC
            // slot the admin serves its declarations through.
            transport_addr: mesh_addr,
            incarnation: incarnation.clone(),
            supersedes,
            endpoints: EndpointSet(slots.clone()),
            // Day 0: PublishRuntimeFactAndCurrentRuntimeMetadata sets them.
            runtime: adoption.is_none().then(|| runtime.clone()),
        },
        // Pending until it can take authority at once (the Ready gate below).
        status: MemberStatus::Pending,
        admin_api_base: Some(api_base.clone()),
        emitted_unix_ms: now_ms(),
        extra,
        data_dir: adoption.is_none().then(|| cfg.data_dir.display().to_string()),
    }));
    // nodes.storage and mesh.storage: this birth and its Mesh, so a restart on this data dir is
    // the same logical node in the same Mesh.
    {
        let d = digest.lock().unwrap().clone();
        nodes_storage
            .put_own(&crate::storage::NodeRecord {
                node_id: node_id.clone(),
                name: name.clone(),
                incarnation_id: incarnation.clone(),
                transport_id: d.node.transport_id.clone(),
                transport_addr: d.node.transport_addr,
                endpoints: d.node.endpoints.0.clone(),
                listeners: vec![("control".into(), listener.local_addr().map_err(|e| e.to_string())?)],
                declared: None,
            })
            .map_err(storage_err)?;
        mesh_storage.put_mesh(&crate::storage::MeshRecord { mesh_id: mesh_id.clone(), name: cfg.mesh.clone() }).map_err(storage_err)?;
    }
    // nodes.storage contacts: the births this admin hears, as bootstrap hints for a later restart.
    // A contact whose path another logical node now holds is dropped; nothing here is topology.
    let contacts_task = {
        let (book, nodes_storage, me, fabric_id) = (membership.book.clone(), nodes_storage.clone(), node_id.clone(), cfg.fabric_id.clone());
        tokio::spawn(async move {
            let mut written: HashMap<NodeId, crate::storage::NodeRecord> =
                nodes_storage.contacts().unwrap_or_default().into_iter().map(|c| (c.node_id.clone(), c)).collect();
            loop {
                let heard: Vec<MeshDigest> = book.current(SILENT_AFTER).into_iter().filter(|d| d.fabric_id == fabric_id && d.node.node_id != me).collect();
                for d in &heard {
                    let mut r = crate::storage::NodeRecord {
                        node_id: d.node.node_id.clone(),
                        name: d.node.name.clone(),
                        incarnation_id: d.node.incarnation.clone(),
                        transport_id: d.node.transport_id.clone(),
                        transport_addr: d.node.transport_addr,
                        endpoints: d.node.endpoints.0.clone(),
                        listeners: Vec::new(),
                        declared: None,
                    };
                    // The contact is what is heard; the declared state on the row is the
                    // authority's (`status_rpc`) and rides along, never overwritten from gossip.
                    let same = |w: &crate::storage::NodeRecord| crate::storage::NodeRecord { declared: None, ..w.clone() } == r;
                    if !written.get(&r.node_id).is_some_and(same) {
                        r.declared = nodes_storage
                            .contacts()
                            .ok()
                            .and_then(|cs| cs.into_iter().find(|c| c.node_id == r.node_id && c.incarnation_id == r.incarnation_id))
                            .and_then(|c| c.declared);
                        if nodes_storage.put_contact(&r).is_ok() {
                            written.insert(r.node_id.clone(), r);
                        }
                    }
                }
                let replaced: Vec<NodeId> = written
                    .values()
                    .filter(|w| heard.iter().any(|d| d.node.name == w.name && d.node.node_id != w.node_id))
                    .map(|w| w.node_id.clone())
                    .collect();
                for id in replaced {
                    if nodes_storage.remove_contact(&id).is_ok() {
                        written.remove(&id);
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    };
    if let Some(a) = adoption {
        a.publish(|f, dir| {
            let mut d = digest.lock().unwrap();
            d.node.runtime = Some(f.clone());
            d.data_dir = Some(dir);
        })
        .map_err(|e| e.to_string())?;
    }
    let _ = entry.set(EntryState {
        name: name.clone(),
        fabric: cfg.fabric.clone(),
        fabric_id: cfg.fabric_id.clone(),
        provider: policy.provider,
        records: records.clone(),
        book: book.clone(),
        digest: digest.clone(),
        accepted: accepted.clone(),
        shutdown: shutdown_control.clone(),
    });
    if let Some(l) = &cfg.launch {
        // The pipeline's assignment, not a re-minted one.
        digest.lock().unwrap().node.endpoints = EndpointSet(l.endpoints.clone());
    }
    let mut tasks = vec![contacts_task, node_rpc_feed];
    // Authority-capable Ready: this admin commits `ReadyForTraffic` (and so
    // becomes eligible for every seat) only once it can manage every birth it
    // holds: each live member publishes a runtime fact this admin's provider
    // adopts in its own control domain, and the Day-0 admin holds a Complete
    // receipt for every step of its own adoption. Until then it publishes
    // Pending.
    {
        let (digest, book, provider, me, membership) = (digest.clone(), book.clone(), prepared.provider.clone(), name.clone(), membership.clone());
        let day0 = cfg.launch.is_none().then(|| cfg.data_dir.clone());
        let hydrated = accepted.clone();
        let (incarnation, node_id, mesh_id, fabric_id) = (incarnation.clone(), node_id.clone(), mesh_id.clone(), cfg.fabric_id.clone());
        let runtime = runtime.clone();
        tasks.push(tokio::spawn(async move {
            let mut reported = None;
            loop {
                let held: Vec<MeshDigest> = book
                    .current(SILENT_AFTER)
                    .into_iter()
                    .filter(|d| d.node.name != me && d.status != MemberStatus::Leaving)
                    .collect();
                let mut blocked = authority_blockers(&held, &*provider);
                if let Some(dir) = &day0 {
                    blocked.extend(adoption_missing(dir).into_iter().map(|s| format!("{me}: no Complete receipt for its own {s}")));
                }
                blocked.extend(hydration_blocker(&me, &hydrated));
                if blocked.is_empty() {
                    let mut d = digest.lock().unwrap();
                    if d.status != MemberStatus::Pending {
                        break;
                    }
                    d.status = MemberStatus::ReadyForTraffic;
                    drop(d);
                    tracing::info_span!(
                        "rafka.mesh.node.update.via-ready",
                        node = %me,
                        incarnation_id = %incarnation.0,
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
                    )
                    .in_scope(|| tracing::info!("ready for traffic: every held birth's runtime is adoptable"));
                    break;
                }
                if reported.as_ref() != Some(&blocked) {
                    tracing::info_span!("rafka.node_admin.runtime.reject.via-not-authority-capable", node = %me, blocked = blocked.len(), detail = %blocked.join("; "))
                        .in_scope(|| tracing::info!("not ready: a held birth's runtime cannot be managed from here"));
                    reported = Some(blocked);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }));
    }
    let d = digest.clone();
    let publisher = membership.publish_every(Duration::from_millis(500), move || {
        let mut d = d.lock().unwrap().clone();
        d.emitted_unix_ms = now_ms();
        d
    });

    let hierarchy;
    // The hierarchy: while this admin is its mesh's primary it publishes the
    // mesh's members on the backbone and forwards the other meshes onto its
    // mesh channel; while it is the fabric primary it publishes the fabric's
    // status. It joins the backbone through every admin it knows and its mesh
    // channel through every member of its mesh it knows.
    {
        let (backbone, membership, topology, me, mesh, builds) = (backbone.clone(), membership.clone(), control.topology.clone(), name.clone(), cfg.mesh.clone(), builds.clone());
        let adapter = runner.builds.clone();
        // Provisional: the architecture measures this default from stripped-full sizes under the
        // scale and chaos proofs (rafka-v2 #2900); 10 rounds is a placeholder, not the answer.
        let full_every: u32 = std::env::var("RAFKA_FULL_EVERY_ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(10);
        hierarchy = tokio::spawn(async move {
            let mut last_overlays: Option<(Vec<rafka_mesh_entity::LifecycleOp>, Vec<rafka_mesh_entity::LifecycleOp>)> = None;
            let mut rounds: u32 = 0;
            loop {
                // The open lifecycle overlays, derived from the Build facts each round: a
                // successor primary publishes the same ones from the same facts.
                if let Ok(facts) = adapter.facts().await {
                    for op in crate::build_state::in_flight_ops(&crate::build_state::fold(&facts)) {
                        membership.book.deleting(op);
                    }
                }
                let t = topology.read().await.clone();
                // Cut off, or within one silence window of healing, its view
                // authorizes nothing: it publishes as no primary.
                let live = membership.authorizes();
                backbone.set_mesh_primary(live && t.cohort_primary(&mesh, NodeKind::NodeAdmin).is_some_and(|n| n.name == me));
                backbone.set_fabric_primary(live && t.fabric_primary().is_some_and(|n| n.name == me));
                let heard = membership.book.current(SILENT_AFTER);
                let mine: Vec<MeshDigest> = heard.iter().filter(|d| d.node.name.mesh == mesh).cloned().collect();
                let status = serde_json::to_value(t.fabric.status).ok().and_then(|v| v.as_str().map(String::from)).unwrap_or_default();
                backbone.publish(&membership, mine.clone(), &status).await;
                // The mesh's own members hear the overlays and departures alone on their
                // channel: on a change, and every `full_every` rounds as anti-entropy.
                if live && t.cohort_primary(&mesh, NodeKind::NodeAdmin).is_some_and(|n| n.name == me) {
                    let now = (membership.book.in_flight(), membership.book.departed());
                    rounds += 1;
                    if last_overlays.as_ref() != Some(&now) || rounds % full_every == 0 {
                        let _ = membership.publish_overlays(&mesh, &me.to_string()).await;
                        last_overlays = Some(now);
                    }
                }
                let addr = rafka_mesh_transport::membership::gossip_addr;
                let admins: Vec<_> = heard.iter().filter(|d| d.node.name.kind == NodeKind::NodeAdmin && d.node.name != me).filter_map(addr).collect();
                builds.join_admins(admins.clone()).await;
                backbone.join_admins(admins).await;
                let _ = membership.join_peers(mine.iter().filter(|d| d.node.name != me).filter_map(addr).collect()).await;
                tokio::time::sleep(rafka_mesh_transport::membership::PUBLISH_EVERY).await;
            }
        });
    }
    // The projection, refreshed into the control plane's topology.
    {
        let (topology, fabric, fabric_id, provider, book, records) = (control.topology.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, book.clone(), records.clone());
        let elections = ElectionLog::new(name.clone());
        tasks.push(tokio::spawn(async move {
            loop {
                let t = project(&fabric, &fabric_id, provider, &book, &records);
                elections.observe(&t);
                *topology.write().await = t;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }));
    }
    let executor;
    // The executor: continues every Build whose next operation this admin
    // executes (`executor::executor_for`). A launched admin starts it only
    // after its entry pull, so its first view is its launcher's.
    {
        let exec = BuildExecutor {
            executor: name.to_string(),
            builds: builds.clone(),
            topology: control.topology.clone(),
            runner: runner.clone(),
        };
        let (topology, submitted) = (control.topology.clone(), control.build_submitted.clone());
        let (book, records, fabric, fabric_id, provider, cut_off_view) = (book.clone(), records.clone(), cfg.fabric.clone(), cfg.fabric_id.clone(), policy.provider, membership.clone());
        let (me, deployer, accepted, drift_builds) = (name.clone(), runner.provider.clone(), accepted.clone(), builds.clone());
        let frozen = shutdown_control.clone();
        executor = tokio::spawn(async move {
            let mut started = HashSet::new();
            loop {
                // A fabric shutdown freezes reconciliation: no Build attempt, no drift recovery, no
                // rebirth from here on (fabric-mesh-lifecycle.md §11.1).
                if frozen.held().is_some() {
                    return;
                }
                // Decide on, and plan from, the view as it is now: a cached view
                // can predate the members that make another admin primary.
                let now = project(&fabric, &fabric_id, provider, &book, &records);
                *topology.write().await = now.clone();
                // Every Build whose next operation this admin executes; none
                // while it is cut off or within one silence window of healing
                // (its view then authorizes nothing). The fabric authority first
                // starts a reconciliation Build for proven drift.
                if cut_off_view.authorizes() {
                    reconcile_drift(&me, &now, &accepted, &book, &*deployer, &*drift_builds, &mut started).await;
                    exec.reconcile_active().await;
                }
                tokio::select! {
                    _ = submitted.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(300)) => {}
                }
            }
        });
    }
    // A fabric shutdown: freeze (the executor stops, this admin says Draining), then this admin's
    // part of the drain. The fabric-primary stops itself last by waking the shutdown route.
    {
        let (abort, digest_w, membership_w, me, records_w) = (executor.abort_handle(), digest.clone(), membership.clone(), name.clone(), records.clone());
        let (seen, drain_control) = (shutdown_control.subscribe(), shutdown_control.clone());
        let stopper: Arc<dyn crate::shutdown::Stopper> = Arc::new(AdminStopper { runner: runner.clone(), topology: control.topology.clone() });
        let done = control.shutdown.clone();
        let _ = control.fabric_shutdown.set(Arc::new(crate::http::ShutdownSeat {
            control: shutdown_control.clone(),
            me: name.clone(),
            node_id: node_id.clone(),
        }));
        tasks.push(tokio::spawn(async move {
            let mut seen = seen;
            while seen.borrow_and_update().is_none() {
                if seen.changed().await.is_err() {
                    return;
                }
            }
            // The seats stay as they were before any admin drained (FML-26): held before this
            // admin says Draining, so no view of its own moves them.
            records_w.hold_seats();
            abort.abort();
            let d = {
                let mut d = digest_w.lock().unwrap();
                d.status = MemberStatus::Draining;
                d.emitted_unix_ms = now_ms();
                d.clone()
            };
            let _ = membership_w.publish(&d).await;
            tracing::info_span!("rafka.node_admin.fabric.update.via-shutdown-frozen", node = %me)
                .in_scope(|| tracing::info!("reconciliation frozen; Draining"));
            if crate::shutdown::drain(me, drain_control, stopper).await {
                done.notify_waiters();
            }
        }));
    }
    let app = router(control.clone(), axum::Router::new());
    tasks.push(tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::info!(error = %e, "control API stopped");
        }
    }));
    let _ = std::fs::write(cfg.data_dir.join("node-admin.json"), serde_json::json!({ "api_base": api_base, "node": name.to_string() }).to_string());
    Ok(Running { api_base, control, runner, membership, digest, node_rpc, backbone: backbone.clone(), router: iroh_router, publisher, executor, hierarchy, tasks })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT (Luke 2026-10-05): a whole-mesh retire takes the mesh's ordinary members first,
    /// then its admin cohort, and its admin primary last. Must NOT happen: an admin
    /// before a member (path order puts `admin` before `rpc`).
    #[test]
    fn a_mesh_retire_takes_members_first_and_its_admin_primary_last() {
        let p = |s: &str| -> PathName { s.parse().unwrap() };
        let members = vec![p("mesh2.admin.1"), p("mesh2.admin.2"), p("mesh2.rpc.2"), p("mesh2.rpc.1")];
        let order = retire_mesh_order(members, Some(&p("mesh2.admin.1")));
        assert_eq!(order, vec![p("mesh2.rpc.1"), p("mesh2.rpc.2"), p("mesh2.admin.2"), p("mesh2.admin.1")]);
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
        let mut extra = BTreeMap::new();
        extra.insert(MESH_ID.to_string(), mesh_id(&name.mesh).to_string());
        MeshDigest {
            fabric_id: fabric1(),
            node: MeshNode {
                node_id: NodeId::mint(),
                name: name.clone(),
                transport_id: TransportId(format!("key-{name}")),
                transport_addr: "127.0.0.1:41000".parse().unwrap(),
                incarnation: IncarnationId::mint(),
                supersedes: None,
                endpoints: EndpointSet(vec![]),
                runtime: None,
            },
            status,
            admin_api_base: (name.kind == NodeKind::NodeAdmin).then(|| format!("http://admin-{name}")),
            emitted_unix_ms: now_ms(),
            extra,
            data_dir: None,
        }
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
        assert_eq!(t.fabric.status, ScopeStatus::ReadyForTraffic);
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
        draining.emitted_unix_ms += 1;
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
            d.emitted_unix_ms += 1;
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
            d.extra.insert("ready_since_ms".into(), since.to_string());
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
                deployment_id: "dep".into(),
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

    #[test]
    fn an_admin_without_a_hydrated_fabric_pointer_is_never_authority_capable() {
        let me: PathName = "mesh1.admin.2".parse().unwrap();
        use crate::fabric_storage::FabricStorage as _;
        let storage = Arc::new(crate::fabric_storage::MemoryFabricStorage::new());
        storage.put_fabric(&crate::fabric_storage::FabricRecord { fabric_id: fabric1(), name: "fabric1".into(), build_id: None }).unwrap();
        let store = AcceptedStore::new(storage.clone(), "mesh1.admin.2");
        let blocked = hydration_blocker(&me, &store).expect("blocked while unhydrated");
        assert!(blocked.starts_with("mesh1.admin.2: holds no Fabric.build_id"), "{blocked}");
        store.point(&crate::build::BuildId("bld-0".into()), "test").unwrap();
        assert_eq!(hydration_blocker(&me, &store), None);
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

    #[test]
    fn a_launch_record_never_revives_a_birth_that_another_admin_retired() {
        // This admin launched mesh1.rpc.1; another admin (the primary) retired
        // it: its last word is Leaving, then silence. This admin's own launch
        // record must not bring it back as ready.
        let book = DigestBook::default();
        let left = digest("mesh1.rpc.1", MemberStatus::Leaving);
        book.record(left.clone());
        let records = Records::default();
        let mut n = Node::allocated(left.node.name.clone());
        n.incarnation_id = Some(left.node.incarnation.clone());
        n.status = NodeStatus::ReadyForTraffic;
        records.publish(n.clone());
        std::thread::sleep(SILENT_AFTER + Duration::from_millis(200));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert!(t.node(&left.node.name).is_none(), "{:?}", t.node(&left.node.name));
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
            event_at_ms: 1,
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
}
