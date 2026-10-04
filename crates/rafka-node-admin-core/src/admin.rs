//! The node-admin runtime (`rafka-node-admin`; `docs/i143/design.md` §3–§4).
//!
//! A node-admin is a fabric member that serves the control API. The first
//! one bootstraps the fabric (its `MESH_SPAWN_TYPE` becomes fabric policy);
//! every other one is launched by an admin's deployment pipeline and inherits
//! the policy from the admin it joins.
//!
//! Every admin holds the same two projections over iroh-gossip: fabric
//! membership (each member's digest) and the fabric's Build facts. From
//! membership it derives the observed topology it publishes on its views,
//! with each cohort's primary elected by `election` (the member ready for
//! traffic longest), and the admin primary of the lowest-named mesh as
//! fabric primary. The fabric
//! primary executes Builds: it claims each active Build's next attempt and
//! runs what is left through the deployment pipeline (create, restart,
//! retire) and the lifecycle pipeline (a node's `Pending -> ReadyForTraffic`).

use crate::build::BuildOperation;
use crate::build_state::BuildStateAdapter;
use crate::deployment::endpoint::{slots_for, EndpointAllocator};
use crate::deployment::pipeline::{
    CreateRequest, DeploymentPipeline, LaunchTemplate, NodeObserver, RetireRequest, Timeouts, TopologySink,
};
use crate::deployment::provider::{DeploymentHandle, DeploymentProvider, FabricPolicy, TerminationMode};
use crate::executor::{BuildExecutor, OperationRunner};
use crate::election::{elect, Candidate, ElectionLog};
use crate::fabric_builds::FabricBuildStateAdapter;
use crate::http::{router, ControlPlane};
use crate::lifecycle::{
    HookRegistry, LifecycleScope, LifecycleState, LifecycleTransitionPipeline, MemoryReceiptLog, ShapeFacts, Transition,
    TransitionKey, TransitionResult,
};
use crate::model::*;
use crate::topology::Topology;
use iroh::endpoint::presets;
use iroh::protocol::Router as IrohRouter;
use iroh::{Endpoint, EndpointAddr, SecretKey};
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::{EndpointSet, MemberStatus, MeshDigest, MeshNode};
use rafka_mesh_transport::membership::{DigestBook, Membership};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

/// The digest key carrying a node-admin's mesh id.
pub const MESH_ID: &str = "mesh_id";
/// How long a member stays in the view without a fresh digest.
const SILENT_AFTER: Duration = Duration::from_secs(3);

/// Everything a node-admin reads from its environment.
#[derive(Debug, Clone)]
pub struct AdminConfig {
    pub fabric: String,
    pub mesh: String,
    pub mesh_id: Option<String>,
    pub data_dir: PathBuf,
    pub bin_dir: PathBuf,
    /// `MESH_SPAWN_TYPE` as given (normalised by the fabric policy).
    pub spawn_type: Option<String>,
    /// The HTTP bind of a bootstrap admin (`RAFKA_NODE_ADMIN_API_BIND`).
    pub api_bind: SocketAddr,
    /// Set when a deployment pipeline launched this admin.
    pub launch: Option<Launch>,
    /// The control API of an admin already in the fabric.
    pub join: Option<String>,
    /// Passed on to every runtime this admin launches.
    pub passthrough: BTreeMap<String, String>,
}

impl AdminConfig {
    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let launch = if get(rafka_mesh_entity::launch::ENV_NODE_ID).is_some() { Some(Launch::from_env(&get)?) } else { None };
        let fabric = launch.as_ref().map(|l| l.fabric.clone()).or_else(|| get("RAFKA_FABRIC")).unwrap_or_else(|| "fabric1".into());
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
            mesh,
            mesh_id: get("RAFKA_MESH_ID"),
            data_dir,
            bin_dir,
            spawn_type: get(crate::deployment::provider::SPAWN_TYPE_ENV),
            api_bind,
            launch,
            join: get("RAFKA_NODE_ADMIN_JOIN"),
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
}

impl TopologySink for Records {
    fn publish(&self, node: Node) {
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
pub fn project(fabric: &str, provider: ProviderKind, book: &DigestBook, records: &Records) -> Topology {
    let removed = records.removed.lock().unwrap().clone();
    let recorded = records.nodes.lock().unwrap().clone();
    let mut nodes: BTreeMap<PathName, Node> = BTreeMap::new();
    // Each birth's election claim, by incarnation.
    let mut claims: HashMap<IncarnationId, u64> = HashMap::new();
    let mut mesh_ids: BTreeMap<String, MeshId> = records.meshes.lock().unwrap().clone();
    for d in book.all() {
        if d.fabric != fabric {
            continue;
        }
        let Some((_, age)) = book.get(&d.node.node_id.0) else { continue };
        let name = d.node.name.clone();
        if removed.contains(&(name.clone(), Some(d.node.incarnation.clone()))) {
            continue;
        }
        let silent = age > SILENT_AFTER;
        if silent && d.status == MemberStatus::Leaving {
            continue;
        }
        if let Some(id) = d.extra.get(MESH_ID) {
            mesh_ids.entry(name.mesh.clone()).or_insert_with(|| MeshId(id.clone()));
        }
        let mut n = Node::allocated(name.clone());
        n.node_id = d.node.node_id.clone();
        n.fabric_id = Some(d.node.fabric_id.clone());
        n.incarnation_id = Some(d.node.incarnation.clone());
        n.provider = Some(provider);
        n.status = if silent { NodeStatus::Dead } else { node_status(d.status) };
        n.admin_api_base = d.admin_api_base.clone();
        n.endpoints = d.node.endpoints.0.clone();
        if let Some(since) = d.ready_since() {
            claims.insert(d.node.incarnation.clone(), since);
        }
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
    // Births this admin started that have not reported yet.
    for (name, r) in recorded {
        nodes.entry(name).or_insert(r);
    }
    let mut topology = Topology {
        fabric: Fabric { name: fabric.into(), status: ScopeStatus::Pending, provider },
        meshes: Vec::new(),
        nodes: nodes.into_values().collect(),
    };
    let mesh_names: BTreeSet<String> = mesh_ids.keys().cloned().chain(topology.nodes.iter().map(|n| n.mesh.clone())).collect();
    // Each cohort's primary: the member ready for traffic longest.
    let mut cohorts: BTreeMap<(String, NodeKind), Vec<usize>> = BTreeMap::new();
    for (i, n) in topology.nodes.iter().enumerate() {
        if n.status == NodeStatus::ReadyForTraffic {
            cohorts.entry((n.mesh.clone(), n.kind)).or_default().push(i);
        }
    }
    for members in cohorts.values() {
        let candidates: Vec<Candidate<'_>> = members
            .iter()
            .map(|&i| {
                let n = &topology.nodes[i];
                Candidate { name: &n.name, ready_since: n.incarnation_id.as_ref().and_then(|i| claims.get(i).copied()) }
            })
            .collect();
        if let Some(k) = elect(&candidates) {
            let i = members[k];
            topology.nodes[i].is_primary = true;
        }
    }
    let fabric_primary = mesh_names.iter().find_map(|m| {
        topology.nodes.iter().position(|n| n.mesh == *m && n.kind == NodeKind::NodeAdmin && n.is_primary)
    });
    if let Some(i) = fabric_primary {
        topology.nodes[i].is_fabric_primary = true;
        topology.fabric.status = ScopeStatus::ReadyForTraffic;
    }
    for m in mesh_names {
        let ready = topology.cohort_primary(&m, NodeKind::NodeAdmin).is_some();
        let id = mesh_ids.get(&m).cloned().unwrap_or_else(|| MeshId(format!("unknown-{m}")));
        topology.meshes.push(Mesh { id, name: m, status: if ready { ScopeStatus::ReadyForTraffic } else { ScopeStatus::Pending } });
    }
    topology
}

/// Readiness, drain and admission seen through each member's own digest:
/// a node publishes `ReadyForTraffic` once it serves, and `Draining` right
/// after it starts refusing new work with a typed `Draining` (node-rpc §35).
pub struct MembershipObserver {
    pub book: DigestBook,
}

impl MembershipObserver {
    fn digest_of(&self, node: &Node) -> Option<MeshDigest> {
        self.book.get(&node.node_id.0).map(|(d, _)| d).filter(|d| Some(&d.node.incarnation) == node.incarnation_id.as_ref())
    }
}

#[async_trait::async_trait]
impl NodeObserver for MembershipObserver {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        self.book.get(&node_id.0).is_some_and(|(d, _)| &d.node.incarnation == incarnation)
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
    pub fabric_provider: ProviderKind,
    pub book: DigestBook,
    /// This admin's own path.
    pub me: PathName,
}

impl AdminRunner {
    /// Publish the view as it is now, so a Build that completes after this
    /// operation is never read back against an older view.
    async fn refresh_view(&self) {
        let t = project(&self.fabric, self.fabric_provider, &self.book, &self.records);
        *self.topology.write().await = t;
    }

    fn template_for(&self, kind: NodeKind, mesh: &str) -> LaunchTemplate {
        let mut t = self.template.clone();
        match kind {
            NodeKind::RpcNode => t.executable = self.bin_dir.join(format!("rafka-rpc-node{}", std::env::consts::EXE_SUFFIX)),
            NodeKind::NodeAdmin => {
                t.executable = self.bin_dir.join(format!("rafka-node-admin{}", std::env::consts::EXE_SUFFIX));
                t.env.extend(self.admin_env.clone());
                if let Some(id) = self.records.meshes.lock().unwrap().get(mesh) {
                    t.env.insert("RAFKA_MESH_ID".into(), id.0.clone());
                }
            }
        }
        t
    }

    fn pipeline<'a>(&'a self, template: &'a LaunchTemplate) -> DeploymentPipeline<'a> {
        DeploymentPipeline {
            provider: &*self.provider,
            allocator: &self.allocator,
            observer: &*self.observer,
            sink: &*self.records,
            builds: &*self.builds,
            template,
            timeouts: Timeouts::default(),
        }
    }

    /// The runtime of `node`'s birth: this admin's own handle, or the one the
    /// Build that launched it recorded (another admin's launch, e.g. a lost
    /// fabric primary's), adopted. Found through the birth's `AllocateIdentity`
    /// receipt (its incarnation) and the `DeployRuntime` receipt of the same
    /// deployment.
    async fn handle_for(&self, node: &Node) -> Result<(Node, DeploymentHandle), String> {
        if let Some((rec, h)) = self.handles.lock().unwrap().get(&node.name).cloned() {
            if node.incarnation_id.is_none() || rec.incarnation_id == node.incarnation_id {
                return Ok((rec, h));
            }
        }
        let incarnation = node.incarnation_id.clone().ok_or_else(|| format!("{} has no known birth", node.name))?;
        let facts = self.builds.facts().await.map_err(|e| format!("reading Build facts to adopt {}: {e}", node.name))?;
        let ops = [format!("create-node:{}", node.name), format!("restart-node:{}", node.name)];
        let identity = facts.iter().find_map(|f| match f {
            crate::build_state::BuildFact::Step(r)
                if r.step == "AllocateIdentity" && ops.contains(&r.operation)
                    && r.output.as_ref().and_then(|o| o.get("incarnation")).and_then(|v| v.as_str()) == Some(incarnation.0.as_str()) =>
            {
                r.output.clone()
            }
            _ => None,
        });
        let identity = identity.ok_or_else(|| format!("no Build recorded the birth {} of {}; it cannot be adopted", incarnation.0, node.name))?;
        let deployment = identity.get("deployment_id").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let handle = facts
            .iter()
            .find_map(|f| match f {
                crate::build_state::BuildFact::Step(r) if r.step == "DeployRuntime" && ops.contains(&r.operation) => r
                    .output
                    .clone()
                    .and_then(|o| serde_json::from_value::<DeploymentHandle>(o).ok())
                    .filter(|h| h.deployment_id.0 == deployment),
                _ => None,
            })
            .ok_or_else(|| format!("no Build recorded the runtime of {} (deployment {deployment}); it cannot be adopted", node.name))?;
        let mut record = node.clone();
        record.deployment_id = Some(handle.deployment_id.clone());
        if record.data_dir.is_none() {
            let node_id = identity.get("node_id").and_then(|v| v.as_str()).unwrap_or(&node.node_id.0).to_string();
            record.data_dir = Some(self.template.data_root.join(format!("{}-{}", node.name, node_id)).display().to_string());
        }
        tracing::info!(node = %node.name, deployment_id = %handle.deployment_id, "adopted a runtime another admin launched");
        self.handles.lock().unwrap().insert(node.name.clone(), (record.clone(), handle.clone()));
        Ok((record, handle))
    }

    /// A new birth at a path whose previous birth this view holds as not live
    /// (killed, or only unheard): if that runtime still runs, stop it first,
    /// so a path never has two live runtimes.
    async fn fence_predecessor(&self, path: &PathName) {
        let Some(prev) = self.topology.read().await.node(path).cloned() else { return };
        if prev.status.is_live() || prev.incarnation_id.is_none() {
            return;
        }
        let incarnation = prev.incarnation_id.clone().map(|i| i.0).unwrap_or_default();
        let outcome = match self.handle_for(&prev).await {
            Err(e) => format!("not-found: {e}"),
            Ok((_, h)) => match self.provider.inspect(&h).await {
                crate::deployment::provider::DeploymentStatus::Running => {
                    match self.provider.terminate(&h, TerminationMode::Graceful { grace: Duration::from_secs(8) }).await {
                        Ok(()) => "terminated".to_string(),
                        Err(e) => format!("terminate-failed: {e}"),
                    }
                }
                other => format!("not-running: {other:?}"),
            },
        };
        self.handles.lock().unwrap().remove(path);
        tracing::info_span!("rafka.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = %outcome)
            .in_scope(|| tracing::info!("the previous birth at this path is fenced before a new one"));
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
            self.fence_predecessor(node).await;
        }
        let template = self.template_for(node.kind, &node.mesh);
        let req = CreateRequest { build_id: build_id.clone(), attempt, node: node.clone(), slots: slots_for(node.kind), restart_of };
        let created = self.pipeline(&template).create(&req).await.map_err(|e| e.to_string())?;
        self.bring_into_traffic(&created.node).await?;
        self.handles.lock().unwrap().insert(node.clone(), (created.node, created.handle));
        Ok(())
    }

    async fn retire(&self, build_id: &crate::build::BuildId, attempt: u32, node: &PathName, permanent: bool) -> Result<Option<Node>, String> {
        let seen = self.topology.read().await.node(node).cloned();
        let (record, handle) = match seen {
            Some(n) => self.handle_for(&n).await?,
            None => self.handles.lock().unwrap().get(node).cloned().ok_or_else(|| format!("{node} is not in this admin's view"))?,
        };
        let template = self.template_for(node.kind, &node.mesh);
        let req = RetireRequest { build_id: build_id.clone(), attempt, node: record.clone(), handle, permanent };
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
                let members: Vec<PathName> = {
                    let view = self.topology.read().await;
                    let mut m: BTreeSet<PathName> = view.nodes.iter().filter(|n| n.mesh == *mesh && n.status.is_live()).map(|n| n.name.clone()).collect();
                    m.extend(self.handles.lock().unwrap().keys().filter(|n| n.mesh == *mesh).cloned());
                    m.into_iter().collect()
                };
                for node in members {
                    let span = tracing::info_span!("rafka.node_admin.node.delete.via-build", build_id = %build_id, node = %node, attempt);
                    self.retire(build_id, attempt, &node, true).instrument(span).await?;
                }
                self.records.meshes.lock().unwrap().remove(mesh);
                Ok(())
            }
        }
    }
}

/// A running node-admin: what `run` needs to keep alive and shut down.
pub struct Running {
    pub api_base: String,
    pub control: Arc<ControlPlane>,
    pub runner: Arc<AdminRunner>,
    pub membership: Membership,
    pub digest: Arc<Mutex<MeshDigest>>,
    router: IrohRouter,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Running {
    /// Say `Leaving`, stop serving and close the endpoint.
    pub async fn leave(self) {
        let mut d = self.digest.lock().unwrap().clone();
        d.status = MemberStatus::Leaving;
        d.emitted_unix_ms = now_ms();
        *self.digest.lock().unwrap() = d.clone();
        let _ = self.membership.publish(&d).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        for t in self.tasks {
            t.abort();
        }
        let _ = self.router.shutdown().await;
    }
}

/// Bring a node-admin up: identity, policy, membership, Build state, the
/// control API, the projection and the executor.
pub async fn start(cfg: AdminConfig) -> Result<Running, String> {
    let key = load_or_mint_key(&cfg.data_dir)?;
    // Identity and the two slots: assigned by the launching pipeline, or the
    // bootstrap admin's own.
    let (name, node_id, incarnation, supersedes, mesh_addr, control_addr, seeds) = match &cfg.launch {
        Some(l) => {
            let slot = |s: &str| l.endpoints.iter().find(|e| e.slot == s).map(|e| e.addr).ok_or_else(|| format!("launch assigns no `{s}` slot"));
            (l.name.clone(), l.node_id.clone(), l.incarnation.clone(), l.supersedes.clone(), slot("mesh")?, slot("control")?, l.seeds.clone())
        }
        None => (
            PathName::new(&cfg.mesh, NodeKind::NodeAdmin, 1),
            NodeId::mint(),
            IncarnationId::mint(),
            None,
            SocketAddr::new(cfg.api_bind.ip(), 0),
            cfg.api_bind,
            Vec::new(),
        ),
    };
    // Fabric policy: bootstrapped from MESH_SPAWN_TYPE, or inherited.
    let policy = match &cfg.join {
        None => FabricPolicy::bootstrap(cfg.spawn_type.as_deref()).map_err(|e| e.to_string())?,
        Some(base) => {
            let fabric = rafka_node_admin_client::NodeAdminClient::new(base.clone()).fabric().await.map_err(|e| format!("joining {base}: {e}"))?;
            let established = FabricPolicy {
                provider: match fabric.provider {
                    rafka_node_admin_client::ProviderKind::Process => ProviderKind::Process,
                    rafka_node_admin_client::ProviderKind::Container => ProviderKind::Container,
                },
            };
            FabricPolicy::inherit(established, cfg.spawn_type.as_deref()).map_err(|e| e.to_string())?
        }
    };

    // The mesh endpoint: gossip for membership and Build facts.
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(key.clone())
        .alpns(vec![iroh_gossip::ALPN.to_vec()])
        .relay_mode(iroh::RelayMode::Disabled)
        .bind_addr(mesh_addr)
        .map_err(|e| format!("mesh slot {mesh_addr}: {e}"))?
        .bind()
        .await
        .map_err(|e| format!("mesh slot {mesh_addr}: {e}"))?;
    let mesh_addr = endpoint.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap_or(mesh_addr);
    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
    let iroh_router = IrohRouter::builder(endpoint.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    let seed_addrs: Vec<EndpointAddr> = seeds
        .iter()
        .filter_map(|(k, a)| k.parse::<iroh::PublicKey>().ok().map(|pk| EndpointAddr::new(pk).with_ip_addr(*a)))
        .collect();
    let membership = Membership::join(&gossip, &endpoint, &cfg.fabric, seed_addrs.clone()).await.map_err(|e| format!("membership: {e}"))?;
    let builds = Arc::new(FabricBuildStateAdapter::join(&gossip, &endpoint, &cfg.fabric, seed_addrs).await.map_err(|e| e.to_string())?);

    // The control API.
    let listener = tokio::net::TcpListener::bind(control_addr).await.map_err(|e| format!("control slot {control_addr}: {e}"))?;
    let api_base = format!("http://{}", listener.local_addr().map_err(|e| e.to_string())?);
    let records = Arc::new(Records::default());
    let mesh_id = cfg.mesh_id.clone().map(MeshId).unwrap_or_else(MeshId::mint);
    records.meshes.lock().unwrap().insert(cfg.mesh.clone(), mesh_id.clone());
    let book = membership.book.clone();
    let control = Arc::new(ControlPlane::new(builds.clone(), project(&cfg.fabric, policy.provider, &book, &records)));

    // The deployment hand (used while this admin is fabric primary).
    let prepared = crate::deployment::prepare(policy, &cfg.fabric).await.map_err(|e| e.to_string())?;
    let mut admin_env = BTreeMap::new();
    admin_env.insert(crate::deployment::provider::SPAWN_TYPE_ENV.to_string(), format!("{:?}", policy.provider).to_lowercase());
    admin_env.insert("RAFKA_NODE_ADMIN_JOIN".to_string(), api_base.clone());
    admin_env.insert("RAFKA_BIN_DIR".to_string(), cfg.bin_dir.display().to_string());
    let data_root = cfg.data_dir.parent().map(Path::to_path_buf).unwrap_or_else(|| cfg.data_dir.clone());
    let template = LaunchTemplate {
        fabric: cfg.fabric.clone(),
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
        fabric_provider: policy.provider,
        book: book.clone(),
        me: name.clone(),
    });

    // Own digest, published on the membership cadence.
    let mut extra = BTreeMap::new();
    extra.insert(MESH_ID.to_string(), mesh_id.0.clone());
    // Ready for traffic from birth: the election claim is this instant.
    extra.insert(rafka_mesh_entity::READY_SINCE.to_string(), now_ms().to_string());
    let digest = Arc::new(Mutex::new(MeshDigest {
        fabric: cfg.fabric.clone(),
        node: MeshNode {
            node_id: node_id.clone(),
            name: name.clone(),
            fabric_id: FabricId(key.public().to_string()),
            incarnation,
            supersedes,
            endpoints: EndpointSet(vec![
                EndpointSlot { slot: "mesh".into(), addr: mesh_addr, freshness: FreshnessToken::mint() },
                EndpointSlot {
                    slot: "control".into(),
                    addr: listener.local_addr().map_err(|e| e.to_string())?,
                    freshness: FreshnessToken::mint(),
                },
            ]),
        },
        status: MemberStatus::ReadyForTraffic,
        admin_api_base: Some(api_base.clone()),
        emitted_unix_ms: now_ms(),
        extra,
    }));
    if let Some(l) = &cfg.launch {
        // The pipeline's assignment, not a re-minted one.
        digest.lock().unwrap().node.endpoints = EndpointSet(l.endpoints.clone());
    }
    let mut tasks = Vec::new();
    let d = digest.clone();
    tasks.push(membership.publish_every(Duration::from_millis(500), move || {
        let mut d = d.lock().unwrap().clone();
        d.emitted_unix_ms = now_ms();
        d
    }));

    // The projection, refreshed into the control plane's topology.
    {
        let (topology, fabric, provider, book, records) = (control.topology.clone(), cfg.fabric.clone(), policy.provider, book.clone(), records.clone());
        let elections = ElectionLog::new(name.to_string());
        tasks.push(tokio::spawn(async move {
            loop {
                let t = project(&fabric, provider, &book, &records);
                elections.observe(&t);
                *topology.write().await = t;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }));
    }
    // The executor: active while this admin is fabric primary. An admin that
    // joined an existing fabric first waits to hear the admin it joined: until
    // then its view holds only itself and would name itself fabric primary.
    {
        let exec = BuildExecutor { executor: name.to_string(), builds: builds.clone(), topology: control.topology.clone(), runner: runner.clone() };
        let (topology, me, submitted) = (control.topology.clone(), name.clone(), control.build_submitted.clone());
        let (join, book, records, fabric, provider) = (cfg.join.clone(), book.clone(), records.clone(), cfg.fabric.clone(), policy.provider);
        tasks.push(tokio::spawn(async move {
            let mut heard_fabric = join.is_none();
            loop {
                if !heard_fabric {
                    heard_fabric = book.all().iter().any(|d| d.admin_api_base.as_deref() == join.as_deref());
                    if heard_fabric {
                        tracing::info_span!("rafka.node_admin.fabric.update.via-join", node = %me, joined = join.as_deref().unwrap_or(""))
                            .in_scope(|| tracing::info!("heard the fabric; eligible to execute Builds"));
                    }
                }
                // Decide on, and plan from, the view as it is now: a cached view
                // can predate the members that make another admin primary.
                let now = project(&fabric, provider, &book, &records);
                let primary = heard_fabric && now.fabric_primary().is_some_and(|p| p.name == me);
                *topology.write().await = now;
                if primary {
                    exec.reconcile_active().await;
                }
                tokio::select! {
                    _ = submitted.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(300)) => {}
                }
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
    Ok(Running { api_base, control, runner, membership, digest, router: iroh_router, tasks })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(name: &str, status: MemberStatus) -> MeshDigest {
        let name: PathName = name.parse().unwrap();
        let mut extra = BTreeMap::new();
        extra.insert(MESH_ID.to_string(), format!("id-{}", name.mesh));
        MeshDigest {
            fabric: "fabric1".into(),
            node: MeshNode {
                node_id: NodeId::mint(),
                name: name.clone(),
                fabric_id: FabricId(format!("key-{name}")),
                incarnation: IncarnationId::mint(),
                supersedes: None,
                endpoints: EndpointSet(vec![]),
            },
            status,
            admin_api_base: (name.kind == NodeKind::NodeAdmin).then(|| format!("http://admin-{name}")),
            emitted_unix_ms: now_ms(),
            extra,
        }
    }

    fn view(digests: &[MeshDigest]) -> Topology {
        let book = DigestBook::default();
        for d in digests {
            book.record(d.clone());
        }
        project("fabric1", ProviderKind::Process, &book, &Records::default())
    }

    #[test]
    fn without_claims_the_lowest_ready_ordinal_is_primary_and_the_lowest_mesh_holds_the_fabric() {
        use MemberStatus::*;
        let t = view(&[
            digest("mesh2.admin.1", ReadyForTraffic),
            digest("mesh2.rpc.2", ReadyForTraffic),
            digest("mesh2.rpc.1", Pending),
            digest("mesh1.admin.2", ReadyForTraffic),
            digest("mesh1.admin.1", ReadyForTraffic),
            digest("mesh1.rpc.3", ReadyForTraffic),
        ]);
        let primaries: Vec<String> = t.nodes.iter().filter(|n| n.is_primary).map(|n| n.name.to_string()).collect();
        assert_eq!(primaries, ["mesh1.admin.1", "mesh1.rpc.3", "mesh2.admin.1", "mesh2.rpc.2"], "a pending member is never primary");
        assert_eq!(t.fabric_primary().map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.1"));
        assert!(t.violations().is_empty(), "{:?}", t.violations());
        assert_eq!(t.mesh_view("mesh2").unwrap().id, "id-mesh2");
        assert_eq!(t.fabric.status, ScopeStatus::ReadyForTraffic);
    }

    #[test]
    fn the_member_ready_longest_is_primary_and_a_recreated_path_does_not_take_over() {
        let claim = |name: &str, since: u64| {
            let mut d = digest(name, MemberStatus::ReadyForTraffic);
            d.extra.insert(rafka_mesh_entity::READY_SINCE.into(), since.to_string());
            d
        };
        // rpc.1 was killed and recreated: its new birth claims a later instant.
        let t = view(&[claim("mesh1.admin.1", 10), claim("mesh1.rpc.1", 900), claim("mesh1.rpc.2", 200), claim("mesh1.rpc.3", 300)]);
        let primaries: Vec<String> = t.nodes.iter().filter(|n| n.is_primary).map(|n| n.name.to_string()).collect();
        assert_eq!(primaries, ["mesh1.admin.1", "mesh1.rpc.2"]);
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
    fn removed_births_leave_the_view_and_a_newer_birth_at_the_path_does_not() {
        let book = DigestBook::default();
        let old = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(old.clone());
        let records = Records::default();
        let mut n = Node::allocated(old.node.name.clone());
        n.incarnation_id = Some(old.node.incarnation.clone());
        records.publish(n);
        records.remove(&old.node.name);
        let t = project("fabric1", ProviderKind::Process, &book, &records);
        assert!(t.node(&old.node.name).is_none(), "the removed birth is gone");
        let mut newer = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        newer.node.node_id = NodeId::mint();
        book.record(newer.clone());
        let t = project("fabric1", ProviderKind::Process, &book, &records);
        assert_eq!(t.node(&old.node.name).unwrap().incarnation_id.as_ref(), Some(&newer.node.incarnation));
    }
}
