//! i143 functional: node-admin's Build executor converges the topology after a loss, and only
//! after a proven one.
//!
//! The real chain runs end to end — `admin::reconcile_drift` (the fabric-primary inspects each
//! unheard birth's runtime and opens the next attempt of the accepted Build on proof),
//! the fabric-primary's drive (`DriveEnv::run`: the claim on its own log, `accepted::plan`, `executor_for`,
//! `build.attempt.run` to each executor in turn, hand-off, receipts),
//! `election::resolve` (the roles), and `path_investigation::target` before every create — over a world
//! fixture: a provider whose runtimes run or have exited, and a probe answering whether a birth
//! answers. Only the world is a fixture; every decision is the product code's.
//!
//! The fabric-primary's drift repairs node-admins only: an exited member of a mesh with a live
//! mesh-primary is that mesh-primary's recovery (node-recovery.md; R-BI7), opened and claimed at its
//! own Build door (`ControlPlane::open_recovery`) and run by its own executor in its mesh's drift lane
//! (`build_run::run_recovery`), with no fabric-primary taking part.
//!
//! The cells: an exited rpc node is recovered once by its mesh-primary, whatever its exit code;
//! a silent node whose runtime runs opens no attempt and is never replaced; a lost mesh-primary admin and a lost fabric-primary admin are
//! recovered by the right executor; members unheard through a lost forwarder are targeted, never
//! duplicated; a path the view does not hold but where a runtime runs is held. After every
//! loss: exactly the lost paths have new births, every path of the accepted topology has
//! exactly one live birth, and the accepted Build is complete (a recovery's attempt in its mesh's
//! lane, a drift repair's in the fabric lane).

use rdm_core::{MeshDigest, MeshNode, MemberStatus, RuntimeFact, RuntimeLocator, RuntimeProvider};
use rdm_transport::topology::DigestView;
use rdm_admin::accepted::{AcceptedStore, FabricTopology, MeshTopology};
use rdm_admin::build::{BuildId, BuildOperation};
use rdm_admin::build_state::{BuildState, BuildStateAdapter as _, MemoryBuildStateAdapter};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, Inspected, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::build_drive::{Drive, DriveEnd, DriveEnv, OpenGate};
use rdm_admin::executor::OperationRunner;
use rdm_admin::node_investigation::Finding;
use rdm_admin::path_investigation::{investigate_path, TargetOutcome, PathProbe};
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

const DOMAIN: &str = "process:test-world";

/// One runtime of the world: which birth it is, and whether it runs.
#[derive(Debug, Clone)]
struct Runtime {
    path: PathName,
    node_id: String,
    status: DeploymentStatus,
    answers: bool,
}

/// The world the admins act in: runtimes by pid.
#[derive(Default)]
struct World {
    runtimes: Mutex<BTreeMap<u32, Runtime>>,
    next_pid: Mutex<u32>,
}

impl World {
    fn add(&self, path: &PathName, node_id: &str) -> u32 {
        let mut p = self.next_pid.lock().unwrap();
        *p += 1;
        let pid = 1000 + *p;
        self.runtimes.lock().unwrap().insert(pid, Runtime { path: path.clone(), node_id: node_id.into(), status: DeploymentStatus::Running, answers: true });
        pid
    }
    fn of(&self, node_id: &str) -> Option<Runtime> {
        self.runtimes.lock().unwrap().values().find(|r| r.node_id == node_id).cloned()
    }
    /// The runtime is killed: it exited, and it answers nothing.
    fn kill(&self, node_id: &str) {
        for r in self.runtimes.lock().unwrap().values_mut().filter(|r| r.node_id == node_id) {
            r.status = DeploymentStatus::Exited { code: None };
            r.answers = false;
        }
    }
    /// The runtime exited with `code`.
    fn exit_with(&self, node_id: &str, code: Option<i32>) {
        for r in self.runtimes.lock().unwrap().values_mut().filter(|r| r.node_id == node_id) {
            r.status = DeploymentStatus::Exited { code };
            r.answers = false;
        }
    }
    /// The runtime is frozen: it runs, and it answers nothing.
    fn freeze(&self, node_id: &str) {
        for r in self.runtimes.lock().unwrap().values_mut().filter(|r| r.node_id == node_id) {
            r.answers = false;
        }
    }
}

/// The provider: inspection is the world's.
struct Provider(Arc<World>);

/// The drive's departure proof over the estate: an executor's departure is proven by nothing here
/// (the drive's executors in these cells never leave).
struct WorldExits;

#[async_trait::async_trait]
impl rdm_admin::build_drive::ExecutorInspection for WorldExits {
    async fn exited_or_os_stopped(&self, executor: &rdm_core::framing::Birth) -> Result<(), String> {
        Err(format!("{executor:?}'s runtime is not proven exited"))
    }
}

#[async_trait::async_trait]
impl DeploymentProvider for Provider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        DOMAIN.into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn { node: spec.node.clone(), reason: "births are made by the runner in this world".into() })
    }
    async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        Ok(())
    }
    async fn inspect(&self, h: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
        h.pid.and_then(|p| self.0.runtimes.lock().unwrap().get(&p).map(|r| r.status.clone())).unwrap_or(DeploymentStatus::Unknown)
    }
    async fn signal_stop(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
    }
}

/// The target's questions, answered by the world.
struct Probe(Arc<World>);

#[async_trait::async_trait]
impl PathProbe for Probe {
    async fn investigate(&self, node: &Node) -> Finding {
        if self.0.of(node.node_id.as_str()).is_some_and(|r| r.answers) {
            return Finding::Reached { answer: rdm_core::status::StatusReply::Applied, carrier: None };
        }
        match self.0.of(node.node_id.as_str()).map(|r| r.status) {
            None => Finding::Unproven { reason: format!("{} has no runtime in this world", node.node_id) },
            Some(DeploymentStatus::Running) => Finding::Running,
            Some(DeploymentStatus::OsStopped) => Finding::OsStopped,
            Some(DeploymentStatus::Exited { code }) => Finding::Exited { code },
            Some(DeploymentStatus::Unknown) => Finding::Unproven { reason: "the provider cannot tell its runtime's state: Unknown".into() },
        }
    }
}

/// The node id of the birth `me` names in `t`: the drift pass's authority identity.
fn own_id(t: &rdm_admin::topology::Topology, me: &PathName) -> rdm_core::NodeId {
    t.node(me).map(|n| n.node_id.clone()).unwrap_or_else(|| panic!("{me:?} is not in the view"))
}

fn fact(pid: u32) -> RuntimeFact {
    RuntimeFact { deployment_id: rdm_core::fixture_crockford60(&format!("dep-{pid}")), provider: RuntimeProvider::Process, control_domain: DOMAIN.into(), locator: RuntimeLocator::Process { pid, start: 1 } }
}

/// The estate: the world, the accepted Build, the admins' shared view and the topology view.
struct Estate {
    world: Arc<World>,
    builds: Arc<MemoryBuildStateAdapter>,
    accepted: Arc<AcceptedStore>,
    view: Arc<RwLock<Topology>>,
    digest_view: DigestView,
    provider: Provider,
    runner: Arc<Runner>,
    dispatch: Arc<crate::loopback::Loopback>,
    /// Each mesh's in-flight row (R-BI7 caveat 3), one set of rows for every admin of the estate, as
    /// the fabric's shared store root holds them.
    claims: Arc<dyn rdm_admin::storage::MeshStorage>,
    /// The durable runtime rows of births the fabric-primary's view no longer holds: what its drift pass
    /// reads beside its view (`reconcile_drift`'s `durable`).
    rows_only: std::sync::Mutex<Vec<rdm_admin::storage::RuntimeRow>>,
    /// The application the fabric-primary's drive sends its Build notices to (Q256).
    told: crate::Told,
    /// Every birth's lifecycle record, as the fabric's shared store root holds them: claimed for each
    /// birth the estate's view has held.
    rows: Arc<rdm_admin::storage::MemoryNodesStorage>,
}

const SHAPE: [(&str, u32, u32); 2] = [("mesh1", 2, 3), ("mesh2", 2, 3)];

/// An rpc node of `mesh` that holds no role in the view (the lowest name among them).
async fn secondary_rpc(e: &Estate, mesh: &str) -> PathName {
    let t = e.view.read().await;
    t.nodes.iter().filter(|n| n.mesh == mesh && n.kind == NodeKind::RpcNode && !n.is_primary).map(|n| n.name.clone()).min().expect("an rpc node holding no primary role")
}

/// Every path of the accepted shape born, live, held in the view and the topology view; roles elected.
async fn estate() -> Estate {
    let world = Arc::new(World::default());
    let fabric_id = FabricId::mint();
    let topology = FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: SHAPE.iter().map(|(m, a, r)| ((*m).to_string(), MeshTopology::of(&rdm_admin::build::MeshDesired::of(*m, [(rdm_core::NodeKind::NodeAdmin, *a), (rdm_core::NodeKind::RpcNode, *r)])))).collect() };
    let mut nodes = Vec::new();
    let digest_view = DigestView::default();
    for m in topology.meshes.values() {
        for p in &m.nodes {
            nodes.push(birth(&world, &digest_view, &fabric_id, &p.clone(), &m.name, m.spec(p).unwrap().kind, None));
        }
    }
    rdm_admin::election::resolve(&mut nodes);
    let view = Arc::new(RwLock::new(Topology {
        fabric: Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: SHAPE.iter().map(|(m, _, _)| Mesh { id: Some(MeshId::mint()), name: (*m).into(), status: ScopeStatus::Ready }).collect(),
        nodes,
    }));
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let fp = view.read().await.fabric_primary().unwrap().clone();
    let fp_birth = rdm_core::framing::Birth { node_id: fp.node_id, incarnation: fp.incarnation_id.unwrap() };
    let accepted = AcceptedStore::seeded(&*builds, fabric_id.clone(), topology, &fp_birth, &fp.mesh).await.unwrap();
    let runner = Arc::new(Runner { world: world.clone(), view: view.clone(), digest_view: digest_view.clone(), fabric_id, ran: Mutex::new(Vec::new()), born: Mutex::new(Vec::new()), builds: builds.clone(), hear_after_birth: std::sync::atomic::AtomicBool::new(false), recovering: Mutex::new(Vec::new()), fail_recovery_create: std::sync::atomic::AtomicBool::new(false) });
    let dispatch = crate::loopback::Loopback::new(builds.clone(), builds.clone(), view.clone(), runner.clone());
    Estate { provider: Provider(world.clone()), world, builds, accepted, view, digest_view, runner, dispatch, rows_only: Default::default(), claims: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()), told: crate::Told::default(), rows: Arc::default() }
}

/// A new birth at `path`: a runtime in the world, a digest in the topology view, a live node.
fn birth(world: &World, digest_view: &DigestView, fabric_id: &FabricId, path: &PathName, mesh: &str, kind: NodeKind, supersedes: Option<IncarnationId>) -> Node {
    let mut n = Node::allocated(path.clone(), mesh, kind);
    n.node_id = NodeId::mint();
    n.incarnation_id = Some(IncarnationId::mint());
    n.endpoint_id = Some(EndpointId(iroh::SecretKey::generate().public().to_string()));
    n.status = NodeStatus::Ready;
    let pid = world.add(path, n.node_id.as_str());
    digest_view.record(MeshDigest { held_hash: Default::default(),
        fabric_id: fabric_id.clone(),
        node: MeshNode { mesh: n.mesh.clone(), kind: n.kind,
            node_id: n.node_id.clone(),
            name: n.name.clone(),
            endpoint_id: n.endpoint_id.clone().unwrap(),
            transport_addr: "127.0.0.1:1".parse().unwrap(),
            incarnation: n.incarnation_id.clone().unwrap(),
            supersedes,
            runtime: Some(fact(pid)),
        },
        status: MemberStatus::Ready,
        node_state: Default::default(),
        admin_api_base: None,
        emitted_at_rafka_ms: 0,
        digest_seq: 1,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    });
    n
}

/// Realises `CreateNode` as the admin does (a Build's node.create is the create workflow,
/// `AdminRunner::step`): the real target first; a birth only when it clears.
struct Runner {
    world: Arc<World>,
    view: Arc<RwLock<Topology>>,
    digest_view: DigestView,
    fabric_id: FabricId,
    ran: Mutex<Vec<(PathName, BuildOperation, TargetOutcome)>>,
    born: Mutex<Vec<PathName>>,
    /// The Build log the recovery's receipts are written to, as the admin's delete, create and update
    /// workflows write them.
    builds: Arc<MemoryBuildStateAdapter>,
    /// Topology repair runs right after every birth, before the attempt's next operation: what a
    /// recovered forwarder does to the executor's view mid-attempt.
    hear_after_birth: std::sync::atomic::AtomicBool,
    /// The operations of every recovery run, by key, in the order the executor ran them.
    recovering: Mutex<Vec<rdm_core::OperationId>>,
    /// The next recovery's create fails once (its launch never came up), after its delete completed.
    fail_recovery_create: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl OperationRunner for Runner {
    async fn run(&self, build_id: &BuildId, lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        // A node recovery's three operations (node-recovery.md "Steps"), each writing the receipts its
        // workflow writes: the delete's NodeDeleted naming the exited birth and its Complete, the
        // create's Complete, the update's Complete. The fabric-primary's planning and recovery dedup
        // read them.
        let receipt = |operation: rdm_core::OperationId, step: &str, output: Option<serde_json::Value>| rdm_admin::build_state::BuildStepReceipt {
            mesh: lane.map(str::to_string),
            build_id: build_id.clone(),
            attempt,
            operation,
            step: step.into(),
            outcome: rdm_admin::build_state::StepOutcome::Complete,
            output,
            executor: None,
        };
        use rdm_admin::build_state::BuildStateAdapter as _;
        let admitted_operation = op.key();
        let created;
        let op = match op {
            BuildOperation::Step { mesh, kind, step: rdm_core::build::Step::Create { node, .. }, .. } => {
                created = BuildOperation::CreateNode { mesh: mesh.clone(), kind: *kind, node: node.clone(), over: None, executable: None, entry: None };
                &created
            }
            other => other,
        };
        match op {
            // node.delete of the exited instance (node-delete.md "Crashed target"): no RPC, no stop
            // frames, no probe; the provider's exited verdict is the target. A runtime that still runs is
            // no proof: refused by name, as the admin's crashed-target delete inspects before it deletes.
            BuildOperation::DeleteLiveNode { node: path, exited: Some(from), .. } => {
                self.recovering.lock().unwrap().push(admitted_operation.clone());
                if let Some(r) = self.world.runtimes.lock().unwrap().values().find(|r| r.node_id == from.node_id.as_str()) {
                    if matches!(r.status, DeploymentStatus::Running) {
                        return Err(format!("{path:?}'s birth {} at {} still runs: not proven exited", from.node_id, from.incarnation.as_str()).into());
                    }
                }
                self.builds.append_step_receipt(&receipt(admitted_operation.clone(), "NodeDeleted", Some(serde_json::to_value(rdm_core::LifecycleOp { mesh: op.mesh().unwrap().to_owned(), kind: op.kind().unwrap(), build_id: build_id.to_string(), attempt, operation: admitted_operation.clone(), node_id: from.node_id.clone(), incarnation: from.incarnation.clone(), name: path.clone(), event_at_rafka_ms: 0, traceparent: None, tracestate: None }).unwrap()))).await.map_err(|e| e.to_string())?;
                self.view.write().await.nodes.retain(|x| !(x.node_id == from.node_id && x.incarnation_id.as_ref() == Some(&from.incarnation)));
                self.builds.append_step_receipt(&receipt(admitted_operation.clone(), "Complete", None)).await.map_err(|e| e.to_string())?;
                return Ok(());
            }
            // node.create at the same path and name with a new node.id and no supersedes.
            BuildOperation::CreateNode { node: path, over: Some(_), .. } => {
                self.recovering.lock().unwrap().push(admitted_operation.clone());
                if self.fail_recovery_create.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    return Err(format!("{path:?}: the recovered node's launch never came up").into());
                }
                let n = birth(&self.world, &self.digest_view, &self.fabric_id, &path.clone(), op.mesh().unwrap(), op.kind().unwrap(), None);
                let mut v = self.view.write().await;
                v.nodes.retain(|x| x.name != *path);
                v.nodes.push(n);
                rdm_admin::election::resolve(&mut v.nodes);
                drop(v);
                self.born.lock().unwrap().push(path.clone());
                self.builds.append_step_receipt(&receipt(admitted_operation.clone(), "Complete", None)).await.map_err(|e| e.to_string())?;
                return Ok(());
            }
            _ => {}
        }
        // A creation at a path whose exact birth the provider's proof clears: the target stands for the
        // exact terminal proof.
        let BuildOperation::CreateNode { node: path, over: None, .. } = op else { return Err(format!("unexpected {op:?}").into()) };
        let prev = self.view.read().await.node(path).cloned();
        let out = investigate_path(path, prev.clone(), &Probe(self.world.clone())).await;
        self.ran.lock().unwrap().push((path.clone(), op.clone(), out.clone()));
        if matches!(out, TargetOutcome::Clear { .. }) {
            let n = birth(&self.world, &self.digest_view, &self.fabric_id, &path.clone(), op.mesh().unwrap(), op.kind().unwrap(), prev.and_then(|p| p.incarnation_id));
            let mut v = self.view.write().await;
            v.nodes.retain(|x| x.name != *path);
            v.nodes.push(n);
            rdm_admin::election::resolve(&mut v.nodes);
            self.born.lock().unwrap().push(path.clone());
            drop(v);
            if self.hear_after_birth.load(std::sync::atomic::Ordering::SeqCst) {
                hear(&self.world, &self.view).await;
            }
        }
        Ok(())
    }
}

/// Topology repair: every `Dead` node whose runtime runs and answers, in a mesh with a live admin
/// to forward it, is heard again.
async fn hear(world: &World, view: &RwLock<Topology>) {
    let mut v = view.write().await;
    let forwarded: BTreeSet<String> = v.nodes.iter().filter(|n| n.kind == NodeKind::NodeAdmin && n.status.is_live()).map(|n| n.mesh.clone()).collect();
    let mut changed = false;
    for n in v.nodes.iter_mut().filter(|n| n.status == NodeStatus::PendingReconnect && forwarded.contains(&n.mesh)) {
        if world.of(n.node_id.as_str()).is_some_and(|r| matches!(r.status, DeploymentStatus::Running) && r.answers) {
            n.status = NodeStatus::Ready;
            changed = true;
        }
    }
    if changed {
        rdm_admin::election::resolve(&mut v.nodes);
    }
}

impl Estate {
    async fn hear(&self) {
        hear(&self.world, &self.view).await;
    }

    /// A view change: `paths` are unheard (Dead) in the view; the roles are elected again.
    async fn unheard(&self, paths: &[&PathName]) {
        let mut v = self.view.write().await;
        for n in v.nodes.iter_mut().filter(|n| paths.contains(&&n.name)) {
            n.status = NodeStatus::PendingReconnect;
        }
        rdm_admin::election::resolve(&mut v.nodes);
    }

    /// `path`'s birth leaves the fabric-primary's view: only its durable runtime row names it now, the
    /// drift pass's own case (Q335: a birth a view holds is the primary watch's or its mesh-primary's).
    async fn to_rows_only(&self, path: &PathName) {
        let id = self.node_id(path).await;
        let (d, _) = self.digest_view.get(&id).expect("the birth's digest is held");
        self.rows_only.lock().unwrap().push(rdm_admin::storage::RuntimeRow { kind: d.node.kind, mesh: d.node.mesh.clone(), node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime: d.node.runtime.clone().unwrap(), data_dir: None });
        let mut v = self.view.write().await;
        v.nodes.retain(|n| n.name != *path);
        rdm_admin::election::resolve(&mut v.nodes);
    }

    async fn node_id(&self, path: &PathName) -> String {
        self.view.read().await.nodes.iter().find(|n| &n.name == path).unwrap().node_id.to_string()
    }

    async fn fabric_primary(&self) -> Option<PathName> {
        self.view.read().await.fabric_primary().map(|n| n.name.clone())
    }

    /// The fabric-primary's drift pass, as the admin's executor loop runs it.
    async fn drift(&self) -> Option<(BuildId, u32)> {
        let me: PathName = self.fabric_primary().await?;
        let t = self.view.read().await.clone();
        let mut started = HashSet::new();
        let rows = self.rows_only.lock().unwrap().clone();
        rdm_admin::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &self.digest_view, &self.provider, &*self.builds, &rdm_admin::build_claim::AttemptContexts::in_memory(), &tokio::sync::Mutex::new(()), &rows, &mut started, &mut Default::default(), &|_| None, &Default::default()).await
    }

    /// The fabric-primary drives the accepted Build until no Build is active: the claim on its own
    /// log, `build.attempt.run` to each executor `executor_for` names, hand-offs and receipts are
    /// the product's. Between passes the topology view does what gossip does in the product: a runtime
    /// that runs and answers, in a mesh that has a live admin to forward its digests, is heard
    /// again and leaves `Dead` in the view.
    async fn converge(&self) {
        self.converge_under(Arc::new(OpenGate)).await
    }

    /// [`Self::converge`] with the fabric-primary's drive reading `gate`: the target on a peer mesh the
    /// investigation holds unheard is the gate's `peer_mesh`.
    async fn converge_under(&self, gate: Arc<dyn rdm_admin::build_drive::Gate>) {
        for _ in 0..8 {
            let env = self.drive_env_under(gate.clone()).await;
            if let Some(current) = self.builds.current().await.unwrap() {
                if matches!(current.state, BuildState::Pending | BuildState::Running) {
                    let done = env.run(&Drive::detached(current.build_id.clone())).await;
                    if std::env::var("CONVERGE_TRACE").is_ok() {
                        eprintln!("converge: {done:?}");
                    }
                }
            }
            self.hear().await;
            let current = self.builds.current().await.unwrap().unwrap();
            if !matches!(current.state, BuildState::Pending | BuildState::Running) {
                return;
            }
        }
        panic!("the accepted Build did not finish: {:?}", self.builds.current().await.unwrap().map(|b| (b.state, b.attempt)));
    }

    /// The fabric-primary's drive over the estate: the claim decided at its own door, as the
    /// product decides it.
    async fn drive_env(&self) -> Arc<DriveEnv> {
        self.drive_env_under(Arc::new(OpenGate)).await
    }

    async fn drive_env_under(&self, gate: Arc<dyn rdm_admin::build_drive::Gate>) -> Arc<DriveEnv> {
        let primary: PathName = self.fabric_primary().await.expect("a fabric-primary");
        let primary_id = own_id(&*self.view.read().await, &primary);
        let door = Arc::new(rdm_admin::build_claim::ClaimDoor {
            node_id: primary_id.clone(),
            incarnation: self.view.read().await.node(&primary).unwrap().incarnation_id.clone().unwrap(),
            me: primary.clone(),
            topology: self.view.clone(),
            builds: self.builds.clone(),
            contexts: Arc::new(rdm_admin::build_claim::AttemptContexts::in_memory()),
            claims: self.claims.clone(),
            claimed: Default::default(),
        });
        Arc::new(DriveEnv { me_mesh: self.view.read().await.node(&primary).unwrap().mesh.clone(),
            me: primary.clone(),
            topology: self.view.clone(),
            builds: self.builds.clone(),
            door,
            dispatcher: self.dispatch.clone(),
            gate,
            departure: Arc::new(WorldExits),
            verdicts: Arc::new(crate::loopback::NoVerdicts),
            notices: self.told.notices(),
            turns: rdm_admin::stop_gate::StopGate::new(),
        })
    }

    /// One pass of the fabric-primary's drive over the accepted Build.
    async fn drive_once(&self) -> DriveEnd {
        let current = self.builds.current().await.unwrap().expect("an accepted Build");
        self.drive_env().await.run(&Drive::detached(current.build_id)).await
    }

    /// `path`, a node-admin whose runtime runs and answers, is heard again; `lost`, its mesh's
    /// primary proven exited, holds no role: its mesh elects `path` (election.rs: lowest Ready).
    async fn elect(&self, path: &PathName, lost: &PathName) {
        let mut v = self.view.write().await;
        for n in v.nodes.iter_mut() {
            if &n.name == path {
                n.status = NodeStatus::Ready;
            } else if &n.name == lost {
                n.status = NodeStatus::Dead;
            }
        }
        rdm_admin::election::resolve(&mut v.nodes);
    }

    /// The fabric-primary's first drive pass after a peer mesh lost its primary while its other
    /// node-admin `survivor` is held silent (R-D1; R-A2 "cross-mesh only when the every node-admin of the mesh is
    /// proven lost"; elections §5: the role goes to the lowest Ready NodeId of its own mesh): the
    /// drive stalls naming the mesh and `survivor`, nothing is dispatched and nothing is born; then
    /// `survivor` is primary by its own mesh and is the executor from there.
    async fn stalls_until_elected(&self, lost_mesh: &str, lost: &PathName) -> PathName {
        let survivor = self.view.read().await.nodes_of_type(lost_mesh, NodeKind::NodeAdmin).map(|n| n.name.clone()).find(|n| n != lost).expect("a second admin in the lost mesh");
        match self.drive_once().await {
            DriveEnd::Stalled(why) => assert!(why.contains(lost_mesh) && why.contains(self.node_id(&survivor).await.as_str()), "the stall names the mesh and its held admin: {why}"),
            other => panic!("the fabric-primary never acts on {lost_mesh} while {survivor:?} is held: {other:?}"),
        }
        assert!(self.dispatch.dispatched.lock().unwrap().is_empty() && self.born().is_empty(), "nothing dispatched and nothing born while {survivor:?} is held");
        self.elect(&survivor, lost).await;
        assert_eq!(self.mesh_primary_of(&survivor).await, survivor, "{lost_mesh} elects its own surviving admin");
        survivor
    }

    /// Every attempt the drive dispatched ran on `executor`.
    async fn executed_only_by(&self, executor: &PathName) {
        let node = self.view.read().await.node(executor).expect("fixture executor").clone();
        let executor = rdm_core::framing::Birth { node_id: node.node_id, incarnation: node.incarnation_id.unwrap() };
        let dispatched = self.dispatch.dispatched.lock().unwrap().clone();
        assert!(!dispatched.is_empty() && dispatched.iter().all(|(x, _)| x == &executor), "the primary same-mesh admin executes the recovery: {dispatched:?}");
    }

    /// The topology holds: every path of the accepted shape has exactly one live birth.
    async fn holds_the_shape(&self) {
        let v = self.view.read().await;
        let mut per_path: BTreeMap<PathName, usize> = BTreeMap::new();
        for n in v.nodes.iter().filter(|n| n.status.is_live()) {
            *per_path.entry(n.name.clone()).or_default() += 1;
        }
        let want: BTreeSet<PathName> = SHAPE.iter().flat_map(|(m, a, r)| MeshTopology::of(&rdm_admin::build::MeshDesired::of(*m, [(rdm_core::NodeKind::NodeAdmin, *a), (rdm_core::NodeKind::RpcNode, *r)])).nodes.into_iter().map(|p| p.clone())).collect();
        assert_eq!(per_path.keys().cloned().collect::<BTreeSet<_>>(), want, "every accepted path is live");
        assert!(per_path.values().all(|c| *c == 1), "one live birth per path: {per_path:?}");
        // And the world: one running runtime per path.
        let mut running: BTreeMap<PathName, usize> = BTreeMap::new();
        for r in self.world.runtimes.lock().unwrap().values().filter(|r| matches!(r.status, DeploymentStatus::Running)) {
            *running.entry(r.path.clone()).or_default() += 1;
        }
        assert!(running.values().all(|c| *c == 1), "one running runtime per path in the world: {running:?}");
    }

    /// `me`'s Build door over the estate's Build log, view, accepted Build and in-flight rows.
    async fn door_at(&self, me: &PathName) -> Arc<rdm_admin::control_plane::ControlPlane> {
        // Each birth the view holds has its lifecycle record claimed, as its create or join claims it.
        for n in self.view.read().await.births() {
            if rdm_admin::storage::NodesStorage::node(&*self.rows, &n.node_id).await.unwrap().is_none() {
                let row = rdm_admin::storage::NodeRow::new(n.name.clone(), n.mesh.clone(), n.kind, n.node_id.clone(), n.status);
                let _ = rdm_admin::node_rows::write_record(&*self.rows, &row).await;
            }
        }
        let mut cp = crate::control_plane(self.builds.clone(), self.accepted.clone(), me.clone(), self.view.read().await.clone(), crate::adopted_time());
        cp.topology = self.view.clone();
        cp.claims = self.claims.clone();
        let _ = cp.node_rows.set(self.rows.clone());
        Arc::new(cp)
    }

    /// The admin that recovers `path` (node-recovery.md "Reconciler"; mesh-recovery.md:8-9): the
    /// admin its mesh elects in its place for a node-admin, else its mesh's primary.
    async fn recoverer_of(&self, path: &PathName) -> PathName {
        let p = path.clone();
        let v = self.view.read().await;
        let recoverer = if v.node(&p).unwrap().kind == NodeKind::NodeAdmin { rdm_admin::executor::node_admin_recoverer(&p, &v.node(&p).unwrap().mesh, &v).and_then(|id| v.nodes.iter().find(|n| n.node_id == id).map(|n| n.name.clone())) } else { v.node_type_primary(&v.node(&p).unwrap().mesh, NodeKind::NodeAdmin).map(|n| n.name.clone()) };
        recoverer.expect("a live admin recovers the path")
    }

    /// The recoverer's own open of `path`'s proven-exited birth (node-recovery.md sequence step 1 as
    /// R-BI7 moves it): opened and claimed at its own door, not run.
    async fn submit_recovery(&self, path: &PathName) -> Result<rdm_admin::control_plane::Recovery, rdm_admin::control_plane::Refusal> {
        let n = self.view.read().await.node(path).cloned().expect("the exited birth is in the view");
        self.door_at(&self.recoverer_of(path).await).await.open_recovery("node-recovery", n.name.clone(), n.node_id.clone(), n.incarnation_id.clone().unwrap()).await
    }

    /// The recoverer runs the recovery attempt `opened` (opened at its door) through its own executor
    /// to its terminal (`build_run::run_opened_recovery`).
    async fn run_opened(&self, recoverer: &PathName, opened: rdm_admin::control_plane::Opened) -> rdm_core::build::BuildReply {
        let door = self.dispatch.door(&self.view.read().await.node(&recoverer).expect("fixture recoverer").birth().unwrap());
        match rdm_admin::build_run::run_opened_recovery(&*self.door_at(recoverer).await, &door, opened).await {
            Ok(rdm_admin::build_run::Recovered::Ran { terminal, .. }) => terminal,
            other => panic!("{recoverer:?} runs the recovery it opened to a terminal: {other:?}"),
        }
    }

    /// `path`'s recoverer opens and claims its recovery at its own door and runs it through its own
    /// executor to the terminal (`build_run::run_recovery`, R-BI7): no fabric-primary takes part.
    async fn recover(&self, path: &PathName) -> rdm_admin::control_plane::Opened {
        let n = self.view.read().await.node(path).cloned().expect("the exited birth is in the view");
        let recoverer = self.recoverer_of(path).await;
        let door = self.dispatch.door(&self.view.read().await.node(&recoverer).expect("fixture recoverer").birth().unwrap());
        match rdm_admin::build_run::run_recovery(&*self.door_at(&recoverer).await, &door, "node-recovery", n.name.clone(), n.node_id.clone(), n.incarnation_id.clone().unwrap()).await {
            Ok(rdm_admin::build_run::Recovered::Ran { opened, .. }) => opened,
            other => panic!("{recoverer:?} runs {path:?}'s recovery to a terminal: {other:?}"),
        }
    }

    /// `mesh`'s drift lane of the current Build.
    async fn lane(&self, mesh: &str) -> rdm_admin::build_state::Lane {
        self.builds.current().await.unwrap().unwrap().lanes.get(mesh).cloned().unwrap_or_else(|| panic!("the current Build holds no lane of {mesh}"))
    }

    /// The recovery of `path` ran in its mesh's lane, claimed and executed by `recoverer` alone, and
    /// the fabric-primary's drive dispatched nothing.
    async fn recovered_by(&self, path: &PathName, recoverer: &PathName) {
        let mesh = self.view.read().await.node(path).expect("fixture node").mesh.clone();
        let lane = self.lane(&mesh).await;
        let node = self.view.read().await.node(recoverer).expect("fixture recoverer").clone();
        let birth = rdm_core::framing::Birth { node_id: node.node_id.clone(), incarnation: node.incarnation_id.unwrap() };
        assert_eq!((lane.executor.as_ref(), lane.claimant.as_ref()), (Some(&birth), Some(&node.node_id)), "{mesh}'s lane is claimed and run by {recoverer:?}: {lane:?}");
        assert!(self.dispatch.dispatched.lock().unwrap().is_empty(), "the fabric-primary dispatched nothing: {:?}", self.dispatch.dispatched.lock().unwrap());
    }

    /// The mesh-primary of `path`'s mesh.
    async fn mesh_primary_of(&self, path: &PathName) -> PathName {
        let mesh = self.view.read().await.node(path).expect("fixture node").mesh.clone();
        self.view.read().await.node_type_primary(&mesh, NodeKind::NodeAdmin).unwrap().name.clone()
    }

    /// The node-admin of the fabric-primary's own mesh that is not the fabric-primary: a birth the
    /// fabric-primary's drift proves and repairs (its own mesh is never deferred or held).
    async fn sibling_admin(&self) -> PathName {
        let fp = self.fabric_primary().await.expect("a fabric-primary");
        let mesh = self.view.read().await.node(&fp).expect("fixture primary").mesh.clone();
        self.view.read().await.nodes_of_type(&mesh, NodeKind::NodeAdmin).map(|n| n.name.clone()).find(|n| *n != fp).expect("a second admin in the fabric-primary's mesh")
    }

    fn born(&self) -> Vec<PathName> {
        self.runner.born.lock().unwrap().clone()
    }

    fn investigations_of(&self, path: &PathName) -> Vec<TargetOutcome> {
        self.runner.ran.lock().unwrap().iter().filter(|(p, _, _)| p == path).map(|(_, _, o)| o.clone()).collect()
    }
}

/// CONTRACT (node-recovery.md, "Steps"; R-BI7): an exited ordinary member is its mesh-primary's to
/// recover. The fabric-primary's drift opens nothing for it; the mesh-primary opens and claims the
/// recovery at its own door as attempt 1 of its mesh's lane of the same Build (reason Recovery) and
/// runs it itself, as two operations in order: node.delete of the exited instance
/// (`node.delete:<path>`), then node.create at the same separate path and name; no
/// node.update follows (proposal node-properties-own-commands.md §5 item 8). The fabric-primary
/// dispatches nothing and the fabric lane is untouched.
/// Exactly the lost path has a new node (new node.id), `path_investigation::target` probes nothing and nothing else is
/// created.
/// An rpc node's recovery by its mesh-primary reopens nothing: the Build stays Complete and the application is sent no Build
/// notice (fabric-node-hooks.md `on_build_started`, `on_build_ended`; Q256, Q258).
#[tokio::test]
async fn an_exited_rpc_node_is_recovered_once_by_its_mesh_primary_and_nothing_else_is_created() {
    let e = estate().await;
    let before = e.builds.current().await.unwrap().unwrap();
    let old = e.node_id(&PathName::new("mesh1.rpc", "2").unwrap()).await;
    let exited = e.view.read().await.node(&rdm_core::PathName::new("mesh1.rpc", "2").unwrap()).cloned().unwrap();
    e.world.kill(&old);
    e.unheard(&[&PathName::new("mesh1.rpc", "2").unwrap()]).await;
    assert_eq!(e.drift().await, None, "the fabric-primary repairs no ordinary member");
    let mp = e.mesh_primary_of(&PathName::new("mesh1.rpc", "2").unwrap()).await;
    let opened = e.recover(&PathName::new("mesh1.rpc", "2").unwrap()).await;
    // node-recovery.md sequence step 1: a repeat submit for the same exited birth answers
    // AlreadyApplied from the recovery's receipts, naming its attempt, and opens nothing.
    let repeat = e.door_at(&mp).await.open_recovery("node-recovery", exited.name.clone(), exited.node_id.clone(), exited.incarnation_id.clone().unwrap()).await;
    assert!(matches!(&repeat, Ok(rdm_admin::control_plane::Recovery::AlreadyApplied(o)) if o.attempt == opened.attempt && o.mesh == opened.mesh), "{repeat:?}");
    assert_eq!((opened.build_id.clone(), opened.mesh.as_deref(), opened.attempt), (before.build_id.clone(), Some("mesh1"), 1), "the same Build, attempt 1 of mesh1's lane");
    e.recovered_by(&PathName::new("mesh1.rpc", "2").unwrap(), &mp).await;
    assert_eq!(e.runner.recovering.lock().unwrap().clone(), vec![rdm_core::OperationId::node("node.delete", &exited.node_id), rdm_core::OperationId::location("node.create", &exited.name)], "node.delete, then node.create, once each, and nothing after");
    assert_eq!(e.born(), vec![PathName::new("mesh1.rpc", "2").unwrap()], "exactly the lost path is recovered");
    assert_eq!(e.investigations_of(&PathName::new("mesh1.rpc", "2").unwrap()), vec![], "the exited verdict targets the old birth: no probe");
    assert_ne!(e.node_id(&PathName::new("mesh1.rpc", "2").unwrap()).await, old, "a new node, not the old one");
    e.holds_the_shape().await;
    let after = e.builds.current().await.unwrap().unwrap();
    assert_eq!((after.build_id, after.attempt, after.state), (before.build_id, before.attempt, BuildState::Complete), "the fabric lane is untouched");
    let lane = e.lane("mesh1").await;
    assert_eq!((lane.attempt, lane.state), (1, BuildState::Complete), "mesh1's lane: {lane:?}");
    // An rpc node's recovery by its mesh-primary reopens nothing (Q256, Q258): the fabric-primary's pass finds no Build in
    // progress, the Build stays Complete, and the application is sent no Build notice.
    e.converge().await;
    assert_eq!(e.builds.current().await.unwrap().unwrap().state, BuildState::Complete, "the Build stays completed");
    let told = e.told.settled(0).await;
    assert!(told.is_empty(), "an rpc node's recovery by its mesh-primary sends no on_build_started or on_build_ended: {told:?}");
}

/// CONTRACT (node-recovery.md "Steps", "Failure and receipts"; rdm-code-conformance.md §2 item 4): a
/// recovery whose node.create fails after its node.delete completed is not a recovery yet. A repeat
/// submit for the exited birth is not answered AlreadyApplied (no node was created at the path): it
/// opens the next attempt of the same Build, which runs only what is left, the create, never the
/// delete again. Once the create completes, a repeat submit answers AlreadyApplied naming that
/// attempt.
/// Must NOT happen: a second node.delete of the exited birth (a second NodeDeleted), or the exited
/// birth answered as recovered while nothing stands at its path.
#[tokio::test]
async fn a_recovery_whose_create_failed_resumes_at_its_create_and_never_deletes_again() {
    let e = estate().await;
    let before = e.builds.current().await.unwrap().unwrap();
    let path = &PathName::new("mesh1.rpc", "2").unwrap();
    let exited = e.view.read().await.node(path).cloned().unwrap();
    e.world.kill(exited.node_id.as_str());
    e.unheard(&[path]).await;
    e.runner.fail_recovery_create.store(true, std::sync::atomic::Ordering::SeqCst);
    let mp = e.mesh_primary_of(path).await;
    let first = e.recover(path).await;
    assert_eq!(e.lane("mesh1").await.state, BuildState::Failed, "the create failed the attempt");
    assert!(e.born().is_empty(), "nothing was created");
    let submit = || async { e.door_at(&mp).await.open_recovery("node-recovery", exited.name.clone(), exited.node_id.clone(), exited.incarnation_id.clone().unwrap()).await };
    let resumed = match submit().await {
        Ok(rdm_admin::control_plane::Recovery::Opened(o)) => o,
        other => panic!("a recovery whose create failed is not recovered: the repeat submit opens its next attempt, got {other:?}"),
    };
    assert_eq!((resumed.build_id.clone(), resumed.mesh.clone(), resumed.attempt), (first.build_id.clone(), first.mesh.clone(), first.attempt + 1), "the same Build, its lane's next attempt");
    e.run_opened(&mp, resumed.clone()).await;
    assert_eq!(
        e.runner.recovering.lock().unwrap().clone(),
        vec![rdm_core::OperationId::node("node.delete", &exited.node_id), rdm_core::OperationId::location("node.create", &exited.name), rdm_core::OperationId::location("node.create", &exited.name)],
        "the delete once; the failed create, then only what was left"
    );
    use rdm_admin::build_state::BuildStateAdapter as _;
    let build = e.builds.read_build(&before.build_id).await.unwrap();
    assert_eq!(build.lanes["mesh1"].steps.iter().filter(|s| s.operation == rdm_core::OperationId::node("node.delete", &exited.node_id) && s.step == "NodeDeleted").count(), 1, "one NodeDeleted of the exited birth");
    assert_eq!(e.born(), vec![path.clone()], "exactly the lost path is recovered");
    assert!(matches!(submit().await, Ok(rdm_admin::control_plane::Recovery::AlreadyApplied(o)) if o.attempt == resumed.attempt), "recovered by the resumed attempt");
    e.holds_the_shape().await;
    assert_eq!(e.lane("mesh1").await.state, BuildState::Complete);
    assert_eq!((build.attempt, build.state), (before.attempt, BuildState::Complete), "the fabric lane is untouched");
}

#[tokio::test]
async fn a_silent_node_whose_runtime_runs_opens_no_attempt_and_is_never_replaced() {
    let e = estate().await;
    let before = e.builds.current().await.unwrap().unwrap();
    let silent = e.sibling_admin().await;
    let frozen = e.node_id(&silent).await;
    e.world.freeze(&frozen);
    e.unheard(&[&silent]).await;
    assert_eq!(e.drift().await, None, "silence is not proof: no attempt");
    e.converge().await;
    assert!(e.born().is_empty(), "nothing was created: {:?}", e.born());
    let after = e.builds.current().await.unwrap().unwrap();
    assert_eq!((after.attempt, after.state), (before.attempt, BuildState::Complete));
    assert_eq!(e.world.of(&frozen).unwrap().status, DeploymentStatus::Running, "the frozen runtime still runs");
}

/// CONTRACT (mesh-recovery.md: "A lost member of a mesh with a live mesh-primary is node recovery,
/// driven by that mesh-primary"; node-recovery.md): a peer mesh's primary admin is lost while its
/// second admin lives, so the second admin holds the mesh's role and the loss is node recovery. The
/// fabric-primary's drift opens nothing; the new mesh-primary opens the recovery as attempt 1 of its
/// mesh's lane of the same Build and runs it itself (R-BI7), and exactly the lost path gets a new
/// node.id. Must NOT happen: a proven-drift Replace of the lost admin.
#[tokio::test]
async fn a_lost_mesh_primary_admin_is_recovered_by_the_admin_its_mesh_elects_and_only_it() {
    let e = estate().await;
    let before = e.builds.current().await.unwrap().unwrap();
    let fp = e.fabric_primary().await.expect("a fabric-primary");
    let fp_mesh = e.view.read().await.node(&fp).unwrap().mesh.clone();
    let other = if fp_mesh == "mesh1" { "mesh2" } else { "mesh1" };
    // The other mesh's primary admin is lost.
    let primary = e.view.read().await.node_type_primary(other, NodeKind::NodeAdmin).unwrap().name.clone();
    let old = e.node_id(&primary).await;
    e.world.kill(&old);
    e.unheard(&[&primary]).await;
    let new_primary = e.mesh_primary_of(&primary).await;
    assert_ne!(new_primary, primary, "the surviving admin holds its mesh's role");
    assert_eq!(e.drift().await, None, "the fabric-primary repairs no node-admin of a mesh with a live mesh-primary");
    let opened = e.recover(&primary).await;
    assert_eq!((opened.build_id, opened.mesh.as_deref(), opened.attempt), (before.build_id.clone(), Some(other), 1), "the same Build, attempt 1 of the mesh's lane");
    e.recovered_by(&primary, &new_primary).await;
    assert_eq!(e.born(), vec![primary.clone()], "exactly the lost admin is recovered");
    assert_ne!(e.node_id(&primary).await, old, "a new node.id");
    e.holds_the_shape().await;
}

/// CONTRACT: docs/architecture/lifecycles/node-recovery.md step 2
/// "For an unheard mesh-primary the fabric-primary runs it, without the half-floor wait, after the
/// election has named the new mesh-primary ... with no ladder of its own (Q335)."
/// CONTRACT (Q335: one investigation, one recovery; elections §4.1): both node-admins of a peer mesh
/// exit, and the fabric-primary's view holds them, unheard. No node-admin of that mesh is Ready, so the
/// election names no successor. The fabric-primary's drift pass opens nothing for either node-admin: a
/// node-admin a view holds is the primary watch's to recover (the lost mesh-primary) or its
/// mesh-primary's (the other). The fabric-primary's node recovery of the lost mesh-primary, as its
/// primary watch submits it, re-creates it once at its path.name, executed by the fabric-primary; a drift
/// pass after it opens nothing either. Must NOT happen: a drift attempt for a view-held node-admin beside
/// that recovery, or the mesh-primary re-created twice.
#[tokio::test]
async fn a_view_held_lost_mesh_primary_is_recovered_once_by_the_primary_watch_and_the_drift_pass_opens_nothing_for_it() {
    let e = estate().await;
    let fp = e.fabric_primary().await.expect("a fabric-primary");
    let fp_mesh = e.view.read().await.node(&fp).unwrap().mesh.clone();
    let other = if fp_mesh == "mesh1" { "mesh2" } else { "mesh1" };
    let admins = [PathName::new(&format!("{other}.admin"), "1").unwrap(), PathName::new(&format!("{other}.admin"), "2").unwrap()];
    let primary = e.view.read().await.node_type_primary(other, NodeKind::NodeAdmin).unwrap().name.clone();
    let old = e.node_id(&primary).await;
    for p in &admins {
        let id = e.node_id(p).await;
        e.world.exit_with(&id, Some(137));
    }
    // Unheard in the fabric-primary's view: the record still names the lost birth the mesh's primary (a
    // silent primary keeps its role until it is proven gone), and nothing is Ready for the election to name.
    for n in e.view.write().await.nodes.iter_mut().filter(|n| admins.contains(&n.name)) {
        n.status = NodeStatus::PendingReconnect;
    }
    assert_eq!(e.drift().await, None, "the drift pass opens nothing for a node-admin a view holds");
    let n = e.view.read().await.node(&primary).cloned().expect("the lost mesh-primary is in the view");
    let door = e.dispatch.door(&e.view.read().await.node(&fp).unwrap().birth().unwrap());
    match rdm_admin::build_run::run_recovery(&*e.door_at(&fp).await, &door, "node-recovery", n.name.clone(), n.node_id.clone(), n.incarnation_id.clone().unwrap()).await {
        Ok(rdm_admin::build_run::Recovered::Ran { .. }) => {}
        other => panic!("the fabric-primary runs the recovery of {primary:?} to a terminal: {other:?}"),
    }
    assert_eq!(e.born(), vec![primary.clone()], "exactly the lost mesh-primary is re-created, once");
    assert_ne!(e.node_id(&primary).await, old, "a new node.id at its path.name");
    assert_eq!(e.drift().await, None, "and the drift pass opens nothing after it");
    assert_eq!(e.born(), vec![primary.clone()], "nothing else is created");
}

/// CONTRACT (node-recovery.md, mesh-recovery.md:8-9): a former fabric-primary that hands off its
/// roles (`node.primary.update`) to its mesh's second admin before exiting is a lost member of a
/// mesh with a live mesh-primary: the successor's drift opens nothing for it, and the admin its
/// mesh elects recovers it as a new node.id at the same path.
#[tokio::test]
async fn a_lost_fabric_primary_is_recovered_by_its_own_meshs_new_primary() {
    let e = estate().await;
    let fp = e.fabric_primary().await.expect("a fabric-primary");
    let fp_mesh = e.view.read().await.node(&fp).unwrap().mesh.clone();
    let second = e.view.read().await.nodes_of_type(&fp_mesh, NodeKind::NodeAdmin).map(|n| n.name.clone()).find(|n| *n != fp).expect("a second admin in the fabric-primary's mesh");
    {
        let mut v = e.view.write().await;
        for n in v.nodes.iter_mut() {
            if n.name.clone() == fp {
                n.is_primary = false;
                n.is_fabric_primary = false;
            } else if n.name.clone() == second {
                n.is_primary = true;
                n.is_fabric_primary = true;
            }
        }
    }
    let old = e.node_id(&fp).await;
    e.world.kill(&old);
    e.unheard(&[&fp]).await;
    let successor = e.fabric_primary().await.expect("a successor");
    assert!(successor != fp, "the role moved: {successor:?}");
    let new_primary = e.mesh_primary_of(&fp).await;
    assert_ne!(new_primary, fp, "its mesh's second admin holds the mesh's role");
    assert_eq!(e.drift().await, None, "the successor's drift repairs no node-admin of a mesh with a live mesh-primary");
    e.recover(&fp).await;
    e.recovered_by(&fp, &new_primary).await;
    assert_eq!(e.born(), vec![fp.clone()], "exactly the lost fabric primary's path is recovered");
    assert_ne!(e.node_id(&fp).await, old, "a new node.id");
    e.holds_the_shape().await;
}

/// CONTRACT (R-D1: the fabric-primary acts on a peer mesh's node-admin only when every admin of
/// that mesh is proven gone; Q335: a birth its view holds is the primary watch's, a birth only its runtime row names the drift pass's; R-A2:
/// cross-mesh only when every node-admin of the mesh is proven lost): a peer mesh's primary admin exits and every
/// member of that mesh goes silent at once (its forwarder is gone), its second admin included,
/// though it runs and answers. The fabric-primary's drive stalls by name while that admin is held,
/// and once its own mesh elects it, it executes the repair: only the exited admin is recovered and no
/// member that answers is created over.
#[tokio::test]
async fn members_unheard_through_a_lost_forwarder_are_held_never_duplicated() {
    let e = estate().await;
    let fp = e.fabric_primary().await.expect("a fabric-primary");
    let fp_mesh = e.view.read().await.node(&fp).unwrap().mesh.clone();
    let lost = if fp_mesh == "mesh1" { "mesh2" } else { "mesh1" };
    // The other mesh's primary admin dies; in the fabric primary's view every member of that mesh
    // goes silent at once (its forwarder is gone), though all but the dead admin run and answer.
    let primary = e.view.read().await.node_type_primary(lost, NodeKind::NodeAdmin).unwrap().name.clone();
    e.world.kill(&e.node_id(&primary).await);
    let members: Vec<PathName> = e.view.read().await.nodes.iter().filter(|n| n.mesh == lost).map(|n| n.name.clone()).collect();
    let refs: Vec<&PathName> = members.iter().collect();
    e.unheard(&refs).await;
    // The fabric-primary's view no longer holds the exited admin's birth: only its runtime row names it
    // (a birth a view holds is the primary watch's to recover, Q335).
    e.to_rows_only(&primary).await;
    e.drift().await.expect("the one proven loss opens the next attempt");
    let survivor = e.stalls_until_elected(lost, &primary).await;
    e.converge().await;
    e.executed_only_by(&survivor).await;
    assert_eq!(e.born(), vec![primary.clone()], "only the proven-exited admin is recovered: {:?}", e.runner.ran.lock().unwrap());
    for m in members.iter().filter(|m| **m != primary) {
        assert!(e.investigations_of(m).iter().all(|o| *o == TargetOutcome::Alive), "{m:?} answers: never created over: {:?}", e.investigations_of(m));
    }
    // Heard again through the recovered forwarder: the topology holds.
    e.holds_the_shape().await;
}

/// CONTRACT (fabric-node-hooks.md `on_build_started`, `on_build_ended`, Q256: "Each time ... an attempt
/// that reopens a Build that has ended is claimed, the fabric-primary sends the application one
/// `on_build_started` notice"; "its reason (... the reopening attempt's: a drift repair ...)"; "Each
/// `on_build_started` is followed by one `on_build_ended` for the same opening"; Q258: the case is the
/// fabric-primary's own reopening, never a node's recovery by its mesh-primary). The case: an unheard mesh-primary's
/// recovery the fabric-primary opens itself (R-D1). The accepted Build is complete when a peer mesh's
/// primary admin exits with every node of its mesh unheard; the fabric-primary's drift opens the next
/// attempt of the same Build, carrying the Create of the lost admin; once the mesh elects its surviving
/// admin, the attempt is claimed and the repair converges. The application is told `on_build_started`
/// once, at the reopening attempt's claim, then `on_build_ended` `Complete` once, both naming the
/// reopening attempt's reason as opened. Must NOT happen: the repair's opening or ending untold, an
/// ending before its opening, or a second `on_build_started` for an attempt that continues the repair.
#[tokio::test]
async fn a_fabric_primarys_drift_repair_of_a_completed_build_sends_one_started_then_one_ended_with_its_reopening_reason() {
    let e = estate().await;
    let fp = e.fabric_primary().await.expect("a fabric-primary");
    let fp_mesh = e.view.read().await.node(&fp).unwrap().mesh.clone();
    let lost = if fp_mesh == "mesh1" { "mesh2" } else { "mesh1" };
    let primary = e.view.read().await.node_type_primary(lost, NodeKind::NodeAdmin).unwrap().name.clone();
    e.world.kill(&e.node_id(&primary).await);
    let members: Vec<PathName> = e.view.read().await.nodes.iter().filter(|n| n.mesh == lost).map(|n| n.name.clone()).collect();
    let refs: Vec<&PathName> = members.iter().collect();
    e.unheard(&refs).await;
    // The fabric-primary's view no longer holds the exited admin's birth: only its runtime row names it
    // (a birth a view holds is the primary watch's to recover, Q335).
    e.to_rows_only(&primary).await;
    let (build, opened) = e.drift().await.expect("the one proven loss opens the next attempt");
    // The fabric-primary's drift opens an attempt carrying an action with reason `steps`, and one
    // carrying none (a surplus birth) with `proven-drift` (admin::reconcile_drift).
    let reopened = e.builds.read_build(&build).await.unwrap();
    assert_eq!((reopened.opened, reopened.reason), (opened, rdm_admin::build_state::AttemptReason::Steps), "the drift repair reopens the completed Build at attempt {opened}, carrying the lost admin's Create");
    e.stalls_until_elected(lost, &primary).await;
    e.converge().await;
    let done = e.builds.read_build(&build).await.unwrap();
    assert_eq!(done.state, BuildState::Complete, "the repair converged: {:?}", (done.state, done.attempt));
    let told = e.told.settled(2).await;
    let id = build.to_string();
    let keys: Vec<(String, u32, &str)> = told.iter().map(crate::Notice::key).collect();
    assert_eq!(keys, vec![(id.clone(), opened, "started"), (id.clone(), done.attempt, "complete")], "one opening at the reopening attempt's claim, then one ending: {told:?}");
    for n in &told {
        let reason = match n {
            crate::Notice::Started(c) => c.reason,
            crate::Notice::Ended(c) => c.reason,
        };
        assert_eq!(reason, reopened.reason, "each notice names the reopening attempt's reason: {n:?}");
    }
    eprintln!("told: {keys:?}");
}

#[tokio::test]
async fn a_path_the_view_does_not_hold_but_where_a_runtime_runs_is_held() {
    let e = estate().await;
    // rpc.3 is not in this view at all (never heard here), but its runtime runs, frozen.
    let hidden = e.node_id(&PathName::new("mesh1.rpc", "3").unwrap()).await;
    e.world.freeze(&hidden);
    e.view.write().await.nodes.retain(|n| n.name != PathName::new("mesh1.rpc", "3").unwrap());
    // A real loss elsewhere opens a recovery whose plan is exactly that birth: the path the view
    // lacks is its own attempt's, so this one neither creates over it nor targets it.
    e.world.kill(&e.node_id(&PathName::new("mesh1.rpc", "2").unwrap()).await);
    e.unheard(&[&PathName::new("mesh1.rpc", "2").unwrap()]).await;
    e.recover(&PathName::new("mesh1.rpc", "2").unwrap()).await;
    assert_eq!(e.born(), vec![PathName::new("mesh1.rpc", "2").unwrap()], "the absent-but-running path is not created over");
    assert_eq!(e.investigations_of(&PathName::new("mesh1.rpc", "3").unwrap()), vec![], "an attempt that carries an action plans only its birth");
}

/// The seeded soak's finding (seed 7, round 3, trace in the spans of
/// `fabric_soak__seeded`): the lost mesh's primary is recovered as the attempt's first operation, its
/// members are heard again through it before the attempt's next operation, and that operation
/// must find them live and leave them alone. A plan made on the older view is not a licence. The
/// lost mesh's second admin runs and answers, so the fabric-primary stalls by name until that
/// mesh elects it, and the in its role admin executes the attempt (R-D1, R-A2).
#[tokio::test]
async fn members_heard_again_mid_attempt_through_the_recovered_forwarder_are_never_created_over() {
    let e = estate().await;
    e.runner.hear_after_birth.store(true, std::sync::atomic::Ordering::SeqCst);
    let fp = e.fabric_primary().await.expect("a fabric-primary");
    let fp_mesh = e.view.read().await.node(&fp).unwrap().mesh.clone();
    let lost = if fp_mesh == "mesh1" { "mesh2" } else { "mesh1" };
    let primary = e.view.read().await.node_type_primary(lost, NodeKind::NodeAdmin).unwrap().name.clone();
    e.world.kill(&e.node_id(&primary).await);
    let members: Vec<PathName> = e.view.read().await.nodes.iter().filter(|n| n.mesh == lost).map(|n| n.name.clone()).collect();
    let refs: Vec<&PathName> = members.iter().collect();
    e.unheard(&refs).await;
    // The fabric-primary's view no longer holds the exited admin's birth: only its runtime row names it
    // (a birth a view holds is the primary watch's to recover, Q335).
    e.to_rows_only(&primary).await;
    e.drift().await.expect("the one proven loss opens the next attempt");
    let survivor = e.stalls_until_elected(lost, &primary).await;
    e.converge().await;
    e.executed_only_by(&survivor).await;
    assert_eq!(e.born(), vec![primary.clone()], "only the proven-exited admin is recovered: {:?}", e.runner.ran.lock().unwrap());
    for m in members.iter().filter(|m| **m != primary) {
        assert!(e.investigations_of(m).iter().all(|o| *o == TargetOutcome::Alive), "{m:?} is live in the view by the time its operation runs: never created over: {:?}", e.investigations_of(m));
    }
    e.holds_the_shape().await;
}

/// CONTRACT (node-recovery.md, "Lineage: No supersedes"): an exit carrying the TRANSPORT_STOPPED code
/// (4) is recovered like any other exit. The fabric-primary opens no start; the mesh-primary's
/// recovery creates a new node.id at the path, never the same identity under a new incarnation.
#[tokio::test]
async fn a_transport_stopped_exit_is_recovered_as_a_new_node_never_restarted() {
    let e = estate().await;
    let before = e.builds.current().await.unwrap().unwrap();
    let id = e.node_id(&PathName::new("mesh1.rpc", "2").unwrap()).await;
    e.world.exit_with(&id, Some(rdm_core::runtime::TRANSPORT_STOPPED_EXIT_CODE));
    e.unheard(&[&PathName::new("mesh1.rpc", "2").unwrap()]).await;
    assert_eq!(e.drift().await, None, "the fabric-primary restarts no ordinary member");
    e.recover(&PathName::new("mesh1.rpc", "2").unwrap()).await;
    let opened = e.lane("mesh1").await;
    assert_eq!(opened.reason, rdm_admin::build_state::AttemptReason::Recovery);
    assert!(matches!(&opened.action, Some(rdm_admin::accepted::AttemptAction::Recover { path, node_id, .. }) if path == &PathName::new("mesh1.rpc", "2").unwrap() && node_id.as_str() == id), "{:?}", opened.action);
    assert_eq!(e.born(), vec![PathName::new("mesh1.rpc", "2").unwrap()]);
    assert_ne!(e.node_id(&PathName::new("mesh1.rpc", "2").unwrap()).await, id, "a new node.id");
    e.holds_the_shape().await;
    let after = e.builds.current().await.unwrap().unwrap();
    assert_eq!((after.build_id, after.attempt, after.state), (before.build_id, before.attempt, BuildState::Complete), "the fabric lane is untouched");
    assert_eq!((opened.attempt, opened.state), (1, BuildState::Complete), "attempt 1 of mesh1's lane");
}

/// CONTRACT (mesh-recovery.md:8-9 "A lost member of a mesh with a live mesh-primary is node
/// recovery, driven by that mesh-primary"; node-start.md: a start rejoins a stopped live process, so a
/// exited verdict is never started again): a secondary node-admin of a peer mesh (its mesh-primary live and
/// not the fabric-primary) that exits, with the TRANSPORT_STOPPED code (4) and with another code
/// (137), is recovered by its mesh-primary: the fabric-primary's drift opens nothing, the recovery
/// is attempt 1 of the mesh's lane of the same Build keyed on the exact exited birth, run by that
/// mesh-primary itself (R-BI7), and the path gets a new node.id. Must NOT happen: a proven-drift
/// Replace or a start.
#[tokio::test]
async fn a_secondary_node_admins_exit_is_recovered_by_its_mesh_primary_whatever_its_code() {
    for code in [Some(rdm_core::runtime::TRANSPORT_STOPPED_EXIT_CODE), Some(137)] {
        let e = estate().await;
        let before = e.builds.current().await.unwrap().unwrap();
        let fp = e.fabric_primary().await.expect("a fabric-primary");
        let peer = if e.view.read().await.node(&fp).unwrap().mesh == "mesh1" { "mesh2" } else { "mesh1" };
        let mp = e.view.read().await.node_type_primary(peer, NodeKind::NodeAdmin).unwrap().name.clone();
        let admin = e.view.read().await.nodes_of_type(peer, NodeKind::NodeAdmin).map(|n| n.name.clone()).find(|n| *n != mp).unwrap();
        let id = e.node_id(&admin).await;
        let incarnation = e.view.read().await.node(&admin).and_then(|n| n.incarnation_id.clone()).unwrap();
        e.world.exit_with(&id, code);
        e.unheard(&[&admin]).await;
        assert_eq!(e.mesh_primary_of(&admin).await, mp, "{code:?}: the mesh keeps its live primary");
        assert_eq!(e.drift().await, None, "{code:?}: the fabric-primary opens nothing for a secondary node-admin");
        let opened = e.recover(&admin).await;
        assert_eq!((opened.build_id.clone(), opened.mesh.as_deref(), opened.attempt), (before.build_id.clone(), Some(peer), 1), "{code:?}: the same Build, attempt 1 of the mesh's lane");
        let current = e.lane(peer).await;
        assert_eq!(current.reason, rdm_admin::build_state::AttemptReason::Recovery, "{code:?}");
        assert!(
            matches!(&current.action, Some(rdm_admin::accepted::AttemptAction::Recover { path, node_id, from_incarnation, .. }) if path.clone() == admin && node_id.as_str() == id && *from_incarnation == incarnation),
            "{code:?}: a Recover of the exact exited birth: {:?}",
            current.action
        );
        e.recovered_by(&admin, &mp).await;
        assert_eq!(e.born(), vec![admin.clone()], "{code:?}: exactly the lost admin is recovered");
        assert_ne!(e.node_id(&admin).await, id, "{code:?}: a new node.id");
        e.holds_the_shape().await;
    }
}

/// CONTRACT: every exit, whatever its code (1, unknown, 137), is recovered by the mesh-primary, one
/// exited birth per attempt of its mesh's lane: a submit while another recovery runs in the mesh is
/// refused BuildInProgress naming the Build, the mesh and the mesh-primary holding it (one attempt in
/// flight per mesh, R-BI7 caveat 3), and is opened once that one ends. The fabric-primary opens
/// nothing for any of them.
#[tokio::test]
async fn every_exit_is_recovered_one_birth_per_attempt_and_a_submit_waits_for_the_running_one() {
    let e = estate().await;
    let before = e.builds.current().await.unwrap().unwrap();
    let (a, b, c) = (e.node_id(&PathName::new("mesh1.rpc", "1").unwrap()).await, e.node_id(&PathName::new("mesh1.rpc", "2").unwrap()).await, e.node_id(&PathName::new("mesh1.rpc", "3").unwrap()).await);
    e.world.exit_with(&c, Some(1));
    e.world.exit_with(&a, None);
    e.world.exit_with(&b, Some(137));
    e.unheard(&[&PathName::new("mesh1.rpc", "1").unwrap(), &PathName::new("mesh1.rpc", "2").unwrap(), &PathName::new("mesh1.rpc", "3").unwrap()]).await;
    assert_eq!(e.drift().await, None, "the fabric-primary repairs no ordinary member");
    let paths = [PathName::new("mesh1.rpc", "1").unwrap(), PathName::new("mesh1.rpc", "2").unwrap(), PathName::new("mesh1.rpc", "3").unwrap()];
    let mp = e.mesh_primary_of(&PathName::new("mesh1.rpc", "1").unwrap()).await;
    for (pass, path) in paths.iter().enumerate() {
        let Ok(rdm_admin::control_plane::Recovery::Opened(o)) = e.submit_recovery(path).await else { panic!("pass {pass}: {path:?}'s recovery was not opened") };
        assert_eq!((o.build_id.clone(), o.mesh.as_deref(), o.attempt), (before.build_id.clone(), Some("mesh1"), 1 + pass as u32));
        if let Some(next) = paths.get(pass + 1) {
            let waits = e.submit_recovery(next).await;
            assert!(
                matches!(waits, Err(rdm_admin::control_plane::Refusal::BuildInProgress(ref held)) if held.current_build_id == before.build_id && held.mesh.as_deref() == Some("mesh1") && held.held_by.as_ref().map(NodeId::as_str) == Some(e.node_id(&mp).await.as_str())),
                "pass {pass}: {next:?} waits for the running recovery: {:?}",
                waits.map(|_| ()).map_err(|r| format!("{r:?}"))
            );
        }
        e.run_opened(&mp, o).await;
        assert_eq!(e.born().last(), Some(path));
    }
    assert_eq!(e.born().len(), 3);
    assert!(paths.iter().all(|p| e.investigations_of(p).is_empty()), "no recovery targets by probe");
    e.holds_the_shape().await;
    assert_eq!(e.drift().await, None, "nothing is left to repair");
}

/// A successor fabric primary that never heard a dead sibling holds nothing of it but the durable
/// row its predecessor handed over: that row is the exact runtime to inspect, and its exited birth is
/// recovered first, before the hung admin whose runtime runs (node-recovery.md step 3).
#[tokio::test]
async fn a_successor_recovers_a_sibling_admin_known_only_by_its_durable_runtime_row() {
    let e = estate().await;
    let before = e.builds.current().await.unwrap().unwrap();
    // A peer mesh's node-admins are all lost (its first admin hung and silent, so no live
    // mesh-primary is left to recover anything: mesh recovery); its second admin is one the
    // successor never heard.
    let peer = if e.view.read().await.fabric_primary().unwrap().mesh == "mesh1" { "mesh2" } else { "mesh1" };
    let lost = PathName::new(&format!("{peer}.admin"), "2").unwrap();
    let lost = &lost;
    let hung = PathName::new(&format!("{peer}.admin"), "1").unwrap();
    e.world.freeze(&e.node_id(&hung).await);
    e.unheard(&[&hung]).await;
    let old = e.node_id(lost).await;
    let row = {
        let (d, _) = e.digest_view.get(&old).unwrap();
        rdm_admin::storage::RuntimeRow { kind: d.node.kind, mesh: d.node.mesh.clone(), node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime: d.node.runtime.clone().unwrap(), data_dir: None }
    };
    e.world.kill(&old);
    // The successor's topology view and view: everything but the lost admin.
    let heard = DigestView::default();
    for d in e.digest_view.all().into_iter().filter(|d| &d.node.name != lost) {
        heard.record(d);
    }
    let mut t = e.view.read().await.clone();
    t.nodes.retain(|n| &n.name != lost);
    rdm_admin::election::resolve(&mut t.nodes);
    let me: PathName = t.fabric_primary().unwrap().name.clone();
    let mut started = HashSet::new();
    let contexts = rdm_admin::build_claim::AttemptContexts::in_memory();
    let opened = rdm_admin::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &heard, &e.provider, &*e.builds, &contexts, &tokio::sync::Mutex::new(()), std::slice::from_ref(&row), &mut started, &mut Default::default(), &|_| None, &Default::default()).await;
    assert_eq!(opened, Some((before.build_id.clone(), before.attempt + 1)), "the durable row's exact runtime exited: the next attempt of the same Build is open");
    let current = e.builds.current().await.unwrap().unwrap();
    let action = current.action;
    assert_eq!(one_step_create_of(&action).as_ref(), Some(lost), "the durable row's birth reads exited: the mesh that lost its mesh-primary gets node.create of its node-admin, a single step, never a Recover or a replace: {action:?}");
    assert_eq!(current.reason, rdm_admin::build_state::AttemptReason::Steps, "the attempt carries steps");
    let _ = old;
}

/// CONTRACT (mesh-recovery.md:8-9 "A lost member of a mesh with a live mesh-primary is node
/// recovery, driven by that mesh-primary"; node-recovery.md): a secondary node-admin of a peer mesh
/// that its mesh-primary never heard and never launched, known to it only by the durable runtime row
/// its maker handed over, is watched by that mesh-primary's tick as a silent birth (`PendingReconnect`,
/// the row's exact node id and incarnation); once TRUE OFFLINE it is due for recovery, the provider
/// proves its exact runtime exited from the row, and the recovery submitted at the fabric-primary's
/// door is the next attempt of the same Build, executed by that mesh-primary, with a new node.id at
/// the path. The fabric-primary's drift, holding the same row, opens nothing. Must NOT happen: the
/// birth watched by nobody (no recovery ever), or a proven-drift Replace from the fabric-primary.
#[tokio::test]
async fn a_secondary_node_admin_known_only_by_its_durable_row_is_recovered_by_its_mesh_primary() {
    let (capture, _guard) = crate::receipt_refusal::Capture::new("i143-3051-durable-row");
    let e = estate().await;
    let before = e.builds.current().await.unwrap().unwrap();
    let fp = e.fabric_primary().await.expect("a fabric-primary");
    let peer = if e.view.read().await.node(&fp).unwrap().mesh == "mesh1" { "mesh2" } else { "mesh1" };
    let mp: PathName = e.view.read().await.node_type_primary(peer, NodeKind::NodeAdmin).unwrap().name.clone();
    let lost = e.view.read().await.nodes_of_type(peer, NodeKind::NodeAdmin).map(|n| n.name.clone()).find(|n| *n != mp.clone()).unwrap();
    let old = e.node_id(&lost).await;
    let row = {
        let (d, _) = e.digest_view.get(&old).unwrap();
        rdm_admin::storage::RuntimeRow { kind: d.node.kind, mesh: d.node.mesh.clone(), node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime: d.node.runtime.clone().unwrap(), data_dir: None }
    };
    e.world.kill(&old);
    // Nobody heard the lost admin: it is in no view and no topology view, only in the row.
    let heard = DigestView::default();
    for d in e.digest_view.all().into_iter().filter(|d| d.node.name != lost) {
        heard.record(d);
    }
    {
        let mut v = e.view.write().await;
        v.nodes.retain(|n| n.name.clone() != lost);
        rdm_admin::election::resolve(&mut v.nodes);
    }
    let t = e.view.read().await.clone();
    assert_eq!(t.node_type_primary(peer, NodeKind::NodeAdmin).map(|n| n.name.clone()), Some(mp.clone()), "the mesh keeps its live primary");

    // The fabric-primary's drift folds the row and leaves the birth to that mesh-primary.
    let me = fp.clone();
    let contexts = rdm_admin::build_claim::AttemptContexts::in_memory();
    let opened = rdm_admin::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &heard, &e.provider, &*e.builds, &contexts, &tokio::sync::Mutex::new(()), std::slice::from_ref(&row), &mut HashSet::new(), &mut Default::default(), &|_| None, &Default::default()).await;
    assert_eq!(opened, None, "the fabric-primary opens nothing for a node-admin its mesh-primary recovers");

    // The mesh-primary's tick: what it watches, from its view and its durable rows.
    let accepted = e.builds.current().await.unwrap().unwrap().topology;
    let births = rdm_admin::admin::durable_births(&t, std::slice::from_ref(&row), &|p| accepted.contains(p), &|id| heard.is_deleted(id));
    let watched = rdm_admin::admin::watched_births(&t, &t.node(&mp).expect("the mesh-primary is in the view").node_id, peer, &births);
    let w = watched.get(&old).unwrap_or_else(|| panic!("{mp:?}'s tick watches {lost:?}, known only by its durable row: watched {:?}", watched.values().map(|n| n.name.clone()).collect::<Vec<_>>()));
    assert_eq!((w.name.clone(), w.incarnation_id.clone(), w.status), (lost.clone(), Some(row.incarnation_id.clone()), NodeStatus::PendingReconnect), "the row's exact birth, silent");
    // TRUE OFFLINE (the tickle's verdict): due for recovery at the primary, and at nobody else.
    let offline: HashSet<(NodeId, IncarnationId)> = [(row.node_id.clone(), row.incarnation_id.clone())].into();
    let due: Vec<PathName> = rdm_admin::admin::recovery_due(true, peer, &watched, &offline).iter().map(|n| n.name.clone()).collect();
    assert_eq!(due, vec![lost.clone()], "{mp:?} inspects {lost:?} for recovery");
    // The provider's verdict, from the row.
    let verdict = rdm_admin::admin::inspection_of(&e.provider, &None, Some(&row)).await;
    assert!(matches!(verdict, Inspected::Exited(_)), "the row's exact runtime reads exited: {verdict:?}");

    // Its lifecycle record stands on the store root beside its runtime row (claimed at its birth).
    rdm_admin::node_rows::write_record(&*e.rows, &rdm_admin::storage::NodeRow::new(row.name.clone(), row.mesh.clone(), row.kind, row.node_id.clone(), NodeStatus::Ready)).await.unwrap().unwrap();
    // The mesh-primary opens and claims the recovery at its own door and runs it (R-BI7).
    let submitted = e.door_at(&mp.clone()).await.open_recovery("node-recovery", row.name.clone(), row.node_id.clone(), row.incarnation_id.clone()).await;
    let Ok(rdm_admin::control_plane::Recovery::Opened(o)) = submitted else { panic!("{lost:?}'s recovery was not opened: {:?}", submitted.map(|_| ()).map_err(|r| format!("{r:?}"))) };
    assert_eq!((o.build_id.clone(), o.mesh.as_deref(), o.attempt), (before.build_id.clone(), Some(peer), 1), "the same Build, attempt 1 of the mesh's lane");
    e.run_opened(&mp.clone(), o).await;
    e.recovered_by(&lost, &mp.clone()).await;
    assert_eq!(e.born(), vec![lost.clone()], "exactly the lost admin is recovered");
    assert_ne!(e.node_id(&lost).await, old, "a new node.id");
    e.holds_the_shape().await;
    let admitted: Vec<_> = capture.spans("rdm.node_admin.build.update.via-recovery").into_iter().filter(|(_, a)| a.get("node_path") == Some(&lost.path) && a.get("node_name") == Some(&lost.name) && a.get("node_id") == Some(&old)).collect();
    assert_eq!(admitted.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["admitted".to_string()], "one recovery of the row's exact birth, admitted: {admitted:?}");
    assert!(capture.spans("rdm.node_admin.build.reject.via-drift-unprovable").is_empty(), "the fabric-primary's drift names nothing for the birth");
    eprintln!("span rdm.node_admin.build.update.via-recovery trace_id={} {:?}", admitted[0].0, admitted[0].1);
}

/// The cell's span directory: the gate's, else this cell's own under the workspace target.
fn evidence_dir(cell: &str) -> std::path::PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => std::path::PathBuf::from(d),
        Err(_) => std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/3061/unit").join(cell),
    }
}

/// Two openers of one attempt number: the requested attempt (REST) and the proven-drift pass read the
/// same settled Build, so both compute the next attempt as `k + 1`. The first append takes the number;
/// the loser is told by name, never acknowledged for an attempt that carries someone else's action.
/// CONTRACT (R-BI7 caveat 3: "A topology Build's operation on mesh X and mesh X's own drift attempt
/// cannot both be open: whichever claims first on mesh X's rows wins, the other is refused
/// build-in-progress naming the holder"): a member of mesh1 has exited when the fabric-primary
/// accepts a node.create in mesh2; that Build's plan also repairs mesh1's unheard path, first, and it
/// is not claimed yet when mesh1's primary opens and claims its member's recovery in mesh1's lane. The
/// fabric-primary's drive then claims nothing in mesh1: its pass stalls naming mesh1 and the
/// mesh-primary holding it, and nothing is dispatched. Once the mesh-primary's run reaches its verdict
/// and releases mesh1, the fabric-primary's next pass claims what is left (the node.create in mesh2,
/// the lost path being live again) and the Build completes. Must NOT happen: the lost path created
/// twice, an operation dispatched in mesh1 while the recovery holds it, or the drive still held after
/// the release.
#[tokio::test]
async fn a_topology_build_claims_nothing_in_a_mesh_its_recovery_holds_and_runs_once_the_recovery_releases_it() {
    let e = estate().await;
    let path = &PathName::new("mesh1.rpc", "2").unwrap();
    e.world.kill(&e.node_id(path).await);
    e.unheard(&[path]).await;
    let mp = e.mesh_primary_of(path).await;
    let fp = e.fabric_primary().await.expect("a fabric-primary");
    let door = e.door_at(&fp).await;
    let create = door.submit("build.create", crate::named_request(&door, rdm_admin::accepted::TopologyChange::AddNode { mesh: "mesh2".into(), node_kind: NodeKind::RpcNode }).await).await.expect("the node.create is accepted: no attempt holds a mesh yet");
    let Ok(rdm_admin::control_plane::Recovery::Opened(recovery)) = e.submit_recovery(path).await else { panic!("mesh1's primary opens its member's recovery: the node.create's attempt is not claimed") };
    assert_eq!((recovery.build_id.clone(), recovery.mesh.as_deref()), (create.build_id.clone(), Some("mesh1")), "the recovery is an attempt of the current Build, in mesh1's lane");
    match e.drive_once().await {
        DriveEnd::Stalled(why) => assert!(why.contains("mesh mesh1") && why.contains(e.node_id(&mp).await.as_str()) && why.contains("one attempt in flight per mesh"), "the stall names mesh1 and its holder: {why}"),
        other => panic!("nothing is claimed in mesh1 while its recovery holds it: {other:?}"),
    }
    assert!(e.dispatch.dispatched.lock().unwrap().is_empty(), "nothing is dispatched while the recovery holds mesh1");
    let terminal = e.run_opened(&mp, recovery.clone()).await;
    assert_eq!(terminal, rdm_core::build::BuildReply::Complete { build_id: recovery.build_id.to_string(), attempt: recovery.attempt }, "the recovery ran to its verdict");
    e.converge().await;
    let after = e.builds.read_build(&create.build_id).await.unwrap();
    assert_eq!(after.state, BuildState::Complete, "the node.create ran once the recovery released mesh1: {after:?}");
    assert_eq!(e.born().iter().filter(|b| *b == path).count(), 1, "the lost path is created once, by its recovery: {:?}", e.born());
    assert_eq!(e.born().len(), 2, "and the node.create its new path: {:?}", e.born());
    let v = e.view.read().await;
    for p in after.topology.meshes.values().flat_map(|m| m.nodes.iter()) {
        assert_eq!(v.nodes.iter().filter(|n| n.name == *p && n.status.is_live()).count(), 1, "{p:?}: one live birth of every accepted path");
    }
}

mod one_attempt_number {
    use super::*;
    use rdm_admin::control_plane::Refusal;
    use rdm_admin::build_state::{AttemptOpened, AttemptReason, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome};
    use rdm_admin::control_plane::ControlPlane;
    use tokio::sync::Notify;

    /// The write a [`Gate`] stops at the log's door.
    #[derive(Clone, Copy, PartialEq)]
    enum Holds {
        /// An `open_attempt` of this reason, after its caller computed the number.
        Attempt(AttemptReason),
        /// The first `publish_accepted`: a submitted change past its check, inside its durable write.
        Acceptance,
    }

    /// The estate's Build log with one gate: the write it `holds` stops at the log's door until
    /// the cell lets it write.
    struct Gate {
        inner: Arc<MemoryBuildStateAdapter>,
        holds: Holds,
        arrived: Notify,
        release: Notify,
    }

    #[async_trait::async_trait]
    impl BuildStateAdapter for Gate {
        async fn publish_accepted(&self, a: &BuildAccepted) -> Result<(), BuildStateError> {
            if self.holds == Holds::Acceptance {
                self.arrived.notify_one();
                self.release.notified().await;
            }
            self.inner.publish_accepted(a).await
        }
        async fn open_attempt(&self, o: &AttemptOpened) -> Result<(), BuildStateError> {
            if self.holds == Holds::Attempt(o.reason) {
                self.arrived.notify_one();
                self.release.notified().await;
            }
            self.inner.open_attempt(o).await
        }
        async fn read_build(&self, b: &BuildId) -> Result<BuildProjection, BuildStateError> {
            self.inner.read_build(b).await
        }
        async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, rdm_admin::build_state::BuildStateError> {
            self.inner.current_build().await
        }
        async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, rdm_admin::build_state::BuildStateError> {
            self.inner.newest_accepted_at_ms().await
        }
        async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
            self.inner.list_active().await
        }
        async fn claim_attempt(&self, c: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
            self.inner.claim_attempt(c).await
        }
        async fn adopt_claim(&self, c: &BuildAttemptClaim) -> Result<(), BuildStateError> {
            self.inner.adopt_claim(c).await
        }
        async fn append_step_receipt(&self, r: &BuildStepReceipt) -> Result<(), BuildStateError> {
            self.inner.append_step_receipt(r).await
        }
        async fn append_attempt_receipt(&self, r: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
            self.inner.append_attempt_receipt(r).await
        }
        async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
            self.inner.facts().await
        }
        async fn committed_facts(&self, build: Option<&rdm_admin::build::BuildId>) -> Result<Vec<rdm_admin::build_state::BuildFact>, rdm_admin::build_state::BuildStateError> {
            let facts = self.facts().await?;
            Ok(match build { Some(b) => facts.into_iter().filter(|f| f.build_id() == b).collect(), None => facts })
        }
        async fn recovery_of(&self, path: &rdm_admin::model::PathName, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Option<(rdm_admin::build::BuildId, Option<String>, u32, rdm_core::OperationId)>, BuildStateError> {
            self.inner.recovery_of(path, node_id, incarnation).await
        }
        async fn mesh_deleted_of(&self, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Option<(rdm_admin::build::BuildId, u32, rdm_core::OperationId)>, BuildStateError> {
            self.inner.mesh_deleted_of(node_id, incarnation).await
        }
        async fn exits_owed(&self, build_id: &rdm_admin::build::BuildId, mesh: &str) -> Result<Vec<rdm_admin::build_state::OwedExit>, BuildStateError> {
            self.inner.exits_owed(build_id, mesh).await
        }
        async fn forget(&self, b: &BuildId) -> Result<(), BuildStateError> {
            self.inner.forget(b).await
        }
    }

    fn gate(e: &Estate, holds: Holds) -> Arc<Gate> {
        Arc::new(Gate { inner: e.builds.clone(), holds, arrived: Notify::new(), release: Notify::new() })
    }

    /// The fabric-primary's REST door over `builds`, on the estate's view and accepted Build, and its
    /// node records: one row per birth the estate created, as its nodes.storage holds them. The door
    /// accepts an action on a birth from its record (the atlas, R-FA1), never from the gossip view
    /// (Q197): the view says which births are unheard, the records that they were created.
    async fn door(e: &Estate, builds: Arc<dyn BuildStateAdapter>) -> Arc<ControlPlane> {
        let me: PathName = e.fabric_primary().await.expect("a fabric-primary");
        let mut cp = crate::control_plane(builds, e.accepted.clone(), me, e.view.read().await.clone(), crate::adopted_time());
        cp.topology = e.view.clone();
        crate::bind_records(&cp, &[&*e.view.read().await]).await;
        Arc::new(cp)
    }

    /// A peer mesh loses every node-admin: their runtimes exited and the fabric-primary's view no longer
    /// holds them, only their runtime rows do, so no mesh-primary is left to recover them and the drift
    /// pass has a birth to prove (a birth a view holds is the primary watch's, Q335; a member of a mesh
    /// with a live mesh-primary is that mesh-primary's). Returns the Build's attempt before anything opens.
    async fn lose_a_node(e: &Estate) -> u32 {
        let before = e.builds.current().await.unwrap().unwrap();
        let peer = if e.view.read().await.fabric_primary().unwrap().mesh == "mesh1" { "mesh2" } else { "mesh1" };
        let lost = [PathName::new(&format!("{peer}.admin"), "1").unwrap(), PathName::new(&format!("{peer}.admin"), "2").unwrap()];
        for l in &lost {
            e.world.kill(&e.node_id(l).await);
        }
        e.unheard(&[&lost[0], &lost[1]]).await;
        for l in &lost {
            e.to_rows_only(l).await;
        }
        before.attempt
    }

    async fn opened_facts(e: &Estate, attempt: u32) -> Vec<AttemptOpened> {
        e.builds.facts().await.unwrap().into_iter().filter_map(|f| match f { BuildFact::Opened(o) if o.attempt == attempt => Some(o), _ => None }).collect()
    }

    /// The proven-drift pass over `builds`, under an admission of its own: the number race of two
    /// authorities, which only the log's insert-and-fail decides.
    async fn drift_pass(e: &Estate, builds: &dyn BuildStateAdapter) -> Option<(BuildId, u32)> {
        drift_pass_under(e, builds, &tokio::sync::Mutex::new(())).await
    }

    /// The proven-drift pass over `builds`, under `admission`.
    async fn drift_pass_under(e: &Estate, builds: &dyn BuildStateAdapter, admission: &tokio::sync::Mutex<()>) -> Option<(BuildId, u32)> {
        let me: PathName = e.fabric_primary().await.expect("a fabric-primary");
        let t = e.view.read().await.clone();
        let mut started = HashSet::new();
        let rows = e.rows_only.lock().unwrap().clone();
        rdm_admin::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &e.digest_view, &e.provider, builds, &rdm_admin::build_claim::AttemptContexts::in_memory(), admission, &rows, &mut started, &mut Default::default(), &|_| None, &Default::default()).await
    }

    /// CONTRACT: the proven-drift pass has read the settled Build and computed attempt k+1 for a lost
    /// node when a requested stop of another node opens k+1 first. The stop holds the number:
    /// the drift pass is told it lost, opens nothing and returns no attempt to execute, and the one
    /// Opened fact of k+1 is the stop's. Canon: build_state.rs `AttemptOpened` (insert-and-fail on
    /// `(build_id, attempt)`), CLAUDE.md "an acknowledged write is the write that executes".
    #[tokio::test]
    async fn a_drift_attempt_that_loses_its_number_to_a_requested_stop_opens_nothing() {
        let e = estate().await;
        let k = lose_a_node(&e).await;
        let g = gate(&e, Holds::Attempt(AttemptReason::Steps));
        let cp = door(&e, e.builds.clone()).await;
        // The drift pass either stops at the log's door with its number computed, or it ended
        // without opening anything: that is the cell's failure, named, never a wait.
        let drift = drift_pass(&e, &*g);
        tokio::pin!(drift);
        tokio::select! {
            biased;
            _ = g.arrived.notified() => {}
            ended = &mut drift => panic!("the drift pass ended without reaching the Build log's door to open attempt {}: it proved no loss ({ended:?})", k + 1),
        }
        let rest = cp.open_attempt("POST /api/nodes/{name}/stop", AttemptReason::Stop, rdm_core::PathName::new("mesh2.rpc", "1").unwrap(), rdm_admin::control_plane::ActionKind::Stop).await;
        g.release.notify_one();
        let drift = drift.await;
        let rest = rest.expect("the stop took the free number");
        assert_eq!(rest.attempt, k + 1);
        let held = opened_facts(&e, k + 1).await;
        assert_eq!(held.len(), 1, "one Opened fact for attempt {}: {held:?}", k + 1);
        assert_eq!((held[0].reason, held[0].opened_by.as_str()), (AttemptReason::Stop, own_id(&*e.view.read().await, &cp.me).as_str()), "the number is the stop's: {held:?}");
        assert_eq!(drift, None, "the drift pass lost the number: it opened nothing and has no attempt to execute");
    }

    /// CONTRACT: the requested stop has read the settled Build and computed attempt k+1 when the
    /// proven-drift pass opens k+1 for a lost node first. The drift attempt holds the number: the
    /// stop is refused 409 `attempt-taken` naming the attempt, never acknowledged 202 for an attempt
    /// that replaces the lost node and stops nothing, and no second fact is appended.
    #[tokio::test]
    async fn a_requested_stop_that_loses_its_number_to_a_drift_attempt_is_refused_by_name() {
        let e = estate().await;
        let k = lose_a_node(&e).await;
        let g = gate(&e, Holds::Attempt(AttemptReason::Stop));
        let cp = door(&e, g.clone()).await;
        let rest = cp.open_attempt("POST /api/nodes/{name}/stop", AttemptReason::Stop, rdm_core::PathName::new("mesh2.rpc", "1").unwrap(), rdm_admin::control_plane::ActionKind::Stop);
        tokio::pin!(rest);
        // The stop either stops at the log's door with its number computed, or it ended without
        // opening anything: that is the cell's failure, named, never a wait.
        tokio::select! {
            biased;
            _ = g.arrived.notified() => {}
            ended = &mut rest => panic!("the stop ended without reaching the Build log's door to open attempt {}: {ended:?}", k + 1),
        }
        let drift = drift_pass(&e, &*e.builds).await;
        g.release.notify_one();
        let rest = rest.await;
        assert_eq!(drift, Some((e.builds.current_build().await.unwrap().into_build_id().unwrap(), k + 1)), "the drift pass took the free number");
        let held = opened_facts(&e, k + 1).await;
        assert_eq!(held.len(), 1, "one Opened fact for attempt {}: {held:?}", k + 1);
        assert_eq!(held[0].reason, AttemptReason::Steps, "the number is the drift attempt's, a single node.create step: {held:?}");
        let refusal = rest.expect_err("the stop lost the number and was told so");
        match refusal {
            Refusal::AttemptTaken(detail) => assert!(detail.contains(&format!("{}", k + 1)), "the refusal names the attempt: {detail}"),
            other => panic!("the stop was refused attempt-taken, not {other:?}"),
        }
    }

    /// CONTRACT: the proven-drift pass has read the accepted Build settled and computed attempt k+1
    /// for a lost node while a submitted topology change, past the same check, is inside its durable
    /// write. The submitted change is admitted as the next Build; the drift pass opens nothing on
    /// the Build that is no longer current, writes no Opened fact, and returns no attempt to
    /// execute. One Build is active: the submitted one. Must NOT happen: an attempt of a superseded
    /// Build opened beside the new Build, two plans of two topologies in flight. Canon:
    /// ops-naming-proposal.md "Two topology changes ... never run together: one Build is in flight".
    #[tokio::test]
    async fn a_drift_attempt_checked_while_a_submitted_change_is_written_opens_nothing() {
        let cell = "a_drift_attempt_checked_while_a_submitted_change_is_written_opens_nothing";
        let dir = evidence_dir(cell);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // The cell's own capture, in its own file of `dir`: this thread's default dispatcher, and
        // every task of the cell runs on this thread (a current-thread runtime).
        let capture = rdm_transport::EvidenceCapture::start(&dir, "rafka-node-admin-core-s35").expect("the cell's evidence file can be created");
        let root = tracing::info_span!("rdm.test.build.admission.drift-versus-submit");
        use tracing::Instrument as _;
        let e = estate().await;
        let k = lose_a_node(&e).await;
        let before = e.builds.current().await.unwrap().unwrap();
        let g = gate(&e, Holds::Acceptance);
        let cp = door(&e, g.clone()).await;
        let submit = tokio::spawn({
            let cp = cp.clone();
            async move { cp.submit("POST /api/topology", crate::named_request(&cp, rdm_admin::accepted::TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: rdm_core::NodeKind::RpcNode }).await).await }.instrument(root.clone())
        });
        g.arrived.notified().await;
        // The drift pass of the same fabric-primary: the admission its door holds.
        let mut pass = Box::pin(drift_pass_under(&e, &*e.builds, &cp.admission).instrument(root.clone()));
        // One poll of the drift pass while the submitted change is held inside its write.
        let early = tokio::select! {
            biased;
            r = &mut pass => Some(r),
            _ = std::future::ready(()) => None,
        };
        let waited = early.is_none();
        g.release.notify_one();
        let admitted = submit.await.unwrap().expect("the submitted change is admitted").build_id;
        let drift = match early {
            Some(r) => r,
            None => (&mut pass).await,
        };
        drop(pass);
        assert_eq!(drift, None, "the drift pass opened an attempt of {} beside the admitted Build {admitted}", before.build_id);
        assert!(waited, "the drift pass waited at the admission the submitted change held");
        let opened: Vec<AttemptOpened> = opened_facts(&e, k + 1).await.into_iter().filter(|o| o.build_id == before.build_id).collect();
        assert!(opened.is_empty(), "no attempt of the superseded Build is open: {opened:?}");
        let active = e.builds.list_active().await.unwrap();
        assert_eq!(active.iter().map(|b| b.build_id.clone()).collect::<Vec<_>>(), vec![admitted.clone()], "one Build is active: the submitted one");
        drop(root);
        let spans = capture.spans().expect("the cell's spans can be read");
        let refused: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.reject.via-build-in-progress").collect();
        assert_eq!(refused.len(), 1, "the drift pass is refused once, by name: {refused:?}");
        assert!(!spans.iter().any(|s| s["name"] == "rdm.node_admin.build.update.via-proven-drift"), "a refused drift pass opened nothing and names no attempt update");
        let created = spans.iter().find(|s| s["name"] == "rdm.node_admin.build.create.via-rest").expect("the submitted change's span");
        assert_eq!(refused[0]["trace_id"], created["trace_id"], "the refusal and the admitted Build are one story");
        let a = &refused[0]["attributes"];
        assert_eq!(a["route"], "proven-drift", "{a}");
        assert_eq!(a["build_id"], before.build_id.log().to_string().as_str(), "{a}");
        assert!(a["accepted_now"].as_str().unwrap_or_default().contains(admitted.as_str()), "the refusal names the Build admitted first: {a}");
        println!("trace_id={} admitted={admitted} refused_drift_on={}", refused[0]["trace_id"].as_str().unwrap_or_default(), before.build_id);
    }

}

/// CONTRACT: a departure is not drift. A birth whose proven departure the authority holds
/// (`NodeDeleted`) may still have its durable runtime row; that row names a runtime that left on
/// purpose, and no attempt repairs it. Must NOT happen: the deleted node is re-created as drift.
#[tokio::test]
async fn a_durable_runtime_row_of_a_deleted_birth_opens_no_attempt() {
    let e = estate().await;
    let lost = e.view.read().await.nodes.iter().find(|n| n.kind == NodeKind::NodeAdmin && !n.is_primary && !n.is_fabric_primary).expect("a non-primary admin").name.clone();
    let old = e.node_id(&lost).await;
    let (row, digest) = {
        let (d, _) = e.digest_view.get(&old).unwrap();
        (rdm_admin::storage::RuntimeRow { kind: d.node.kind, mesh: d.node.mesh.clone(), node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime: d.node.runtime.clone().unwrap(), data_dir: None }, d)
    };
    e.world.kill(&old);
    let heard = DigestView::default();
    for d in e.digest_view.all().into_iter().filter(|d| d.node.name != lost) {
        heard.record(d);
    }
    heard.depart(rdm_core::LifecycleOp { kind: digest.node.kind, mesh: digest.node.mesh.clone(), build_id: "b".into(), attempt: 1, operation: rdm_core::OperationId::node("node.delete", &digest.node.node_id), node_id: digest.node.node_id.clone(), incarnation: digest.node.incarnation.clone(), name: digest.node.name.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None });
    let mut t = e.view.read().await.clone();
    t.nodes.retain(|n| n.name != lost);
    rdm_admin::election::resolve(&mut t.nodes);
    let me: PathName = t.fabric_primary().unwrap().name.clone();
    let mut started = HashSet::new();
    let contexts = rdm_admin::build_claim::AttemptContexts::in_memory();
    let opened = rdm_admin::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &heard, &e.provider, &*e.builds, &contexts, &tokio::sync::Mutex::new(()), std::slice::from_ref(&row), &mut started, &mut Default::default(), &|_| None, &Default::default()).await;
    assert_eq!(opened, None, "a proven departure is held: its runtime row is not drift");
}

mod unheard_mesh_hold {
    use super::*;

    pub(super) type Held = HashSet<(NodeId, IncarnationId)>;

    /// The fabric primary's mesh, and the other one.
    pub(super) async fn meshes(e: &Estate) -> (String, String) {
        let fp = e.fabric_primary().await.expect("a fabric-primary");
        let own = e.view.read().await.node(&fp).unwrap().mesh.clone();
        let peer = SHAPE.iter().map(|(m, _, _)| (*m).to_string()).find(|m| *m != own).expect("a peer mesh");
        (own, peer)
    }

    /// One drift pass of the fabric-primary, `peer` naming how long each peer mesh has gone unheard past
    /// the staleness floor (R-D1).
    pub(super) async fn pass(e: &Estate, started: &mut HashSet<(BuildId, rdm_admin::admin::DriftMemo)>, held: &mut Held, peer: &(dyn Fn(&str) -> Option<u64> + Sync)) -> Option<(BuildId, u32)> {
        let records = exit_records();
        let me: PathName = e.fabric_primary().await.expect("a fabric-primary");
        let t = e.view.read().await.clone();
        // The provider's probe of what is recorded at a path, as the running admin's runner is (set once).
        records.set_path_probe(Arc::new(Probe(e.world.clone())));
        rdm_admin::admin::reconcile_drift(&crate::adopted_time(), &me, &own_id(&t, &me), &t, &e.digest_view, &e.provider, &*e.builds, &rdm_admin::build_claim::AttemptContexts::in_memory(), &tokio::sync::Mutex::new(()), &[], started, held, peer, &Default::default()).await
    }

    const UNHEARD: Option<u64> = Some(4_000);
    const HEARD: Option<u64> = None;

    #[tokio::test]
    async fn an_exited_birth_of_an_unheard_peer_mesh_opens_no_attempt_and_is_kept_as_evidence_once() {
        let e = estate().await;
        let (_, peer_mesh) = meshes(&e).await;
        let path = PathName::new(&format!("{peer_mesh}.rpc"), "1").unwrap();
        let id = e.node_id(&path).await;
        e.world.exit_with(&id, Some(137));
        e.unheard(&[&path]).await;
        let before = e.builds.current().await.unwrap().unwrap();
        let mut held = Held::new();
        let mut started = HashSet::new();
        let unheard = |_: &str| UNHEARD;
        assert_eq!(pass(&e, &mut started, &mut held, &unheard).await, None, "the mesh is unheard: its exited birth authorizes nothing");
        assert_eq!(held.len(), 1, "the evidence of the one exited birth is held: {held:?}");
        assert_eq!(pass(&e, &mut started, &mut held, &unheard).await, None);
        assert_eq!(held.len(), 1, "the same birth is one hold, not one per pass");
        let after = e.builds.current().await.unwrap().unwrap();
        assert_eq!(after.attempt, before.attempt, "no attempt was opened");
        assert!(e.born().is_empty(), "no node was created: {:?}", e.born());
    }

    /// The hold ends with the silence and leaves nothing behind; the exited ordinary birth is then its
    /// own mesh-primary's to recover (node-recovery.md), never the fabric-primary's.
    #[tokio::test]
    async fn the_hold_ends_when_the_mesh_is_heard_again_and_the_exited_birth_is_left_to_its_mesh_primary() {
        let e = estate().await;
        let (_, peer_mesh) = meshes(&e).await;
        let path = PathName::new(&format!("{peer_mesh}.rpc"), "1").unwrap();
        let id = e.node_id(&path).await;
        e.world.exit_with(&id, Some(137));
        e.unheard(&[&path]).await;
        let before = e.builds.current().await.unwrap().unwrap();
        let mut held = Held::new();
        let mut started = HashSet::new();
        assert_eq!(pass(&e, &mut started, &mut held, &|_| UNHEARD).await, None);
        assert_eq!(held.len(), 1);
        let opened = pass(&e, &mut started, &mut held, &|_| HEARD).await;
        assert_eq!(opened, None, "the mesh is heard again: its ordinary member is its mesh-primary's to recover");
        assert!(held.is_empty(), "the hold left nothing behind: {held:?}");
        let o = e.recover(&path).await;
        assert_eq!((o.build_id, o.mesh.as_deref(), o.attempt), (before.build_id.clone(), Some(peer_mesh.as_str()), 1));
        assert_eq!(e.born(), vec![path.clone()], "the mesh-primary's recovery puts the path back");
    }

    #[tokio::test]
    async fn a_birth_of_the_fabric_primarys_own_mesh_is_never_held() {
        let e = estate().await;
        let path = e.sibling_admin().await;
        let id = e.node_id(&path).await;
        e.world.exit_with(&id, Some(1));
        e.unheard(&[&path]).await;
        let before = e.builds.current().await.unwrap().unwrap();
        let mut held = Held::new();
        let mut started = HashSet::new();
        let opened = pass(&e, &mut started, &mut held, &|_| UNHEARD).await;
        assert_eq!(opened, None, "the own mesh's secondary node-admin is its mesh-primary's to recover, never the drift's");
        assert!(held.is_empty(), "the own mesh is heard by definition: nothing of it is held");
        let o = e.recover(&path).await;
        assert_eq!((o.build_id, o.mesh.as_deref(), o.attempt), (before.build_id.clone(), Some(e.view.read().await.node(&path).unwrap().mesh.as_str()), 1));
        assert_eq!(e.born(), vec![path.clone()], "its mesh-primary's recovery puts the path back");
    }

}

/// CONTRACT: a live birth the accepted topology does not name is surplus, deleted by an attempt of its
/// own, UNLESS the Build already receipted its deletion (`NodeDeleting` of that exact birth): that
/// operation deletes it, and this view is only late in hearing it go.
#[tokio::test]
async fn a_birth_whose_delete_the_build_receipted_is_not_surplus() {
    use rdm_admin::build_state::{BuildStateAdapter, BuildStepReceipt, StepOutcome};
    for receipted in [false, true] {
        let e = estate().await;
        let current = e.builds.current().await.unwrap().unwrap();
        let extra = birth(&e.world, &e.digest_view, &e.view.read().await.fabric.id.clone(), &PathName::new("mesh1.rpc", "9").unwrap(), "mesh1", NodeKind::RpcNode, None);
        e.view.write().await.nodes.push(extra.clone());
        if receipted {
            let op = rdm_core::LifecycleOp { kind: extra.kind, mesh: extra.mesh.clone(),
                build_id: current.build_id.to_string(),
                attempt: current.attempt,
                operation: rdm_core::OperationId::node("node.delete", &extra.node_id),
                node_id: extra.node_id.clone(),
                incarnation: extra.incarnation_id.clone().unwrap(),
                name: extra.name.clone(),
                event_at_rafka_ms: 0,
                traceparent: None,
                tracestate: None,
            };
            e.builds
                .append_step_receipt(&BuildStepReceipt { mesh: None, build_id: current.build_id.clone(), attempt: current.attempt, operation: op.operation.clone(), step: "NodeDeleting".into(), outcome: StepOutcome::Complete, output: serde_json::to_value(&op).ok(), executor: None })
                .await
                .unwrap();
            assert_eq!(e.drift().await, None, "a receipted deletion is not surplus");
        } else {
            assert!(e.drift().await.is_some(), "an unreceipted live birth outside the accepted topology is surplus");
        }
    }
}

/// The records a drift pass puts its exited verdicts through: a status store held in memory.
/// The node the attempt's action creates when it is the single step `node.create` of one node
/// (node-create.md "A node-admin created into a live mesh" step 1: the node-admin joins the live mesh on the same accepted Build); `None`
/// for any other action.
fn one_step_create_of(action: &Option<rdm_admin::accepted::AttemptAction>) -> Option<PathName> {
    use rdm_core::build::Step;
    match action {
        Some(rdm_admin::accepted::AttemptAction::Steps { steps, .. }) => match steps.as_slice() {
            [Step::Create { node, .. }] => Some(node.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn exit_records() -> rdm_admin::admin::Records {
    let r = rdm_admin::admin::Records::default();
    r.set_status(std::sync::Arc::new(rdm_admin::storage::MemoryNodesStorage::default()), std::sync::Arc::new(rdm_admin::storage::MemoryMeshStorage::default()), std::sync::Arc::new(rdm_admin::fabric_storage::MemoryFabricStorage::new()), "mesh1".into(), rdm_core::PathName::new("mesh1.admin", "1").unwrap(), rdm_core::NodeId::mint());
    r
}

mod refused_caller_terminate {
    use super::*;
    use rdm_admin::control_plane::{ControlPlane, Recovery, Refusal};

    /// CONTRACT (Q217, Luke: "if they come back and they are deleted they get actually deleted";
    /// node-investigation.md): a birth whose recovery wrote its record deleted and whose re-create has
    /// not completed (the path node recovery still owes a re-create) comes back and is decided
    /// `delete-deleted-birth`; its report for `node.terminate` is admitted by the fabric-primary, whose
    /// atlas holds its record deleted, as the next attempt of the current Build. Must NOT happen: the
    /// terminate refused `unknown-node` because the recovery's workflow has not completed, the caller's
    /// runtime left running. A birth whose record is live and that no workflow deleted is still
    /// refused `unknown-node`.
    #[tokio::test]
    async fn a_deleted_birth_whose_re_create_has_not_completed_is_admitted_for_node_terminate() {
        let e = estate().await;
        let me: PathName = e.fabric_primary().await.expect("a fabric-primary");
        let mut cp = crate::control_plane(e.builds.clone(), e.accepted.clone(), me.clone(), e.view.read().await.clone(), crate::adopted_time());
        cp.topology = e.view.clone();
        let rows = crate::bind_records(&cp, &[&*e.view.read().await]).await;
        let cp = Arc::new(cp);
        let before = e.builds.current().await.unwrap().unwrap();
        let path: PathName = secondary_rpc(&e, &e.view.read().await.node(&me).unwrap().mesh).await;
        let n = e.view.read().await.node(&path).cloned().unwrap();
        let incarnation = n.incarnation_id.clone().unwrap();
        // Live and deleted by no workflow: not one node.terminate acts on.
        let live = cp.open_terminate(tracing::Span::none(), "returned-birth", path.clone(), n.node_id.clone(), incarnation.clone()).await;
        assert!(matches!(live, Err(Refusal::Reject(rdm_admin::build::BuildReject::UnknownNode { .. }))), "a live birth no workflow deleted is refused unknown-node: {live:?}");
        // Its recovery's node.delete wrote its record deleted; the re-create has not completed.
        let mut row = rdm_admin::storage::NodeRow::new(path.clone(), n.mesh.clone(), n.kind, n.node_id.clone(), NodeStatus::Deleted);
        row.birth_mut(&incarnation, &path);
        {
            use rdm_admin::storage::NodesStorage as _;
            rows.put_node(&row).await.unwrap();
        }
        crate::bind_store(&cp, rows.clone(), &*e.view.read().await).await;
        let opened = match cp.open_terminate(tracing::Span::none(), "returned-birth", path.clone(), n.node_id.clone(), incarnation.clone()).await {
            Ok(Recovery::Opened(o)) => o,
            other => panic!("the deleted birth's node.terminate is admitted as the next attempt: {other:?}"),
        };
        assert_eq!((opened.build_id.clone(), opened.attempt), (before.build_id.clone(), before.attempt + 1), "the next attempt of the current Build");
        let action = e.builds.current().await.unwrap().unwrap().action;
        assert!(format!("{action:?}").contains("Terminate"), "the attempt terminates the exact birth: {action:?}");
    }

    /// CONTRACT (node-investigation.md; Q217; red in the tail at 72891eaf7, build_op's resumed executor:
    /// its mesh-primary decided it `delete-deleted-birth` at its join and reported it, and the
    /// fabric-primary refused the termination `unknown-node`, its atlas not yet holding the mesh-primary's
    /// deleted row and the recovery's re-create not completed, so the runtime was never ended): a birth a
    /// node workflow deleted, whose `NodeDeleted` receipt the current Build holds, is admitted for
    /// `node.terminate` whatever this fabric-primary's atlas holds of its row. Must NOT happen: the report
    /// refused `unknown-node` while the Build that deleted the birth holds its receipt.
    #[tokio::test]
    async fn a_birth_the_current_build_holds_deleted_is_admitted_for_node_terminate_before_its_row_is_heard() {
        let e = estate().await;
        let me: PathName = e.fabric_primary().await.expect("a fabric-primary");
        let mut cp = crate::control_plane(e.builds.clone(), e.accepted.clone(), me.clone(), e.view.read().await.clone(), crate::adopted_time());
        cp.topology = e.view.clone();
        crate::bind_records(&cp, &[&*e.view.read().await]).await;
        let cp = Arc::new(cp);
        let before = e.builds.current().await.unwrap().unwrap();
        let path: PathName = secondary_rpc(&e, &e.view.read().await.node(&me).unwrap().mesh).await;
        let n = e.view.read().await.node(&path).cloned().unwrap();
        let incarnation = n.incarnation_id.clone().unwrap();
        // The recovery's node.delete of this exact birth receipted NodeDeleted in the current Build; this
        // fabric-primary's atlas holds no deleted row of it.
        let operation = rdm_core::OperationId::node("node.delete", &n.node_id);
        let op = rdm_core::LifecycleOp { mesh: n.mesh.clone(), kind: n.kind, build_id: before.build_id.to_string(), attempt: before.attempt, operation: operation.clone(), node_id: n.node_id.clone(), incarnation: incarnation.clone(), name: path.clone(), event_at_rafka_ms: 0, traceparent: None, tracestate: None };
        e.builds
            .append_step_receipt(&rdm_admin::build_state::BuildStepReceipt { mesh: None, build_id: before.build_id.clone(), attempt: before.attempt, operation, step: "NodeDeleted".into(), outcome: rdm_admin::build_state::StepOutcome::Complete, output: serde_json::to_value(&op).ok(), executor: None })
            .await
            .unwrap();
        let opened = match cp.open_terminate(tracing::Span::none(), "returned-birth", path.clone(), n.node_id.clone(), incarnation.clone()).await {
            Ok(Recovery::Opened(o)) => o,
            other => panic!("the birth the current Build deleted is admitted for node.terminate: {other:?}"),
        };
        assert_eq!((opened.build_id.clone(), opened.attempt), (before.build_id.clone(), before.attempt + 1), "the next attempt of the current Build");
    }
}
