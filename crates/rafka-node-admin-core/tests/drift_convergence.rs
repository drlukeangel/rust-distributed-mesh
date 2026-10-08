//! i143 functional: node-admin's Build executor converges the topology after a loss, and only
//! after a proven one.
//!
//! The real chain runs end to end — `admin::reconcile_drift` (the fabric-primary inspects each
//! unheard birth's runtime and opens the next attempt of the accepted Build on proof),
//! `BuildExecutor::reconcile_active` (claim, `accepted::plan`, `executor_for`, hand-off, receipts),
//! `election::resolve` (the seats), and `fence::fence` before every create — over a world
//! fixture: a provider whose runtimes run or have exited, and a probe answering whether a birth
//! answers. Only the world is a fixture; every decision is the product code's.
//!
//! The cells: an exited rpc node is reborn once; a silent node whose runtime runs opens no
//! attempt and is never replaced; a lost mesh-primary admin and a lost fabric-primary admin are
//! reborn by the right executor; members unheard through a lost forwarder are fenced, never
//! duplicated; a path the view does not hold but where a runtime runs is held. After every
//! loss: exactly the lost paths have new births, every path of the accepted topology has
//! exactly one live birth, and the accepted Build is complete with one more attempt.

use rafka_mesh_entity::{MeshDigest, MeshNode, MemberStatus, RuntimeFact, RuntimeLocator, RuntimeProvider};
use rafka_mesh_transport::membership::DigestBook;
use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology, MeshTopology};
use rafka_node_admin_core::build::{BuildId, BuildOperation};
use rafka_node_admin_core::build_state::{BuildState, MemoryBuildStateAdapter};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::executor::{BuildExecutor, OperationRunner};
use rafka_node_admin_core::fence::{fence, FenceOutcome, PathProbe};
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::topology::Topology;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

const DOMAIN: &str = "process:test-world";

/// One runtime of the world: which birth it is, and whether it runs.
#[derive(Debug, Clone)]
struct Runtime {
    path: String,
    node_id: String,
    pid: u32,
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
    fn add(&self, path: &str, node_id: &str) -> u32 {
        let mut p = self.next_pid.lock().unwrap();
        *p += 1;
        let pid = 1000 + *p;
        self.runtimes.lock().unwrap().insert(pid, Runtime { path: path.into(), node_id: node_id.into(), pid, status: DeploymentStatus::Running, answers: true });
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

#[async_trait::async_trait]
impl DeploymentProvider for Provider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        DOMAIN.into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn { node: spec.node.to_string(), reason: "births are made by the runner in this world".into() })
    }
    async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        Ok(())
    }
    async fn inspect(&self, h: &DeploymentHandle) -> DeploymentStatus {
        h.pid.and_then(|p| self.0.runtimes.lock().unwrap().get(&p).map(|r| r.status.clone())).unwrap_or(DeploymentStatus::Unknown)
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
        None
    }
}

/// The fence's questions, answered by the world.
struct Probe(Arc<World>);

#[async_trait::async_trait]
impl PathProbe for Probe {
    async fn answers(&self, node: &Node) -> bool {
        self.0.of(node.node_id.as_str()).is_some_and(|r| r.answers)
    }
    async fn inspect(&self, node: &Node) -> Result<DeploymentStatus, String> {
        self.0.of(node.node_id.as_str()).map(|r| r.status).ok_or_else(|| format!("{} has no runtime in this world", node.name))
    }
    async fn recorded(&self, path: &PathName) -> Vec<(String, DeploymentStatus)> {
        self.0.runtimes.lock().unwrap().values().filter(|r| r.path == path.to_string()).map(|r| (format!("pid {}", r.pid), r.status.clone())).collect()
    }
}

fn fact(pid: u32) -> RuntimeFact {
    RuntimeFact { deployment_id: format!("dep-{pid}"), provider: RuntimeProvider::Process, control_domain: DOMAIN.into(), locator: RuntimeLocator::Process { pid, start: 1 } }
}

/// The estate: the world, the accepted Build, the admins' shared view and membership.
struct Estate {
    world: Arc<World>,
    builds: Arc<MemoryBuildStateAdapter>,
    accepted: Arc<AcceptedStore>,
    view: Arc<RwLock<Topology>>,
    book: DigestBook,
    provider: Provider,
    runner: Arc<Runner>,
}

const SHAPE: [(&str, u32, u32); 2] = [("mesh1", 2, 3), ("mesh2", 2, 3)];

/// Every path of the accepted shape born, live, held in the view and membership; seats elected.
async fn estate() -> Estate {
    let world = Arc::new(World::default());
    let fabric_id = FabricId::mint();
    let topology = FabricTopology { fabric: "fabric1".into(), meshes: SHAPE.iter().map(|(m, a, r)| ((*m).to_string(), MeshTopology::of(&rafka_node_admin_core::build::MeshDesired::of(*m, [(rafka_mesh_entity::NodeKind::NodeAdmin, *a), (rafka_mesh_entity::NodeKind::RpcNode, *r)])))).collect() };
    let mut nodes = Vec::new();
    let book = DigestBook::default();
    for m in topology.meshes.values() {
        for p in &m.nodes {
            nodes.push(birth(&world, &book, &fabric_id, &p.to_string(), None));
        }
    }
    rafka_node_admin_core::election::resolve(&mut nodes);
    let view = Arc::new(RwLock::new(Topology {
        fabric: Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: SHAPE.iter().map(|(m, _, _)| Mesh { id: Some(MeshId::mint()), name: (*m).into(), status: ScopeStatus::ReadyForTraffic }).collect(),
        nodes,
    }));
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let fp = view.read().await.fabric_primary().unwrap().name.to_string();
    let accepted = AcceptedStore::seeded(&*builds, fabric_id.clone(), topology, &fp).await.unwrap();
    let runner = Arc::new(Runner { world: world.clone(), view: view.clone(), book: book.clone(), fabric_id, ran: Mutex::new(Vec::new()), born: Mutex::new(Vec::new()), restarted: Mutex::new(Vec::new()), hear_after_birth: std::sync::atomic::AtomicBool::new(false) });
    Estate { provider: Provider(world.clone()), world, builds, accepted, view, book, runner }
}

/// A new birth at `path`: a runtime in the world, a digest in membership, a live node.
fn birth(world: &World, book: &DigestBook, fabric_id: &FabricId, path: &str, supersedes: Option<IncarnationId>) -> Node {
    birth_as(world, book, fabric_id, path, supersedes, NodeId::mint())
}

/// [`birth`] under a given NodeId: a restart's new incarnation of the same node.
fn birth_as(world: &World, book: &DigestBook, fabric_id: &FabricId, path: &str, supersedes: Option<IncarnationId>, node_id: NodeId) -> Node {
    let mut n = Node::allocated(path.parse().unwrap());
    n.node_id = node_id;
    n.incarnation_id = Some(IncarnationId::mint());
    n.endpoint_id = Some(EndpointId(iroh::SecretKey::generate().public().to_string()));
    n.status = NodeStatus::ReadyForTraffic;
    let pid = world.add(path, n.node_id.as_str());
    book.record(MeshDigest {
        fabric_id: fabric_id.clone(),
        node: MeshNode {
            node_id: n.node_id.clone(),
            name: n.name.clone(),
            endpoint_id: n.endpoint_id.clone().unwrap(),
            transport_addr: "127.0.0.1:1".parse().unwrap(),
            incarnation: n.incarnation_id.clone().unwrap(),
            supersedes,
            runtime: Some(fact(pid)),
        },
        status: MemberStatus::ReadyForTraffic,
        admin_api_base: None,
        emitted_at_rafka_ms: 0,
        digest_seq: 1,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        extra: Default::default(),
    });
    n
}

/// Realises `CreateNode` as the admin does: the real fence first; a birth only when it clears.
struct Runner {
    world: Arc<World>,
    view: Arc<RwLock<Topology>>,
    book: DigestBook,
    fabric_id: FabricId,
    ran: Mutex<Vec<(String, BuildOperation, FenceOutcome)>>,
    born: Mutex<Vec<String>>,
    restarted: Mutex<Vec<String>>,
    /// Membership repair runs right after every birth, before the attempt's next operation: what a
    /// reborn forwarder does to the executor's view mid-attempt.
    hear_after_birth: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl OperationRunner for Runner {
    async fn run(&self, _: &BuildId, _attempt: u32, op: &BuildOperation) -> Result<(), String> {
        if let BuildOperation::RestartNode { node: path } = op {
            // The same identity, a new incarnation: the exited birth's NodeId is kept.
            let prev = self.view.read().await.node(path).cloned().ok_or_else(|| format!("{path} is not in the view"))?;
            self.restarted.lock().unwrap().push(path.to_string());
            let n = birth_as(&self.world, &self.book, &self.fabric_id, &path.to_string(), prev.incarnation_id, prev.node_id);
            let mut v = self.view.write().await;
            v.nodes.retain(|x| x.name != *path);
            v.nodes.push(n);
            rafka_node_admin_core::election::resolve(&mut v.nodes);
            return Ok(());
        }
        let BuildOperation::CreateNode { node: path, .. } = op else { return Err(format!("unexpected {op:?}")) };
        let prev = self.view.read().await.node(path).cloned();
        let out = fence(path, prev.clone(), &Probe(self.world.clone())).await;
        self.ran.lock().unwrap().push((path.to_string(), op.clone(), out.clone()));
        if matches!(out, FenceOutcome::Clear { .. }) {
            let n = birth(&self.world, &self.book, &self.fabric_id, &path.to_string(), prev.and_then(|p| p.incarnation_id));
            let mut v = self.view.write().await;
            v.nodes.retain(|x| x.name != *path);
            v.nodes.push(n);
            rafka_node_admin_core::election::resolve(&mut v.nodes);
            self.born.lock().unwrap().push(path.to_string());
            drop(v);
            if self.hear_after_birth.load(std::sync::atomic::Ordering::SeqCst) {
                hear(&self.world, &self.view).await;
            }
        }
        Ok(())
    }
}

/// Membership repair: every `Dead` node whose runtime runs and answers, in a mesh with a live admin
/// to forward it, is heard again.
async fn hear(world: &World, view: &RwLock<Topology>) {
    let mut v = view.write().await;
    let forwarded: BTreeSet<String> = v.nodes.iter().filter(|n| n.kind == NodeKind::NodeAdmin && n.status.is_live()).map(|n| n.mesh.clone()).collect();
    let mut changed = false;
    for n in v.nodes.iter_mut().filter(|n| n.status == NodeStatus::PendingReconnect && forwarded.contains(&n.mesh)) {
        if world.of(n.node_id.as_str()).is_some_and(|r| matches!(r.status, DeploymentStatus::Running) && r.answers) {
            n.status = NodeStatus::ReadyForTraffic;
            changed = true;
        }
    }
    if changed {
        rafka_node_admin_core::election::resolve(&mut v.nodes);
    }
}

impl Estate {
    async fn hear(&self) {
        hear(&self.world, &self.view).await;
    }

    /// A view change: `paths` are unheard (Dead) in the view; the seats are elected again.
    async fn unheard(&self, paths: &[&str]) {
        let mut v = self.view.write().await;
        for n in v.nodes.iter_mut().filter(|n| paths.contains(&n.name.to_string().as_str())) {
            n.status = NodeStatus::PendingReconnect;
        }
        rafka_node_admin_core::election::resolve(&mut v.nodes);
    }

    async fn node_id(&self, path: &str) -> String {
        self.view.read().await.nodes.iter().find(|n| n.name.to_string() == path).unwrap().node_id.to_string()
    }

    async fn fabric_primary(&self) -> String {
        self.view.read().await.fabric_primary().map(|n| n.name.to_string()).unwrap_or_default()
    }

    /// The fabric-primary's drift pass, as the admin's executor loop runs it.
    async fn drift(&self) -> Option<(BuildId, u32)> {
        let me: PathName = self.fabric_primary().await.parse().ok()?;
        let t = self.view.read().await.clone();
        let mut started = HashSet::new();
        rafka_node_admin_core::admin::reconcile_drift(&me, &t, &self.accepted, &self.book, &self.provider, &*self.builds, &rafka_node_admin_core::build_claim::AttemptContexts::in_memory(), &[], &mut started, &|_| false).await
    }

    /// Every live admin runs its executor until no Build is active: claims, hand-offs and
    /// receipts are the product's. Between passes membership does what gossip does in the
    /// product: a runtime that runs and answers, in a mesh that has a live admin to forward its
    /// digests, is heard again and leaves `Dead` in the view.
    async fn converge(&self) {
        for _ in 0..8 {
            let admins: Vec<String> = self.view.read().await.nodes.iter().filter(|n| n.kind == NodeKind::NodeAdmin && n.status.is_live()).map(|n| n.name.to_string()).collect();
            for a in admins {
                // The claim is put to the fabric-primary's door, as the product puts it over Node RPC.
                let primary: PathName = self.fabric_primary().await.parse().expect("a fabric-primary");
                let door = Arc::new(rafka_node_admin_core::build_claim::ClaimDoor {
                    me: primary,
                    topology: self.view.clone(),
                    builds: self.builds.clone(),
                    contexts: Arc::new(rafka_node_admin_core::build_claim::AttemptContexts::in_memory()),
                });
                let exec = BuildExecutor {
                    executor: a.clone(),
                    accepted: self.accepted.clone(),
                    builds: self.builds.clone(),
                    topology: self.view.clone(),
                    runner: self.runner.clone(),
                    claimer: Arc::new(rafka_node_admin_core::build_claim::DoorClaimer(door)),
                };
                let done = exec.reconcile_active().await;
                if std::env::var("CONVERGE_TRACE").is_ok() {
                    eprintln!("converge: {a} -> {done:?}");
                }
            }
            self.hear().await;
            let current = self.accepted.current(&*self.builds).await.unwrap();
            if !matches!(current.state, BuildState::Pending | BuildState::Running) {
                return;
            }
        }
        panic!("the accepted Build did not finish: {:?}", self.accepted.current(&*self.builds).await.map(|b| (b.state, b.attempt)));
    }

    /// The topology holds: every path of the accepted shape has exactly one live birth.
    async fn holds_the_shape(&self) {
        let v = self.view.read().await;
        let mut per_path: BTreeMap<String, usize> = BTreeMap::new();
        for n in v.nodes.iter().filter(|n| n.status.is_live()) {
            *per_path.entry(n.name.to_string()).or_default() += 1;
        }
        let want: BTreeSet<String> = SHAPE.iter().flat_map(|(m, a, r)| MeshTopology::of(&rafka_node_admin_core::build::MeshDesired::of(*m, [(rafka_mesh_entity::NodeKind::NodeAdmin, *a), (rafka_mesh_entity::NodeKind::RpcNode, *r)])).nodes.into_iter().map(|p| p.to_string())).collect();
        assert_eq!(per_path.keys().cloned().collect::<BTreeSet<_>>(), want, "every accepted path is live");
        assert!(per_path.values().all(|c| *c == 1), "one live birth per path: {per_path:?}");
        // And the world: one running runtime per path.
        let mut running: BTreeMap<String, usize> = BTreeMap::new();
        for r in self.world.runtimes.lock().unwrap().values().filter(|r| matches!(r.status, DeploymentStatus::Running)) {
            *running.entry(r.path.clone()).or_default() += 1;
        }
        assert!(running.values().all(|c| *c == 1), "one running runtime per path in the world: {running:?}");
    }

    fn born(&self) -> Vec<String> {
        self.runner.born.lock().unwrap().clone()
    }

    fn fence_of(&self, path: &str) -> Vec<FenceOutcome> {
        self.runner.ran.lock().unwrap().iter().filter(|(p, _, _)| p == path).map(|(_, _, o)| o.clone()).collect()
    }
}

#[tokio::test]
async fn an_exited_rpc_node_is_reborn_once_and_nothing_else_is_created() {
    let e = estate().await;
    let before = e.accepted.current(&*e.builds).await.unwrap();
    let old = e.node_id("mesh1.rpc.2").await;
    e.world.kill(&old);
    e.unheard(&["mesh1.rpc.2"]).await;
    let (build, attempt) = e.drift().await.expect("a proven loss opens the next attempt");
    assert_eq!((build.clone(), attempt), (before.build_id.clone(), before.attempt + 1), "the same Build, one more attempt");
    e.converge().await;
    assert_eq!(e.born(), vec!["mesh1.rpc.2".to_string()], "exactly the lost path is reborn");
    assert!(matches!(e.fence_of("mesh1.rpc.2").as_slice(), [FenceOutcome::Clear { gone: Some(_) }]), "{:?}", e.fence_of("mesh1.rpc.2"));
    assert_ne!(e.node_id("mesh1.rpc.2").await, old, "a new birth, not the old one");
    e.holds_the_shape().await;
    let after = e.accepted.current(&*e.builds).await.unwrap();
    assert_eq!((after.build_id, after.attempt, after.state), (before.build_id, before.attempt + 1, BuildState::Complete));
}

#[tokio::test]
async fn a_silent_node_whose_runtime_runs_opens_no_attempt_and_is_never_replaced() {
    let e = estate().await;
    let before = e.accepted.current(&*e.builds).await.unwrap();
    let frozen = e.node_id("mesh1.rpc.2").await;
    e.world.freeze(&frozen);
    e.unheard(&["mesh1.rpc.2"]).await;
    assert_eq!(e.drift().await, None, "silence is not proof: no attempt");
    e.converge().await;
    assert!(e.born().is_empty(), "nothing was created: {:?}", e.born());
    let after = e.accepted.current(&*e.builds).await.unwrap();
    assert_eq!((after.attempt, after.state), (before.attempt, BuildState::Complete));
    assert_eq!(e.world.of(&frozen).unwrap().status, DeploymentStatus::Running, "the frozen runtime still runs");
}

#[tokio::test]
async fn a_lost_mesh_primary_admin_is_reborn_and_only_it() {
    let e = estate().await;
    let fp = e.fabric_primary().await;
    let fp_mesh = fp.split('.').next().unwrap().to_string();
    let other = if fp_mesh == "mesh1" { "mesh2" } else { "mesh1" };
    // The other mesh's primary admin is lost.
    let primary = e.view.read().await.cohort_primary(other, NodeKind::NodeAdmin).unwrap().name.to_string();
    e.world.kill(&e.node_id(&primary).await);
    e.unheard(&[&primary]).await;
    assert_ne!(e.view.read().await.cohort_primary(other, NodeKind::NodeAdmin).unwrap().name.to_string(), primary, "the surviving admin holds its mesh's seat");
    e.drift().await.expect("a proven loss opens the next attempt");
    e.converge().await;
    assert_eq!(e.born(), vec![primary.clone()], "exactly the lost admin is reborn");
    e.holds_the_shape().await;
}

#[tokio::test]
async fn a_lost_fabric_primary_is_proven_and_reborn_by_its_successor() {
    let e = estate().await;
    let fp = e.fabric_primary().await;
    e.world.kill(&e.node_id(&fp).await);
    e.unheard(&[&fp]).await;
    let successor = e.fabric_primary().await;
    assert!(!successor.is_empty() && successor != fp, "the seat moved: {successor}");
    e.drift().await.expect("the successor proves the loss and opens the next attempt");
    e.converge().await;
    assert_eq!(e.born(), vec![fp.clone()], "exactly the lost fabric primary's path is reborn");
    e.holds_the_shape().await;
}

#[tokio::test]
async fn members_unheard_through_a_lost_forwarder_are_fenced_never_duplicated() {
    let e = estate().await;
    let fp = e.fabric_primary().await;
    let fp_mesh = fp.split('.').next().unwrap().to_string();
    let lost = if fp_mesh == "mesh1" { "mesh2" } else { "mesh1" };
    // The other mesh's primary admin dies; in the fabric primary's view every member of that mesh
    // goes silent at once (its forwarder is gone), though all but the dead admin run and answer.
    let primary = e.view.read().await.cohort_primary(lost, NodeKind::NodeAdmin).unwrap().name.to_string();
    e.world.kill(&e.node_id(&primary).await);
    let members: Vec<String> = e.view.read().await.nodes.iter().filter(|n| n.mesh == lost).map(|n| n.name.to_string()).collect();
    let refs: Vec<&str> = members.iter().map(String::as_str).collect();
    e.unheard(&refs).await;
    e.drift().await.expect("the one proven loss opens the next attempt");
    e.converge().await;
    assert_eq!(e.born(), vec![primary.clone()], "only the proven-exited admin is reborn: {:?}", e.runner.ran.lock().unwrap());
    for m in members.iter().filter(|m| **m != primary) {
        assert!(e.fence_of(m).iter().all(|o| *o == FenceOutcome::Alive), "{m} answers: never created over: {:?}", e.fence_of(m));
    }
    // Heard again through the reborn forwarder: the topology holds.
    e.holds_the_shape().await;
}

#[tokio::test]
async fn a_path_the_view_does_not_hold_but_where_a_runtime_runs_is_held() {
    let e = estate().await;
    // rpc.3 is not in this view at all (never heard here), but its runtime runs, frozen.
    let hidden = e.node_id("mesh1.rpc.3").await;
    e.world.freeze(&hidden);
    e.view.write().await.nodes.retain(|n| n.name.to_string() != "mesh1.rpc.3");
    // A real loss elsewhere opens an attempt whose plan also names the path the view lacks.
    e.world.kill(&e.node_id("mesh1.rpc.2").await);
    e.unheard(&["mesh1.rpc.2"]).await;
    e.drift().await.expect("the proven loss opens the next attempt");
    e.converge().await;
    assert_eq!(e.born(), vec!["mesh1.rpc.2".to_string()], "the absent-but-running path is held, not created over");
    assert_eq!(e.fence_of("mesh1.rpc.3"), vec![FenceOutcome::Held]);
}

/// The seeded soak's finding (seed 7, round 3, trace in the spans of
/// `fabric_soak__seeded`): the lost mesh's primary is reborn as the attempt's first operation, its
/// members are heard again through it before the attempt's next operation, and that operation
/// must find them live and leave them alone. A plan made on the older view is not a licence.
#[tokio::test]
async fn members_heard_again_mid_attempt_through_the_reborn_forwarder_are_never_created_over() {
    let e = estate().await;
    e.runner.hear_after_birth.store(true, std::sync::atomic::Ordering::SeqCst);
    let fp = e.fabric_primary().await;
    let fp_mesh = fp.split('.').next().unwrap().to_string();
    let lost = if fp_mesh == "mesh1" { "mesh2" } else { "mesh1" };
    let primary = e.view.read().await.cohort_primary(lost, NodeKind::NodeAdmin).unwrap().name.to_string();
    e.world.kill(&e.node_id(&primary).await);
    let members: Vec<String> = e.view.read().await.nodes.iter().filter(|n| n.mesh == lost).map(|n| n.name.to_string()).collect();
    let refs: Vec<&str> = members.iter().map(String::as_str).collect();
    e.unheard(&refs).await;
    e.drift().await.expect("the one proven loss opens the next attempt");
    e.converge().await;
    assert_eq!(e.born(), vec![primary.clone()], "only the proven-exited admin is reborn: {:?}", e.runner.ran.lock().unwrap());
    for m in members.iter().filter(|m| **m != primary) {
        assert!(e.fence_of(m).iter().all(|o| *o == FenceOutcome::Alive), "{m} is live in the view by the time its operation runs: never created over: {:?}", e.fence_of(m));
    }
    e.holds_the_shape().await;
}

/// CONTRACT: a runtime whose exit the provider proves carries the TRANSPORT_STOPPED code (4) is
/// restarted: the next attempt of the same Build is a ProvenDrift Restart fenced to that birth,
/// and the path is re-created under the SAME NodeId with a new incarnation. Nothing else is born.
#[tokio::test]
async fn a_transport_stopped_exit_is_restarted_with_the_same_identity() {
    let e = estate().await;
    let before = e.accepted.current(&*e.builds).await.unwrap();
    let id = e.node_id("mesh1.rpc.2").await;
    let old_inc = e.view.read().await.node(&"mesh1.rpc.2".parse().unwrap()).unwrap().incarnation_id.clone().unwrap();
    e.world.exit_with(&id, Some(rafka_mesh_entity::runtime::TRANSPORT_STOPPED_EXIT_CODE));
    e.unheard(&["mesh1.rpc.2"]).await;
    e.drift().await.expect("a proven loss opens the next attempt");
    let opened = e.accepted.current(&*e.builds).await.unwrap();
    assert_eq!(opened.reason, rafka_node_admin_core::build_state::AttemptReason::ProvenDrift);
    assert_eq!(opened.action, Some(rafka_node_admin_core::accepted::AttemptAction::Restart { path: "mesh1.rpc.2".parse().unwrap(), from_incarnation: old_inc.clone() }));
    e.converge().await;
    assert_eq!(e.runner.restarted.lock().unwrap().clone(), vec!["mesh1.rpc.2".to_string()]);
    assert!(e.born().is_empty(), "no new node is created: {:?}", e.born());
    assert_eq!(e.node_id("mesh1.rpc.2").await, id, "the same NodeId");
    assert_ne!(e.view.read().await.node(&"mesh1.rpc.2".parse().unwrap()).unwrap().incarnation_id, Some(old_inc), "a new incarnation");
    e.holds_the_shape().await;
    let after = e.accepted.current(&*e.builds).await.unwrap();
    assert_eq!((after.build_id, after.attempt, after.state), (before.build_id, before.attempt + 1, BuildState::Complete));
}

/// CONTRACT: every other unplanned exit, and an exit whose reason is unknown, is replaced: a
/// Replace attempt fenced to the exited birth, a new node at the path. Three exited births are
/// three attempts, one per pass, lowest path.name first.
#[tokio::test]
async fn every_other_exit_is_replaced_one_birth_per_attempt_in_path_order() {
    let e = estate().await;
    let before = e.accepted.current(&*e.builds).await.unwrap();
    let (a, b, c) = (e.node_id("mesh1.rpc.1").await, e.node_id("mesh1.rpc.2").await, e.node_id("mesh1.rpc.3").await);
    e.world.exit_with(&c, Some(1));
    e.world.exit_with(&a, None);
    e.world.exit_with(&b, Some(137));
    e.unheard(&["mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.3"]).await;
    for (pass, path) in ["mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.3"].into_iter().enumerate() {
        let opened = e.drift().await;
        assert!(opened.is_some(), "pass {pass}: an exited birth opens the next attempt; born so far {:?}", e.born());
        assert_eq!(opened.unwrap().1, before.attempt + 1 + pass as u32);
        let current = e.accepted.current(&*e.builds).await.unwrap();
        let Some(rafka_node_admin_core::accepted::AttemptAction::Replace { path: p, .. }) = current.action else { panic!("pass {pass}: not a Replace: {:?}", current.action) };
        assert_eq!(p.to_string(), path, "pass {pass}: the lowest remaining exited path");
        e.converge().await;
        assert_eq!(e.born().last().map(String::as_str), Some(path));
    }
    assert_eq!(e.born().len(), 3);
    assert!(e.runner.restarted.lock().unwrap().is_empty());
    assert!(e.fence_of("mesh1.rpc.2").iter().all(|o| matches!(o, FenceOutcome::Clear { gone: Some(_) })));
    e.holds_the_shape().await;
    assert_eq!(e.drift().await, None, "nothing is left to repair");
}

/// A successor fabric primary that never heard a dead sibling holds nothing of it but the durable
/// row its predecessor handed over: that row is the exact runtime to inspect.
#[tokio::test]
async fn a_successor_proves_a_sibling_admins_exit_from_its_durable_runtime_row() {
    let e = estate().await;
    let before = e.accepted.current(&*e.builds).await.unwrap();
    // mesh1's cohort is lost; its second admin is one the successor never heard.
    let lost = "mesh1.admin.2";
    let old = e.node_id(lost).await;
    let row = {
        let (d, _) = e.book.get(&old).unwrap();
        rafka_node_admin_core::storage::RuntimeRow { node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime: d.node.runtime.clone().unwrap(), data_dir: None }
    };
    e.world.kill(&old);
    // The successor's membership and view: everything but the lost admin.
    let heard = DigestBook::default();
    for d in e.book.all().into_iter().filter(|d| d.node.name.to_string() != lost) {
        heard.record(d);
    }
    let mut t = e.view.read().await.clone();
    t.nodes.retain(|n| n.name.to_string() != lost);
    rafka_node_admin_core::election::resolve(&mut t.nodes);
    let me: PathName = t.fabric_primary().unwrap().name.clone();
    let mut started = HashSet::new();
    let contexts = rafka_node_admin_core::build_claim::AttemptContexts::in_memory();
    let opened = rafka_node_admin_core::admin::reconcile_drift(&me, &t, &e.accepted, &heard, &e.provider, &*e.builds, &contexts, std::slice::from_ref(&row), &mut started, &|_| false).await;
    assert_eq!(opened, Some((before.build_id.clone(), before.attempt + 1)), "the durable row's exact runtime exited: the next attempt of the same Build is open");
}

#[tokio::test]
async fn a_durable_runtime_row_of_a_runtime_that_runs_opens_no_attempt() {
    let e = estate().await;
    let lost = "mesh1.admin.2";
    let old = e.node_id(lost).await;
    let row = {
        let (d, _) = e.book.get(&old).unwrap();
        rafka_node_admin_core::storage::RuntimeRow { node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime: d.node.runtime.clone().unwrap(), data_dir: None }
    };
    e.world.freeze(&old);
    let heard = DigestBook::default();
    for d in e.book.all().into_iter().filter(|d| d.node.name.to_string() != lost) {
        heard.record(d);
    }
    let mut t = e.view.read().await.clone();
    t.nodes.retain(|n| n.name.to_string() != lost);
    rafka_node_admin_core::election::resolve(&mut t.nodes);
    let me: PathName = t.fabric_primary().unwrap().name.clone();
    let mut started = HashSet::new();
    let contexts = rafka_node_admin_core::build_claim::AttemptContexts::in_memory();
    let opened = rafka_node_admin_core::admin::reconcile_drift(&me, &t, &e.accepted, &heard, &e.provider, &*e.builds, &contexts, std::slice::from_ref(&row), &mut started, &|_| false).await;
    assert_eq!(opened, None, "a silent member whose runtime runs is held, never replaced");
}
