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
        load: None,
        gossip: None,
    });
    n
}

/// Realises `CreateNode` and `ReplaceNode` as the admin does: the real fence first; a birth only when it clears.
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
        // A replacement is realised like a creation at a path whose exact birth the provider's proof
        // clears: the fence stands for the exact terminal proof the replace's stop step obtains.
        let (BuildOperation::CreateNode { node: path } | BuildOperation::ReplaceNode { node: path, .. }) = op else { return Err(format!("unexpected {op:?}")) };
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
        rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &t, &self.accepted, &self.book, &self.provider, &*self.builds, &rafka_node_admin_core::build_claim::AttemptContexts::in_memory(), &[], &mut started, &mut Default::default(), &|_| None, &|_| false, &|_| false, &|_, _| {}).await
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
    // A real loss elsewhere opens an attempt whose plan is exactly that birth: the path the view
    // lacks is its own attempt's, so this one neither creates over it nor fences it.
    e.world.kill(&e.node_id("mesh1.rpc.2").await);
    e.unheard(&["mesh1.rpc.2"]).await;
    e.drift().await.expect("the proven loss opens the next attempt");
    e.converge().await;
    assert_eq!(e.born(), vec!["mesh1.rpc.2".to_string()], "the absent-but-running path is not created over");
    assert_eq!(e.fence_of("mesh1.rpc.3"), vec![], "an attempt that carries an action plans only its birth");
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
    let opened = rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &t, &e.accepted, &heard, &e.provider, &*e.builds, &contexts, std::slice::from_ref(&row), &mut started, &mut Default::default(), &|_| None, &|_| false, &|_| false, &|_, _| {}).await;
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
    let opened = rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &t, &e.accepted, &heard, &e.provider, &*e.builds, &contexts, std::slice::from_ref(&row), &mut started, &mut Default::default(), &|_| None, &|_| false, &|_| false, &|_, _| {}).await;
    assert_eq!(opened, None, "a silent member whose runtime runs is held, never replaced");
}

/// Two openers of one attempt number: the requested attempt (REST) and the proven-drift pass read the
/// same settled Build, so both compute the next attempt as `k + 1`. The first append takes the number;
/// the loser is told by name, never acknowledged for an attempt that carries someone else's action.
mod one_attempt_number {
    use super::*;
    use axum::response::IntoResponse;
    use rafka_node_admin_core::build_state::{AttemptOpened, AttemptReason, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome};
    use rafka_node_admin_core::http::ControlPlane;
    use tokio::sync::Notify;

    /// The estate's Build log with one gate: an `open_attempt` of `reason` stops at the log's door
    /// (after its caller computed the number) until the cell lets it write.
    struct Gate {
        inner: Arc<MemoryBuildStateAdapter>,
        holds: AttemptReason,
        arrived: Notify,
        release: Notify,
    }

    #[async_trait::async_trait]
    impl BuildStateAdapter for Gate {
        async fn publish_accepted(&self, a: &BuildAccepted) -> Result<(), BuildStateError> {
            self.inner.publish_accepted(a).await
        }
        async fn open_attempt(&self, o: &AttemptOpened) -> Result<(), BuildStateError> {
            if o.reason == self.holds {
                self.arrived.notify_one();
                self.release.notified().await;
            }
            self.inner.open_attempt(o).await
        }
        async fn read_build(&self, b: &BuildId) -> Result<BuildProjection, BuildStateError> {
            self.inner.read_build(b).await
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
        async fn forget(&self, b: &BuildId) -> Result<(), BuildStateError> {
            self.inner.forget(b).await
        }
    }

    fn gate(e: &Estate, holds: AttemptReason) -> Arc<Gate> {
        Arc::new(Gate { inner: e.builds.clone(), holds, arrived: Notify::new(), release: Notify::new() })
    }

    /// The fabric-primary's REST door over `builds`, on the estate's view and accepted Build.
    async fn door(e: &Estate, builds: Arc<dyn BuildStateAdapter>) -> Arc<ControlPlane> {
        let me: PathName = e.fabric_primary().await.parse().unwrap();
        let mut cp = ControlPlane::new(builds, e.accepted.clone(), me, e.view.read().await.clone()).with_rafka_time(crate::adopted_time());
        cp.topology = e.view.clone();
        Arc::new(cp)
    }

    /// `mesh1.rpc.2` is lost: its runtime exited and the view holds it unheard, so the drift pass has
    /// a birth to prove. Returns the Build's attempt before anything opens.
    async fn lose_a_node(e: &Estate) -> u32 {
        let before = e.accepted.current(&*e.builds).await.unwrap();
        e.world.kill(&e.node_id("mesh1.rpc.2").await);
        e.unheard(&["mesh1.rpc.2"]).await;
        before.attempt
    }

    async fn opened_facts(e: &Estate, attempt: u32) -> Vec<AttemptOpened> {
        e.builds.facts().await.unwrap().into_iter().filter_map(|f| match f { BuildFact::Opened(o) if o.attempt == attempt => Some(o), _ => None }).collect()
    }

    async fn drift_pass(e: &Estate, builds: &dyn BuildStateAdapter) -> Option<(BuildId, u32)> {
        let me: PathName = e.fabric_primary().await.parse().unwrap();
        let t = e.view.read().await.clone();
        let mut started = HashSet::new();
        rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &t, &e.accepted, &e.book, &e.provider, builds, &rafka_node_admin_core::build_claim::AttemptContexts::in_memory(), &[], &mut started, &mut Default::default(), &|_| None, &|_| false, &|_| false, &|_, _| {}).await
    }

    /// CONTRACT: the proven-drift pass has read the settled Build and computed attempt k+1 for a lost
    /// node when a requested restart of another node opens k+1 first. The restart holds the number:
    /// the drift pass is told it lost, opens nothing and returns no attempt to execute, and the one
    /// Opened fact of k+1 is the restart's. Canon: build_state.rs `AttemptOpened` (insert-and-fail on
    /// `(build_id, attempt)`), CLAUDE.md "an acknowledged write is the write that executes".
    #[tokio::test]
    async fn a_drift_attempt_that_loses_its_number_to_a_requested_restart_opens_nothing() {
        let e = estate().await;
        let k = lose_a_node(&e).await;
        let g = gate(&e, AttemptReason::ProvenDrift);
        let cp = door(&e, e.builds.clone()).await;
        let (drift, rest) = tokio::join!(drift_pass(&e, &*g), async {
            g.arrived.notified().await;
            let r = cp.open_attempt("POST /api/nodes/{name}/restart", AttemptReason::Restart, "mesh2.rpc.1".parse().unwrap(), rafka_node_admin_core::http::ActionKind::Restart, None).await;
            g.release.notify_one();
            r
        });
        let rest = rest.expect("the restart took the free number");
        assert_eq!(rest.attempt, k + 1);
        let held = opened_facts(&e, k + 1).await;
        assert_eq!(held.len(), 1, "one Opened fact for attempt {}: {held:?}", k + 1);
        assert_eq!((held[0].reason, held[0].opened_by.as_str()), (AttemptReason::Restart, cp.me.to_string().as_str()), "the number is the restart's: {held:?}");
        assert_eq!(drift, None, "the drift pass lost the number: it opened nothing and has no attempt to execute");
    }

    /// CONTRACT: the requested restart has read the settled Build and computed attempt k+1 when the
    /// proven-drift pass opens k+1 for a lost node first. The drift attempt holds the number: the
    /// restart is refused 409 `attempt-taken` naming the attempt, never acknowledged 202 for an attempt
    /// that replaces the lost node and restarts nothing, and no second fact is appended.
    #[tokio::test]
    async fn a_requested_restart_that_loses_its_number_to_a_drift_attempt_is_refused_by_name() {
        let e = estate().await;
        let k = lose_a_node(&e).await;
        let g = gate(&e, AttemptReason::Restart);
        let cp = door(&e, g.clone()).await;
        let (rest, drift) = tokio::join!(cp.open_attempt("POST /api/nodes/{name}/restart", AttemptReason::Restart, "mesh2.rpc.1".parse().unwrap(), rafka_node_admin_core::http::ActionKind::Restart, None), async {
            g.arrived.notified().await;
            let d = drift_pass(&e, &*e.builds).await;
            g.release.notify_one();
            d
        });
        assert_eq!(drift, Some((e.accepted.build_id().await.unwrap(), k + 1)), "the drift pass took the free number");
        let held = opened_facts(&e, k + 1).await;
        assert_eq!(held.len(), 1, "one Opened fact for attempt {}: {held:?}", k + 1);
        assert_eq!(held[0].reason, AttemptReason::ProvenDrift, "the number is the drift attempt's: {held:?}");
        let refusal = rest.expect_err("the restart lost the number and was told so");
        let response = refusal.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);
        let body: serde_json::Value = serde_json::from_slice(&http_body_util::BodyExt::collect(response.into_body()).await.unwrap().to_bytes()).unwrap();
        assert_eq!(body["error"], "attempt-taken", "{body}");
        assert!(body["detail"].as_str().unwrap().contains(&format!("{}", k + 1)), "the refusal names the attempt: {body}");
    }
}

/// CONTRACT: a fabric primary that took the seat holds a peer mesh it never heard on the backbone as
/// `ready` from topology alone (topology is not liveness). Once the ladder has decided that mesh's
/// rebirth, its node-admins are proven from their exact runtimes and repaired like any unheard birth.
/// Must NOT happen: the decided mesh waits forever because its view never says it is silent.
#[tokio::test]
async fn a_decided_peer_mesh_held_ready_from_topology_is_proven_and_repaired() {
    let e = estate().await;
    let before = e.accepted.current(&*e.builds).await.unwrap();
    let me: PathName = e.fabric_primary().await.parse().unwrap();
    let lost = if me.mesh == "mesh1" { "mesh2" } else { "mesh1" };
    for i in 1..=2 {
        let id = e.node_id(&format!("{lost}.admin.{i}")).await;
        e.world.kill(&id);
    }
    let t = e.view.read().await.clone();
    assert!(t.cohort(lost, NodeKind::NodeAdmin).all(|a| a.status.is_live()), "the view still holds the dead admins ready");
    let mut started = HashSet::new();
    let contexts = rafka_node_admin_core::build_claim::AttemptContexts::in_memory();
    let undecided = rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &t, &e.accepted, &e.book, &e.provider, &*e.builds, &contexts, &[], &mut started, &mut Default::default(), &|_| None, &|_| false, &|_| false, &|_, _| {}).await;
    assert_eq!(undecided, None, "nothing names the mesh's admins before the ladder decides");
    let seen = std::sync::Mutex::new(Vec::new());
    let decided = rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &t, &e.accepted, &e.book, &e.provider, &*e.builds, &contexts, &[], &mut started, &mut Default::default(), &|_| None, &|_| false, &|m| m == lost, &|n, i| seen.lock().unwrap().push((n.clone(), i.clone()))).await;
    assert_eq!(decided, Some((before.build_id.clone(), before.attempt + 1)), "the decided mesh's exited admins open the next attempt");
    assert_eq!(seen.lock().unwrap().len(), 2, "both proven exits are handed to the view, so neither dead admin holds the mesh's seat");
}

/// CONTRACT: a departure is not drift. A birth whose proven departure the authority holds
/// (`NodeDeleted`) may still have its durable runtime row; that row names a runtime that left on
/// purpose, and no attempt repairs it. Must NOT happen: the deleted node is re-created as drift.
#[tokio::test]
async fn a_durable_runtime_row_of_a_departed_birth_opens_no_attempt() {
    let e = estate().await;
    let lost = "mesh1.admin.2";
    let old = e.node_id(lost).await;
    let (row, digest) = {
        let (d, _) = e.book.get(&old).unwrap();
        (rafka_node_admin_core::storage::RuntimeRow { node_id: d.node.node_id.clone(), name: d.node.name.clone(), incarnation_id: d.node.incarnation.clone(), runtime: d.node.runtime.clone().unwrap(), data_dir: None }, d)
    };
    e.world.kill(&old);
    let heard = DigestBook::default();
    for d in e.book.all().into_iter().filter(|d| d.node.name.to_string() != lost) {
        heard.record(d);
    }
    heard.depart(rafka_mesh_entity::LifecycleOp { build_id: "b".into(), attempt: 1, operation: format!("retire-node:{lost}"), node_id: digest.node.node_id.clone(), incarnation: digest.node.incarnation.clone(), name: digest.node.name.clone(), event_at_rafka_ms: 1 });
    let mut t = e.view.read().await.clone();
    t.nodes.retain(|n| n.name.to_string() != lost);
    rafka_node_admin_core::election::resolve(&mut t.nodes);
    let me: PathName = t.fabric_primary().unwrap().name.clone();
    let mut started = HashSet::new();
    let contexts = rafka_node_admin_core::build_claim::AttemptContexts::in_memory();
    let opened = rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &t, &e.accepted, &heard, &e.provider, &*e.builds, &contexts, std::slice::from_ref(&row), &mut started, &mut Default::default(), &|_| None, &|_| false, &|_| false, &|_, _| {}).await;
    assert_eq!(opened, None, "a proven departure is held: its runtime row is not drift");
}

/// R-D1: a peer mesh the fabric primary holds unheard is held as a mesh. The provider's proof that one of
/// its births exited is kept as evidence and authorizes no Restart, Replace or NodeDeleted; a heard mesh is
/// repaired as ever; the hold ends with the silence and leaves nothing behind.
mod unheard_mesh_hold {
    use super::*;
    use rafka_node_admin_core::investigate::PeerMesh;

    type Held = HashSet<(NodeId, IncarnationId)>;

    /// The fabric primary's mesh, and the other one.
    async fn meshes(e: &Estate) -> (String, String) {
        let fp = e.fabric_primary().await;
        let own = fp.split('.').next().unwrap().to_string();
        let peer = SHAPE.iter().map(|(m, _, _)| (*m).to_string()).find(|m| *m != own).expect("a peer mesh");
        (own, peer)
    }

    async fn pass(e: &Estate, started: &mut HashSet<(BuildId, u32, Vec<String>)>, held: &mut Held, peer: &(dyn Fn(&str) -> Option<PeerMesh> + Sync)) -> Option<(BuildId, u32)> {
        let me: PathName = e.fabric_primary().await.parse().expect("a fabric-primary");
        let t = e.view.read().await.clone();
        rafka_node_admin_core::admin::reconcile_drift(&crate::adopted_time(), &me, &t, &e.accepted, &e.book, &e.provider, &*e.builds, &rafka_node_admin_core::build_claim::AttemptContexts::in_memory(), &[], started, held, &|m| peer(m).and_then(|p| p.unheard_ms), &|m| peer(m).is_some_and(|p| !p.rebirth_decided), &|m| peer(m).is_some_and(|p| p.rebirth_decided), &|_, _| {}).await
    }

    const UNHEARD: Option<PeerMesh> = Some(PeerMesh { unheard_ms: Some(4_000), rebirth_decided: false });
    const HEARD: Option<PeerMesh> = Some(PeerMesh { unheard_ms: None, rebirth_decided: false });

    #[tokio::test]
    async fn an_exited_birth_of_an_unheard_peer_mesh_opens_no_attempt_and_is_kept_as_evidence_once() {
        let e = estate().await;
        let (_, peer_mesh) = meshes(&e).await;
        let path = format!("{peer_mesh}.rpc.1");
        let id = e.node_id(&path).await;
        e.world.exit_with(&id, Some(137));
        e.unheard(&[path.as_str()]).await;
        let before = e.accepted.current(&*e.builds).await.unwrap();
        let mut held = Held::new();
        let mut started = HashSet::new();
        let unheard = |_: &str| UNHEARD;
        assert_eq!(pass(&e, &mut started, &mut held, &unheard).await, None, "the mesh is unheard: its exited birth authorizes nothing");
        assert_eq!(held.len(), 1, "the evidence of the one exited birth is held: {held:?}");
        assert_eq!(pass(&e, &mut started, &mut held, &unheard).await, None);
        assert_eq!(held.len(), 1, "the same birth is one hold, not one per pass");
        let after = e.accepted.current(&*e.builds).await.unwrap();
        assert_eq!(after.attempt, before.attempt, "no attempt was opened");
        assert!(e.born().is_empty(), "no node was created: {:?}", e.born());
    }

    #[tokio::test]
    async fn the_hold_ends_when_the_mesh_is_heard_again_and_the_exited_birth_is_repaired() {
        let e = estate().await;
        let (_, peer_mesh) = meshes(&e).await;
        let path = format!("{peer_mesh}.rpc.1");
        let id = e.node_id(&path).await;
        e.world.exit_with(&id, Some(137));
        e.unheard(&[path.as_str()]).await;
        let before = e.accepted.current(&*e.builds).await.unwrap();
        let mut held = Held::new();
        let mut started = HashSet::new();
        assert_eq!(pass(&e, &mut started, &mut held, &|_| UNHEARD).await, None);
        assert_eq!(held.len(), 1);
        let opened = pass(&e, &mut started, &mut held, &|_| HEARD).await;
        assert_eq!(opened, Some((before.build_id.clone(), before.attempt + 1)), "the mesh is heard again: the exited birth is repaired as ever");
        assert!(held.is_empty(), "the hold left nothing behind: {held:?}");
        let Some(rafka_node_admin_core::accepted::AttemptAction::Replace { path: p, .. }) = e.accepted.current(&*e.builds).await.unwrap().action else { panic!("not a Replace") };
        assert_eq!(p.to_string(), path);
    }

    #[tokio::test]
    async fn a_birth_of_the_fabric_primarys_own_mesh_is_never_held() {
        let e = estate().await;
        let (own, _) = meshes(&e).await;
        let path = format!("{own}.rpc.1");
        let id = e.node_id(&path).await;
        e.world.exit_with(&id, Some(1));
        e.unheard(&[path.as_str()]).await;
        let before = e.accepted.current(&*e.builds).await.unwrap();
        let mut held = Held::new();
        let mut started = HashSet::new();
        let opened = pass(&e, &mut started, &mut held, &|_| UNHEARD).await;
        assert_eq!(opened, Some((before.build_id.clone(), before.attempt + 1)), "the own mesh is heard by definition");
        assert!(held.is_empty());
    }

    #[tokio::test]
    async fn only_the_first_recovering_node_admin_of_a_reborn_mesh_is_replaced_from_here() {
        let e = estate().await;
        let (_, peer_mesh) = meshes(&e).await;
        let admins = [format!("{peer_mesh}.admin.1"), format!("{peer_mesh}.admin.2")];
        let rpc = format!("{peer_mesh}.rpc.1");
        for p in admins.iter().chain([&rpc]) {
            let id = e.node_id(p).await;
            e.world.exit_with(&id, Some(1));
        }
        let all: Vec<&str> = admins.iter().map(String::as_str).chain([rpc.as_str()]).collect();
        e.unheard(&all).await;
        let before = e.accepted.current(&*e.builds).await.unwrap();
        let mut held = Held::new();
        let mut started = HashSet::new();
        assert_eq!(pass(&e, &mut started, &mut held, &|_| UNHEARD).await, None, "before the ladder's decision the mesh's node-admins wait for it");
        let rpc_id = e.node_id(&rpc).await;
        assert_eq!(held.iter().map(|(n, _)| n.to_string()).collect::<Vec<_>>(), vec![rpc_id], "only the ordinary node's exit is held as evidence; the admins wait for the decision");
        let decided = |_: &str| Some(PeerMesh { unheard_ms: Some(30_000), rebirth_decided: true });
        let opened = pass(&e, &mut started, &mut held, &decided).await;
        assert_eq!(opened, Some((before.build_id.clone(), before.attempt + 1)), "the decision releases the first recovering node-admin");
        let Some(rafka_node_admin_core::accepted::AttemptAction::Replace { path, .. }) = e.accepted.current(&*e.builds).await.unwrap().action else { panic!("not a Replace") };
        assert_eq!(path.to_string(), admins[0], "the first admin in path order");
        let first = e.node_id(&admins[0]).await;
        assert!(!held.iter().any(|(n, _)| n.as_str() == first), "the admin the decision released is not held");
        // The decision released one node-admin: that release is spent until the mesh is heard again, so
        // the second node-admin's exit is evidence, not an attempt.
        assert!(started.iter().any(|k| k.2 == vec!["recovery".to_string(), peer_mesh.clone()]), "the release is recorded: {started:?}");
    }
}

/// CONTRACT: a live birth the accepted topology does not name is surplus, retired by an attempt of its
/// own, UNLESS the Build already receipted its retirement (`NodeDeleting` of that exact birth): that
/// operation retires it, and this view is only late in hearing it go.
#[tokio::test]
async fn a_birth_whose_retirement_the_build_receipted_is_not_surplus() {
    use rafka_node_admin_core::build_state::{BuildStateAdapter, BuildStepReceipt, StepOutcome};
    for receipted in [false, true] {
        let e = estate().await;
        let current = e.accepted.current(&*e.builds).await.unwrap();
        let extra = birth(&e.world, &e.book, &e.view.read().await.fabric.id.clone(), "mesh1.rpc.9", None);
        e.view.write().await.nodes.push(extra.clone());
        if receipted {
            let op = rafka_mesh_entity::LifecycleOp {
                build_id: current.build_id.to_string(),
                attempt: current.attempt,
                operation: "retire-node:mesh1.rpc.9".into(),
                node_id: extra.node_id.clone(),
                incarnation: extra.incarnation_id.clone().unwrap(),
                name: extra.name.clone(),
                event_at_rafka_ms: 0,
            };
            e.builds
                .append_step_receipt(&BuildStepReceipt { build_id: current.build_id.clone(), attempt: current.attempt, operation: op.operation.clone(), step: "NodeDeleting".into(), outcome: StepOutcome::Complete, output: serde_json::to_value(&op).ok(), executor: None })
                .await
                .unwrap();
            assert_eq!(e.drift().await, None, "a receipted retirement is not surplus");
        } else {
            assert!(e.drift().await.is_some(), "an unreceipted live birth outside the accepted topology is surplus");
        }
    }
}
