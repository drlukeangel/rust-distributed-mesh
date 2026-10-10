//! The typed `node` objects, end to end: a workflow call returns a reply stream read from the
//! fabric-primary's Build family (op `0x20`), and the receipts are written by the real create
//! pipeline running in the same process.
//!
//! The two acceptance contracts of ops-naming ("A failure names its step", "A broken stream is
//! not a failure") are the first two cells. A workflow's `node.start` steps run today inside
//! `node.create`, so the start whose join fails is a create whose join fails.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode, NodeId, RuntimeFact};
use rafka_node_admin_client::{BuildCarrier, Builds, CallEnd, Frame, NodeAdminClient, NodeEvent, NodeOp, NodeSpec, NodeStep, Nodes, Resume, WorkflowKind, WorkflowStream};
use rafka_node_admin_core::build::{BuildId, BuildOperation};
use rafka_node_admin_core::build_claim::{AttemptContexts, ClaimDoor};
use rafka_node_admin_core::build_drive::{Dispatched, Dispatcher, DriveEnv, Drives, OpenGate};
use rafka_node_admin_core::build_op::{local_dispatched, BuildDoor, BuildSlot};
use rafka_node_admin_core::build_run::{AttemptRuns, FramedBuilds, RunDoor};
use rafka_node_admin_core::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
use rafka_node_admin_core::executor::{BuildExecutor, OperationRunner};
use rafka_node_rpc::{NodeRpcClient, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{CreateRequest, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, Timeouts, TopologySink};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::http::ControlPlane;
use rafka_node_admin_core::join::Joins;
use rafka_node_admin_core::model::{Fabric, Mesh, Node, NodeKind, NodeStatus, PathName, ProviderKind, ScopeStatus};
use rafka_node_admin_core::topology::Topology;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    provider: opentelemetry_sdk::trace::TracerProvider,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt;
    crate::enable_callsites();
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::TracerProvider::builder().with_simple_exporter(exporter.clone()).build();
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("node-objects"));
    Capture { exporter, provider, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
}

impl Capture {
    fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        let d = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(f)
    }

    fn spans(&self) -> Vec<Value> {
        let _ = self.provider.force_flush();
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({"name": s.name, "attributes": attributes})
            })
            .collect()
    }
}

fn spans_named<'a>(spans: &'a [Value], name: &str) -> Vec<&'a Value> {
    spans.iter().filter(|s| s["name"] == name).collect()
}

/// A provider whose runtime stays up: the node's own report is fed by the test.
struct Up;
#[async_trait::async_trait]
impl DeploymentProvider for Up {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        "node-objects".into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        Ok(DeploymentHandle { deployment_id: spec.deployment_id.clone(), provider: ProviderKind::Process, pid: Some(1), start: Some(1), container: None, domain: Some("node-objects".into()) })
    }
    async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        Ok(())
    }
    async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
        DeploymentStatus::Running
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
        None
    }
}

/// The membership the admin holds of the birth: it hears the birth only once `joined` is set.
struct Membership {
    published: Mutex<Option<Publication>>,
    joined: AtomicBool,
    /// While set, the node is not ready and this is what its Ready check names.
    not_ready: Mutex<Option<String>>,
}

#[async_trait::async_trait]
impl NodeObserver for Membership {
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        self.joined.load(Ordering::SeqCst).then(|| self.published.lock().unwrap().clone()).flatten()
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        match self.not_ready.lock().unwrap().clone() {
            Some(why) => Err(why),
            None => Ok(()),
        }
    }
}

#[derive(Default)]
struct Published(Mutex<Vec<Node>>);
impl TopologySink for Published {
    fn publish(&self, n: Node) {
        self.0.lock().unwrap().push(n);
    }
    fn remove(&self, _: &rafka_node_admin_core::model::PathName) {}
}

fn node(name: &str, primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.is_primary = primary;
    n.incarnation_id = Some(IncarnationId::mint());
    if n.kind == NodeKind::NodeAdmin {
        n.admin_api_base = Some(format!("http://127.0.0.1:1800{}", n.name.ordinal));
        n.is_fabric_primary = primary;
    }
    n
}

fn mn() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(rafka_mesh_entity::MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes: vec![node("mesh1.admin.1", true), node("mesh1.rpc.1", true), node("mesh1.rpc.2", false)],
    }
}

/// The fabric-primary's Build door over `rig`'s Build state: the control plane that accepts a change,
/// the drive that claims on its own log, and the executor door that runs the create pipeline.
async fn door(rig: &Arc<Rig>, join: Duration) -> Arc<BuildDoor> {
    let t = mn();
    let accepted = rafka_node_admin_core::accepted::AcceptedStore::seeded(&*rig.builds, t.fabric.id.clone(), rafka_node_admin_core::accepted::FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let control = Arc::new(ControlPlane::new(rig.builds.clone(), accepted.clone(), me.clone(), t, crate::adopted_time()));
    let runner: Arc<dyn OperationRunner> = Arc::new(RigRunner { rig: rig.clone(), join });
    let exec = Arc::new(BuildExecutor { executor: me.to_string(), builds: rig.builds.clone(), topology: control.topology.clone(), runner });
    let run = Arc::new(RunDoor { me: me.clone(), builds: rig.builds.clone(), exec, runs: rig.runs.clone(), local: rig.local.clone() });
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: control.topology.clone(),
        accepted,
        builds: rig.builds.clone(),
        door: Arc::new(ClaimDoor { me: me.clone(), topology: control.topology.clone(), builds: rig.builds.clone(), contexts: Arc::new(AttemptContexts::in_memory()) }),
        dispatcher: Arc::new(Local(run.clone())),
        gate: Arc::new(OpenGate),
        departure: Arc::new(crate::loopback::Proofs::default()),
        verdicts: Arc::new(crate::loopback::NoVerdicts),
    });
    Arc::new(BuildDoor { me, control, drives: Arc::new(Drives::default()), env, run })
}

/// The fabric-primary's Build door over a Build log of its own: the executor writes its step
/// receipts on `rig`'s log, the fabric-primary's log holds the claim it decided and the verdict the
/// executor reported, and no step receipt reaches it (the Build topic delivers those apart, later).
async fn door_with_own_log(rig: &Arc<Rig>, fp: Arc<MemoryBuildStateAdapter>, join: Duration) -> Arc<BuildDoor> {
    let t = mn();
    let accepted = rafka_node_admin_core::accepted::AcceptedStore::seeded(&*fp, t.fabric.id.clone(), rafka_node_admin_core::accepted::FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let control = Arc::new(ControlPlane::new(fp.clone(), accepted.clone(), me.clone(), t, crate::adopted_time()));
    let runner: Arc<dyn OperationRunner> = Arc::new(RigRunner { rig: rig.clone(), join });
    let exec = Arc::new(BuildExecutor { executor: me.to_string(), builds: rig.builds.clone(), topology: control.topology.clone(), runner });
    let run = Arc::new(RunDoor { me: me.clone(), builds: rig.builds.clone(), exec, runs: rig.runs.clone(), local: rig.local.clone() });
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: control.topology.clone(),
        accepted,
        builds: fp.clone(),
        door: Arc::new(ClaimDoor { me: me.clone(), topology: control.topology.clone(), builds: fp.clone(), contexts: Arc::new(AttemptContexts::in_memory()) }),
        dispatcher: Arc::new(Local(run.clone())),
        gate: Arc::new(OpenGate),
        departure: Arc::new(crate::loopback::Proofs::default()),
        verdicts: Arc::new(rafka_node_admin_core::build_op::LocalVerdicts { local: fp }),
    });
    Arc::new(BuildDoor { me, control, drives: Arc::new(Drives::default()), env, run })
}

/// The one executor of the fixture is the fabric-primary itself.
struct Local(Arc<RunDoor>);

#[async_trait::async_trait]
impl Dispatcher for Local {
    async fn dispatch(&self, executor: &PathName, build_id: &BuildId, attempt: u32, context: rafka_node_rpc_contract::context::CallContext, intent: Vec<Vec<u8>>, plan: rafka_node_admin_core::executor::RunPlan) -> Dispatched {
        local_dispatched(self.0.attempt_run(build_id, attempt, &executor.to_string(), &context, &intent, &plan).await)
    }
}

/// Runs the create operation through the pipeline over the fixture's fakes, as the executing admin does.
struct RigRunner {
    rig: Arc<Rig>,
    join: Duration,
}

#[async_trait::async_trait]
impl OperationRunner for RigRunner {
    async fn run(&self, build_id: &BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String> {
        match op {
            BuildOperation::CreateNode { node } => self.rig.pipeline_create(build_id, attempt, &node.to_string(), self.join).await,
            other => Err(format!("the fixture creates nodes only, not {other:?}")),
        }
    }
}

/// A node-RPC server on loopback that serves the Build family from `door`, and a carrier to it.
struct Served {
    router: Router,
    carrier: BuildCarrier,
}

async fn serve(door: Arc<BuildDoor>) -> Served {
    let (node_id, incarnation, key) = (NodeId::mint(), IncarnationId::mint(), SecretKey::generate());
    let slot: BuildSlot = Arc::new(OnceLock::new());
    let _ = slot.set(door);
    let server = rafka_node_admin_core::build_op::serve(ServerBuilder::new(), slot)
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
        .unwrap();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(ResolvedNode { node_id, name: "mesh1.admin.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation });
    let caller = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = Arc::new(NodeRpcClient::new(caller, resolver).with_caller_system("rdm"));
    Served { router, carrier: BuildCarrier::new(client, "mesh1.admin.1".parse().unwrap()) }
}

/// The reads of the node objects go to one admin's control API; no cell of this file reads one.
fn nodes_over(carrier: &BuildCarrier) -> Nodes {
    Nodes::new(NodeAdminClient::new("http://127.0.0.1:1"), carrier.clone())
}

struct Rig {
    builds: Arc<dyn BuildStateAdapter>,
    local: Arc<MemoryBuildStateAdapter>,
    runs: Arc<AttemptRuns>,
    joins: Arc<Joins>,
    membership: Arc<Membership>,
    sink: Arc<Published>,
    template: LaunchTemplate,
    data_root: std::path::PathBuf,
}

impl Rig {
    fn new() -> Rig {
        let data_root = std::env::temp_dir().join(format!("node-objects-{}", NodeId::mint()));
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/node-objects".into(),
            seeds: vec![],
            launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: Default::default(),
            data_root: data_root.clone(),
            mesh_issuer: None,
        };
        let runs = Arc::new(AttemptRuns::default());
        let local = Arc::new(MemoryBuildStateAdapter::new());
        Rig { builds: Arc::new(FramedBuilds::new(local.clone(), runs.clone())), local, runs, joins: Arc::new(Joins::default()), membership: Arc::new(Membership { published: Mutex::new(None), joined: AtomicBool::new(false), not_ready: Mutex::new(None) }), sink: Arc::default(), template, data_root }
    }

    /// Run the create pipeline for `name` in `attempt` of `build`, as the executing admin does once
    /// the fabric-primary has claimed the attempt for it.
    async fn pipeline_create(&self, build: &BuildId, attempt: u32, name: &str, join: Duration) -> Result<(), String> {
        let pipeline = DeploymentPipeline {
            provider: &Up,
            joins: &self.joins,
            observer: &*self.membership,
            sink: &*self.sink,
            lifecycle: &NoLifecycleEvents,
            builds: &*self.builds,
            template: &self.template,
            timeouts: Timeouts { join, ..Timeouts::default() },
        };
        let req = CreateRequest { build_id: build.clone(), attempt, node: name.parse().unwrap(), spec: &RPC_NODE, restart_of: None, held_runtimes: Vec::new() };
        pipeline.create(&req).await.map(|_| ()).map_err(|e| e.to_string())
    }

    /// The birth reports where it bound once the create has made its runtime fact available: the
    /// node's own `JoinNode`, as the pipeline's `WaitForBind` waits for it.
    async fn report_bind(&self, build: &BuildId, name: &str) {
        let op = format!("create-node:{name}");
        let name: rafka_node_admin_core::model::PathName = name.parse().unwrap();
        loop {
            // The executor's own log holds the Build once the call that carries it has arrived.
            let Ok(view) = self.builds.read_build(build).await else {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            };
            let done = |step: &str| view.steps.iter().find(|s| s.operation == op && s.step == step && s.outcome == rafka_node_admin_core::build_state::StepOutcome::Complete);
            if let (Some(identity), Some(storage), Some(_)) = (done("AllocateIdentity"), done("PrepareStorage"), done("MakeRuntimeFactAvailableToBirth")) {
                let id = identity.output.clone().unwrap();
                let node_id = NodeId::parse(id["node_id"].as_str().unwrap()).unwrap();
                let incarnation = IncarnationId(id["incarnation"].as_str().unwrap().to_string());
                let endpoint_id = EndpointId(storage.output.clone().unwrap().as_str().unwrap().to_string());
                let data_dir = self.data_root.join(format!("{name}-{node_id}"));
                let runtime: RuntimeFact = RuntimeFact::read_record(&data_dir).expect("the create wrote the runtime record").unwrap();
                let digest = MeshDigest {
                    fabric_id: FabricId::mint(),
                    node: MeshNode { node_id, name: name.clone(), endpoint_id, transport_addr: "127.0.0.1:34567".parse().unwrap(), incarnation, supersedes: None, runtime: Some(runtime.clone()) },
                    status: MemberStatus::Pending,
                    admin_api_base: None,
                    digest_seq: 0,
                    emitted_at_rafka_ms: 0,
                    data_dir: Some(data_dir.display().to_string()),
                    mesh_id: None,
                    in_flight: None,
                    extra: Default::default(),
                    load: None,
                    gossip: None,
                };
                *self.membership.published.lock().unwrap() = Some(Publication { runtime: Some(runtime), data_dir: Some(data_dir.display().to_string()) });
                self.joins.report(&digest);
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.data_root);
    }
}

/// Read a stream to its end. A stream that yields nothing for `within` is a defect of the stream,
/// reported with the frames it did deliver.
async fn drain(stream: &mut WorkflowStream, within: Duration) -> (Vec<Frame>, Option<CallEnd>) {
    let mut frames = Vec::new();
    loop {
        match tokio::time::timeout(within, stream.next()).await {
            Err(_) => panic!("the stream went quiet; frames so far: {frames:?}"),
            Ok(None) => return (frames, None),
            Ok(Some(Ok(f))) => frames.push(f),
            Ok(Some(Err(end))) => return (frames, Some(end)),
        }
    }
}

const ATTEMPT_ONE_STEPS: [&str; 10] = [
    "AllocateIdentity",
    "PrepareStorage",
    "PrepareNetwork",
    "DeployRuntime",
    "RegisterExactRuntimeHandle",
    "ResolveProviderControlDomain",
    "MakeRuntimeFactAvailableToBirth",
    "WaitForBind",
    "PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata",
    "ApplyMeshPending",
];

/// CONTRACT (ops-naming acceptance 1, "A failure names its step"): a create whose process starts
/// and whose join then fails ends its stream `Started`, `node.created`, `node.started`, then
/// `Failed { step: node.join, reason }`. `node.joined` and `node.ready` are never emitted, the
/// node never reaches ready-for-traffic, and the call's span names the failed step.
#[test]
fn a_create_whose_join_fails_names_the_join_step_and_emits_nothing_after_it() {
    let cap = capture();
    let (frames, end, published) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let served = serve(door(&rig, Duration::from_millis(300)).await).await;
        let nodes = nodes_over(&served.carrier);
        let mut stream = nodes.create(&NodeSpec { mesh: "mesh1".into(), kind: NodeKind::RpcNode }).await.unwrap();
        let build = BuildId(stream.accepted().build_id.0.clone());
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, "mesh1.rpc.3").await }
        });
        let (frames, end) = drain(&mut stream, Duration::from_secs(5)).await;
        let published = rig.sink.0.lock().unwrap().clone();
        (frames, end, published)
    });
    let attempt = 1;
    let reason = match frames.last() {
        Some(Frame::Failed { step: NodeStep::Join, reason }) => reason.clone(),
        other => panic!("the stream ends Failed at node.join, not {other:?}; frames {frames:?}"),
    };
    assert!(reason.contains("no membership digest"), "the failure carries the step's own reason: {reason}");
    assert_eq!(
        frames[..frames.len() - 1],
        [Frame::Started { build_id: match &frames[0] { Frame::Started { build_id, .. } => build_id.clone(), f => panic!("{f:?}") }, attempt }, Frame::Event(NodeEvent::Created), Frame::Event(NodeEvent::Started)]
    );
    assert!(end.is_none(), "a failed step ends the stream with its terminal frame, not a broken stream");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Event(NodeEvent::Joined | NodeEvent::Ready) | Frame::Complete)), "nothing downstream of the failed step is emitted: {frames:?}");
    assert!(published.iter().all(|n| n.status != NodeStatus::ReadyForTraffic), "the node never reached ready-for-traffic");

    let spans = cap.spans();
    let call = spans_named(&spans, "rdm.node_admin.node.create.via-workflow");
    assert_eq!(call.len(), 1, "{spans:?}");
    assert_eq!(call[0]["attributes"]["outcome"], "failed");
    assert_eq!(call[0]["attributes"]["failed_step"], "node.join");
    for emitted in ["created", "started"] {
        assert_eq!(spans_named(&spans, &format!("rdm.node_admin.node.{emitted}.via-reply-frame")).len(), 1, "node.{emitted} was delivered once");
    }
    for never in ["joined", "ready"] {
        assert!(spans_named(&spans, &format!("rdm.node_admin.node.{never}.via-reply-frame")).is_empty(), "node.{never} is never emitted");
    }
    let join_step: Vec<&Value> = spans_named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|s| s["attributes"]["step"] == "WaitForMeshJoin").collect();
    assert_eq!(join_step.len(), 1, "the pipeline ran the join step once");
    assert_eq!(join_step[0]["attributes"]["outcome"], "failed", "and it failed there");
}

/// CONTRACT: a create whose node has joined but is not ready (its Ready check names a blocker, as an
/// application's `hydrate_before_ready` does) streams one `Blocked { operation, step, reason }` frame
/// naming the step in flight and the check's own reason, however many times the check is polled with
/// that reason; the blocker is live progress, so the stream goes on, and when the node is ready it
/// emits `node.ready` and `Complete`.
#[test]
fn a_create_whose_node_is_not_ready_streams_the_blocker_the_ready_check_names() {
    let cap = capture();
    let (blocked, after) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let why = "mesh1.rpc.3: hydrate_before_ready is blocked (attempt 1): pulling the accepting authority; it runs again on authority-ready";
        *rig.membership.not_ready.lock().unwrap() = Some(why.to_string());
        rig.membership.joined.store(true, Ordering::SeqCst);
        let served = serve(door(&rig, Duration::from_secs(30)).await).await;
        let nodes = nodes_over(&served.carrier);
        let mut stream = nodes.create(&NodeSpec { mesh: "mesh1".into(), kind: NodeKind::RpcNode }).await.unwrap();
        let build = BuildId(stream.accepted().build_id.0.clone());
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, "mesh1.rpc.3").await }
        });
        let mut blocked = None;
        while blocked.is_none() {
            match tokio::time::timeout(Duration::from_secs(5), stream.next()).await.expect("the stream went quiet before any Blocked frame") {
                Some(Ok(f @ Frame::Blocked { .. })) => blocked = Some(f),
                Some(Ok(_)) => {}
                other => panic!("the stream ended before the node's blocker was framed: {other:?}"),
            }
        }
        // The Ready check polls again with the same reason: no second frame. Then the node is ready.
        tokio::time::sleep(Duration::from_millis(350)).await;
        *rig.membership.not_ready.lock().unwrap() = None;
        let (after, end) = drain(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "the stream ends with its terminal frame: {end:?}");
        (blocked.unwrap(), after)
    });
    assert_eq!(
        blocked,
        Frame::Blocked {
            operation: "create-node:mesh1.rpc.3".into(),
            step: "WaitForNodeReady".into(),
            reason: "mesh1.rpc.3: hydrate_before_ready is blocked (attempt 1): pulling the accepting authority; it runs again on authority-ready".into()
        }
    );
    assert_eq!(after, [Frame::Event(NodeEvent::Ready), Frame::Complete], "a repeated blocker makes no second frame, and the ready node ends the stream: {after:?}");
}

/// CONTRACT (ops-naming acceptance 2, "A broken stream is not a failure"): the transport between
/// the caller and the fabric-primary is cut while the create is at its join. The caller's stream
/// ends `Indeterminate`, never `Failed`. The cut cancels nothing: `build.get` over a new transport
/// returns the receipts of exactly the steps that completed, `Resume` names the join as the first
/// step without a `Complete` receipt, and the re-submit by the Build id streams the same Build
/// (never a second one) to its end once the birth is heard: no completed step runs again, each
/// still has one receipt, and the steps' spans say the attempt ran them once.
#[test]
fn a_cut_stream_is_indeterminate_and_the_resume_reruns_no_completed_step() {
    let cap = capture();
    let (frames, end, receipts_after_cut, resume, receipts_after, final_frames, disposition) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let door = door(&rig, Duration::from_secs(60)).await;
        let served = serve(door.clone()).await;
        let nodes = nodes_over(&served.carrier);
        let mut stream = nodes.create(&NodeSpec { mesh: "mesh1".into(), kind: NodeKind::RpcNode }).await.unwrap();
        let build = BuildId(stream.accepted().build_id.0.clone());
        let accepted = stream.accepted().clone();
        // The birth reports its bind; the join is not heard yet.
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, "mesh1.rpc.3").await }
        });
        let mut frames = Vec::new();
        while !frames.contains(&Frame::Event(NodeEvent::Started)) {
            match tokio::time::timeout(Duration::from_secs(5), stream.next()).await.expect("the stream went quiet before node.started") {
                Some(Ok(f)) => frames.push(f),
                other => panic!("the stream ended before the cut: {other:?}"),
            }
        }
        // The cut: the transport under the caller's stream goes down once the step before the join
        // has its receipt.
        loop {
            let view = rig.builds.read_build(&build).await.unwrap();
            if view.steps.iter().any(|s| s.step == "ApplyMeshPending") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        served.router.shutdown().await.unwrap();
        let end = loop {
            match tokio::time::timeout(Duration::from_secs(5), stream.next()).await.expect("the cut stream never ended") {
                Some(Ok(f)) => frames.push(f),
                Some(Err(end)) => break end,
                None => panic!("the stream ended without saying how: {frames:?}"),
            }
        };
        // Re-attach over a new transport to the same fabric-primary.
        let again = serve(door).await;
        let builds = Builds::new(again.carrier.clone());
        let receipts_after_cut = builds.get(&accepted.build_id).await.unwrap();
        let resume = Resume::from_receipts(&WorkflowKind::Create, &receipts_after_cut, accepted.attempt).unwrap();
        // The birth is heard this time: the Build the cut did not cancel goes on to its end, and the
        // re-submit streams it.
        let mut resumed = nodes_over(&again.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let disposition = resumed.disposition();
        rig.membership.joined.store(true, Ordering::SeqCst);
        let (final_frames, resumed_end) = drain(&mut resumed, Duration::from_secs(10)).await;
        assert!(resumed_end.is_none(), "the resumed stream ends with its terminal frame: {resumed_end:?}");
        let receipts_after = builds.get(&accepted.build_id).await.unwrap();
        (frames, end, receipts_after_cut, resume, receipts_after, final_frames, disposition)
    });
    assert!(matches!(end, CallEnd::Indeterminate { .. }), "a cut stream is Indeterminate, not {end:?}");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Failed { .. } | Frame::Complete)), "no step is inferred failed or complete from a broken stream: {frames:?}");
    assert!(frames.contains(&Frame::Event(NodeEvent::Started)) && !frames.contains(&Frame::Event(NodeEvent::Joined)));

    let completed_steps = |r: &rafka_node_admin_client::BuildReceipts, attempt: u32| -> Vec<String> {
        r.steps.iter().filter(|s| s.attempt == attempt && s.result == rafka_node_rpc_contract::build::StepResult::Complete).map(|s| s.step.clone()).collect()
    };
    assert_eq!(receipts_after_cut.steps.len(), ATTEMPT_ONE_STEPS.len(), "build.get returns exactly the receipts of the steps that completed: {:?}", receipts_after_cut.steps);
    assert_eq!(completed_steps(&receipts_after_cut, 1), ATTEMPT_ONE_STEPS);
    assert_eq!(resume.from, Some(("create-node".to_string(), "WaitForMeshJoin".to_string())), "the resume starts at the first step without a Complete receipt");
    assert_eq!(resume.completed.len(), ATTEMPT_ONE_STEPS.len());
    assert_eq!(disposition, rafka_node_rpc_contract::build::Disposition::Attached, "the re-submit attached to the Build the cut left running");

    for step in ATTEMPT_ONE_STEPS {
        assert_eq!(receipts_after.steps.iter().filter(|s| s.step == step).count(), 1, "{step} was not run again: one receipt, from attempt 1");
    }
    assert_eq!(receipts_after.attempt, 1, "the cut opened no second attempt");
    assert_eq!(completed_steps(&receipts_after, 1).len(), ATTEMPT_ONE_STEPS.len() + 3, "the same attempt went on through the join to its end");
    assert!(matches!(&final_frames[0], Frame::Started { .. }));
    assert_eq!(final_frames[1..], [Frame::Event(NodeEvent::Created), Frame::Event(NodeEvent::Started), Frame::Event(NodeEvent::Joined), Frame::Event(NodeEvent::Ready), Frame::Complete]);

    let spans = cap.spans();
    let calls = spans_named(&spans, "rdm.node_admin.node.create.via-workflow");
    assert_eq!(calls.len(), 2, "the create and its re-submit: {spans:?}");
    assert_eq!(calls.iter().filter(|c| c["attributes"]["outcome"] == "indeterminate").count(), 1, "the cut call ended indeterminate");
    assert_eq!(calls.iter().filter(|c| c["attributes"]["outcome"] == "complete").count(), 1, "the re-submit ended complete");
    let steps = spans_named(&spans, "rdm.node_admin.deployment.update.via-step");
    for step in ATTEMPT_ONE_STEPS {
        let ran: Vec<_> = steps.iter().filter(|s| s["attributes"]["step"] == step).collect();
        assert_eq!(ran.len(), 1, "{step} ran once: {ran:?}");
        assert_eq!(ran[0]["attributes"]["outcome"], "complete");
    }
}

/// CONTRACT (R-ON8): a re-submit of a Build that completed is answered from what the
/// fabric-primary's own drive recorded off the calls it made, never from the Build facts its log
/// holds. The fabric-primary's log holds the verdict the executor reported and not one step receipt
/// (the Build topic delivers those separately, and may deliver the verdict first), yet the
/// re-submit is `AlreadyApplied` with every event the create named and the terminal frame.
#[test]
fn a_resubmit_after_the_end_replays_the_frames_the_drive_recorded_not_the_facts_the_log_holds() {
    let cap = capture();
    let (first, again, disposition) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let fp = Arc::new(MemoryBuildStateAdapter::new());
        let served = serve(door_with_own_log(&rig, fp.clone(), Duration::from_secs(30)).await).await;
        let nodes = nodes_over(&served.carrier);
        rig.membership.joined.store(true, Ordering::SeqCst);
        let mut stream = nodes.create(&NodeSpec { mesh: "mesh1".into(), kind: NodeKind::RpcNode }).await.unwrap();
        let build = BuildId(stream.accepted().build_id.0.clone());
        let accepted = stream.accepted().clone();
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, "mesh1.rpc.3").await }
        });
        let (first, end) = drain(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        assert!(fp.facts().await.unwrap().iter().all(|f| !matches!(f, rafka_node_admin_core::build_state::BuildFact::Step(_))), "the fabric-primary's log holds no step receipt");
        let mut resumed = nodes_over(&served.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let disposition = resumed.disposition();
        let (again, end) = drain(&mut resumed, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        (first, again, disposition)
    });
    assert_eq!(first[1..], [Frame::Event(NodeEvent::Created), Frame::Event(NodeEvent::Started), Frame::Event(NodeEvent::Joined), Frame::Event(NodeEvent::Ready), Frame::Complete], "{first:?}");
    assert_eq!(disposition, rafka_node_rpc_contract::build::Disposition::AlreadyApplied);
    assert_eq!(again[1..], first[1..], "every event again, from the drive's own record: {again:?}");
}

/// CONTRACT (R-ON8): a fabric-primary that took the seat after a Build ended holds no frame of it.
/// Its re-submit is `AlreadyApplied` and the frames are read from the executor of the last attempt
/// (`build.attempt.run` attaches to what that admin wrote itself), not from step facts the new
/// fabric-primary's log may not hold yet.
#[test]
fn a_fabric_primary_with_no_drive_of_an_ended_build_replays_it_from_the_executor() {
    let cap = capture();
    let (first, again, disposition) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let fp = Arc::new(MemoryBuildStateAdapter::new());
        let before = door_with_own_log(&rig, fp.clone(), Duration::from_secs(30)).await;
        let served = serve(before.clone()).await;
        rig.membership.joined.store(true, Ordering::SeqCst);
        let mut stream = nodes_over(&served.carrier).create(&NodeSpec { mesh: "mesh1".into(), kind: NodeKind::RpcNode }).await.unwrap();
        let build = BuildId(stream.accepted().build_id.0.clone());
        let accepted = stream.accepted().clone();
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, "mesh1.rpc.3").await }
        });
        let (first, end) = drain(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        // The same Build log and executor under a fabric-primary that never drove this Build.
        let after = Arc::new(BuildDoor { me: before.me.clone(), control: before.control.clone(), drives: Arc::new(Drives::default()), env: before.env.clone(), run: before.run.clone() });
        let successor = serve(after).await;
        let mut resumed = nodes_over(&successor.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let disposition = resumed.disposition();
        let (again, end) = drain(&mut resumed, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        (first, again, disposition)
    });
    assert_eq!(disposition, rafka_node_rpc_contract::build::Disposition::AlreadyApplied);
    assert_eq!(again[1..], first[1..], "every event again, replayed by the executor of the last attempt: {again:?}");
}

/// CONTRACT: a valid put ends `NotBackedToday` naming the call, because no op carries the put;
/// an invalid one is refused before that. Calls whose request no op carries say so by name.
#[test]
fn calls_no_op_carries_today_say_so_by_name() {
    use rafka_node_admin_client::{MetaField, MetaFieldIsRdmOwned, NodeMeta};
    let cap = capture();
    cap.run(async {
        let rig = Arc::new(Rig::new());
        let served = serve(door(&rig, Duration::from_secs(1)).await).await;
        let nodes = nodes_over(&served.carrier);
        let held = NodeMeta {
            node_id: NodeId::mint(),
            incarnation: Some(IncarnationId::mint()),
            endpoint: Some("ep-1".into()),
            transport_addr: Some("127.0.0.1:4000".parse().unwrap()),
            exe: Some("rpc-node@abc123".into()),
            status: rafka_node_admin_client::NodeStatus::ReadyForTraffic,
            name: "mesh1.rpc.1".parse().unwrap(),
            mesh: "mesh1".into(),
            kind: NodeKind::RpcNode,
            otel_debug: false,
        };
        assert!(matches!(nodes.update(&held, &held, false), Ok(CallEnd::NotBackedToday { op: NodeOp::Update, .. })));
        assert_eq!(nodes.update(&NodeMeta { status: rafka_node_admin_client::NodeStatus::Dead, ..held.clone() }, &held, false).unwrap_err(), MetaFieldIsRdmOwned { field: MetaField::Status });
        for (end, op) in [(nodes.connections_delete(), NodeOp::ConnectionsDelete), (nodes.config_get(), NodeOp::ConfigGet), (nodes.config_update(), NodeOp::ConfigUpdate), (nodes.start(), NodeOp::Start)] {
            assert!(matches!(end, CallEnd::NotBackedToday { op: got, .. } if got == op), "{end:?}");
        }
    });
}

/// CONTRACT: every step the create and retire pipelines record is one the client names. The
/// receipts of a whole create fold to `node.created`, `node.started`, `node.joined`, `node.ready`
/// and `Complete`; the receipts of a whole delete fold to the draining, drained, left, stopped and
/// deleted events and `Complete`; the retire leg of a restart carries `node.restarting`. A step the
/// client does not name is refused by name, never skipped.
#[test]
fn every_pipeline_step_is_named_by_the_client() {
    use rafka_node_admin_core::deployment::pipeline::{CreateStep, RetireStep};
    let view = |op: &str, steps: Vec<&str>| -> rafka_node_admin_client::BuildView {
        serde_json::from_value(json!({
            "build_id": "bld-1", "topology": {}, "submitted_at_ms": 0, "state": "running", "attempt": 1, "executor": null,
            "steps": steps.iter().map(|s| json!({"attempt": 1, "operation": op, "step": s, "outcome": "complete"})).collect::<Vec<_>>(),
            "last_failure": null, "reason": "requested"
        }))
        .unwrap()
    };
    let create: Vec<&str> = CreateStep::ORDER.iter().map(|s| s.name()).collect();
    let f = rafka_node_admin_client::fold(&WorkflowKind::Create, &view("create-node:mesh1.rpc.3", create), 1).unwrap();
    assert_eq!(f.frames, [Frame::Event(NodeEvent::Created), Frame::Event(NodeEvent::Started), Frame::Event(NodeEvent::Joined), Frame::Event(NodeEvent::Ready), Frame::Complete]);
    let name: rafka_mesh_entity::PathName = "mesh1.rpc.3".parse().unwrap();
    let retire: Vec<&str> = RetireStep::ORDER.iter().map(|s| s.name()).collect();
    let f = rafka_node_admin_client::fold(&WorkflowKind::Delete(name.clone()), &view("retire-node:mesh1.rpc.3", retire), 1).unwrap();
    assert_eq!(
        f.frames,
        [Frame::Event(NodeEvent::Draining), Frame::Event(NodeEvent::Drained), Frame::Event(NodeEvent::Left), Frame::Event(NodeEvent::Stopped), Frame::Event(NodeEvent::Deleted), Frame::Complete]
    );
    let restart = vec!["NodeRestarting", "DrainNode", "AwaitNodeDrained", "StopNode", "AwaitNodeLeft", "TerminateRuntime", "RemoveTopologyMembership", "Complete"];
    let f = rafka_node_admin_client::fold(&WorkflowKind::Restart(name.clone()), &view("retire-node:mesh1.rpc.3", restart), 1).unwrap();
    assert_eq!(f.frames.first(), Some(&Frame::Event(NodeEvent::Restarting)));
    assert!(!f.frames.contains(&Frame::Complete), "the retire leg of a restart is not the restart's end");
    let unknown = view("create-node:mesh1.rpc.3", vec!["AllocateIdentity", "InventedStep"]);
    assert_eq!(
        rafka_node_admin_client::fold(&WorkflowKind::Create, &unknown, 1).unwrap_err(),
        CallEnd::UnrecognisedReceipt { operation: "create-node:mesh1.rpc.3".into(), step: "InventedStep".into() }
    );
}
