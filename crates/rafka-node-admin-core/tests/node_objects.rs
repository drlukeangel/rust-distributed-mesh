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
use rafka_node_admin_core::build_drive::{DepartureProof, Dispatched, Dispatcher, DriveEnv, Drives, InnerItem, InnerStream, OpenGate};
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
    capture: rafka_mesh_telemetry::SpanCapture,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    crate::enable_callsites();
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (capture, layer) = rafka_mesh_telemetry::span_capture("node-objects", exporter.clone());
    Capture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
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
        self.capture.flush(Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let finished = self.exporter.get_finished_spans().unwrap();
        crate::assert_own_story(finished.iter().map(|s| s.name.as_ref()));
        finished
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({"name": s.name, "trace_id": s.span_context.trace_id().to_string(), "attributes": attributes})
            })
            .collect()
    }
}

/// A cell that panics leaves its trace: every span its capture holds (name, ids, attributes) goes to
/// `target/i143-testkit/node_objects/<cell>.<pid>.spans.json`, so a red reads from its own spans
/// (did the admin accept the connection, how did the call end) and never from a rerun.
impl Drop for Capture {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        let flushed = self.capture.flush(Duration::from_secs(10));
        let spans: Vec<Value> = self
            .exporter
            .get_finished_spans()
            .unwrap_or_default()
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({
                    "name": s.name,
                    "trace_id": s.span_context.trace_id().to_string(),
                    "span_id": s.span_context.span_id().to_string(),
                    "parent_span_id": s.parent_span_id.to_string(),
                    "start": s.start_time.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_micros() as u64).unwrap_or(0),
                    "end": s.end_time.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_micros() as u64).unwrap_or(0),
                    "attributes": attributes,
                })
            })
            .collect();
        let cell = std::thread::current().name().unwrap_or("unnamed").replace("::", "__");
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit/node_objects");
        let path = dir.join(format!("{cell}.{}.spans.json", std::process::id()));
        let written = std::fs::create_dir_all(&dir).and_then(|_| std::fs::write(&path, serde_json::to_vec_pretty(&spans).unwrap()));
        eprintln!("node_objects: the panicking cell's {} spans -> {} (flush: {flushed:?}, write: {written:?})", spans.len(), path.display());
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
    /// While set, the node reports Pending with no blocker: its Ready check fails with this and names nothing.
    pending: Mutex<Option<String>>,
}

#[async_trait::async_trait]
impl NodeObserver for Membership {
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        self.joined.load(Ordering::SeqCst).then(|| self.published.lock().unwrap().clone()).flatten()
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        match self.not_ready.lock().unwrap().clone().or_else(|| self.pending.lock().unwrap().clone()) {
            Some(why) => Err(why),
            None => Ok(()),
        }
    }
    async fn ready_blocker(&self, _: &Node) -> Option<String> {
        self.not_ready.lock().unwrap().clone()
    }
}

#[derive(Default)]
struct Published(Mutex<Vec<Node>>);
impl TopologySink for Published {
    fn publish(&self, n: Node) {
        self.0.lock().unwrap().push(n);
    }
    fn remove(&self, _: &rafka_node_admin_core::model::PathName) {}
    fn remove_birth(&self, _: &rafka_node_admin_core::model::PathName, _: &rafka_mesh_entity::NodeId, _: &rafka_mesh_entity::IncarnationId) {}
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
    door_via(rig, join, |run| Arc::new(Local(run)), Arc::new(crate::loopback::Proofs::default())).await
}

/// [`door`], whose drive calls `dispatcher` (made over the in-process executor door) and reads an
/// executor's departure from `departure`.
async fn door_via(rig: &Arc<Rig>, join: Duration, dispatcher: impl FnOnce(Arc<RunDoor>) -> Arc<dyn Dispatcher>, departure: Arc<dyn DepartureProof>) -> Arc<BuildDoor> {
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
        door: Arc::new(ClaimDoor { me: me.clone(), node_id: control.topology.read().await.node(&me).expect("the fabric-primary is in the view").node_id.clone(), topology: control.topology.clone(), builds: rig.builds.clone(), contexts: Arc::new(AttemptContexts::in_memory()) }),
        dispatcher: dispatcher(run.clone()),
        gate: Arc::new(OpenGate),
        departure,
        verdicts: Arc::new(crate::loopback::NoVerdicts),
        rows: Arc::new(rafka_node_admin_core::node_rows::NodeRows::new(me.clone(), control.topology.read().await.node(&me).expect("the fabric-primary is in the view").node_id.clone(), Arc::new(rafka_node_admin_core::storage::MemoryNodesStorage::default()))),
    });
    Arc::new(BuildDoor { me, control, drives: Arc::new(Drives::default()), env, run, seats: rafka_mesh_transport::membership::SeatBook::default(), incarnation: rafka_mesh_entity::IncarnationId::mint() })
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
        door: Arc::new(ClaimDoor { me: me.clone(), node_id: control.topology.read().await.node(&me).expect("the fabric-primary is in the view").node_id.clone(), topology: control.topology.clone(), builds: fp.clone(), contexts: Arc::new(AttemptContexts::in_memory()) }),
        dispatcher: Arc::new(Local(run.clone())),
        gate: Arc::new(OpenGate),
        departure: Arc::new(crate::loopback::Proofs::default()),
        verdicts: Arc::new(rafka_node_admin_core::build_op::LocalVerdicts { local: fp }),
        rows: Arc::new(rafka_node_admin_core::node_rows::NodeRows::new(me.clone(), control.topology.read().await.node(&me).expect("the fabric-primary is in the view").node_id.clone(), Arc::new(rafka_node_admin_core::storage::MemoryNodesStorage::default()))),
    });
    Arc::new(BuildDoor { me, control, drives: Arc::new(Drives::default()), env, run, seats: rafka_mesh_transport::membership::SeatBook::default(), incarnation: rafka_mesh_entity::IncarnationId::mint() })
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
        Rig { builds: Arc::new(FramedBuilds::new(local.clone(), runs.clone())), local, runs, joins: Arc::new(Joins::default()), membership: Arc::new(Membership { published: Mutex::new(None), joined: AtomicBool::new(false), not_ready: Mutex::new(None), pending: Mutex::new(None) }), sink: Arc::default(), template, data_root }
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
                    config: Default::default(),
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
/// and whose join then fails ends its stream `Started`, then
/// `Failed { step: node.join, reason }`. `node.created`, `node.joined` and `node.started` are never emitted, the
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
        [Frame::Started { build_id: match &frames[0] { Frame::Started { build_id, .. } => build_id.clone(), f => panic!("{f:?}") }, attempt }]
    );
    assert!(end.is_none(), "a failed step ends the stream with its terminal frame, not a broken stream");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Event(NodeEvent::Created | NodeEvent::Joined | NodeEvent::Started) | Frame::Complete)), "nothing downstream of the failed step is emitted: {frames:?}");
    assert!(published.iter().all(|n| n.status != NodeStatus::ReadyForTraffic), "the node never reached ready-for-traffic");

    let spans = cap.spans();
    let call = spans_named(&spans, "rdm.node_admin.node.create.via-workflow");
    assert_eq!(call.len(), 1, "{spans:?}");
    assert_eq!(call[0]["attributes"]["outcome"], "failed");
    assert_eq!(call[0]["attributes"]["failed_step"], "node.join");
    for never in ["created", "joined", "started"] {
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
/// emits `node.created` and `Complete`.
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
    assert_eq!(after, [Frame::Event(NodeEvent::Started), Frame::Event(NodeEvent::Created), Frame::Complete], "a repeated blocker makes no second frame, and the ready node ends the stream: {after:?}");
}

/// CONTRACT: a create whose node has joined but only reports Pending (no hook has named a blocker)
/// streams no `Blocked` frame however long the Ready check fails; when the node is ready the stream
/// emits `node.created` and `Complete`.
#[test]
fn a_create_whose_node_is_merely_pending_streams_no_blocked_frame() {
    let cap = capture();
    let (frames, end) = cap.run(async {
        let rig = Arc::new(Rig::new());
        *rig.membership.pending.lock().unwrap() = Some("mesh1.rpc.3 reports Pending".to_string());
        rig.membership.joined.store(true, Ordering::SeqCst);
        let served = serve(door(&rig, Duration::from_secs(30)).await).await;
        let nodes = nodes_over(&served.carrier);
        let mut stream = nodes.create(&NodeSpec { mesh: "mesh1".into(), kind: NodeKind::RpcNode }).await.unwrap();
        let build = BuildId(stream.accepted().build_id.0.clone());
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, "mesh1.rpc.3").await }
        });
        tokio::time::sleep(Duration::from_millis(450)).await;
        *rig.membership.pending.lock().unwrap() = None;
        drain(&mut stream, Duration::from_secs(10)).await
    });
    assert!(end.is_none(), "the stream ends with its terminal frame: {end:?}");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Blocked { .. })), "a Pending node with no named blocker is not a Blocked frame: {frames:?}");
    assert_eq!(frames[frames.len() - 2..], [Frame::Event(NodeEvent::Created), Frame::Complete], "the ready node ends the stream: {frames:?}");
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
        // The cut: the executor's process dies at the join and the caller's wire is severed. The
        // join is where it is cut once the step before it has its receipt.
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
    assert!(!frames.iter().any(|f| matches!(f, Frame::Event(NodeEvent::Joined | NodeEvent::Started | NodeEvent::Created))), "no event past the join was delivered: {frames:?}");

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
    assert_eq!(final_frames[1..], [Frame::Event(NodeEvent::Joined), Frame::Event(NodeEvent::Started), Frame::Event(NodeEvent::Created), Frame::Complete]);

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
    assert_eq!(first[1..], [Frame::Event(NodeEvent::Joined), Frame::Event(NodeEvent::Started), Frame::Event(NodeEvent::Created), Frame::Complete], "{first:?}");
    assert_eq!(disposition, rafka_node_rpc_contract::build::Disposition::AlreadyApplied);
    assert_eq!(again[1..], first[1..], "every event again, from the drive's own record: {again:?}");
}

/// CONTRACT (node-rpc-envelope.md "Caller execution and certainty": "a completed Build answers
/// AlreadyApplied with its terminal frame"): the reconcile loop's tick that read the Build active
/// just before its verdict landed, and so asks for its drive after the drive ended `Complete`, is
/// handed the completed drive itself. The record a re-submit is answered from stays the drive
/// that made the calls: the re-submit is `AlreadyApplied` with every frame and the terminal frame.
/// Must NOT happen: the completed drive replaced by a new one, which reattaches to the executor
/// (a second `build.attempt.run` of the ended attempt) and makes a re-submit read a running drive
/// and answer `Attached` (fabric_build__build_op gate fa338efb, build_op.rs:233).
#[test]
fn a_reconcile_tick_after_the_end_keeps_the_completed_drive_and_a_resubmit_is_already_applied() {
    let cap = capture();
    let (build, kept, first, again, disposition) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let fp = Arc::new(MemoryBuildStateAdapter::new());
        let door = door_with_own_log(&rig, fp.clone(), Duration::from_secs(30)).await;
        let served = serve(door.clone()).await;
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
        let completed = door.drives.get(&build).expect("the drive that completed the Build is held");
        assert!(completed.completed(), "the drive ended Complete");
        // The reconcile loop's `ensure_active`, acting on a `list_active` read taken before the
        // verdict entered the log.
        let ensured = door.drives.ensure(&door.env, &build, accepted.attempt);
        let kept = Arc::ptr_eq(&ensured, &completed) && door.drives.get(&build).is_some_and(|d| Arc::ptr_eq(&d, &completed));
        let mut resumed = nodes_over(&served.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let disposition = resumed.disposition();
        let (again, end) = drain(&mut resumed, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        (build, kept, first, again, disposition)
    });
    assert!(kept, "the reconcile tick is handed the completed drive, never a new one in its place");
    assert_eq!(disposition, rafka_node_rpc_contract::build::Disposition::AlreadyApplied);
    assert_eq!(again[1..], first[1..], "every event again, from the drive's own record: {again:?}");
    let spans = cap.spans();
    let dispatches: Vec<_> = spans_named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().filter(|s| s["attributes"]["build_id"] == build.0.as_str()).collect();
    assert_eq!(dispatches.len(), 1, "one build.attempt.run, from the drive that made the calls: {dispatches:?}");
    eprintln!("span rdm.node_admin.build.update.via-dispatch {}", dispatches[0]);
}

/// CONTRACT (node-recovery.md "Numbered end-to-end sequence" step 1: "A repeat submit answers
/// AlreadyApplied or BuildInProgress, never a second recovery"): over op `0x20`, a recovery submit
/// for an exited birth already recovered answers `Started { AlreadyApplied }` naming the Build and
/// attempt whose `recover-node` operation recovered it, then the terminal `Complete` of that attempt
/// and nothing else. Must NOT happen: a drive made or started for the Build (a drive of an ended
/// Build claims its next attempt), or an attempt opened.
#[test]
fn a_recovery_submit_for_a_birth_already_recovered_answers_already_applied_on_the_wire_and_drives_nothing() {
    use rafka_node_admin_core::build_state::{AttemptOutcome, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStepReceipt, StepOutcome};
    let cap = capture();
    let (opened_before, opened_after, b0, recovering, stream, frames, drive) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let door = door(&rig, Duration::from_secs(30)).await;
        let cp = door.control.clone();
        let path: PathName = "mesh1.rpc.2".parse().unwrap();
        let exited = cp.topology.read().await.node(&path).unwrap().clone();
        let (node_id, birth) = (exited.node_id.clone(), exited.incarnation_id.clone().unwrap());
        let b0 = cp.accepted.build_id().await.unwrap();
        let settle = |attempt: u32| {
            let (builds, b0) = (rig.builds.clone(), b0.clone());
            async move {
                builds.claim_attempt(&BuildAttemptClaim { build_id: b0.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
                builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: b0, attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
            }
        };
        settle(1).await;
        let recovering = match cp.open_recovery("build.create", path.clone(), node_id.clone(), birth.clone()).await {
            Ok(rafka_node_admin_core::http::Recovery::Opened(o)) => o,
            other => panic!("the first recovery is admitted: {other:?}"),
        };
        // The recovery's receipts as the pipeline writes them: NodeDeleted names the exited birth,
        // Complete commits no output.
        let deleted = rafka_mesh_entity::LifecycleOp { build_id: b0.to_string(), attempt: recovering.attempt, operation: format!("recover-node:{path}"), node_id: node_id.clone(), incarnation: birth.clone(), name: path.clone(), event_at_rafka_ms: 1 };
        for (step, output) in [("NodeDeleted", Some(serde_json::to_value(&deleted).unwrap())), ("Complete", None)] {
            rig.builds
                .append_step_receipt(&BuildStepReceipt { build_id: b0.clone(), attempt: recovering.attempt, operation: format!("recover-node:{path}"), step: step.into(), outcome: StepOutcome::Complete, output, executor: Some("mesh1.admin.1".into()) })
                .await
                .unwrap();
        }
        settle(recovering.attempt).await;
        let opened = |facts: Vec<BuildFact>| facts.iter().filter(|f| matches!(f, BuildFact::Opened(_))).count();
        let opened_before = opened(rig.builds.facts().await.unwrap());
        let served = serve(door.clone()).await;
        let mut stream = served.carrier.create(rafka_node_rpc_contract::build::BuildChange::Recover { node: path.clone(), node_id: node_id.clone(), incarnation: birth.clone() }).await.expect("the repeat is answered with a stream");
        let head = (stream.build_id().0.clone(), stream.attempt(), stream.disposition());
        let mut frames = Vec::new();
        while let Some(f) = tokio::time::timeout(Duration::from_secs(5), stream.next()).await.expect("the answer ends") {
            frames.push(f);
        }
        let opened_after = opened(rig.builds.facts().await.unwrap());
        let drive = door.drives.get(&b0).is_some();
        (opened_before, opened_after, b0, recovering, head, frames, drive)
    });
    assert_eq!(stream, (b0.0.clone(), recovering.attempt, rafka_node_rpc_contract::build::Disposition::AlreadyApplied), "Started names the Build and attempt that recovered the birth");
    assert_eq!(frames.len(), 1, "one terminal and nothing else: {frames:?}");
    assert!(matches!(&frames[0], Ok(rafka_node_admin_client::BuildFrame::Complete { attempt }) if *attempt == recovering.attempt), "{frames:?}");
    assert_eq!(opened_after, opened_before, "no attempt is opened");
    assert!(!drive, "no drive is made for the Build");
    let spans = cap.spans();
    let applied = spans_named(&spans, "rdm.node_admin.build.reject.via-recovery-already-applied");
    assert_eq!(applied.len(), 1, "the repeat is answered from the receipt that recovered the birth");
    eprintln!("span rdm.node_admin.build.reject.via-recovery-already-applied {}", applied[0]);
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
        let after = Arc::new(BuildDoor { me: before.me.clone(), control: before.control.clone(), drives: Arc::new(Drives::default()), env: before.env.clone(), run: before.run.clone(), seats: before.seats.clone(), incarnation: before.incarnation.clone() });
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

/// CONTRACT: a put that changes a field RDM owns is refused before anything is sent, and one that
/// changes otel_debug, which no row field carries, ends `NotBackedToday`. Calls whose request no op
/// carries say so by name.
#[test]
fn calls_no_op_carries_today_say_so_by_name() {
    use rafka_node_admin_client::{MetaField, NodeMeta};
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
            runtime: Some("dep-1".into()),
            status: rafka_node_admin_client::NodeStatus::ReadyForTraffic,
            name: "mesh1.rpc.1".parse().unwrap(),
            mesh: "mesh1".into(),
            kind: NodeKind::RpcNode,
            otel_debug: false,
        };
        // The put reaches the node's mesh-primary only with what its request carries: a body that
        // changes a field RDM owns is refused here, and one that changes otel_debug has no row field.
        let (mp, route) = (NodeId::mint(), rafka_node_rpc::RouteChoice::Direct);
        assert_eq!(
            nodes.update(&NodeMeta { status: rafka_node_admin_client::NodeStatus::Dead, ..held.clone() }, &held, &mp, &route).await.expect("a typed refusal"),
            rafka_node_rpc_contract::status::StatusReply::RejectedMetaFieldIsRdmOwned { field: MetaField::Status.name().into() }
        );
        assert!(matches!(nodes.update(&held, &NodeMeta { otel_debug: true, ..held.clone() }, &mp, &route).await, Err(CallEnd::NotBackedToday { op: NodeOp::Update, .. })));
        for (end, op) in [(nodes.connections_delete(), NodeOp::ConnectionsDelete), (nodes.config_get(), NodeOp::ConfigGet)] {
            assert!(matches!(end, CallEnd::NotBackedToday { op: got, .. } if got == op), "{end:?}");
        }
    });
}

/// CONTRACT: every step the create and retire pipelines record is one the client names. The
/// receipts of a whole create fold to `node.joined`, `node.started`, `node.created`
/// and `Complete`; the receipts of a whole delete fold to the draining, drained, connections-deleted, stopped and
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
    assert_eq!(f.frames, [Frame::Event(NodeEvent::Joined), Frame::Event(NodeEvent::Started), Frame::Event(NodeEvent::Created), Frame::Complete]);
    let name: rafka_mesh_entity::PathName = "mesh1.rpc.3".parse().unwrap();
    let retire: Vec<&str> = RetireStep::ORDER.iter().map(|s| s.name()).collect();
    let f = rafka_node_admin_client::fold(&WorkflowKind::Delete(name.clone()), &view("retire-node:mesh1.rpc.3", retire.clone()), 1).unwrap();
    assert_eq!(
        f.frames,
        [Frame::Event(NodeEvent::Draining), Frame::Event(NodeEvent::Drained), Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Event(NodeEvent::Stopped), Frame::Event(NodeEvent::Deleted), Frame::Complete]
    );
    let restart = vec!["NodeRestarting", "DrainNode", "AwaitNodeDrained", "StopNode", "AwaitNodeLeft", "Complete"];
    let f = rafka_node_admin_client::fold(&WorkflowKind::Restart(name.clone()), &view("retire-node:mesh1.rpc.3", restart), 1).unwrap();
    assert_eq!(f.frames.first(), Some(&Frame::Event(NodeEvent::Restarting)));
    assert!(!f.frames.contains(&Frame::Complete), "the retire leg of a restart is not the restart's end");
    // The SeatTransfer step runs only for a node-admin target (it moves the seats it holds before it
    // drains or stops; the pipeline decides on the target's kind): a node-admin's workflow has the
    // row and its receipts fold to the same events; an rpc node's workflow has no such row.
    let admin: rafka_mesh_entity::PathName = "mesh1.admin.2".parse().unwrap();
    let admin_retire: Vec<&str> = ["NodeDeleting", "SeatTransfer"].into_iter().chain(retire.iter().copied().skip(1)).collect();
    let f = rafka_node_admin_client::fold(&WorkflowKind::Delete(admin.clone()), &view("retire-node:mesh1.admin.2", admin_retire), 1).unwrap();
    assert_eq!(
        f.frames,
        [Frame::Event(NodeEvent::Draining), Frame::Event(NodeEvent::Drained), Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Event(NodeEvent::Stopped), Frame::Event(NodeEvent::Deleted), Frame::Complete]
    );
    let f = rafka_node_admin_client::fold(&WorkflowKind::Stop(admin.clone()), &view("stop-node:mesh1.admin.2", vec!["SeatTransfer", "StopNode", "AwaitNodeLeft", "Complete"]), 1).unwrap();
    assert_eq!(f.frames, [Frame::Event(NodeEvent::Draining), Frame::Event(NodeEvent::Drained), Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Event(NodeEvent::Stopped), Frame::Complete]);
    let f = rafka_node_admin_client::fold(&WorkflowKind::Stop(name.clone()), &view("stop-node:mesh1.rpc.3", vec!["StopNode", "AwaitNodeLeft", "Complete"]), 1).unwrap();
    assert_eq!(f.frames, [Frame::Event(NodeEvent::Draining), Frame::Event(NodeEvent::Drained), Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Event(NodeEvent::Stopped), Frame::Complete]);
    // A node-admin whose transfer has not run yet: the fold stops at it, and a resume starts there.
    let f = rafka_node_admin_client::fold(&WorkflowKind::Delete(admin.clone()), &view("retire-node:mesh1.admin.2", vec!["NodeDeleting"]), 1).unwrap();
    assert_eq!((f.frames.len(), f.next.as_ref().map(|(_, s)| s.as_str())), (0, Some("SeatTransfer")));
    let r = rafka_node_admin_client::Resume::from_view(&WorkflowKind::Delete(admin.clone()), &view("retire-node:mesh1.admin.2", vec!["NodeDeleting"]), 1).unwrap();
    assert_eq!(r.from, Some(("retire-node".to_string(), "SeatTransfer".to_string())), "nothing after NodeDeleting is held: the workflow stands at SeatTransfer");
    // A node-admin's SeatTransfer row is a step of its workflow whatever comes after: receipts past it
    // with none for it fold no further than it (nothing is inferred from its absence).
    let skipped: Vec<&str> = ["NodeDeleting"].into_iter().chain(retire.iter().copied().skip(1)).collect();
    let f = rafka_node_admin_client::fold(&WorkflowKind::Delete(admin.clone()), &view("retire-node:mesh1.admin.2", skipped), 1).unwrap();
    assert_eq!((f.frames.len(), f.next.as_ref().map(|(_, s)| s.as_str())), (0, Some("SeatTransfer")), "{:?}", f.frames);
    // An rpc node's workflow has no SeatTransfer row: its delete with only NodeDeleting held resumes
    // at DrainNode, and a SeatTransfer receipt under it is refused by name, never folded.
    let r = rafka_node_admin_client::Resume::from_view(&WorkflowKind::Delete(name.clone()), &view("retire-node:mesh1.rpc.3", vec!["NodeDeleting"]), 1).unwrap();
    assert_eq!(r.from, Some(("retire-node".to_string(), "DrainNode".to_string())));
    for (kind, op, steps) in [
        (WorkflowKind::Delete(name.clone()), "retire-node:mesh1.rpc.3", vec!["NodeDeleting", "SeatTransfer"]),
        (WorkflowKind::Stop(name.clone()), "stop-node:mesh1.rpc.3", vec!["SeatTransfer"]),
    ] {
        assert_eq!(rafka_node_admin_client::fold(&kind, &view(op, steps), 1).unwrap_err(), CallEnd::UnrecognisedReceipt { operation: op.into(), step: "SeatTransfer".into() }, "{kind:?}");
    }
    let unknown = view("create-node:mesh1.rpc.3", vec!["AllocateIdentity", "InventedStep"]);
    assert_eq!(
        rafka_node_admin_client::fold(&WorkflowKind::Create, &unknown, 1).unwrap_err(),
        CallEnd::UnrecognisedReceipt { operation: "create-node:mesh1.rpc.3".into(), step: "InventedStep".into() }
    );
}

/// One attempt's run as an executor streams it to the fabric-primary: its frames, then its end.
struct Script(std::collections::VecDeque<InnerItem>);

#[async_trait::async_trait]
impl InnerStream for Script {
    async fn next(&mut self) -> InnerItem {
        self.0.pop_front().unwrap_or(InnerItem::Ended)
    }
}

/// The executor of a restart of `mesh1.rpc.1`, attempt by attempt. The first run is lost once its
/// `DrainNode` frame has reached the fabric-primary: the receipt lived in the lost executor's own
/// log alone (its broadcast never left), so no Build log the successor reads holds it, and the
/// executor's exact runtime is proven exited. The next run holds no `DrainNode` receipt: it
/// reissues that uncertain step (node-rpc-envelope.md "Caller execution and certainty") and runs
/// the restart to its end.
struct LostAfterDrainNode {
    runs: Mutex<u32>,
    proofs: Arc<crate::loopback::Proofs>,
}

#[async_trait::async_trait]
impl Dispatcher for LostAfterDrainNode {
    async fn dispatch(&self, executor: &PathName, build_id: &BuildId, attempt: u32, _context: rafka_node_rpc_contract::context::CallContext, _intent: Vec<Vec<u8>>, _plan: rafka_node_admin_core::executor::RunPlan) -> Dispatched {
        use rafka_node_rpc_contract::build::BuildReply;
        let step = |operation: &str, step: &str| InnerItem::Frame(BuildReply::Step { build_id: build_id.0.clone(), attempt, operation: format!("{operation}:mesh1.rpc.1"), step: step.into() });
        let run = {
            let mut runs = self.runs.lock().unwrap();
            *runs += 1;
            *runs
        };
        let items = if run == 1 {
            self.proofs.exited.lock().unwrap().insert(executor.to_string());
            vec![step("retire-node", "NodeRestarting"), step("retire-node", "DrainNode"), InnerItem::Broke(format!("{executor} was lost after its DrainNode receipt"))]
        } else {
            vec![
                step("retire-node", "DrainNode"),
                step("retire-node", "AwaitNodeDrained"),
                step("retire-node", "StopNode"),
                step("retire-node", "AwaitNodeLeft"),
                step("retire-node", "Complete"),
                step("restart-node", "StartNode"),
                step("restart-node", "WaitForNodeReady"),
                step("restart-node", "Complete"),
                InnerItem::Frame(BuildReply::Complete { build_id: build_id.0.clone(), attempt }),
            ]
        };
        Dispatched::Stream(Box::new(Script(items.into())))
    }
}

/// CONTRACT (node-rpc-envelope.md "Caller execution and certainty": a changing call streams each
/// completed step's event once): the executor of a restart is lost after its `DrainNode` frame
/// reached the caller and before that receipt reached any Build log the successor reads. The
/// successor's attempt reissues `DrainNode` (an uncertain step to it) and writes its own receipt:
/// two receipts of one step, one per attempt, each a frame under its own receipt key. The caller's
/// stream reads them as the client's receipt fold does, per step the latest attempt deciding, so
/// `node.draining` is delivered once and every other event of the restart once, in order.
#[test]
fn a_step_a_successor_attempt_completes_again_after_its_executor_was_lost_is_one_event_to_the_caller() {
    let cap = capture();
    let frames = cap.run(async {
        let rig = Arc::new(Rig::new());
        let proofs = Arc::new(crate::loopback::Proofs::default());
        let script = Arc::new(LostAfterDrainNode { runs: Mutex::new(0), proofs: proofs.clone() });
        let door = door_via(&rig, Duration::from_secs(60), move |_| script, proofs).await;
        let served = serve(door).await;
        let mut stream = nodes_over(&served.carrier).restart(&"mesh1.rpc.1".parse().unwrap()).await.expect("the fabric-primary accepts the restart");
        let (frames, end) = drain(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "the stream ends with its terminal frame: {end:?} after {frames:?}");
        frames
    });
    let events: Vec<NodeEvent> = frames.iter().filter_map(|f| if let Frame::Event(e) = f { Some(*e) } else { None }).collect();
    assert_eq!(
        events,
        [NodeEvent::Restarting, NodeEvent::Draining, NodeEvent::Drained, NodeEvent::ConnectionsDeleted, NodeEvent::Stopped, NodeEvent::Joined, NodeEvent::Started, NodeEvent::Restarted],
        "every event of the restart once, in order: {frames:?}"
    );
    assert_eq!(frames.last(), Some(&Frame::Complete));

    let spans = cap.spans();
    let lost: Vec<_> = spans_named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().filter(|s| s["attributes"]["outcome"] == "executor-lost").collect();
    assert_eq!(lost.len(), 1, "the first attempt's executor was lost: {spans:?}");
    assert_eq!(spans_named(&spans, "rdm.node_admin.build.update.via-departure-proven").len(), 1, "its departure was proven before the successor's attempt");
    assert_eq!(spans_named(&spans, "rdm.node_admin.node.draining.via-reply-frame").len(), 1, "node.draining was delivered once");
}
