//! The typed `node` objects, end to end: a workflow call over the control API returns a reply
//! stream derived from the Build's receipts, and the receipts are written by the real create
//! pipeline running in the same process.
//!
//! The two acceptance contracts of ops-naming ("A failure names its step", "A broken stream is
//! not a failure") are the first two cells. A workflow's `node.start` steps run today inside
//! `node.create`, so the start whose join fails is a create whose join fails.

use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode, NodeId, RuntimeFact};
use rafka_node_admin_client::{Builds, CallEnd, Frame, NodeAdminClient, NodeEvent, NodeSpec, NodeStep, Nodes, Resume, WorkflowKind, WorkflowStream};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter};
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{CreateRequest, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, Timeouts, TopologySink};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::http::{router, ControlPlane};
use rafka_node_admin_core::join::Joins;
use rafka_node_admin_core::model::{Fabric, Mesh, Node, NodeKind, NodeStatus, ProviderKind, ScopeStatus};
use rafka_node_admin_core::topology::Topology;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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
}

#[async_trait::async_trait]
impl NodeObserver for Membership {
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        self.joined.load(Ordering::SeqCst).then(|| self.published.lock().unwrap().clone()).flatten()
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        Ok(())
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

/// The real control router over `builds`, on a loopback port.
async fn serve(builds: Arc<MemoryBuildStateAdapter>) -> (String, Arc<ControlPlane>) {
    let t = mn();
    let accepted = rafka_node_admin_core::accepted::AcceptedStore::seeded(&*builds, t.fabric.id.clone(), rafka_node_admin_core::accepted::FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds, accepted, "mesh1.admin.1".parse().unwrap(), t, crate::adopted_time()));
    let app = router(cp.clone(), axum::Router::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (base, cp)
}

/// A TCP hop between the client and the control API that the test can sever: every open
/// connection is closed and no new one is accepted.
struct Wire {
    base: String,
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    listener: tokio::task::JoinHandle<()>,
}

impl Wire {
    async fn to(upstream: &str) -> Wire {
        let upstream = upstream.trim_start_matches("http://").to_string();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::default();
        let held = tasks.clone();
        let listener = tokio::spawn(async move {
            while let Ok((mut down, _)) = listener.accept().await {
                let upstream = upstream.clone();
                let h = tokio::spawn(async move {
                    if let Ok(mut up) = tokio::net::TcpStream::connect(&upstream).await {
                        let _ = tokio::io::copy_bidirectional(&mut down, &mut up).await;
                    }
                });
                held.lock().unwrap().push(h);
            }
        });
        Wire { base, tasks, listener }
    }

    fn sever(&self) {
        self.listener.abort();
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
    }
}

struct Rig {
    builds: Arc<MemoryBuildStateAdapter>,
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
        Rig { builds: Arc::new(MemoryBuildStateAdapter::new()), joins: Arc::new(Joins::default()), membership: Arc::new(Membership { published: Mutex::new(None), joined: AtomicBool::new(false) }), sink: Arc::default(), template, data_root }
    }

    /// Claim the next attempt of `build` and run the create pipeline for `name` in it, as the
    /// executing admin would.
    async fn create(&self, build: &BuildId, name: &str, join: Duration) -> Result<(), String> {
        let attempt = self.builds.read_build(build).await.unwrap().attempt + 1;
        self.builds.claim_attempt(&BuildAttemptClaim { build_id: build.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
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
            let view = self.builds.read_build(build).await.unwrap();
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
        let rig = Rig::new();
        let (base, _cp) = serve(rig.builds.clone()).await;
        let nodes = Nodes::new(NodeAdminClient::new(base));
        let mut stream = nodes.create(&NodeSpec { mesh: "mesh1".into(), kind: NodeKind::RpcNode }).await.unwrap();
        let build = BuildId(stream.accepted().build_id.0.clone());
        let (run, _) = tokio::join!(rig.create(&build, "mesh1.rpc.3", Duration::from_millis(300)), rig.report_bind(&build, "mesh1.rpc.3"));
        assert!(run.unwrap_err().contains("WaitForMeshJoin"), "the pipeline failed at the join step");
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

/// CONTRACT (ops-naming acceptance 2, "A broken stream is not a failure"): the wire between the
/// caller and the control API is cut while the create is at its join. The caller's stream ends
/// `Indeterminate`, never `Failed`. `build.get` over a new connection returns the receipts of
/// exactly the steps that completed, `Resume` names the join as the first step without a
/// `Complete` receipt, and the next attempt re-runs none of the completed steps: each still has
/// one receipt, the next attempt's receipts begin at the join, and the reused steps' spans say so.
#[test]
fn a_cut_stream_is_indeterminate_and_the_resume_reruns_no_completed_step() {
    let cap = capture();
    let (frames, end, view_after_cut, resume, view_after, final_frames) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let (base, _cp) = serve(rig.builds.clone()).await;
        let wire = Wire::to(&base).await;
        let nodes = Nodes::new(NodeAdminClient::new(wire.base.clone()));
        let mut stream = nodes.create(&NodeSpec { mesh: "mesh1".into(), kind: NodeKind::RpcNode }).await.unwrap();
        let build = BuildId(stream.accepted().build_id.0.clone());
        let accepted = stream.accepted().clone();
        // The executor runs the create; the birth reports its bind; the join is never heard.
        let executor = {
            let rig = rig.clone();
            let build = build.clone();
            tokio::spawn(async move { let _ = rig.create(&build, "mesh1.rpc.3", Duration::from_secs(60)).await; })
        };
        tokio::spawn({
            let rig = rig.clone();
            let build = build.clone();
            async move { rig.report_bind(&build, "mesh1.rpc.3").await }
        });
        let mut frames = Vec::new();
        while !frames.contains(&Frame::Event(NodeEvent::Started)) {
            match tokio::time::timeout(Duration::from_secs(5), stream.next()).await.expect("the stream went quiet before node.started") {
                Some(Ok(f)) => frames.push(f),
                other => panic!("the stream ended before the cut: {other:?}"),
            }
        }
        // The cut: the executor's process dies at the join and the caller's wire is severed.
        executor.abort();
        let _ = executor.await;
        wire.sever();
        let end = loop {
            match tokio::time::timeout(Duration::from_secs(5), stream.next()).await.expect("the severed stream never ended") {
                Some(Ok(f)) => frames.push(f),
                Some(Err(end)) => break end,
                None => panic!("the stream ended without saying how: {frames:?}"),
            }
        };
        // Re-attach over a new connection.
        let builds = Builds::new(NodeAdminClient::new(base.clone()));
        let view_after_cut = builds.get(&accepted.build_id).await.unwrap();
        let resume = Resume::from_view(&WorkflowKind::Create, &view_after_cut, accepted.attempt).unwrap();
        // The next attempt resumes: the birth is heard this time.
        rig.membership.joined.store(true, Ordering::SeqCst);
        rig.create(&build, "mesh1.rpc.3", Duration::from_secs(5)).await.unwrap();
        let view_after = builds.get(&accepted.build_id).await.unwrap();
        let folded = rafka_node_admin_client::fold(&WorkflowKind::Create, &view_after, accepted.attempt).unwrap();
        (frames, end, view_after_cut, resume, view_after, folded.frames)
    });
    assert!(matches!(end, CallEnd::Indeterminate { .. }), "a cut stream is Indeterminate, not {end:?}");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Failed { .. } | Frame::Complete)), "no step is inferred failed or complete from a broken stream: {frames:?}");
    assert!(frames.contains(&Frame::Event(NodeEvent::Started)) && !frames.contains(&Frame::Event(NodeEvent::Joined)));

    let completed_steps = |view: &rafka_node_admin_client::BuildView, attempt: u32| -> Vec<String> {
        view.steps.iter().filter(|s| s.attempt == attempt && s.outcome == json!("complete")).map(|s| s.step.clone()).collect()
    };
    assert_eq!(view_after_cut.steps.len(), ATTEMPT_ONE_STEPS.len(), "build.get returns exactly the receipts of the steps that completed: {:?}", view_after_cut.steps);
    assert_eq!(completed_steps(&view_after_cut, 1), ATTEMPT_ONE_STEPS);
    assert_eq!(resume.from, Some(("create-node".to_string(), "WaitForMeshJoin".to_string())), "the resume starts at the first step without a Complete receipt");
    assert_eq!(resume.completed.len(), ATTEMPT_ONE_STEPS.len());

    for step in ATTEMPT_ONE_STEPS {
        assert_eq!(view_after.steps.iter().filter(|s| s.step == step).count(), 1, "{step} was not re-run: one receipt, from attempt 1");
    }
    assert_eq!(completed_steps(&view_after, 2), ["WaitForMeshJoin", "WaitForNodeReady", "Complete"], "attempt 2 begins at the join");
    assert_eq!(final_frames, [Frame::Event(NodeEvent::Created), Frame::Event(NodeEvent::Started), Frame::Event(NodeEvent::Joined), Frame::Event(NodeEvent::Ready), Frame::Complete]);

    let spans = cap.spans();
    let call = spans_named(&spans, "rdm.node_admin.node.create.via-workflow");
    assert_eq!(call.len(), 1, "{spans:?}");
    assert_eq!(call[0]["attributes"]["outcome"], "indeterminate");
    let attempt_two: Vec<(String, String)> = spans_named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .filter(|s| s["attributes"]["attempt"] == "2")
        .map(|s| (s["attributes"]["step"].as_str().unwrap().to_string(), s["attributes"]["outcome"].as_str().unwrap().to_string()))
        .collect();
    for step in ATTEMPT_ONE_STEPS {
        assert!(attempt_two.contains(&(step.to_string(), "reused".to_string())), "{step} was reused in attempt 2, not re-run: {attempt_two:?}");
    }
    assert!(attempt_two.contains(&("WaitForMeshJoin".to_string(), "complete".to_string())));
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
