//! The typed `node` objects, end to end: a workflow call returns a reply stream read from the
//! fabric-primary's Build family (op `0x20`), and the receipts are written by the real create
//! pipeline running in the same process.
//!
//! The two acceptance contracts of ops-naming ("A failure names its step", "A broken stream is
//! not a failure") are the first two cells. A workflow's `node.start` steps run today inside
//! `node.create`, so the start whose join fails is a create whose join fails.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode, NodeId, RuntimeFact};
use rdm_client::{BuildCarrier, Builds, CallEnd, Frame, NodeEvent, NodeOp, NodeStep, Nodes, Resume, WorkflowKind, WorkflowStream};
use rdm_admin::build::{BuildId, BuildOperation};
use rdm_admin::build_claim::{AttemptContexts, ClaimDoor};
use rdm_admin::build_drive::{DepartureProof, Dispatched, Dispatcher, DriveEnv, Drives, InnerItem, InnerStream, OpenGate};
use rdm_admin::build_op::{local_dispatched, BuildDoor, BuildSlot};
use rdm_admin::build_run::{AttemptRuns, FramedBuilds, RunDoor};
use rdm_admin::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::executor::{BuildExecutor, OperationRunner};
use rdm_transport::{NodeRpcClient, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rdm_admin::deployment::endpoint::RPC_NODE;
use rdm_admin::deployment::pipeline::{CreateRequest, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, Timeouts, TopologySink};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::control_plane::ControlPlane;
use rdm_admin::join::Joins;
use rdm_admin::model::{Fabric, Mesh, Node, NodeKind, NodeStatus, PathName, ProviderKind, ScopeStatus};
use rdm_admin::topology::Topology;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("node-objects", exporter.clone());
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
        rt.block_on(async {
            // Warm the blocking pool before the cell's clock starts, so PrepareStorage's first
            // blocking_io call runs on an existing blocking thread rather than paying thread spawn.
            let _ = tokio::task::spawn_blocking(|| ()).await;
            f.await
        })
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
/// `$TMPDIR/i143-testkit/node_objects/<cell>.<pid>.spans.json`, so a red reads from its own spans
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
        let dir = std::env::temp_dir().join("i143-testkit/node_objects");
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
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
    }
}

/// The topology view the admin holds of the birth: it hears the birth only once `joined` is set.
struct TopologyView {
    /// While set, the node is not ready and this is what its Ready check names.
    not_ready: Mutex<Option<String>>,
    /// While set, the node reports Pending with no blocker: its Ready check fails with this and names nothing.
    pending: Mutex<Option<String>>,
}

#[async_trait::async_trait]
impl NodeObserver for TopologyView {
    /// No digest of any birth reaches this fixture: the pipeline's waits complete from the report
    /// `report_bind` makes, and no cell here runs the lost-executor rule.
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        None
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
struct Published(Mutex<Vec<Node>>, std::sync::OnceLock<Arc<rdm_admin::node_rows::NodeRows>>);
impl TopologySink for Published {
    fn publish(&self, n: Node) {
        self.0.lock().unwrap().push(n);
    }
    fn remove(&self, _: &rdm_admin::model::PathName) {}
    fn remove_birth(&self, _: &rdm_admin::model::PathName, _: &rdm_core::NodeId, _: &rdm_core::IncarnationId) {}
    fn node_rows(&self) -> Option<Arc<rdm_admin::node_rows::NodeRows>> {
        self.1.get().cloned()
    }
}

fn node(name: &str, primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.incarnation_id = Some(IncarnationId::mint());
    if n.kind == NodeKind::NodeAdmin {
        n.admin_api_base = Some(format!("http://127.0.0.1:1800{}", n.name.ordinal));
        n.is_fabric_primary = primary;
    }
    n
}

/// One mesh: `mesh1.admin.1` (the fabric-primary), `mesh1.rpc.1` and `mesh1.rpc.2`, which holds mesh1's
/// rpc role. The cells stop `mesh1.rpc.1`, which holds no role, so no node.primary.update precedes
/// the stop (proposal Part C1) and the drive's gate of this fixture, which moves no role, is never
/// asked to; a recovery of an exited birth moves no role (`executor::role_move`).
fn mn() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(rdm_core::MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![node("mesh1.admin.1", true), node("mesh1.rpc.1", false), node("mesh1.rpc.2", true)],
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
    let accepted = rdm_admin::accepted::AcceptedStore::seeded(&*rig.builds, t.fabric.id.clone(), rdm_admin::accepted::FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    // The fabric-primary's node records, one store for its door and its writer (boot.rs): its door
    // accepts an action on a birth from its record, never from the gossip view (Q197).
    let records = crate::records_of(&t).await;
    let control = Arc::new(ControlPlane::new(rig.builds.clone(), accepted.clone(), me.clone(), t, crate::adopted_time()));
    let _ = control.node_rows.set(records.clone());
    let runner: Arc<dyn OperationRunner> = Arc::new(RigRunner { rig: rig.clone(), join });
    let exec = Arc::new(BuildExecutor { executor: me.to_string(), builds: rig.builds.clone(), topology: control.topology.clone(), runner });
    let run = Arc::new(RunDoor { me: me.clone(), builds: rig.builds.clone(), exec, runs: rig.runs.clone(), local: rig.local.clone(), gate: rdm_admin::stop_gate::StopGate::new() });
    let rows = Arc::new(rdm_admin::node_rows::NodeRows::new(me.clone(), control.topology.read().await.node(&me).expect("the fabric-primary is in the view").node_id.clone(), control.topology.clone(), records));
    let _ = rig.sink.1.set(rows.clone());
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: control.topology.clone(),
        builds: rig.builds.clone(),
        door: Arc::new(ClaimDoor {
            me: me.clone(),
            node_id: control
                .topology
                .read()
                .await
                .node(&me)
                .expect("the fabric-primary is in the view")
                .node_id
                .clone(),
            topology: control.topology.clone(),
            builds: rig.builds.clone(),
            contexts: Arc::new(AttemptContexts::in_memory()),
            claims: control.claims.clone(),
            claimed: Default::default(),
        }),
        dispatcher: dispatcher(run.clone()),
        gate: Arc::new(OpenGate),
        departure,
        verdicts: Arc::new(crate::loopback::NoVerdicts),
        notices: rig.told.notices(),
        turns: rdm_admin::stop_gate::StopGate::new(),
    });
    Arc::new(BuildDoor { me, control, drives: Arc::new(Drives::default()), env, run, primaries: rdm_transport::topology::PrimaryView::default(), incarnation: rdm_core::IncarnationId::mint() })
}

/// The Build door of `mesh1.admin.2`, a node-admin of the fabric that does not hold the fabric-primary
/// role (`mesh1.admin.1` holds it): its claim door carries its own birth's node id.
async fn door_not_fabric_primary(rig: &Arc<Rig>) -> Arc<BuildDoor> {
    let mut t = mn();
    t.nodes.push(node("mesh1.admin.2", false));
    let accepted = rdm_admin::accepted::AcceptedStore::seeded(&*rig.builds, t.fabric.id.clone(), rdm_admin::accepted::FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let me: PathName = "mesh1.admin.2".parse().unwrap();
    let control = Arc::new(ControlPlane::new(rig.builds.clone(), accepted.clone(), me.clone(), t, crate::adopted_time()));
    let runner: Arc<dyn OperationRunner> = Arc::new(RigRunner { rig: rig.clone(), join: Duration::from_millis(300) });
    let exec = Arc::new(BuildExecutor { executor: me.to_string(), builds: rig.builds.clone(), topology: control.topology.clone(), runner });
    let run = Arc::new(RunDoor { me: me.clone(), builds: rig.builds.clone(), exec, runs: rig.runs.clone(), local: rig.local.clone(), gate: rdm_admin::stop_gate::StopGate::new() });
    let node_id = control.topology.read().await.node(&me).expect("the admin is in the view").node_id.clone();
    let rows = Arc::new(rdm_admin::node_rows::NodeRows::new(me.clone(), node_id.clone(), control.topology.clone(), Arc::new(rdm_admin::storage::MemoryNodesStorage::default())));
    let _ = rig.sink.1.set(rows);
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: control.topology.clone(),
        builds: rig.builds.clone(),
        door: Arc::new(ClaimDoor {
            me: me.clone(),
            node_id,
            topology: control.topology.clone(),
            builds: rig.builds.clone(),
            contexts: Arc::new(AttemptContexts::in_memory()),
            claims: control.claims.clone(),
            claimed: Default::default(),
        }),
        dispatcher: Arc::new(Local(run.clone())),
        gate: Arc::new(OpenGate),
        departure: Arc::new(crate::loopback::Proofs::default()),
        verdicts: Arc::new(crate::loopback::NoVerdicts),
        notices: rig.told.notices(),
        turns: rdm_admin::stop_gate::StopGate::new(),
    });
    Arc::new(BuildDoor { me, control, drives: Arc::new(Drives::default()), env, run, primaries: rdm_transport::topology::PrimaryView::default(), incarnation: rdm_core::IncarnationId::mint() })
}

/// The fabric-primary's Build door over a Build log of its own: the executor writes its step
/// receipts on `rig`'s log, the fabric-primary's log holds the claim it decided and the verdict the
/// executor reported, and no step receipt reaches it (the Build topic delivers those apart, later).
async fn door_with_own_log(rig: &Arc<Rig>, fp: Arc<MemoryBuildStateAdapter>, join: Duration) -> Arc<BuildDoor> {
    // The executor's own log beside the fabric's shared Build storage, the fabric-primary's log (Q292).
    rig.local.persist_in(fp.clone());
    let t = mn();
    let accepted = rdm_admin::accepted::AcceptedStore::seeded(&*fp, t.fabric.id.clone(), rdm_admin::accepted::FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let control = Arc::new(ControlPlane::new(fp.clone(), accepted.clone(), me.clone(), t, crate::adopted_time()));
    let runner: Arc<dyn OperationRunner> = Arc::new(RigRunner { rig: rig.clone(), join });
    let exec = Arc::new(BuildExecutor { executor: me.to_string(), builds: rig.builds.clone(), topology: control.topology.clone(), runner });
    let run = Arc::new(RunDoor { me: me.clone(), builds: rig.builds.clone(), exec, runs: rig.runs.clone(), local: rig.local.clone(), gate: rdm_admin::stop_gate::StopGate::new() });
    let rows = Arc::new(rdm_admin::node_rows::NodeRows::new(me.clone(), control.topology.read().await.node(&me).expect("the fabric-primary is in the view").node_id.clone(), control.topology.clone(), Arc::new(rdm_admin::storage::MemoryNodesStorage::default())));
    let _ = rig.sink.1.set(rows);
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: control.topology.clone(),
        builds: fp.clone(),
        door: Arc::new(ClaimDoor {
            me: me.clone(),
            node_id: control
                .topology
                .read()
                .await
                .node(&me)
                .expect("the fabric-primary is in the view")
                .node_id
                .clone(),
            topology: control.topology.clone(),
            builds: fp.clone(),
            contexts: Arc::new(AttemptContexts::in_memory()),
            claims: control.claims.clone(),
            claimed: Default::default(),
        }),
        dispatcher: Arc::new(Local(run.clone())),
        gate: Arc::new(OpenGate),
        departure: Arc::new(crate::loopback::Proofs::default()),
        verdicts: Arc::new(rdm_admin::build_op::LocalVerdicts { local: fp }),
        notices: rig.told.notices(),
        turns: rdm_admin::stop_gate::StopGate::new(),
    });
    Arc::new(BuildDoor { me, control, drives: Arc::new(Drives::default()), env, run, primaries: rdm_transport::topology::PrimaryView::default(), incarnation: rdm_core::IncarnationId::mint() })
}

/// The one executor of the fixture is the fabric-primary itself.
struct Local(Arc<RunDoor>);

#[async_trait::async_trait]
impl Dispatcher for Local {
    async fn dispatch(&self, _turn: &rdm_admin::stop_gate::Turn, executor: &PathName, build_id: &BuildId, attempt: u32, context: rdm_core::context::CallContext, plan: rdm_admin::executor::RunPlan) -> Dispatched {
        local_dispatched(self.0.attempt_run(build_id, attempt, &executor.to_string(), &context, &plan).await)
    }
}

/// Runs the create operation through the pipeline over the fixture's fakes, as the executing admin does.
struct RigRunner {
    rig: Arc<Rig>,
    join: Duration,
}

#[async_trait::async_trait]
impl OperationRunner for RigRunner {
    async fn run(&self, build_id: &BuildId, _lane: Option<&str>, attempt: u32, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        match op {
            BuildOperation::CreateNode { node, over: None, .. } => self.rig.pipeline_create(build_id, attempt, &node.to_string(), self.join).await.map_err(Into::into),
            other => Err(format!("the fixture creates nodes only, not {other:?}").into()),
        }
    }
}

/// A node-RPC server on loopback that serves the Build family from `door`, and a carrier to it.
struct Served {
    router: Router,
    carrier: BuildCarrier,
    door: Arc<BuildDoor>,
}

async fn serve(door: Arc<BuildDoor>) -> Served {
    let (node_id, incarnation, key) = (NodeId::mint(), IncarnationId::mint(), SecretKey::generate());
    let slot: BuildSlot = Arc::new(OnceLock::new());
    let _ = slot.set(door.clone());
    let server = rdm_admin::build_op::serve(ServerBuilder::new(), slot)
        .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .unwrap();
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(ResolvedNode { node_id, name: "mesh1.admin.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation });
    let caller = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = Arc::new(NodeRpcClient::new(caller, resolver).with_caller_system("rdm"));
    Served { router, carrier: BuildCarrier::new(client, "mesh1.admin.1".parse().unwrap()), door }
}

/// `node.create` of mesh1's next rpc node, named on its executable (proposal
/// executable-bindings-at-create.md rule 1: the complete-topology request, the count expanded at the
/// door), over `nodes`, built from what `served`'s door reads.
async fn create_rpc(nodes: &Nodes, served: &Served) -> Result<WorkflowStream, CallEnd> {
    let rdm_admin::accepted::TopologyChange::Reconcile { based_on, nodes: entries, .. } = crate::named_request(&served.door.control, rdm_admin::accepted::TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }).await else {
        unreachable!("a create is named as the complete topology")
    };
    nodes.create_named(based_on, entries).await
}

/// The node objects over `carrier`; no cell of this file reads a view.
fn nodes_over(carrier: &BuildCarrier) -> Nodes {
    Nodes::new(carrier.clone())
}

struct Rig {
    builds: Arc<dyn BuildStateAdapter>,
    local: Arc<MemoryBuildStateAdapter>,
    runs: Arc<AttemptRuns>,
    joins: Arc<Joins>,
    topology_view: Arc<TopologyView>,
    sink: Arc<Published>,
    template: LaunchTemplate,
    data_root: std::path::PathBuf,
    /// The application the fabric-primary's drive sends its Build notices to.
    told: crate::Told,
}

impl Rig {
    fn new() -> Rig {
        let data_root = std::env::temp_dir().join(format!("node-objects-{}", NodeId::mint()));
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/node-objects".into(),
            seeds: vec![],
            launcher: rdm_core::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: Default::default(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let runs = Arc::new(AttemptRuns::default());
        let local = Arc::new(MemoryBuildStateAdapter::new());
        Rig { builds: Arc::new(FramedBuilds::new(local.clone(), runs.clone())), local, runs, joins: Arc::new(Joins::default()), topology_view: Arc::new(TopologyView { not_ready: Mutex::new(None), pending: Mutex::new(None) }), sink: Arc::default(), template, data_root, told: crate::Told::default() }
    }

    /// Run the create pipeline for `name` in `attempt` of `build`, as the executing admin does once
    /// the fabric-primary has claimed the attempt for it; `join` bounds the waits for the birth's
    /// report (`WaitForBind`, `WaitForMeshJoin`).
    async fn pipeline_create(&self, build: &BuildId, attempt: u32, name: &str, join: Duration) -> Result<(), String> {
        let pipeline = DeploymentPipeline {
            provider: &Up,
            joins: &self.joins,
            observer: &*self.topology_view,
            sink: &*self.sink,
            lifecycle: &NoLifecycleEvents,
            builds: &*self.builds,
            template: &self.template,
            timeouts: Timeouts { bind: join, join, ..Timeouts::default() },
        };
        let req = CreateRequest { build_id: build.clone(), attempt, node: name.parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None };
        pipeline.create(&req).await.map(|_| ()).map_err(|e| e.to_string())
    }

    /// The birth reports where it bound once the create has made its runtime fact available: the
    /// node's own `JoinNode`, the validated report the pipeline's `WaitForBind` and `WaitForMeshJoin`
    /// complete from (node-create.md step 5).
    async fn report_bind(&self, build: &BuildId, name: &str) {
        let op = format!("node.create:{name}");
        let name: rdm_admin::model::PathName = name.parse().unwrap();
        loop {
            // The executor's own log holds the Build once the call that carries it has arrived.
            let Ok(view) = self.builds.read_build(build).await else {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            };
            let done = |step: &str| view.steps.iter().find(|s| s.operation == op && s.step == step && s.outcome == rdm_admin::build_state::StepOutcome::Complete);
            if let (Some(identity), Some(storage), Some(_)) = (done("AllocateIdentity"), done("PrepareStorage"), done("MakeRuntimeFactAvailableToBirth")) {
                let id = identity.output.clone().unwrap();
                let node_id = NodeId::parse(id["node_id"].as_str().unwrap()).unwrap();
                let incarnation = IncarnationId::parse(id["incarnation"].as_str().unwrap()).unwrap();
                let endpoint_id = EndpointId(storage.output.clone().unwrap().as_str().unwrap().to_string());
                let data_dir = self.data_root.join(format!("{name}-{node_id}"));
                let runtime: RuntimeFact = RuntimeFact::read_record(&data_dir).expect("the create wrote the runtime record").unwrap();
                let digest = MeshDigest { held_hash: Default::default(),
                    fabric_id: FabricId::mint(),
                    node: MeshNode { node_id, name: name.clone(), endpoint_id, transport_addr: "127.0.0.1:34567".parse().unwrap(), incarnation, supersedes: None, runtime: Some(runtime.clone()) },
                    status: MemberStatus::Pending,
                    node_state: Default::default(),
                    admin_api_base: None,
                    digest_seq: 0,
                    emitted_at_rafka_ms: 0,
                    data_dir: Some(data_dir.display().to_string()),
                    mesh_id: None,
                    in_flight: None,
                    load: None,
                    gossip: None, is_primary: None, is_fabric_primary: None
                };
                self.joins.report(&rdm_admin::join::JoinReport { digest: digest.clone(), listeners: Vec::new() });
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
async fn collect_frames(stream: &mut WorkflowStream, within: Duration) -> (Vec<Frame>, Option<CallEnd>) {
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

const ATTEMPT_ONE_STEPS: [&str; 7] = [
    "AllocateIdentity",
    "PrepareStorage",
    "PrepareNetwork",
    "DeployRuntime",
    "RegisterExactRuntimeHandle",
    "ResolveProviderControlDomain",
    "MakeRuntimeFactAvailableToBirth",
];

/// CONTRACT (ops-naming acceptance 1, "A failure names its step"; node-create.md "Join completion
/// and failure evidence": only an accepted full report completes the bind/join waits; Q245, Q252: a
/// failed create is created again by the next attempt, once): a create whose process starts and whose
/// birth never reports where it bound (no `JoinNode` arrives) fails its bind wait, and the next
/// attempt's create fails it again; the stream ends `Started`, then `Failed { step: node.create,
/// reason }` from `WaitForBind`, within the deadline and with no Blocked frame before it.
/// `node.created`, `node.joined` and `node.started` are never emitted, the node never reaches
/// ready, and the call's span names the failed step.
#[test]
fn a_create_whose_birth_never_reports_names_the_bind_wait_and_emits_nothing_after_it() {
    let cap = capture();
    let (frames, end, published) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let served = serve(door(&rig, Duration::from_millis(300)).await).await;
        let nodes = nodes_over(&served.carrier);
        let mut stream = create_rpc(&nodes, &served).await.unwrap();
        let (frames, end) = tokio::time::timeout(Duration::from_secs(20), collect_frames(&mut stream, Duration::from_secs(15))).await.expect("the stream ends within 20 s");
        let published = rig.sink.0.lock().unwrap().clone();
        (frames, end, published)
    });
    let attempt = 1;
    let reason = match frames.last() {
        Some(Frame::Failed { step: NodeStep::Create, reason }) => reason.clone(),
        other => panic!("the stream ends Failed at node.create, not {other:?}; frames {frames:?}"),
    };
    assert!(reason.contains("did not report where it bound"), "the failure carries the step's own reason: {reason}");
    assert_eq!(
        frames[..frames.len() - 1],
        [Frame::Started { build_id: match &frames[0] { Frame::Started { build_id, .. } => build_id.clone(), f => panic!("{f:?}") }, attempt }]
    );
    assert!(end.is_none(), "a failed step ends the stream with its terminal frame, not a broken stream");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Event(NodeEvent::Created | NodeEvent::Joined | NodeEvent::Started) | Frame::Complete)), "nothing downstream of the failed step is emitted: {frames:?}");
    assert!(published.iter().all(|n| n.status != NodeStatus::Ready), "the node never reached ready");

    let spans = cap.spans();
    let call = spans_named(&spans, "rdm.node_admin.node.create.via-workflow");
    assert_eq!(call.len(), 1, "{spans:?}");
    assert_eq!(call[0]["attributes"]["outcome"], "failed");
    assert_eq!(call[0]["attributes"]["failed_step"], "node.create");
    for (never, span) in [("created", "rdm.node_admin.node.create.via-created-frame"), ("joined", "rdm.node_admin.node.update.via-joined-frame"), ("started", "rdm.node_admin.node.update.via-started-frame")] {
        assert!(spans_named(&spans, span).is_empty(), "node.{never} is never emitted");
    }
    let bind_step: Vec<&Value> = spans_named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|s| s["attributes"]["step"] == "WaitForBind").collect();
    let mut bind_attempts: Vec<&str> = bind_step.iter().map(|s| s["attributes"]["attempt"].as_str().unwrap_or("")).collect();
    bind_attempts.sort();
    assert_eq!(bind_attempts, ["1", "2"], "the pipeline ran the bind wait once in the first attempt and once in the next attempt's create");
    assert!(bind_step.iter().all(|s| s["attributes"]["outcome"] == "failed"), "and it failed there both times: {bind_step:?}");
    assert!(spans_named(&spans, "rdm.node_admin.deployment.update.via-step").iter().all(|s| s["attributes"]["step"] != "WaitForMeshJoin"), "nothing after the bind wait ran: {spans:?}");
    let prepare: Vec<&Value> = spans_named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|s| s["attributes"]["step"] == "PrepareStorage" && s["attributes"]["attempt"] == "1").collect();
    assert_eq!(prepare.len(), 1, "PrepareStorage ran once in the first attempt: {spans:?}");
    assert_eq!(prepare[0]["attributes"]["awaited"], "entry-spec,claim,transport-key", "PrepareStorage names every await it ran (a plain create reads its create spec first): {}", prepare[0]);
    assert_eq!(prepare[0]["attributes"]["awaiting"], "done", "PrepareStorage completed every await: {}", prepare[0]);
    let claim_row: Vec<&Value> = spans_named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().filter(|s| s["attributes"]["moment"] == "claim").collect();
    assert_eq!(claim_row.len(), 2, "each attempt's PrepareStorage opened the claim via-node-row span: {claim_row:?}");
    for claim in &claim_row {
        assert_eq!(claim["attributes"]["outcome"], "row-written", "the claim row was written: {claim}");
        assert_eq!(claim["attributes"]["awaited"], "writer-check,read-previous-row,read-old-row,put-node-row", "the claim span names every await of the claim: {claim}");
        assert_eq!(claim["attributes"]["awaiting"], "done", "the claim completed every await: {claim}");
    }
    // Each step's wall and, for one that ran blocking calls, their wait for a thread and their run.
    let walls: Vec<String> = spans_named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .map(|s| {
            let a = &s["attributes"];
            let io = a.get("io_calls").map(|_| format!(" io_calls={} io_queued_ms={} io_ms={}", a["io_calls"], a["io_queued_ms"], a["io_ms"])).unwrap_or_default();
            format!("{}={}ms{io}", a["step"].as_str().unwrap_or_default(), a.get("elapsed_ms").and_then(Value::as_str).unwrap_or("?"))
        })
        .collect();
    eprintln!("trace {}: rdm.node_admin.node.create.via-workflow outcome=failed failed_step=node.create; steps {}", call[0]["trace_id"], walls.join(", "));
}

/// CONTRACT: a create whose node has joined but is not ready (its Ready check names a blocker, as an
/// application's `pre_node_ready` does) streams one `Blocked { operation, step, reason }` frame
/// naming the step in flight and the check's own reason, however many times the check is polled with
/// that reason; the blocker is live progress, so the stream goes on, and when the node is ready it
/// emits `node.created` and `Complete`.
#[test]
fn a_create_whose_node_is_not_ready_streams_the_blocker_the_ready_check_names() {
    let cap = capture();
    let (blocked, after) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let why = "mesh1.rpc.3: pre_node_ready is blocked (attempt 1): pulling the accepting authority; it runs again on primary-ready";
        *rig.topology_view.not_ready.lock().unwrap() = Some(why.to_string());
        let served = serve(door(&rig, Duration::from_secs(30)).await).await;
        let nodes = nodes_over(&served.carrier);
        let mut stream = create_rpc(&nodes, &served).await.unwrap();
        let build = stream.accepted().build_id.clone();
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
        *rig.topology_view.not_ready.lock().unwrap() = None;
        let (after, end) = collect_frames(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "the stream ends with its terminal frame: {end:?}");
        (blocked.unwrap(), after)
    });
    assert_eq!(
        blocked,
        Frame::Blocked {
            operation: "node.create:mesh1.rpc.3".into(),
            step: "WaitForNodeReady".into(),
            reason: "mesh1.rpc.3: pre_node_ready is blocked (attempt 1): pulling the accepting authority; it runs again on primary-ready".into()
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
        *rig.topology_view.pending.lock().unwrap() = Some("mesh1.rpc.3 reports Pending".to_string());
        let served = serve(door(&rig, Duration::from_secs(30)).await).await;
        let nodes = nodes_over(&served.carrier);
        let mut stream = create_rpc(&nodes, &served).await.unwrap();
        let build = stream.accepted().build_id.clone();
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, "mesh1.rpc.3").await }
        });
        tokio::time::sleep(Duration::from_millis(450)).await;
        *rig.topology_view.pending.lock().unwrap() = None;
        collect_frames(&mut stream, Duration::from_secs(10)).await
    });
    assert!(end.is_none(), "the stream ends with its terminal frame: {end:?}");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Blocked { .. })), "a Pending node with no named blocker is not a Blocked frame: {frames:?}");
    assert_eq!(frames[frames.len() - 2..], [Frame::Event(NodeEvent::Created), Frame::Complete], "the ready node ends the stream: {frames:?}");
}

/// CONTRACT (ops-naming acceptance 2, "A broken stream is not a failure"): the transport between
/// the caller and the fabric-primary is cut while the create waits for its birth's report (the
/// `JoinNode` that completes `WaitForBind` and `WaitForMeshJoin`, node-create.md step 5). The
/// caller's stream ends `Indeterminate`, never `Failed`. The cut cancels nothing: `build.get [id]` over a
/// new transport returns the receipts of exactly the steps that completed, `Resume` names the bind
/// wait as the first step without a `Complete` receipt, and the re-submit by the Build id streams
/// the same Build (never a second one) to its end once the birth is heard: no completed step runs
/// again, each still has one receipt, and the steps' spans say the attempt ran them once.
#[test]
fn a_cut_stream_is_indeterminate_and_the_resume_reruns_no_completed_step() {
    let cap = capture();
    let (frames, end, receipts_after_cut, resume, receipts_after, final_frames, disposition) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let door = door(&rig, Duration::from_secs(60)).await;
        let served = serve(door.clone()).await;
        let nodes = nodes_over(&served.carrier);
        let mut stream = create_rpc(&nodes, &served).await.unwrap();
        let build = stream.accepted().build_id.clone();
        let accepted = stream.accepted().clone();
        let mut frames = Vec::new();
        // The cut: the caller's wire is severed while the create waits for the birth's report, which
        // is not heard yet. The wait is where it is cut once the step before it has its receipt.
        loop {
            let view = rig.builds.read_build(&build).await.unwrap();
            if view.steps.iter().any(|s| s.step == "MakeRuntimeFactAvailableToBirth") {
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
        let receipts_after_cut = builds.get(&rdm_client::Filter::new().eq("id", [accepted.build_id.to_string()])).await.unwrap().remove(0);
        let resume = Resume::from_receipts(&WorkflowKind::Create, &receipts_after_cut, accepted.attempt).unwrap();
        // The birth is heard this time: the Build the cut did not cancel goes on to its end, and the
        // re-submit streams it.
        let mut resumed = nodes_over(&again.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let disposition = resumed.disposition();
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, "mesh1.rpc.3").await }
        });
        let (final_frames, resumed_end) = collect_frames(&mut resumed, Duration::from_secs(10)).await;
        assert!(resumed_end.is_none(), "the resumed stream ends with its terminal frame: {resumed_end:?}");
        let receipts_after = builds.get(&rdm_client::Filter::new().eq("id", [accepted.build_id.to_string()])).await.unwrap().remove(0);
        (frames, end, receipts_after_cut, resume, receipts_after, final_frames, disposition)
    });
    assert!(matches!(end, CallEnd::Indeterminate { .. }), "a cut stream is Indeterminate, not {end:?}");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Failed { .. } | Frame::Complete)), "no step is inferred failed or complete from a broken stream: {frames:?}");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Event(NodeEvent::Joined | NodeEvent::Started | NodeEvent::Created))), "no event past the join was delivered: {frames:?}");

    let completed_steps = |r: &rdm_client::Build, attempt: u32| -> Vec<String> {
        r.steps.iter().filter(|s| s.attempt == attempt && s.result == rdm_core::build::StepResult::Complete).map(|s| s.step.clone()).collect()
    };
    assert_eq!(receipts_after_cut.steps.len(), ATTEMPT_ONE_STEPS.len(), "build.get returns exactly the receipts of the steps that completed: {:?}", receipts_after_cut.steps);
    assert_eq!(completed_steps(&receipts_after_cut, 1), ATTEMPT_ONE_STEPS);
    assert_eq!(resume.from, Some(("node.create".to_string(), "WaitForBind".to_string())), "the resume starts at the first step without a Complete receipt");
    assert_eq!(resume.completed.len(), ATTEMPT_ONE_STEPS.len());
    assert_eq!(disposition, rdm_core::build::Disposition::Attached, "the re-submit attached to the Build the cut left running");

    for step in ATTEMPT_ONE_STEPS {
        assert_eq!(receipts_after.steps.iter().filter(|s| s.step == step).count(), 1, "{step} was not run again: one receipt, from attempt 1");
    }
    assert_eq!(receipts_after.attempt, 1, "the cut opened no second attempt");
    assert_eq!(completed_steps(&receipts_after, 1).len(), ATTEMPT_ONE_STEPS.len() + 6, "the same attempt went on through the bind wait and the join to its end");
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
        let mut stream = create_rpc(&nodes, &served).await.unwrap();
        let build = stream.accepted().build_id.clone();
        let accepted = stream.accepted().clone();
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, "mesh1.rpc.3").await }
        });
        let (first, end) = collect_frames(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        assert!(fp.facts().await.unwrap().iter().all(|f| !matches!(f, rdm_admin::build_state::BuildFact::Step(_))), "the fabric-primary's log holds no step receipt");
        let mut resumed = nodes_over(&served.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let disposition = resumed.disposition();
        let (again, end) = collect_frames(&mut resumed, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        (first, again, disposition)
    });
    assert_eq!(first[1..], [Frame::Event(NodeEvent::Joined), Frame::Event(NodeEvent::Started), Frame::Event(NodeEvent::Created), Frame::Complete], "{first:?}");
    assert_eq!(disposition, rdm_core::build::Disposition::AlreadyApplied);
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
        let mut stream = create_rpc(&nodes, &served).await.unwrap();
        let build = stream.accepted().build_id.clone();
        let accepted = stream.accepted().clone();
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, "mesh1.rpc.3").await }
        });
        let (first, end) = collect_frames(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        let completed = door.drives.get(&build).expect("the drive that completed the Build is held");
        assert!(completed.completed(), "the drive ended Complete");
        // The reconcile loop's `ensure_active`, acting on a `list_active` read taken before the
        // verdict entered the log.
        let ensured = door.drives.ensure(&door.env, &build, accepted.attempt);
        let kept = Arc::ptr_eq(&ensured, &completed) && door.drives.get(&build).is_some_and(|d| Arc::ptr_eq(&d, &completed));
        let mut resumed = nodes_over(&served.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let disposition = resumed.disposition();
        let (again, end) = collect_frames(&mut resumed, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        (build, kept, first, again, disposition)
    });
    assert!(kept, "the reconcile tick is handed the completed drive, never a new one in its place");
    assert_eq!(disposition, rdm_core::build::Disposition::AlreadyApplied);
    assert_eq!(again[1..], first[1..], "every event again, from the drive's own record: {again:?}");
    let spans = cap.spans();
    let dispatches: Vec<_> = spans_named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().filter(|s| s["attributes"]["build_id"] == build.log().to_string().as_str()).collect();
    assert_eq!(dispatches.len(), 1, "one build.attempt.run, from the drive that made the calls: {dispatches:?}");
    eprintln!("span rdm.node_admin.build.update.via-dispatch {}", dispatches[0]);
}

/// A Build log read by a reconcile tick that took its current-Build read before the Build's latest
/// verdict landed: `current_build` answers the pointer as read then, and every other read is the log
/// as it is now.
struct ReadBeforeTheVerdict {
    inner: Arc<dyn BuildStateAdapter>,
    current: rdm_admin::build_state::CurrentBuild,
}

#[async_trait::async_trait]
impl BuildStateAdapter for ReadBeforeTheVerdict {
    async fn publish_accepted(&self, a: &rdm_admin::build_state::BuildAccepted) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.inner.publish_accepted(a).await
    }
    async fn open_attempt(&self, o: &rdm_admin::build_state::AttemptOpened) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.inner.open_attempt(o).await
    }
    async fn read_build(&self, id: &BuildId) -> Result<rdm_admin::build_state::BuildProjection, rdm_admin::build_state::BuildStateError> {
        self.inner.read_build(id).await
    }
    async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, rdm_admin::build_state::BuildStateError> {
        Ok(self.current.clone())
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, rdm_admin::build_state::BuildStateError> {
        self.inner.newest_accepted_at_ms().await
    }
    async fn list_active(&self) -> Result<Vec<rdm_admin::build_state::BuildProjection>, rdm_admin::build_state::BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, c: &rdm_admin::build_state::BuildAttemptClaim) -> Result<rdm_admin::build_state::ClaimOutcome, rdm_admin::build_state::BuildStateError> {
        self.inner.claim_attempt(c).await
    }
    async fn adopt_claim(&self, c: &rdm_admin::build_state::BuildAttemptClaim) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.inner.adopt_claim(c).await
    }
    async fn append_step_receipt(&self, r: &rdm_admin::build_state::BuildStepReceipt) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.inner.append_step_receipt(r).await
    }
    async fn append_attempt_receipt(&self, r: &rdm_admin::build_state::BuildAttemptReceipt) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.inner.append_attempt_receipt(r).await
    }
    async fn facts(&self) -> Result<Vec<rdm_admin::build_state::BuildFact>, rdm_admin::build_state::BuildStateError> {
        self.inner.facts().await
    }
    async fn committed_facts(&self, build: Option<&rdm_admin::build::BuildId>) -> Result<Vec<rdm_admin::build_state::BuildFact>, rdm_admin::build_state::BuildStateError> {
        self.inner.committed_facts(build).await
    }
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, rdm_admin::build_state::BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, rdm_admin::build_state::BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<rdm_admin::build_state::OwedExit>, rdm_admin::build_state::BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
    }
    async fn forget(&self, id: &BuildId) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.inner.forget(id).await
    }
}

/// CONTRACT (R-BI3, i143-rulings-2026-10-12-rx-authz.md: "A failed Build is never current, and nothing
/// resumes it on its own; a caller resubmits it"): the reconcile loop's tick (`ensure_active`) reads the
/// current Build's id, then the Build. When the Build's attempt failed between the two reads (a
/// stop that was never admitted), the Build it read is Failed and the tick drives nothing:
/// it makes no drive and returns none, so no next attempt is claimed, and it says so on
/// `rdm.node_admin.build.reject.via-not-in-progress-when-read` naming the Build, its attempt and its
/// state. Must NOT happen: a drive of the failed Build's next attempt from the tick (the land gate on
/// 7b2310bb, node_restart.rs:212: attempt 3 claimed 2 ms after attempt 2 failed, by a drive with no
/// parent span, the reconcile loop's).
#[test]
fn a_reconcile_tick_whose_build_failed_between_its_reads_drives_and_claims_nothing() {
    use rdm_admin::build_state::{AttemptOpened, AttemptOutcome, AttemptReason, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildState, CurrentBuild};
    let cap = capture();
    let (b0, read, ensured, held, claims) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let door = door(&rig, Duration::from_secs(30)).await;
        let b0 = rig.builds.current_build().await.unwrap().into_build_id().unwrap();
        let claim = |attempt: u32| BuildAttemptClaim { claimant: None, mesh: None, build_id: b0.clone(), attempt, executor: "mesh1.admin.1".into() };
        rig.builds.claim_attempt(&claim(1)).await.unwrap();
        rig.builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: b0.clone(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();
        // A stop reopens the completed Build; its attempt is claimed and running.
        rig.builds.open_attempt(&AttemptOpened { build_id: b0.clone(), attempt: 2, reason: AttemptReason::Stop, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 1, mesh: None, context: None }).await.unwrap();
        rig.builds.claim_attempt(&claim(2)).await.unwrap();
        // The tick's current-Build read, taken now; the attempt's verdict lands before its Build read.
        let before = rig.builds.current_build().await.unwrap();
        assert_eq!(before, CurrentBuild::InProgress(b0.clone()), "the stop's attempt is in progress when the tick reads the current Build");
        rig.builds
            .append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: b0.clone(), attempt: 2, outcome: AttemptOutcome::Failed { reason: "mesh1.rpc.2: stop-node was not admitted".into(), code: rdm_core::outcome::Code::DeadlineExceeded } })
            .await
            .unwrap();
        let read = rig.builds.read_build(&b0).await.unwrap();
        let e = &door.env;
        let env = Arc::new(DriveEnv { me: e.me.clone(), topology: e.topology.clone(), builds: Arc::new(ReadBeforeTheVerdict { inner: rig.builds.clone(), current: before }), door: e.door.clone(), dispatcher: e.dispatcher.clone(), gate: e.gate.clone(), departure: e.departure.clone(), verdicts: e.verdicts.clone(), notices: e.notices.clone(), turns: e.turns.clone() });
        let drives = Arc::new(Drives::default());
        let ensured = rdm_admin::build_op::ensure_active(&drives, &env).await.len();
        let held = drives.get(&b0).is_some();
        let claims: Vec<u32> = rig.builds.facts().await.unwrap().into_iter().filter_map(|f| match f {
            BuildFact::Claim(c) if c.build_id == b0 => Some(c.attempt),
            _ => None,
        }).collect();
        assert_eq!((read.state, read.attempt), (BuildState::Failed, 2), "the tick's Build read is the failed attempt: {read:?}");
        (b0, read, ensured, held, claims)
    });
    assert_eq!(ensured, 0, "the tick drives nothing of the Build it read failed ({:?} at attempt {})", read.state, read.attempt);
    assert!(!held, "no drive of {b0} is made");
    assert_eq!(claims, vec![1, 2], "no attempt after the failed one is claimed");
    let spans = cap.spans();
    let told = spans_named(&spans, "rdm.node_admin.build.reject.via-not-in-progress-when-read");
    assert!(told.iter().any(|s| s["attributes"]["build_id"] == b0.log().to_string().as_str() && s["attributes"]["attempt"] == "2" && s["attributes"]["state"] == "Failed"), "the tick names the Build it read failed: {told:?}");
    eprintln!("span rdm.node_admin.build.reject.via-not-in-progress-when-read {}", told[0]);
}

/// CONTRACT (node-recovery.md "Numbered end-to-end sequence" step 1: "A repeat submit answers
/// AlreadyApplied or BuildInProgress, never a second recovery"): over op `0x20`, a recovery submit
/// for an exited birth already recovered answers `Started { AlreadyApplied }` naming the Build and
/// attempt whose recovery (its delete, then its create) recovered it, then the terminal `Complete` of that attempt
/// and nothing else. Must NOT happen: a drive made or started for the Build (a drive of an ended
/// Build claims its next attempt), or an attempt opened.
#[test]
fn a_recovery_submit_for_a_birth_already_recovered_answers_already_applied_on_the_wire_and_drives_nothing() {
    use rdm_admin::build_state::{AttemptOutcome, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStepReceipt, StepOutcome};
    let cap = capture();
    let (opened_before, opened_after, b0, recovering, stream, frames, drive) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let door = door(&rig, Duration::from_secs(30)).await;
        let cp = door.control.clone();
        let path: PathName = "mesh1.rpc.2".parse().unwrap();
        let exited = cp.topology.read().await.node(&path).unwrap().clone();
        let (node_id, birth) = (exited.node_id.clone(), exited.incarnation_id.clone().unwrap());
        let b0 = cp.builds.current_build().await.unwrap().into_build_id().unwrap();
        let settle = |attempt: u32| {
            let (builds, b0) = (rig.builds.clone(), b0.clone());
            async move {
                builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: b0.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
                builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: b0, attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
            }
        };
        settle(1).await;
        let recovering = match cp.open_recovery("build.create", path.clone(), node_id.clone(), birth.clone()).await {
            Ok(rdm_admin::control_plane::Recovery::Opened(o)) => o,
            other => panic!("the first recovery is admitted: {other:?}"),
        };
        // The recovery's receipts as its two workflows write them, in the lane its door opened it in
        // (R-BI7: the member's mesh's lane): the delete's NodeDeleted names the exited birth, and
        // each operation's Complete commits no output; the create is the last.
        let deleted = rdm_core::LifecycleOp { build_id: b0.to_string(), attempt: recovering.attempt, operation: format!("node.delete:{path}"), node_id: node_id.clone(), incarnation: birth.clone(), name: path.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        for (operation, step, output) in [("node.delete", "NodeDeleted", Some(serde_json::to_value(&deleted).unwrap())), ("node.delete", "Complete", None), ("node.create", "Complete", None)] {
            rig.builds
                .append_step_receipt(&BuildStepReceipt { mesh: recovering.mesh.clone(), build_id: b0.clone(), attempt: recovering.attempt, operation: format!("{operation}:{path}"), step: step.into(), outcome: StepOutcome::Complete, output, executor: Some("mesh1.admin.1".into()) })
                .await
                .unwrap();
        }
        // The recovery's verdict in its own lane: its door opened and claimed it there (R-BI7).
        rig.builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: recovering.mesh.clone(), build_id: b0.clone(), attempt: recovering.attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
        let opened = |facts: Vec<BuildFact>| facts.iter().filter(|f| matches!(f, BuildFact::Opened(_))).count();
        let opened_before = opened(rig.builds.facts().await.unwrap());
        let served = serve(door.clone()).await;
        let mut stream = served.carrier.create(rdm_core::build::BuildChange::Recover { node: path.clone(), node_id: node_id.clone(), incarnation: birth.clone() }).await.expect("the repeat is answered with a stream");
        let head = (stream.build_id().to_string(), stream.attempt(), stream.disposition());
        let mut frames = Vec::new();
        while let Some(f) = tokio::time::timeout(Duration::from_secs(5), stream.next()).await.expect("the answer ends") {
            frames.push(f);
        }
        let opened_after = opened(rig.builds.facts().await.unwrap());
        let drive = door.drives.get(&b0).is_some();
        (opened_before, opened_after, b0, recovering, head, frames, drive)
    });
    assert_eq!(stream, (b0.to_string(), recovering.attempt, rdm_core::build::Disposition::AlreadyApplied), "Started names the Build and attempt that recovered the birth");
    assert_eq!(frames.len(), 1, "one terminal and nothing else: {frames:?}");
    assert!(matches!(&frames[0], Ok(rdm_client::BuildFrame::Complete { attempt }) if *attempt == recovering.attempt), "{frames:?}");
    assert_eq!(opened_after, opened_before, "no attempt is opened");
    assert!(!drive, "no drive is made for the Build");
    let spans = cap.spans();
    let applied = spans_named(&spans, "rdm.node_admin.build.reject.via-recovery-already-applied");
    assert_eq!(applied.len(), 1, "the repeat is answered from the receipt that recovered the birth");
    eprintln!("span rdm.node_admin.build.reject.via-recovery-already-applied {}", applied[0]);
}

/// CONTRACT (R-ON8): a fabric-primary that took the role after a Build ended holds no frame of it.
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
        let mut stream = create_rpc(&nodes_over(&served.carrier), &served).await.unwrap();
        let build = stream.accepted().build_id.clone();
        let accepted = stream.accepted().clone();
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, "mesh1.rpc.3").await }
        });
        let (first, end) = collect_frames(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        // The same Build log and executor under a fabric-primary that never drove this Build.
        let after = Arc::new(BuildDoor { me: before.me.clone(), control: before.control.clone(), drives: Arc::new(Drives::default()), env: before.env.clone(), run: before.run.clone(), primaries: before.primaries.clone(), incarnation: before.incarnation.clone() });
        let successor = serve(after).await;
        let mut resumed = nodes_over(&successor.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let disposition = resumed.disposition();
        let (again, end) = collect_frames(&mut resumed, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        (first, again, disposition)
    });
    assert_eq!(disposition, rdm_core::build::Disposition::AlreadyApplied);
    assert_eq!(again[1..], first[1..], "every event again, replayed by the executor of the last attempt: {again:?}");
}

/// CONTRACT (fabric-node-hooks.md `on_build_started`, `on_build_ended`, Q256: "Each time a Build's first
/// attempt is claimed ... the fabric-primary sends the application one `on_build_started` notice";
/// "Each `on_build_started` is followed by one `on_build_ended` for the same opening"; "a successor that
/// adopts the state calls nothing"; "a fabric-primary lost before it sends it leaves none, and `build.get`
/// is the durable read"): a create Build sends the application `on_build_started` at the claim of its
/// first attempt (reason `requested`), then one `on_build_ended` once its `Complete` is recorded, naming
/// the attempt that completed it, each under its own span (outcome `sent`). An admin that became
/// fabric-primary after the Build ended adopts it `Complete`, sends nothing, and the Build reads `Complete`; a
/// re-submit that replays the ended Build, or a reconcile tick that asks for its drive again, sends no
/// second notice. Must NOT happen: an ending the application is never told of, an ending told before its
/// opening, or a notice for a replayed or re-read terminal, or from the fabric-primary that adopted it.
#[test]
fn a_build_sends_started_at_its_first_claim_then_ended_and_a_successor_that_adopts_it_sends_none() {
    let cap = capture();
    let (build, attempt, after_complete, after_adoption, disposition, state) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let fp = Arc::new(MemoryBuildStateAdapter::new());
        let before = door_with_own_log(&rig, fp.clone(), Duration::from_secs(30)).await;
        let served = serve(before.clone()).await;
        let mut stream = create_rpc(&nodes_over(&served.carrier), &served).await.unwrap();
        let build = stream.accepted().build_id.clone();
        let accepted = stream.accepted().clone();
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, "mesh1.rpc.3").await }
        });
        let (frames, end) = collect_frames(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        assert_eq!(frames.last(), Some(&Frame::Complete), "the create's Build completed: {frames:?}");
        let attempt = fp.read_build(&build).await.unwrap().attempt;
        let after_complete = rig.told.settled(2).await;
        // A re-submit after the end replays the drive's own record, and the reconcile loop's tick asks
        // for the drive again: neither is a second transition.
        let mut replayed = nodes_over(&served.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let (_, end) = collect_frames(&mut replayed, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        let _ = before.drives.ensure(&before.env, &build, accepted.attempt);
        let after_replay = rig.told.settled(2).await;
        assert_eq!(after_replay.len(), 2, "a replayed or re-read terminal sends no second notice: {after_replay:?}");
        // The same Build log under a fabric-primary that never drove this Build: it adopts it Complete.
        let after = Arc::new(BuildDoor { me: before.me.clone(), control: before.control.clone(), drives: Arc::new(Drives::default()), env: before.env.clone(), run: before.run.clone(), primaries: before.primaries.clone(), incarnation: before.incarnation.clone() });
        let successor = serve(after).await;
        let mut resumed = nodes_over(&successor.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let disposition = resumed.disposition();
        let (_, end) = collect_frames(&mut resumed, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        let after_adoption = rig.told.settled(2).await;
        let state = fp.read_build(&build).await.unwrap().state;
        (build, attempt, after_complete, after_adoption, disposition, state)
    });
    let id = build.to_string();
    let keys: Vec<(String, u32, &str)> = after_complete.iter().map(crate::Notice::key).collect();
    assert_eq!(keys, vec![(id.clone(), 1, "started"), (id.clone(), attempt, "complete")], "started at the first claim, then ended Complete: {after_complete:?}");
    let crate::Notice::Started(started) = &after_complete[0] else { unreachable!() };
    assert_eq!(started.reason, rdm_admin::build_state::AttemptReason::Requested, "the first attempt's reason: {started:?}");
    let crate::Notice::Ended(ended) = &after_complete[1] else { unreachable!() };
    assert_eq!((ended.reason, &ended.state), (rdm_admin::build_state::AttemptReason::Requested, &rdm_admin::wiring::BuildEndedState::Complete), "{ended:?}");
    assert_eq!(disposition, rdm_core::build::Disposition::AlreadyApplied, "the successor adopted the ended Build");
    assert_eq!(after_adoption.len(), 2, "the successor that adopted the ended Build sends nothing: {after_adoption:?}");
    assert_eq!(state, rdm_admin::build_state::BuildState::Complete, "build.get reads the terminal");
    let spans = cap.spans();
    let logged = build.log().to_string();
    for (name, want) in [("rdm.node_admin.build.update.via-on-build-started", ("1", "requested", None)), ("rdm.node_admin.build.update.via-on-build-ended", (&*attempt.to_string(), "requested", Some("complete")))] {
        let sent: Vec<&Value> = spans_named(&spans, name).into_iter().filter(|s| s["attributes"]["build_id"] == logged.as_str()).collect();
        assert_eq!(sent.len(), 1, "one {name} span for the Build: {sent:?}");
        let a = &sent[0]["attributes"];
        assert_eq!((a["outcome"].as_str(), a["attempt"].as_str(), a["reason"].as_str()), (Some("sent"), Some(want.0), Some(want.1)), "{}", sent[0]);
        if let Some(state) = want.2 {
            assert_eq!(a["state"].as_str(), Some(state), "{}", sent[0]);
        }
        eprintln!("span {name} {}", sent[0]);
    }
}

/// CONTRACT (fabric-node-hooks.md `on_build_started`, `on_build_ended`, Q256: "An attempt that continues a
/// Build in progress (a takeover, or the next attempt after a hand-back) sends none"; "A Build that ends
/// `Failed` ... `Failed { step, reason, code }` as its caller reads it on the Build stream"): a create
/// whose birth never reports where it bound hands its first attempt back and its next attempt fails the
/// Build. The application is told `on_build_started` once, at attempt 1, and one `on_build_ended`
/// `Failed` at attempt 2, naming the step and the reason the stream ends with. Must NOT happen: a second
/// `on_build_started` for the attempt after the hand-back, or a Failed ending the application is not told of.
#[test]
fn a_build_that_hands_back_and_fails_sends_one_started_and_one_failed_ended_naming_the_step() {
    let cap = capture();
    let (frames, told) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let served = serve(door(&rig, Duration::from_millis(300)).await).await;
        let mut stream = create_rpc(&nodes_over(&served.carrier), &served).await.unwrap();
        let (frames, end) = tokio::time::timeout(Duration::from_secs(20), collect_frames(&mut stream, Duration::from_secs(15))).await.expect("the stream ends within 20 s");
        assert!(end.is_none(), "{end:?}");
        (frames, rig.told.settled(2).await)
    });
    let Some(Frame::Failed { reason, .. }) = frames.last() else { panic!("the create's Build ended Failed: {frames:?}") };
    let id = match &frames[0] {
        Frame::Started { build_id, .. } => build_id.to_string(),
        f => panic!("{f:?}"),
    };
    let keys: Vec<(String, u32, &str)> = told.iter().map(crate::Notice::key).collect();
    assert_eq!(keys, vec![(id.clone(), 1, "started"), (id.clone(), 2, "failed")], "one started at attempt 1, none at the attempt after the hand-back, one Failed ending: {told:?}");
    let crate::Notice::Ended(ended) = &told[1] else { unreachable!() };
    match &ended.state {
        rdm_admin::wiring::BuildEndedState::Failed { step, reason: why, .. } => {
            assert!(!step.is_empty(), "the Failed ending names its step: {ended:?}");
            assert_eq!(why, reason, "the Failed ending's reason is the one the stream ends with: {ended:?}");
        }
        other => panic!("the ending is Failed: {other:?}"),
    }
    let spans = cap.spans();
    let ended_spans = spans_named(&spans, "rdm.node_admin.build.update.via-on-build-ended");
    assert_eq!(ended_spans.len(), 1, "{ended_spans:?}");
    assert_eq!((ended_spans[0]["attributes"]["state"].as_str(), ended_spans[0]["attributes"]["attempt"].as_str()), (Some("failed"), Some("2")), "{}", ended_spans[0]);
    assert_eq!(spans_named(&spans, "rdm.node_admin.build.update.via-on-build-started").len(), 1, "one started span");
    eprintln!("span rdm.node_admin.build.update.via-on-build-ended {}", ended_spans[0]);
}

/// CONTRACT: a `node.name.update` whose name is outside the name grammar (proposal
/// node-properties-own-commands.md §9 A) is refused `RejectedName` by name before anything is sent,
/// as are tags that are not a node's tag set (R-TG1) and a read's filter its entity's schema refuses
/// (Q167 R-RD2: an empty values list, a tag value that is not a label, a property the row has not). A `node.connection.delete` naming no connection is refused by name before
/// anything is sent (R-CR3). Calls whose request no op carries say so by name.
#[test]
fn calls_no_op_carries_today_say_so_by_name() {
    let cap = capture();
    cap.run(async {
        let rig = Arc::new(Rig::new());
        let served = serve(door(&rig, Duration::from_secs(1)).await).await;
        let nodes = nodes_over(&served.carrier);
        let node_id = NodeId::mint();
        let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(node_id.clone()), node_id, incarnation: IncarnationId::mint() };
        // The rename reaches the node's mesh-primary only with a name within the grammar.
        let (mp, route) = (NodeId::mint(), rdm_transport::RouteChoice::Direct);
        for (bad, says) in [("Rpc", "\"Rpc\""), (".old", "starts with '.'"), ("", "empty")] {
            let refused = nodes.name_update(&birth, &mp, &route, bad).await.expect("a typed refusal");
            assert!(matches!(&refused, rdm_core::status::StatusReply::RejectedName { reason } if reason.contains(says)), "{bad:?}: {refused:?}");
        }
        // Labels that are not a node's tag set, and a filter its entity's schema refuses (Q167), are refused here.
        let not_a_label = nodes.tags_create(&birth, &mp, &route, &["Rabbit".to_string()].into()).await.expect("a typed refusal");
        assert!(matches!(&not_a_label, rdm_core::status::StatusReply::RejectedTags { reason } if reason.contains("\"Rabbit\"")), "{not_a_label:?}");
        let filter = rdm_client::Filter::new;
        for (filter, says) in [(filter().eq("tags", Vec::<String>::new()), "(tags, []) names no value"), (filter().eq("tags", ["a/b"]), "(tags, [a/b])"), (filter().eq("zone", ["eu-1"]), "(zone, [eu-1]) names no property")] {
            match rdm_client::Nodes::new(served.carrier.clone()).get(&filter).await {
                Err(CallEnd::Rejected { reason, detail }) => assert!(reason == "rejected-filter" && detail.contains(says), "node.get {filter:?}: {reason}: {detail}"),
                other => panic!("{filter:?} is refused by name before anything is sent, not {other:?}"),
            }
        }
        match rdm_client::Meshes::new(served.carrier.clone()).get(&filter().eq("tags", ["red"])).await {
            Err(CallEnd::Rejected { reason, detail }) => assert!(reason == "rejected-filter" && detail.contains("(tags, [red]) names no property of a mesh row"), "mesh.get: {reason}: {detail}"),
            other => panic!("a mesh pair on a node property is refused by name before anything is sent, not {other:?}"),
        }
        // node.connection.delete naming no connection is refused by name, and nothing is sent.
        assert!(
            matches!(nodes.connections_delete(&birth, &mp, &route, &rdm_core::status::ConnectionScope::Connections(vec![])).await, Ok(rdm_core::status::StatusReply::RejectedConnectionScope { reason }) if reason.contains("at least one")),
            "an empty Connections list is refused by name"
        );
        let end = nodes.config_get();
        assert!(matches!(end, CallEnd::NotBackedToday { op: NodeOp::ConfigGet, .. }), "{end:?}");
    });
}

/// CONTRACT: every step the create and delete pipelines record is one the client names. The
/// receipts of a whole create fold to `node.joined`, `node.started`, `node.created`
/// and `Complete`; the receipts of a whole delete fold to the connections-deleted, stopped and
/// deleted events and `Complete`. A step the client does not name is refused by name, never skipped.
#[test]
fn every_pipeline_step_is_named_by_the_client() {
    use rdm_admin::deployment::pipeline::{CreateStep, DeleteStep};
    let view = |op: &str, steps: Vec<&str>| -> rdm_client::BuildView {
        serde_json::from_value(json!({
            "build_id": "3qcfk1kq47m9", "topology": {}, "submitted_at_ms": 0, "state": "running", "attempt": 1, "executor": null,
            "steps": steps.iter().map(|s| json!({"attempt": 1, "operation": op, "step": s, "outcome": "complete"})).collect::<Vec<_>>(),
            "last_failure": null, "reason": "requested"
        }))
        .unwrap()
    };
    let create: Vec<&str> = CreateStep::ORDER.iter().map(|s| s.name()).collect();
    let f = rdm_client::fold(&WorkflowKind::Create, &view("node.create:mesh1.rpc.3", create), 1).unwrap();
    assert_eq!(f.frames, [Frame::Event(NodeEvent::Joined), Frame::Event(NodeEvent::Started), Frame::Event(NodeEvent::Created), Frame::Complete]);
    let name: rdm_core::PathName = "mesh1.rpc.3".parse().unwrap();
    let delete: Vec<&str> = DeleteStep::ORDER.iter().map(|s| s.name()).collect();
    let f = rdm_client::fold(&WorkflowKind::Delete(name.clone()), &view("node.delete:mesh1.rpc.3", delete.clone()), 1).unwrap();
    assert_eq!(
        f.frames,
        [Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Event(NodeEvent::Stopped), Frame::Event(NodeEvent::Deleted), Frame::Complete]
    );
    // A node-admin's workflow is any node's: its roles move by node.primary.update before the operation
    // is dispatched, never as a step of it (the approved proposal node-primary-update-fabric-mesh-and-
    // node-type.md, Part D: the SeatTransfer step goes), so its receipts fold to the same events.
    let admin: rdm_core::PathName = "mesh1.admin.2".parse().unwrap();
    let f = rdm_client::fold(&WorkflowKind::Delete(admin.clone()), &view("node.delete:mesh1.admin.2", delete.clone()), 1).unwrap();
    assert_eq!(
        f.frames,
        [Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Event(NodeEvent::Stopped), Frame::Event(NodeEvent::Deleted), Frame::Complete]
    );
    let f = rdm_client::fold(&WorkflowKind::Stop(admin.clone()), &view("node.stop:mesh1.admin.2", vec!["StopNode", "AwaitNodeLeft", "Complete"]), 1).unwrap();
    assert_eq!(f.frames, [Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Event(NodeEvent::Stopped), Frame::Complete]);
    let f = rdm_client::fold(&WorkflowKind::Stop(name.clone()), &view("node.stop:mesh1.rpc.3", vec!["StopNode", "AwaitNodeLeft", "Complete"]), 1).unwrap();
    assert_eq!(f.frames, [Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Event(NodeEvent::Stopped), Frame::Complete]);
    // A standalone node.connection.delete { All } ends on the birth's node.connections.deleted: the
    // process is not stopped and nothing is deleted.
    let f = rdm_client::fold(&WorkflowKind::DeleteConnections(name.clone()), &view("node.connection.delete:mesh1.rpc.3", vec!["DeleteConnections", "Complete"]), 1).unwrap();
    assert_eq!(f.frames, [Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Complete]);
    // A node-admin's delete with only NodeDeleting held resumes at StopNode, as any node's does.
    let r = rdm_client::Resume::from_view(&WorkflowKind::Delete(admin.clone()), &view("node.delete:mesh1.admin.2", vec!["NodeDeleting"]), 1).unwrap();
    assert_eq!(r.from, Some(("node.delete".to_string(), "StopNode".to_string())));
    // A SeatTransfer receipt is a deleted step: refused by name under any target, never folded.
    for (kind, op, steps) in [
        (WorkflowKind::Delete(name.clone()), "node.delete:mesh1.rpc.3", vec!["NodeDeleting", "SeatTransfer"]),
        (WorkflowKind::Stop(name.clone()), "node.stop:mesh1.rpc.3", vec!["SeatTransfer"]),
        (WorkflowKind::Stop(admin.clone()), "node.stop:mesh1.admin.2", vec!["SeatTransfer"]),
    ] {
        assert_eq!(rdm_client::fold(&kind, &view(op, steps), 1).unwrap_err(), CallEnd::UnrecognisedReceipt { operation: op.into(), step: "SeatTransfer".into() }, "{kind:?}");
    }
    let unknown = view("node.create:mesh1.rpc.3", vec!["AllocateIdentity", "InventedStep"]);
    assert_eq!(
        rdm_client::fold(&WorkflowKind::Create, &unknown, 1).unwrap_err(),
        CallEnd::UnrecognisedReceipt { operation: "node.create:mesh1.rpc.3".into(), step: "InventedStep".into() }
    );
}

/// CONTRACT (node-delete.md: a stopped birth's node.delete runs no stop half, its stop steps are
/// already done; lifecycles/node-replace.md 3): `node.delete` of a `<path>.old` path acts only on
/// the stopped predecessor a rename left there, or one whose exit is proven (the fabric-primary
/// admits nothing else at a `.old` path), so its workflow is NodeDeleting, TerminateRuntime,
/// NodeDeleted, Complete. A caller that lost the stream after `NodeDeleting` resumes at
/// TerminateRuntime, never at a stop that delete never runs; a stop-half receipt under it is
/// refused by name. A bare path's delete keeps its stop half (a live birth's runs it).
#[test]
fn a_dot_old_delete_resumes_after_node_deleting_at_terminate_runtime() {
    let view = |op: &str, steps: Vec<&str>| -> rdm_client::BuildView {
        serde_json::from_value(json!({
            "build_id": "3qcfk1kq47m9", "topology": {}, "submitted_at_ms": 0, "state": "running", "attempt": 1, "executor": null,
            "steps": steps.iter().map(|s| json!({"attempt": 1, "operation": op, "step": s, "outcome": "complete"})).collect::<Vec<_>>(),
            "last_failure": null, "reason": "delete"
        }))
        .unwrap()
    };
    let old: rdm_core::PathName = "mesh1.rpc.3.old".parse().unwrap();
    let r = rdm_client::Resume::from_view(&WorkflowKind::Delete(old.clone()), &view("node.delete:mesh1.rpc.3.old", vec!["NodeDeleting"]), 1).unwrap();
    assert_eq!(r.from, Some(("node.delete".to_string(), "TerminateRuntime".to_string())), "the stopped predecessor's delete stands at TerminateRuntime");
    let f = rdm_client::fold(&WorkflowKind::Delete(old.clone()), &view("node.delete:mesh1.rpc.3.old", vec!["NodeDeleting", "TerminateRuntime", "NodeDeleted", "Complete"]), 1).unwrap();
    assert_eq!(f.frames, [Frame::Event(NodeEvent::Deleted), Frame::Complete]);
    assert_eq!(
        rdm_client::fold(&WorkflowKind::Delete(old.clone()), &view("node.delete:mesh1.rpc.3.old", vec!["NodeDeleting", "StopNode"]), 1).unwrap_err(),
        CallEnd::UnrecognisedReceipt { operation: "node.delete:mesh1.rpc.3.old".into(), step: "StopNode".into() }
    );
    let bare: rdm_core::PathName = "mesh1.rpc.3".parse().unwrap();
    let r = rdm_client::Resume::from_view(&WorkflowKind::Delete(bare), &view("node.delete:mesh1.rpc.3", vec!["NodeDeleting"]), 1).unwrap();
    assert_eq!(r.from, Some(("node.delete".to_string(), "StopNode".to_string())), "a bare path's delete keeps its stop half");
}

/// One attempt's run as an executor streams it to the fabric-primary: its frames, then its end.
struct Script(std::collections::VecDeque<InnerItem>);

#[async_trait::async_trait]
impl InnerStream for Script {
    async fn next(&mut self) -> InnerItem {
        self.0.pop_front().unwrap_or(InnerItem::Ended)
    }
}

/// The executor of a stop of `mesh1.rpc.1`, attempt by attempt. The first run is lost once its
/// `StopNode` frame has reached the fabric-primary: the receipt lived in the lost executor's own
/// log alone (its broadcast never left), so no Build log the successor reads holds it, and the
/// executor's exact runtime is proven exited. The next run holds no `StopNode` receipt: it
/// reissues that uncertain step (node-rpc-envelope.md "Caller execution and certainty") and runs
/// the stop to its end.
struct LostAfterStopNode {
    runs: Mutex<u32>,
    proofs: Arc<crate::loopback::Proofs>,
}

#[async_trait::async_trait]
impl Dispatcher for LostAfterStopNode {
    async fn dispatch(&self, _turn: &rdm_admin::stop_gate::Turn, executor: &PathName, build_id: &BuildId, attempt: u32, _context: rdm_core::context::CallContext, _plan: rdm_admin::executor::RunPlan) -> Dispatched {
        use rdm_core::build::BuildReply;
        let step = |operation: &str, step: &str| {
            InnerItem::Frame(BuildReply::Step {
                build_id: build_id.to_string(),
                attempt,
                operation: format!("{operation}:mesh1.rpc.1"),
                step: step.into(),
                receipt: None,
            })
        };
        let run = {
            let mut runs = self.runs.lock().unwrap();
            *runs += 1;
            *runs
        };
        let items = if run == 1 {
            self.proofs.exited.lock().unwrap().insert(executor.to_string());
            vec![step("node.stop", "StopNode"), InnerItem::Broke(format!("{executor} was lost after its StopNode receipt"))]
        } else {
            vec![
                step("node.stop", "StopNode"),
                step("node.stop", "AwaitNodeLeft"),
                step("node.stop", "Complete"),
                InnerItem::Frame(BuildReply::Complete { build_id: build_id.to_string(), attempt }),
            ]
        };
        Dispatched::Stream(Box::new(Script(items.into())))
    }
}

/// CONTRACT (node-rpc-envelope.md "Caller execution and certainty": a changing call streams each
/// completed step's event once): the executor of a stop is lost after its `StopNode` frame
/// reached the caller and before that receipt reached any Build log the successor reads. The
/// successor's attempt reissues `StopNode` (an uncertain step to it) and writes its own receipt:
/// two receipts of one step, one per attempt, each a frame under its own receipt key. The caller's
/// stream reads them as the client's receipt fold does, per step the latest attempt deciding, so
/// `node.connections.deleted` is delivered once and every other event of the stop once, in order.
#[test]
fn a_step_a_successor_attempt_completes_again_after_its_executor_was_lost_is_one_event_to_the_caller() {
    let cap = capture();
    let frames = cap.run(async {
        let rig = Arc::new(Rig::new());
        let proofs = Arc::new(crate::loopback::Proofs::default());
        let script = Arc::new(LostAfterStopNode { runs: Mutex::new(0), proofs: proofs.clone() });
        let door = door_via(&rig, Duration::from_secs(60), move |_| script, proofs).await;
        let served = serve(door).await;
        let mut stream = nodes_over(&served.carrier).stop(&"mesh1.rpc.1".parse().unwrap()).await.expect("the fabric-primary accepts the stop");
        let (frames, end) = collect_frames(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "the stream ends with its terminal frame: {end:?} after {frames:?}");
        frames
    });
    let events: Vec<NodeEvent> = frames.iter().filter_map(|f| if let Frame::Event(e) = f { Some(*e) } else { None }).collect();
    assert_eq!(
        events,
        [NodeEvent::ConnectionsDeleted, NodeEvent::Stopped],
        "every event of the stop once, in order: {frames:?}"
    );
    assert_eq!(frames.last(), Some(&Frame::Complete));

    let spans = cap.spans();
    let lost: Vec<_> = spans_named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().filter(|s| s["attributes"]["outcome"] == "executor-lost").collect();
    assert_eq!(lost.len(), 1, "the first attempt's executor was lost: {spans:?}");
    assert_eq!(spans_named(&spans, "rdm.node_admin.build.update.via-departure-proven").len(), 1, "its departure was proven before the successor's attempt");
    assert_eq!(spans_named(&spans, "rdm.node_admin.connection.delete.via-deleted-frame").len(), 1, "node.connections.deleted was delivered once");
}

/// CONTRACT: a Build call asked of a node-admin that does not hold the fabric-primary role is refused
/// `not-fabric-primary`, naming the fabric-primary that admin sees, before either door admits anything,
/// and the refusal has its own span as every other Build refusal has (one honest name per reason,
/// `rdm.node_admin.build.reject.via-<reason>`): `rdm.node_admin.build.reject.via-not-fabric-primary` carrying the
/// call's route, the refusing admin and the fabric-primary it named. No `build.create` or `build.update`
/// span opens: nothing was admitted.
#[test]
fn a_build_call_to_an_admin_that_is_not_fabric_primary_is_refused_under_its_own_reject_span_naming_the_fabric_primary() {
    let cap = capture();
    let end = cap.run(async {
        let rig = Arc::new(Rig::new());
        let served = serve(door_not_fabric_primary(&rig).await).await;
        served.carrier.create(rdm_core::build::BuildChange::Stop { node: "mesh1.rpc.1".parse().unwrap() }).await.err()
    });
    match &end {
        Some(CallEnd::Rejected { reason, detail }) => {
            assert_eq!(reason, "not-fabric-primary", "the admin without the role refuses by name: {detail}");
            assert!(detail.contains("names mesh1.admin.1"), "the refusal names the fabric-primary the admin sees: {detail}");
        }
        other => panic!("a Build call to an admin without the fabric-primary role ends Rejected not-fabric-primary, not {other:?}"),
    }
    let spans = cap.spans();
    let refused = spans_named(&spans, "rdm.node_admin.build.reject.via-not-fabric-primary");
    assert_eq!(refused.len(), 1, "the refusal has its own reject span: {spans:?}");
    let a = &refused[0]["attributes"];
    assert_eq!((a["route"].as_str(), a["node"].as_str(), a["fabric_primary"].as_str()), (Some("build.create"), Some("mesh1.admin.2"), Some("mesh1.admin.1")), "the reject span names the call, the refusing admin and the fabric-primary it named: {}", refused[0]);
    for door in ["rdm.node_admin.build.create.via-node-rpc", "rdm.node_admin.build.update.via-node-rpc"] {
        assert!(spans_named(&spans, door).is_empty(), "nothing was admitted: no {door} span: {spans:?}");
    }
}
