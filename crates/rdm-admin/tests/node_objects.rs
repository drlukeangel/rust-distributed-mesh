//! The typed `node` objects, end to end: a workflow call returns a reply stream read from the
//! fabric-primary's Build family (op `0x20`), and the step events are written by the real create
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

use rdm_admin::build_drive::{Dispatched, Dispatcher, DriveEnv, Drives, InnerItem, InnerStream, OpenGate};
use rdm_admin::build_op::{local_dispatched, BuildService, BuildSlot};
use rdm_admin::build_run::{FramedBuilds, SectionRunService, SectionRuns};
use rdm_admin::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::executor::{BuildExecutor, OperationRunner};
use rdm_transport::{NodeRpcClient, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rdm_admin::deployment::endpoint::NODE_SPEC;
use rdm_admin::deployment::pipeline::{CreateRequest, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, Timeouts, TopologySink};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::control_plane::ControlPlane;
use rdm_admin::join::Joins;
use rdm_admin::model::{Fabric, Mesh, Node, NodeStatus, PathName, ProviderKind, ScopeStatus};
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

/// The incarnation each launch of these cells was handed, by node id (`RDM_NODE_ID`,
/// `RDM_INCARNATION_ID` in its launch): the birth the node's own report names.
fn launched() -> &'static Mutex<std::collections::HashMap<String, IncarnationId>> {
    static LAUNCHED: OnceLock<Mutex<std::collections::HashMap<String, IncarnationId>>> = OnceLock::new();
    LAUNCHED.get_or_init(Default::default)
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
    async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
        if let (Some(node_id), Some(incarnation)) = (spec.env.get(rdm_core::launch::ENV_NODE_ID), spec.env.get(rdm_core::launch::ENV_INCARNATION)) {
            launched().lock().unwrap().insert(node_id.clone(), IncarnationId::parse(incarnation).expect("the launch's incarnation"));
        }
        Ok(DeploymentHandle { deployment_id: spec.deployment_id.clone(), provider: ProviderKind::Process, pid: Some(1), start: Some(1), container: None, domain: Some("node-objects".into()) })
    }
    async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        Ok(())
    }
    async fn inspect(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
        DeploymentStatus::Running
    }
    async fn signal_stop(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
    }
}

/// The topology view the admin holds of the birth: it hears the birth only once `joined` is set.
struct TopologyView {
    /// While set, the node is not ready and this is what its hook answered: its probe's `Pending`
    /// reason, or its start's `StartFailed` at the hook.
    not_ready: Mutex<Option<rdm_admin::node_hooks::ProbeAnswer>>,
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
        let hook = self.not_ready.lock().unwrap().clone().map(|a| match a {
            rdm_admin::node_hooks::ProbeAnswer::Pending(why) | rdm_admin::node_hooks::ProbeAnswer::StartFailed(why) => why,
        });
        match hook.or_else(|| self.pending.lock().unwrap().clone()) {
            Some(why) => Err(why),
            None => Ok(()),
        }
    }
    async fn ready_hook_answer(&self, _: &Node) -> Option<rdm_admin::node_hooks::ProbeAnswer> {
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

fn node(name: rdm_core::PathName, mesh: &str, r#type: &str, primary: bool) -> Node {
    let mut n = Node::allocated(name, mesh, r#type);
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.incarnation_id = Some(IncarnationId::mint());
    if n.r#type == "node-admin" {
        n.admin_api_base = Some("http://127.0.0.1:18001".into());
        n.is_fabric_primary = primary;
    }
    n
}

/// One mesh: `mesh1.node-admin.1` (the fabric-primary), `mesh1.node.1` and `mesh1.node.2`, which holds mesh1's
/// rpc role. The cells stop `mesh1.node.1`, which holds no role, so no node.primary.update precedes
/// the stop (proposal Part C1) and the drive's gate of this fixture, which moves no role, is never
/// asked to; a recovery of an exited birth moves no role (`executor::role_move`).
fn mn() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(rdm_core::MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes: vec![node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true), node(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", false), node(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", true)],
    }
}

/// The fabric-primary's `BuildService` over `rig`'s Build state: the control plane that accepts a change,
/// the drive over the Build's events, and the `SectionRunService` that runs the create pipeline.
async fn build_service(rig: &Arc<Rig>, join: Duration) -> Arc<BuildService> {
    build_service_via(rig, join, |run| Arc::new(Local(run))).await
}

/// [`build_service`], whose drive calls `dispatcher` (made over the in-process `SectionRunService`).
async fn build_service_via(rig: &Arc<Rig>, join: Duration, dispatcher: impl FnOnce(Arc<SectionRunService>) -> Arc<dyn Dispatcher>) -> Arc<BuildService> {
    let t = mn();
    let fabric_id = t.fabric.id.clone();
    let accepted = rdm_admin::accepted::AcceptedStore::seeded(&*rig.builds, t.fabric.id.clone(), rdm_admin::accepted::FabricTopology::of_observed(&t), &t.node(&PathName::new("mesh1.node-admin", "1").unwrap()).unwrap().birth().unwrap()).await.unwrap();
    let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
    // The fabric-primary's node records, one store for its `BuildService` and its writer (boot.rs): its `BuildService`
    // accepts an action on a birth from its record, never from the gossip view (Q197).
    let records = crate::records_of(&t).await;
    let executor_birth = t.node(&me).unwrap().birth().unwrap();
    let control = Arc::new(crate::control_plane(rig.builds.clone(), accepted.clone(), me.clone(), t, crate::adopted_time()));
    crate::bind_store(&control, records.clone());
    let runner: Arc<dyn OperationRunner> = Arc::new(RigRunner { rig: rig.clone(), join });
    let exec = Arc::new(BuildExecutor { executor: executor_birth.clone(), builds: rig.builds.clone(), topology: control.topology.clone(), runner });
    let run = Arc::new(SectionRunService { me_path: me.path.clone(), me_name: me.name.clone(), builds: rig.builds.clone(), exec, runs: rig.runs.clone(), local: rig.local.clone(), gate: rdm_admin::stop_gate::StopGate::new(), atlas: Default::default() });
    let rows = Arc::new(rdm_admin::node_rows::NodeRows::new(me.clone(), control.topology.read().await.node(&me).expect("the fabric-primary is in the view").node_id.clone(), control.topology.clone(), records));
    let _ = rig.sink.1.set(rows.clone());
    let env = Arc::new(DriveEnv { me_mesh: "mesh1".into(),
        me_path: me.path.clone(), me_name: me.name.clone(),
        birth: executor_birth.clone(),
        topology: control.topology.clone(),
        builds: rig.builds.clone(),
        dispatcher: dispatcher(run.clone()),
        gate: Arc::new(OpenGate),
        notices: crate::written_by(rig.told.notices(), &*rig.builds, &fabric_id, &executor_birth),
        turns: rdm_admin::stop_gate::StopGate::new(),
        send: rdm_transport::Send::default(),
    });
    Arc::new(BuildService { me_path: me.path.clone(), me_name: me.name.clone(), control, drives: Arc::new(Drives::default()), env, run, primaries: rdm_gossip::primary::PrimaryView::default(), incarnation: executor_birth.incarnation.clone() })
}

/// The `BuildService` of `mesh1.node-admin.2`, a node-admin of the fabric that does not hold the fabric-primary
/// role (`mesh1.node-admin.1` holds it): its `BuildService` carries its own birth's node id.
async fn build_service_not_fabric_primary(rig: &Arc<Rig>) -> Arc<BuildService> {
    let mut t = mn();
    let fabric_id = t.fabric.id.clone();
    t.nodes.push(node(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", false));
    let accepted = rdm_admin::accepted::AcceptedStore::seeded(&*rig.builds, t.fabric.id.clone(), rdm_admin::accepted::FabricTopology::of_observed(&t), &t.node(&PathName::new("mesh1.node-admin", "1").unwrap()).unwrap().birth().unwrap()).await.unwrap();
    let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "2").unwrap();
    let executor_birth = t.node(&me).unwrap().birth().unwrap();
    let control = Arc::new(crate::control_plane(rig.builds.clone(), accepted.clone(), me.clone(), t, crate::adopted_time()));
    let runner: Arc<dyn OperationRunner> = Arc::new(RigRunner { rig: rig.clone(), join: Duration::from_millis(300) });
    let exec = Arc::new(BuildExecutor { executor: executor_birth.clone(), builds: rig.builds.clone(), topology: control.topology.clone(), runner });
    let run = Arc::new(SectionRunService { me_path: me.path.clone(), me_name: me.name.clone(), builds: rig.builds.clone(), exec, runs: rig.runs.clone(), local: rig.local.clone(), gate: rdm_admin::stop_gate::StopGate::new(), atlas: Default::default() });
    let node_id = control.topology.read().await.node(&me).expect("the admin is in the view").node_id.clone();
    let rows = Arc::new(rdm_admin::node_rows::NodeRows::new(me.clone(), node_id.clone(), control.topology.clone(), Arc::new(rdm_admin::storage::MemoryNodesStorage::default())));
    let _ = rig.sink.1.set(rows);
    let env = Arc::new(DriveEnv { me_mesh: "mesh1".into(),
        me_path: me.path.clone(), me_name: me.name.clone(),
        birth: executor_birth.clone(),
        topology: control.topology.clone(),
        builds: rig.builds.clone(),
        dispatcher: Arc::new(Local(run.clone())),
        gate: Arc::new(OpenGate),
        notices: crate::written_by(rig.told.notices(), &*rig.builds, &fabric_id, &executor_birth),
        turns: rdm_admin::stop_gate::StopGate::new(),
        send: rdm_transport::Send::default(),
    });
    Arc::new(BuildService { me_path: me.path.clone(), me_name: me.name.clone(), control, drives: Arc::new(Drives::default()), env, run, primaries: rdm_gossip::primary::PrimaryView::default(), incarnation: executor_birth.incarnation.clone() })
}

/// The one admin of the fixture that runs a section is the fabric-primary itself.
struct Local(Arc<SectionRunService>);

#[async_trait::async_trait]
impl Dispatcher for Local {
    async fn dispatch(&self, build_id: &BuildId, mesh: &str, context: rdm_core::context::CallContext) -> Dispatched {
        local_dispatched(self.0.section_run(build_id, mesh, &context).await)
    }
    async fn run_own(&self, turn: &rdm_admin::stop_gate::Turn, build_id: &BuildId, mesh: &str, context: rdm_core::context::CallContext, operations: Vec<BuildOperation>) -> Dispatched {
        local_dispatched(self.0.run_under(turn, build_id, mesh, &context, operations).await)
    }
}

/// Runs the create operation through the pipeline over the fixture's fakes, as the executing admin does.
struct RigRunner {
    rig: Arc<Rig>,
    join: Duration,
}

#[async_trait::async_trait]
impl OperationRunner for RigRunner {
    async fn run(&self, build_id: &BuildId, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        match op {
            BuildOperation::CreateNode { path: node_path, name: node_name, node_id, over: None, .. } => { let node = &rdm_core::PathName { path: node_path.clone(), name: node_name.clone() }; self.rig.pipeline_create(build_id, node, node_id, self.join).await.map_err(Into::into) },
            other => Err(format!("the fixture creates nodes only, not {other:?}").into()),
        }
    }
}

/// A node-RPC server on loopback that serves the Build family from `service`, and a carrier to it.
struct Served {
    router: Router,
    carrier: BuildCarrier,
    service: Arc<BuildService>,
}

async fn serve(service: Arc<BuildService>) -> Served {
    let (node_id, incarnation, key) = (NodeId::mint(), IncarnationId::mint(), SecretKey::generate());
    let slot: BuildSlot = Arc::new(OnceLock::new());
    let _ = slot.set(service.clone());
    let server = rdm_admin::build_op::serve(ServerBuilder::new(), slot)
        .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .unwrap();
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
    let resolver = Arc::new(StaticResolver::new());
    // The fabric-primary the service's view names resolves to this one served `BuildService` too: a redirect it
    // answers is followed once and ends at the `BuildService` again.
    let fabric_primary = service.control.topology.read().await.fabric_primary().map(|n| n.node_id.clone()).filter(|id| *id != node_id);
    if let Some(fp) = fabric_primary {
        resolver.insert(ResolvedNode { mesh: "mesh1".into(), r#type: "node-admin".into(), node_id: fp, path: "mesh1.node-admin".into(), name: "1".into(), endpoint_id: key.public(), transport_addr: addr, incarnation: incarnation.clone() });
    }
    resolver.insert(ResolvedNode { mesh: "mesh1".into(), r#type: "node-admin".into(), node_id, path: "mesh1.node-admin".into(), name: "1".into(), endpoint_id: key.public(), transport_addr: addr, incarnation });
    let caller = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = Arc::new(NodeRpcClient::new(caller, resolver).with_caller_system("rdm"));
    Served { router, carrier: BuildCarrier::new(client, rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()), service }
}

/// `node.create` of mesh1's next node, named on its executable: a Create of that node alone
/// (build.md §1), its path the one the count expands to against what `served`'s `BuildService` reads.
async fn create_rpc(nodes: &Nodes, served: &Served) -> Result<WorkflowStream, CallEnd> {
    let rdm_core::build::BuildChange::Create { nodes: added, .. } = crate::named_request(&served.service.control, |t: &rdm_admin::topology::Topology| crate::grown_by_one(t, "mesh1", "node")).await else {
        unreachable!("a node is added by a Create")
    };
    nodes.create(added).await
}

/// The node objects over `carrier`; no cell of this file reads a view.
fn nodes_over(carrier: &BuildCarrier) -> Nodes {
    Nodes::new(carrier.clone())
}

struct Rig {
    builds: Arc<dyn BuildStateAdapter>,
    local: Arc<MemoryBuildStateAdapter>,
    runs: Arc<SectionRuns>,
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
            launcher: rdm_core::launch::Launcher { mesh: "mesh1".into(), path: "mesh1.node-admin".into(), name: "1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: Default::default(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let runs = Arc::new(SectionRuns::default());
        let local = Arc::new(MemoryBuildStateAdapter::new());
        // The executing admin's step log, as boot wires it: each step its pipelines run written as its
        // own `rdm.build.step` event (build.md §5).
        let steps = rdm_admin::events_storage::EventsWriter::on(local.events(), &template.fabric_id, &template.launcher.node_id);
        Rig { builds: Arc::new(FramedBuilds::new(local.clone(), runs.clone()).writing_steps(steps, "mesh1")), local, runs, joins: Arc::new(Joins::default()), topology_view: Arc::new(TopologyView { not_ready: Mutex::new(None), pending: Mutex::new(None) }), sink: Arc::default(), template, data_root, told: crate::Told::default() }
    }

    /// Run the create pipeline for `name` of `build`, under the node.id the Build record names for it
    /// (D58), as the mesh-primary that runs its section does; `join` bounds the waits for the birth's
    /// report (`WaitForBind`, `WaitForMeshJoin`).
    async fn pipeline_create(&self, build: &BuildId, name: &PathName, node_id: &NodeId, join: Duration) -> Result<(), String> {
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
        let req = CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", node_id), node_id: node_id.clone(), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(build.clone()), path: name.path.clone(), name: name.name.clone(), spec: &NODE_SPEC, data_dir: None, held_runtimes: Vec::new(), entry: None };
        pipeline.create(&req).await.map(|_| ()).map_err(|e| e.to_string())
    }

    /// The key of `build`'s create of `name`, `node.create:<node_id>` under the node.id its record names
    /// (D58), once the Build is in this admin's log.
    async fn create_key(&self, build: &BuildId, name: &PathName) -> rdm_core::OperationId {
        loop {
            if let Some(key) = self.builds.read_build(build).await.ok().and_then(|view| view.topology.create_key(name)) {
                return key;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// The birth reports where it bound once the create has made its runtime fact available: the
    /// node's own `JoinNode`, the validated report the pipeline's `WaitForBind` and `WaitForMeshJoin`
    /// complete from (node-create.md step 5).
    async fn report_bind(&self, build: &BuildId, name: &PathName) {
        let op = self.create_key(build, name).await;
        loop {
            // The executor's own log holds the Build once the call that carries it has arrived.
            let Ok(view) = self.builds.read_build(build).await else {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            };
            let done = |step: &str| view.completed(&op.to_string(), step);
            if done("AllocateIdentity") && done("PrepareStorage") && done("MakeRuntimeFactAvailableToBirth") {
                // The birth as the create launched it (D58, D67): the node.id the create names, the
                // incarnation its launch handed the node, the endpoint the transport key in its data dir.
                let node_id = view.topology.created_id(name).expect("the Build creates the node").clone();
                let Some(incarnation) = launched().lock().unwrap().get(node_id.as_str()).cloned() else {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                };
                let data_dir = rdm_admin::deployment::node_data_dir(&self.data_root, &node_id);
                let endpoint_id = rdm_admin::deployment::pipeline::endpoint_id(&data_dir).expect("the create wrote the transport key");
                let runtime: RuntimeFact = RuntimeFact::read_record(&data_dir).expect("the create wrote the runtime record").unwrap();
                let digest = MeshDigest { 
                    fabric_id: FabricId::mint(),
                    node: MeshNode { mesh: view.topology.node_spec(&name).unwrap().mesh.clone(), r#type: view.topology.node_spec(&name).unwrap().r#type.clone(), node_id, path: name.path.clone(), name: name.name.clone(), endpoint_id, transport_addr: "127.0.0.1:34567".parse().unwrap(), incarnation, supersedes: None, runtime: Some(runtime.clone()) },
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

const STEPS_BEFORE_THE_BIND: [&str; 7] = [
    "AllocateIdentity",
    "PrepareStorage",
    "PrepareNetwork",
    "DeployRuntime",
    "RegisterExactRuntimeHandle",
    "ResolveProviderControlDomain",
    "MakeRuntimeFactAvailableToBirth",
];

/// CONTRACT (ops-naming acceptance 1, "A failure names its step"; node-create.md "Join completion
/// and failure evidence": only an accepted full report completes the bind/join waits; Q346: a failed
/// step ends its mesh's section, never the Build): a create whose process starts and whose birth never
/// reports where it bound (no `JoinNode` arrives) fails its bind wait once: mesh1's section ends there
/// and the Build ends `incomplete`. The stream ends `Started`, then `Failed { step: node.create,
/// reason }` from `WaitForBind`, within the deadline and with no Pending frame before it.
/// `node.created`, `node.joined` and `node.started` are never emitted, the node never reaches
/// ready, and the call's span names the failed step.
#[test]
fn a_create_whose_birth_never_reports_names_the_bind_wait_and_emits_nothing_after_it() {
    let cap = capture();
    let (frames, end, published) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let served = serve(build_service(&rig, Duration::from_millis(300)).await).await;
        let nodes = nodes_over(&served.carrier);
        let mut stream = create_rpc(&nodes, &served).await.unwrap();
        let (frames, end) = tokio::time::timeout(Duration::from_secs(20), collect_frames(&mut stream, Duration::from_secs(15))).await.expect("the stream ends within 20 s");
        let published = rig.sink.0.lock().unwrap().clone();
        (frames, end, published)
    });
    let reason = match frames.last() {
        Some(Frame::Failed { step: NodeStep::Create, reason }) => reason.clone(),
        other => panic!("the stream ends Failed at node.create, not {other:?}; frames {frames:?}"),
    };
    assert!(reason.contains("did not report where it bound"), "the failure carries the step's own reason: {reason}");
    assert_eq!(
        frames[..frames.len() - 1],
        [Frame::Started { build_id: match &frames[0] { Frame::Started { build_id, .. } => build_id.clone(), f => panic!("{f:?}") } }]
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
    assert_eq!(bind_step.len(), 1, "the pipeline ran the bind wait once: the failed step ended the section: {bind_step:?}");
    assert!(bind_step.iter().all(|s| s["attributes"]["outcome"] == "failed"), "and it failed: {bind_step:?}");
    assert!(spans_named(&spans, "rdm.node_admin.deployment.update.via-step").iter().all(|s| s["attributes"]["step"] != "WaitForMeshJoin"), "nothing after the bind wait ran: {spans:?}");
    let prepare: Vec<&Value> = spans_named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|s| s["attributes"]["step"] == "PrepareStorage").collect();
    assert_eq!(prepare.len(), 1, "PrepareStorage ran once: {spans:?}");
    assert_eq!(prepare[0]["attributes"]["awaited"], "name-check,endpoint_id", "PrepareStorage names every await it ran: {}", prepare[0]);
    assert_eq!(prepare[0]["attributes"]["awaiting"], "done", "PrepareStorage completed every await: {}", prepare[0]);
    assert!(spans_named(&spans, "rdm.node_admin.node.create.via-node-row").is_empty(), "a create writes no record: the node writes its own at its start (Q356)");
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

/// CONTRACT (node-rpc-envelope.md, Build "Frames": Pending is "emitted when its reason changes"; its
/// only source is the subject's Status `Pending`): a create whose node has joined and whose
/// `pre_node_ready` answers `Pending` (its probe's Status `Pending`) streams one
/// `Pending { operation, step, reason }` frame naming the step in flight and the hook's own reason,
/// however many times the node is probed with that reason; it is live progress, so the stream goes
/// on, and when the node is ready it emits `node.created` and `Complete`.
#[test]
fn a_create_whose_node_answers_pending_streams_its_reason_once() {
    let cap = capture();
    let (pending, after, key) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let why = "mesh1.node.3: pre_node_ready answered Pending: pulling its node-admin";
        *rig.topology_view.not_ready.lock().unwrap() = Some(rdm_admin::node_hooks::ProbeAnswer::Pending(why.to_string()));
        let served = serve(build_service(&rig, Duration::from_secs(30)).await).await;
        let nodes = nodes_over(&served.carrier);
        let mut stream = create_rpc(&nodes, &served).await.unwrap();
        let build = stream.accepted().expect("the call opened a Build").build_id.clone();
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, &PathName::new("mesh1.node", "3").unwrap()).await }
        });
        let mut pending = None;
        while pending.is_none() {
            match tokio::time::timeout(Duration::from_secs(5), stream.next()).await.expect("the stream went quiet before any Pending frame") {
                Some(Ok(f @ Frame::Pending { .. })) => pending = Some(f),
                Some(Ok(_)) => {}
                other => panic!("the stream ended before the node's Pending was framed: {other:?}"),
            }
        }
        // The node is probed again with the same reason: no second frame. Then the node is ready.
        tokio::time::sleep(Duration::from_millis(350)).await;
        *rig.topology_view.not_ready.lock().unwrap() = None;
        let (after, end) = collect_frames(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "the stream ends with its terminal frame: {end:?}");
        let key = rig.create_key(&build, &PathName::new("mesh1.node", "3").unwrap()).await;
        (pending.unwrap(), after, key)
    });
    assert_eq!(
        pending,
        Frame::Pending {
            operation: key.to_string(),
            step: "WaitForNodeReady".into(),
            reason: "mesh1.node.3: pre_node_ready answered Pending: pulling its node-admin".into()
        }
    );
    assert_eq!(after, [Frame::Event(NodeEvent::Started), Frame::Event(NodeEvent::Created), Frame::Complete], "a repeated reason makes no second frame, and the ready node ends the stream: {after:?}");
}

/// CONTRACT (fabric-node-hooks.md "Where the failure lands": a Build-started node's failed start
/// lands as "the step's `rdm.build.step` event, `failed { reason }`; its mesh's section ends, and the
/// Build ends `incomplete`"; the executor "ends its `WaitForNodeReady` step failed from its Status
/// `StartFailed { step: node.pre-node-ready, reason }` reply"): a create whose node has joined and
/// whose start ended Failed at its hook (its hook's attempts spent answering `Pending`) fails
/// `WaitForNodeReady` at once with the hook's last reason, long before the step's own bound: the node
/// joined, and the stream ends `Failed` at the node's start naming it. Must NOT happen: the step waiting out its bound,
/// `node.created` or `Complete`, or the reason lost.
#[test]
fn a_create_whose_nodes_start_ended_failed_at_its_hook_fails_its_ready_step_with_the_hooks_reason() {
    let cap = capture();
    let (frames, end, took) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let why = "mesh1.node.3: its start ended Failed at node.pre-node-ready: rows still not published (pre_node_ready answered Pending on each of its 3 attempts)";
        *rig.topology_view.not_ready.lock().unwrap() = Some(rdm_admin::node_hooks::ProbeAnswer::StartFailed(why.to_string()));
        let served = serve(build_service(&rig, Duration::from_secs(30)).await).await;
        let nodes = nodes_over(&served.carrier);
        let started = std::time::Instant::now();
        let mut stream = create_rpc(&nodes, &served).await.unwrap();
        let build = stream.accepted().expect("the call opened a Build").build_id.clone();
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, &PathName::new("mesh1.node", "3").unwrap()).await }
        });
        let (frames, end) = tokio::time::timeout(Duration::from_secs(20), collect_frames(&mut stream, Duration::from_secs(15))).await.expect("the stream ends within 20 s");
        (frames, end, started.elapsed())
    });
    assert!(end.is_none(), "a failed step ends the stream with its terminal frame: {end:?}");
    let reason = match frames.last() {
        Some(Frame::Failed { step: NodeStep::Start, reason }) => reason.clone(),
        other => panic!("the stream ends Failed at the node's start, not {other:?}; frames {frames:?}"),
    };
    assert!(reason.contains("WaitForNodeReady") && reason.contains("rows still not published") && reason.contains("node.pre-node-ready"), "the failure names the ready wait, the hook's last reason and its step: {reason}");
    assert!(frames.iter().any(|f| matches!(f, Frame::Event(NodeEvent::Joined))), "the node joined before its start failed: {frames:?}");
    assert!(took < Duration::from_secs(15), "the step ends on the StartFailed answer, not on its 30 s bound: {took:?}");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Event(NodeEvent::Created) | Frame::Complete)), "nothing downstream of the failed start is emitted: {frames:?}");
}

/// CONTRACT: a create whose node has joined but only reports Pending (no hook has named a blocker)
/// streams no `Pending` frame however long the Ready check fails; when the node is ready the stream
/// emits `node.created` and `Complete`.
#[test]
fn a_create_whose_node_is_merely_pending_streams_no_pending_frame() {
    let cap = capture();
    let (frames, end) = cap.run(async {
        let rig = Arc::new(Rig::new());
        *rig.topology_view.pending.lock().unwrap() = Some("mesh1.node.3 reports Pending".to_string());
        let served = serve(build_service(&rig, Duration::from_secs(30)).await).await;
        let nodes = nodes_over(&served.carrier);
        let mut stream = create_rpc(&nodes, &served).await.unwrap();
        let build = stream.accepted().expect("the call opened a Build").build_id.clone();
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, &PathName::new("mesh1.node", "3").unwrap()).await }
        });
        tokio::time::sleep(Duration::from_millis(450)).await;
        *rig.topology_view.pending.lock().unwrap() = None;
        collect_frames(&mut stream, Duration::from_secs(10)).await
    });
    assert!(end.is_none(), "the stream ends with its terminal frame: {end:?}");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Pending { .. })), "a Pending node whose hook answered nothing is not a Pending frame: {frames:?}");
    assert_eq!(frames[frames.len() - 2..], [Frame::Event(NodeEvent::Created), Frame::Complete], "the ready node ends the stream: {frames:?}");
}

/// CONTRACT (ops-naming acceptance 2, "A broken stream is not a failure"): the transport between
/// the caller and the fabric-primary is cut while the create waits for its birth's report (the
/// `JoinNode` that completes `WaitForBind` and `WaitForMeshJoin`, node-create.md step 5). The
/// caller's stream ends `Indeterminate`, never `Failed`. The cut cancels nothing: `build.get [id]` over a
/// new transport returns the step events of exactly the steps that completed, `Resume` names the bind
/// wait as the first step without a `complete` event, and the re-submit by the Build id streams
/// the same Build (never a second one) to its end once the birth is heard: no completed step runs
/// again, each still has one step event, and the steps' spans say the run ran them once.
#[test]
fn a_cut_stream_is_indeterminate_and_the_resume_reruns_no_completed_step() {
    let cap = capture();
    let (frames, end, build_after_cut, resume, build_after, final_frames, disposition, key) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let service = build_service(&rig, Duration::from_secs(60)).await;
        let served = serve(service.clone()).await;
        let nodes = nodes_over(&served.carrier);
        let mut stream = create_rpc(&nodes, &served).await.unwrap();
        let build = stream.accepted().expect("the call opened a Build").build_id.clone();
        let accepted = stream.accepted().cloned().expect("the call opened a Build");
        let mut frames = Vec::new();
        // The cut: the caller's wire is severed while the create waits for the birth's report, which
        // is not heard yet. The wait is where it is cut once the step before it has its step event.
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
        let again = serve(service).await;
        let builds = Builds::new(again.carrier.clone());
        let build_after_cut = builds.get(&rdm_client::Filter::new().eq("id", [accepted.build_id.to_string()])).await.unwrap().remove(0);
        let resume = Resume::from_step_events(&WorkflowKind::Create, &build_after_cut).unwrap();
        // The birth is heard this time: the Build the cut did not cancel goes on to its end, and the
        // re-submit streams it.
        let mut resumed = nodes_over(&again.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let disposition = resumed.disposition();
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, &PathName::new("mesh1.node", "3").unwrap()).await }
        });
        let (final_frames, resumed_end) = collect_frames(&mut resumed, Duration::from_secs(10)).await;
        assert!(resumed_end.is_none(), "the resumed stream ends with its terminal frame: {resumed_end:?}");
        let build_after = builds.get(&rdm_client::Filter::new().eq("id", [accepted.build_id.to_string()])).await.unwrap().remove(0);
        let key = rig.create_key(&build, &PathName::new("mesh1.node", "3").unwrap()).await;
        (frames, end, build_after_cut, resume, build_after, final_frames, disposition, key)
    });
    assert!(matches!(end, CallEnd::Indeterminate { .. }), "a cut stream is Indeterminate, not {end:?}");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Failed { .. } | Frame::Complete)), "no step is inferred failed or complete from a broken stream: {frames:?}");
    assert!(!frames.iter().any(|f| matches!(f, Frame::Event(NodeEvent::Joined | NodeEvent::Started | NodeEvent::Created))), "no event past the join was delivered: {frames:?}");

    let completed_steps = |r: &rdm_client::Build| -> Vec<String> { r.completed_steps().into_iter().map(|(_, _, step)| step).collect() };
    let step_events = |r: &rdm_client::Build| -> Vec<String> { r.events.iter().filter(|e| e["type"] == "rdm.build.step").filter_map(|e| e["data"]["step"].as_str().map(str::to_string)).collect() };
    assert_eq!(step_events(&build_after_cut).len(), STEPS_BEFORE_THE_BIND.len(), "build.get returns exactly the step events of the steps that completed: {:?}", build_after_cut.events);
    assert_eq!(completed_steps(&build_after_cut), STEPS_BEFORE_THE_BIND);
    assert_eq!(resume.from, Some((key.to_string(), "WaitForBind".to_string())), "the resume starts at the first step without a complete step event");
    assert_eq!(resume.completed.len(), STEPS_BEFORE_THE_BIND.len());
    assert_eq!(disposition, Some(rdm_core::build::Disposition::Attached), "the re-submit attached to the Build the cut left running");

    for step in STEPS_BEFORE_THE_BIND {
        assert_eq!(step_events(&build_after).iter().filter(|s| *s == step).count(), 1, "{step} was not run again: one step event");
    }
    assert_eq!(completed_steps(&build_after).len(), STEPS_BEFORE_THE_BIND.len() + 6, "the same run went on through the bind wait and the join to its end");
    assert!(matches!(&final_frames[0], Frame::Started { .. }));
    assert_eq!(final_frames[1..], [Frame::Event(NodeEvent::Joined), Frame::Event(NodeEvent::Started), Frame::Event(NodeEvent::Created), Frame::Complete]);

    let spans = cap.spans();
    let calls = spans_named(&spans, "rdm.node_admin.node.create.via-workflow");
    assert_eq!(calls.len(), 2, "the create and its re-submit: {spans:?}");
    assert_eq!(calls.iter().filter(|c| c["attributes"]["outcome"] == "indeterminate").count(), 1, "the cut call ended indeterminate");
    assert_eq!(calls.iter().filter(|c| c["attributes"]["outcome"] == "complete").count(), 1, "the re-submit ended complete");
    let steps = spans_named(&spans, "rdm.node_admin.deployment.update.via-step");
    for step in STEPS_BEFORE_THE_BIND {
        let ran: Vec<_> = steps.iter().filter(|s| s["attributes"]["step"] == step).collect();
        assert_eq!(ran.len(), 1, "{step} ran once: {ran:?}");
        assert_eq!(ran[0]["attributes"]["outcome"], "complete");
    }
}

/// CONTRACT (R-ON8; node-rpc-envelope.md "Caller execution and certainty": "a completed Build answers
/// AlreadyApplied with its terminal frame"): a re-submit of a Build that completed is answered from what
/// the fabric-primary's own drive recorded off the calls it made: the re-submit is `AlreadyApplied` with
/// every event the create named and the terminal frame.
#[test]
fn a_resubmit_after_the_end_replays_the_frames_the_drive_recorded_not_the_facts_the_log_holds() {
    let cap = capture();
    let (first, again, disposition) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let served = serve(build_service(&rig, Duration::from_secs(30)).await).await;
        let nodes = nodes_over(&served.carrier);
        let mut stream = create_rpc(&nodes, &served).await.unwrap();
        let build = stream.accepted().expect("the call opened a Build").build_id.clone();
        let accepted = stream.accepted().cloned().expect("the call opened a Build");
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, &PathName::new("mesh1.node", "3").unwrap()).await }
        });
        let (first, end) = collect_frames(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");

        let mut resumed = nodes_over(&served.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let disposition = resumed.disposition();
        let (again, end) = collect_frames(&mut resumed, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        (first, again, disposition)
    });
    assert_eq!(first[1..], [Frame::Event(NodeEvent::Joined), Frame::Event(NodeEvent::Started), Frame::Event(NodeEvent::Created), Frame::Complete], "{first:?}");
    assert_eq!(disposition, Some(rdm_core::build::Disposition::AlreadyApplied));
    assert_eq!(again[1..], first[1..], "every event again, from the drive's own record: {again:?}");
}

/// CONTRACT (node-rpc-envelope.md "Caller execution and certainty": "a completed Build answers
/// AlreadyApplied with its terminal frame"): the reconcile loop's tick that read the Build active
/// just before its verdict landed, and so asks for its drive after the drive ended `Complete`, is
/// handed the completed drive itself. The record a re-submit is answered from stays the drive
/// that made the calls: the re-submit is `AlreadyApplied` with every frame and the terminal frame.
/// Must NOT happen: the completed drive replaced by a new one, which runs the section again and makes
/// a re-submit read a running drive and answer `Attached` (fabric_build__build_op gate fa338efb).
#[test]
fn a_reconcile_tick_after_the_end_keeps_the_completed_drive_and_a_resubmit_is_already_applied() {
    let cap = capture();
    let (build, kept, first, again, disposition) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let service = build_service(&rig, Duration::from_secs(30)).await;
        let served = serve(service.clone()).await;
        let nodes = nodes_over(&served.carrier);
        let mut stream = create_rpc(&nodes, &served).await.unwrap();
        let build = stream.accepted().expect("the call opened a Build").build_id.clone();
        let accepted = stream.accepted().cloned().expect("the call opened a Build");
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, &PathName::new("mesh1.node", "3").unwrap()).await }
        });
        let (first, end) = collect_frames(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        let completed = service.drives.get(&build).expect("the drive that completed the Build is held");
        assert!(completed.completed(), "the drive ended Complete");
        // The reconcile loop's `ensure_active`, acting on a `list_active` read taken before the
        // verdict entered the log.
        let ensured = service.drives.ensure(&service.env, &build);
        let kept = Arc::ptr_eq(&ensured, &completed) && service.drives.get(&build).is_some_and(|d| Arc::ptr_eq(&d, &completed));
        let mut resumed = nodes_over(&served.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let disposition = resumed.disposition();
        let (again, end) = collect_frames(&mut resumed, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        (build, kept, first, again, disposition)
    });
    assert!(kept, "the reconcile tick is handed the completed drive, never a new one in its place");
    assert_eq!(disposition, Some(rdm_core::build::Disposition::AlreadyApplied));
    assert_eq!(again[1..], first[1..], "every event again, from the drive's own record: {again:?}");
    let spans = cap.spans();
    let sections: Vec<_> = spans_named(&spans, "rdm.node_admin.build.serve.via-own-section").into_iter().filter(|s| s["attributes"]["build_id"] == build.log().to_string().as_str()).collect();
    assert_eq!(sections.len(), 1, "one run of mesh1's section, from the drive that made the calls: {sections:?}");
    eprintln!("span rdm.node_admin.build.serve.via-own-section {}", sections[0]);
}

/// A Build store read by a reconcile tick that took its current-Build read before the Build's
/// `rdm.build.ended` landed: `current_build` answers as read then, and every other read is the store
/// as it is now.
struct ReadBeforeTheEnd {
    inner: Arc<dyn BuildStateAdapter>,
    current: rdm_admin::build_state::CurrentBuild,
}

#[async_trait::async_trait]
impl BuildStateAdapter for ReadBeforeTheEnd {
    async fn publish_accepted(&self, a: &rdm_admin::build_state::BuildAccepted) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.inner.publish_accepted(a).await
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
    async fn forget(&self, id: &BuildId) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.inner.forget(id).await
    }
    async fn accepted_newest_first(&self) -> Result<Vec<rdm_admin::build::BuildId>, rdm_admin::build_state::BuildStateError> {
        self.inner.accepted_newest_first().await
    }
    fn events(&self) -> std::sync::Arc<dyn rdm_admin::events_storage::EventsStorage> {
        self.inner.events()
    }
}


/// CONTRACT (R-BI3: "A failed Build is never current, and nothing resumes it on its own; a caller
/// resubmits it"; Q346: an incomplete Build is skipped the same way): the reconcile loop's tick
/// (`ensure_active`) reads the current Build's id, then the Build. When the Build ended `incomplete`
/// between the two reads, the Build it read is no longer in progress and the tick drives nothing: it
/// makes no drive and returns none, and says so on
/// `rdm.node_admin.build.reject.via-not-in-progress-when-read` naming the Build and its state.
/// Must NOT happen: a drive of the incomplete Build from the tick (the land gate on 7b2310bb: a Build
/// run again 2 ms after it ended, by a drive with no parent span, the reconcile loop's).
#[test]
fn a_reconcile_tick_whose_build_ended_incomplete_between_its_reads_drives_nothing() {
    use rdm_admin::build_state::{BuildState, CurrentBuild};
    use rdm_admin::events_storage::{EndedSection, EndedState};
    let cap = capture();
    let (b0, read, ensured, held) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let service = build_service(&rig, Duration::from_secs(30)).await;
        let b0 = rig.builds.current_build().await.unwrap().into_build_id().unwrap();
        let by = service.control.topology.read().await.node(&service.me()).unwrap().birth().unwrap();
        // A resubmitted run of the Build is in progress when the tick reads the current Build.
        crate::start_build(&*rig.builds, &b0, &by).await;
        let before = rig.builds.current_build().await.unwrap();
        assert_eq!(before, CurrentBuild::InProgress(b0.clone()), "the run is in progress when the tick reads the current Build");
        // The run's end lands before the tick's Build read.
        let short = EndedState::Incomplete { sections: vec![EndedSection { mesh: "mesh1".into(), op: "node.stop:000000000002".into(), step: "StopNode".into(), reason: "mesh1.node.2: stop-node was not admitted".into(), code: "DEADLINE_EXCEEDED".into() }] };
        crate::end_build(&*rig.builds, &b0, &by, short).await;
        let read = rig.builds.read_build(&b0).await.unwrap();
        let e = &service.env;
        let env = Arc::new(DriveEnv { me_mesh: e.me_mesh.clone(), me_path: e.me_path.clone(), me_name: e.me_name.clone(), birth: e.birth.clone(), topology: e.topology.clone(), builds: Arc::new(ReadBeforeTheEnd { inner: rig.builds.clone(), current: before }), dispatcher: e.dispatcher.clone(), gate: e.gate.clone(), notices: e.notices.clone(), turns: e.turns.clone(), send: e.send });
        let drives = Arc::new(Drives::default());
        let ensured = rdm_admin::build_op::ensure_active(&drives, &env).await.len();
        let held = drives.get(&b0).is_some();
        (b0, read, ensured, held)
    });
    assert_eq!(read.state, BuildState::Incomplete, "the tick's Build read is the incomplete run: {read:?}");
    assert_eq!(ensured, 0, "the tick drives nothing of the Build it read incomplete");
    assert!(!held, "no drive of {b0} is made");
    let spans = cap.spans();
    let told = spans_named(&spans, "rdm.node_admin.build.reject.via-not-in-progress-when-read");
    assert!(told.iter().any(|s| s["attributes"]["build_id"] == b0.log().to_string().as_str() && s["attributes"]["state"] == "Incomplete"), "the tick names the Build it read incomplete: {told:?}");
    eprintln!("span rdm.node_admin.build.reject.via-not-in-progress-when-read {}", told[0]);
}

/// CONTRACT (R-ON8; build.md §5: a step is its `rdm.build.step` event): a fabric-primary that took the
/// role after a Build ended holds no frame of it. Its re-submit is `AlreadyApplied` and the frames are
/// read from the Build's step events.
#[test]
fn a_fabric_primary_with_no_drive_of_an_ended_build_replays_it_from_its_step_events() {
    let cap = capture();
    let (first, again, disposition) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let before = build_service(&rig, Duration::from_secs(30)).await;
        let served = serve(before.clone()).await;
        let mut stream = create_rpc(&nodes_over(&served.carrier), &served).await.unwrap();
        let build = stream.accepted().expect("the call opened a Build").build_id.clone();
        let accepted = stream.accepted().cloned().expect("the call opened a Build");
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, &PathName::new("mesh1.node", "3").unwrap()).await }
        });
        let (first, end) = collect_frames(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        // The same Build log and executor under a fabric-primary that never drove this Build.
        let after = Arc::new(BuildService { me_path: before.me_path.clone(), me_name: before.me_name.clone(), control: before.control.clone(), drives: Arc::new(Drives::default()), env: before.env.clone(), run: before.run.clone(), primaries: before.primaries.clone(), incarnation: before.incarnation.clone() });
        let successor = serve(after).await;
        let mut resumed = nodes_over(&successor.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let disposition = resumed.disposition();
        let (again, end) = collect_frames(&mut resumed, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        (first, again, disposition)
    });
    assert_eq!(disposition, Some(rdm_core::build::Disposition::AlreadyApplied));
    assert_eq!(again[1..], first[1..], "every event again, replayed from the step events: {again:?}");
}

/// CONTRACT (fabric-node-hooks.md `on_build_started`: "Each time the fabric-primary writes a Build's
/// `rdm.build.started` event ... it sends the application one `on_build_started` notice from that
/// write"; `on_build_ended`: "Each time a Build's terminal state is recorded"; Q256: "a successor that
/// adopts the state calls nothing"; "a fabric-primary lost before it sends it leaves none, and `build.get`
/// is the durable read"): a create Build sends the application `on_build_started` from its
/// `rdm.build.started`, then one `on_build_ended` once its `Complete` is recorded, each under its own
/// span (outcome `sent`). An admin that became fabric-primary after the Build ended adopts it
/// `Complete`, sends nothing, and the Build reads `Complete`; a re-submit that replays the ended Build,
/// or a reconcile tick that asks for its drive again, sends no second notice. Must NOT happen: an ending
/// the application is never told of, an ending told before its opening, or a notice for a replayed or
/// re-read terminal, or from the fabric-primary that adopted it.
#[test]
fn a_build_sends_started_then_ended_and_a_successor_that_adopts_it_sends_none() {
    let cap = capture();
    let (build, after_complete, after_adoption, disposition, state) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let before = build_service(&rig, Duration::from_secs(30)).await;
        let served = serve(before.clone()).await;
        let mut stream = create_rpc(&nodes_over(&served.carrier), &served).await.unwrap();
        let build = stream.accepted().expect("the call opened a Build").build_id.clone();
        let accepted = stream.accepted().cloned().expect("the call opened a Build");
        tokio::spawn({
            let (rig, build) = (rig.clone(), build.clone());
            async move { rig.report_bind(&build, &PathName::new("mesh1.node", "3").unwrap()).await }
        });
        let (frames, end) = collect_frames(&mut stream, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        assert_eq!(frames.last(), Some(&Frame::Complete), "the create's Build completed: {frames:?}");
        let after_complete = rig.told.settled(2).await;
        // A re-submit after the end replays the drive's own record, and the reconcile loop's tick asks
        // for the drive again: neither is a second transition.
        let mut replayed = nodes_over(&served.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let (_, end) = collect_frames(&mut replayed, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        let _ = before.drives.ensure(&before.env, &build);
        let after_replay = rig.told.settled(2).await;
        assert_eq!(after_replay.len(), 2, "a replayed or re-read terminal sends no second notice: {after_replay:?}");
        // The same Build log under a fabric-primary that never drove this Build: it adopts it Complete.
        let after = Arc::new(BuildService { me_path: before.me_path.clone(), me_name: before.me_name.clone(), control: before.control.clone(), drives: Arc::new(Drives::default()), env: before.env.clone(), run: before.run.clone(), primaries: before.primaries.clone(), incarnation: before.incarnation.clone() });
        let successor = serve(after).await;
        let mut resumed = nodes_over(&successor.carrier).resume(WorkflowKind::Create, &accepted).await.unwrap();
        let disposition = resumed.disposition();
        let (_, end) = collect_frames(&mut resumed, Duration::from_secs(10)).await;
        assert!(end.is_none(), "{end:?}");
        let after_adoption = rig.told.settled(2).await;
        let state = rig.builds.read_build(&build).await.unwrap().state;
        (build, after_complete, after_adoption, disposition, state)
    });
    let id = build.to_string();
    let keys: Vec<(String, &str)> = after_complete.iter().map(crate::Notice::key).collect();
    assert_eq!(keys, vec![(id.clone(), "started"), (id.clone(), "complete")], "started at its rdm.build.started, then ended Complete: {after_complete:?}");
    let crate::Notice::Ended(ended) = &after_complete[1] else { unreachable!() };
    assert_eq!(ended.state, rdm_admin::wiring::BuildEndedState::Complete, "{ended:?}");
    assert_eq!(disposition, Some(rdm_core::build::Disposition::AlreadyApplied), "the successor adopted the ended Build");
    assert_eq!(after_adoption.len(), 2, "the successor that adopted the ended Build sends nothing: {after_adoption:?}");
    assert_eq!(state, rdm_admin::build_state::BuildState::Complete, "build.get reads the terminal");
    let spans = cap.spans();
    let logged = build.log().to_string();
    for (name, want) in [("rdm.node_admin.build.update.via-on-build-started", None), ("rdm.node_admin.build.update.via-on-build-ended", Some("complete"))] {
        let sent: Vec<&Value> = spans_named(&spans, name).into_iter().filter(|s| s["attributes"]["build_id"] == logged.as_str()).collect();
        assert_eq!(sent.len(), 1, "one {name} span for the Build: {sent:?}");
        let a = &sent[0]["attributes"];
        assert_eq!(a["outcome"].as_str(), Some("sent"), "{}", sent[0]);
        if let Some(state) = want {
            assert_eq!(a["state"].as_str(), Some(state), "{}", sent[0]);
        }
        eprintln!("span {name} {}", sent[0]);
    }
}

/// CONTRACT (fabric-node-hooks.md `on_build_started`, `on_build_ended`; Q346: a failed step ends its mesh's
/// section and the Build ends `incomplete`): a create whose birth never reports where it bound fails its
/// bind wait, which ends mesh1's section, and the Build ends `incomplete`. The application is told
/// `on_build_started` once and one `on_build_ended` `Incomplete { sections }`, its section naming the
/// step that failed and the reason the stream ends with. Must NOT happen: a second `on_build_started`,
/// or an ending the application is not told of.
#[test]
fn a_build_whose_create_fails_sends_one_started_and_one_incomplete_ended_naming_the_step() {
    let cap = capture();
    let (frames, told) = cap.run(async {
        let rig = Arc::new(Rig::new());
        let served = serve(build_service(&rig, Duration::from_millis(300)).await).await;
        let mut stream = create_rpc(&nodes_over(&served.carrier), &served).await.unwrap();
        let (frames, end) = tokio::time::timeout(Duration::from_secs(20), collect_frames(&mut stream, Duration::from_secs(15))).await.expect("the stream ends within 20 s");
        assert!(end.is_none(), "{end:?}");
        (frames, rig.told.settled(2).await)
    });
    let Some(Frame::Failed { reason, .. }) = frames.last() else { panic!("the create's stream ended Failed: {frames:?}") };
    let id = match &frames[0] {
        Frame::Started { build_id, .. } => build_id.to_string(),
        f => panic!("{f:?}"),
    };
    let keys: Vec<(String, &str)> = told.iter().map(crate::Notice::key).collect();
    assert_eq!(keys, vec![(id.clone(), "started"), (id, "incomplete")], "one started, one incomplete ending: {told:?}");
    let crate::Notice::Ended(ended) = &told[1] else { unreachable!() };
    match &ended.state {
        rdm_admin::wiring::BuildEndedState::Incomplete { sections } => assert!(matches!(sections.as_slice(), [u] if u.mesh == "mesh1" && u.step == "WaitForBind" && reason.contains(&u.reason)), "the ending names the failed step and the stream's reason {reason:?}: {ended:?}"),
        other => panic!("the ending is Incomplete: {other:?}"),
    }
    let spans = cap.spans();
    let ended_spans = spans_named(&spans, "rdm.node_admin.build.update.via-on-build-ended");
    assert_eq!(ended_spans.len(), 1, "{ended_spans:?}");
    assert_eq!(ended_spans[0]["attributes"]["state"].as_str(), Some("incomplete"), "{}", ended_spans[0]);
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
        let served = serve(build_service(&rig, Duration::from_secs(1)).await).await;
        let nodes = nodes_over(&served.carrier);
        let node_id = NodeId::mint();
        let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(node_id.clone()), node_id, incarnation: IncarnationId::mint() };
        // The rename reaches the node's mesh-primary only with a name within the grammar.
        let (mp, route) = (NodeId::mint(), rdm_transport::RouteChoice::Direct);
        for (bad, says) in [("Rpc", "name `Rpc` must contain"), (".old", "name `.old` must contain"), ("", "name `` must contain")] {
            let refused = nodes.name_update(&birth, &mp, bad).await.expect("a typed refusal");
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
            matches!(nodes.delete_connections(&rdm_core::PathName::new("mesh1.node", "1").unwrap(), &rdm_core::status::ConnectionScope::Connections(vec![])).await, Err(CallEnd::Rejected { reason, detail }) if reason == "rejected-connection-scope" && detail.contains("at least one")),
            "an empty Connections list is refused by name"
        );
        let end = nodes.config_get();
        assert!(matches!(end, CallEnd::NotBackedToday { op: NodeOp::ConfigGet, .. }), "{end:?}");
    });
}

/// CONTRACT: every step the create and delete pipelines record is one the client names. The
/// step events of a whole create fold to `node.joined`, `node.started`, `node.created`
/// and `Complete`; the step events of a whole delete fold to the connections-deleted, stopped and
/// deleted events and `Complete`. A step the client does not name is refused by name, never skipped.
#[test]
fn every_pipeline_step_is_named_by_the_client() {
    use rdm_admin::deployment::pipeline::{CreateStep, DeleteStep};
    let view = |op: &rdm_core::OperationId, steps: Vec<&str>| -> rdm_client::BuildView {
        serde_json::from_value(json!({
            "build_id": "3qcfk1kq47m9", "topology": {}, "time": 0, "state": "running",
            "steps": steps.iter().map(|s| json!({"mesh": "mesh1", "operation": op.to_string(), "step": s, "outcome": "complete"})).collect::<Vec<_>>(),
            "last_failure": null
        }))
        .unwrap()
    };
    let rpc_id = NodeId::mint();
    let admin_id = NodeId::mint();
    let create: Vec<&str> = CreateStep::ORDER.iter().map(|s| s.name()).collect();
    let f = rdm_client::fold(&WorkflowKind::Create, &view(&rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&PathName::new("mesh1.node", "3").unwrap())), create)).unwrap();
    assert_eq!(f.frames, [Frame::Event(NodeEvent::Joined), Frame::Event(NodeEvent::Started), Frame::Event(NodeEvent::Created), Frame::Complete]);
    let name: rdm_core::PathName = rdm_core::PathName::new("mesh1.node", "3").unwrap();
    let delete: Vec<&str> = DeleteStep::ORDER.iter().map(|s| s.name()).collect();
    let f = rdm_client::fold(&WorkflowKind::Delete(vec![rpc_id.clone()]), &view(&rdm_core::OperationId::node("node.delete", &rpc_id), delete.clone())).unwrap();
    assert_eq!(
        f.frames,
        [Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Event(NodeEvent::Stopped), Frame::Event(NodeEvent::Deleted), Frame::Complete]
    );
    // A node-admin's workflow is any node's: its roles move by node.primary.update before the operation
    // is dispatched, never as a step of it (the approved proposal node-primary-update-fabric-mesh-and-
    // node-type.md, Part D: the SeatTransfer step goes), so its step events fold to the same events.
    let admin: rdm_core::PathName = rdm_core::PathName::new("mesh1.node-admin", "2").unwrap();
    let f = rdm_client::fold(&WorkflowKind::Delete(vec![admin_id.clone()]), &view(&rdm_core::OperationId::node("node.delete", &admin_id), delete.clone())).unwrap();
    assert_eq!(
        f.frames,
        [Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Event(NodeEvent::Stopped), Frame::Event(NodeEvent::Deleted), Frame::Complete]
    );
    let f = rdm_client::fold(&WorkflowKind::Stop(admin.clone()), &view(&rdm_core::OperationId::node("node.stop", &admin_id), vec!["StopNode", "AwaitNodeLeft", "Complete"])).unwrap();
    assert_eq!(f.frames, [Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Event(NodeEvent::Stopped), Frame::Complete]);
    let f = rdm_client::fold(&WorkflowKind::Stop(name.clone()), &view(&rdm_core::OperationId::node("node.stop", &rpc_id), vec!["StopNode", "AwaitNodeLeft", "Complete"])).unwrap();
    assert_eq!(f.frames, [Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Event(NodeEvent::Stopped), Frame::Complete]);
    // A standalone node.connection.delete { All } ends on the birth's node.connections.deleted: the
    // process is not stopped and nothing is deleted.
    let f = rdm_client::fold(&WorkflowKind::DeleteConnections(name.clone()), &view(&rdm_core::OperationId::node("node.connection.delete", &rpc_id), vec!["DeleteConnections", "Complete"])).unwrap();
    assert_eq!(f.frames, [Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Complete]);
    // A node-admin's delete with only NodeDeleting held resumes at StopNode, as any node's does.
    let r = rdm_client::Resume::from_view(&WorkflowKind::Delete(vec![admin_id.clone()]), &view(&rdm_core::OperationId::node("node.delete", &admin_id), vec!["NodeDeleting"])).unwrap();
    assert_eq!(r.from, Some((rdm_core::OperationId::node("node.delete", &admin_id).to_string(), "StopNode".to_string())));
    // A SeatTransfer step event is a deleted step: refused by name under any target, never folded.
    for (kind, op, steps) in [
        (WorkflowKind::Delete(vec![rpc_id.clone()]), rdm_core::OperationId::node("node.delete", &rpc_id), vec!["NodeDeleting", "SeatTransfer"]),
        (WorkflowKind::Stop(name.clone()), rdm_core::OperationId::node("node.stop", &rpc_id), vec!["SeatTransfer"]),
        (WorkflowKind::Stop(admin.clone()), rdm_core::OperationId::node("node.stop", &admin_id), vec!["SeatTransfer"]),
    ] {
        assert_eq!(rdm_client::fold(&kind, &view(&op, steps)).unwrap_err(), CallEnd::UnrecognisedStepEvent { operation: op.to_string(), step: "SeatTransfer".into() }, "{kind:?}");
    }
    let unknown = view(&rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&PathName::new("mesh1.node", "3").unwrap())), vec!["AllocateIdentity", "InventedStep"]);
    assert_eq!(
        rdm_client::fold(&WorkflowKind::Create, &unknown).unwrap_err(),
        CallEnd::UnrecognisedStepEvent { operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "3").unwrap())).to_string(), step: "InventedStep".into() }
    );
}

/// Deletion progress follows step events, independently of path or name spelling.
#[test]
fn deletion_progress_does_not_infer_lifecycle_state_from_path_or_name() {
    let deleted = NodeId::mint();
    let operation = rdm_core::OperationId::node("node.delete", &deleted);
    let view = |steps: Vec<&str>| -> rdm_client::BuildView {
        serde_json::from_value(json!({
            "build_id": "3qcfk1kq47m9", "topology": {}, "time": 0, "state": "running",
            "steps": steps.iter().map(|s| json!({"mesh": "mesh1", "operation": operation.to_string(), "step": s, "outcome": "complete"})).collect::<Vec<_>>(),
            "last_failure": null
        })).unwrap()
    };
    {
        let kind = WorkflowKind::Delete(vec![deleted.clone()]);
        let pending = rdm_client::Resume::from_view(&kind, &view(vec!["NodeDeleting"])).unwrap();
        assert_eq!(pending.from, Some((operation.to_string(), "StopNode".into())), "a spelling is not proof of a stopped birth");
        let folded = rdm_client::fold(&kind, &view(vec!["NodeDeleting", "TerminateRuntime", "NodeDeleted", "Complete"])).unwrap();
        assert_eq!(folded.frames, [Frame::Event(NodeEvent::Deleted), Frame::Complete]);
    }
}

/// One section's run as its mesh-primary streams it to the fabric-primary: its frames, then its end.
struct Script(std::collections::VecDeque<InnerItem>);

#[async_trait::async_trait]
impl InnerStream for Script {
    async fn next(&mut self) -> InnerItem {
        self.0.pop_front().unwrap_or(InnerItem::Ended)
    }
}

/// CONTRACT: a Build call asked of a node-admin that does not hold the fabric-primary role is refused
/// `not-fabric-primary`, naming the fabric-primary that admin sees, before either `BuildService` admits anything,
/// and the refusal has its own span as every other Build refusal has (one honest name per reason,
/// `rdm.node_admin.build.reject.via-<reason>`): `rdm.node_admin.build.reject.via-not-fabric-primary` carrying the
/// call's route, the refusing admin and the fabric-primary it named. No `build.create` or `build.update`
/// span opens: nothing was admitted.
#[test]
fn a_build_call_to_an_admin_that_is_not_fabric_primary_is_refused_under_its_own_reject_span_naming_the_fabric_primary() {
    let cap = capture();
    let end = cap.run(async {
        let rig = Arc::new(Rig::new());
        let served = serve(build_service_not_fabric_primary(&rig).await).await;
        served.carrier.create(rdm_core::build::BuildChange::DeleteFabric).await.err()
    });
    match &end {
        Some(CallEnd::Rejected { reason, detail }) => {
            assert_eq!(reason, "not-fabric-primary", "the admin without the role refuses by name: {detail}");
            assert!(detail.contains("already followed"), "the redirect to the fabric-primary the admin sees is followed once: {detail}");
        }
        other => panic!("a Build call to an admin without the fabric-primary role ends Rejected not-fabric-primary, not {other:?}"),
    }
    let spans = cap.spans();
    let refused = spans_named(&spans, "rdm.node_admin.build.reject.via-not-fabric-primary");
    // Once at the first call and once at the redirect it named, the same `BuildService`.
    assert_eq!(refused.len(), 2, "the refusal has its own reject span: {spans:?}");
    let a = &refused[0]["attributes"];
    assert_eq!((a["route"].as_str(), a["node_path"].as_str(), a["node_name"].as_str(), a["fabric_primary"].as_str().is_some_and(|f| !f.is_empty())), (Some("build.create"), Some("mesh1.node-admin"), Some("2"), true), "the reject span names the call, the refusing admin and the fabric-primary it named: {}", refused[0]);
    for name in ["rdm.node_admin.build.create.via-node-rpc", "rdm.node_admin.build.update.via-node-rpc"] {
        assert!(spans_named(&spans, name).is_empty(), "nothing was admitted: no {name} span: {spans:?}");
    }
}
