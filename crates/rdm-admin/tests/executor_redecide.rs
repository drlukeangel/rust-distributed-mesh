//! i143 acceptance (rafka-v2 #2892, finding 1), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2892-unit`, which exports `I143_ACCEPTANCE_DIR`; the
//! cell leaves `result.json` (its direct observations) and `spans.json` (every span of its own
//! runtime, captured by an in-memory OTel exporter) there.
//!
//! A launched birth carries its launcher's endpoint as its only seed. A create re-run by a
//! successor executor (another birth of the lost admin, another endpoint) must not hand on a
//! birth that never joined: that runtime names the lost admin and can never join. It decides the
//! birth afresh, under its own endpoint. Receipts that do not depend on the executor stay.

use rdm_core::{FabricId, IncarnationId, NodeId};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rdm_admin::deployment::endpoint::NODE_SPEC;
use rdm_admin::join::Joins;
use rdm_admin::deployment::pipeline::{
    CreateRequest, CreateStep, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, Timeouts, TopologySink,
};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::model::{DeploymentId, Node, PathName, ProviderKind};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2892/unit").join(cell),
    }
}

/// This cell's own span capture: the product span pipeline (`rdm_transport::span_capture`: its span filter and
/// `BatchSpanProcessor`) in front of an in-memory exporter, behind a dispatcher installed on
/// every thread of the cell's own runtime.
struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
    service: String,
}

fn capture(cell: &str) -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let service = format!("i143-2892-{cell}");
    let (capture, layer) = rdm_transport::span_capture(&service, exporter.clone());
    Capture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)), service }
}

impl Capture {
    fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        let d = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(f)
    }

    fn spans(&self) -> Vec<Value> {
        self.capture.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let finished = self.exporter.get_finished_spans().unwrap();
        crate::assert_own_story(finished.iter().map(|s| s.name.as_ref()));
        finished
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({
                    "name": s.name,
                    "trace_id": s.span_context.trace_id().to_string(),
                    "span_id": s.span_context.span_id().to_string(),
                    "parent_span_id": s.parent_span_id.to_string(),
                    "resource": {"service.name": self.service},
                    "attributes": attributes,
                })
            })
            .collect()
    }
}

/// Where an executor's pipeline dies (its future is dropped there).
#[derive(Clone, Copy, PartialEq)]
enum Dies {
    /// Never: the run completes.
    Not,
    /// Waiting for the birth's report, which does not come while the executor lives: the runtime is
    /// spawned, no join is admitted, no bind or join receipt (node-create.md step 5: the waits complete
    /// from the validated report alone). The birth keeps offering its report, as a real node retries
    /// its `JoinNode` until an admin answers it.
    BeforeJoin,
    /// Waiting for Ready: the birth has joined.
    AfterJoin,
}

/// One host's runtimes, shared by every executor of the cell: what is launched with which seeds,
/// what is terminated, what still runs.
struct Fleet {
    dies: Arc<Mutex<Dies>>,
    reached: Notify,
    next_pid: AtomicU32,
    spawned: Mutex<Vec<Spawn>>,
    terminated: Mutex<Vec<u32>>,
    /// Each executor's deployments, by the endpoint key its launches carry as their seed: the
    /// admin a launched birth reports its join to.
    executors: Mutex<BTreeMap<String, Arc<Joins>>>,
    /// The fabric's nodes store, shared by every executor: the node records each create writes, and
    /// the part each birth writes itself at its join (its bound addresses): what is durable (D67).
    store: Arc<rdm_admin::storage::MemoryNodesStorage>,
}

#[derive(Clone, Debug)]
struct Spawn {
    pid: u32,
    node_id: NodeId,
    seeds: String,
    data_dir: PathBuf,
    handle: DeploymentHandle,
}

impl Fleet {
    fn new() -> Arc<Self> {
        Arc::new(Self { dies: Arc::new(Mutex::new(Dies::Not)), reached: Notify::new(), next_pid: AtomicU32::new(40_000), spawned: Mutex::new(vec![]), terminated: Mutex::new(vec![]), executors: Mutex::new(BTreeMap::new()), store: Arc::default() })
    }
    fn spawns(&self) -> Vec<Spawn> {
        self.spawned.lock().unwrap().clone()
    }
    fn running(&self, pid: u32) -> bool {
        self.spawned.lock().unwrap().iter().any(|s| s.pid == pid) && !self.terminated.lock().unwrap().contains(&pid)
    }
}

const DOMAIN: &str = "i143-2892";

/// The digest a launched node reports at its join, from the launch it was handed.
fn fake_node_digest(spec: &ResolvedNodeLaunch, runtime: Option<rdm_core::RuntimeFact>) -> rdm_core::MeshDigest {
    let env = |k: &str| spec.env.get(k).cloned().unwrap_or_else(|| panic!("the launch names {k}"));
    let key_hex = std::fs::read_to_string(spec.data_dir.join("node-key")).expect("PrepareStorage wrote the node key");
    let key = iroh::SecretKey::from_bytes(&hex::decode(key_hex.trim()).unwrap().try_into().unwrap());
    rdm_core::MeshDigest { 
        fabric_id: rdm_core::FabricId::parse(&env("RDM_FABRIC_ID")).unwrap(),
        node: rdm_core::MeshNode { mesh: spec.mesh.clone(), r#type: spec.r#type.clone(),
            node_id: NodeId::parse(&env("RDM_NODE_ID")).unwrap(),
            path: (env("RDM_NODE_PATH")).to_string(), name: (env("RDM_NODE_NAME")).to_string(),
            endpoint_id: rdm_core::EndpointId(key.public().to_string()),
            transport_addr: "127.0.0.1:34567".parse().unwrap(),
            incarnation: IncarnationId::parse(&env("RDM_INCARNATION_ID")).unwrap(),
            supersedes: spec.env.get("RDM_SUPERSEDES").filter(|s| !s.is_empty()).map(|s| IncarnationId::parse(s).unwrap()),
            runtime,
        },
        status: rdm_core::MemberStatus::Pending,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: Some(spec.data_dir.display().to_string()),
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    }
}

#[async_trait::async_trait]
impl DeploymentProvider for Fleet {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        DOMAIN.into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
        let pid = self.next_pid.fetch_add(1, Ordering::SeqCst);
        let handle = DeploymentHandle {
            deployment_id: spec.deployment_id.clone(),
            provider: ProviderKind::Process,
            pid: Some(pid),
            start: Some(1),
            container: None,
            domain: Some(DOMAIN.into()),
        };
        // The birth reports where it bound to the executor that launched it, as a real node does at
        // its join, once that executor has registered the deployment (after `DeployRuntime`). While
        // the executor is to die before the join, the report does not come; the birth keeps offering it
        // and is heard by whichever registration of its deployment stands once the executor is gone.
        if let Some(joins) = spec.env.get("RDM_SEEDS").and_then(|s| s.split('@').next()).and_then(|k| self.executors.lock().unwrap().get(k).cloned()) {
            let digest = fake_node_digest(spec, handle.fact());
            let dies = self.dies.clone();
            let store = self.store.clone();
            tokio::spawn(async move {
                for _ in 0..500 {
                    let withheld = *dies.lock().unwrap() == Dies::BeforeJoin;
                    if !withheld && matches!(joins.standing(&digest, &[]), rdm_admin::join::Standing::Deployed { .. }) {
                        joins.report(&rdm_admin::join::JoinReport { digest: digest.clone(), listeners: Vec::new() });
                        let bound = rdm_node::node_records::NodeProperties {
                            birth: rdm_node::node_records::RecordBirth { node_id: digest.node.node_id.clone(), incarnation_id: digest.node.incarnation.clone() },
                            config: Default::default(),
                            tags: Default::default(),
                            endpoint_id: Some(digest.node.endpoint_id.clone()),
                            transport_addr: Some(digest.node.transport_addr),
                            listeners: Vec::new(),
                        };
                        rdm_admin::storage::NodesStorage::put_properties(&*store, &bound).await.unwrap();
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            });
        }
        self.spawned.lock().unwrap().push(Spawn {
            pid,
            node_id: NodeId::parse(spec.env.get("RDM_NODE_ID").expect("the launch names its node")).unwrap(),
            seeds: spec.env.get("RDM_SEEDS").cloned().unwrap_or_default(),
            data_dir: spec.data_dir.clone(),
            handle: handle.clone(),
        });
        Ok(handle)
    }
    async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        self.terminated.lock().unwrap().push(h.pid.unwrap());
        Ok(())
    }
    async fn inspect(&self, h: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
        // The executor waiting for a report that never comes dies at its wait's inspection of the runtime.
        if *self.dies.lock().unwrap() == Dies::BeforeJoin {
            self.reached.notify_one();
            std::future::pending::<()>().await;
        }
        if self.running(h.pid.unwrap()) {
            DeploymentStatus::Running
        } else {
            DeploymentStatus::Exited { code: Some(143) }
        }
    }
    async fn signal_stop(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        use rdm_admin::deployment::provider::Found;
        Ok(match self.spawned.lock().unwrap().iter().find(|s| s.handle.deployment_id == spec.deployment_id) {
            Some(s) if self.running(s.pid) => Found::Running(s.handle.clone()),
            Some(_) => Found::Exited(None),
            None => Found::NeverLaunched,
        })
    }
}

#[async_trait::async_trait]
impl NodeObserver for Fleet {
    async fn joined(&self, node_id: &NodeId, _: &IncarnationId) -> Option<Publication> {
        let s = self.spawned.lock().unwrap().iter().rev().find(|s| &s.node_id == node_id)?.clone();
        Some(Publication { runtime: s.handle.fact(), data_dir: Some(s.data_dir.display().to_string()) })
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        if *self.dies.lock().unwrap() == Dies::AfterJoin {
            self.reached.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

/// An executor's sink: the node rows it writes, as mesh1's primary, to the fleet's store.
struct Rows(Arc<rdm_admin::node_rows::NodeRows>);
impl TopologySink for Rows {
    fn publish(&self, _: Node) {}
    fn remove(&self, _: &PathName) {}
    fn remove_birth(&self, _: &PathName, _: &rdm_core::NodeId, _: &rdm_core::IncarnationId) {}
    fn node_rows(&self) -> Option<Arc<rdm_admin::node_rows::NodeRows>> {
        Some(self.0.clone())
    }
}

/// An executor: one admin birth, named by its own endpoint (the seed every launch it makes carries).
struct Executor {
    template: LaunchTemplate,
    joins: Arc<Joins>,
    rows: Rows,
    /// The Build state this executor writes its step events through, as its own birth.
    builds: rdm_admin::build_run::FramedBuilds,
}

fn executor(fleet: &Fleet, data_root: &std::path::Path, key: &str, store: &Arc<MemoryBuildStateAdapter>, fabric_id: &FabricId) -> Executor {
    let joins = Arc::new(Joins::default());
    fleet.executors.lock().unwrap().insert(key.to_string(), joins.clone());
    let launcher = rdm_core::launch::Launcher { mesh: "mesh1".into(), path: "mesh1.node-admin".into(), name: "1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let mut admin = Node::allocated(PathName::new(&launcher.path, &launcher.name).unwrap(), "mesh1", "node-admin");
    admin.node_id = launcher.node_id.clone();
    admin.incarnation_id = Some(launcher.incarnation.clone());
    admin.status = rdm_admin::model::NodeStatus::Ready;
    admin.is_primary = true;
    admin.is_fabric_primary = true;
    let view = Arc::new(tokio::sync::RwLock::new(rdm_admin::topology::Topology {
        fabric: rdm_admin::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: rdm_admin::model::ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![rdm_admin::model::Mesh { id: Some(rdm_core::MeshId::mint()), name: "mesh1".into(), status: rdm_admin::model::ScopeStatus::Ready }],
        nodes: vec![admin.clone()],
    }));
    let builds_store = store;
    let store: Arc<dyn rdm_admin::storage::NodesStorage> = fleet.store.clone();
    let rows = Rows(Arc::new(rdm_admin::node_rows::NodeRows::new(admin.location(), admin.node_id.clone(), view, store)));
    let inner: Arc<dyn BuildStateAdapter> = builds_store.clone();
    let mut events = rdm_admin::events_storage::EventsWriter::on(inner.events(), fabric_id, &launcher.node_id);
    events.source.incarnation = launcher.incarnation.clone();
    let builds = rdm_admin::build_run::FramedBuilds::new(inner, Arc::new(rdm_admin::build_run::SectionRuns::default())).writing_steps(events, "mesh1");
    Executor {
        template: LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/testkit-node".into(),
            seeds: vec![(key.into(), "127.0.0.1:34000".parse().unwrap())],
            launcher,
            env: BTreeMap::new(),
            data_root: data_root.to_path_buf(),
            store_dir: data_root.to_path_buf(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        },
        joins,
        rows,
        builds,
    }
}

impl Executor {
    fn pipeline<'a>(&'a self, fleet: &'a Fleet) -> DeploymentPipeline<'a> {
        DeploymentPipeline {
            provider: fleet,
            joins: &self.joins,
            observer: fleet,
            sink: &self.rows,
            lifecycle: &NoLifecycleEvents,
            builds: &self.builds,
            template: &self.template,
            timeouts: Timeouts::default(),
        }
    }
}

async fn accepted_build(builds: &dyn BuildStateAdapter) -> BuildId {
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted {
            build_id: build_id.clone(),
            topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()),
            change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() },
            source: None, time: 0,
        })
        .await
        .unwrap();
    build_id
}

fn request(build_id: &BuildId, node: &PathName) -> CreateRequest {
    CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(node)), node_id: rdm_core::fixture_node_id(node), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(build_id.clone()), path: node.path.clone(), name: node.name.clone(), spec: &NODE_SPEC, data_dir: None, held_runtimes: Vec::new(), entry: None }
}

/// A run by `lost`, killed at `dies`.
async fn lost_run(fleet: &Fleet, lost: &Executor, build_id: &BuildId, node: &PathName, dies: Dies) {
    *fleet.dies.lock().unwrap() = dies;
    let pipeline = lost.pipeline(fleet);
    let req = request(build_id, node);
    tokio::select! {
        r = pipeline.create(&req) => panic!("the lost executor was to die mid-run, but finished: {:?}", r.map(|c| c.node.name)),
        _ = fleet.reached.notified() => {}
    }
    *fleet.dies.lock().unwrap() = Dies::Not;
}

fn complete_steps(view: &rdm_admin::build_state::BuildProjection, step: CreateStep, node: &PathName) -> usize {
    view.steps.iter().filter(|r| r.step == step.name() && r.operation == rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(node)).to_string() && r.outcome == StepOutcome::Complete).count()
}

/// CONTRACT (#2892): a create re-run by a successor executor (the lost admin's recovery, at
/// another endpoint) does not hand on a birth that was launched by the lost executor and never
/// joined: that runtime's only seed is the lost admin's endpoint, where a dial can reach another
/// process (UnknownIssuer) and the birth never joins. The successor refuses it by name, stops it,
/// and decides the birth afresh: a new birth of the Build's node.id (D58), launched under its own endpoint. The same birth
/// handed on after it joined (the seed did its work) is kept; an executor re-running its own work
/// reuses it.
#[test]
fn successor_executor_redecides_outputs_owned_by_lost_incarnation() {
    let cell = "successor_executor_redecides_outputs_owned_by_lost_incarnation";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let data_root = std::env::temp_dir().join(format!("i143-2892-{}", NodeId::mint()));
    let result = cap.run(async {
        let fleet = Fleet::new();
        let fabric_id = FabricId::mint();
        let store = Arc::new(MemoryBuildStateAdapter::new());
        let builds = rdm_admin::build_run::FramedBuilds::stepping(store.clone(), &fabric_id, &NodeId::mint(), "mesh1");
        let lost = executor(&fleet, &data_root, "lostadminkey", &store, &fabric_id);
        let successor = executor(&fleet, &data_root, "successoradminkey", &store, &fabric_id);

        // CONTROL: the same executor re-runs its own cut-short run: its birth is handed on.
        let own = rdm_core::PathName::new("mesh1.node", "1").unwrap();
        let build_own = accepted_build(&builds).await;
        lost_run(&fleet, &lost, &build_own, &own, Dies::BeforeJoin).await;
        let own_birth = fleet.spawns().last().cloned().expect("the lost run launched the birth");
        let again = lost.pipeline(&fleet).create(&request(&build_own, &own)).await.unwrap_or_else(|e| panic!("control re-run: {e}"));
        assert_eq!(fleet.spawns().len(), 1, "the executor's own re-run launches no second runtime");
        assert_eq!(again.node.node_id, own_birth.node_id, "the executor's own re-run keeps its decided identity");
        assert!(fleet.terminated.lock().unwrap().is_empty(), "nothing of the executor's own is stopped");

        // CONTROL: a birth that JOINED under the lost executor is handed on by the successor.
        let joined = rdm_core::PathName::new("mesh1.node", "2").unwrap();
        let build_joined = accepted_build(&builds).await;
        lost_run(&fleet, &lost, &build_joined, &joined, Dies::AfterJoin).await;
        let spawns_before = fleet.spawns().len();
        let joined_birth = fleet.spawns().last().cloned().unwrap();
        let kept = successor.pipeline(&fleet).create(&request(&build_joined, &joined)).await.unwrap_or_else(|e| panic!("joined re-run: {e}"));
        assert_eq!(fleet.spawns().len(), spawns_before, "a birth that joined is not launched again");
        assert_eq!(kept.node.node_id, joined_birth.node_id);
        assert!(fleet.running(joined_birth.pid), "a birth that joined keeps running");

        // THE FINDING: the lost executor's launch never joined; its seed is the lost endpoint.
        let stranded = rdm_core::PathName::new("mesh1.node", "3").unwrap();
        let build_stranded = accepted_build(&builds).await;
        lost_run(&fleet, &lost, &build_stranded, &stranded, Dies::BeforeJoin).await;
        let dead = fleet.spawns().last().cloned().unwrap();
        assert!(dead.seeds.contains("lostadminkey"), "the lost run seeded its birth with the lost admin: {}", dead.seeds);
        let spawns_before = fleet.spawns().len();
        let redone = successor.pipeline(&fleet).create(&request(&build_stranded, &stranded)).await.unwrap_or_else(|e| panic!("successor re-run: {e}"));
        let spawns = fleet.spawns();
        assert_eq!(spawns.len(), spawns_before + 1, "the successor launches the birth afresh");
        let fresh = spawns.last().unwrap();
        assert!(!fresh.seeds.contains("lostadminkey") && fresh.seeds.contains("successoradminkey"), "the new birth is seeded with the successor only: {}", fresh.seeds);
        // The node.id is the Build's own for the create (D58): a birth decided afresh is a new deployment of it.
        assert_ne!(fresh.handle.deployment_id, dead.handle.deployment_id, "a fresh birth: the stranded runtime is not this birth's");
        assert_eq!(redone.node.node_id, fresh.node_id);
        assert!(!fleet.running(dead.pid), "the stranded runtime that names the lost admin is stopped");
        assert!(fleet.running(fresh.pid));

        let view = builds.read_build(&build_stranded).await.unwrap();
        json!({
            "cell": cell,
            "lost_executor_seed": dead.seeds,
            "stranded_runtime": {"pid": dead.pid, "node_id": dead.node_id.to_string(), "terminated": !fleet.running(dead.pid)},
            "successor_birth": {"pid": fresh.pid, "node_id": fresh.node_id.to_string(), "seeds": fresh.seeds},
            "deploy_runtime_complete_steps": complete_steps(&view, CreateStep::DeployRuntime, &stranded),
            "joined_birth_kept": {"node_id": joined_birth.node_id.to_string(), "pid": joined_birth.pid},
            "own_rerun_kept": own_birth.node_id.to_string(),
            "terminated_pids": fleet.terminated.lock().unwrap().clone(),
            "lost_executor": lost.template.launcher.node_id.to_string(),
            "successor_executor": successor.template.launcher.node_id.to_string(),
        })
    });
    let spans = cap.spans();
    let rejections: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.deployment.reject.via-lost-executor").collect();
    assert_eq!(rejections.len(), 1, "exactly the stranded birth is refused by name: {} such spans", rejections.len());
    let a = &rejections[0]["attributes"];
    assert_eq!((&a["node_path"], &a["node_name"]), (&serde_json::json!("mesh1.node"), &serde_json::json!("3")));
    assert!(a["recorded_executor"].as_str().unwrap().contains(result["lost_executor"].as_str().unwrap()), "{a}");
    assert!(a["executor"].as_str().unwrap().contains(result["successor_executor"].as_str().unwrap()), "{a}");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    let mut result = result;
    result["rejection_span_attributes"] = rejections[0]["attributes"].clone();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    let _ = std::fs::remove_dir_all(&data_root);
}
