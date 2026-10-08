//! i143 acceptance (rafka-v2 #2892, finding 1), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2892-unit`, which exports `I143_ACCEPTANCE_DIR`; the
//! cell leaves `result.json` (its direct observations) and `spans.json` (every span of its own
//! runtime, captured by an in-memory OTel exporter) there.
//!
//! A launched birth carries its launcher's endpoint as its only seed. A create re-run by a
//! successor executor (another birth of the lost admin, another endpoint) must not hand on a
//! birth that never joined: that runtime names the lost admin and can never join. It decides the
//! birth afresh, under its own endpoint. Receipts that do not depend on the executor stay.

use rafka_mesh_entity::{FabricId, IncarnationId, NodeId};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::join::Joins;
use rafka_node_admin_core::deployment::pipeline::{
    CreateRequest, CreateStep, DeploymentPipeline, DrainOutcome, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, Timeouts, TopologySink,
};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::model::{DeploymentId, Node, PathName, ProviderKind};
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

/// This cell's own span capture: an in-memory OTel exporter behind a dispatcher installed on
/// every thread of the cell's own runtime.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    provider: opentelemetry_sdk::trace::TracerProvider,
    dispatch: tracing::Dispatch,
    service: String,
}

fn capture(cell: &str) -> Capture {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let service = format!("i143-2892-{cell}");
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .with_resource(opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new("service.name", service.clone())]))
        .build();
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("i143-2892"));
    Capture { exporter, provider, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)), service }
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
        let _ = self.provider.force_flush();
        self.exporter
            .get_finished_spans()
            .unwrap()
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
    /// Waiting for the birth to join the mesh: the runtime is spawned, no join receipt.
    BeforeJoin,
    /// Waiting for Ready: the birth has joined.
    AfterJoin,
}

/// One host's runtimes, shared by every executor of the cell: what is launched with which seeds,
/// what is terminated, what still runs.
struct Fleet {
    dies: Mutex<Dies>,
    reached: Notify,
    next_pid: AtomicU32,
    spawned: Mutex<Vec<Spawn>>,
    terminated: Mutex<Vec<u32>>,
    /// Each executor's deployments, by the endpoint key its launches carry as their seed: the
    /// admin a launched birth reports its join to.
    executors: Mutex<BTreeMap<String, Arc<Joins>>>,
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
        Arc::new(Self { dies: Mutex::new(Dies::Not), reached: Notify::new(), next_pid: AtomicU32::new(40_000), spawned: Mutex::new(vec![]), terminated: Mutex::new(vec![]), executors: Mutex::new(BTreeMap::new()) })
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
fn fake_node_digest(spec: &ResolvedNodeLaunch, runtime: Option<rafka_mesh_entity::RuntimeFact>) -> rafka_mesh_entity::MeshDigest {
    let env = |k: &str| spec.env.get(k).cloned().unwrap_or_else(|| panic!("the launch names {k}"));
    let key_hex = std::fs::read_to_string(spec.data_dir.join("node-key")).expect("PrepareStorage wrote the node key");
    let key = iroh::SecretKey::from_bytes(&hex::decode(key_hex.trim()).unwrap().try_into().unwrap());
    rafka_mesh_entity::MeshDigest {
        fabric_id: rafka_mesh_entity::FabricId::parse(&env("RDM_FABRIC_ID")).unwrap(),
        node: rafka_mesh_entity::MeshNode {
            node_id: NodeId::parse(&env("RDM_NODE_ID")).unwrap(),
            name: env("RDM_NODE_NAME").parse().unwrap(),
            endpoint_id: rafka_mesh_entity::EndpointId(key.public().to_string()),
            transport_addr: "127.0.0.1:34567".parse().unwrap(),
            incarnation: IncarnationId(env("RDM_INCARNATION_ID")),
            supersedes: spec.env.get("RDM_SUPERSEDES").filter(|s| !s.is_empty()).cloned().map(IncarnationId),
            runtime,
        },
        status: rafka_mesh_entity::MemberStatus::Pending,
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: Some(spec.data_dir.display().to_string()),
        mesh_id: None,
        in_flight: None,
        extra: Default::default(),
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
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
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
        // its join, once that executor has registered the deployment (after `DeployRuntime`).
        if let Some(joins) = spec.env.get("RDM_SEEDS").and_then(|s| s.split('@').next()).and_then(|k| self.executors.lock().unwrap().get(k).cloned()) {
            let digest = fake_node_digest(spec, handle.fact());
            tokio::spawn(async move {
                for _ in 0..500 {
                    if joins.standing(&digest) == rafka_node_admin_core::join::Standing::Deployed {
                        joins.report(&digest);
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
    async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        self.terminated.lock().unwrap().push(h.pid.unwrap());
        Ok(())
    }
    async fn inspect(&self, h: &DeploymentHandle) -> DeploymentStatus {
        if self.running(h.pid.unwrap()) {
            DeploymentStatus::Running
        } else {
            DeploymentStatus::Exited { code: Some(143) }
        }
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, spec: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
        self.spawned.lock().unwrap().iter().find(|s| s.handle.deployment_id == spec.deployment_id && self.running(s.pid)).map(|s| s.handle.clone())
    }
}

#[async_trait::async_trait]
impl NodeObserver for Fleet {
    async fn joined(&self, node_id: &NodeId, _: &IncarnationId) -> Option<Publication> {
        if *self.dies.lock().unwrap() == Dies::BeforeJoin {
            self.reached.notify_one();
            std::future::pending::<()>().await;
        }
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
    async fn drain(&self, _: &Node) -> DrainOutcome {
        DrainOutcome::NotSent { reason: "this cell retires nothing".into() }
    }
    async fn drained(&self, _: &Node) -> bool {
        true
    }
    async fn admission_closed(&self, _: &Node) -> Result<(), String> {
        Ok(())
    }
}

struct Discard;
impl TopologySink for Discard {
    fn publish(&self, _: Node) {}
    fn remove(&self, _: &PathName) {}
}

/// An executor: one admin birth, named by its own endpoint (the seed every launch it makes carries).
struct Executor {
    template: LaunchTemplate,
    joins: Arc<Joins>,
}

fn executor(fleet: &Fleet, data_root: &std::path::Path, key: &str) -> Executor {
    let joins = Arc::new(Joins::default());
    fleet.executors.lock().unwrap().insert(key.to_string(), joins.clone());
    Executor {
        template: LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rafka-rpc-node".into(),
            seeds: vec![(key.into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: data_root.to_path_buf(),
        },
        joins,
    }
}

impl Executor {
    fn pipeline<'a>(&'a self, fleet: &'a Fleet, builds: &'a MemoryBuildStateAdapter) -> DeploymentPipeline<'a> {
        DeploymentPipeline {
            provider: fleet,
            joins: &self.joins,
            observer: fleet,
            sink: &Discard,
            lifecycle: &NoLifecycleEvents,
            builds,
            template: &self.template,
            timeouts: Timeouts::default(),
        }
    }
}

async fn accepted_build(builds: &MemoryBuildStateAdapter) -> BuildId {
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted {
            build_id: build_id.clone(),
            topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"),
            submitted_change: None,
            submitted_at_ms: 0,
        })
        .await
        .unwrap();
    build_id
}

fn request(build_id: &BuildId, attempt: u32, node: &PathName) -> CreateRequest {
    CreateRequest { build_id: build_id.clone(), attempt, node: node.clone(), spec: &RPC_NODE, restart_of: None, mesh_seeds: Vec::new(), mesh_primary: false }
}

/// Attempt 1 by `lost`, killed at `dies`.
async fn lost_attempt(fleet: &Fleet, lost: &Executor, builds: &MemoryBuildStateAdapter, build_id: &BuildId, node: &PathName, dies: Dies) {
    *fleet.dies.lock().unwrap() = dies;
    let pipeline = lost.pipeline(fleet, builds);
    let req = request(build_id, 1, node);
    tokio::select! {
        r = pipeline.create(&req) => panic!("the lost executor was to die mid-run, but finished: {:?}", r.map(|c| c.node.name)),
        _ = fleet.reached.notified() => {}
    }
    *fleet.dies.lock().unwrap() = Dies::Not;
}

fn complete_attempts(view: &rafka_node_admin_core::build_state::BuildProjection, step: CreateStep, node: &PathName) -> Vec<u32> {
    view.steps.iter().filter(|r| r.step == step.name() && r.operation == format!("create-node:{node}") && r.outcome == StepOutcome::Complete).map(|r| r.attempt).collect()
}

/// CONTRACT (#2892): a create re-run by a successor executor (the lost admin's recovery, at
/// another endpoint) does not hand on a birth that was launched by the lost executor and never
/// joined: that runtime's only seed is the lost admin's endpoint, where a dial can reach another
/// process (UnknownIssuer) and the birth never joins. The successor refuses it by name, stops it,
/// and decides the birth afresh: a new identity, launched under its own endpoint. The same birth
/// handed on after it joined (the seed did its work), and a decision that names no executor (the
/// identity of a node never launched), are kept; an executor re-running its own work reuses it.
#[test]
fn successor_executor_redecides_outputs_owned_by_lost_incarnation() {
    let cell = "successor_executor_redecides_outputs_owned_by_lost_incarnation";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let data_root = std::env::temp_dir().join(format!("i143-2892-{}", NodeId::mint()));
    let result = cap.run(async {
        let fleet = Fleet::new();
        let builds = MemoryBuildStateAdapter::new();
        let lost = executor(&fleet, &data_root, "lostadminkey");
        let successor = executor(&fleet, &data_root, "successoradminkey");

        // CONTROL: the same executor re-runs its own cut-short run: its birth is handed on.
        let own = "mesh1.rpc.1".parse::<PathName>().unwrap();
        let build_own = accepted_build(&builds).await;
        lost_attempt(&fleet, &lost, &builds, &build_own, &own, Dies::BeforeJoin).await;
        let own_birth = fleet.spawns().last().cloned().expect("attempt 1 launched the birth");
        let again = lost.pipeline(&fleet, &builds).create(&request(&build_own, 2, &own)).await.unwrap_or_else(|e| panic!("control re-run: {e}"));
        assert_eq!(fleet.spawns().len(), 1, "the executor's own re-run launches no second runtime");
        assert_eq!(again.node.node_id, own_birth.node_id, "the executor's own re-run keeps its decided identity");
        assert!(fleet.terminated.lock().unwrap().is_empty(), "nothing of the executor's own is stopped");

        // CONTROL: a birth that JOINED under the lost executor is handed on by the successor.
        let joined = "mesh1.rpc.2".parse::<PathName>().unwrap();
        let build_joined = accepted_build(&builds).await;
        lost_attempt(&fleet, &lost, &builds, &build_joined, &joined, Dies::AfterJoin).await;
        let spawns_before = fleet.spawns().len();
        let joined_birth = fleet.spawns().last().cloned().unwrap();
        let kept = successor.pipeline(&fleet, &builds).create(&request(&build_joined, 2, &joined)).await.unwrap_or_else(|e| panic!("joined re-run: {e}"));
        assert_eq!(fleet.spawns().len(), spawns_before, "a birth that joined is not launched again");
        assert_eq!(kept.node.node_id, joined_birth.node_id);
        assert!(fleet.running(joined_birth.pid), "a birth that joined keeps running");

        // THE FINDING: the lost executor's launch never joined; its seed is the lost endpoint.
        let stranded = "mesh1.rpc.3".parse::<PathName>().unwrap();
        let build_stranded = accepted_build(&builds).await;
        lost_attempt(&fleet, &lost, &builds, &build_stranded, &stranded, Dies::BeforeJoin).await;
        let dead = fleet.spawns().last().cloned().unwrap();
        assert!(dead.seeds.contains("lostadminkey"), "attempt 1 seeded its birth with the lost admin: {}", dead.seeds);
        let spawns_before = fleet.spawns().len();
        let redone = successor.pipeline(&fleet, &builds).create(&request(&build_stranded, 2, &stranded)).await.unwrap_or_else(|e| panic!("successor re-run: {e}"));
        let spawns = fleet.spawns();
        assert_eq!(spawns.len(), spawns_before + 1, "the successor launches the birth afresh");
        let fresh = spawns.last().unwrap();
        assert!(!fresh.seeds.contains("lostadminkey") && fresh.seeds.contains("successoradminkey"), "the new birth is seeded with the successor only: {}", fresh.seeds);
        assert_ne!(fresh.node_id, dead.node_id, "a fresh identity: the stranded birth is not this node");
        assert_eq!(redone.node.node_id, fresh.node_id);
        assert!(!fleet.running(dead.pid), "the stranded runtime that names the lost admin is stopped");
        assert!(fleet.running(fresh.pid));

        // A node the lost executor never launched: its identity decision is executor-independent.
        let unborn = "mesh1.rpc.4".parse::<PathName>().unwrap();
        let build_unborn = accepted_build(&builds).await;
        let decided = rafka_node_admin_core::build_state::BuildStepReceipt {
            build_id: build_unborn.clone(),
            attempt: 1,
            operation: format!("create-node:{unborn}"),
            step: CreateStep::AllocateIdentity.name().into(),
            outcome: StepOutcome::Complete,
            output: Some(json!({"node_id": NodeId::mint(), "incarnation": IncarnationId::mint(), "supersedes": null, "deployment_id": DeploymentId::mint()})),
            executor: Some("lostadminkey".into()),
        };
        let decided_node_id = decided.output.as_ref().unwrap()["node_id"].as_str().unwrap().to_string();
        builds.append_step_receipt(&decided).await.unwrap();
        let born = successor.pipeline(&fleet, &builds).create(&request(&build_unborn, 2, &unborn)).await.unwrap_or_else(|e| panic!("unborn re-run: {e}"));
        assert_eq!(born.node.node_id.to_string(), decided_node_id, "an identity no runtime was made under is kept");

        let view = builds.read_build(&build_stranded).await.unwrap();
        json!({
            "cell": cell,
            "lost_executor_seed": dead.seeds,
            "stranded_runtime": {"pid": dead.pid, "node_id": dead.node_id.to_string(), "terminated": !fleet.running(dead.pid)},
            "successor_birth": {"pid": fresh.pid, "node_id": fresh.node_id.to_string(), "seeds": fresh.seeds},
            "deploy_runtime_complete_attempts": complete_attempts(&view, CreateStep::DeployRuntime, &stranded),
            "joined_birth_kept": {"node_id": joined_birth.node_id.to_string(), "pid": joined_birth.pid},
            "own_rerun_kept": own_birth.node_id.to_string(),
            "unborn_identity_kept": decided_node_id,
            "terminated_pids": fleet.terminated.lock().unwrap().clone(),
        })
    });
    let spans = cap.spans();
    let rejections: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.deployment.reject.via-lost-executor").collect();
    assert_eq!(rejections.len(), 1, "exactly the stranded birth is refused by name: {} such spans", rejections.len());
    let a = &rejections[0]["attributes"];
    assert_eq!(a["node"], "mesh1.rpc.3");
    assert!(a["recorded_executor"].as_str().unwrap().contains("lostadminkey"), "{a}");
    assert!(a["executor"].as_str().unwrap().contains("successoradminkey"), "{a}");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    let mut result = result;
    result["rejection_span_attributes"] = rejections[0]["attributes"].clone();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    let _ = std::fs::remove_dir_all(&data_root);
}
