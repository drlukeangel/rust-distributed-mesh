//! i143.e12.s57 functional: node recovery is two workflows, never a sequence of its own
//! (node-recovery.md "Steps"; rdm-code-conformance.md §0.1, §2 item 4): the node.delete workflow on a
//! crashed target (no RPC, no stop frames, node.deleted from the mesh-primary's exited verdict), then the
//! node.create workflow at the same path.name over the inherited data dir (the same endpoint key, a new
//! node.id, incarnation and port). Nothing follows the create (proposal node-properties-own-commands.md
//! §5 item 8).
//!
//! Its own module of the testkit binary: it asserts spans.

use crate::common;

use common::{add_node, admin_side, publish_build, template, LiveMesh, Published, Spans};
use rdm_core::{FabricId, IncarnationId, LifecycleOp, NodeId};
use rdm_admin::build_state::{BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rdm_admin::deployment::endpoint::NODE_SPEC;
use rdm_admin::deployment::pipeline::{CreateRequest, CommandAdmission, CommandContext, Completion, DeploymentPipeline, LifecycleEvents, NodeObserver, Publication, DeleteRequest, Timeouts};
use rdm_admin::node_commands::NodeCommand;
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{DeploymentProvider, DeploymentStatus, TerminationMode};
use rdm_admin::model::{Node, PathName};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

type Log = Arc<Mutex<Vec<String>>>;

/// The live observer, logging every connections delete it sends and every readiness it proves, in order.
struct Logged<'a> {
    inner: &'a LiveMesh,
    log: Log,
}

#[async_trait::async_trait]
impl NodeObserver for Logged<'_> {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<Publication> {
        self.inner.joined(node_id, incarnation).await
    }
    async fn ready(&self, node: &Node) -> Result<(), String> {
        let r = self.inner.ready(node).await;
        if r.is_ok() {
            self.log.lock().unwrap().push(format!("ready:{}", node.node_id));
        }
        r
    }
    async fn send_command(&self, node: &Node, cmd: NodeCommand, ctx: &CommandContext) -> CommandAdmission {
        self.log.lock().unwrap().push(format!("{}:{}", cmd.operation_prefix(), node.node_id));
        self.inner.send_command(node, cmd, ctx).await
    }
    async fn await_completion(&self, node: &Node, cmd: NodeCommand, ctx: &CommandContext, within: Duration) -> Completion {
        let c = self.inner.await_completion(node, cmd, ctx, within).await;
        self.log.lock().unwrap().push(format!("awaited:{}:{}", cmd.operation_prefix(), node.node_id));
        c
    }
}

/// The lifecycle events a recovery publishes, in the order they leave the executor.
struct Recorder(Log);

#[async_trait::async_trait]
impl LifecycleEvents for Recorder {
    async fn deleting(&self, op: &LifecycleOp) {
        self.0.lock().unwrap().push(format!("NodeDeleting:{}:{:?}", op.node_id, op.operation));
    }
    async fn deleted(&self, op: &LifecycleOp) {
        self.0.lock().unwrap().push(format!("NodeDeleted:{}:{:?}", op.node_id, op.operation));
    }
    // A recovery publishes no stop, connections delete, leaving or mesh-deleted event; this cell records the removal
    // events it asserts, and states the rest as not recorded.
    async fn stopped(&self, _op: &LifecycleOp) {}
    async fn joined(&self, _op: &LifecycleOp) {}
    async fn leaving(&self, _op: &LifecycleOp) {}
    fn now_rafka_ms(&self) -> u64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
    }
}

struct Rig {
    admin: common::AdminSide,
    template: rdm_admin::deployment::pipeline::LaunchTemplate,
    builds: rdm_admin::build_run::FramedBuilds,
    sink: Published,
    provider: ProcessDeploymentProvider,
    log: Log,
}

async fn rig() -> Rig {
    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&admin);
    Rig { admin, template, builds: common::stepping(&fabric, "mesh1"), sink: Published::default(), provider: ProcessDeploymentProvider::new(), log: Log::default() }
}

impl Rig {
    fn pipeline<'a>(&'a self, observer: &'a Logged<'a>, lifecycle: &'a Recorder) -> DeploymentPipeline<'a> {
        DeploymentPipeline {
            provider: &self.provider,
            joins: &self.admin.joins,
            observer,
            sink: &self.sink,
            lifecycle,
            builds: &self.builds,
            template: &self.template,
            timeouts: Timeouts::default(),
        }
    }
}

fn path() -> PathName {
    rdm_core::PathName::new("mesh1.node", "1").unwrap()
}

/// The steps of `operation` whose `rdm.build.step` event ended `complete`, in event.id order.
fn completed(steps: &[rdm_admin::build_state::BuildStep], operation: &rdm_core::OperationId) -> Vec<String> {
    let op = operation.to_string();
    steps.iter().filter(|r| r.operation == op && r.outcome == StepOutcome::Complete).map(|r| r.step.clone()).collect()
}

/// The node.delete of an unreached birth, step by step: nothing is sent to it.
const DELETE_ORDER: [&str; 5] = ["NodeDeleting", "TerminateRuntime", "NodeDeleted", "RemoveTopologyMembership", "Complete"];

/// The node.create over the inherited data dir, step by step.
const CREATE_ORDER: [&str; 13] = [
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
    "WaitForMeshJoin",
    "WaitForNodeReady",
    "Complete",
];

/// CONTRACT (node-recovery.md, Operation summary "Steps", "Retained", "New"; sequence steps 3-5;
/// node-delete.md "Crashed target"; proposal node-properties-own-commands.md §5 item 8): node recovery
/// runs the delete and create workflows, each as its own operation of the Build, in order. node.delete of the unreached birth (`node.delete:<path>`):
/// NodeDeleting, `TerminateRuntime` reading the exact runtime at its instant (nothing sent to the
/// process: no connections delete, no stop frames; nothing terminated, it reads exited), NodeDeleted once for the
/// exact birth, exactly that birth removed; the old birth is targeted before the new one is created;
/// node.create at the same path.name (`node.create:<path>`): the same data dir (its storage still
/// there), a new endpoint key written into it (Q358), a new node.id and incarnation, no supersedes,
/// ready. No operation
/// writes a receipt of another.
/// Must NOT happen: one fused `recover-node:<path>` operation, a rename, a stop, a
/// storage handoff or release, a path-keyed removal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovery_runs_node_delete_of_the_crashed_target_then_node_create_over_its_data_dir() {
    let spans = Spans::default();
    rdm_transport::enable_callsites();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let rig = rig().await;
    let observer = Logged { inner: &rig.admin.observer, log: rig.log.clone() };
    let recorder = Recorder(rig.log.clone());
    let pipeline = rig.pipeline(&observer, &recorder);

    let birth_build = publish_build(&rig.builds, add_node()).await;
    let first = pipeline
        .create(&CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&(path()))), node_id: (rdm_core::fixture_node_id(&(path()))).clone(), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(birth_build), path: path().path, name: path().name, spec: &NODE_SPEC, data_dir: None, held_runtimes: Vec::new(), entry: None })
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));
    let old_dir = std::path::PathBuf::from(first.node.data_dir.clone().expect("the birth's directory"));
    std::fs::create_dir_all(old_dir.join("wal")).unwrap();
    std::fs::write(old_dir.join("wal").join("seg-0"), b"records").unwrap();
    // The runtime exits outside any workflow.
    rig.provider.terminate(&first.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }, rdm_admin::deployment::deadline::Deadline::step()).await.unwrap();
    assert!(matches!(rig.provider.inspect(&first.handle, rdm_admin::deployment::deadline::Deadline::step()).await, DeploymentStatus::Exited { .. }), "the runtime exited");
    rig.log.lock().unwrap().clear();

    let recover_build = publish_build(&rig.builds, add_node()).await;
    // node.delete of the unreached birth.
    pipeline
        .delete_node(&DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(first.node).node_id), build_id: Some(recover_build.clone()), node: first.node.clone(), handle: first.handle.clone(), unreached: true })
        .await
        .unwrap_or_else(|e| panic!("delete: {e}"));
    // The executor targets the exited birth before the create over its data dir.
    rig.log.lock().unwrap().push(format!("targeted:{}", first.node.node_id));
    // node.create over the exited birth's data dir, under the node.id minted for the re-create (D58).
    let old_inc = first.node.incarnation_id.clone().unwrap();
    let second_id = NodeId::mint();
    let second = pipeline
        .create(&CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &second_id), node_id: second_id.clone(), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(recover_build.clone()), path: path().path, name: path().name, spec: &NODE_SPEC, data_dir: Some(old_dir.clone()), held_runtimes: Vec::new(), entry: None })
        .await
        .unwrap_or_else(|e| panic!("create over the inherited data dir: {e}"));

    // Two operations, each its own workflow's steps once, in order.
    let view = rig.builds.read_build(&recover_build).await.unwrap();
    let (delete, create) = (rdm_core::OperationId::node("node.delete", &first.node.node_id), rdm_core::OperationId::node("node.create", &second_id));
    assert_eq!(completed(&view.steps, &delete), DELETE_ORDER.map(String::from).to_vec());
    assert_eq!(completed(&view.steps, &create), CREATE_ORDER.map(String::from).to_vec());
    let (delete_op, create_op) = (delete.to_string(), create.to_string());
    let operations: Vec<&String> = view.steps.iter().map(|r| &r.operation).collect();
    assert!(operations.iter().all(|o| [&delete_op, &create_op].contains(o)), "no other operation: {operations:?}");
    let first_of = |o: &String| operations.iter().position(|x| *x == o).unwrap();
    let last_of = |o: &String| operations.iter().rposition(|x| *x == o).unwrap();
    assert!(last_of(&delete_op) < first_of(&create_op), "the delete, then the create: {operations:?}");
    // Each step event names the node it acted on (its `subject`, build.md §5): the delete's the exited
    // node, the create's the new node.id.
    let subject = |op: &String, step: &str| view.steps.iter().find(|r| r.operation == *op && r.step == step).and_then(|r| r.subject.clone()).unwrap_or_else(|| panic!("{op} {step} names its node"));
    assert_eq!(subject(&delete_op, "NodeDeleted"), first.node.node_id, "NodeDeleted names the exited node");
    assert_eq!(subject(&create_op, "AllocateIdentity"), second_id, "the create's AllocateIdentity names the new node.id");

    // Retained: path.name, data dir and its storage. New: node.id, incarnation, endpoint, runtime.
    assert_eq!(second.node.name, first.node.name);
    assert_ne!(second.node.node_id, first.node.node_id, "a new node.id");
    assert_ne!(second.node.incarnation_id, first.node.incarnation_id);
    assert_ne!(second.node.endpoint_id, first.node.endpoint_id, "a new endpoint_id, never the predecessor's (Q358)");
    assert_eq!(rdm_admin::deployment::pipeline::endpoint_id(&old_dir).ok(), second.node.endpoint_id, "the new key is written into the inherited data dir as the new node's own");
    assert_eq!(second.node.data_dir, first.node.data_dir, "the inherited data dir");
    assert_eq!(std::fs::read(old_dir.join("wal").join("seg-0")).unwrap(), b"records", "storage retained");
    assert_eq!(rig.provider.inspect(&second.handle, rdm_admin::deployment::deadline::Deadline::step()).await, DeploymentStatus::Running);

    // Delete, target, then create: nothing sent to the exited process.
    let log = rig.log.lock().unwrap().clone();
    let (old, new) = (first.node.node_id.to_string(), second.node.node_id.to_string());
    assert!(!log.iter().any(|l| l.starts_with("node.connection.delete:") || l.starts_with("node.stop:")), "nothing is sent to the exited birth: {log:?}");
    let at = |what: &str| log.iter().position(|l| l.starts_with(what)).unwrap_or_else(|| panic!("{what} not in {log:?}"));
    assert!(at(&format!("NodeDeleting:{old}:{delete:?}")) < at(&format!("NodeDeleted:{old}:{delete:?}")), "{log:?}");
    assert!(at(&format!("NodeDeleted:{old}:{delete:?}")) < at(&format!("targeted:{old}")) && at(&format!("targeted:{old}")) < at(&format!("ready:{new}")), "node.deleted, the target, then the new node: {log:?}");
    assert_eq!(log.iter().filter(|l| l.starts_with("NodeDeleted")).count(), 1, "{log:?}");
    assert!(rig.sink.removed.lock().unwrap().is_empty(), "no path-keyed removal: only the exact exited birth leaves");
    assert_eq!(*rig.sink.removed_births.lock().unwrap(), vec![(path(), first.node.node_id.clone(), old_inc.clone())], "the exited instance leaves the path once");

    // Spans: one pipeline per workflow.
    let all = spans.0.lock().unwrap().clone();
    for kind in ["delete", "create"] {
        assert!(all.values().any(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-pipeline" && f.get("pipeline").map(String::as_str) == Some(kind) && f.get("build_id") == Some(&recover_build.log().to_string())), "the {kind} workflow ran as its pipeline");
    }
    assert!(!all.values().any(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-pipeline" && f.get("pipeline").map(String::as_str) == Some("recover")), "no recovery pipeline of its own");
    rig.provider.terminate(&second.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }, rdm_admin::deployment::deadline::Deadline::step()).await.unwrap();
    assert!(matches!(rig.provider.inspect(&second.handle, rdm_admin::deployment::deadline::Deadline::step()).await, DeploymentStatus::Exited { .. }), "the new node was stopped");
    let _ = std::fs::remove_dir_all(&rig.template.data_root);
}

/// CONTRACT (node-recovery.md sequence step 3: "it mints a new endpoint key and writes it there as the
/// new node's own, never reading the predecessor's"; storage.md §1: "What is in the data dir is the
/// application's to judge"; Q358, Q359): node recovery's create over the predecessor's data dir whose
/// `node-key` does not decode, beside the application's data, completes: `PrepareStorage` writes a new
/// key over it and the node runs on the same directory with a new endpoint, its data untouched.
/// Must NOT happen: the create refused for what the directory holds, the predecessor's key read, the
/// application's data touched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_over_a_predecessors_data_dir_writes_a_new_endpoint_key_and_never_reads_what_the_dir_holds() {
    let spans = Spans::default();
    rdm_transport::enable_callsites();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let rig = rig().await;
    let observer = Logged { inner: &rig.admin.observer, log: rig.log.clone() };
    let recorder = Recorder(rig.log.clone());
    let pipeline = rig.pipeline(&observer, &recorder);

    let birth_build = publish_build(&rig.builds, add_node()).await;
    let first = pipeline
        .create(&CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&(path()))), node_id: (rdm_core::fixture_node_id(&(path()))).clone(), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(birth_build), path: path().path, name: path().name, spec: &NODE_SPEC, data_dir: None, held_runtimes: Vec::new(), entry: None })
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));
    let old_dir = std::path::PathBuf::from(first.node.data_dir.clone().expect("the birth's directory"));
    rig.provider.terminate(&first.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }, rdm_admin::deployment::deadline::Deadline::step()).await.unwrap();
    // The predecessor's key does not decode; the application's data sits beside it.
    std::fs::write(old_dir.join("node-key"), "not-valid-hex").unwrap();
    std::fs::write(old_dir.join("app-data"), b"the application's").unwrap();

    let recover_build = publish_build(&rig.builds, add_node()).await;
    let recreated = NodeId::mint();
    let created = pipeline
        .create(&CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &recreated), node_id: recreated.clone(), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(recover_build.clone()), path: path().path, name: path().name, spec: &NODE_SPEC, data_dir: Some(old_dir.clone()), held_runtimes: Vec::new(), entry: None })
        .await
        .unwrap_or_else(|e| panic!("a create over the predecessor's data dir is refused for what it holds: {e}"));
    assert_eq!(created.node.data_dir, first.node.data_dir, "the predecessor's data dir");
    assert!(created.node.endpoint_id.is_some() && created.node.endpoint_id != first.node.endpoint_id, "a new endpoint_id: {:?}", created.node.endpoint_id);
    assert_eq!(rdm_admin::deployment::pipeline::endpoint_id(&old_dir).ok(), created.node.endpoint_id, "the new key is the one written there");
    assert_eq!(std::fs::read(old_dir.join("app-data")).unwrap(), b"the application's", "the application's data is untouched");
    assert_eq!(rig.provider.inspect(&created.handle, rdm_admin::deployment::deadline::Deadline::step()).await, DeploymentStatus::Running);
    let all = spans.0.lock().unwrap().clone();
    let prepared = all.values().find(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-step" && f.get("step").map(String::as_str) == Some("PrepareStorage") && f.get("inherited").map(String::as_str) == Some("true")).map(|(_, _, f)| f.clone()).expect("the re-create's PrepareStorage span");
    assert_eq!(prepared.get("data_dir"), Some(&old_dir.display().to_string()), "PrepareStorage names the inherited data dir: {prepared:?}");
    assert_eq!(prepared.get("endpoint_id").cloned(), created.node.endpoint_id.as_ref().map(|e| e.0.clone()), "and the new endpoint_id: {prepared:?}");
    rig.provider.terminate(&created.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }, rdm_admin::deployment::deadline::Deadline::step()).await.unwrap();
    let _ = std::fs::remove_dir_all(&rig.template.data_root);
}

