//! i143.e12.s33 functional: the recovery pipeline runs ONE operation `recover-node:<path>` in the
//! order node-recovery.md gives ("Numbered end-to-end sequence" steps 3-5): node.delete of the exited
//! instance (no RPC, no stop frames, node.deleted from the exit proof), the old birth fenced, then
//! node.create at the same path.name over the inherited data dir with the same endpoint key and a new
//! node.id, incarnation and port.
//!
//! Its own module of the testkit binary: it asserts spans.

use crate::common;

use common::{add_node, admin_side, publish_build, template, LiveMesh, Published, Spans};
use rafka_mesh_entity::{FabricId, IncarnationId, LifecycleOp, NodeId};
use rafka_node_admin_core::build_state::{BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{CreateRequest, CommandAdmission, CommandContext, Completion, DeploymentPipeline, LifecycleEvents, NodeObserver, Publication, RecoverRequest, Timeouts};
use rafka_node_admin_core::node_commands::NodeCommand;
use rafka_node_admin_core::deployment::process::ProcessDeploymentProvider;
use rafka_node_admin_core::deployment::provider::{DeploymentProvider, DeploymentStatus, TerminationMode};
use rafka_node_admin_core::model::{Node, PathName};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

type Log = Arc<Mutex<Vec<String>>>;

/// The live observer, logging every drain it sends and every readiness it proves, in order.
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

/// The lifecycle events a replace publishes, in the order they leave the executor.
struct Recorder(Log);

#[async_trait::async_trait]
impl LifecycleEvents for Recorder {
    async fn deleting(&self, op: &LifecycleOp) {
        self.0.lock().unwrap().push(format!("NodeDeleting:{}:{}", op.node_id, op.operation));
    }
    async fn deleted(&self, op: &LifecycleOp) {
        self.0.lock().unwrap().push(format!("NodeDeleted:{}:{}", op.node_id, op.operation));
    }
    async fn restarting(&self, op: &LifecycleOp) {
        self.0.lock().unwrap().push(format!("NodeRestarting:{}", op.node_id));
    }
    fn now_rafka_ms(&self) -> u64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
    }
}

struct Rig {
    admin: common::AdminSide,
    template: rafka_node_admin_core::deployment::pipeline::LaunchTemplate,
    builds: MemoryBuildStateAdapter,
    sink: Published,
    provider: ProcessDeploymentProvider,
    log: Log,
}

async fn rig() -> Rig {
    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&fabric, admin.seed.clone(), admin.launcher.clone());
    Rig { admin, template, builds: MemoryBuildStateAdapter::new(), sink: Published::default(), provider: ProcessDeploymentProvider::new(), log: Log::default() }
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
    "mesh1.rpc.1".parse().unwrap()
}

fn receipts(steps: &[rafka_node_admin_core::build_state::BuildStepReceipt], operation: &str) -> Vec<String> {
    steps.iter().filter(|r| r.operation == operation && r.outcome == StepOutcome::Complete).map(|r| r.step.clone()).collect()
}

/// Every step of a recovery, in order: the delete of the exited instance, then the create.
const RECOVER_ORDER: [&str; 16] = [
    "NodeDeleting",
    "TerminateRuntime",
    "NodeDeleted",
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

/// CONTRACT (node-recovery.md, Operation summary "Steps", "Retained", "New"; sequence steps 3-5): a
/// recovery of an exited birth is ONE operation `recover-node:<path>`. NodeDeleting, the provider's
/// exit proof (nothing sent to the dead process: no drain, no stop frames), NodeDeleted once for the
/// exact exited birth, spanned with the mesh primary's exit proof it is published from; the old birth
/// is fenced before the new one is created; then the create at the same path.name: the same data dir
/// (its storage still there) and the same endpoint key, a new node.id and incarnation, no supersedes
/// (lineage is not chained across node.ids), ready. The `Complete` receipt names the EXITED birth.
/// Must NOT happen: a rename to `<path>.old`, a drain, a stop, a storage handoff or release.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_deletes_the_exited_instance_then_creates_a_new_node_over_its_data_dir_and_key() {
    let spans = Spans::default();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let rig = rig().await;
    let observer = Logged { inner: &rig.admin.observer, log: rig.log.clone() };
    let recorder = Recorder(rig.log.clone());
    let pipeline = rig.pipeline(&observer, &recorder);

    let birth_build = publish_build(&rig.builds, add_node()).await;
    let first = pipeline
        .create(&CreateRequest { build_id: birth_build, attempt: 1, node: path(), spec: &RPC_NODE, restart_of: None, held_runtimes: Vec::new() })
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));
    let old_dir = std::path::PathBuf::from(first.node.data_dir.clone().expect("the birth's directory"));
    std::fs::create_dir_all(old_dir.join("wal")).unwrap();
    std::fs::write(old_dir.join("wal").join("seg-0"), b"records").unwrap();
    // The runtime exits outside any workflow; the provider's exact inspection is the proof.
    rig.provider.terminate(&first.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
    let code = match rig.provider.inspect(&first.handle).await {
        DeploymentStatus::Exited { code } => code,
        other => panic!("the runtime exited: {other:?}"),
    };
    rig.log.lock().unwrap().clear();
    let old_inc = first.node.incarnation_id.clone().unwrap();
    let exit = rafka_node_admin_core::status_storage::NodeExitRow { node_id: first.node.node_id.clone(), name: path(), incarnation_id: old_inc.clone(), exit_code: code, proof: "provider".into(), observed_at_ms: 1 };

    let recover_build = publish_build(&rig.builds, add_node()).await;
    let log = rig.log.clone();
    let second = pipeline
        .recover(
            &RecoverRequest { build_id: recover_build.clone(), attempt: 2, exited: first.node.clone(), handle: first.handle.clone(), exit, spec: &RPC_NODE },
            |old| async move { log.lock().unwrap().push(format!("fenced:{}", old.node_id)) },
            |_| async { Ok(()) },
        )
        .await
        .unwrap_or_else(|e| panic!("recover: {e}"));

    // One operation, every step once, in order.
    let view = rig.builds.read_build(&recover_build).await.unwrap();
    let operation = format!("recover-node:{}", path());
    assert_eq!(receipts(&view.steps, &operation), RECOVER_ORDER.map(String::from).to_vec());
    assert!(view.steps.iter().all(|r| r.operation == operation), "no other operation: {:?}", view.steps.iter().map(|r| r.operation.as_str()).collect::<std::collections::BTreeSet<_>>());
    let output = |step: &str| view.steps.iter().find(|r| r.step == step).and_then(|r| r.output.clone()).unwrap_or_else(|| panic!("{step} has an output"));
    assert_eq!(output("Complete"), serde_json::json!({ "node_id": first.node.node_id, "incarnation": old_inc }), "Complete names the exited birth");
    assert_eq!(output("NodeDeleted")["incarnation"], serde_json::to_value(&old_inc).unwrap(), "NodeDeleted names the exact exited birth");
    assert!(output("AllocateIdentity")["supersedes"].is_null(), "no lineage across node.ids: {}", output("AllocateIdentity"));

    // Retained: path.name, data dir and its storage, endpoint key. New: node.id, incarnation, runtime.
    assert_eq!(second.node.name, first.node.name);
    assert_ne!(second.node.node_id, first.node.node_id, "a new node.id");
    assert_ne!(second.node.incarnation_id, first.node.incarnation_id);
    assert_eq!(second.node.endpoint_id, first.node.endpoint_id, "the same endpoint key, loaded from the inherited data dir");
    assert_eq!(second.node.data_dir, first.node.data_dir, "the inherited data dir");
    assert_eq!(std::fs::read(old_dir.join("wal").join("seg-0")).unwrap(), b"records", "storage retained");
    assert_eq!(rig.provider.inspect(&second.handle).await, DeploymentStatus::Running);

    // Delete, fence, then create: nothing sent to the exited process.
    let log = rig.log.lock().unwrap().clone();
    let (old, new) = (first.node.node_id.to_string(), second.node.node_id.to_string());
    assert!(!log.iter().any(|l| l.starts_with("drain-node:") || l.starts_with("stop-node:")), "nothing is sent to the exited birth: {log:?}");
    let at = |what: &str| log.iter().position(|l| l.starts_with(what)).unwrap_or_else(|| panic!("{what} not in {log:?}"));
    assert!(at(&format!("NodeDeleting:{old}:{operation}")) < at(&format!("NodeDeleted:{old}:{operation}")), "{log:?}");
    assert!(at(&format!("NodeDeleted:{old}:{operation}")) < at(&format!("fenced:{old}")) && at(&format!("fenced:{old}")) < at(&format!("ready:{new}")), "node.deleted, the fence, then the new node: {log:?}");
    assert_eq!(log.iter().filter(|l| l.starts_with("NodeDeleted")).count(), 1, "{log:?}");
    assert!(rig.sink.renamed.lock().unwrap().is_empty(), "no rename: the exited instance is deleted, never renamed");
    assert_eq!(*rig.sink.removed.lock().unwrap(), vec![path()], "the exited instance leaves the path once");

    // Spans: the operation's pipeline, and node.deleted published from the exit proof.
    let all = spans.0.lock().unwrap().clone();
    assert!(all.values().any(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-pipeline" && f.get("pipeline").map(String::as_str) == Some("recover") && f.get("build_id") == Some(&recover_build.0)));
    let (_, parent, proof) = all.values().find(|(n, _, _)| n == "rdm.node_admin.node.delete.via-exit-proof").expect("node.deleted is published from the exit proof, spanned");
    assert_eq!(parent.as_deref(), Some("rdm.node_admin.deployment.update.via-step"));
    assert_eq!(proof.get("node_id"), Some(&old));
    assert_eq!(proof.get("incarnation_id"), Some(&old_inc.0));
    assert_eq!(proof.get("proof").map(String::as_str), Some("provider"));
    rig.provider.terminate(&second.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
    assert!(matches!(rig.provider.inspect(&second.handle).await, DeploymentStatus::Exited { .. }), "the new node was stopped");
    let _ = std::fs::remove_dir_all(&rig.template.data_root);
}

/// CONTRACT (node-recovery.md, Operation summary "Retained": "endpoint key"; sequence step 4: "it
/// loads the endpoint key from that data dir"): a recovery whose inherited data dir holds no
/// `node-key` fails by name before anything of the exited birth changes: no `recover-node` receipt,
/// no node.deleting, the path not removed, and no key minted into the inherited dir.
/// Must NOT happen: a new endpoint key minted for the recovered node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovery_whose_inherited_data_dir_has_no_endpoint_key_fails_by_name_and_mints_none() {
    // Its own capture, set before any span of this run: the refusal's trace is this cell's evidence,
    // and both cells of the stem hold a subscriber when their callsites first register.
    let spans = Spans::default();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let rig = rig().await;
    let observer = Logged { inner: &rig.admin.observer, log: rig.log.clone() };
    let recorder = Recorder(rig.log.clone());
    let pipeline = rig.pipeline(&observer, &recorder);

    let birth_build = publish_build(&rig.builds, add_node()).await;
    let first = pipeline
        .create(&CreateRequest { build_id: birth_build, attempt: 1, node: path(), spec: &RPC_NODE, restart_of: None, held_runtimes: Vec::new() })
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));
    let old_dir = std::path::PathBuf::from(first.node.data_dir.clone().expect("the birth's directory"));
    rig.provider.terminate(&first.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
    let code = match rig.provider.inspect(&first.handle).await {
        DeploymentStatus::Exited { code } => code,
        other => panic!("the runtime exited: {other:?}"),
    };
    // The inherited data dir lost its endpoint key.
    std::fs::remove_file(old_dir.join("node-key")).unwrap();
    rig.log.lock().unwrap().clear();
    let old_inc = first.node.incarnation_id.clone().unwrap();
    let exit = rafka_node_admin_core::status_storage::NodeExitRow { node_id: first.node.node_id.clone(), name: path(), incarnation_id: old_inc, exit_code: code, proof: "provider".into(), observed_at_ms: 1 };

    let recover_build = publish_build(&rig.builds, add_node()).await;
    let outcome = pipeline
        .recover(
            &RecoverRequest { build_id: recover_build.clone(), attempt: 2, exited: Node { endpoint_id: None, ..first.node.clone() }, handle: first.handle.clone(), exit, spec: &RPC_NODE },
            |_| async {},
            |_| async { Ok(()) },
        )
        .await;
    let err = match outcome {
        Err(e) => e,
        Ok(created) => {
            rig.provider.terminate(&created.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
            panic!("a recovery with no endpoint key in its inherited data dir was created as {} with key {:?}", created.node.node_id, created.node.endpoint_id);
        }
    };
    let text = err.to_string();
    assert!(text.contains("node-key") && text.contains("inherited data dir"), "the refusal names the missing key in the inherited data dir: {text}");
    assert!(!old_dir.join("node-key").exists(), "no key is minted into the inherited data dir");
    let view = rig.builds.read_build(&recover_build).await.unwrap();
    assert!(view.steps.iter().all(|r| r.outcome != StepOutcome::Complete), "no step of the recovery completed: {:?}", view.steps.iter().map(|r| (r.step.as_str(), &r.outcome)).collect::<Vec<_>>());
    let log = rig.log.lock().unwrap().clone();
    assert!(!log.iter().any(|l| l.starts_with("NodeDeleting") || l.starts_with("NodeDeleted")), "nothing of the exited birth was deleted: {log:?}");
    assert!(rig.sink.removed.lock().unwrap().is_empty(), "the path was not removed");
    let all = spans.0.lock().unwrap().clone();
    assert!(all.values().any(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-pipeline" && f.get("pipeline").map(String::as_str) == Some("recover") && f.get("build_id") == Some(&recover_build.0)), "the recovery ran as its pipeline");
    assert!(!all.values().any(|(n, _, _)| n == "rdm.node_admin.node.delete.via-exit-proof"), "no node.deleted is published");
    let _ = std::fs::remove_dir_all(&rig.template.data_root);
}
