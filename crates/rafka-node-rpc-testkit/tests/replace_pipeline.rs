//! i143.e12.s4 functional: the replace pipeline runs ONE operation `replace-node:<path>` in the
//! order node-replace.md and node-recovery.md give. The predecessor drains (unless the provider
//! already proves it terminal), gives up its path, the successor's identity is allocated, the
//! predecessor is stopped with the provider's exact terminal proof and its storage handed over, the
//! successor starts, joins and is ready, and only then is the old identity deleted.
//!
//! Its own test binary: it asserts spans (see `process_pipeline_failure.rs`).

use crate::common;

use common::{add_node, admin_side, publish_build, template, LiveMesh, Published, Spans};
use rafka_mesh_entity::meta::{NodeMeta, PersistentRetireDisposition, StorageMeta};
use rafka_mesh_entity::{FabricId, IncarnationId, LifecycleOp, NodeId};
use rafka_node_admin_core::build_state::{BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{CreateRequest, CommandAdmission, CommandContext, Completion, DeploymentPipeline, LifecycleEvents, NodeObserver, Publication, ReplaceRequest, Timeouts};
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

/// Every step of a replace of a live birth, in order.
const REPLACE_ORDER: [&str; 23] = [
    "NodeDeleting",
    "DrainNode",
    "AwaitNodeDrained",
    "RenamePredecessor",
    "AllocateIdentity",
    "PrepareStorage",
    "PrepareNetwork",
    "StopNode",
    "AwaitNodeLeft",
    "TerminateRuntime",
    "HandoffStorage",
    "DeployRuntime",
    "RegisterExactRuntimeHandle",
    "ResolveProviderControlDomain",
    "MakeRuntimeFactAvailableToBirth",
    "WaitForBind",
    "PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata",
    "ApplyMeshPending",
    "WaitForMeshJoin",
    "WaitForNodeReady",
    "NodeDeleted",
    "ReleaseStorage",
    "Complete",
];

fn persistent(preserve: bool) -> rafka_node_admin_core::accepted::FabricTopology {
    let mut t = add_node();
    let disposition = if preserve { PersistentRetireDisposition::Preserve } else { PersistentRetireDisposition::Release };
    let meta = NodeMeta { storage: StorageMeta::Persistent { on_retire: disposition }, placement: Default::default() };
    t.meshes.get_mut("mesh1").unwrap().set_meta(&"mesh1.rpc.1".parse().unwrap(), meta).unwrap();
    t
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

/// CONTRACT: a replace of a live, persistent-storage birth is one operation `replace-node:<path>`. The
/// old birth is drained, is renamed `<path>.old` (spanned), is stopped with the provider's exact
/// terminal proof, and its storage directories move into the successor's empty directory before the
/// successor starts; the successor is ready, and only then is the old identity's `NodeDeleted`
/// published, once, naming the exact old birth. The predecessor's directory keeps its files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replace_admits_the_successor_before_the_old_identity_is_deleted() {
    let spans = Spans::default();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let rig = rig().await;
    let observer = Logged { inner: &rig.admin.observer, log: rig.log.clone() };
    let recorder = Recorder(rig.log.clone());
    let pipeline = rig.pipeline(&observer, &recorder);

    let birth_build = publish_build(&rig.builds, persistent(true)).await;
    let first = pipeline
        .create(&CreateRequest { build_id: birth_build, attempt: 1, node: path(), spec: &RPC_NODE, restart_of: None, held_runtimes: Vec::new() })
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));
    let old_dir = std::path::PathBuf::from(first.node.data_dir.clone().expect("the birth's directory"));
    std::fs::create_dir_all(old_dir.join("wal").join("segments")).unwrap();
    std::fs::write(old_dir.join("wal").join("segments").join("seg-0"), b"records").unwrap();
    rig.log.lock().unwrap().clear();

    let replace_build = publish_build(&rig.builds, persistent(true)).await;
    let second = pipeline
        .replace(&ReplaceRequest { build_id: replace_build.clone(), attempt: 1, predecessor: first.node.clone(), handle: first.handle.clone(), spec: &RPC_NODE, held_runtimes: Vec::new() }, None, |_| async { Ok(()) })
        .await
        .unwrap_or_else(|e| panic!("replace: {e}"));

    // One operation, every step once, in the order the lifecycle gives.
    let view = rig.builds.read_build(&replace_build).await.unwrap();
    let operation = format!("replace-node:{}", path());
    assert_eq!(receipts(&view.steps, &operation), REPLACE_ORDER.map(String::from).to_vec());
    assert!(view.steps.iter().all(|r| r.operation == operation), "no retire-node or create-node operation: {:?}", view.steps.iter().map(|r| r.operation.as_str()).collect::<std::collections::BTreeSet<_>>());

    // The successor is a new node at the same path, the predecessor is terminal.
    assert_ne!(second.node.node_id, first.node.node_id);
    assert_ne!(second.node.endpoint_id, first.node.endpoint_id, "a new transport key");
    assert_eq!(second.node.name, first.node.name);
    assert!(matches!(rig.provider.inspect(&first.handle).await, DeploymentStatus::Exited { .. }));
    assert_eq!(rig.provider.inspect(&second.handle).await, DeploymentStatus::Running);

    // The events leave in order: NodeDeleting, the drain, the successor's readiness, NodeDeleted.
    let log = rig.log.lock().unwrap().clone();
    let (old, new) = (first.node.node_id.to_string(), second.node.node_id.to_string());
    let at = |what: &str| log.iter().position(|l| l.starts_with(what)).unwrap_or_else(|| panic!("{what} not in {log:?}"));
    let deleting = at(&format!("NodeDeleting:{old}:{operation}"));
    let drain = at(&format!("drain-node:{old}"));
    let drained = at(&format!("awaited:drain-node:{old}"));
    let stop = at(&format!("stop-node:{old}"));
    let left = at(&format!("awaited:stop-node:{old}"));
    let ready = at(&format!("ready:{new}"));
    let deleted = at(&format!("NodeDeleted:{old}:{operation}"));
    assert!(deleting < drain && drain < drained && drained < stop && stop < left && left < ready && ready < deleted, "drain, node-drained, stop-node, node-left, then the successor's readiness, then NodeDeleted last: {log:?}");
    assert_eq!(log.iter().filter(|l| l.starts_with("NodeDeleted")).count(), 1, "{log:?}");
    let deleted_receipt = view.steps.iter().find(|r| r.step == "NodeDeleted").and_then(|r| r.output.clone()).unwrap();
    assert_eq!(deleted_receipt["incarnation"], serde_json::to_value(first.node.incarnation_id.clone().unwrap()).unwrap(), "NodeDeleted names the exact old birth");

    // The predecessor gave up its path before anything was created; the sink saw exactly that birth.
    assert_eq!(*rig.sink.renamed.lock().unwrap(), vec![(path(), first.node.node_id.clone(), first.node.incarnation_id.clone().unwrap())]);
    assert!(rig.sink.removed.lock().unwrap().is_empty(), "the successor's path is never removed");
    let all = spans.0.lock().unwrap().clone();
    let (_, parent, rename) = all
        .values()
        .find(|(n, _, f)| n == "rdm.node_admin.node.update.via-replace-rename" && f.get("build_id") == Some(&replace_build.0))
        .expect("the rename is spanned");
    assert_eq!(parent.as_deref(), Some("rdm.node_admin.deployment.update.via-step"));
    assert_eq!(rename.get("from"), Some(&path().to_string()));
    assert_eq!(rename.get("to"), Some(&format!("{}.old", path())));
    assert_eq!(rename.get("node_id"), Some(&old));
    assert!(all.values().any(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-pipeline" && f.get("pipeline").map(String::as_str) == Some("replace") && f.get("build_id") == Some(&replace_build.0)));

    // The storage went into the successor's empty directory; the predecessor's files stayed with it.
    let new_dir = std::path::PathBuf::from(second.node.data_dir.clone().unwrap());
    assert_eq!(std::fs::read(new_dir.join("wal").join("segments").join("seg-0")).unwrap(), b"records", "the WAL is the successor's");
    assert!(old_dir.join("node-key").is_file(), "the predecessor's identity files stay with it: {old_dir:?}");
    assert!(!old_dir.join("wal").exists(), "directories moved, they were not copied");
    assert!(old_dir.exists(), "Persistent/Preserve: the predecessor's directory stays");
    // The successor is this test's runtime: it is stopped, with the provider's proof, before the test ends.
    rig.provider.terminate(&second.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
    assert!(matches!(rig.provider.inspect(&second.handle).await, DeploymentStatus::Exited { .. }), "the successor was stopped");
    let _ = std::fs::remove_dir_all(&rig.template.data_root);
}

/// CONTRACT: a replace of a birth the provider already proves terminal sends it nothing: no drain, no
/// Leaving, no admission wait (node-recovery.md). The sequence is NodeDeleting, rename, the successor's
/// identity, the terminal proof and handoff, the successor's start and readiness, NodeDeleted. With
/// ephemeral storage nothing is handed over and the predecessor's directory is released after the departure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_of_a_proven_terminal_birth_sends_it_no_drain_and_hands_over_no_ephemeral_storage() {
    let rig = rig().await;
    let observer = Logged { inner: &rig.admin.observer, log: rig.log.clone() };
    let recorder = Recorder(rig.log.clone());
    let pipeline = rig.pipeline(&observer, &recorder);

    let birth_build = publish_build(&rig.builds, add_node()).await;
    let first = pipeline
        .create(&CreateRequest { build_id: birth_build, attempt: 1, node: path(), spec: &RPC_NODE, restart_of: None, held_runtimes: Vec::new() })
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));
    let old_dir = std::path::PathBuf::from(first.node.data_dir.clone().unwrap());
    std::fs::create_dir_all(old_dir.join("scratch")).unwrap();
    // The runtime dies; the provider's exact inspection is the proof.
    rig.provider.terminate(&first.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
    assert!(matches!(rig.provider.inspect(&first.handle).await, DeploymentStatus::Exited { .. }));
    rig.log.lock().unwrap().clear();

    let replace_build = publish_build(&rig.builds, add_node()).await;
    let second = pipeline
        .replace(&ReplaceRequest { build_id: replace_build.clone(), attempt: 1, predecessor: first.node.clone(), handle: first.handle.clone(), spec: &RPC_NODE, held_runtimes: Vec::new() }, None, |_| async { Ok(()) })
        .await
        .unwrap_or_else(|e| panic!("replace: {e}"));

    let view = rig.builds.read_build(&replace_build).await.unwrap();
    let operation = format!("replace-node:{}", path());
    let want: Vec<String> = REPLACE_ORDER.iter().filter(|s| !["DrainNode", "AwaitNodeDrained", "StopNode", "AwaitNodeLeft"].contains(s)).map(|s| s.to_string()).collect();
    assert_eq!(receipts(&view.steps, &operation), want);
    let log = rig.log.lock().unwrap().clone();
    assert!(!log.iter().any(|l| l.starts_with("drain-node:") || l.starts_with("stop-node:")), "nothing is sent to a proven-terminal birth: {log:?}");
    let (old, new) = (first.node.node_id.to_string(), second.node.node_id.to_string());
    let at = |what: &str| log.iter().position(|l| l.starts_with(what)).unwrap_or_else(|| panic!("{what} not in {log:?}"));
    assert!(at(&format!("NodeDeleting:{old}")) < at(&format!("ready:{new}")) && at(&format!("ready:{new}")) < at(&format!("NodeDeleted:{old}")), "{log:?}");

    // Ephemeral: nothing moved, and the renamed directory is gone after the departure.
    let new_dir = std::path::PathBuf::from(second.node.data_dir.clone().unwrap());
    assert!(!new_dir.join("scratch").exists(), "ephemeral storage is not handed over");
    assert!(!old_dir.exists(), "the predecessor's directory is released after NodeDeleted");
    // The successor is this test's runtime: it is stopped, with the provider's proof, before the test ends.
    rig.provider.terminate(&second.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
    assert!(matches!(rig.provider.inspect(&second.handle).await, DeploymentStatus::Exited { .. }), "the successor was stopped");
    let _ = std::fs::remove_dir_all(&rig.template.data_root);
}
