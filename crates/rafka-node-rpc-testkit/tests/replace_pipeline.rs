//! i143.e12.s4 functional: the replace pipeline runs ONE operation `replace-node:<path>`.
//!
//! A replace of a live birth follows Rafka's recipe (node-replace.md 1-3): the complete stop parks
//! the predecessor, it is renamed `<path>.old` while parked, and the successor is created at the
//! freed name with a new node.id, incarnation and directory. The predecessor stays parked with its
//! data: the data transfer is Rafka's own step after create, and the predecessor's delete is a later
//! step of the recipe. A birth the provider already proves terminal is the recovery of an exited
//! node: it is sent nothing, ended with the provider's proof, and its departure follows the
//! successor's readiness.
//!
//! The span-asserting cell runs in its own process (`own_process`).

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

/// The lifecycle events a replace publishes, in the order they leave the executor. With `peer`, each
/// frame is also taken by that membership's book as the executor's mesh channel delivers it to every
/// admin (`GossipLifecycle`), so the book holds what a peer admin holds at each step.
struct Recorder<'a> {
    log: Log,
    peer: Option<&'a rafka_mesh_transport::membership::Membership>,
}

impl Recorder<'_> {
    async fn hear(&self, f: rafka_mesh_transport::membership::Frame) {
        if let Some(m) = self.peer {
            let _ = m.publish_lifecycle(&f).await;
        }
    }
}

#[async_trait::async_trait]
impl LifecycleEvents for Recorder<'_> {
    async fn deleting(&self, op: &LifecycleOp) {
        self.log.lock().unwrap().push(format!("NodeDeleting:{}:{}", op.node_id, op.operation));
        self.hear(rafka_mesh_transport::membership::Frame::NodeDeleting { op: op.clone(), forwarded_by: None }).await;
    }
    async fn deleted(&self, op: &LifecycleOp) {
        self.log.lock().unwrap().push(format!("NodeDeleted:{}:{}", op.node_id, op.operation));
        self.hear(rafka_mesh_transport::membership::Frame::NodeDeleted { op: op.clone(), forwarded_by: None }).await;
    }
    async fn restarting(&self, op: &LifecycleOp) {
        self.log.lock().unwrap().push(format!("NodeRestarting:{}", op.node_id));
    }
    async fn stopped(&self, op: &LifecycleOp) {
        self.log.lock().unwrap().push(format!("NodeStopped:{}:{}", op.node_id, op.operation));
        self.hear(rafka_mesh_transport::membership::Frame::NodeStopped { op: op.clone(), forwarded_by: None }).await;
    }
    fn now_rafka_ms(&self) -> u64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
    }
}

/// Every step of a replace of a live birth, in order (node-replace.md 1-3).
const REPLACE_ORDER: [&str; 18] = [
    "DrainNode",
    "AwaitNodeDrained",
    "StopNode",
    "AwaitNodeLeft",
    "RenamePredecessor",
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

/// Every step of the recovery of a birth the provider already proves terminal, in order.
const RECOVERY_ORDER: [&str; 19] = [
    "NodeDeleting",
    "RenamePredecessor",
    "AllocateIdentity",
    "PrepareStorage",
    "PrepareNetwork",
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
    fn pipeline<'a>(&'a self, observer: &'a Logged<'a>, lifecycle: &'a Recorder<'a>) -> DeploymentPipeline<'a> {
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

/// CONTRACT: a replace of a live, persistent-storage birth is one operation `replace-node:<path>` in
/// the recipe's order (node-replace.md: "node[1].stop runs the complete stop and leaves the
/// predecessor parked, with its incarnation and data retained", then "node[1].update renames name to
/// `<name>.old` while parked", then "node.create claims the freed name and starts the successor with a
/// new node.id/incarnation"; "Rafka transfers its data as its own recipe step"). The old birth is
/// drained and stopped, and the stop's departure is published (`NodeStopped`, so every book drops it)
/// before it is renamed `<path>.old` (spanned, its receipt naming the exact old birth) and before the
/// successor's identity exists. The successor is created in its own new directory while the
/// predecessor is still parked: at the successor's readiness, and after the operation completes, the
/// predecessor's process runs (parked) and its data is where it was, untouched. The replace deletes
/// nothing: no NodeDeleting, no terminal proof, no storage moved, no NodeDeleted (the predecessor's
/// delete is the recipe's own later step). At the successor's readiness a peer admin's view (its book,
/// without the executor's records) holds one live birth at the path, the successor, and the path
/// resolves to the successor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replace_creates_the_successor_while_the_predecessor_stays_parked_with_its_data() {
    // It installs a scoped subscriber: tracing's process-wide callsite interest cache is not shared
    // with the cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "replace_creates_the_successor_while_the_predecessor_stays_parked_with_its_data") {
        return;
    }
    let spans = Spans::default();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let rig = rig().await;
    let observer = Logged { inner: &rig.admin.observer, log: rig.log.clone() };
    let recorder = Recorder { log: rig.log.clone(), peer: Some(&rig.admin.observer.membership) };
    let pipeline = rig.pipeline(&observer, &recorder);

    let birth_build = publish_build(&rig.builds, persistent(true)).await;
    let first = pipeline
        .create(&CreateRequest { build_id: birth_build, attempt: 1, node: path(), spec: &RPC_NODE, restart_of: None, held_runtimes: Vec::new() })
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));
    let old_dir = std::path::PathBuf::from(first.node.data_dir.clone().expect("the birth's directory"));
    let segment = old_dir.join("wal").join("segments").join("seg-0");
    std::fs::create_dir_all(segment.parent().unwrap()).unwrap();
    std::fs::write(&segment, b"records").unwrap();
    rig.log.lock().unwrap().clear();

    let replace_build = publish_build(&rig.builds, persistent(true)).await;
    // At the successor's readiness: what a peer admin holds, and the predecessor's process and data.
    type AtReady = (Vec<Node>, Result<rafka_node_rpc::ResolvedNode, String>, NodeId, DeploymentStatus, Option<Vec<u8>>);
    let peer_at_ready: Arc<Mutex<Option<AtReady>>> = Arc::default();
    let book = &rig.admin.observer.membership.book;
    let fabric_id = &rig.template.fabric_id;
    let (provider, first_handle, at_segment) = (&rig.provider, &first.handle, &segment);
    let at_ready = peer_at_ready.clone();
    let second = pipeline
        .replace(&ReplaceRequest { build_id: replace_build.clone(), attempt: 1, predecessor: first.node.clone(), handle: first.handle.clone(), spec: &RPC_NODE, held_runtimes: Vec::new() }, None, move |successor| async move {
            let view = rafka_node_admin_core::admin::project("fabric1", fabric_id, rafka_node_admin_core::model::ProviderKind::Process, book, &rafka_node_admin_core::admin::Records::default());
            let at_path: Vec<Node> = view.nodes.into_iter().filter(|n| n.name == path()).collect();
            let resolver = rafka_node_rpc::LiveNodeResolver::default();
            rafka_node_admin_core::node_rpc::feed_once(book, &resolver, "peer");
            let resolved = rafka_node_rpc::NodeResolver::resolve(&resolver, &rafka_node_rpc::NodeTarget::CurrentPath(path())).map_err(|e| format!("{e:?}"));
            let predecessor = provider.inspect(first_handle).await;
            let data = std::fs::read(at_segment).ok();
            *at_ready.lock().unwrap() = Some((at_path, resolved, successor.node_id.clone(), predecessor, data));
            Ok(())
        })
        .await
        .unwrap_or_else(|e| panic!("replace: {e}"));
    let (at_path, resolved, successor_id, predecessor_at_ready, data_at_ready) = peer_at_ready.lock().unwrap().take().expect("the successor reached readiness");
    assert_eq!(predecessor_at_ready, DeploymentStatus::Running, "the predecessor is parked, its process alive, while the successor is created and made ready");
    assert_eq!(data_at_ready.as_deref(), Some(&b"records"[..]), "the predecessor's data is in its own directory at the successor's readiness: {segment:?}");
    let live: Vec<(String, String, String)> = at_path.iter().filter(|n| n.status.is_live()).map(|n| (n.node_id.to_string(), format!("{:?}", n.status), n.incarnation_id.as_ref().map(|i| i.0.clone()).unwrap_or_default())).collect();
    assert_eq!(live.len(), 1, "a peer's view holds one live birth at {}: {live:?}", path());
    assert_eq!(live[0].0, successor_id.to_string(), "the live birth at {} is the successor: {live:?}", path());
    assert_eq!(resolved.map(|r| r.node_id), Ok(successor_id.clone()), "a peer resolves {} to the successor once the predecessor is parked and renamed", path());

    // One operation, every step once, in the recipe's order.
    let view = rig.builds.read_build(&replace_build).await.unwrap();
    let operation = format!("replace-node:{}", path());
    assert_eq!(receipts(&view.steps, &operation), REPLACE_ORDER.map(String::from).to_vec());
    assert!(view.steps.iter().all(|r| r.operation == operation), "no retire-node or create-node operation: {:?}", view.steps.iter().map(|r| r.operation.as_str()).collect::<std::collections::BTreeSet<_>>());
    let renamed_receipt = view.steps.iter().find(|r| r.step == "RenamePredecessor").and_then(|r| r.output.clone()).expect("the rename's receipt");
    assert_eq!(renamed_receipt["node_id"], serde_json::to_value(&first.node.node_id).unwrap(), "the rename's receipt names the exact old node: {renamed_receipt}");
    assert_eq!(renamed_receipt["incarnation"], serde_json::to_value(first.node.incarnation_id.clone().unwrap()).unwrap(), "the rename's receipt names the exact old birth: {renamed_receipt}");

    // The successor is a new node at the same path in its own directory; the predecessor is parked.
    assert_ne!(second.node.node_id, first.node.node_id);
    assert_ne!(second.node.endpoint_id, first.node.endpoint_id, "a new transport key");
    assert_eq!(second.node.name, first.node.name);
    assert_eq!(rig.provider.inspect(&first.handle).await, DeploymentStatus::Running, "the replace ends the predecessor parked, never exited");
    assert_eq!(rig.provider.inspect(&second.handle).await, DeploymentStatus::Running);

    // The events leave in order: the drain, the stop and its NodeStopped, then the successor's readiness.
    let log = rig.log.lock().unwrap().clone();
    let (old, new) = (first.node.node_id.to_string(), second.node.node_id.to_string());
    let at = |what: &str| log.iter().position(|l| l.starts_with(what)).unwrap_or_else(|| panic!("{what} not in {log:?}"));
    let drain = at(&format!("drain-node:{old}"));
    let drained = at(&format!("awaited:drain-node:{old}"));
    let stop = at(&format!("stop-node:{old}"));
    let left = at(&format!("awaited:stop-node:{old}"));
    let stopped = at(&format!("NodeStopped:{old}:stop-node:{}", path()));
    let ready = at(&format!("ready:{new}"));
    assert!(drain < drained && drained < stop && stop < left && left < stopped && stopped < ready, "drain, node-drained, stop-node, node-left, its NodeStopped, then the successor's readiness: {log:?}");
    assert_eq!(log.iter().filter(|l| l.starts_with("NodeStopped")).count(), 1, "the stop's departure is published once: {log:?}");
    assert!(!log.iter().any(|l| l.starts_with("NodeDeleting") || l.starts_with("NodeDeleted")), "a replace deletes nothing: the predecessor's delete is the recipe's own later step: {log:?}");

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

    // RDM moved no data: the predecessor's directory holds all of it, the successor's none of it.
    let new_dir = std::path::PathBuf::from(second.node.data_dir.clone().unwrap());
    assert_ne!(new_dir, old_dir, "the successor has its own directory");
    assert_eq!(std::fs::read(&segment).unwrap(), b"records", "the WAL stays the predecessor's: the data transfer is Rafka's step");
    assert!(!new_dir.join("wal").exists(), "nothing was handed to the successor's directory: {new_dir:?}");
    assert!(old_dir.join("node-key").is_file(), "the predecessor's identity files stay with it: {old_dir:?}");
    // Both runtimes are this test's: each is stopped, with the provider's proof, before the test ends.
    for h in [&first.handle, &second.handle] {
        rig.provider.terminate(h, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
        assert!(matches!(rig.provider.inspect(h).await, DeploymentStatus::Exited { .. }), "the runtime was stopped");
    }
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
    let recorder = Recorder { log: rig.log.clone(), peer: None };
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
    let want: Vec<String> = RECOVERY_ORDER.map(String::from).to_vec();
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
