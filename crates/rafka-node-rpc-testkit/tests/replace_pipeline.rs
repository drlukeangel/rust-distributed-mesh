//! i143.e12.s4 functional: the replace pipeline runs ONE operation `replace-node:<path>`.
//!
//! A replace of a live birth follows Rafka's recipe (node-replace.md 1-3): the complete stop parks
//! the predecessor, it is renamed `<path>.old` while parked, and the successor is created at the
//! freed name with a new node.id, incarnation and directory. The predecessor stays parked with its
//! data: the data transfer is Rafka's own step after create, and the predecessor's delete is a later
//! step of the recipe. A birth the provider already proves terminal is the recovery of an exited
//! node: it is sent nothing, ended with the provider's proof, and deleted (its Deleted row and
//! NodeDeleted) before the successor is created (node-recovery.md: node.delete, then node.create).
//!
//! The span-asserting cell runs in its own process (`own_process`), and its capture enables every
//! callsite before installing its dispatcher (`rafka_mesh_telemetry::span_capture` calls `enable_callsites`).

use crate::common;

use common::{add_node, admin_side, publish_build, template, LiveMesh, Published};
use rafka_mesh_entity::meta::{NodeMeta, PersistentRetireDisposition, StorageMeta};
use rafka_mesh_entity::{FabricId, IncarnationId, LifecycleOp, NodeId};
use rafka_node_admin_core::build_state::{BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{CreateRequest, CommandAdmission, CommandContext, Completion, DeploymentPipeline, LifecycleEvents, NodeObserver, Publication, ReplaceRequest, RetireRequest, Timeouts};
use rafka_node_admin_core::node_commands::NodeCommand;
use rafka_node_admin_core::deployment::process::ProcessDeploymentProvider;
use rafka_node_admin_core::deployment::provider::{DeploymentProvider, DeploymentStatus, TerminationMode};
use rafka_node_admin_core::model::{Node, PathName};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

type Log = Arc<Mutex<Vec<String>>>;

/// The live observer, logging every command it sends and every readiness it proves, in order. With
/// `stop_not_sent`, a `stop-node` never reaches the birth (`NotSent`): the birth keeps running,
/// as a predecessor that never admits its stop does.
struct Logged<'a> {
    inner: &'a LiveMesh,
    log: Log,
    stop_not_sent: bool,
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
        if self.stop_not_sent && cmd == NodeCommand::Stop {
            return CommandAdmission::NotSent { reason: format!("the cell holds back stop-node to {}: the birth never admits it", node.name) };
        }
        self.inner.send_command(node, cmd, ctx).await
    }
    async fn await_completion(&self, node: &Node, cmd: NodeCommand, ctx: &CommandContext, within: Duration) -> Completion {
        let c = self.inner.await_completion(node, cmd, ctx, within).await;
        self.log.lock().unwrap().push(format!("awaited:{}:{}", cmd.operation_prefix(), node.node_id));
        c
    }
    /// The stand-in writer of the mesh's rows: the cell's sink keeps no rows, so the rename is logged
    /// and answered `NameUpdated` (the validator and the row's move are node_name_update's cells').
    async fn update_name(&self, node: &Node, to: &PathName) -> rafka_node_rpc_contract::status::StatusReply {
        self.log.lock().unwrap().push(format!("node.name.update:{}:{to}", node.node_id));
        rafka_node_rpc_contract::status::StatusReply::NameUpdated { node_id: node.node_id.clone(), incarnation: node.incarnation_id.clone().expect("a known birth") }
    }
}

/// The lifecycle events a replace publishes, in the order they leave the executor. With `peer`, each
/// frame is also taken by that membership's book as the executor's mesh channel delivers it to every
/// admin (`GossipLifecycle`), so the book holds what a peer admin holds at each step: a status event
/// is published by the mesh's seated primary, the birth the peer's seat record names.
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

    /// The status event `kind` of `op`, as the seated primary of `op`'s mesh publishes it.
    async fn hear_status(&self, kind: rafka_mesh_transport::membership::StatusEvent, op: &LifecycleOp) {
        if let Some(m) = self.peer {
            let holder = m.seats().mesh(&op.name.mesh).expect("the peer holds its mesh's seat record");
            let publisher = rafka_mesh_entity::PublisherId { node: format!("{}.admin.1", op.name.mesh), incarnation: holder.incarnation };
            let _ = m.publish_lifecycle(&kind.frame(op.clone(), publisher, 0)).await;
        }
    }
}

#[async_trait::async_trait]
impl LifecycleEvents for Recorder<'_> {
    async fn deleting(&self, op: &LifecycleOp) {
        self.log.lock().unwrap().push(format!("NodeDeleting:{}:{}", op.node_id, op.operation));
        self.hear_status(rafka_mesh_transport::membership::StatusEvent::Deleting, op).await;
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
        self.hear_status(rafka_mesh_transport::membership::StatusEvent::Stopped, op).await;
    }
    async fn stop_completed(&self, _op: &LifecycleOp) {}
    async fn joined(&self, op: &LifecycleOp) {
        self.log.lock().unwrap().push(format!("node.joined:{}:{}", op.node_id, op.operation));
        if let Some(m) = self.peer {
            m.book.joined(op);
        }
    }
    // The replace's order is asserted from its steps; the connections-deleting and leaving hooks and the mesh-deleted
    // events are not part of what this recorder hears.
    async fn connections_deleting(&self, _op: &LifecycleOp) {}
    async fn leaving(&self, _op: &LifecycleOp) {}
    async fn mesh_leaving(&self, _event: &rafka_node_admin_core::deployment::pipeline::MeshLeaveEvent) {}
    async fn mesh_leave(&self, _event: &rafka_node_admin_core::deployment::pipeline::MeshLeaveEvent) {}
    async fn mesh_left(&self, _event: &rafka_node_admin_core::deployment::pipeline::MeshLeaveEvent) {}
    fn now_rafka_ms(&self) -> u64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
    }
}

/// Every step of a replace of a live birth, in order (node-replace.md 1-3).
const REPLACE_ORDER: [&str; 16] = [
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

/// Every step of the recovery of a birth the provider already proves terminal, in order. It was
/// never stopped, so it is renamed only on its exit proof (node-update.md "Rename precondition"),
/// and the old identity's delete releases no storage (node-delete.md "Storage").
const RECOVERY_ORDER: [&str; 18] = [
    "NodeDeleting",
    "TerminateRuntime",
    "RenamePredecessor",
    "NodeDeleted",
    "AllocateIdentity",
    "PrepareStorage",
    "PrepareNetwork",
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
    let template = template(&admin);
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
/// stopped (its connections deleted inside the stop), and the stop's departure is published (`NodeStopped`, so every book drops it)
/// before it is renamed `<path>.old` (spanned, its receipt naming the exact old birth) and before the
/// successor's identity exists. The successor is created in its own new directory while the
/// predecessor is still parked: at the successor's readiness, and after the operation completes, the
/// predecessor's process runs (parked) and its data is where it was, untouched. The replace deletes
/// nothing: no NodeDeleting, no terminal proof, no storage moved, no NodeDeleted (the predecessor's
/// delete is the recipe's own later step). At the successor's readiness a peer admin's view (its book,
/// without the executor's records) holds one live birth at the path, the successor, and the path
/// resolves to the successor.
#[test]
fn replace_creates_the_successor_while_the_predecessor_stays_parked_with_its_data() {
    // It installs a scoped subscriber: tracing's process-wide callsite interest cache is not shared
    // with the cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "replace_creates_the_successor_while_the_predecessor_stays_parked_with_its_data") {
        return;
    }
    let cap = crate::common::capture("replace_creates_the_successor_while_the_predecessor_stays_parked_with_its_data");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let rig = rig().await;
        let observer = Logged { inner: &rig.admin.observer, log: rig.log.clone(), stop_not_sent: false };
        // The stand-in admin is mesh1's primary: the peer holds its seat record, as an entry read names it.
        let launcher = &rig.admin.launcher;
        rig.admin.observer.membership.learn_seats(&[(rafka_mesh_entity::Seat::MeshPrimary, rafka_mesh_entity::SeatHolder { mesh: "mesh1".into(), node_id: launcher.node_id.clone(), incarnation: launcher.incarnation.clone(), epoch: 1 }, false)], "entry");
        let recorder = Recorder { log: rig.log.clone(), peer: Some(&rig.admin.observer.membership) };
        let pipeline = rig.pipeline(&observer, &recorder);

        let birth_build = publish_build(&rig.builds, persistent(true)).await;
        let first = pipeline
            .create(&CreateRequest { build_id: birth_build, attempt: 1, node: path(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new() })
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
        let live: Vec<(String, String, String)> = at_path.iter().filter(|n| n.status.is_live()).map(|n| (n.node_id.to_string(), format!("{:?}", n.status), n.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default())).collect();
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

        // The events leave in order: the stop and its NodeStopped, then the successor's readiness.
        let log = rig.log.lock().unwrap().clone();
        let (old, new) = (first.node.node_id.to_string(), second.node.node_id.to_string());
        let at = |what: &str| log.iter().position(|l| l.starts_with(what)).unwrap_or_else(|| panic!("{what} not in {log:?}"));
        let stop = at(&format!("stop-node:{old}"));
        // The stop's `Stopped` reply is its left reply (node-stop.md: stop-node answers on its own call):
        // no wait for a second completion follows it.
        assert!(!log.iter().any(|l| l.starts_with(&format!("awaited:stop-node:{old}"))), "the Stopped reply is node-left; nothing waits for another: {log:?}");
        let stopped = at(&format!("NodeStopped:{old}:stop-node:{}", path()));
        let ready = at(&format!("ready:{new}"));
        assert!(stop < stopped && stopped < ready, "stop-node (its Stopped reply is node-left), its NodeStopped, then the successor's readiness: {log:?}");
        let renamed = at(&format!("node.name.update:{old}:{}.old", path()));
        assert!(stopped < renamed && renamed < ready, "the stopped predecessor's node.name.update to .old, after its NodeStopped: {log:?}");
        assert_eq!(log.iter().filter(|l| l.starts_with("NodeStopped")).count(), 1, "the stop's departure is published once: {log:?}");
        assert!(!log.iter().any(|l| l.starts_with("NodeDeleting") || l.starts_with("NodeDeleted")), "a replace deletes nothing: the predecessor's delete is the recipe's own later step: {log:?}");

        // The predecessor gave up its path before anything was created; the sink saw exactly that birth.
        assert_eq!(*rig.sink.renamed.lock().unwrap(), vec![(path(), first.node.node_id.clone(), first.node.incarnation_id.clone().unwrap())]);
        assert!(rig.sink.removed.lock().unwrap().is_empty(), "the successor's path is never removed");
        let all = spans.0.lock().unwrap().clone();
        let (_, parent, rename) = all
            .values()
            .find(|(n, _, f)| n == "rdm.node_admin.node.update.via-replace-rename" && f.get("build_id") == Some(&replace_build.to_string()))
            .expect("the rename is spanned");
        assert_eq!(parent.as_deref(), Some("rdm.node_admin.deployment.update.via-step"));
        assert_eq!(rename.get("from"), Some(&path().to_string()));
        assert_eq!(rename.get("to"), Some(&format!("{}.old", path())));
        assert_eq!(rename.get("node_id"), Some(&old));
        assert!(all.values().any(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-pipeline" && f.get("pipeline").map(String::as_str) == Some("replace") && f.get("build_id") == Some(&replace_build.to_string())));

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
    })
}

/// CONTRACT: a replace whose predecessor never admits stop-node (`NotSent`: the birth runs on,
/// never stopped) fails at the stop's wait step by name and renames nothing (node-replace.md
/// step 2: "node[1].stop runs the complete stop and leaves the predecessor stopped"; node-update.md,
/// "Rename precondition": the node is not ready-for-traffic and holds no connection; node-stop.md, the
/// complete stop's only proof is the birth's `stopped`). The run is stop-node and its wait,
/// which fails naming the birth, stop-node and the admission it got. The predecessor keeps its
/// path.name and its process runs; no successor identity is allocated and no `Complete` is
/// receipted; no `NodeStopped` is published, as nothing parked. A birth that does not answer goes to
/// node recovery, never a lenient replace. What must NOT happen: the predecessor's path.name given up
/// while its process still runs (a RenamePredecessor receipt or span), or anything of a successor.
#[test]
fn a_predecessor_that_never_admits_its_stop_keeps_its_name_and_the_replace_fails_by_name() {
    if crate::own_process::delegated(module_path!(), "a_predecessor_that_never_admits_its_stop_keeps_its_name_and_the_replace_fails_by_name") {
        return;
    }
    let cap = crate::common::capture("a_predecessor_that_never_admits_its_stop_keeps_its_name_and_the_replace_fails_by_name");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let rig = rig().await;
        let observer = Logged { inner: &rig.admin.observer, log: rig.log.clone(), stop_not_sent: true };
        let recorder = Recorder { log: rig.log.clone(), peer: None };
        let pipeline = rig.pipeline(&observer, &recorder);

        let birth_build = publish_build(&rig.builds, persistent(true)).await;
        let first = pipeline
            .create(&CreateRequest { build_id: birth_build, attempt: 1, node: path(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new() })
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));
        rig.log.lock().unwrap().clear();

        let replace_build = publish_build(&rig.builds, persistent(true)).await;
        let e = match pipeline
            .replace(&ReplaceRequest { build_id: replace_build.clone(), attempt: 1, predecessor: first.node.clone(), handle: first.handle.clone(), spec: &RPC_NODE, held_runtimes: Vec::new() }, None, |_| async { Ok(()) })
            .await
        {
            Ok(created) => panic!("a predecessor that never admitted stop-node is not stopped, yet the replace created {}", created.node.node_id),
            Err(e) => e,
        };
        assert_eq!(e.step, "AwaitNodeLeft", "{e}");
        assert!(e.reason.contains(&first.node.name.to_string()) && e.reason.contains("stop-node") && e.reason.contains("NotSent"), "the failure names the birth, the command and the admission it got: {e}");

        // The stop was asked for and its wait failed; nothing after it ran.
        let view = rig.builds.read_build(&replace_build).await.unwrap();
        let operation = format!("replace-node:{}", path());
        let steps = receipts(&view.steps, &operation);
        assert!(steps.iter().any(|s| s == "StopNode"), "stop-node was asked: {steps:?}");
        for never in ["RenamePredecessor", "TerminateRuntime", "AllocateIdentity", "Complete"] {
            assert!(!steps.iter().any(|s| s == never), "no {never} receipt: {steps:?}");
        }
        let log = rig.log.lock().unwrap().clone();
        let old = first.node.node_id.to_string();
        assert!(log.iter().any(|l| l == &format!("stop-node:{old}")), "stop-node was asked: {log:?}");
        assert!(!log.iter().any(|l| l.starts_with("NodeStopped")), "nothing parked, so no NodeStopped: {log:?}");
        assert_eq!(rig.provider.inspect(&first.handle).await, DeploymentStatus::Running, "the predecessor's process runs: it was never stopped");
        assert!(rig.sink.renamed.lock().unwrap().is_empty(), "the predecessor keeps its path.name");
        let all = spans.0.lock().unwrap().clone();
        assert!(
            !all.values().any(|(n, _, f)| n == "rdm.node_admin.node.update.via-replace-rename" && f.get("build_id") == Some(&replace_build.to_string())),
            "no rename is spanned"
        );

        rig.provider.terminate(&first.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
        assert!(matches!(rig.provider.inspect(&first.handle).await, DeploymentStatus::Exited { .. }), "the predecessor was stopped");
        let _ = std::fs::remove_dir_all(&rig.template.data_root);
    })
}

/// CONTRACT: a replace of a birth the provider already proves terminal sends it nothing: no stop, no
/// Leaving, no admission wait (node-recovery.md). It is node.delete of the exited instance, then
/// node.create (node-recovery.md "Steps"; step 3 then step 4): NodeDeleting, the terminal proof,
/// rename (a predecessor that was never stopped is renamed only on its exit proof, node-update.md
/// "Rename precondition"), NodeDeleted (the predecessor's Deleted row, written while the executor is
/// still the writer of the mesh's rows: a mesh's only admin replaced by the fabric-primary as acting
/// primary hands the seat to its successor at readiness, R13), then the successor's identity,
/// handoff, start and readiness. With ephemeral storage nothing is handed over, and the old
/// identity's delete keeps the predecessor's directory and its endpoint key (node-delete.md step 3:
/// "Retain the data dir, node-identity.json and endpoint key").
#[test]
fn recovery_of_a_proven_terminal_birth_sends_it_no_command_and_hands_over_no_ephemeral_storage() {
    let cap = crate::common::capture("recovery_of_a_proven_terminal_birth_sends_it_no_command_and_hands_over_no_ephemeral_storage");
    cap.run(crate::common::Threads::Workers(4), async move {
        let rig = rig().await;
        let observer = Logged { inner: &rig.admin.observer, log: rig.log.clone(), stop_not_sent: false };
        let recorder = Recorder { log: rig.log.clone(), peer: None };
        let pipeline = rig.pipeline(&observer, &recorder);

        let birth_build = publish_build(&rig.builds, add_node()).await;
        let first = pipeline
            .create(&CreateRequest { build_id: birth_build, attempt: 1, node: path(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new() })
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
        assert!(at(&format!("NodeDeleting:{old}")) < at(&format!("NodeDeleted:{old}")) && at(&format!("NodeDeleted:{old}")) < at(&format!("ready:{new}")), "node.delete of the exited instance, then node.create: {log:?}");

        // Ephemeral: nothing moved, and the predecessor's directory stays after the departure.
        let new_dir = std::path::PathBuf::from(second.node.data_dir.clone().unwrap());
        assert!(!new_dir.join("scratch").exists(), "ephemeral storage is not handed over");
        assert!(old_dir.join("scratch").is_dir() && old_dir.join("node-key").is_file(), "the old identity's delete keeps the predecessor's directory and its endpoint key: {old_dir:?}");
        assert!(view.steps.iter().all(|r| r.step != "ReleaseStorage"), "no step of a replace releases storage: {:?}", receipts(&view.steps, &operation));
        // The successor is this test's runtime: it is stopped, with the provider's proof, before the test ends.
        rig.provider.terminate(&second.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
        assert!(matches!(rig.provider.inspect(&second.handle).await, DeploymentStatus::Exited { .. }), "the successor was stopped");
        let _ = std::fs::remove_dir_all(&rig.template.data_root);
    })
}

/// CONTRACT: a replace hands its successor the storage of its own predecessor (and of a successor an
/// earlier attempt of the same operation left behind), never the storage of another birth at the
/// path. A birth deleted earlier at `mesh1.rpc.1` keeps its data dir (node-delete.md "Storage":
/// "Retain data dir and endpoint key"); the path's next birth then exits and is replaced: the
/// recovery of a proven-terminal birth is the replace that hands storage over (a live predecessor
/// stays parked with its data, node-replace.md). The successor's WAL is the replaced birth's, and
/// the deleted birth's WAL stays in its own data dir. Must NOT happen:
/// the deleted birth's storage moved into the successor's directory, or HandoffStorage refused
/// because the deleted birth's WAL collides with the predecessor's.
#[test]
fn a_replace_never_hands_over_the_storage_of_a_birth_deleted_earlier_at_its_path() {
    let cap = crate::common::capture("a_replace_never_hands_over_the_storage_of_a_birth_deleted_earlier_at_its_path");
    cap.run(crate::common::Threads::Workers(4), async move {
        let rig = rig().await;
        let observer = Logged { inner: &rig.admin.observer, log: rig.log.clone(), stop_not_sent: false };
        let recorder = Recorder { log: rig.log.clone(), peer: None };
        let pipeline = rig.pipeline(&observer, &recorder);
        let seg = |dir: &std::path::Path| dir.join("wal").join("segments").join("seg-0");

        // The path's first birth, with a WAL, is deleted: its data dir stays.
        let deleted = pipeline
            .create(&CreateRequest { build_id: publish_build(&rig.builds, persistent(true)).await, attempt: 1, node: path(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new() })
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));
        let deleted_dir = std::path::PathBuf::from(deleted.node.data_dir.clone().expect("the birth's directory"));
        std::fs::create_dir_all(seg(&deleted_dir).parent().unwrap()).unwrap();
        std::fs::write(seg(&deleted_dir), b"the deleted birth's records").unwrap();
        pipeline
            .retire(&RetireRequest { build_id: publish_build(&rig.builds, persistent(true)).await, attempt: 1, node: deleted.node.clone(), handle: deleted.handle.clone(), exit: None })
            .await
            .unwrap_or_else(|e| panic!("delete: {e}"));
        assert_eq!(std::fs::read(seg(&deleted_dir)).unwrap(), b"the deleted birth's records", "the delete keeps the birth's storage");

        // The path's next birth, with its own WAL, is replaced.
        let current = pipeline
            .create(&CreateRequest { build_id: publish_build(&rig.builds, persistent(true)).await, attempt: 1, node: path(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new() })
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));
        let current_dir = std::path::PathBuf::from(current.node.data_dir.clone().expect("the birth's directory"));
        assert_ne!(current_dir, deleted_dir, "a new node at the path has a directory of its own");
        std::fs::create_dir_all(seg(&current_dir).parent().unwrap()).unwrap();
        std::fs::write(seg(&current_dir), b"the replaced birth's records").unwrap();
        // The runtime dies; the provider's exact inspection is the proof.
        rig.provider.terminate(&current.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
        assert!(matches!(rig.provider.inspect(&current.handle).await, DeploymentStatus::Exited { .. }));
        let replace_build = publish_build(&rig.builds, persistent(true)).await;
        let second = pipeline
            .replace(&ReplaceRequest { build_id: replace_build.clone(), attempt: 1, predecessor: current.node.clone(), handle: current.handle.clone(), spec: &RPC_NODE, held_runtimes: Vec::new() }, None, |_| async { Ok(()) })
            .await
            .unwrap_or_else(|e| panic!("replace: {e}"));

        let new_dir = std::path::PathBuf::from(second.node.data_dir.clone().unwrap());
        assert_eq!(std::fs::read(seg(&new_dir)).unwrap(), b"the replaced birth's records", "the successor's WAL is its predecessor's");
        assert_eq!(std::fs::read(seg(&deleted_dir)).unwrap(), b"the deleted birth's records", "the deleted birth's storage stays in its own data dir");
        assert!(!current_dir.join("wal").exists(), "the predecessor's storage moved, it was not copied");
        rig.provider.terminate(&second.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
        assert!(matches!(rig.provider.inspect(&second.handle).await, DeploymentStatus::Exited { .. }), "the successor was stopped");
        let _ = std::fs::remove_dir_all(&rig.template.data_root);
    })
}
