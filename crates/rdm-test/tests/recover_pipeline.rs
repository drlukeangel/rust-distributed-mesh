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
use rdm_admin::deployment::endpoint::RPC_NODE;
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

fn receipts(steps: &[rdm_admin::build_state::BuildStepReceipt], operation: &str) -> Vec<String> {
    steps.iter().filter(|r| r.operation == operation && r.outcome == StepOutcome::Complete).map(|r| r.step.clone()).collect()
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
/// there) and the same endpoint key, a new node.id and incarnation, no supersedes, ready. No operation
/// writes a receipt of another, and every receipt has a wire shape on the Build topic.
/// Must NOT happen: one fused `recover-node:<path>` operation, a rename to `<path>.old`, a stop, a
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
        .create(&CreateRequest { build_id: birth_build, attempt: 1, node: path(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None })
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));
    let old_dir = std::path::PathBuf::from(first.node.data_dir.clone().expect("the birth's directory"));
    std::fs::create_dir_all(old_dir.join("wal")).unwrap();
    std::fs::write(old_dir.join("wal").join("seg-0"), b"records").unwrap();
    // The runtime exits outside any workflow.
    rig.provider.terminate(&first.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
    assert!(matches!(rig.provider.inspect(&first.handle).await, DeploymentStatus::Exited { .. }), "the runtime exited");
    rig.log.lock().unwrap().clear();
    let old_inc = first.node.incarnation_id.clone().unwrap();

    let recover_build = publish_build(&rig.builds, add_node()).await;
    // node.delete of the unreached birth.
    pipeline
        .delete_node(&DeleteRequest { build_id: recover_build.clone(), attempt: 2, node: first.node.clone(), handle: first.handle.clone(), unreached: true })
        .await
        .unwrap_or_else(|e| panic!("delete: {e}"));
    // The executor targets the exited birth before the create that takes its key.
    rig.log.lock().unwrap().push(format!("targeted:{}", first.node.node_id));
    // node.create over the exited birth's data dir.
    let second = pipeline
        .create(&CreateRequest { build_id: recover_build.clone(), attempt: 2, node: path(), spec: &RPC_NODE, data_dir: Some(old_dir.clone()), held_runtimes: Vec::new(), entry: None })
        .await
        .unwrap_or_else(|e| panic!("create over the inherited data dir: {e}"));

    // Two operations, each its own workflow's steps once, in order.
    let view = rig.builds.read_build(&recover_build).await.unwrap();
    let (delete, create) = (format!("node.delete:{}", path()), format!("node.create:{}", path()));
    assert_eq!(receipts(&view.steps, &delete), DELETE_ORDER.map(String::from).to_vec());
    assert_eq!(receipts(&view.steps, &create), CREATE_ORDER.map(String::from).to_vec());
    let operations: Vec<&str> = view.steps.iter().map(|r| r.operation.as_str()).collect();
    assert!(operations.iter().all(|o| [delete.as_str(), create.as_str()].contains(o)), "no other operation: {operations:?}");
    let first_of = |o: &str| operations.iter().position(|x| *x == o).unwrap();
    let last_of = |o: &str| operations.iter().rposition(|x| *x == o).unwrap();
    assert!(last_of(&delete) < first_of(&create), "the delete, then the create: {operations:?}");
    // Every receipt the recovery wrote rides the fabric's Build topic and FetchBuildFacts to the
    // fabric-primary, whose planning and recovery dedup read them: none is refused for want of a wire shape.
    let facts: Vec<rdm_admin::build_state::BuildFact> = view.steps.iter().cloned().map(rdm_admin::build_state::BuildFact::Step).collect();
    let (_, refused) = rdm_admin::fabric_builds::encode_chunks(facts);
    assert!(refused.is_empty(), "a receipt of the recovery has no wire shape: {refused:?}");
    let output = |operation: &str, step: &str| view.steps.iter().find(|r| r.operation == operation && r.step == step).and_then(|r| r.output.clone()).unwrap_or_else(|| panic!("{operation} {step} has an output"));
    assert_eq!(output(&delete, "NodeDeleted")["node_id"], serde_json::to_value(&first.node.node_id).unwrap(), "NodeDeleted names the exited node");
    assert_eq!(output(&delete, "NodeDeleted")["incarnation"], serde_json::to_value(&old_inc).unwrap(), "NodeDeleted names the exact exited birth");
    assert_eq!(output(&delete, "TerminateRuntime")["verdict"], "exited", "the TerminateRuntime step read the runtime exited at its instant: the create takes the inherited data dir");
    assert!(output(&create, "AllocateIdentity")["supersedes"].is_null(), "no lineage across node.ids: {}", output(&create, "AllocateIdentity"));

    // Retained: path.name, data dir and its storage, endpoint key. New: node.id, incarnation, runtime.
    assert_eq!(second.node.name, first.node.name);
    assert_ne!(second.node.node_id, first.node.node_id, "a new node.id");
    assert_ne!(second.node.incarnation_id, first.node.incarnation_id);
    assert_eq!(second.node.endpoint_id, first.node.endpoint_id, "the same endpoint key, loaded from the inherited data dir");
    assert_eq!(second.node.data_dir, first.node.data_dir, "the inherited data dir");
    assert_eq!(std::fs::read(old_dir.join("wal").join("seg-0")).unwrap(), b"records", "storage retained");
    assert_eq!(rig.provider.inspect(&second.handle).await, DeploymentStatus::Running);

    // Delete, target, then create: nothing sent to the exited process.
    let log = rig.log.lock().unwrap().clone();
    let (old, new) = (first.node.node_id.to_string(), second.node.node_id.to_string());
    assert!(!log.iter().any(|l| l.starts_with("node.connection.delete:") || l.starts_with("node.stop:")), "nothing is sent to the exited birth: {log:?}");
    let at = |what: &str| log.iter().position(|l| l.starts_with(what)).unwrap_or_else(|| panic!("{what} not in {log:?}"));
    assert!(at(&format!("NodeDeleting:{old}:{delete}")) < at(&format!("NodeDeleted:{old}:{delete}")), "{log:?}");
    assert!(at(&format!("NodeDeleted:{old}:{delete}")) < at(&format!("targeted:{old}")) && at(&format!("targeted:{old}")) < at(&format!("ready:{new}")), "node.deleted, the target, then the new node: {log:?}");
    assert_eq!(log.iter().filter(|l| l.starts_with("NodeDeleted")).count(), 1, "{log:?}");
    assert!(rig.sink.removed.lock().unwrap().is_empty(), "no path-keyed removal: only the exact exited birth leaves");
    assert_eq!(*rig.sink.removed_births.lock().unwrap(), vec![(path(), first.node.node_id.clone(), old_inc.clone())], "the exited instance leaves the path once");

    // Spans: one pipeline per workflow.
    let all = spans.0.lock().unwrap().clone();
    for kind in ["delete", "create"] {
        assert!(all.values().any(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-pipeline" && f.get("pipeline").map(String::as_str) == Some(kind) && f.get("build_id") == Some(&recover_build.log().to_string())), "the {kind} workflow ran as its pipeline");
    }
    assert!(!all.values().any(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-pipeline" && f.get("pipeline").map(String::as_str) == Some("recover")), "no recovery pipeline of its own");
    rig.provider.terminate(&second.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
    assert!(matches!(rig.provider.inspect(&second.handle).await, DeploymentStatus::Exited { .. }), "the new node was stopped");
    let _ = std::fs::remove_dir_all(&rig.template.data_root);
}

/// CONTRACT (node-recovery.md, Operation summary "Retained": "endpoint key"; sequence step 4: "it
/// loads the endpoint key from that data dir"): node recovery's create over an inherited data dir that
/// holds no `node-key` fails by name at PrepareStorage, and no key is minted into that dir: nothing is
/// launched and no create step after it completes.
/// Must NOT happen: a new endpoint key minted for the recovered node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_over_an_inherited_data_dir_with_no_endpoint_key_fails_by_name_and_mints_none() {
    // Its own capture, set before any span of this run: the refusal's trace is this cell's evidence.
    let spans = Spans::default();
    rdm_transport::enable_callsites();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let rig = rig().await;
    let observer = Logged { inner: &rig.admin.observer, log: rig.log.clone() };
    let recorder = Recorder(rig.log.clone());
    let pipeline = rig.pipeline(&observer, &recorder);

    let birth_build = publish_build(&rig.builds, add_node()).await;
    let first = pipeline
        .create(&CreateRequest { build_id: birth_build, attempt: 1, node: path(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None })
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));
    let old_dir = std::path::PathBuf::from(first.node.data_dir.clone().expect("the birth's directory"));
    rig.provider.terminate(&first.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
    // The inherited data dir lost its endpoint key.
    std::fs::remove_file(old_dir.join("node-key")).unwrap();
    rig.log.lock().unwrap().clear();

    let recover_build = publish_build(&rig.builds, add_node()).await;
    let outcome = pipeline
        .create(&CreateRequest { build_id: recover_build.clone(), attempt: 2, node: path(), spec: &RPC_NODE, data_dir: Some(old_dir.clone()), held_runtimes: Vec::new(), entry: None })
        .await;
    let err = match outcome {
        Err(e) => e,
        Ok(created) => {
            rig.provider.terminate(&created.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
            panic!("a create over an inherited data dir with no endpoint key was created as {} with key {:?}", created.node.node_id, created.node.endpoint_id);
        }
    };
    assert_eq!(err.step, "PrepareStorage", "{err}");
    let text = err.to_string();
    assert!(text.contains("node-key") && text.contains("inherited data dir"), "the refusal names the missing key in the inherited data dir: {text}");
    assert!(!old_dir.join("node-key").exists(), "no key is minted into the inherited data dir");
    let view = rig.builds.read_build(&recover_build).await.unwrap();
    let completed: Vec<&str> = view.steps.iter().filter(|r| r.outcome == StepOutcome::Complete).map(|r| r.step.as_str()).collect();
    assert_eq!(completed, vec!["AllocateIdentity"], "nothing after the identity completed: {completed:?}");
    let all = spans.0.lock().unwrap().clone();
    assert!(all.values().any(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-pipeline" && f.get("pipeline").map(String::as_str) == Some("create") && f.get("build_id") == Some(&recover_build.log().to_string())), "the create ran as its pipeline");
    let _ = std::fs::remove_dir_all(&rig.template.data_root);
}

/// CONTRACT (node-recovery.md "Lost data dir (R-FM2)", deployment-provider.md `PrepareStorage`,
/// crit:e12.s87.1, crit:e12.s87.2, crit:e12.s87.3): `PrepareStorage` classifies an inherited
/// `<data_dir>` into two distinct cases:
/// - **Gone** (`!<data_dir>.is_dir()`): `InheritedDirError::Gone`; once the hold expires, recovery
///   allocates a fresh `<data_dir>` (`data_dir: None`) with a new `node.id` and a new endpoint key,
///   never re-minting into the absent path.
/// - **Broken** (`<data_dir>` exists on disk, `node-key` missing or corrupt):
///   `InheritedDirError::Broken`; `PrepareStorage` fails without minting a new key, and once
///   `<data_dir>/node-key` is repaired from backup, `create` over `Some(old_dir)` succeeds with the
///   retained endpoint key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepare_storage_distinguishes_a_gone_inherited_data_dir_from_a_broken_one_and_recovers_each_by_its_rule() {
    use rdm_admin::deployment::pipeline::{inspect_transport_key, InheritedDirError};
    let spans = Spans::default();
    rdm_transport::enable_callsites();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let rig = rig().await;
    let observer = Logged { inner: &rig.admin.observer, log: rig.log.clone() };
    let recorder = Recorder(rig.log.clone());
    let pipeline = rig.pipeline(&observer, &recorder);

    let birth_build = publish_build(&rig.builds, add_node()).await;
    let first = pipeline
        .create(&CreateRequest { build_id: birth_build, attempt: 1, node: path(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None })
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));
    let old_dir = std::path::PathBuf::from(first.node.data_dir.clone().expect("the birth's directory"));
    let saved_key = std::fs::read_to_string(old_dir.join("node-key")).expect("read original node-key");
    rig.provider.terminate(&first.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();

    // Case 2 (Broken): corrupt `node-key` while `old_dir` still exists on disk.
    std::fs::write(old_dir.join("node-key"), "not-valid-hex").unwrap();
    assert!(matches!(inspect_transport_key(&old_dir), Err(InheritedDirError::Broken(_))), "corrupt key in existing dir is Broken");
    let broken_build = publish_build(&rig.builds, add_node()).await;
    let err = pipeline
        .create(&CreateRequest { build_id: broken_build.clone(), attempt: 2, node: path(), spec: &RPC_NODE, data_dir: Some(old_dir.clone()), held_runtimes: Vec::new(), entry: None })
        .await
        .expect_err("broken inherited data_dir must fail PrepareStorage");
    assert_eq!(err.step, "PrepareStorage", "{err}");
    assert_eq!(std::fs::read_to_string(old_dir.join("node-key")).unwrap(), "not-valid-hex", "broken node-key is never overwritten or re-minted");

    // Repaired: restore `node-key` and verify recovery retains the endpoint key over `old_dir`.
    std::fs::write(old_dir.join("node-key"), &saved_key).unwrap();
    assert_eq!(inspect_transport_key(&old_dir), Ok(first.node.endpoint_id.clone().unwrap()));
    let repaired = pipeline
        .create(&CreateRequest { build_id: broken_build, attempt: 3, node: path(), spec: &RPC_NODE, data_dir: Some(old_dir.clone()), held_runtimes: Vec::new(), entry: None })
        .await
        .unwrap_or_else(|e| panic!("repaired create: {e}"));
    assert_eq!(repaired.node.endpoint_id, first.node.endpoint_id, "repaired dir retains endpoint key");
    assert_eq!(repaired.node.data_dir, first.node.data_dir, "repaired dir keeps the same path");
    rig.provider.terminate(&repaired.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();

    // Case 1 (Gone): remove `old_dir` completely; `inspect_transport_key` classifies it as `Gone`,
    // and recovery over a newly allocated `data_dir` (`data_dir: None`) mints a new key in a new dir.
    std::fs::remove_dir_all(&old_dir).unwrap();
    assert!(matches!(inspect_transport_key(&old_dir), Err(InheritedDirError::Gone(_))), "absent dir is Gone");
    let gone_build = publish_build(&rig.builds, add_node()).await;
    let fresh = pipeline
        .create(&CreateRequest { build_id: gone_build, attempt: 4, node: path(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None })
        .await
        .unwrap_or_else(|e| panic!("gone recovery over fresh dir: {e}"));
    assert_ne!(fresh.node.node_id, repaired.node.node_id, "new node.id minted");
    assert_ne!(fresh.node.endpoint_id, first.node.endpoint_id, "new endpoint key minted in fresh dir");
    assert_ne!(fresh.node.data_dir, first.node.data_dir, "fresh data_dir allocated beside old_dir");
    assert!(!old_dir.exists(), "absent old_dir is never recreated");
    rig.provider.terminate(&fresh.handle, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
    let _ = std::fs::remove_dir_all(&rig.template.data_root);
}

