//! The mesh-delete resumption (i143.e12.s39, s49; mesh-delete.md steps 1, 3 and "Resume after the
//! primary's loss"): the deleting mesh-primary's member workflows are the product's
//! (`mesh_leave::members_exited` over each member's node.delete, `DeploymentPipeline::retire`); the
//! provider, the Build state and the view are fixtures.
//!
//! mesh2 leaves: two ordinary nodes, one other node-admin and the final primary `mesh2.admin.1`.
//! In attempt 1 one ordinary node exits and the provider cannot inspect the other after its stop
//! ladder, so the ordinary tier is incomplete and the node-admin tier never starts. The resumed
//! attempt sees the view as the first one left it: neither ordinary node is live any more.

use rafka_mesh_entity::{FabricId, IncarnationId, LifecycleOp, MeshId, NodeId};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome, MemoryBuildStateAdapter};
use rafka_node_admin_core::fabric_builds::BuildMessage;
use rafka_node_admin_core::deployment::pipeline::{DeploymentPipeline, LaunchTemplate, LifecycleEvents, MeshLeaveEvent, NodeObserver, NoLifecycleEvents, Publication, RuntimeProof, TerminalReceipt, Timeouts, TopologySink};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::join::Joins;
use rafka_node_admin_core::mesh_leave::{birth_row_deleted, completion_of, mesh_left_proof, record_completion, replayed_handoff, roster_records, roster_rows_read, mesh_left_rows, accepted_roster, accepted_row, admission_roster, admitted_of, resume_after_loss, amend_roster, amended_roster, first_mismatch, handoff_verdict, handoff_verdict_from_read, manifest_reference, members_exited, operation, owner_resume, record_predecessor_exit, Admitted, Amended, ExitManifest, HandoffVerdict, LeaveKey, OwnerResume, RosterAmended, RosterBirth, STEP_ACCEPTED_ROSTER, STEP_LEAVING_ROSTER, STEP_MESH_DELETED, STEP_OTHER_MEMBERS_EXITED, STEP_ROSTER_AMENDED};
use rafka_node_rpc_contract::status::StatusReply;
use rafka_node_admin_core::model::{DeploymentId, Fabric, Mesh, Node, NodeStatus, PathName, ProviderKind, ScopeStatus};
use rafka_node_admin_core::node_rows::{Claimed, NodeRows, RowRefused};
use rafka_node_admin_core::status_rpc::Declared;
use rafka_node_admin_core::status_storage::{MemoryStatusStorage, StatusStorage};
use rafka_node_admin_core::storage::{MemoryNodesStorage, NodeRow, NodesStorage};
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc_contract::status::MeshState;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Mutex;

const DOMAIN: &str = "mesh-leave-resume";
const FAILING: &str = "mesh2.rpc.1";
const EXITING: &str = "mesh2.rpc.2";
const OTHER_ADMIN: &str = "mesh2.admin.2";
const FINAL: &str = "mesh2.admin.1";

/// This cell's own span capture: the product span pipeline (`rafka_mesh_telemetry::span_capture`: its
/// span filter and `BatchSpanProcessor`) in front of an in-memory exporter, behind a dispatcher
/// installed on every thread of the cell's own runtime.
pub(crate) struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    capture: rafka_mesh_telemetry::SpanCapture,
    dispatch: tracing::Dispatch,
}

pub(crate) fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (capture, layer) = rafka_mesh_telemetry::span_capture("mesh2.admin.1", exporter.clone());
    Capture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
}

/// The trace id of `span`, a cell's root, as the capture's pipeline stamps it.
pub(crate) fn trace_id(span: &tracing::Span) -> String {
    span.in_scope(rafka_mesh_telemetry::current_traceparent).and_then(|tp| tp.split('-').nth(1).map(str::to_string)).expect("the cell's root span has a trace context: the capture is installed on this thread")
}

impl Capture {
    pub(crate) fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        let d = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(f)
    }

    pub(crate) fn spans(&self) -> Vec<Value> {
        self.capture.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
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
                    "attributes": attributes,
                })
            })
            .collect()
    }
}

/// The Build state as a node-admin holds it: every fact is sent in its gossip wire shape
/// (`BuildMessage::to_bytes`, the Build-fact codec `fabric_builds` broadcasts with) and the fact
/// the wire gives back is the one recorded. A step output with no wire shape is refused here
/// exactly as `FabricBuildStateAdapter` refuses it.
#[derive(Default)]
struct WireBuilds(MemoryBuildStateAdapter);

impl WireBuilds {
    fn wire(fact: BuildFact) -> Result<BuildFact, BuildStateError> {
        let bytes = BuildMessage { nonce: 1, facts: vec![fact], fabric: None, shutdown: None }.to_bytes()?;
        let mut back = BuildMessage::from_bytes(&bytes).map_err(|e| BuildStateError::Unencodable(e.to_string()))?;
        Ok(back.facts.pop().expect("one fact sent, one fact read"))
    }
}

#[async_trait::async_trait]
impl BuildStateAdapter for WireBuilds {
    async fn publish_accepted(&self, a: &BuildAccepted) -> Result<(), BuildStateError> {
        let BuildFact::Accepted(a) = Self::wire(BuildFact::Accepted(a.clone()))? else { unreachable!() };
        self.0.publish_accepted(&a).await
    }
    async fn open_attempt(&self, o: &AttemptOpened) -> Result<(), BuildStateError> {
        let BuildFact::Opened(o) = Self::wire(BuildFact::Opened(o.clone()))? else { unreachable!() };
        self.0.open_attempt(&o).await
    }
    async fn read_build(&self, id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.0.read_build(id).await
    }
    async fn current_build(&self) -> Result<rafka_node_admin_core::build_state::CurrentBuild, rafka_node_admin_core::build_state::BuildStateError> {
        self.0.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, rafka_node_admin_core::build_state::BuildStateError> {
        self.0.newest_accepted_at_ms().await
    }
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.0.list_active().await
    }
    async fn claim_attempt(&self, c: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        self.0.claim_attempt(c).await
    }
    async fn adopt_claim(&self, c: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.0.adopt_claim(c).await
    }
    async fn append_step_receipt(&self, r: &BuildStepReceipt) -> Result<(), BuildStateError> {
        let BuildFact::Step(r) = Self::wire(BuildFact::Step(r.clone()))? else { unreachable!() };
        self.0.append_step_receipt(&r).await
    }
    async fn append_attempt_receipt(&self, r: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        let BuildFact::Attempt(r) = Self::wire(BuildFact::Attempt(r.clone()))? else { unreachable!() };
        self.0.append_attempt_receipt(&r).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.0.facts().await
    }
    async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
        self.0.forget(id).await
    }
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.0.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.0.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &rafka_node_admin_core::build::BuildId, mesh: &str) -> Result<Vec<rafka_node_admin_core::build_state::OwedExit>, BuildStateError> {
        self.0.exits_owed(build_id, mesh).await
    }
}

/// The host's runtimes: every termination, and the one inspection the provider cannot answer.
#[derive(Default)]
struct Fleet {
    pids: BTreeMap<u32, String>,
    terminated: Mutex<Vec<String>>,
    blind_once: Mutex<Option<u32>>,
}

#[async_trait::async_trait]
impl DeploymentProvider for Fleet {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        DOMAIN.into()
    }
    async fn spawn(&self, _: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        unreachable!("a mesh leave launches nothing")
    }
    async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        self.terminated.lock().unwrap().push(self.pids[&h.pid.unwrap()].clone());
        Ok(())
    }
    async fn inspect(&self, h: &DeploymentHandle) -> DeploymentStatus {
        let mut blind = self.blind_once.lock().unwrap();
        if *blind == h.pid {
            *blind = None;
            return DeploymentStatus::Unknown;
        }
        DeploymentStatus::Exited { code: Some(0) }
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rafka_node_admin_core::deployment::provider::Found, DeployError> {
        Ok(rafka_node_admin_core::deployment::provider::Found::NeverLaunched)
    }
}

/// No birth is commanded over Node RPC here: every drain/stop command is `NotSent` and its wait is
/// `NotAwaited`; the provider's terminal proof is what the workflows are judged by.
struct NoRpc;
#[async_trait::async_trait]
impl NodeObserver for NoRpc {
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        None
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        Ok(())
    }
}

/// The view: the last record the pipeline published for each node, and the births a node.delete
/// removed from it.
#[derive(Default)]
struct View(Mutex<BTreeMap<String, Node>>, Mutex<BTreeSet<String>>);
impl TopologySink for View {
    fn publish(&self, n: Node) {
        self.0.lock().unwrap().insert(n.name.to_string(), n);
    }
    fn remove(&self, _: &PathName) {}
    fn remove_birth(&self, name: &PathName, node_id: &NodeId, incarnation: &IncarnationId) {
        let mut view = self.0.lock().unwrap();
        if view.get(&name.to_string()).is_some_and(|n| n.node_id == *node_id && n.incarnation_id.as_ref() == Some(incarnation)) {
            view.remove(&name.to_string());
        }
        self.1.lock().unwrap().insert(name.to_string());
    }
}

impl View {
    /// `records` as the view holds them now: a birth a node.delete removed is no longer held, a
    /// published one is its last record, any other its record as given.
    fn holds(&self, records: &[Node]) -> Vec<Node> {
        let (view, removed) = (self.0.lock().unwrap(), self.1.lock().unwrap());
        records.iter().filter(|n| !removed.contains(&n.name.to_string())).map(|n| view.get(&n.name.to_string()).cloned().unwrap_or_else(|| n.clone())).collect()
    }
}

fn node(name: &str) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

fn handle(name: &str, pid: u32) -> DeploymentHandle {
    DeploymentHandle { deployment_id: DeploymentId(format!("dep-{name}")), provider: ProviderKind::Process, pid: Some(pid), start: Some(1), container: None, domain: Some(DOMAIN.into()) }
}

/// The roster as a view holds it at admission: every live member of mesh2.
fn live_roster(records: &[Node]) -> Result<Vec<RosterBirth>, String> {
    records.iter().filter(|n| n.status.is_live()).map(RosterBirth::of).collect()
}

fn births(receipts: &[TerminalReceipt]) -> BTreeSet<String> {
    receipts.iter().map(|r| r.node.clone()).collect()
}

fn sorted(mut v: Vec<RosterBirth>) -> Vec<RosterBirth> {
    v.sort();
    v
}

/// A member's row write that writes nothing: a cell about something other than the rows.
fn no_rows(_: RosterBirth) -> std::future::Ready<Result<(), String>> {
    std::future::ready(Ok(()))
}

fn tier_of(name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("tier".to_string(), format!("tier-of-{name}"))])
}

/// The view of a writer of mesh2's rows: `records`, with `primary` the mesh's primary node-admin (a
/// live seat) and `fabric_primary` holding the fabric seat.
fn writer_view(records: &[Node], primary: Option<&str>, fabric_primary: Option<Node>) -> Topology {
    let mut nodes: Vec<Node> = records.iter().cloned().map(|mut n| {
        n.is_primary = Some(n.name.to_string().as_str()) == primary;
        n
    }).collect();
    nodes.extend(fabric_primary);
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes,
    }
}

/// Every node's row in `storage` as its create left it (live, its config), held by `rows`.
async fn rows_written(storage: &MemoryNodesStorage, rows: &NodeRows, records: &[Node]) {
    for n in records {
        storage.put_node_row(&NodeRow { name: n.name.clone(), node_id: n.node_id.clone(), status: NodeStatus::ReadyForTraffic, config: n.config.clone(), tags: Default::default(), allocated: None }).await.unwrap().unwrap();
        assert!(matches!(rows.claim(&n.name, &n.node_id, "create-node", None).await, Ok(Claimed::HeldByNode(_))), "the writer holds {}'s row", n.name);
    }
}

fn artifact_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cells/mesh_leave_resume")
}

/// CONTRACT: a resumed mesh leave runs only the members without an Exited receipt and proves the
/// whole accepted roster. Attempt 1: one ordinary node exits, the other's exit cannot be proven, so
/// the ordinary tier is incomplete and the node-admin tier does not start. Attempt 2 (the view no
/// longer shows either ordinary node live) keeps the roster both sides accepted at admission, keeps
/// the exited node's receipt and does not run it again, runs the unproven node and then the other
/// node-admin; the exit manifest and the MeshLeft proof set cover every accepted birth. Every
/// receipt goes through the Build-fact wire codec, as a node-admin's Build state sends it.
/// R-MN1 (mesh-leave.md step 3): on each member's exact Exited receipt the mesh-primary writes that
/// member's row Deleted, its node id and config kept, and never its own. The exited node's row put
/// is lost in attempt 1 after its receipt; attempt 2, which keeps that receipt and does not run the
/// node again, still writes its row Deleted (the view no longer names it: the writer's record of its
/// row is what it is written from), so no Deleted row is inferred from a retained receipt.
#[test]
fn a_resumed_leave_keeps_the_exited_members_receipts_and_runs_only_the_rest() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-leave-resume-{}", NodeId::mint()));
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| {
        let mut n = node(n);
        n.config = tier_of(&n.name.to_string());
        n
    }).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 41_000 + i as u32))).collect();
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(), ..Default::default() };
    *fleet.blind_once.lock().unwrap() = handles[FAILING].pid;
    let me = RosterBirth::of(records.iter().find(|n| n.name.to_string() == FINAL).unwrap()).unwrap();
    let final_runtime = RuntimeProof::of(&handles[FINAL].fact().unwrap());
    let view = View::default();
    let calls: Mutex<Vec<(u32, String)>> = Mutex::new(Vec::new());
    let storage = Arc::new(MemoryNodesStorage::default());
    let final_id = records.iter().find(|n| n.name.to_string() == FINAL).unwrap().node_id.clone();
    let rows = NodeRows::new(FINAL.parse().unwrap(), final_id, Arc::new(RwLock::new(writer_view(&records, Some(FINAL), None))), storage.clone());

    let (first, fp1, fp2, mp1, mp2, second, records2, trace_id) = cap.run(async {
        rows_written(&storage, &rows, &records).await;
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.0.clone(), attempt, operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rafka-rpc-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rafka_mesh_entity::launch::Launcher { name: FINAL.parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            mesh_issuer: None,
        };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let shutdown = |attempt: u32| {
            let (pipeline, handles, calls, build_id) = (&pipeline, &handles, &calls, &build_id);
            move |n: Node| async move {
                let name = n.name.to_string();
                calls.lock().unwrap().push((attempt, name.clone()));
                pipeline.retire(&rafka_node_admin_core::deployment::pipeline::RetireRequest { build_id: (*build_id).clone(), attempt, node: n, handle: handles[&name].clone(), exit: None }).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-leave-resume", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Attempt 1: both sides capture the roster at admission; the ordinary tier is incomplete.
        let fp1 = accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let mp1 = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records)).await.unwrap();
        // The exited node's row put is lost after its Exited receipt: attempt 1 writes the rest.
        let lost_put = |b: RosterBirth| {
            let (rows, records, outer) = (&rows, &records, &outer);
            async move {
                if b.node == EXITING {
                    return Err(format!("{} {}'s row put is lost after its Exited receipt", b.node, b.incarnation_id));
                }
                birth_row_deleted(Some(rows), FINAL, outer, records, &b).await
            }
        };
        let first = members_exited(&builds, &key(1), FINAL, &mp1, &me, final_runtime.clone(), &records, &[], shutdown(1), lost_put).await;

        // Attempt 2 reads the view the first one left: the node its node.delete removed is gone.
        let records2: Vec<Node> = view.holds(&records);
        let fp2 = accepted_roster(&builds, &key(2), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &final_runtime, || live_roster(&records2)).await.unwrap();
        let mp2 = accepted_roster(&builds, &key(2), STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records2)).await.unwrap();
        // The view attempt 2's rows are written over no longer names the exited node.
        let departed: Vec<Node> = records2.iter().filter(|n| n.name.to_string() != EXITING).cloned().collect();
        let put = |b: RosterBirth| {
            let (rows, departed, outer) = (&rows, &departed, &outer);
            async move { birth_row_deleted(Some(rows), FINAL, outer, departed, &b).await }
        };
        let second = members_exited(&builds, &key(2), FINAL, &mp2, &me, final_runtime.clone(), &records2, &[], shutdown(2), put).await;
        (first, fp1, fp2, mp1, mp2, second, records2, trace_id)
    });
    let spans = cap.spans();
    std::fs::create_dir_all(artifact_dir()).unwrap();
    std::fs::write(artifact_dir().join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", artifact_dir().join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    // Attempt 1: the ordinary tier is incomplete by name, and the node-admin tier never started.
    let err = first.expect_err("the ordinary tier holds an unproven exit");
    assert!(err.contains("tier ordinary-nodes is incomplete") && err.contains(FAILING), "{err}");
    let calls = calls.into_inner().unwrap();
    let ran = |attempt: u32| calls.iter().filter(|(a, _)| *a == attempt).map(|(_, n)| n.clone()).collect::<BTreeSet<_>>();
    assert_eq!(ran(1), BTreeSet::from([FAILING.to_string(), EXITING.to_string()]), "attempt 1 ran the ordinary tier only");
    assert!(records2.iter().all(|n| n.name.to_string() != EXITING), "the resumed attempt's view no longer holds the node attempt 1's node.delete removed: {records2:?}");
    assert!(records2.iter().filter(|n| n.kind == rafka_node_admin_core::model::NodeKind::RpcNode).all(|n| !n.status.is_live()), "the resumed attempt's view shows neither ordinary node live: {records2:?}");

    // The accepted roster is the one captured at admission, on both sides, in every attempt.
    let accepted = sorted(fp1.clone());
    assert_eq!(accepted.len(), 4, "every birth of mesh2: {accepted:?}");
    assert_eq!(sorted(mp1), accepted, "the leaving primary accepted the same roster");
    assert_eq!(sorted(fp2.clone()), accepted, "the fabric-primary's resumed attempt keeps the roster it accepted at admission");
    assert_eq!(sorted(mp2), accepted, "the leaving primary's resumed attempt keeps the roster it accepted at admission");

    // Attempt 2: the exited member is not run again; the rest are.
    let manifest = second.unwrap_or_else(|e| panic!("the resumed attempt completes: {e}"));
    assert_eq!(ran(2), BTreeSet::from([FAILING.to_string(), OTHER_ADMIN.to_string()]), "attempt 2 runs only the members with no Exited receipt");
    assert_eq!(fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == EXITING).count(), 1, "the exited member's runtime was stopped once");
    assert_eq!(births(&manifest.receipts), BTreeSet::from([FAILING.to_string(), EXITING.to_string(), OTHER_ADMIN.to_string()]), "the manifest carries every other member's receipt");
    assert_eq!(sorted(manifest.roster.clone()), accepted, "the manifest names the whole accepted roster");
    assert_eq!(first_mismatch(&manifest, &fp2, &me, false), None, "the fabric-primary's handoff validation accepts it");

    // MeshLeft: with the final primary's own exit, the proof set covers every accepted birth.
    let mut all = manifest.clone();
    all.final_runtime = None;
    all.receipts.push(TerminalReceipt { node: me.node.clone(), node_id: me.node_id.clone(), incarnation_id: me.incarnation_id.clone(), runtime: Some(final_runtime.clone()), exit_code: Some(0) });
    assert_eq!(first_mismatch(&all, &fp2, &me, true), None, "MeshLeft covers the whole accepted roster");

    // The trace: both attempts' tiers and the retained receipt, under the cell's one trace.
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let tiers: Vec<(String, String, String)> = named("rdm.node_admin.mesh.update.via-leave-tier").iter().map(|s| (s["attributes"]["attempt"].as_str().unwrap_or("").to_string(), s["attributes"]["tier"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect();
    assert!(tiers.contains(&("1".into(), "ordinary-nodes".into(), "incomplete".into())), "{tiers:?}");
    assert!(tiers.contains(&("2".into(), "node-admins".into(), "complete".into())), "{tiers:?}");
    let retained = named("rdm.node_admin.mesh.resolve.via-retained-receipts");
    assert!(retained.iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["retained"] == "1" && s["attributes"]["remaining"] == "2"), "attempt 2 kept one receipt and ran two members: {retained:?}");
    let recorded = named("rdm.node_admin.mesh.update.via-exit-manifest-recorded");
    assert!(recorded.iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["receipts"] == "3" && s["attributes"]["roster"] == "4"), "{recorded:?}");

    // R-MN1 step 3: every other member's row is Deleted, readable at its path.name with its node id
    // and config; the exited node's too, though attempt 2 kept its receipt and did not run it. The
    // final primary's own row is not the mesh-primary's to delete (step 7 is the fabric-primary's).
    let read = |name: &str| {
        let storage = storage.clone();
        let name: PathName = name.parse().unwrap();
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async move { storage.node_row_at(&name).await.unwrap() })
    };
    for n in records.iter().filter(|n| n.name.to_string() != FINAL) {
        let row = read(&n.name.to_string()).unwrap_or_else(|| panic!("{} has a row", n.name));
        assert_eq!((row.status, &row.node_id, &row.config), (NodeStatus::Deleted, &n.node_id, &n.config), "{}'s row is Deleted with its node id and config: {row:?}", n.name);
    }
    let last = read(FINAL).unwrap();
    assert_ne!(last.status, NodeStatus::Deleted, "the final primary's row is not deleted at step 3: {last:?}");
    let deleted = named("rdm.node_admin.node.delete.via-node-row");
    let exited_row: Vec<&Value> = deleted.iter().filter(|s| s["attributes"]["subject"] == EXITING).collect();
    assert!(exited_row.iter().any(|s| s["attributes"]["outcome"] == "row-deleted" && s["attributes"]["config_from"] == "writer-record"), "attempt 2 writes the retained member's row Deleted from the writer's record: {deleted:#?}");
    for name in [FAILING, OTHER_ADMIN] {
        assert!(deleted.iter().any(|s| s["attributes"]["subject"] == name && s["attributes"]["outcome"] == "row-deleted"), "{name}'s row is written Deleted: {deleted:#?}");
    }
    assert!(!deleted.iter().any(|s| s["attributes"]["subject"] == FINAL), "{deleted:#?}");
    eprintln!("trace {trace_id}: rdm.node_admin.node.delete.via-node-row subject={EXITING} outcome=row-deleted config_from=writer-record (retained receipt, attempt 2)");
}

/// CONTRACT: an incomplete tier is refused by name, never awaited. Attempt 1 leaves the ordinary
/// tier incomplete (one ordinary node exits, the other's exit cannot be proven; the node-admin tier
/// never starts), so the leaving primary records no exit manifest. The fabric-primary's verdict on
/// the handoff over the sender's Build facts (as the wire carries them) refuses it
/// `RejectedIncompleteManifest` naming every roster birth without an Exited receipt (the unproven
/// ordinary node and the other node-admin), their count, and not the one that exited nor the final
/// primary (node-rpc-envelope.md, "Mesh delete command pair": "A stale/conflicting round or
/// incomplete manifest is refused by name with a typed Status refusal naming the mismatch"). What
/// must NOT happen: `NotReady`, the answer of a door that is not serving yet.
#[test]
fn an_incomplete_tier_is_refused_by_name_naming_the_births_without_an_exited_receipt() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-leave-incomplete-{}", NodeId::mint()));
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 42_000 + i as u32))).collect();
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(), ..Default::default() };
    *fleet.blind_once.lock().unwrap() = handles[FAILING].pid;
    let me = RosterBirth::of(records.iter().find(|n| n.name.to_string() == FINAL).unwrap()).unwrap();
    let final_runtime = RuntimeProof::of(&handles[FINAL].fact().unwrap());
    let view = View::default();

    let (tier, verdict, trace_id) = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.0.clone(), attempt: 1, operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rafka-rpc-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rafka_mesh_entity::launch::Launcher { name: FINAL.parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            mesh_issuer: None,
        };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let root = tracing::info_span!("cell.mesh-leave-incomplete", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let roster = accepted_roster(&builds, &key, STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let mp = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let tier = members_exited(&builds, &key, FINAL, &mp, &me, final_runtime.clone(), &records, &[], |n: Node| {
            let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
            async move {
                let name = n.name.to_string();
                pipeline.retire(&rafka_node_admin_core::deployment::pipeline::RetireRequest { build_id: (*build_id).clone(), attempt: 1, node: n, handle: handles[&name].clone(), exit: None }).await.map_err(|e| e.to_string())
            }
        }, no_rows)
        .await;
        // The handoff the leaving primary sends for the incomplete run, judged over its facts.
        let facts = builds.facts().await.unwrap();
        let admitted = Admitted { roster, final_primary: me.clone(), final_runtime: final_runtime.clone(), amendments: Vec::new() };
        let verdict = handoff_verdict(&facts, &key, &admitted, &manifest_reference(&key.build_id, 1, &outer, false));
        (tier, verdict, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("incomplete");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    let err = tier.expect_err("the ordinary tier holds an unproven exit");
    assert!(err.contains("tier ordinary-nodes is incomplete"), "{err}");
    let (missing, without_exit, reason) = match verdict.refusal() {
        Some(StatusReply::RejectedIncompleteManifest { missing, without_exit, reason }) => (missing, without_exit, reason),
        other => panic!("an incomplete tier is refused by name, RejectedIncompleteManifest with the births named: {other:?} ({verdict:?})"),
    };
    let names: BTreeSet<String> = without_exit.iter().map(|b| b.split(' ').next().unwrap_or_default().to_string()).collect();
    assert_eq!((missing, names), (2, BTreeSet::from([FAILING.to_string(), OTHER_ADMIN.to_string()])), "every roster birth without an Exited receipt is named, and only those: {without_exit:?}");
    assert!(reason.contains(FAILING) && reason.contains(OTHER_ADMIN) && reason.contains(STEP_OTHER_MEMBERS_EXITED), "the reason names what was read and found missing: {reason}");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let refused = named("rdm.node_admin.mesh.reject.via-incomplete-handoff");
    assert!(refused.iter().any(|s| s["attributes"]["without_exit"].as_str().is_some_and(|w| w.contains(FAILING) && w.contains(OTHER_ADMIN))), "{refused:?}");
}

/// CONTRACT: mesh.delete ends at mesh.deleted (mesh-delete.md steps 5-7; R-MD1 items 2-3). Attempt
/// 1 runs to the validated handoff: both sides record the roster, the member workflows record the
/// exit manifest, the owner's verdict accepts it and records the `MeshDeleted` completion over every
/// birth but the final primary, which nothing stops. A resumed owner reads that receipt and calls
/// nothing (`OwnerResume::Deleted`); a repeated handoff of the same final birth is `AlreadyApplied`
/// and records no second receipt, one naming another birth is refused by name. The final birth's
/// end is the ordinary node.delete (`DeploymentPipeline::retire`, a separate Build): the completion
/// index names the mesh.delete for that birth, and its `TerminateRuntime` receipt with the
/// completion is the complete proof set mesh-left records, the final primary included; a receipt of
/// another runtime proves nothing.
#[test]
fn mesh_delete_ends_at_mesh_deleted_and_mesh_left_takes_the_final_birth_from_its_own_node_delete() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-delete-completion-{}", NodeId::mint()));
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 43_000 + i as u32))).collect();
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(), ..Default::default() };
    let last_record = records.iter().find(|n| n.name.to_string() == FINAL).cloned().unwrap();
    let me = RosterBirth::of(&last_record).unwrap();
    let final_runtime = RuntimeProof::of(&handles[FINAL].fact().unwrap());
    let view = View::default();

    let out = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let (build_id, delete_build) = (BuildId::mint(), BuildId::mint());
        for b in [&build_id, &delete_build] {
            builds
                .publish_accepted(&BuildAccepted { build_id: b.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
                .await
                .unwrap();
        }
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.0.clone(), attempt: 1, operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rafka-rpc-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            mesh_issuer: None,
        };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let root = tracing::info_span!("cell.mesh-delete-completion", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        accepted_roster(&builds, &key, STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let admitted = admitted_of(&builds.read_build(&build_id).await.unwrap().steps, &outer).unwrap().unwrap();
        let mp = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records)).await.unwrap();
        members_exited(&builds, &key, FINAL, &mp, &me, final_runtime.clone(), &records, &[], |n: Node| {
            let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
            async move {
                let name = n.name.to_string();
                pipeline.retire(&rafka_node_admin_core::deployment::pipeline::RetireRequest { build_id: (*build_id).clone(), attempt: 1, node: n, handle: handles[&name].clone(), exit: None }).await.map_err(|e| e.to_string())
            }
        }, no_rows)
        .await
        .expect("every other member exits");
        // The owner's validated handoff, then mesh.deleted's completion: nothing of the final primary
        // is stopped.
        let facts = builds.facts().await.unwrap();
        let HandoffVerdict::Accept(manifest) = handoff_verdict(&facts, &key, &admitted, &manifest_reference(&build_id.0, 1, &outer, false)) else { panic!("the handoff is accepted") };
        record_completion(&builds, &key, "mesh1.admin.1", &manifest).await.expect("mesh.deleted's completion is recorded");
        let terminated_at_completion = fleet.terminated.lock().unwrap().clone();
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        let resume = owner_resume(&steps, &outer);
        let replay = replayed_handoff(&steps, &key, &last_record.node_id, &me.incarnation_id);
        let other = replayed_handoff(&steps, &key, &NodeId::mint(), "another-birth");
        let completions = steps.iter().filter(|r| r.step == STEP_MESH_DELETED).count();
        let indexed = builds.mesh_deleted_of(&last_record.node_id, last_record.incarnation_id.as_ref().unwrap()).await.unwrap();
        // The final primary's own node.delete, a separate Build: the ordinary workflow, live target.
        let req = rafka_node_admin_core::deployment::pipeline::RetireRequest { build_id: delete_build.clone(), attempt: 1, node: last_record.clone(), handle: handles[FINAL].clone(), exit: None };
        pipeline.retire(&req).await.expect("the final primary's node.delete completes");
        let terminal: TerminalReceipt = builds
            .read_build(&delete_build)
            .await
            .unwrap()
            .steps
            .into_iter()
            .find(|r| r.operation == format!("retire-node:{FINAL}") && r.step == "TerminateRuntime")
            .and_then(|r| r.output)
            .map(|v| serde_json::from_value(v).unwrap())
            .expect("the node.delete proved the final birth's exit");
        let completion = completion_of(&steps, &outer).unwrap().unwrap();
        let proof = mesh_left_proof(&completion, &terminal);
        let recycled = mesh_left_proof(&completion, &TerminalReceipt { runtime: Some(RuntimeProof { locator_fingerprint: "recycled".into(), ..terminal.runtime.clone().unwrap() }), ..terminal.clone() });
        (manifest, terminated_at_completion, resume, replay, other, completions, indexed, (build_id, outer), completion, proof, recycled, trace_id)
    });
    let (manifest, terminated_at_completion, resume, replay, other, completions, indexed, (build_id, outer), completion, proof, recycled, trace_id) = out;
    let spans = cap.spans();
    let dir = artifact_dir().join("mesh-deleted");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    assert!(!terminated_at_completion.contains(&FINAL.to_string()), "mesh.delete stops nothing of its final primary: {terminated_at_completion:?}");
    assert_eq!(births(&manifest.receipts), BTreeSet::from([FAILING.to_string(), EXITING.to_string(), OTHER_ADMIN.to_string()]), "mesh.deleted covers every birth but the final primary");
    assert_eq!(resume, Ok(OwnerResume::Deleted(manifest.clone())), "a resumed owner reads the completion and calls nothing");
    assert_eq!(completion, manifest);
    assert_eq!(replay, Some(StatusReply::AlreadyApplied), "a repeated handoff of the final birth is AlreadyApplied");
    assert!(matches!(&other, Some(StatusReply::RejectedUnmatchedCompletion { field, .. }) if field == "final_primary"), "a handoff naming another birth is refused by name: {other:?}");
    assert_eq!(completions, 1, "the replay recorded no second completion receipt");
    assert_eq!(indexed, Some((build_id, 1, outer.clone())), "the completion index names the mesh.delete for the final birth");
    let all = proof.unwrap_or_else(|e| panic!("the final birth's own exit proof completes the set: {e}"));
    assert_eq!(first_mismatch(&all, &all.roster, &me, true), None, "MeshLeft covers every accepted birth, the final primary included");
    assert_eq!(births(&all.receipts), BTreeSet::from([FAILING.to_string(), EXITING.to_string(), OTHER_ADMIN.to_string(), FINAL.to_string()]));
    assert!(recycled.as_ref().is_err_and(|e| e.starts_with("final_runtime")), "a receipt of another runtime proves nothing: {recycled:?}");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let recorded = named("rdm.node_admin.mesh.update.via-mesh-deleted-receipt");
    assert!(recorded.iter().any(|s| s["attributes"]["receipts"] == "3" && s["attributes"]["roster"] == "4" && s["attributes"]["final_primary"] == FINAL), "{recorded:?}");
    let replayed = named("rdm.node_admin.mesh.resolve.via-handoff-replayed");
    assert!(replayed.iter().any(|s| s["attributes"]["outcome"] == "already-applied"), "{replayed:?}");
    eprintln!("trace {trace_id}: via-mesh-deleted-receipt receipts=3 roster=4, via-handoff-replayed already-applied, retire-node:{FINAL} TerminateRuntime then the MeshLeft proof set over 4 births");
}

/// CONTRACT: R-MN1 with mesh-delete.md step 7 (the final primary's own node.delete, the
/// fabric-primary its executor, "as acting primary of a mesh that has no primary, writes its row
/// deleted"), R-NS1 item 2 (from the stop of a mesh's last admin the fabric-primary is the writer of
/// a mesh with no primary) and R-MD1 item 2 (mesh-left ensures the mesh row Dead). While the final
/// primary holds its seat live, the fabric-primary writes none of mesh2's rows and a second claim of
/// `mesh2.admin.1` is refused `name-held`; once it is stopped (parked, its seat held) the
/// fabric-primary writes its row Deleted, node id and config kept, as its node.delete's NodeDeleted
/// step does; mesh-left puts the mesh row Dead as acting primary when no Dead row is held; and only
/// then a new birth claims `mesh2.admin.1` `row-seeded` from the Deleted row (ops-naming "config
/// belongs to the role").
#[test]
fn the_final_primarys_node_delete_writes_its_row_deleted_once_it_is_stopped_and_only_then_the_name_is_claimed_again() {
    let cap = capture();
    let fp = {
        let mut n = node("mesh1.admin.1");
        n.is_fabric_primary = true;
        n
    };
    let mut last = node(FINAL);
    last.config = tier_of(FINAL);
    let storage = Arc::new(MemoryNodesStorage::default());
    let status = MemoryStatusStorage::default();
    let declared = Mutex::new(Declared::default());
    let view = Arc::new(RwLock::new(writer_view(std::slice::from_ref(&last), None, Some(fp.clone()))));
    let rows = NodeRows::new(fp.name.clone(), fp.node_id.clone(), view.clone(), storage.clone());
    let mesh_id = MeshId::mint();
    let key = LeaveKey { mesh_id: mesh_id.clone(), build_id: BuildId::mint().0, attempt: 1, operation: operation(&mesh_id) };
    let deleting = format!("retire-node:{FINAL}");
    let (second, live, stopped, after, row, statuses, again, trace_id) = cap.run(async {
        let root = tracing::info_span!("cell.final-primary-row-deleted", operation = %key.operation);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        // The fabric-primary claimed the mesh's first admin's name when it created it, before the
        // mesh had a primary.
        rows_written(&storage, &rows, std::slice::from_ref(&last)).await;
        // A live mesh2.admin.1 holds its name: a second birth's claim is refused.
        let second = rows.claim(&last.name, &NodeId::mint(), "create-node:mesh2.admin.1", None).await;
        // Seated and live: the fabric-primary is not the writer of mesh2's rows.
        view.write().await.nodes.iter_mut().filter(|n| n.name == last.name).for_each(|n| n.is_primary = true);
        let live = rows.deleted(&last, &deleting).await;
        // Stopped, its seat still held (R-EL1): no primary for the writer rule (R-NS1 item 2).
        view.write().await.nodes.iter_mut().filter(|n| n.name == last.name).for_each(|n| {
            n.status = NodeStatus::Leaving;
            n.parked = true;
        });
        let stopped = rows.deleted(&last, &deleting).await;
        let exited = rafka_node_admin_core::deployment::pipeline::TerminalReceipt { node: last.name.to_string(), node_id: last.node_id.to_string(), incarnation_id: last.incarnation_id.clone().unwrap().0, runtime: None, exit_code: Some(0) };
        let after = mesh_left_rows(Some(&rows), &status, &rafka_node_admin_core::storage::MemoryMeshStorage::default(), &declared, "mesh1.admin.1", &key, "mesh2", std::slice::from_ref(&exited)).await;
        let row = storage.node_row_at(&last.name).await.unwrap();
        let statuses = status.mesh_statuses().await.unwrap();
        let dead_held = status.mesh_status_held_of("mesh2", &rafka_node_admin_core::status_storage::MeshStatusRow { mesh_id: mesh_id.clone(), state: MeshState::Dead }).await.unwrap();
        // mesh2 created again: its first admin's claim wins over the Deleted row.
        let again = rows.claim(&last.name, &NodeId::mint(), "create-node:mesh2.admin.1", None).await;
        (second, live, stopped, after, row, (statuses, dead_held), again, trace_id)
    });
    let (statuses, dead_held) = statuses;
    let spans = cap.spans();
    let dir = artifact_dir().join("final-primary-row-deleted");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());

    assert!(matches!(&second, Err(RowRefused::NameHeld(h)) if h.by == last.node_id), "a live mesh2.admin.1 refuses a second claim: {second:?}");
    assert!(live.is_err(), "while the final primary holds its seat live the fabric-primary writes none of mesh2's rows: {live:?}");
    stopped.unwrap_or_else(|e| panic!("once stopped, the fabric-primary writes the final primary's row Deleted as acting primary: {e}"));
    after.unwrap_or_else(|e| panic!("mesh-left writes its rows: {e}"));
    let row = row.expect("the final primary's row stays, readable at its path.name");
    assert_eq!((row.status, &row.node_id, &row.config), (NodeStatus::Deleted, &last.node_id, &last.config), "{row:?}");
    assert!(dead_held, "the mesh row is Dead among mesh2's own status rows (status/mesh2/, R-MD1)");
    assert_eq!(statuses.get(&mesh_id), None, "no row of mesh2 among the fabric-primary's own mesh's status rows: {statuses:?}");
    assert_eq!(declared.lock().unwrap().meshes.get(&mesh_id), Some(&MeshState::Dead), "and held as applied");
    match &again {
        Ok(Claimed::Written { row, seeded_from }) => {
            assert_eq!(seeded_from.as_ref(), Some(&last.node_id), "seeded from the Deleted row");
            assert_eq!(row.config, last.config, "the role's config");
        }
        other => panic!("the recreated mesh's admin claims mesh2.admin.1 over the Deleted row: {other:?}"),
    }
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let deleted: Vec<String> = named("rdm.node_admin.node.delete.via-node-row").iter().filter(|s| s["attributes"]["subject"] == FINAL).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect();
    assert_eq!(deleted, ["not-mesh-primary", "row-deleted"], "refused while the final primary is live and seated, written once it is stopped: {deleted:?}");
    let dead = named("rdm.node_admin.mesh.update.via-dead-row");
    assert!(dead.iter().any(|s| s["attributes"]["mesh_id"] == mesh_id.as_str() && s["attributes"]["outcome"] == "applied" && s["attributes"]["writer"] == "acting-primary"), "no Dead row held: the fabric-primary writes Dead as acting primary: {dead:#?}");
    let claims: Vec<String> = named("rdm.node_admin.node.create.via-node-row").iter().filter(|s| s["attributes"]["subject"] == FINAL).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect();
    assert_eq!(claims, ["row-held-by-node", "name-held", "row-seeded"], "{claims:?}");
    eprintln!("trace {trace_id}: delete.via-node-row not-mesh-primary then row-deleted (stopped), mesh.update.via-dead-row applied acting-primary, via-node-row claim row-seeded seeded_from={}", last.node_id);
}

const NEW_ADMIN: &str = "mesh2.admin.3";

/// The exact Exited receipt the provider's inspection of `b`'s runtime `runtime` proves.
fn exited(b: &RosterBirth, runtime: &RuntimeProof) -> TerminalReceipt {
    TerminalReceipt { node: b.node.clone(), node_id: b.node_id.clone(), incarnation_id: b.incarnation_id.clone(), runtime: Some(runtime.clone()), exit_code: Some(137) }
}

fn step_facts(facts: &[BuildFact]) -> Vec<BuildStepReceipt> {
    facts.iter().filter_map(|f| if let BuildFact::Step(r) = f { Some(r.clone()) } else { None }).collect()
}

/// CONTRACT: a mesh leave whose final primary is lost mid-leave resumes under a named amendment and
/// completes over both primaries (R11; mesh-leave.md, "Resume after the primary's loss"). Attempt 1:
/// the final primary `mesh2.admin.1` runs the ordinary tier, `mesh2.rpc.2` exits and `mesh2.rpc.1`'s
/// exit cannot be proven, then the final primary is lost. The fabric-primary records the lost
/// primary's exact Exited first, then `RosterAmended { mesh2.admin.1 -> mesh2.admin.2 }` (the
/// surviving admin the election seated); the identical amendment replays `AlreadyApplied` and a
/// second amendment for the same predecessor is refused by name. Attempt 2: the amended primary runs
/// only `mesh2.rpc.1` (`mesh2.rpc.2` and the lost primary keep their receipts, nobody re-runs them),
/// its manifest is accepted against the amended admission, a stale manifest naming the predecessor as
/// final primary is refused by name, mesh.deleted is recorded from it, and once the amended primary's
/// own node.delete proves its exit the MeshLeft proof set covers every accepted birth, both primaries
/// included.
#[test]
fn a_primary_lost_mid_leave_is_replaced_by_a_named_amendment_and_the_leave_completes_over_both_primaries() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-leave-amended-{}", NodeId::mint()));
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 44_000 + i as u32))).collect();
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(), ..Default::default() };
    *fleet.blind_once.lock().unwrap() = handles[FAILING].pid;
    let birth = |name: &str| RosterBirth::of(records.iter().find(|n| n.name.to_string() == name).unwrap()).unwrap();
    let (lost, survivor) = (birth(FINAL), birth(OTHER_ADMIN));
    let (lost_runtime, survivor_runtime) = (RuntimeProof::of(&handles[FINAL].fact().unwrap()), RuntimeProof::of(&handles[OTHER_ADMIN].fact().unwrap()));
    let view = View::default();
    let calls: Mutex<Vec<(u32, String)>> = Mutex::new(Vec::new());
    let amendment = RosterAmended { predecessor: lost.clone(), final_primary: survivor.clone(), final_runtime: survivor_runtime.clone() };

    let out = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.0.clone(), attempt, operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rafka-rpc-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rafka_mesh_entity::launch::Launcher { name: FINAL.parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            mesh_issuer: None,
        };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let shutdown = |attempt: u32| {
            let (pipeline, handles, calls, build_id) = (&pipeline, &handles, &calls, &build_id);
            move |n: Node| async move {
                let name = n.name.to_string();
                calls.lock().unwrap().push((attempt, name.clone()));
                pipeline.retire(&rafka_node_admin_core::deployment::pipeline::RetireRequest { build_id: (*build_id).clone(), attempt, node: n, handle: handles[&name].clone(), exit: None }).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-leave-amended", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Attempt 1: both sides capture the roster; the ordinary tier is incomplete; the primary is lost.
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let mp1 = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, FINAL, &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key(1), FINAL, &mp1, &lost, lost_runtime.clone(), &records, &[], shutdown(1), no_rows).await;
        assert!(first.is_err(), "attempt 1's ordinary tier is incomplete");

        // The resume, attempt 2: the lost primary's exact Exited first, then the amendment.
        record_predecessor_exit(&builds, &key(2), "mesh1.admin.1", &exited(&lost, &lost_runtime)).await.expect("the lost primary's exact exit is recorded");
        let applied = amend_roster(&builds, &key(2), "mesh1.admin.1", &amendment).await;
        let replayed = amend_roster(&builds, &key(2), "mesh1.admin.1", &amendment).await;
        let second_amendment = amend_roster(&builds, &key(2), "mesh1.admin.1", &RosterAmended { final_primary: birth(EXITING), ..amendment.clone() }).await;
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        let admitted = admitted_of(&steps, &outer).expect("a roster is recorded").expect("the admission reads");

        // The amended primary's member workflows, over the leaving roster plus the amendments.
        let leaving = accepted_row(&steps, &outer, STEP_LEAVING_ROSTER).unwrap().unwrap().roster;
        let mp2 = amended_roster(&leaving, &steps, &outer).unwrap();
        let records2: Vec<Node> = view.holds(&records);
        let manifest = members_exited(&builds, &key(2), OTHER_ADMIN, &mp2, &survivor, survivor_runtime.clone(), &records2, &[], shutdown(2), no_rows).await.expect("the amended primary's member workflows complete");
        let facts = builds.facts().await.unwrap();
        let verdict = handoff_verdict(&facts, &key(2), &admitted, &manifest_reference(&build_id.0, 2, &outer, false));
        // A stale hand-off: the same receipts, naming the predecessor as final primary.
        let stale_manifest = ExitManifest { final_primary: lost.clone(), ..manifest.clone() };
        let stale_facts: Vec<BuildFact> = facts
            .iter()
            .map(|f| match f {
                BuildFact::Step(r) if r.step == STEP_OTHER_MEMBERS_EXITED && r.attempt == 2 => BuildFact::Step(BuildStepReceipt { output: serde_json::to_value(&stale_manifest).ok(), ..r.clone() }),
                other => other.clone(),
            })
            .collect();
        let stale = handoff_verdict(&stale_facts, &key(2), &admitted, &manifest_reference(&build_id.0, 2, &outer, false));
        // mesh.deleted from the amended primary's accepted manifest; its own end is a separate
        // node.delete, whose exact exit completes mesh-left's proof set.
        let HandoffVerdict::Accept(completed) = verdict.clone() else { panic!("the amended primary's manifest is accepted: {verdict:?}") };
        record_completion(&builds, &key(2), "mesh1.admin.1", &completed).await.expect("mesh.deleted is recorded");
        let delete_build = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: delete_build.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let last = records2.iter().find(|n| n.name.to_string() == OTHER_ADMIN).cloned().unwrap();
        let terminal = pipeline.retire(&rafka_node_admin_core::deployment::pipeline::RetireRequest { build_id: delete_build, attempt: 1, node: last, handle: handles[OTHER_ADMIN].clone(), exit: None }).await.expect("the amended primary's node.delete proves its exit");
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        let mesh_left = completion_of(&steps, &outer).expect("a completion is recorded").map_err(|e| e.to_string()).and_then(|c| mesh_left_proof(&c, &terminal));
        (applied, replayed, second_amendment, admitted, verdict, stale, (owner_resume(&steps, &outer), mesh_left), steps, trace_id)
    });
    let (applied, replayed, second_amendment, admitted, verdict, stale, (resume, mesh_left), steps, trace_id) = out;
    let spans = cap.spans();
    let dir = artifact_dir().join("amended");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    assert_eq!(applied, Ok(Amended::Applied), "the amendment is recorded after the lost primary's exact exit");
    assert_eq!(replayed, Ok(Amended::AlreadyApplied), "the identical amendment replays AlreadyApplied");
    let refused = second_amendment.expect_err("a second amendment for the same predecessor is refused");
    assert!(refused.contains("second amendment") && refused.contains(FINAL), "the refusal names the predecessor: {refused}");
    assert_eq!(steps.iter().filter(|r| r.step == STEP_ROSTER_AMENDED).count(), 1, "one RosterAmended receipt for one predecessor");
    assert_eq!(admitted.final_primary, survivor, "the amended admission names the surviving admin as final primary");
    assert_eq!(admitted.final_runtime, survivor_runtime);
    assert_eq!(admitted.amendments, vec![amendment.clone()]);
    let ran: Vec<(u32, String)> = calls.lock().unwrap().clone();
    assert_eq!(ran.iter().filter(|(a, _)| *a == 2).map(|(_, n)| n.clone()).collect::<Vec<_>>(), vec![FAILING.to_string()], "the amended primary runs only the member without an Exited receipt: {ran:?}");
    assert!(matches!(verdict, HandoffVerdict::Accept(_)), "the amended primary's manifest is accepted: {verdict:?}");
    match &stale {
        HandoffVerdict::Unmatched { field, expected, reported } => {
            assert_eq!(field, "final_primary", "{stale:?}");
            assert!(expected.contains(OTHER_ADMIN) && reported.contains(FINAL) && reported.contains("predecessor"), "the refusal names both births: {stale:?}");
        }
        other => panic!("a manifest naming the predecessor is refused by name, not {other:?}"),
    }
    let resume = resume.unwrap_or_else(|e| panic!("the resume reads the receipts: {e}"));
    assert!(matches!(&resume, OwnerResume::Deleted(m) if m.final_primary == survivor), "a resumed owner reads the amended primary's mesh.deleted and calls nothing: {resume:?}");
    let all = mesh_left.unwrap_or_else(|e| panic!("the amended primary's own node.delete completes the MeshLeft proof set: {e}"));
    assert_eq!(births(&all.receipts), BTreeSet::from([FAILING.to_string(), EXITING.to_string(), OTHER_ADMIN.to_string(), FINAL.to_string()]), "MeshLeft covers both primaries");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.mesh.update.via-roster-amended").iter().any(|s| s["attributes"]["predecessor"].as_str().is_some_and(|p| p.contains(FINAL)) && s["attributes"]["final_primary"].as_str().is_some_and(|p| p.contains(OTHER_ADMIN)) && s["attributes"]["outcome"] == "applied"));
    assert!(named("rdm.node_admin.mesh.reject.via-second-amendment").iter().any(|s| s["attributes"]["predecessor"].as_str().is_some_and(|p| p.contains(FINAL))));
    assert!(named("rdm.node_admin.mesh.resolve.via-retained-receipts").iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["retained"] == "2" && s["attributes"]["remaining"] == "1"));
}

/// CONTRACT: a silent but unproven final primary refuses the resume by name (R11 step 2; R-D1:
/// unheard is not dead). With no exact Exited receipt of the predecessor in the Build, the
/// amendment is refused naming the predecessor and nothing is recorded; a recorded Exited of a
/// different runtime (a recycled pid) proves nothing either.
#[test]
fn an_unproven_silent_primary_refuses_the_resume_and_no_amendment_is_recorded() {
    let cap = capture();
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let birth = |name: &str| RosterBirth::of(records.iter().find(|n| n.name.to_string() == name).unwrap()).unwrap();
    let (lost, survivor) = (birth(FINAL), birth(OTHER_ADMIN));
    let lost_runtime = RuntimeProof::of(&handle(FINAL, 45_000).fact().unwrap());
    let recycled = RuntimeProof::of(&handle(FINAL, 45_001).fact().unwrap());
    let amendment = RosterAmended { predecessor: lost.clone(), final_primary: survivor.clone(), final_runtime: RuntimeProof::of(&handle(OTHER_ADMIN, 45_002).fact().unwrap()) };
    let (unproven, recycled_exit, steps, trace_id) = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.0.clone(), attempt, operation: outer.clone() };
        let root = tracing::info_span!("cell.mesh-leave-unproven", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let unproven = amend_roster(&builds, &key(2), "mesh1.admin.1", &amendment).await;
        let recycled_exit = record_predecessor_exit(&builds, &key(2), "mesh1.admin.1", &exited(&lost, &recycled)).await;
        (unproven, recycled_exit, builds.read_build(&build_id).await.unwrap().steps, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("unproven");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());

    let refused = unproven.expect_err("an unproven predecessor refuses the amendment");
    assert!(refused.contains(FINAL) && refused.contains("no exact Exited"), "the refusal names the unproven predecessor: {refused}");
    let recycled_refused = recycled_exit.expect_err("an Exited of another runtime proves nothing");
    assert!(recycled_refused.contains("runtime"), "{recycled_refused}");
    assert!(!steps.iter().any(|r| r.step == STEP_ROSTER_AMENDED), "nothing is amended");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.mesh.reject.via-unproven-primary").iter().any(|s| s["attributes"]["predecessor"].as_str().is_some_and(|p| p.contains(FINAL))));
}

/// CONTRACT: a repeated loss records one amendment per predecessor and the chain is explicit (R11
/// step 8). `mesh2.admin.1` is lost, `mesh2.admin.2` is amended in; `mesh2.admin.2` is lost too with
/// no admin left, and the one node-admin the fabric-primary created, `mesh2.admin.3` (a new birth,
/// not in the accepted roster), is amended in. The admission is the accepted roster plus
/// `mesh2.admin.3`, its final primary `mesh2.admin.3`; the leaving primary's roster is extended the
/// same way; a hand-off manifest that omits the amendment (its roster lacks `mesh2.admin.3`) is
/// refused by name.
#[test]
fn a_repeated_loss_records_one_amendment_per_predecessor_and_a_hand_off_omitting_one_is_refused() {
    let cap = capture();
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let fresh = node(NEW_ADMIN);
    let birth = |n: &Node| RosterBirth::of(n).unwrap();
    let by = |name: &str| birth(records.iter().find(|n| n.name.to_string() == name).unwrap());
    let runtime = |name: &str, pid: u32| RuntimeProof::of(&handle(name, pid).fact().unwrap());
    let (first, second, third) = (by(FINAL), by(OTHER_ADMIN), birth(&fresh));
    let a1 = RosterAmended { predecessor: first.clone(), final_primary: second.clone(), final_runtime: runtime(OTHER_ADMIN, 46_001) };
    let a2 = RosterAmended { predecessor: second.clone(), final_primary: third.clone(), final_runtime: runtime(NEW_ADMIN, 46_002) };
    let (r1, r2, admitted, mp, omitted, trace_id) = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.0.clone(), attempt, operation: outer.clone() };
        let root = tracing::info_span!("cell.mesh-leave-repeated-loss", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &first, &runtime(FINAL, 46_000), || live_roster(&records)).await.unwrap();
        let leaving = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, FINAL, &first, &runtime(FINAL, 46_000), || live_roster(&records)).await.unwrap();
        record_predecessor_exit(&builds, &key(2), "mesh1.admin.1", &exited(&first, &runtime(FINAL, 46_000))).await.unwrap();
        let r1 = amend_roster(&builds, &key(2), "mesh1.admin.1", &a1).await;
        record_predecessor_exit(&builds, &key(3), "mesh1.admin.1", &exited(&second, &runtime(OTHER_ADMIN, 46_001))).await.unwrap();
        let r2 = amend_roster(&builds, &key(3), "mesh1.admin.1", &a2).await;
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        let admitted = admitted_of(&steps, &outer).expect("a roster is recorded").expect("the admission reads");
        let mp = amended_roster(&leaving, &steps, &outer).unwrap();
        // A hand-off manifest over the accepted roster alone: it omits the amendment.
        let receipts = vec![exited(&by(FAILING), &runtime(FAILING, 46_003)), exited(&by(EXITING), &runtime(EXITING, 46_004)), exited(&first, &runtime(FINAL, 46_000)), exited(&second, &runtime(OTHER_ADMIN, 46_001))];
        let omitting = ExitManifest { mesh_id: mesh_id.to_string(), roster: leaving.clone(), final_primary: third.clone(), final_runtime: Some(a2.final_runtime.clone()), receipts };
        let facts = vec![BuildFact::Step(BuildStepReceipt { build_id: build_id.clone(), attempt: 3, operation: outer.clone(), step: STEP_OTHER_MEMBERS_EXITED.into(), outcome: rafka_node_admin_core::build_state::StepOutcome::Complete, output: serde_json::to_value(&omitting).ok(), executor: Some(NEW_ADMIN.into()) })];
        let omitted = handoff_verdict(&facts, &key(3), &admitted, &manifest_reference(&build_id.0, 3, &outer, false));
        let _ = step_facts(&facts);
        (r1, r2, admitted, mp, omitted, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("repeated-loss");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());

    assert_eq!((r1, r2), (Ok(Amended::Applied), Ok(Amended::Applied)), "one amendment per predecessor");
    assert_eq!(admitted.amendments, vec![a1, a2.clone()], "the chain is explicit, in order from the admitted primary");
    assert_eq!(admitted.final_primary, third, "the last amendment names the final primary");
    assert_eq!(admitted.final_runtime, a2.final_runtime);
    assert_eq!(sorted(admitted.roster.clone()), sorted(records.iter().map(birth).chain([third.clone()]).collect()), "the roster is the accepted one plus the created admin");
    assert_eq!(sorted(mp), sorted(admitted.roster.clone()), "the leaving primary runs over the same extended roster");
    match &omitted {
        HandoffVerdict::Unmatched { field, expected, reported } => {
            assert_eq!(field, "roster_amendment", "{omitted:?}");
            assert!(expected.contains(NEW_ADMIN) && reported.contains("omits"), "the refusal names the amended birth: {omitted:?}");
        }
        other => panic!("a manifest omitting the amendment is refused by name, not {other:?}"),
    }
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert_eq!(named("rdm.node_admin.mesh.update.via-roster-amended").iter().filter(|s| s["attributes"]["outcome"] == "applied").count(), 2);
}

/// CONTRACT: the amended final primary admits `LeaveMesh` from the owner's Build facts when gossip has
/// not yet brought it the amendment (R11 step 4; mesh-leave.md, "Resume after the primary's loss",
/// step 3: gossip is never authority). The fabric-primary recorded the lost primary's exact Exited
/// and `RosterAmended { mesh2.admin.1 -> mesh2.admin.2 }` in its own Build facts; `mesh2.admin.2`'s
/// own facts hold only the roster captured in attempt 1. Its admission reads the owner's facts: the
/// roster admits it, and the lost primary's exit receipt read from the owner keeps it out of the
/// node-admin tier, so no member workflow runs and the manifest covers both primaries.
#[test]
fn an_amended_primary_admits_the_leave_from_the_owners_facts_before_gossip_brings_the_amendment() {
    let cap = capture();
    let records: Vec<Node> = [OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 46_000 + i as u32))).collect();
    let birth = |name: &str| RosterBirth::of(records.iter().find(|n| n.name.to_string() == name).unwrap()).unwrap();
    let (lost, survivor) = (birth(FINAL), birth(OTHER_ADMIN));
    let (lost_runtime, survivor_runtime) = (RuntimeProof::of(&handles[FINAL].fact().unwrap()), RuntimeProof::of(&handles[OTHER_ADMIN].fact().unwrap()));
    let ran: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let (out, trace_id) = cap.run(async {
        let (owner, own) = (WireBuilds(MemoryBuildStateAdapter::new()), WireBuilds(MemoryBuildStateAdapter::new()));
        let build_id = BuildId::mint();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.0.clone(), attempt, operation: outer.clone() };
        for b in [&owner, &own] {
            b.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
            accepted_roster(b, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
            accepted_roster(b, &key(1), STEP_LEAVING_ROSTER, FINAL, &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        }
        let root = tracing::info_span!("cell.mesh-leave-owner-amendment", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        // The fabric-primary's resume, in its own facts only: gossip has not delivered it.
        record_predecessor_exit(&owner, &key(2), "mesh1.admin.1", &exited(&lost, &lost_runtime)).await.expect("the lost primary's exact exit is recorded");
        amend_roster(&owner, &key(2), "mesh1.admin.1", &RosterAmended { predecessor: lost.clone(), final_primary: survivor.clone(), final_runtime: survivor_runtime.clone() }).await.expect("the amendment is recorded");
        // mesh2.admin.2 admits LeaveMesh.
        let steps = own.read_build(&build_id).await.unwrap().steps;
        let leaving = accepted_row(&steps, &outer, STEP_LEAVING_ROSTER).unwrap().unwrap().roster;
        let owner_facts = owner.facts().await.unwrap();
        let (roster, known) = admission_roster(&leaving, &steps, &outer, &survivor, || async { Ok(owner_facts) }).await.expect("the admission reads");
        // Once its own Build facts hold the amendment naming it (gossip delivered it), the admission
        // reads the owner for nothing and names the amended admission from its own facts.
        let amended_steps = owner.read_build(&build_id).await.unwrap().steps;
        let (from_own, none) = admission_roster(&leaving, &amended_steps, &outer, &survivor, || async { Err::<Vec<BuildFact>, String>("the owner is read although this admin's own facts name it final primary".into()) }).await.expect("the admission reads its own facts");
        assert_eq!((sorted(from_own), none.len()), (sorted(roster.clone()), 0), "its own facts admit the same amended roster");
        let manifest = members_exited(&own, &key(2), OTHER_ADMIN, &roster, &survivor, survivor_runtime.clone(), &records, &known, |n: Node| {
            let ran = &ran;
            async move {
                ran.lock().unwrap().push(n.name.to_string());
                Err::<TerminalReceipt, String>(format!("{} is run again", n.name))
            }
        }, no_rows)
        .await;
        (manifest, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("owner_amendment");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let manifest = out.unwrap_or_else(|e| panic!("the amended primary admits the leave from the owner's facts: {e}"));
    assert!(ran.lock().unwrap().is_empty(), "no member workflow runs: the lost primary's exit receipt is read from the owner: {:?}", ran.lock().unwrap());
    assert_eq!(manifest.final_primary, survivor, "the manifest names the amended primary");
    assert_eq!(sorted(manifest.roster.clone()), sorted(vec![lost.clone(), survivor.clone()]), "the roster covers both primaries");
    assert_eq!(births(&manifest.receipts), BTreeSet::from([FINAL.to_string()]), "the lost primary's exit receipt is in the manifest");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.mesh.resolve.via-owner-build-facts").iter().any(|s| s["attributes"]["amendments"] == "1" && s["attributes"]["me"].as_str().is_some_and(|m| m.contains(OTHER_ADMIN))), "the admission read the owner's Build facts");
    assert!(named("rdm.node_admin.mesh.resolve.via-own-build-facts").iter().any(|s| s["attributes"]["amendments"] == "1" && s["attributes"]["me"].as_str().is_some_and(|m| m.contains(OTHER_ADMIN))), "the admission from its own facts names the amendment that admits it");
    assert!(named("rdm.node_admin.mesh.resolve.via-retained-receipts").iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["retained"] == "1" && s["attributes"]["remaining"] == "0"));
}

/// CONTRACT: the fabric-primary's own resume branch proves the lost final primary's exit from its
/// durable runtime row, never from the view (R11 steps 2-3; mesh-leave.md, "Resume after the
/// primary's loss", steps 1-2). No topology view is consulted: the lost `mesh2.admin.1` has left
/// every projection. With no runtime row of that birth the resume is refused by name and nothing is
/// amended (R-D1); with the row every admin stored the moment it first held the birth, the provider's
/// exact Exited is recorded as its roster receipt and `RosterAmended` admits `mesh2.admin.2`.
#[test]
fn the_fabric_primary_proves_a_lost_primarys_exit_from_its_durable_runtime_row_and_amends() {
    let cap = capture();
    let records: Vec<Node> = [OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 47_000 + i as u32))).collect();
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(), ..Default::default() };
    let birth = |name: &str| RosterBirth::of(records.iter().find(|n| n.name.to_string() == name).unwrap()).unwrap();
    let (lost, survivor) = (birth(FINAL), birth(OTHER_ADMIN));
    let (lost_runtime, survivor_runtime) = (RuntimeProof::of(&handles[FINAL].fact().unwrap()), RuntimeProof::of(&handles[OTHER_ADMIN].fact().unwrap()));
    let lost_node = records.iter().find(|n| n.name.to_string() == FINAL).unwrap();
    let row = rafka_node_admin_core::storage::RuntimeRow { node_id: lost_node.node_id.clone(), name: lost_node.name.clone(), incarnation_id: lost_node.incarnation_id.clone().unwrap(), runtime: handles[FINAL].fact().unwrap(), data_dir: None };
    // Where the proven exit is recorded: the fabric-primary's status store, held in memory.
    let exit_records = rafka_node_admin_core::admin::Records::default();
    exit_records.set_status(Arc::new(MemoryStatusStorage::default()), "mesh1".into(), "mesh1.admin.1".parse().unwrap(), NodeId::mint());
    let (without_row, with_row, before, after, trace_id) = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.0.clone(), attempt, operation: outer.clone() };
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let root = tracing::info_span!("cell.mesh-leave-fp-resume", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let admitted = |steps: &[BuildStepReceipt]| admitted_of(steps, &outer).unwrap().unwrap();
        let adm = admitted(&builds.read_build(&build_id).await.unwrap().steps);
        let without_row = resume_after_loss(&builds, &fleet, &exit_records, None, &key(2), "mesh1.admin.1", &adm, &survivor, &survivor_runtime).await;
        let before = admitted(&builds.read_build(&build_id).await.unwrap().steps);
        let with_row = resume_after_loss(&builds, &fleet, &exit_records, Some(&row), &key(2), "mesh1.admin.1", &adm, &survivor, &survivor_runtime).await;
        let after = admitted(&builds.read_build(&build_id).await.unwrap().steps);
        (without_row, with_row, before, after, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("fp_resume");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let refusal = without_row.expect_err("no runtime row of the lost birth: the resume is refused");
    let rafka_node_admin_core::executor::RunEnd::Incomplete(refusal) = refusal else { panic!("an unproven lost primary leaves the leave incomplete, not failed: {refusal:?}") };
    assert!(refusal.contains("not proven exited") && refusal.contains(FINAL) && refusal.contains("no durable runtime row"), "the refusal names the lost birth and the missing row: {refusal}");
    assert_eq!(before.final_primary, lost, "nothing is amended while the lost primary is unproven");
    assert_eq!(with_row, Ok(Amended::Applied), "with the durable runtime row the exit is proven and the amendment recorded");
    assert_eq!(after.final_primary, survivor, "the amendment admits the surviving admin");
    assert_eq!(after.final_runtime, survivor_runtime);
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.mesh.reject.via-unproven-primary").iter().any(|s| s["attributes"]["reason"].as_str().is_some_and(|r| r.contains("no durable runtime row"))));
    assert!(named("rdm.node_admin.mesh.update.via-predecessor-exit-recorded").iter().any(|s| s["attributes"]["predecessor"].as_str().is_some_and(|p| p.contains(FINAL))));
    assert!(named("rdm.node_admin.mesh.update.via-roster-amended").iter().any(|s| s["attributes"]["outcome"] == "applied" && s["attributes"]["final_primary"].as_str().is_some_and(|p| p.contains(OTHER_ADMIN))));
}

/// CONTRACT: when no admin of the leaving mesh survives, the one node-admin the fabric-primary
/// creates for it admits `LeaveMesh` over the roster the owner admitted plus the amendment that
/// names it, and never captures a roster of its own (mesh-leave.md, "Resume after the primary's
/// loss": "The accepted roster is captured once and never recaptured"; step 2: "or, when none
/// survives, the one node-admin the fabric-primary created for it"). mesh2 held one admin,
/// `mesh2.admin.1`, lost mid ordinary tier after `mesh2.rpc.2` exited. The fabric-primary recorded
/// the lost primary's exact Exited and `RosterAmended { mesh2.admin.1 -> mesh2.admin.3 }`; the new
/// admin is created at the next free admin ordinal above every one the roster and the view hold.
/// Its own Build facts hold no `LeavingRoster`: it reads the owner's, records no roster naming
/// itself, runs only `mesh2.rpc.1`, and its manifest passes the owner's hand-off validation.
#[test]
fn a_created_admin_of_a_leaving_mesh_runs_the_owners_admitted_roster_and_captures_none() {
    let cap = capture();
    let records: Vec<Node> = [FINAL, EXITING, FAILING, NEW_ADMIN].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 48_000 + i as u32))).collect();
    let birth = |name: &str| RosterBirth::of(records.iter().find(|n| n.name.to_string() == name).unwrap()).unwrap();
    let runtime = |name: &str| RuntimeProof::of(&handles[name].fact().unwrap());
    let (lost, created) = (birth(FINAL), birth(NEW_ADMIN));
    let admitted_births: Vec<Node> = records.iter().filter(|n| n.name.to_string() != NEW_ADMIN).cloned().collect();
    // The path: above every admin ordinal the roster and the view hold, departed births included.
    let held: Vec<PathName> = ["mesh2.admin.1", "mesh2.admin.2.old", "mesh2.rpc.4"].iter().map(|p| p.parse().unwrap()).collect();
    let path = rafka_node_admin_core::mesh_leave::leaving_mesh_admin_path("mesh2", &live_roster(&admitted_births).unwrap(), &held);
    let ran: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let (out, own_steps, verdict, trace_id) = cap.run(async {
        let (owner, own) = (WireBuilds(MemoryBuildStateAdapter::new()), WireBuilds(MemoryBuildStateAdapter::new()));
        let build_id = BuildId::mint();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.0.clone(), attempt, operation: outer.clone() };
        for b in [&owner, &own] {
            b.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
        }
        accepted_roster(&owner, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &runtime(FINAL), || live_roster(&admitted_births)).await.unwrap();
        // mesh2.rpc.2's exact exit, recorded by the lost primary and gossiped to the owner.
        owner
            .append_step_receipt(&BuildStepReceipt {
                build_id: build_id.clone(),
                attempt: 1,
                operation: rafka_node_admin_core::mesh_leave::member_operation(&outer, EXITING),
                step: "TerminateRuntime".into(),
                outcome: rafka_node_admin_core::build_state::StepOutcome::Complete,
                output: serde_json::to_value(exited(&birth(EXITING), &runtime(EXITING))).ok(),
                executor: Some(FINAL.into()),
            })
            .await
            .unwrap();
        let root = tracing::info_span!("cell.mesh-leave-created-admin", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        record_predecessor_exit(&owner, &key(2), "mesh1.admin.1", &exited(&lost, &runtime(FINAL))).await.expect("the lost primary's exact exit is recorded first");
        amend_roster(&owner, &key(2), "mesh1.admin.1", &RosterAmended { predecessor: lost.clone(), final_primary: created.clone(), final_runtime: runtime(NEW_ADMIN) }).await.expect("the amendment admits the created admin");
        // mesh2.admin.3 admits LeaveMesh; its capture would be the live members it sees now.
        let owner_facts = owner.facts().await.unwrap();
        let live_now: Vec<Node> = records.iter().filter(|n| [FAILING, NEW_ADMIN].contains(&n.name.to_string().as_str())).cloned().collect();
        let roster = rafka_node_admin_core::mesh_leave::leaving_roster(&own, &own.0, &key(2), NEW_ADMIN, &created, &runtime(NEW_ADMIN), || live_roster(&live_now), || async { Ok(owner_facts) }).await;
        let out = match roster {
            Ok((roster, known)) => members_exited(&own, &key(2), NEW_ADMIN, &roster, &created, runtime(NEW_ADMIN), &records, &known, |n: Node| {
                let (ran, t) = (&ran, exited(&RosterBirth::of(&n).unwrap(), &runtime(&n.name.to_string())));
                async move {
                    ran.lock().unwrap().push(n.name.to_string());
                    Ok::<TerminalReceipt, String>(t)
                }
            }, no_rows)
            .await,
            Err(e) => Err(e),
        };
        let own_steps = own.read_build(&build_id).await.unwrap().steps;
        let admitted = admitted_of(&owner.read_build(&build_id).await.unwrap().steps, &outer).unwrap().unwrap();
        let verdict = handoff_verdict(&own.facts().await.unwrap(), &key(2), &admitted, &manifest_reference(&build_id.0, 2, &outer, false));
        (out, own_steps, verdict, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("created_admin");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    assert_eq!(path.map(|p| p.to_string()), Ok(NEW_ADMIN.to_string()), "the created admin takes the next admin ordinal above every one held");
    let manifest = out.unwrap_or_else(|e| panic!("the created admin runs the owner's admitted roster: {e}"));
    assert!(!own_steps.iter().any(|r| r.step == STEP_LEAVING_ROSTER), "the created admin captures no roster of its own: {:?}", own_steps.iter().map(|r| r.step.clone()).collect::<Vec<_>>());
    assert_eq!(*ran.lock().unwrap(), vec![FAILING.to_string()], "only the member without an exit receipt runs");
    assert_eq!(sorted(manifest.roster.clone()), sorted(vec![lost.clone(), birth(EXITING), birth(FAILING), created.clone()]), "the roster is the admitted one plus the amendment");
    assert!(matches!(verdict, HandoffVerdict::Accept(_)), "the owner accepts the hand-off: {verdict:?}");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.mesh.resolve.via-owner-admitted-roster").iter().any(|s| s["attributes"]["amendments"] == "1" && s["attributes"]["roster"] == "4" && s["attributes"]["me"].as_str().is_some_and(|m| m.contains(NEW_ADMIN))), "the admission took the owner's admitted roster");
    assert!(named("rdm.node_admin.mesh.resolve.via-retained-receipts").iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["retained"] == "2" && s["attributes"]["remaining"] == "1"));
}

/// CONTRACT (R11 form (b); node-recovery.md step 2-3, run by the seat watch: "A node-admin watching
/// a silent seat holder (seat watch) runs this same ladder", then "the provider about the exact
/// runtime recorded at launch"): the node-admin the fabric-primary created for a leaving mesh holds
/// the lost primary's seat record but never heard its digest (the primary was gone before the admin
/// was born). Its seat watch proves the holder's exit from the runtime row handed into its
/// nodes.storage at its create, the exact runtime recorded at launch, so the seat yields to it. With
/// no digest and no row the finding stays unproven: silence is not death.
#[test]
fn a_created_admins_seat_watch_proves_the_lost_holders_exit_from_its_runtime_row() {
    use rafka_node_admin_core::seat_watch::{inspect_birth, Finding};
    use rafka_node_admin_core::storage::{MemoryNodesStorage, NodesStorage, RuntimeRow};
    let lost = node(FINAL);
    let h = handle(FINAL, 49_001);
    let fleet = Fleet { pids: [(49_001, FINAL.to_string())].into_iter().collect(), ..Default::default() };
    let book = rafka_mesh_transport::membership::DigestBook::default();
    let (without_row, with_row) = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let nodes = MemoryNodesStorage::default();
        let without_row = inspect_birth(&fleet, &book, Some(&nodes), &lost).await;
        nodes.put_runtime(&RuntimeRow { node_id: lost.node_id.clone(), name: lost.name.clone(), incarnation_id: lost.incarnation_id.clone().unwrap(), runtime: h.fact().unwrap(), data_dir: None }).await.unwrap();
        let with_row = inspect_birth(&fleet, &book, Some(&nodes), &lost).await;
        (without_row, with_row)
    });
    assert!(matches!(&without_row, Finding::Unproven { reason } if reason.contains("no runtime row")), "no digest and no row: unproven, naming both: {without_row:?}");
    assert_eq!(with_row, Finding::Exited { code: Some(0) }, "the runtime row recorded at launch is what the provider inspects");
}

/// CONTRACT (R11, the leave acts on the immutable accepted roster's exact births; mesh-leave.md
/// "Resume after the primary's loss" step 3, "Failure/certainty behavior": "No timeout or gossip
/// absence proves exit"): the admin the fabric-primary created for a leaving mesh with no admin
/// left works the roster, not its view's liveness. `mesh2.rpc.1` was stopped by the lost primary
/// before its exit was proven, and the created admin's view does not list it. It is still a roster
/// birth with no Exited receipt, and its record is the one this admin's membership holds of that
/// exact birth: the record of another birth at the path is never taken.
/// Its workflow runs, its row is written Deleted on its receipt, and the tier completes.
#[test]
fn a_roster_birth_the_view_does_not_list_is_worked_from_the_record_its_membership_holds() {
    let cap = capture();
    let records: Vec<Node> = [FAILING, EXITING, FINAL].iter().map(|n| node(n)).collect();
    let birth = |name: &str| RosterBirth::of(records.iter().find(|n| n.name.to_string() == name).unwrap()).unwrap();
    let (stopped, me) = (birth(FAILING), birth(FINAL));
    let roster = vec![stopped.clone(), birth(EXITING), me.clone()];
    // The created admin's view: itself and the member still heard; the stopped one is gone from it.
    let view: Vec<Node> = records.iter().filter(|n| n.name.to_string() != FAILING).cloned().collect();
    let mut held_stopped = records.iter().find(|n| n.name.to_string() == FAILING).unwrap().clone();
    held_stopped.status = NodeStatus::Leaving;
    let mut other_birth = held_stopped.clone();
    other_birth.incarnation_id = Some(IncarnationId::mint());

    let stale = roster_records(&view, &roster, |b| (b.node == FAILING).then(|| other_birth.clone()));
    assert!(!stale.iter().any(|n| n.name.to_string() == FAILING), "a held record of another birth at the path is not the roster birth's: {stale:?}");
    let worked = roster_records(&view, &roster, |b| (b.node == FAILING).then(|| held_stopped.clone()));
    assert_eq!(worked.iter().find(|n| n.name.to_string() == FAILING), Some(&held_stopped), "the stopped roster birth is worked from the record its membership holds: {worked:?}");
    assert_eq!(worked.len(), 3, "every roster birth has its record: {worked:?}");

    let runtime = RuntimeProof::of(&handle(FINAL, 46_000).fact().unwrap());
    let (manifest, deleted) = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let key = LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.0.clone(), attempt: 2, operation: operation(&mesh_id) };
        let deleted = std::sync::Mutex::new(Vec::new());
        // The leave's records, under the cell's capture: the held record is named as it is taken.
        let worked = roster_records(&view, &roster, |b| (b.node == FAILING).then(|| held_stopped.clone()));
        let manifest = members_exited(&builds, &key, FINAL, &roster, &me, runtime.clone(), &worked, &[], |n: Node| {
            let runtime = runtime.clone();
            async move { Ok(exited(&RosterBirth::of(&n).unwrap(), &runtime)) }
        }, |b: RosterBirth| {
            deleted.lock().unwrap().push(b.node.clone());
            std::future::ready(Ok(()))
        })
        .await;
        (manifest, deleted.into_inner().unwrap())
    });
    let manifest = manifest.expect("the tier completes over every roster birth");
    assert!(births(&manifest.receipts).contains(FAILING), "the stopped birth's exact Exited receipt is in the manifest: {:?}", births(&manifest.receipts));
    assert!(deleted.iter().any(|n| n == FAILING), "its row is written Deleted on its receipt: {deleted:?}");
    let held = cap.spans().into_iter().filter(|s| s["name"] == "rdm.node_admin.mesh.resolve.via-held-record").collect::<Vec<_>>();
    assert_eq!(held.iter().map(|s| (s["attributes"]["member"].as_str().unwrap_or("").to_string(), s["attributes"]["status"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>(), vec![(FAILING.to_string(), "Leaving".to_string())], "{held:#?}");
}

/// CONTRACT (mesh-delete.md, "Resume after the primary's loss", step 3: the new final primary "runs
/// only the tiers without a terminal receipt"; R11: the accepted roster is immutable and decides what
/// runs; R13: "Rebuild after a seat change reads the mesh's own rows"): the admin created for a
/// leaving mesh with no admin left joined after `mesh2.rpc.1` stopped, so neither its view nor its
/// membership holds that birth. The roster birth runs from the roster itself: its member workflow is
/// handed a record naming exactly that birth (path.name, node id, incarnation), its exact Exited
/// receipt is in the manifest, and its row, read from the mesh's own rows before the tiers run, is
/// written Deleted with its config kept. Must NOT happen: the tier refused "this view holds no record
/// of the accepted birth" (no_survivor, created mesh2.admin.2, attempts 2 and 3), or a Deleted row
/// written with a config the writer never read.
#[test]
fn a_roster_birth_the_created_admin_never_held_runs_from_the_roster_and_its_row_is_written_deleted() {
    let cap = capture();
    let all: Vec<Node> = [FAILING, EXITING, NEW_ADMIN]
        .iter()
        .map(|n| {
            let mut n = node(n);
            n.config = tier_of(&n.name.to_string());
            n
        })
        .collect();
    let birth = |name: &str| RosterBirth::of(all.iter().find(|n| n.name.to_string() == name).unwrap()).unwrap();
    let (stopped, me) = (birth(FAILING), birth(NEW_ADMIN));
    let roster = vec![stopped.clone(), birth(EXITING), me.clone()];
    // The created admin's records: itself and the member it hears; never the stopped birth.
    let records: Vec<Node> = all.iter().filter(|n| n.name.to_string() != FAILING).cloned().collect();
    let storage = Arc::new(MemoryNodesStorage::default());
    let me_id = all.iter().find(|n| n.name.to_string() == NEW_ADMIN).unwrap().node_id.clone();
    let rows = NodeRows::new(NEW_ADMIN.parse().unwrap(), me_id, Arc::new(RwLock::new(writer_view(&records, Some(NEW_ADMIN), None))), storage.clone());
    let runtime = RuntimeProof::of(&handle(NEW_ADMIN, 47_000).fact().unwrap());
    let ran: Mutex<Vec<Node>> = Mutex::new(Vec::new());
    let (manifest, trace_id) = cap.run(async {
        // Every member's row as its create left it. The created admin's writer holds the rows of the
        // births its view names; the stopped birth's row it never held.
        for n in &all {
            storage.put_node_row(&NodeRow { name: n.name.clone(), node_id: n.node_id.clone(), status: NodeStatus::ReadyForTraffic, config: n.config.clone(), tags: Default::default(), allocated: None }).await.unwrap().unwrap();
        }
        for n in &records {
            assert!(matches!(rows.claim(&n.name, &n.node_id, "create-node", None).await, Ok(Claimed::HeldByNode(_))), "the writer holds {}'s row", n.name);
        }
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let key = LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.0.clone(), attempt: 2, operation: operation(&mesh_id) };
        let root = tracing::info_span!("cell.mesh-leave-roster-birth", build_id = %build_id, operation = %key.operation);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        roster_rows_read(Some(&rows), NEW_ADMIN, &key.operation, &roster, &records).await;
        let manifest = members_exited(&builds, &key, NEW_ADMIN, &roster, &me, runtime.clone(), &records, &[], |n: Node| {
            ran.lock().unwrap().push(n.clone());
            let runtime = runtime.clone();
            async move { Ok(exited(&RosterBirth::of(&n).unwrap(), &runtime)) }
        }, |b: RosterBirth| {
            let (rows, records, operation) = (&rows, &records, key.operation.clone());
            async move { birth_row_deleted(Some(rows), NEW_ADMIN, &operation, records, &b).await }
        })
        .await;
        (manifest, trace_id)
    });
    let manifest = manifest.expect("the tier completes over every roster birth, the one the created admin never held included");
    assert!(births(&manifest.receipts).contains(FAILING), "the stopped birth's exact Exited receipt is in the manifest: {:?}", births(&manifest.receipts));
    let ran = ran.into_inner().unwrap();
    let worked = ran.iter().find(|n| n.name.to_string() == FAILING).expect("the stopped roster birth's workflow ran");
    assert_eq!(RosterBirth::of(worked).unwrap(), stopped, "its workflow is handed exactly the roster birth");
    let row = cap.run(async { storage.node_row_at(&FAILING.parse().unwrap()).await.unwrap() }).expect("the stopped birth's row stays at its path.name");
    assert_eq!((row.status, row.node_id.to_string(), row.config.clone()), (NodeStatus::Deleted, stopped.node_id.clone(), tier_of(FAILING)), "its row is written Deleted with the config its node held: {row:?}");
    let spans = cap.spans();
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name).cloned().collect::<Vec<_>>();
    let from_roster = named("rdm.node_admin.mesh.resolve.via-roster-birth");
    assert_eq!(from_roster.iter().map(|s| s["attributes"]["member"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>(), vec![FAILING.to_string()], "{from_roster:#?}");
    let read = named("rdm.node_admin.mesh.resolve.via-roster-row");
    assert_eq!(read.iter().map(|s| (s["attributes"]["member"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>(), vec![(FAILING.to_string(), "held".to_string())], "{read:#?}");
    let deleted = named("rdm.node_admin.node.delete.via-node-row").into_iter().filter(|s| s["attributes"]["subject"] == FAILING).collect::<Vec<_>>();
    assert!(deleted.iter().any(|s| s["attributes"]["outcome"] == "row-deleted" && s["attributes"]["config_from"] == "writer-record"), "{deleted:#?}");
    println!("trace_id={trace_id} via-roster-birth={} via-roster-row={}", from_roster[0]["trace_id"], read[0]["trace_id"]);
}

/// CONTRACT: the leaving mesh-primary admits `LeaveMesh` for a Build whose Accepted fact gossip has
/// not yet brought it (the command outran the fact, gate 044519ab canonical_ids: admitted at .810,
/// "no Build" at .811, Accepted heard at .905). Its own log holding no fact of the Build is the case
/// "its own do not yet admit it": the owner's Build facts are read (FetchBuildFacts) and absorbed
/// into its own Build log (node-build-hydration.md: "Facts are absorbed into the existing Build
/// log"), and the leave runs over the roster the owner accepted. Its own log then holds the Build.
#[test]
fn a_leaving_primary_that_has_not_heard_the_build_runs_the_leave_over_the_owners_facts() {
    let cap = capture();
    let records: Vec<Node> = [FINAL, EXITING].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 49_000 + i as u32))).collect();
    let me = RosterBirth::of(&records[0]).unwrap();
    let my_runtime = RuntimeProof::of(&handles[FINAL].fact().unwrap());
    let (roster, held, trace_id) = cap.run(async {
        let (owner, own) = (WireBuilds(MemoryBuildStateAdapter::new()), WireBuilds(MemoryBuildStateAdapter::new()));
        let build_id = BuildId::mint();
        let mesh_id = MeshId::mint();
        let key = LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.0.clone(), attempt: 1, operation: operation(&mesh_id) };
        owner.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
        accepted_roster(&owner, &key, STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &my_runtime, || live_roster(&records)).await.unwrap();
        let owner_facts = owner.facts().await.unwrap();
        let root = tracing::info_span!("cell.mesh-leave-unheard-build", build_id = %build_id);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let reads = std::sync::atomic::AtomicUsize::new(0);
        let roster = rafka_node_admin_core::mesh_leave::leaving_roster(&own, &own.0, &key, FINAL, &me, &my_runtime, || live_roster(&records), || async {
            reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(owner_facts)
        })
        .await;
        assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 1, "the owner's Build facts are read once");
        (roster, own.read_build(&build_id).await.map(|p| p.steps.iter().map(|r| r.step.clone()).collect::<Vec<_>>()), trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("unheard_build");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let (roster, _) = roster.unwrap_or_else(|e| panic!("a leaving primary that has not heard the Build runs the leave over the owner's facts: {e}"));
    assert_eq!(sorted(roster), sorted(live_roster(&records).unwrap()), "the roster is the one the owner accepted");
    let held = held.unwrap_or_else(|e| panic!("the owner's facts are absorbed: this admin's own log holds the Build: {e}"));
    assert!(held.iter().any(|s| s == STEP_ACCEPTED_ROSTER) && held.iter().any(|s| s == STEP_LEAVING_ROSTER), "its own log holds the owner's AcceptedRoster and the LeavingRoster it captured: {held:?}");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.build.update.via-owner-facts-absorbed").iter().any(|s| s["attributes"]["executor"] == FINAL && s["attributes"]["facts"] == "2" && s["attributes"]["steps"] == "1"), "the owner's Build facts were absorbed into this admin's own log: {:?}", spans.iter().map(|s| s["name"].clone()).collect::<Vec<_>>());
}

/// CONTRACT (mesh-leave.md step 4: "The fabric-primary validates exact roster coverage"; line 79:
/// "Missing receipts ... leave the workflow incomplete"): the final primary that calls mesh-leave
/// and holds no fact of the Build at all (FetchBuildFacts answers UnknownBuild) recorded no exit
/// manifest, so the fabric-primary's verdict is Incomplete at once, naming every roster birth but
/// the final primary as without an Exited receipt and naming the unknown Build; the open leave ends
/// inside the call instead of waiting out its handoff bound (gate 044519ab canonical_ids: 120 s).
/// A read that could not decide (not ready, unreached) stays undecided and the leave stays open: it is
/// refused `RejectedSourceFailed` naming the read, never answered as this admin's own `NotReady`.
#[test]
fn a_final_primary_that_holds_no_fact_of_the_build_ends_the_open_leave_incomplete() {
    let records: Vec<Node> = [FINAL, EXITING, FAILING].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 50_000 + i as u32))).collect();
    let roster = live_roster(&records).unwrap();
    let me = RosterBirth::of(&records[0]).unwrap();
    let admitted = Admitted { roster: roster.clone(), final_primary: me.clone(), final_runtime: RuntimeProof::of(&handles[FINAL].fact().unwrap()), amendments: Vec::new() };
    let mesh_id = MeshId::mint();
    let key = LeaveKey { mesh_id: mesh_id.clone(), build_id: BuildId::mint().0, attempt: 1, operation: operation(&mesh_id) };
    let reference = manifest_reference(&key.build_id, 1, &key.operation, false);
    let cap = capture();
    let (verdict, not_ready, trace_id) = cap.run(async {
        let root = tracing::info_span!("cell.mesh-leave-unknown-build-handoff", build_id = %key.build_id);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let unknown = rafka_node_admin_core::build_facts_read::FetchFailure::UnknownBuild(format!("the responder holds no fact of {}", key.build_id));
        let verdict = handoff_verdict_from_read(Err(unknown), &key, &admitted, &reference, "mesh1.admin.1", FINAL);
        let not_ready = handoff_verdict_from_read(Err(rafka_node_admin_core::build_facts_read::FetchFailure::NotReady("hydrating".into())), &key, &admitted, &reference, "mesh1.admin.1", FINAL);
        (verdict, not_ready, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("unknown_build_handoff");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    match verdict {
        Ok(v @ HandoffVerdict::Incomplete { .. }) => {
            let expected: BTreeSet<String> = roster.iter().filter(|b| **b != me).map(|b| format!("{} {}", b.node, b.incarnation_id)).collect();
            let HandoffVerdict::Incomplete { reason, without_exit } = &v else { unreachable!() };
            assert_eq!(without_exit.iter().cloned().collect::<BTreeSet<_>>(), expected, "every roster birth but the final primary is without an Exited receipt");
            assert!(reason.contains("holds no fact of") && reason.contains(&key.build_id), "the reason names the unknown Build: {reason}");
            assert!(
                matches!(v.refusal(), Some(StatusReply::RejectedIncompleteManifest { missing: 2, ref without_exit, ref reason }) if without_exit.iter().cloned().collect::<BTreeSet<_>>() == expected && reason.contains("holds no fact of")),
                "the handoff is refused by name, the births and the unknown Build named: {:?}",
                v.refusal()
            );
        }
        other => panic!("a final primary that holds no fact of the Build ends the leave incomplete, not undecided: {other:?}"),
    }
    assert!(matches!(&not_ready, Err(StatusReply::RejectedSourceFailed { cause, .. }) if cause.contains("hydrating")), "a read that could not decide leaves the leave open, refused by name as a read that failed: {not_ready:?}");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert_eq!(named("rdm.node_admin.mesh.reject.via-incomplete-handoff").iter().filter(|s| s["attributes"]["roster"] == "3" && s["attributes"]["exited"] == "0").count(), 1, "one incomplete handoff is refused, for the unknown Build only");
}

/// A Build log whose executor is cut once: the `step` receipt of `operation` is refused, as the fault
/// door's `Receipt` cut leaves it (`FaultedBuilds::append_step_receipt` parks BEFORE the receipt is
/// durable, so the step's work is done and its receipt is not). Everything else reads and writes the
/// log it wraps.
struct CutBuilds<'a> {
    inner: &'a WireBuilds,
    cut: Mutex<Option<(&'static str, String)>>,
}

#[async_trait::async_trait]
impl BuildStateAdapter for CutBuilds<'_> {
    async fn publish_accepted(&self, a: &BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(a).await
    }
    async fn open_attempt(&self, o: &AttemptOpened) -> Result<(), BuildStateError> {
        self.inner.open_attempt(o).await
    }
    async fn read_build(&self, id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.inner.read_build(id).await
    }
    async fn current_build(&self) -> Result<rafka_node_admin_core::build_state::CurrentBuild, rafka_node_admin_core::build_state::BuildStateError> {
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, rafka_node_admin_core::build_state::BuildStateError> {
        self.inner.newest_accepted_at_ms().await
    }
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, c: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        self.inner.claim_attempt(c).await
    }
    async fn adopt_claim(&self, c: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.inner.adopt_claim(c).await
    }
    async fn append_step_receipt(&self, r: &BuildStepReceipt) -> Result<(), BuildStateError> {
        let cut = {
            let mut cut = self.cut.lock().unwrap();
            if cut.as_ref().is_some_and(|(step, operation)| *step == r.step && *operation == r.operation) {
                cut.take()
            } else {
                None
            }
        };
        if let Some((step, operation)) = cut {
            return Err(BuildStateError::Io(format!("the executor is cut after the work of {step} and before its receipt of {operation} is durable")));
        }
        self.inner.append_step_receipt(r).await
    }
    async fn append_attempt_receipt(&self, r: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.inner.append_attempt_receipt(r).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(id).await
    }
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<rafka_node_admin_core::build_state::OwedExit>, BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
    }
}

/// The departures a pipeline publishes: the operation of every `NodeDeleted` it published.
#[derive(Default)]
struct Departures(Mutex<Vec<String>>);

#[async_trait::async_trait]
impl LifecycleEvents for Departures {
    async fn deleting(&self, _: &LifecycleOp) {}
    async fn deleted(&self, op: &LifecycleOp) {
        self.0.lock().unwrap().push(op.operation.clone());
    }
    async fn restarting(&self, _: &LifecycleOp) {}
    async fn stopped(&self, _: &LifecycleOp) {}
    async fn joined(&self, _: &LifecycleOp) {}
    async fn draining(&self, _: &LifecycleOp) {}
    async fn leaving(&self, _: &LifecycleOp) {}
    async fn mesh_leaving(&self, _: &MeshLeaveEvent) {}
    async fn mesh_leave(&self, _: &MeshLeaveEvent) {}
    async fn mesh_left(&self, _: &MeshLeaveEvent) {}
    fn now_rafka_ms(&self) -> u64 {
        0
    }
}

/// CONTRACT: a member's delete is done only when every step of it has its receipt, and a resumed
/// leave runs the steps that have none, in order, and never one that has (mesh-delete.md, "Resume
/// after the primary's loss" step 3: the new mesh-primary "runs only the tiers without a terminal
/// receipt"; node-delete.md step 4: the executor records the complete delete receipt and emits
/// node.deleted; the Build carrier: "Completed steps are not rerun"). Attempt 1: `mesh2.rpc.1` is
/// stopped and its runtime terminated, the provider's exact Exited is receipted (TerminateRuntime),
/// and the executor is cut before NodeDeleted's receipt is durable: NodeDeleted is never published
/// and the delete has no `Complete`. `mesh2.rpc.2` runs its whole delete. Attempt 2 (another
/// mesh-primary, the same Build) takes `mesh2.rpc.2` as done and finishes `mesh2.rpc.1`'s delete:
/// its runtime is not terminated again (TerminateRuntime is reused), NodeDeleted is executed,
/// receipted and published once, and the delete ends in `Complete`. Every other peer keeps the
/// stopped birth until mesh-left if NodeDeleted is never published: the exit proof alone is not the
/// departure.
#[test]
fn a_delete_cut_after_its_exit_proof_is_finished_by_the_resumed_attempt_from_its_missing_steps() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-leave-cut-delete-{}", NodeId::mint()));
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 44_000 + i as u32))).collect();
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(), ..Default::default() };
    let me = RosterBirth::of(records.iter().find(|n| n.name.to_string() == FINAL).unwrap()).unwrap();
    let final_runtime = RuntimeProof::of(&handles[FINAL].fact().unwrap());
    let view = View::default();
    let departures = Departures::default();
    let calls: Mutex<Vec<(u32, String)>> = Mutex::new(Vec::new());

    let (first, cut_steps, second, steps, trace_id) = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.0.clone(), attempt, operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rafka-rpc-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rafka_mesh_entity::launch::Launcher { name: FINAL.parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            mesh_issuer: None,
        };
        let cut = CutBuilds { inner: &builds, cut: Mutex::new(Some(("NodeDeleted", format!("retire-node:{FAILING}")))) };
        let cut_pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &departures, builds: &cut, template: &template, timeouts: Timeouts::default() };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &departures, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let shutdown_1 = |n: Node| {
            let (pipeline, handles, calls, build_id) = (&cut_pipeline, &handles, &calls, &build_id);
            async move {
                let name = n.name.to_string();
                calls.lock().unwrap().push((1, name.clone()));
                pipeline.retire(&rafka_node_admin_core::deployment::pipeline::RetireRequest { build_id: (*build_id).clone(), attempt: 1, node: n, handle: handles[&name].clone(), exit: None }).await.map_err(|e| e.to_string())
            }
        };
        let shutdown_2 = |n: Node| {
            let (pipeline, handles, calls, build_id) = (&pipeline, &handles, &calls, &build_id);
            async move {
                let name = n.name.to_string();
                calls.lock().unwrap().push((2, name.clone()));
                pipeline.retire(&rafka_node_admin_core::deployment::pipeline::RetireRequest { build_id: (*build_id).clone(), attempt: 2, node: n, handle: handles[&name].clone(), exit: None }).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-leave-cut-delete", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let mp = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key(1), FINAL, &mp, &me, final_runtime.clone(), &records, &[], shutdown_1, no_rows).await;
        let cut_steps = builds.read_build(&build_id).await.unwrap().steps;
        let records2: Vec<Node> = view.holds(&records);
        let mp2 = accepted_roster(&builds, &key(2), STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records2)).await.unwrap();
        let second = members_exited(&builds, &key(2), FINAL, &mp2, &me, final_runtime.clone(), &records2, &[], shutdown_2, no_rows).await;
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        (first, cut_steps, second, steps, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("cut_delete");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    let err = first.expect_err("attempt 1 is cut inside mesh2.rpc.1's delete: the ordinary tier is incomplete");
    assert!(err.contains("tier ordinary-nodes is incomplete") && err.contains(FAILING), "{err}");
    let calls = calls.into_inner().unwrap();
    let ran = |attempt: u32| calls.iter().filter(|(a, _)| *a == attempt).map(|(_, n)| n.clone()).collect::<BTreeSet<_>>();
    assert_eq!(ran(1), BTreeSet::from([FAILING.to_string(), EXITING.to_string()]), "attempt 1 ran the ordinary tier");
    let of = |steps: &[BuildStepReceipt], node: &str, step: &str| steps.iter().filter(|r| r.operation == format!("retire-node:{node}") && r.step == step && r.outcome == rafka_node_admin_core::build_state::StepOutcome::Complete).map(|r| r.attempt).collect::<Vec<_>>();
    // The cut left rpc.1's exit proof and no more; rpc.2's delete ran to its end.
    assert_eq!((of(&cut_steps, FAILING, "TerminateRuntime"), of(&cut_steps, FAILING, "NodeDeleted"), of(&cut_steps, FAILING, "Complete")), (vec![1], vec![], vec![]), "the cut left {FAILING}'s exit proof and no NodeDeleted: {cut_steps:#?}");
    assert_eq!(of(&cut_steps, EXITING, "Complete"), vec![1], "{EXITING}'s delete ran to its end");

    // Attempt 2: the exit proof is not the delete. The delete with no NodeDeleted is run to its end.
    assert_eq!(ran(2), BTreeSet::from([FAILING.to_string(), OTHER_ADMIN.to_string()]), "attempt 2 finishes {FAILING}'s delete (its exit proof alone is not a finished delete) and runs the node-admin tier, and does not run {EXITING}, whose delete has every step's receipt");
    let manifest = second.unwrap_or_else(|e| panic!("the resumed attempt completes: {e}"));
    assert_eq!(births(&manifest.receipts), BTreeSet::from([FAILING.to_string(), EXITING.to_string(), OTHER_ADMIN.to_string()]), "the manifest carries every other member's receipt");
    assert_eq!(of(&steps, FAILING, "TerminateRuntime"), vec![1], "{FAILING}'s completed TerminateRuntime is not run again");
    assert_eq!((of(&steps, FAILING, "NodeDeleted"), of(&steps, FAILING, "Complete")), (vec![2], vec![2]), "{FAILING}'s NodeDeleted and Complete are receipted once, by the resumed attempt");
    assert_eq!(fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == FAILING).count(), 1, "{FAILING}'s runtime was terminated once, in attempt 1");
    assert_eq!(fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == EXITING).count(), 1, "{EXITING}'s runtime was terminated once");
    let mut published = departures.0.into_inner().unwrap();
    published.sort();
    let mut want: Vec<String> = [FAILING, EXITING, OTHER_ADMIN].iter().map(|n| format!("retire-node:{n}")).collect();
    want.sort();
    assert_eq!(published, want, "each member's NodeDeleted is published once; {FAILING}'s by the resumed attempt");

    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let step = |node: &str, step: &str, attempt: &str| named("rdm.node_admin.deployment.update.via-step").into_iter().filter(|s| s["attributes"]["node"] == node && s["attributes"]["step"] == step && s["attributes"]["attempt"] == attempt).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>();
    assert_eq!(step(FAILING, "TerminateRuntime", "2"), vec!["reused".to_string()], "attempt 2 reuses the receipted TerminateRuntime");
    assert_eq!(step(FAILING, "NodeDeleted", "2"), vec!["complete".to_string()], "attempt 2 executes the NodeDeleted that has no receipt");
    let retained = named("rdm.node_admin.mesh.resolve.via-retained-receipts");
    assert!(retained.iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["retained"] == "1" && s["attributes"]["retained_births"] == EXITING && s["attributes"]["remaining"] == "2"), "attempt 2 keeps the one finished delete and runs the other two members: {retained:?}");
    eprintln!("trace {trace_id}: attempt 2 via-step {FAILING} TerminateRuntime reused, NodeDeleted complete; via-retained-receipts retained 1 ({EXITING}) remaining 2");
}
