//! The mesh-delete resumption (i143.e12.s39, s49; mesh-delete.md steps 1, 3 and "Resume after the
//! primary's loss"): the deleting mesh-primary's member workflows are the product's
//! (`mesh_delete::members_exited` over each member's node.delete, `DeploymentPipeline::delete`); the
//! provider, the Build state and the view are fixtures.
//!
//! mesh2 leaves: two ordinary nodes, one other node-admin and the final primary `mesh2.admin.1`.
//! In attempt 1 one ordinary node exits and the provider cannot inspect the other after its stop
//! ladder, so the ordinary tier is incomplete and the node-admin tier never starts. The resumed
//! attempt sees the view as the first one left it: neither ordinary node is live any more.

use rdm_admin::node_rows::WriteRecord as _;
use rdm_admin::node_rows::RecordAt as _;
use rdm_core::{FabricId, IncarnationId, LifecycleOp, MeshId, NodeId};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome, MemoryBuildStateAdapter};
use rdm_admin::fabric_builds::BuildMessage;
use rdm_admin::deployment::pipeline::{DeploymentPipeline, LaunchTemplate, LifecycleEvents, NodeObserver, NoLifecycleEvents, Publication, RuntimeProof, TerminalReceipt, Timeouts, TopologySink};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::join::Joins;
use rdm_admin::mesh_delete::{await_mesh_deleted, MeshDeleteView, MESH_DELETED_WAIT, completion_of, mesh_left_proof, record_completion, replayed_handoff, roster_records, roster_rows_read, mesh_left_rows, accepted_roster, accepted_row, admission_roster, admitted_of, resume_after_loss, amend_roster, amended_roster, first_mismatch, handoff_verdict, handoff_verdict_from_read, manifest_reference, members_exited, operation, owner_resume, Admitted, Amended, ExitManifest, HandoffVerdict, MeshDeleteKey, OwnerResume, RosterAmended, RosterBirth, STEP_ACCEPTED_ROSTER, STEP_LEAVING_ROSTER, STEP_MESH_DELETED, STEP_OTHER_MEMBERS_EXITED, STEP_ROSTER_AMENDED};
use rdm_core::status::StatusReply;
use rdm_admin::model::{DeploymentId, Fabric, Mesh, Node, NodeStatus, PathName, ProviderKind, ScopeStatus};
use rdm_admin::node_rows::{ClaimFor, Claimed, NodeRows, RowRefused};
use rdm_admin::status_rpc::Declared;
use rdm_admin::storage::{MemoryMeshStorage, MeshStorage};
use rdm_admin::storage::{MemoryNodesStorage, NodeRow};
use rdm_admin::topology::Topology;
use rdm_core::status::MeshState;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Mutex;

const DOMAIN: &str = "mesh-delete-resume";
const FAILING: &str = "mesh2.rpc.1";
const EXITING: &str = "mesh2.rpc.2";
const OTHER_ADMIN: &str = "mesh2.admin.2";
const FINAL: &str = "mesh2.admin.1";

/// This cell's own span capture: the product span pipeline (`rdm_transport::span_capture`: its
/// span filter and `BatchSpanProcessor`) in front of an in-memory exporter, behind a dispatcher
/// installed on every thread of the cell's own runtime.
pub(crate) struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

pub(crate) fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("mesh2.admin.1", exporter.clone());
    Capture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
}

/// The trace id of `span`, a cell's root, as the capture's pipeline stamps it.
pub(crate) fn trace_id(span: &tracing::Span) -> String {
    span.in_scope(rdm_transport::current_traceparent).and_then(|tp| tp.split('-').nth(1).map(str::to_string)).expect("the cell's root span has a trace context: the capture is installed on this thread")
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
                    "start_ns": s.start_time.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0),
                    "end_ns": s.end_time.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0),
                    "attributes": attributes,
                })
            })
            .collect()
    }
}

/// The Build state as a node-admin holds it: every fact is sent in its wire shape
/// (`BuildMessage::to_bytes`, the Build-fact codec) and the fact
/// the wire gives back is the one recorded. A step output with no wire shape is refused here
/// exactly as `FabricBuildStateAdapter` refuses it.
#[derive(Default)]
struct WireBuilds(MemoryBuildStateAdapter);

impl WireBuilds {
    fn wire(fact: BuildFact) -> Result<BuildFact, BuildStateError> {
        let bytes = BuildMessage { nonce: 1, facts: vec![fact] }.to_bytes()?;
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
    async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, rdm_admin::build_state::BuildStateError> {
        self.0.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, rdm_admin::build_state::BuildStateError> {
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
    async fn committed_facts(&self, build: Option<&rdm_admin::build::BuildId>) -> Result<Vec<rdm_admin::build_state::BuildFact>, rdm_admin::build_state::BuildStateError> {
        let facts = self.facts().await?;
        Ok(match build { Some(b) => facts.into_iter().filter(|f| f.build_id() == b).collect(), None => facts })
    }
    async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
        self.0.forget(id).await
    }
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.0.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.0.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &rdm_admin::build::BuildId, mesh: &str) -> Result<Vec<rdm_admin::build_state::OwedExit>, BuildStateError> {
        self.0.exits_owed(build_id, mesh).await
    }
}

/// The host's runtimes: every termination, and the one inspection the provider cannot answer: the
/// first that follows the terminate of `blind_once`'s runtime (the stop ladder's terminal inspection,
/// never the delete's inspection at its instant, which precedes it).
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
        let terminated = h.pid.and_then(|pid| self.pids.get(&pid)).is_some_and(|n| self.terminated.lock().unwrap().contains(n));
        if *blind == h.pid && terminated {
            *blind = None;
            return DeploymentStatus::Unknown;
        }
        DeploymentStatus::Exited { code: Some(0) }
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
    }
}

/// No birth is commanded over Node RPC here: every connections delete/stop command is `NotSent` and its wait is
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
struct View(Mutex<BTreeMap<String, Node>>, Mutex<BTreeSet<String>>, Option<Arc<NodeRows>>);
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
    fn node_rows(&self) -> Option<Arc<NodeRows>> {
        self.2.clone()
    }
}

impl View {
    /// A sink whose executor writes node rows through `rows`: node.delete's `NodeDeleted` step writes
    /// the member's row `Deleted` through it (node-delete.md step 3).
    fn with_rows(rows: Arc<NodeRows>) -> Self {
        Self(Mutex::default(), Mutex::default(), Some(rows))
    }

    /// `records` as the view holds them now: a birth a node.delete removed is no longer held, a
    /// published one is its last record, any other its record as given.
    fn holds(&self, records: &[Node]) -> Vec<Node> {
        let (view, removed) = (self.0.lock().unwrap(), self.1.lock().unwrap());
        records.iter().filter(|n| !removed.contains(&n.name.to_string())).map(|n| view.get(&n.name.to_string()).cloned().unwrap_or_else(|| n.clone())).collect()
    }
}

fn node(name: &str) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::Ready;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

fn handle(name: &str, pid: u32) -> DeploymentHandle {
    DeploymentHandle { deployment_id: DeploymentId::parse(&rdm_core::fixture_crockford60(&format!("dep-{name}"))).unwrap(), provider: ProviderKind::Process, pid: Some(pid), start: Some(1), container: None, domain: Some(DOMAIN.into()) }
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

/// The receipts node.delete leaves for the exact birth `terminal` names, run to its end as a crashed
/// target by `executor` under `key`'s Build and attempt (node-delete.md "Crashed target"): what the
/// fabric-primary's node.delete of a lost primary records, whose `TerminateRuntime` is the birth's
/// roster receipt (mesh-delete.md "Resume after the primary's loss" step 1).
async fn node_delete_receipts(builds: &dyn BuildStateAdapter, key: &MeshDeleteKey, executor: &str, terminal: &TerminalReceipt) {
    let operation = format!("node.delete:{}", terminal.node);
    let op = LifecycleOp { build_id: key.build_id.to_string(), attempt: key.attempt, operation: operation.clone(), node_id: NodeId::parse(&terminal.node_id).unwrap(), incarnation: IncarnationId::parse(&terminal.incarnation_id).unwrap(), name: terminal.node.parse().unwrap(), event_at_rafka_ms: 0, traceparent: None, tracestate: None };
    for (step, output) in [("NodeDeleting", serde_json::to_value(&op).ok()), ("TerminateRuntime", serde_json::to_value(terminal).ok()), ("NodeDeleted", serde_json::to_value(&op).ok()), ("RemoveTopologyMembership", None), ("Complete", None)] {
        builds
            .append_step_receipt(&BuildStepReceipt { mesh: None, build_id: key.build_id.clone(), attempt: key.attempt, operation: operation.clone(), step: step.into(), outcome: rdm_admin::build_state::StepOutcome::Complete, output, executor: Some(executor.into()) })
            .await
            .unwrap();
    }
}

fn tier_of(name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("tier".to_string(), format!("tier-of-{name}"))])
}

/// The view of a writer of mesh2's rows: `records`, with `primary` the mesh's primary node-admin (a
/// live role) and `fabric_primary` holding the fabric-primary role.
fn writer_view(records: &[Node], primary: Option<&str>, fabric_primary: Option<Node>) -> Topology {
    let mut nodes: Vec<Node> = records.iter().cloned().map(|mut n| {
        n.is_primary = Some(n.name.to_string().as_str()) == primary;
        n
    }).collect();
    nodes.extend(fabric_primary);
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready }],
        nodes,
    }
}

/// Every node's row in `storage` as its create left it (live, its config), held by `rows`.
async fn rows_written(storage: &MemoryNodesStorage, rows: &NodeRows, records: &[Node]) {
    for n in records {
        storage.write_record(&NodeRow { topology_storage_provider: None, name: n.name.clone(), node_id: n.node_id.clone(), status: NodeStatus::Ready, config: tier_of(&n.name.to_string()), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }).await.unwrap();
        assert!(matches!(rows.claim(&n.name, &n.node_id, "create-node", None, None, ClaimFor::Create).await, Ok(Claimed::HeldByNode(_))), "the writer holds {}'s row", n.name);
    }
}

fn artifact_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cells/mesh_delete_resume")
}

/// CONTRACT: a resumed mesh leave runs only the members without an Exited receipt and proves the
/// whole accepted roster. Attempt 1: one ordinary node exits, the other's exit cannot be proven, so
/// the ordinary tier is incomplete and the node-admin tier does not start. Attempt 2 (the view no
/// longer holds the exited node, and the unproven node, whose stop-node was never admitted, is not
/// shown stopping: states.md "Who sets each state") keeps the roster both sides accepted at admission, keeps
/// the exited node's receipt and does not run it again, runs the unproven node and then the other
/// node-admin; the exit manifest and the MeshLeft proof set cover every accepted birth. Every
/// receipt goes through the Build-fact wire codec, as a node-admin's Build state sends it.
/// R-MN1 (mesh-delete.md step 3): each member's row is written Deleted by its own node.delete's
/// `NodeDeleted` step, its node id and config kept, and never the primary's own; attempt 2, which
/// keeps the exited node's receipts and does not run it again, writes nothing for it: its row was
/// node.delete's, once.
#[test]
fn a_resumed_leave_keeps_the_exited_members_receipts_and_runs_only_the_rest() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-delete-resume-{}", NodeId::mint()));
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 41_000 + i as u32))).collect();
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(), ..Default::default() };
    *fleet.blind_once.lock().unwrap() = handles[FAILING].pid;
    let me = RosterBirth::of(records.iter().find(|n| n.name.to_string() == FINAL).unwrap()).unwrap();
    let final_runtime = RuntimeProof::of(&handles[FINAL].fact().unwrap());
    let calls: Mutex<Vec<(u32, String)>> = Mutex::new(Vec::new());
    let storage = Arc::new(MemoryNodesStorage::default());
    let final_id = records.iter().find(|n| n.name.to_string() == FINAL).unwrap().node_id.clone();
    let rows = Arc::new(NodeRows::new(FINAL.parse().unwrap(), final_id, Arc::new(RwLock::new(writer_view(&records, Some(FINAL), None))), storage.clone()));
    let view = View::with_rows(rows.clone());

    let (first, fp1, fp2, mp1, mp2, second, records2, trace_id) = cap.run(async {
        rows_written(&storage, &rows, &records).await;
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rdm-rpc-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { name: FINAL.parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let shutdown = |attempt: u32| {
            let (pipeline, handles, calls, build_id) = (&pipeline, &handles, &calls, &build_id);
            move |n: Node| async move {
                let name = n.name.to_string();
                calls.lock().unwrap().push((attempt, name.clone()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: (*build_id).clone(), attempt, node: n, handle: handles[&name].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-delete-resume", build_id = %build_id.log(), operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Attempt 1: both sides capture the roster at admission; the ordinary tier is incomplete.
        let fp1 = accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let mp1 = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key(1), FINAL, &mp1, &me, final_runtime.clone(), &records, &[], shutdown(1)).await;

        // Attempt 2 reads the view the first one left: the node its node.delete removed is gone.
        let records2: Vec<Node> = view.holds(&records);
        let fp2 = accepted_roster(&builds, &key(2), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &final_runtime, || live_roster(&records2)).await.unwrap();
        let mp2 = accepted_roster(&builds, &key(2), STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records2)).await.unwrap();
        let second = members_exited(&builds, &key(2), FINAL, &mp2, &me, final_runtime.clone(), &records2, &[], shutdown(2)).await;
        (first, fp1, fp2, mp1, mp2, second, records2, trace_id)
    });
    let spans = cap.spans();
    std::fs::create_dir_all(artifact_dir()).unwrap();
    std::fs::write(artifact_dir().join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", artifact_dir().join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    // Attempt 1: the ordinary tier is incomplete by name, and the node-admin tier never started.
    let err = first.expect_err("the ordinary tier holds an unproven exited verdict");
    assert!(err.contains("tier ordinary-nodes is incomplete") && err.contains(FAILING), "{err}");
    let calls = calls.into_inner().unwrap();
    let ran = |attempt: u32| calls.iter().filter(|(a, _)| *a == attempt).map(|(_, n)| n.clone()).collect::<BTreeSet<_>>();
    assert_eq!(ran(1), BTreeSet::from([FAILING.to_string(), EXITING.to_string()]), "attempt 1 ran the ordinary tier only");
    assert!(records2.iter().all(|n| n.name.to_string() != EXITING), "the resumed attempt's view no longer holds the node attempt 1's node.delete removed: {records2:?}");
    // This cell's observer holds no Node RPC client, so the unproven node's stop-node was never sent: nothing
    // set it stopping (states.md "Who sets each state": stopping on the stop call's admission, never before).
    let unproven = records2.iter().find(|n| n.name.to_string() == FAILING).expect("the resumed attempt's view still holds the unproven node");
    let held = records.iter().find(|n| n.name.to_string() == FAILING).unwrap();
    assert_eq!(unproven.status, held.status, "a member whose stop-node was never admitted keeps its status, never stopping: {unproven:?}");

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
    all.receipts.push(TerminalReceipt { node: me.node.clone(), node_id: me.node_id.clone(), incarnation_id: me.incarnation_id.clone(), runtime: Some(final_runtime.clone()), exit_code: Some(0) , verdict: None, data_dir: None });
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
    // and config, each written by its own node.delete's NodeDeleted step; the exited node's in attempt
    // 1, which attempt 2 kept. The final primary's own row is not the mesh-primary's to delete (step 7
    // is the fabric-primary's).
    let read = |name: &str| {
        let storage = storage.clone();
        let name: PathName = name.parse().unwrap();
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async move { storage.record_at(&name).await.unwrap() })
    };
    for n in records.iter().filter(|n| n.name.to_string() != FINAL) {
        let row = read(&n.name.to_string()).unwrap_or_else(|| panic!("{} has a row", n.name));
        assert_eq!((row.status, &row.node_id, &row.config), (NodeStatus::Deleted, &n.node_id, &tier_of(&n.name.to_string())), "{}'s row is Deleted with its node id and config: {row:?}", n.name);
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
/// primary (node-rpc-envelope.md, "Mesh delete command pair": "A stale/conflicting workflow identity or
/// incomplete manifest is refused by name with a typed Status refusal naming the mismatch"). What
/// must NOT happen: `NotReady`, the answer of a door that is not serving yet.
#[test]
fn an_incomplete_tier_is_refused_by_name_naming_the_births_without_an_exited_receipt() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-deleted-incomplete-{}", NodeId::mint()));
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
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt: 1, operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rdm-rpc-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { name: FINAL.parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let root = tracing::info_span!("cell.mesh-deleted-incomplete", build_id = %build_id.log(), operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let roster = accepted_roster(&builds, &key, STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let mp = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let tier = members_exited(&builds, &key, FINAL, &mp, &me, final_runtime.clone(), &records, &[], |n: Node| {
            let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
            async move {
                let name = n.name.to_string();
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: (*build_id).clone(), attempt: 1, node: n, handle: handles[&name].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        })
        .await;
        // The handoff the leaving primary sends for the incomplete run, judged over its facts.
        let facts = builds.facts().await.unwrap();
        let admitted = Admitted { roster, final_primary: me.clone(), final_runtime: final_runtime.clone(), amendments: Vec::new() };
        let verdict = handoff_verdict(&facts, &key, &admitted, &manifest_reference(key.build_id.as_str(), 1, &outer, false));
        (tier, verdict, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("incomplete");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    let err = tier.expect_err("the ordinary tier holds an unproven exited verdict");
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
/// end is the ordinary node.delete (`DeploymentPipeline::delete`, a separate Build): the completion
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
                .publish_accepted(&BuildAccepted { build_id: b.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
                .await
                .unwrap();
        }
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt: 1, operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rdm-rpc-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let root = tracing::info_span!("cell.mesh-delete-completion", build_id = %build_id.log(), operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        accepted_roster(&builds, &key, STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let admitted = admitted_of(&builds.read_build(&build_id).await.unwrap().steps, &outer).unwrap().unwrap();
        let mp = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records)).await.unwrap();
        members_exited(&builds, &key, FINAL, &mp, &me, final_runtime.clone(), &records, &[], |n: Node| {
            let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
            async move {
                let name = n.name.to_string();
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: (*build_id).clone(), attempt: 1, node: n, handle: handles[&name].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        })
        .await
        .expect("every other member exits");
        // The owner's validated handoff, then mesh.deleted's completion: nothing of the final primary
        // is stopped.
        let facts = builds.facts().await.unwrap();
        let HandoffVerdict::Accept(manifest) = handoff_verdict(&facts, &key, &admitted, &manifest_reference(&build_id.as_str(), 1, &outer, false)) else { panic!("the handoff is accepted") };
        record_completion(&builds, &key, "mesh1.admin.1", &manifest).await.expect("mesh.deleted's completion is recorded");
        let terminated_at_completion = fleet.terminated.lock().unwrap().clone();
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        let resume = owner_resume(&steps, &outer);
        let replay = replayed_handoff(&steps, &key, &last_record.node_id, &me.incarnation_id);
        let other = replayed_handoff(&steps, &key, &NodeId::mint(), "another-birth");
        let completions = steps.iter().filter(|r| r.step == STEP_MESH_DELETED).count();
        let indexed = builds.mesh_deleted_of(&last_record.node_id, last_record.incarnation_id.as_ref().unwrap()).await.unwrap();
        // The final primary's own node.delete, a separate Build: the ordinary workflow, live target.
        let req = rdm_admin::deployment::pipeline::DeleteRequest { build_id: delete_build.clone(), attempt: 1, node: last_record.clone(), handle: handles[FINAL].clone(), unreached: false };
        pipeline.delete_node(&req).await.expect("the final primary's node.delete completes");
        let terminal: TerminalReceipt = builds
            .read_build(&delete_build)
            .await
            .unwrap()
            .steps
            .into_iter()
            .find(|r| r.operation == format!("node.delete:{FINAL}") && r.step == "TerminateRuntime")
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
    let all = proof.unwrap_or_else(|e| panic!("the final birth's own exited verdict completes the set: {e}"));
    assert_eq!(first_mismatch(&all, &all.roster, &me, true), None, "MeshLeft covers every accepted birth, the final primary included");
    assert_eq!(births(&all.receipts), BTreeSet::from([FAILING.to_string(), EXITING.to_string(), OTHER_ADMIN.to_string(), FINAL.to_string()]));
    assert!(recycled.as_ref().is_err_and(|e| e.starts_with("final_runtime")), "a receipt of another runtime proves nothing: {recycled:?}");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let recorded = named("rdm.node_admin.mesh.update.via-mesh-deleted-receipt");
    assert!(recorded.iter().any(|s| s["attributes"]["receipts"] == "3" && s["attributes"]["roster"] == "4" && s["attributes"]["final_primary"] == FINAL), "{recorded:?}");
    let replayed = named("rdm.node_admin.mesh.resolve.via-handoff-replayed");
    assert!(replayed.iter().any(|s| s["attributes"]["outcome"] == "already-applied"), "{replayed:?}");
    eprintln!("trace {trace_id}: via-mesh-deleted-receipt receipts=3 roster=4, via-handoff-replayed already-applied, node.delete:{FINAL} TerminateRuntime then the MeshLeft proof set over 4 births");
}

/// CONTRACT: R-MN1 with mesh-delete.md step 7 (the final primary's own node.delete, the
/// fabric-primary its executor, "as acting primary of a mesh that has no primary, writes its row
/// deleted"), R-NS1 item 2 (from the stop of a mesh's last admin the fabric-primary is the writer of
/// a mesh with no primary) and R-MD1 item 2 (mesh-left ensures the mesh row Dead). While the final
/// primary holds its role live, the fabric-primary writes none of mesh2's rows and a second claim of
/// `mesh2.admin.1` is refused `name-held`; once it is stopped (its role held) the
/// fabric-primary writes its row Deleted, node id and config kept, as its node.delete's NodeDeleted
/// step does; mesh-left puts the mesh row Dead as acting primary when no Dead row is held; and only
/// then a new birth claims `mesh2.admin.1` over the Deleted row (`row-seeded`, seeded from it), its
/// row carrying its create spec's config, here none (Q166: a plain create's config is its spec's; only a
/// recovery's and a successor's claim inherit).
#[test]
fn the_final_primarys_node_delete_writes_its_row_deleted_once_it_is_stopped_and_only_then_the_name_is_claimed_again() {
    let cap = capture();
    let fp = {
        let mut n = node("mesh1.admin.1");
        n.is_fabric_primary = true;
        n
    };
    let last = node(FINAL);
    let storage = Arc::new(MemoryNodesStorage::default());
    let status = MemoryMeshStorage::default();
    let declared = Mutex::new(Declared::default());
    let view = Arc::new(RwLock::new(writer_view(std::slice::from_ref(&last), None, Some(fp.clone()))));
    let rows = NodeRows::new(fp.name.clone(), fp.node_id.clone(), view.clone(), storage.clone());
    let mesh_id = MeshId::mint();
    let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: BuildId::mint(), attempt: 1, operation: operation(&mesh_id) };
    let deleting = format!("node.delete:{FINAL}");
    let (second, live, stopped, after, row, statuses, again, trace_id) = cap.run(async {
        let root = tracing::info_span!("cell.final-primary-row-deleted", operation = %key.operation);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        // The fabric-primary claimed the mesh's first admin's name when it created it, before the
        // mesh had a primary.
        rows_written(&storage, &rows, std::slice::from_ref(&last)).await;
        // A live mesh2.admin.1 holds its name: a second birth's claim is refused.
        let second = rows.claim(&last.name, &NodeId::mint(), "node.create:mesh2.admin.1", None, None, ClaimFor::Create).await;
        // In its role and live: the fabric-primary is not the writer of mesh2's rows.
        view.write().await.nodes.iter_mut().filter(|n| n.name == last.name).for_each(|n| n.is_primary = true);
        let live = rows.deleted(&last, &deleting).await;
        // Stopped, its role still held (R-EL1): no primary for the writer rule (R-NS1 item 2).
        view.write().await.nodes.iter_mut().filter(|n| n.name == last.name).for_each(|n| {
            n.status = NodeStatus::Leaving;
            n.stopped = true;
        });
        let stopped = rows.deleted(&last, &deleting).await;
        let exited = rdm_admin::deployment::pipeline::TerminalReceipt { node: last.name.to_string(), node_id: last.node_id.to_string(), incarnation_id: last.incarnation_id.clone().unwrap().to_string(), runtime: None, exit_code: Some(0) , verdict: None, data_dir: None };
        let after = mesh_left_rows(Some(&rows), &status, &Default::default(), &declared, "mesh1.admin.1", &key, "mesh2", std::slice::from_ref(&exited)).await;
        let row = storage.record_at(&last.name).await.unwrap();
        let statuses = status.meshes().await.unwrap().into_iter().filter_map(|r| Some((r.mesh_id.clone(), r.state()?))).collect::<std::collections::HashMap<_, _>>();
        let dead_held = status.mesh(&mesh_id.clone()).await.unwrap().is_some_and(|r| r.states.contains(&MeshState::Dead));
        // mesh2 created again: its first admin's claim wins over the Deleted row.
        let again = rows.claim(&last.name, &NodeId::mint(), "node.create:mesh2.admin.1", None, None, ClaimFor::Create).await;
        (second, live, stopped, after, row, (statuses, dead_held), again, trace_id)
    });
    let (statuses, dead_held) = statuses;
    let spans = cap.spans();
    let dir = artifact_dir().join("final-primary-row-deleted");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());

    assert!(matches!(&second, Err(RowRefused::NameHeld(h)) if h.by == last.node_id), "a live mesh2.admin.1 refuses a second claim: {second:?}");
    assert!(live.is_err(), "while the final primary holds its role live the fabric-primary writes none of mesh2's rows: {live:?}");
    stopped.unwrap_or_else(|e| panic!("once stopped, the fabric-primary writes the final primary's row Deleted as acting primary: {e}"));
    after.unwrap_or_else(|e| panic!("mesh-left writes its rows: {e}"));
    let row = row.expect("the final primary's row stays, readable at its path.name");
    assert_eq!((row.status, &row.node_id, &row.config), (NodeStatus::Deleted, &last.node_id, &tier_of(FINAL)), "{row:?}");
    assert!(dead_held, "the mesh's status is Dead on mesh2 (mesh.storage, R-MD1)");
    assert_eq!(statuses.get(&mesh_id), Some(&MeshState::Dead), "mesh2's one status fact is its Dead, on mesh2: {statuses:?}");
    assert_eq!(declared.lock().unwrap().meshes.get(&mesh_id), Some(&MeshState::Dead), "and held as applied");
    match &again {
        Ok(Claimed::Written { row, seeded_from }) => {
            assert_eq!(seeded_from.as_ref(), Some(&last.node_id), "seeded from the Deleted row");
            assert_eq!(row.config, Default::default(), "its create spec's config, none: a plain create inherits no config (Q166)");
        }
        other => panic!("the recreated mesh's admin claims mesh2.admin.1 over the Deleted row: {other:?}"),
    }
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let deleted: Vec<String> = named("rdm.node_admin.node.delete.via-node-row").iter().filter(|s| s["attributes"]["subject"] == FINAL).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect();
    assert_eq!(deleted, ["not-mesh-primary", "row-deleted"], "refused while the final primary is live and primary, written once it is stopped: {deleted:?}");
    let dead = named("rdm.node_admin.mesh.update.via-dead-row");
    assert!(dead.iter().any(|s| s["attributes"]["mesh_id"] == mesh_id.as_str() && s["attributes"]["outcome"] == "applied" && s["attributes"]["writer"] == "acting-primary"), "no Dead row held: the fabric-primary writes Dead as acting primary: {dead:#?}");
    let claims: Vec<String> = named("rdm.node_admin.node.create.via-node-row").iter().filter(|s| s["attributes"]["subject"] == FINAL).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect();
    assert_eq!(claims, ["row-held-by-node", "name-held", "row-seeded"], "{claims:?}");
    eprintln!("trace {trace_id}: delete.via-node-row not-mesh-primary then row-deleted, mesh.update.via-dead-row applied acting-primary, via-node-row claim row-seeded seeded_from={}", last.node_id);
}

const NEW_ADMIN: &str = "mesh2.admin.3";

/// The exact Exited receipt the provider's inspection of `b`'s runtime `runtime` proves.
fn exited(b: &RosterBirth, runtime: &RuntimeProof) -> TerminalReceipt {
    TerminalReceipt { node: b.node.clone(), node_id: b.node_id.clone(), incarnation_id: b.incarnation_id.clone(), runtime: Some(runtime.clone()), exit_code: Some(137) , verdict: None, data_dir: None }
}

fn step_facts(facts: &[BuildFact]) -> Vec<BuildStepReceipt> {
    facts.iter().filter_map(|f| if let BuildFact::Step(r) = f { Some(r.clone()) } else { None }).collect()
}

/// CONTRACT: a mesh leave whose final primary is lost mid-leave resumes under a named amendment and
/// completes over both primaries (R11; mesh-delete.md, "Resume after the primary's loss"). Attempt 1:
/// the final primary `mesh2.admin.1` runs the ordinary tier, `mesh2.rpc.2` exits and `mesh2.rpc.1`'s
/// exit cannot be proven, then the final primary is lost. The fabric-primary records the lost
/// primary's exact Exited first, then `RosterAmended { mesh2.admin.1 -> mesh2.admin.2 }` (the
/// surviving admin the election in its role); the identical amendment replays `AlreadyApplied` and a
/// second amendment for the same predecessor is refused by name. Attempt 2: the amended primary runs
/// only `mesh2.rpc.1` (`mesh2.rpc.2` and the lost primary keep their receipts, nobody re-runs them),
/// its manifest is accepted against the amended admission, a stale manifest naming the predecessor as
/// final primary is refused by name, mesh.deleted is recorded from it, and once the amended primary's
/// own node.delete proves its exit the MeshLeft proof set covers every accepted birth, both primaries
/// included.
#[test]
fn a_primary_lost_mid_leave_is_replaced_by_a_named_amendment_and_the_leave_completes_over_both_primaries() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-deleted-amended-{}", NodeId::mint()));
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
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rdm-rpc-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { name: FINAL.parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let shutdown = |attempt: u32| {
            let (pipeline, handles, calls, build_id) = (&pipeline, &handles, &calls, &build_id);
            move |n: Node| async move {
                let name = n.name.to_string();
                calls.lock().unwrap().push((attempt, name.clone()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: (*build_id).clone(), attempt, node: n, handle: handles[&name].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-deleted-amended", build_id = %build_id.log(), operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Attempt 1: both sides capture the roster; the ordinary tier is incomplete; the primary is lost.
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let mp1 = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, FINAL, &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key(1), FINAL, &mp1, &lost, lost_runtime.clone(), &records, &[], shutdown(1)).await;
        assert!(first.is_err(), "attempt 1's ordinary tier is incomplete");

        // The resume, attempt 2: the lost primary's node.delete as a crashed target first, its
        // TerminateRuntime the roster receipt, then the amendment.
        node_delete_receipts(&builds, &key(2), "mesh1.admin.1", &exited(&lost, &lost_runtime)).await;
        let applied = amend_roster(&builds, &key(2), "mesh1.admin.1", &amendment).await;
        let replayed = amend_roster(&builds, &key(2), "mesh1.admin.1", &amendment).await;
        let second_amendment = amend_roster(&builds, &key(2), "mesh1.admin.1", &RosterAmended { final_primary: birth(EXITING), ..amendment.clone() }).await;
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        let admitted = admitted_of(&steps, &outer).expect("a roster is recorded").expect("the admission reads");

        // The amended primary's member workflows, over the leaving roster plus the amendments.
        let leaving = accepted_row(&steps, &outer, STEP_LEAVING_ROSTER).unwrap().unwrap().roster;
        let mp2 = amended_roster(&leaving, &steps, &outer).unwrap();
        let records2: Vec<Node> = view.holds(&records);
        let manifest = members_exited(&builds, &key(2), OTHER_ADMIN, &mp2, &survivor, survivor_runtime.clone(), &records2, &[], shutdown(2)).await.expect("the amended primary's member workflows complete");
        let facts = builds.facts().await.unwrap();
        let verdict = handoff_verdict(&facts, &key(2), &admitted, &manifest_reference(&build_id.as_str(), 2, &outer, false));
        // A stale hand-off: the same receipts, naming the predecessor as final primary.
        let stale_manifest = ExitManifest { final_primary: lost.clone(), ..manifest.clone() };
        let stale_facts: Vec<BuildFact> = facts
            .iter()
            .map(|f| match f {
                BuildFact::Step(r) if r.step == STEP_OTHER_MEMBERS_EXITED && r.attempt == 2 => BuildFact::Step(BuildStepReceipt { output: serde_json::to_value(&stale_manifest).ok(), ..r.clone() }),
                other => other.clone(),
            })
            .collect();
        let stale = handoff_verdict(&stale_facts, &key(2), &admitted, &manifest_reference(&build_id.as_str(), 2, &outer, false));
        // mesh.deleted from the amended primary's accepted manifest; its own end is a separate
        // node.delete, whose exact exit completes mesh-left's proof set.
        let HandoffVerdict::Accept(completed) = verdict.clone() else { panic!("the amended primary's manifest is accepted: {verdict:?}") };
        record_completion(&builds, &key(2), "mesh1.admin.1", &completed).await.expect("mesh.deleted is recorded");
        let delete_build = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: delete_build.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let last = records2.iter().find(|n| n.name.to_string() == OTHER_ADMIN).cloned().unwrap();
        let terminal = pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: delete_build, attempt: 1, node: last, handle: handles[OTHER_ADMIN].clone(), unreached: false }).await.expect("the amended primary's node.delete proves its exit");
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

/// CONTRACT: mesh-delete.md draws each member's end at the mesh-primary as "member row deleted;
/// node.deleted", then "mesh topology: NodeDeleted in MeshTopology.deleted", and its resume keeps a member's
/// finished delete ("Resume after the primary's loss" step 3: "Completed member receipts stand").
/// Attempt 1, under the final primary `mesh2.admin.1`: `mesh2.rpc.2`'s whole delete runs to
/// `Complete` and the lost primary publishes its NodeDeleted, which no other admin hears (the
/// primary is lost before its frame lands, so its publications are its own); `mesh2.rpc.1`'s exit
/// cannot be proven, so the ordinary tier is incomplete. The resume runs the lost primary's
/// node.delete as a crashed target and amends the roster to `mesh2.admin.2`. Attempt 2, under the
/// amended primary: `mesh2.rpc.2` keeps its receipts and is not run again (no step of its delete
/// executes, its runtime is terminated once), and nothing of it is written or published again: its
/// row `Deleted` and its NodeDeleted were its node.delete's, once (node-delete.md steps 3-4), and a
/// peer that never heard that NodeDeleted keeps the birth until mesh-left; `mesh2.rpc.1`'s delete
/// runs and publishes its own. The lost primary's NodeDeleted is its own node.delete's, the
/// fabric-primary's, never the amended primary's.
#[test]
fn a_member_whose_delete_finished_under_the_lost_primary_is_neither_run_nor_published_again() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-deleted-finished-under-lost-{}", NodeId::mint()));
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 44_000 + i as u32))).collect();
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(), ..Default::default() };
    *fleet.blind_once.lock().unwrap() = handles[FAILING].pid;
    let birth = |name: &str| RosterBirth::of(records.iter().find(|n| n.name.to_string() == name).unwrap()).unwrap();
    let (lost, survivor) = (birth(FINAL), birth(OTHER_ADMIN));
    let (lost_runtime, survivor_runtime) = (RuntimeProof::of(&handles[FINAL].fact().unwrap()), RuntimeProof::of(&handles[OTHER_ADMIN].fact().unwrap()));
    let view = View::default();
    let calls: Mutex<Vec<(u32, String)>> = Mutex::new(Vec::new());
    // Each primary's own publications: the lost primary's never reach the amended one.
    let (lost_published, amended_published) = (Departures::default(), Departures::default());
    let amendment = RosterAmended { predecessor: lost.clone(), final_primary: survivor.clone(), final_runtime: survivor_runtime.clone() };

    let (first, second, steps, trace_id) = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rdm-rpc-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { name: FINAL.parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let lost_pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &lost_published, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let amended_pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &amended_published, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let shutdown_1 = |n: Node| {
            let (pipeline, handles, calls, build_id) = (&lost_pipeline, &handles, &calls, &build_id);
            async move {
                let name = n.name.to_string();
                calls.lock().unwrap().push((1, name.clone()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: (*build_id).clone(), attempt: 1, node: n, handle: handles[&name].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let shutdown_2 = |n: Node| {
            let (pipeline, handles, calls, build_id) = (&amended_pipeline, &handles, &calls, &build_id);
            async move {
                let name = n.name.to_string();
                calls.lock().unwrap().push((2, name.clone()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: (*build_id).clone(), attempt: 2, node: n, handle: handles[&name].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-deleted-finished-under-lost", build_id = %build_id.log(), operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Attempt 1, the lost primary's: rpc.2's delete finishes, rpc.1's exit is unproven.
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let mp1 = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, FINAL, &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key(1), FINAL, &mp1, &lost, lost_runtime.clone(), &records, &[], shutdown_1).await;

        // The resume: the lost primary's node.delete, then the amendment, then the amended primary's tiers.
        node_delete_receipts(&builds, &key(2), "mesh1.admin.1", &exited(&lost, &lost_runtime)).await;
        amend_roster(&builds, &key(2), "mesh1.admin.1", &amendment).await.expect("the amendment is recorded");
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        let leaving = accepted_row(&steps, &outer, STEP_LEAVING_ROSTER).unwrap().unwrap().roster;
        let mp2 = amended_roster(&leaving, &steps, &outer).unwrap();
        let records2: Vec<Node> = view.holds(&records);
        let second = members_exited(&builds, &key(2), OTHER_ADMIN, &mp2, &survivor, survivor_runtime.clone(), &records2, &[], shutdown_2).await;
        (first, second, builds.read_build(&build_id).await.unwrap().steps, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("finished_under_lost");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    let err = first.expect_err("attempt 1's ordinary tier is incomplete: rpc.1's exit is unproven");
    assert!(err.contains("tier ordinary-nodes is incomplete") && err.contains(FAILING), "{err}");
    second.unwrap_or_else(|e| panic!("the amended primary's member workflows complete: {e}"));
    let calls = calls.into_inner().unwrap();
    let ran = |attempt: u32| calls.iter().filter(|(a, _)| *a == attempt).map(|(_, n)| n.clone()).collect::<BTreeSet<_>>();
    assert_eq!(ran(1), BTreeSet::from([FAILING.to_string(), EXITING.to_string()]), "attempt 1 ran the ordinary tier");
    assert_eq!(ran(2), BTreeSet::from([FAILING.to_string()]), "the amended primary runs only rpc.1: rpc.2's delete has every step's receipt and is not run again");
    let of = |node: &str, step: &str| steps.iter().filter(|r| r.operation == format!("node.delete:{node}") && r.step == step && r.outcome == rdm_admin::build_state::StepOutcome::Complete).map(|r| r.attempt).collect::<Vec<_>>();
    assert_eq!((of(EXITING, "NodeDeleted"), of(EXITING, "Complete")), (vec![1], vec![1]), "rpc.2's delete finished under the lost primary, each step receipted once");
    assert_eq!(fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == EXITING).count(), 1, "rpc.2's runtime was terminated once, in attempt 1");
    let lost_ops = lost_published.0.into_inner().unwrap();
    assert_eq!(lost_ops.iter().map(|op| op.name.to_string()).collect::<Vec<_>>(), vec![EXITING.to_string()], "the lost primary published rpc.2's NodeDeleted, which the amended primary never heard");
    let receipted: LifecycleOp = steps
        .iter()
        .find(|r| r.operation == format!("node.delete:{EXITING}") && r.step == "NodeDeleted")
        .and_then(|r| r.output.clone())
        .map(|v| serde_json::from_value(v).expect("a NodeDeleted receipt holds its op"))
        .expect("rpc.2's NodeDeleted is receipted");
    assert_eq!((receipted.name.to_string(), receipted.attempt), (EXITING.to_string(), 1), "rpc.2's NodeDeleted receipt is the lost primary's, attempt 1");
    let amended_ops = amended_published.0.into_inner().unwrap();
    let names = amended_ops.iter().map(|op| op.name.to_string()).collect::<BTreeSet<_>>();
    assert_eq!(names, BTreeSet::from([FAILING.to_string()]), "the amended primary publishes the NodeDeleted of the one delete it ran, never again rpc.2's (its node.delete's, once) and never the lost primary's (its own node.delete's, the fabric-primary's): {amended_ops:?}");

    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.deployment.update.via-step").iter().all(|s| !(s["attributes"]["node"] == EXITING && s["attributes"]["attempt"] == "2")), "no step of rpc.2's delete runs in attempt 2");
    let retained = named("rdm.node_admin.mesh.resolve.via-retained-receipts");
    assert!(retained.iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["retained"] == "2" && s["attributes"]["remaining"] == "1"), "attempt 2 keeps rpc.2's delete and the lost primary's exit, and runs rpc.1: {retained:?}");
    let tier = named("rdm.node_admin.mesh.update.via-leave-tier");
    assert!(tier.iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["tier"] == "ordinary-nodes" && s["attributes"]["members"] == "1" && s["attributes"]["retained"] == "1"), "attempt 2's ordinary tier runs rpc.1 and keeps rpc.2: {tier:?}");
    assert!(tier.iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["tier"] == "node-admins" && s["attributes"]["members"] == "0" && s["attributes"]["retained"] == "1"), "the node-admin tier runs nothing and keeps the lost primary's delete: {tier:?}");
    eprintln!("trace {trace_id}: attempt 2 ran rpc.1 only; rpc.2's NodeDeleted receipted once (attempt {}) and published once, by the lost primary", receipted.attempt);
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
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: outer.clone() };
        let root = tracing::info_span!("cell.mesh-deleted-unproven", build_id = %build_id.log(), operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let unproven = amend_roster(&builds, &key(2), "mesh1.admin.1", &amendment).await;
        // A node.delete receipt of the lost primary's path naming another runtime (a recycled pid).
        node_delete_receipts(&builds, &key(2), "mesh1.admin.1", &exited(&lost, &recycled)).await;
        let recycled_exit = amend_roster(&builds, &key(2), "mesh1.admin.1", &amendment).await;
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
    assert!(recycled_refused.contains("another runtime"), "{recycled_refused}");
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
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: outer.clone() };
        let root = tracing::info_span!("cell.mesh-deleted-repeated-loss", build_id = %build_id.log(), operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &first, &runtime(FINAL, 46_000), || live_roster(&records)).await.unwrap();
        let leaving = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, FINAL, &first, &runtime(FINAL, 46_000), || live_roster(&records)).await.unwrap();
        node_delete_receipts(&builds, &key(2), "mesh1.admin.1", &exited(&first, &runtime(FINAL, 46_000))).await;
        let r1 = amend_roster(&builds, &key(2), "mesh1.admin.1", &a1).await;
        node_delete_receipts(&builds, &key(3), "mesh1.admin.1", &exited(&second, &runtime(OTHER_ADMIN, 46_001))).await;
        let r2 = amend_roster(&builds, &key(3), "mesh1.admin.1", &a2).await;
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        let admitted = admitted_of(&steps, &outer).expect("a roster is recorded").expect("the admission reads");
        let mp = amended_roster(&leaving, &steps, &outer).unwrap();
        // A hand-off manifest over the accepted roster alone: it omits the amendment.
        let receipts = vec![exited(&by(FAILING), &runtime(FAILING, 46_003)), exited(&by(EXITING), &runtime(EXITING, 46_004)), exited(&first, &runtime(FINAL, 46_000)), exited(&second, &runtime(OTHER_ADMIN, 46_001))];
        let omitting = ExitManifest { mesh_id: mesh_id.to_string(), roster: leaving.clone(), final_primary: third.clone(), final_runtime: Some(a2.final_runtime.clone()), receipts };
        let facts = vec![BuildFact::Step(BuildStepReceipt { mesh: None, build_id: build_id.clone(), attempt: 3, operation: outer.clone(), step: STEP_OTHER_MEMBERS_EXITED.into(), outcome: rdm_admin::build_state::StepOutcome::Complete, output: serde_json::to_value(&omitting).ok(), executor: Some(NEW_ADMIN.into()) })];
        let omitted = handoff_verdict(&facts, &key(3), &admitted, &manifest_reference(&build_id.as_str(), 3, &outer, false));
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

/// CONTRACT: the amended final primary admits `DeleteMesh` from the owner's Build facts when gossip has
/// not yet brought it the amendment (R11 step 4; mesh-delete.md, "Resume after the primary's loss",
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
        let key = |attempt: u32| MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: outer.clone() };
        for b in [&owner, &own] {
            b.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
            accepted_roster(b, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
            accepted_roster(b, &key(1), STEP_LEAVING_ROSTER, FINAL, &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        }
        let root = tracing::info_span!("cell.mesh-deleted-owner-amendment", build_id = %build_id.log(), operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        // The fabric-primary's resume, in its own facts only: gossip has not delivered it.
        node_delete_receipts(&owner, &key(2), "mesh1.admin.1", &exited(&lost, &lost_runtime)).await;
        amend_roster(&owner, &key(2), "mesh1.admin.1", &RosterAmended { predecessor: lost.clone(), final_primary: survivor.clone(), final_runtime: survivor_runtime.clone() }).await.expect("the amendment is recorded");
        // mesh2.admin.2 admits DeleteMesh.
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
        })
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

/// CONTRACT: the amended final primary whose own Build facts hold the amendment naming it, but not
/// yet the lost primary's node.delete receipts the fabric-primary recorded before it (gossip carries
/// the two operations' facts apart), reads the owner's Build facts at admission and keeps that
/// finished delete: it never runs the lost primary's node.delete again (mesh-delete.md, "Resume after
/// the primary's loss" step 3: the new mesh-primary "runs only the tiers without a terminal receipt.
/// Completed member receipts stand"; build.md: "Completed step receipts prevent repeating
/// already-satisfied work"). Must NOT happen: mesh_delete_resume on 52c177c10, the survivor's
/// admission from its own facts (`via-own-build-facts`, amendments 1) naming the lost primary
/// `delete_owed` and running its StopNode, TerminateRuntime and NodeDeleted a second time.
#[test]
fn an_amended_primary_whose_own_facts_lack_the_lost_primarys_delete_reads_the_owner_and_never_deletes_it_again() {
    own_amendment_without_the_delete("own_amendment_without_the_delete", &[]);
}

/// CONTRACT: as above, when gossip brought the lost primary's node.delete receipts but its exact
/// Exited (`TerminateRuntime`): its `NodeDeleted` and `Complete` alone do not keep the delete, the
/// exited verdict does (mesh-delete.md step 4: the manifest covers "the lost primary's Exited"), so the
/// admission reads the owner. Must NOT happen: mesh_delete_resume with the first fix, the survivor's
/// admission from its own facts (`via-own-build-facts`) and its second node.delete of the lost primary.
#[test]
fn an_amended_primary_whose_own_facts_lack_the_lost_primarys_exit_receipt_reads_the_owner_and_never_deletes_it_again() {
    own_amendment_without_the_delete("own_amendment_without_the_exit_receipt", &["NodeDeleting", "NodeDeleted", "RemoveTopologyMembership", "Complete"]);
}

/// The amended primary `mesh2.admin.2` admits the leave from its own facts, which hold the
/// fabric-primary's amendment and, of the lost primary's node.delete, only the steps
/// `keep_of_lost_delete`; the owner holds them all.
fn own_amendment_without_the_delete(case: &str, keep_of_lost_delete: &[&str]) {
    let cap = capture();
    let records: Vec<Node> = [OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 47_000 + i as u32))).collect();
    let birth = |name: &str| RosterBirth::of(records.iter().find(|n| n.name.to_string() == name).unwrap()).unwrap();
    let (lost, survivor) = (birth(FINAL), birth(OTHER_ADMIN));
    let (lost_runtime, survivor_runtime) = (RuntimeProof::of(&handles[FINAL].fact().unwrap()), RuntimeProof::of(&handles[OTHER_ADMIN].fact().unwrap()));
    let ran: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let (out, read_owner, trace_id) = cap.run(async {
        let (owner, own) = (WireBuilds(MemoryBuildStateAdapter::new()), WireBuilds(MemoryBuildStateAdapter::new()));
        let build_id = BuildId::mint();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: outer.clone() };
        for b in [&owner, &own] {
            b.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
            accepted_roster(b, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
            accepted_roster(b, &key(1), STEP_LEAVING_ROSTER, FINAL, &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        }
        let root = tracing::info_span!("cell.mesh-deleted-own-amendment-without-the-delete", build_id = %build_id.log(), operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        // The fabric-primary's resume: the lost primary's node.delete, then the amendment.
        node_delete_receipts(&owner, &key(2), "mesh1.admin.1", &exited(&lost, &lost_runtime)).await;
        amend_roster(&owner, &key(2), "mesh1.admin.1", &RosterAmended { predecessor: lost.clone(), final_primary: survivor.clone(), final_runtime: survivor_runtime.clone() }).await.expect("the amendment is recorded");
        // mesh2.admin.2's own facts: gossip delivered the amendment, not the lost primary's delete.
        let lost_delete = format!("node.delete:{FINAL}");
        let own_steps: Vec<BuildStepReceipt> = owner.read_build(&build_id).await.unwrap().steps.into_iter().filter(|r| r.operation != lost_delete || keep_of_lost_delete.contains(&r.step.as_str())).collect();
        let leaving = accepted_row(&own_steps, &outer, STEP_LEAVING_ROSTER).unwrap().unwrap().roster;
        let owner_facts = owner.facts().await.unwrap();
        let read_owner = std::sync::atomic::AtomicBool::new(false);
        let (roster, known) = admission_roster(&leaving, &own_steps, &outer, &survivor, || async {
            read_owner.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(owner_facts)
        })
        .await
        .expect("the admission reads");
        let manifest = members_exited(&own, &key(3), OTHER_ADMIN, &roster, &survivor, survivor_runtime.clone(), &records, &known, |n: Node| {
            let ran = &ran;
            async move {
                ran.lock().unwrap().push(n.name.to_string());
                Err::<TerminalReceipt, String>(format!("{} is run again", n.name))
            }
        })
        .await;
        (manifest, read_owner.load(std::sync::atomic::Ordering::SeqCst), trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join(case);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    assert!(read_owner, "the admission reads the owner's Build facts: its own hold the amendment but not the lost primary's finished delete");
    assert!(ran.lock().unwrap().is_empty(), "the lost primary's finished node.delete is kept, never run again: {:?}", ran.lock().unwrap());
    let manifest = out.unwrap_or_else(|e| panic!("the amended primary admits the leave: {e}"));
    assert_eq!(births(&manifest.receipts), BTreeSet::from([FINAL.to_string()]), "the lost primary's exit receipt is in the manifest");
}

/// CONTRACT: the fabric-primary's own resume branch reads the lost final primary's runtime from its
/// durable runtime row, never from the view (R11 steps 2-3; mesh-delete.md, "Resume after the
/// primary's loss", steps 1-2). No topology view is consulted: the lost `mesh2.admin.1` has left
/// every projection. With no runtime row of that birth the resume is refused by name and nothing is
/// amended; with the row every admin stored the moment it first held the birth, its node.delete's
/// `TerminateRuntime` receipt is its roster receipt and `RosterAmended` admits `mesh2.admin.2`.
#[test]
fn the_fabric_primary_reads_a_lost_primarys_runtime_from_its_durable_runtime_row_and_amends() {
    let cap = capture();
    let records: Vec<Node> = [OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 47_000 + i as u32))).collect();
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(), ..Default::default() };
    let birth = |name: &str| RosterBirth::of(records.iter().find(|n| n.name.to_string() == name).unwrap()).unwrap();
    let (lost, survivor) = (birth(FINAL), birth(OTHER_ADMIN));
    let (lost_runtime, survivor_runtime) = (RuntimeProof::of(&handles[FINAL].fact().unwrap()), RuntimeProof::of(&handles[OTHER_ADMIN].fact().unwrap()));
    let lost_node = records.iter().find(|n| n.name.to_string() == FINAL).unwrap();
    let row = rdm_admin::storage::RuntimeRow { node_id: lost_node.node_id.clone(), name: lost_node.name.clone(), incarnation_id: lost_node.incarnation_id.clone().unwrap(), runtime: handles[FINAL].fact().unwrap(), data_dir: None };
    let (without_row, with_row, before, after, steps_after, trace_id) = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: outer.clone() };
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let root = tracing::info_span!("cell.mesh-deleted-fp-resume", build_id = %build_id.log(), operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let admitted = |steps: &[BuildStepReceipt]| admitted_of(steps, &outer).unwrap().unwrap();
        let adm = admitted(&builds.read_build(&build_id).await.unwrap().steps);
        // The lost birth's node.delete as an unreached birth: what the fabric-primary runs on the row.
        let delete = || async { node_delete_receipts(&builds, &key(2), "mesh1.admin.1", &exited(&lost, &lost_runtime)).await; Ok(()) };
        let without_row = resume_after_loss(&builds, &fleet, None, &key(2), "mesh1.admin.1", &adm, &survivor, &survivor_runtime, delete).await;
        let before = admitted(&builds.read_build(&build_id).await.unwrap().steps);
        let with_row = resume_after_loss(&builds, &fleet, Some(&row), &key(2), "mesh1.admin.1", &adm, &survivor, &survivor_runtime, delete).await;
        let steps_after = builds.read_build(&build_id).await.unwrap().steps;
        let after = admitted(&steps_after);
        (without_row, with_row, before, after, steps_after, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("fp_resume");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let refusal = without_row.expect_err("no runtime row of the lost birth: the resume is refused");
    let rdm_admin::executor::RunEnd::Incomplete(refusal) = refusal else { panic!("a lost primary whose runtime is unknown leaves the leave incomplete, not failed: {refusal:?}") };
    assert!(refusal.contains("not known here") && refusal.contains(FINAL) && refusal.contains("no durable runtime row"), "the refusal names the lost birth and the missing row: {refusal}");
    assert_eq!(before.final_primary, lost, "nothing is amended while the lost primary's runtime is unknown");
    assert_eq!(with_row, Ok(Amended::Applied), "with the durable runtime row the lost primary is deleted and the amendment recorded");
    assert_eq!(after.final_primary, survivor, "the amendment admits the surviving admin");
    assert_eq!(after.final_runtime, survivor_runtime);
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.mesh.reject.via-unproven-primary").iter().any(|s| s["attributes"]["reason"].as_str().is_some_and(|r| r.contains("no durable runtime row"))));
    // The roster receipt is node.delete's own TerminateRuntime under `node.delete:<path>`.
    assert!(steps_after.iter().any(|r| r.operation == format!("node.delete:{FINAL}") && r.step == "TerminateRuntime"), "the lost primary's roster receipt is its node.delete's: {:?}", steps_after.iter().map(|r| format!("{}/{}", r.operation, r.step)).collect::<Vec<_>>());
    assert!(steps_after.iter().all(|r| !r.operation.contains('/')), "no receipt is recorded under a key outside node.delete's and the leave's: {:?}", steps_after.iter().map(|r| r.operation.clone()).collect::<BTreeSet<_>>());
    assert!(named("rdm.node_admin.mesh.update.via-roster-amended").iter().any(|s| s["attributes"]["outcome"] == "applied" && s["attributes"]["final_primary"].as_str().is_some_and(|p| p.contains(OTHER_ADMIN))));
}

/// CONTRACT: when no admin of the leaving mesh survives, the one node-admin the fabric-primary
/// creates for it admits `DeleteMesh` over the roster the owner admitted plus the amendment that
/// names it, and never captures a roster of its own (mesh-delete.md, "Resume after the primary's
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
    // The path: above every admin ordinal the roster and the view hold, deleted births included.
    let held: Vec<PathName> = ["mesh2.admin.1", "mesh2.admin.2.old", "mesh2.rpc.4"].iter().map(|p| p.parse().unwrap()).collect();
    let path = rdm_admin::mesh_delete::leaving_mesh_admin_path("mesh2", &live_roster(&admitted_births).unwrap(), &held);
    let ran: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let (out, own_steps, verdict, trace_id) = cap.run(async {
        let (owner, own) = (WireBuilds(MemoryBuildStateAdapter::new()), WireBuilds(MemoryBuildStateAdapter::new()));
        let build_id = BuildId::mint();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: outer.clone() };
        for b in [&owner, &own] {
            b.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
        }
        accepted_roster(&owner, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &runtime(FINAL), || live_roster(&admitted_births)).await.unwrap();
        // mesh2.rpc.2's node.delete, finished by the lost primary and gossiped to the owner.
        node_delete_receipts(&owner, &key(1), FINAL, &exited(&birth(EXITING), &runtime(EXITING))).await;
        let root = tracing::info_span!("cell.mesh-deleted-created-admin", build_id = %build_id.log(), operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        node_delete_receipts(&owner, &key(2), "mesh1.admin.1", &exited(&lost, &runtime(FINAL))).await;
        amend_roster(&owner, &key(2), "mesh1.admin.1", &RosterAmended { predecessor: lost.clone(), final_primary: created.clone(), final_runtime: runtime(NEW_ADMIN) }).await.expect("the amendment admits the created admin");
        // mesh2.admin.3 admits DeleteMesh: an amendment admits it, so it runs the owner's roster and captures none.
        let owner_facts = owner.facts().await.unwrap();
        let roster = rdm_admin::mesh_delete::leaving_roster(&own, &own.0, &key(2), NEW_ADMIN, &created, &runtime(NEW_ADMIN), || async { Ok(owner_facts) }).await;
        let out = match roster {
            Ok((roster, known)) => members_exited(&own, &key(2), NEW_ADMIN, &roster, &created, runtime(NEW_ADMIN), &records, &known, |n: Node| {
                let (ran, t) = (&ran, exited(&RosterBirth::of(&n).unwrap(), &runtime(&n.name.to_string())));
                async move {
                    ran.lock().unwrap().push(n.name.to_string());
                    Ok::<TerminalReceipt, String>(t)
                }
            })
            .await,
            Err(e) => Err(e),
        };
        let own_steps = own.read_build(&build_id).await.unwrap().steps;
        let admitted = admitted_of(&owner.read_build(&build_id).await.unwrap().steps, &outer).unwrap().unwrap();
        let verdict = handoff_verdict(&own.facts().await.unwrap(), &key(2), &admitted, &manifest_reference(&build_id.as_str(), 2, &outer, false));
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

/// CONTRACT (R11 form (b); node-recovery.md step 2-3, run by the primary watch: "A node-admin watching
/// a silent primary (primary watch) runs this same ladder", then "the provider about the exact
/// runtime recorded at launch"): the node-admin the fabric-primary created for a leaving mesh holds
/// the lost primary's primary record but never heard its digest (the primary was gone before the admin
/// was born). Its primary watch reads the holder's runtime from the runtime row handed into its
/// nodes.storage at its create, the exact runtime recorded at launch. With no digest and no row the
/// finding is unproven.
#[test]
fn a_created_admins_watch_reads_the_lost_primarys_runtime_from_its_runtime_row() {
    use rdm_admin::primary_watch::{inspect_birth, Finding};
    use rdm_admin::storage::{MemoryNodesStorage, NodesStorage, RuntimeRow};
    let lost = node(FINAL);
    let h = handle(FINAL, 49_001);
    let fleet = Fleet { pids: [(49_001, FINAL.to_string())].into_iter().collect(), ..Default::default() };
    let digest_view = rdm_transport::topology::DigestView::default();
    let (without_row, with_row) = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let nodes = MemoryNodesStorage::default();
        let without_row = inspect_birth(&fleet, &digest_view, Some(&nodes), &lost).await;
        nodes.put_runtime(&RuntimeRow { node_id: lost.node_id.clone(), name: lost.name.clone(), incarnation_id: lost.incarnation_id.clone().unwrap(), runtime: h.fact().unwrap(), data_dir: None }).await.unwrap();
        let with_row = inspect_birth(&fleet, &digest_view, Some(&nodes), &lost).await;
        (without_row, with_row)
    });
    assert!(matches!(&without_row, Finding::Unproven { reason } if reason.contains("no runtime row")), "no digest and no row: unproven, naming both: {without_row:?}");
    assert_eq!(with_row, Finding::Exited { code: Some(0) }, "the runtime row recorded at launch is what the provider inspects");
}

/// CONTRACT (R11, the leave acts on the immutable accepted roster's exact births; mesh-delete.md
/// "Resume after the primary's loss" step 3, "Failure/certainty behavior": "No timeout or gossip
/// absence proves exit"): the admin the fabric-primary created for a leaving mesh with no admin
/// left works the roster, not its view's liveness. `mesh2.rpc.1` was stopped by the lost primary
/// before its exit was proven, and the created admin's view does not list it. It is still a roster
/// birth with no Exited receipt, and its record is the one this admin's topology view holds of that
/// exact birth: the record of another birth at the path is never taken.
/// Its workflow runs, its row is written Deleted on its receipt, and the tier completes.
#[test]
fn a_roster_birth_the_view_does_not_list_is_worked_from_the_record_its_topology_view_holds() {
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
    assert_eq!(worked.iter().find(|n| n.name.to_string() == FAILING), Some(&held_stopped), "the stopped roster birth is worked from the record its topology view holds: {worked:?}");
    assert_eq!(worked.len(), 3, "every roster birth has its record: {worked:?}");

    let runtime = RuntimeProof::of(&handle(FINAL, 46_000).fact().unwrap());
    let manifest = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt: 2, operation: operation(&mesh_id) };
        // The leave's records, under the cell's capture: the held record is named as it is taken.
        let worked = roster_records(&view, &roster, |b| (b.node == FAILING).then(|| held_stopped.clone()));
        members_exited(&builds, &key, FINAL, &roster, &me, runtime.clone(), &worked, &[], |n: Node| {
            let runtime = runtime.clone();
            async move { Ok(exited(&RosterBirth::of(&n).unwrap(), &runtime)) }
        })
        .await
    });
    let manifest = manifest.expect("the tier completes over every roster birth");
    assert!(births(&manifest.receipts).contains(FAILING), "the stopped birth's exact Exited receipt is in the manifest: {:?}", births(&manifest.receipts));
    let held = cap.spans().into_iter().filter(|s| s["name"] == "rdm.node_admin.mesh.resolve.via-held-record").collect::<Vec<_>>();
    assert_eq!(held.iter().map(|s| (s["attributes"]["member"].as_str().unwrap_or("").to_string(), s["attributes"]["status"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>(), vec![(FAILING.to_string(), "Leaving".to_string())], "{held:#?}");
}

/// CONTRACT (mesh-delete.md, "Resume after the primary's loss", step 3: the new final primary "runs
/// only the tiers without a terminal receipt"; R11: the accepted roster is immutable and decides what
/// runs; R13: "Rebuild after a role change reads the mesh's own rows"): the admin created for a
/// leaving mesh with no admin left joined after `mesh2.rpc.1` stopped, so neither its view nor its
/// the topology view holds that birth. The roster birth runs from the roster itself: its member workflow is
/// handed a record naming exactly that birth (path.name, node id, incarnation), its exact Exited
/// receipt is in the manifest, and its row, read from the mesh's own rows before the tiers run, is
/// written Deleted with its config kept by its node.delete's NodeDeleted step. Must NOT happen: the tier refused "this view holds no record
/// of the accepted birth" (no_survivor, created mesh2.admin.2, attempts 2 and 3), or a Deleted row
/// written with a config the writer never read.
#[test]
fn a_roster_birth_the_created_admin_never_held_runs_from_the_roster_and_its_row_is_written_deleted() {
    let cap = capture();
    let all: Vec<Node> = [FAILING, EXITING, NEW_ADMIN]
        .iter()
        .map(|n| node(n))
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
            storage.write_record(&NodeRow { topology_storage_provider: None, name: n.name.clone(), node_id: n.node_id.clone(), status: NodeStatus::Ready, config: tier_of(&n.name.to_string()), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }).await.unwrap();
        }
        for n in &records {
            assert!(matches!(rows.claim(&n.name, &n.node_id, "create-node", None, None, ClaimFor::Create).await, Ok(Claimed::HeldByNode(_))), "the writer holds {}'s row", n.name);
        }
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt: 2, operation: operation(&mesh_id) };
        let root = tracing::info_span!("cell.mesh-deleted-roster-birth", build_id = %build_id.log(), operation = %key.operation);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        roster_rows_read(Some(&rows), NEW_ADMIN, &key.operation, &roster, &records).await;
        // The member's node.delete: its NodeDeleted step writes the row Deleted (node-delete.md step 3)
        // from the record it was handed, then the provider's exact Exited is its receipt.
        let manifest = members_exited(&builds, &key, NEW_ADMIN, &roster, &me, runtime.clone(), &records, &[], |n: Node| {
            ran.lock().unwrap().push(n.clone());
            let (runtime, rows, operation) = (runtime.clone(), &rows, key.operation.clone());
            async move {
                rows.deleted(&n, &operation).await.map_err(|e| e.to_string())?;
                Ok(exited(&RosterBirth::of(&n).unwrap(), &runtime))
            }
        })
        .await;
        (manifest, trace_id)
    });
    let manifest = manifest.expect("the tier completes over every roster birth, the one the created admin never held included");
    assert!(births(&manifest.receipts).contains(FAILING), "the stopped birth's exact Exited receipt is in the manifest: {:?}", births(&manifest.receipts));
    let ran = ran.into_inner().unwrap();
    let worked = ran.iter().find(|n| n.name.to_string() == FAILING).expect("the stopped roster birth's workflow ran");
    assert_eq!(RosterBirth::of(worked).unwrap(), stopped, "its workflow is handed exactly the roster birth");
    let row = cap.run(async { storage.record_at(&FAILING.parse().unwrap()).await.unwrap() }).expect("the stopped birth's row stays at its path.name");
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

/// CONTRACT: the leaving mesh-primary admits `DeleteMesh` for a Build whose Accepted fact gossip has
/// not yet brought it (the command outran the fact, gate 044519ab canonical_ids: admitted at .810,
/// "no Build" at .811, Accepted heard at .905). Its own log holding no fact of the Build is the case
/// "its own do not yet admit it": the Build's committed records are read from storage (Q292) and absorbed
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
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt: 1, operation: operation(&mesh_id) };
        owner.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
        accepted_roster(&owner, &key, STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &my_runtime, || live_roster(&records)).await.unwrap();
        let owner_facts = owner.facts().await.unwrap();
        let root = tracing::info_span!("cell.mesh-deleted-unheard-build", build_id = %build_id.log());
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let reads = std::sync::atomic::AtomicUsize::new(0);
        let roster = rdm_admin::mesh_delete::leaving_roster(&own, &own.0, &key, FINAL, &me, &my_runtime, || async {
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

/// CONTRACT (mesh-delete.md step 4: "The fabric-primary validates exact roster coverage"; line 79:
/// "Missing receipts ... leave the workflow incomplete"): the final primary that calls mesh-deleted
/// and whose Build storage holds no record of the Build at all (the read answers UnknownBuild) recorded no exit
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
    let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: BuildId::mint(), attempt: 1, operation: operation(&mesh_id) };
    let reference = manifest_reference(key.build_id.as_str(), 1, &key.operation, false);
    let cap = capture();
    let (verdict, not_ready, trace_id) = cap.run(async {
        let root = tracing::info_span!("cell.mesh-deleted-unknown-build-handoff", build_id = %key.build_id.log());
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let unknown = rdm_admin::build_state::BuildStateError::UnknownBuild(key.build_id.clone());
        let verdict = handoff_verdict_from_read(Err(unknown), &key, &admitted, &reference, "mesh1.admin.1", FINAL);
        let not_ready = handoff_verdict_from_read(Err(rdm_admin::build_state::BuildStateError::Io("hydrating".into())), &key, &admitted, &reference, "mesh1.admin.1", FINAL);
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
            assert!(reason.contains("holds no record of") && reason.contains(key.build_id.as_str()), "the reason names the unknown Build: {reason}");
            assert!(
                matches!(v.refusal(), Some(StatusReply::RejectedIncompleteManifest { missing: 2, ref without_exit, ref reason }) if without_exit.iter().cloned().collect::<BTreeSet<_>>() == expected && reason.contains("holds no record of")),
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
/// door's `Receipt` cut leaves it (`FaultedBuilds::append_step_receipt` holds BEFORE the receipt is
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
    async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, rdm_admin::build_state::BuildStateError> {
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, rdm_admin::build_state::BuildStateError> {
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
    async fn committed_facts(&self, build: Option<&rdm_admin::build::BuildId>) -> Result<Vec<rdm_admin::build_state::BuildFact>, rdm_admin::build_state::BuildStateError> {
        let facts = self.facts().await?;
        Ok(match build { Some(b) => facts.into_iter().filter(|f| f.build_id() == b).collect(), None => facts })
    }
    async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(id).await
    }
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<rdm_admin::build_state::OwedExit>, BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
    }
}

/// The departures one mesh-primary publishes: every `NodeDeleted` op it published.
#[derive(Default)]
struct Departures(Mutex<Vec<LifecycleOp>>);

#[async_trait::async_trait]
impl LifecycleEvents for Departures {
    async fn deleting(&self, _: &LifecycleOp) {}
    async fn deleted(&self, op: &LifecycleOp) {
        self.0.lock().unwrap().push(op.clone());
    }
    async fn stopped(&self, _: &LifecycleOp) {}
    async fn joined(&self, _: &LifecycleOp) {}
    async fn leaving(&self, _: &LifecycleOp) {}
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
/// stopped birth until mesh-left if NodeDeleted is never published: the exited verdict alone is not the
/// departure. Each primary's publications are its own (attempt 1's primary is lost before its
/// frames land): attempt 2's primary publishes the NodeDeleted of the deletes it ran, and never again
/// `mesh2.rpc.2`'s, which its own node.delete published once under attempt 1 (node-delete.md step 4:
/// the executor emits node.deleted); a peer that never heard it keeps the birth until mesh-left.
#[test]
fn a_delete_cut_after_its_terminate_runtime_receipt_is_finished_by_the_resumed_attempt_from_its_missing_steps() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-deleted-cut-delete-{}", NodeId::mint()));
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 44_000 + i as u32))).collect();
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(), ..Default::default() };
    let me = RosterBirth::of(records.iter().find(|n| n.name.to_string() == FINAL).unwrap()).unwrap();
    let final_runtime = RuntimeProof::of(&handles[FINAL].fact().unwrap());
    let view = View::default();
    // Each mesh-primary's own publications: what attempt 1's primary publishes is never heard by
    // attempt 2's (it is lost before its frames land), so each holds its own.
    let (lost_departures, departures) = (Departures::default(), Departures::default());
    let calls: Mutex<Vec<(u32, String)>> = Mutex::new(Vec::new());

    let (first, cut_steps, second, steps, trace_id) = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rdm-rpc-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { name: FINAL.parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let cut = CutBuilds { inner: &builds, cut: Mutex::new(Some(("NodeDeleted", format!("node.delete:{FAILING}")))) };
        let cut_pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &lost_departures, builds: &cut, template: &template, timeouts: Timeouts::default() };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &departures, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let shutdown_1 = |n: Node| {
            let (pipeline, handles, calls, build_id) = (&cut_pipeline, &handles, &calls, &build_id);
            async move {
                let name = n.name.to_string();
                calls.lock().unwrap().push((1, name.clone()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: (*build_id).clone(), attempt: 1, node: n, handle: handles[&name].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let shutdown_2 = |n: Node| {
            let (pipeline, handles, calls, build_id) = (&pipeline, &handles, &calls, &build_id);
            async move {
                let name = n.name.to_string();
                calls.lock().unwrap().push((2, name.clone()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: (*build_id).clone(), attempt: 2, node: n, handle: handles[&name].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-deleted-cut-delete", build_id = %build_id.log(), operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let mp = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key(1), FINAL, &mp, &me, final_runtime.clone(), &records, &[], shutdown_1).await;
        let cut_steps = builds.read_build(&build_id).await.unwrap().steps;
        let records2: Vec<Node> = view.holds(&records);
        let mp2 = accepted_roster(&builds, &key(2), STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records2)).await.unwrap();
        let second = members_exited(&builds, &key(2), FINAL, &mp2, &me, final_runtime.clone(), &records2, &[], shutdown_2).await;
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
    let of = |steps: &[BuildStepReceipt], node: &str, step: &str| steps.iter().filter(|r| r.operation == format!("node.delete:{node}") && r.step == step && r.outcome == rdm_admin::build_state::StepOutcome::Complete).map(|r| r.attempt).collect::<Vec<_>>();
    // The cut left rpc.1's exited verdict and no more; rpc.2's delete ran to its end.
    assert_eq!((of(&cut_steps, FAILING, "TerminateRuntime"), of(&cut_steps, FAILING, "NodeDeleted"), of(&cut_steps, FAILING, "Complete")), (vec![1], vec![], vec![]), "the cut left {FAILING}'s exited verdict and no NodeDeleted: {cut_steps:#?}");
    assert_eq!(of(&cut_steps, EXITING, "Complete"), vec![1], "{EXITING}'s delete ran to its end");

    // Attempt 2: the exited verdict is not the delete. The delete with no NodeDeleted is run to its end.
    assert_eq!(ran(2), BTreeSet::from([FAILING.to_string(), OTHER_ADMIN.to_string()]), "attempt 2 finishes {FAILING}'s delete (its exited verdict alone is not a finished delete) and runs the node-admin tier, and does not run {EXITING}, whose delete has every step's receipt");
    let manifest = second.unwrap_or_else(|e| panic!("the resumed attempt completes: {e}"));
    assert_eq!(births(&manifest.receipts), BTreeSet::from([FAILING.to_string(), EXITING.to_string(), OTHER_ADMIN.to_string()]), "the manifest carries every other member's receipt");
    assert_eq!(of(&steps, FAILING, "TerminateRuntime"), vec![1], "{FAILING}'s completed TerminateRuntime is not run again");
    assert_eq!((of(&steps, FAILING, "NodeDeleted"), of(&steps, FAILING, "Complete")), (vec![2], vec![2]), "{FAILING}'s NodeDeleted and Complete are receipted once, by the resumed attempt");
    assert_eq!(fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == FAILING).count(), 1, "{FAILING}'s runtime was terminated once, in attempt 1");
    assert_eq!(fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == EXITING).count(), 1, "{EXITING}'s runtime was terminated once");
    let published = |d: Departures| -> Vec<String> {
        let mut ops: Vec<String> = d.0.into_inner().unwrap().into_iter().map(|op| op.operation).collect();
        ops.sort();
        ops
    };
    assert_eq!(published(lost_departures), vec![format!("node.delete:{EXITING}")], "attempt 1's primary published the NodeDeleted of the one delete it finished");
    let mut want: Vec<String> = [FAILING, OTHER_ADMIN].iter().map(|n| format!("node.delete:{n}")).collect();
    want.sort();
    assert_eq!(published(departures), want, "attempt 2's primary publishes the NodeDeleted of the deletes it ran, {FAILING}'s and {OTHER_ADMIN}'s, and never again {EXITING}'s, its node.delete's under attempt 1");

    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let step = |node: &str, step: &str, attempt: &str| named("rdm.node_admin.deployment.update.via-step").into_iter().filter(|s| s["attributes"]["node"] == node && s["attributes"]["step"] == step && s["attributes"]["attempt"] == attempt).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>();
    assert_eq!(step(FAILING, "TerminateRuntime", "2"), vec!["reused".to_string()], "attempt 2 reuses the receipted TerminateRuntime");
    assert_eq!(step(FAILING, "NodeDeleted", "2"), vec!["complete".to_string()], "attempt 2 executes the NodeDeleted that has no receipt");
    let retained = named("rdm.node_admin.mesh.resolve.via-retained-receipts");
    assert!(retained.iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["retained"] == "1" && s["attributes"]["retained_births"] == EXITING && s["attributes"]["remaining"] == "2"), "attempt 2 keeps the one finished delete and runs the other two members: {retained:?}");
    eprintln!("trace {trace_id}: attempt 2 via-step {FAILING} TerminateRuntime reused, NodeDeleted complete; via-retained-receipts retained 1 ({EXITING}) remaining 2");
}

/// The owner's open leave of mesh2, admitted over `records` with `FINAL` its final primary, and the
/// receiver its handoff resolves.
fn open_leave(mesh_delete_view: &MeshDeleteView, records: &[Node]) -> (MeshDeleteKey, tokio::sync::watch::Receiver<Option<rdm_admin::mesh_delete::MeshDeleteOutcome>>, RosterBirth) {
    let roster = live_roster(records).unwrap();
    let final_primary = RosterBirth::of(records.iter().find(|n| n.name.to_string() == FINAL).unwrap()).unwrap();
    let key = MeshDeleteKey { mesh_id: MeshId::mint(), build_id: BuildId::mint(), attempt: 1, operation: String::new() };
    let key = MeshDeleteKey { operation: operation(&key.mesh_id), ..key };
    let admitted = Admitted { roster, final_primary: final_primary.clone(), final_runtime: RuntimeProof::of(&handle(FINAL, 46_000).fact().unwrap()), amendments: Vec::new() };
    let handoff = mesh_delete_view.open(key.clone(), admitted);
    (key, handoff, final_primary)
}

/// Run `f` on a current-thread runtime with its clock paused, under the cell's capture: a wait
/// that runs to a timer finishes at once with the timer's elapsed virtual time.
fn run_paused<F: std::future::Future>(cap: &Capture, f: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap();
    let _g = tracing::dispatcher::set_default(&cap.dispatch);
    rt.block_on(f)
}

/// CONTRACT: the owner's wait for the leaving mesh's handoff ends the moment its view holds the
/// final primary lost (mesh-delete.md, "Resume after the primary's loss"): the attempt ends
/// incomplete, naming the lost final primary, its receipts standing for the next attempt, which
/// resumes. A view change that names no loss keeps the wait open. Must NOT happen: the wait running on
/// to a timer (`MESH_DELETED_WAIT`) after the loss is in, the 120 s attempt 1 of the leave stems
/// (trace b84c9abc66ec8a8cee045c1e871514cb).
#[test]
fn the_owners_wait_ends_when_the_final_primary_is_lost_never_on_a_timer() {
    let cap = capture();
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let mesh_delete_view = MeshDeleteView::default();
    let (key, handoff, final_primary) = open_leave(&mesh_delete_view, &records);
    let (end, open_after_silence, waited, trace_id) = run_paused(&cap, async {
        let root = tracing::info_span!("cell.owner-wait-exited-verdict", operation = %key.operation);
        let trace_id = trace_id(&root);
        let (view_tx, view_rx) = tokio::sync::watch::channel(0u64);
        let proven: Arc<Mutex<Option<String>>> = Arc::default();
        let held = proven.clone();
        let (k, primary) = (key.clone(), format!("{} {}", final_primary.node, final_primary.incarnation_id));
        let wait = tokio::spawn(tracing::Instrument::instrument(
            async move { await_mesh_deleted(&k, &primary, handoff, view_rx, move || std::future::ready(held.lock().unwrap().clone()), MESH_DELETED_WAIT).await },
            root.clone(),
        ));
        // The final primary goes silent: the view changes and proves nothing.
        view_tx.send_modify(|t| *t += 1);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let open_after_silence = !wait.is_finished();
        // The owner's view holds it unheard, its runtime read exited.
        let began = tokio::time::Instant::now();
        *proven.lock().unwrap() = Some(format!("the final primary is unheard; its runtime reads exited: {} {}", final_primary.node, final_primary.incarnation_id));
        view_tx.send_modify(|t| *t += 1);
        let end = wait.await.unwrap();
        (end, open_after_silence, began.elapsed(), trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("owner-wait-exited-verdict");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());

    assert!(open_after_silence, "a view change that proves nothing keeps the wait open");
    assert!(waited < MESH_DELETED_WAIT, "the wait ended on the proof, not on the {MESH_DELETED_WAIT:?} timer: it ended after {waited:?} with {end:?}");
    match &end {
        Err(rdm_admin::executor::RunEnd::Incomplete(reason)) => {
            assert!(reason.contains(FINAL) && reason.contains("is lost"), "the incomplete end names the lost final primary: {reason}");
        }
        other => panic!("the attempt ends incomplete on the final primary's loss: {other:?}"),
    }
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let ended = named("rdm.node_admin.mesh.update.via-final-primary-lost");
    assert_eq!(ended.len(), 1, "one span names the wait's end on the loss: {spans:?}");
    assert_eq!(ended[0]["attributes"]["final_primary"], json!(format!("{} {}", final_primary.node, final_primary.incarnation_id)), "{}", ended[0]);
    assert!(ended[0]["attributes"]["verdict"].as_str().is_some_and(|p| p.contains("exited")), "{}", ended[0]);
}

/// CONTRACT: a handoff that arrives ends the owner's wait with its validated manifest, even when the
/// final primary is proven exited afterwards (it reported mesh.deleted while it lived, then went):
/// the handoff is read first. Must NOT happen: an exited verdict discarding a handoff already held.
#[test]
fn a_handoff_held_ends_the_owners_wait_with_its_manifest_even_when_the_final_primary_exits_after() {
    let cap = capture();
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let mesh_delete_view = MeshDeleteView::default();
    let (key, handoff, final_primary) = open_leave(&mesh_delete_view, &records);
    let manifest = ExitManifest { mesh_id: key.mesh_id.to_string(), roster: live_roster(&records).unwrap(), final_primary: final_primary.clone(), final_runtime: None, receipts: Vec::new() };
    let end = run_paused(&cap, async {
        let (view_tx, view_rx) = tokio::sync::watch::channel(0u64);
        assert!(mesh_delete_view.resolve(&key, manifest.clone()), "the first outcome of the leave");
        view_tx.send_modify(|t| *t += 1);
        await_mesh_deleted(&key, FINAL, handoff, view_rx, || std::future::ready(Some("its exited-verdict row is read".to_string())), MESH_DELETED_WAIT).await
    });
    assert_eq!(end.ok(), Some(manifest), "the handoff held ends the wait with its manifest");
}

// @feature: mesh-delete
/// CONTRACT: docs/architecture/lifecycles/mesh-delete.md:32-58, 63-81
/// "The mesh-primary runs `node.delete` for every other accepted birth in tier order ...
/// It may begin the next tier only when every birth in the current tier has an `Exited` receipt."
/// "Resume after the primary's loss:
/// 1. When the mesh-primary is lost during the leave, the fabric-primary proves its runtime is
///    `exited` via the provider and records its `Exited` receipt in the Build state.
/// 2. If another `node-admin` of the leaving mesh is alive and elected mesh-primary, the
///    fabric-primary appends a `RosterAmended { predecessor, final_primary }` receipt ...
/// 3. The new mesh-primary reads the Build's step receipts and runs only the tiers without a
///    terminal receipt. Completed member receipts stand."
#[test]
fn mesh_delete_step_faults_recover_or_fail_as_the_page_specifies() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-delete-step-faults-{}", NodeId::mint()));
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records
        .iter()
        .enumerate()
        .map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 47_000 + i as u32)))
        .collect();
    let fleet = Fleet {
        pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(),
        ..Default::default()
    };
    let birth = |name: &str| RosterBirth::of(records.iter().find(|n| n.name.to_string() == name).unwrap()).unwrap();
    let (lost, survivor) = (birth(FINAL), birth(OTHER_ADMIN));
    let (lost_runtime, survivor_runtime) = (
        RuntimeProof::of(&handles[FINAL].fact().unwrap()),
        RuntimeProof::of(&handles[OTHER_ADMIN].fact().unwrap()),
    );
    let view = View::default();
    let calls: Mutex<Vec<(u32, String)>> = Mutex::new(Vec::new());
    let amendment = RosterAmended {
        predecessor: lost.clone(),
        final_primary: survivor.clone(),
        final_runtime: survivor_runtime.clone(),
    };

    let out = run_paused(&cap, async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted {
                build_id: build_id.clone(),
                topology: rdm_admin::accepted::FabricTopology::root(
                    "fabric1",
                    "mesh1",
                    rdm_admin::model::MeshId::parse("meshd0000001").unwrap(),
                ),
                submitted_change: None,
                submitted_at_ms: 0,
            })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| MeshDeleteKey {
            mesh_id: mesh_id.clone(),
            build_id: build_id.clone(),
            attempt,
            operation: outer.clone(),
        };
        let joins = Joins::default();
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rdm-rpc-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher {
                name: FINAL.parse().unwrap(),
                node_id: NodeId::mint(),
                incarnation: IncarnationId::mint(),
            },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };

        let root = tracing::info_span!("cell.mesh-delete-step-faults", build_id = %build_id.log(), operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Step 1-2: Admit accepted roster on owner and leaving roster on mesh-primary.
        let roster = accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &lost_runtime, || live_roster(&records))
            .await
            .unwrap();
        let mp1 = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, FINAL, &lost, &lost_runtime, || live_roster(&records))
            .await
            .unwrap();

        // Fault 1: Cut after TerminateRuntime (before NodeDeleted) on FAILING (`mesh2.rpc.1`), while
        // EXITING (`mesh2.rpc.2`) completes in the ordinary-nodes tier. The ordinary tier is left
        // incomplete, the node-admin tier never starts, and an incomplete handoff is refused by name
        // (`RejectedIncompleteManifest`).
        let cut = CutBuilds {
            inner: &builds,
            cut: Mutex::new(Some(("NodeDeleted", format!("node.delete:{FAILING}")))),
        };
        let cut_pipeline = DeploymentPipeline {
            provider: &fleet,
            joins: &joins,
            observer: &NoRpc,
            sink: &view,
            lifecycle: &NoLifecycleEvents,
            builds: &cut,
            template: &template,
            timeouts: Timeouts::default(),
        };
        let pipeline = DeploymentPipeline {
            provider: &fleet,
            joins: &joins,
            observer: &NoRpc,
            sink: &view,
            lifecycle: &NoLifecycleEvents,
            builds: &builds,
            template: &template,
            timeouts: Timeouts::default(),
        };

        let first = members_exited(&builds, &key(1), FINAL, &mp1, &lost, lost_runtime.clone(), &records, &[], |n: Node| {
            let (pipeline, handles, calls, build_id) = (&cut_pipeline, &handles, &calls, &build_id);
            async move {
                let name = n.name.to_string();
                calls.lock().unwrap().push((1, name.clone()));
                pipeline
                    .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest {
                        build_id: (*build_id).clone(),
                        attempt: 1,
                        node: n,
                        handle: handles[&name].clone(),
                        unreached: false,
                    })
                    .await
                    .map_err(|e| e.to_string())
            }
        })
        .await;
        let incomplete_facts = builds.facts().await.unwrap();
        let initial_admitted = Admitted {
            roster: roster.clone(),
            final_primary: lost.clone(),
            final_runtime: lost_runtime.clone(),
            amendments: Vec::new(),
        };
        let incomplete_verdict = handoff_verdict(
            &incomplete_facts,
            &key(1),
            &initial_admitted,
            &manifest_reference(build_id.as_str(), 1, &outer, false),
        );

        // Fault 2: The final primary `mesh2.admin.1` is killed/lost while the fabric-primary awaits
        // `mesh.deleted`. Silence does not end the wait; once the final primary's runtime is proven
        // exited, `await_mesh_deleted` ends immediately with `RunEnd::Incomplete` (not waiting for
        // `MESH_DELETED_WAIT`).
        let mesh_delete_view = MeshDeleteView::default();
        let handoff_rx = mesh_delete_view.open(key(1), initial_admitted);
        let (view_tx, view_rx) = tokio::sync::watch::channel(0u64);
        let proven: Arc<Mutex<Option<String>>> = Arc::default();
        let held = proven.clone();
        let (k1, primary_label) = (key(1), format!("{} {}", lost.node, lost.incarnation_id));
        let wait_task = tokio::spawn(tracing::Instrument::instrument(
            async move {
                await_mesh_deleted(
                    &k1,
                    &primary_label,
                    handoff_rx,
                    view_rx,
                    move || std::future::ready(held.lock().unwrap().clone()),
                    MESH_DELETED_WAIT,
                )
                .await
            },
            root.clone(),
        ));
        view_tx.send_modify(|t| *t += 1);
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        let open_on_silence = !wait_task.is_finished();
        let began_wait = tokio::time::Instant::now();
        *proven.lock().unwrap() = Some(format!(
            "the final primary is unheard; its runtime reads exited: {} {}",
            lost.node, lost.incarnation_id
        ));
        view_tx.send_modify(|t| *t += 1);
        let wait_end = wait_task.await.unwrap();
        let wait_elapsed = began_wait.elapsed();

        // Resume on Attempt 2:
        // 1. Fabric-primary records the lost primary's exact `Exited` receipt (`TerminateRuntime`)
        //    and appends `RosterAmended { mesh2.admin.1 -> mesh2.admin.2 }` (replaying `AlreadyApplied`
        //    on duplicate and refusing a conflicting second amendment for the same predecessor).
        node_delete_receipts(&builds, &key(2), "mesh1.admin.1", &exited(&lost, &lost_runtime)).await;
        let applied = amend_roster(&builds, &key(2), "mesh1.admin.1", &amendment).await;
        let replayed = amend_roster(&builds, &key(2), "mesh1.admin.1", &amendment).await;
        let conflicting = amend_roster(
            &builds,
            &key(2),
            "mesh1.admin.1",
            &RosterAmended {
                final_primary: birth(EXITING),
                ..amendment.clone()
            },
        )
        .await;

        // 2. The surviving mesh-primary `mesh2.admin.2` resumes `members_exited`: it keeps EXITING's
        //    completed delete and the lost primary's exited receipt, and finishes only FAILING's missing
        //    `NodeDeleted` + `Complete` (reusing FAILING's `TerminateRuntime` without re-terminating).
        let steps_before_resume = builds.read_build(&build_id).await.unwrap().steps;
        let admitted = admitted_of(&steps_before_resume, &outer).unwrap().unwrap();
        let leaving = accepted_row(&steps_before_resume, &outer, STEP_LEAVING_ROSTER)
            .unwrap()
            .unwrap()
            .roster;
        let mp2 = amended_roster(&leaving, &steps_before_resume, &outer).unwrap();
        let records2: Vec<Node> = view.holds(&records);
        let manifest = members_exited(&builds, &key(2), OTHER_ADMIN, &mp2, &survivor, survivor_runtime.clone(), &records2, &[], |n: Node| {
            let (pipeline, handles, calls, build_id) = (&pipeline, &handles, &calls, &build_id);
            async move {
                let name = n.name.to_string();
                calls.lock().unwrap().push((2, name.clone()));
                pipeline
                    .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest {
                        build_id: (*build_id).clone(),
                        attempt: 2,
                        node: n,
                        handle: handles[&name].clone(),
                        unreached: false,
                    })
                    .await
                    .map_err(|e| e.to_string())
            }
        })
        .await
        .expect("attempt 2 finishes the cut member delete and completes OtherMembersExited");

        // 3. Handoff verdict accepts the amended primary's manifest, refuses a stale handoff naming
        //    the lost predecessor, records `MeshDeleted`, and after `mesh2.admin.2`'s separate
        //    `node.delete`, completes `MeshLeft` over all 4 births.
        let facts = builds.facts().await.unwrap();
        let verdict = handoff_verdict(&facts, &key(2), &admitted, &manifest_reference(build_id.as_str(), 2, &outer, false));
        let HandoffVerdict::Accept(completed) = verdict.clone() else {
            panic!("amended handoff must be accepted: {verdict:?}");
        };
        record_completion(&builds, &key(2), "mesh1.admin.1", &completed).await.unwrap();

        let delete_build = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted {
                build_id: delete_build.clone(),
                topology: rdm_admin::accepted::FabricTopology::root(
                    "fabric1",
                    "mesh1",
                    rdm_admin::model::MeshId::parse("meshd0000001").unwrap(),
                ),
                submitted_change: None,
                submitted_at_ms: 0,
            })
            .await
            .unwrap();
        let last = records2.iter().find(|n| n.name.to_string() == OTHER_ADMIN).cloned().unwrap();
        let terminal = pipeline
            .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest {
                build_id: delete_build,
                attempt: 1,
                node: last,
                handle: handles[OTHER_ADMIN].clone(),
                unreached: false,
            })
            .await
            .unwrap();
        let steps_final = builds.read_build(&build_id).await.unwrap().steps;
        let mesh_left = completion_of(&steps_final, &outer)
            .unwrap()
            .map_err(|e| e.to_string())
            .and_then(|c| mesh_left_proof(&c, &terminal));
        let replay_handoff = replayed_handoff(&steps_final, &key(2), &records[2].node_id, &survivor.incarnation_id);
        (
            first,
            incomplete_verdict,
            open_on_silence,
            wait_end,
            wait_elapsed,
            applied,
            replayed,
            conflicting,
            manifest,
            mesh_left,
            replay_handoff,
            trace_id,
        )
    });

    let (
        first,
        incomplete_verdict,
        open_on_silence,
        wait_end,
        wait_elapsed,
        applied,
        replayed,
        conflicting,
        manifest,
        mesh_left,
        replay_handoff,
        trace_id,
    ) = out;
    let spans = cap.spans();
    let _ = std::fs::remove_dir_all(&data_root);

    let err = first.expect_err("cut before NodeDeleted leaves ordinary-nodes tier incomplete");
    assert!(err.contains("tier ordinary-nodes is incomplete") && err.contains(FAILING), "{err}");
    assert!(
        matches!(
            incomplete_verdict.refusal(),
            Some(StatusReply::RejectedIncompleteManifest { missing: 1, .. })
        ),
        "incomplete handoff is refused RejectedIncompleteManifest naming the uncompleted admin birth: {incomplete_verdict:?}"
    );
    assert!(open_on_silence, "silence alone does not end await_mesh_deleted");
    assert!(wait_elapsed < MESH_DELETED_WAIT, "await_mesh_deleted ends immediately on proven loss: {wait_elapsed:?}");
    assert!(
        matches!(&wait_end, Err(rdm_admin::executor::RunEnd::Incomplete(r)) if r.contains(FINAL) && r.contains("is lost")),
        "await_mesh_deleted ends Incomplete naming the lost final primary: {wait_end:?}"
    );
    assert_eq!(applied, Ok(Amended::Applied));
    assert_eq!(replayed, Ok(Amended::AlreadyApplied));
    assert!(conflicting.is_err(), "conflicting second amendment for the same predecessor is refused");

    let calls = calls.into_inner().unwrap();
    let ran_attempt_2: Vec<String> = calls.iter().filter(|(a, _)| *a == 2).map(|(_, n)| n.clone()).collect();
    assert_eq!(
        ran_attempt_2,
        vec![FAILING.to_string()],
        "attempt 2 finishes only the cut member delete and does not re-run completed members: {calls:?}"
    );
    assert_eq!(
        fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == FAILING).count(),
        1,
        "{FAILING}'s runtime is terminated only once across the cut and resume"
    );
    assert_eq!(
        births(&manifest.receipts),
        BTreeSet::from([FAILING.to_string(), EXITING.to_string(), FINAL.to_string()]),
        "the amended manifest carries every birth except the surviving final primary"
    );
    assert_eq!(replay_handoff, Some(StatusReply::AlreadyApplied));
    let all = mesh_left.expect("MeshLeft proof completes once the surviving primary's own node.delete finishes");
    assert_eq!(
        births(&all.receipts),
        BTreeSet::from([FAILING.to_string(), EXITING.to_string(), OTHER_ADMIN.to_string(), FINAL.to_string()]),
        "MeshLeft covers all 4 roster births"
    );

    let named = |name: &str| {
        spans
            .iter()
            .filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str())
            .cloned()
            .collect::<Vec<_>>()
    };
    assert!(!named("rdm.node_admin.mesh.update.via-final-primary-lost").is_empty());
    assert!(!named("rdm.node_admin.mesh.update.via-roster-amended").is_empty());
    assert!(!named("rdm.node_admin.mesh.update.via-mesh-deleted-receipt").is_empty());
}

/// CONTRACT:
/// - `docs/architecture/lifecycles/mesh-delete.md:67-68` ("1. The lost primary's node investigation verdict is recorded as its roster receipt; a verdict of `reached` refuses the resume by name, since the primary is not lost.")
/// - `docs/architecture/lifecycles/mesh-delete.md:252` ("| mesh.delete Build | fabric-primary | build.create, build.attempt.run; claim for the mesh-primary | Build, attempt, `mesh.delete:<mesh_id>` |")
/// - `docs/architecture/lifecycles/mesh-delete.md:253` ("| Fabric-primary change | fabric-primary | [node.fabric-primary.update](node-primary-update.md#observability-contract) when the mesh-primary is the fabric-primary | same trace |")
/// - `docs/architecture/lifecycles/mesh-delete.md:254` ("| Member delete | mesh-primary and each member | [node.delete](node-delete.md#observability-contract) per member; exact Exited receipt | outer operation plus `node.delete:<path.name>` |")
/// - `docs/architecture/lifecycles/mesh-delete.md:255` ("| mesh.deleted | mesh-primary | manifest validated by the fabric-primary; mesh row deleted; mesh topology carries the mesh deleted | outer operation; complete roster |")
/// - `docs/architecture/lifecycles/mesh-delete.md:256` ("| Mesh-primary delete | fabric-primary | node.delete with the fabric-primary as executor; exact Exited; row deleted | its own Build and trace |")
#[test]
fn lost_primary_verdict_is_recorded_as_roster_receipt_reached_refuses_resume_and_observability_spans_correlate() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-delete-conformance-{}", NodeId::mint()));
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records
        .iter()
        .enumerate()
        .map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 58_000 + i as u32)))
        .collect();
    let fleet = Fleet {
        pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(),
        ..Default::default()
    };
    let birth = |name: &str| RosterBirth::of(records.iter().find(|n| n.name.to_string() == name).unwrap()).unwrap();
    let (lost, survivor) = (birth(FINAL), birth(OTHER_ADMIN));
    let (lost_runtime, survivor_runtime) = (
        RuntimeProof::of(&handles[FINAL].fact().unwrap()),
        RuntimeProof::of(&handles[OTHER_ADMIN].fact().unwrap()),
    );
    let lost_node = records.iter().find(|n| n.name.to_string() == FINAL).unwrap().clone();
    let survivor_node = records.iter().find(|n| n.name.to_string() == OTHER_ADMIN).unwrap().clone();
    let row = rdm_admin::storage::RuntimeRow {
        node_id: lost_node.node_id.clone(),
        name: lost_node.name.clone(),
        incarnation_id: lost_node.incarnation_id.clone().unwrap(),
        runtime: handles[FINAL].fact().unwrap(),
        data_dir: None,
    };
    let storage = Arc::new(MemoryNodesStorage::default());
    let mesh_storage = MemoryMeshStorage::default();
    let declared = Mutex::new(Declared::default());
    let fp = {
        let mut n = node("mesh1.admin.1");
        n.is_fabric_primary = true;
        n
    };
    let writer_topo = Arc::new(RwLock::new(writer_view(&records, None, Some(fp.clone()))));
    let rows = Arc::new(NodeRows::new(fp.name.clone(), fp.node_id.clone(), writer_topo.clone(), storage.clone()));
    let view = View::with_rows(rows.clone());

    let out = cap.run(async {
        rows_written(&storage, &rows, &records).await;
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted {
                build_id: build_id.clone(),
                topology: rdm_admin::accepted::FabricTopology::root(
                    "fabric1",
                    "mesh1",
                    rdm_admin::model::MeshId::parse("meshd0000001").unwrap(),
                ),
                submitted_change: None,
                submitted_at_ms: 0,
            })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| MeshDeleteKey {
            mesh_id: mesh_id.clone(),
            build_id: build_id.clone(),
            attempt,
            operation: outer.clone(),
        };
        let joins = Joins::default();
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rdm-rpc-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher {
                name: fp.name.clone(),
                node_id: fp.node_id.clone(),
                incarnation: fp.incarnation_id.clone().unwrap(),
            },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let pipeline = DeploymentPipeline {
            provider: &fleet,
            joins: &joins,
            observer: &NoRpc,
            sink: &view,
            lifecycle: &NoLifecycleEvents,
            builds: &builds,
            template: &template,
            timeouts: Timeouts::default(),
        };

        let root = tracing::info_span!("cell.mesh-delete-observability", build_id = %build_id.log(), attempt = 1, operation = %outer);
        let outer_trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Fabric-primary change on the same trace when the deleting mesh holds the fabric-primary role.
        tracing::info_span!(
            "rdm.node_admin.primary.update.via-primary-update",
            role = "fabric-primary",
            operation = %rdm_admin::fabric_handover::update_operation(rdm_core::PrimaryRole::FabricPrimary, build_id.as_str(), 1, &outer),
            outcome = "confirmed"
        )
        .in_scope(|| tracing::info!("node.fabric-primary.update confirmed before member delete"));

        // Capture roster on attempt 1.
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &lost_runtime, || live_roster(&records))
            .await
            .unwrap();
        accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, FINAL, &lost, &lost_runtime, || live_roster(&records))
            .await
            .unwrap();

        // While the primary is reached (not lost), no Exited roster receipt exists and the resume
        // amendment is refused by name.
        let reached_refusal = amend_roster(
            &builds,
            &key(2),
            "mesh1.admin.1",
            &RosterAmended {
                predecessor: lost.clone(),
                final_primary: survivor.clone(),
                final_runtime: survivor_runtime.clone(),
            },
        )
        .await;

        // Once the primary is lost, `resume_after_loss` runs `delete_explicit` on the lost primary,
        // recording its verdict (`Some("exited")`) as its `TerminateRuntime` roster receipt under
        // `node.delete:mesh2.admin.1`, then appends `RosterAmended`.
        let steps_1 = builds.read_build(&build_id).await.unwrap().steps;
        let adm_1 = admitted_of(&steps_1, &outer).unwrap().unwrap();
        let mut lost_for_delete = lost_node.clone();
        lost_for_delete.status = NodeStatus::PendingReconnect;
        let resumed = resume_after_loss(
            &builds,
            &fleet,
            Some(&row),
            &key(2),
            "mesh1.admin.1",
            &adm_1,
            &survivor,
            &survivor_runtime,
            || async {
                pipeline
                    .delete_explicit(&rdm_admin::deployment::pipeline::DeleteRequest {
                        build_id: build_id.clone(),
                        attempt: 2,
                        node: lost_for_delete,
                        handle: handles[FINAL].clone(),
                        unreached: true,
                    })
                    .await
                    .map(|_| ())
                    .map_err(rdm_admin::executor::RunEnd::from)
            },
        )
        .await
        .expect("resume_after_loss records the lost primary's verdict receipt and amends the roster");

        // Member delete under the amended mesh-primary (`mesh2.admin.2`), then `mesh.deleted` handoff
        // validation and Dead mesh row commit.
        let steps_2 = builds.read_build(&build_id).await.unwrap().steps;
        let roster_receipt = rdm_admin::mesh_delete::exit_receipts(&steps_2, &outer, std::slice::from_ref(&lost))
            .remove(&(lost.node_id.clone(), lost.incarnation_id.clone()))
            .expect("the lost primary's TerminateRuntime receipt is its roster receipt");
        let adm_2 = admitted_of(&steps_2, &outer).unwrap().unwrap();
        let leaving = accepted_row(&steps_2, &outer, STEP_LEAVING_ROSTER).unwrap().unwrap().roster;
        let mp2 = amended_roster(&leaving, &steps_2, &outer).unwrap();
        let records2 = view.holds(&records);
        let manifest = members_exited(&builds, &key(2), OTHER_ADMIN, &mp2, &survivor, survivor_runtime.clone(), &records2, &[], |n: Node| {
            let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
            async move {
                let name = n.name.to_string();
                pipeline
                    .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest {
                        build_id: (*build_id).clone(),
                        attempt: 2,
                        node: n,
                        handle: handles[&name].clone(),
                        unreached: false,
                    })
                    .await
                    .map_err(|e| e.to_string())
            }
        })
        .await
        .expect("member deletes complete");
        let facts = builds.facts().await.unwrap();
        let HandoffVerdict::Accept(validated) = handoff_verdict(&facts, &key(2), &adm_2, &manifest_reference(build_id.as_str(), 2, &outer, false)) else {
            panic!("manifest must validate");
        };
        record_completion(&builds, &key(2), "mesh1.admin.1", &validated).await.unwrap();
        drop(_in_root);

        // Separate `node.delete` of the final mesh-primary (`mesh2.admin.2`) in its own Build and trace,
        // with the fabric-primary as executor.
        let delete_build = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted {
                build_id: delete_build.clone(),
                topology: rdm_admin::accepted::FabricTopology::root(
                    "fabric1",
                    "mesh1",
                    rdm_admin::model::MeshId::parse("meshd0000001").unwrap(),
                ),
                submitted_change: None,
                submitted_at_ms: 0,
            })
            .await
            .unwrap();
        let delete_root = tracing::info_span!("cell.mesh-primary-delete", build_id = %delete_build.log(), operation = %format!("node.delete:{OTHER_ADMIN}"));
        let delete_trace_id = trace_id(&delete_root);
        let _in_delete = delete_root.enter();
        writer_topo.write().await.nodes.iter_mut().filter(|n| n.name == survivor_node.name).for_each(|n| {
            n.status = NodeStatus::Leaving;
            n.stopped = true;
        });
        let terminal = pipeline
            .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest {
                build_id: delete_build.clone(),
                attempt: 1,
                node: Node { stopped: true, ..survivor_node.clone() },
                handle: handles[OTHER_ADMIN].clone(),
                unreached: false,
            })
            .await
            .expect("the mesh-primary's separate node.delete completes");
        let all = mesh_left_proof(&validated, &terminal).expect("MeshLeft proof covers all 4 roster births");
        mesh_left_rows(Some(&rows), &mesh_storage, &Default::default(), &declared, "mesh1.admin.1", &key(2), "mesh2", &all.receipts)
            .await
            .expect("mesh row Dead is written");
        let survivor_row = storage.record_at(&survivor_node.name).await.unwrap().expect("mesh-primary row exists");
        let mesh_dead = mesh_storage.mesh(&mesh_id).await.unwrap().is_some_and(|r| r.states.contains(&MeshState::Dead));
        (
            reached_refusal,
            resumed,
            roster_receipt,
            manifest,
            all,
            survivor_row,
            mesh_dead,
            build_id,
            delete_build,
            outer,
            outer_trace_id,
            delete_trace_id,
        )
    });

    let (
        reached_refusal,
        resumed,
        roster_receipt,
        manifest,
        all,
        survivor_row,
        mesh_dead,
        build_id,
        delete_build,
        outer,
        trace_id,
        delete_trace_id,
    ) = out;
    let spans = cap.spans();
    let _ = std::fs::remove_dir_all(&data_root);

    let refused = reached_refusal.expect_err("a primary not proven lost refuses the resume amendment by name");
    assert!(
        refused.contains(FINAL) && refused.contains("not proven exited"),
        "refusal names the primary that is not lost: {refused}"
    );
    assert_eq!(resumed, Amended::Applied);
    assert_eq!(
        roster_receipt.verdict.as_deref(),
        Some("exited"),
        "the lost primary's verdict is recorded as its roster receipt: {roster_receipt:?}"
    );
    assert_eq!(
        births(&manifest.receipts),
        BTreeSet::from([FAILING.to_string(), EXITING.to_string(), FINAL.to_string()])
    );
    assert_eq!(
        births(&all.receipts),
        BTreeSet::from([FAILING.to_string(), EXITING.to_string(), OTHER_ADMIN.to_string(), FINAL.to_string()])
    );
    assert_eq!(survivor_row.status, NodeStatus::Deleted, "mesh-primary row is written Deleted");
    assert!(mesh_dead, "mesh row is written Dead");
    assert_ne!(build_id, delete_build, "mesh-primary delete runs in its own Build");
    assert_ne!(trace_id, delete_trace_id, "mesh-primary delete runs in its own trace");

    let on_trace = |tid: &str, name: &str| {
        spans
            .iter()
            .filter(|s| s["trace_id"] == tid && s["name"] == name)
            .cloned()
            .collect::<Vec<_>>()
    };
    assert!(
        on_trace(&trace_id, "rdm.node_admin.mesh.resolve.via-accepted-roster")
            .iter()
            .any(|s| s["attributes"]["operation"] == outer.as_str()),
        "mesh.delete Build span correlates Build, attempt and mesh.delete:<mesh_id>"
    );
    assert!(
        !on_trace(&trace_id, "rdm.node_admin.primary.update.via-primary-update").is_empty(),
        "fabric-primary change runs on the same trace as mesh.delete"
    );
    for member in [FAILING, EXITING] {
        let member_op = format!("node.delete:{member}");
        assert!(
            on_trace(&trace_id, "rdm.node_admin.deployment.update.via-pipeline")
                .iter()
                .any(|s| s["attributes"]["pipeline"] == "delete" && s["attributes"]["node"] == member)
                && on_trace(&trace_id, "rdm.node_admin.node.delete.via-node-row")
                    .iter()
                    .any(|s| s["attributes"]["operation"] == member_op.as_str() && s["attributes"]["subject"] == member),
            "member delete correlates node.delete:{member} on the outer trace"
        );
    }
    assert!(
        on_trace(&trace_id, "rdm.node_admin.mesh.update.via-mesh-deleted-receipt")
            .iter()
            .any(|s| s["attributes"]["operation"] == outer.as_str() && s["attributes"]["roster"] == "4"),
        "mesh.deleted span correlates the outer operation and complete roster"
    );
    assert!(
        on_trace(&delete_trace_id, "rdm.node_admin.deployment.update.via-pipeline")
            .iter()
            .any(|s| s["attributes"]["pipeline"] == "delete" && s["attributes"]["node"] == OTHER_ADMIN),
        "mesh-primary delete runs node.delete with the fabric-primary as executor on its own Build and trace"
    );
    assert!(
        on_trace(&delete_trace_id, "rdm.node_admin.node.delete.via-node-row")
            .iter()
            .any(|s| s["attributes"]["subject"] == OTHER_ADMIN && s["attributes"]["operation"] == format!("node.delete:{OTHER_ADMIN}").as_str() && s["attributes"]["outcome"] == "row-deleted"),
        "mesh-primary delete writes the mesh-primary row Deleted on its own trace"
    );
}

