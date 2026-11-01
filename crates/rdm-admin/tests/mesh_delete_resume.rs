//! The mesh-delete resumption (i143.e12.s39, s49; mesh-delete.md steps 1, 3 and "Resume after the
//! primary's loss"): the deleting mesh-primary's member workflows are the product's
//! (`mesh_delete::members_exited` over each member's node.delete, `DeploymentPipeline::delete`); the
//! provider, the Build state and the view are fixtures.
//!
//! mesh2 leaves: two ordinary nodes, one other node-admin and the final primary path `mesh2.admin`, name `1`.
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
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::Mutex;

const DOMAIN: &str = "mesh-delete-resume";

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
    let (capture, layer) = rdm_transport::span_capture(DOMAIN, exporter.clone());
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
        self.0.committed_facts(build).await
    }
    async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
        self.0.forget(id).await
    }
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, rdm_core::OperationId)>, BuildStateError> {
        self.0.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, rdm_core::OperationId)>, BuildStateError> {
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
    async fn spawn(&self, _: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
        unreachable!("a mesh leave launches nothing")
    }
    async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        self.terminated.lock().unwrap().push(self.pids[&h.pid.unwrap()].clone());
        Ok(())
    }
    async fn inspect(&self, h: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
        let mut blind = self.blind_once.lock().unwrap();
        let terminated = h.pid.and_then(|pid| self.pids.get(&pid)).is_some_and(|n| self.terminated.lock().unwrap().contains(n));
        if *blind == h.pid && terminated {
            *blind = None;
            return DeploymentStatus::Unknown;
        }
        DeploymentStatus::Exited { code: Some(0) }
    }
    async fn signal_stop(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
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
struct View(Mutex<BTreeMap<NodeId, Node>>, Mutex<HashSet<rdm_core::framing::Birth>>, Option<Arc<NodeRows>>);
impl TopologySink for View {
    fn publish(&self, n: Node) {
        self.0.lock().unwrap().insert(n.node_id.clone(), n);
    }
    fn remove(&self, _: &PathName) {}
    fn remove_birth(&self, _: &PathName, node_id: &NodeId, incarnation: &IncarnationId) {
        let mut view = self.0.lock().unwrap();
        if view.get(node_id).is_some_and(|n| n.incarnation_id.as_ref() == Some(incarnation)) {
            view.remove(node_id);
        }
        self.1.lock().unwrap().insert(rdm_core::framing::Birth { node_id: node_id.clone(), incarnation: incarnation.clone() });
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
        records.iter().map(|n| view.get(&n.node_id).cloned().unwrap_or_else(|| n.clone()))
            .filter(|n| !n.incarnation_id.as_ref().is_some_and(|incarnation| removed.contains(&rdm_core::framing::Birth { node_id: n.node_id.clone(), incarnation: incarnation.clone() })))
            .collect()
    }
}

fn node_at(path: &str, name: &str, mesh: &str, kind: rdm_core::NodeKind) -> Node {
    let mut n = Node::allocated(PathName::new(path, name).unwrap(), mesh, kind);
    n.status = NodeStatus::Ready;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

struct Members {
    failing: Node,
    exiting: Node,
    other_admin: Node,
    final_admin: Node,
    fp: Node,
}

impl Members {
    fn new() -> Self {
        Self {
            failing: node_at("mesh2.rpc", "1", "mesh2", rdm_core::NodeKind::RpcNode),
            exiting: node_at("mesh2.rpc", "2", "mesh2", rdm_core::NodeKind::RpcNode),
            other_admin: node_at("mesh2.admin", "2", "mesh2", rdm_core::NodeKind::NodeAdmin),
            final_admin: node_at("mesh2.admin", "1", "mesh2", rdm_core::NodeKind::NodeAdmin),
            fp: node_at("mesh1.admin", "1", "mesh1", rdm_core::NodeKind::NodeAdmin),
        }
    }
}

fn runtime_handles(records: &[Node], first_pid: u32) -> BTreeMap<NodeId, DeploymentHandle> {
    records.iter().enumerate().map(|(i, n)| (n.node_id.clone(), DeploymentHandle {
        deployment_id: DeploymentId::mint(), provider: ProviderKind::Process, pid: Some(first_pid + i as u32),
        start: Some(1), container: None, domain: Some(DOMAIN.into()),
    })).collect()
}

/// CONTRACT: removing an old birth preserves a newer incarnation and a different node at the
/// same location; renaming does not change which birth is removed.
#[test]
fn removed_birth_does_not_hide_a_new_incarnation_or_location_successor() {
    let original = node_at("mesh2.rpc", "north", "mesh2", rdm_core::NodeKind::RpcNode);
    let mut renamed = original.clone();
    renamed.name = PathName::new("mesh2.rpc", "north-old").unwrap();
    let successor = node_at("mesh2.rpc", "north", "mesh2", rdm_core::NodeKind::RpcNode);
    let mut newer = original.clone();
    newer.incarnation_id = Some(IncarnationId::mint());
    let view = View::default();
    view.publish(renamed.clone());
    view.remove_birth(&original.name, &original.node_id, original.incarnation_id.as_ref().unwrap());
    assert!(view.holds(&[renamed]).is_empty());
    view.publish(newer.clone());
    view.publish(successor.clone());
    view.remove_birth(&original.name, &original.node_id, original.incarnation_id.as_ref().unwrap());
    let held = view.holds(&[original, successor.clone()]);
    assert_eq!(held.len(), 2);
    assert!(held.iter().any(|n| n.node_id == newer.node_id && n.incarnation_id == newer.incarnation_id));
    assert!(held.iter().any(|n| n.node_id == successor.node_id && n.incarnation_id == successor.incarnation_id));
}

/// The roster as a view holds it at admission: every live member of mesh2.
fn live_roster(records: &[Node]) -> Result<Vec<RosterBirth>, String> {
    records.iter().filter(|n| n.status.is_live()).map(RosterBirth::of).collect()
}

fn sorted(mut v: Vec<RosterBirth>) -> Vec<RosterBirth> {
    v.sort();
    v
}

/// The receipts node.delete leaves for the exact birth `terminal` names, run to its end as a crashed
/// target by `executor` under `key`'s Build and attempt (node-delete.md "Crashed target"): what the
/// fabric-primary's node.delete of a lost primary records, whose `TerminateRuntime` is the birth's
/// roster receipt (mesh-delete.md "Resume after the primary's loss" step 1).
fn executor_birth(subject: &RosterBirth) -> rdm_core::framing::Birth {
    rdm_core::framing::Birth { node_id: NodeId::parse(&subject.node_id).unwrap(), incarnation: subject.birth().unwrap() }
}

async fn node_delete_receipts(builds: &dyn BuildStateAdapter, key: &MeshDeleteKey, executor: &rdm_core::framing::Birth, subject: &RosterBirth, terminal: &TerminalReceipt) {
    let subject_birth = executor_birth(subject);
    assert_eq!(terminal.node_id, subject.node_id);
    assert_eq!(terminal.incarnation_id, subject.incarnation_id);
    let operation = rdm_core::OperationId::node("node.delete", &subject_birth.node_id);
    let op = LifecycleOp { mesh: subject.mesh.clone(), kind: subject.kind, build_id: key.build_id.to_string(), attempt: key.attempt, operation: operation.clone(), node_id: subject_birth.node_id, incarnation: subject_birth.incarnation, name: subject.node.clone(), event_at_rafka_ms: 0, traceparent: None, tracestate: None };
    for (step, output) in [("NodeDeleting", serde_json::to_value(&op).ok()), ("TerminateRuntime", serde_json::to_value(terminal).ok()), ("NodeDeleted", serde_json::to_value(&op).ok()), ("RemoveTopologyMembership", None), ("Complete", None)] {
        builds
            .append_step_receipt(&BuildStepReceipt { mesh: None, build_id: key.build_id.clone(), attempt: key.attempt, operation: operation.clone(), step: step.into(), outcome: rdm_admin::build_state::StepOutcome::Complete, output, executor: Some(executor.clone()) })
            .await
            .unwrap();
    }
}

fn tier_of(name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("tier".to_string(), format!("tier-of-{name}"))])
}

/// The view of a writer of mesh2's rows: `records`, with `primary` the mesh's primary node-admin (a
/// live role) and `fabric_primary` holding the fabric-primary role.
fn writer_view(records: &[Node], primary: Option<&NodeId>, fabric_primary: Option<Node>) -> Topology {
    let mut nodes: Vec<Node> = records.iter().cloned().map(|mut n| {
        n.is_primary = Some(&n.node_id) == primary;
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
        storage.write_record(&NodeRow { replacement_from: None, kind: n.kind, mesh: n.mesh.clone(), initial_properties: None, topology_storage_provider: None, name: n.name.clone(), node_id: n.node_id.clone(), status: NodeStatus::Ready, config: tier_of(n.node_id.as_str()), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }).await.unwrap();
        assert!(matches!(rows.claim(&n.name, &n.mesh.clone(), n.kind, &n.node_id, &rdm_core::OperationId::location("node.create", &n.name), None, None, ClaimFor::Create).await, Ok(Claimed::HeldByNode(_))), "the writer holds {:?}'s row", n.name);
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
    let Members { failing, exiting, other_admin, final_admin, fp } = Members::new();
    let records = vec![failing.clone(), exiting.clone(), other_admin.clone(), final_admin.clone()];
    let handles = runtime_handles(&records, 41_000);
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.to_string())).collect(), ..Default::default() };
    *fleet.blind_once.lock().unwrap() = handles[&failing.node_id].pid;
    let me = RosterBirth::of(&final_admin).unwrap();
    let final_runtime = RuntimeProof::of(&handles[&final_admin.node_id].fact().unwrap());
    let calls: Mutex<Vec<(u32, String)>> = Mutex::new(Vec::new());
    let storage = Arc::new(MemoryNodesStorage::default());
    let final_id = final_admin.node_id.clone();
    let mut topology = writer_view(&records, None, None);
    topology.nodes.iter_mut().find(|n| n.node_id == final_id).unwrap().is_primary = true;
    let rows = Arc::new(NodeRows::new(final_admin.name.clone(), final_id, Arc::new(RwLock::new(topology)), storage.clone()));
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
            launcher: rdm_core::launch::Launcher { mesh: "mesh2".into(), name: final_admin.name.clone(), node_id: final_admin.node_id.clone(), incarnation: final_admin.incarnation_id.clone().unwrap() },
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
                let node_id = n.node_id.clone();
                calls.lock().unwrap().push((attempt, node_id.to_string()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: (*build_id).clone(), attempt, node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-delete-resume", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.recipe_step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Attempt 1: both sides capture the roster at admission; the ordinary tier is incomplete.
        let fp1 = accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, fp.node_id.as_str(), &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let mp1 = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key(1), final_admin.node_id.as_str(), &mp1, &me, final_runtime.clone(), &records, &[], shutdown(1)).await;

        // Attempt 2 reads the view the first one left: the node its node.delete removed is gone.
        let records2: Vec<Node> = view.holds(&records);
        let fp2 = accepted_roster(&builds, &key(2), STEP_ACCEPTED_ROSTER, fp.node_id.as_str(), &me, &final_runtime, || live_roster(&records2)).await.unwrap();
        let mp2 = accepted_roster(&builds, &key(2), STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), &me, &final_runtime, || live_roster(&records2)).await.unwrap();
        let second = members_exited(&builds, &key(2), final_admin.node_id.as_str(), &mp2, &me, final_runtime.clone(), &records2, &[], shutdown(2)).await;
        (first, fp1, fp2, mp1, mp2, second, records2, trace_id)
    });
    let spans = cap.spans();
    std::fs::create_dir_all(artifact_dir()).unwrap();
    std::fs::write(artifact_dir().join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", artifact_dir().join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    // Attempt 1: the ordinary tier is incomplete by name, and the node-admin tier never started.
    let err = first.expect_err("the ordinary tier holds an unproven exited verdict");
    assert!(err.contains("tier ordinary-nodes is incomplete") && err.contains(failing.node_id.as_str()), "{err}");
    let calls = calls.into_inner().unwrap();
    let ran = |attempt: u32| calls.iter().filter(|(a, _)| *a == attempt).map(|(_, n)| n.clone()).collect::<BTreeSet<_>>();
    assert_eq!(ran(1), BTreeSet::from([failing.node_id.to_string(), exiting.node_id.to_string()]), "attempt 1 ran the ordinary tier only");
    assert!(records2.iter().all(|n| n.node_id != exiting.node_id), "the resumed attempt's view no longer holds the node attempt 1's node.delete removed: {records2:?}");
    // This cell's observer holds no Node RPC client, so the unproven node's stop-node was never sent: nothing
    // set it stopping (states.md "Who sets each state": stopping on the stop call's admission, never before).
    let unproven = records2.iter().find(|n| n.node_id == failing.node_id).expect("the resumed attempt's view still holds the unproven node");
    let held = records.iter().find(|n| n.node_id == failing.node_id).unwrap();
    assert_eq!(unproven.status, held.status, "a member whose stop-node was never admitted keeps its status, never stopping: {unproven:?}");

    // The accepted roster is the one captured at admission, on both sides, in every attempt.
    let accepted = sorted(fp1.clone());
    assert_eq!(accepted.len(), 4, "every birth of mesh2: {accepted:?}");
    assert_eq!(sorted(mp1), accepted, "the leaving primary accepted the same roster");
    assert_eq!(sorted(fp2.clone()), accepted, "the fabric-primary's resumed attempt keeps the roster it accepted at admission");
    assert_eq!(sorted(mp2), accepted, "the leaving primary's resumed attempt keeps the roster it accepted at admission");

    // Attempt 2: the exited member is not run again; the rest are.
    let manifest = second.unwrap_or_else(|e| panic!("the resumed attempt completes: {e}"));
    assert_eq!(ran(2), BTreeSet::from([failing.node_id.to_string(), other_admin.node_id.to_string()]), "attempt 2 runs only the members with no Exited receipt");
    assert_eq!(fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == exiting.node_id.as_str()).count(), 1, "the exited member's runtime was stopped once");
    assert_eq!(manifest.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(), BTreeSet::from([&failing, &exiting, &other_admin].map(|n| (n.node_id.to_string(), n.incarnation_id.as_ref().unwrap().to_string()))), "the manifest carries every other member's receipt");
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

    // R-MN1 step 3: every other member's row is Deleted, readable by its node id
    // and config, each written by its own node.delete's NodeDeleted step; the exited node's in attempt
    // 1, which attempt 2 kept. The final primary's own row is not the mesh-primary's to delete (step 7
    // is the fabric-primary's).
    let read = |node_id: &NodeId| {
        let storage = storage.clone();
        let node_id = node_id.clone();
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async move { rdm_admin::storage::NodesStorage::node(storage.as_ref(), &node_id).await.unwrap() })
    };
    for n in records.iter().filter(|n| n.node_id != final_admin.node_id) {
        let row = read(&n.node_id).unwrap_or_else(|| panic!("{:?} has a row", n.name));
        assert_eq!((row.status, &row.node_id, &row.config), (NodeStatus::Deleted, &n.node_id, &tier_of(n.node_id.as_str())), "{:?}'s row is Deleted with its node id and config: {row:?}", n.name);
    }
    let last = read(&final_admin.node_id).unwrap();
    assert_ne!(last.status, NodeStatus::Deleted, "the final primary's row is not deleted at step 3: {last:?}");
    let deleted = named("rdm.node_admin.node.delete.via-node-row");
    let exited_row: Vec<&Value> = deleted.iter().filter(|s| s["attributes"]["node_id"] == exiting.node_id.as_str()).collect();
    assert!(exited_row.iter().any(|s| s["attributes"]["outcome"] == "row-deleted" && s["attributes"]["config_from"] == "writer-record"), "attempt 2 writes the retained member's row Deleted from the writer's record: {deleted:#?}");
    for node_id in [&failing.node_id, &other_admin.node_id] {
        assert!(deleted.iter().any(|s| s["attributes"]["node_id"] == node_id.as_str() && s["attributes"]["outcome"] == "row-deleted"), "{node_id}'s row is written Deleted: {deleted:#?}");
    }
    assert!(!deleted.iter().any(|s| s["attributes"]["node_id"] == final_admin.node_id.as_str()), "{deleted:#?}");
    eprintln!("trace {trace_id}: rdm.node_admin.node.delete.via-node-row node_id={} outcome=row-deleted config_from=writer-record (retained receipt, attempt 2)", exiting.node_id);
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
    let Members { failing, exiting, other_admin, final_admin, fp } = Members::new();
    let records = vec![failing.clone(), exiting.clone(), other_admin.clone(), final_admin.clone()];
    let handles = runtime_handles(&records, 42_000);
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.to_string())).collect(), ..Default::default() };
    *fleet.blind_once.lock().unwrap() = handles[&failing.node_id].pid;
    let me = RosterBirth::of(&final_admin).unwrap();
    let final_runtime = RuntimeProof::of(&handles[&final_admin.node_id].fact().unwrap());
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
            launcher: rdm_core::launch::Launcher { mesh: "mesh2".into(), name: final_admin.name.clone(), node_id: final_admin.node_id.clone(), incarnation: final_admin.incarnation_id.clone().unwrap() },
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
        let root = tracing::info_span!("cell.mesh-deleted-incomplete", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.recipe_step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let roster = accepted_roster(&builds, &key, STEP_ACCEPTED_ROSTER, fp.node_id.as_str(), &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let mp = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let tier = members_exited(&builds, &key, final_admin.node_id.as_str(), &mp, &me, final_runtime.clone(), &records, &[], |n: Node| {
            let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
            async move {
                let node_id = n.node_id.clone();
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: (*build_id).clone(), attempt: 1, node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
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
    let names: BTreeSet<String> = without_exit.iter().cloned().collect();
    assert_eq!((missing, names), (2, BTreeSet::from([&failing, &other_admin].map(|n| format!("{} {}", n.node_id, n.incarnation_id.as_ref().unwrap())))), "every roster birth without an Exited receipt is named, and only those: {without_exit:?}");
    assert!(reason.contains(failing.node_id.as_str()) && reason.contains(other_admin.node_id.as_str()) && reason.contains(STEP_OTHER_MEMBERS_EXITED), "the reason names what was read and found missing: {reason}");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let refused = named("rdm.node_admin.mesh.reject.via-incomplete-handoff");
    assert!(refused.iter().any(|s| s["attributes"]["without_exit"].as_str().is_some_and(|w| w.contains(failing.node_id.as_str()) && w.contains(other_admin.node_id.as_str()))), "{refused:?}");
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
    let Members { failing, exiting, other_admin, final_admin, fp } = Members::new();
    let records = vec![failing.clone(), exiting.clone(), other_admin.clone(), final_admin.clone()];
    let handles = runtime_handles(&records, 43_000);
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.to_string())).collect(), ..Default::default() };
    let last_record = final_admin.clone();
    let me = RosterBirth::of(&last_record).unwrap();
    let final_runtime = RuntimeProof::of(&handles[&final_admin.node_id].fact().unwrap());
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
            launcher: rdm_core::launch::Launcher { mesh: final_admin.mesh.clone(), name: final_admin.name.clone(), node_id: final_admin.node_id.clone(), incarnation: final_admin.incarnation_id.clone().unwrap() },
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
        let root = tracing::info_span!("cell.mesh-delete-completion", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.recipe_step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        accepted_roster(&builds, &key, STEP_ACCEPTED_ROSTER, fp.node_id.as_str(), &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let admitted = admitted_of(&builds.read_build(&build_id).await.unwrap().steps, &outer).unwrap().unwrap();
        let mp = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), &me, &final_runtime, || live_roster(&records)).await.unwrap();
        members_exited(&builds, &key, final_admin.node_id.as_str(), &mp, &me, final_runtime.clone(), &records, &[], |n: Node| {
            let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
            async move {
                let node_id = n.node_id.clone();
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: (*build_id).clone(), attempt: 1, node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        })
        .await
        .expect("every other member exits");
        // The owner's validated handoff, then mesh.deleted's completion: nothing of the final primary
        // is stopped.
        let facts = builds.facts().await.unwrap();
        let HandoffVerdict::Accept(manifest) = handoff_verdict(&facts, &key, &admitted, &manifest_reference(&build_id.as_str(), 1, &outer, false)) else { panic!("the handoff is accepted") };
        record_completion(&builds, &key, fp.node_id.as_str(), &manifest).await.expect("mesh.deleted's completion is recorded");
        let terminated_at_completion = fleet.terminated.lock().unwrap().clone();
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        let resume = owner_resume(&steps, &outer);
        let replay = replayed_handoff(&steps, &key, &last_record.node_id, &me.incarnation_id);
        let other = replayed_handoff(&steps, &key, &NodeId::mint(), "another-birth");
        let completions = steps.iter().filter(|r| r.step == STEP_MESH_DELETED).count();
        let indexed = builds.mesh_deleted_of(&last_record.node_id, last_record.incarnation_id.as_ref().unwrap()).await.unwrap();
        // The final primary's own node.delete, a separate Build: the ordinary workflow, live target.
        let req = rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(last_record).node_id), build_id: delete_build.clone(), attempt: 1, node: last_record.clone(), handle: handles[&final_admin.node_id].clone(), unreached: false };
        let mut fp_template = template.clone();
        fp_template.launcher = rdm_core::launch::Launcher { mesh: fp.mesh.clone(), name: fp.name.clone(), node_id: fp.node_id.clone(), incarnation: fp.incarnation_id.clone().unwrap() };
        let fp_pipeline = DeploymentPipeline { template: &fp_template, ..pipeline };
        fp_pipeline.delete_node(&req).await.expect("the final primary's node.delete completes");
        let terminal: TerminalReceipt = builds
            .read_build(&delete_build)
            .await
            .unwrap()
            .steps
            .into_iter()
            .find(|r| r.operation == req.operation && r.step == "TerminateRuntime")
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

    assert!(!terminated_at_completion.contains(&final_admin.node_id.to_string()), "mesh.delete stops nothing of its final primary: {terminated_at_completion:?}");
    assert_eq!(manifest.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(), BTreeSet::from([&failing, &exiting, &other_admin].map(|n| (n.node_id.to_string(), n.incarnation_id.as_ref().unwrap().to_string()))), "mesh.deleted covers every birth but the final primary");
    assert_eq!(resume, Ok(OwnerResume::Deleted(manifest.clone())), "a resumed owner reads the completion and calls nothing");
    assert_eq!(completion, manifest);
    assert_eq!(replay, Some(StatusReply::AlreadyApplied), "a repeated handoff of the final birth is AlreadyApplied");
    assert!(matches!(&other, Some(StatusReply::RejectedUnmatchedCompletion { field, .. }) if field == "final_primary"), "a handoff naming another birth is refused by name: {other:?}");
    assert_eq!(completions, 1, "the replay recorded no second completion receipt");
    assert_eq!(indexed, Some((build_id, 1, outer.clone())), "the completion index names the mesh.delete for the final birth");
    let all = proof.unwrap_or_else(|e| panic!("the final birth's own exited verdict completes the set: {e}"));
    assert_eq!(first_mismatch(&all, &all.roster, &me, true), None, "MeshLeft covers every accepted birth, the final primary included");
    assert_eq!(all.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(), BTreeSet::from([&failing, &exiting, &other_admin, &final_admin].map(|n| (n.node_id.to_string(), n.incarnation_id.as_ref().unwrap().to_string()))));
    assert!(recycled.as_ref().is_err_and(|e| e.starts_with("final_runtime")), "a receipt of another runtime proves nothing: {recycled:?}");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let recorded = named("rdm.node_admin.mesh.update.via-mesh-deleted-receipt");
    assert!(recorded.iter().any(|s| s["attributes"]["receipts"] == "3" && s["attributes"]["roster"] == "4" && s["attributes"]["final_primary_node_id"] == final_admin.node_id.as_str()), "{recorded:?}");
    let replayed = named("rdm.node_admin.mesh.resolve.via-handoff-replayed");
    assert!(replayed.iter().any(|s| s["attributes"]["outcome"] == "already-applied"), "{replayed:?}");
    eprintln!("trace {trace_id}: via-mesh-deleted-receipt receipts=3 roster=4, via-handoff-replayed already-applied, node.delete node_id={} TerminateRuntime then the MeshLeft proof set over 4 births", final_admin.node_id);
}

/// CONTRACT: R-MN1 with mesh-delete.md step 7 (the final primary's own node.delete, the
/// fabric-primary its executor, "as acting primary of a mesh that has no primary, writes its row
/// deleted"), R-NS1 item 2 (from the stop of a mesh's last admin the fabric-primary is the writer of
/// a mesh with no primary) and R-MD1 item 2 (mesh-left ensures the mesh row Dead). While the final
/// primary holds its role live, the fabric-primary writes none of mesh2's rows and a second claim of
/// path `mesh2.admin`, name `1` is refused `name-held`; once it is stopped (its role held) the
/// fabric-primary writes its row Deleted, node id and config kept, as its node.delete's NodeDeleted
/// step does; mesh-left puts the mesh row Dead as acting primary when no Dead row is held; and only
/// then a new birth claims path `mesh2.admin`, name `1` over the Deleted row (`row-seeded`, seeded from it), its
/// row carrying its create spec's config, here none (Q166: a plain create's config is its spec's; only a
/// recovery's and a successor's claim inherit).
#[test]
fn the_final_primarys_node_delete_writes_its_row_deleted_once_it_is_stopped_and_only_then_the_name_is_claimed_again() {
    let cap = capture();
    let fp = {
        let mut n = node_at("mesh1.admin", "1", "mesh1", rdm_core::NodeKind::NodeAdmin);
        n.is_fabric_primary = true;
        n
    };
    let last = node_at("mesh2.admin", "1", "mesh2", rdm_core::NodeKind::NodeAdmin);
    let storage = Arc::new(MemoryNodesStorage::default());
    let status = MemoryMeshStorage::default();
    let declared = Mutex::new(Declared::default());
    let view = Arc::new(RwLock::new(writer_view(std::slice::from_ref(&last), None, Some(fp.clone()))));
    let rows = NodeRows::new(fp.name.clone(), fp.node_id.clone(), view.clone(), storage.clone());
    let mesh_id = MeshId::mint();
    let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: BuildId::mint(), attempt: 1, operation: operation(&mesh_id) };
    let deleting = rdm_core::OperationId::node("node.delete", &last.node_id);
    let (second, live, stopped, after, row, statuses, again, trace_id) = cap.run(async {
        let root = tracing::info_span!("cell.final-primary-row-deleted", operation = %key.operation.action, operation_subject = ?key.operation.subject, operation_step = ?key.operation.recipe_step, operation_occurrence = ?key.operation.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        // The fabric-primary claimed the mesh's first admin's name when it created it, before the
        // mesh had a primary.
        rows_written(&storage, &rows, std::slice::from_ref(&last)).await;
        // A live path `mesh2.admin`, name `1` holds its name: a second birth's claim is refused.
        let second = rows.claim(&last.name, &last.mesh.clone(), last.kind, &NodeId::mint(), &rdm_core::OperationId::location("node.create", &rdm_core::PathName::new("mesh2.admin", "1").unwrap()), None, None, ClaimFor::Create).await;
        // In its role and live: the fabric-primary is not the writer of mesh2's rows.
        view.write().await.nodes.iter_mut().filter(|n| n.node_id == last.node_id).for_each(|n| n.is_primary = true);
        let live = rows.deleted(&last, &deleting).await;
        // Stopped, its role still held (R-EL1): no primary for the writer rule (R-NS1 item 2).
        view.write().await.nodes.iter_mut().filter(|n| n.node_id == last.node_id).for_each(|n| {
            n.status = NodeStatus::Leaving;
            n.stopped = true;
        });
        let stopped = rows.deleted(&last, &deleting).await;
        let exited = rdm_admin::deployment::pipeline::TerminalReceipt { node: last.name.clone(), node_id: last.node_id.to_string(), incarnation_id: last.incarnation_id.clone().unwrap().to_string(), runtime: None, exit_code: Some(0) , verdict: None, data_dir: None };
        let after = mesh_left_rows(Some(&rows), &status, &Default::default(), &declared, fp.node_id.as_str(), &key, "mesh2", std::slice::from_ref(&exited)).await;
        let row = rdm_admin::storage::NodesStorage::node(storage.as_ref(), &last.node_id).await.unwrap();
        let statuses = status.meshes().await.unwrap().into_iter().filter_map(|r| Some((r.mesh_id.clone(), r.state()?))).collect::<std::collections::HashMap<_, _>>();
        let dead_held = status.mesh(&mesh_id.clone()).await.unwrap().is_some_and(|r| r.states.contains(&MeshState::Dead));
        // mesh2 created again: its first admin's claim wins over the Deleted row.
        let again = rows.claim(&last.name, &last.mesh.clone(), last.kind, &NodeId::mint(), &rdm_core::OperationId::location("node.create", &rdm_core::PathName::new("mesh2.admin", "1").unwrap()), None, None, ClaimFor::Create).await;
        (second, live, stopped, after, row, (statuses, dead_held), again, trace_id)
    });
    let (statuses, dead_held) = statuses;
    let spans = cap.spans();
    let dir = artifact_dir().join("final-primary-row-deleted");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());

    assert!(matches!(&second, Err(RowRefused::NameHeld(h)) if h.by == last.node_id), "a live path `mesh2.admin`, name `1` refuses a second claim: {second:?}");
    assert!(live.is_err(), "while the final primary holds its role live the fabric-primary writes none of mesh2's rows: {live:?}");
    stopped.unwrap_or_else(|e| panic!("once stopped, the fabric-primary writes the final primary's row Deleted as acting primary: {e}"));
    after.unwrap_or_else(|e| panic!("mesh-left writes its rows: {e}"));
    let row = row.expect("the final primary's row stays, readable by its node id");
    assert_eq!((row.status, &row.node_id, &row.config), (NodeStatus::Deleted, &last.node_id, &tier_of(last.node_id.as_str())), "{row:?}");
    assert!(dead_held, "the mesh's status is Dead on mesh2 (mesh.storage, R-MD1)");
    assert_eq!(statuses.get(&mesh_id), Some(&MeshState::Dead), "mesh2's one status fact is its Dead, on mesh2: {statuses:?}");
    assert_eq!(declared.lock().unwrap().meshes.get(&mesh_id), Some(&MeshState::Dead), "and held as applied");
    match &again {
        Ok(Claimed::Written { row, seeded_from }) => {
            assert_eq!(seeded_from.as_ref(), None, "a plain create names no seed: the Deleted row's location is not provenance");
            assert_eq!(row.config, Default::default(), "its create spec's config, none: a plain create inherits no config (Q166)");
        }
        other => panic!("the recreated mesh's admin claims path `mesh2.admin`, name `1` over the Deleted row: {other:?}"),
    }
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let deleted: Vec<String> = named("rdm.node_admin.node.delete.via-node-row").iter().filter(|s| s["attributes"]["subject_path"] == last.name.path && s["attributes"]["subject_name"] == last.name.name).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect();
    assert_eq!(deleted, ["not-mesh-primary", "row-deleted"], "refused while the final primary is live and primary, written once it is stopped: {deleted:?}");
    let dead = named("rdm.node_admin.mesh.update.via-dead-row");
    assert!(dead.iter().any(|s| s["attributes"]["mesh_id"] == mesh_id.as_str() && s["attributes"]["outcome"] == "applied" && s["attributes"]["writer"] == "acting-primary"), "no Dead row held: the fabric-primary writes Dead as acting primary: {dead:#?}");
    let claims: Vec<String> = named("rdm.node_admin.node.create.via-node-row").iter().filter(|s| s["attributes"]["subject_path"] == last.name.path && s["attributes"]["subject_name"] == last.name.name).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect();
    assert_eq!(claims, ["row-held-by-node", "name-held", "row-written"], "a plain create's claim names no seed: {claims:?}");
    eprintln!("trace {trace_id}: delete.via-node-row not-mesh-primary then row-deleted, mesh.update.via-dead-row applied acting-primary, via-node-row claim row-seeded seeded_from={}", last.node_id);
}


/// The exact Exited receipt the provider's inspection of `b`'s runtime `runtime` proves.
fn exited(b: &RosterBirth, runtime: &RuntimeProof) -> TerminalReceipt {
    TerminalReceipt { node: b.node.clone(), node_id: b.node_id.clone(), incarnation_id: b.incarnation_id.clone(), runtime: Some(runtime.clone()), exit_code: Some(137) , verdict: None, data_dir: None }
}

fn step_facts(facts: &[BuildFact]) -> Vec<BuildStepReceipt> {
    facts.iter().filter_map(|f| if let BuildFact::Step(r) = f { Some(r.clone()) } else { None }).collect()
}

/// CONTRACT: a mesh leave whose final primary is lost mid-leave resumes under a named amendment and
/// completes over both primaries (R11; mesh-delete.md, "Resume after the primary's loss"). Attempt 1:
/// the final primary path `mesh2.admin`, name `1` runs the ordinary tier, path `mesh2.rpc`, name `2` exits and path `mesh2.rpc`, name `1`'s
/// exit cannot be proven, then the final primary is lost. The fabric-primary records the lost
/// primary's exact Exited first, then `RosterAmended { path `mesh2.admin`, name `1` -> path `mesh2.admin`, name `2` }` (the
/// surviving admin the election in its role); the identical amendment replays `AlreadyApplied` and a
/// second amendment for the same predecessor is refused by name. Attempt 2: the amended primary runs
/// only path `mesh2.rpc`, name `1` (path `mesh2.rpc`, name `2` and the lost primary keep their receipts, nobody re-runs them),
/// its manifest is accepted against the amended admission, a stale manifest naming the predecessor as
/// final primary is refused by name, mesh.deleted is recorded from it, and once the amended primary's
/// own node.delete proves its exit the MeshLeft proof set covers every accepted birth, both primaries
/// included.
#[test]
fn a_primary_lost_mid_leave_is_replaced_by_a_named_amendment_and_the_leave_completes_over_both_primaries() {
    let fp_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-deleted-amended-{}", NodeId::mint()));
    let Members { failing, exiting, other_admin, final_admin, .. } = Members::new();
    let records = vec![failing.clone(), exiting.clone(), other_admin.clone(), final_admin.clone()];
    let handles = runtime_handles(&records, 44_000);
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.to_string())).collect(), ..Default::default() };
    *fleet.blind_once.lock().unwrap() = handles[&failing.node_id].pid;
    let birth = |n: &Node| RosterBirth::of(n).unwrap();
    let (lost, survivor) = (birth(&final_admin), birth(&other_admin));
    let (lost_runtime, survivor_runtime) = (RuntimeProof::of(&handles[&final_admin.node_id].fact().unwrap()), RuntimeProof::of(&handles[&other_admin.node_id].fact().unwrap()));
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
            launcher: rdm_core::launch::Launcher { mesh: "mesh2".into(), name: final_admin.name.clone(), node_id: final_admin.node_id.clone(), incarnation: final_admin.incarnation_id.clone().unwrap() },
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
        let mut amended_template = template.clone();
        amended_template.launcher = rdm_core::launch::Launcher { mesh: other_admin.mesh.clone(), name: other_admin.name.clone(), node_id: other_admin.node_id.clone(), incarnation: other_admin.incarnation_id.clone().unwrap() };
        let amended_pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &amended_template, timeouts: Timeouts::default() };
        let shutdown = |attempt: u32| {
            let (pipeline, handles, calls, build_id) = (if attempt == 1 { &pipeline } else { &amended_pipeline }, &handles, &calls, &build_id);
            move |n: Node| async move {
                let node_id = n.node_id.clone();
                calls.lock().unwrap().push((attempt, node_id.to_string()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: (*build_id).clone(), attempt, node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-deleted-amended", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.recipe_step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Attempt 1: both sides capture the roster; the ordinary tier is incomplete; the primary is lost.
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, fp_birth.node_id.as_str(), &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let mp1 = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key(1), final_admin.node_id.as_str(), &mp1, &lost, lost_runtime.clone(), &records, &[], shutdown(1)).await;
        assert!(first.is_err(), "attempt 1's ordinary tier is incomplete");

        // The resume, attempt 2: the lost primary's node.delete as a crashed target first, its
        // TerminateRuntime the roster receipt, then the amendment.
        node_delete_receipts(&builds, &key(2), &fp_birth, &lost, &exited(&lost, &lost_runtime)).await;
        let applied = amend_roster(&builds, &key(2), fp_birth.node_id.as_str(), &amendment).await;
        let replayed = amend_roster(&builds, &key(2), fp_birth.node_id.as_str(), &amendment).await;
        let second_amendment = amend_roster(&builds, &key(2), fp_birth.node_id.as_str(), &RosterAmended { final_primary: birth(&exiting), ..amendment.clone() }).await;
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        let admitted = admitted_of(&steps, &outer).expect("a roster is recorded").expect("the admission reads");

        // The amended primary's member workflows, over the leaving roster plus the amendments.
        let leaving = accepted_row(&steps, &outer, STEP_LEAVING_ROSTER).unwrap().unwrap().roster;
        let mp2 = amended_roster(&leaving, &steps, &outer).unwrap();
        let records2: Vec<Node> = view.holds(&records);
        let manifest = members_exited(&builds, &key(2), other_admin.node_id.as_str(), &mp2, &survivor, survivor_runtime.clone(), &records2, &[], shutdown(2)).await.expect("the amended primary's member workflows complete");
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
        record_completion(&builds, &key(2), fp_birth.node_id.as_str(), &completed).await.expect("mesh.deleted is recorded");
        let delete_build = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: delete_build.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let last = records2.iter().find(|n| n.node_id == other_admin.node_id).cloned().unwrap();
        let mut fp_template = template.clone();
        fp_template.launcher = rdm_core::launch::Launcher { mesh: "mesh1".into(), name: PathName::new("mesh1.admin", "1").unwrap(), node_id: fp_birth.node_id.clone(), incarnation: fp_birth.incarnation.clone() };
        let fp_pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &fp_template, timeouts: Timeouts::default() };
        let terminal = fp_pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(last).node_id), build_id: delete_build, attempt: 1, node: last, handle: handles[&other_admin.node_id].clone(), unreached: false }).await.expect("the amended primary's node.delete proves its exit");
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
    assert!(refused.contains("second amendment") && refused.contains(final_admin.node_id.as_str()), "the refusal names the predecessor: {refused}");
    assert_eq!(steps.iter().filter(|r| r.step == STEP_ROSTER_AMENDED).count(), 1, "one RosterAmended receipt for one predecessor");
    assert_eq!(admitted.final_primary, survivor, "the amended admission names the surviving admin as final primary");
    assert_eq!(admitted.final_runtime, survivor_runtime);
    assert_eq!(admitted.amendments, vec![amendment.clone()]);
    let ran: Vec<(u32, String)> = calls.lock().unwrap().clone();
    assert_eq!(ran.iter().filter(|(a, _)| *a == 2).map(|(_, n)| n.clone()).collect::<Vec<_>>(), vec![failing.node_id.to_string()], "the amended primary runs only the member without an Exited receipt: {ran:?}");
    assert!(matches!(verdict, HandoffVerdict::Accept(_)), "the amended primary's manifest is accepted: {verdict:?}");
    match &stale {
        HandoffVerdict::Unmatched { field, expected, reported } => {
            assert_eq!(field, "final_primary", "{stale:?}");
            assert!(expected.contains(other_admin.node_id.as_str()) && reported.contains(final_admin.node_id.as_str()) && reported.contains("predecessor"), "the refusal names both births: {stale:?}");
        }
        other => panic!("a manifest naming the predecessor is refused by name, not {other:?}"),
    }
    let resume = resume.unwrap_or_else(|e| panic!("the resume reads the receipts: {e}"));
    assert!(matches!(&resume, OwnerResume::Deleted(m) if m.final_primary == survivor), "a resumed owner reads the amended primary's mesh.deleted and calls nothing: {resume:?}");
    let all = mesh_left.unwrap_or_else(|e| panic!("the amended primary's own node.delete completes the MeshLeft proof set: {e}"));
    assert_eq!(all.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(), records.iter().map(|n| (n.node_id.to_string(), n.incarnation_id.as_ref().unwrap().to_string())).collect::<BTreeSet<_>>(), "MeshLeft covers both primaries");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.mesh.update.via-roster-amended").iter().any(|s| s["attributes"]["predecessor"].as_str().is_some_and(|p| p.contains(final_admin.node_id.as_str())) && s["attributes"]["final_primary"].as_str().is_some_and(|p| p.contains(other_admin.node_id.as_str())) && s["attributes"]["outcome"] == "applied"));
    assert!(named("rdm.node_admin.mesh.reject.via-second-amendment").iter().any(|s| s["attributes"]["predecessor"].as_str().is_some_and(|p| p.contains(final_admin.node_id.as_str()))));
    assert!(named("rdm.node_admin.mesh.resolve.via-retained-receipts").iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["retained"] == "2" && s["attributes"]["remaining"] == "1"));
}

/// CONTRACT: mesh-delete.md draws each member's end at the mesh-primary as "member row deleted;
/// node.deleted", then "mesh topology: NodeDeleted in MeshTopology.deleted", and its resume keeps a member's
/// finished delete ("Resume after the primary's loss" step 3: "Completed member receipts stand").
/// Attempt 1, under the final primary path `mesh2.admin`, name `1`: path `mesh2.rpc`, name `2`'s whole delete runs to
/// `Complete` and the lost primary publishes its NodeDeleted, which no other admin hears (the
/// primary is lost before its frame lands, so its publications are its own); path `mesh2.rpc`, name `1`'s exit
/// cannot be proven, so the ordinary tier is incomplete. The resume runs the lost primary's
/// node.delete as a crashed target and amends the roster to path `mesh2.admin`, name `2`. Attempt 2, under the
/// amended primary: path `mesh2.rpc`, name `2` keeps its receipts and is not run again (no step of its delete
/// executes, its runtime is terminated once), and nothing of it is written or published again: its
/// row `Deleted` and its NodeDeleted were its node.delete's, once (node-delete.md steps 3-4), and a
/// peer that never heard that NodeDeleted keeps the birth until mesh-left; path `mesh2.rpc`, name `1`'s delete
/// runs and publishes its own. The lost primary's NodeDeleted is its own node.delete's, the
/// fabric-primary's, never the amended primary's.
#[test]
fn a_member_whose_delete_finished_under_the_lost_primary_is_neither_run_nor_published_again() {
    let fp_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-deleted-finished-under-lost-{}", NodeId::mint()));
    let Members { failing, exiting, other_admin, final_admin, .. } = Members::new();
    let records = vec![failing.clone(), exiting.clone(), other_admin.clone(), final_admin.clone()];
    let handles = runtime_handles(&records, 44_000);
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.to_string())).collect(), ..Default::default() };
    *fleet.blind_once.lock().unwrap() = handles[&failing.node_id].pid;
    let birth = |n: &Node| RosterBirth::of(n).unwrap();
    let (lost, survivor) = (birth(&final_admin), birth(&other_admin));
    let (lost_runtime, survivor_runtime) = (RuntimeProof::of(&handles[&final_admin.node_id].fact().unwrap()), RuntimeProof::of(&handles[&other_admin.node_id].fact().unwrap()));
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
            launcher: rdm_core::launch::Launcher { mesh: "mesh2".into(), name: final_admin.name.clone(), node_id: final_admin.node_id.clone(), incarnation: final_admin.incarnation_id.clone().unwrap() },
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
        let mut amended_template = template.clone();
        amended_template.launcher = rdm_core::launch::Launcher { mesh: other_admin.mesh.clone(), name: other_admin.name.clone(), node_id: other_admin.node_id.clone(), incarnation: other_admin.incarnation_id.clone().unwrap() };
        let amended_pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &amended_published, builds: &builds, template: &amended_template, timeouts: Timeouts::default() };
        let shutdown_1 = |n: Node| {
            let (pipeline, handles, calls, build_id) = (&lost_pipeline, &handles, &calls, &build_id);
            async move {
                let node_id = n.node_id.clone();
                calls.lock().unwrap().push((1, node_id.to_string()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: (*build_id).clone(), attempt: 1, node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let shutdown_2 = |n: Node| {
            let (pipeline, handles, calls, build_id) = (&amended_pipeline, &handles, &calls, &build_id);
            async move {
                let node_id = n.node_id.clone();
                calls.lock().unwrap().push((2, node_id.to_string()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: (*build_id).clone(), attempt: 2, node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-deleted-finished-under-lost", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.recipe_step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Attempt 1, the lost primary's: rpc.2's delete finishes, rpc.1's exit is unproven.
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, fp_birth.node_id.as_str(), &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let mp1 = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key(1), final_admin.node_id.as_str(), &mp1, &lost, lost_runtime.clone(), &records, &[], shutdown_1).await;

        // The resume: the lost primary's node.delete, then the amendment, then the amended primary's tiers.
        node_delete_receipts(&builds, &key(2), &fp_birth, &lost, &exited(&lost, &lost_runtime)).await;
        amend_roster(&builds, &key(2), fp_birth.node_id.as_str(), &amendment).await.expect("the amendment is recorded");
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        let leaving = accepted_row(&steps, &outer, STEP_LEAVING_ROSTER).unwrap().unwrap().roster;
        let mp2 = amended_roster(&leaving, &steps, &outer).unwrap();
        let records2: Vec<Node> = view.holds(&records);
        let second = members_exited(&builds, &key(2), other_admin.node_id.as_str(), &mp2, &survivor, survivor_runtime.clone(), &records2, &[], shutdown_2).await;
        (first, second, builds.read_build(&build_id).await.unwrap().steps, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("finished_under_lost");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    let err = first.expect_err("attempt 1's ordinary tier is incomplete: rpc.1's exit is unproven");
    assert!(err.contains("tier ordinary-nodes is incomplete") && err.contains(failing.node_id.as_str()), "{err}");
    second.unwrap_or_else(|e| panic!("the amended primary's member workflows complete: {e}"));
    let calls = calls.into_inner().unwrap();
    let ran = |attempt: u32| calls.iter().filter(|(a, _)| *a == attempt).map(|(_, n)| n.clone()).collect::<BTreeSet<_>>();
    assert_eq!(ran(1), BTreeSet::from([failing.node_id.to_string(), exiting.node_id.to_string()]), "attempt 1 ran the ordinary tier");
    assert_eq!(ran(2), BTreeSet::from([failing.node_id.to_string()]), "the amended primary runs only rpc.1: rpc.2's delete has every step's receipt and is not run again");
    let of = |node: &NodeId, step: &str| steps.iter().filter(|r| r.operation == rdm_core::OperationId::node("node.delete", node) && r.step == step && r.outcome == rdm_admin::build_state::StepOutcome::Complete).map(|r| r.attempt).collect::<Vec<_>>();
    assert_eq!((of(&exiting.node_id, "NodeDeleted"), of(&exiting.node_id, "Complete")), (vec![1], vec![1]), "rpc.2's delete finished under the lost primary, each step receipted once");
    assert_eq!(fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == exiting.node_id.as_str()).count(), 1, "rpc.2's runtime was terminated once, in attempt 1");
    let lost_ops = lost_published.0.into_inner().unwrap();
    assert_eq!(lost_ops.iter().map(|op| op.node_id.to_string()).collect::<Vec<_>>(), vec![exiting.node_id.to_string()], "the lost primary published rpc.2's NodeDeleted, which the amended primary never heard");
    let receipted: LifecycleOp = steps
        .iter()
        .find(|r| r.operation == rdm_core::OperationId::node("node.delete", &exiting.node_id) && r.step == "NodeDeleted")
        .and_then(|r| r.output.clone())
        .map(|v| serde_json::from_value(v).expect("a NodeDeleted receipt holds its op"))
        .expect("rpc.2's NodeDeleted is receipted");
    assert_eq!((receipted.node_id.to_string(), receipted.attempt), (exiting.node_id.to_string(), 1), "rpc.2's NodeDeleted receipt is the lost primary's, attempt 1");
    let amended_ops = amended_published.0.into_inner().unwrap();
    let names = amended_ops.iter().map(|op| op.node_id.to_string()).collect::<BTreeSet<_>>();
    assert_eq!(names, BTreeSet::from([failing.node_id.to_string()]), "the amended primary publishes the NodeDeleted of the one delete it ran, never again rpc.2's (its node.delete's, once) and never the lost primary's (its own node.delete's, the fabric-primary's): {amended_ops:?}");

    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.deployment.update.via-step").iter().all(|s| !(s["attributes"]["node_path"] == exiting.name.path && s["attributes"]["node_name"] == exiting.name.name && s["attributes"]["attempt"] == "2")), "no step of rpc.2's delete runs in attempt 2");
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
    let fp_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let cap = capture();
    let Members { failing, exiting, other_admin, final_admin, .. } = Members::new();
    let records = vec![failing.clone(), exiting.clone(), other_admin.clone(), final_admin.clone()];
    let birth = |n: &Node| RosterBirth::of(n).unwrap();
    let (lost, survivor) = (birth(&final_admin), birth(&other_admin));
    let lost_runtime = RuntimeProof::of(&runtime_handles(std::slice::from_ref(&final_admin), 45_000)[&final_admin.node_id].fact().unwrap());
    let recycled = RuntimeProof::of(&runtime_handles(std::slice::from_ref(&final_admin), 45_001)[&final_admin.node_id].fact().unwrap());
    let amendment = RosterAmended { predecessor: lost.clone(), final_primary: survivor.clone(), final_runtime: RuntimeProof::of(&runtime_handles(std::slice::from_ref(&other_admin), 45_002)[&other_admin.node_id].fact().unwrap()) };
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
        let root = tracing::info_span!("cell.mesh-deleted-unproven", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.recipe_step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, fp_birth.node_id.as_str(), &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let unproven = amend_roster(&builds, &key(2), fp_birth.node_id.as_str(), &amendment).await;
        // A node.delete receipt of the lost primary's exact birth naming another runtime (a recycled pid).
        node_delete_receipts(&builds, &key(2), &fp_birth, &lost, &exited(&lost, &recycled)).await;
        let recycled_exit = amend_roster(&builds, &key(2), fp_birth.node_id.as_str(), &amendment).await;
        (unproven, recycled_exit, builds.read_build(&build_id).await.unwrap().steps, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("unproven");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());

    let refused = unproven.expect_err("an unproven predecessor refuses the amendment");
    assert!(refused.contains(final_admin.node_id.as_str()) && refused.contains("no exact Exited"), "the refusal names the unproven predecessor: {refused}");
    let recycled_refused = recycled_exit.expect_err("an Exited of another runtime proves nothing");
    assert!(recycled_refused.contains("another runtime"), "{recycled_refused}");
    assert!(!steps.iter().any(|r| r.step == STEP_ROSTER_AMENDED), "nothing is amended");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.mesh.reject.via-unproven-primary").iter().any(|s| s["attributes"]["predecessor"].as_str().is_some_and(|p| p.contains(final_admin.node_id.as_str()))));
}

/// CONTRACT: a repeated loss records one amendment per predecessor and the chain is explicit (R11
/// step 8). path `mesh2.admin`, name `1` is lost, path `mesh2.admin`, name `2` is amended in; path `mesh2.admin`, name `2` is lost too with
/// no admin left, and the one node-admin the fabric-primary created, path `mesh2.admin`, name `3` (a new birth,
/// not in the accepted roster), is amended in. The admission is the accepted roster plus
/// path `mesh2.admin`, name `3`, its final primary path `mesh2.admin`, name `3`; the leaving primary's roster is extended the
/// same way; a hand-off manifest that omits the amendment (its roster lacks path `mesh2.admin`, name `3`) is
/// refused by name.
#[test]
fn a_repeated_loss_records_one_amendment_per_predecessor_and_a_hand_off_omitting_one_is_refused() {
    let fp_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let cap = capture();
    let Members { failing, exiting, other_admin, final_admin, .. } = Members::new();
    let records = vec![failing.clone(), exiting.clone(), other_admin.clone(), final_admin.clone()];
    let fresh = node_at("mesh2.admin", "3", "mesh2", rdm_core::NodeKind::NodeAdmin);
    let birth = |n: &Node| RosterBirth::of(n).unwrap();
    
    let mut runtime_nodes = records.clone();
    runtime_nodes.push(fresh.clone());
    let handles = runtime_handles(&runtime_nodes, 46_000);
    let runtime = |node: &Node| RuntimeProof::of(&handles[&node.node_id].fact().unwrap());
    let (first, second, third) = (birth(&final_admin), birth(&other_admin), birth(&fresh));
    let a1 = RosterAmended { predecessor: first.clone(), final_primary: second.clone(), final_runtime: runtime(&other_admin) };
    let a2 = RosterAmended { predecessor: second.clone(), final_primary: third.clone(), final_runtime: runtime(&fresh) };
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
        let root = tracing::info_span!("cell.mesh-deleted-repeated-loss", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.recipe_step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, fp_birth.node_id.as_str(), &first, &runtime(&final_admin), || live_roster(&records)).await.unwrap();
        let leaving = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), &first, &runtime(&final_admin), || live_roster(&records)).await.unwrap();
        node_delete_receipts(&builds, &key(2), &fp_birth, &first, &exited(&first, &runtime(&final_admin))).await;
        let r1 = amend_roster(&builds, &key(2), fp_birth.node_id.as_str(), &a1).await;
        node_delete_receipts(&builds, &key(3), &fp_birth, &second, &exited(&second, &runtime(&other_admin))).await;
        let r2 = amend_roster(&builds, &key(3), fp_birth.node_id.as_str(), &a2).await;
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        let admitted = admitted_of(&steps, &outer).expect("a roster is recorded").expect("the admission reads");
        let mp = amended_roster(&leaving, &steps, &outer).unwrap();
        // A hand-off manifest over the accepted roster alone: it omits the amendment.
        let receipts = vec![exited(&birth(&failing), &runtime(&failing)), exited(&birth(&exiting), &runtime(&exiting)), exited(&first, &runtime(&final_admin)), exited(&second, &runtime(&other_admin))];
        let omitting = ExitManifest { mesh_id: mesh_id.to_string(), roster: leaving.clone(), final_primary: third.clone(), final_runtime: Some(a2.final_runtime.clone()), receipts };
        let facts = vec![BuildFact::Step(BuildStepReceipt { mesh: None, build_id: build_id.clone(), attempt: 3, operation: outer.clone(), step: STEP_OTHER_MEMBERS_EXITED.into(), outcome: rdm_admin::build_state::StepOutcome::Complete, output: serde_json::to_value(&omitting).ok(), executor: Some(executor_birth(&third)) })];
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
            assert!(expected.contains(fresh.node_id.as_str()) && reported.contains("omits"), "the refusal names the amended birth: {omitted:?}");
        }
        other => panic!("a manifest omitting the amendment is refused by name, not {other:?}"),
    }
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert_eq!(named("rdm.node_admin.mesh.update.via-roster-amended").iter().filter(|s| s["attributes"]["outcome"] == "applied").count(), 2);
}

/// CONTRACT: the amended final primary admits `DeleteMesh` from the owner's Build facts when gossip has
/// not yet brought it the amendment (R11 step 4; mesh-delete.md, "Resume after the primary's loss",
/// step 3: gossip is never authority). The fabric-primary recorded the lost primary's exact Exited
/// and `RosterAmended { path `mesh2.admin`, name `1` -> path `mesh2.admin`, name `2` }` in its own Build facts; path `mesh2.admin`, name `2`'s
/// own facts hold only the roster captured in attempt 1. Its admission reads the owner's facts: the
/// roster admits it, and the lost primary's exit receipt read from the owner keeps it out of the
/// node-admin tier, so no member workflow runs and the manifest covers both primaries.
#[test]
fn an_amended_primary_admits_the_leave_from_the_owners_facts_before_gossip_brings_the_amendment() {
    let fp_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let cap = capture();
    let Members { other_admin, final_admin, .. } = Members::new();
    let records = vec![other_admin.clone(), final_admin.clone()];
    let handles = runtime_handles(&records, 46_000);
    let birth = |n: &Node| RosterBirth::of(n).unwrap();
    let (lost, survivor) = (birth(&final_admin), birth(&other_admin));
    let (lost_runtime, survivor_runtime) = (RuntimeProof::of(&handles[&final_admin.node_id].fact().unwrap()), RuntimeProof::of(&handles[&other_admin.node_id].fact().unwrap()));
    let ran: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let (out, trace_id) = cap.run(async {
        let (owner, own) = (WireBuilds(MemoryBuildStateAdapter::new()), WireBuilds(MemoryBuildStateAdapter::new()));
        let build_id = BuildId::mint();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: outer.clone() };
        for b in [&owner, &own] {
            b.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
            accepted_roster(b, &key(1), STEP_ACCEPTED_ROSTER, fp_birth.node_id.as_str(), &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
            accepted_roster(b, &key(1), STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        }
        let root = tracing::info_span!("cell.mesh-deleted-owner-amendment", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.recipe_step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        // The fabric-primary's resume, in its own facts only: gossip has not delivered it.
        node_delete_receipts(&owner, &key(2), &fp_birth, &lost, &exited(&lost, &lost_runtime)).await;
        amend_roster(&owner, &key(2), fp_birth.node_id.as_str(), &RosterAmended { predecessor: lost.clone(), final_primary: survivor.clone(), final_runtime: survivor_runtime.clone() }).await.expect("the amendment is recorded");
        // path `mesh2.admin`, name `2` admits DeleteMesh.
        let steps = own.read_build(&build_id).await.unwrap().steps;
        let leaving = accepted_row(&steps, &outer, STEP_LEAVING_ROSTER).unwrap().unwrap().roster;
        let owner_facts = owner.facts().await.unwrap();
        let (roster, known) = admission_roster(&leaving, &steps, &outer, &survivor, || async { Ok(owner_facts) }).await.expect("the admission reads");
        // Once its own Build facts hold the amendment naming it (gossip delivered it), the admission
        // reads the owner for nothing and names the amended admission from its own facts.
        let amended_steps = owner.read_build(&build_id).await.unwrap().steps;
        let (from_own, none) = admission_roster(&leaving, &amended_steps, &outer, &survivor, || async { Err::<Vec<BuildFact>, String>("the owner is read although this admin's own facts name it final primary".into()) }).await.expect("the admission reads its own facts");
        assert_eq!((sorted(from_own), none.len()), (sorted(roster.clone()), 0), "its own facts admit the same amended roster");
        let manifest = members_exited(&own, &key(2), other_admin.node_id.as_str(), &roster, &survivor, survivor_runtime.clone(), &records, &known, |n: Node| {
            let ran = &ran;
            async move {
                ran.lock().unwrap().push(n.node_id.to_string());
                Err::<TerminalReceipt, String>(format!("{} is run again", n.node_id))
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
    assert_eq!(manifest.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(), BTreeSet::from([(lost.node_id.clone(), lost.incarnation_id.clone())]), "the lost primary's exit receipt is in the manifest");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.mesh.resolve.via-owner-build-facts").iter().any(|s| s["attributes"]["amendments"] == "1" && s["attributes"]["me"].as_str().is_some_and(|m| m.contains(other_admin.node_id.as_str()))), "the admission read the owner's Build facts");
    assert!(named("rdm.node_admin.mesh.resolve.via-own-build-facts").iter().any(|s| s["attributes"]["amendments"] == "1" && s["attributes"]["me"].as_str().is_some_and(|m| m.contains(other_admin.node_id.as_str()))), "the admission from its own facts names the amendment that admits it");
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

/// The amended primary path `mesh2.admin`, name `2` admits the leave from its own facts, which hold the
/// fabric-primary's amendment and, of the lost primary's node.delete, only the steps
/// `keep_of_lost_delete`; the owner holds them all.
fn own_amendment_without_the_delete(case: &str, keep_of_lost_delete: &[&str]) {
    let fp_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let cap = capture();
    let Members { other_admin, final_admin, .. } = Members::new();
    let records = vec![other_admin.clone(), final_admin.clone()];
    let handles = runtime_handles(&records, 47_000);
    let birth = |n: &Node| RosterBirth::of(n).unwrap();
    let (lost, survivor) = (birth(&final_admin), birth(&other_admin));
    let (lost_runtime, survivor_runtime) = (RuntimeProof::of(&handles[&final_admin.node_id].fact().unwrap()), RuntimeProof::of(&handles[&other_admin.node_id].fact().unwrap()));
    let ran: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let (out, read_owner, trace_id) = cap.run(async {
        let (owner, own) = (WireBuilds(MemoryBuildStateAdapter::new()), WireBuilds(MemoryBuildStateAdapter::new()));
        let build_id = BuildId::mint();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: outer.clone() };
        for b in [&owner, &own] {
            b.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
            accepted_roster(b, &key(1), STEP_ACCEPTED_ROSTER, fp_birth.node_id.as_str(), &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
            accepted_roster(b, &key(1), STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        }
        let root = tracing::info_span!("cell.mesh-deleted-own-amendment-without-the-delete", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.recipe_step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        // The fabric-primary's resume: the lost primary's node.delete, then the amendment.
        node_delete_receipts(&owner, &key(2), &fp_birth, &lost, &exited(&lost, &lost_runtime)).await;
        amend_roster(&owner, &key(2), fp_birth.node_id.as_str(), &RosterAmended { predecessor: lost.clone(), final_primary: survivor.clone(), final_runtime: survivor_runtime.clone() }).await.expect("the amendment is recorded");
        // path `mesh2.admin`, name `2`'s own facts: gossip delivered the amendment, not the lost primary's delete.
        let lost_delete = rdm_core::OperationId::node("node.delete", &final_admin.node_id);
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
        let manifest = members_exited(&own, &key(3), other_admin.node_id.as_str(), &roster, &survivor, survivor_runtime.clone(), &records, &known, |n: Node| {
            let ran = &ran;
            async move {
                ran.lock().unwrap().push(n.node_id.to_string());
                Err::<TerminalReceipt, String>(format!("{} is run again", n.node_id))
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
    assert_eq!(manifest.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(), BTreeSet::from([(lost.node_id.clone(), lost.incarnation_id.clone())]), "the lost primary's exit receipt is in the manifest");
}

/// CONTRACT: the fabric-primary's own resume branch reads the lost final primary's runtime from its
/// durable runtime row, never from the view (R11 steps 2-3; mesh-delete.md, "Resume after the
/// primary's loss", steps 1-2). No topology view is consulted: the lost path `mesh2.admin`, name `1` has left
/// every projection. With no runtime row of that birth the resume is refused by name and nothing is
/// amended; with the row every admin stored the moment it first held the birth, its node.delete's
/// `TerminateRuntime` receipt is its roster receipt and `RosterAmended` admits path `mesh2.admin`, name `2`.
#[test]
fn the_fabric_primary_reads_a_lost_primarys_runtime_from_its_durable_runtime_row_and_amends() {
    let fp_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let cap = capture();
    let Members { other_admin, final_admin, .. } = Members::new();
    let records = vec![other_admin.clone(), final_admin.clone()];
    let handles = runtime_handles(&records, 47_000);
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.to_string())).collect(), ..Default::default() };
    let birth = |n: &Node| RosterBirth::of(n).unwrap();
    let (lost, survivor) = (birth(&final_admin), birth(&other_admin));
    let (lost_runtime, survivor_runtime) = (RuntimeProof::of(&handles[&final_admin.node_id].fact().unwrap()), RuntimeProof::of(&handles[&other_admin.node_id].fact().unwrap()));
    let lost_node = &final_admin;
    let row = rdm_admin::storage::RuntimeRow { kind: lost_node.kind, mesh: lost_node.mesh.clone(), node_id: lost_node.node_id.clone(), name: lost_node.name.clone(), incarnation_id: lost_node.incarnation_id.clone().unwrap(), runtime: handles[&final_admin.node_id].fact().unwrap(), data_dir: None };
    let (without_row, with_row, before, after, steps_after, outer, trace_id) = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt, operation: outer.clone() };
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, fp_birth.node_id.as_str(), &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let root = tracing::info_span!("cell.mesh-deleted-fp-resume", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.recipe_step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let admitted = |steps: &[BuildStepReceipt]| admitted_of(steps, &outer).unwrap().unwrap();
        let adm = admitted(&builds.read_build(&build_id).await.unwrap().steps);
        // The lost birth's node.delete as an unreached birth: what the fabric-primary runs on the row.
        let delete = || async { node_delete_receipts(&builds, &key(2), &fp_birth, &lost, &exited(&lost, &lost_runtime)).await; Ok(()) };
        let without_row = resume_after_loss(&builds, &fleet, None, &key(2), fp_birth.node_id.as_str(), &adm, &survivor, &survivor_runtime, delete).await;
        let before = admitted(&builds.read_build(&build_id).await.unwrap().steps);
        let with_row = resume_after_loss(&builds, &fleet, Some(&row), &key(2), fp_birth.node_id.as_str(), &adm, &survivor, &survivor_runtime, delete).await;
        let steps_after = builds.read_build(&build_id).await.unwrap().steps;
        let after = admitted(&steps_after);
        (without_row, with_row, before, after, steps_after, outer, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("fp_resume");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let refusal = without_row.expect_err("no runtime row of the lost birth: the resume is refused");
    let rdm_admin::executor::RunEnd::Incomplete(refusal) = refusal else { panic!("a lost primary whose runtime is unknown leaves the leave incomplete, not failed: {refusal:?}") };
    assert!(refusal.contains("not known here") && refusal.contains(final_admin.node_id.as_str()) && refusal.contains("no durable runtime row"), "the refusal names the lost birth and the missing row: {refusal}");
    assert_eq!(before.final_primary, lost, "nothing is amended while the lost primary's runtime is unknown");
    assert_eq!(with_row, Ok(Amended::Applied), "with the durable runtime row the lost primary is deleted and the amendment recorded");
    assert_eq!(after.final_primary, survivor, "the amendment admits the surviving admin");
    assert_eq!(after.final_runtime, survivor_runtime);
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.mesh.reject.via-unproven-primary").iter().any(|s| s["attributes"]["reason"].as_str().is_some_and(|r| r.contains("no durable runtime row"))));
    // The roster receipt is node.delete's own TerminateRuntime under `node.delete` with an explicit node ID.
    assert!(steps_after.iter().any(|r| r.operation == rdm_core::OperationId::node("node.delete", &final_admin.node_id) && r.step == "TerminateRuntime"), "the lost primary's roster receipt is its node.delete's: {:?}", steps_after.iter().map(|r| format!("{:?}/{}", r.operation, r.step)).collect::<Vec<_>>());
    assert!(steps_after.iter().all(|r| r.operation == rdm_core::OperationId::node("node.delete", &final_admin.node_id) || r.operation == outer), "no receipt is recorded under a key outside node.delete's and the leave's: {:?}", steps_after.iter().map(|r| r.operation.clone()).collect::<BTreeSet<_>>());
    assert!(named("rdm.node_admin.mesh.update.via-roster-amended").iter().any(|s| s["attributes"]["outcome"] == "applied" && s["attributes"]["final_primary"].as_str().is_some_and(|p| p.contains(other_admin.node_id.as_str()))));
}

/// CONTRACT: when no admin of the leaving mesh survives, the one node-admin the fabric-primary
/// creates for it admits `DeleteMesh` over the roster the owner admitted plus the amendment that
/// names it, and never captures a roster of its own (mesh-delete.md, "Resume after the primary's
/// loss": "The accepted roster is captured once and never recaptured"; step 2: "or, when none
/// survives, the one node-admin the fabric-primary created for it"). mesh2 held one admin,
/// path `mesh2.admin`, name `1`, lost mid ordinary tier after path `mesh2.rpc`, name `2` exited. The fabric-primary recorded
/// the lost primary's exact Exited and `RosterAmended { path `mesh2.admin`, name `1` -> path `mesh2.admin`, name `3` }`; the new
/// admin is created at the first generated name whose pair and reserved predecessor pair are both free.
/// Its own Build facts hold no `LeavingRoster`: it reads the owner's, records no roster naming
/// itself, runs only path `mesh2.rpc`, name `1`, and its manifest passes the owner's hand-off validation.
#[test]
fn a_created_admin_of_a_leaving_mesh_runs_the_owners_admitted_roster_and_captures_none() {
    let fp_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let cap = capture();
    let Members { failing, exiting, final_admin, .. } = Members::new();
    let new_admin = node_at("mesh2.admin", "3", "mesh2", rdm_core::NodeKind::NodeAdmin);
    let records = vec![final_admin.clone(), exiting.clone(), failing.clone(), new_admin.clone()];
    let handles = runtime_handles(&records, 48_000);
    let birth = |n: &Node| RosterBirth::of(n).unwrap();
    let runtime = |node_id: &NodeId| RuntimeProof::of(&handles[node_id].fact().unwrap());
    let (lost, created) = (birth(&final_admin), birth(&new_admin));
    let admitted_births: Vec<Node> = records.iter().filter(|n| n.node_id != new_admin.node_id).cloned().collect();
    // The name: skip occupied pairs and reserved predecessor pairs, without decoding names.
    let held = vec![PathName::new("mesh2.admin", "1").unwrap(), PathName::new("mesh2.admin", "2-old").unwrap(), PathName::new("mesh2.rpc", "4").unwrap()];
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
        accepted_roster(&owner, &key(1), STEP_ACCEPTED_ROSTER, fp_birth.node_id.as_str(), &lost, &runtime(&final_admin.node_id), || live_roster(&admitted_births)).await.unwrap();
        // path `mesh2.rpc`, name `2`'s node.delete, finished by the lost primary and gossiped to the owner.
        node_delete_receipts(&owner, &key(1), &executor_birth(&lost), &birth(&exiting), &exited(&birth(&exiting), &runtime(&exiting.node_id))).await;
        let root = tracing::info_span!("cell.mesh-deleted-created-admin", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.recipe_step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        node_delete_receipts(&owner, &key(2), &fp_birth, &lost, &exited(&lost, &runtime(&final_admin.node_id))).await;
        amend_roster(&owner, &key(2), fp_birth.node_id.as_str(), &RosterAmended { predecessor: lost.clone(), final_primary: created.clone(), final_runtime: runtime(&new_admin.node_id) }).await.expect("the amendment admits the created admin");
        // path `mesh2.admin`, name `3` admits DeleteMesh: an amendment admits it, so it runs the owner's roster and captures none.
        let owner_facts = owner.facts().await.unwrap();
        let roster = rdm_admin::mesh_delete::leaving_roster(&own, &own.0, &key(2), new_admin.node_id.as_str(), &created, &runtime(&new_admin.node_id), || async { Ok(owner_facts) }).await;
        let out = match roster {
            Ok((roster, known)) => members_exited(&own, &key(2), new_admin.node_id.as_str(), &roster, &created, runtime(&new_admin.node_id), &records, &known, |n: Node| {
                let (ran, t) = (&ran, exited(&RosterBirth::of(&n).unwrap(), &runtime(&n.node_id)));
                async move {
                    ran.lock().unwrap().push(n.node_id.to_string());
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
    assert_eq!(path, Ok(PathName::new("mesh2.admin", "3").unwrap()), "the created admin takes the first free generated pair");
    let manifest = out.unwrap_or_else(|e| panic!("the created admin runs the owner's admitted roster: {e}"));
    assert!(!own_steps.iter().any(|r| r.step == STEP_LEAVING_ROSTER), "the created admin captures no roster of its own: {:?}", own_steps.iter().map(|r| r.step.clone()).collect::<Vec<_>>());
    assert_eq!(*ran.lock().unwrap(), vec![failing.node_id.to_string()], "only the member without an exit receipt runs");
    assert_eq!(sorted(manifest.roster.clone()), sorted(vec![lost.clone(), birth(&exiting), birth(&failing), created.clone()]), "the roster is the admitted one plus the amendment");
    assert!(matches!(verdict, HandoffVerdict::Accept(_)), "the owner accepts the hand-off: {verdict:?}");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    assert!(named("rdm.node_admin.mesh.resolve.via-owner-admitted-roster").iter().any(|s| s["attributes"]["amendments"] == "1" && s["attributes"]["roster"] == "4" && s["attributes"]["me"].as_str().is_some_and(|m| m.contains(new_admin.node_id.as_str()))), "the admission took the owner's admitted roster");
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
    let lost = node_at("mesh2.admin", "1", "mesh2", rdm_core::NodeKind::NodeAdmin);
    let h = runtime_handles(std::slice::from_ref(&lost), 49_001)[&lost.node_id].clone();
    let fleet = Fleet { pids: [(49_001, lost.node_id.to_string())].into_iter().collect(), ..Default::default() };
    let digest_view = rdm_transport::topology::DigestView::default();
    let (without_row, with_row) = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let nodes = MemoryNodesStorage::default();
        let without_row = inspect_birth(&fleet, &digest_view, Some(&nodes), &lost).await;
        nodes.put_runtime(&RuntimeRow { kind: lost.kind, mesh: lost.mesh.clone(), node_id: lost.node_id.clone(), name: lost.name.clone(), incarnation_id: lost.incarnation_id.clone().unwrap(), runtime: h.fact().unwrap(), data_dir: None }).await.unwrap();
        let with_row = inspect_birth(&fleet, &digest_view, Some(&nodes), &lost).await;
        (without_row, with_row)
    });
    assert!(matches!(&without_row, Finding::Unproven { reason } if reason.contains("no runtime row")), "no digest and no row: unproven, naming both: {without_row:?}");
    assert_eq!(with_row, Finding::Exited { code: Some(0) }, "the runtime row recorded at launch is what the provider inspects");
}

/// CONTRACT (R11, the leave acts on the immutable accepted roster's exact births; mesh-delete.md
/// "Resume after the primary's loss" step 3, "Failure/certainty behavior": "No timeout or gossip
/// absence proves exit"): the admin the fabric-primary created for a leaving mesh with no admin
/// left works the roster, not its view's liveness. path `mesh2.rpc`, name `1` was stopped by the lost primary
/// before its exit was proven, and the created admin's view does not list it. It is still a roster
/// birth with no Exited receipt, and its record is the one this admin's topology view holds of that
/// exact birth: the record of another birth at the path is never taken.
/// Its workflow runs, its row is written Deleted on its receipt, and the tier completes.
#[test]
fn a_roster_birth_the_view_does_not_list_is_worked_from_the_record_its_topology_view_holds() {
    let cap = capture();
    let Members { failing, exiting, final_admin, .. } = Members::new();
    let records = vec![failing.clone(), exiting.clone(), final_admin.clone()];
    let handles = runtime_handles(&records, 46_000);
    let birth = |n: &Node| RosterBirth::of(n).unwrap();
    let (stopped, me) = (birth(&failing), birth(&final_admin));
    let roster = vec![stopped.clone(), birth(&exiting), me.clone()];
    // The created admin's view: itself and the member still heard; the stopped one is gone from it.
    let view: Vec<Node> = records.iter().filter(|n| n.node_id != failing.node_id).cloned().collect();
    let mut held_stopped = records.iter().find(|n| n.node_id == failing.node_id).unwrap().clone();
    held_stopped.status = NodeStatus::Leaving;
    let mut other_birth = held_stopped.clone();
    other_birth.incarnation_id = Some(IncarnationId::mint());

    let stale = roster_records(&view, &roster, |b| (b.node_id == failing.node_id.as_str()).then(|| other_birth.clone()));
    assert!(!stale.iter().any(|n| n.node_id == failing.node_id), "a held record of another birth at the path is not the roster birth's: {stale:?}");
    let worked = roster_records(&view, &roster, |b| (b.node_id == failing.node_id.as_str()).then(|| held_stopped.clone()));
    assert_eq!(worked.iter().find(|n| n.node_id == failing.node_id), Some(&held_stopped), "the stopped roster birth is worked from the record its topology view holds: {worked:?}");
    assert_eq!(worked.len(), 3, "every roster birth has its record: {worked:?}");

    let runtime = RuntimeProof::of(&handles[&final_admin.node_id].fact().unwrap());
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
        let worked = roster_records(&view, &roster, |b| (b.node_id == failing.node_id.as_str()).then(|| held_stopped.clone()));
        members_exited(&builds, &key, final_admin.node_id.as_str(), &roster, &me, runtime.clone(), &worked, &[], |n: Node| {
            let runtime = RuntimeProof::of(&handles[&n.node_id].fact().unwrap());
            async move { Ok(exited(&RosterBirth::of(&n).unwrap(), &runtime)) }
        })
        .await
    });
    let manifest = manifest.expect("the tier completes over every roster birth");
    assert!(manifest.receipts.iter().any(|r| r.node_id == stopped.node_id && r.incarnation_id == stopped.incarnation_id), "the stopped birth's exact Exited receipt is in the manifest: {:?}", manifest.receipts);
    let held = cap.spans().into_iter().filter(|s| s["name"] == "rdm.node_admin.mesh.resolve.via-held-record").collect::<Vec<_>>();
    assert_eq!(held.iter().map(|s| (s["attributes"]["member"].as_str().unwrap_or("").to_string(), s["attributes"]["status"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>(), vec![(failing.node_id.to_string(), "Leaving".to_string())], "{held:#?}");
}

/// CONTRACT (mesh-delete.md, "Resume after the primary's loss", step 3: the new final primary "runs
/// only the tiers without a terminal receipt"; R11: the accepted roster is immutable and decides what
/// runs; R13: "Rebuild after a role change reads the mesh's own rows"): the admin created for a
/// leaving mesh with no admin left joined after path `mesh2.rpc`, name `1` stopped, so neither its view nor its
/// the topology view holds that birth. The roster birth runs from the roster itself: its member workflow is
/// handed a record naming exactly that birth (separate path/name, node id, incarnation), its exact Exited
/// receipt is in the manifest, and its row, read from the mesh's own rows before the tiers run, is
/// written Deleted with its config kept by its node.delete's NodeDeleted step. Must NOT happen: the tier refused "this view holds no record
/// of the accepted birth" (no_survivor, created path `mesh2.admin`, name `2`, attempts 2 and 3), or a Deleted row
/// written with a config the writer never read.
#[test]
fn a_roster_birth_the_created_admin_never_held_runs_from_the_roster_and_its_row_is_written_deleted() {
    let cap = capture();
    let Members { failing, exiting, .. } = Members::new();
    let created = node_at("mesh2.admin", "3", "mesh2", rdm_core::NodeKind::NodeAdmin);
    let all = vec![failing.clone(), exiting.clone(), created.clone()];
    let handles = runtime_handles(&all, 47_000);
    let birth = |n: &Node| RosterBirth::of(n).unwrap();
    let (stopped, me) = (birth(&failing), birth(&created));
    let roster = vec![stopped.clone(), birth(&exiting), me.clone()];
    // The created admin's records: itself and the member it hears; never the stopped birth.
    let records: Vec<Node> = all.iter().filter(|n| n.node_id != failing.node_id).cloned().collect();
    let storage = Arc::new(MemoryNodesStorage::default());
    let me_id = created.node_id.clone();
    let rows = NodeRows::new(me.node.clone(), me_id.clone(), Arc::new(RwLock::new(writer_view(&records, Some(&me_id), None))), storage.clone());
    let runtime = RuntimeProof::of(&handles[&created.node_id].fact().unwrap());
    let ran: Mutex<Vec<Node>> = Mutex::new(Vec::new());
    let (manifest, trace_id) = cap.run(async {
        // Every member's row as its create left it. The created admin's writer holds the rows of the
        // births its view names; the stopped birth's row it never held.
        for n in &all {
            storage.write_record(&NodeRow { replacement_from: None, kind: n.kind, mesh: n.mesh.clone(), initial_properties: None, topology_storage_provider: None, name: n.name.clone(), node_id: n.node_id.clone(), status: NodeStatus::Ready, config: tier_of(n.node_id.as_str()), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }).await.unwrap();
        }
        for n in &records {
            assert!(matches!(rows.claim(&n.name, &n.mesh.clone(), n.kind, &n.node_id, &rdm_core::OperationId::location("node.create", &n.name), None, None, ClaimFor::Create).await, Ok(Claimed::HeldByNode(_))), "the writer holds {:?}'s row", n.name);
        }
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt: 2, operation: operation(&mesh_id) };
        let root = tracing::info_span!("cell.mesh-deleted-roster-birth", build_id = %build_id.log(), operation = %key.operation.action, operation_subject = ?key.operation.subject, operation_step = ?key.operation.recipe_step, operation_occurrence = ?key.operation.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        roster_rows_read(Some(&rows), created.node_id.as_str(), &key.operation, &roster, &records).await;
        // The member's node.delete: its NodeDeleted step writes the row Deleted (node-delete.md step 3)
        // from the record it was handed, then the provider's exact Exited is its receipt.
        let manifest = members_exited(&builds, &key, created.node_id.as_str(), &roster, &me, runtime.clone(), &records, &[], |n: Node| {
            ran.lock().unwrap().push(n.clone());
            let (runtime, rows, operation) = (RuntimeProof::of(&handles[&n.node_id].fact().unwrap()), &rows, rdm_core::OperationId::node("node.delete", &n.node_id));
            async move {
                rows.deleted(&n, &operation).await.map_err(|e| e.to_string())?;
                Ok(exited(&RosterBirth::of(&n).unwrap(), &runtime))
            }
        })
        .await;
        (manifest, trace_id)
    });
    let manifest = manifest.expect("the tier completes over every roster birth, the one the created admin never held included");
    assert!(manifest.receipts.iter().any(|r| r.node_id == stopped.node_id && r.incarnation_id == stopped.incarnation_id), "the stopped birth's exact Exited receipt is in the manifest: {:?}", manifest.receipts);
    let ran = ran.into_inner().unwrap();
    let worked = ran.iter().find(|n| n.node_id == failing.node_id).expect("the stopped roster birth's workflow ran");
    assert_eq!(RosterBirth::of(worked).unwrap(), stopped, "its workflow is handed exactly the roster birth");
    let row = cap.run(async { rdm_admin::storage::NodesStorage::node(storage.as_ref(), &failing.node_id).await.unwrap() }).expect("the stopped birth's row stays at its node id");
    assert_eq!((row.status, row.node_id.to_string(), row.config.clone()), (NodeStatus::Deleted, stopped.node_id.clone(), tier_of(&stopped.node_id)), "its row is written Deleted with the config its node held: {row:?}");
    let spans = cap.spans();
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name).cloned().collect::<Vec<_>>();
    let from_roster = named("rdm.node_admin.mesh.resolve.via-roster-birth");
    assert_eq!(from_roster.iter().map(|s| s["attributes"]["member"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>(), vec![failing.node_id.to_string()], "{from_roster:#?}");
    let read = named("rdm.node_admin.mesh.resolve.via-roster-row");
    assert_eq!(read.iter().map(|s| (s["attributes"]["member"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>(), vec![(failing.node_id.to_string(), "held".to_string())], "{read:#?}");
    let deleted = named("rdm.node_admin.node.delete.via-node-row").into_iter().filter(|s| s["attributes"]["node_id"] == failing.node_id.as_str()).collect::<Vec<_>>();
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
    let Members { failing, exiting, final_admin, fp, .. } = Members::new();
    let records = vec![final_admin.clone(), exiting.clone()];
    let handles = runtime_handles(&records, 49_000);
    let me = RosterBirth::of(&final_admin).unwrap();
    let my_runtime = RuntimeProof::of(&handles[&final_admin.node_id].fact().unwrap());
    let (roster, held, trace_id) = cap.run(async {
        let (owner, own) = (WireBuilds(MemoryBuildStateAdapter::new()), WireBuilds(MemoryBuildStateAdapter::new()));
        let build_id = BuildId::mint();
        let mesh_id = MeshId::mint();
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), attempt: 1, operation: operation(&mesh_id) };
        owner.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
        accepted_roster(&owner, &key, STEP_ACCEPTED_ROSTER, fp.node_id.as_str(), &me, &my_runtime, || live_roster(&records)).await.unwrap();
        let owner_facts = owner.facts().await.unwrap();
        let root = tracing::info_span!("cell.mesh-deleted-unheard-build", build_id = %build_id.log());
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let reads = std::sync::atomic::AtomicUsize::new(0);
        let roster = rdm_admin::mesh_delete::leaving_roster(&own, &own.0, &key, final_admin.node_id.as_str(), &me, &my_runtime, || async {
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
    assert!(named("rdm.node_admin.build.update.via-owner-facts-absorbed").iter().any(|s| s["attributes"]["executor"] == final_admin.node_id.as_str() && s["attributes"]["facts"] == "2" && s["attributes"]["steps"] == "1"), "the owner's Build facts were absorbed into this admin's own log: {:?}", spans.iter().map(|s| s["name"].clone()).collect::<Vec<_>>());
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
    let Members { failing, exiting, final_admin, fp, .. } = Members::new();
    let records = vec![final_admin.clone(), failing.clone(), exiting.clone()];
    let handles = runtime_handles(&records, 50_000);
    let roster = live_roster(&records).unwrap();
    let me = RosterBirth::of(&final_admin).unwrap();
    let admitted = Admitted { roster: roster.clone(), final_primary: me.clone(), final_runtime: RuntimeProof::of(&handles[&final_admin.node_id].fact().unwrap()), amendments: Vec::new() };
    let mesh_id = MeshId::mint();
    let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: BuildId::mint(), attempt: 1, operation: operation(&mesh_id) };
    let reference = manifest_reference(key.build_id.as_str(), 1, &key.operation, false);
    let cap = capture();
    let (verdict, not_ready, trace_id) = cap.run(async {
        let root = tracing::info_span!("cell.mesh-deleted-unknown-build-handoff", build_id = %key.build_id.log());
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let unknown = rdm_admin::build_state::BuildStateError::UnknownBuild(key.build_id.clone());
        let verdict = handoff_verdict_from_read(Err(unknown), &key, &admitted, &reference, fp.node_id.as_str(), final_admin.node_id.as_str());
        let not_ready = handoff_verdict_from_read(Err(rdm_admin::build_state::BuildStateError::Io("hydrating".into())), &key, &admitted, &reference, fp.node_id.as_str(), final_admin.node_id.as_str());
        (verdict, not_ready, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("unknown_build_handoff");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    match verdict {
        Ok(v @ HandoffVerdict::Incomplete { .. }) => {
            let expected: BTreeSet<String> = roster.iter().filter(|b| **b != me).map(|b| format!("{} {}", b.node_id, b.incarnation_id)).collect();
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
    cut: Mutex<Option<(&'static str, rdm_core::OperationId)>>,
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
            return Err(BuildStateError::Io(format!("the executor is cut after the work of {step} and before its receipt of {operation:?} is durable")));
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
        self.inner.committed_facts(build).await
    }
    async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(id).await
    }
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, rdm_core::OperationId)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, rdm_core::OperationId)>, BuildStateError> {
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
/// node.deleted; the Build carrier: "Completed steps are not rerun"). Attempt 1: path `mesh2.rpc`, name `1` is
/// stopped and its runtime terminated, the provider's exact Exited is receipted (TerminateRuntime),
/// and the executor is cut before NodeDeleted's receipt is durable: NodeDeleted is never published
/// and the delete has no `Complete`. path `mesh2.rpc`, name `2` runs its whole delete. Attempt 2 (another
/// mesh-primary, the same Build) takes path `mesh2.rpc`, name `2` as done and finishes path `mesh2.rpc`, name `1`'s delete:
/// its runtime is not terminated again (TerminateRuntime is reused), NodeDeleted is executed,
/// receipted and published once, and the delete ends in `Complete`. Every other peer keeps the
/// stopped birth until mesh-left if NodeDeleted is never published: the exited verdict alone is not the
/// departure. Each primary's publications are its own (attempt 1's primary is lost before its
/// frames land): attempt 2's primary publishes the NodeDeleted of the deletes it ran, and never again
/// path `mesh2.rpc`, name `2`'s, which its own node.delete published once under attempt 1 (node-delete.md step 4:
/// the executor emits node.deleted); a peer that never heard it keeps the birth until mesh-left.
#[test]
fn a_delete_cut_after_its_terminate_runtime_receipt_is_finished_by_the_resumed_attempt_from_its_missing_steps() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-deleted-cut-delete-{}", NodeId::mint()));
    let Members { failing, exiting, other_admin, final_admin, fp } = Members::new();
    let records = vec![failing.clone(), exiting.clone(), other_admin.clone(), final_admin.clone()];
    let handles = runtime_handles(&records, 44_000);
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.to_string())).collect(), ..Default::default() };
    let me = RosterBirth::of(&final_admin).unwrap();
    let final_runtime = RuntimeProof::of(&handles[&final_admin.node_id].fact().unwrap());
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
            launcher: rdm_core::launch::Launcher { mesh: "mesh2".into(), name: final_admin.name.clone(), node_id: final_admin.node_id.clone(), incarnation: final_admin.incarnation_id.clone().unwrap() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let cut = CutBuilds { inner: &builds, cut: Mutex::new(Some(("NodeDeleted", rdm_core::OperationId::node("node.delete", &failing.node_id)))) };
        let cut_pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &lost_departures, builds: &cut, template: &template, timeouts: Timeouts::default() };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &departures, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let shutdown_1 = |n: Node| {
            let (pipeline, handles, calls, build_id) = (&cut_pipeline, &handles, &calls, &build_id);
            async move {
                let node_id = n.node_id.clone();
                calls.lock().unwrap().push((1, node_id.to_string()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: (*build_id).clone(), attempt: 1, node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let shutdown_2 = |n: Node| {
            let (pipeline, handles, calls, build_id) = (&pipeline, &handles, &calls, &build_id);
            async move {
                let node_id = n.node_id.clone();
                calls.lock().unwrap().push((2, node_id.to_string()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: (*build_id).clone(), attempt: 2, node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-deleted-cut-delete", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.recipe_step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let mp = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key(1), final_admin.node_id.as_str(), &mp, &me, final_runtime.clone(), &records, &[], shutdown_1).await;
        let cut_steps = builds.read_build(&build_id).await.unwrap().steps;
        let records2: Vec<Node> = view.holds(&records);
        let mp2 = accepted_roster(&builds, &key(2), STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), &me, &final_runtime, || live_roster(&records2)).await.unwrap();
        let second = members_exited(&builds, &key(2), final_admin.node_id.as_str(), &mp2, &me, final_runtime.clone(), &records2, &[], shutdown_2).await;
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        (first, cut_steps, second, steps, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("cut_delete");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    let err = first.expect_err("attempt 1 is cut inside path `mesh2.rpc`, name `1`'s delete: the ordinary tier is incomplete");
    assert!(err.contains("tier ordinary-nodes is incomplete") && err.contains(failing.node_id.as_str()), "{err}");
    let calls = calls.into_inner().unwrap();
    let ran = |attempt: u32| calls.iter().filter(|(a, _)| *a == attempt).map(|(_, n)| n.clone()).collect::<BTreeSet<_>>();
    assert_eq!(ran(1), BTreeSet::from([failing.node_id.to_string(), exiting.node_id.to_string()]), "attempt 1 ran the ordinary tier");
    let of = |steps: &[BuildStepReceipt], node: &NodeId, step: &str| steps.iter().filter(|r| r.operation == rdm_core::OperationId::node("node.delete", node) && r.step == step && r.outcome == rdm_admin::build_state::StepOutcome::Complete).map(|r| r.attempt).collect::<Vec<_>>();
    // The cut left rpc.1's exited verdict and no more; rpc.2's delete ran to its end.
    assert_eq!((of(&cut_steps, &failing.node_id, "TerminateRuntime"), of(&cut_steps, &failing.node_id, "NodeDeleted"), of(&cut_steps, &failing.node_id, "Complete")), (vec![1], vec![], vec![]), "the cut left the cut member's exited verdict and no NodeDeleted: {cut_steps:#?}");
    assert_eq!(of(&cut_steps, &exiting.node_id, "Complete"), vec![1], "the completed member's delete ran to its end");

    // Attempt 2: the exited verdict is not the delete. The delete with no NodeDeleted is run to its end.
    assert_eq!(ran(2), BTreeSet::from([failing.node_id.to_string(), other_admin.node_id.to_string()]), "attempt 2 finishes the cut member's delete (its exited verdict alone is not a finished delete) and runs the node-admin tier, and does not run the completed member, whose delete has every step's receipt");
    let manifest = second.unwrap_or_else(|e| panic!("the resumed attempt completes: {e}"));
    assert_eq!(manifest.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(), [&failing, &exiting, &other_admin].iter().map(|n| (n.node_id.to_string(), n.incarnation_id.as_ref().unwrap().to_string())).collect::<BTreeSet<_>>(), "the manifest carries every other member's receipt");
    assert_eq!(of(&steps, &failing.node_id, "TerminateRuntime"), vec![1], "the cut member's completed TerminateRuntime is not run again");
    assert_eq!((of(&steps, &failing.node_id, "NodeDeleted"), of(&steps, &failing.node_id, "Complete")), (vec![2], vec![2]), "the cut member's NodeDeleted and Complete are receipted once, by the resumed attempt");
    assert_eq!(fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == failing.node_id.as_str()).count(), 1, "the cut member's runtime was terminated once, in attempt 1");
    assert_eq!(fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == exiting.node_id.as_str()).count(), 1, "the completed member's runtime was terminated once");
    let published = |d: Departures| -> Vec<rdm_core::OperationId> {
        let mut ops: Vec<rdm_core::OperationId> = d.0.into_inner().unwrap().into_iter().map(|op| op.operation).collect();
        ops.sort();
        ops
    };
    assert_eq!(published(lost_departures), vec![rdm_core::OperationId::node("node.delete", &exiting.node_id)], "attempt 1's primary published the NodeDeleted of the one delete it finished");
    let mut want: Vec<rdm_core::OperationId> = [&failing.node_id, &other_admin.node_id].iter().map(|n| rdm_core::OperationId::node("node.delete", n)).collect();
    want.sort();
    assert_eq!(published(departures), want, "attempt 2's primary publishes the NodeDeleted of the deletes it ran, the cut member's and the other admin's, and never again the completed member's, its node.delete's under attempt 1");

    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let step = |node: &Node, step: &str, attempt: &str| named("rdm.node_admin.deployment.update.via-step").into_iter().filter(|s| s["attributes"]["node_path"] == node.name.path && s["attributes"]["node_name"] == node.name.name && s["attributes"]["step"] == step && s["attributes"]["attempt"] == attempt).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>();
    assert_eq!(step(&failing, "TerminateRuntime", "2"), vec!["reused".to_string()], "attempt 2 reuses the receipted TerminateRuntime");
    assert_eq!(step(&failing, "NodeDeleted", "2"), vec!["complete".to_string()], "attempt 2 executes the NodeDeleted that has no receipt");
    let retained = named("rdm.node_admin.mesh.resolve.via-retained-receipts");
    assert!(retained.iter().any(|s| s["attributes"]["attempt"] == "2" && s["attributes"]["retained"] == "1" && s["attributes"]["retained_births"] == exiting.node_id.as_str() && s["attributes"]["remaining"] == "2"), "attempt 2 keeps the one finished delete and runs the other two members: {retained:?}");
    eprintln!("trace {trace_id}: attempt 2 via-step the cut member TerminateRuntime reused, NodeDeleted complete; via-retained-receipts retained 1 (the completed member) remaining 2");
}

/// The owner's open leave of mesh2, admitted over `records` with `FINAL` its final primary, and the
/// receiver its handoff resolves.
fn open_leave(mesh_delete_view: &MeshDeleteView, records: &[Node], final_node: &Node) -> (MeshDeleteKey, tokio::sync::watch::Receiver<Option<rdm_admin::mesh_delete::MeshDeleteOutcome>>, RosterBirth) {
    let roster = live_roster(records).unwrap();
    let final_primary = RosterBirth::of(final_node).unwrap();
    let mesh_id = MeshId::mint();
    let key = MeshDeleteKey { operation: operation(&mesh_id), mesh_id, build_id: BuildId::mint(), attempt: 1 };
    let admitted = Admitted { roster, final_primary: final_primary.clone(), final_runtime: RuntimeProof::of(&runtime_handles(std::slice::from_ref(final_node), 46_000)[&final_node.node_id].fact().unwrap()), amendments: Vec::new() };
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
    let Members { failing, exiting, other_admin, final_admin, .. } = Members::new();
    let records = vec![failing, exiting, other_admin, final_admin.clone()];
    let mesh_delete_view = MeshDeleteView::default();
    let (key, handoff, final_primary) = open_leave(&mesh_delete_view, &records, &final_admin);
    let (end, open_after_silence, waited, trace_id) = run_paused(&cap, async {
        let root = tracing::info_span!("cell.owner-wait-exited-verdict", operation = %key.operation.action, operation_subject = ?key.operation.subject, operation_step = ?key.operation.recipe_step, operation_occurrence = ?key.operation.occurrence);
        let trace_id = trace_id(&root);
        let (view_tx, view_rx) = tokio::sync::watch::channel(0u64);
        let proven: Arc<Mutex<Option<String>>> = Arc::default();
        let held = proven.clone();
        let (k, primary) = (key.clone(), format!("{} {}", final_primary.node_id, final_primary.incarnation_id));
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
        *proven.lock().unwrap() = Some(format!("the final primary is unheard; its runtime reads exited: {} {}", final_primary.node_id, final_primary.incarnation_id));
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
            assert!(reason.contains(final_admin.node_id.as_str()) && reason.contains("is lost"), "the incomplete end names the lost final primary: {reason}");
        }
        other => panic!("the attempt ends incomplete on the final primary's loss: {other:?}"),
    }
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let ended = named("rdm.node_admin.mesh.update.via-final-primary-lost");
    assert_eq!(ended.len(), 1, "one span names the wait's end on the loss: {spans:?}");
    assert_eq!(ended[0]["attributes"]["final_primary"], json!(format!("{} {}", final_primary.node_id, final_primary.incarnation_id)), "{}", ended[0]);
    assert!(ended[0]["attributes"]["verdict"].as_str().is_some_and(|p| p.contains("exited")), "{}", ended[0]);
}

/// CONTRACT: a handoff that arrives ends the owner's wait with its validated manifest, even when the
/// final primary is proven exited afterwards (it reported mesh.deleted while it lived, then went):
/// the handoff is read first. Must NOT happen: an exited verdict discarding a handoff already held.
#[test]
fn a_handoff_held_ends_the_owners_wait_with_its_manifest_even_when_the_final_primary_exits_after() {
    let cap = capture();
    let Members { failing, exiting, other_admin, final_admin, .. } = Members::new();
    let records = vec![failing, exiting, other_admin, final_admin.clone()];
    let mesh_delete_view = MeshDeleteView::default();
    let (key, handoff, final_primary) = open_leave(&mesh_delete_view, &records, &final_admin);
    let manifest = ExitManifest { mesh_id: key.mesh_id.to_string(), roster: live_roster(&records).unwrap(), final_primary: final_primary.clone(), final_runtime: None, receipts: Vec::new() };
    let end = run_paused(&cap, async {
        let (view_tx, view_rx) = tokio::sync::watch::channel(0u64);
        assert!(mesh_delete_view.resolve(&key, manifest.clone()), "the first outcome of the leave");
        view_tx.send_modify(|t| *t += 1);
        await_mesh_deleted(&key, final_admin.node_id.as_str(), handoff, view_rx, || std::future::ready(Some("its exited-verdict row is read".to_string())), MESH_DELETED_WAIT).await
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
    let fp_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-delete-step-faults-{}", NodeId::mint()));
    let Members { failing, exiting, other_admin, final_admin, .. } = Members::new();
    let records = vec![failing.clone(), exiting.clone(), other_admin.clone(), final_admin.clone()];
    let handles = runtime_handles(&records, 47_000);
    let fleet = Fleet {
        pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.to_string())).collect(),
        ..Default::default()
    };
    let birth = |n: &Node| RosterBirth::of(n).unwrap();
    let (lost, survivor) = (birth(&final_admin), birth(&other_admin));
    let (lost_runtime, survivor_runtime) = (
        RuntimeProof::of(&handles[&final_admin.node_id].fact().unwrap()),
        RuntimeProof::of(&handles[&other_admin.node_id].fact().unwrap()),
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
                mesh: "mesh2".to_owned(), name: final_admin.name.clone(),
                node_id: final_admin.node_id.clone(),
                incarnation: final_admin.incarnation_id.clone().unwrap(),
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

        let root = tracing::info_span!("cell.mesh-delete-step-faults", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.recipe_step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Step 1-2: Admit accepted roster on owner and leaving roster on mesh-primary.
        let roster = accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, fp_birth.node_id.as_str(), &lost, &lost_runtime, || live_roster(&records))
            .await
            .unwrap();
        let mp1 = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), &lost, &lost_runtime, || live_roster(&records))
            .await
            .unwrap();

        // Fault 1: Cut after TerminateRuntime (before NodeDeleted) on FAILING (path `mesh2.rpc`, name `1`), while
        // EXITING (path `mesh2.rpc`, name `2`) completes in the ordinary-nodes tier. The ordinary tier is left
        // incomplete, the node-admin tier never starts, and an incomplete handoff is refused by name
        // (`RejectedIncompleteManifest`).
        let cut = CutBuilds {
            inner: &builds,
            cut: Mutex::new(Some(("NodeDeleted", rdm_core::OperationId::node("node.delete", &failing.node_id)))),
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
        let mut resumed_template = template.clone();
        resumed_template.launcher = rdm_core::launch::Launcher { mesh: other_admin.mesh.clone(), name: other_admin.name.clone(), node_id: other_admin.node_id.clone(), incarnation: other_admin.incarnation_id.clone().unwrap() };
        let pipeline = DeploymentPipeline {
            provider: &fleet,
            joins: &joins,
            observer: &NoRpc,
            sink: &view,
            lifecycle: &NoLifecycleEvents,
            builds: &builds,
            template: &resumed_template,
            timeouts: Timeouts::default(),
        };

        let first = members_exited(&builds, &key(1), final_admin.node_id.as_str(), &mp1, &lost, lost_runtime.clone(), &records, &[], |n: Node| {
            let (pipeline, handles, calls, build_id) = (&cut_pipeline, &handles, &calls, &build_id);
            async move {
                let node_id = n.node_id.clone();
                calls.lock().unwrap().push((1, node_id.to_string()));
                pipeline
                    .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id),
                        build_id: (*build_id).clone(),
                        attempt: 1,
                        node: n,
                        handle: handles[&node_id].clone(),
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

        // Fault 2: The final primary path `mesh2.admin`, name `1` is killed/lost while the fabric-primary awaits
        // `mesh.deleted`. Silence does not end the wait; once the final primary's runtime is proven
        // exited, `await_mesh_deleted` ends immediately with `RunEnd::Incomplete` (not waiting for
        // `MESH_DELETED_WAIT`).
        let mesh_delete_view = MeshDeleteView::default();
        let handoff_rx = mesh_delete_view.open(key(1), initial_admitted);
        let (view_tx, view_rx) = tokio::sync::watch::channel(0u64);
        let proven: Arc<Mutex<Option<String>>> = Arc::default();
        let held = proven.clone();
        let (k1, primary_label) = (key(1), format!("{} {}", lost.node_id, lost.incarnation_id));
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
            lost.node_id, lost.incarnation_id
        ));
        view_tx.send_modify(|t| *t += 1);
        let wait_end = wait_task.await.unwrap();
        let wait_elapsed = began_wait.elapsed();

        // Resume on Attempt 2:
        // 1. Fabric-primary records the lost primary's exact `Exited` receipt (`TerminateRuntime`)
        //    and appends `RosterAmended { path `mesh2.admin`, name `1` -> path `mesh2.admin`, name `2` }` (replaying `AlreadyApplied`
        //    on duplicate and refusing a conflicting second amendment for the same predecessor).
        node_delete_receipts(&builds, &key(2), &fp_birth, &lost, &exited(&lost, &lost_runtime)).await;
        let applied = amend_roster(&builds, &key(2), fp_birth.node_id.as_str(), &amendment).await;
        let replayed = amend_roster(&builds, &key(2), fp_birth.node_id.as_str(), &amendment).await;
        let conflicting = amend_roster(
            &builds,
            &key(2),
            fp_birth.node_id.as_str(),
            &RosterAmended {
                final_primary: birth(&exiting),
                ..amendment.clone()
            },
        )
        .await;

        // 2. The surviving mesh-primary path `mesh2.admin`, name `2` resumes `members_exited`: it keeps EXITING's
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
        let manifest = members_exited(&builds, &key(2), other_admin.node_id.as_str(), &mp2, &survivor, survivor_runtime.clone(), &records2, &[], |n: Node| {
            let (pipeline, handles, calls, build_id) = (&pipeline, &handles, &calls, &build_id);
            async move {
                let node_id = n.node_id.clone();
                calls.lock().unwrap().push((2, node_id.to_string()));
                pipeline
                    .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id),
                        build_id: (*build_id).clone(),
                        attempt: 2,
                        node: n,
                        handle: handles[&node_id].clone(),
                        unreached: false,
                    })
                    .await
                    .map_err(|e| e.to_string())
            }
        })
        .await
        .expect("attempt 2 finishes the cut member delete and completes OtherMembersExited");

        // 3. Handoff verdict accepts the amended primary's manifest, refuses a stale handoff naming
        //    the lost predecessor, records `MeshDeleted`, and after path `mesh2.admin`, name `2`'s separate
        //    `node.delete`, completes `MeshLeft` over all 4 births.
        let facts = builds.facts().await.unwrap();
        let verdict = handoff_verdict(&facts, &key(2), &admitted, &manifest_reference(build_id.as_str(), 2, &outer, false));
        let HandoffVerdict::Accept(completed) = verdict.clone() else {
            panic!("amended handoff must be accepted: {verdict:?}");
        };
        record_completion(&builds, &key(2), fp_birth.node_id.as_str(), &completed).await.unwrap();

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
        let last = records2.iter().find(|n| n.node_id == other_admin.node_id).cloned().unwrap();
        let mut fp_template = template.clone();
        fp_template.launcher = rdm_core::launch::Launcher { mesh: "mesh1".into(), name: PathName::new("mesh1.admin", "1").unwrap(), node_id: fp_birth.node_id.clone(), incarnation: fp_birth.incarnation.clone() };
        let fp_pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &fp_template, timeouts: Timeouts::default() };
        let terminal = fp_pipeline
            .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(last).node_id),
                build_id: delete_build,
                attempt: 1,
                node: last,
                handle: handles[&other_admin.node_id].clone(),
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
    assert!(err.contains("tier ordinary-nodes is incomplete") && err.contains(failing.node_id.as_str()), "{err}");
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
        matches!(&wait_end, Err(rdm_admin::executor::RunEnd::Incomplete(r)) if r.contains(final_admin.node_id.as_str()) && r.contains("is lost")),
        "await_mesh_deleted ends Incomplete naming the lost final primary: {wait_end:?}"
    );
    assert_eq!(applied, Ok(Amended::Applied));
    assert_eq!(replayed, Ok(Amended::AlreadyApplied));
    assert!(conflicting.is_err(), "conflicting second amendment for the same predecessor is refused");

    let calls = calls.into_inner().unwrap();
    let ran_attempt_2: Vec<String> = calls.iter().filter(|(a, _)| *a == 2).map(|(_, n)| n.clone()).collect();
    assert_eq!(
        ran_attempt_2,
        vec![failing.node_id.to_string()],
        "attempt 2 finishes only the cut member delete and does not re-run completed members: {calls:?}"
    );
    assert_eq!(
        fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == failing.node_id.as_str()).count(),
        1,
        "the cut member's runtime is terminated only once across the cut and resume"
    );
    assert_eq!(
        manifest.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(),
        [&failing, &exiting, &final_admin].iter().map(|n| (n.node_id.to_string(), n.incarnation_id.as_ref().unwrap().to_string())).collect::<BTreeSet<_>>(),
        "the amended manifest carries every birth except the surviving final primary"
    );
    assert_eq!(replay_handoff, Some(StatusReply::AlreadyApplied));
    let all = mesh_left.expect("MeshLeft proof completes once the surviving primary's own node.delete finishes");
    assert_eq!(
        all.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(),
        [&failing, &exiting, &other_admin, &final_admin].iter().map(|n| (n.node_id.to_string(), n.incarnation_id.as_ref().unwrap().to_string())).collect::<BTreeSet<_>>(),
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
/// - `docs/architecture/lifecycles/mesh-delete.md:254` ("| Member delete | mesh-primary and each member | [node.delete](node-delete.md#observability-contract) per member; exact Exited receipt | outer operation plus `node.delete` with an explicit node ID |")
/// - `docs/architecture/lifecycles/mesh-delete.md:255` ("| mesh.deleted | mesh-primary | manifest validated by the fabric-primary; mesh row deleted; mesh topology carries the mesh deleted | outer operation; complete roster |")
/// - `docs/architecture/lifecycles/mesh-delete.md:256` ("| Mesh-primary delete | fabric-primary | node.delete with the fabric-primary as executor; exact Exited; row deleted | its own Build and trace |")
#[test]
fn lost_primary_verdict_is_recorded_as_roster_receipt_reached_refuses_resume_and_observability_spans_correlate() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-delete-conformance-{}", NodeId::mint()));
    let Members { failing, exiting, other_admin, final_admin, .. } = Members::new();
    let records = vec![failing.clone(), exiting.clone(), other_admin.clone(), final_admin.clone()];
    let handles = runtime_handles(&records, 58_000);
    let fleet = Fleet {
        pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.to_string())).collect(),
        ..Default::default()
    };
    let birth = |n: &Node| RosterBirth::of(n).unwrap();
    let (lost, survivor) = (birth(&final_admin), birth(&other_admin));
    let (lost_runtime, survivor_runtime) = (
        RuntimeProof::of(&handles[&final_admin.node_id].fact().unwrap()),
        RuntimeProof::of(&handles[&other_admin.node_id].fact().unwrap()),
    );
    let lost_node = records.iter().find(|n| n.node_id == final_admin.node_id).unwrap().clone();
    let survivor_node = records.iter().find(|n| n.node_id == other_admin.node_id).unwrap().clone();
    let row = rdm_admin::storage::RuntimeRow { kind: lost_node.kind, mesh: lost_node.mesh.clone(),
        node_id: lost_node.node_id.clone(),
        name: lost_node.name.clone(),
        incarnation_id: lost_node.incarnation_id.clone().unwrap(),
        runtime: handles[&final_admin.node_id].fact().unwrap(),
        data_dir: None,
    };
    let storage = Arc::new(MemoryNodesStorage::default());
    let mesh_storage = MemoryMeshStorage::default();
    let declared = Mutex::new(Declared::default());
    let fp = {
        let mut n = node_at("mesh1.admin", "1", "mesh1", rdm_core::NodeKind::NodeAdmin);
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
                mesh: fp.mesh.clone(), name: fp.name.clone(),
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

        let mut member_template = template.clone();
        member_template.launcher = rdm_core::launch::Launcher { mesh: other_admin.mesh.clone(), name: other_admin.name.clone(), node_id: other_admin.node_id.clone(), incarnation: other_admin.incarnation_id.clone().unwrap() };
        let member_pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &member_template, timeouts: Timeouts::default() };
        let root = tracing::info_span!("cell.mesh-delete-observability", build_id = %build_id.log(), attempt = 1, operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.recipe_step, operation_occurrence = ?outer.occurrence);
        let outer_trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Fabric-primary change on the same trace when the deleting mesh holds the fabric-primary role.
        tracing::info_span!(
            "rdm.node_admin.primary.update.via-primary-update",
            role = "fabric-primary",
            operation = ?rdm_admin::fabric_handover::update_operation(rdm_core::PrimaryRole::FabricPrimary, &outer),
            outcome = "confirmed"
        )
        .in_scope(|| tracing::info!("node.fabric-primary.update confirmed before member delete"));

        // Capture roster on attempt 1.
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, fp.node_id.as_str(), &lost, &lost_runtime, || live_roster(&records))
            .await
            .unwrap();
        accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), &lost, &lost_runtime, || live_roster(&records))
            .await
            .unwrap();

        // While the primary is reached (not lost), no Exited roster receipt exists and the resume
        // amendment is refused by name.
        let reached_refusal = amend_roster(
            &builds,
            &key(2),
            fp.node_id.as_str(),
            &RosterAmended {
                predecessor: lost.clone(),
                final_primary: survivor.clone(),
                final_runtime: survivor_runtime.clone(),
            },
        )
        .await;

        // Once the primary is lost, `resume_after_loss` runs `delete_explicit` on the lost primary,
        // recording its verdict (`Some("exited")`) as its `TerminateRuntime` roster receipt under
        // `node.delete:path `mesh2.admin`, name `1``, then appends `RosterAmended`.
        let steps_1 = builds.read_build(&build_id).await.unwrap().steps;
        let adm_1 = admitted_of(&steps_1, &outer).unwrap().unwrap();
        let mut lost_for_delete = lost_node.clone();
        lost_for_delete.status = NodeStatus::PendingReconnect;
        let resumed = resume_after_loss(
            &builds,
            &fleet,
            Some(&row),
            &key(2),
            fp.node_id.as_str(),
            &adm_1,
            &survivor,
            &survivor_runtime,
            || async {
                pipeline
                    .delete_explicit(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(lost_for_delete).node_id),
                        build_id: build_id.clone(),
                        attempt: 2,
                        node: lost_for_delete,
                        handle: handles[&final_admin.node_id].clone(),
                        unreached: true,
                    })
                    .await
                    .map(|_| ())
                    .map_err(rdm_admin::executor::RunEnd::from)
            },
        )
        .await
        .expect("resume_after_loss records the lost primary's verdict receipt and amends the roster");

        // Member delete under the amended mesh-primary (path `mesh2.admin`, name `2`), then `mesh.deleted` handoff
        // validation and Dead mesh row commit.
        let steps_2 = builds.read_build(&build_id).await.unwrap().steps;
        let roster_receipt = rdm_admin::mesh_delete::exit_receipts(&steps_2, std::slice::from_ref(&lost))
            .remove(&(lost.node_id.clone(), lost.incarnation_id.clone()))
            .expect("the lost primary's TerminateRuntime receipt is its roster receipt");
        let adm_2 = admitted_of(&steps_2, &outer).unwrap().unwrap();
        let leaving = accepted_row(&steps_2, &outer, STEP_LEAVING_ROSTER).unwrap().unwrap().roster;
        let mp2 = amended_roster(&leaving, &steps_2, &outer).unwrap();
        let records2 = view.holds(&records);
        let manifest = members_exited(&builds, &key(2), other_admin.node_id.as_str(), &mp2, &survivor, survivor_runtime.clone(), &records2, &[], |n: Node| {
            let (pipeline, handles, build_id) = (&member_pipeline, &handles, &build_id);
            async move {
                let node_id = n.node_id.clone();
                pipeline
                    .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id),
                        build_id: (*build_id).clone(),
                        attempt: 2,
                        node: n,
                        handle: handles[&node_id].clone(),
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
        record_completion(&builds, &key(2), fp.node_id.as_str(), &validated).await.unwrap();
        drop(_in_root);

        // Separate `node.delete` of the final mesh-primary (path `mesh2.admin`, name `2`) in its own Build and trace,
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
        let delete_operation = rdm_core::OperationId::node("node.delete", &other_admin.node_id);
        let delete_root = tracing::info_span!("cell.mesh-primary-delete", build_id = %delete_build.log(), operation = %delete_operation.action, operation_subject = ?delete_operation.subject, operation_step = ?delete_operation.recipe_step, operation_occurrence = ?delete_operation.occurrence);
        let delete_trace_id = trace_id(&delete_root);
        let _in_delete = delete_root.enter();
        writer_topo.write().await.nodes.iter_mut().filter(|n| n.name == survivor_node.name).for_each(|n| {
            n.status = NodeStatus::Leaving;
            n.stopped = true;
        });
        let terminal = pipeline
            .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(Node { stopped: true, ..survivor_node.clone() }).node_id),
                build_id: delete_build.clone(),
                attempt: 1,
                node: Node { stopped: true, ..survivor_node.clone() },
                handle: handles[&other_admin.node_id].clone(),
                unreached: false,
            })
            .await
            .expect("the mesh-primary's separate node.delete completes");
        let all = mesh_left_proof(&validated, &terminal).expect("MeshLeft proof covers all 4 roster births");
        mesh_left_rows(Some(&rows), &mesh_storage, &Default::default(), &declared, fp.node_id.as_str(), &key(2), "mesh2", &all.receipts)
            .await
            .expect("mesh row Dead is written");
        let survivor_row = rdm_admin::storage::NodesStorage::node(&*storage, &survivor_node.node_id).await.unwrap().expect("mesh-primary row exists");
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
        refused.contains(final_admin.node_id.as_str()) && refused.contains("not proven exited"),
        "refusal names the primary that is not lost: {refused}"
    );
    assert_eq!(resumed, Amended::Applied);
    assert_eq!(
        roster_receipt.verdict.as_deref(),
        Some("exited"),
        "the lost primary's verdict is recorded as its roster receipt: {roster_receipt:?}"
    );
    assert_eq!(
        manifest.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(),
        [&failing, &exiting, &final_admin].iter().map(|n| (n.node_id.to_string(), n.incarnation_id.as_ref().unwrap().to_string())).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        all.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(),
        [&failing, &exiting, &other_admin, &final_admin].iter().map(|n| (n.node_id.to_string(), n.incarnation_id.as_ref().unwrap().to_string())).collect::<BTreeSet<_>>()
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
            .any(|s| s["attributes"]["operation"] == outer.action && s["attributes"]["operation_subject"] == format!("{:?}", outer.subject)),
        "mesh.delete Build span correlates Build, attempt and mesh.delete:<mesh_id>"
    );
    assert!(
        !on_trace(&trace_id, "rdm.node_admin.primary.update.via-primary-update").is_empty(),
        "fabric-primary change runs on the same trace as mesh.delete"
    );
    for member in [&failing, &exiting] {
        let member_op = rdm_core::OperationId::node("node.delete", &member.node_id);
        assert!(
            on_trace(&trace_id, "rdm.node_admin.deployment.update.via-pipeline")
                .iter()
                .any(|s| s["attributes"]["pipeline"] == "delete" && s["attributes"]["node_path"] == member.name.path && s["attributes"]["node_name"] == member.name.name)
                && on_trace(&trace_id, "rdm.node_admin.node.delete.via-node-row")
                    .iter()
                    .any(|s| s["attributes"]["operation"] == member_op.action && s["attributes"]["operation_subject"] == format!("{:?}", member_op.subject) && s["attributes"]["node_id"] == member.node_id.as_str()),
            "member delete correlates node ID {} on the outer trace", member.node_id
        );
    }
    assert!(
        on_trace(&trace_id, "rdm.node_admin.mesh.update.via-mesh-deleted-receipt")
            .iter()
            .any(|s| s["attributes"]["operation"] == outer.action && s["attributes"]["operation_subject"] == format!("{:?}", outer.subject) && s["attributes"]["roster"] == "4"),
        "mesh.deleted span correlates the outer operation and complete roster"
    );
    assert!(
        on_trace(&delete_trace_id, "rdm.node_admin.deployment.update.via-pipeline")
            .iter()
            .any(|s| s["attributes"]["pipeline"] == "delete" && s["attributes"]["node_path"] == other_admin.name.path && s["attributes"]["node_name"] == other_admin.name.name),
        "mesh-primary delete runs node.delete with the fabric-primary as executor on its own Build and trace"
    );
    assert!(
        on_trace(&delete_trace_id, "rdm.node_admin.node.delete.via-node-row")
            .iter()
            .any(|s| s["attributes"]["node_id"] == other_admin.node_id.as_str() && s["attributes"]["operation"] == "node.delete" && s["attributes"]["operation_subject"] == format!("{:?}", rdm_core::OperationId::node("node.delete", &other_admin.node_id).subject) && s["attributes"]["outcome"] == "row-deleted"),
        "mesh-primary delete writes the mesh-primary row Deleted on its own trace"
    );
}

