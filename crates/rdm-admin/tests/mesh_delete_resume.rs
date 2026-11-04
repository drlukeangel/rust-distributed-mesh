//! The mesh-delete resumption (i143.e12.s39, s49; mesh-delete.md steps 1, 3 and "Resume after the
//! primary's loss"): the deleting mesh-primary's member workflows are the product's
//! (`mesh_delete::members_exited` over each member's node.delete, `DeploymentPipeline::delete`); the
//! provider, the Build state and the view are fixtures.
//!
//! mesh2 leaves: two ordinary nodes, one other node-admin and the final primary path `mesh2.node-admin`, name `1`.
//! In run 1 one ordinary node exits and the provider cannot inspect the other after its stop
//! ladder, so the ordinary tier is incomplete and the node-admin tier never starts. The resumed
//! run sees the view as the first one left it: neither ordinary node is live any more.

use rdm_core::{FabricId, IncarnationId, LifecycleOp, MeshId, NodeId};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildProjection, BuildStateAdapter, BuildStateError};
use rdm_admin::deployment::pipeline::{DeploymentPipeline, LaunchTemplate, LifecycleEvents, NodeObserver, NoLifecycleEvents, Publication, RuntimeIdentity, TerminalReceipt, Timeouts, TopologySink};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::join::Joins;
use rdm_admin::mesh_delete::{await_mesh_deleted, MeshDeleteView, MESH_DELETED_WAIT, record_completion, replayed_handoff, roster_records, roster_rows_read, mesh_left_rows, accepted_roster, first_mismatch, handoff_verdict, handoff_verdict_from_read, manifest_of, manifest_reference, members_exited, operation, owner_resume, stored_roster, Admitted, ExitManifest, HandoffVerdict, MeshDeleteKey, OwnerResume, RosterBirth, STEP_ACCEPTED_ROSTER, STEP_LEAVING_ROSTER, STEP_MESH_DELETED, STEP_OTHER_MEMBERS_EXITED};
use rdm_core::status::StatusReply;
use rdm_admin::model::{DeploymentId, Fabric, Mesh, Node, NodeStatus, PathName, ProviderKind, ScopeStatus};
use rdm_admin::node_rows::{RowFor, RowCreated, NodeRows, RowRefused};
use rdm_admin::status_rpc::Declared;
use rdm_admin::storage::{MemoryMeshStorage, MeshStorage};
use rdm_admin::storage::{MemoryNodesStorage, NodeRecord};
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

fn node_at(path: &str, name: &str, mesh: &str, r#type: &str) -> Node {
    let mut n = Node::allocated(PathName::new(path, name).unwrap(), mesh, r#type);
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
            failing: node_at("mesh2.node", "1", "mesh2", "node"),
            exiting: node_at("mesh2.node", "2", "mesh2", "node"),
            other_admin: node_at("mesh2.node-admin", "2", "mesh2", "node-admin"),
            final_admin: node_at("mesh2.node-admin", "1", "mesh2", "node-admin"),
            fp: node_at("mesh1.node-admin", "1", "mesh1", "node-admin"),
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
    let original = node_at("mesh2.node", "north", "mesh2", "node");
    let mut renamed = original.clone();
    { let at = PathName::new("mesh2.node", "north-old").unwrap(); renamed.path = at.path; renamed.name = at.name; }
    let successor = node_at("mesh2.node", "north", "mesh2", "node");
    let mut newer = original.clone();
    newer.incarnation_id = Some(IncarnationId::mint());
    let view = View::default();
    view.publish(renamed.clone());
    view.remove_birth(&original.location(), &original.node_id, original.incarnation_id.as_ref().unwrap());
    assert!(view.holds(&[renamed]).is_empty());
    view.publish(newer.clone());
    view.publish(successor.clone());
    view.remove_birth(&original.location(), &original.node_id, original.incarnation_id.as_ref().unwrap());
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
/// target by `executor` under `key`'s Build (node-delete.md "Crashed target"): what the
/// fabric-primary's node.delete of a lost primary records, whose `TerminateRuntime` is the birth's
/// roster receipt (mesh-delete.md "Resume after the primary's loss" step 1).
fn executor_birth(subject: &RosterBirth) -> rdm_core::framing::Birth {
    rdm_core::framing::Birth { node_id: NodeId::parse(&subject.node_id).unwrap(), incarnation: subject.birth().unwrap() }
}

async fn node_delete_receipts(builds: &dyn BuildStateAdapter, key: &MeshDeleteKey, _executor: &rdm_core::framing::Birth, subject: &RosterBirth, terminal: &TerminalReceipt) {
    let subject_birth = executor_birth(subject);
    assert_eq!(terminal.node_id, subject.node_id);
    assert_eq!(terminal.incarnation_id, subject.incarnation_id);
    let operation = rdm_core::OperationId::node("node.delete", &subject_birth.node_id);
    for step in ["NodeDeleting", "TerminateRuntime", "NodeDeleted", "RemoveTopologyMembership", "Complete"] {
        builds
            .write_step(&rdm_admin::build_state::StepWrite { build_id: key.build_id.clone(), mesh: None, operation: operation.clone(), step: step.into(), outcome: rdm_admin::build_state::StepOutcome::Complete })
            .await
            .unwrap();
    }
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
        let mut row = rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new(n.path.clone(), n.name.clone()).unwrap(), &n.r#type, n.node_id.clone(), rdm_core::IncarnationId::mint(), NodeStatus::Ready);
        if let Some(incarnation) = &n.incarnation_id {
            row.birth_mut(incarnation, &n.location());
        }
        storage.put_node(&row).await.unwrap();
        assert!(matches!(rows.create_row(&n.location(), &n.mesh.clone(), &n.r#type, &n.node_id, &rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&n.location())), None, None, RowFor::Create).await, Ok(RowCreated::HeldByNode(_))), "the writer holds {:?}'s row", n.name);
    }
}

/// The roster of mesh2 (`mesh_id`) as the node records of `storage` hold it at the step, over the step
/// events of `build_id` (D67: the topology of the moment is the records; a birth this Build's own
/// node.delete deleted keeps its `Deleted` record).
async fn records_roster(storage: &MemoryNodesStorage, builds: &dyn BuildStateAdapter, build_id: &BuildId, mesh_id: &MeshId) -> Result<Vec<RosterBirth>, String> {
    let steps = builds.read_build(build_id).await.map_err(|e| e.to_string())?.steps;
    let rows = rdm_admin::storage::NodesStorage::nodes(storage, "mesh2").await.map_err(|e| e.to_string())?;
    Ok(stored_roster(&rows, "mesh2", mesh_id, &steps))
}

fn artifact_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cells/mesh_delete_resume")
}

/// CONTRACT: a resumed mesh leave runs only the members without an Exited receipt and proves the
/// whole roster. Run 1: one ordinary node exits, the other's exit cannot be proven, so
/// the ordinary tier is incomplete and the node-admin tier does not start. Run 2 (the view no
/// longer holds the exited node, and the unproven node, whose stop-node was never admitted, is not
/// shown stopping: states.md "Who sets each state") keeps the roster both sides accepted at admission, keeps
/// the exited node's receipt and does not run it again, runs the unproven node and then the other
/// node-admin; the exit manifest and the MeshLeft proof set cover every accepted birth. Every
/// step is written as an `rdm.build.step` event of the run's step log.
/// R-MN1 (mesh-delete.md step 3): each member's row is written Deleted by its own node.delete's
/// `NodeDeleted` step, its node id and config kept, and never the primary's own; run 2, which
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
    let final_runtime = RuntimeIdentity::of(&handles[&final_admin.node_id].fact().unwrap());
    let calls: Mutex<Vec<(u32, String)>> = Mutex::new(Vec::new());
    let storage = Arc::new(MemoryNodesStorage::default());
    let final_id = final_admin.node_id.clone();
    let mut topology = writer_view(&records, None, None);
    topology.nodes.iter_mut().find(|n| n.node_id == final_id).unwrap().is_primary = true;
    let rows = Arc::new(NodeRows::new(final_admin.location(), final_id, Arc::new(RwLock::new(topology)), storage.clone()));
    let view = View::with_rows(rows.clone());

    let (first, fp1, fp2, mp1, mp2, second, records2, trace_id) = cap.run(async {
        rows_written(&storage, &rows, &records).await;
        let builds = crate::stepping(&NodeId::mint(), "mesh2");
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/testkit-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { mesh: "mesh2".into(), path: final_admin.path.clone(), name: final_admin.name.clone(), node_id: final_admin.node_id.clone(), incarnation: final_admin.incarnation_id.clone().unwrap() },
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
        let shutdown = |run: u32| {
            let (pipeline, handles, calls, build_id) = (&pipeline, &handles, &calls, &build_id);
            move |n: Node| async move {
                let node_id = n.node_id.clone();
                calls.lock().unwrap().push((run, node_id.to_string()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: Some((*build_id).clone()), node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-delete-resume", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Run 1: both sides read the roster from mesh2's node records; the ordinary tier is incomplete.
        let at_step = records_roster(&storage, &builds, &build_id, &mesh_id).await;
        let fp1 = accepted_roster(&builds, &key, STEP_ACCEPTED_ROSTER, fp.node_id.as_str(), || at_step.clone()).await.unwrap();
        let mp1 = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), || at_step).await.unwrap();
        let first = members_exited(&builds, &key, final_admin.node_id.as_str(), &mp1, &me, final_runtime.clone(), &records, shutdown(1)).await;

        // Run 2 reads the view the first one left: the node its node.delete removed is gone from the
        // view, and its record stands Deleted by this Build's own node.delete.
        let records2: Vec<Node> = view.holds(&records);
        let at_step = records_roster(&storage, &builds, &build_id, &mesh_id).await;
        let fp2 = accepted_roster(&builds, &key, STEP_ACCEPTED_ROSTER, fp.node_id.as_str(), || at_step.clone()).await.unwrap();
        let mp2 = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), || at_step).await.unwrap();
        let second = members_exited(&builds, &key, final_admin.node_id.as_str(), &mp2, &me, final_runtime.clone(), &records2, shutdown(2)).await;
        (first, fp1, fp2, mp1, mp2, second, records2, trace_id)
    });
    let spans = cap.spans();
    std::fs::create_dir_all(artifact_dir()).unwrap();
    std::fs::write(artifact_dir().join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", artifact_dir().join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    // Run 1: the ordinary tier is incomplete by name, and the node-admin tier never started.
    let err = first.expect_err("the ordinary tier holds an unproven exited verdict");
    assert!(err.contains("tier ordinary-nodes is incomplete") && err.contains(failing.node_id.as_str()), "{err}");
    let calls = calls.into_inner().unwrap();
    let ran = |run: u32| calls.iter().filter(|(a, _)| *a == run).map(|(_, n)| n.clone()).collect::<BTreeSet<_>>();
    assert_eq!(ran(1), BTreeSet::from([failing.node_id.to_string(), exiting.node_id.to_string()]), "run 1 ran the ordinary tier only");
    assert!(records2.iter().all(|n| n.node_id != exiting.node_id), "the resumed run's view no longer holds the node run 1's node.delete removed: {records2:?}");
    // This cell's observer holds no Node RPC client, so the unproven node's stop-node was never sent: nothing
    // set it stopping (states.md "Who sets each state": stopping on the stop call's admission, never before).
    let unproven = records2.iter().find(|n| n.node_id == failing.node_id).expect("the resumed run's view still holds the unproven node");
    let held = records.iter().find(|n| n.node_id == failing.node_id).unwrap();
    assert_eq!(unproven.status, held.status, "a member whose stop-node was never admitted keeps its status, never stopping: {unproven:?}");

    // The roster is mesh2's node records at the step, on both sides, in every run: the member run 1
    // deleted keeps its Deleted record.
    let accepted = sorted(fp1.clone());
    assert_eq!(accepted.len(), 4, "every birth of mesh2: {accepted:?}");
    assert_eq!(sorted(mp1), accepted, "the leaving primary reads the same roster");
    assert_eq!(sorted(fp2.clone()), accepted, "the fabric-primary's resumed run reads the same roster from the records");
    assert_eq!(sorted(mp2), accepted, "the leaving primary's resumed run reads the same roster from the records");

    // Run 2: the exited member is not run again; the rest are.
    let manifest = second.unwrap_or_else(|e| panic!("the resumed run completes: {e}"));
    assert_eq!(ran(2), BTreeSet::from([failing.node_id.to_string(), other_admin.node_id.to_string()]), "run 2 runs only the members with no Exited receipt");
    assert_eq!(fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == exiting.node_id.as_str()).count(), 1, "the exited member's runtime was stopped once");
    assert_eq!(manifest.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(), BTreeSet::from([&failing, &exiting, &other_admin].map(|n| (n.node_id.to_string(), n.incarnation_id.as_ref().unwrap().to_string()))), "the manifest carries every other member's receipt");
    assert_eq!(sorted(manifest.roster.clone()), accepted, "the manifest names the whole roster");
    assert_eq!(first_mismatch(&manifest, &fp2, &me, false), None, "the fabric-primary's handoff validation accepts it");

    // MeshLeft: with the final primary's own exit, the proof set covers every accepted birth.
    let mut all = manifest.clone();
    all.final_runtime = None;
    all.receipts.push(TerminalReceipt { path: me.node.path.clone(), name: me.node.name.clone(), node_id: me.node_id.clone(), incarnation_id: me.incarnation_id.clone(), runtime: Some(final_runtime.clone()), exit_code: Some(0) , verdict: None, data_dir: None });
    assert_eq!(first_mismatch(&all, &fp2, &me, true), None, "MeshLeft covers the whole roster");

    // The trace: both runs' tiers and the retained receipt, under the cell's one trace.
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let tiers: Vec<(String, String)> = named("rdm.node_admin.mesh.update.via-leave-tier").iter().map(|s| (s["attributes"]["tier"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect();
    assert_eq!(tiers, vec![("ordinary-nodes".into(), "incomplete".into()), ("ordinary-nodes".into(), "complete".into()), ("node-admins".into(), "complete".into())], "run 1's ordinary tier ends incomplete, run 2 completes both tiers");
    let retained = named("rdm.node_admin.mesh.resolve.via-retained-receipts");
    assert!(retained.iter().any(|s| s["attributes"]["retained"] == "1" && s["attributes"]["remaining"] == "2"), "run 2 kept one receipt and ran two members: {retained:?}");
    let recorded = named("rdm.node_admin.mesh.update.via-exit-manifest-recorded");
    assert!(recorded.iter().any(|s| s["attributes"]["receipts"] == "3" && s["attributes"]["roster"] == "4"), "{recorded:?}");

    // R-MN1 step 3: every other member's row is Deleted, readable by its node id,
    // each written by its own node.delete's NodeDeleted step; the exited node's in run
    // 1, which run 2 kept. The final primary's own row is not the mesh-primary's to delete (step 7
    // is the fabric-primary's).
    let read = |node_id: &NodeId| {
        let storage = storage.clone();
        let node_id = node_id.clone();
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async move { rdm_admin::storage::NodesStorage::node(storage.as_ref(), &node_id).await.unwrap() })
    };
    for n in records.iter().filter(|n| n.node_id != final_admin.node_id) {
        let row = read(&n.node_id).unwrap_or_else(|| panic!("{:?} has a row", n.name));
        assert_eq!((row.status, &row.node_id), (NodeStatus::Deleted, &n.node_id), "{:?}'s row is Deleted with its node id: {row:?}", n.name);
    }
    let last = read(&final_admin.node_id).unwrap();
    assert_ne!(last.status, NodeStatus::Deleted, "the final primary's row is not deleted at step 3: {last:?}");
    let deleted = named("rdm.node_admin.node.delete.via-node-row");
    let exited_row: Vec<&Value> = deleted.iter().filter(|s| s["attributes"]["node_id"] == exiting.node_id.as_str()).collect();
    assert!(exited_row.iter().any(|s| s["attributes"]["outcome"] == "row-deleted"), "the retained member's row is written Deleted from the writer's record: {deleted:#?}");
    for node_id in [&failing.node_id, &other_admin.node_id] {
        assert!(deleted.iter().any(|s| s["attributes"]["node_id"] == node_id.as_str() && s["attributes"]["outcome"] == "row-deleted"), "{node_id}'s row is written Deleted: {deleted:#?}");
    }
    assert!(!deleted.iter().any(|s| s["attributes"]["node_id"] == final_admin.node_id.as_str()), "{deleted:#?}");
    eprintln!("trace {trace_id}: rdm.node_admin.node.delete.via-node-row node_id={} outcome=row-deleted config_from=writer-record (retained receipt)", exiting.node_id);
}

/// CONTRACT: an incomplete tier is refused by name, never awaited. Run 1 leaves the ordinary
/// tier incomplete (one ordinary node exits, the other's exit cannot be proven; the node-admin tier
/// never starts), so the leaving primary records no exit manifest. The fabric-primary's verdict on
/// the handoff over the sender's Build facts (as the wire carries them) refuses it
/// `RejectedIncompleteManifest` naming every roster birth without an Exited receipt (the unproven
/// ordinary node and the other node-admin), their count, and not the one that exited nor the final
/// primary (node-rpc-envelope.md, "Mesh delete command pair": "A stale/conflicting workflow identity or
/// incomplete manifest is refused by name with a typed Status refusal naming the mismatch"). What
/// must NOT happen: `NotReady`, the answer of a readiness gate that is not open yet.
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
    let final_runtime = RuntimeIdentity::of(&handles[&final_admin.node_id].fact().unwrap());
    let view = View::default();

    let (tier, verdict, trace_id) = cap.run(async {
        let builds = crate::stepping(&NodeId::mint(), "mesh2");
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/testkit-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { mesh: "mesh2".into(), path: final_admin.path.clone(), name: final_admin.name.clone(), node_id: final_admin.node_id.clone(), incarnation: final_admin.incarnation_id.clone().unwrap() },
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
        let root = tracing::info_span!("cell.mesh-deleted-incomplete", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let roster = accepted_roster(&builds, &key, STEP_ACCEPTED_ROSTER, fp.node_id.as_str(), || live_roster(&records)).await.unwrap();
        let mp = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), || live_roster(&records)).await.unwrap();
        let tier = members_exited(&builds, &key, final_admin.node_id.as_str(), &mp, &me, final_runtime.clone(), &records, |n: Node| {
            let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
            async move {
                let node_id = n.node_id.clone();
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: Some((*build_id).clone()), node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        })
        .await;
        // The handoff the leaving primary sends for the incomplete run, judged over its facts.
        let facts = builds.read_build(&key.build_id).await.unwrap().steps;
        let admitted = Admitted { roster, final_primary: me.clone(), final_runtime: final_runtime.clone() };
        let verdict = handoff_verdict(&facts, &key, &admitted, &manifest_reference(key.build_id.as_str(), &outer, false));
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

/// CONTRACT: mesh.delete ends at mesh.deleted (mesh-delete.md steps 5-7; R-MD1 items 2-3). Run
/// 1 goes to the validated handoff: both sides record the roster, the member workflows record the
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
    let final_runtime = RuntimeIdentity::of(&handles[&final_admin.node_id].fact().unwrap());
    let view = View::default();

    let out = cap.run(async {
        let builds = crate::stepping(&NodeId::mint(), "mesh2");
        let (build_id, delete_build) = (BuildId::mint(), BuildId::mint());
        for b in [&build_id, &delete_build] {
            builds
                .publish_accepted(&BuildAccepted { build_id: b.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 0 })
                .await
                .unwrap();
        }
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/testkit-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { mesh: final_admin.mesh.clone(), path: final_admin.path.clone(), name: final_admin.name.clone(), node_id: final_admin.node_id.clone(), incarnation: final_admin.incarnation_id.clone().unwrap() },
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
        let root = tracing::info_span!("cell.mesh-delete-completion", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let roster = accepted_roster(&builds, &key, STEP_ACCEPTED_ROSTER, fp.node_id.as_str(), || live_roster(&records)).await.unwrap();
        let admitted = Admitted { roster, final_primary: me.clone(), final_runtime: final_runtime.clone() };
        let mp = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), || live_roster(&records)).await.unwrap();
        members_exited(&builds, &key, final_admin.node_id.as_str(), &mp, &me, final_runtime.clone(), &records, |n: Node| {
            let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
            async move {
                let node_id = n.node_id.clone();
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: Some((*build_id).clone()), node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        })
        .await
        .expect("every other member exits");
        // The owner's validated handoff, then mesh.deleted's completion: nothing of the final primary
        // is stopped.
        let facts = builds.read_build(&key.build_id).await.unwrap().steps;
        let HandoffVerdict::Accept(manifest) = handoff_verdict(&facts, &key, &admitted, &manifest_reference(&build_id.as_str(), &outer, false)) else { panic!("the handoff is accepted") };
        record_completion(&builds, &key, fp.node_id.as_str(), &manifest).await.expect("mesh.deleted's completion is recorded");
        let terminated_at_completion = fleet.terminated.lock().unwrap().clone();
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        let resume = owner_resume(&steps, &outer.to_string());
        let replay = replayed_handoff(&steps, &key, Some(&me), &last_record.node_id, &me.incarnation_id);
        let other = replayed_handoff(&steps, &key, Some(&me), &NodeId::mint(), "another-birth");
        let completions = steps.iter().filter(|r| r.step == STEP_MESH_DELETED).count();
        // The final primary's own node.delete, a separate Build: the ordinary workflow, live target.
        let req = rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(last_record).node_id), build_id: Some(delete_build.clone()), node: last_record.clone(), handle: handles[&final_admin.node_id].clone(), unreached: false };
        let mut fp_template = template.clone();
        fp_template.launcher = rdm_core::launch::Launcher { mesh: fp.mesh.clone(), path: fp.path.clone(), name: fp.name.clone(), node_id: fp.node_id.clone(), incarnation: fp.incarnation_id.clone().unwrap() };
        let fp_pipeline = DeploymentPipeline { template: &fp_template, ..pipeline };
        fp_pipeline.delete_node(&req).await.expect("the final primary's node.delete completes");
        // MeshLeft's proof set: every birth's exit, the final birth's from its own node.delete's
        // `TerminateRuntime` on the separate Build.
        let mut all_steps = builds.read_build(&build_id).await.unwrap().steps;
        all_steps.extend(builds.read_build(&delete_build).await.unwrap().steps);
        let left_manifest = manifest_of(&key, &manifest.roster, &me, None, &all_steps, true);
        (manifest, terminated_at_completion, resume, replay, other, completions, left_manifest, trace_id)
    });
    let (manifest, terminated_at_completion, resume, replay, other, completions, all, trace_id) = out;
    let spans = cap.spans();
    let dir = artifact_dir().join("mesh-deleted");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    assert!(!terminated_at_completion.contains(&final_admin.node_id.to_string()), "mesh.delete stops nothing of its final primary: {terminated_at_completion:?}");
    assert_eq!(manifest.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(), BTreeSet::from([&failing, &exiting, &other_admin].map(|n| (n.node_id.to_string(), n.incarnation_id.as_ref().unwrap().to_string()))), "mesh.deleted covers every birth but the final primary");
    assert_eq!(resume, OwnerResume::Deleted, "a resumed owner reads the completion and calls nothing");
    assert_eq!(replay, Some(StatusReply::AlreadyApplied), "a repeated handoff of the final birth is AlreadyApplied");
    assert!(matches!(&other, Some(StatusReply::RejectedUnmatchedCompletion { field, .. }) if field == "final_primary"), "a handoff naming another birth is refused by name: {other:?}");
    assert_eq!(completions, 1, "the replay recorded no second completion receipt");
    assert_eq!(first_mismatch(&all, &all.roster, &me, true), None, "MeshLeft covers every birth, the final primary included, its exit from its own node.delete: {all:?}");
    assert_eq!(all.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(), BTreeSet::from([&failing, &exiting, &other_admin, &final_admin].map(|n| (n.node_id.to_string(), n.incarnation_id.as_ref().unwrap().to_string()))));
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
/// a mesh with no primary) and R-MD1 item 2 (mesh-left ensures the mesh row deleted). While the final
/// primary holds its role live, the fabric-primary writes none of mesh2's rows and a second claim of
/// path `mesh2.node-admin`, name `1` is refused `name-held`; once it is stopped (its role held) the
/// fabric-primary writes its row Deleted, node id and config kept, as its node.delete's NodeDeleted
/// step does; mesh-left puts the mesh row deleted as acting primary when no Dead row is held; and only
/// then a new birth claims path `mesh2.node-admin`, name `1` over the Deleted row (`row-seeded`, seeded from it), its
/// row carrying its create spec's config, here none (Q166: a plain create's config is its spec's; only a
/// recovery's and a successor's claim inherit).
#[test]
fn the_final_primarys_node_delete_writes_its_row_deleted_once_it_is_stopped_and_only_then_the_name_is_claimed_again() {
    let cap = capture();
    let fp = {
        let mut n = node_at("mesh1.node-admin", "1", "mesh1", "node-admin");
        n.is_fabric_primary = true;
        n
    };
    let last = node_at("mesh2.node-admin", "1", "mesh2", "node-admin");
    let storage = Arc::new(MemoryNodesStorage::default());
    let status = MemoryMeshStorage::default();
    let declared = Mutex::new(Declared::default());
    let view = Arc::new(RwLock::new(writer_view(std::slice::from_ref(&last), None, Some(fp.clone()))));
    let rows = NodeRows::new(fp.location(), fp.node_id.clone(), view.clone(), storage.clone());
    let mesh_id = MeshId::mint();
    let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: BuildId::mint(), operation: operation(&mesh_id) };
    let deleting = rdm_core::OperationId::node("node.delete", &last.node_id);
    let (second, live, stopped, after, row, statuses, again, trace_id) = cap.run(async {
        let root = tracing::info_span!("cell.final-primary-row-deleted", operation = %key.operation.action, operation_subject = ?key.operation.subject, operation_step = ?key.operation.step, operation_occurrence = ?key.operation.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        // The fabric-primary claimed the mesh's first admin's name when it created it, before the
        // mesh had a primary.
        rows_written(&storage, &rows, std::slice::from_ref(&last)).await;
        // A live path `mesh2.node-admin`, name `1` holds its name: a second birth's claim is refused.
        let second = rows.create_row(&last.location(), &last.mesh.clone(), &last.r#type, &NodeId::mint(), &rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh2.node-admin", "1").unwrap())), None, None, RowFor::Create).await;
        // In its role and live: the fabric-primary is not the writer of mesh2's rows.
        view.write().await.nodes.iter_mut().filter(|n| n.node_id == last.node_id).for_each(|n| n.is_primary = true);
        let live = rows.deleted(&last, &deleting).await;
        // Stopped, its role still held (R-EL1): no primary for the writer rule (R-NS1 item 2).
        view.write().await.nodes.iter_mut().filter(|n| n.node_id == last.node_id).for_each(|n| {
            n.status = NodeStatus::Leaving;
            n.stopped = true;
        });
        let stopped = rows.deleted(&last, &deleting).await;
        let exited = rdm_admin::deployment::pipeline::TerminalReceipt { path: last.path.clone(), name: last.name.clone(), node_id: last.node_id.to_string(), incarnation_id: last.incarnation_id.clone().unwrap().to_string(), runtime: None, exit_code: Some(0) , verdict: None, data_dir: None };
        let after = mesh_left_rows(Some(&rows), &status, &Default::default(), &declared, fp.node_id.as_str(), &key, "mesh2", std::slice::from_ref(&exited)).await;
        let row = rdm_admin::storage::NodesStorage::node(storage.as_ref(), &last.node_id).await.unwrap();
        let statuses = status.meshes().await.unwrap().into_iter().map(|r| (r.mesh_id.clone(), r.state)).collect::<std::collections::HashMap<_, _>>();
        let dead_held = status.mesh(&mesh_id.clone()).await.unwrap().is_some_and(|r| r.state == MeshState::Deleted);
        // mesh2 created again: its first admin's claim wins over the Deleted row.
        let again = rows.create_row(&last.location(), &last.mesh.clone(), &last.r#type, &NodeId::mint(), &rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh2.node-admin", "1").unwrap())), None, None, RowFor::Create).await;
        (second, live, stopped, after, row, (statuses, dead_held), again, trace_id)
    });
    let (statuses, dead_held) = statuses;
    let spans = cap.spans();
    let dir = artifact_dir().join("final-primary-row-deleted");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());

    assert!(matches!(&second, Err(RowRefused::NameHeld(h)) if h.by == last.node_id), "a live path `mesh2.node-admin`, name `1` refuses a second claim: {second:?}");
    assert!(live.is_err(), "while the final primary holds its role live the fabric-primary writes none of mesh2's rows: {live:?}");
    stopped.unwrap_or_else(|e| panic!("once stopped, the fabric-primary writes the final primary's row Deleted as acting primary: {e}"));
    after.unwrap_or_else(|e| panic!("mesh-left writes its rows: {e}"));
    let row = row.expect("the final primary's row stays, readable by its node id");
    assert_eq!((row.status, &row.node_id), (NodeStatus::Deleted, &last.node_id), "{row:?}");
    assert!(dead_held, "the mesh's status is deleted on mesh2 (mesh.storage, R-MD1)");
    assert_eq!(statuses.get(&mesh_id), Some(&MeshState::Deleted), "mesh2's one status fact is its Dead, on mesh2: {statuses:?}");
    assert_eq!(declared.lock().unwrap().meshes.get(&mesh_id), Some(&MeshState::Deleted), "and held as applied");
    match &again {
        Ok(RowCreated::Written { row, seeded_from }) => {
            assert_eq!(seeded_from.as_ref(), None, "a plain create names no seed: the Deleted row's location is not provenance");
            assert_eq!(row.initial_properties.as_ref().map(|p| p.config.clone()), Some(Default::default()), "its create spec's config, none: a plain create inherits no config (Q166)");
        }
        other => panic!("the recreated mesh's admin claims path `mesh2.node-admin`, name `1` over the Deleted row: {other:?}"),
    }
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let deleted: Vec<String> = named("rdm.node_admin.node.delete.via-node-row").iter().filter(|s| s["attributes"]["subject_path"] == last.path && s["attributes"]["subject_name"] == last.name).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect();
    assert_eq!(deleted, ["not-mesh-primary", "row-deleted"], "refused while the final primary is live and primary, written once it is stopped: {deleted:?}");
    let dead = named("rdm.node_admin.mesh.update.via-deleted-row");
    assert!(dead.iter().any(|s| s["attributes"]["mesh_id"] == mesh_id.as_str() && s["attributes"]["outcome"] == "applied" && s["attributes"]["writer"] == "acting-primary"), "no Dead row held: the fabric-primary writes Dead as acting primary: {dead:#?}");
    let claims: Vec<String> = named("rdm.node_admin.node.create.via-node-row").iter().filter(|s| s["attributes"]["subject_path"] == last.path && s["attributes"]["subject_name"] == last.name).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).collect();
    assert_eq!(claims, ["row-held-by-node", "name-held", "row-written"], "a plain create's claim names no seed: {claims:?}");
    eprintln!("trace {trace_id}: delete.via-node-row not-mesh-primary then row-deleted, mesh.update.via-deleted-row applied acting-primary, via-node-row claim row-seeded seeded_from={}", last.node_id);
}


/// The exact Exited receipt the provider's inspection of `b`'s runtime `runtime` proves.
fn exited(b: &RosterBirth, runtime: &RuntimeIdentity) -> TerminalReceipt {
    TerminalReceipt { path: b.node.path.clone(), name: b.node.name.clone(), node_id: b.node_id.clone(), incarnation_id: b.incarnation_id.clone(), runtime: Some(runtime.clone()), exit_code: Some(137) , verdict: None, data_dir: None }
}

/// CONTRACT: mesh-delete.md draws each member's end at the mesh-primary as "member row deleted;
/// node.deleted", then "mesh topology: NodeDeleted in MeshTopology.deleted", and its resume keeps a member's
/// finished delete ("Resume after the primary's loss" step 3: "Completed member receipts stand").
/// Run 1, under the final primary path `mesh2.node-admin`, name `1`: path `mesh2.node`, name `2`'s whole delete runs to
/// `Complete` and the lost primary publishes its NodeDeleted, which no other admin hears (the
/// primary is lost before its frame lands, so its publications are its own); path `mesh2.node`, name `1`'s exit
/// cannot be proven, so the ordinary tier is incomplete. The resume runs the lost primary's
/// node.delete as a crashed target, and path `mesh2.node-admin`, name `2` runs the leave over the mesh's records. Run 2,
/// under that survivor: path `mesh2.node`, name `2` keeps its receipts and is not run again (no step of its delete
/// executes, its runtime is terminated once), and nothing of it is written or published again: its
/// row `Deleted` and its NodeDeleted were its node.delete's, once (node-delete.md steps 3-4), and a
/// peer that never heard that NodeDeleted keeps the birth until mesh-left; path `mesh2.node`, name `1`'s delete
/// runs and publishes its own. The lost primary's NodeDeleted is its own node.delete's, the
/// fabric-primary's, never the survivor's.
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
    let (lost_runtime, survivor_runtime) = (RuntimeIdentity::of(&handles[&final_admin.node_id].fact().unwrap()), RuntimeIdentity::of(&handles[&other_admin.node_id].fact().unwrap()));
    let view = View::default();
    let calls: Mutex<Vec<(u32, String)>> = Mutex::new(Vec::new());
    // Each primary's own publications: the lost primary's never reach the survivor.
    let (lost_published, amended_published) = (Departures::default(), Departures::default());

    let (first, second, steps, trace_id) = cap.run(async {
        let builds = crate::stepping(&NodeId::mint(), "mesh2");
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/testkit-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { mesh: "mesh2".into(), path: final_admin.path.clone(), name: final_admin.name.clone(), node_id: final_admin.node_id.clone(), incarnation: final_admin.incarnation_id.clone().unwrap() },
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
        amended_template.launcher = rdm_core::launch::Launcher { mesh: other_admin.mesh.clone(), path: other_admin.path.clone(), name: other_admin.name.clone(), node_id: other_admin.node_id.clone(), incarnation: other_admin.incarnation_id.clone().unwrap() };
        let amended_pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &amended_published, builds: &builds, template: &amended_template, timeouts: Timeouts::default() };
        let shutdown_1 = |n: Node| {
            let (pipeline, handles, calls, build_id) = (&lost_pipeline, &handles, &calls, &build_id);
            async move {
                let node_id = n.node_id.clone();
                calls.lock().unwrap().push((1, node_id.to_string()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: Some((*build_id).clone()), node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let shutdown_2 = |n: Node| {
            let (pipeline, handles, calls, build_id) = (&amended_pipeline, &handles, &calls, &build_id);
            async move {
                let node_id = n.node_id.clone();
                calls.lock().unwrap().push((2, node_id.to_string()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: Some((*build_id).clone()), node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-deleted-finished-under-lost", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Run 1, the lost primary's: rpc.2's delete finishes, rpc.1's exit is unproven.
        accepted_roster(&builds, &key, STEP_ACCEPTED_ROSTER, fp_birth.node_id.as_str(), || live_roster(&records)).await.unwrap();
        let mp1 = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key, final_admin.node_id.as_str(), &mp1, &lost, lost_runtime.clone(), &records, shutdown_1).await;
        // The resume: the lost primary's node.delete, then the survivor's tiers over the same births,
        // the mesh's records (D67).
        node_delete_receipts(&builds, &key, &fp_birth, &lost, &exited(&lost, &lost_runtime)).await;
        let mp2 = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, other_admin.node_id.as_str(), || live_roster(&records)).await.unwrap();
        let records2: Vec<Node> = view.holds(&records);
        let second = members_exited(&builds, &key, other_admin.node_id.as_str(), &mp2, &survivor, survivor_runtime.clone(), &records2, shutdown_2).await;
        (first, second, builds.read_build(&build_id).await.unwrap().steps, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("finished_under_lost");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    let err = first.expect_err("run 1's ordinary tier is incomplete: rpc.1's exit is unproven");
    assert!(err.contains("tier ordinary-nodes is incomplete") && err.contains(failing.node_id.as_str()), "{err}");
    second.unwrap_or_else(|e| panic!("the survivor's member workflows complete: {e}"));
    let calls = calls.into_inner().unwrap();
    let ran = |run: u32| calls.iter().filter(|(a, _)| *a == run).map(|(_, n)| n.clone()).collect::<BTreeSet<_>>();
    assert_eq!(ran(1), BTreeSet::from([failing.node_id.to_string(), exiting.node_id.to_string()]), "run 1 ran the ordinary tier");
    assert_eq!(ran(2), BTreeSet::from([failing.node_id.to_string()]), "the survivor runs only rpc.1: rpc.2's delete has every step's event and is not run again");
    let of = |node: &NodeId, step: &str| steps.iter().filter(|r| r.operation == rdm_core::OperationId::node("node.delete", node).to_string() && r.step == step && r.outcome == rdm_admin::build_state::StepOutcome::Complete).count();
    assert_eq!((of(&exiting.node_id, "NodeDeleted"), of(&exiting.node_id, "Complete")), (1, 1), "rpc.2's delete finished under the lost primary, each step's event written once");
    assert_eq!(fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == exiting.node_id.as_str()).count(), 1, "rpc.2's runtime was terminated once, in run 1");
    let lost_ops = lost_published.0.into_inner().unwrap();
    assert_eq!(lost_ops.iter().map(|op| op.node_id.to_string()).collect::<Vec<_>>(), vec![exiting.node_id.to_string()], "the lost primary published rpc.2's NodeDeleted, which the survivor never heard");
    let amended_ops = amended_published.0.into_inner().unwrap();
    let names = amended_ops.iter().map(|op| op.node_id.to_string()).collect::<BTreeSet<_>>();
    assert_eq!(names, BTreeSet::from([failing.node_id.to_string()]), "the survivor publishes the NodeDeleted of the one delete it ran, never again rpc.2's (its node.delete's, once) and never the lost primary's (its own node.delete's, the fabric-primary's): {amended_ops:?}");

    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let retained = named("rdm.node_admin.mesh.resolve.via-retained-receipts");
    assert!(retained.iter().any(|s| s["attributes"]["retained"] == "2" && s["attributes"]["remaining"] == "1"), "run 2 keeps rpc.2's delete and the lost primary's exit, and runs rpc.1: {retained:?}");
    let tier = named("rdm.node_admin.mesh.update.via-leave-tier");
    assert!(tier.iter().any(|s| s["attributes"]["tier"] == "ordinary-nodes" && s["attributes"]["members"] == "1" && s["attributes"]["retained"] == "1"), "run 2's ordinary tier runs rpc.1 and keeps rpc.2: {tier:?}");
    assert!(tier.iter().any(|s| s["attributes"]["tier"] == "node-admins" && s["attributes"]["members"] == "0" && s["attributes"]["retained"] == "1"), "the node-admin tier runs nothing and keeps the lost primary's delete: {tier:?}");
    eprintln!("trace {trace_id}: run 2 ran rpc.1 only; rpc.2's NodeDeleted written once and published once, by the lost primary");
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
    let lost = node_at("mesh2.node-admin", "1", "mesh2", "node-admin");
    let h = runtime_handles(std::slice::from_ref(&lost), 49_001)[&lost.node_id].clone();
    let fleet = Fleet { pids: [(49_001, lost.node_id.to_string())].into_iter().collect(), ..Default::default() };
    let digest_view = rdm_transport::topology::DigestView::default();
    let (without_row, with_row) = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let nodes = MemoryNodesStorage::default();
        let without_row = inspect_birth(&fleet, &digest_view, Some(&nodes), &lost).await;
        nodes.put_runtime(&RuntimeRow { r#type: lost.r#type.clone(), mesh: lost.mesh.clone(), node_id: lost.node_id.clone(), path: lost.path.clone(), name: lost.name.clone(), incarnation_id: lost.incarnation_id.clone().unwrap(), runtime: h.fact().unwrap(), data_dir: None }).await.unwrap();
        let with_row = inspect_birth(&fleet, &digest_view, Some(&nodes), &lost).await;
        (without_row, with_row)
    });
    assert!(matches!(&without_row, Finding::Unproven { reason } if reason.contains("no runtime row")), "no digest and no row: unproven, naming both: {without_row:?}");
    assert_eq!(with_row, Finding::Exited { code: Some(0) }, "the runtime row recorded at launch is what the provider inspects");
}

/// CONTRACT (R11, the leave acts on the immutable accepted roster's exact births; mesh-delete.md
/// "Resume after the primary's loss" step 3, "Failure/certainty behavior": "No timeout or gossip
/// absence proves exit"): the admin the fabric-primary created for a leaving mesh with no admin
/// left works the roster, not its view's liveness. path `mesh2.node`, name `1` was stopped by the lost primary
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

    let runtime = RuntimeIdentity::of(&handles[&final_admin.node_id].fact().unwrap());
    let manifest = cap.run(async {
        let builds = crate::stepping(&NodeId::mint(), "mesh2");
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), operation: operation(&mesh_id) };
        // The leave's records, under the cell's capture: the held record is named as it is taken.
        let worked = roster_records(&view, &roster, |b| (b.node_id == failing.node_id.as_str()).then(|| held_stopped.clone()));
        members_exited(&builds, &key, final_admin.node_id.as_str(), &roster, &me, runtime.clone(), &worked, |n: Node| {
            let runtime = RuntimeIdentity::of(&handles[&n.node_id].fact().unwrap());
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
/// leaving mesh with no admin left joined after path `mesh2.node`, name `1` stopped, so neither its view nor its
/// the topology view holds that birth. The roster birth runs from the roster itself: its member workflow is
/// handed a record naming exactly that birth (separate path/name, node id, incarnation), its exact Exited
/// receipt is in the manifest, and its row, read from the mesh's own rows before the tiers run, is
/// written Deleted with its config kept by its node.delete's NodeDeleted step. Must NOT happen: the tier refused "this view holds no record
/// of the accepted birth" (no_survivor, created path `mesh2.node-admin`, name `2`, runs 2 and 3), or a Deleted row
/// written with a config the writer never read.
#[test]
fn a_roster_birth_the_created_admin_never_held_runs_from_the_roster_and_its_row_is_written_deleted() {
    let cap = capture();
    let Members { failing, exiting, .. } = Members::new();
    let created = node_at("mesh2.node-admin", "3", "mesh2", "node-admin");
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
    let runtime = RuntimeIdentity::of(&handles[&created.node_id].fact().unwrap());
    let ran: Mutex<Vec<Node>> = Mutex::new(Vec::new());
    let (manifest, trace_id) = cap.run(async {
        // Every member's row as its create left it. The created admin's writer holds the rows of the
        // births its view names; the stopped birth's row it never held.
        for n in &all {
            storage.put_node(&rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new(n.path.clone(), n.name.clone()).unwrap(), &n.r#type, n.node_id.clone(), rdm_core::IncarnationId::mint(), NodeStatus::Ready)).await.unwrap();
        }
        for n in &records {
            assert!(matches!(rows.create_row(&n.location(), &n.mesh.clone(), &n.r#type, &n.node_id, &rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&n.location())), None, None, RowFor::Create).await, Ok(RowCreated::HeldByNode(_))), "the writer holds {:?}'s row", n.name);
        }
        let builds = crate::stepping(&NodeId::mint(), "mesh2");
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), operation: operation(&mesh_id) };
        let root = tracing::info_span!("cell.mesh-deleted-roster-birth", build_id = %build_id.log(), operation = %key.operation.action, operation_subject = ?key.operation.subject, operation_step = ?key.operation.step, operation_occurrence = ?key.operation.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        roster_rows_read(Some(&rows), created.node_id.as_str(), &key.operation, &roster, &records).await;
        // The member's node.delete: its NodeDeleted step writes the row Deleted (node-delete.md step 3)
        // from the record it was handed, then the provider's exact Exited is its receipt.
        let manifest = members_exited(&builds, &key, created.node_id.as_str(), &roster, &me, runtime.clone(), &records, |n: Node| {
            ran.lock().unwrap().push(n.clone());
            let (runtime, rows, operation) = (RuntimeIdentity::of(&handles[&n.node_id].fact().unwrap()), &rows, rdm_core::OperationId::node("node.delete", &n.node_id));
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
    assert_eq!((row.status, row.node_id.to_string()), (NodeStatus::Deleted, stopped.node_id.clone()), "its row is written Deleted: {row:?}");
    let spans = cap.spans();
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name).cloned().collect::<Vec<_>>();
    let from_roster = named("rdm.node_admin.mesh.resolve.via-roster-birth");
    assert_eq!(from_roster.iter().map(|s| s["attributes"]["member"].as_str().unwrap_or("").to_string()).collect::<Vec<_>>(), vec![failing.node_id.to_string()], "{from_roster:#?}");
    let read = named("rdm.node_admin.mesh.resolve.via-roster-row");
    assert_eq!(read.iter().map(|s| (s["attributes"]["member"].as_str().unwrap_or("").to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>(), vec![(failing.node_id.to_string(), "held".to_string())], "{read:#?}");
    let deleted = named("rdm.node_admin.node.delete.via-node-row").into_iter().filter(|s| s["attributes"]["node_id"] == failing.node_id.as_str()).collect::<Vec<_>>();
    assert!(deleted.iter().any(|s| s["attributes"]["outcome"] == "row-deleted"), "{deleted:#?}");
    println!("trace_id={trace_id} via-roster-birth={} via-roster-row={}", from_roster[0]["trace_id"], read[0]["trace_id"]);
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
    let admitted = Admitted { roster: roster.clone(), final_primary: me.clone(), final_runtime: RuntimeIdentity::of(&handles[&final_admin.node_id].fact().unwrap()) };
    let mesh_id = MeshId::mint();
    let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: BuildId::mint(), operation: operation(&mesh_id) };
    let reference = manifest_reference(key.build_id.as_str(), &key.operation, false);
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

/// A Build log whose executor is cut once: the `step` step event of `operation` is refused, as the fault
/// service's `StepEvent` cut leaves it (`FaultedBuilds::write_step` holds BEFORE the step event is
/// durable, so the step's work is done and its step event is not). Everything else reads and writes the
/// log it wraps.
struct CutBuilds<'a> {
    inner: &'a rdm_admin::build_run::FramedBuilds,
    cut: Mutex<Option<(&'static str, rdm_core::OperationId)>>,
}

#[async_trait::async_trait]
impl BuildStateAdapter for CutBuilds<'_> {
    async fn publish_accepted(&self, a: &BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(a).await
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
    async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(id).await
    }
    async fn accepted_newest_first(&self) -> Result<Vec<rdm_admin::build::BuildId>, rdm_admin::build_state::BuildStateError> {
        self.inner.accepted_newest_first().await
    }
    fn events(&self) -> std::sync::Arc<dyn rdm_admin::events_storage::EventsStorage> {
        self.inner.events()
    }
    async fn write_step(&self, w: &rdm_admin::build_state::StepWrite) -> Result<(), BuildStateError> {
        let cut = {
            let mut cut = self.cut.lock().unwrap();
            if cut.as_ref().is_some_and(|(step, operation)| *step == w.step && *operation == w.operation) {
                cut.take()
            } else {
                None
            }
        };
        if let Some((step, operation)) = cut {
            return Err(BuildStateError::Io(format!("the executor is cut after the work of {step} and before its event of {operation:?} is durable")));
        }
        self.inner.write_step(w).await
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
/// node.deleted; the Build carrier: "Completed steps are not rerun"). Run 1: path `mesh2.node`, name `1` is
/// stopped and its runtime terminated, the provider's exact Exited is receipted (TerminateRuntime),
/// and the executor is cut before NodeDeleted's receipt is durable: NodeDeleted is never published
/// and the delete has no `Complete`. path `mesh2.node`, name `2` runs its whole delete. Run 2 (another
/// mesh-primary, the same Build) takes path `mesh2.node`, name `2` as done and finishes path `mesh2.node`, name `1`'s delete:
/// its runtime is not terminated again (TerminateRuntime is reused), NodeDeleted is executed,
/// receipted and published once, and the delete ends in `Complete`. Every other peer keeps the
/// stopped birth until mesh-left if NodeDeleted is never published: the exited verdict alone is not the
/// departure. Each primary's publications are its own (run 1's primary is lost before its
/// frames land): run 2's primary publishes the NodeDeleted of the deletes it ran, and never again
/// path `mesh2.node`, name `2`'s, which its own node.delete published once in run 1 (node-delete.md step 4:
/// the executor emits node.deleted); a peer that never heard it keeps the birth until mesh-left.
#[test]
fn a_delete_cut_after_its_terminate_runtime_receipt_is_finished_by_the_resumed_run_from_its_missing_steps() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-deleted-cut-delete-{}", NodeId::mint()));
    let Members { failing, exiting, other_admin, final_admin, .. } = Members::new();
    let records = vec![failing.clone(), exiting.clone(), other_admin.clone(), final_admin.clone()];
    let handles = runtime_handles(&records, 44_000);
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.to_string())).collect(), ..Default::default() };
    let me = RosterBirth::of(&final_admin).unwrap();
    let final_runtime = RuntimeIdentity::of(&handles[&final_admin.node_id].fact().unwrap());
    let view = View::default();
    // Each mesh-primary's own publications: what run 1's primary publishes is never heard by
    // run 2's (it is lost before its frames land), so each holds its own.
    let (lost_departures, departures) = (Departures::default(), Departures::default());
    let calls: Mutex<Vec<(u32, String)>> = Mutex::new(Vec::new());

    let (first, cut_steps, second, steps, trace_id) = cap.run(async {
        let builds = crate::stepping(&NodeId::mint(), "mesh2");
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 0 })
            .await
            .unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: build_id.clone(), operation: outer.clone() };
        let joins = Joins::default();
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/testkit-node".into(),
            seeds: vec![("finalprimarykey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { mesh: "mesh2".into(), path: final_admin.path.clone(), name: final_admin.name.clone(), node_id: final_admin.node_id.clone(), incarnation: final_admin.incarnation_id.clone().unwrap() },
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
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: Some((*build_id).clone()), node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let shutdown_2 = |n: Node| {
            let (pipeline, handles, calls, build_id) = (&pipeline, &handles, &calls, &build_id);
            async move {
                let node_id = n.node_id.clone();
                calls.lock().unwrap().push((2, node_id.to_string()));
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: Some((*build_id).clone()), node: n, handle: handles[&node_id].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-deleted-cut-delete", build_id = %build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, operation_step = ?outer.step, operation_occurrence = ?outer.occurrence);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let mp = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key, final_admin.node_id.as_str(), &mp, &me, final_runtime.clone(), &records, shutdown_1).await;
        let cut_steps = builds.read_build(&build_id).await.unwrap().steps;
        let records2: Vec<Node> = view.holds(&records);
        // The roster is the mesh's records (D67): the member run 1 deleted keeps its Deleted record, though
        // the view no longer lists it.
        let mp2 = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, final_admin.node_id.as_str(), || live_roster(&records)).await.unwrap();
        let second = members_exited(&builds, &key, final_admin.node_id.as_str(), &mp2, &me, final_runtime.clone(), &records2, shutdown_2).await;
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        (first, cut_steps, second, steps, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("cut_delete");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    let err = first.expect_err("run 1 is cut inside path `mesh2.node`, name `1`'s delete: the ordinary tier is incomplete");
    assert!(err.contains("tier ordinary-nodes is incomplete") && err.contains(failing.node_id.as_str()), "{err}");
    let calls = calls.into_inner().unwrap();
    let ran = |run: u32| calls.iter().filter(|(a, _)| *a == run).map(|(_, n)| n.clone()).collect::<BTreeSet<_>>();
    assert_eq!(ran(1), BTreeSet::from([failing.node_id.to_string(), exiting.node_id.to_string()]), "run 1 ran the ordinary tier");
    let of = |steps: &[rdm_admin::build_state::BuildStep], node: &NodeId, step: &str| steps.iter().filter(|r| r.operation == rdm_core::OperationId::node("node.delete", node).to_string() && r.step == step && r.outcome == rdm_admin::build_state::StepOutcome::Complete).count();
    // The cut left rpc.1's exited verdict and no more; rpc.2's delete ran to its end.
    assert_eq!((of(&cut_steps, &failing.node_id, "TerminateRuntime"), of(&cut_steps, &failing.node_id, "NodeDeleted"), of(&cut_steps, &failing.node_id, "Complete")), (1, 0, 0), "the cut left the cut member's exited verdict and no NodeDeleted: {cut_steps:#?}");
    assert_eq!(of(&cut_steps, &exiting.node_id, "Complete"), 1, "the completed member's delete ran to its end");
    // Run 2: the exited verdict is not the delete. The delete with no NodeDeleted is run to its end.
    assert_eq!(ran(2), BTreeSet::from([failing.node_id.to_string(), other_admin.node_id.to_string()]), "run 2 finishes the cut member's delete (its exited verdict alone is not a finished delete) and runs the node-admin tier, and does not run the completed member, whose delete has every step's receipt");
    let manifest = second.unwrap_or_else(|e| panic!("the resumed run completes: {e}"));
    assert_eq!(manifest.receipts.iter().map(|r| (r.node_id.clone(), r.incarnation_id.clone())).collect::<BTreeSet<_>>(), [&failing, &exiting, &other_admin].iter().map(|n| (n.node_id.to_string(), n.incarnation_id.as_ref().unwrap().to_string())).collect::<BTreeSet<_>>(), "the manifest carries every other member's receipt");
    assert_eq!(of(&steps, &failing.node_id, "TerminateRuntime"), 1, "the cut member's completed TerminateRuntime is not run again");
    assert_eq!((of(&steps, &failing.node_id, "NodeDeleted"), of(&steps, &failing.node_id, "Complete")), (1, 1), "the cut member's NodeDeleted and Complete are written once, by the resumed run");
    assert_eq!(fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == failing.node_id.as_str()).count(), 1, "the cut member's runtime was terminated once, in run 1");
    assert_eq!(fleet.terminated.lock().unwrap().iter().filter(|n| n.as_str() == exiting.node_id.as_str()).count(), 1, "the completed member's runtime was terminated once");
    let published = |d: Departures| -> Vec<rdm_core::OperationId> {
        let mut ops: Vec<rdm_core::OperationId> = d.0.into_inner().unwrap().into_iter().map(|op| op.operation).collect();
        ops.sort();
        ops
    };
    assert_eq!(published(lost_departures), vec![rdm_core::OperationId::node("node.delete", &exiting.node_id)], "run 1's primary published the NodeDeleted of the one delete it finished");
    let mut want: Vec<rdm_core::OperationId> = [&failing.node_id, &other_admin.node_id].iter().map(|n| rdm_core::OperationId::node("node.delete", n)).collect();
    want.sort();
    assert_eq!(published(departures), want, "run 2's primary publishes the NodeDeleted of the deletes it ran, the cut member's and the other admin's, and never again the completed member's, its node.delete's in run 1");

    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let step = |node: &Node, step: &str| named("rdm.node_admin.deployment.update.via-step").into_iter().filter(|s| s["attributes"]["node_path"] == node.path && s["attributes"]["node_name"] == node.name && s["attributes"]["step"] == step).map(|s| s["attributes"]["outcome"].as_str().unwrap_or("").to_string()).last();
    assert_eq!(step(&failing, "TerminateRuntime"), Some("reused".to_string()), "run 2 reuses the TerminateRuntime whose event ended complete");
    assert_eq!(step(&failing, "NodeDeleted"), Some("complete".to_string()), "run 2 executes the NodeDeleted that has no event");
    let retained = named("rdm.node_admin.mesh.resolve.via-retained-receipts");
    assert!(retained.iter().any(|s| s["attributes"]["retained"] == "1" && s["attributes"]["retained_births"] == exiting.node_id.as_str() && s["attributes"]["remaining"] == "2"), "run 2 keeps the one finished delete and runs the other two members: {retained:?}");
    eprintln!("trace {trace_id}: run 2 via-step the cut member TerminateRuntime reused, NodeDeleted complete; via-retained-receipts retained 1 (the completed member) remaining 2");
}

/// The owner's open leave of mesh2, admitted over `records` with `FINAL` its final primary, and the
/// receiver its handoff resolves.
fn open_leave(mesh_delete_view: &MeshDeleteView, records: &[Node], final_node: &Node) -> (MeshDeleteKey, tokio::sync::watch::Receiver<Option<rdm_admin::mesh_delete::MeshDeleteOutcome>>, RosterBirth) {
    let roster = live_roster(records).unwrap();
    let final_primary = RosterBirth::of(final_node).unwrap();
    let mesh_id = MeshId::mint();
    let key = MeshDeleteKey { operation: operation(&mesh_id), mesh_id, build_id: BuildId::mint()};
    let admitted = Admitted { roster, final_primary: final_primary.clone(), final_runtime: RuntimeIdentity::of(&runtime_handles(std::slice::from_ref(final_node), 46_000)[&final_node.node_id].fact().unwrap()) };
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
/// final primary lost (mesh-delete.md, "Resume after the primary's loss"): the run ends
/// incomplete, naming the lost final primary, its receipts standing for the next run, which
/// resumes. A view change that names no loss keeps the wait open. Must NOT happen: the wait running on
/// to a timer (`MESH_DELETED_WAIT`) after the loss is in, the 120 s run 1 of the leave stems
/// (trace b84c9abc66ec8a8cee045c1e871514cb).
#[test]
fn the_owners_wait_ends_when_the_final_primary_is_lost_never_on_a_timer() {
    let cap = capture();
    let Members { failing, exiting, other_admin, final_admin, .. } = Members::new();
    let records = vec![failing, exiting, other_admin, final_admin.clone()];
    let mesh_delete_view = MeshDeleteView::default();
    let (key, handoff, final_primary) = open_leave(&mesh_delete_view, &records, &final_admin);
    let (end, open_after_silence, waited, trace_id) = run_paused(&cap, async {
        let root = tracing::info_span!("cell.owner-wait-exited-verdict", operation = %key.operation.action, operation_subject = ?key.operation.subject, operation_step = ?key.operation.step, operation_occurrence = ?key.operation.occurrence);
        let trace_id = trace_id(&root);
        let (view_tx, view_rx) = tokio::sync::watch::channel(0u64);
        let exit_read: Arc<Mutex<Option<String>>> = Arc::default();
        let held = exit_read.clone();
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
        *exit_read.lock().unwrap() = Some(format!("the final primary is unheard; its runtime reads exited: {} {}", final_primary.node_id, final_primary.incarnation_id));
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
        other => panic!("the run ends incomplete on the final primary's loss: {other:?}"),
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

