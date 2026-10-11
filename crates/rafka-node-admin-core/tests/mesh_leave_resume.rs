//! The mesh-leave resumption (i143.e12.s39; mesh-leave.md steps 1, 3 and "Completion proof"): the
//! leaving mesh-primary's member workflows are the product's (`mesh_leave::members_exited` over
//! `DeploymentPipeline::shutdown_member`); the provider, the Build state and the view are fixtures.
//!
//! mesh2 leaves: two ordinary nodes, one other node-admin and the final primary `mesh2.admin.1`.
//! In attempt 1 one ordinary node exits and the provider cannot inspect the other after its stop
//! ladder, so the ordinary tier is incomplete and the node-admin tier never starts. The resumed
//! attempt sees the view as the first one left it: neither ordinary node is live any more.

use rafka_mesh_entity::{FabricId, IncarnationId, MeshId, NodeId};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome, MemoryBuildStateAdapter};
use rafka_node_admin_core::fabric_builds::BuildMessage;
use rafka_node_admin_core::deployment::pipeline::{DeploymentPipeline, LaunchTemplate, NodeObserver, NoLifecycleEvents, Publication, RuntimeProof, TerminalReceipt, Timeouts, TopologySink};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::join::Joins;
use rafka_node_admin_core::mesh_leave::{accepted_roster, accepted_row, admission_roster, admitted_of, resume_after_loss, amend_roster, amended_roster, first_mismatch, handoff_verdict, manifest_reference, members_exited, operation, owner_resume, record_predecessor_exit, Admitted, Amended, ExitManifest, HandoffVerdict, LeaveKey, RosterAmended, RosterBirth, STEP_ACCEPTED_ROSTER, STEP_LEAVING_ROSTER, STEP_OTHER_MEMBERS_EXITED, STEP_ROSTER_AMENDED};
use rafka_node_rpc_contract::status::StatusReply;
use rafka_node_admin_core::model::{DeploymentId, Node, NodeStatus, PathName, ProviderKind};
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
    crate::enable_callsites();
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
    async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32)>, BuildStateError> {
        self.0.recovery_of(path, node_id, incarnation).await
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
    async fn find(&self, _: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
        None
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

/// The view: the last record the pipeline published for each node.
#[derive(Default)]
struct View(Mutex<BTreeMap<String, Node>>);
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
#[test]
fn a_resumed_leave_keeps_the_exited_members_receipts_and_runs_only_the_rest() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-leave-resume-{}", NodeId::mint()));
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 41_000 + i as u32))).collect();
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(), ..Default::default() };
    *fleet.blind_once.lock().unwrap() = handles[FAILING].pid;
    let me = RosterBirth::of(records.iter().find(|n| n.name.to_string() == FINAL).unwrap()).unwrap();
    let final_runtime = RuntimeProof::of(&handles[FINAL].fact().unwrap());
    let view = View::default();
    let calls: Mutex<Vec<(u32, String)>> = Mutex::new(Vec::new());

    let (first, fp1, fp2, mp1, mp2, second, records2, trace_id) = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 })
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
            mesh_issuer: None,
        };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let shutdown = |attempt: u32| {
            let (pipeline, handles, calls, build_id, outer) = (&pipeline, &handles, &calls, &build_id, &outer);
            move |mut n: Node| async move {
                let name = n.name.to_string();
                calls.lock().unwrap().push((attempt, name.clone()));
                pipeline.shutdown_member(build_id, attempt, outer, &mut n, &handles[&name]).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-leave-resume", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Attempt 1: both sides capture the roster at admission; the ordinary tier is incomplete.
        let fp1 = accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let mp1 = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key(1), FINAL, &mp1, &me, final_runtime.clone(), &records, &[], shutdown(1)).await;

        // Attempt 2 reads the view the first one left.
        let records2: Vec<Node> = records.iter().map(|n| view.0.lock().unwrap().get(&n.name.to_string()).cloned().unwrap_or_else(|| n.clone())).collect();
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
    let err = first.expect_err("the ordinary tier holds an unproven exit");
    assert!(err.contains("tier ordinary-nodes is incomplete") && err.contains(FAILING), "{err}");
    let calls = calls.into_inner().unwrap();
    let ran = |attempt: u32| calls.iter().filter(|(a, _)| *a == attempt).map(|(_, n)| n.clone()).collect::<BTreeSet<_>>();
    assert_eq!(ran(1), BTreeSet::from([FAILING.to_string(), EXITING.to_string()]), "attempt 1 ran the ordinary tier only");
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
}

/// CONTRACT: an incomplete tier is refused by name, never awaited. Attempt 1 leaves the ordinary
/// tier incomplete (one ordinary node exits, the other's exit cannot be proven; the node-admin tier
/// never starts), so the leaving primary records no exit manifest. The fabric-primary's verdict on
/// the handoff over the sender's Build facts (as the wire carries them) refuses it `NotReady`
/// naming every roster birth without an Exited receipt (the unproven ordinary node and the other
/// node-admin) and not the one that exited (node-rpc-envelope.md, "Mesh leave command pair": "an
/// incomplete manifest is refused NotReady with the mismatch named").
#[test]
fn an_incomplete_tier_is_refused_not_ready_naming_the_births_without_an_exited_receipt() {
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
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 })
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
            mesh_issuer: None,
        };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let root = tracing::info_span!("cell.mesh-leave-incomplete", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let roster = accepted_roster(&builds, &key, STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let mp = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let tier = members_exited(&builds, &key, FINAL, &mp, &me, final_runtime.clone(), &records, &[], |mut n: Node| {
            let (pipeline, handles, build_id, outer) = (&pipeline, &handles, &build_id, &outer);
            async move {
                let name = n.name.to_string();
                pipeline.shutdown_member(build_id, 1, outer, &mut n, &handles[&name]).await.map_err(|e| e.to_string())
            }
        })
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
    let reason = match verdict.refusal() {
        Some(StatusReply::NotReady { reason }) => reason,
        other => panic!("an incomplete tier is refused NotReady with the births named: {other:?} ({verdict:?})"),
    };
    assert!(reason.contains(FAILING) && reason.contains(OTHER_ADMIN), "every roster birth without an Exited receipt is named: {reason}");
    assert!(!reason.contains(EXITING) && !reason.contains(&format!("{FINAL} ")), "the exited birth and the final primary are not named: {reason}");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let refused = named("rdm.node_admin.mesh.reject.via-incomplete-handoff");
    assert!(refused.iter().any(|s| s["attributes"]["without_exit"].as_str().is_some_and(|w| w.contains(FAILING) && w.contains(OTHER_ADMIN))), "{refused:?}");
}

/// CONTRACT: a fabric-primary that resumes the leave after the final primary exited, before
/// `MeshLeft` was recorded, finishes it from the Build's receipts and calls nothing. Attempt 1 runs
/// to the end of the final primary's stop: both sides record the roster, the member workflows record
/// the exit manifest, and the owner's stop of the final primary records its exact `TerminateRuntime`
/// receipt, then `MeshLeft` is never recorded (the owner is lost). The resumed owner reads, from its
/// own Build facts, the roster accepted at admission (with the final primary it names), the final
/// primary's Exited receipt and the `OtherMembersExited` manifest, and holds the complete proof set
/// that permits `MeshLeft` (mesh-leave.md, "Completion proof": one exact Exited receipt for every
/// accepted roster birth, the final primary included).
#[test]
fn an_owner_resuming_after_the_final_primary_exited_records_mesh_left_from_the_receipts() {
    let cap = capture();
    let data_root = std::env::temp_dir().join(format!("mesh-leave-final-exited-{}", NodeId::mint()));
    let records: Vec<Node> = [FAILING, EXITING, OTHER_ADMIN, FINAL].iter().map(|n| node(n)).collect();
    let handles: BTreeMap<String, DeploymentHandle> = records.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 43_000 + i as u32))).collect();
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(), ..Default::default() };
    let me = RosterBirth::of(records.iter().find(|n| n.name.to_string() == FINAL).unwrap()).unwrap();
    let final_runtime = RuntimeProof::of(&handles[FINAL].fact().unwrap());
    let view = View::default();

    let (roster, resume, trace_id) = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 })
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
            mesh_issuer: None,
        };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let root = tracing::info_span!("cell.mesh-leave-final-exited", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let roster = accepted_roster(&builds, &key, STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let mp = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records)).await.unwrap();
        members_exited(&builds, &key, FINAL, &mp, &me, final_runtime.clone(), &records, &[], |mut n: Node| {
            let (pipeline, handles, build_id, outer) = (&pipeline, &handles, &build_id, &outer);
            async move {
                let name = n.name.to_string();
                pipeline.shutdown_member(build_id, 1, outer, &mut n, &handles[&name]).await.map_err(|e| e.to_string())
            }
        })
        .await
        .expect("every other member exits");
        // The owner's stop of the final primary (lead_leave step 5); the owner is lost before MeshLeft.
        let mut last = records.iter().find(|n| n.name.to_string() == FINAL).cloned().unwrap();
        pipeline.shutdown_member(&build_id, 1, &outer, &mut last, &handles[FINAL]).await.expect("the final primary exits");
        // The resumed owner reads its own Build facts.
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        (roster, owner_resume(&steps, &outer), trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("final-exited");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let _ = std::fs::remove_dir_all(&data_root);

    let resume = resume.unwrap_or_else(|e| panic!("the resume reads the receipts: {e}"));
    let all = resume.mesh_left_manifest().unwrap_or_else(|| panic!("the final primary's exit is proven: the resume holds the MeshLeft proof set, not {resume:?}"));
    assert_eq!(first_mismatch(all, &roster, &me, true), None, "MeshLeft covers every accepted birth, the final primary included");
    assert_eq!(births(&all.receipts), BTreeSet::from([FAILING.to_string(), EXITING.to_string(), OTHER_ADMIN.to_string(), FINAL.to_string()]));
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let resumed = named("rdm.node_admin.mesh.resolve.via-recorded-final-exit");
    assert!(resumed.iter().any(|s| s["attributes"]["receipts"] == "4" && s["attributes"]["roster"] == "4" && s["attributes"]["final_primary"].as_str().is_some_and(|f| f.contains(FINAL))), "{resumed:?}");
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
/// final primary is refused by name, and once the owner stops the amended primary the MeshLeft proof
/// set covers every accepted birth, both primaries included.
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
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 })
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
            mesh_issuer: None,
        };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let shutdown = |attempt: u32| {
            let (pipeline, handles, calls, build_id, outer) = (&pipeline, &handles, &calls, &build_id, &outer);
            move |mut n: Node| async move {
                let name = n.name.to_string();
                calls.lock().unwrap().push((attempt, name.clone()));
                pipeline.shutdown_member(build_id, attempt, outer, &mut n, &handles[&name]).await.map_err(|e| e.to_string())
            }
        };
        let root = tracing::info_span!("cell.mesh-leave-amended", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();

        // Attempt 1: both sides capture the roster; the ordinary tier is incomplete; the primary is lost.
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let mp1 = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, FINAL, &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key(1), FINAL, &mp1, &lost, lost_runtime.clone(), &records, &[], shutdown(1)).await;
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
        let records2: Vec<Node> = records.iter().map(|n| view.0.lock().unwrap().get(&n.name.to_string()).cloned().unwrap_or_else(|| n.clone())).collect();
        let manifest = members_exited(&builds, &key(2), OTHER_ADMIN, &mp2, &survivor, survivor_runtime.clone(), &records2, &[], shutdown(2)).await.expect("the amended primary's member workflows complete");
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
        // The owner stops the amended primary; the resumed owner reads the receipts.
        let mut last = records2.iter().find(|n| n.name.to_string() == OTHER_ADMIN).cloned().unwrap();
        pipeline.shutdown_member(&build_id, 2, &outer, &mut last, &handles[OTHER_ADMIN]).await.expect("the amended primary exits");
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        (applied, replayed, second_amendment, admitted, verdict, stale, owner_resume(&steps, &outer), steps, trace_id)
    });
    let (applied, replayed, second_amendment, admitted, verdict, stale, resume, steps, trace_id) = out;
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
    let all = resume.mesh_left_manifest().unwrap_or_else(|| panic!("the amended primary's exit is proven: the resume holds the MeshLeft proof set, not {resume:?}"));
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
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 })
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
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 })
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
            b.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
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
    let (without_row, with_row, before, after, trace_id) = cap.run(async {
        let builds = WireBuilds(MemoryBuildStateAdapter::new());
        let build_id = BuildId::mint();
        builds.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
        let mesh_id = MeshId::mint();
        let outer = operation(&mesh_id);
        let key = |attempt: u32| LeaveKey { mesh_id: mesh_id.clone(), build_id: build_id.0.clone(), attempt, operation: outer.clone() };
        accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &lost, &lost_runtime, || live_roster(&records)).await.unwrap();
        let root = tracing::info_span!("cell.mesh-leave-fp-resume", build_id = %build_id, operation = %outer);
        let trace_id = trace_id(&root);
        let _in_root = root.enter();
        let admitted = |steps: &[BuildStepReceipt]| admitted_of(steps, &outer).unwrap().unwrap();
        let adm = admitted(&builds.read_build(&build_id).await.unwrap().steps);
        let without_row = resume_after_loss(&builds, &fleet, &[], &key(2), "mesh1.admin.1", &adm, &survivor, &survivor_runtime).await;
        let before = admitted(&builds.read_build(&build_id).await.unwrap().steps);
        let with_row = resume_after_loss(&builds, &fleet, std::slice::from_ref(&row), &key(2), "mesh1.admin.1", &adm, &survivor, &survivor_runtime).await;
        let after = admitted(&builds.read_build(&build_id).await.unwrap().steps);
        (without_row, with_row, before, after, trace_id)
    });
    let spans = cap.spans();
    let dir = artifact_dir().join("fp_resume");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let refusal = without_row.expect_err("no runtime row of the lost birth: the resume is refused");
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
            b.publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
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
        let roster = rafka_node_admin_core::mesh_leave::leaving_roster(&own, &key(2), NEW_ADMIN, &created, &runtime(NEW_ADMIN), || live_roster(&live_now), || async { Ok(owner_facts) }).await;
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
