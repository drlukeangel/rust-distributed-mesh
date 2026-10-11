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
use rafka_node_admin_core::mesh_leave::{accepted_roster, first_mismatch, handoff_verdict, manifest_reference, members_exited, operation, Admitted, LeaveKey, RosterBirth, STEP_ACCEPTED_ROSTER, STEP_LEAVING_ROSTER};
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

/// This cell's own span capture: an in-memory OTel exporter behind a dispatcher installed on
/// every thread of the cell's own runtime.
pub(crate) struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    provider: opentelemetry_sdk::trace::TracerProvider,
    dispatch: tracing::Dispatch,
}

pub(crate) fn capture() -> Capture {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt;
    crate::enable_callsites();
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .with_resource(opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new("service.name", "mesh2.admin.1")]))
        .build();
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("mesh-leave-resume"));
    Capture { exporter, provider, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
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
        let _ = self.provider.force_flush();
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
        let trace_id = {
            use opentelemetry::trace::TraceContextExt as _;
            use tracing_opentelemetry::OpenTelemetrySpanExt as _;
            root.context().span().span_context().trace_id().to_string()
        };
        let _in_root = root.enter();

        // Attempt 1: both sides capture the roster at admission; the ordinary tier is incomplete.
        let fp1 = accepted_roster(&builds, &key(1), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let mp1 = accepted_roster(&builds, &key(1), STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let first = members_exited(&builds, &key(1), FINAL, &mp1, &me, final_runtime.clone(), &records, shutdown(1)).await;

        // Attempt 2 reads the view the first one left.
        let records2: Vec<Node> = records.iter().map(|n| view.0.lock().unwrap().get(&n.name.to_string()).cloned().unwrap_or_else(|| n.clone())).collect();
        let fp2 = accepted_roster(&builds, &key(2), STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &final_runtime, || live_roster(&records2)).await.unwrap();
        let mp2 = accepted_roster(&builds, &key(2), STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records2)).await.unwrap();
        let second = members_exited(&builds, &key(2), FINAL, &mp2, &me, final_runtime.clone(), &records2, shutdown(2)).await;
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
        let trace_id = {
            use opentelemetry::trace::TraceContextExt as _;
            use tracing_opentelemetry::OpenTelemetrySpanExt as _;
            root.context().span().span_context().trace_id().to_string()
        };
        let _in_root = root.enter();
        let roster = accepted_roster(&builds, &key, STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let mp = accepted_roster(&builds, &key, STEP_LEAVING_ROSTER, FINAL, &me, &final_runtime, || live_roster(&records)).await.unwrap();
        let tier = members_exited(&builds, &key, FINAL, &mp, &me, final_runtime.clone(), &records, |mut n: Node| {
            let (pipeline, handles, build_id, outer) = (&pipeline, &handles, &build_id, &outer);
            async move {
                let name = n.name.to_string();
                pipeline.shutdown_member(build_id, 1, outer, &mut n, &handles[&name]).await.map_err(|e| e.to_string())
            }
        })
        .await;
        // The handoff the leaving primary sends for the incomplete run, judged over its facts.
        let facts = builds.facts().await.unwrap();
        let admitted = Admitted { roster, final_primary: me.clone(), final_runtime: final_runtime.clone() };
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
