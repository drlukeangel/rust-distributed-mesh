//! The stopped-node garbage collector (fabric-node-lifecycle.md §6.9 "The stopped-node garbage
//! collector", R-GC1): the fabric-primary sweeps every `stopped_node_ttl`, ages each stopped birth
//! from the current stopped period's own stop receipt in the Build log, skips a birth an open Build
//! still references, and submits one ordinary node.delete Build per sweep; the delete's admission
//! runs the stopped and open-Build checks again, serialized with it. Every cell writes the Build log
//! it decides from: nothing here depends on timing.

use rafka_mesh_entity::LifecycleOp;
use rafka_node_admin_core::accepted::{AcceptedStore, AttemptAction, FabricTopology, TopologyChange};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{
    AttemptOpened, AttemptOutcome, AttemptReason, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStateAdapter, BuildStepReceipt, MemoryBuildStateAdapter, StepOutcome,
};
use rafka_node_admin_core::http::{ControlPlane, Refusal};
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::stopped_gc::{ttl_from_env, Swept, Verdict, SHIPPED_TTL};
use rafka_node_admin_core::topology::Topology;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

const TTL: Duration = Duration::from_secs(60);
const STOPPED: &str = "mesh1.rpc.1";
const YOUNG: &str = "mesh1.rpc.2";

fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.incarnation_id = Some(IncarnationId::mint());
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n
}

/// One mesh: two node-admins (`fabric` holds the fabric seat) and three rpc nodes; `mesh1.rpc.1`
/// and `mesh1.rpc.2` are stopped (Leaving, parked, the process alive).
fn view(fabric: &str) -> Topology {
    let mut nodes = vec![node("mesh1.admin.1", fabric == "mesh1.admin.1", fabric == "mesh1.admin.1"), node("mesh1.admin.2", fabric == "mesh1.admin.2", fabric == "mesh1.admin.2"), node("mesh1.rpc.3", true, false)];
    for p in [STOPPED, YOUNG] {
        let mut n = node(p, false, false);
        n.status = NodeStatus::Leaving;
        n.parked = true;
        nodes.push(n);
    }
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes,
    }
}

struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    pipeline: rafka_mesh_telemetry::SpanCapture,
}

impl Capture {
    fn spans(&self, name: &str) -> Vec<(String, std::collections::BTreeMap<String, String>)> {
        self.pipeline.flush(Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let spans: Vec<_> = self.exporter.get_finished_spans().unwrap().into_iter().filter(|s| s.name == name).map(|s| (s.span_context.trace_id().to_string(), s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect())).collect();
        for (trace_id, attrs) in &spans {
            eprintln!("span {name} trace_id={trace_id} {attrs:?}");
        }
        spans
    }
}

fn capture(cell: &str) -> (Capture, tracing::subscriber::DefaultGuard) {
    crate::enable_callsites();
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rafka_mesh_telemetry::span_capture(cell, exporter.clone());
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    (Capture { exporter, pipeline }, guard)
}

/// The fabric-primary `me` over `builds`, its accepted Build seeded from `t` and settled.
async fn plane(builds: Arc<MemoryBuildStateAdapter>, t: Topology, me: &str) -> Arc<ControlPlane> {
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), me).await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds.clone(), accepted, me.parse().unwrap(), t, crate::adopted_time()));
    let b0 = cp.accepted.build_id().await.unwrap();
    settle(&builds, &b0).await;
    cp
}

/// The Build `id`'s current attempt claimed and converged.
async fn settle(builds: &MemoryBuildStateAdapter, id: &BuildId) {
    let p = builds.read_build(id).await.unwrap();
    let attempt = p.attempt_to_drive();
    builds.claim_attempt(&BuildAttemptClaim { build_id: id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
    builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
}

fn birth(t: &Topology, p: &str) -> (PathName, NodeId, IncarnationId) {
    let n = t.node(&p.parse().unwrap()).unwrap();
    (n.name.clone(), n.node_id.clone(), n.incarnation_id.clone().unwrap())
}

/// A standalone stop (`stop` true) or start of the exact birth, as its own attempt of the accepted
/// Build: the attempt carries the fenced action, and its operation's `Complete` receipt is written
/// at `at_ms` (a stop's names the birth and its rafka-time, the stop's lifecycle op).
async fn period(cp: &ControlPlane, builds: &MemoryBuildStateAdapter, (path, node_id, incarnation): &(PathName, NodeId, IncarnationId), stop: bool, at_ms: u64) {
    let b0 = cp.accepted.build_id().await.unwrap();
    let attempt = builds.read_build(&b0).await.unwrap().attempt + 1;
    let (action, reason, operation) = if stop {
        (AttemptAction::Stop { path: path.clone(), from_incarnation: incarnation.clone() }, AttemptReason::Stop, format!("stop-node:{path}"))
    } else {
        (AttemptAction::Start { path: path.clone(), from_incarnation: incarnation.clone() }, AttemptReason::Start, format!("start-node:{path}"))
    };
    let op = LifecycleOp { build_id: b0.to_string(), attempt, operation: operation.clone(), node_id: node_id.clone(), incarnation: incarnation.clone(), name: path.clone(), event_at_rafka_ms: at_ms };
    builds.absorb(&[
        BuildFact::Opened(AttemptOpened { build_id: b0.clone(), attempt, reason, action: Some(action), opened_by: "mesh1.admin.1".into(), opened_at_ms: at_ms }),
        BuildFact::Claim(BuildAttemptClaim { build_id: b0.clone(), attempt, executor: "mesh1.admin.1".into() }),
        BuildFact::Step(BuildStepReceipt { build_id: b0.clone(), attempt, operation, step: "Complete".into(), outcome: StepOutcome::Complete, output: stop.then(|| serde_json::to_value(&op).unwrap()), executor: Some("mesh1.admin.1".into()) }),
        BuildFact::Attempt(BuildAttemptReceipt { build_id: b0.clone(), attempt, outcome: AttemptOutcome::Converged }),
    ]);
}

fn now(cp: &ControlPlane) -> u64 {
    cp.time.now_ms()
}

/// CONTRACT (§6.9 Action, Effect, Span): `mesh1.rpc.1`, stopped ten ttl ago by its own receipted
/// stop, and `mesh1.rpc.2`, stopped just now, are both stopped. One sweep accepts exactly one
/// ordinary node.delete Build, the one `DELETE /api/nodes/mesh1.rpc.1` accepts (`RemoveNode`), whose
/// topology lacks `mesh1.rpc.1` and still names `mesh1.rpc.2`; the sweep is spanned
/// `rdm.node_admin.node.delete.via-stopped-ttl` with the node, its stop receipt's time, the ttl and
/// the Build. Must NOT happen: a delete of the node stopped inside the ttl, or any write beside the Build.
#[tokio::test]
async fn a_node_stopped_past_the_ttl_is_collected_by_one_ordinary_delete_build() {
    let (spans, _g) = capture("i143-3072-collected");
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view("mesh1.admin.1");
    let (old, young) = (birth(&t, STOPPED), birth(&t, YOUNG));
    let cp = plane(builds.clone(), t, "mesh1.admin.1").await;
    let b0 = cp.accepted.build_id().await.unwrap();
    let stopped_at = now(&cp) - 10 * TTL.as_millis() as u64;
    period(&cp, &builds, &old, true, stopped_at).await;
    period(&cp, &builds, &young, true, now(&cp)).await;

    let swept = cp.sweep_stopped(Some(TTL)).await;
    let Swept::Submitted { node, build_id } = swept else { panic!("one node.delete Build is accepted for the expired stopped node: {swept:?}") };
    assert_eq!(node, old.0);
    assert_ne!(build_id, b0, "a new topology Build, never an attempt of the settled one");
    let p = builds.read_build(&build_id).await.unwrap();
    assert_eq!(p.submitted_change, Some(TopologyChange::RemoveNode { node: old.0.clone() }), "the ordinary node.delete Build");
    assert!(!p.topology.contains(&old.0) && p.topology.contains(&young.0), "only the expired node leaves the topology: {:?}", p.topology);
    assert_eq!(cp.accepted.build_id().await, Some(build_id.clone()), "Fabric.build_id names the delete");

    let collected = spans.spans("rdm.node_admin.node.delete.via-stopped-ttl");
    assert_eq!(collected.len(), 1, "{collected:?}");
    let a = &collected[0].1;
    assert_eq!((a["node"].as_str(), a["stopped_at"].as_str(), a["ttl"].as_str(), a["build_id"].as_str()), (STOPPED, stopped_at.to_string().as_str(), "60000", build_id.0.as_str()), "{a:?}");
}

/// CONTRACT (§6.9 Setting): unset, `RDM_STOPPED_NODE_TTL` is RDM's shipped 30 minutes; blank means
/// forever. A sweep at a blank ttl decides nothing and submits nothing, even for a node stopped
/// long ago; a malformed value is refused by name.
#[tokio::test]
async fn a_blank_ttl_runs_no_sweep() {
    assert_eq!(ttl_from_env(None), Ok(Some(SHIPPED_TTL)));
    assert_eq!(SHIPPED_TTL, Duration::from_secs(1800));
    assert_eq!(ttl_from_env(Some("")), Ok(None));
    assert_eq!(ttl_from_env(Some("  ")), Ok(None));
    assert_eq!(ttl_from_env(Some("2s")), Ok(Some(Duration::from_secs(2))));
    assert_eq!(ttl_from_env(Some("1h")), Ok(Some(Duration::from_secs(3600))));
    assert_eq!(ttl_from_env(Some("750ms")), Ok(Some(Duration::from_millis(750))));
    assert!(ttl_from_env(Some("soon")).unwrap_err().contains("RDM_STOPPED_NODE_TTL"));
    assert!(ttl_from_env(Some("0s")).unwrap_err().contains("RDM_STOPPED_NODE_TTL"), "a zero ttl is refused: blank is how forever is said, never zero");

    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view("mesh1.admin.1");
    let old = birth(&t, STOPPED);
    let cp = plane(builds.clone(), t, "mesh1.admin.1").await;
    let b0 = cp.accepted.build_id().await.unwrap();
    period(&cp, &builds, &old, true, 1).await;
    assert_eq!(cp.sweep_stopped(None).await, Swept::Forever);
    assert_eq!(cp.accepted.build_id().await, Some(b0), "nothing was submitted");
}

/// CONTRACT (§6.9 Recheck): the scan selects `mesh1.rpc.1`, stopped past the ttl. Before the
/// delete is admitted the node is started (its start receipted, the view holding it live again):
/// the admission runs the stopped check again and refuses by name, `node-not-stopped`; a second
/// candidate, referenced by a Build opened after the scan, is refused `referenced-by-open-build`.
/// Must NOT happen: a delete accepted from the scan's stale decision.
#[tokio::test]
async fn a_node_started_after_the_scan_is_refused_at_the_delete_admission() {
    let (spans, _g) = capture("i143-3072-recheck");
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view("mesh1.admin.1");
    let (old, other) = (birth(&t, STOPPED), birth(&t, YOUNG));
    let cp = plane(builds.clone(), t, "mesh1.admin.1").await;
    let b0 = cp.accepted.build_id().await.unwrap();
    period(&cp, &builds, &old, true, 1).await;
    period(&cp, &builds, &other, true, 2).await;
    let candidates: Vec<_> = cp.scan_stopped(TTL, now(&cp)).await.into_iter().filter_map(|v| if let Verdict::Expired(c) = v { Some(c) } else { None }).collect();
    assert_eq!(candidates.iter().map(|c| c.node.to_string()).collect::<Vec<_>>(), vec![STOPPED, YOUNG], "both are stopped past the ttl at the scan");

    // After the scan: mesh1.rpc.1 is started.
    period(&cp, &builds, &old, false, now(&cp)).await;
    cp.topology.write().await.nodes.iter_mut().filter(|n| n.name == old.0).for_each(|n| {
        n.parked = false;
        n.status = NodeStatus::ReadyForTraffic;
    });
    let refused = cp.collect_stopped(&candidates[0], TTL).await;
    assert!(matches!(&refused, Err(Refusal::Reject(r)) if r.reason() == "node-not-stopped"), "{refused:?}");

    // After the scan: a Build opens a replacement of mesh1.rpc.2's birth.
    let attempt = builds.read_build(&b0).await.unwrap().attempt + 1;
    builds.absorb(&[BuildFact::Opened(AttemptOpened { build_id: b0.clone(), attempt, reason: AttemptReason::Replace, action: Some(AttemptAction::Replace { path: other.0.clone(), from_incarnation: other.2.clone() }), opened_by: "mesh1.admin.1".into(), opened_at_ms: now(&cp) })]);
    let referenced = cp.collect_stopped(&candidates[1], TTL).await;
    assert!(matches!(&referenced, Err(Refusal::Reject(r)) if r.reason() == "referenced-by-open-build"), "{referenced:?}");

    assert_eq!(cp.accepted.build_id().await, Some(b0), "no delete was accepted");
    assert_eq!(spans.spans("rdm.node_admin.build.reject.via-node-not-stopped").len(), 1);
    assert_eq!(spans.spans("rdm.node_admin.build.reject.via-referenced-by-open-build").len(), 1);
    assert!(spans.spans("rdm.node_admin.node.delete.via-stopped-ttl").is_empty());
}

/// CONTRACT (§6.9 Selection: "except a birth an open Build still references (a replace
/// mid-transfer keeps its rollback point)"): `mesh1.rpc.1`, stopped past the ttl, is the birth a
/// replacement left mid-transfer (its attempt failed, the Build is not settled). The scan says it is
/// referenced by that Build and the sweep submits nothing for it.
#[tokio::test]
async fn a_stopped_birth_an_open_replace_references_is_skipped() {
    let (spans, _g) = capture("i143-3072-referenced");
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view("mesh1.admin.1");
    let old = birth(&t, STOPPED);
    let cp = plane(builds.clone(), t, "mesh1.admin.1").await;
    let b0 = cp.accepted.build_id().await.unwrap();
    period(&cp, &builds, &old, true, 1).await;
    let young = birth(&cp.topology.read().await.clone(), YOUNG);
    period(&cp, &builds, &young, true, now(&cp)).await;
    let attempt = builds.read_build(&b0).await.unwrap().attempt + 1;
    builds.absorb(&[
        BuildFact::Opened(AttemptOpened { build_id: b0.clone(), attempt, reason: AttemptReason::Replace, action: Some(AttemptAction::Replace { path: old.0.clone(), from_incarnation: old.2.clone() }), opened_by: "mesh1.admin.1".into(), opened_at_ms: now(&cp) }),
        BuildFact::Claim(BuildAttemptClaim { build_id: b0.clone(), attempt, executor: "mesh1.admin.1".into() }),
        BuildFact::Attempt(BuildAttemptReceipt { build_id: b0.clone(), attempt, outcome: AttemptOutcome::Failed { reason: "the successor did not report ready".into() } }),
    ]);

    let verdicts = cp.scan_stopped(TTL, now(&cp)).await;
    assert!(verdicts.contains(&Verdict::Referenced { node: old.0.clone(), build_id: b0.clone() }), "{verdicts:?}");
    assert_eq!(cp.sweep_stopped(Some(TTL)).await, Swept::Nothing);
    assert_eq!(cp.accepted.build_id().await, Some(b0));
    let skipped = spans.spans("rdm.node_admin.node.evict.via-open-build-reference");
    assert!(skipped.iter().any(|(_, a)| a["node"] == STOPPED), "{skipped:?}");
}

/// CONTRACT (§6.9 Age: "never an earlier stop before a later start"): `mesh1.rpc.1` was stopped ten
/// ttl ago, started, and stopped again just now: its age is the latest stop, and it is young.
/// `mesh1.rpc.2` was stopped ten ttl ago and started, and is parked with no stop receipt of its own
/// since: no receipt opens its current stopped period, so it is skipped by name, never expired.
#[tokio::test]
async fn an_earlier_stop_before_a_later_start_does_not_count() {
    let (spans, _g) = capture("i143-3072-current-period");
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view("mesh1.admin.1");
    let (a, b) = (birth(&t, STOPPED), birth(&t, YOUNG));
    let cp = plane(builds.clone(), t, "mesh1.admin.1").await;
    let b0 = cp.accepted.build_id().await.unwrap();
    let long_ago = now(&cp) - 10 * TTL.as_millis() as u64;
    period(&cp, &builds, &a, true, long_ago).await;
    period(&cp, &builds, &a, false, long_ago + 1).await;
    let latest = now(&cp);
    period(&cp, &builds, &a, true, latest).await;
    period(&cp, &builds, &b, true, long_ago).await;
    period(&cp, &builds, &b, false, long_ago + 1).await;

    let verdicts = cp.scan_stopped(TTL, now(&cp)).await;
    assert!(verdicts.contains(&Verdict::Young { node: a.0.clone(), stopped_at_ms: latest }), "{verdicts:?}");
    assert!(verdicts.iter().any(|v| matches!(v, Verdict::Skipped { node, .. } if *node == b.0)), "{verdicts:?}");
    assert_eq!(cp.sweep_stopped(Some(TTL)).await, Swept::Nothing);
    assert_eq!(cp.accepted.build_id().await, Some(b0));
    assert!(spans.spans("rdm.node_admin.node.evict.via-no-current-stop-receipt").iter().any(|(_, x)| x["node"] == YOUNG));
}

/// CONTRACT (§6.9 Action: "a `BuildInProgress` refusal loses nothing, the candidate is seen
/// again"): two nodes stopped past the ttl, and another topology Build in flight. The sweep is
/// refused `BuildInProgress` naming that Build and accepts nothing; once it settles, the next sweep
/// collects the first candidate, and the sweep after that delete settles collects the second.
#[tokio::test]
async fn a_candidate_refused_build_in_progress_is_collected_on_a_later_sweep() {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view("mesh1.admin.1");
    let (a, b) = (birth(&t, STOPPED), birth(&t, YOUNG));
    let cp = plane(builds.clone(), t, "mesh1.admin.1").await;
    period(&cp, &builds, &a, true, 1).await;
    period(&cp, &builds, &b, true, 2).await;
    let other = cp.submit("build.create", TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }).await.expect("a topology change is accepted").build_id;

    assert_eq!(cp.sweep_stopped(Some(TTL)).await, Swept::BuildInProgress { node: a.0.clone(), current_build_id: other.clone() });
    assert_eq!(cp.accepted.build_id().await, Some(other.clone()));
    settle(&builds, &other).await;
    let first = match cp.sweep_stopped(Some(TTL)).await {
        Swept::Submitted { node, build_id } if node == a.0 => build_id,
        s => panic!("the next sweep collects the first candidate: {s:?}"),
    };
    // One topology Build at a time: the second candidate waits for the delete to settle.
    assert!(matches!(cp.sweep_stopped(Some(TTL)).await, Swept::BuildInProgress { ref node, ref current_build_id } if *node == b.0 && *current_build_id == first));
    settle(&builds, &first).await;
    cp.topology.write().await.nodes.retain(|n| n.name != a.0);
    assert!(matches!(cp.sweep_stopped(Some(TTL)).await, Swept::Submitted { ref node, .. } if *node == b.0));
}

/// CONTRACT (§6.9 Who: "On a fabric-primary handover the successor sweeps from the same durable
/// receipts"): the Build log of the old fabric-primary is held by its successor `mesh1.admin.2`.
/// The successor ages `mesh1.rpc.1` from the same stop receipt (its time, never the handover's) and
/// collects it; the old holder, no longer holding the fabric seat, sweeps nothing.
#[tokio::test]
async fn a_successor_fabric_primary_sweeps_from_the_same_receipts() {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view("mesh1.admin.1");
    let old = birth(&t, STOPPED);
    let first = plane(builds.clone(), t.clone(), "mesh1.admin.1").await;
    let stopped_at = now(&first) - 10 * TTL.as_millis() as u64;
    period(&first, &builds, &old, true, stopped_at).await;

    // The handover: mesh1.admin.2 holds the seat and the same Build log.
    let mut moved = t.clone();
    for n in moved.nodes.iter_mut() {
        n.is_fabric_primary = n.name.to_string() == "mesh1.admin.2";
        if n.kind == NodeKind::NodeAdmin {
            n.is_primary = n.is_fabric_primary;
        }
    }
    *first.topology.write().await = moved.clone();
    let held = Arc::new(MemoryBuildStateAdapter::new());
    held.absorb(&builds.facts().await.unwrap());
    let accepted = AcceptedStore::seeded(&*held, moved.fabric.id.clone(), FabricTopology::of_observed(&moved), "mesh1.admin.2").await.unwrap();
    let successor = ControlPlane::new(held.clone(), accepted, "mesh1.admin.2".parse().unwrap(), moved, crate::adopted_time());
    let b = successor.accepted.build_id().await.unwrap();
    if !matches!(held.read_build(&b).await.unwrap().state, rafka_node_admin_core::build_state::BuildState::Complete) {
        settle(&held, &b).await;
    }

    let verdicts = successor.scan_stopped(TTL, now(&successor)).await;
    assert!(verdicts.iter().any(|v| matches!(v, Verdict::Expired(c) if c.node == old.0 && c.stopped_at_ms == stopped_at)), "the successor ages the node from the same receipt: {verdicts:?}");
    assert!(matches!(successor.sweep_stopped(Some(TTL)).await, Swept::Submitted { ref node, .. } if *node == old.0));
    assert_eq!(first.sweep_stopped(Some(TTL)).await, Swept::NotFabricPrimary);
}
