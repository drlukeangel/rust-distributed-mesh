//! The stopped-node garbage collector (node-identity-and-state.md §6.9 "The stopped-node garbage
//! collector", R-GC1): the fabric-primary sweeps every `stopped_node_ttl`, ages each stopped birth
//! from the current stopped period's own stop, recorded on the node record (`stopped_at_rafka_ms`,
//! D50), skips a birth an open Build still references, and submits one ordinary node.delete Build per
//! sweep; the delete's admission runs the stopped and open-Build checks again, serialized with it.
//! Every cell writes the records it decides from: nothing here depends on timing.

use rdm_admin::accepted::{AcceptedStore, AttemptAction, FabricTopology};
use rdm_core::build::BuildChange;
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{
    AttemptOpened, AttemptOutcome, AttemptReason, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStateAdapter, MemoryBuildStateAdapter,
};
use rdm_admin::control_plane::{ControlPlane, Refusal};
use rdm_admin::model::*;
use rdm_admin::stopped_gc::{ttl_from_env, Swept, Verdict, SHIPPED_TTL};
use rdm_admin::topology::Topology;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

const TTL: Duration = Duration::from_secs(60);
fn stopped() -> PathName { PathName::new("mesh1.node", "1").unwrap() }
fn young() -> PathName { PathName::new("mesh1.node", "2").unwrap() }

fn node(name: rdm_core::PathName, mesh: &str, r#type: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name, mesh, r#type);
    n.status = NodeStatus::Ready;
    n.incarnation_id = Some(IncarnationId::mint());
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n
}

/// One mesh: two node-admins (`fabric` holds the fabric-primary role) and three nodes; `mesh1.node.1`
/// and `mesh1.node.2` are stopped (Leaving, the process alive).
fn view(fabric: &PathName) -> Topology {
    let mut nodes = vec![node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", *fabric == PathName::new("mesh1.node-admin", "1").unwrap(), *fabric == PathName::new("mesh1.node-admin", "1").unwrap()), node(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", *fabric == PathName::new("mesh1.node-admin", "2").unwrap(), *fabric == PathName::new("mesh1.node-admin", "2").unwrap()), node(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", true, false)];
    for p in [stopped(), young()] {
        let mut n = node(p, "mesh1", "node", false, false);
        n.status = NodeStatus::Leaving;
        n.stopped = true;
        nodes.push(n);
    }
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
        nodes,
    }
}

struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    pipeline: rdm_transport::SpanCapture,
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
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rdm_transport::span_capture(cell, exporter.clone());
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    (Capture { exporter, pipeline }, guard)
}

/// The fabric-primary `me` over `builds`, its accepted Build seeded from `t` and settled.
async fn plane(builds: Arc<MemoryBuildStateAdapter>, t: Topology, me: &PathName) -> Arc<ControlPlane> {
    let cp = plane_unbound(builds, t, me).await;
    let _ = cp.node_rows.set(Arc::new(rdm_admin::storage::MemoryNodesStorage::default()));
    cp
}

/// [`plane`] with no node records bound: every record read is refused by name.
async fn plane_unbound(builds: Arc<MemoryBuildStateAdapter>, t: Topology, me: &PathName) -> Arc<ControlPlane> {
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), desired(&t), &t.node(me).unwrap().birth().unwrap(), &t.node(me).unwrap().mesh).await.unwrap();
    let cp = Arc::new(crate::control_plane(builds.clone(), accepted, me.clone(), t, crate::adopted_time()));
    let b0 = cp.builds.current_build().await.unwrap().into_build_id().unwrap();
    settle(&builds, &b0, &executor(&cp).await).await;
    cp
}

/// The accepted topology: every node, the stopped ones included (a stop is an attempt of the
/// accepted Build and leaves its topology as it is).
fn desired(t: &Topology) -> FabricTopology {
    let mut live = t.clone();
    for n in live.nodes.iter_mut() {
        n.stopped = false;
        n.status = NodeStatus::Ready;
    }
    FabricTopology::of_observed(&live)
}

async fn executor(cp: &ControlPlane) -> rdm_core::framing::Birth {
    cp.topology.read().await.node(&cp.me()).unwrap().birth().unwrap()
}

/// The Build `id`'s current attempt claimed and converged.
async fn settle(builds: &MemoryBuildStateAdapter, id: &BuildId, executor_birth: &rdm_core::framing::Birth) {
    let p = builds.read_build(id).await.unwrap();
    let attempt = p.attempt_to_drive();
    builds.claim_attempt(&BuildAttemptClaim { claimed_mesh: None, executor_mesh: "mesh1".into(), claimant: None, mesh: None, build_id: id.clone(), attempt, executor: executor_birth.clone() }).await.unwrap();
    builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
}

fn birth(t: &Topology, p: &PathName) -> (PathName, NodeId, IncarnationId) {
    let n = t.node(p).unwrap();
    (n.location(), n.node_id.clone(), n.incarnation_id.clone().unwrap())
}

/// A stop (`stop` true) or start of the exact birth, as its mesh-primary records it on the node record:
/// a stop writes the stopped period's `stopped_at_rafka_ms` at `at_ms`, the start's ready clears it (D50).
async fn period(cp: &ControlPlane, _builds: &MemoryBuildStateAdapter, (path, node_id, incarnation): &(PathName, NodeId, IncarnationId), stop: bool, at_ms: u64) {
    let rows = cp.node_rows.get().expect("the plane binds the node records").clone();
    let base = rdm_admin::storage::NodeRow::new(path.clone(), "mesh1", "node", node_id.clone(), NodeStatus::Ready);
    rdm_admin::node_rows::WrittenRows::default()
        .update(&*rows, node_id, Some(base), |r| r.birth_mut(incarnation, path).stopped_at_rafka_ms = stop.then_some(at_ms))
        .await
        .unwrap();
}

fn now(cp: &ControlPlane) -> u64 {
    cp.time.now_ms()
}

/// CONTRACT (§6.9 Action, Effect, Span): `mesh1.node.1`, stopped ten ttl ago by its own recorded
/// stop, and `mesh1.node.2`, stopped just now, are both stopped. One sweep accepts exactly one
/// ordinary node.delete Build, the one `DELETE /api/nodes/mesh1.node.1` accepts (`RemoveNode`), whose
/// topology lacks `mesh1.node.1` and still names `mesh1.node.2`; the sweep is spanned
/// `rdm.node_admin.node.delete.via-stopped-ttl` with the node, its recorded stop's time, the ttl and
/// the Build. Must NOT happen: a delete of the node stopped inside the ttl, or any write beside the Build.
#[tokio::test]
async fn a_node_stopped_past_the_ttl_is_collected_by_one_ordinary_delete_build() {
    let (spans, _g) = capture("i143-3072-collected");
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view(&PathName::new("mesh1.node-admin", "1").unwrap());
    let (old, young) = (birth(&t, &stopped()), birth(&t, &young()));
    let cp = plane(builds.clone(), t, &PathName::new("mesh1.node-admin", "1").unwrap()).await;
    let b0 = cp.builds.current_build().await.unwrap().into_build_id().unwrap();
    let stopped_at = now(&cp) - 10 * TTL.as_millis() as u64;
    period(&cp, &builds, &old, true, stopped_at).await;
    period(&cp, &builds, &young, true, now(&cp)).await;

    let swept = cp.sweep_stopped(Some(TTL)).await;
    let Swept::Submitted { path: node_path, name: node_name, build_id } = swept else { panic!("one node.delete Build is accepted for the expired stopped node: {swept:?}") }; let node = &rdm_core::PathName { path: node_path.clone(), name: node_name.clone() };
    assert_eq!(*node, old.0);
    assert_ne!(build_id, b0, "a new topology Build, never an attempt of the settled one");
    let p = builds.read_build(&build_id).await.unwrap();
    assert_eq!(p.submitted_change, Some(BuildChange::Delete { meshes: Vec::new(), node_ids: vec![old.1.clone()] }), "the ordinary node.delete Build");
    assert!(!p.topology.contains(&old.0) && p.topology.contains(&young.0), "only the expired node leaves the topology: {:?}", p.topology);
    assert_eq!(cp.builds.current_build().await.unwrap().into_build_id(), Some(build_id.clone()), "the current build_id names the delete");

    let collected = spans.spans("rdm.node_admin.node.delete.via-stopped-ttl");
    assert_eq!(collected.len(), 1, "{collected:?}");
    let a = &collected[0].1;
    assert_eq!(((a["node_path"].as_str(), a["node_name"].as_str()), a["stopped_at"].as_str(), a["ttl"].as_str(), a["build_id"].as_str()), (("mesh1.node", "1"), stopped_at.to_string().as_str(), "60000", build_id.as_str()), "{a:?}");
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
    let t = view(&PathName::new("mesh1.node-admin", "1").unwrap());
    let old = birth(&t, &stopped());
    let cp = plane(builds.clone(), t, &PathName::new("mesh1.node-admin", "1").unwrap()).await;
    let b0 = cp.builds.current_build().await.unwrap().into_build_id().unwrap();
    period(&cp, &builds, &old, true, 1).await;
    assert_eq!(cp.sweep_stopped(None).await, Swept::Forever);
    assert_eq!(cp.builds.current_build().await.unwrap().into_build_id(), Some(b0), "nothing was submitted");
}

/// CONTRACT (§6.9 Age: "The current stopped period's own stop, recorded on the node record"):
/// `mesh1.node.1` was stopped ten ttl ago, started, and stopped again just now: its age is the latest
/// stop, and it is young. `mesh1.node.2` was stopped ten ttl ago and started, and is stopped with no
/// stop recorded since: the record holds no stop of its current stopped period, so it is skipped by
/// name, never expired.
#[tokio::test]
async fn an_earlier_stop_before_a_later_start_does_not_count() {
    let (spans, _g) = capture("i143-3072-current-period");
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view(&PathName::new("mesh1.node-admin", "1").unwrap());
    let (a, b) = (birth(&t, &stopped()), birth(&t, &young()));
    let cp = plane(builds.clone(), t, &PathName::new("mesh1.node-admin", "1").unwrap()).await;
    let b0 = cp.builds.current_build().await.unwrap().into_build_id().unwrap();
    let long_ago = now(&cp) - 10 * TTL.as_millis() as u64;
    period(&cp, &builds, &a, true, long_ago).await;
    period(&cp, &builds, &a, false, long_ago + 1).await;
    let latest = now(&cp);
    period(&cp, &builds, &a, true, latest).await;
    period(&cp, &builds, &b, true, long_ago).await;
    period(&cp, &builds, &b, false, long_ago + 1).await;

    let verdicts = cp.scan_stopped(TTL, now(&cp)).await;
    assert!(verdicts.contains(&Verdict::Young { path: a.0.path.clone(), name: a.0.name.clone(), stopped_at_ms: latest }), "{verdicts:?}");
    assert!(verdicts.iter().any(|v| matches!(v, Verdict::NotRecorded { path, name, .. } if *path == b.0.path && *name == b.0.name)), "{verdicts:?}");
    assert_eq!(cp.sweep_stopped(Some(TTL)).await, Swept::Nothing);
    assert_eq!(cp.builds.current_build().await.unwrap().into_build_id(), Some(b0));
    assert!(spans.spans("rdm.node_admin.node.reject.via-no-recorded-stop").iter().any(|(_, x)| (x["node_path"].as_str(), x["node_name"].as_str()) == (young().path.as_str(), young().name.as_str())));
}

/// CONTRACT (§6.9 Action: "a `BuildInProgress` refusal loses nothing, the candidate is seen
/// again"): two nodes stopped past the ttl, and another topology Build in flight. The sweep is
/// refused `BuildInProgress` naming that Build and accepts nothing; once it settles, the next sweep
/// collects the first candidate, and the sweep after that delete settles collects the second.
#[tokio::test]
async fn a_candidate_refused_build_in_progress_is_collected_on_a_later_sweep() {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view(&PathName::new("mesh1.node-admin", "1").unwrap());
    let (a, b) = (birth(&t, &stopped()), birth(&t, &young()));
    let cp = plane(builds.clone(), t, &PathName::new("mesh1.node-admin", "1").unwrap()).await;
    period(&cp, &builds, &a, true, 1).await;
    period(&cp, &builds, &b, true, 2).await;
    let other = cp.submit("build.create", crate::named_request(&cp, move |t: &rdm_admin::topology::Topology| crate::grown_by_one(t, "mesh1", "node")).await).await.expect("a topology change is accepted").build_id;

    assert_eq!(cp.sweep_stopped(Some(TTL)).await, Swept::BuildInProgress { path: a.0.path.clone(), name: a.0.name.clone(), current_build_id: other.clone() });
    assert_eq!(cp.builds.current_build().await.unwrap().into_build_id(), Some(other.clone()));
    settle(&builds, &other, &executor(&cp).await).await;
    let first = match cp.sweep_stopped(Some(TTL)).await {
        Swept::Submitted { path, name, build_id } if *path == a.0.path && *name == a.0.name => build_id,
        s => panic!("the next sweep collects the first candidate: {s:?}"),
    };
    // One topology Build at a time: the second candidate waits for the delete to settle.
    assert!(matches!(cp.sweep_stopped(Some(TTL)).await, Swept::BuildInProgress { ref path, ref name, ref current_build_id } if *path == b.0.path && *name == b.0.name && *current_build_id == first));
    settle(&builds, &first, &executor(&cp).await).await;
    cp.topology.write().await.nodes.retain(|n| n.location() != a.0);
    assert!(matches!(cp.sweep_stopped(Some(TTL)).await, Swept::Submitted { ref path, ref name, .. } if *path == b.0.path && *name == b.0.name));
}

/// CONTRACT (§6.9 Who: "On a fabric-primary handover the successor sweeps from the same durable
/// records"): the node records and the Build log of the old fabric-primary are held by its successor
/// `mesh1.node-admin.2`. The successor ages `mesh1.node.1` from the same recorded stop (its time, never the
/// handover's) and collects it; the old primary, no longer holding the fabric-primary role, sweeps
/// nothing.
#[tokio::test]
async fn a_successor_fabric_primary_sweeps_from_the_same_receipts() {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view(&PathName::new("mesh1.node-admin", "1").unwrap());
    let old = birth(&t, &stopped());
    let first = plane(builds.clone(), t.clone(), &PathName::new("mesh1.node-admin", "1").unwrap()).await;
    let stopped_at = now(&first) - 10 * TTL.as_millis() as u64;
    period(&first, &builds, &old, true, stopped_at).await;

    // The handover: mesh1.node-admin.2 holds the role and the same Build log.
    let mut moved = t.clone();
    for n in moved.nodes.iter_mut() {
        n.is_fabric_primary = n.location() == rdm_core::PathName::new("mesh1.node-admin", "2").unwrap();
        if n.r#type == "node-admin" {
            n.is_primary = n.is_fabric_primary;
        }
    }
    *first.topology.write().await = moved.clone();
    let held = Arc::new(MemoryBuildStateAdapter::new());
    held.absorb(&builds.facts().await.unwrap()).await;
    let accepted = AcceptedStore::seeded(&*held, moved.fabric.id.clone(), desired(&moved), &moved.node(&PathName::new("mesh1.node-admin", "2").unwrap()).unwrap().birth().unwrap(), "mesh1").await.unwrap();
    let successor = crate::control_plane(held.clone(), accepted, rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), moved, crate::adopted_time());
    let _ = successor.node_rows.set(first.node_rows.get().unwrap().clone());
    let b = successor.builds.current_build().await.unwrap().into_build_id().unwrap();
    if !matches!(held.read_build(&b).await.unwrap().state, rdm_admin::build_state::BuildState::Complete) {
        settle(&held, &b, &executor(&successor).await).await;
    }

    let verdicts = successor.scan_stopped(TTL, now(&successor)).await;
    assert!(verdicts.iter().any(|v| matches!(v, Verdict::Expired(c) if c.location() == old.0 && c.stopped_at_ms == stopped_at)), "the successor ages the node from the same recorded stop: {verdicts:?}");
    assert!(matches!(successor.sweep_stopped(Some(TTL)).await, Swept::Submitted { ref path, ref name, .. } if *path == old.0.path && *name == old.0.name));
    assert_eq!(first.sweep_stopped(Some(TTL)).await, Swept::NotFabricPrimary);
}

/// CONTRACT (§6.9 Selection, "Distinct facts get distinct arms and distinct spans"): once the sweep's
/// node.delete Build of `mesh1.node.1` is accepted, the view still holds the stopped birth until the
/// delete runs. The next sweep says why it is not collected again: its delete is under way, spanned
/// `rdm.node_admin.node.reject.via-delete-under-way` with the node. Must NOT happen: that fact told as
/// a missing recorded stop (`via-no-recorded-stop`).
#[tokio::test]
async fn a_stopped_node_whose_delete_is_under_way_is_skipped_by_its_own_name() {
    let (spans, _g) = capture("i143-3072-delete-under-way");
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view(&PathName::new("mesh1.node-admin", "1").unwrap());
    let old = birth(&t, &stopped());
    let cp = plane(builds.clone(), t, &PathName::new("mesh1.node-admin", "1").unwrap()).await;
    period(&cp, &builds, &old, true, 1).await;
    let Swept::Submitted { build_id, .. } = cp.sweep_stopped(Some(TTL)).await else { panic!("the expired node is collected") };

    assert_eq!(cp.sweep_stopped(Some(TTL)).await, Swept::Nothing, "the delete is the one Build for it");
    assert_eq!(cp.builds.current_build().await.unwrap().into_build_id(), Some(build_id));
    let under_way = spans.spans("rdm.node_admin.node.reject.via-delete-under-way");
    assert!(under_way.iter().any(|(_, a)| (a["node_path"].as_str(), a["node_name"].as_str()) == (stopped().path.as_str(), stopped().name.as_str())), "{under_way:?}");
    let told_as_missing = spans.spans("rdm.node_admin.node.reject.via-no-recorded-stop");
    assert!(!told_as_missing.iter().any(|(_, a)| (a["node_path"].as_str(), a["node_name"].as_str()) == (stopped().path.as_str(), stopped().name.as_str())), "a delete under way is not a missing recorded stop: {told_as_missing:?}");
}

/// CONTRACT (§6.9 Age: a birth whose record cannot be read is skipped by name, never treated as
/// expired): the fabric-primary holds no node records to read `mesh1.node.1`'s stop from. The sweep
/// skips it, spanned `rdm.node_admin.node.reject.via-unreadable-node-record` with the node and the
/// reason, and submits nothing. Must NOT happen: that fact told as no recorded stop
/// (`via-no-recorded-stop`), or a delete.
#[tokio::test]
async fn a_stopped_node_whose_record_cannot_be_read_is_skipped_by_its_own_name() {
    let (spans, _g) = capture("i143-3072-unreadable-record");
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = view(&PathName::new("mesh1.node-admin", "1").unwrap());
    let cp = plane_unbound(builds.clone(), t, &PathName::new("mesh1.node-admin", "1").unwrap()).await;
    let b0 = cp.builds.current_build().await.unwrap().into_build_id().unwrap();

    assert_eq!(cp.sweep_stopped(Some(TTL)).await, Swept::Nothing);
    assert_eq!(cp.builds.current_build().await.unwrap().into_build_id(), Some(b0), "nothing was submitted");
    let unreadable = spans.spans("rdm.node_admin.node.reject.via-unreadable-node-record");
    assert!(unreadable.iter().any(|(_, a)| (a["node_path"].as_str(), a["node_name"].as_str()) == (stopped().path.as_str(), stopped().name.as_str()) && a["reason"].contains("not bound")), "{unreadable:?}");
    let told_as_missing = spans.spans("rdm.node_admin.node.reject.via-no-recorded-stop");
    assert!(!told_as_missing.iter().any(|(_, a)| (a["node_path"].as_str(), a["node_name"].as_str()) == (stopped().path.as_str(), stopped().name.as_str())), "an unreadable record is not a missing stop: {told_as_missing:?}");
}

/// A builds store whose current-Build read fails, named: every other read passes through.
struct UnreadableCurrent(MemoryBuildStateAdapter);

#[async_trait::async_trait]
impl rdm_admin::build_state::BuildStateAdapter for UnreadableCurrent {
    async fn publish_accepted(&self, a: &rdm_admin::build_state::BuildAccepted) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.0.publish_accepted(a).await
    }
    async fn open_attempt(&self, o: &rdm_admin::build_state::AttemptOpened) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.0.open_attempt(o).await
    }
    async fn read_build(&self, id: &BuildId) -> Result<rdm_admin::build_state::BuildProjection, rdm_admin::build_state::BuildStateError> {
        self.0.read_build(id).await
    }
    async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, rdm_admin::build_state::BuildStateError> {
        Err(rdm_admin::build_state::BuildStateError::Io("the cell's builds store refuses the acceptance-order index read".into()))
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, rdm_admin::build_state::BuildStateError> {
        self.0.newest_accepted_at_ms().await
    }
    async fn list_active(&self) -> Result<Vec<rdm_admin::build_state::BuildProjection>, rdm_admin::build_state::BuildStateError> {
        self.0.list_active().await
    }
    async fn claim_attempt(&self, c: &rdm_admin::build_state::BuildAttemptClaim) -> Result<rdm_admin::build_state::ClaimOutcome, rdm_admin::build_state::BuildStateError> {
        self.0.claim_attempt(c).await
    }
    async fn adopt_claim(&self, c: &rdm_admin::build_state::BuildAttemptClaim) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.0.adopt_claim(c).await
    }
    async fn append_step_receipt(&self, r: &rdm_admin::build_state::BuildStepReceipt) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.0.append_step_receipt(r).await
    }
    async fn append_attempt_receipt(&self, r: &rdm_admin::build_state::BuildAttemptReceipt) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.0.append_attempt_receipt(r).await
    }
    async fn facts(&self) -> Result<Vec<rdm_admin::build_state::BuildFact>, rdm_admin::build_state::BuildStateError> {
        self.0.facts().await
    }
    async fn committed_facts(&self, build: Option<&rdm_admin::build::BuildId>) -> Result<Vec<rdm_admin::build_state::BuildFact>, rdm_admin::build_state::BuildStateError> {
        self.0.committed_facts(build).await
    }
    async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(BuildId, u32, rdm_core::OperationId)>, rdm_admin::build_state::BuildStateError> {
        self.0.mesh_deleted_of(node_id, incarnation).await
    }
    async fn forget(&self, id: &BuildId) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.0.forget(id).await
    }
}

/// CONTRACT (§6.9 Age, "Distinct facts get distinct arms and distinct spans"; CLAUDE.md: "Never
/// written" and "could not be read" are different errors): a builds store whose current-Build read
/// fails cannot say whether a stopped node is still in the accepted topology, so the sweep skips it
/// as an unreadable Build log (`rdm.node_admin.node.reject.via-unreadable-build-log`, naming the
/// store's error); a store that holds no current Build skips it as `via-no-current-build`. Neither is
/// a delete under way, and nothing is submitted.
#[tokio::test]
async fn a_stopped_node_whose_current_build_cannot_be_read_is_skipped_as_an_unreadable_build_log() {
    let (spans, _g) = capture("i143-3072-unreadable-accepted");
    let t = view(&PathName::new("mesh1.node-admin", "1").unwrap());
    let held = MemoryBuildStateAdapter::new();
    let accepted = AcceptedStore::seeded(&held, t.fabric.id.clone(), desired(&t), &t.node(&PathName::new("mesh1.node-admin", "1").unwrap()).unwrap().birth().unwrap(), "mesh1").await.unwrap();
    let cp = crate::control_plane(Arc::new(UnreadableCurrent(held)), accepted.clone(), rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), t.clone(), crate::adopted_time());
    assert_eq!(cp.sweep_stopped(Some(TTL)).await, Swept::Nothing);
    let unreadable = spans.spans("rdm.node_admin.node.reject.via-unreadable-build-log");
    assert!(unreadable.iter().any(|(_, a)| (a["node_path"].as_str(), a["node_name"].as_str()) == (stopped().path.as_str(), stopped().name.as_str()) && a["reason"].contains("refuses the acceptance-order index read")), "{unreadable:?}");
    let empty = crate::control_plane(Arc::new(MemoryBuildStateAdapter::new()), accepted, rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), t, crate::adopted_time());
    assert_eq!(empty.sweep_stopped(Some(TTL)).await, Swept::Nothing);
    let none = spans.spans("rdm.node_admin.node.reject.via-no-current-build");
    assert!(none.iter().any(|(_, a)| (a["node_path"].as_str(), a["node_name"].as_str()) == (stopped().path.as_str(), stopped().name.as_str())), "{none:?}");
    assert!(spans.spans("rdm.node_admin.node.reject.via-delete-under-way").is_empty(), "neither is a delete under way");
}
