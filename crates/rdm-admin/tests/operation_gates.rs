//! The gates before an operation (build.md §4.4): every operation on the topology, a Build or a
//! standalone node.stop, node.start, node.connection.delete or node.delete, passes the
//! fabric-primary's gates in the page's order before anything runs; each gate reads one source, and a
//! gate that fails refuses by name and nothing is written.
//!
//! Each cell drives the fabric-primary's control plane in process over the memory Build store, with
//! the view set by the cell. Nothing here depends on timing.

use rdm_admin::accepted::{AcceptedStore, AttemptAction, FabricTopology, TopologyChange};
use rdm_admin::build::{BuildId, BuildReject};
use rdm_admin::build_claim::AttemptContexts;
use rdm_admin::build_state::{
    AttemptOpened, AttemptReason, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome, LocalBuildLog, MemoryBuildStateAdapter,
};
use rdm_admin::control_plane::{ActionKind, ControlPlane, Refusal};
use rdm_admin::investigate::PeerMesh;
use rdm_admin::model::*;
use rdm_admin::node_investigation::Finding;
use rdm_admin::topology::Topology;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn node(name: &str, status: NodeStatus, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = status;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// Two meshes: mesh1 (the fabric-primary's) with an admin and two rpc nodes, rpc.1 in `rpc1`, and
/// mesh2 with its admin.
fn view(rpc1: NodeStatus) -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![
            Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready },
            Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready },
        ],
        nodes: vec![
            node("mesh1.admin.1", NodeStatus::Ready, true, true),
            node("mesh1.rpc.1", rpc1, false, false),
            node("mesh1.rpc.2", NodeStatus::Ready, false, false),
            node("mesh2.admin.1", NodeStatus::Ready, true, false),
        ],
    }
}

/// The Build store; armed, a racer's open of another action takes the attempt number a request asks
/// for the moment before the request's own open reaches the store (another writer's insert won).
struct Raced {
    inner: MemoryBuildStateAdapter,
    armed: AtomicBool,
}

#[async_trait::async_trait]
impl BuildStateAdapter for Raced {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(accepted).await
    }
    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        if self.armed.swap(false, Ordering::SeqCst) {
            let path: PathName = "mesh1.rpc.2".parse().unwrap();
            let racer = AttemptOpened { action: Some(AttemptAction::Stop { path, node_id: NodeId::mint(), from_incarnation: IncarnationId::mint() }), opened_by: "the racer".into(), ..opened.clone() };
            self.inner.open_attempt(&racer).await?;
        }
        self.inner.open_attempt(opened).await
    }
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.inner.read_build(build_id).await
    }
    async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, BuildStateError> {
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError> {
        self.inner.newest_accepted_at_ms().await
    }
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        self.inner.claim_attempt(claim).await
    }
    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.inner.adopt_claim(claim).await
    }
    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        self.inner.append_step_receipt(receipt).await
    }
    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.inner.append_attempt_receipt(receipt).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(build_id).await
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

struct Rig {
    builds: Arc<Raced>,
    contexts: Arc<MemoryBuildStateAdapter>,
    cp: Arc<ControlPlane>,
}

async fn rig(t: Topology) -> Rig {
    let builds = Arc::new(Raced { inner: MemoryBuildStateAdapter::new(), armed: AtomicBool::new(false) });
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let contexts = Arc::new(MemoryBuildStateAdapter::new());
    let mut cp = ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t.clone(), crate::adopted_time())
        .with_contexts(Arc::new(AttemptContexts::on(contexts.clone() as Arc<dyn LocalBuildLog>)));
    // The node records a create writes: the gates read a birth from its record, never from the gossip view (Q207).
    let _ = cp.node_rows.set(crate::records_of(&t).await);
    cp.topology = Arc::new(tokio::sync::RwLock::new(t));
    Rig { builds, contexts, cp: Arc::new(cp) }
}

fn reason(r: &Result<rdm_admin::control_plane::Opened, Refusal>) -> String {
    match r {
        Ok(o) => format!("admitted as attempt {} of {}", o.attempt, o.build_id),
        Err(Refusal::Reject(j)) => j.reason().to_string(),
        Err(Refusal::BuildInProgress(h)) => format!("build-in-progress (mesh {:?})", h.mesh),
        Err(other) => format!("{other:?}"),
    }
}

/// CONTRACT (build.md §4.4, gate order): "The node's mesh holds no attempt of its own" (third) is
/// read before "The node's state admits the action" (fifth): a node.delete Build in a mesh its own
/// drift attempt holds is refused `build-in-progress` naming the mesh, whatever the node's state.
/// Must NOT happen: the delete refused `node-not-live` by the later gate first.
#[tokio::test]
async fn a_delete_in_a_mesh_its_own_attempt_holds_is_refused_build_in_progress_before_the_state_gate() {
    let r = rig(view(NodeStatus::PendingReconnect)).await;
    let current = r.builds.current().await.unwrap().expect("the seeded Build is current");
    let drift = AttemptOpened { mesh: Some("mesh1".into()), build_id: current.build_id.clone(), attempt: 1, reason: AttemptReason::ProvenDrift, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 1, context: None };
    r.builds.open_attempt(&drift).await.expect("mesh1's drift lane opens its attempt 1");
    let out = r.cp.submit("gates-delete-held", TopologyChange::RemoveNode { node: "mesh1.rpc.1".parse().unwrap() }).await;
    match &out {
        Err(Refusal::BuildInProgress(held)) => assert_eq!(held.mesh.as_deref(), Some("mesh1"), "the refusal names the held mesh"),
        _ => panic!("the delete is refused build-in-progress naming mesh1 before its state is read, not {}", reason(&out)),
    }
}

/// CONTRACT (build.md §4.4, gate order): "A node stands at the path" (fourth) is read before "The
/// node's mesh is heard" (sixth): a steps Build whose step acts on a path no node stands at, in a peer
/// mesh held unheard, is refused `unknown-node` naming the step (Q223).
/// Must NOT happen: the steps refused `unheard-mesh` by the later gate first.
#[tokio::test]
async fn a_step_on_no_node_is_refused_unknown_before_the_heard_gate() {
    let r = rig(view(NodeStatus::Ready)).await;
    let _ = r.cp.peer_mesh.set(Arc::new(|m: &str| (m == "mesh2").then_some(PeerMesh { unheard_ms: Some(40_000), recovery_decided: false, unreachable_decided: false })));
    let steps = vec![rdm_core::build::Step::Stop { node: "mesh2.rpc.9".parse().unwrap() }];
    let span = tracing::info_span!("gates-steps");
    let out = r.cp.open_steps_in(span, "gates-steps-unknown", steps).await;
    assert!(matches!(&out, Err(Refusal::Reject(BuildReject::UnknownNode { step: Some(1), .. }))), "the steps are refused unknown-node, naming the step, before the heard gate (Q223), not {}", reason(&out));
}

/// CONTRACT (build.md §4.4: "A gate that fails refuses by name and nothing is written"; gate "The
/// attempt is claimed | the Build store | `attempt-taken`"): a node.stop whose attempt number another
/// writer's open took first is refused `attempt-taken`, and the context that attempt holds is the
/// other writer's still.
/// Must NOT happen: the refused stop's context put over the attempt another action holds.
#[tokio::test]
async fn an_attempt_taken_refusal_writes_no_attempt_context() {
    let r = rig(view(NodeStatus::Ready)).await;
    let current = r.builds.current().await.unwrap().expect("the seeded Build is current");
    let theirs = rdm_core::context::CallContext { caller_system: Some("the racer".into()), traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()), tracestate: None, baggage: None };
    // The number the stop asks for: the fabric lane's next.
    let n = current.attempt + 1;
    r.contexts.put_attempt_context(&current.build_id, None, n, &theirs).await.unwrap();
    r.builds.armed.store(true, Ordering::SeqCst);
    let span = tracing::info_span!("gates-attempt-taken");
    let out = r.cp.open_attempt_in(span, "gates-attempt-taken", AttemptReason::Stop, "mesh1.rpc.1".parse().unwrap(), ActionKind::Stop).await;
    assert!(matches!(&out, Err(Refusal::AttemptTaken(_))), "the stop is refused attempt-taken, not {}", reason(&out));
    let held = r.contexts.attempt_context(&current.build_id, None, n).await.unwrap();
    assert_eq!(held, Some(theirs), "nothing is written by the refused stop: attempt {n}'s context is the racer's");
}

/// The Build store; every open is followed at once by a drive pass's claim of the attempt it opened,
/// decided before the opener's call returns: what a drive pass already running does when it claims
/// the attempt the moment the open lands.
struct ClaimedInTheOpen {
    inner: MemoryBuildStateAdapter,
    contexts: Arc<AttemptContexts>,
    claims: rdm_admin::storage::MemoryMeshStorage,
    claimed: std::sync::Mutex<Option<rdm_core::context::CallContext>>,
}

#[async_trait::async_trait]
impl BuildStateAdapter for ClaimedInTheOpen {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(accepted).await
    }
    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        self.inner.open_attempt(opened).await?;
        let claim = BuildAttemptClaim { build_id: opened.build_id.clone(), attempt: opened.attempt, executor: "mesh1.admin.1".into(), claimant: Some("mesh1.admin.1".into()), mesh: None };
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        if let rdm_core::build_claim::BuildClaimReply::Won { context } = rdm_admin::build_claim::decide_claim(&me, &self.inner, &self.claims, &self.contexts, &claim, "mesh1", None).await {
            *self.claimed.lock().unwrap() = Some(context);
        }
        Ok(())
    }
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.inner.read_build(build_id).await
    }
    async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, BuildStateError> {
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError> {
        self.inner.newest_accepted_at_ms().await
    }
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        self.inner.claim_attempt(claim).await
    }
    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.inner.adopt_claim(claim).await
    }
    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        self.inner.append_step_receipt(receipt).await
    }
    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.inner.append_attempt_receipt(receipt).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(build_id).await
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

/// CONTRACT (build.md §4.4, gate "The attempt is claimed"; build_claim.rs `for_claim_lane`: "The
/// context `attempt` is claimed with: its own"): an attempt's call context is written with its open,
/// in the one record, so a drive pass that claims the attempt the moment the open lands claims it with
/// the opening request's context, never the previous attempt's.
/// Must NOT happen: the claim between the open and a later context write taking the previous
/// attempt's context (its reconcile parented to another request's span).
#[tokio::test]
async fn a_claim_the_moment_the_open_lands_takes_the_opening_requests_context() {
    let t = view(NodeStatus::Ready);
    let contexts = Arc::new(AttemptContexts::on(Arc::new(MemoryBuildStateAdapter::new()) as Arc<dyn LocalBuildLog>));
    let builds = Arc::new(ClaimedInTheOpen { inner: MemoryBuildStateAdapter::new(), contexts: contexts.clone(), claims: Default::default(), claimed: Default::default() });
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let mut cp = ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t.clone(), crate::adopted_time()).with_contexts(contexts.clone());
    // The node records a create writes: the gates read a birth from its record, never from the gossip view (Q207).
    let _ = cp.node_rows.set(crate::records_of(&t).await);
    cp.topology = Arc::new(tokio::sync::RwLock::new(t));
    let current = builds.current().await.unwrap().expect("the seeded Build is current");
    // The previous attempt's context: another request's trace.
    let previous = rdm_core::context::CallContext { caller_system: Some("rdm".into()), traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()), tracestate: None, baggage: None };
    contexts.put(&current.build_id, current.attempt, &previous).await.unwrap();
    let opened = cp
        .open_attempt_in(tracing::info_span!("gates-claimed-in-the-open"), "gates-claimed-in-the-open", AttemptReason::Stop, "mesh1.rpc.1".parse().unwrap(), ActionKind::Stop)
        .await
        .expect("the stop is admitted");
    assert_eq!(opened.attempt, current.attempt + 1);
    let claimed = builds.claimed.lock().unwrap().clone().expect("the drive pass won the attempt's claim");
    assert_ne!(claimed.traceparent, previous.traceparent, "the claim took the previous attempt's context: {claimed:?}");
    assert_eq!(contexts.get(&opened.build_id, opened.attempt).await.unwrap(), Some(claimed), "the attempt's context is the one its claim took");
}

/// Node investigation as the cells set it (node-investigation.md): the provider reads the one exact
/// birth `exited` names (node id and incarnation) exited; every other birth is unproven.
struct Investigation {
    exited: Option<(NodeId, IncarnationId)>,
}

#[async_trait::async_trait]
impl rdm_admin::seat_revision::PathProbe for Investigation {
    async fn investigate(&self, node: &Node) -> Finding {
        match &self.exited {
            Some((id, incarnation)) if *id == node.node_id && Some(incarnation) == node.incarnation_id.as_ref() => Finding::Exited { code: Some(137) },
            _ => Finding::Unproven { reason: "the provider holds no exited runtime of this exact birth".into() },
        }
    }
}

/// The gates rig over `t`, whose accepted topology names mesh1.rpc.1, with the fabric-primary's view
/// then marking that birth Dead: silence, the same birth (node id and incarnation) unheard.
async fn dead_rig(t: Topology) -> Rig {
    let r = rig(t).await;
    let path: PathName = "mesh1.rpc.1".parse().unwrap();
    r.cp.topology.write().await.nodes.iter_mut().find(|n| n.name == path).unwrap().status = NodeStatus::Dead;
    r
}

fn birth_of(t: &Topology, path: &str) -> (NodeId, IncarnationId) {
    let n = t.node(&path.parse().unwrap()).unwrap();
    (n.node_id.clone(), n.incarnation_id.clone().unwrap())
}

async fn delete_of(r: &Rig, path: &str) -> Result<rdm_admin::control_plane::Opened, Refusal> {
    r.cp.submit("gates-delete", TopologyChange::RemoveNode { node: path.parse().unwrap() }).await
}

/// CONTRACT (build.md §4.4 gate 5: "a delete a stopped one, one whose exit is proven, or a live one";
/// node-delete.md Never evidence: "Gossip, silence, Ping failure and timeout never say the runtime
/// exited"; Q219): a birth the view marks Dead (silence) that is neither stopped nor has the node
/// investigation's `exited` verdict fails gate 5 `node-not-live`, with no investigation held and
/// with one that cannot say it exited.
/// Must NOT happen: a silence-derived Dead birth admitted as live or as exited.
#[tokio::test]
async fn a_dead_birth_with_no_exited_verdict_is_refused_node_not_live() {
    let r = dead_rig(view(NodeStatus::Ready)).await;
    let before = r.builds.current().await.unwrap().map(|b| b.build_id);
    let out = delete_of(&r, "mesh1.rpc.1").await;
    assert!(matches!(&out, Err(Refusal::Reject(BuildReject::NodeNotLive { .. }))), "no investigation is held: refused node-not-live, not {}", reason(&out));
    let _ = r.cp.investigation.set(Arc::new(Investigation { exited: None }));
    let out = delete_of(&r, "mesh1.rpc.1").await;
    assert!(matches!(&out, Err(Refusal::Reject(BuildReject::NodeNotLive { .. }))), "the investigation's verdict is unproven: refused node-not-live, not {}", reason(&out));
    assert_eq!(r.builds.current().await.unwrap().map(|b| b.build_id), before, "nothing is accepted");
}

/// CONTRACT (build.md §4.4 gate 5, "one whose exit is proven"; node-delete.md Crashed target; Q219:
/// "The exact runtime's Exited admits it"): a Dead birth whose exact runtime the node investigation
/// reads exited is admitted, and its delete carries that birth.
/// Must NOT happen: the exited birth refused, or its delete planned for another birth.
#[tokio::test]
async fn the_exact_runtimes_exited_verdict_admits_a_dead_births_delete() {
    let t = view(NodeStatus::Ready);
    let r = dead_rig(t.clone()).await;
    let _ = r.cp.investigation.set(Arc::new(Investigation { exited: Some(birth_of(&t, "mesh1.rpc.1")) }));
    let out = delete_of(&r, "mesh1.rpc.1").await;
    let opened = out.unwrap_or_else(|e| panic!("the exited birth's delete is admitted, not {}", reason(&Err(e))));
    let build = r.builds.read_build(&opened.build_id).await.unwrap();
    let (node_id, incarnation) = birth_of(&t, "mesh1.rpc.1");
    assert!(build.topology.pinned.iter().any(|(p, b)| p.to_string() == "mesh1.rpc.1" && b.node_id == node_id && b.incarnation == incarnation), "the delete is pinned to the exited birth: {:?}", build.topology.pinned);
}

/// CONTRACT (Q219: "Another birth's exit evidence admits nothing"): the investigation reads exited a
/// runtime of the same node id under another incarnation, or of another node id under the same
/// incarnation; the Dead birth at the path is not that runtime, and its delete is refused
/// `node-not-live`.
/// Must NOT happen: exit evidence matched on the node id alone or the incarnation alone.
#[tokio::test]
async fn another_births_exit_evidence_admits_nothing() {
    let t = view(NodeStatus::Ready);
    let (node_id, incarnation) = birth_of(&t, "mesh1.rpc.1");
    for other in [(node_id.clone(), IncarnationId::mint()), (NodeId::mint(), incarnation.clone())] {
        let r = dead_rig(t.clone()).await;
        let _ = r.cp.investigation.set(Arc::new(Investigation { exited: Some(other) }));
        let out = delete_of(&r, "mesh1.rpc.1").await;
        assert!(matches!(&out, Err(Refusal::Reject(BuildReject::NodeNotLive { .. }))), "another birth's exit admits nothing: refused node-not-live, not {}", reason(&out));
    }
}

/// CONTRACT (build.md §4.4 gate 5, "or a live one, which its own sequence stops first (node.delete
/// Live target)"; Q220): a live birth's delete is admitted and planned as node.delete of that birth,
/// whose sequence composes node.stop before TerminateRuntime (`node_delete_pipeline`'s order); no
/// investigation is asked.
/// Must NOT happen: a live birth refused, or planned as the delete of a stopped birth, which skips its stop.
#[tokio::test]
async fn a_live_births_delete_is_admitted_and_planned_to_stop_it_first() {
    let t = view(NodeStatus::Ready);
    let r = rig(t.clone()).await;
    let out = delete_of(&r, "mesh1.rpc.1").await;
    let opened = out.unwrap_or_else(|e| panic!("the live birth's delete is admitted, not {}", reason(&Err(e))));
    let build = r.builds.read_build(&opened.build_id).await.unwrap();
    let ops = rdm_admin::accepted::plan_for_build(&build, &t).operations;
    let delete = ops.iter().find(|op| op.key() == "node.delete:mesh1.rpc.1").unwrap_or_else(|| panic!("the delete is planned: {ops:?}"));
    assert!(matches!(delete, rdm_admin::build::BuildOperation::DeleteLiveNode { exited: None, birth: Some(_), .. }), "the live birth's delete composes its stop: {delete:?}");
}

/// CONTRACT (build.md §4.4 gate 6: "The node's mesh is heard | the fabric-primary's view | refused by
/// name, naming the mesh and how long it has gone unheard (R-D1)"; Q221 X: "Gate 6 runs at acceptance
/// for every ordinary Build"): a node.delete Build on a node of mesh2, which the fabric-primary's
/// investigation holds unheard, is refused at acceptance naming mesh2 and how long it is unheard, and
/// nothing is accepted.
/// Must NOT happen: the Build accepted and held only later, at its drive.
#[tokio::test]
async fn a_build_on_a_node_of_an_unheard_mesh_is_refused_at_acceptance() {
    let mut t = view(NodeStatus::Ready);
    t.nodes.push(node("mesh2.rpc.1", NodeStatus::Ready, false, false));
    let r = rig(t).await;
    let _ = r.cp.peer_mesh.set(Arc::new(|m: &str| (m == "mesh2").then_some(PeerMesh { unheard_ms: Some(40_000), recovery_decided: false, unreachable_decided: false })));
    let before = r.builds.current().await.unwrap().map(|b| b.build_id);
    let out = delete_of(&r, "mesh2.rpc.1").await;
    match &out {
        Err(Refusal::Reject(BuildReject::UnheardMesh { mesh, unheard_ms, .. })) => assert_eq!((mesh.as_str(), *unheard_ms), ("mesh2", 40_000)),
        _ => panic!("refused naming mesh2 and how long it is unheard, not {}", reason(&out)),
    }
    assert_eq!(r.builds.current().await.unwrap().map(|b| b.build_id), before, "nothing is accepted");
}
