//! R-A1 / R-T4 / R-X1 / R-BI7 functional: a Build attempt's claim is decided by its claimant's
//! claim door (the fabric-primary for a topology attempt, a mesh's mesh-primary for that mesh's
//! drift attempt), and `BuildClaim` (op `0x1C`) is that door on the wire.
//!
//! Each admin is a real Node RPC server on loopback with its own Build log and its own view. An
//! executor never claims (the fabric-primary claims for it and calls it with `build.attempt.run`),
//! so these cells put claims to the door the way the fabric-primary does: a claim past the log the
//! door holds is refused by name (`Lost`, `NotOpen`), an admin that is not the claimant decides
//! nothing and names who may claim, and the context of an attempt rides `Won`.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::{EndpointId, IncarnationId, NodeId, NodeKind};
use rdm_admin::accepted::FabricTopology;
use rdm_admin::build::BuildId;
use rdm_admin::build_claim::{self, AttemptContexts, ClaimDoor};
use rdm_admin::mesh_claims::InFlight;
use rdm_admin::build_state::{
    AttemptOpened, AttemptOutcome, AttemptReason, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter, MemoryBuildStateAdapter,
};
use rdm_admin::model::{Fabric, Node, NodeStatus, ProviderKind, ScopeStatus};
use rdm_admin::topology::Topology;
use rdm_transport::{NodeRpcClient, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rdm_core::build_claim::BuildClaimReply;
use rdm_core::context::CallContext;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

struct Birth {
    node: Node,
    key: SecretKey,
}

fn birth(name: &str, fabric_primary: bool) -> Birth {
    let key = SecretKey::generate();
    let mut node = Node::allocated(name.parse().unwrap());
    node.kind = NodeKind::NodeAdmin;
    node.mesh = "mesh1".into();
    node.node_id = NodeId::mint();
    node.endpoint_id = Some(EndpointId(key.public().to_string()));
    node.incarnation_id = Some(IncarnationId::mint());
    node.provider = Some(ProviderKind::Process);
    node.status = NodeStatus::Ready;
    node.is_primary = fabric_primary;
    node.is_fabric_primary = fabric_primary;
    node.transport_addr = Some("127.0.0.1:1".parse().unwrap());
    Birth { node, key }
}

fn view(nodes: Vec<Node>) -> Arc<RwLock<Topology>> {
    Arc::new(RwLock::new(Topology {
        fabric: Fabric { id: rdm_core::FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: Vec::new(),
        nodes,
    }))
}

/// An admin serving `BuildClaim` over its own Build log and its own view.
struct Admin {
    resolved: ResolvedNode,
    door: Arc<ClaimDoor>,
    _router: Router,
}

async fn elect(
    b: &Birth,
    topology: Arc<RwLock<Topology>>,
    builds: Arc<dyn BuildStateAdapter>,
    contexts: Arc<AttemptContexts>,
) -> Admin {
    let door = Arc::new(ClaimDoor {
        me: b.node.name.clone(),
        node_id: b.node.node_id.clone(),
        topology,
        builds,
        contexts,
        claims: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()),
        claimed: Default::default(),
    });
    let slot: build_claim::ClaimSlot = Arc::new(OnceLock::new());
    let _ = slot.set(door.clone());
    let server = build_claim::serve(ServerBuilder::new(), slot)
        .build(ServedBirth { node_id: b.node.node_id.to_string(), incarnation: b.node.incarnation_id.clone().unwrap().to_string() })
        .unwrap();
    let ep = rdm_transport::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
    let resolved = ResolvedNode {
        node_id: b.node.node_id.clone(),
        name: b.node.name.clone(),
        endpoint_id: b.key.public(),
        transport_addr: addr,
        incarnation: b.node.incarnation_id.clone().unwrap(),
    };
    Admin { resolved, door, _router: router }
}

/// A Build whose log holds `settled` converged attempts: accepted, then claim + converged each.
async fn settled(builds: &MemoryBuildStateAdapter, id: &BuildId, holder: &str, settled: u32) {
    builds
        .publish_accepted(&BuildAccepted { build_id: id.clone(), topology: FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    for attempt in 1..=settled {
        if attempt > 1 {
            open(builds, id, attempt).await;
        }
        builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: id.clone(), attempt, executor: holder.into() }).await.unwrap();
        builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
    }
}

async fn open(builds: &MemoryBuildStateAdapter, id: &BuildId, attempt: u32) {
    builds.open_attempt(&AttemptOpened { mesh: None, build_id: id.clone(), attempt, reason: AttemptReason::Stop, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 0, context: None }).await.unwrap();
}

/// A fabric-primary P whose log holds the Build at `settled` converged attempts, listing the
/// re-born executor's birth in its view the way the product's digest view does.
async fn primary(id: &BuildId, settled_attempts: u32, contexts: Arc<AttemptContexts>, executor: &Birth) -> (Birth, Admin, Arc<MemoryBuildStateAdapter>) {
    let p = birth("mesh1.admin.1", true);
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    settled(&builds, id, "mesh1.admin.1", settled_attempts).await;
    let topology = view(vec![p.node.clone(), executor.node.clone()]);
    let s = elect(&p, topology, builds.clone(), contexts).await;
    (p, s, builds)
}

/// CONTRACT: a claim of an attempt the fabric-primary's log holds for another executor is `Lost`,
/// naming the holder, and the log is unchanged by it. Whatever log the claimant holds is never
/// consulted: only the fabric-primary's decides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_of_a_held_attempt_is_lost_to_its_claimant_and_changes_nothing() {
    let id = BuildId::mint();
    let executor = birth("mesh1.admin.2", false);
    let (_p, p_admin, p_builds) = primary(&id, 6, Arc::new(AttemptContexts::in_memory()), &executor).await;
    assert_eq!(p_admin.door.claim("mesh1.admin.2", &id, 1).await, BuildClaimReply::Lost { holder: "mesh1.admin.1".into() });
    assert_eq!(p_builds.read_build(&id).await.unwrap().attempt, 6, "the fabric-primary's log is unchanged by the refused claim");
}

/// CONTRACT: the fabric-primary answers `NotOpen` for an attempt that is not the Build's next,
/// naming the one that is open; the open attempt is won once, and the loser of it is named.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_past_the_open_attempt_is_not_open_and_names_the_next() {
    let id = BuildId::mint();
    let executor = birth("mesh1.admin.2", false);
    let (_p, p_admin, p_builds) = primary(&id, 3, Arc::new(AttemptContexts::in_memory()), &executor).await;
    open(&p_builds, &id, 4).await;
    assert_eq!(p_admin.door.claim("mesh1.admin.2", &id, 9).await, BuildClaimReply::NotOpen { next: Some(4) });
    let first = p_admin.door.claim("mesh1.admin.2", &id, 4).await;
    assert!(matches!(first, BuildClaimReply::Won { .. }), "{first:?}");
    assert_eq!(p_admin.door.claim("mesh1.admin.2", &id, 4).await, first, "a repeat claim by the holder answers the same");
    assert_eq!(p_admin.door.claim("mesh1.admin.3", &id, 4).await, BuildClaimReply::Lost { holder: "mesh1.admin.2".into() });
}

/// CONTRACT (build.md §5.2: a second claimant of one attempt loses and runs nothing; R-BI7 caveat 3:
/// one attempt in flight per mesh): a claim of an attempt the store already holds for an executor of
/// another mesh is `Lost`, and the in-flight row it took in its own executor's mesh is released, so
/// that mesh is free for the Build's next attempt once the held attempt ends. A lost claim whose
/// holder acts in the same mesh leaves the row: it is the held attempt's, released at its verdict.
/// Must NOT happen: a lost claim that holds a mesh its attempt never acts in, refusing every later
/// attempt there build-in-progress (a successor fabric-primary whose first read had not yet seen the
/// attempt its predecessor claimed).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_lost_to_an_executor_of_another_mesh_leaves_its_own_mesh_free_for_the_next_attempt() {
    let id = BuildId::mint();
    let executor = birth("mesh1.admin.2", false);
    let (_p, p_admin, p_builds) = primary(&id, 0, Arc::new(AttemptContexts::in_memory()), &executor).await;
    let first = p_admin.door.claim("mesh1.admin.1", &id, 1).await;
    assert!(matches!(first, BuildClaimReply::Won { .. }), "{first:?}");
    // The same attempt, claimed again for an admin of mesh2 by a log that had not seen the first claim.
    assert_eq!(p_admin.door.claim("mesh2.admin.1", &id, 1).await, BuildClaimReply::Lost { holder: "mesh1.admin.1".into() });
    assert_eq!(p_admin.door.claims.in_flight("mesh2").await.unwrap(), None, "the lost claim holds no row in mesh2, where its attempt never acts");
    // Lost to a holder of the same mesh: the row is the held attempt's and stays.
    assert_eq!(p_admin.door.claim("mesh1.admin.2", &id, 1).await, BuildClaimReply::Lost { holder: "mesh1.admin.1".into() });
    let held = p_admin.door.claims.in_flight("mesh1").await.unwrap().expect("mesh1's row of the held attempt stands");
    assert_eq!((held.attempt, held.claimant.as_str()), (1, "mesh1.admin.1"));
    // Attempt 1 hands off to mesh2.admin.1, and its verdict releases mesh1's row, as the drive does.
    p_builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt: 1, outcome: AttemptOutcome::HandedOff { to: "mesh2.admin.1".into() } }).await.unwrap();
    p_admin.door.claims.release_in_flight("mesh1", &held).await.unwrap();
    let next = p_admin.door.claim("mesh2.admin.1", &id, 2).await;
    assert!(matches!(next, BuildClaimReply::Won { .. }), "the next attempt takes mesh2: {next:?}");
}

/// CONTRACT: `Won` carries the attempt's context from the fabric-primary's local record; an
/// attempt with no record of its own inherits the previous attempt's; an attempt nothing knows
/// starts a new trace (an empty context).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_won_claim_returns_the_attempts_context_and_a_hand_off_inherits_it() {
    let id = BuildId::mint();
    let contexts = Arc::new(AttemptContexts::in_memory());
    let executor = birth("mesh1.admin.2", false);
    let (_p, p_admin, p_builds) = primary(&id, 3, contexts.clone(), &executor).await;
    let rest = CallContext { caller_system: Some("rdm".into()), traceparent: Some(TRACEPARENT.into()), tracestate: None, baggage: None };
    contexts.put(&id, 4, &rest).await.unwrap();
    open(&p_builds, &id, 4).await;
    assert_eq!(p_admin.door.claim("mesh1.admin.2", &id, 4).await, BuildClaimReply::Won { context: rest.clone() });
    // The next attempt (a hand-off) has no record of its own.
    p_builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt: 4, outcome: AttemptOutcome::HandedOff { to: "mesh2.admin.1".into() } }).await.unwrap();
    // The verdict releases mesh1's in-flight row attempt 4 held, as the drive does at a hand-off
    // (R-BI7 caveat 3: one attempt in flight per mesh).
    p_admin.door.claims.release_in_flight("mesh1", &InFlight { build_id: id.clone(), lane: None, attempt: 4, claimant: "mesh1.admin.1".into(), claimant_node: None }).await.unwrap();
    assert_eq!(contexts.get(&id, 5).await.unwrap(), None);
    assert_eq!(p_admin.door.claim("mesh2.admin.1", &id, 5).await, BuildClaimReply::Won { context: rest.clone() }, "the hand-off continues the trace");
    assert_eq!(contexts.get(&id, 5).await.unwrap(), Some(rest), "and keeps it under its own attempt");
    // An unrelated Build the primary has no record for starts a new trace.
    let other = BuildId::mint();
    settled(&p_builds, &other, "mesh1.admin.1", 1).await;
    open(&p_builds, &other, 2).await;
    match p_admin.door.claim("mesh1.admin.2", &other, 2).await {
        BuildClaimReply::Won { context } => assert_eq!(context.traceparent, None),
        other => panic!("{other:?}"),
    }
}

/// CONTRACT (R-BI7 caveat 2: a topology Build's attempt is claimed by the fabric-primary, a mesh's
/// drift attempt by that mesh's mesh-primary, decided from the receiver's own view of the roles): an
/// admin that holds neither role decides nothing, for the fabric lane or its mesh's drift lane, and
/// names both claimants its view shows (`NotClaimant`). mesh2's mesh-primary, which is not the
/// fabric-primary, is refused a fabric-lane claim the same way and wins its own mesh's drift-lane
/// claim at its own door, recorded with itself as the claimant. Must NOT happen: a claim decided by
/// an admin that is not its claimant, or a refusal that does not name who may claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_put_to_an_admin_that_is_not_its_claimant_names_who_may_claim_and_a_mesh_primary_claims_its_own_drift_attempt() {
    let id = BuildId::mint();
    let executor = birth("mesh1.admin.2", false);
    let (p, _p_admin, _p_builds) = primary(&id, 2, Arc::new(AttemptContexts::in_memory()), &executor).await;
    let q = birth("mesh1.admin.3", false);
    let q_view = view(vec![p.node.clone(), q.node.clone(), executor.node.clone()]);
    let q_admin = elect(&q, q_view, Arc::new(MemoryBuildStateAdapter::new()), Arc::new(AttemptContexts::in_memory())).await;
    let names_both = BuildClaimReply::NotClaimant { mesh: Some("mesh1".into()), fabric_primary: Some("mesh1.admin.1".into()), mesh_primary: Some("mesh1.admin.1".into()) };
    assert_eq!(q_admin.door.claim("mesh1.admin.2", &id, 1).await, names_both, "an admin holding neither role decides no fabric-lane claim and names both claimants");
    assert_eq!(q_admin.door.claim_in("mesh1.admin.3", &id, Some("mesh1"), 1).await, names_both, "nor a claim of its mesh's drift lane");

    // mesh2's mesh-primary, not the fabric-primary: its own drift lane's attempt 1 is open.
    let mut m = birth("mesh2.admin.1", false);
    m.node.mesh = "mesh2".into();
    m.node.is_primary = true;
    let m_builds = Arc::new(MemoryBuildStateAdapter::new());
    settled(&m_builds, &id, "mesh1.admin.1", 2).await;
    m_builds.open_attempt(&AttemptOpened { mesh: Some("mesh2".into()), build_id: id.clone(), attempt: 1, reason: AttemptReason::Recovery, action: None, opened_by: "mesh2.admin.1".into(), opened_at_ms: 0, context: None }).await.unwrap();
    let m_admin = elect(&m, view(vec![p.node.clone(), m.node.clone()]), m_builds.clone(), Arc::new(AttemptContexts::in_memory())).await;
    let fabric_lane = m_admin.door.claim("mesh2.admin.1", &id, 3).await;
    assert_eq!(fabric_lane, BuildClaimReply::NotClaimant { mesh: Some("mesh2".into()), fabric_primary: Some("mesh1.admin.1".into()), mesh_primary: Some("mesh2.admin.1".into()) }, "a mesh-primary decides no fabric-lane claim");
    let own = m_admin.door.claim_in("mesh2.admin.1", &id, Some("mesh2"), 1).await;
    assert!(matches!(own, BuildClaimReply::Won { .. }), "mesh2's mesh-primary wins its own drift lane's claim: {own:?}");
    let p = m_builds.read_build(&id).await.unwrap();
    let lane = &p.lanes["mesh2"];
    assert_eq!((lane.attempt, lane.executor.as_deref(), lane.claimant.as_deref()), (1, Some("mesh2.admin.1"), Some("mesh2.admin.1")), "the drift lane's claim records its claimant: {lane:?}");
    assert_eq!((p.attempt, p.executor.as_deref()), (2, Some("mesh1.admin.1")), "the fabric lane is untouched");
}

/// CONTRACT: the claim is decided for the executor the request names, at its exact birth as the
/// fabric-primary's view holds it (node-rpc.md §14: never the caller): the executor's own endpoint
/// naming another birth of it is refused by name about that subject, and nothing is claimed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_naming_a_birth_the_view_does_not_hold_is_refused_by_name() {
    use rdm_core::build_claim::{BuildClaim, BuildClaimRequest};
    let id = BuildId::mint();
    let re_born_birth = birth("mesh1.admin.2", false);
    let (p, p_admin, p_builds) = primary(&id, 1, Arc::new(AttemptContexts::in_memory()), &re_born_birth).await;
    open(&p_builds, &id, 2).await;
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(p_admin.resolved.clone());
    let ep = rdm_transport::endpoint::bind(re_born_birth.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = NodeRpcClient::new(ep, resolver);
    let req = BuildClaimRequest::ClaimAttempt { build_id: id.to_string(), attempt: 2, executor_node_id: re_born_birth.node.node_id.clone(), executor_incarnation: IncarnationId::mint(), mesh: None };
    let (out, _) = client.call::<BuildClaim>(&rdm_transport::NodeTarget::ExactNode(p.node.node_id.clone()), &req, &rdm_transport::CallOptions::default()).await;
    match out {
        rdm_core::outcome::RpcOutcome::Reply(reply) => match reply.value() {
            BuildClaimReply::PeerUnresolved { reason } => assert!(reason.contains("names birth"), "{reason}"),
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
    assert_eq!(p_builds.read_build(&id).await.unwrap().attempt, 1, "nothing was claimed");
}

/// CONTRACT (node-rpc.md §14: "Nothing inside the mesh checks who is calling"): a claim put from an
/// endpoint the fabric-primary's view holds no birth for is decided for the executor the request
/// names, at its exact birth: `Won`, the attempt claimed for that executor's path.name. A request
/// naming a birth of the executor the view does not hold is refused by name about that subject
/// (`PeerUnresolved`), and nothing is claimed. What must NOT happen: `Unauthorized` naming the caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_from_an_endpoint_the_fabric_primary_never_heard_of_is_decided_for_the_executor_it_names() {
    use rdm_core::build_claim::{BuildClaim, BuildClaimRequest};
    let id = BuildId::mint();
    let executor = birth("mesh1.admin.2", false);
    let (p, p_admin, p_builds) = primary(&id, 1, Arc::new(AttemptContexts::in_memory()), &executor).await;
    open(&p_builds, &id, 2).await;
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(p_admin.resolved.clone());
    // A fresh endpoint key, in no birth of the fabric-primary's view.
    let stranger = SecretKey::generate();
    let ep = rdm_transport::endpoint::bind(stranger, "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = NodeRpcClient::new(ep, resolver);
    let call = |req: BuildClaimRequest| {
        let client = &client;
        let target = rdm_transport::NodeTarget::ExactNode(p.node.node_id.clone());
        async move {
            match client.call::<BuildClaim>(&target, &req, &rdm_transport::CallOptions::default()).await.0 {
                rdm_core::outcome::RpcOutcome::Reply(reply) => reply.value().clone(),
                other => panic!("{other:?}"),
            }
        }
    };
    // A birth of the executor the view does not hold: refused about the subject, nothing claimed.
    let other_birth = BuildClaimRequest::ClaimAttempt { build_id: id.to_string(), attempt: 2, executor_node_id: executor.node.node_id.clone(), executor_incarnation: IncarnationId::mint(), mesh: None };
    match call(other_birth).await {
        BuildClaimReply::PeerUnresolved { reason } => assert!(reason.contains("names birth") && reason.contains("mesh1.admin.2"), "{reason}"),
        other => panic!("a birth the view does not hold is refused about the subject: {other:?}"),
    }
    assert_eq!(p_builds.read_build(&id).await.unwrap().attempt, 1, "nothing was claimed");
    // The executor's exact birth: Won, the attempt claimed for that executor.
    let exact = BuildClaimRequest::ClaimAttempt { build_id: id.to_string(), attempt: 2, executor_node_id: executor.node.node_id.clone(), executor_incarnation: executor.node.incarnation_id.clone().unwrap(), mesh: None };
    assert!(matches!(call(exact).await, BuildClaimReply::Won { .. }), "the claim is decided for the executor the request names");
    let build = p_builds.read_build(&id).await.unwrap();
    assert_eq!((build.attempt, build.executor.as_deref()), (2, Some("mesh1.admin.2")), "attempt 2 is claimed for the executor the request names");
}

/// CONTRACT (node-investigation.md: "A node that comes back after it was re-created is a deleted birth
/// ... No admit reason exists for it, and none is added"; fabric-recovery.md step 3; Q181, Luke
/// 2026-10-26: "a node cut off should have been replaced, and so blocked from joining"): a claim whose
/// executor's exact birth the store's node rows hold deleted is refused by name (`deleted birth`)
/// before the mesh's in-flight row is taken: nothing is taken, nothing is recorded on the Build log.
/// The same claim of the live birth wins. Must NOT happen: a deleted birth holding a mesh's in-flight
/// row or an attempt of the Build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_by_a_deleted_birth_is_refused_by_name_and_takes_nothing() {
    use rdm_admin::storage::{MeshStorage as _, NodeRow, NodesStorage as _};
    let id = BuildId::mint();
    let builds = MemoryBuildStateAdapter::new();
    settled(&builds, &id, "mesh1.admin.1", 1).await;
    open(&builds, &id, 2).await;
    let claims = rdm_admin::storage::MemoryMeshStorage::default();
    let contexts = AttemptContexts::in_memory();
    let rows = rdm_admin::storage::MemoryNodesStorage::default();
    let me: rdm_core::PathName = "mesh1.admin.2".parse().unwrap();
    let me_id = NodeId::mint();
    rows.put_node(&NodeRow::new(me.clone(), me_id.clone(), NodeStatus::Deleted)).await.unwrap();
    let claim = BuildAttemptClaim { claimant: Some(me.to_string()), mesh: None, build_id: id.clone(), attempt: 2, executor: me.to_string() };
    let refused = build_claim::decide_claim(&me, &builds, &claims, &contexts, &claim, "mesh1", Some((&rows, &me_id))).await;
    assert!(matches!(&refused, BuildClaimReply::NotReady { reason } if reason.contains("deleted birth") && reason.contains("mesh1.admin.2")), "a deleted birth's claim is refused by name: {refused:?}");
    assert_eq!(claims.in_flight("mesh1").await.unwrap(), None, "a deleted birth takes no in-flight row");
    let p = builds.read_build(&id).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (1, None), "nothing of the refused claim is on the Build log: attempt 2 stays unclaimed: {p:?}");

    rows.put_node(&NodeRow::new(me.clone(), me_id.clone(), NodeStatus::Ready)).await.unwrap();
    let won = build_claim::decide_claim(&me, &builds, &claims, &contexts, &claim, "mesh1", Some((&rows, &me_id))).await;
    assert!(matches!(won, BuildClaimReply::Won { .. }), "the live birth's claim wins: {won:?}");
}
