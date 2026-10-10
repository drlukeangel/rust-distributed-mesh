//! R-A1 / R-T4 / R-X1 functional: a Build attempt's claim is decided on the fabric-primary's
//! Build log by the claim door, and `BuildClaim` (op `0x1C`) is that door on the wire.
//!
//! Each admin is a real Node RPC server on loopback with its own Build log and its own view. An
//! executor never claims (the fabric-primary claims for it and calls it with `build.attempt.run`),
//! so these cells put claims to the door the way the fabric-primary does: a claim past the log the
//! door holds is refused by name (`Lost`, `NotOpen`), a non-primary decides nothing and names the
//! one it sees, and the context of an attempt rides `Won`.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{EndpointId, IncarnationId, NodeId, NodeKind};
use rafka_node_admin_core::accepted::FabricTopology;
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_claim::{self, AttemptContexts, ClaimDoor};
use rafka_node_admin_core::build_state::{
    AttemptOpened, AttemptOutcome, AttemptReason, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter, MemoryBuildStateAdapter,
};
use rafka_node_admin_core::model::{Fabric, Node, NodeStatus, ProviderKind, ScopeStatus};
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc::{NodeRpcClient, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::build_claim::BuildClaimReply;
use rafka_node_rpc_contract::context::CallContext;
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
    node.status = NodeStatus::ReadyForTraffic;
    node.is_primary = fabric_primary;
    node.is_fabric_primary = fabric_primary;
    node.transport_addr = Some("127.0.0.1:1".parse().unwrap());
    Birth { node, key }
}

fn view(nodes: Vec<Node>) -> Arc<RwLock<Topology>> {
    Arc::new(RwLock::new(Topology {
        fabric: Fabric { id: rafka_mesh_entity::FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: Vec::new(),
        nodes,
    }))
}

/// An admin serving `BuildClaim` over its own Build log and its own view.
struct Seat {
    resolved: ResolvedNode,
    door: Arc<ClaimDoor>,
    _router: Router,
}

async fn seat(b: &Birth, topology: Arc<RwLock<Topology>>, builds: Arc<dyn BuildStateAdapter>, contexts: Arc<AttemptContexts>) -> Seat {
    let door = Arc::new(ClaimDoor { me: b.node.name.clone(), topology, builds, contexts });
    let slot: build_claim::ClaimSlot = Arc::new(OnceLock::new());
    let _ = slot.set(door.clone());
    let server = build_claim::serve(ServerBuilder::new(), slot)
        .seal(ServedBirth { node_id: b.node.node_id.to_string(), incarnation: b.node.incarnation_id.clone().unwrap().0 })
        .unwrap();
    let ep = rafka_node_rpc::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolved = ResolvedNode {
        node_id: b.node.node_id.clone(),
        name: b.node.name.clone(),
        endpoint_id: b.key.public(),
        transport_addr: addr,
        incarnation: b.node.incarnation_id.clone().unwrap(),
    };
    Seat { resolved, door, _router: router }
}

/// A Build whose log holds `settled` converged attempts: accepted, then claim + converged each.
async fn settled(builds: &MemoryBuildStateAdapter, id: &BuildId, holder: &str, settled: u32) {
    builds
        .publish_accepted(&BuildAccepted { build_id: id.clone(), topology: FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    for attempt in 1..=settled {
        if attempt > 1 {
            open(builds, id, attempt).await;
        }
        builds.claim_attempt(&BuildAttemptClaim { build_id: id.clone(), attempt, executor: holder.into() }).await.unwrap();
        builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
    }
}

async fn open(builds: &MemoryBuildStateAdapter, id: &BuildId, attempt: u32) {
    builds.open_attempt(&AttemptOpened { build_id: id.clone(), attempt, reason: AttemptReason::Restart, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 0 }).await.unwrap();
}

/// A fabric-primary P whose log holds the Build at `settled` converged attempts, listing the
/// re-born executor's birth in its view the way the product's digest book does.
async fn primary(id: &BuildId, settled_attempts: u32, contexts: Arc<AttemptContexts>, executor: &Birth) -> (Birth, Seat, Arc<MemoryBuildStateAdapter>) {
    let p = birth("mesh1.admin.1", true);
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    settled(&builds, id, "mesh1.admin.1", settled_attempts).await;
    let topology = view(vec![p.node.clone(), executor.node.clone()]);
    let s = seat(&p, topology, builds.clone(), contexts).await;
    (p, s, builds)
}

/// CONTRACT: a claim of an attempt the fabric-primary's log holds for another executor is `Lost`,
/// naming the holder, and the log is unchanged by it. Whatever log the claimant holds is never
/// consulted: only the fabric-primary's decides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_of_a_held_attempt_is_lost_to_its_holder_and_changes_nothing() {
    let id = BuildId::mint();
    let executor = birth("mesh1.admin.2", false);
    let (_p, p_seat, p_builds) = primary(&id, 6, Arc::new(AttemptContexts::in_memory()), &executor).await;
    assert_eq!(p_seat.door.claim("mesh1.admin.2", &id, 1).await, BuildClaimReply::Lost { holder: "mesh1.admin.1".into() });
    assert_eq!(p_builds.read_build(&id).await.unwrap().attempt, 6, "the fabric-primary's log is unchanged by the refused claim");
}

/// CONTRACT: the fabric-primary answers `NotOpen` for an attempt that is not the Build's next,
/// naming the one that is open; the open attempt is won once, and the loser of it is named.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_past_the_open_attempt_is_not_open_and_names_the_next() {
    let id = BuildId::mint();
    let executor = birth("mesh1.admin.2", false);
    let (_p, p_seat, p_builds) = primary(&id, 3, Arc::new(AttemptContexts::in_memory()), &executor).await;
    open(&p_builds, &id, 4).await;
    assert_eq!(p_seat.door.claim("mesh1.admin.2", &id, 9).await, BuildClaimReply::NotOpen { next: Some(4) });
    let first = p_seat.door.claim("mesh1.admin.2", &id, 4).await;
    assert!(matches!(first, BuildClaimReply::Won { .. }), "{first:?}");
    assert_eq!(p_seat.door.claim("mesh1.admin.2", &id, 4).await, first, "a repeat claim by the holder answers the same");
    assert_eq!(p_seat.door.claim("mesh1.admin.3", &id, 4).await, BuildClaimReply::Lost { holder: "mesh1.admin.2".into() });
}

/// CONTRACT: `Won` carries the attempt's context from the fabric-primary's local record; an
/// attempt with no record of its own inherits the previous attempt's; an attempt nothing knows
/// starts a new trace (an empty context).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_won_claim_returns_the_attempts_context_and_a_hand_off_inherits_it() {
    let id = BuildId::mint();
    let contexts = Arc::new(AttemptContexts::in_memory());
    let executor = birth("mesh1.admin.2", false);
    let (_p, p_seat, p_builds) = primary(&id, 3, contexts.clone(), &executor).await;
    let rest = CallContext { caller_system: Some("rdm".into()), traceparent: Some(TRACEPARENT.into()), tracestate: None, baggage: None };
    contexts.put(&id, 4, &rest).await.unwrap();
    open(&p_builds, &id, 4).await;
    assert_eq!(p_seat.door.claim("mesh1.admin.2", &id, 4).await, BuildClaimReply::Won { context: rest.clone() });
    // The next attempt (a hand-off) has no record of its own.
    p_builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: id.clone(), attempt: 4, outcome: AttemptOutcome::HandedOff { to: "mesh2.admin.1".into() } }).await.unwrap();
    assert_eq!(contexts.get(&id, 5).unwrap(), None);
    assert_eq!(p_seat.door.claim("mesh2.admin.1", &id, 5).await, BuildClaimReply::Won { context: rest.clone() }, "the hand-off continues the trace");
    assert_eq!(contexts.get(&id, 5).unwrap(), Some(rest), "and keeps it under its own attempt");
    // An unrelated Build the primary has no record for starts a new trace.
    let other = BuildId::mint();
    settled(&p_builds, &other, "mesh1.admin.1", 1).await;
    open(&p_builds, &other, 2).await;
    match p_seat.door.claim("mesh1.admin.2", &other, 2).await {
        BuildClaimReply::Won { context } => assert_eq!(context.traceparent, None),
        other => panic!("{other:?}"),
    }
}

/// CONTRACT: an admin that is not the fabric-primary decides nothing and names the one its view
/// shows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_put_to_a_non_primary_is_redirected_to_the_fabric_primary_it_sees() {
    let id = BuildId::mint();
    let executor = birth("mesh1.admin.2", false);
    let (p, _p_seat, _p_builds) = primary(&id, 2, Arc::new(AttemptContexts::in_memory()), &executor).await;
    let q = birth("mesh1.admin.3", false);
    let q_view = view(vec![p.node.clone(), q.node.clone(), executor.node.clone()]);
    let q_seat = seat(&q, q_view, Arc::new(MemoryBuildStateAdapter::new()), Arc::new(AttemptContexts::in_memory())).await;
    let reply = BuildClaimReply::NotFabricPrimary { fabric_primary: Some("mesh1.admin.1".into()) };
    assert_eq!(q_seat.door.claim("mesh1.admin.2", &id, 1).await, reply, "a non-primary decides nothing and names the one it sees");
}

/// CONTRACT: the sender is the authenticated peer, held to the request: a request naming another
/// birth of the executor is refused by name, and nothing is claimed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_naming_another_birth_than_the_caller_is_refused_by_name() {
    use rafka_node_rpc_contract::build_claim::{BuildClaim, BuildClaimRequest};
    let id = BuildId::mint();
    let reborn_birth = birth("mesh1.admin.2", false);
    let (p, p_seat, p_builds) = primary(&id, 1, Arc::new(AttemptContexts::in_memory()), &reborn_birth).await;
    open(&p_builds, &id, 2).await;
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(p_seat.resolved.clone());
    let ep = rafka_node_rpc::endpoint::bind(reborn_birth.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = NodeRpcClient::new(ep, resolver);
    let req = BuildClaimRequest::ClaimAttempt { build_id: id.0.clone(), attempt: 2, executor_node_id: reborn_birth.node.node_id.clone(), executor_incarnation: IncarnationId::mint() };
    let (out, _) = client.call::<BuildClaim>(&rafka_node_rpc::NodeTarget::ExactNode(p.node.node_id.clone()), &req, &rafka_node_rpc::CallOptions::default()).await;
    match out {
        rafka_node_rpc_contract::outcome::RpcOutcome::Reply(reply) => match reply.value() {
            BuildClaimReply::Unauthorized { reason } => assert!(reason.contains("names birth"), "{reason}"),
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
    assert_eq!(p_builds.read_build(&id).await.unwrap().attempt, 1, "nothing was claimed");
}
