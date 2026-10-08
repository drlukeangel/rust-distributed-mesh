//! R-A1 / R-T4 / R-X1 functional: a Build attempt's claim is decided on the fabric-primary's
//! Build log over Node RPC (`BuildClaim`, op `0x1C`), and the executor runs only on `Won`.
//!
//! Each admin is a real Node RPC server on loopback with its own Build log and its own view; the
//! executor under test claims through `FabricPrimaryClaimer` over a real client. The defect these
//! cells pin: a re-born admin whose log is behind the Build claims the attempt its stale log
//! names, and two executors ran one attempt. Now its claim is put to the fabric-primary, whose
//! log refuses it (`Lost`, `NotOpen`) and the executor's runner is never called.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{EndpointId, IncarnationId, NodeId, NodeKind};
use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology};
use rafka_node_admin_core::build::{BuildId, BuildOperation};
use rafka_node_admin_core::build_claim::{self, AttemptContexts, ClaimDoor, Claimed, FabricPrimaryClaimer};
use rafka_node_admin_core::build_state::{
    AttemptOpened, AttemptOutcome, AttemptReason, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter, MemoryBuildStateAdapter,
};
use rafka_node_admin_core::executor::{BuildExecutor, OperationRunner, Reconciled};
use rafka_node_admin_core::model::{Fabric, Node, NodeStatus, ProviderKind, ScopeStatus};
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc::{NodeRpcClient, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::context::CallContext;
use std::sync::atomic::{AtomicUsize, Ordering};
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

#[derive(Default)]
struct CountingRunner {
    runs: AtomicUsize,
}

#[async_trait::async_trait]
impl OperationRunner for CountingRunner {
    async fn run(&self, _: &BuildId, _: u32, _: &BuildOperation) -> Result<(), String> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// The re-born executor: its own empty-ish Build log, its own view naming `primary_in_view`, a
/// real client to every seat in `reachable`.
struct Reborn {
    builds: Arc<MemoryBuildStateAdapter>,
    exec: BuildExecutor,
    runner: Arc<CountingRunner>,
}

async fn reborn(id: &BuildId, b: &Birth, mut view_nodes: Vec<Node>, reachable: &[&ResolvedNode], name_in_view: &str) -> Reborn {
    view_nodes.push(b.node.clone());
    let topology = view(view_nodes);
    // The executor's view names `name_in_view` as the fabric-primary.
    for n in topology.write().await.nodes.iter_mut() {
        n.is_fabric_primary = n.name.to_string() == name_in_view;
        n.is_primary = n.is_fabric_primary;
    }
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    // A stale log: the acceptance and nothing after it.
    builds
        .publish_accepted(&BuildAccepted { build_id: id.clone(), topology: FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    let resolver = Arc::new(StaticResolver::new());
    for r in reachable {
        resolver.insert((*r).clone());
    }
    let ep = rafka_node_rpc::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm"));
    let own_door = Arc::new(ClaimDoor { me: b.node.name.clone(), topology: topology.clone(), builds: builds.clone(), contexts: Arc::new(AttemptContexts::in_memory()) });
    let claimer = Arc::new(FabricPrimaryClaimer {
        me: b.node.name.clone(),
        node_id: b.node.node_id.clone(),
        incarnation: b.node.incarnation_id.clone().unwrap(),
        topology: topology.clone(),
        door: own_door,
        client,
    });
    let runner = Arc::new(CountingRunner::default());
    let storage = Arc::new(rafka_node_admin_core::fabric_storage::MemoryFabricStorage::new());
    let exec = BuildExecutor { executor: b.node.name.to_string(), accepted: Arc::new(AcceptedStore::new(storage, "mesh1.admin.2")), builds: builds.clone(), topology, runner: runner.clone(), claimer };
    Reborn { builds, exec, runner }
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

/// CONTRACT: a re-born admin whose log is behind the Build claims the attempt that log names.
/// The fabric-primary's log holds that attempt for another executor, so the claim is `Lost`,
/// naming the holder, and the re-born executor's runner is never called. The stale local log is
/// not consulted for the decision.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reborn_executor_with_a_stale_log_is_refused_by_the_fabric_primary_and_runs_nothing() {
    let id = BuildId::mint();
    let reborn_birth = birth("mesh1.admin.2", false);
    let (p, p_seat, p_builds) = primary(&id, 6, Arc::new(AttemptContexts::in_memory()), &reborn_birth).await;
    let r = reborn(&id, &reborn_birth, vec![p.node.clone()], &[&p_seat.resolved], "mesh1.admin.1").await;

    let stale = r.builds.read_build(&id).await.unwrap();
    assert_eq!(stale.attempt, 0, "the re-born log folds to attempt 0 of a Build the primary holds at 6");
    // What the executor did before the claim moved to the fabric-primary: its own stale log says Won.
    let own = Arc::new(MemoryBuildStateAdapter::new());
    own.absorb(&r.builds.facts().await.unwrap());
    assert_eq!(
        own.claim_attempt(&BuildAttemptClaim { build_id: id.clone(), attempt: 1, executor: "mesh1.admin.2".into() }).await.unwrap(),
        rafka_node_admin_core::build_state::ClaimOutcome::Won,
        "the stale log alone would have let the re-born executor take attempt 1 of a Build at attempt 6"
    );
    let out = r.exec.reconcile(&stale).await;
    assert_eq!(out, Reconciled::Lost { attempt: 1, holder: "mesh1.admin.1".into() });
    assert_eq!(r.runner.runs.load(Ordering::SeqCst), 0, "a refused claim runs nothing");
    assert_eq!(p_builds.read_build(&id).await.unwrap().attempt, 6, "the fabric-primary's log is unchanged by the refused claim");
    assert_eq!(r.builds.read_build(&id).await.unwrap().attempt, 0, "nothing was recorded on the re-born log either");
}

/// CONTRACT: the fabric-primary answers `NotOpen` for an attempt that is not the Build's next,
/// naming the one that is open; the executor runs nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_past_the_open_attempt_is_not_open_and_names_the_next() {
    let id = BuildId::mint();
    let reborn_birth = birth("mesh1.admin.2", false);
    let (p, p_seat, p_builds) = primary(&id, 3, Arc::new(AttemptContexts::in_memory()), &reborn_birth).await;
    open(&p_builds, &id, 4).await;
    let r = reborn(&id, &reborn_birth, vec![p.node.clone()], &[&p_seat.resolved], "mesh1.admin.1").await;
    let claimer = &r.exec.claimer;
    assert_eq!(claimer.claim("mesh1.admin.2", &id, 9).await, Claimed::NotOpen { next: Some(4) });
    // Claim of the open attempt wins, and re-claiming it is the same Won.
    let first = claimer.claim("mesh1.admin.2", &id, 4).await;
    assert!(matches!(first, Claimed::Won { .. }), "{first:?}");
    assert_eq!(claimer.claim("mesh1.admin.2", &id, 4).await, first, "a repeat claim by the holder answers the same");
    // The loser of the same attempt is named.
    assert_eq!(p_seat.door.claim("mesh1.admin.3", &id, 4).await, rafka_node_rpc_contract::build_claim::BuildClaimReply::Lost { holder: "mesh1.admin.2".into() });
}

/// CONTRACT: `Won` carries the attempt's context from the fabric-primary's local record; an
/// attempt with no record of its own inherits the previous attempt's; an attempt nothing knows
/// starts a new trace (an empty context).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_won_claim_returns_the_attempts_context_and_a_hand_off_inherits_it() {
    let id = BuildId::mint();
    let contexts = Arc::new(AttemptContexts::in_memory());
    let reborn_birth = birth("mesh1.admin.2", false);
    let (p, p_seat, p_builds) = primary(&id, 3, contexts.clone(), &reborn_birth).await;
    let rest = CallContext { caller_system: Some("rdm".into()), traceparent: Some(TRACEPARENT.into()), tracestate: None, baggage: None };
    contexts.put(&id, 4, &rest).await.unwrap();
    open(&p_builds, &id, 4).await;
    let r = reborn(&id, &reborn_birth, vec![p.node.clone()], &[&p_seat.resolved], "mesh1.admin.1").await;
    assert_eq!(r.exec.claimer.claim("mesh1.admin.2", &id, 4).await, Claimed::Won { context: rest.clone() });
    // The next attempt (a hand-off) has no record of its own.
    p_builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: id.clone(), attempt: 4, outcome: AttemptOutcome::HandedOff { to: "mesh2.admin.1".into() } }).await.unwrap();
    assert_eq!(contexts.get(&id, 5).unwrap(), None);
    assert_eq!(r.exec.claimer.claim("mesh1.admin.2", &id, 5).await, Claimed::Won { context: rest.clone() }, "the hand-off continues the trace");
    assert_eq!(contexts.get(&id, 5).unwrap(), Some(rest), "and keeps it under its own attempt");
    // An unrelated Build the primary has no record for starts a new trace.
    let other = BuildId::mint();
    settled(&p_builds, &other, "mesh1.admin.1", 1).await;
    open(&p_builds, &other, 2).await;
    match r.exec.claimer.claim("mesh1.admin.2", &other, 2).await {
        Claimed::Won { context } => assert_eq!(context.traceparent, None),
        other => panic!("{other:?}"),
    }
}

/// CONTRACT: an executor whose view names an admin that is not the fabric-primary is redirected
/// by that admin's `NotFabricPrimary` to the one it sees, asks that one, and takes its decision.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_put_to_a_non_primary_follows_its_redirect_to_the_fabric_primary() {
    let id = BuildId::mint();
    let reborn_birth = birth("mesh1.admin.2", false);
    let (p, p_seat, _p_builds) = primary(&id, 2, Arc::new(AttemptContexts::in_memory()), &reborn_birth).await;
    // Q is an admin whose view shows P as the fabric-primary.
    let q = birth("mesh1.admin.3", false);
    let q_view = view(vec![p.node.clone(), q.node.clone(), reborn_birth.node.clone()]);
    let q_seat = seat(&q, q_view, Arc::new(MemoryBuildStateAdapter::new()), Arc::new(AttemptContexts::in_memory())).await;
    // The executor's view wrongly names Q.
    let r = reborn(&id, &reborn_birth, vec![p.node.clone(), q.node.clone()], &[&p_seat.resolved, &q_seat.resolved], "mesh1.admin.3").await;
    let out = r.exec.claimer.claim("mesh1.admin.2", &id, 1).await;
    assert_eq!(out, Claimed::Lost { holder: "mesh1.admin.1".into() }, "P's log decided it, reached through Q's redirect");
    let reply = rafka_node_rpc_contract::build_claim::BuildClaimReply::NotFabricPrimary { fabric_primary: Some("mesh1.admin.1".into()) };
    assert_eq!(q_seat.door.claim("mesh1.admin.2", &id, 1).await, reply, "a non-primary decides nothing and names the one it sees");
}

/// CONTRACT: a claim that cannot be put to the fabric-primary (its address is unreachable, or no
/// admin holds the seat in the executor's view) is a claim not made: the attempt does not run and
/// no local log decides in its place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_that_cannot_reach_the_fabric_primary_runs_nothing() {
    let id = BuildId::mint();
    let reborn_birth = birth("mesh1.admin.2", false);
    let (p, p_seat, p_builds) = primary(&id, 1, Arc::new(AttemptContexts::in_memory()), &reborn_birth).await;
    open(&p_builds, &id, 2).await;
    // The primary exists in the view, but the executor's client does not resolve it.
    let r = reborn(&id, &reborn_birth, vec![p.node.clone()], &[], "mesh1.admin.1").await;
    let _keep = &p_seat;
    let stale = r.builds.read_build(&id).await.unwrap();
    match r.exec.reconcile(&stale).await {
        Reconciled::Failed { attempt: 1, reason } => assert!(reason.contains("claiming attempt 1"), "{reason}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(r.runner.runs.load(Ordering::SeqCst), 0);
    // No fabric-primary in the view at all.
    let none = reborn(&id, &reborn_birth, vec![p.node.clone()], &[&p_seat.resolved], "nobody").await;
    match none.exec.claimer.claim("mesh1.admin.2", &id, 1).await {
        Claimed::Undecided { reason } => assert!(reason.contains("no fabric-primary"), "{reason}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(none.builds.read_build(&id).await.unwrap().attempt, 0, "the local log claimed nothing");
}

/// CONTRACT: the sender is the authenticated peer, held to the request: a request naming another
/// birth of the executor is refused by name, and nothing is claimed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_naming_another_birth_than_the_caller_is_refused_by_name() {
    use rafka_node_rpc_contract::build_claim::{BuildClaim, BuildClaimReply, BuildClaimRequest};
    let id = BuildId::mint();
    let reborn_birth = birth("mesh1.admin.2", false);
    let (p, p_seat, p_builds) = primary(&id, 1, Arc::new(AttemptContexts::in_memory()), &reborn_birth).await;
    open(&p_builds, &id, 2).await;
    let _r = reborn(&id, &reborn_birth, vec![p.node.clone()], &[&p_seat.resolved], "mesh1.admin.1").await;
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
