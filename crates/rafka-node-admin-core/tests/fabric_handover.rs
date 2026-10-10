//! The fabric-primary handover (R-ST2, op `0x21`) between two in-process node-admins over real
//! Node RPC: the incumbent of mesh1 hands the fabric seat to the outside mesh-primary of mesh2.
//! Each cell reads the spans the handover emitted, in the order they happened.
// @feature: node-lifecycle

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{FabricId, IncarnationId, MeshId, NodeId, Seat, SeatHolder};
use rafka_mesh_transport::membership::{Backbone, Membership};
use rafka_node_admin_core::admin::Records;
use rafka_node_admin_core::fabric_handover::{HandoverDoor, HandoverError, HandoverSlot, OwnContacts, Stage};
use rafka_node_admin_core::fabric_storage::{FabricStorage, MemoryFabricStorage};
use rafka_node_admin_core::model::{EndpointId, Fabric, Node, NodeStatus, ProviderKind, ScopeStatus};
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::handover::{FabricPrimaryHandover, FabricPrimaryHandoverReply as Reply, FabricPrimaryHandoverRequest as Request, HandoverBirth};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use tracing_subscriber::layer::SubscriberExt;

/// Every span, in the order it was created: name and fields.
#[derive(Clone, Default)]
struct Spans(Arc<Mutex<Vec<(String, BTreeMap<String, String>)>>>);

struct Fields<'a>(&'a mut BTreeMap<String, String>);
impl tracing::field::Visit for Fields<'_> {
    fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
        self.0.insert(f.name().into(), v.into());
    }
    fn record_u64(&mut self, f: &tracing::field::Field, v: u64) {
        self.0.insert(f.name().into(), v.to_string());
    }
    fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
        self.0.insert(f.name().into(), format!("{v:?}"));
    }
}

impl<S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>> tracing_subscriber::Layer<S> for Spans {
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, _: &tracing::span::Id, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = BTreeMap::new();
        attrs.record(&mut Fields(&mut fields));
        self.0.lock().unwrap().push((attrs.metadata().name().into(), fields));
    }
}

impl Spans {
    fn names(&self) -> Vec<String> {
        self.0.lock().unwrap().iter().map(|(n, _)| n.clone()).collect()
    }
    fn named(&self, name: &str) -> Vec<BTreeMap<String, String>> {
        self.0.lock().unwrap().iter().filter(|(n, _)| n == name).map(|(_, f)| f.clone()).collect()
    }
    fn position(&self, name: &str) -> Option<usize> {
        self.names().iter().position(|n| n == name)
    }
}

const COMMITTED: &str = "rdm.node_admin.fabric.update.via-handover-committed";
const CONFIRMED: &str = "rdm.node_admin.fabric.update.via-handover-confirmed";
const REJECTED: &str = "rdm.node_admin.fabric.reject.via-handover";
const ANNOUNCED: &str = "rdm.mesh.seat.update.via-announce-new-fabric-primary";
const CALL: &str = "rdm.node_rpc.request.update.via-call";

fn view(nodes: Vec<Node>) -> Topology {
    Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process }, meshes: vec![], nodes }
}

struct Admin {
    door: Arc<HandoverDoor>,
    slot: HandoverSlot,
    client: Arc<NodeRpcClient>,
    resolver: Arc<StaticResolver>,
    resolved: ResolvedNode,
    storage: Arc<MemoryFabricStorage>,
    _router: Router,
}

struct Pair {
    incumbent: Admin,
    successor: Admin,
    mesh1_id: MeshId,
}

async fn admin(fabric: &FabricId, mesh: &str, path: &str, node_id: &str, fill_slot: bool) -> (Admin, iroh::Endpoint) {
    let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let time = rafka_mesh_transport::clock::RafkaTime::unadopted();
    time.adopt(5_000_000);
    let (node_id, incarnation) = (NodeId::parse(node_id).unwrap(), IncarnationId::mint());
    let membership = Membership::join(&gossip, &ep, fabric, mesh, &MeshId::mint(), path, Arc::new(time), vec![]).await.unwrap();
    let backbone = Backbone::join(&gossip, &ep, &membership, mesh, path, incarnation.clone(), vec![]).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    let client = Arc::new(NodeRpcClient::new(ep.clone(), resolver.clone()));
    let storage = Arc::new(MemoryFabricStorage::new());
    let slot: HandoverSlot = Arc::new(OnceLock::new());
    let server = rafka_node_admin_core::fabric_handover::serve(ServerBuilder::new(), slot.clone()).seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() }).expect("the catalog seals");
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip).accept(rafka_node_rpc::ALPN, server).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { node_id: node_id.clone(), name: path.parse().unwrap(), endpoint_id: ep.id(), transport_addr: addr, incarnation: incarnation.clone() };
    let door = Arc::new(HandoverDoor {
        fabric: "fabric1".into(),
        me: path.parse().unwrap(),
        node_id,
        incarnation,
        topology: Arc::new(tokio::sync::RwLock::new(view(vec![]))),
        book: membership.book.clone(),
        membership,
        backbone,
        storage: storage.clone(),
        client: client.clone(),
        records: Arc::new(Records::default()),
        contacts: OnceLock::new(),
        handovers: Default::default(),
    });
    let _ = door.contacts.set(OwnContacts { endpoint_id: ep.id().to_string(), transport_addr: addr, admin_api_base: Some(format!("http://127.0.0.1:{}", addr.port() + 1)) });
    if fill_slot {
        let _ = slot.set(door.clone());
    }
    (Admin { door, slot, client, resolver, resolved, storage, _router: router }, ep)
}

fn node_of(a: &Admin, mesh_status: NodeStatus, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(a.resolved.name.clone());
    n.node_id = a.resolved.node_id.clone();
    n.incarnation_id = Some(a.resolved.incarnation.clone());
    n.endpoint_id = Some(EndpointId(a.resolved.endpoint_id.to_string()));
    n.status = mesh_status;
    n.is_primary = true;
    n.is_fabric_primary = fabric_primary;
    n
}

fn birth(a: &Admin) -> HandoverBirth {
    HandoverBirth { mesh: a.resolved.name.mesh.clone(), node_id: a.resolved.node_id.clone(), incarnation: a.resolved.incarnation.clone() }
}

fn holder_of(a: &Admin, epoch: u64) -> SeatHolder {
    SeatHolder { mesh: a.resolved.name.mesh.clone(), node_id: a.resolved.node_id.clone(), incarnation: a.resolved.incarnation.clone(), epoch }
}

/// mesh1's admin holds the fabric seat at epoch 7; mesh2's admin (`successor_status`) is the only
/// other mesh-primary. `third` adds a mesh3 admin with a lower NodeId.
async fn fabric(successor_status: NodeStatus, third: bool, serve_incumbent: bool) -> (Pair, Option<Admin>) {
    let fabric_id = FabricId::mint();
    let (incumbent, _) = admin(&fabric_id, "mesh1", "mesh1.admin.1", "000000000009", serve_incumbent).await;
    let (successor, _) = admin(&fabric_id, "mesh2", "mesh2.admin.1", "000000000005", true).await;
    let third = if third { Some(admin(&fabric_id, "mesh3", "mesh3.admin.1", "000000000001", true).await.0) } else { None };
    let mut nodes = vec![node_of(&incumbent, NodeStatus::ReadyForTraffic, true), node_of(&successor, successor_status, false)];
    if let Some(t) = &third {
        nodes.push(node_of(t, NodeStatus::ReadyForTraffic, false));
    }
    let mut peers = vec![&incumbent, &successor];
    peers.extend(third.iter());
    for a in &peers {
        *a.door.topology.write().await = view(nodes.clone());
        for p in &peers {
            a.resolver.insert(p.resolved.clone());
        }
        a.door.membership.seats().take(Seat::FabricPrimary, &holder_of(&incumbent, 7));
        a.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&incumbent, 1));
        a.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&successor, 1));
        if let Some(t) = &third {
            a.door.membership.seats().take(Seat::MeshPrimary, &holder_of(t, 1));
        }
    }
    (Pair { incumbent, successor, mesh1_id: MeshId::mint() }, third)
}

async fn send(from: &Admin, to: &Admin, req: Request) -> Reply {
    let (out, _) = from.client.call::<FabricPrimaryHandover>(&NodeTarget::ExactNode(to.resolved.node_id.clone()), &req, &CallOptions::default()).await;
    out.reply().unwrap_or_else(|| panic!("no reply: {out:?}")).value().clone()
}

fn take(f: &Pair, epoch: u64, operation: &str) -> Request {
    Request::TakeFabricPrimary { fabric: "fabric1".into(), incumbent: birth(&f.incumbent), successor: birth(&f.successor), expected_epoch: epoch, operation: operation.into() }
}

fn taken(f: &Pair, epoch: u64, committed: u64, operation: &str) -> Request {
    Request::FabricPrimaryTaken { fabric: "fabric1".into(), incumbent: birth(&f.incumbent), successor: birth(&f.successor), expected_epoch: epoch, committed_epoch: committed, operation: operation.into() }
}

/// The spans of the handover are made on the server tasks of both admins, on any worker thread:
/// the cell owns its process and installs the global subscriber.
fn capture() -> Spans {
    let spans = Spans::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(spans.clone())).expect("this cell owns its process");
    spans
}

async fn until(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..400 {
        if f() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("never observed: {what}");
}

/// CONTRACT: the incumbent hands the fabric seat to the outside mesh-primary and returns only once
/// the transfer is confirmed. In order: the command, the successor's durable commit at the next
/// epoch, new-fabric-primary, the completion, the incumbent's confirmation. Both admins then hold
/// the successor as fabric holder at epoch 8, the successor's seat row is durable, and the
/// incumbent keeps its own mesh-primary record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incumbent_hands_the_fabric_seat_over_and_the_transfer_is_confirmed_in_order() {
    if crate::own_process::delegated(module_path!(), "incumbent_hands_the_fabric_seat_over_and_the_transfer_is_confirmed_in_order") {
        return;
    }
    let spans = capture();
    let (f, _) = fabric(NodeStatus::ReadyForTraffic, false, true).await;
    f.incumbent.door.hand_over("mesh1", &f.mesh1_id).await.expect("the handover is confirmed");
    let at = |n: &str| spans.position(n).unwrap_or_else(|| panic!("no {n} span among {:?}", spans.names()));
    let first_call = at(CALL);
    assert!(first_call < at(COMMITTED), "the command is sent before the successor commits");
    assert!(at(COMMITTED) < at(ANNOUNCED), "new-fabric-primary is published after the commit");
    assert!(at(ANNOUNCED) < at(CONFIRMED), "the incumbent confirms after new-fabric-primary was published");
    let calls = spans.names().iter().enumerate().filter(|(_, n)| *n == CALL).map(|(i, _)| i).collect::<Vec<_>>();
    assert!(calls.len() >= 2 && calls.iter().any(|c| *c > at(ANNOUNCED) && *c < at(CONFIRMED)), "FabricPrimaryTaken is called between the announcement and the confirmation: {calls:?}");
    let committed = &spans.named(COMMITTED)[0];
    assert_eq!((committed["expected_epoch"].as_str(), committed["committed_epoch"].as_str()), ("7", "8"));
    assert_eq!(committed["operation"], format!("fabric-primary-handover:{}", f.mesh1_id));
    let confirmed = &spans.named(CONFIRMED)[0];
    assert_eq!((confirmed["expected_epoch"].as_str(), confirmed["committed_epoch"].as_str()), ("7", "8"));
    assert!(spans.named(REJECTED).is_empty(), "{:?}", spans.named(REJECTED));
    for a in [&f.incumbent, &f.successor] {
        assert_eq!(a.door.membership.seats().fabric(), Some(holder_of(&f.successor, 8)), "{} holds the committed record", a.resolved.name);
    }
    assert_eq!(f.incumbent.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.incumbent, 1)), "the incumbent keeps its mesh-primary seat");
    let rows = f.successor.storage.seats().await.unwrap();
    assert!(rows.iter().any(|r| r.seat == Seat::FabricPrimary && r.holder == holder_of(&f.successor, 8)), "the committed seat is durable: {rows:?}");
    let op = format!("fabric-primary-handover:{}", f.mesh1_id);
    assert!(matches!(f.incumbent.door.handovers.get(&op), Some((_, Stage::Confirmed(8)))));
    until("the successor holds the confirmation", || matches!(f.successor.door.handovers.get(&op), Some((_, Stage::Confirmed(8))))).await;
    f.successor.door.confirmed(&f.mesh1_id).await.expect("the new fabric-primary's gate is open");
}

/// CONTRACT: a duplicate command and a duplicate completion commit one epoch and replay the same
/// answer; a repeated hand-over of a confirmed operation sends nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicate_command_or_completion_commits_one_epoch() {
    if crate::own_process::delegated(module_path!(), "a_duplicate_command_or_completion_commits_one_epoch") {
        return;
    }
    let spans = capture();
    let (f, _) = fabric(NodeStatus::ReadyForTraffic, false, true).await;
    f.incumbent.door.hand_over("mesh1", &f.mesh1_id).await.unwrap();
    let op = format!("fabric-primary-handover:{}", f.mesh1_id);
    let calls_before = spans.named(CALL).len();
    assert_eq!(send(&f.incumbent, &f.successor, take(&f, 7, &op)).await, Reply::AlreadyApplied, "the identical command replays");
    until("the replayed completion is sent again", || spans.named(CALL).len() > calls_before).await;
    assert_eq!(send(&f.successor, &f.incumbent, taken(&f, 7, 8, &op)).await, Reply::AlreadyApplied, "the identical completion replays");
    f.incumbent.door.hand_over("mesh1", &f.mesh1_id).await.expect("a confirmed operation is confirmed");
    assert_eq!(spans.named(COMMITTED).len(), 1, "one commit");
    assert_eq!(spans.named(CONFIRMED).len(), 1, "one confirmation");
    for a in [&f.incumbent, &f.successor] {
        assert_eq!(a.door.membership.seats().fabric().map(|h| h.epoch), Some(8), "{}: one committed epoch", a.resolved.name);
    }
    assert_eq!(f.successor.storage.seats().await.unwrap().iter().filter(|r| r.seat == Seat::FabricPrimary && r.holder.epoch == 8).count(), 1);
}

/// CONTRACT: a wrong birth, a stale epoch, a conflicting payload and an authority that is not the
/// seat holder are each refused by their own typed reply and each emit the reject span naming
/// the reason; nothing commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_birth_a_stale_epoch_and_a_conflict_are_each_refused_by_name() {
    if crate::own_process::delegated(module_path!(), "a_wrong_birth_a_stale_epoch_and_a_conflict_are_each_refused_by_name") {
        return;
    }
    let spans = capture();
    let (f, _) = fabric(NodeStatus::ReadyForTraffic, false, true).await;
    let op = "fabric-primary-handover:refusals";
    let other_birth = |b: &HandoverBirth| HandoverBirth { incarnation: IncarnationId("another-birth".into()), ..b.clone() };
    // The command names a birth that is not the receiver's, or not the caller's.
    let wrong_successor = Request::TakeFabricPrimary { fabric: "fabric1".into(), incumbent: birth(&f.incumbent), successor: other_birth(&birth(&f.successor)), expected_epoch: 7, operation: op.into() };
    assert_eq!(send(&f.incumbent, &f.successor, wrong_successor).await, Reply::RejectedWrongBirth { role: "successor".into() });
    let wrong_incumbent = Request::TakeFabricPrimary { fabric: "fabric1".into(), incumbent: other_birth(&birth(&f.incumbent)), successor: birth(&f.successor), expected_epoch: 7, operation: op.into() };
    assert_eq!(send(&f.incumbent, &f.successor, wrong_incumbent).await, Reply::RejectedWrongBirth { role: "incumbent".into() });
    // A completion naming a successor that is not the caller, or an incumbent that is not the receiver.
    let taken_wrong_successor = Request::FabricPrimaryTaken { fabric: "fabric1".into(), incumbent: birth(&f.incumbent), successor: other_birth(&birth(&f.successor)), expected_epoch: 7, committed_epoch: 8, operation: op.into() };
    assert_eq!(send(&f.successor, &f.incumbent, taken_wrong_successor).await, Reply::RejectedWrongBirth { role: "successor".into() });
    let taken_wrong_incumbent = Request::FabricPrimaryTaken { fabric: "fabric1".into(), incumbent: other_birth(&birth(&f.incumbent)), successor: birth(&f.successor), expected_epoch: 7, committed_epoch: 8, operation: op.into() };
    assert_eq!(send(&f.successor, &f.incumbent, taken_wrong_incumbent).await, Reply::RejectedWrongBirth { role: "incumbent".into() });
    // A stale epoch: the seat is at 7.
    assert_eq!(send(&f.incumbent, &f.successor, take(&f, 5, op)).await, Reply::RejectedStaleEpoch { expected: 5, current: 7 });
    // A completion whose committed epoch is not the next one.
    assert_eq!(send(&f.successor, &f.incumbent, taken(&f, 7, 9, op)).await, Reply::RejectedStaleEpoch { expected: 8, current: 9 });
    // A command from a caller that is not the seat holder: the successor's record says someone else.
    let impostor = Request::TakeFabricPrimary { fabric: "fabric1".into(), incumbent: birth(&f.incumbent), successor: birth(&f.successor), expected_epoch: 7, operation: "fabric-primary-handover:impostor".into() };
    let seat_elsewhere = SeatHolder { mesh: "mesh9".into(), node_id: NodeId::parse("000000000003").unwrap(), incarnation: IncarnationId("x".into()), epoch: 8 };
    f.successor.door.membership.seats().take(Seat::FabricPrimary, &seat_elsewhere);
    assert_eq!(send(&f.incumbent, &f.successor, impostor).await, Reply::RejectedNotAuthority);
    // A wrong fabric is a conflict.
    let wrong_fabric = Request::TakeFabricPrimary { fabric: "fabric9".into(), incumbent: birth(&f.incumbent), successor: birth(&f.successor), expected_epoch: 7, operation: op.into() };
    assert_eq!(send(&f.incumbent, &f.successor, wrong_fabric).await, Reply::RejectedConflict { operation: op.into() });
    let reasons: Vec<String> = spans.named(REJECTED).iter().map(|f| f["reason"].clone()).collect();
    for reason in ["wrong-birth", "stale-epoch", "not-authority", "conflict"] {
        assert!(reasons.iter().any(|r| r == reason), "no reject span for {reason}: {reasons:?}");
    }
    assert!(spans.named(COMMITTED).is_empty() && spans.named(CONFIRMED).is_empty(), "nothing committed");
    assert_eq!(f.incumbent.door.membership.seats().fabric(), Some(holder_of(&f.incumbent, 7)), "the incumbent still holds the seat");
}

/// CONTRACT: the same operation with another payload is a conflict, and the replay lookup comes
/// before the stale-epoch check: the identical command after the commit replays, the same
/// operation naming another epoch conflicts, and a completion naming another committed epoch is
/// refused against the confirmed one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflicting_payload_for_a_committed_operation_is_refused() {
    if crate::own_process::delegated(module_path!(), "a_conflicting_payload_for_a_committed_operation_is_refused") {
        return;
    }
    let _spans = capture();
    let (f, _) = fabric(NodeStatus::ReadyForTraffic, false, true).await;
    let op = format!("fabric-primary-handover:{}", f.mesh1_id);
    f.incumbent.door.hand_over("mesh1", &f.mesh1_id).await.unwrap();
    assert_eq!(send(&f.incumbent, &f.successor, take(&f, 7, &op)).await, Reply::AlreadyApplied);
    assert_eq!(send(&f.incumbent, &f.successor, take(&f, 8, &op)).await, Reply::RejectedConflict { operation: op.clone() });
    assert_eq!(send(&f.successor, &f.incumbent, taken(&f, 7, 9, &op)).await, Reply::RejectedStaleEpoch { expected: 8, current: 9 }, "a completion naming another committed epoch is refused against the confirmed one");
    assert_eq!(f.successor.door.membership.seats().fabric().map(|h| h.epoch), Some(8));
}

/// CONTRACT: with no eligible Ready mesh-primary outside the incumbent's mesh the handover is
/// blocked before any command: the mesh keeps running, no node RPC call is made, and the reject
/// span names the reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incumbent_with_no_eligible_successor_blocks_the_handover_and_sends_nothing() {
    if crate::own_process::delegated(module_path!(), "incumbent_with_no_eligible_successor_blocks_the_handover_and_sends_nothing") {
        return;
    }
    let spans = capture();
    let (f, _) = fabric(NodeStatus::Pending, false, true).await;
    let err = f.incumbent.door.hand_over("mesh1", &f.mesh1_id).await.expect_err("blocked");
    assert!(matches!(err, HandoverError::NoEligibleSuccessor(_)), "{err:?}");
    assert!(spans.named(CALL).is_empty(), "no command was sent: {:?}", spans.names());
    let rejects = spans.named(REJECTED);
    assert_eq!(rejects.len(), 1);
    assert_eq!(rejects[0]["reason"], "no-eligible-successor");
    assert_eq!(f.incumbent.door.membership.seats().fabric(), Some(holder_of(&f.incumbent, 7)));
    assert!(f.incumbent.door.handovers.get(&format!("fabric-primary-handover:{}", f.mesh1_id)).is_none(), "nothing was recorded");
}

/// CONTRACT: the successor is the one the leadership calculation selects with the incumbent's mesh
/// excluded (the lowest Ready NodeId among the outside mesh-primaries): a command to another
/// mesh-primary is refused as not eligible and commits nothing; the incumbent's own selection
/// names the lowest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_command_to_a_mesh_primary_the_calculation_does_not_select_is_refused() {
    if crate::own_process::delegated(module_path!(), "a_command_to_a_mesh_primary_the_calculation_does_not_select_is_refused") {
        return;
    }
    let spans = capture();
    let (f, third) = fabric(NodeStatus::ReadyForTraffic, true, true).await;
    let third = third.unwrap();
    let op = "fabric-primary-handover:not-selected";
    assert!(matches!(send(&f.incumbent, &f.successor, take(&f, 7, op)).await, Reply::RejectedNotEligible { .. }));
    assert!(spans.named(REJECTED).iter().any(|f| f["reason"] == "not-eligible"));
    assert_eq!(f.successor.door.membership.seats().fabric(), Some(holder_of(&f.incumbent, 7)));
    // The incumbent's own handover goes to mesh3's admin, the lowest NodeId.
    f.incumbent.door.hand_over("mesh1", &f.mesh1_id).await.expect("the handover is confirmed");
    for a in [&f.incumbent, &f.successor, &third] {
        let _ = a;
    }
    assert_eq!(f.incumbent.door.membership.seats().fabric(), Some(holder_of(&third, 8)));
    assert_eq!(spans.named(COMMITTED)[0]["successor"], format!("mesh3:{}:{}", third.resolved.node_id, third.resolved.incarnation.0));
}

/// CONTRACT: a handover committed at the successor whose completion the incumbent did not
/// acknowledge does not open the gate: `confirmed` sends the identical completion again, and
/// opens only on the incumbent's matched answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_fabric_primary_reconciles_an_unacknowledged_handover_before_the_leave_opens() {
    if crate::own_process::delegated(module_path!(), "new_fabric_primary_reconciles_an_unacknowledged_handover_before_the_leave_opens") {
        return;
    }
    let spans = capture();
    let (f, _) = fabric(NodeStatus::ReadyForTraffic, false, false).await;
    let op = format!("fabric-primary-handover:{}", f.mesh1_id);
    // The incumbent's door is not serving yet: the command goes straight from its client, and the
    // successor's completion finds the door not ready.
    assert_eq!(send(&f.incumbent, &f.successor, take(&f, 7, &op)).await, Reply::Applied);
    until("the successor committed", || matches!(f.successor.door.handovers.get(&op), Some((_, Stage::Committed(8))))).await;
    until("the completion was refused", || spans.named(REJECTED).iter().any(|f| f["call"] == "taken")).await;
    let err = f.successor.door.confirmed(&f.mesh1_id).await.expect_err("the gate is closed while the completion is unacknowledged");
    assert!(matches!(err, HandoverError::Refused(_) | HandoverError::Unconfirmed(_)), "{err:?}");
    assert!(matches!(f.successor.door.handovers.get(&op), Some((_, Stage::Committed(8)))));
    assert!(spans.named(CONFIRMED).is_empty());
    // The incumbent's door serves: the reconciliation sends the identical completion and it matches.
    let _ = f.incumbent.slot.set(f.incumbent.door.clone());
    f.successor.door.confirmed(&f.mesh1_id).await.expect("reconciled");
    assert!(matches!(f.successor.door.handovers.get(&op), Some((_, Stage::Confirmed(8)))));
    assert_eq!(spans.named(CONFIRMED).len(), 1);
    assert_eq!(f.incumbent.door.membership.seats().fabric(), Some(holder_of(&f.successor, 8)), "the incumbent yielded when it matched the completion");
}
