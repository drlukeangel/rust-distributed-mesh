//! The fabric-primary handover (R-ST2, op `0x21`) between two in-process node-admins over real
//! Node RPC: the incumbent of mesh1 hands the fabric seat to the outside mesh-primary of mesh2.
//! Each cell reads the spans the handover emitted, in the order they happened.
// @feature: node-lifecycle

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{FabricId, IncarnationId, MeshId, NodeId, Seat, SeatHolder};
use rafka_mesh_transport::membership::{Backbone, Membership};
use rafka_node_admin_core::admin::Records;
use rafka_node_admin_core::fabric_handover::{HandoverDoor, HandoverError, HandoverSlot, OwnContacts, SeatTransferOutcome, Stage, WonClaims};
use rafka_node_admin_core::fabric_storage::{FabricStorage, FileFabricStorage, HandoverRow, HandoverStage, MemoryFabricStorage};
use rafka_node_admin_core::model::{EndpointId, Fabric, Node, NodeStatus, ProviderKind, ScopeStatus};
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::handover::{FabricPrimaryHandover, FabricPrimaryHandoverReply as Reply, FabricPrimaryHandoverRequest as Request, HandoverBirth, SeatEpoch};
use rafka_mesh_entity::CommittedSeat;
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
    storage: Arc<dyn FabricStorage>,
    claims: Arc<TestClaims>,
    fenced: Arc<Mutex<Vec<(String, Option<String>)>>>,
    _router: Router,
}

/// The won claims a cell names: (build, attempt) -> executor path.name.
#[derive(Default)]
struct TestClaims(Mutex<BTreeMap<(String, u32), String>>);

#[async_trait::async_trait]
impl WonClaims for TestClaims {
    async fn executor(&self, build_id: &str, attempt: u32) -> Result<Option<String>, String> {
        Ok(self.0.lock().unwrap().get(&(build_id.to_string(), attempt)).cloned())
    }
}

struct Pair {
    incumbent: Admin,
    successor: Admin,
    mesh1_id: MeshId,
}

async fn admin(fabric: &FabricId, mesh: &str, path: &str, node_id: &str, fill_slot: bool) -> (Admin, iroh::Endpoint) {
    admin_over(fabric, mesh, path, node_id, fill_slot, Arc::new(MemoryFabricStorage::new())).await
}

/// An admin born over `storage`: a new birth (a new incarnation and a new endpoint key) of the
/// node `node_id`, holding whatever `storage` already holds.
async fn admin_over(fabric: &FabricId, mesh: &str, path: &str, node_id: &str, fill_slot: bool, storage: Arc<dyn FabricStorage>) -> (Admin, iroh::Endpoint) {
    let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let time = rafka_mesh_transport::clock::RafkaTime::unadopted();
    time.adopt(5_000_000);
    let (node_id, incarnation) = (NodeId::parse(node_id).unwrap(), IncarnationId::mint());
    let membership = Membership::join(&gossip, &ep, fabric, mesh, &MeshId::mint(), path, Arc::new(time), vec![]).await.unwrap();
    let backbone = Backbone::join(&gossip, &ep, &membership, mesh, path, incarnation.clone(), vec![]).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    let client = Arc::new(NodeRpcClient::new(ep.clone(), resolver.clone()));
    let slot: HandoverSlot = Arc::new(OnceLock::new());
    let server = rafka_node_admin_core::fabric_handover::serve(ServerBuilder::new(), slot.clone()).seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() }).expect("the catalog seals");
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip).accept(rafka_node_rpc::ALPN, server).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { node_id: node_id.clone(), name: path.parse().unwrap(), endpoint_id: ep.id(), transport_addr: addr, incarnation: incarnation.clone() };
    let claims = Arc::new(TestClaims::default());
    let fenced: Arc<Mutex<Vec<(String, Option<String>)>>> = Arc::default();
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
        contacts: OwnContacts { endpoint_id: ep.id().to_string(), transport_addr: addr, admin_api_base: Some(format!("http://127.0.0.1:{}", addr.port() + 1)) },
        handovers: Default::default(),
        fence_builds: {
            let fenced = fenced.clone();
            Arc::new(move |by: String, to: Option<String>| {
                fenced.lock().unwrap().push((by, to));
                Box::pin(async {})
            })
        },
        claims: claims.clone(),
    });
    if fill_slot {
        let _ = slot.set(door.clone());
    }
    (Admin { door, slot, client, resolver, resolved, storage, claims, fenced, _router: router }, ep)
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
    // The incumbent holds the confirmation once the record is held; the serving task makes the
    // confirmation span right after, so the span is read once it exists.
    until("the incumbent's confirmation span", || spans.position(CONFIRMED).is_some()).await;
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
    until("the incumbent's confirmation span", || spans.position(CONFIRMED).is_some()).await;
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

fn operation_of(f: &Pair) -> String {
    format!("fabric-primary-handover:{}", f.mesh1_id)
}

/// A data dir of this cell alone, removed by the cell.
struct DataDir(std::path::PathBuf);
impl DataDir {
    fn new(tag: &str) -> Self {
        let d = std::env::temp_dir().join(format!("{tag}-{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        std::fs::create_dir_all(&d).unwrap();
        Self(d)
    }
}
impl Drop for DataDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// CONTRACT (fabric-primary-handover.md, step 6): every stage a node reaches is a durable row in its
/// own `fabric.storage`, written with the stage and read back by a fresh storage over the same data
/// dir. The incumbent records what it sent and then the confirmation; the successor records the
/// commit and then the acknowledged completion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_stage_of_the_handover_is_a_durable_row_on_both_admins() {
    if crate::own_process::delegated(module_path!(), "each_stage_of_the_handover_is_a_durable_row_on_both_admins") {
        return;
    }
    let _spans = capture();
    let (incumbent_dir, successor_dir) = (DataDir::new("handover-incumbent"), DataDir::new("handover-successor"));
    let fabric_id = FabricId::mint();
    let (incumbent, _) = admin_over(&fabric_id, "mesh1", "mesh1.admin.1", "000000000009", true, Arc::new(FileFabricStorage::open(&incumbent_dir.0).unwrap())).await;
    let (successor, _) = admin_over(&fabric_id, "mesh2", "mesh2.admin.1", "000000000005", true, Arc::new(FileFabricStorage::open(&successor_dir.0).unwrap())).await;
    let f = join(incumbent, successor).await;
    let op = operation_of(&f);
    f.incumbent.door.hand_over("mesh1", &f.mesh1_id).await.expect("confirmed");
    until("the successor holds the confirmation", || matches!(f.successor.door.handovers.get(&op), Some((_, Stage::Confirmed(8))))).await;
    let successor_rows = FileFabricStorage::open(&successor_dir.0).unwrap().handovers().await.unwrap();
    let incumbent_rows = FileFabricStorage::open(&incumbent_dir.0).unwrap().handovers().await.unwrap();
    let expect = |rows: &[HandoverRow], stage: HandoverStage| {
        assert_eq!(rows.len(), 1, "one handover: {rows:?}");
        let r = &rows[0];
        assert_eq!((r.operation.as_str(), r.fabric.as_str(), r.expected_epoch, r.stage), (op.as_str(), "fabric1", 7, stage));
        assert_eq!((&r.incumbent, &r.successor), (&birth(&f.incumbent), &birth(&f.successor)));
    };
    expect(&successor_rows, HandoverStage::Confirmed { committed_epoch: 8 });
    expect(&incumbent_rows, HandoverStage::Confirmed { committed_epoch: 8 });
}

/// Two admins already placed in one view with the seat at epoch 7 held by the incumbent.
async fn join(incumbent: Admin, successor: Admin) -> Pair {
    let nodes = vec![node_of(&incumbent, NodeStatus::ReadyForTraffic, true), node_of(&successor, NodeStatus::ReadyForTraffic, false)];
    for a in [&incumbent, &successor] {
        *a.door.topology.write().await = view(nodes.clone());
        for p in [&incumbent, &successor] {
            a.resolver.insert(p.resolved.clone());
        }
        a.door.membership.seats().take(Seat::FabricPrimary, &holder_of(&incumbent, 7));
        a.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&incumbent, 1));
        a.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&successor, 1));
    }
    Pair { incumbent, successor, mesh1_id: MeshId::mint() }
}

/// CONTRACT (fabric-primary-handover.md, step 6): a successor that restarts between the commit and
/// the confirmation comes back as a new birth over its own data dir. It holds the commit it
/// recorded, so its gate refuses by name and the mesh leave does not open; the incumbent refuses
/// the completion of a birth that is no longer the successor. What must NOT happen: the restarted
/// admin finding no record and proceeding to the leave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successor_restarted_between_commit_and_confirmation_keeps_the_leave_closed() {
    if crate::own_process::delegated(module_path!(), "a_successor_restarted_between_commit_and_confirmation_keeps_the_leave_closed") {
        return;
    }
    let spans = capture();
    let dir = DataDir::new("handover-restart");
    let fabric_id = FabricId::mint();
    let (incumbent, _) = admin(&fabric_id, "mesh1", "mesh1.admin.1", "000000000009", false).await;
    let (successor, _) = admin_over(&fabric_id, "mesh2", "mesh2.admin.1", "000000000005", true, Arc::new(FileFabricStorage::open(&dir.0).unwrap())).await;
    let f = join(incumbent, successor).await;
    let op = operation_of(&f);
    // The incumbent's door is not serving: the commit happens and its completion is not acknowledged.
    assert_eq!(send(&f.incumbent, &f.successor, take(&f, 7, &op)).await, Reply::Applied);
    until("the successor committed", || matches!(f.successor.door.handovers.get(&op), Some((_, Stage::Committed(8))))).await;
    until("the completion was refused", || spans.named(REJECTED).iter().any(|r| r["call"] == "taken")).await;
    // The successor exits and is born again over its data dir.
    let committed_birth = birth(&f.successor);
    drop(f.successor);
    let (reborn, _) = admin_over(&fabric_id, "mesh2", "mesh2.admin.1", "000000000005", true, Arc::new(FileFabricStorage::open(&dir.0).unwrap())).await;
    assert_ne!(birth(&reborn).incarnation, committed_birth.incarnation, "a restart is a new birth");
    assert!(reborn.door.handovers.get(&op).is_none(), "nothing is held before the admin reads its storage");
    assert_eq!(reborn.door.restore().await.unwrap(), 1);
    let (payload, stage) = reborn.door.handovers.get(&op).expect("the recorded handover");
    assert_eq!((payload.successor, stage), (committed_birth, Stage::Committed(8)));
    // The reborn successor sits in the incumbent's view, and the incumbent's door now serves.
    let nodes = vec![node_of(&f.incumbent, NodeStatus::ReadyForTraffic, true), node_of(&reborn, NodeStatus::ReadyForTraffic, false)];
    *f.incumbent.door.topology.write().await = view(nodes.clone());
    *reborn.door.topology.write().await = view(nodes);
    f.incumbent.resolver.insert(reborn.resolved.clone());
    reborn.resolver.insert(f.incumbent.resolved.clone());
    reborn.door.membership.seats().take(Seat::FabricPrimary, &holder_of(&f.incumbent, 7));
    let _ = f.incumbent.slot.set(f.incumbent.door.clone());
    let err = reborn.door.confirmed(&f.mesh1_id).await.expect_err("the leave stays closed");
    assert!(matches!(&err, HandoverError::Refused(r) if r.contains("rejected-wrong-birth") && r.contains("successor")), "refused by name: {err:?}");
    assert!(spans.named(CONFIRMED).is_empty(), "the incumbent confirmed nothing");
    assert_eq!(f.incumbent.door.membership.seats().fabric(), Some(holder_of(&f.incumbent, 7)), "the incumbent did not yield to a birth that is not the committed successor");
    assert!(matches!(reborn.door.handovers.get(&op), Some((_, Stage::Committed(8)))), "still unconfirmed");
}

/// CONTRACT: a handover record that says committed while the durable fabric seat says otherwise is
/// not trusted: the gate refuses and names both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_record_with_no_durable_seat_is_refused_by_name() {
    if crate::own_process::delegated(module_path!(), "a_committed_record_with_no_durable_seat_is_refused_by_name") {
        return;
    }
    let _spans = capture();
    let storage = Arc::new(MemoryFabricStorage::new());
    let fabric_id = FabricId::mint();
    let (incumbent, _) = admin(&fabric_id, "mesh1", "mesh1.admin.1", "000000000009", true).await;
    let (successor, _) = admin_over(&fabric_id, "mesh2", "mesh2.admin.1", "000000000005", true, storage.clone()).await;
    let f = join(incumbent, successor).await;
    let op = operation_of(&f);
    storage
        .put_handover(&HandoverRow { operation: op.clone(), fabric: "fabric1".into(), incumbent: birth(&f.incumbent), successor: birth(&f.successor), expected_epoch: 7, stage: HandoverStage::Committed { committed_epoch: 8 } })
        .await
        .unwrap();
    assert_eq!(f.successor.door.restore().await.unwrap(), 1);
    let err = f.successor.door.confirmed(&f.mesh1_id).await.expect_err("no durable seat at epoch 8");
    assert!(matches!(&err, HandoverError::Unconfirmed(r) if r.contains("committed at epoch 8") && r.contains("durable fabric seat")), "{err:?}");
}

/// CONTRACT: the incumbent's wait for the completion has no bound of its own. The completion is
/// refused while the incumbent's door is not serving; the attempt stays open and blocking. It
/// reacts to events only: once the door serves, a seat record changing sends the identical command,
/// the successor replays its completion and the attempt confirms. What must NOT happen: the
/// attempt failing on a clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_incumbent_stays_open_until_a_late_completion_and_confirms_on_the_event() {
    if crate::own_process::delegated(module_path!(), "the_incumbent_stays_open_until_a_late_completion_and_confirms_on_the_event") {
        return;
    }
    let spans = capture();
    let (f, _) = fabric(NodeStatus::ReadyForTraffic, false, false).await;
    let op = operation_of(&f);
    let door = f.incumbent.door.clone();
    let mesh1_id = f.mesh1_id.clone();
    let attempt = tokio::spawn(async move { door.hand_over("mesh1", &mesh1_id).await });
    until("the successor committed", || matches!(f.successor.door.handovers.get(&op), Some((_, Stage::Committed(8))))).await;
    until("the completion was refused", || spans.named(REJECTED).iter().any(|r| r["call"] == "taken")).await;
    let sent = spans.named(CALL).len();
    // Nothing happens, and the attempt is open: no completion, no event, no clock.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(!attempt.is_finished(), "the attempt stays open");
    assert_eq!(spans.named(CALL).len(), sent, "and sends nothing without an event");
    assert!(spans.named(CONFIRMED).is_empty());
    // The incumbent's door serves; a seat record changing is the event.
    let _ = f.incumbent.slot.set(f.incumbent.door.clone());
    f.incumbent.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&f.successor, 2));
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(20), attempt).await.expect("the event confirmed the attempt").unwrap();
    outcome.expect("confirmed on the late completion");
    until("the incumbent's confirmation span", || spans.position(CONFIRMED).is_some()).await;
    assert_eq!(spans.named(CONFIRMED).len(), 1);
    assert_eq!(f.incumbent.door.membership.seats().fabric(), Some(holder_of(&f.successor, 8)));
}

/// CONTRACT: repeats of one command and repeats of one completion that arrive together are decided
/// one at a time: one commit, one confirmation, and every other call replays. What must NOT
/// happen: two calls both finding no record and both committing or confirming.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_repeats_of_the_command_and_the_completion_commit_and_confirm_once() {
    if crate::own_process::delegated(module_path!(), "concurrent_repeats_of_the_command_and_the_completion_commit_and_confirm_once") {
        return;
    }
    let spans = capture();
    let (f, _) = fabric(NodeStatus::ReadyForTraffic, false, false).await;
    let op = operation_of(&f);
    let replies = futures_util::future::join_all((0..8).map(|_| send(&f.incumbent, &f.successor, take(&f, 7, &op)))).await;
    assert_eq!(replies.iter().filter(|r| **r == Reply::Applied).count(), 1, "one call commits: {replies:?}");
    assert!(replies.iter().all(|r| matches!(r, Reply::Applied | Reply::AlreadyApplied)), "{replies:?}");
    until("the completion was refused while the incumbent's door is not serving", || spans.named(REJECTED).iter().any(|r| r["call"] == "taken")).await;
    let _ = f.incumbent.slot.set(f.incumbent.door.clone());
    let replies = futures_util::future::join_all((0..8).map(|_| send(&f.successor, &f.incumbent, taken(&f, 7, 8, &op)))).await;
    // The serving incumbent also hears the successor's own event-driven completion, which can be
    // the call that confirms before these eight arrive: then every one of these replays.
    assert!(replies.iter().filter(|r| **r == Reply::Applied).count() <= 1, "at most one call confirms: {replies:?}");
    assert!(replies.iter().all(|r| matches!(r, Reply::Applied | Reply::AlreadyApplied)), "{replies:?}");
    until("the incumbent's confirmation span", || spans.position(CONFIRMED).is_some()).await;
    assert_eq!(spans.named(COMMITTED).len(), 1, "one commit");
    assert_eq!(spans.named(CONFIRMED).len(), 1, "one confirmation");
}


// ---- TakeSeats: Build-driven seat transfer (node-rpc-envelope.md; R-EL1a) ----

const SEATS_COMMITTED: &str = "rdm.node_admin.seat.update.via-seats-committed";
const SEATS_INSTALLED: &str = "rdm.node_admin.seat.update.via-seats-installed";
const SEATS_REJECTED: &str = "rdm.node_admin.seat.reject.via-take-seats";
const SEATED_TOGETHER: &str = "rdm.mesh.seat.update.via-announce-seated-together";

/// mesh1: P (`mesh1.admin.1`) holds both seats (mesh1 at epoch 3, the fabric at epoch 7); N
/// (`mesh1.admin.2`) is the admin the election seats once P's birth is gone; M (`mesh1.admin.3`) is
/// a Ready admin of the same mesh with a higher NodeId. mesh2: X, its primary, with the lowest NodeId
/// of the fabric (so a cross-mesh calculation would pick X, never N). Build `b1` attempt 1 is won by
/// N, Build `b2` attempt 1 by M.
struct Seats {
    p: Admin,
    n: Admin,
    m: Admin,
    x: Admin,
}

async fn seats_fabric() -> Seats {
    let fabric_id = FabricId::mint();
    let (p, _) = admin(&fabric_id, "mesh1", "mesh1.admin.1", "000000000002", true).await;
    let (n, _) = admin(&fabric_id, "mesh1", "mesh1.admin.2", "000000000003", true).await;
    let (m, _) = admin(&fabric_id, "mesh1", "mesh1.admin.3", "000000000009", true).await;
    let (x, _) = admin(&fabric_id, "mesh2", "mesh2.admin.1", "000000000001", true).await;
    let mut pn = node_of(&p, NodeStatus::ReadyForTraffic, true);
    pn.is_primary = true;
    let mut nn = node_of(&n, NodeStatus::ReadyForTraffic, false);
    nn.is_primary = false;
    let mut mn = node_of(&m, NodeStatus::ReadyForTraffic, false);
    mn.is_primary = false;
    let xn = node_of(&x, NodeStatus::ReadyForTraffic, false);
    let nodes = vec![pn, nn, mn, xn];
    for a in [&p, &n, &m, &x] {
        *a.door.topology.write().await = view(nodes.clone());
        for b in [&p, &n, &m, &x] {
            a.resolver.insert(b.resolved.clone());
        }
        a.door.membership.seats().take(Seat::FabricPrimary, &holder_of(&p, 7));
        a.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&p, 3));
        a.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&x, 1));
        a.claims.0.lock().unwrap().insert(("b1".into(), 1), "mesh1.admin.2".into());
        a.claims.0.lock().unwrap().insert(("b2".into(), 1), "mesh1.admin.3".into());
    }
    Seats { p, n, m, x }
}

fn take_seats(holder: &Admin, successor: &Admin, mesh_epoch: u64, fabric_epoch: u64, build: &str, operation: &str) -> Request {
    Request::TakeSeats {
        fabric: "fabric1".into(),
        holder: birth(holder),
        successor: birth(successor),
        seats: vec![SeatEpoch { seat: Seat::MeshPrimary, expected_epoch: mesh_epoch }, SeatEpoch { seat: Seat::FabricPrimary, expected_epoch: fabric_epoch }],
        build_id: build.into(),
        attempt: 1,
        operation: operation.into(),
    }
}

fn committed_to(a: &Admin, mesh_epoch: u64, fabric_epoch: u64) -> Vec<CommittedSeat> {
    vec![CommittedSeat { seat: Seat::MeshPrimary, holder: holder_of(a, mesh_epoch) }, CommittedSeat { seat: Seat::FabricPrimary, holder: holder_of(a, fabric_epoch) }]
}

/// CONTRACT (R-EL1a; node-rpc-envelope.md "TakeSeats"): the executor N calls the holder P. P commits
/// ONE durable row holding both seats at their next epochs (mesh1 4, fabric 8), installs both, fences
/// its Build log naming N, publishes SeatedTogether, and answers SeatsCommitted; N installs the
/// committed seats from the reply. In order: P's commit, then N's install. What must NOT happen: a
/// second seat row written for the same move, or N holding a seat P has not committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seat_holder_commits_both_seats_to_the_executor_in_one_row() {
    if crate::own_process::delegated(module_path!(), "a_seat_holder_commits_both_seats_to_the_executor_in_one_row") {
        return;
    }
    let spans = capture();
    let f = seats_fabric().await;
    let p_node = f.n.door.topology.read().await.members().find(|x| x.name.to_string() == "mesh1.admin.1").cloned().unwrap();
    let out = f.n.door.take_seats(&p_node, "b1", 1, "replace-node:mesh1.admin.1").await.expect("the transfer commits");
    assert_eq!(out, SeatTransferOutcome::Committed { seats: committed_to(&f.n, 4, 8) });
    for a in [&f.p, &f.n] {
        assert_eq!((a.door.membership.seats().mesh("mesh1"), a.door.membership.seats().fabric()), (Some(holder_of(&f.n, 4)), Some(holder_of(&f.n, 8))), "{} holds both committed seats", a.resolved.name);
    }
    let rows = f.p.storage.seat_transfers().await.unwrap();
    assert_eq!(rows.len(), 1, "one row: {rows:?}");
    assert_eq!(rows[0].seats, committed_to(&f.n, 4, 8));
    let standing = f.p.storage.seats().await.unwrap();
    assert!(standing.iter().any(|r| r.seat == Seat::FabricPrimary && r.holder == holder_of(&f.n, 8)) && standing.iter().any(|r| r.seat == Seat::MeshPrimary && r.holder == holder_of(&f.n, 4)), "the row is what the standing seats fold: {standing:?}");
    assert_eq!(f.p.fenced.lock().unwrap().iter().map(|(_, to)| to.clone()).collect::<Vec<_>>(), vec![Some("mesh1.admin.2".to_string())], "P's Build log is fenced naming N");
    until("N's install span", || spans.position(SEATS_INSTALLED).is_some()).await;
    let at = |n: &str| spans.position(n).unwrap_or_else(|| panic!("no {n} span among {:?}", spans.names()));
    assert!(at(SEATS_COMMITTED) < at(SEATED_TOGETHER), "the committed row is announced after it is written");
    assert!(at(SEATED_TOGETHER) < at(SEATS_INSTALLED), "P announces before it answers; N installs from the answer");
    let c = &spans.named(SEATS_COMMITTED)[0];
    assert_eq!((c["build_id"].as_str(), c["attempt"].as_str(), c["mesh_epoch"].as_str(), c["fabric_epoch"].as_str()), ("b1", "1", "4", "8"));
    assert!(spans.named(SEATS_REJECTED).is_empty(), "{:?}", spans.named(SEATS_REJECTED));
    let _ = (&f.m, &f.x);
}

/// CONTRACT: a lost reply is retried with the identical call and answered with the same committed
/// seats before any stale-seat check (P's seats are already at 4 and 8); the same operation with a
/// different payload is RejectedConflict. Nothing commits twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_identical_retry_answers_the_same_seats_and_another_payload_is_refused() {
    let f = seats_fabric().await;
    let op = "drain-node:mesh1.admin.1";
    let first = send(&f.n, &f.p, take_seats(&f.p, &f.n, 3, 7, "b1", op)).await;
    assert_eq!(first, Reply::SeatsCommitted { seats: committed_to(&f.n, 4, 8) });
    // The reply was lost: the executor's own step sends the identical call again.
    let p_node = f.n.door.topology.read().await.members().find(|x| x.name.to_string() == "mesh1.admin.1").cloned().unwrap();
    assert_eq!(f.n.door.take_seats(&p_node, "b1", 1, op).await, Ok(SeatTransferOutcome::Committed { seats: committed_to(&f.n, 4, 8) }), "the retry is answered from the row");
    assert_eq!(send(&f.n, &f.p, take_seats(&f.p, &f.n, 3, 7, "b1", op)).await, first);
    assert_eq!(send(&f.n, &f.p, take_seats(&f.p, &f.n, 3, 6, "b1", op)).await, Reply::RejectedConflict { operation: op.into() });
    assert_eq!(f.p.storage.seat_transfers().await.unwrap().len(), 1);
    assert_eq!(f.p.door.membership.seats().fabric(), Some(holder_of(&f.n, 8)), "one epoch committed");
}

/// CONTRACT: P validates the executor before anything moves, each refusal by name: an admin the
/// attempt's Won claim does not name; an admin the claim names that is not the one the election
/// seats once P's birth is gone; a stale seat epoch; a holder birth that is not P's. Nothing commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_take_seats_from_an_admin_that_is_not_the_seated_executor_is_refused_by_name() {
    if crate::own_process::delegated(module_path!(), "a_take_seats_from_an_admin_that_is_not_the_seated_executor_is_refused_by_name") {
        return;
    }
    let spans = capture();
    let f = seats_fabric().await;
    // M calls under b1, whose claim names N.
    let r = send(&f.m, &f.p, take_seats(&f.p, &f.m, 3, 7, "b1", "drain-node:mesh1.admin.1")).await;
    assert!(matches!(&r, Reply::RejectedNotEligible { reason } if reason.contains("mesh1.admin.2")), "{r:?}");
    // M calls under b2, whose claim names M: the election seats N once P is gone, not M.
    let r = send(&f.m, &f.p, take_seats(&f.p, &f.m, 3, 7, "b2", "stop-node:mesh1.admin.1")).await;
    assert!(matches!(&r, Reply::RejectedNotEligible { reason } if reason.contains("mesh1.admin.2")), "{r:?}");
    // N with a stale mesh epoch.
    assert_eq!(send(&f.n, &f.p, take_seats(&f.p, &f.n, 2, 7, "b1", "drain-node:mesh1.admin.1")).await, Reply::RejectedStaleSeat { seat: Seat::MeshPrimary, expected: 2, current: 3 });
    // N naming another birth of P.
    let mut wrong = take_seats(&f.p, &f.n, 3, 7, "b1", "restart-node:mesh1.admin.1");
    if let Request::TakeSeats { holder, .. } = &mut wrong {
        holder.incarnation = IncarnationId("another-birth".into());
    }
    assert_eq!(send(&f.n, &f.p, wrong).await, Reply::RejectedWrongBirth { role: "holder".into() });
    // X, another mesh's primary, is never the successor of a same-mesh transfer.
    f.p.claims.0.lock().unwrap().insert(("b3".into(), 1), "mesh2.admin.1".into());
    let r = send(&f.x, &f.p, take_seats(&f.p, &f.x, 3, 7, "b3", "drain-node:mesh1.admin.1")).await;
    assert!(matches!(&r, Reply::RejectedNotEligible { .. }), "{r:?}");
    let reasons: Vec<String> = spans.named(SEATS_REJECTED).iter().map(|f| f["reason"].clone()).collect();
    for reason in ["not-the-claimed-executor", "not-seated-once-gone", "stale-seat", "wrong-birth"] {
        assert!(reasons.iter().any(|r| r == reason), "no reject span for {reason}: {reasons:?}");
    }
    assert!(spans.named(SEATS_COMMITTED).is_empty(), "nothing committed");
    assert!(f.p.storage.seat_transfers().await.unwrap().is_empty());
    assert_eq!(f.p.door.membership.seats().fabric(), Some(holder_of(&f.p, 7)), "P still holds the fabric seat");
}

/// CONTRACT: an executor that cannot reach the holder never moves past the transfer on silence; once
/// the holder's birth is proven gone it reads the seat records it holds: a SeatedTogether naming it
/// is the committed result; none means no transfer happened (the ordinary election seats it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_executor_that_cannot_reach_a_proven_lost_holder_reads_the_held_seat_records() {
    if crate::own_process::delegated(module_path!(), "an_executor_that_cannot_reach_a_proven_lost_holder_reads_the_held_seat_records") {
        return;
    }
    let spans = capture();
    let f = seats_fabric().await;
    let p_node = f.n.door.topology.read().await.members().find(|x| x.name.to_string() == "mesh1.admin.1").cloned().unwrap();
    let p_inc = f.p.resolved.incarnation.clone();
    let op_key = "drain-node:mesh1.admin.1";
    // The transfer committed at P (its reply is lost to N's step), then P's process is gone.
    let Reply::SeatsCommitted { seats } = send(&f.n, &f.p, take_seats(&f.p, &f.n, 3, 7, "b1", op_key)).await else { panic!("committed") };
    let Seats { p, n, m, x } = f;
    let _ = p._router.shutdown().await;
    // N's step sends the identical call, which nothing answers: it waits, never moving on silence.
    let step = tokio::spawn({
        let door = n.door.clone();
        let p_node = p_node.clone();
        async move { door.take_seats(&p_node, "b1", 1, op_key).await }
    });
    until("N's call to the gone holder is uncertain", || spans.named(SEATS_REJECTED).iter().any(|f| f["side"] == "executor" && f["reason"] == "uncertain")).await;
    assert!(!step.is_finished(), "an uncertain call is never the end of the step");
    // P's SeatedTogether reaches N, and P's birth is proven gone.
    let op = rafka_mesh_entity::LifecycleOp { build_id: "b1".into(), attempt: 1, operation: op_key.into(), node_id: p_node.node_id.clone(), incarnation: p_inc.clone(), name: "mesh1.admin.1".parse().unwrap(), event_at_rafka_ms: 1 };
    n.door.membership.seats().mark_gone(p_inc.clone());
    n.door.membership.seats().take_together(&op, &holder_of(&n, 4), &seats, false);
    let out = tokio::time::timeout(std::time::Duration::from_secs(30), step).await.expect("a proven-lost holder ends the wait").unwrap();
    assert_eq!(out, Ok(SeatTransferOutcome::HeldRecords { seats: committed_to(&n, 4, 8) }));
    // M, which heard no transfer, holds none: the ordinary election applies.
    m.door.membership.seats().mark_gone(p_inc);
    m.claims.0.lock().unwrap().insert(("b9".into(), 1), "mesh1.admin.3".into());
    let out = tokio::time::timeout(std::time::Duration::from_secs(30), m.door.take_seats(&p_node, "b9", 1, op_key)).await.expect("a proven-lost holder ends the wait");
    assert_eq!(out, Ok(SeatTransferOutcome::NoTransfer));
    let _ = x;
}
