//! The fabric-primary handover (R-ST2, op `0x21`) between two in-process node-admins over real
//! Node RPC: the incumbent of mesh1 hands the fabric seat to the outside mesh-primary of mesh2.
//! Each cell reads the spans the handover emitted, in the order they happened.
// @feature: node-lifecycle

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{FabricId, IncarnationId, MeshId, NodeId, Seat, SeatHolder};
use rafka_mesh_transport::membership::{Backbone, Membership};
use rafka_node_admin_core::admin::Records;
use rafka_node_admin_core::election::{resolve_with, Incumbency};
use rafka_node_admin_core::build_facts_read::FetchFailure;
use rafka_node_admin_core::fabric_handover::{won_claim_from_read, HandoverDoor, HandoverError, HandoverSlot, OwnContacts, ProjectView, SeatTransferOutcome, SourceFailure, Stage, WonClaims, WriteCutOff};
use rafka_node_admin_core::fabric_storage::{FabricIdentity, FabricPointer, FabricRecord, FabricShutdown, FabricStorage, FabricStorageError, FileFabricStorage, HandoverRow, HandoverStage, MemoryFabricStorage, SeatRow, SeatTransferRow};
use rafka_node_admin_core::model::{EndpointId, Fabric, Node, NodeStatus, ProviderKind, ScopeStatus};
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::handover::{CommittedSeat, FabricPrimaryHandover, FabricPrimaryHandoverReply as Reply, FabricPrimaryHandoverRequest as Request, HandoverBirth, Seat as WireSeat, SeatEpoch};
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
    /// Every write cut-off the holder's SeatTransfer step applied, in order.
    cut_offs: Arc<Mutex<Vec<WriteCutOff>>>,
    /// Holds the cut-off's drain open while `false`: a write still issued when the step cuts off.
    drained: tokio::sync::watch::Sender<bool>,
    /// The admin's Node RPC server: a cell drains it as the admin's DrainNode does.
    server: rafka_node_rpc::NodeRpcServer,
    _router: Router,
}

/// The won claims a cell names: (build, attempt) -> executor path.name; and the reads of the
/// fabric-primary's Build facts a cell plants for a (build, attempt), answered as the admin's own
/// claims read answers them (`won_claim_from_read`).
#[derive(Default)]
struct TestClaims(Mutex<BTreeMap<(String, u32), String>>, Mutex<BTreeMap<(String, u32), FetchFailure>>);

#[async_trait::async_trait]
impl WonClaims for TestClaims {
    async fn executor(&self, build_id: &str, attempt: u32) -> Result<Option<String>, SourceFailure> {
        if let Some(failed) = self.1.lock().unwrap().get(&(build_id.to_string(), attempt)).cloned() {
            return won_claim_from_read(Err(failed), build_id, attempt, "mesh1.admin.1 (000000000002)");
        }
        Ok(self.0.lock().unwrap().get(&(build_id.to_string(), attempt)).cloned())
    }
}

/// The error a [`FailingStorage`] answers for a method it is told to fail.
const PLANTED: &str = "planted: the disk refused";

/// A `fabric.storage` held in memory that fails each method named in `fail` with an Io error naming
/// the method: what a store whose disk refuses answers.
#[derive(Default)]
struct FailingStorage {
    inner: MemoryFabricStorage,
    fail: Mutex<std::collections::BTreeSet<&'static str>>,
}

impl FailingStorage {
    fn failing(&self, method: &'static str) {
        self.fail.lock().unwrap().insert(method);
    }
    fn check(&self, method: &'static str) -> Result<(), FabricStorageError> {
        if self.fail.lock().unwrap().contains(method) {
            return Err(FabricStorageError::Io { file: format!("fabric/{method}"), reason: PLANTED.into() });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl FabricStorage for FailingStorage {
    async fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError> {
        self.check("fabric")?;
        self.inner.fabric().await
    }
    async fn fabric_rows(&self) -> Result<(Option<FabricIdentity>, Option<FabricPointer>), FabricStorageError> {
        self.check("fabric_rows")?;
        self.inner.fabric_rows().await
    }
    async fn put_identity(&self, identity: &FabricIdentity) -> Result<FabricIdentity, FabricStorageError> {
        self.check("put_identity")?;
        self.inner.put_identity(identity).await
    }
    async fn put_pointer(&self, pointer: &FabricPointer) -> Result<(), FabricStorageError> {
        self.check("put_pointer")?;
        self.inner.put_pointer(pointer).await
    }
    async fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError> {
        self.check("shutdown")?;
        self.inner.shutdown().await
    }
    async fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError> {
        self.check("put_shutdown")?;
        self.inner.put_shutdown(shutdown).await
    }
    async fn put_seat(&self, row: &SeatRow) -> Result<(), FabricStorageError> {
        self.check("put_seat")?;
        self.inner.put_seat(row).await
    }
    async fn seats(&self) -> Result<Vec<SeatRow>, FabricStorageError> {
        self.check("seats")?;
        self.inner.seats().await
    }
    async fn put_handover(&self, row: &HandoverRow) -> Result<(), FabricStorageError> {
        self.check("put_handover")?;
        self.inner.put_handover(row).await
    }
    async fn handovers(&self) -> Result<Vec<HandoverRow>, FabricStorageError> {
        self.check("handovers")?;
        self.inner.handovers().await
    }
    async fn put_seat_transfer(&self, row: &SeatTransferRow) -> Result<(), FabricStorageError> {
        self.check("put_seat_transfer")?;
        self.inner.put_seat_transfer(row).await
    }
    async fn seat_transfer(&self, holder_mesh: &str, operation: &str) -> Result<Option<SeatTransferRow>, FabricStorageError> {
        self.check("seat_transfer")?;
        self.inner.seat_transfer(holder_mesh, operation).await
    }
    async fn seat_transfers(&self) -> Result<Vec<SeatTransferRow>, FabricStorageError> {
        self.check("seat_transfers")?;
        self.inner.seat_transfers().await
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
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip).accept(rafka_node_rpc::ALPN, server.clone()).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { node_id: node_id.clone(), name: path.parse().unwrap(), endpoint_id: ep.id(), transport_addr: addr, incarnation: incarnation.clone() };
    let claims = Arc::new(TestClaims::default());
    let topology = Arc::new(tokio::sync::RwLock::new(view(vec![])));
    let cut_offs: Arc<Mutex<Vec<WriteCutOff>>> = Arc::default();
    let (drained, drain_gate) = tokio::sync::watch::channel(true);
    // The cell's view is set by hand, not projected from digests: the projection re-runs the one
    // election over its members with the seat records this admin holds now, as the admin's own
    // projection does (`admin::view_projector`).
    let project_view: ProjectView = {
        let (topology, seats) = (topology.clone(), membership.seats().clone());
        Arc::new(move || {
            let (topology, seats) = (topology.clone(), seats.clone());
            Box::pin(async move {
                let inc = Incumbency { fabric: seats.fabric(), meshes: seats.meshes(), lost: Default::default() };
                resolve_with(&mut topology.write().await.nodes, &inc);
            })
        })
    };
    let door = Arc::new(HandoverDoor {
        fabric: "fabric1".into(),
        me: path.parse().unwrap(),
        node_id,
        incarnation,
        topology,
        book: membership.book.clone(),
        membership,
        backbone,
        storage: storage.clone(),
        client: client.clone(),
        records: Arc::new(Records::default()),
        contacts: OwnContacts { endpoint_id: ep.id().to_string(), transport_addr: addr, admin_api_base: Some(format!("http://127.0.0.1:{}", addr.port() + 1)) },
        handovers: Default::default(),
        decisions: Arc::default(),
        claims: claims.clone(),
        project_view,
        cut_off_writes: {
            let cut_offs = cut_offs.clone();
            Arc::new(move |c: WriteCutOff| {
                cut_offs.lock().unwrap().push(c);
                let mut gate = drain_gate.clone();
                Box::pin(async move {
                    let _ = gate.wait_for(|open| *open).await;
                })
            })
        },
    });
    if fill_slot {
        let _ = slot.set(door.clone());
    }
    (Admin { door, slot, client, resolver, resolved, storage, claims, cut_offs, drained, server, _router: router }, ep)
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

/// One call; a call with no reply panics naming the outcome and the leg it used (the dial's failure,
/// or the pooled connection it rode).
async fn send(from: &Admin, to: &Admin, req: Request) -> Reply {
    let (out, ev) = from.client.call::<FabricPrimaryHandover>(&NodeTarget::ExactNode(to.resolved.node_id.clone()), &req, &CallOptions::default()).await;
    out.reply().unwrap_or_else(|| panic!("no reply: {out:?} evidence {ev:?}")).value().clone()
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

fn fabric_primary_of(t: &Topology) -> Option<String> {
    t.fabric_primary().map(|n| n.name.to_string())
}

/// CONTRACT (fabric-node-lifecycle-elections.md §4.4: "The committed next-epoch seat record is
/// authoritative input to the resolver"): the successor answers the command only once its view
/// names it the fabric-primary, so a Build resubmitted to it right after the commit is admitted by
/// its Build door and driven from the committed seat; once the handover is confirmed, the
/// incumbent's view names the successor. What must NOT happen: the successor answering Applied
/// while its view still names the incumbent (its Build door then refuses the resubmit
/// not-fabric-primary, naming the incumbent the seat moved from).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_successor_answers_the_command_with_its_view_naming_it_the_fabric_primary() {
    let (f, _) = fabric(NodeStatus::ReadyForTraffic, false, true).await;
    let op = format!("fabric-primary-handover:{}", f.mesh1_id);
    assert_eq!(send(&f.incumbent, &f.successor, take(&f, 7, &op)).await, Reply::Applied);
    assert_eq!(fabric_primary_of(&*f.successor.door.topology.read().await).as_deref(), Some("mesh2.admin.1"), "the successor's view names it once it answered the command");
    f.incumbent.door.hand_over("mesh1", &f.mesh1_id).await.expect("the handover is confirmed");
    assert_eq!(fabric_primary_of(&*f.incumbent.door.topology.read().await).as_deref(), Some("mesh2.admin.1"), "the incumbent's view names the successor once it holds the committed record");
}

/// CONTRACT (node-rpc-envelope.md "TakeSeats"; elections §4.4): the seat row is the resolver's
/// input on both sides. When P answers SeatsCommitted its view names N as mesh1's primary and the
/// fabric-primary, and when N's SeatTransfer step returns, N's view names N: N's Build door admits
/// the caller's resubmit of the same Build as the new fabric-primary. What must NOT happen: either
/// view still naming P after the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_views_name_the_executor_once_the_seat_row_is_committed() {
    let f = seats_fabric().await;
    let p_node = f.n.door.topology.read().await.members().find(|x| x.name.to_string() == "mesh1.admin.1").cloned().unwrap();
    f.n.door.take_seats(&p_node, "b1", 1, "replace-node:mesh1.admin.1").await.expect("the transfer commits");
    for a in [&f.p, &f.n] {
        let t = a.door.topology.read().await;
        assert_eq!(fabric_primary_of(&t).as_deref(), Some("mesh1.admin.2"), "{}: the fabric-primary is N", a.resolved.name);
        assert_eq!(t.cohort_primary("mesh1", rafka_node_admin_core::model::NodeKind::NodeAdmin).map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.2"), "{}: mesh1's primary is N", a.resolved.name);
    }
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
    // The command names a birth that is not the receiver's, or one the receiver's view does not hold.
    let wrong_successor = Request::TakeFabricPrimary { fabric: "fabric1".into(), incumbent: birth(&f.incumbent), successor: other_birth(&birth(&f.successor)), expected_epoch: 7, operation: op.into() };
    assert_eq!(send(&f.incumbent, &f.successor, wrong_successor).await, Reply::RejectedWrongBirth { role: "successor".into() });
    let wrong_incumbent = Request::TakeFabricPrimary { fabric: "fabric1".into(), incumbent: other_birth(&birth(&f.incumbent)), successor: birth(&f.successor), expected_epoch: 7, operation: op.into() };
    assert_eq!(send(&f.incumbent, &f.successor, wrong_incumbent).await, Reply::RejectedWrongBirth { role: "incumbent".into() });
    // A completion naming a successor the receiver's view does not hold, or an incumbent that is not the receiver.
    let taken_wrong_successor = Request::FabricPrimaryTaken { fabric: "fabric1".into(), incumbent: birth(&f.incumbent), successor: other_birth(&birth(&f.successor)), expected_epoch: 7, committed_epoch: 8, operation: op.into() };
    assert_eq!(send(&f.successor, &f.incumbent, taken_wrong_successor).await, Reply::RejectedWrongBirth { role: "successor".into() });
    let taken_wrong_incumbent = Request::FabricPrimaryTaken { fabric: "fabric1".into(), incumbent: other_birth(&birth(&f.incumbent)), successor: birth(&f.successor), expected_epoch: 7, committed_epoch: 8, operation: op.into() };
    assert_eq!(send(&f.successor, &f.incumbent, taken_wrong_incumbent).await, Reply::RejectedWrongBirth { role: "incumbent".into() });
    // A stale epoch: the seat is at 7.
    assert_eq!(send(&f.incumbent, &f.successor, take(&f, 5, op)).await, Reply::RejectedStaleEpoch { expected: 5, current: 7 });
    // A completion whose committed epoch is not the next one.
    assert_eq!(send(&f.successor, &f.incumbent, taken(&f, 7, 9, op)).await, Reply::RejectedStaleEpoch { expected: 8, current: 9 });
    // A command naming an incumbent that is not the seat holder: the successor's record says someone else.
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
    let (incumbent, _) = admin_over(&fabric_id, "mesh1", "mesh1.admin.1", "000000000009", true, Arc::new(FileFabricStorage::open(&rafka_node_admin_core::store_root::StoreRoot::at(&incumbent_dir.0), &rafka_node_admin_core::model::IncarnationId("test".into())).await.unwrap())).await;
    let (successor, _) = admin_over(&fabric_id, "mesh2", "mesh2.admin.1", "000000000005", true, Arc::new(FileFabricStorage::open(&rafka_node_admin_core::store_root::StoreRoot::at(&successor_dir.0), &rafka_node_admin_core::model::IncarnationId("test".into())).await.unwrap())).await;
    let f = join(incumbent, successor).await;
    let op = operation_of(&f);
    f.incumbent.door.hand_over("mesh1", &f.mesh1_id).await.expect("confirmed");
    until("the successor holds the confirmation", || matches!(f.successor.door.handovers.get(&op), Some((_, Stage::Confirmed(8))))).await;
    let successor_rows = FileFabricStorage::open(&rafka_node_admin_core::store_root::StoreRoot::at(&successor_dir.0), &rafka_node_admin_core::model::IncarnationId("test".into())).await.unwrap().handovers().await.unwrap();
    let incumbent_rows = FileFabricStorage::open(&rafka_node_admin_core::store_root::StoreRoot::at(&incumbent_dir.0), &rafka_node_admin_core::model::IncarnationId("test".into())).await.unwrap().handovers().await.unwrap();
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
    let (successor, _) = admin_over(&fabric_id, "mesh2", "mesh2.admin.1", "000000000005", true, Arc::new(FileFabricStorage::open(&rafka_node_admin_core::store_root::StoreRoot::at(&dir.0), &rafka_node_admin_core::model::IncarnationId("test".into())).await.unwrap())).await;
    let f = join(incumbent, successor).await;
    let op = operation_of(&f);
    // The incumbent's door is not serving: the commit happens and its completion is not acknowledged.
    assert_eq!(send(&f.incumbent, &f.successor, take(&f, 7, &op)).await, Reply::Applied);
    until("the successor committed", || matches!(f.successor.door.handovers.get(&op), Some((_, Stage::Committed(8))))).await;
    until("the completion was refused", || spans.named(REJECTED).iter().any(|r| r["call"] == "taken")).await;
    // The successor exits and is born again over its data dir.
    let committed_birth = birth(&f.successor);
    drop(f.successor);
    let (reborn, _) = admin_over(&fabric_id, "mesh2", "mesh2.admin.1", "000000000005", true, Arc::new(FileFabricStorage::open(&rafka_node_admin_core::store_root::StoreRoot::at(&dir.0), &rafka_node_admin_core::model::IncarnationId("test".into())).await.unwrap())).await;
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
    seats_fabric_with(|| async { Arc::new(MemoryFabricStorage::new()) as Arc<dyn FabricStorage> }).await
}

/// The seats fabric with each admin's fabric.storage given by `storage` (one per admin).
async fn seats_fabric_with<F: std::future::Future<Output = Arc<dyn FabricStorage>>>(storage: impl Fn() -> F) -> Seats {
    let fabric_id = FabricId::mint();
    let (p, _) = admin_over(&fabric_id, "mesh1", "mesh1.admin.1", "000000000002", true, storage().await).await;
    let (n, _) = admin_over(&fabric_id, "mesh1", "mesh1.admin.2", "000000000003", true, storage().await).await;
    let (m, _) = admin_over(&fabric_id, "mesh1", "mesh1.admin.3", "000000000009", true, storage().await).await;
    let (x, _) = admin_over(&fabric_id, "mesh2", "mesh2.admin.1", "000000000001", true, storage().await).await;
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
        seats: vec![SeatEpoch { seat: WireSeat::MeshPrimary { mesh: "mesh1".into() }, expected_epoch: mesh_epoch }, SeatEpoch { seat: WireSeat::FabricPrimary, expected_epoch: fabric_epoch }],
        build_id: build.into(),
        attempt: 1,
        operation: operation.into(),
    }
}

fn committed_to(a: &Admin, mesh_epoch: u64, fabric_epoch: u64) -> Vec<CommittedSeat> {
    vec![CommittedSeat::of(Seat::MeshPrimary, &holder_of(a, mesh_epoch)), CommittedSeat::of(Seat::FabricPrimary, &holder_of(a, fabric_epoch))]
}

/// CONTRACT (R-EL1a; node-rpc-envelope.md "TakeSeats"): the executor N calls the holder P. P commits
/// ONE durable row holding both seats at their next epochs (mesh1 4, fabric 8), installs both (the moved
/// record fences its Build log naming N), publishes SeatedTogether, and answers SeatsCommitted; N installs the
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
    // The moved record is P's fence: read for P's birth, it names N.
    let p_birth = (p_node.node_id.clone(), p_node.incarnation_id.clone().unwrap());
    // This fixture's admins are wired by view, with no digests in their books: N's path.name is read
    // from P's view here (the admin reads it from its digest book, `admin::fabric_seat_source`).
    let view = f.p.door.topology.read().await.clone();
    let read = rafka_node_admin_core::seat_watch::fabric_seat_record(f.p.door.membership.seats().fabric().as_ref(), &p_birth, &Default::default(), |id| view.own(id).map(|n| n.name.to_string()));
    assert!(matches!(&read, rafka_node_admin_core::fabric_builds::SeatRecord::Moved { to: Some(to), .. } if to == "mesh1.admin.2"), "P's seat record fences its Build log naming N: {read:?}");
    until("N's install span", || spans.position(SEATS_INSTALLED).is_some()).await;
    let at = |n: &str| spans.position(n).unwrap_or_else(|| panic!("no {n} span among {:?}", spans.names()));
    assert!(at(SEATS_COMMITTED) < at(SEATED_TOGETHER), "the committed row is announced after it is written");
    assert!(at(SEATED_TOGETHER) < at(SEATS_INSTALLED), "P announces before it answers; N installs from the answer");
    let c = &spans.named(SEATS_COMMITTED)[0];
    assert_eq!((c["build_id"].as_str(), c["attempt"].as_str(), c["mesh_epoch"].as_str(), c["fabric_epoch"].as_str()), ("b1", "1", "4", "8"));
    assert!(spans.named(SEATS_REJECTED).is_empty(), "{:?}", spans.named(SEATS_REJECTED));
    let _ = (&f.m, &f.x);
}

/// CONTRACT (R14 item 4; node-rpc-envelope.md "TakeSeats"): the holder's SeatTransfer step cuts
/// off the writes its moving seats authorize (both seats here, so the fabric record's too) and
/// waits for every issued write to drain BEFORE it writes the seat row. While the drain is held
/// open (a write still on the disk) no seat row exists and the call is unanswered; once it
/// drains, the one row is written and SeatsCommitted answered. What must NOT happen: the seat row
/// written while an old-seat write is still issued, or a commit with no cut-off.
#[test]
fn the_holder_drains_its_seat_writes_before_it_writes_the_seat_row() {
    let cap = crate::mesh_leave_resume::capture();
    let op = "drain-node:mesh1.admin.1";
    let trace_id = cap.run(async {
        let root = tracing::info_span!("rdm.test.seat.update.via-take-seats-drain");
        let trace_id = crate::mesh_leave_resume::trace_id(&root);
        tracing::Instrument::instrument(the_holder_drains(op), root).await;
        trace_id
    });
    println!("trace_id={trace_id}");
    let spans = cap.spans();
    let ours = |name: &str| spans.iter().filter(|s| s["name"] == name && s["attributes"]["operation"] == op).collect::<Vec<_>>();
    let drained = ours("rdm.node_admin.seat.update.via-writes-drained");
    assert_eq!(drained.len(), 1, "one drain span for the transfer: {:?}", spans.iter().map(|s| s["name"].clone()).collect::<Vec<_>>());
    assert_eq!(drained[0]["attributes"]["fabric_seat"], "true");
    let committed = ours("rdm.node_admin.seat.update.via-seats-committed");
    assert_eq!(committed.len(), 1, "one commit span for the transfer");
    assert_eq!((drained[0]["trace_id"].as_str(), committed[0]["trace_id"].as_str()), (Some(trace_id.as_str()), Some(trace_id.as_str())), "the holder's drain and commit are the cell's own story");
}

async fn the_holder_drains(op: &'static str) {
    let f = seats_fabric().await;
    f.p.drained.send_replace(false);
    let call = tokio::spawn({
        let (n, p) = (f.n.client.clone(), f.p.resolved.node_id.clone());
        let req = take_seats(&f.p, &f.n, 3, 7, "b1", op);
        tracing::Instrument::in_current_span(async move {
            let (out, _) = n.call::<FabricPrimaryHandover>(&NodeTarget::ExactNode(p), &req, &CallOptions::default()).await;
            out.reply().map(|r| r.value().clone())
        })
    });
    until("P cuts off its seat writes", || !f.p.cut_offs.lock().unwrap().is_empty()).await;
    assert!(
        matches!(&f.p.cut_offs.lock().unwrap()[..], [WriteCutOff::Set { reason, fabric_seat: true }] if reason.contains(op) && reason.contains("mesh1")),
        "one cut-off naming the transfer, the fabric seat included: {:?}",
        f.p.cut_offs.lock().unwrap()
    );
    assert!(f.p.storage.seat_transfers().await.unwrap().is_empty(), "the seat row was written while a write of the old seat was still issued");
    assert!(!call.is_finished(), "P answered before its writes drained");
    f.p.drained.send_replace(true);
    let reply = call.await.unwrap();
    assert!(matches!(&reply, Some(Reply::SeatsCommitted { seats }) if *seats == committed_to(&f.n, 4, 8)), "{reply:?}");
    assert_eq!(f.p.storage.seat_transfers().await.unwrap().len(), 1, "one row, written after the drain");
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

/// CONTRACT (node-rpc-envelope.md "TakeSeats", lost replies): P keeps serving TakeSeats after the
/// row. The reply was lost and P has since been drained (its Node RPC server refuses new calls
/// typed Draining, as its DrainNode makes it): the executor's identical retry is still answered
/// from the row with the same committed seats, never refused Draining, and nothing commits twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_after_the_holder_drained_is_answered_from_the_row() {
    let f = seats_fabric().await;
    let op = "replace-node:mesh1.admin.1";
    let first = send(&f.n, &f.p, take_seats(&f.p, &f.n, 3, 7, "b1", op)).await;
    assert_eq!(first, Reply::SeatsCommitted { seats: committed_to(&f.n, 4, 8) });
    f.p.server.drain();
    assert_eq!(send(&f.n, &f.p, take_seats(&f.p, &f.n, 3, 7, "b1", op)).await, first, "a drained holder answers the identical retry from the row");
    assert_eq!(f.p.storage.seat_transfers().await.unwrap().len(), 1, "nothing committed twice");
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
    assert_eq!(send(&f.n, &f.p, take_seats(&f.p, &f.n, 2, 7, "b1", "drain-node:mesh1.admin.1")).await, Reply::RejectedStaleSeat { seat: WireSeat::MeshPrimary { mesh: "mesh1".into() }, expected: 2, current: 3 });
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

/// CONTRACT (node-rpc-envelope.md "TakeSeats: Build-driven seat transfer": "once P's birth is proven
/// lost it reads the committed seat row. A row naming N is the committed result and N installs it;
/// no row means the transfer never happened"; R-SR1=A): every admin's fabric.storage is on the
/// fabric's one store root. P commits the transfer as ONE row under its own mesh's directory
/// (`meshes/mesh1/seat-transfers/`) and its reply is lost; no SeatedTogether reaches N. N never
/// moves on silence; once P's birth is proven gone it point-reads P's committed row on the root and
/// installs the seats it names. M, executing another operation of P's that no row records, finds
/// none: no transfer happened (the ordinary election seats the successor). M, asked for P's committed
/// operation, finds the row naming N and refuses by name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_executor_that_cannot_reach_a_proven_lost_holder_reads_the_committed_seat_row() {
    if crate::own_process::delegated(module_path!(), "an_executor_that_cannot_reach_a_proven_lost_holder_reads_the_committed_seat_row") {
        return;
    }
    // The cell's spans: the in-memory names the waits read, and the product span pipeline
    // (`rafka_mesh_telemetry::span_capture`) stamping the trace ids the proof names.
    let spans = Spans::default();
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rafka_mesh_telemetry::span_capture("mesh1.admin.2", exporter.clone());
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(spans.clone()).with(layer)).expect("this cell owns its process");
    let story = tracing::info_span!("rdm.test.seat.update.via-lost-holder-row");
    let trace_id = story.in_scope(rafka_mesh_telemetry::current_traceparent).and_then(|tp| tp.split('-').nth(1).map(str::to_string)).expect("the cell's root span has a trace context");
    let root = DataDir::new("seat-transfer-root");
    let f = seats_fabric_with(|| {
        let at = rafka_node_admin_core::store_root::StoreRoot::at(&root.0);
        async move { Arc::new(FileFabricStorage::open(&at, &rafka_node_admin_core::model::IncarnationId::mint()).await.unwrap()) as Arc<dyn FabricStorage> }
    })
    .await;
    let p_node = f.n.door.topology.read().await.members().find(|x| x.name.to_string() == "mesh1.admin.1").cloned().unwrap();
    let p_inc = f.p.resolved.incarnation.clone();
    let op_key = "drain-node:mesh1.admin.1";
    // The transfer committed at P (its reply is lost to N's step), then P's process is gone.
    let Reply::SeatsCommitted { seats } = tracing::Instrument::instrument(send(&f.n, &f.p, take_seats(&f.p, &f.n, 3, 7, "b1", op_key)), story.clone()).await else { panic!("committed") };
    assert_eq!(seats, committed_to(&f.n, 4, 8));
    let row_file = root.0.join("meshes").join("mesh1").join("seat-transfers").join("drain-node_mesh1_admin_1.json");
    assert!(row_file.exists(), "P's seat-transfer row is not under its own mesh's directory on the root: {}", row_file.display());
    assert!(!root.0.join("fabric").join("seat-transfers").exists(), "a seat-transfer row was written under fabric/, the fabric-primary's");
    let Seats { p, n, m, x } = f;
    let _ = p._router.shutdown().await;
    // N's step sends the identical call, which nothing answers: it waits, never moving on silence.
    let step = tokio::spawn({
        let door = n.door.clone();
        let p_node = p_node.clone();
        tracing::Instrument::instrument(async move { door.take_seats(&p_node, "b1", 1, op_key).await }, story.clone())
    });
    until("N's call to the gone holder is uncertain", || spans.named(SEATS_REJECTED).iter().any(|f| f["side"] == "executor" && f["reason"] == "uncertain")).await;
    assert!(!step.is_finished(), "an uncertain call is never the end of the step");
    // No SeatedTogether reaches N; P's birth is proven gone.
    n.door.membership.seats().mark_gone(p_inc.clone());
    let out = tokio::time::timeout(std::time::Duration::from_secs(30), step).await.expect("a proven-lost holder ends the wait").unwrap();
    assert_eq!(out, Ok(SeatTransferOutcome::CommittedRow { seats: committed_to(&n, 4, 8) }));
    let book = n.door.membership.seats();
    assert!(book.mesh("mesh1").is_some_and(|h| h.is_birth(&n.resolved.node_id, &n.resolved.incarnation) && h.epoch == 4), "N did not install the mesh seat the row names");
    assert!(book.fabric().is_some_and(|h| h.is_birth(&n.resolved.node_id, &n.resolved.incarnation) && h.epoch == 8), "N did not install the fabric seat the row names");
    // M: an operation of P's that no row records is no transfer.
    m.door.membership.seats().mark_gone(p_inc);
    m.claims.0.lock().unwrap().insert(("b9".into(), 1), "mesh1.admin.3".into());
    let out = tokio::time::timeout(std::time::Duration::from_secs(30), m.door.take_seats(&p_node, "b9", 1, "stop-node:mesh1.admin.1")).await.expect("a proven-lost holder ends the wait");
    assert_eq!(out, Ok(SeatTransferOutcome::NoTransfer));
    // M asked for the operation P committed to N: the row names N, refused by name.
    let out = tokio::time::timeout(std::time::Duration::from_secs(30), m.door.take_seats(&p_node, "b9", 1, op_key)).await.expect("a proven-lost holder ends the wait");
    let e = out.expect_err("M took seats P's committed row gives N");
    assert!(e.contains(op_key) && e.contains(&n.resolved.node_id.to_string()), "{e}");
    let _ = x;
    // The proof: N's read of P's committed row is the cell's own story.
    drop(story);
    pipeline.flush(std::time::Duration::from_secs(10)).expect("the cell's spans reach its exporter");
    let finished = exporter.get_finished_spans().unwrap();
    let attr = |s: &opentelemetry_sdk::export::trace::SpanData, k: &str| s.attributes.iter().find(|kv| kv.key.as_str() == k).map(|kv| kv.value.to_string());
    let read = finished.iter().find(|s| s.name == "rdm.node_admin.seat.update.via-committed-row").expect("N's committed-row read left no span");
    println!("trace_id={trace_id}");
    for s in finished.iter().filter(|s| s.span_context.trace_id().to_string() == trace_id) {
        println!("span {} {:?}", s.name, attr(s, "outcome").or_else(|| attr(s, "reason")));
    }
    assert_eq!(read.span_context.trace_id().to_string(), trace_id, "the committed-row read is the cell's own story");
    assert_eq!(attr(read, "outcome").as_deref(), Some("held"));
    assert_eq!(attr(read, "successor").as_deref(), Some(format!("mesh1:{}:{}", n.resolved.node_id, n.resolved.incarnation.0).as_str()));
}

/// CONTRACT (node-rpc-envelope.md "TakeSeats: Build-driven seat transfer", Boundary): TakeSeats moves
/// seats inside the holder's mesh only. An executor of another mesh (mesh2.admin.1) asked to take
/// the seats of the fabric-primary P refuses the step by name (`rdm.node_admin.seat.reject.via-take-seats`,
/// side executor, reason other-mesh-fabric-seat) and calls nothing: P commits no row and keeps both
/// seats. The fabric seat leaves P's mesh only through TakeFabricPrimary inside a mesh-leave. A
/// holder of its own mesh's seat alone (mesh2.admin.1, executed from mesh1) is `OtherMesh`: that seat
/// stays with it until its birth is gone and the ordinary election seats the next.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_executor_of_another_mesh_refuses_to_take_the_fabric_seat_by_name() {
    if crate::own_process::delegated(module_path!(), "an_executor_of_another_mesh_refuses_to_take_the_fabric_seat_by_name") {
        return;
    }
    let spans = capture();
    let f = seats_fabric().await;
    let view = f.n.door.topology.read().await.clone();
    let member = |name: &str| view.members().find(|x| x.name.to_string() == name).cloned().unwrap();
    let p_node = member("mesh1.admin.1");
    let e = f.x.door.take_seats(&p_node, "b1", 1, "replace-node:mesh1.admin.1").await.expect_err("an executor of mesh2 cannot take mesh1's fabric-primary seats");
    assert!(e.contains("mesh1.admin.1") && e.contains("mesh1") && e.contains("mesh2.admin.1") && e.contains("TakeFabricPrimary") && e.contains("mesh-leave"), "the refusal names the holder, its mesh, the executor and the one way the fabric seat leaves: {e}");
    until("the executor's refusal span", || !spans.named(SEATS_REJECTED).is_empty()).await;
    let r = &spans.named(SEATS_REJECTED)[0];
    assert_eq!((r["side"].as_str(), r["reason"].as_str()), ("executor", "other-mesh-fabric-seat"), "{r:?}");
    assert!(f.p.storage.seat_transfers().await.unwrap().is_empty(), "P commits no row");
    assert_eq!(f.p.door.membership.seats().fabric(), Some(holder_of(&f.p, 7)), "P keeps the fabric seat");
    assert!(spans.named(SEATS_COMMITTED).is_empty(), "{:?}", spans.named(SEATS_COMMITTED));

    let x_node = member("mesh2.admin.1");
    assert_eq!(f.p.door.take_seats(&x_node, "b2", 1, "restart-node:mesh2.admin.1").await, Ok(SeatTransferOutcome::OtherMesh { holder_mesh: "mesh2".into() }), "a mesh seat alone stays with its holder");
    let _ = (&f.m, &f.n);
}

/// CONTRACT (R14 item 4: "the holder cuts off its writes to the stores its moving seats authorize,
/// drains them, then writes the seat row"; fabric-node-lifecycle.md §4.3: the mesh-primary writes
/// every status of its mesh): a moving mesh seat cuts off the mesh's status rows on the fabric's
/// store root with its Mesh row, Node rows and contacts: a status write after the cut-off is refused
/// by name, naming the transfer; a lift gives the writes back.
#[test]
fn a_moving_mesh_seat_cuts_off_the_mesh_status_rows() {
    use rafka_node_admin_core::fabric_handover::{SeatStores, WriteCutOff};
    use rafka_node_admin_core::status_storage::{MeshStatusRow, StatusStorage};
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let d = std::env::temp_dir().join(format!("seat-stores-{}", rafka_node_admin_core::model::NodeId::mint()));
        let root = rafka_node_admin_core::store_root::StoreRoot::at(&d.join("store"));
        let inc = rafka_node_admin_core::model::IncarnationId::mint();
        let stores = SeatStores {
            mesh: std::sync::Arc::new(rafka_node_admin_core::storage::FileMeshStorage::open(&root, &inc).await.unwrap()),
            nodes: std::sync::Arc::new(rafka_node_admin_core::storage::FileNodesStorage::open(&d.join("data"), &root, "mesh1", &inc).await.unwrap()),
            fabric: std::sync::Arc::new(rafka_node_admin_core::fabric_storage::FileFabricStorage::open(&root, &inc).await.unwrap()),
            status: std::sync::Arc::new(rafka_node_admin_core::status_storage::FileStatusStorage::open(&root, "mesh1", &inc).await.unwrap()),
        };
        let row = MeshStatusRow { mesh_id: rafka_node_admin_core::model::MeshId::mint(), state: rafka_node_rpc_contract::status::MeshState::ReadyForTraffic };
        stores.apply(WriteCutOff::Set { reason: "take-seats:mesh1.admin.2".into(), fabric_seat: false }).await;
        match stores.status.put_mesh_status(&row).await {
            Err(rafka_node_admin_core::record_store::StorageError::CutOff { reason, .. }) => assert!(reason.contains("take-seats:mesh1.admin.2"), "{reason}"),
            other => panic!("a status write after the moving seat's cut-off: {other:?}"),
        }
        stores.apply(WriteCutOff::Lift).await;
        stores.status.put_mesh_status(&row).await.expect("a lift gives the status writes back");
        let _ = std::fs::remove_dir_all(&d);
    });
}

/// CONTRACT (R14 item 4; node-rpc-envelope.md, TakeSeats: "P's execution authority ends at the
/// row's epoch"): a holder that opened its stores before the fabric's first state-commit holds what
/// it writes in its scratchpad. A seat transfer cuts those stores off; once its seat row commits,
/// every row the moving seats' stores still hold (the Mesh row, the mesh's status rows and, the
/// fabric seat moving too, the Fabric identity) is dropped: never read again, and a later
/// publication writes none of them. What must NOT happen: a row whose writer's authority ended
/// kept and read in place of the store root, or written there after the seat row.
#[test]
fn a_committed_seat_transfer_drops_every_row_its_holder_still_held() {
    use rafka_node_admin_core::fabric_handover::{SeatStores, WriteCutOff};
    use rafka_node_admin_core::fabric_storage::FabricStorage as _;
    use rafka_node_admin_core::status_storage::{MeshStatusRow, StatusStorage};
    use rafka_node_admin_core::storage::MeshStorage as _;
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let d = std::env::temp_dir().join(format!("seat-stores-held-{}", rafka_node_admin_core::model::NodeId::mint()));
        let pad = rafka_node_admin_core::record_store::Scratchpad::new();
        let root = rafka_node_admin_core::store_root::StoreRoot::held_at(&d.join("store"), pad.clone());
        let inc = rafka_node_admin_core::model::IncarnationId::mint();
        let stores = SeatStores {
            mesh: std::sync::Arc::new(rafka_node_admin_core::storage::FileMeshStorage::open(&root, &inc).await.unwrap()),
            nodes: std::sync::Arc::new(rafka_node_admin_core::storage::FileNodesStorage::open(&d.join("data"), &root, "mesh1", &inc).await.unwrap()),
            fabric: std::sync::Arc::new(rafka_node_admin_core::fabric_storage::FileFabricStorage::open(&root, &inc).await.unwrap()),
            status: std::sync::Arc::new(rafka_node_admin_core::status_storage::FileStatusStorage::open(&root, "mesh1", &inc).await.unwrap()),
        };
        stores.mesh.put_mesh(&rafka_node_admin_core::storage::MeshRecord { mesh_id: rafka_node_admin_core::model::MeshId::mint(), name: "mesh1".into() }).await.unwrap();
        stores.status.put_mesh_status(&MeshStatusRow { mesh_id: rafka_node_admin_core::model::MeshId::mint(), state: rafka_node_rpc_contract::status::MeshState::ReadyForTraffic }).await.unwrap();
        stores.fabric.put_identity(&rafka_node_admin_core::fabric_storage::FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() }).await.unwrap();
        assert!(stores.mesh.mesh("mesh1").await.unwrap().is_some(), "the holder reads the Mesh row it holds");
        let reason = "the seats of mesh1.admin.1 moved to mesh1 000000000002 by take-seats:b1";
        stores.apply(WriteCutOff::Set { reason: reason.into(), fabric_seat: true }).await;
        stores.apply(WriteCutOff::Committed { reason: reason.into(), fabric_seat: true }).await;
        assert!(stores.mesh.mesh("mesh1").await.unwrap().is_none(), "a Mesh row dropped at the commit is never read");
        assert!(stores.fabric.fabric().await.unwrap().is_none(), "a Fabric identity dropped at the commit is never read");
        let published = pad.publish().await.unwrap();
        assert_eq!((published.written(), published.dropped(), published.held()), (0, 0, 0), "nothing the holder held is published after the seat row: {published:?}");
        fn files(dir: &std::path::Path, out: &mut Vec<String>) {
            for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
                if e.path().is_dir() {
                    files(&e.path(), out);
                } else {
                    out.push(e.path().display().to_string());
                }
            }
        }
        let mut on_root = Vec::new();
        files(&d.join("store"), &mut on_root);
        assert!(on_root.is_empty(), "no row reached the store root: {on_root:?}");
        let _ = std::fs::remove_dir_all(&d);
    });
}

/// CONTRACT (R14 items 2 and 4; R13: the fabric-primary writes another mesh's rows only as acting
/// primary of a mesh with no primary, an authority its fabric seat gives it; mesh-leave.md step 7):
/// a holder that opened its stores before the fabric's first state-commit holds the rows it wrote as
/// acting primary of mesh2 (mesh2's Node row and its Mesh status row) in its scratchpad. Once a seat
/// transfer moving the fabric seat commits, every one of them is dropped: never read again, and a
/// later publication writes none of them. What must NOT happen: a row its writer held as acting
/// primary kept after the fabric seat moved, read in place of the store root or written there.
#[test]
fn a_committed_fabric_seat_transfer_drops_the_rows_its_holder_held_as_acting_primary_of_another_mesh() {
    use rafka_node_admin_core::fabric_handover::{SeatStores, WriteCutOff};
    use rafka_node_admin_core::status_storage::{MeshStatusRow, StatusStorage as _};
    use rafka_node_admin_core::storage::{NodeRow, NodesStorage as _};
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let d = std::env::temp_dir().join(format!("seat-stores-acting-{}", NodeId::mint()));
        let pad = rafka_node_admin_core::record_store::Scratchpad::new();
        let root = rafka_node_admin_core::store_root::StoreRoot::held_at(&d.join("store"), pad.clone());
        let inc = IncarnationId::mint();
        let stores = SeatStores {
            mesh: std::sync::Arc::new(rafka_node_admin_core::storage::FileMeshStorage::open(&root, &inc).await.unwrap()),
            nodes: std::sync::Arc::new(rafka_node_admin_core::storage::FileNodesStorage::open(&d.join("data"), &root, "mesh1", &inc).await.unwrap()),
            fabric: std::sync::Arc::new(FileFabricStorage::open(&root, &inc).await.unwrap()),
            status: std::sync::Arc::new(rafka_node_admin_core::status_storage::FileStatusStorage::open(&root, "mesh1", &inc).await.unwrap()),
        };
        let row = NodeRow { name: "mesh2.rpc.1".parse().unwrap(), node_id: NodeId::mint(), status: NodeStatus::Deleted, config: Default::default() };
        let dead = MeshStatusRow { mesh_id: MeshId::mint(), state: rafka_node_rpc_contract::status::MeshState::Dead };
        stores.nodes.put_node_row(&row).await.unwrap().expect("mesh2.rpc.1 is a free key");
        stores.status.put_mesh_status_of("mesh2", &dead).await.unwrap();
        assert_eq!(stores.nodes.node_row_at(&row.name).await.unwrap(), Some(row.clone()), "the holder reads the mesh2 Node row it holds");
        assert!(stores.status.mesh_status_held_of("mesh2", &dead).await.unwrap(), "the holder reads the mesh2 status row it holds");
        let reason = "the seats of mesh1.admin.1 moved to mesh1 000000000002 by take-seats:b1";
        stores.apply(WriteCutOff::Set { reason: reason.into(), fabric_seat: true }).await;
        stores.apply(WriteCutOff::Committed { reason: reason.into(), fabric_seat: true }).await;
        assert_eq!(stores.nodes.node_row_at(&row.name).await.unwrap(), None, "a mesh2 Node row dropped at the commit is never read");
        assert!(!stores.status.mesh_status_held_of("mesh2", &dead).await.unwrap(), "a mesh2 status row dropped at the commit is never read");
        let published = pad.publish().await.unwrap();
        assert_eq!((published.written(), published.dropped(), published.held()), (0, 0, 0), "nothing the holder held as acting primary is published after the seat row: {published:?}");
        let mut on_root = Vec::new();
        let mut dirs = vec![d.join("store")];
        while let Some(dir) = dirs.pop() {
            for e in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                if e.path().is_dir() {
                    dirs.push(e.path());
                } else {
                    on_root.push(e.path().display().to_string());
                }
            }
        }
        assert!(on_root.is_empty(), "no row reached the store root: {on_root:?}");
        let _ = std::fs::remove_dir_all(&d);
    });
}

// ---- No caller check inside the mesh (node-rpc.md §14) ----

/// An admin endpoint no view of the fabric holds a birth for, reaching `to`: the caller a receiver
/// never heard of.
async fn stranger_to(to: &[&Admin]) -> Admin {
    let (stranger, _) = admin(&FabricId::mint(), "mesh9", "mesh9.admin.1", "000000000077", false).await;
    for a in to {
        stranger.resolver.insert(a.resolved.clone());
        assert!(a.door.topology.read().await.births().all(|n| n.endpoint_id.as_ref().map(|e| e.0.as_str()) != Some(stranger.resolved.endpoint_id.to_string().as_str())), "the stranger is in no birth of {}'s view", a.resolved.name);
    }
    stranger
}

/// CONTRACT (node-rpc.md §14: "Nothing inside the mesh checks who is calling"): a handover command
/// and a handover completion sent from an endpoint the receiver's view holds no birth for are each
/// decided on the births the payload names. The successor commits the seat at the next epoch and its
/// own completion confirms it at the incumbent; a completion from the stranger moves the incumbent's
/// seat record to the successor it names. The direction refusals stay, decided by the payload and
/// what the receiver holds: an incumbent birth the successor's view does not hold, or a successor
/// birth the incumbent's view does not hold, is `RejectedWrongBirth`. What must NOT happen:
/// `Unauthorized` naming the caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handover_command_and_completion_from_an_endpoint_no_view_holds_are_decided_on_the_births_they_name() {
    if crate::own_process::delegated(module_path!(), "a_handover_command_and_completion_from_an_endpoint_no_view_holds_are_decided_on_the_births_they_name") {
        return;
    }
    let spans = capture();
    let other_birth = |b: &HandoverBirth| HandoverBirth { incarnation: IncarnationId("another-birth".into()), ..b.clone() };
    // The command.
    let (f, _) = fabric(NodeStatus::ReadyForTraffic, false, true).await;
    let stranger = stranger_to(&[&f.incumbent, &f.successor]).await;
    let op = "fabric-primary-handover:stranger";
    let wrong_incumbent = Request::TakeFabricPrimary { fabric: "fabric1".into(), incumbent: other_birth(&birth(&f.incumbent)), successor: birth(&f.successor), expected_epoch: 7, operation: op.into() };
    assert_eq!(send(&stranger, &f.successor, wrong_incumbent).await, Reply::RejectedWrongBirth { role: "incumbent".into() });
    assert_eq!(send(&stranger, &f.successor, take(&f, 7, op)).await, Reply::Applied, "the command is decided on the births it names");
    until("the incumbent confirms the successor's commit", || f.incumbent.door.membership.seats().fabric() == Some(holder_of(&f.successor, 8))).await;
    assert_eq!(f.successor.door.membership.seats().fabric(), Some(holder_of(&f.successor, 8)));
    until("the incumbent's confirmation span", || spans.position(CONFIRMED).is_some()).await;
    // The completion.
    let (g, _) = fabric(NodeStatus::ReadyForTraffic, false, true).await;
    let stranger = stranger_to(&[&g.incumbent, &g.successor]).await;
    let wrong_successor = Request::FabricPrimaryTaken { fabric: "fabric1".into(), incumbent: birth(&g.incumbent), successor: other_birth(&birth(&g.successor)), expected_epoch: 7, committed_epoch: 8, operation: op.into() };
    assert_eq!(send(&stranger, &g.incumbent, wrong_successor).await, Reply::RejectedWrongBirth { role: "successor".into() });
    assert_eq!(g.incumbent.door.membership.seats().fabric(), Some(holder_of(&g.incumbent, 7)), "a refused completion moves nothing");
    assert_eq!(send(&stranger, &g.incumbent, taken(&g, 7, 8, op)).await, Reply::Applied, "the completion is decided on the births it names");
    assert_eq!(g.incumbent.door.membership.seats().fabric(), Some(holder_of(&g.successor, 8)));
    let reasons: Vec<String> = spans.named(REJECTED).iter().map(|f| f["reason"].clone()).collect();
    assert!(reasons.iter().all(|r| r == "wrong-birth"), "only the payload's births are refused: {reasons:?}");
    assert_eq!(spans.named(COMMITTED).len(), 1, "one commit, the successor's");
    assert_eq!(spans.named(CONFIRMED).len(), 2, "both incumbents confirmed");
}

/// CONTRACT (node-rpc.md §14; node-rpc-envelope.md "TakeSeats"): a TakeSeats call from an endpoint
/// the holder's view holds no birth for is decided on the successor it names: the executor the
/// attempt's Won claim names and the admin the election seats once the holder is gone. P commits both
/// seats to N in one row and answers SeatsCommitted. The direction refusals stay: a successor the Won
/// claim does not name is `RejectedNotEligible`, a successor birth P's view does not hold is
/// `RejectedWrongBirth`. What must NOT happen: `Unauthorized` naming the caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_take_seats_call_from_an_endpoint_the_holder_never_heard_of_is_decided_on_the_successor_it_names() {
    if crate::own_process::delegated(module_path!(), "a_take_seats_call_from_an_endpoint_the_holder_never_heard_of_is_decided_on_the_successor_it_names") {
        return;
    }
    let spans = capture();
    let f = seats_fabric().await;
    let stranger = stranger_to(&[&f.p]).await;
    let op = "replace-node:mesh1.admin.1";
    assert!(matches!(send(&stranger, &f.p, take_seats(&f.p, &f.m, 3, 7, "b1", op)).await, Reply::RejectedNotEligible { .. }), "the Won claim of b1 names N, not M");
    let mut wrong = take_seats(&f.p, &f.n, 3, 7, "b1", op);
    if let Request::TakeSeats { successor, .. } = &mut wrong {
        successor.incarnation = IncarnationId("another-birth".into());
    }
    assert_eq!(send(&stranger, &f.p, wrong).await, Reply::RejectedWrongBirth { role: "successor".into() });
    assert_eq!(send(&stranger, &f.p, take_seats(&f.p, &f.n, 3, 7, "b1", op)).await, Reply::SeatsCommitted { seats: committed_to(&f.n, 4, 8) }, "the call is decided on the successor it names");
    assert_eq!((f.p.door.membership.seats().mesh("mesh1"), f.p.door.membership.seats().fabric()), (Some(holder_of(&f.n, 4)), Some(holder_of(&f.n, 8))), "P moved both seats to N");
    assert_eq!(spans.named(SEATS_COMMITTED).len(), 1);
    let reasons: Vec<String> = spans.named(SEATS_REJECTED).iter().map(|f| f["reason"].clone()).collect();
    assert!(reasons.iter().all(|r| r == "not-the-claimed-executor" || r == "wrong-birth"), "only the payload's successor is refused: {reasons:?}");
    let _ = (&f.m, &f.x);
}

/// CONTRACT (node-rpc-envelope.md, op 0x21: "Epoch overflow cannot commit and is refused by name with
/// a typed refusal"; TakeSeats: the row moves every seat "at each seat's next epoch"): a seat whose
/// epoch is `u64::MAX` cannot advance. A TakeFabricPrimary naming the fabric seat at `u64::MAX`, the
/// FabricPrimaryTaken completing that transfer, and a TakeSeats naming the mesh1 mesh-primary seat at
/// `u64::MAX` are each refused `RejectedEpochOverflow { seat, epoch: u64::MAX }`, the receiver's reject
/// span naming the reason `epoch-overflow`, the seat and the epoch. Nothing commits: no seat row, no
/// seat-transfer row, no write cut-off, no committed span, every seat where it was. What must NOT
/// happen: `NotReady` (a door that is not serving yet), or any seat moved.
#[test]
fn a_seat_epoch_at_its_maximum_is_refused_by_name_and_nothing_commits() {
    let cap = crate::mesh_leave_resume::capture();
    let op = "fabric-primary-handover:epoch-overflow";
    let seats_op = "drain-node:mesh1.admin.1";
    let (replies, trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.fabric.reject.via-epoch-overflow");
        let trace_id = crate::mesh_leave_resume::trace_id(&root);
        let replies = tracing::Instrument::instrument(epoch_overflow(op, seats_op), root).await;
        (replies, trace_id)
    });
    let spans = cap.spans();
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cells/fabric_handover/epoch_overflow");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    println!("trace_id={trace_id} spans={}", dir.join("spans.json").display());
    let max = WireSeat::FabricPrimary;
    let mesh1 = WireSeat::MeshPrimary { mesh: "mesh1".into() };
    assert_eq!(
        replies,
        vec![
            ("take-fabric-primary", Reply::RejectedEpochOverflow { seat: max.clone(), epoch: u64::MAX }),
            ("fabric-primary-taken", Reply::RejectedEpochOverflow { seat: max, epoch: u64::MAX }),
            ("take-seats", Reply::RejectedEpochOverflow { seat: mesh1, epoch: u64::MAX }),
        ],
        "each call naming a seat at u64::MAX is refused by name"
    );
    let ours = |name: &str| spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str()).cloned().collect::<Vec<_>>();
    let refused = ours(REJECTED);
    for (call, side) in [("take", "successor"), ("taken", "incumbent")] {
        let r = refused.iter().find(|s| s["attributes"]["call"] == call && s["attributes"]["side"] == side).unwrap_or_else(|| panic!("no {call} reject span at the {side}: {refused:?}"));
        assert_eq!((r["attributes"]["reason"].as_str(), r["attributes"]["expected_epoch"].as_str()), (Some("epoch-overflow"), Some("18446744073709551615")), "{r:?}");
        assert!(r["attributes"]["detail"].as_str().is_some_and(|d| d.contains("fabric-primary") && d.contains("18446744073709551615")), "the detail names the seat and its epoch: {r:?}");
    }
    let seats_refused = ours(SEATS_REJECTED);
    assert_eq!(seats_refused.len(), 1, "one refusal of the seat transfer: {seats_refused:?}");
    assert_eq!(seats_refused[0]["attributes"]["reason"], "epoch-overflow");
    assert!(seats_refused[0]["attributes"]["detail"].as_str().is_some_and(|d| d.contains("mesh-primary") && d.contains("18446744073709551615")), "{seats_refused:?}");
    for name in [COMMITTED, CONFIRMED, SEATS_COMMITTED, "rdm.node_admin.seat.update.via-writes-drained"] {
        assert!(ours(name).is_empty(), "nothing commits: {name} {:?}", ours(name));
    }
}

/// The three calls of [`a_seat_epoch_at_its_maximum_is_refused_by_name_and_nothing_commits`], each
/// reply named by its call, and the state each receiver holds after it.
async fn epoch_overflow(op: &'static str, seats_op: &'static str) -> Vec<(&'static str, Reply)> {
    let mut replies = Vec::new();
    // The fabric seat at u64::MAX, held by mesh1's admin, on both admins.
    let (f, _) = fabric(NodeStatus::ReadyForTraffic, false, true).await;
    for a in [&f.incumbent, &f.successor] {
        a.door.membership.seats().take(Seat::FabricPrimary, &holder_of(&f.incumbent, u64::MAX));
    }
    replies.push(("take-fabric-primary", send(&f.incumbent, &f.successor, take(&f, u64::MAX, op)).await));
    replies.push(("fabric-primary-taken", send(&f.successor, &f.incumbent, taken(&f, u64::MAX, u64::MAX, op)).await));
    for a in [&f.incumbent, &f.successor] {
        assert_eq!(a.door.membership.seats().fabric(), Some(holder_of(&f.incumbent, u64::MAX)), "{}: the fabric seat did not move", a.resolved.name);
        assert!(a.storage.seats().await.unwrap().is_empty(), "{}: no seat row is written", a.resolved.name);
    }
    // P holds the mesh1 mesh-primary seat at u64::MAX; N, the Won claim's executor, takes the seats.
    let s = seats_fabric().await;
    s.p.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&s.p, u64::MAX));
    replies.push(("take-seats", send(&s.n, &s.p, take_seats(&s.p, &s.n, u64::MAX, 7, "b1", seats_op)).await));
    assert!(s.p.storage.seat_transfers().await.unwrap().is_empty(), "no seat-transfer row is written");
    assert!(s.p.cut_offs.lock().unwrap().is_empty(), "no write is cut off for a transfer that cannot commit: {:?}", s.p.cut_offs.lock().unwrap());
    assert_eq!((s.p.door.membership.seats().mesh("mesh1"), s.p.door.membership.seats().fabric()), (Some(holder_of(&s.p, u64::MAX)), Some(holder_of(&s.p, 7))), "P's seats did not move");
    let _ = (&s.m, &s.x);
    replies
}

/// The cell's spans, written to `target/cells/fabric_handover/<cell>/spans.json`; its trace id and
/// that path are printed.
fn keep_cell(cell: &str, trace_id: &str, spans: &[serde_json::Value]) {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cells/fabric_handover").join(cell);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(spans).unwrap()).unwrap();
    println!("cell={cell} trace_id={trace_id} spans={}", dir.join("spans.json").display());
}

/// The spans named `name` in the cell's own trace.
fn in_trace(spans: &[serde_json::Value], trace_id: &str, name: &str) -> Vec<serde_json::Value> {
    spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id).cloned().collect()
}

/// The `(source, op, cause)` of a `RejectedSourceFailed`; any other reply panics, naming it.
fn source_failed(r: &Reply) -> (String, String, String) {
    match r {
        Reply::RejectedSourceFailed { source, op, cause } => (source.clone(), op.clone(), cause.clone()),
        other => panic!("refused by name, RejectedSourceFailed naming what failed, the operation and the cause: {other:?}"),
    }
}

/// The `(reason, source, op, cause)` a reject span carries.
fn source_fields(span: &serde_json::Value) -> (String, String, String, String) {
    let a = |k: &str| span["attributes"][k].as_str().unwrap_or_default().to_string();
    (a("reason"), a("source"), a("op"), a("cause"))
}

/// CONTRACT (node-rpc-envelope.md, TakeSeats: "The commit is one durable row holding every seat P
/// transfers"; "Each seat's `expected_epoch` must be the seat's current epoch, else
/// RejectedStaleSeat"): a TakeSeats that leaves out a seat the holder holds names no epoch for it,
/// stale or current. P holds the mesh1 mesh-primary seat at 3 and the fabric seat at 7; N names the
/// mesh1 seat alone. It is refused `RejectedSeatOmitted { seat: FabricPrimary, current: 7 }`, the
/// holder's reject span naming the reason `seat-omitted`, the seat and the epoch it holds. Nothing
/// commits: no seat-transfer row, no write cut-off, both seats where they were. What must NOT
/// happen: `RejectedStaleSeat { expected: 0, .. }`, a 0 standing for "not named".
#[test]
fn a_take_seats_that_leaves_out_a_held_seat_is_refused_by_name_and_nothing_commits() {
    let cap = crate::mesh_leave_resume::capture();
    let (reply, trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.seat.reject.via-seat-omitted");
        let trace_id = crate::mesh_leave_resume::trace_id(&root);
        let reply = tracing::Instrument::instrument(
            async {
                let s = seats_fabric().await;
                let mut req = take_seats(&s.p, &s.n, 3, 7, "b1", "drain-node:mesh1.admin.1");
                if let Request::TakeSeats { seats, .. } = &mut req {
                    seats.retain(|e| e.seat != WireSeat::FabricPrimary);
                }
                let reply = send(&s.n, &s.p, req).await;
                assert!(s.p.storage.seat_transfers().await.unwrap().is_empty(), "no seat-transfer row is written");
                assert!(s.p.cut_offs.lock().unwrap().is_empty(), "no write is cut off: {:?}", s.p.cut_offs.lock().unwrap());
                assert_eq!((s.p.door.membership.seats().mesh("mesh1"), s.p.door.membership.seats().fabric()), (Some(holder_of(&s.p, 3)), Some(holder_of(&s.p, 7))), "P's seats did not move");
                let _ = (&s.m, &s.x);
                reply
            },
            root,
        )
        .await;
        (reply, trace_id)
    });
    let spans = cap.spans();
    keep_cell("seat_omitted", &trace_id, &spans);
    assert_eq!(reply, Reply::RejectedSeatOmitted { seat: WireSeat::FabricPrimary, current: 7 }, "the seat left out is named with the epoch the holder holds it at");
    let refused = in_trace(&spans, &trace_id, SEATS_REJECTED);
    let holder = refused.iter().find(|s| s["attributes"]["side"] == "holder").unwrap_or_else(|| panic!("no holder reject span: {refused:?}"));
    let a = |k: &str| holder["attributes"][k].as_str().unwrap_or_default().to_string();
    assert_eq!((a("reason"), a("seat"), a("current")), ("seat-omitted".to_string(), "fabric-primary".to_string(), "7".to_string()), "{holder:?}");
    assert!(in_trace(&spans, &trace_id, SEATS_COMMITTED).is_empty(), "nothing commits");
}

/// mesh1's admin holds the fabric seat at 7 and mesh2's admin is the only other mesh-primary, as
/// [`fabric`], each over its own [`FailingStorage`] (returned incumbent's, successor's). With
/// `successor_holds_fabric_seat` false the successor holds no fabric seat record.
async fn failing_pair(successor_holds_fabric_seat: bool) -> (Pair, Arc<FailingStorage>, Arc<FailingStorage>) {
    failing_pair_holding(true, successor_holds_fabric_seat).await
}

/// [`failing_pair`] where each admin holds the fabric seat record (the incumbent at 7) only when its
/// flag says so: an admin whose flag is false holds no fabric seat record, none was ever written to
/// its seat records.
async fn failing_pair_holding(incumbent_holds_fabric_seat: bool, successor_holds_fabric_seat: bool) -> (Pair, Arc<FailingStorage>, Arc<FailingStorage>) {
    let fabric_id = FabricId::mint();
    let (incumbent_store, successor_store) = (Arc::new(FailingStorage::default()), Arc::new(FailingStorage::default()));
    let (incumbent, _) = admin_over(&fabric_id, "mesh1", "mesh1.admin.1", "000000000009", true, incumbent_store.clone()).await;
    let (successor, _) = admin_over(&fabric_id, "mesh2", "mesh2.admin.1", "000000000005", true, successor_store.clone()).await;
    let nodes = vec![node_of(&incumbent, NodeStatus::ReadyForTraffic, true), node_of(&successor, NodeStatus::ReadyForTraffic, false)];
    for a in [&incumbent, &successor] {
        *a.door.topology.write().await = view(nodes.clone());
        for p in [&incumbent, &successor] {
            a.resolver.insert(p.resolved.clone());
        }
        let holds = if a.resolved.name.mesh == "mesh1" { incumbent_holds_fabric_seat } else { successor_holds_fabric_seat };
        if holds {
            a.door.membership.seats().take(Seat::FabricPrimary, &holder_of(&incumbent, 7));
        }
        a.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&incumbent, 1));
        a.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&successor, 1));
    }
    (Pair { incumbent, successor, mesh1_id: MeshId::mint() }, incumbent_store, successor_store)
}

/// CONTRACT (CLAUDE.md error rule: "Name what failed and where ... 'Never written' and 'could not be
/// read' are different errors"; node-rpc-envelope.md "Node RPC readiness gate": NotReady means "the
/// door or the handler cannot serve this call yet"): a handover call whose commit needs a
/// `fabric.storage` write that fails is refused `RejectedSourceFailed` naming what failed, the
/// operation and the cause, never NotReady. Three calls, each on a fresh pair: a TakeFabricPrimary
/// whose successor's store refuses the handover row at Committed(8); one whose successor's store
/// refuses the fabric seat row; and a FabricPrimaryTaken whose incumbent's store refuses the handover
/// row at Confirmed(8). Each is the receiver's reject span with reason `source-failed` and the same
/// three fields; the successor's fabric seat never moves for a commit it could not make durable.
#[test]
fn a_handover_call_whose_store_refuses_a_row_is_refused_by_name() {
    let cap = crate::mesh_leave_resume::capture();
    let op = "fabric-primary-handover:source-failed";
    let (replies, trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.fabric.reject.via-source-failed");
        let trace_id = crate::mesh_leave_resume::trace_id(&root);
        let replies = tracing::Instrument::instrument(
            async {
                let mut replies = Vec::new();
                let (f, _, successor_store) = failing_pair(true).await;
                successor_store.failing("put_handover");
                replies.push(("handover-row", send(&f.incumbent, &f.successor, take(&f, 7, op)).await));
                assert_eq!(f.successor.door.membership.seats().fabric(), Some(holder_of(&f.incumbent, 7)), "the successor's fabric seat did not move");
                let (f, _, successor_store) = failing_pair(true).await;
                successor_store.failing("put_seat");
                replies.push(("seat-row", send(&f.incumbent, &f.successor, take(&f, 7, op)).await));
                assert_eq!(f.successor.door.membership.seats().fabric(), Some(holder_of(&f.incumbent, 7)), "the successor's fabric seat did not move");
                let (f, incumbent_store, _) = failing_pair(true).await;
                incumbent_store.failing("put_handover");
                replies.push(("confirmed-row", send(&f.successor, &f.incumbent, taken(&f, 7, 8, op)).await));
                replies
            },
            root,
        )
        .await;
        (replies, trace_id)
    });
    let spans = cap.spans();
    keep_cell("source_failed", &trace_id, &spans);
    let fields: BTreeMap<&str, (String, String, String)> = replies.iter().map(|(case, r)| (*case, source_failed(r))).collect();
    let (source, op_, cause) = &fields["handover-row"];
    assert!(source.contains("fabric.storage") && source.contains("mesh2.admin.1") && source.contains("handover row") && op_.contains("put") && op_.contains("Committed") && cause.contains(PLANTED), "{:?}", fields["handover-row"]);
    let (source, op_, cause) = &fields["seat-row"];
    assert!(source.contains("fabric.storage") && source.contains("mesh2.admin.1") && source.contains("seat row") && op_.contains("put") && op_.contains("fabric-primary") && op_.contains('8') && cause.contains(PLANTED), "{:?}", fields["seat-row"]);
    let (source, op_, cause) = &fields["confirmed-row"];
    assert!(source.contains("fabric.storage") && source.contains("mesh1.admin.1") && source.contains("handover row") && op_.contains("put") && op_.contains("Confirmed") && cause.contains(PLANTED), "{:?}", fields["confirmed-row"]);
    let refused = in_trace(&spans, &trace_id, REJECTED);
    for (case, call, side) in [("handover-row", "take", "successor"), ("seat-row", "take", "successor"), ("confirmed-row", "taken", "incumbent")] {
        let (source, op_, cause) = fields[case].clone();
        let named = refused.iter().filter(|s| s["attributes"]["call"] == call && s["attributes"]["side"] == side).map(source_fields).collect::<Vec<_>>();
        assert!(named.contains(&("source-failed".to_string(), source, op_, cause)), "{case}: the {side}'s reject span names the reason source-failed and the three fields: {named:?}");
    }
    assert!(in_trace(&spans, &trace_id, COMMITTED).is_empty(), "no commit is announced for a row that was not made durable");
}

/// CONTRACT (CLAUDE.md error rule: "Distinct facts get distinct arms and distinct spans. 'Never
/// written' and 'could not be read' are different errors, and neither is an empty answer"): a
/// handover call to an admin that holds no fabric seat record, none ever written to its seat records,
/// is refused `RejectedRecordAbsent` naming the record and the admin whose records hold no such
/// record. Two calls, each on a fresh pair: a TakeFabricPrimary to a successor that holds no fabric
/// seat record (it cannot compare the epoch the command names), and a FabricPrimaryTaken to an
/// incumbent that recorded no handover and holds no fabric seat record (it cannot compare the seat
/// it held with the completion). Each is the receiver's reject span with reason `record-absent` and
/// the `record` and `holder` it names. Nothing moves and nothing is written: no seat, no handover
/// row. What must NOT happen: `RejectedSourceFailed` (a read that failed: nothing failed here),
/// `RejectedNotAuthority` (a seat record naming another holder: there is none), or NotReady.
#[test]
fn a_handover_call_to_an_admin_that_holds_no_fabric_seat_record_is_refused_as_never_written() {
    let cap = crate::mesh_leave_resume::capture();
    let op = "fabric-primary-handover:record-absent";
    let (replies, trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.fabric.reject.via-record-absent");
        let trace_id = crate::mesh_leave_resume::trace_id(&root);
        let replies = tracing::Instrument::instrument(
            async {
                let mut replies = Vec::new();
                let (f, _, successor_store) = failing_pair_holding(true, false).await;
                replies.push(("take", send(&f.incumbent, &f.successor, take(&f, 7, op)).await));
                assert_eq!(f.successor.door.membership.seats().fabric(), None, "the successor took no fabric seat");
                assert!(successor_store.handovers().await.unwrap().is_empty() && successor_store.seats().await.unwrap().is_empty(), "the successor wrote no handover row and no seat row");
                let (f, incumbent_store, _) = failing_pair_holding(false, true).await;
                replies.push(("taken", send(&f.successor, &f.incumbent, taken(&f, 7, 8, op)).await));
                assert_eq!(f.incumbent.door.membership.seats().fabric(), None, "the incumbent moved no fabric seat");
                assert!(incumbent_store.handovers().await.unwrap().is_empty(), "the incumbent wrote no handover row");
                replies
            },
            root,
        )
        .await;
        (replies, trace_id)
    });
    let spans = cap.spans();
    keep_cell("record_absent", &trace_id, &spans);
    assert_eq!(
        replies,
        vec![
            ("take", Reply::RejectedRecordAbsent { record: "the fabric-primary seat record of fabric1".into(), holder: "mesh2.admin.1".into() }),
            ("taken", Reply::RejectedRecordAbsent { record: "the fabric-primary seat record of fabric1".into(), holder: "mesh1.admin.1".into() }),
        ],
        "each call is refused naming the record never written and the admin whose records hold none"
    );
    let refused = in_trace(&spans, &trace_id, REJECTED);
    for (call, side, holder) in [("take", "successor", "mesh2.admin.1"), ("taken", "incumbent", "mesh1.admin.1")] {
        let r = refused.iter().find(|s| s["attributes"]["call"] == call && s["attributes"]["side"] == side).unwrap_or_else(|| panic!("no {call} reject span at the {side}: {refused:?}"));
        let a = |k: &str| r["attributes"][k].as_str().unwrap_or_default().to_string();
        assert_eq!((a("reason"), a("record"), a("holder")), ("record-absent".to_string(), "the fabric-primary seat record of fabric1".to_string(), holder.to_string()), "{r:?}");
        assert!(a("detail").contains("never written") && a("detail").contains('7'), "the detail says the record was never written and names the epoch it could not be compared with: {r:?}");
        assert_eq!((a("source"), a("op"), a("cause")), (String::new(), String::new(), String::new()), "an absent record names no failed source: {r:?}");
    }
    for name in [COMMITTED, CONFIRMED, ANNOUNCED] {
        assert!(in_trace(&spans, &trace_id, name).is_empty(), "nothing commits: {name}");
    }
}

/// The seats fabric with each admin over its own [`FailingStorage`]; P's is returned.
async fn failing_seats() -> (Seats, Arc<FailingStorage>) {
    let made: Arc<Mutex<Vec<Arc<FailingStorage>>>> = Arc::default();
    let s = seats_fabric_with({
        let made = made.clone();
        move || {
            let store = Arc::new(FailingStorage::default());
            made.lock().unwrap().push(store.clone());
            async move { store as Arc<dyn FabricStorage> }
        }
    })
    .await;
    let p_store = made.lock().unwrap()[0].clone();
    (s, p_store)
}

/// CONTRACT (CLAUDE.md error rule; node-rpc-envelope.md TakeSeats: N "is the executor the attempt's
/// Won claim names ... else RejectedNotEligible"): a TakeSeats whose checks read something that fails
/// is refused `RejectedSourceFailed` naming it, never NotReady, and a claim the fabric-primary never
/// wrote is no claim. Four calls, each on a fresh seats fabric: P's store cannot read the
/// operation's seat-transfer row; P cannot read the fabric-primary's Build facts for the claim
/// (unreached); the fabric-primary holds no fact of the Build (never written), answered
/// `RejectedNotEligible` "no Won claim"; P's store refuses the seat-transfer row, and the write
/// cut-off it set is lifted. Each is P's reject span (`source-failed` with the three fields, or
/// `not-the-claimed-executor`); no seat moves.
#[test]
fn a_take_seats_whose_row_or_claim_read_fails_or_whose_row_put_fails_is_refused_by_name() {
    let cap = crate::mesh_leave_resume::capture();
    let seats_op = "drain-node:mesh1.admin.1";
    let (replies, trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.seat.reject.via-source-failed");
        let trace_id = crate::mesh_leave_resume::trace_id(&root);
        let replies = tracing::Instrument::instrument(
            async {
                let mut replies = Vec::new();
                let unmoved = |s: &Seats| (s.p.door.membership.seats().mesh("mesh1"), s.p.door.membership.seats().fabric()) == (Some(holder_of(&s.p, 3)), Some(holder_of(&s.p, 7)));
                let (s, p_store) = failing_seats().await;
                p_store.failing("seat_transfer");
                replies.push(("row-read", send(&s.n, &s.p, take_seats(&s.p, &s.n, 3, 7, "b1", seats_op)).await));
                assert!(s.p.cut_offs.lock().unwrap().is_empty() && unmoved(&s), "nothing is cut off or moved");
                let (s, _) = failing_seats().await;
                s.p.claims.1.lock().unwrap().insert(("b1".into(), 1), FetchFailure::Unreached("no path to the fabric-primary".into()));
                replies.push(("claim-read", send(&s.n, &s.p, take_seats(&s.p, &s.n, 3, 7, "b1", seats_op)).await));
                assert!(s.p.cut_offs.lock().unwrap().is_empty() && unmoved(&s), "nothing is cut off or moved");
                let (s, _) = failing_seats().await;
                s.p.claims.1.lock().unwrap().insert(("b1".into(), 1), FetchFailure::UnknownBuild("the responder holds no fact of b1".into()));
                replies.push(("claim-never-written", send(&s.n, &s.p, take_seats(&s.p, &s.n, 3, 7, "b1", seats_op)).await));
                assert!(s.p.cut_offs.lock().unwrap().is_empty() && unmoved(&s), "nothing is cut off or moved");
                let (s, p_store) = failing_seats().await;
                p_store.failing("put_seat_transfer");
                replies.push(("row-put", send(&s.n, &s.p, take_seats(&s.p, &s.n, 3, 7, "b1", seats_op)).await));
                assert!(matches!(s.p.cut_offs.lock().unwrap().as_slice(), [WriteCutOff::Set { .. }, WriteCutOff::Lift]), "the cut-off is lifted when the row was not written: {:?}", s.p.cut_offs.lock().unwrap());
                assert!(unmoved(&s), "no seat moves");
                replies
            },
            root,
        )
        .await;
        (replies, trace_id)
    });
    let spans = cap.spans();
    keep_cell("take_seats_source_failed", &trace_id, &spans);
    let reply = |case: &str| replies.iter().find(|(c, _)| *c == case).map(|(_, r)| r.clone()).unwrap();
    let (source, op_, cause) = source_failed(&reply("row-read"));
    assert!(source.contains("fabric.storage") && source.contains("mesh1.admin.1") && source.contains("seat-transfer row") && op_.contains("read") && op_.contains(seats_op) && cause.contains(PLANTED), "{source} | {op_} | {cause}");
    let (source, op_, cause) = source_failed(&reply("claim-read"));
    assert!(source.contains("Build log of the fabric-primary") && op_.contains("fetch-build-facts b1") && cause.contains("unreached"), "{source} | {op_} | {cause}");
    assert!(matches!(reply("claim-never-written"), Reply::RejectedNotEligible { ref reason } if reason.contains("no Won claim of attempt 1 of Build b1")), "a claim never written is no claim: {:?}", reply("claim-never-written"));
    let (source, op_, cause) = source_failed(&reply("row-put"));
    assert!(source.contains("fabric.storage") && source.contains("mesh1.admin.1") && source.contains("seat-transfer row") && op_.contains("put") && op_.contains(seats_op) && cause.contains(PLANTED), "{source} | {op_} | {cause}");
    let named = in_trace(&spans, &trace_id, SEATS_REJECTED).iter().filter(|s| s["attributes"]["side"] == "holder").map(source_fields).collect::<Vec<_>>();
    for case in ["row-read", "claim-read", "row-put"] {
        let (source, op_, cause) = source_failed(&reply(case));
        assert!(named.contains(&("source-failed".to_string(), source, op_, cause)), "{case}: P's reject span names the reason source-failed and the three fields: {named:?}");
    }
    assert!(named.iter().any(|(reason, ..)| reason == "not-the-claimed-executor"), "a claim never written is refused as no claim: {named:?}");
    assert!(in_trace(&spans, &trace_id, SEATS_COMMITTED).is_empty(), "nothing commits");
}
