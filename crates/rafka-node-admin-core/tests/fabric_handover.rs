//! node.primary.update and node.fabric-primary.update (the approved proposal
//! docs/plans/proposal/node-primary-update-fabric-mesh-and-node-type.md, Part C; R-MT1) between
//! in-process node-admins over real Node RPC on op `0x21`. The holder of a seat is the row writer of
//! its node-admin cohort: it pre-checks its successor, says "primary updating", cuts off its seat's
//! writes (every issued one finished), writes the ONE committed row, then calls `PrimaryUpdate` on the successor, which
//! installs from the row and answers `node.primary.updated`. Each cell reads the spans the update
//! emitted, in the order they happened.
// @feature: node-lifecycle

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{FabricId, IncarnationId, MeshId, NodeId, Seat, SeatHolder};
use rafka_mesh_transport::membership::{Backbone, Membership};
use rafka_node_admin_core::admin::Records;
use rafka_node_admin_core::election::{resolve_with, Incumbency};
use rafka_node_admin_core::build_facts_read::{BuildFactsDoor, BuildFactsSlot};
use rafka_node_admin_core::build_op::{BuildDoor, BuildSlot};
use rafka_node_admin_core::fabric_handover::{update_operation, HandoverDoor, HandoverSlot, Moving, ProjectView, WriteCutOff};
use rafka_node_admin_core::fabric_storage::{FabricClaim, FabricIdentity, FabricNameHeld, FabricShutdown, FabricStorage, FabricStorageError, FileFabricStorage, MemoryFabricStorage, SeatTransferRow};
use rafka_node_admin_core::model::{EndpointId, Fabric, Node, NodeStatus, ProviderKind, ScopeStatus};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, Found, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::status_storage::MemoryStatusStorage;
use rafka_node_admin_core::storage::MemoryNodesStorage;
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use rafka_node_rpc_contract::handover::{CommittedSeat, HandoverBirth, NodePrimaryUpdate, NodePrimaryUpdateReply as Reply, NodePrimaryUpdateRequest as Request, Seat as WireSeat, SeatEpoch, SeatUpdate};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use tracing_subscriber::layer::SubscriberExt;

/// Every span, in the order it was created: name and fields (the ones recorded after it was created
/// too), and when it was created and closed.
#[derive(Clone, Default)]
struct Spans(Arc<Mutex<Vec<Captured>>>);

/// One captured span.
struct Captured {
    name: String,
    fields: BTreeMap<String, String>,
    created: std::time::Instant,
    closed: Option<std::time::Instant>,
}

/// Where a span sits in its [`Spans`], kept in the span's extensions so a later record or its close
/// finds it.
struct At(usize);

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
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = BTreeMap::new();
        attrs.record(&mut Fields(&mut fields));
        let at = {
            let mut all = self.0.lock().unwrap();
            all.push(Captured { name: attrs.metadata().name().into(), fields, created: std::time::Instant::now(), closed: None });
            all.len() - 1
        };
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(At(at));
        }
    }
    fn on_record(&self, id: &tracing::span::Id, values: &tracing::span::Record<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let Some(at) = ctx.span(id).and_then(|s| s.extensions().get::<At>().map(|a| a.0)) else { return };
        if let Some(c) = self.0.lock().unwrap().get_mut(at) {
            values.record(&mut Fields(&mut c.fields));
        }
    }
    fn on_close(&self, id: tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let Some(at) = ctx.span(&id).and_then(|s| s.extensions().get::<At>().map(|a| a.0)) else { return };
        if let Some(c) = self.0.lock().unwrap().get_mut(at) {
            c.closed = Some(std::time::Instant::now());
        }
    }
}

impl Spans {
    fn names(&self) -> Vec<String> {
        self.0.lock().unwrap().iter().map(|c| c.name.clone()).collect()
    }
    fn named(&self, name: &str) -> Vec<BTreeMap<String, String>> {
        self.0.lock().unwrap().iter().filter(|c| c.name == name).map(|c| c.fields.clone()).collect()
    }
    /// Every span as one line: milliseconds from the first span to its creation and to its close,
    /// its name and its fields.
    fn lines(&self) -> Vec<String> {
        let all = self.0.lock().unwrap();
        let Some(t0) = all.first().map(|c| c.created) else { return vec![] };
        let ms = |t: std::time::Instant| t.saturating_duration_since(t0).as_millis();
        all.iter().map(|c| format!("{:>7}ms {:>7} {} {:?}", ms(c.created), c.closed.map(|t| format!("{}ms", ms(t))).unwrap_or_else(|| "open".into()), c.name, c.fields)).collect()
    }
    fn position(&self, name: &str) -> Option<usize> {
        self.names().iter().position(|n| n == name)
    }
}

/// The holder's write cut-off, every issued write finished (C1 step 4).
const CUT_OFF: &str = "rdm.node_admin.seat.update.via-writes-cut-off";
/// The holder's one committed row (C1 step 5).
const ROW_WRITTEN: &str = "rdm.node_admin.seat.update.via-seat-row-written";
/// The successor installed from the row and answered node.primary.updated (C1 step 6).
const UPDATED: &str = "rdm.node_admin.seat.update.via-primary-updated";
/// The commander installed the committed seats from the answer.
const INSTALLED: &str = "rdm.node_admin.seat.update.via-seats-installed";
/// A refusal of the primitive, at the holder or the successor.
const REJECTED: &str = "rdm.node_admin.seat.reject.via-primary-update";
/// The look at the successor's exact birth after a call that did not answer node.primary.updated:
/// node-recovery's tickle ladder, then at most one retry of the call (C1 failure contract).
const LADDER: &str = "rdm.node_admin.seat.resolve.via-successor-ladder";
/// "primary updating", said by the holder before its cut-off (C1 step 3).
const ANNOUNCE_UPDATING: &str = "rdm.mesh.seat.update.via-announce-primary-updating";
/// "primary updated", said by the holder once the successor answered node.primary.updated (C1 step 7;
/// R-MT1: "NA2 updates itself and answers node.primary.updated; NA1 gossips that the primary updated to NA2").
const ANNOUNCE_UPDATED: &str = "rdm.mesh.seat.update.via-announce-primary-updated";
/// The resuming executor acknowledged a committed row before an operation on its holder (C1 failure
/// contract: "node.primary.updated answered to the resuming executor's call, never a read alone").
const ROW_ACKNOWLEDGED: &str = "rdm.node_admin.seat.update.via-row-acknowledged";
/// One Node RPC call at its caller: its target, op, outcome, reason and elapsed time.
const CALL: &str = "rdm.node_rpc.request.update.via-call";
/// The retired successor-side row writer: no span of it may appear.
const SEATS_COMMITTED: &str = "rdm.node_admin.seat.update.via-seats-committed";

fn view(nodes: Vec<Node>) -> Topology {
    Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process }, meshes: vec![], nodes }
}

struct Admin {
    door: Arc<HandoverDoor>,
    client: Arc<NodeRpcClient>,
    resolver: Arc<StaticResolver>,
    resolved: ResolvedNode,
    storage: Arc<dyn FabricStorage>,
    /// The status rows on the fabric's store root this admin's records fold and point-read (set at
    /// birth, as the admin sets its own before its door fills): one store shared by every admin of a
    /// fabric, its rows keyed by the mesh each names (R14).
    status: Arc<MemoryStatusStorage>,
    /// Every write cut-off the holder's SeatTransfer step applied, in order.
    cut_offs: Arc<Mutex<Vec<WriteCutOff>>>,
    /// Holds the cut-off open while `false`: a write still issued when the step cuts off.
    writes_finish: tokio::sync::watch::Sender<bool>,
    /// Holds the admin's view projection while `false`: an admin that wrote or took a seat and has
    /// not answered yet (what a process that dies there leaves).
    view_open: tokio::sync::watch::Sender<bool>,
    /// The admin's Node RPC server: a cell stops it taking new calls as node.connections.delete does.
    server: rafka_node_rpc::NodeRpcServer,
    /// The admin's `FetchBuildFacts` door (op `0x1F`): `NotReady` until a cell fills it.
    facts_slot: BuildFactsSlot,
    /// The admin's Build door (op `0x20`): `NotReady` until a cell fills it.
    build_slot: BuildSlot,
    /// A door set here fills the admin's handover slot (op `0x21`, `NotReady` while empty) as its
    /// server answers a ping.
    ready_on_ping: Arc<OnceLock<Arc<HandoverDoor>>>,
    /// The provider the admin's tickle ladder inspects a runtime through.
    provider: Arc<Inspected>,
    /// The admin's nodes.storage: the runtime row of each birth its ladder's provider rung reads.
    nodes: Arc<MemoryNodesStorage>,
    _router: Router,
}

/// The error a [`FailingStorage`] answers for a method it is told to fail.
const PLANTED: &str = "planted: the disk refused";

/// A `fabric.storage` held in memory that fails each method named in `fail` with an Io error naming
/// the method: what a store whose disk refuses answers. `put_open` holds a seat-transfer row's put
/// while `false`: a write on its way to the disk.
struct FailingStorage {
    inner: MemoryFabricStorage,
    fail: Mutex<std::collections::BTreeSet<&'static str>>,
    put_open: tokio::sync::watch::Sender<bool>,
    /// How many seat-transfer row puts reached the store.
    puts: std::sync::atomic::AtomicUsize,
}

impl Default for FailingStorage {
    fn default() -> Self {
        Self { inner: MemoryFabricStorage::new(), fail: Mutex::default(), put_open: tokio::sync::watch::channel(true).0, puts: Default::default() }
    }
}

impl FailingStorage {
    fn failing(&self, method: &'static str) {
        self.fail.lock().unwrap().insert(method);
    }
    fn mended(&self, method: &'static str) {
        self.fail.lock().unwrap().remove(method);
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
    async fn fabric(&self) -> Result<Option<FabricIdentity>, FabricStorageError> {
        self.check("fabric")?;
        self.inner.fabric().await
    }
    async fn claim_identity(&self, identity: &FabricIdentity) -> Result<Result<FabricClaim, FabricNameHeld>, FabricStorageError> {
        self.check("claim_identity")?;
        self.inner.claim_identity(identity).await
    }
    async fn release_name(&self, identity: &FabricIdentity) -> Result<bool, FabricStorageError> {
        self.inner.release_name(identity).await
    }
    async fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError> {
        self.check("shutdown")?;
        self.inner.shutdown().await
    }
    async fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError> {
        self.check("put_shutdown")?;
        self.inner.put_shutdown(shutdown).await
    }
    async fn put_seat_transfer(&self, row: &SeatTransferRow) -> Result<(), FabricStorageError> {
        self.puts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _ = self.put_open.subscribe().wait_for(|open| *open).await;
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

/// mesh1's admin (the incumbent) holds the fabric seat at epoch 7 and mesh2's admin (the successor)
/// is the only other mesh-primary.
struct Pair {
    incumbent: Admin,
    successor: Admin,
}

/// The deployment provider an admin of a cell inspects runtimes through: it launched nothing here,
/// and it answers a runtime exited when the cell names its deployment in `exited`, running otherwise.
#[derive(Default)]
struct Inspected {
    exited: Mutex<std::collections::BTreeSet<String>>,
}

#[async_trait::async_trait]
impl DeploymentProvider for Inspected {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        "fabric-handover".into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn { node: spec.node.to_string(), reason: "a fabric_handover cell births nothing".into() })
    }
    async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        Ok(())
    }
    async fn inspect(&self, h: &DeploymentHandle) -> DeploymentStatus {
        if self.exited.lock().unwrap().contains(&h.deployment_id.to_string()) {
            DeploymentStatus::Exited { code: Some(0) }
        } else {
            DeploymentStatus::Running
        }
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<Found, DeployError> {
        Ok(Found::NeverLaunched)
    }
}

async fn admin(fabric: &FabricId, mesh: &str, path: &str, node_id: &str, fill_slot: bool) -> (Admin, iroh::Endpoint) {
    admin_over(fabric, mesh, path, node_id, fill_slot, Arc::new(MemoryFabricStorage::new()), Arc::default()).await
}

/// An admin born over `storage` and the status rows `status`: a new birth (a new incarnation and a
/// new endpoint key) of the node `node_id`, holding whatever both already hold.
async fn admin_over(fabric: &FabricId, mesh: &str, path: &str, node_id: &str, fill_slot: bool, storage: Arc<dyn FabricStorage>, status: Arc<MemoryStatusStorage>) -> (Admin, iroh::Endpoint) {
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
    let (facts_slot, build_slot): (BuildFactsSlot, BuildSlot) = (Arc::new(OnceLock::new()), Arc::new(OnceLock::new()));
    // Ping, the core family every node built on Node RPC serves: the tickle ladder's direct rung. A door
    // left in `ready_on_ping` fills the handover slot as the ping is answered: an admin whose door held
    // no view at one call and holds it by the next.
    let ready_on_ping: Arc<OnceLock<Arc<HandoverDoor>>> = Arc::new(OnceLock::new());
    let pinged = {
        let (slot, ready) = (slot.clone(), ready_on_ping.clone());
        ServerBuilder::new().serve::<Ping, _, _>(OpOwner::Core, move |_peer: rafka_node_rpc::PeerContext, req: PingRequest| {
            let (slot, ready) = (slot.clone(), ready.clone());
            async move {
                if let Some(door) = ready.get() {
                    let _ = slot.set(door.clone());
                }
                let PingRequest::Ping { payload } = req;
                Ok(PingReply::Pong { payload })
            }
        })
    };
    let served = rafka_node_admin_core::build_facts_read::serve(rafka_node_admin_core::fabric_handover::serve(pinged, slot.clone()), facts_slot.clone());
    let server = rafka_node_admin_core::build_op::serve(served, build_slot.clone()).seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() }).expect("the catalog seals");
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip).accept(rafka_node_rpc::ALPN, server.clone()).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { node_id: node_id.clone(), name: path.parse().unwrap(), endpoint_id: ep.id(), transport_addr: addr, incarnation: incarnation.clone() };
    let topology = Arc::new(tokio::sync::RwLock::new(view(vec![])));
    let cut_offs: Arc<Mutex<Vec<WriteCutOff>>> = Arc::default();
    let (writes_finish, writes_gate) = tokio::sync::watch::channel(true);
    let (view_open, view_gate) = tokio::sync::watch::channel(true);
    // The cell's view is set by hand, not projected from digests: the projection re-runs the one
    // election over its members with the seat records this admin holds now, as the admin's own
    // projection does (`admin::view_projector`).
    let records = {
        let records = Arc::new(Records::default());
        records.set_status(status.clone(), mesh.to_string(), path.parse().unwrap(), node_id.clone());
        records
    };
    // The births proven gone are the seat book's and those whose exit-proof row the records read, as
    // the admin's own lost set (`admin::incumbency_of`).
    let project_view: ProjectView = {
        let (topology, seats, records) = (topology.clone(), membership.seats().clone(), records.clone());
        Arc::new(move || {
            let (topology, seats, records, mut gate) = (topology.clone(), seats.clone(), records.clone(), view_gate.clone());
            Box::pin(async move {
                let _ = gate.wait_for(|open| *open).await;
                let mut view = topology.write().await;
                let mut lost = seats.gone();
                lost.extend(view.nodes.iter().filter_map(|n| n.incarnation_id.clone().filter(|i| records.is_exited(&n.node_id, i))));
                let inc = Incumbency { fabric: seats.fabric(), meshes: seats.meshes(), cohorts: Default::default(), lost };
                resolve_with(&mut view.nodes, &inc);
            })
        })
    };
    let (provider, nodes) = (Arc::new(Inspected::default()), Arc::new(MemoryNodesStorage::default()));
    let looker = rafka_node_admin_core::seat_watch::Looker { me: path.parse().unwrap(), me_id: node_id.clone(), membership: membership.clone(), client: client.clone(), provider: provider.clone(), nodes: Some(nodes.clone()) };
    let door = Arc::new(HandoverDoor {
        fabric: "fabric1".into(),
        me: path.parse().unwrap(),
        node_id: node_id.clone(),
        incarnation,
        topology,
        book: membership.book.clone(),
        membership,
        backbone,
        storage: storage.clone(),
        client: client.clone(),
        records,
        locks: Default::default(),
        memo: Default::default(),
        decisions: Arc::default(),
        builds: Arc::new(rafka_node_admin_core::build_state::MemoryBuildStateAdapter::new()),
        project_view,
        cut_off_writes: {
            let cut_offs = cut_offs.clone();
            Arc::new(move |c: WriteCutOff| {
                cut_offs.lock().unwrap().push(c);
                let mut gate = writes_gate.clone();
                Box::pin(async move {
                    let _ = gate.wait_for(|open| *open).await;
                })
            })
        },
        looker,
    });
    if fill_slot {
        let _ = slot.set(door.clone());
    }
    (Admin { door, client, resolver, resolved, storage, status, cut_offs, writes_finish, view_open, server, facts_slot, build_slot, ready_on_ping, provider, nodes, _router: router }, ep)
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

/// [`Pair`], each admin born over the `fabric.storage` `storage` gives it (one store shared by every
/// admin is the fabric's one store root, R14).
async fn fabric_with(storage: impl Fn() -> Arc<dyn FabricStorage>) -> Pair {
    let (fabric_id, status) = (FabricId::mint(), Arc::<MemoryStatusStorage>::default());
    let (incumbent, _) = admin_over(&fabric_id, "mesh1", "mesh1.admin.1", "000000000009", true, storage(), status.clone()).await;
    let (successor, _) = admin_over(&fabric_id, "mesh2", "mesh2.admin.1", "000000000005", true, storage(), status).await;
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
    Pair { incumbent, successor }
}

/// One call; a call with no reply panics naming the outcome and the leg it used (the dial's failure,
/// or the pooled connection it rode).
async fn send(from: &Admin, to: &Admin, req: Request) -> Reply {
    let (out, ev) = from.client.call::<NodePrimaryUpdate>(&NodeTarget::ExactNode(to.resolved.node_id.clone()), &req, &CallOptions::default()).await;
    out.reply().unwrap_or_else(|| panic!("no reply: {out:?} evidence {ev:?}")).value().clone()
}

/// A captured cell leaves its spans whether it passed or panicked: every line of [`Spans::lines`]
/// goes to `target/i143-testkit/fabric_handover/<cell>.<pid>.spans.txt`, so a red reads from its own
/// spans and never from a rerun.
struct SpanFile(Spans);

impl Drop for SpanFile {
    fn drop(&mut self) {
        let cell = std::thread::current().name().unwrap_or("unnamed").replace("::", "__");
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit/fabric_handover");
        let path = dir.join(format!("{cell}.{}.spans.txt", std::process::id()));
        let lines = self.0.lines();
        let written = std::fs::create_dir_all(&dir).and_then(|_| std::fs::write(&path, lines.join("\n")));
        eprintln!("fabric_handover: the cell's {} spans -> {} (write: {written:?})", lines.len(), path.display());
    }
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

fn fabric_primary_of(t: &Topology) -> Option<String> {
    t.fabric_primary().map(|n| n.name.to_string())
}

/// One finished span of a traced cell: its name, trace and attributes.
struct Traced {
    name: String,
    trace_id: String,
    attributes: BTreeMap<String, String>,
}

/// A cell's own span capture: the spans its threads open, and nobody else's. The product span
/// pipeline (`rafka_mesh_telemetry::span_capture`) in front of an in-memory exporter stamps the trace
/// ids, and [`Spans`] keeps every span in the order it was created with the fields recorded on it.
/// Both sit behind one dispatcher that is the default of the calling thread and of every thread of
/// the runtime [`TraceCapture::run`] builds for the cell, so two cells of one process never see each
/// other's spans.
struct TraceCapture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    capture: rafka_mesh_telemetry::SpanCapture,
    dispatch: tracing::Dispatch,
    spans: Spans,
}

fn trace_capture(service: &'static str) -> TraceCapture {
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let spans = Spans::default();
    let (capture, layer) = rafka_mesh_telemetry::span_capture(service, exporter.clone());
    TraceCapture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(spans.clone()).with(layer)), spans }
}

impl TraceCapture {
    /// Run the cell's `f` to completion on a runtime of four workers of its own, every one of whose
    /// threads has this capture as its default dispatcher (the update's spans are made on the server
    /// tasks of every admin, on any worker thread); `f` itself runs on the calling thread, whose default
    /// it is until `run` returns. The runtime is dropped when `f` ends.
    fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        let d = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d))).build().unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(f)
    }

    /// Run the cell's story under a root span named `root` on the capture's runtime; the runtime is
    /// dropped when the story ends. Returns the root's trace id and what the story returned.
    fn story<F: std::future::Future>(&self, root: &'static str, f: F) -> (String, F::Output) {
        self.run(async {
            let root = tracing::info_span!("rdm.test.fabric_handover", cell = root);
            let traceparent = rafka_mesh_telemetry::traceparent_of(&root).expect("the cell's root span has a trace context");
            let trace_id = traceparent.split('-').nth(1).expect("a traceparent names its trace").to_string();
            (trace_id, tracing::Instrument::instrument(f, root).await)
        })
    }

    fn spans(&self) -> Vec<Traced> {
        self.capture.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let all: Vec<Traced> = self
            .exporter
            .get_finished_spans()
            .unwrap_or_default()
            .into_iter()
            .map(|s| Traced { name: s.name.to_string(), trace_id: s.span_context.trace_id().to_string(), attributes: s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect() })
            .collect();
        crate::assert_own_story(all.iter().map(|s| s.name.as_str()));
        all
    }
}

/// A traced cell leaves its trace whether it passed or panicked: every span its capture holds
/// (name, trace, attributes) goes to `target/i143-testkit/fabric_handover/<cell>.<pid>.spans.json`,
/// so a red reads from its own spans and never from a rerun.
impl Drop for TraceCapture {
    fn drop(&mut self) {
        let flushed = self.capture.flush(std::time::Duration::from_secs(10));
        let spans: Vec<serde_json::Value> = self
            .exporter
            .get_finished_spans()
            .unwrap_or_default()
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, serde_json::Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), serde_json::Value::String(kv.value.to_string()))).collect();
                serde_json::json!({"name": s.name, "trace_id": s.span_context.trace_id().to_string(), "span_id": s.span_context.span_id().to_string(), "parent_span_id": s.parent_span_id.to_string(), "attributes": attributes})
            })
            .collect();
        let cell = std::thread::current().name().unwrap_or("unnamed").replace("::", "__");
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit/fabric_handover");
        let path = dir.join(format!("{cell}.{}.spans.json", std::process::id()));
        let written = std::fs::create_dir_all(&dir).and_then(|_| std::fs::write(&path, serde_json::to_vec_pretty(&spans).unwrap()));
        eprintln!("fabric_handover: the cell's {} spans -> {} (flush: {flushed:?}, write: {written:?})", spans.len(), path.display());
    }
}

/// A drive's executor call that never answers: the cell reads the re-submit's admission, not a run.
struct Held;

#[async_trait::async_trait]
impl rafka_node_admin_core::build_drive::Dispatcher for Held {
    async fn dispatch(&self, _turn: &rafka_node_admin_core::park_gate::Turn, _: &rafka_node_admin_core::model::PathName, _: &rafka_node_admin_core::build::BuildId, _: u32, _: rafka_node_rpc_contract::context::CallContext, _: Vec<Vec<u8>>, _: rafka_node_admin_core::executor::RunPlan) -> rafka_node_admin_core::build_drive::Dispatched {
        std::future::pending().await
    }
}

/// An executor that runs no operation: the cell's admins are called for no run.
struct NoRuns;

#[async_trait::async_trait]
impl rafka_node_admin_core::executor::OperationRunner for NoRuns {
    async fn run(&self, _: &rafka_node_admin_core::build::BuildId, _lane: Option<&str>, _: u32, op: &rafka_node_admin_core::build::BuildOperation) -> Result<(), rafka_node_admin_core::executor::RunEnd> {
        Err(format!("the cell runs no operation, not {op:?}").into())
    }
}

/// `a`'s Build door over the Build log `builds` and the Fabric identity holder `accepted`: its
/// control plane reads the view `a`'s handover door projects (so a committed seat names `a` the
/// fabric-primary there too), its seat book is `a`'s, and the incumbent it reads an unheard
/// re-submitted Build from is the one `a`'s committed transfer names.
async fn build_door(a: &Admin, builds: Arc<rafka_node_admin_core::build_state::MemoryBuildStateAdapter>, accepted: Arc<rafka_node_admin_core::accepted::AcceptedStore>) -> Arc<BuildDoor> {
    use rafka_node_admin_core::build_drive::{DriveEnv, Drives, OpenGate};
    let me = a.resolved.name.clone();
    let dyn_builds: Arc<dyn rafka_node_admin_core::build_state::BuildStateAdapter> = builds.clone();
    let mut control = rafka_node_admin_core::control_plane::ControlPlane::new(dyn_builds.clone(), accepted.clone(), me.clone(), a.door.topology.read().await.clone(), crate::adopted_time());
    control.topology = a.door.topology.clone();
    let control = Arc::new(control);
    let exec = Arc::new(rafka_node_admin_core::executor::BuildExecutor { executor: me.to_string(), builds: dyn_builds.clone(), topology: control.topology.clone(), runner: Arc::new(NoRuns) });
    let run = Arc::new(rafka_node_admin_core::build_run::RunDoor { me: me.clone(), builds: dyn_builds.clone(), exec, runs: Arc::default(), local: builds.clone(), gate: rafka_node_admin_core::park_gate::ParkGate::new() });
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: control.topology.clone(),
        builds: dyn_builds.clone(),
        door: Arc::new(rafka_node_admin_core::build_claim::ClaimDoor { me: me.clone(), node_id: a.resolved.node_id.clone(), topology: control.topology.clone(), builds: dyn_builds.clone(), contexts: Arc::new(rafka_node_admin_core::build_claim::AttemptContexts::in_memory()), claims: control.claims.clone() }),
        dispatcher: Arc::new(Held),
        gate: Arc::new(OpenGate),
        departure: Arc::new(crate::loopback::Proofs::default()),
        verdicts: Arc::new(crate::loopback::NoVerdicts),
        rows: Arc::new(rafka_node_admin_core::node_rows::NodeRows::new(me.clone(), a.resolved.node_id.clone(), control.topology.clone(), Arc::new(rafka_node_admin_core::storage::MemoryNodesStorage::default()))),
        turns: rafka_node_admin_core::park_gate::ParkGate::new(),
    });
    Arc::new(BuildDoor { me, control, drives: Arc::new(Drives::default()), env, run, seats: a.door.membership.seats().clone(), incarnation: a.resolved.incarnation.clone(), incumbent: a.door.clone() })
}

const INCUMBENT_FACTS: &str = "rdm.node_admin.build.update.via-incumbent-facts";
const FACTS_SERVED: &str = "rdm.node_admin.build.serve.via-fetch-facts";

/// CONTRACT (proposal Part C2: "Under mesh.delete the mesh seat does not move at all ... the row
/// commits the fabric seat to the outside mesh-primary and the incumbent stays mesh-primary";
/// build.md §5.2: "the fabric-primary's claim log reaches every admin through the Build topic and
/// FetchBuildFacts (`0x1F`)"): the incumbent P accepted Build B, claimed its attempt 1 on its own log
/// (accepted, it is the current Build there); the fabric seat moves to N, mesh2's primary, by
/// node.fabric-primary.update with P's mesh excluded (P writes the row, N installs from it); N's own
/// log has heard nothing of B (no Build topic runs between them, as when the re-submit outruns the
/// gossip). The caller's re-submit of B to N is admitted: N reads B's facts once from P, the
/// incumbent its installed update names, absorbs them into its own log and attaches to the same
/// attempt 1, claimed by the same executor. What must NOT happen: N answering `UnknownBuild` for a
/// Build its incumbent holds.
#[test]
fn the_new_seat_resumes_a_resubmitted_build_it_has_not_heard_from_its_incumbents_facts() {
    use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology};
    use rafka_node_admin_core::build_state::{BuildAccepted, BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter};
    let cap = trace_capture("fabric-handover");
    let (trace, (resubmitted, held)) = cap.story("the_new_seat_resumes_a_resubmitted_build_it_has_not_heard_from_its_incumbents_facts", async {
        let storage: Arc<dyn FabricStorage> = Arc::new(MemoryFabricStorage::new());
        let f = fabric_with(|| storage.clone()).await;
        storage.claim_identity(&FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() }).await.unwrap().unwrap();
        // P accepts B: the Build durable on its own log, where it is the current Build.
        let b = rafka_node_admin_core::build::BuildId::mint();
        let (p_log, n_log) = (Arc::new(MemoryBuildStateAdapter::new()), Arc::new(MemoryBuildStateAdapter::new()));
        p_log.publish_accepted(&BuildAccepted { build_id: b.clone(), topology: FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 1 }).await.unwrap();
        p_log.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: b.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
        let _ = f.incumbent.facts_slot.set(Arc::new(BuildFactsDoor { me: f.incumbent.resolved.name.clone(), builds: p_log.clone(), floor: Arc::default() }));
        let n_door = build_door(&f.successor, n_log.clone(), Arc::new(AcceptedStore::new(storage.clone(), "mesh2.admin.1"))).await;
        let _ = f.successor.build_slot.set(n_door);
        // The fabric seat moves to N; N's own log still holds nothing of B.
        let op = update_operation(Seat::FabricPrimary, "b0", "shutdown-mesh:mesh1");
        f.incumbent.door.update_primary(Moving::FabricSeatOutside, None, "b0", 1, &op).await.expect("the fabric seat moves outside mesh1");
        assert!(n_log.read_build(&b).await.is_err(), "N has heard nothing of B before the re-submit");
        // The caller re-submits B at the new seat.
        let at_new_seat = rafka_node_admin_client::BuildCarrier::new(f.incumbent.client.clone(), f.successor.resolved.name.clone());
        let resubmitted = at_new_seat.resubmit(&b, 1).await.map(|s| (s.build_id().to_string(), s.attempt(), s.disposition()));
        let held = n_log.read_build(&b).await.map(|p| (p.attempt, p.executor));
        (resubmitted.map(|r| (b.clone(), r)), held)
    });
    let (b, (build_id, attempt, disposition)) = resubmitted.expect("the new seat admits the re-submit by build_id from its incumbent's facts");
    assert_eq!(build_id, b.as_str(), "never a second Build");
    assert_eq!((attempt, disposition), (1, rafka_node_rpc_contract::build::Disposition::Attached), "the re-submit attaches to the attempt it named");
    let (held_attempt, held_executor) = held.expect("N's own log holds B once its incumbent's facts are absorbed");
    assert_eq!((held_attempt, held_executor.as_deref()), (1, Some("mesh1.admin.1")), "no new attempt is claimed: the same attempt, by the same executor");
    let spans = cap.spans();
    let in_trace = |name: &str| spans.iter().filter(|s| s.name == name && s.trace_id == trace).collect::<Vec<_>>();
    let absorbed = in_trace(INCUMBENT_FACTS);
    assert_eq!(absorbed.len(), 1, "one read of the incumbent's facts, in the re-submit's own trace {trace}");
    let a = &absorbed[0].attributes;
    assert_eq!(a.get("outcome").map(String::as_str), Some("absorbed"), "{a:?}");
    assert_eq!(a.get("node").map(String::as_str), Some("mesh2.admin.1"), "{a:?}");
    assert!(a.get("incumbent").is_some_and(|i| i.starts_with("mesh1.admin.1 (000000000009")), "the incumbent the installed update names: {a:?}");
    assert_eq!(a.get("facts").map(String::as_str), Some("2"), "the acceptance and the claim: {a:?}");
    let served = in_trace(FACTS_SERVED);
    assert_eq!(served.len(), 1, "P served the one read, in the same trace");
    let p = &served[0].attributes;
    assert_eq!((p.get("node").map(String::as_str), p.get("outcome").map(String::as_str), p.get("complete").map(String::as_str)), (Some("mesh1.admin.1"), Some("served"), Some("true")), "{p:?}");
    println!("trace {trace}");
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

// ---- node.primary.update and node.fabric-primary.update (proposal Part C) ----

/// mesh1: P (`mesh1.admin.1`) holds mesh1's primary seat at epoch 3; N (`mesh1.admin.2`) is the
/// lowest Ready NodeId of mesh1's other node-admins, the successor P's pre-check computes; M
/// (`mesh1.admin.3`) is a Ready admin of the same mesh with a higher NodeId. mesh2: X, its primary
/// and only node-admin, with the lowest NodeId of the fabric (so a cross-mesh calculation would pick
/// X, never N). [`FabricSeat`] says who holds the fabric seat at epoch 7.
struct Seats {
    p: Admin,
    n: Admin,
    m: Admin,
    x: Admin,
}

/// Who holds the fabric seat of a [`Seats`] fabric.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FabricSeat {
    /// P holds both of its seats: it is the fabric-primary and moves its own (it has nothing above it).
    P,
    /// X is the fabric-primary and commands P's update (Part C1: "The fabric-primary commands a mesh-primary").
    X,
}

/// P holds both seats; every admin over the fabric's one store (R14: one store root per fabric).
async fn seats_fabric() -> Seats {
    seats_fabric_shared(FabricSeat::P).await
}

/// X is the fabric-primary; every admin over the fabric's one store.
async fn commanded_fabric() -> Seats {
    seats_fabric_shared(FabricSeat::X).await
}

/// A seats fabric whose admins share one `fabric.storage`: the fabric's one store root (R14,
/// R-SR1), where the holder writes the seat row and the successor and the fabric-primary point-read it.
async fn seats_fabric_shared(fp: FabricSeat) -> Seats {
    let store: Arc<dyn FabricStorage> = Arc::new(MemoryFabricStorage::new());
    seats_fabric_with(fp, || {
        let store = store.clone();
        async move { store }
    })
    .await
}

/// The seats fabric with each admin's fabric.storage given by `storage`.
async fn seats_fabric_with<F: std::future::Future<Output = Arc<dyn FabricStorage>>>(fp: FabricSeat, storage: impl Fn() -> F) -> Seats {
    seats_fabric_over(fp, true, storage).await
}

/// The seats fabric over the one shared store whose N holds no view at the first call it is made
/// (its handover door `NotReady`) and holds it from its first answered ping on.
async fn seats_fabric_n_ready_on_ping(fp: FabricSeat) -> Seats {
    let store: Arc<dyn FabricStorage> = Arc::new(MemoryFabricStorage::new());
    let f = seats_fabric_over(fp, false, || {
        let store = store.clone();
        async move { store }
    })
    .await;
    let _ = f.n.ready_on_ping.set(f.n.door.clone());
    f
}

/// The seats fabric with each admin's fabric.storage given by `storage`; N's handover door is filled
/// at its birth when `n_ready`.
async fn seats_fabric_over<F: std::future::Future<Output = Arc<dyn FabricStorage>>>(fp: FabricSeat, n_ready: bool, storage: impl Fn() -> F) -> Seats {
    // One store root per fabric (R14): every admin's status rows are the one store's.
    let (fabric_id, status) = (FabricId::mint(), Arc::<MemoryStatusStorage>::default());
    let (p, _) = admin_over(&fabric_id, "mesh1", "mesh1.admin.1", "000000000002", true, storage().await, status.clone()).await;
    let (n, _) = admin_over(&fabric_id, "mesh1", "mesh1.admin.2", "000000000003", n_ready, storage().await, status.clone()).await;
    let (m, _) = admin_over(&fabric_id, "mesh1", "mesh1.admin.3", "000000000009", true, storage().await, status.clone()).await;
    let (x, _) = admin_over(&fabric_id, "mesh2", "mesh2.admin.1", "000000000001", true, storage().await, status).await;
    let mut pn = node_of(&p, NodeStatus::ReadyForTraffic, fp == FabricSeat::P);
    pn.is_primary = true;
    let mut nn = node_of(&n, NodeStatus::ReadyForTraffic, false);
    nn.is_primary = false;
    let mut mn = node_of(&m, NodeStatus::ReadyForTraffic, false);
    mn.is_primary = false;
    let xn = node_of(&x, NodeStatus::ReadyForTraffic, fp == FabricSeat::X);
    let nodes = vec![pn, nn, mn, xn];
    let fabric_holder = match fp {
        FabricSeat::P => holder_of(&p, 7),
        FabricSeat::X => holder_of(&x, 7),
    };
    for a in [&p, &n, &m, &x] {
        *a.door.topology.write().await = view(nodes.clone());
        for b in [&p, &n, &m, &x] {
            a.resolver.insert(b.resolved.clone());
        }
        a.door.membership.seats().take(Seat::FabricPrimary, &fabric_holder);
        a.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&p, 3));
        a.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&x, 1));
    }
    Seats { p, n, m, x }
}

/// The fabric-primary's command (UpdatePrimary) to `holder`: update mesh1's primary seat held at `mesh_epoch`.
fn update_mesh_seat(holder: &Admin, mesh_epoch: u64, build: &str, operation: &str) -> Request {
    Request::UpdatePrimary { fabric: "fabric1".into(), holder: birth(holder), seats: vec![SeatEpoch { seat: WireSeat::MeshPrimary { mesh: "mesh1".into() }, expected_epoch: mesh_epoch }], build_id: build.into(), attempt: 1, operation: operation.into() }
}

/// The holder's node.primary.update call on `successor` moving mesh1's primary seat from `holder` to
/// `epoch`: the content of the committed row.
fn primary_update(holder: &Admin, successor: &Admin, epoch: u64, build: &str, attempt: u32, operation: &str) -> Request {
    Request::PrimaryUpdate { seats: vec![SeatUpdate { seat: WireSeat::MeshPrimary { mesh: "mesh1".into() }, holder: birth(holder), successor: birth(successor), epoch }], build_id: build.into(), attempt, operation: operation.into() }
}

fn committed_mesh(a: &Admin, mesh_epoch: u64) -> Vec<CommittedSeat> {
    vec![CommittedSeat::of(Seat::MeshPrimary, &holder_of(a, mesh_epoch))]
}

fn committed_fabric(a: &Admin, fabric_epoch: u64) -> Vec<CommittedSeat> {
    vec![CommittedSeat::of(Seat::FabricPrimary, &holder_of(a, fabric_epoch))]
}

fn committed_both(a: &Admin, mesh_epoch: u64, fabric_epoch: u64) -> Vec<CommittedSeat> {
    vec![CommittedSeat::of(Seat::MeshPrimary, &holder_of(a, mesh_epoch)), CommittedSeat::of(Seat::FabricPrimary, &holder_of(a, fabric_epoch))]
}

/// The node.primary.updated reply carrying `epochs`.
fn updated(epochs: Vec<CommittedSeat>) -> Reply {
    Reply::PrimaryUpdated { epochs }
}

/// The (side, reason, detail) of every refusal span of the primitive.
fn refusals(spans: &Spans) -> Vec<(String, String, String)> {
    spans.named(REJECTED).iter().map(|f| (f["side"].clone(), f["reason"].clone(), f.get("detail").cloned().unwrap_or_default())).collect()
}

/// CONTRACT (proposal Part C1 steps 2-7 and C2 Q1: "when the mesh-seat successor is in the same mesh
/// (search step 1), the one row of C1 commits both seats to NA2 at their next epochs, which makes NA2
/// mesh-primary and fabric-primary at the same boundary"): P, the fabric-primary, holds both seats and
/// moves them by one node.primary.update to N, its pre-check's successor. In order: P says "primary
/// updating", P cuts off its writes, P writes ONE row moving both seats to N (mesh1 4, fabric 8), N
/// installs from the row and answers node.primary.updated, P says "primary updated". P and N hold both
/// committed records; M, a third admin that heard nothing, reads the one row from the store root and
/// holds both too. P's moved fabric record is the target its Build log is judged against, naming N.
/// What must NOT happen: two
/// rows, a row written by N, a seat moved with no row, or a holder record naming P after the update.
#[test]
fn a_seat_holder_moves_both_seats_to_its_successor_in_one_row() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    cap.run(async {
        let f = seats_fabric().await;
        let p_node = f.p.door.topology.read().await.members().find(|x| x.name.to_string() == "mesh1.admin.1").cloned().unwrap();
        let op = update_operation(Seat::MeshPrimary, "b1", "replace-node:mesh1.admin.1");
        let moved = f.p.door.update_primary(Moving::BothSeats, None, "b1", 1, &op).await.expect("both seats move");
        assert_eq!(moved, committed_both(&f.n, 4, 8));
        for a in [&f.p, &f.n] {
            assert_eq!((a.door.membership.seats().mesh("mesh1"), a.door.membership.seats().fabric()), (Some(holder_of(&f.n, 4)), Some(holder_of(&f.n, 8))), "{} holds both committed seats", a.resolved.name);
        }
        let rows = f.n.storage.seat_transfers().await.unwrap();
        assert_eq!(rows.len(), 1, "one row moves both seats: {rows:?}");
        assert_eq!((&rows[0].holder, &rows[0].successor, &rows[0].seats, rows[0].operation.as_str()), (&birth(&f.p), &birth(&f.n), &committed_both(&f.n, 4, 8), op.as_str()));
        // M heard no gossip: it reads the one row from the store root, as an admin entering does.
        assert_eq!((f.m.door.membership.seats().mesh("mesh1"), f.m.door.membership.seats().fabric()), (Some(holder_of(&f.p, 3)), Some(holder_of(&f.p, 7))), "M heard nothing");
        let read = rafka_node_admin_core::fabric_handover::hydrate_seats(&*f.m.storage, f.m.door.membership.seats()).await.expect("M reads the store root");
        assert_eq!(read, 1, "one row read");
        assert_eq!((f.m.door.membership.seats().mesh("mesh1"), f.m.door.membership.seats().fabric()), (Some(holder_of(&f.n, 4)), Some(holder_of(&f.n, 8))), "M holds both seats the one row commits");
        // The moved record is the target P's Build log is judged against: read for P's birth, it names N.
        let p_birth = (p_node.node_id.clone(), p_node.incarnation_id.clone().unwrap());
        let view = f.p.door.topology.read().await.clone();
        let target = rafka_node_admin_core::seat_watch::fabric_seat_record(f.p.door.membership.seats().fabric().as_ref(), &p_birth, &Default::default(), |id| view.own(id).map(|n| n.name.to_string()));
        assert!(matches!(&target, rafka_node_admin_core::fabric_builds::SeatRecord::Moved { to: Some(to), .. } if to == "mesh1.admin.2"), "P's seat record names N to its Build log: {target:?}");
        let at = |n: &str| spans.position(n).unwrap_or_else(|| panic!("no {n} span among {:?}", spans.names()));
        assert!(at(ANNOUNCE_UPDATING) < at(CUT_OFF), "P says the primary is updating before its cut-off");
        assert!(at(CUT_OFF) < at(ROW_WRITTEN), "P cuts off its writes before it writes the row");
        assert!(at(ROW_WRITTEN) < at(UPDATED), "N installs only after P's row");
        assert!(at(UPDATED) < at(ANNOUNCE_UPDATED), "P says the primary updated once N answered node.primary.updated");
        let said: Vec<String> = spans.named(ANNOUNCE_UPDATED).iter().map(|f| f["node"].clone()).collect();
        assert_eq!(said, vec!["mesh1.admin.1".to_string()], "the holder P, never the successor, says the primary updated");
        let written = spans.named(ROW_WRITTEN);
        assert_eq!(written.len(), 1, "{written:?}");
        assert_eq!((written[0]["node"].as_str(), written[0]["mesh_epoch"].as_str(), written[0]["fabric_epoch"].as_str()), ("mesh1.admin.1", "4", "8"), "P, the cohort's row writer, writes the one row: {written:?}");
        let installed = spans.named(UPDATED);
        assert_eq!(installed.len(), 1, "{installed:?}");
        assert_eq!((installed[0]["node"].as_str(), installed[0]["replay"].as_str(), installed[0].get("holder_exit").map(String::as_str)), ("mesh1.admin.2", "false", Some("none")), "N found no exit-proof row of P, alive: {installed:?}");
        assert!(spans.named(SEATS_COMMITTED).is_empty(), "the successor never writes a seat row");
        assert!(refusals(&spans).is_empty(), "{:?}", refusals(&spans));
        let _ = &f.x;
    });
}

/// The claims of Build attempts `a`'s own Build log holds: `(attempt, executor)`.
async fn claims_held(a: &Admin) -> Vec<(u32, String)> {
    a.door.builds.facts().await.unwrap().into_iter().filter_map(|f| match f {
        rafka_node_admin_core::build_state::BuildFact::Claim(c) => Some((c.attempt, c.executor)),
        _ => None,
    }).collect()
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 step 1: "The
/// holder is commanded from above, as a step of the Build"; build.md: "A live run is reattached, never
/// re-claimed"): the update a fabric-primary runs on its own seats (the fabric seat moves in its row) is a
/// step of the attempt it claimed for itself before its row, so the successor takes that claim into its
/// Build log as it installs, and reads that attempt held by the holder. A mesh-primary's update that the
/// fabric-primary commands (its row moves the mesh seat alone) is a step of the commander's drive, which
/// claims the row's attempt for the successor afterwards: the holder claimed nothing, and the successor
/// takes no claim from that row.
/// Must NOT happen: the successor holding a claim of the row's attempt by a holder that never made it,
/// so that the commander's claim of that attempt for the successor is refused stale-claim there.
#[test]
fn the_successor_takes_the_holders_claim_from_a_fabric_primarys_own_row_and_none_from_a_commanded_row() {
    let cap = trace_capture("fabric-handover");
    cap.run(async {
        let build = "vhpqxdbt5w2g";
        let f = commanded_fabric().await;
        let op = update_operation(Seat::MeshPrimary, build, "stop-node:mesh1.admin.1");
        let reply = send(&f.x, &f.p, update_mesh_seat(&f.p, 3, build, &op)).await;
        assert_eq!(reply, updated(committed_mesh(&f.n, 4)), "the commanded update moved P's mesh seat to N");
        assert_eq!(claims_held(&f.n).await, vec![], "N took no claim from a row its holder ran in no attempt of its own");
        drop(f);
        let g = seats_fabric().await;
        let op = update_operation(Seat::MeshPrimary, build, "stop-node:mesh1.admin.1");
        g.p.door.update_primary(Moving::BothSeats, None, build, 2, &op).await.expect("both seats move");
        assert_eq!(claims_held(&g.n).await, vec![(2, "mesh1.admin.1".to_string())], "N took the fabric-primary's claim of the attempt its own update ran in");
    });
}

/// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 step 7: "The
/// writer publishes 'primary updated' with the same identifiers"; failure contract: "The holder lost
/// after the row, before 'updated': the successor and every reader take the seat from the row; the next
/// writer to read the row publishes 'updated'"; "The workflow continues only after the result is
/// installed and acknowledged: node.primary.updated answered to the resuming executor's call"): P moves
/// both seats to N; N installs from the row on P's call and P says "primary updated". N, now the
/// fabric-primary, acknowledges the row before it dispatches an operation on P: its answer to P's call
/// is its node.primary.updated, so it installs nothing a second time, and while P is alive N says
/// nothing (P said it, step 7). Once P's exit is proven by its exit-proof row, N, the next writer to
/// read the row, says "primary updated" once, however often it acknowledges the row after that.
/// Must NOT happen: the successor saying "primary updated" for a holder that is alive, a second install
/// from the row, or "primary updated" said twice by the successor.
#[test]
fn a_successor_that_answered_the_holders_call_says_primary_updated_only_once_the_holder_is_proven_exited() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    cap.run(async {
        let f = seats_fabric().await;
        let p_node = f.p.door.topology.read().await.members().find(|x| x.name.to_string() == "mesh1.admin.1").cloned().unwrap();
        let op = update_operation(Seat::MeshPrimary, "b1", "stop-node:mesh1.admin.1");
        let moved = f.p.door.update_primary(Moving::BothSeats, None, "b1", 1, &op).await.expect("both seats move");
        assert_eq!(moved, committed_both(&f.n, 4, 8));
        let said = || spans.named(ANNOUNCE_UPDATED).iter().map(|f| f["node"].clone()).collect::<Vec<String>>();
        assert_eq!(said(), vec!["mesh1.admin.1".to_string()], "P said the primary updated on N's answer");
        let acks = || spans.named(ROW_ACKNOWLEDGED).iter().map(|f| (f.get("by").cloned().unwrap_or_default(), f.get("announced").cloned().unwrap_or_default())).collect::<Vec<_>>();
        assert_eq!(f.n.door.row_acknowledged("mesh1", &op, Some(&p_node.name), "b1", 2).await, Ok(true), "N acknowledges the row");
        assert_eq!(said(), vec!["mesh1.admin.1".to_string()], "while P is alive N says nothing: {:?}", acks());
        assert_eq!(acks(), vec![("answered".to_string(), "false".to_string())], "N's answer to P's call is its acknowledgement: no second install");
        assert_eq!(spans.named(UPDATED).len(), 1, "N installed from the row once, on P's call");
        // P's birth is proven gone: its exit-proof row among mesh1's status rows.
        f.n.door.records.put_exit(&p_node, None, "provider").await.expect("P's exit-proof row is recorded");
        for _ in 0..2 {
            assert_eq!(f.n.door.row_acknowledged("mesh1", &op, Some(&p_node.name), "b1", 2).await, Ok(true), "N acknowledges the row");
        }
        assert_eq!(said(), vec!["mesh1.admin.1".to_string(), "mesh1.admin.2".to_string()], "N, the next writer to read the row, says it once P's exit is proven, once: {:?}", acks());
        assert_eq!(spans.named(UPDATED).len(), 1, "still one install");
        assert!(refusals(&spans).is_empty(), "{:?}", refusals(&spans));
        let _ = (&f.m, &f.x);
    });
}

/// CONTRACT (proposal Part C1 step 4, R14 item 4: "NA1 sets its write cut-off and drains every store
/// write already issued"; step 5: "The writer writes the one seat row"): the fabric-primary X commands
/// P; while P's cut-off is held open (a write still on the disk) no seat row exists and X's command is
/// unanswered; once the write finished, P writes the one row and N's node.primary.updated is X's answer. What
/// must NOT happen: the seat row written while an old-seat write is still issued, or a commit with no
/// cut-off.
#[test]
fn the_holder_cuts_off_its_seat_writes_before_it_writes_the_seat_row() {
    let cap = crate::mesh_delete_resume::capture();
    let op = "stop-node:mesh1.admin.1";
    let trace_id = cap.run(async {
        let root = tracing::info_span!("rdm.test.seat.update.via-primary-update-cut-off");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        tracing::Instrument::instrument(the_holder_cuts_off(op), root).await;
        trace_id
    });
    println!("trace_id={trace_id}");
    let spans = cap.spans();
    let ours = |name: &str| spans.iter().filter(|s| s["name"] == name && s["attributes"]["operation"] == op).collect::<Vec<_>>();
    let cut_off = ours(CUT_OFF);
    assert_eq!(cut_off.len(), 1, "one cut-off span for the update: {:?}", spans.iter().map(|s| s["name"].clone()).collect::<Vec<_>>());
    assert_eq!(cut_off[0]["attributes"]["fabric_seat"], "false");
    let written = ours(ROW_WRITTEN);
    assert_eq!(written.len(), 1, "one row for the update");
    assert_eq!(written[0]["attributes"]["node"], "mesh1.admin.1", "the holder writes the row");
    let installed = ours(UPDATED);
    assert_eq!(installed.len(), 1, "one install at the successor");
    for s in [cut_off[0], written[0], installed[0]] {
        assert_eq!(s["trace_id"].as_str(), Some(trace_id.as_str()), "the holder's cut-off and row and the successor's install are the cell's own story: {s:?}");
    }
}

async fn the_holder_cuts_off(op: &'static str) {
    let f = commanded_fabric().await;
    f.p.writes_finish.send_replace(false);
    let call = tokio::spawn({
        let (x, p) = (f.x.client.clone(), f.p.resolved.node_id.clone());
        let req = update_mesh_seat(&f.p, 3, "b1", op);
        tracing::Instrument::in_current_span(async move {
            let (out, _) = x.call::<NodePrimaryUpdate>(&NodeTarget::ExactNode(p), &req, &CallOptions::default()).await;
            out.reply().map(|r| r.value().clone())
        })
    });
    until("P cuts off its seat writes", || !f.p.cut_offs.lock().unwrap().is_empty()).await;
    assert!(
        matches!(&f.p.cut_offs.lock().unwrap()[..], [WriteCutOff::Set { reason, mesh_seat: true, fabric_seat: false }] if reason.contains(op) && reason.contains("mesh1")),
        "one cut-off naming the update, the mesh seat only: {:?}",
        f.p.cut_offs.lock().unwrap()
    );
    assert!(f.p.storage.seat_transfers().await.unwrap().is_empty(), "the seat row was written while a write of the old seat was still issued");
    assert!(!call.is_finished(), "P answered before its issued writes finished");
    f.p.writes_finish.send_replace(true);
    let reply = call.await.unwrap();
    assert_eq!(reply, Some(updated(committed_mesh(&f.n, 4))), "N's node.primary.updated is the answer");
    assert_eq!(f.n.storage.seat_transfers().await.unwrap().len(), 1, "one row, written after the cut-off");
    assert!(matches!(&f.p.cut_offs.lock().unwrap()[..], [WriteCutOff::Set { .. }, WriteCutOff::Committed { fabric_seat: false, .. }]), "the committed row ends P's authority over the cut-off writes: {:?}", f.p.cut_offs.lock().unwrap());
    let _ = (&f.m, &f.x);
}

/// CONTRACT (proposal Part C1 failure contract: "An identical retry of the call replays the committed
/// result before any epoch check; a call with a conflicting payload for the same operation is refused
/// by name"; C3 edit 5): a lost reply is retried with the identical call and answered with the same
/// committed seat before any stale-seat check (mesh1's seat is already at 4): the fabric-primary's
/// identical command to P, its own command door, and P's identical node.primary.update to N each
/// replay the row. The same operation with a conflicting payload (another epoch) is RejectedConflict
/// at N. Nothing commits twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_identical_retry_answers_the_same_seat_and_another_payload_is_refused() {
    let f = commanded_fabric().await;
    let op = "restart-node:mesh1.admin.1";
    let first = send(&f.x, &f.p, update_mesh_seat(&f.p, 3, "b1", op)).await;
    assert_eq!(first, updated(committed_mesh(&f.n, 4)));
    // The reply was lost: the fabric-primary sends the identical command again.
    assert_eq!(send(&f.x, &f.p, update_mesh_seat(&f.p, 3, "b1", op)).await, first, "the identical command replays the row");
    let p_node = f.x.door.topology.read().await.members().find(|x| x.name.to_string() == "mesh1.admin.1").cloned().unwrap();
    assert_eq!(f.x.door.command_update_primary(&p_node, "b1", 1, op).await, Ok(committed_mesh(&f.n, 4)), "the fabric-primary's own command door replays the row");
    assert_eq!(send(&f.p, &f.n, primary_update(&f.p, &f.n, 4, "b1", 1, op)).await, first, "P's identical call to N replays the row");
    assert_eq!(send(&f.p, &f.n, primary_update(&f.p, &f.n, 5, "b1", 1, op)).await, Reply::RejectedConflict { operation: op.into() }, "a conflicting payload for the committed operation");
    assert_eq!(f.n.storage.seat_transfers().await.unwrap().len(), 1);
    assert_eq!(f.n.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.n, 4)), "one epoch committed");
    assert_eq!(f.x.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.n, 4)), "the fabric-primary installed the committed seat");
}

/// CONTRACT (proposal Part C1: "the holder's ... call"; node-rpc-envelope.md, lost replies: a holder
/// keeps serving after the row): the fabric-primary's reply was lost and P's Node RPC server has since
/// stopped taking new calls (as node.connections.delete makes it): the identical command is still
/// answered from the row with the same committed seat, never refused, and nothing
/// commits twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_after_the_holder_stopped_taking_new_calls_is_answered_from_the_row() {
    let f = commanded_fabric().await;
    let op = "replace-node:mesh1.admin.1";
    let first = send(&f.x, &f.p, update_mesh_seat(&f.p, 3, "b1", op)).await;
    assert_eq!(first, updated(committed_mesh(&f.n, 4)));
    f.p.server.drain();
    assert_eq!(send(&f.x, &f.p, update_mesh_seat(&f.p, 3, "b1", op)).await, first, "a holder that takes no new calls answers the identical command from the row");
    assert_eq!(f.n.storage.seat_transfers().await.unwrap().len(), 1, "nothing committed twice");
}

/// CONTRACT (proposal Part C1 failure contract: "After the row, the successor is the committed holder
/// whatever the call returns. The receiver never refuses the call for the birth it names (R-UC1a,
/// R-RX5) ... it installs the committed view from the row and answers"; Part E cell: "the call after
/// the row reaches a process that is not the named successor's birth, the successor alive: no refusal,
/// the successor stays the committed holder, the retry against its resolved birth completes"): P's
/// row commits mesh1's seat to N. The same call reaching M, a process that is not N's birth, is
/// answered node.primary.updated from the row: M installs the committed view naming N and refuses
/// nothing. N stays the committed holder, and the call against N's own birth completes. A command
/// naming another birth of P is judged by the seat P holds and its epoch, never by the birth it names:
/// it moves the seat. What must NOT happen: a wrong-birth, stale-seat or not-eligible refusal at a
/// receiver of the call.
#[test]
fn a_primary_update_call_is_never_refused_for_the_birth_it_names() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    cap.run(async {
        let f = commanded_fabric().await;
        let op = "node.primary.update:b1:stop-node:mesh1.admin.1";
        // A command naming another birth of P: P holds mesh1's seat at 3, so it moves it.
        let mut other_birth = update_mesh_seat(&f.p, 3, "b1", op);
        if let Request::UpdatePrimary { holder, .. } = &mut other_birth {
            holder.incarnation = IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("another-birth")).unwrap();
        }
        assert_eq!(send(&f.x, &f.p, other_birth).await, updated(committed_mesh(&f.n, 4)), "the command is judged by the seat P holds");
        // The call reaching M, which is not N's birth.
        assert_eq!(send(&f.p, &f.m, primary_update(&f.p, &f.n, 4, "b1", 1, op)).await, updated(committed_mesh(&f.n, 4)), "M answers from the row and refuses nothing");
        assert_eq!(f.m.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.n, 4)), "M holds the committed view naming N");
        // The call against N's own birth completes; N stays the committed holder.
        assert_eq!(send(&f.p, &f.n, primary_update(&f.p, &f.n, 4, "b1", 2, op)).await, updated(committed_mesh(&f.n, 4)));
        assert_eq!(f.n.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.n, 4)));
        assert_eq!(f.n.storage.seat_transfers().await.unwrap().len(), 1, "one row");
        assert!(refusals(&spans).is_empty(), "nothing is refused: {:?}", refusals(&spans));
        let _ = &f.x;
    });
}

/// CONTRACT (proposal Part C1 "The no-successor cases", Q7 ruled Luke 2026-10-12: "refused by name,
/// `cannot transfer: no secondary of type <node_type> in mesh <mesh>` ... The holder stays primary,
/// nothing is cut off, no row is written"; C2 search step 3: "neither found: refused by name, `cannot
/// transfer fabric seat: no other node-admin ReadyForTraffic in mesh <mesh>, no other mesh-primary
/// ReadyForTraffic`"): each refused by its exact text under the holder's reject span `no-successor`,
/// nothing cut off, no row, every seat where it was:
/// - P, the fabric-primary, commands X (mesh2's only node-admin) to update mesh2's seat: Q7's text for mesh2;
/// - X, the fabric-primary and mesh2's only node-admin, moving both its seats while P is a Ready
///   mesh-primary of mesh1: the mesh seat has no secondary, Q7's text for mesh2;
/// - a lone admin, the fabric's only node-admin of its only mesh, moving both seats: C2's text;
/// - the same lone admin moving the fabric seat outside its mesh (mesh.delete of the only mesh): C2's text.
#[test]
fn a_transfer_with_no_successor_is_refused_with_q7s_exact_texts_and_nothing_moves() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    cap.run(async {
        let mesh2 = "cannot transfer: no secondary of type node-admin in mesh mesh2";
        let lone_text = "cannot transfer fabric seat: no other node-admin ReadyForTraffic in mesh mesh1, no other mesh-primary ReadyForTraffic";
        let f = seats_fabric().await;
        let only = Request::UpdatePrimary { fabric: "fabric1".into(), holder: birth(&f.x), seats: vec![SeatEpoch { seat: WireSeat::MeshPrimary { mesh: "mesh2".into() }, expected_epoch: 1 }], build_id: "b2".into(), attempt: 1, operation: "node.primary.update:b2:restart-node:mesh2.admin.1".into() };
        assert_eq!(send(&f.p, &f.x, only).await, Reply::RejectedNoSuccessor { reason: mesh2.into() });
        assert!(f.x.cut_offs.lock().unwrap().is_empty(), "nothing is cut off: {:?}", f.x.cut_offs.lock().unwrap());
        assert_eq!(f.x.door.membership.seats().mesh("mesh2"), Some(holder_of(&f.x, 1)), "X keeps mesh2's seat");
        let g = commanded_fabric().await;
        let r = g.x.door.update_primary(Moving::BothSeats, None, "b3", 1, &update_operation(Seat::MeshPrimary, "b3", "stop-node:mesh2.admin.1")).await;
        assert_eq!(r, Err(Reply::RejectedNoSuccessor { reason: mesh2.into() }), "the mesh seat has no secondary though mesh1 has a Ready primary");
        assert_eq!((g.x.door.membership.seats().mesh("mesh2"), g.x.door.membership.seats().fabric()), (Some(holder_of(&g.x, 1)), Some(holder_of(&g.x, 7))), "X keeps both seats");
        assert!(g.x.cut_offs.lock().unwrap().is_empty());
        // The fabric's only node-admin of its only mesh.
        let (lone, _) = admin(&FabricId::mint(), "mesh1", "mesh1.admin.1", "000000000004", true).await;
        *lone.door.topology.write().await = view(vec![node_of(&lone, NodeStatus::ReadyForTraffic, true)]);
        lone.door.membership.seats().take(Seat::FabricPrimary, &holder_of(&lone, 7));
        lone.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&lone, 3));
        let both = lone.door.update_primary(Moving::BothSeats, None, "b4", 1, &update_operation(Seat::MeshPrimary, "b4", "stop-node:mesh1.admin.1")).await;
        assert_eq!(both, Err(Reply::RejectedNoSuccessor { reason: lone_text.into() }), "both searches named");
        let outside = lone.door.update_primary(Moving::FabricSeatOutside, None, "b5", 1, &update_operation(Seat::FabricPrimary, "b5", "shutdown-mesh:mesh1")).await;
        assert_eq!(outside, Err(Reply::RejectedNoSuccessor { reason: lone_text.into() }), "mesh.delete of the only mesh");
        assert!(lone.cut_offs.lock().unwrap().is_empty() && lone.storage.seat_transfers().await.unwrap().is_empty(), "nothing cut off, no row");
        assert_eq!((lone.door.membership.seats().mesh("mesh1"), lone.door.membership.seats().fabric()), (Some(holder_of(&lone, 3)), Some(holder_of(&lone, 7))), "the lone admin keeps both seats");
        let named = refusals(&spans);
        for text in [mesh2, mesh2, lone_text, lone_text] {
            assert!(named.iter().any(|(side, reason, detail)| side == "holder" && reason == "no-successor" && detail == text), "no holder reject span no-successor with {text:?}: {named:?}");
        }
        assert_eq!(named.len(), 4, "{named:?}");
        assert!(spans.named(ROW_WRITTEN).is_empty() && spans.named(CUT_OFF).is_empty(), "nothing cut off, no row");
        assert!(f.x.storage.seat_transfers().await.unwrap().is_empty() && g.x.storage.seat_transfers().await.unwrap().is_empty());
        let _ = (&f.m, &f.n, &g.m, &g.n, &g.p);
    });
}

/// CONTRACT (proposal Part C2: "Under mesh.delete the mesh seat does not move at all: the row commits
/// the fabric seat to the outside mesh-primary and the incumbent stays mesh-primary until its own
/// node.delete"; search step 2: "the lowest NodeId among the other ReadyForTraffic mesh-primaries"): P,
/// the fabric-primary, moves the fabric seat outside mesh1. Its one row commits the fabric seat to X,
/// mesh2's primary, at epoch 8 and moves no mesh seat; X installs and answers; P keeps mesh1's seat at
/// 3; both views name X the fabric-primary and P mesh1's primary. What must NOT happen: the fabric seat
/// going to N inside the departing mesh, or P's mesh seat moving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_seat_moved_outside_its_mesh_leaves_the_holder_mesh_primary() {
    let f = seats_fabric().await;
    let op = update_operation(Seat::FabricPrimary, "b1", "shutdown-mesh:mesh1");
    assert_eq!(f.p.door.update_primary(Moving::FabricSeatOutside, None, "b1", 1, &op).await, Ok(committed_fabric(&f.x, 8)));
    let rows = f.p.storage.seat_transfers().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!((&rows[0].holder, &rows[0].successor, &rows[0].seats), (&birth(&f.p), &birth(&f.x), &committed_fabric(&f.x, 8)), "the row lives under P's mesh and moves the fabric seat only");
    for a in [&f.p, &f.x] {
        assert_eq!((a.door.membership.seats().fabric(), a.door.membership.seats().mesh("mesh1")), (Some(holder_of(&f.x, 8)), Some(holder_of(&f.p, 3))), "{}: X holds the fabric seat, P keeps mesh1's", a.resolved.name);
        let t = a.door.topology.read().await;
        assert_eq!(fabric_primary_of(&t).as_deref(), Some("mesh2.admin.1"), "{}", a.resolved.name);
        assert_eq!(t.cohort_primary("mesh1", rafka_node_admin_core::model::NodeKind::NodeAdmin).map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.1"), "{}", a.resolved.name);
    }
    let _ = (&f.m, &f.n);
}

/// CONTRACT (elections §4.4: the committed seat record is authoritative resolver input; proposal C2
/// Q1): the one row is the resolver's input on both sides. When P's node.primary.update moving both
/// seats answers, P's view and N's view both name N as mesh1's primary and the fabric-primary. What
/// must NOT happen: either view still naming P after the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_views_name_the_successor_once_the_seat_row_is_committed() {
    let f = seats_fabric().await;
    f.p.door.update_primary(Moving::BothSeats, None, "b1", 1, &update_operation(Seat::MeshPrimary, "b1", "replace-node:mesh1.admin.1")).await.expect("both seats move");
    for a in [&f.p, &f.n] {
        let t = a.door.topology.read().await;
        assert_eq!(fabric_primary_of(&t).as_deref(), Some("mesh1.admin.2"), "{}: the fabric-primary is N", a.resolved.name);
        assert_eq!(t.cohort_primary("mesh1", rafka_node_admin_core::model::NodeKind::NodeAdmin).map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.2"), "{}: mesh1's primary is N", a.resolved.name);
    }
    let _ = (&f.m, &f.x);
}

/// The file name of `operation`'s seat-transfer row (`fabric_storage::transfer_key`).
fn transfer_file(operation: &str) -> String {
    format!("{}.json", operation.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' }).collect::<String>())
}

/// CONTRACT (proposal Part C1 failure contract: "The holder lost after the row, before 'updated': the
/// successor and every reader take the seat from the row"; "Discovery is not installation. A later
/// attempt that reads the committed row has discovered the commit, not installed it. The resuming
/// executor installs the committed view locally and obtains the successor's `node.primary.updated`
/// (its installation acknowledgement) before it commands the departing holder's destructive step"):
/// every admin's fabric.storage is on the fabric's one store root. The fabric-primary X commands P to
/// update mesh1's seat; P writes the row under its mesh's directory (`meshes/mesh1/seat-transfers/`)
/// and takes the row's seat, and its process is gone before it calls N. X's command ends at that
/// Indeterminate, incomplete and named; X installs nothing, and N, never called, has installed
/// nothing. Once P's birth is proven gone, X's next attempt for an operation no row records finds none
/// and ends by name (no update happened), and its next attempt of b1 point-reads P's row, calls
/// node.primary.update on N carrying it, and installs only once N answered node.primary.updated: N's
/// install descends from X's row read. P's exit is proven by the provider's exit-proof row X records
/// among mesh1's status rows on the store root (elections §4.1: only the provider's exit proof vacates),
/// so N, from the row's epoch mesh1's row writer, reads that row of the birth its row names as holder
/// and says "primary updated" once, under X's row read (Part C1 failure contract: "The holder lost
/// after the row, before 'updated': ... the next writer to read the row publishes 'updated'.
/// Completion needs neither gossip delivery nor the holder's survival"). What must NOT happen: a
/// command that never ends, a seat installed with no row, X continuing on its read of the row alone,
/// or nobody saying "primary updated" for the committed row.
#[test]
fn a_holder_lost_after_the_row_leaves_the_command_incomplete_and_the_next_attempt_obtains_node_primary_updated() {
    // The cell's spans: the in-memory names the waits read, and the product span pipeline
    // (`rafka_mesh_telemetry::span_capture`) stamping the trace ids the proof names, both the cell's
    // own on its own threads.
    let cap = trace_capture("mesh2.admin.1");
    cap.run(async {
        let story = tracing::info_span!("rdm.test.seat.update.via-lost-holder-row");
        let trace_id = story.in_scope(rafka_mesh_telemetry::current_traceparent).and_then(|tp| tp.split('-').nth(1).map(str::to_string)).expect("the cell's root span has a trace context");
        let root = DataDir::new("primary-update-root");
        let f = seats_fabric_with(FabricSeat::X, || {
            let at = rafka_node_admin_core::store_root::StoreRoot::at(&root.0);
            async move { Arc::new(FileFabricStorage::open(&at, &IncarnationId::mint()).await.unwrap()) as Arc<dyn FabricStorage> }
        })
        .await;
        let p_node = f.x.door.topology.read().await.members().find(|x| x.name.to_string() == "mesh1.admin.1").cloned().unwrap();
        let op = "node.primary.update:b1:stop-node:mesh1.admin.1";
        // P holds its view projection once it wrote its row and took the row's seat: its process dies there,
        // before its call on N.
        f.p.view_open.send_replace(false);
        let first = tokio::spawn({
            let (door, p_node) = (f.x.door.clone(), p_node.clone());
            tracing::Instrument::instrument(async move { door.command_update_primary(&p_node, "b1", 1, op).await }, story.clone())
        });
        until("P wrote its row and took the row's seat", || f.p.door.membership.seats().mesh("mesh1") == Some(holder_of(&f.n, 4))).await;
        let row_file = root.0.join("meshes").join("mesh1").join("seat-transfers").join(transfer_file(op));
        assert!(row_file.exists(), "P's seat-transfer row is not under its mesh's directory on the root: {}", row_file.display());
        assert!(!root.0.join("fabric").join("seats").exists(), "a per-seat row was written under fabric/seats");
        let Seats { p, n, m, x } = f;
        let _ = p._router.shutdown().await;
        let out = tokio::time::timeout(std::time::Duration::from_secs(20), first).await.expect("the command to a holder lost mid-call ends: it is never sent again on its own").unwrap();
        let e = out.expect_err("an uncertain command is no answer").to_string();
        assert!(e.contains("Indeterminate") && e.contains("mesh1.admin.1") && e.contains(op), "the end names the outcome, the holder and the operation: {e}");
        assert_eq!(x.door.membership.seats().mesh("mesh1"), Some(holder_of(&p, 3)), "X installs nothing on an uncertain call");
        assert_eq!(n.door.membership.seats().mesh("mesh1"), Some(holder_of(&p, 3)), "N was never called: it installed nothing");
        // P's birth is proven gone: X records the provider's exit proof, P's exit-proof row among mesh1's
        // status rows on the store root, and X's projection reads it.
        x.door.records.put_exit(&p_node, None, "provider").await.expect("X records P's exit-proof row");
        x.door.records.read_exits(&x.door.book).await;
        let other = "node.primary.update:b9:stop-node:mesh1.admin.1";
        let out = tokio::time::timeout(std::time::Duration::from_secs(20), tracing::Instrument::instrument(x.door.command_update_primary(&p_node, "b9", 1, other), story.clone())).await.expect("a proven-lost holder is not called");
        let e = out.expect_err("no row records an update for b9").to_string();
        assert!(e.contains(other) && e.contains("no seat row") && e.contains("mesh1.admin.1"), "{e}");
        assert_eq!(x.door.membership.seats().mesh("mesh1"), Some(holder_of(&p, 3)), "no row, nothing installed");
        // The next attempt of b1: X reads P's row, and N installs only on X's call.
        let out = tokio::time::timeout(std::time::Duration::from_secs(20), tracing::Instrument::instrument(x.door.command_update_primary(&p_node, "b1", 2, op), story.clone())).await.expect("a proven-lost holder is not called");
        assert_eq!(out, Ok(committed_mesh(&n, 4)), "the next attempt installs once N answered node.primary.updated");
        for a in [&x, &n] {
            assert_eq!(a.door.membership.seats().mesh("mesh1"), Some(holder_of(&n, 4)), "{} holds the seat P's row names", a.resolved.name);
        }
        assert_eq!(x.door.topology.read().await.cohort_primary("mesh1", rafka_node_admin_core::model::NodeKind::NodeAdmin).map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.2"), "X's view names N mesh1's primary");
        let _ = m;
        // The proof: the incomplete command, the no-row end, the row read, N's install under it and X's
        // install after it are the cell's own story.
        drop(story);
        cap.capture.flush(std::time::Duration::from_secs(10)).expect("the cell's spans reach its exporter");
        let finished = cap.exporter.get_finished_spans().unwrap();
        let attr = |s: &opentelemetry_sdk::export::trace::SpanData, k: &str| s.attributes.iter().find(|kv| kv.key.as_str() == k).map(|kv| kv.value.to_string());
        println!("trace_id={trace_id}");
        for s in finished.iter().filter(|s| s.span_context.trace_id().to_string() == trace_id) {
            println!("span {} {:?} {:?}", s.name, attr(s, "outcome").or_else(|| attr(s, "reason")), attr(s, "operation"));
        }
        let ours = |name: &str| finished.iter().filter(|s| s.name == name && s.span_context.trace_id().to_string() == trace_id).collect::<Vec<_>>();
        let commands: Vec<(Option<String>, Option<String>)> = ours("rdm.node_admin.seat.update.via-primary-update").iter().map(|s| (attr(s, "operation"), attr(s, "outcome"))).collect();
        assert_eq!(commands, vec![(Some(op.to_string()), Some("incomplete".to_string())), (Some(other.to_string()), Some("no-update".to_string())), (Some(op.to_string()), Some("updated".to_string()))], "{commands:?}");
        let reads = ours("rdm.node_admin.seat.update.via-committed-row");
        let read = |o: &str| reads.iter().find(|s| attr(s, "outcome").as_deref() == Some(o)).cloned().unwrap_or_else(|| panic!("no committed-row read with outcome {o}: {:?}", reads.iter().map(|s| attr(s, "outcome")).collect::<Vec<_>>()));
        let held = read("held");
        assert_eq!(attr(held, "successor").as_deref(), Some(format!("mesh1:{}:{}", n.resolved.node_id, n.resolved.incarnation.as_str()).as_str()));
        assert_eq!(attr(read("no-row"), "operation").as_deref(), Some(other));
        let installed_at_n: Vec<_> = ours(UPDATED).into_iter().filter(|s| attr(s, "node").as_deref() == Some("mesh1.admin.2")).collect();
        assert_eq!(installed_at_n.len(), 1, "N installed once, on X's call: {:?}", installed_at_n.iter().map(|s| attr(s, "replay")).collect::<Vec<_>>());
        // N's install descends from X's row read: its ancestry, across the call, reaches that span.
        let parent_of = |id: &str| finished.iter().find(|s| s.span_context.span_id().to_string() == id).map(|s| s.parent_span_id.to_string());
        let (read_id, mut at) = (held.span_context.span_id().to_string(), installed_at_n[0].parent_span_id.to_string());
        let mut under_read = false;
        for _ in 0..64 {
            if at == read_id {
                under_read = true;
                break;
            }
            let Some(up) = parent_of(&at) else { break };
            at = up;
        }
        assert!(under_read, "N's install is the answer to X's call under its row read");
        let x_installed: Vec<_> = ours(INSTALLED).into_iter().filter(|s| attr(s, "via").as_deref() == Some("committed-row")).collect();
        assert_eq!(x_installed.len(), 1);
        assert!(x_installed[0].start_time >= installed_at_n[0].end_time, "X installs only after N answered node.primary.updated");
        // N read P's exit-proof row (the birth its row names as holder) and, the next writer to read the
        // row, said "primary updated" once, under X's row read; P, lost before its call, said nothing.
        assert_eq!(attr(installed_at_n[0], "holder_exit").as_deref(), Some("proven"), "N read the exit-proof row of the row's holder");
        let said: Vec<_> = ours(ANNOUNCE_UPDATED);
        assert_eq!(said.iter().map(|s| (attr(s, "node"), attr(s, "operation"))).collect::<Vec<_>>(), vec![(Some("mesh1.admin.2".to_string()), Some(op.to_string()))], "N, never P, says the primary updated, once");
        let mut at = said[0].parent_span_id.to_string();
        let mut said_under_read = false;
        for _ in 0..64 {
            if at == read_id {
                said_under_read = true;
                break;
            }
            let Some(up) = parent_of(&at) else { break };
            at = up;
        }
        assert!(said_under_read, "N says it on its read of the row, X's call under X's row read");
    });
}

/// CONTRACT (proposal Part C1 failure contract: "After the row, the successor is the committed holder
/// whatever the call returns ... A call that reached a process other than the named birth, a refusal
/// or no answer is never an exit proof (elections §4.1: only the provider's exit proof vacates a seat;
/// R-SW1: a failed probe never proves death). The holder's call fails by name, the workflow runs
/// node-recovery's tickle ladder on the successor's exact birth and retries the call against the
/// resolved birth; the seat vacates only on the provider's exit proof, and then the ordinary rule fills
/// it (lowest ReadyForTraffic NodeId of the cohort). No row is rewritten"; "Indeterminate after the
/// row: the row resolves it. An identical retry of the call replays the committed result"; R14 item 4:
/// no write of a seat lands after the row that ends its writer's authority): the fabric-primary X
/// commands P; P writes the row, takes the row's seat and calls N; N installs from the row and its
/// process is gone before it answers. P's call ends by name with no answer, which proves nothing: P
/// runs the tickle ladder on N's exact birth, and its direct ping and its kick through M both find no
/// answer, so P asks the provider, which proves N's runtime exited. P puts N's exit-proof row (proof
/// `provider`) and retries nothing: N's seat vacates and P's view seats the ordinary rule's choice,
/// the lowest ReadyForTraffic NodeId of mesh1's node-admins not proven gone (P itself, in this
/// fabric). P's update ends `Incomplete` naming the proof and X's command is answered by P with it,
/// X installing nothing. X's next attempt sends the identical command: P reads its row, finds N's
/// exit proven and calls N no more: no second update under the operation. What must NOT happen: a
/// retry or a second look at a birth proven exited, X's own deadline ending its command before P
/// answers, a cut-off lifted after the row, a second row, or a seat record moved off the row's.
#[test]
fn a_successor_proven_exited_after_the_row_vacates_its_seat_for_the_ordinary_rule_and_is_called_no_more() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    let _file = SpanFile(spans.clone());
    let (trace_id, ()) = cap.story("successor_proven_exited", async {
        let f = commanded_fabric().await;
        let p_node = f.x.door.topology.read().await.members().find(|x| x.name.to_string() == "mesh1.admin.1").cloned().unwrap();
        let op = "node.primary.update:b1:stop-node:mesh1.admin.1";
        // N's runtime, as recorded at its launch in P's nodes.storage: the exact runtime P's provider
        // inspects.
        let deployment = rafka_node_admin_core::model::DeploymentId::mint().to_string();
        let runtime = rafka_mesh_entity::RuntimeFact { deployment_id: deployment.clone(), provider: rafka_mesh_entity::RuntimeProvider::Process, control_domain: "fabric-handover".into(), locator: rafka_mesh_entity::RuntimeLocator::Process { pid: 4242, start: 1 } };
        let row = rafka_node_admin_core::storage::RuntimeRow { node_id: f.n.resolved.node_id.clone(), name: f.n.resolved.name.clone(), incarnation_id: f.n.resolved.incarnation.clone(), runtime, data_dir: None };
        rafka_node_admin_core::storage::NodesStorage::put_runtime(&*f.p.nodes, &row).await.unwrap();
        // N installs from the row and never answers: its process dies there.
        f.n.view_open.send_replace(false);
        let first = tokio::spawn({
            let (door, p_node) = (f.x.door.clone(), p_node.clone());
            tracing::Instrument::instrument(async move { door.command_update_primary(&p_node, "b1", 1, op).await }, tracing::Span::current())
        });
        until("N installed mesh1's seat from P's row", || f.n.door.membership.seats().mesh("mesh1") == Some(holder_of(&f.n, 4))).await;
        let Seats { p, n, m, x } = f;
        let (n_id, p_id) = (n.resolved.node_id.to_string(), p.resolved.node_id.to_string());
        // N's process is gone: its endpoint closed, its runtime exited.
        p.provider.exited.lock().unwrap().insert(deployment.clone());
        let _ = n._router.shutdown().await;
        let calls = |to: &str| -> Vec<String> { spans.named(CALL).into_iter().filter(|f| f.get("op").map(String::as_str) == Some("33") && f.get("target").is_some_and(|t| t.contains(to))).map(|f| f.get("outcome").cloned().unwrap_or_else(|| "open".into())).collect() };
        let call_lines = || spans.lines().into_iter().filter(|l| l.contains(CALL)).collect::<Vec<_>>().join("\n");
        let out = tokio::time::timeout(std::time::Duration::from_secs(20), first).await.expect("an update whose successor was lost mid-call ends: it is never sent again on its own").unwrap();
        let e = out.expect_err("a successor proven exited answered nothing").to_string();
        assert!(e.contains("incomplete") && e.contains("exited") && e.contains(&n_id) && e.contains(op), "the end is P's answer naming the proof, the successor and the operation: {e}\nthe 0x21 calls:\n{}", call_lines());
        let (to_p, to_n) = (calls(&p_id), calls(&n_id));
        assert!(to_p == ["Reply"] && to_n.len() == 1 && to_n[0] != "Reply" && to_n[0] != "open", "X's command answered by P; P's one call to N ended by name and was retried no more:\n{}", call_lines());
        let looks = spans.named(LADDER);
        assert_eq!(looks.len(), 1, "one look at N's birth: {looks:?}");
        let field = |k: &str| looks[0].get(k).cloned().unwrap_or_default();
        assert_eq!((field("node"), field("side"), field("finding"), field("retry"), field("exit_proof_row")), ("mesh1.admin.1".to_string(), "holder".to_string(), "exit-proven".to_string(), "none".to_string(), "put".to_string()), "{:?}", looks[0]);
        assert_ne!(field("call"), "", "the look names how P's call ended: {:?}", looks[0]);
        let exit = p.door.records.exit_row_of_birth("mesh1", &n.resolved.node_id, &n.resolved.incarnation, "N").await.unwrap().expect("P put N's exit-proof row among mesh1's status rows");
        assert_eq!(exit.proof, "provider");
        assert!(matches!(p.cut_offs.lock().unwrap().as_slice(), [WriteCutOff::Set { .. }, WriteCutOff::Committed { fabric_seat: false, .. }]), "the row ended P's authority over the cut-off writes: {:?}", p.cut_offs.lock().unwrap());
        assert_eq!(p.door.membership.seats().mesh("mesh1"), Some(holder_of(&n, 4)), "the seat record stays the row's: no row is rewritten");
        assert_eq!(x.door.membership.seats().mesh("mesh1"), Some(holder_of(&p, 3)), "X installs nothing on an incomplete update");
        let primary = p.door.topology.read().await.cohort_primary("mesh1", rafka_node_admin_core::model::NodeKind::NodeAdmin).map(|n| n.name.to_string());
        assert_eq!(primary.as_deref(), Some("mesh1.admin.1"), "N's seat vacated on the proof and P's view seats the ordinary rule's choice, the lowest ReadyForTraffic NodeId of mesh1's node-admins not proven gone");
        // X's next attempt: P replays from its row, N's exit is proven, and N is called no more.
        let out = tokio::time::timeout(std::time::Duration::from_secs(20), x.door.command_update_primary(&p_node, "b1", 2, op)).await.expect("the next attempt ends");
        let e = out.expect_err("no second update under the operation").to_string();
        assert!(e.contains(op) && e.contains(&n_id) && e.contains("exit is proven"), "{e}");
        assert_eq!((calls(&p_id).len(), calls(&n_id).len()), (2, 1), "the next command answered by P, which called N no more:\n{}", call_lines());
        assert_eq!(spans.named(LADDER).len(), 1, "no second look at a birth proven exited");
        assert!(refusals(&spans).iter().any(|(side, reason, _)| side == "holder" && reason == "exit-proven"), "{:?}", refusals(&spans));
        assert_eq!(p.storage.seat_transfers().await.unwrap().len(), 1, "no row is rewritten");
        assert_eq!(spans.named(ROW_WRITTEN).len(), 1, "one row written");
        let _ = m;
    });
    println!("cell=successor_proven_exited trace_id={trace_id}");
}

/// CONTRACT (proposal Part C1 failure contract: "After the row, the successor is the committed holder
/// whatever the call returns ... A call that reached a process other than the named birth, a refusal
/// or no answer is never an exit proof (elections §4.1: only the provider's exit proof vacates a seat;
/// R-SW1: a failed probe never proves death). The holder's call fails by name, the workflow runs
/// node-recovery's tickle ladder on the successor's exact birth and retries the call against the
/// resolved birth"; step 7: the holder says "primary updated" once the successor answered
/// node.primary.updated): P holds both its seats and moves them to N by one row. N's door holds no
/// view when P's call reaches it: N refuses the call `NotReady`, a refusal, which proves nothing of N's
/// birth. P looks at N's exact birth with the tickle ladder (node-recovery.md step 2): its direct ping
/// is answered, N's door holding its view by then. P retries the call once against that birth, N
/// installs from the row and answers node.primary.updated, and P says "primary updated" once, after
/// N's answer. What must NOT happen: the update ending `Incomplete` while N answers, a second row, a
/// second look or a second retry, N installing twice, or "primary updated" said before N's answer or
/// twice.
#[test]
fn a_successor_that_refuses_the_holders_call_is_looked_at_and_the_call_is_retried_once_against_its_answering_birth() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    let _file = SpanFile(spans.clone());
    let (trace_id, ()) = cap.story("successor_ladder_retry", async {
        let f = seats_fabric_n_ready_on_ping(FabricSeat::P).await;
        let op = update_operation(Seat::MeshPrimary, "b1", "stop-node:mesh1.admin.1");
        let n_id = f.n.resolved.node_id.to_string();
        let out = tokio::time::timeout(std::time::Duration::from_secs(20), f.p.door.update_primary(Moving::BothSeats, None, "b1", 1, &op)).await.expect("the update ends");
        let calls: Vec<String> = spans.named(CALL).into_iter().filter(|c| c.get("op").map(String::as_str) == Some("33") && c.get("target").is_some_and(|t| t.contains(&n_id))).map(|c| c.get("outcome").cloned().unwrap_or_else(|| "open".into())).collect();
        assert_eq!(out, Ok(committed_both(&f.n, 4, 8)), "P's update completes on N's answer to the retried call: calls to N {calls:?}, refusals {:?}\n{}", refusals(&spans), spans.lines().join("\n"));
        assert_eq!(calls, vec!["Reply".to_string(), "Reply".to_string()], "P called N twice: the refused call and its one retry");
        let looks = spans.named(LADDER);
        assert_eq!(looks.len(), 1, "one look at N's birth: {looks:?}");
        let look = &looks[0];
        let field = |k: &str| look.get(k).cloned().unwrap_or_default();
        assert_eq!((field("node"), field("side"), field("call"), field("finding"), field("retry")), ("mesh1.admin.1".to_string(), "holder".to_string(), "refused".to_string(), "answered".to_string(), "primary-updated".to_string()), "{look:?}");
        assert!(field("successor").contains(&n_id) && field("successor").contains(f.n.resolved.incarnation.as_str()), "the look names N's exact birth: {look:?}");
        assert_eq!(spans.named(ROW_WRITTEN).len(), 1, "one row");
        assert_eq!(f.p.storage.seat_transfers().await.unwrap().len(), 1, "one row on the store root");
        assert_eq!(spans.named(UPDATED).len(), 1, "N installed from the row once, on the retried call");
        assert_eq!(spans.named(ANNOUNCE_UPDATED).iter().map(|s| s["node"].clone()).collect::<Vec<_>>(), vec!["mesh1.admin.1".to_string()], "P says the primary updated once");
        let at = |name: &str| spans.position(name).unwrap_or_else(|| panic!("no {name}"));
        assert!(at(ROW_WRITTEN) < at(LADDER) && at(LADDER) < at(UPDATED) && at(UPDATED) < at(ANNOUNCE_UPDATED), "row, look, N's install, then primary updated:\n{}", spans.lines().join("\n"));
        assert!(refusals(&spans).is_empty(), "the update was refused nowhere: {:?}", refusals(&spans));
        for a in [&f.p, &f.n] {
            assert_eq!((a.door.membership.seats().mesh("mesh1"), a.door.membership.seats().fabric()), (Some(holder_of(&f.n, 4)), Some(holder_of(&f.n, 8))), "{} holds both seats as the row names them", a.resolved.name);
        }
        let _ = (&f.m, &f.x);
    });
    println!("cell=successor_ladder_retry trace_id={trace_id}");
}

/// CONTRACT (proposal Part C3 edit 5: "An identical repeat replays the committed result";
/// node-rpc-envelope.md "Caller execution and certainty": "A later attempt resumes from the recorded
/// receipts ... and validates those receipts against the accepted operation and exact births before it
/// reuses them"): P's row committed mesh1's seat to N in attempt 1; the same update sent to N in
/// attempt 2 (its operation, its Build and both births unchanged) replays the row. The same operation
/// under another Build is RejectedConflict. What must NOT happen: a later attempt refused as a conflict
/// with its own committed row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_attempt_of_the_same_update_is_answered_from_the_row() {
    let f = commanded_fabric().await;
    let op = "node.primary.update:b1:replace-node:mesh1.admin.1";
    let first = send(&f.x, &f.p, update_mesh_seat(&f.p, 3, "b1", op)).await;
    assert_eq!(first, updated(committed_mesh(&f.n, 4)));
    assert_eq!(send(&f.p, &f.n, primary_update(&f.p, &f.n, 4, "b1", 2, op)).await, first, "attempt 2 of the same update replays the row");
    assert_eq!(send(&f.p, &f.n, primary_update(&f.p, &f.n, 4, "b2", 1, op)).await, Reply::RejectedConflict { operation: op.into() }, "another Build under the same operation conflicts");
    assert_eq!(f.n.storage.seat_transfers().await.unwrap().len(), 1, "one row");
    let _ = (&f.m, &f.x);
}

/// CONTRACT (proposal Part C1: one row per update, keyed by its operation; node-rpc-envelope.md op
/// 0x21: "operation identifies one" update): a seat update's operation names its Build, so a later
/// Build's update of the same seat before the same node operation is a new update, never the replay
/// of an earlier one. Build b1's stop of P moves mesh1's seat to N at epoch 4; the seat comes back to
/// P at epoch 5 (as a later update leaves it in every book); Build b2's stop of P moves it again: P
/// writes a second row at epoch 6 and P and N hold N at 6. What must NOT happen: b2's update answered
/// from b1's row (epoch 4), so the seat never moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_builds_update_of_the_same_seat_is_a_new_update() {
    let f = commanded_fabric().await;
    let first = update_operation(Seat::MeshPrimary, "b1", "stop-node:mesh1.admin.1");
    assert_eq!(f.p.door.update_primary(Moving::MeshSeat, None, "b1", 1, &first).await, Ok(committed_mesh(&f.n, 4)));
    for a in [&f.p, &f.n, &f.m, &f.x] {
        a.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&f.p, 5));
    }
    let second = update_operation(Seat::MeshPrimary, "b2", "stop-node:mesh1.admin.1");
    assert_ne!(first, second, "each Build's update is its own operation");
    assert_eq!(f.p.door.update_primary(Moving::MeshSeat, None, "b2", 1, &second).await, Ok(committed_mesh(&f.n, 6)), "b2's update moves the seat again");
    for a in [&f.p, &f.n] {
        assert_eq!(a.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.n, 6)), "{} holds N at epoch 6", a.resolved.name);
    }
    assert_eq!(f.n.storage.seat_transfers().await.unwrap().len(), 2, "one row per update");
}

/// CONTRACT (`HandoverDoor::decisions`: "a seat commit here holds it across the record's move, so a
/// decision in flight commits wholly before the record moves"; proposal Part C1 step 5: "This is the
/// commit. From this write the holder is no longer primary"): P, the fabric-primary, moves both seats
/// to N; while P's row is on its way to the disk, P's Build-log decision lock is held, so no claim is
/// decided by P across the record's move; once the row is written and P took its seats, the lock is
/// free and P names N. What must NOT happen: P's decision lock free while its row is being written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_holders_build_decisions_are_held_while_it_writes_the_seat_row() {
    let store = Arc::new(FailingStorage::default());
    let f = seats_fabric_with(FabricSeat::P, {
        let store = store.clone();
        move || {
            let store = store.clone();
            async move { store as Arc<dyn FabricStorage> }
        }
    })
    .await;
    store.put_open.send_replace(false);
    let moving = tokio::spawn({
        let door = f.p.door.clone();
        async move { door.update_primary(Moving::BothSeats, None, "b1", 1, &update_operation(Seat::MeshPrimary, "b1", "replace-node:mesh1.admin.1")).await }
    });
    until("P's row is on its way to the disk", || store.puts.load(std::sync::atomic::Ordering::SeqCst) == 1).await;
    assert!(f.p.door.decisions.try_lock().is_err(), "P's Build-log decisions are open while its row is on its way to the disk");
    assert_eq!(f.p.door.membership.seats().fabric(), Some(holder_of(&f.p, 7)), "nothing moved before the row");
    store.put_open.send_replace(true);
    assert_eq!(moving.await.unwrap(), Ok(committed_both(&f.n, 4, 8)));
    assert_eq!(f.p.door.membership.seats().fabric(), Some(holder_of(&f.n, 8)), "P took its row's seats");
    assert!(f.p.door.decisions.try_lock().is_ok(), "the decision lock is free once P took its row's seats");
    let _ = (&f.m, &f.x);
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
        stores.apply(WriteCutOff::Set { reason: "node.primary.update:b1:stop-node:mesh1.admin.1".into(), mesh_seat: true, fabric_seat: false }).await;
        match stores.status.put_mesh_status(&row).await {
            Err(rafka_node_admin_core::record_store::StorageError::CutOff { reason, .. }) => assert!(reason.contains("node.primary.update:b1:stop-node:mesh1.admin.1"), "{reason}"),
            other => panic!("a status write after the moving seat's cut-off: {other:?}"),
        }
        stores.apply(WriteCutOff::Lift).await;
        stores.status.put_mesh_status(&row).await.expect("a lift gives the status writes back");
        let _ = std::fs::remove_dir_all(&d);
    });
}

/// CONTRACT (proposal Part C1 step 4: "The mesh seat's cut-off covers the mesh's rows (`meshes/<mesh>`,
/// `nodes/<mesh>/`); the fabric seat's covers `fabric/` and Build acceptance"; Part C2: "Under
/// mesh.delete the mesh seat does not move at all ... the incumbent stays mesh-primary until its own
/// node.delete"): node.fabric-primary.update moving the fabric seat alone cuts off the fabric seat's
/// stores and none of the holder's mesh. Through the cut-off a Fabric record write is refused naming the
/// transfer, while the holder, still its mesh's primary, writes its mesh's status row (the Leaving row
/// mesh.delete's leave writes next); after the row commits it still writes its mesh's status and Mesh
/// rows. What must NOT happen: the holder's mesh rows cut off by a move of the fabric seat alone, so the
/// mesh.delete it precedes refuses delete-mesh on its own Leaving row.
#[test]
fn a_fabric_seat_moved_alone_cuts_off_no_store_of_the_holders_mesh() {
    use rafka_node_admin_core::fabric_handover::SeatStores;
    use rafka_node_admin_core::status_storage::{MeshStatusRow, StatusStorage as _};
    use rafka_node_admin_core::storage::MeshStorage as _;
    use rafka_node_rpc_contract::status::MeshState;
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let d = std::env::temp_dir().join(format!("seat-stores-fabric-alone-{}", NodeId::mint()));
        let root = rafka_node_admin_core::store_root::StoreRoot::at(&d.join("store"));
        let inc = IncarnationId::mint();
        let stores = SeatStores {
            mesh: Arc::new(rafka_node_admin_core::storage::FileMeshStorage::open(&root, &inc).await.unwrap()),
            nodes: Arc::new(rafka_node_admin_core::storage::FileNodesStorage::open(&d.join("data"), &root, "mesh1", &inc).await.unwrap()),
            fabric: Arc::new(FileFabricStorage::open(&root, &inc).await.unwrap()),
            status: Arc::new(rafka_node_admin_core::status_storage::FileStatusStorage::open(&root, "mesh1", &inc).await.unwrap()),
        };
        let reason = "the fabric-primary seat(s) of mesh1.admin.1 are updating to mesh2 000000000002 by node.fabric-primary.update:b1:shutdown-mesh:mesh1";
        let mesh_id = MeshId::mint();
        stores.apply(WriteCutOff::Set { reason: reason.into(), mesh_seat: false, fabric_seat: true }).await;
        match stores.fabric.claim_identity(&FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() }).await {
            Err(e) => assert!(e.to_string().contains("node.fabric-primary.update:b1:shutdown-mesh:mesh1"), "the refusal names the transfer: {e}"),
            Ok(claim) => panic!("a Fabric record write after the fabric seat's cut-off: {claim:?}"),
        }
        stores.status.put_mesh_status(&MeshStatusRow { mesh_id: mesh_id.clone(), state: MeshState::Leaving }).await.expect("the holder writes its mesh's Leaving row through the fabric seat's cut-off");
        stores.apply(WriteCutOff::Committed { reason: reason.into(), mesh_seat: false, fabric_seat: true }).await;
        stores.status.put_mesh_status(&MeshStatusRow { mesh_id: mesh_id.clone(), state: MeshState::Dead }).await.expect("the holder, still mesh1's primary, writes its mesh's status row after the row");
        stores.mesh.claim_mesh(&rafka_node_admin_core::storage::MeshRecord { mesh_id, name: "mesh1".into(), dead: true }).await.expect("the holder writes its Mesh row after the row").expect("mesh1 is a free name");
        let _ = std::fs::remove_dir_all(&d);
    });
}

/// CONTRACT (R14 item 4; proposal Part C1 step 5: "From this write the holder is no longer
/// primary"): a holder that opened its stores before the fabric's first state-commit holds what
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
        stores.mesh.claim_mesh(&rafka_node_admin_core::storage::MeshRecord { mesh_id: rafka_node_admin_core::model::MeshId::mint(), name: "mesh1".into(), dead: false }).await.unwrap().unwrap();
        stores.status.put_mesh_status(&MeshStatusRow { mesh_id: rafka_node_admin_core::model::MeshId::mint(), state: rafka_node_rpc_contract::status::MeshState::ReadyForTraffic }).await.unwrap();
        stores.fabric.claim_identity(&rafka_node_admin_core::fabric_storage::FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() }).await.unwrap().unwrap();
        assert!(stores.mesh.mesh("mesh1").await.unwrap().is_some(), "the holder reads the Mesh row it holds");
        let reason = "the seats of mesh1.admin.1 moved to mesh1 000000000002 by node.primary.update:b1:stop-node:mesh1.admin.1";
        stores.apply(WriteCutOff::Set { reason: reason.into(), mesh_seat: true, fabric_seat: true }).await;
        stores.apply(WriteCutOff::Committed { reason: reason.into(), mesh_seat: true, fabric_seat: true }).await;
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
/// primary of a mesh with no primary, an authority its fabric seat gives it; mesh-delete.md step 7):
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
        let row = NodeRow { name: "mesh2.rpc.1".parse().unwrap(), node_id: NodeId::mint(), status: NodeStatus::Deleted, config: Default::default(), tags: Default::default(), allocated: None, renamed_from: None, inherited_by: None };
        let dead = MeshStatusRow { mesh_id: MeshId::mint(), state: rafka_node_rpc_contract::status::MeshState::Dead };
        stores.nodes.put_node_row(&row).await.unwrap().expect("mesh2.rpc.1 is a free key");
        stores.status.put_mesh_status_of("mesh2", &dead).await.unwrap();
        assert_eq!(stores.nodes.node_row_at(&row.name).await.unwrap(), Some(row.clone()), "the holder reads the mesh2 Node row it holds");
        assert!(stores.status.mesh_status_held_of("mesh2", &dead).await.unwrap(), "the holder reads the mesh2 status row it holds");
        let reason = "the seats of mesh1.admin.1 moved to mesh1 000000000002 by node.primary.update:b1:stop-node:mesh1.admin.1";
        stores.apply(WriteCutOff::Set { reason: reason.into(), mesh_seat: true, fabric_seat: true }).await;
        stores.apply(WriteCutOff::Committed { reason: reason.into(), mesh_seat: true, fabric_seat: true }).await;
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


/// The commanded seats fabric with every admin over one [`FailingStorage`], the fabric's one store
/// root (R14), which is returned.
async fn failing_seats() -> (Seats, Arc<FailingStorage>) {
    let store = Arc::new(FailingStorage::default());
    let s = seats_fabric_with(FabricSeat::X, {
        let store = store.clone();
        move || {
            let store = store.clone();
            async move { store as Arc<dyn FabricStorage> }
        }
    })
    .await;
    (s, store)
}

/// CONTRACT (proposal Part C1 failure contract: "The seat-row write fails between the drain and the
/// commit ... A named error is a confirmed non-commit: the step fails by name naming the error, and
/// NA1 lifts its cut-off under the unchanged epoch, because a primary that cannot write is a worse
/// state than a failed step"; Part E: "seat-row write fails after the drain: the step fails naming the
/// error and NA1 writes again under the unchanged epoch"; CLAUDE.md error rule): the fabric-primary X
/// commands P.
/// - The store cannot read the operation's row: P refuses `RejectedSourceFailed` naming it before
///   anything is cut off.
/// - The store refuses P's row after the cut-off: P answers `RejectedSourceFailed` naming the row and the
///   store's error, lifts its cut-off (`Set` then `Lift`) and is still mesh1's primary at epoch 3; N
///   is never called. Once the store writes again, X's next attempt of the same update has P write
///   the row under the unchanged epoch: the seat moves to N at 4.
/// Each refusal is the holder's reject span `source-failed` with the three fields.
#[test]
fn a_seat_row_write_that_fails_after_the_cut_off_fails_the_step_by_name_and_the_holder_writes_again_under_the_unchanged_epoch() {
    let cap = crate::mesh_delete_resume::capture();
    let seats_op = "stop-node:mesh1.admin.1";
    let (replies, trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.seat.reject.via-source-failed");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let replies = tracing::Instrument::instrument(
            async {
                let mut replies = Vec::new();
                let unmoved = |s: &Seats| [&s.p, &s.n].iter().all(|a| a.door.membership.seats().mesh("mesh1") == Some(holder_of(&s.p, 3)));
                let (s, store) = failing_seats().await;
                store.failing("seat_transfer");
                replies.push(("row-read", send(&s.x, &s.p, update_mesh_seat(&s.p, 3, "b1", seats_op)).await));
                assert!(s.p.cut_offs.lock().unwrap().is_empty() && unmoved(&s), "nothing is cut off or moved");
                let (s, store) = failing_seats().await;
                store.failing("put_seat_transfer");
                replies.push(("row-put", send(&s.x, &s.p, update_mesh_seat(&s.p, 3, "b1", seats_op)).await));
                assert!(matches!(s.p.cut_offs.lock().unwrap().as_slice(), [WriteCutOff::Set { .. }, WriteCutOff::Lift]), "the cut-off is lifted when the row was not written: {:?}", s.p.cut_offs.lock().unwrap());
                assert!(unmoved(&s), "no seat moves: P is still primary under the unchanged epoch");
                // The store writes again: the next attempt writes the row under the unchanged epoch.
                store.mended("put_seat_transfer");
                replies.push(("again", send(&s.x, &s.p, update_mesh_seat(&s.p, 3, "b1", seats_op)).await));
                assert_eq!(s.p.door.membership.seats().mesh("mesh1"), Some(holder_of(&s.n, 4)), "the seat moves at the epoch after the unchanged one");
                let _ = (&s.m, &s.x);
                replies
            },
            root,
        )
        .await;
        (replies, trace_id)
    });
    let spans = cap.spans();
    keep_cell("primary_update_source_failed", &trace_id, &spans);
    let reply = |case: &str| replies.iter().find(|(c, _)| *c == case).map(|(_, r)| r.clone()).unwrap();
    let (source, op_, cause) = source_failed(&reply("row-read"));
    assert!(source.contains("fabric.storage") && source.contains("mesh1.admin.1") && source.contains("seat-transfer row") && op_.contains("read") && op_.contains(seats_op) && cause.contains(PLANTED), "{source} | {op_} | {cause}");
    let (source, op_, cause) = source_failed(&reply("row-put"));
    assert!(source.contains("fabric.storage") && source.contains("mesh1.admin.1") && source.contains("seat-transfer row") && op_.contains("put") && op_.contains(seats_op) && cause.contains(PLANTED), "{source} | {op_} | {cause}");
    assert!(matches!(reply("again"), Reply::PrimaryUpdated { .. }), "{:?}", reply("again"));
    let refused = in_trace(&spans, &trace_id, REJECTED);
    for case in ["row-read", "row-put"] {
        let (source, op_, cause) = source_failed(&reply(case));
        let named = refused.iter().filter(|s| s["attributes"]["side"] == "holder").map(source_fields).collect::<Vec<_>>();
        assert!(named.contains(&("source-failed".to_string(), source, op_, cause)), "{case}: the holder's reject span names the reason source-failed and the three fields: {named:?}");
    }
    assert_eq!(in_trace(&spans, &trace_id, ROW_WRITTEN).len(), 1, "one row, the next attempt's");
}

/// CONTRACT (proposal Part C1 failure contract: "The one uncertain case is NA1 dying between the drain
/// and the rename; NA1 resolves it itself on restart by reading the row (committed: it is no longer
/// primary; absent: it is, under the unchanged epoch)" and "The holder lost after the row, before
/// 'updated': the successor and every reader take the seat from the row; the next writer to read the
/// row publishes 'updated'"; Part C2: "the leadership view is folded from the transfer rows"): every
/// admin over one file store root. P's update of mesh1's seat committed its row and P went down before
/// its call on N, so nobody said "primary updated". P, restarted over the same root, holds the record
/// its join gives it (P at 3) and reads the store root as it enters: the row is committed, so it holds
/// N at 4, its view names N mesh1's primary, and, reading its own committed row whose record is the
/// one it now holds, it says "primary updated" once for that operation. Over a root where P died
/// before the rename no row exists: what it reads leaves P at 3, primary under the unchanged epoch,
/// and it says nothing. What must NOT happen: a restarted holder naming itself primary over its own
/// committed row, a seat record read from anywhere but the row, or a committed row nobody says
/// "primary updated" for.
#[test]
fn a_holder_restarted_over_the_store_root_is_primary_only_when_no_row_committed_its_seat() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    let _file = SpanFile(spans.clone());
    cap.run(async {
        let root = DataDir::new("primary-update-restart");
        let open = |dir: std::path::PathBuf| async move { Arc::new(FileFabricStorage::open(&rafka_node_admin_core::store_root::StoreRoot::at(&dir), &IncarnationId::mint()).await.unwrap()) as Arc<dyn FabricStorage> };
        let f = seats_fabric_with(FabricSeat::X, || open(root.0.clone())).await;
        let op = update_operation(Seat::MeshPrimary, "b1", "restart-node:mesh1.admin.1");
        // P holds its view projection once it wrote its row and took the row's seat: its process dies
        // there, before its call on N.
        f.p.view_open.send_replace(false);
        let update = tokio::spawn({
            let (door, op) = (f.p.door.clone(), op.clone());
            async move { door.update_primary(Moving::MeshSeat, Some(3), "b1", 1, &op).await }
        });
        until("P wrote its row and took the row's seat", || f.p.door.membership.seats().mesh("mesh1") == Some(holder_of(&f.n, 4))).await;
        let _ = f.p._router.shutdown().await;
        update.abort();
        assert!(spans.named(ANNOUNCE_UPDATED).is_empty(), "P went down before \"primary updated\": {:?}", spans.named(ANNOUNCE_UPDATED));
        assert_eq!(f.n.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.p, 3)), "N was never called");
        // P restarted over the same root: its join gives it P at 3, then it reads the root as it enters.
        let (restarted, _) = admin_over(&FabricId::mint(), "mesh1", "mesh1.admin.1", "000000000002", true, open(root.0.clone()).await, f.p.status.clone()).await;
        let mut nodes = vec![node_of(&restarted, NodeStatus::ReadyForTraffic, false), node_of(&f.n, NodeStatus::ReadyForTraffic, false)];
        nodes[1].is_primary = false;
        *restarted.door.topology.write().await = view(nodes);
        restarted.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&f.p, 3));
        let enter = |a: &Admin| {
            let d = a.door.clone();
            async move { rafka_node_admin_core::fabric_handover::enter_seats(&*d.storage, &d.membership, &d.backbone, &d.me, &d.node_id).await }
        };
        let read = enter(&restarted).await.expect("the root is read");
        assert_eq!(read, 1, "one committed row");
        assert_eq!(restarted.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.n, 4)), "committed: it is no longer primary");
        (restarted.door.project_view)().await;
        assert_eq!(restarted.door.topology.read().await.cohort_primary("mesh1", rafka_node_admin_core::model::NodeKind::NodeAdmin).map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.2"), "its view names N");
        let said = || spans.named(ANNOUNCE_UPDATED).iter().map(|f| (f["node"].clone(), f["operation"].clone())).collect::<Vec<_>>();
        assert_eq!(said(), vec![("mesh1.admin.1".to_string(), op.clone())], "the restarted holder, the next to read its committed row, says the primary updated once");
        // A root where P died before the rename holds no row.
        let empty = DataDir::new("primary-update-restart-absent");
        let (absent, _) = admin_over(&FabricId::mint(), "mesh1", "mesh1.admin.1", "000000000002", true, open(empty.0.clone()).await, Arc::default()).await;
        absent.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&f.p, 3));
        let read = enter(&absent).await.expect("the root is read");
        assert_eq!(read, 0, "no row");
        assert_eq!(absent.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.p, 3)), "absent: it is primary under the unchanged epoch");
        assert_eq!(said().len(), 1, "no row, nothing said: {:?}", said());
        let _ = (&f.m, &f.x);
    });
}

/// CONTRACT (proposal Part C3 edit 5, the successor's refusals: "RejectedConflictingPayload (same
/// operation, different content: the row decides); `fabric-not-ready`"; Part C1: "Before the initial
/// fabric first reaches ready-for-traffic, every call is refused `fabric-not-ready` (R-PB1)"): N's
/// fabric has not first reached ready-for-traffic (its view holds the fabric in state-sync and it never
/// knew it ready): P's node.primary.update call on N is refused `RejectedFabricNotReady` naming the
/// initial fabric's state, under the successor's reject span `fabric-not-ready`; N installs nothing.
#[test]
fn the_successor_refuses_a_call_before_the_initial_fabric_is_ready() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    cap.run(async {
        let f = commanded_fabric().await;
        f.n.door.topology.write().await.fabric.status = ScopeStatus::StateSync;
        // Its mesh's status rows on the store root hold no ready-for-traffic row either.
        status_rows_of(&f.n, false).await;
        let op = "node.primary.update:b1:stop-node:mesh1.admin.1";
        let r = send(&f.p, &f.n, primary_update(&f.p, &f.n, 4, "b1", 1, op)).await;
        assert!(matches!(&r, Reply::RejectedFabricNotReady { reason } if reason.contains("state-sync") && reason.contains("ready-for-traffic")), "{r:?}");
        assert_eq!(f.n.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.p, 3)), "N installs nothing");
        let named = refusals(&spans);
        assert!(named.iter().any(|(side, reason, _)| side == "successor" && reason == "fabric-not-ready"), "{named:?}");
        let _ = (&f.m, &f.x);
    });
}

/// CONTRACT (proposal Part C1: "Before the initial fabric first reaches ready-for-traffic, every call
/// is refused `fabric-not-ready` (R-PB1)"; failure contract: "Refused before the row ...: refused by
/// name, nothing changed, the Build's step fails by name. Nothing was cut off, so nothing is
/// restored"; states.md "The initial fabric before ready-for-traffic": the ready-for-traffic row among
/// the mesh's status rows is the durable fact): P, the holder, holds the initial fabric in state-sync
/// and its mesh's status rows on the store root hold no ready-for-traffic row. The fabric-primary's
/// command is refused `RejectedFabricNotReady` naming the state, under the holder's reject span
/// `fabric-not-ready`, before anything: no "primary updating", no cut-off, no row, P still primary, N
/// installs nothing. The root then holds the ready row while P's view still holds state-sync (the
/// fabric state gossip brings can lag the row): the identical command is decided on the row (P's
/// `rdm.node_admin.fabric.update.via-first-ready`, reason `ready-for-traffic-row-on-store-root`), P
/// writes the row and N answers node.primary.updated. What must NOT happen: an announcement, a cut-off
/// or a row before the fabric first reached ready-for-traffic, or a refusal once the root holds the
/// ready row.
#[test]
fn the_holder_refuses_an_update_before_the_initial_fabric_is_ready_and_decides_on_the_root_row() {
    use rafka_node_admin_core::status_storage::{FabricEventRow, StatusStorage};
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    let _file = SpanFile(spans.clone());
    cap.run(async {
        let f = commanded_fabric().await;
        f.p.door.topology.write().await.fabric.status = ScopeStatus::StateSync;
        let rows = status_rows_of(&f.p, false).await;
        let op = "node.primary.update:b1:stop-node:mesh1.admin.1";
        let r = send(&f.x, &f.p, update_mesh_seat(&f.p, 3, "b1", op)).await;
        assert!(matches!(&r, Reply::RejectedFabricNotReady { reason } if reason.contains("state-sync") && reason.contains("ready-for-traffic")), "{r:?}");
        assert!(refusals(&spans).iter().any(|(side, reason, _)| side == "holder" && reason == "fabric-not-ready"), "{:?}", refusals(&spans));
        assert!(spans.named(ANNOUNCE_UPDATING).is_empty() && spans.named(CUT_OFF).is_empty() && spans.named(ROW_WRITTEN).is_empty(), "nothing is said, cut off or written: {:?}", spans.names());
        assert!(f.p.cut_offs.lock().unwrap().is_empty(), "no cut-off: {:?}", f.p.cut_offs.lock().unwrap());
        assert!(f.p.storage.seat_transfers().await.unwrap().is_empty(), "no row");
        assert_eq!(f.p.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.p, 3)), "P is still primary");
        assert_eq!(f.n.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.p, 3)), "N installs nothing");
        // The root holds the ready row; P's view still holds state-sync.
        let fabric_id = f.p.door.topology.read().await.fabric.id.clone();
        rows.put_fabric_event(&FabricEventRow { fabric_id, event: "ready-for-traffic".into() }).await.unwrap();
        let r = send(&f.x, &f.p, update_mesh_seat(&f.p, 3, "b1", op)).await;
        assert_eq!(r, updated(committed_mesh(&f.n, 4)), "decided on the root's ready row");
        let ready: Vec<String> = spans.named("rdm.node_admin.fabric.update.via-first-ready").iter().map(|f| f["reason"].clone()).collect();
        assert_eq!(ready, vec!["ready-for-traffic-row-on-store-root".to_string()], "P decided on the root's ready row");
        assert_eq!(f.p.storage.seat_transfers().await.unwrap().len(), 1, "one row");
        let _ = &f.m;
    });
}

/// `a`'s mesh's status rows on the fabric's store root (what its admin folds and point-reads), holding
/// the ready-for-traffic row of `a`'s fabric when `ready`: the durable fact that the initial fabric
/// first reached ready-for-traffic (states.md, "The initial fabric before ready-for-traffic").
async fn status_rows_of(a: &Admin, ready: bool) -> Arc<MemoryStatusStorage> {
    use rafka_node_admin_core::status_storage::{FabricEventRow, StatusStorage};
    if ready {
        let fabric_id = a.door.topology.read().await.fabric.id.clone();
        a.status.put_fabric_event(&FabricEventRow { fabric_id, event: "ready-for-traffic".into() }).await.unwrap();
    }
    a.status.clone()
}

/// CONTRACT (proposal Part C1: "Before the initial fabric first reaches ready-for-traffic, every call
/// is refused `fabric-not-ready` (R-PB1)" and "After the row, the successor is the committed holder
/// whatever the call returns ... it installs the committed view from the row and answers"; states.md,
/// "The initial fabric before ready-for-traffic": the ready-for-traffic row among the mesh's status
/// rows is the durable fact that the fabric first reached ready-for-traffic): N has not heard the
/// fabric ready yet (its view still holds the initial fabric in state-commit and it never knew it
/// ready), while its mesh's status rows on the store root already hold the ready-for-traffic row. The
/// fabric-primary's command to P is answered node.primary.updated: P writes the row, and N decides
/// R-PB1 on the durable row (`rdm.node_admin.fabric.update.via-first-ready`, reason
/// `ready-for-traffic-row-on-store-root`), installs and answers. What must NOT happen: N refusing
/// `fabric-not-ready` after P's row for a fabric whose ready row is on the root (the live-mesh red:
/// node_stop, trace 8b62c8d41e0832508bc83bb9c926015f, where mesh2.admin.2 refused 3 ms before the
/// fabric state reached it by gossip).
#[test]
fn the_successor_decides_ready_for_traffic_on_the_root_row_not_on_the_state_gossip_has_not_brought() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    cap.run(async {
        let f = commanded_fabric().await;
        f.n.door.topology.write().await.fabric.status = ScopeStatus::StateCommit;
        status_rows_of(&f.n, true).await;
        let op = "node.primary.update:b1:stop-node:mesh1.admin.1";
        let r = send(&f.x, &f.p, update_mesh_seat(&f.p, 3, "b1", op)).await;
        assert_eq!(r, updated(committed_mesh(&f.n, 4)), "N installed P's row and answered node.primary.updated");
        assert_eq!(f.n.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.n, 4)), "N holds mesh1's seat from the row");
        assert!(refusals(&spans).is_empty(), "{:?}", refusals(&spans));
        let ready: Vec<String> = spans.named("rdm.node_admin.fabric.update.via-first-ready").iter().map(|f| f["reason"].clone()).collect();
        assert_eq!(ready, vec!["ready-for-traffic-row-on-store-root".to_string()], "N decided on the root's ready row");
        let _ = &f.m;
    });
}

/// CONTRACT (states.md "Who sets each state": "The fabric-primary declares fabric state through the
/// completed rounds"; "The initial fabric before ready-for-traffic": the initial fabric first reaching
/// ready-for-traffic is the fabric-primary's one fact, and its ready-for-traffic row on the store root
/// is that fact made durable; proposal Part C1: "Before the initial fabric first reaches
/// ready-for-traffic, every call is refused `fabric-not-ready` (R-PB1)"): the fabric-primary X (mesh2)
/// put its own mesh's ready-for-traffic row as it entered ready-for-traffic and admitted the stop. mesh1's
/// own status rows hold no ready row yet (the fabric-primary's ApplyFabricEvent reaches mesh1's primary
/// later), and neither P nor N has heard the fabric ready by gossip (their views hold state-commit). X's
/// command to P is answered node.primary.updated: P and N each decide R-PB1 on the fabric-primary's row,
/// read from mesh2's status rows on the root (`rdm.node_admin.fabric.update.via-first-ready`, reason
/// `ready-for-traffic-row-of-the-fabric-primary-on-store-root`). What must NOT happen: a holder refusing
/// `fabric-not-ready` a command the fabric-primary admitted once the fabric was ready (the gate's red on
/// 3355f190, node_stop, trace c67704501b343b341e584fb214d64089: mesh2.admin.1 refused at 13391 ms, its
/// own mesh's row put at 13848 ms, and the drive stalled and was driven again).
#[test]
fn the_holder_and_successor_decide_ready_for_traffic_on_the_fabric_primarys_row_before_their_own_meshs_row_is_put() {
    use rafka_node_admin_core::status_storage::{FabricEventRow, StatusStorage};
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    cap.run(async {
        let f = commanded_fabric().await;
        for a in [&f.p, &f.n] {
            a.door.topology.write().await.fabric.status = ScopeStatus::StateCommit;
        }
        // One fabric: every admin's view names the same fabric id (the fixture's views mint one each).
        let fabric_id = f.x.door.topology.read().await.fabric.id.clone();
        for a in [&f.p, &f.n, &f.m] {
            a.door.topology.write().await.fabric.id = fabric_id.clone();
        }
        let rows = status_rows_of(&f.p, false).await;
        rows.put_fabric_event_of("mesh2", &FabricEventRow { fabric_id, event: "ready-for-traffic".into() });
        assert!(rows.fabric_events().await.unwrap().is_empty(), "mesh1's own status rows hold no ready-for-traffic row");
        assert_eq!(f.p.door.membership.seats().fabric().map(|h| h.mesh), Some("mesh2".to_string()), "P's seat book names the fabric-primary in mesh2");
        let op = "node.primary.update:b1:stop-node:mesh1.admin.1";
        let r = send(&f.x, &f.p, update_mesh_seat(&f.p, 3, "b1", op)).await;
        assert_eq!(r, updated(committed_mesh(&f.n, 4)), "the command the fabric-primary admitted once the fabric was ready is answered node.primary.updated");
        assert!(refusals(&spans).is_empty(), "nothing is refused fabric-not-ready: {:?}", refusals(&spans));
        assert_eq!(f.n.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.n, 4)), "N holds mesh1's seat from the row");
        let ready: Vec<String> = spans.named("rdm.node_admin.fabric.update.via-first-ready").iter().map(|f| f["reason"].clone()).collect();
        assert_eq!(ready, vec!["ready-for-traffic-row-of-the-fabric-primary-on-store-root".to_string(); 2], "P and N decided on the fabric-primary's row");
        let _ = &f.m;
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

/// CONTRACT (node-rpc.md §14; R-RX1; proposal Part C1): a command to update mesh1's primary seat
/// from an endpoint the holder's view holds no birth for is decided on the seat P holds and the epoch
/// it names: P writes the row, N installs, and P answers node.primary.updated. What must NOT happen:
/// `Unauthorized` naming the caller, or any refusal.
#[test]
fn a_primary_update_command_from_an_endpoint_no_view_holds_is_decided_on_the_seat_it_names() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    cap.run(async {
        let f = commanded_fabric().await;
        let stranger = stranger_to(&[&f.p]).await;
        let op = "replace-node:mesh1.admin.1";
        assert_eq!(send(&stranger, &f.p, update_mesh_seat(&f.p, 3, "b1", op)).await, updated(committed_mesh(&f.n, 4)), "the command is decided on the seat it names");
        for a in [&f.p, &f.n] {
            assert_eq!(a.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.n, 4)), "{} holds N's committed seat", a.resolved.name);
        }
        assert_eq!(spans.named(ROW_WRITTEN).len(), 1);
        assert!(refusals(&spans).is_empty(), "nothing is refused: {:?}", refusals(&spans));
        let _ = (&f.m, &f.x);
    });
}

/// CONTRACT (node-rpc-envelope.md, op 0x21: "Epoch overflow cannot commit and is refused by name with
/// a typed refusal"; proposal Part C1 "Refused before the row ... the writer's own pre-check of the
/// call's epoch"): a seat whose epoch is `u64::MAX` cannot advance. The fabric-primary's command naming
/// mesh1's seat at `u64::MAX` is refused `RejectedEpochOverflow { seat, epoch: u64::MAX }` by the
/// holder, its reject span naming the reason `epoch-overflow`, the seat and the epoch. Nothing commits:
/// no row, no cut-off, the seat where it was.
#[test]
fn a_seat_epoch_at_its_maximum_is_refused_by_name_and_nothing_commits() {
    let cap = crate::mesh_delete_resume::capture();
    let seats_op = "stop-node:mesh1.admin.1";
    let (reply, trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.seat.reject.via-epoch-overflow");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let reply = tracing::Instrument::instrument(
            async {
                let s = commanded_fabric().await;
                s.p.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&s.p, u64::MAX));
                let reply = send(&s.x, &s.p, update_mesh_seat(&s.p, u64::MAX, "b1", seats_op)).await;
                assert!(s.p.storage.seat_transfers().await.unwrap().is_empty(), "no seat-transfer row is written");
                assert!(s.p.cut_offs.lock().unwrap().is_empty(), "no write is cut off for an update that cannot commit: {:?}", s.p.cut_offs.lock().unwrap());
                assert_eq!(s.p.door.membership.seats().mesh("mesh1"), Some(holder_of(&s.p, u64::MAX)), "P's seat did not move");
                let _ = (&s.m, &s.n);
                reply
            },
            root,
        )
        .await;
        (reply, trace_id)
    });
    let spans = cap.spans();
    keep_cell("epoch_overflow", &trace_id, &spans);
    assert_eq!(reply, Reply::RejectedEpochOverflow { seat: WireSeat::MeshPrimary { mesh: "mesh1".into() }, epoch: u64::MAX });
    let refused = in_trace(&spans, &trace_id, REJECTED);
    assert_eq!(refused.len(), 1, "one refusal of the seat update: {refused:?}");
    assert_eq!(refused[0]["attributes"]["reason"], "epoch-overflow");
    assert!(refused[0]["attributes"]["detail"].as_str().is_some_and(|d| d.contains("mesh-primary") && d.contains("18446744073709551615")), "{refused:?}");
    for name in [ROW_WRITTEN, CUT_OFF, UPDATED] {
        assert!(in_trace(&spans, &trace_id, name).is_empty(), "nothing commits: {name}");
    }
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
