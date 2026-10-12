//! node.primary.update and node.fabric-primary.update (the approved proposal
//! rafka-v2 docs/plans/proposal/node-primary-update-fabric-mesh-and-node-type.md, Part C; R-MT1) between
//! in-process node-admins over real Node RPC on op `0x21`. The holder of a seat is the row writer of
//! its node-admin cohort: it pre-checks its successor, says "primary updating", cuts off and drains its
//! seat's writes, writes the ONE committed row, then calls `PrimaryUpdate` on the successor, which
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
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::handover::{CommittedSeat, HandoverBirth, NodePrimaryUpdate, NodePrimaryUpdateReply as Reply, NodePrimaryUpdateRequest as Request, Seat as WireSeat, SeatEpoch, SeatUpdate};
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

const CALL: &str = "rdm.node_rpc.request.update.via-call";
/// The holder's whole update (C1 steps 2-7 at the holder).
const UPDATE: &str = "rdm.node_admin.seat.update.via-primary-updating";
/// The holder's cut-off and drain (C1 step 4).
const DRAINED: &str = "rdm.node_admin.seat.update.via-writes-drained";
/// The holder's one committed row (C1 step 5).
const ROW_WRITTEN: &str = "rdm.node_admin.seat.update.via-seat-row-written";
/// The successor installed from the row and answered node.primary.updated (C1 step 6).
const UPDATED: &str = "rdm.node_admin.seat.update.via-primary-updated";
/// The commander installed the committed seats from the answer.
const INSTALLED: &str = "rdm.node_admin.seat.update.via-seats-installed";
/// A refusal of the primitive, at the holder or the successor.
const REJECTED: &str = "rdm.node_admin.seat.reject.via-primary-update";
/// "primary updating", said by the holder before its cut-off (C1 step 3).
const ANNOUNCE_UPDATING: &str = "rdm.mesh.seat.update.via-announce-primary-updating";
/// "primary updated", said by the successor once it installed from the row (C1 step 7).
const ANNOUNCE_UPDATED: &str = "rdm.mesh.seat.update.via-announce-primary-updated";
/// The retired successor-side row writer: no span of it may appear.
const SEATS_COMMITTED: &str = "rdm.node_admin.seat.update.via-seats-committed";

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
    /// Every write cut-off the holder's SeatTransfer step applied, in order.
    cut_offs: Arc<Mutex<Vec<WriteCutOff>>>,
    /// Holds the cut-off's drain open while `false`: a write still issued when the step cuts off.
    drained: tokio::sync::watch::Sender<bool>,
    /// Holds the admin's view projection while `false`: an admin that wrote or took a seat and has
    /// not answered yet (what a process that dies there leaves).
    view_open: tokio::sync::watch::Sender<bool>,
    /// The admin's Node RPC server: a cell drains it as the admin's DrainNode does.
    server: rafka_node_rpc::NodeRpcServer,
    /// The admin's `FetchBuildFacts` door (op `0x1F`): `NotReady` until a cell fills it.
    facts_slot: BuildFactsSlot,
    /// The admin's Build door (op `0x20`): `NotReady` until a cell fills it.
    build_slot: BuildSlot,
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
    let (facts_slot, build_slot): (BuildFactsSlot, BuildSlot) = (Arc::new(OnceLock::new()), Arc::new(OnceLock::new()));
    let served = rafka_node_admin_core::build_facts_read::serve(rafka_node_admin_core::fabric_handover::serve(ServerBuilder::new(), slot.clone()), facts_slot.clone());
    let server = rafka_node_admin_core::build_op::serve(served, build_slot.clone()).seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() }).expect("the catalog seals");
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip).accept(rafka_node_rpc::ALPN, server.clone()).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { node_id: node_id.clone(), name: path.parse().unwrap(), endpoint_id: ep.id(), transport_addr: addr, incarnation: incarnation.clone() };
    let topology = Arc::new(tokio::sync::RwLock::new(view(vec![])));
    let cut_offs: Arc<Mutex<Vec<WriteCutOff>>> = Arc::default();
    let (drained, drain_gate) = tokio::sync::watch::channel(true);
    let (view_open, view_gate) = tokio::sync::watch::channel(true);
    // The cell's view is set by hand, not projected from digests: the projection re-runs the one
    // election over its members with the seat records this admin holds now, as the admin's own
    // projection does (`admin::view_projector`).
    let project_view: ProjectView = {
        let (topology, seats) = (topology.clone(), membership.seats().clone());
        Arc::new(move || {
            let (topology, seats, mut gate) = (topology.clone(), seats.clone(), view_gate.clone());
            Box::pin(async move {
                let _ = gate.wait_for(|open| *open).await;
                let inc = Incumbency { fabric: seats.fabric(), meshes: seats.meshes(), lost: seats.gone() };
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
        locks: Default::default(),
        decisions: Arc::default(),
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
    (Admin { door, slot, client, resolver, resolved, storage, cut_offs, drained, view_open, server, facts_slot, build_slot, _router: router }, ep)
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
    let fabric_id = FabricId::mint();
    let (incumbent, _) = admin_over(&fabric_id, "mesh1", "mesh1.admin.1", "000000000009", true, storage()).await;
    let (successor, _) = admin_over(&fabric_id, "mesh2", "mesh2.admin.1", "000000000005", true, storage()).await;
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

/// The spans of the update are made on the server tasks of every admin, on any worker thread:
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

fn fabric_primary_of(t: &Topology) -> Option<String> {
    t.fabric_primary().map(|n| n.name.to_string())
}

/// One finished span of a traced cell: its name, trace and attributes.
struct Traced {
    name: String,
    trace_id: String,
    attributes: BTreeMap<String, String>,
}

/// A cell's own span capture with trace ids: the product span pipeline
/// (`rafka_mesh_telemetry::span_capture`) in front of an in-memory exporter, behind a dispatcher
/// installed on every thread of the cell's own runtime, so the cell needs no process of its own.
struct TraceCapture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    capture: rafka_mesh_telemetry::SpanCapture,
    dispatch: tracing::Dispatch,
}

fn trace_capture(service: &'static str) -> TraceCapture {
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (capture, layer) = rafka_mesh_telemetry::span_capture(service, exporter.clone());
    TraceCapture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
}

impl TraceCapture {
    /// Run the cell's story under a root span named `root` on the capture's runtime; the runtime is
    /// dropped when the story ends. Returns the root's trace id and what the story returned.
    fn story<F: std::future::Future>(&self, root: &'static str, f: F) -> (String, F::Output) {
        let d = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d))).build().unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(async {
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
    async fn dispatch(&self, _: &rafka_node_admin_core::model::PathName, _: &rafka_node_admin_core::build::BuildId, _: u32, _: rafka_node_rpc_contract::context::CallContext, _: Vec<Vec<u8>>, _: rafka_node_admin_core::executor::RunPlan) -> rafka_node_admin_core::build_drive::Dispatched {
        std::future::pending().await
    }
}

/// An executor that runs no operation: the cell's admins are called for no run.
struct NoRuns;

#[async_trait::async_trait]
impl rafka_node_admin_core::executor::OperationRunner for NoRuns {
    async fn run(&self, _: &rafka_node_admin_core::build::BuildId, _: u32, op: &rafka_node_admin_core::build::BuildOperation) -> Result<(), rafka_node_admin_core::executor::RunEnd> {
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
    let mut control = rafka_node_admin_core::http::ControlPlane::new(dyn_builds.clone(), accepted.clone(), me.clone(), a.door.topology.read().await.clone(), crate::adopted_time());
    control.topology = a.door.topology.clone();
    let control = Arc::new(control);
    let exec = Arc::new(rafka_node_admin_core::executor::BuildExecutor { executor: me.to_string(), builds: dyn_builds.clone(), topology: control.topology.clone(), runner: Arc::new(NoRuns) });
    let run = Arc::new(rafka_node_admin_core::build_run::RunDoor { me: me.clone(), builds: dyn_builds.clone(), exec, runs: Arc::default(), local: builds.clone() });
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: control.topology.clone(),
        builds: dyn_builds.clone(),
        door: Arc::new(rafka_node_admin_core::build_claim::ClaimDoor { me: me.clone(), node_id: a.resolved.node_id.clone(), topology: control.topology.clone(), builds: dyn_builds.clone(), contexts: Arc::new(rafka_node_admin_core::build_claim::AttemptContexts::in_memory()) }),
        dispatcher: Arc::new(Held),
        gate: Arc::new(OpenGate),
        departure: Arc::new(crate::loopback::Proofs::default()),
        verdicts: Arc::new(crate::loopback::NoVerdicts),
        rows: Arc::new(rafka_node_admin_core::node_rows::NodeRows::new(me.clone(), a.resolved.node_id.clone(), control.topology.clone(), Arc::new(rafka_node_admin_core::storage::MemoryNodesStorage::default()))),
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
        p_log.claim_attempt(&BuildAttemptClaim { build_id: b.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
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
    let fabric_id = FabricId::mint();
    let (p, _) = admin_over(&fabric_id, "mesh1", "mesh1.admin.1", "000000000002", true, storage().await).await;
    let (n, _) = admin_over(&fabric_id, "mesh1", "mesh1.admin.2", "000000000003", true, storage().await).await;
    let (m, _) = admin_over(&fabric_id, "mesh1", "mesh1.admin.3", "000000000009", true, storage().await).await;
    let (x, _) = admin_over(&fabric_id, "mesh2", "mesh2.admin.1", "000000000001", true, storage().await).await;
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
/// updating", P cuts off and drains, P writes ONE row moving both seats to N (mesh1 4, fabric 8), N
/// installs from the row and answers node.primary.updated, N says "primary updated". P and N hold both
/// committed records; M, a third admin that heard nothing, reads the one row from the store root and
/// holds both too. P's moved fabric record fences its Build log naming N. What must NOT happen: two
/// rows, a row written by N, a seat moved with no row, or a holder record naming P after the update.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seat_holder_moves_both_seats_to_its_successor_in_one_row() {
    if crate::own_process::delegated(module_path!(), "a_seat_holder_moves_both_seats_to_its_successor_in_one_row") {
        return;
    }
    let spans = capture();
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
    // The moved record is P's fence: read for P's birth, it names N.
    let p_birth = (p_node.node_id.clone(), p_node.incarnation_id.clone().unwrap());
    let view = f.p.door.topology.read().await.clone();
    let fence = rafka_node_admin_core::seat_watch::fabric_seat_record(f.p.door.membership.seats().fabric().as_ref(), &p_birth, &Default::default(), |id| view.own(id).map(|n| n.name.to_string()));
    assert!(matches!(&fence, rafka_node_admin_core::fabric_builds::SeatRecord::Moved { to: Some(to), .. } if to == "mesh1.admin.2"), "P's seat record fences its Build log naming N: {fence:?}");
    let at = |n: &str| spans.position(n).unwrap_or_else(|| panic!("no {n} span among {:?}", spans.names()));
    assert!(at(ANNOUNCE_UPDATING) < at(DRAINED), "P says the primary is updating before its cut-off");
    assert!(at(DRAINED) < at(ROW_WRITTEN), "P drains before it writes the row");
    assert!(at(ROW_WRITTEN) < at(UPDATED), "N installs only after P's row");
    assert!(at(UPDATED) < at(ANNOUNCE_UPDATED), "N says the primary updated once it installed");
    let written = spans.named(ROW_WRITTEN);
    assert_eq!(written.len(), 1, "{written:?}");
    assert_eq!((written[0]["node"].as_str(), written[0]["mesh_epoch"].as_str(), written[0]["fabric_epoch"].as_str()), ("mesh1.admin.1", "4", "8"), "P, the cohort's row writer, writes the one row: {written:?}");
    let installed = spans.named(UPDATED);
    assert_eq!(installed.len(), 1, "{installed:?}");
    assert_eq!((installed[0]["node"].as_str(), installed[0]["replay"].as_str()), ("mesh1.admin.2", "false"), "{installed:?}");
    assert!(spans.named(SEATS_COMMITTED).is_empty(), "the successor never writes a seat row");
    assert!(refusals(&spans).is_empty(), "{:?}", refusals(&spans));
    let _ = &f.x;
}

/// CONTRACT (proposal Part C1 step 4, R14 item 4: "NA1 sets its write cut-off and drains every store
/// write already issued"; step 5: "The writer writes the one seat row"): the fabric-primary X commands
/// P; while P's drain is held open (a write still on the disk) no seat row exists and X's command is
/// unanswered; once it drains, P writes the one row and N's node.primary.updated is X's answer. What
/// must NOT happen: the seat row written while an old-seat write is still issued, or a commit with no
/// cut-off.
#[test]
fn the_holder_drains_its_seat_writes_before_it_writes_the_seat_row() {
    let cap = crate::mesh_leave_resume::capture();
    let op = "drain-node:mesh1.admin.1";
    let trace_id = cap.run(async {
        let root = tracing::info_span!("rdm.test.seat.update.via-primary-update-drain");
        let trace_id = crate::mesh_leave_resume::trace_id(&root);
        tracing::Instrument::instrument(the_holder_drains(op), root).await;
        trace_id
    });
    println!("trace_id={trace_id}");
    let spans = cap.spans();
    let ours = |name: &str| spans.iter().filter(|s| s["name"] == name && s["attributes"]["operation"] == op).collect::<Vec<_>>();
    let drained = ours(DRAINED);
    assert_eq!(drained.len(), 1, "one drain span for the update: {:?}", spans.iter().map(|s| s["name"].clone()).collect::<Vec<_>>());
    assert_eq!(drained[0]["attributes"]["fabric_seat"], "false");
    let written = ours(ROW_WRITTEN);
    assert_eq!(written.len(), 1, "one row for the update");
    assert_eq!(written[0]["attributes"]["node"], "mesh1.admin.1", "the holder writes the row");
    let installed = ours(UPDATED);
    assert_eq!(installed.len(), 1, "one install at the successor");
    for s in [drained[0], written[0], installed[0]] {
        assert_eq!(s["trace_id"].as_str(), Some(trace_id.as_str()), "the holder's drain and row and the successor's install are the cell's own story: {s:?}");
    }
}

async fn the_holder_drains(op: &'static str) {
    let f = commanded_fabric().await;
    f.p.drained.send_replace(false);
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
        matches!(&f.p.cut_offs.lock().unwrap()[..], [WriteCutOff::Set { reason, fabric_seat: false }] if reason.contains(op) && reason.contains("mesh1")),
        "one cut-off naming the update, the mesh seat only: {:?}",
        f.p.cut_offs.lock().unwrap()
    );
    assert!(f.p.storage.seat_transfers().await.unwrap().is_empty(), "the seat row was written while a write of the old seat was still issued");
    assert!(!call.is_finished(), "P answered before its writes drained");
    f.p.drained.send_replace(true);
    let reply = call.await.unwrap();
    assert_eq!(reply, Some(updated(committed_mesh(&f.n, 4))), "N's node.primary.updated is the answer");
    assert_eq!(f.n.storage.seat_transfers().await.unwrap().len(), 1, "one row, written after the drain");
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
    let op = "drain-node:mesh1.admin.1";
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
/// keeps serving after the row): the fabric-primary's reply was lost and P has since been drained (its
/// Node RPC server refuses new calls typed Draining, as its DrainNode makes it): the identical command
/// is still answered from the row with the same committed seat, never refused Draining, and nothing
/// commits twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_after_the_holder_drained_is_answered_from_the_row() {
    let f = commanded_fabric().await;
    let op = "replace-node:mesh1.admin.1";
    let first = send(&f.x, &f.p, update_mesh_seat(&f.p, 3, "b1", op)).await;
    assert_eq!(first, updated(committed_mesh(&f.n, 4)));
    f.p.server.drain();
    assert_eq!(send(&f.x, &f.p, update_mesh_seat(&f.p, 3, "b1", op)).await, first, "a drained holder answers the identical command from the row");
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_primary_update_call_is_never_refused_for_the_birth_it_names() {
    if crate::own_process::delegated(module_path!(), "a_primary_update_call_is_never_refused_for_the_birth_it_names") {
        return;
    }
    let spans = capture();
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transfer_with_no_successor_is_refused_with_q7s_exact_texts_and_nothing_moves() {
    if crate::own_process::delegated(module_path!(), "a_transfer_with_no_successor_is_refused_with_q7s_exact_texts_and_nothing_moves") {
        return;
    }
    let spans = capture();
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
    assert!(spans.named(ROW_WRITTEN).is_empty() && spans.named(DRAINED).is_empty(), "nothing cut off, no row");
    assert!(f.x.storage.seat_transfers().await.unwrap().is_empty() && g.x.storage.seat_transfers().await.unwrap().is_empty());
    let _ = (&f.m, &f.n, &g.m, &g.n, &g.p);
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
/// install descends from X's row read, and N says "primary updated". What must NOT happen: a command
/// that never ends, a seat installed with no row, or X continuing on its read of the row alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_holder_lost_after_the_row_leaves_the_command_incomplete_and_the_next_attempt_obtains_node_primary_updated() {
    if crate::own_process::delegated(module_path!(), "a_holder_lost_after_the_row_leaves_the_command_incomplete_and_the_next_attempt_obtains_node_primary_updated") {
        return;
    }
    // The cell's spans: the in-memory names the waits read, and the product span pipeline
    // (`rafka_mesh_telemetry::span_capture`) stamping the trace ids the proof names.
    let spans = Spans::default();
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (pipeline, layer) = rafka_mesh_telemetry::span_capture("mesh2.admin.1", exporter.clone());
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(spans.clone()).with(layer)).expect("this cell owns its process");
    let story = tracing::info_span!("rdm.test.seat.update.via-lost-holder-row");
    let trace_id = story.in_scope(rafka_mesh_telemetry::current_traceparent).and_then(|tp| tp.split('-').nth(1).map(str::to_string)).expect("the cell's root span has a trace context");
    let root = DataDir::new("primary-update-root");
    let f = seats_fabric_with(FabricSeat::X, || {
        let at = rafka_node_admin_core::store_root::StoreRoot::at(&root.0);
        async move { Arc::new(FileFabricStorage::open(&at, &IncarnationId::mint()).await.unwrap()) as Arc<dyn FabricStorage> }
    })
    .await;
    let p_node = f.x.door.topology.read().await.members().find(|x| x.name.to_string() == "mesh1.admin.1").cloned().unwrap();
    let p_inc = f.p.resolved.incarnation.clone();
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
    let e = out.expect_err("an uncertain command is no answer");
    assert!(e.contains("Indeterminate") && e.contains("mesh1.admin.1") && e.contains(op), "the end names the outcome, the holder and the operation: {e}");
    assert_eq!(x.door.membership.seats().mesh("mesh1"), Some(holder_of(&p, 3)), "X installs nothing on an uncertain call");
    assert_eq!(n.door.membership.seats().mesh("mesh1"), Some(holder_of(&p, 3)), "N was never called: it installed nothing");
    // P's birth is proven gone (the provider's exit proof).
    x.door.membership.seats().mark_gone(p_inc);
    let other = "node.primary.update:b9:stop-node:mesh1.admin.1";
    let out = tokio::time::timeout(std::time::Duration::from_secs(20), tracing::Instrument::instrument(x.door.command_update_primary(&p_node, "b9", 1, other), story.clone())).await.expect("a proven-lost holder is not called");
    let e = out.expect_err("no row records an update for b9");
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
    pipeline.flush(std::time::Duration::from_secs(10)).expect("the cell's spans reach its exporter");
    let finished = exporter.get_finished_spans().unwrap();
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
    assert_eq!(installed_at_n[0].parent_span_id, held.span_context.span_id(), "N's install is the answer to X's call under its row read");
    let x_installed: Vec<_> = ours(INSTALLED).into_iter().filter(|s| attr(s, "via").as_deref() == Some("committed-row")).collect();
    assert_eq!(x_installed.len(), 1);
    assert!(x_installed[0].start_time >= installed_at_n[0].end_time, "X installs only after N answered node.primary.updated");
}

/// CONTRACT (proposal Part C1 failure contract: "After the row, the successor is the committed holder
/// whatever the call returns ... A call that reached a process other than the named birth, a refusal
/// or no answer is never an exit proof ... The holder's call fails by name ... No row is rewritten";
/// R14 item 4: no write of a seat lands after the row that ends its writer's authority): the
/// fabric-primary X commands P; P writes the row, takes the row's seat and calls N; N installs and
/// holds its answer, and its process is gone before it answers. P's call ends Indeterminate: P's
/// update ends `Incomplete` naming it, but P is no longer primary (its book and its cut-off say the
/// row moved the seat: `Committed`, never `Lift`), and X's command ends incomplete with P's answer,
/// installing nothing. X's next attempt sends the identical command: P replays from its row, calls N
/// again, and that call ends by name too; no second row is written and no seat moves back to P. What
/// must NOT happen: an update that never ends, a cut-off lifted after the row, a second row, or P
/// primary again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successor_lost_mid_call_leaves_the_update_incomplete_and_the_seat_the_successors_from_the_row() {
    if crate::own_process::delegated(module_path!(), "a_successor_lost_mid_call_leaves_the_update_incomplete_and_the_seat_the_successors_from_the_row") {
        return;
    }
    let spans = capture();
    let f = commanded_fabric().await;
    let p_node = f.x.door.topology.read().await.members().find(|x| x.name.to_string() == "mesh1.admin.1").cloned().unwrap();
    let op = "node.primary.update:b1:stop-node:mesh1.admin.1";
    // N holds its answer once it installed from the row: its process dies there.
    f.n.view_open.send_replace(false);
    let first = tokio::spawn({
        let (door, p_node) = (f.x.door.clone(), p_node.clone());
        async move { door.command_update_primary(&p_node, "b1", 1, op).await }
    });
    until("N installed mesh1's seat from P's row", || f.n.door.membership.seats().mesh("mesh1") == Some(holder_of(&f.n, 4))).await;
    let Seats { p, n, m, x } = f;
    let n_id = n.resolved.node_id.to_string();
    let _ = n._router.shutdown().await;
    let out = tokio::time::timeout(std::time::Duration::from_secs(20), first).await.expect("an update whose successor was lost mid-call ends: it is never sent again on its own").unwrap();
    let e = out.expect_err("an uncertain update is no answer");
    assert!(e.contains("Indeterminate") && e.contains(&n_id) && e.contains(op), "the end names the outcome, the successor and the operation: {e}");
    assert!(matches!(p.cut_offs.lock().unwrap().as_slice(), [WriteCutOff::Set { .. }, WriteCutOff::Committed { fabric_seat: false, .. }]), "the row ended P's authority over the cut-off writes: {:?}", p.cut_offs.lock().unwrap());
    assert_eq!(p.door.membership.seats().mesh("mesh1"), Some(holder_of(&n, 4)), "after the row the seat is N's at P, whatever the call returned");
    assert_eq!(x.door.membership.seats().mesh("mesh1"), Some(holder_of(&p, 3)), "X installs nothing on an incomplete update");
    let incomplete: Vec<_> = refusals(&spans).into_iter().filter(|(side, reason, _)| side == "holder" && reason == "indeterminate").collect();
    assert_eq!(incomplete.len(), 1, "P's call after the row ends by name: {:?}", refusals(&spans));
    // X's next attempt: P replays from its row and calls N again, which is still gone.
    let out = tokio::time::timeout(std::time::Duration::from_secs(20), x.door.command_update_primary(&p_node, "b1", 2, op)).await.expect("the next attempt ends");
    let e = out.expect_err("N is still gone: no node.primary.updated");
    assert!(e.contains(op) && e.contains(&n_id), "{e}");
    assert_eq!(p.storage.seat_transfers().await.unwrap().len(), 1, "no row is rewritten");
    assert_eq!(p.door.membership.seats().mesh("mesh1"), Some(holder_of(&n, 4)), "no seat moves back to P");
    assert_eq!(spans.named(ROW_WRITTEN).len(), 1, "one row written");
    let _ = m;
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
    let op = "node.primary.update:b1:drain-node:mesh1.admin.1";
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
        stores.apply(WriteCutOff::Set { reason: "node.primary.update:b1:stop-node:mesh1.admin.1".into(), fabric_seat: false }).await;
        match stores.status.put_mesh_status(&row).await {
            Err(rafka_node_admin_core::record_store::StorageError::CutOff { reason, .. }) => assert!(reason.contains("node.primary.update:b1:stop-node:mesh1.admin.1"), "{reason}"),
            other => panic!("a status write after the moving seat's cut-off: {other:?}"),
        }
        stores.apply(WriteCutOff::Lift).await;
        stores.status.put_mesh_status(&row).await.expect("a lift gives the status writes back");
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
        let row = NodeRow { name: "mesh2.rpc.1".parse().unwrap(), node_id: NodeId::mint(), status: NodeStatus::Deleted, config: Default::default(), tags: Default::default(), allocated: None };
        let dead = MeshStatusRow { mesh_id: MeshId::mint(), state: rafka_node_rpc_contract::status::MeshState::Dead };
        stores.nodes.put_node_row(&row).await.unwrap().expect("mesh2.rpc.1 is a free key");
        stores.status.put_mesh_status_of("mesh2", &dead).await.unwrap();
        assert_eq!(stores.nodes.node_row_at(&row.name).await.unwrap(), Some(row.clone()), "the holder reads the mesh2 Node row it holds");
        assert!(stores.status.mesh_status_held_of("mesh2", &dead).await.unwrap(), "the holder reads the mesh2 status row it holds");
        let reason = "the seats of mesh1.admin.1 moved to mesh1 000000000002 by node.primary.update:b1:stop-node:mesh1.admin.1";
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
/// - The store refuses P's row after the drain: P answers `RejectedSourceFailed` naming the row and the
///   store's error, lifts its cut-off (`Set` then `Lift`) and is still mesh1's primary at epoch 3; N
///   is never called. Once the store writes again, X's next attempt of the same update has P write
///   the row under the unchanged epoch: the seat moves to N at 4.
/// Each refusal is the holder's reject span `source-failed` with the three fields.
#[test]
fn a_seat_row_write_that_fails_after_the_drain_fails_the_step_by_name_and_the_holder_writes_again_under_the_unchanged_epoch() {
    let cap = crate::mesh_leave_resume::capture();
    let seats_op = "drain-node:mesh1.admin.1";
    let (replies, trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.seat.reject.via-source-failed");
        let trace_id = crate::mesh_leave_resume::trace_id(&root);
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
/// primary; absent: it is, under the unchanged epoch)"; Part C2: "the leadership view is folded from
/// the transfer rows"): every admin over one file store root. P's update of mesh1's seat committed
/// its row before P went down; P, restarted over the same root, holds the record its join gives it
/// (P at 3) and reads the store root as it enters: the row is committed, so it holds N at 4 and its
/// view names N mesh1's primary. Over a root where P died before the rename no row exists: what it
/// reads leaves P at 3, primary under the unchanged epoch. What must NOT happen: a restarted holder
/// naming itself primary over its own committed row, or a seat record read from anywhere but the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_holder_restarted_over_the_store_root_is_primary_only_when_no_row_committed_its_seat() {
    let root = DataDir::new("primary-update-restart");
    let open = |dir: std::path::PathBuf| async move { Arc::new(FileFabricStorage::open(&rafka_node_admin_core::store_root::StoreRoot::at(&dir), &IncarnationId::mint()).await.unwrap()) as Arc<dyn FabricStorage> };
    let f = seats_fabric_with(FabricSeat::X, || open(root.0.clone())).await;
    let op = update_operation(Seat::MeshPrimary, "b1", "restart-node:mesh1.admin.1");
    assert_eq!(f.p.door.update_primary(Moving::MeshSeat, Some(3), "b1", 1, &op).await, Ok(committed_mesh(&f.n, 4)));
    // P restarted over the same root: its join gives it P at 3, then it reads the root.
    let (restarted, _) = admin_over(&FabricId::mint(), "mesh1", "mesh1.admin.1", "000000000002", true, open(root.0.clone()).await).await;
    let mut nodes = vec![node_of(&restarted, NodeStatus::ReadyForTraffic, false), node_of(&f.n, NodeStatus::ReadyForTraffic, false)];
    nodes[1].is_primary = false;
    *restarted.door.topology.write().await = view(nodes);
    restarted.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&f.p, 3));
    let read = rafka_node_admin_core::fabric_handover::hydrate_seats(&*restarted.storage, restarted.door.membership.seats()).await.expect("the root is read");
    assert_eq!(read, 1, "one committed row");
    assert_eq!(restarted.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.n, 4)), "committed: it is no longer primary");
    (restarted.door.project_view)().await;
    assert_eq!(restarted.door.topology.read().await.cohort_primary("mesh1", rafka_node_admin_core::model::NodeKind::NodeAdmin).map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.2"), "its view names N");
    // A root where P died before the rename holds no row.
    let empty = DataDir::new("primary-update-restart-absent");
    let (absent, _) = admin_over(&FabricId::mint(), "mesh1", "mesh1.admin.1", "000000000002", true, open(empty.0.clone()).await).await;
    absent.door.membership.seats().take(Seat::MeshPrimary, &holder_of(&f.p, 3));
    let read = rafka_node_admin_core::fabric_handover::hydrate_seats(&*absent.storage, absent.door.membership.seats()).await.expect("the root is read");
    assert_eq!(read, 0, "no row");
    assert_eq!(absent.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.p, 3)), "absent: it is primary under the unchanged epoch");
    let _ = (&f.m, &f.x);
}

/// CONTRACT (proposal Part C3 edit 5, the successor's refusals: "RejectedConflictingPayload (same
/// operation, different content: the row decides); `fabric-not-ready`"; Part C1: "Before the initial
/// fabric first reaches ready-for-traffic, every call is refused `fabric-not-ready` (R-PB1)"): N's
/// fabric has not first reached ready-for-traffic (its view holds the fabric in state-sync and it never
/// knew it ready): P's node.primary.update call on N is refused `RejectedFabricNotReady` naming the
/// initial fabric's state, under the successor's reject span `fabric-not-ready`; N installs nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_successor_refuses_a_call_before_the_initial_fabric_is_ready() {
    if crate::own_process::delegated(module_path!(), "the_successor_refuses_a_call_before_the_initial_fabric_is_ready") {
        return;
    }
    let spans = capture();
    let f = commanded_fabric().await;
    f.n.door.topology.write().await.fabric.status = ScopeStatus::StateSync;
    let op = "node.primary.update:b1:stop-node:mesh1.admin.1";
    let r = send(&f.p, &f.n, primary_update(&f.p, &f.n, 4, "b1", 1, op)).await;
    assert!(matches!(&r, Reply::RejectedFabricNotReady { reason } if reason.contains("state-sync") && reason.contains("ready-for-traffic")), "{r:?}");
    assert_eq!(f.n.door.membership.seats().mesh("mesh1"), Some(holder_of(&f.p, 3)), "N installs nothing");
    let named = refusals(&spans);
    assert!(named.iter().any(|(side, reason, _)| side == "successor" && reason == "fabric-not-ready"), "{named:?}");
    let _ = (&f.m, &f.x);
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_primary_update_command_from_an_endpoint_no_view_holds_is_decided_on_the_seat_it_names() {
    if crate::own_process::delegated(module_path!(), "a_primary_update_command_from_an_endpoint_no_view_holds_is_decided_on_the_seat_it_names") {
        return;
    }
    let spans = capture();
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
}

/// CONTRACT (node-rpc-envelope.md, op 0x21: "Epoch overflow cannot commit and is refused by name with
/// a typed refusal"; proposal Part C1 "Refused before the row ... the writer's own pre-check of the
/// call's epoch"): a seat whose epoch is `u64::MAX` cannot advance. The fabric-primary's command naming
/// mesh1's seat at `u64::MAX` is refused `RejectedEpochOverflow { seat, epoch: u64::MAX }` by the
/// holder, its reject span naming the reason `epoch-overflow`, the seat and the epoch. Nothing commits:
/// no row, no cut-off, the seat where it was.
#[test]
fn a_seat_epoch_at_its_maximum_is_refused_by_name_and_nothing_commits() {
    let cap = crate::mesh_leave_resume::capture();
    let seats_op = "drain-node:mesh1.admin.1";
    let (reply, trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.seat.reject.via-epoch-overflow");
        let trace_id = crate::mesh_leave_resume::trace_id(&root);
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
    for name in [ROW_WRITTEN, DRAINED, UPDATED] {
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
