//! node.primary.update and node.fabric-primary.update (lifecycles/node-primary-update.md;
//! node-rpc-envelope.md "PrimaryUpdate, op `0x21`, rdm") between in-process node-admins and nodes over
//! real Node RPC on op `0x21`. The primary, commanded from above, computes its successor by the election
//! over its own view with itself excluded, finishes the writes it issued (a node-admin), and calls
//! `PrimaryUpdate` on the successor, which installs the election's result and answers
//! `node.primary.updated`; the answer goes up. No row and no frame of its own record the change. Each cell
//! reads the spans the update emitted, in the order they happened.
// @feature: node-lifecycle

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::framing::Birth;
use rdm_core::{FabricId, IncarnationId, MeshId, NodeId, Seat, Primary};
use rdm_transport::topology::{Backbone, TopologyView};
use rdm_admin::admin::Records;
use rdm_admin::election::{resolve_with, Incumbency};
use rdm_admin::fabric_handover::{update_operation, HandoverDoor, HandoverSlot, Moving, ProjectView};
use rdm_admin::model::{EndpointId, Fabric, Node, NodeStatus, ProviderKind, ScopeStatus};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, Found, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::fabric_storage::MemoryFabricStorage;
use rdm_admin::storage::MemoryNodesStorage;
use rdm_admin::topology::Topology;
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rdm_core::catalog::OpOwner;
use rdm_core::ping::{Ping, PingReply, PingRequest};
use rdm_core::handover::{HandoverBirth, NodePrimaryUpdate, NodePrimaryUpdateReply as Reply, NodePrimaryUpdateRequest as Request};
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

/// The primary finished the writes it issued, before its call (node-primary-update.md step 3).
const WRITES_FINISHED: &str = "rdm.node_admin.primary.update.via-issued-writes-finished";
/// The successor installed the election's result and answered node.primary.updated (step 4).
const UPDATED: &str = "rdm.node_admin.primary.update.via-primary-updated";
/// A refusal of the primitive.
const REJECTED: &str = "rdm.node_admin.primary.reject.via-primary-update";
/// node investigation on a successor that did not answer the call.
const INVESTIGATION: &str = "rdm.node_admin.primary.resolve.via-successor-investigation";
/// The commander's node.primary.update.
const COMMAND: &str = "rdm.node_admin.primary.update.via-command";
/// One Node RPC call at its caller: its target, op, outcome, reason and elapsed time.
const CALL: &str = "rdm.node_rpc.request.update.via-call";

fn view(nodes: Vec<Node>) -> Topology {
    Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process }, meshes: vec![], nodes }
}

struct Admin {
    door: Arc<HandoverDoor>,
    client: Arc<NodeRpcClient>,
    resolver: Arc<StaticResolver>,
    resolved: ResolvedNode,
    /// The fabric store this admin's records fold and point-read: one store shared by every admin of
    /// a fabric (R14).
    status: Arc<MemoryFabricStorage>,
    /// How many times the primary finished the writes it issued (node-primary-update.md step 3).
    finishes: Arc<std::sync::atomic::AtomicUsize>,
    /// Holds the finish open while `false`: a write still issued when the primary finishes its writes.
    writes_finish: tokio::sync::watch::Sender<bool>,
    /// A door set here fills the admin's handover slot (op `0x21`, `NotReady` while empty) as its
    /// server answers node investigation's probe.
    ready_on_ping: Arc<OnceLock<Arc<HandoverDoor>>>,
    _router: Router,
}

impl Admin {
    /// How many times this admin finished the writes it issued (node-primary-update.md step 3).
    fn finished(&self) -> usize {
        self.finishes.load(std::sync::atomic::Ordering::SeqCst)
    }
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

async fn admin(fabric: &FabricId, mesh: &str, path: &str, node_id: &str, fill_slot: bool, status: Arc<MemoryFabricStorage>) -> Admin {
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let time = rdm_transport::clock::RafkaTime::unadopted();
    time.adopt(5_000_000);
    let (node_id, incarnation) = (NodeId::parse(node_id).unwrap(), IncarnationId::mint());
    let topology_view = TopologyView::join(&gossip, &ep, fabric, mesh, &MeshId::mint(), path, Arc::new(time), vec![], &[]).await.unwrap();
    let backbone = Backbone::join(&gossip, &ep, &topology_view, mesh, rdm_core::PublisherId { node: path.to_string(), incarnation: incarnation.clone(), node_id: node_id.clone() }, vec![]).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    let client = Arc::new(NodeRpcClient::new(ep.clone(), resolver.clone()));
    let slot: HandoverSlot = Arc::new(OnceLock::new());
    // node investigation's probe, `ProbeNodeState` of this exact birth on 0x1B (node-tickle.md "Probe"),
    // answered `Current` by the live birth. A door left in `ready_on_ping` fills the handover slot as the
    // probe is answered: an admin whose door held no view at one call and holds it by the next.
    let ready_on_ping: Arc<OnceLock<Arc<HandoverDoor>>> = Arc::new(OnceLock::new());
    let pinged = {
        let (slot, ready) = (slot.clone(), ready_on_ping.clone());
        let (probe_slot, probe_ready, me, born) = (slot.clone(), ready_on_ping.clone(), node_id.clone(), incarnation.clone());
        ServerBuilder::new()
            .serve::<Ping, _, _>(OpOwner::Core, move |_peer: rdm_transport::PeerContext, req: PingRequest| {
                let (slot, ready) = (slot.clone(), ready.clone());
                async move {
                    if let Some(door) = ready.get() {
                        let _ = slot.set(door.clone());
                    }
                    let PingRequest::Ping { payload } = req;
                    Ok(PingReply::Pong { payload })
                }
            })
            .serve::<rdm_core::status::Status, _, _>(OpOwner::Product("rdm".into()), move |_peer: rdm_transport::PeerContext, req: rdm_core::status::StatusRequest| {
                let (slot, ready, me, born) = (probe_slot.clone(), probe_ready.clone(), me.clone(), born.clone());
                async move {
                    Ok(match req {
                        rdm_core::status::StatusRequest::ProbeNodeState { node_id, incarnation } if node_id == me && incarnation == born => {
                            if let Some(door) = ready.get() {
                                let _ = slot.set(door.clone());
                            }
                            rdm_core::status::StatusReply::Current { node_id, incarnation, state: rdm_core::status::NodeState::Ready }
                        }
                        other => rdm_core::reply::status::not_ready(format!("this cell's admin answers only the probe of its own birth, not {}", other.op())),
                    })
                }
            })
    };
    let server = rdm_admin::fabric_handover::serve(pinged, slot.clone()).build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() }).expect("the catalog is built");
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip).accept(rdm_transport::ALPN, server).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { node_id: node_id.clone(), name: path.parse().unwrap(), endpoint_id: ep.id(), transport_addr: addr, incarnation: incarnation.clone() };
    let topology = Arc::new(tokio::sync::RwLock::new(view(vec![])));
    let finishes: Arc<std::sync::atomic::AtomicUsize> = Arc::default();
    let (writes_finish, writes_gate) = tokio::sync::watch::channel(true);
    let records = {
        let records = Arc::new(Records::default());
        records.set_status(Arc::new(rdm_admin::storage::MemoryNodesStorage::default()), Arc::new(rdm_admin::storage::MemoryMeshStorage::default()), status.clone(), mesh.to_string(), path.parse().unwrap(), node_id.clone());
        records
    };
    // The cell's view is set by hand, not projected from digests: the projection runs the one election
    // over its members with the primaries this admin holds now, as the admin's own projection does
    // (`admin::view_projector`).
    let project_view: ProjectView = {
        let (topology, seats) = (topology.clone(), topology_view.primaries().clone());
        Arc::new(move || {
            let (topology, seats) = (topology.clone(), seats.clone());
            Box::pin(async move {
                let mut view = topology.write().await;
                let inc = Incumbency { fabric: seats.fabric(), meshes: seats.meshes(), cohorts: seats.node_type_primaries(), lost: seats.gone() };
                resolve_with(&mut view.nodes, &inc);
            })
        })
    };
    let (provider, nodes) = (Arc::new(Inspected::default()), Arc::new(MemoryNodesStorage::default()));
    let looker = rdm_admin::seat_watch::Looker { me: path.parse().unwrap(), me_id: node_id.clone(), topology_view: topology_view.clone(), client: client.clone(), provider: provider.clone(), nodes: Some(nodes.clone()), heard_again: Default::default(), connections: None, commands: None };
    let door = Arc::new(HandoverDoor {
        fabric: "fabric1".into(),
        me: path.parse().unwrap(),
        node_id: node_id.clone(),
        incarnation,
        topology,
        digest_view: topology_view.digest_view.clone(),
        topology_view,
        backbone,
        client: client.clone(),
        records,
        locks: Default::default(),
        memo: Default::default(),
        decisions: Arc::default(),
        project_view,
        finish_writes: {
            let finishes = finishes.clone();
            Arc::new(move || {
                finishes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
    Admin { door, client, resolver, resolved, status, finishes, writes_finish, ready_on_ping, _router: router }
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

fn exact(r: &ResolvedNode) -> Birth {
    Birth { node_id: r.node_id.clone(), incarnation: r.incarnation.clone() }
}

fn primary_of(a: &Admin, revision: u64) -> Primary {
    Primary { mesh: a.resolved.name.mesh.clone(), node_id: a.resolved.node_id.clone(), incarnation: a.resolved.incarnation.clone(), revision }
}

/// One entry of a call's election result: `node_type`, `from` to `to`, at `revision`.
fn entry(node_type: &str, from: &ResolvedNode, to: &ResolvedNode, revision: u64) -> (String, Birth, Birth, u64) {
    (node_type.to_string(), exact(from), exact(to), revision)
}

/// One call; a call with no reply panics naming the outcome and the leg it used.
async fn send(from: &Admin, to: &Admin, req: Request) -> Reply {
    let (out, ev) = from.client.call::<NodePrimaryUpdate>(&NodeTarget::ExactNode(to.resolved.node_id.clone()), &req, &CallOptions::default()).await;
    out.reply().unwrap_or_else(|| panic!("no reply: {out:?} evidence {ev:?}")).value().clone()
}

/// The primary record `holder` holds for `seat` in `a`'s view (the election's input).
async fn primary_at(a: &Admin, seat: Seat, holder: &Primary) {
    a.door.topology_view.primaries().take(seat, holder);
}

/// The (side, reason, detail) of every refusal span of the primitive.
fn refusals(spans: &Spans) -> Vec<(String, String, String)> {
    spans.named(REJECTED).iter().map(|f| (f["side"].clone(), f["reason"].clone(), f.get("detail").cloned().unwrap_or_default())).collect()
}

/// A captured cell leaves its spans whether it passed or panicked: every line of [`Spans::lines`]
/// goes to `$TMPDIR/i143-testkit/fabric_handover/<cell>.<pid>.spans.txt`, so a red reads from its own
/// spans and never from a rerun.
struct SpanFile(Spans);

impl Drop for SpanFile {
    fn drop(&mut self) {
        let cell = std::thread::current().name().unwrap_or("unnamed").replace("::", "__");
        let dir = std::env::temp_dir().join("i143-testkit/fabric_handover");
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
/// pipeline (`rdm_transport::span_capture`) in front of an in-memory exporter stamps the trace
/// ids, and [`Spans`] keeps every span in the order it was created with the fields recorded on it.
/// Both sit behind one dispatcher that is the default of the calling thread and of every thread of
/// the runtime [`TraceCapture::run`] builds for the cell, so two cells of one process never see each
/// other's spans.
struct TraceCapture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
    spans: Spans,
}

fn trace_capture(service: &'static str) -> TraceCapture {
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let spans = Spans::default();
    let (capture, layer) = rdm_transport::span_capture(service, exporter.clone());
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
            let traceparent = rdm_transport::traceparent_of(&root).expect("the cell's root span has a trace context");
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
/// (name, trace, attributes) goes to `$TMPDIR/i143-testkit/fabric_handover/<cell>.<pid>.spans.json`,
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
        let dir = std::env::temp_dir().join("i143-testkit/fabric_handover");
        let path = dir.join(format!("{cell}.{}.spans.json", std::process::id()));
        let written = std::fs::create_dir_all(&dir).and_then(|_| std::fs::write(&path, serde_json::to_vec_pretty(&spans).unwrap()));
        eprintln!("fabric_handover: the cell's {} spans -> {} (flush: {flushed:?}, write: {written:?})", spans.len(), path.display());
    }
}

/// mesh1: P (`mesh1.admin.1`) is mesh1's mesh-primary at revision 3; N (`mesh1.admin.2`) is the lowest
/// Ready NodeId of mesh1's other node-admins, the successor P's election names; M (`mesh1.admin.3`) is a
/// Ready admin of the same mesh with a higher NodeId. mesh2: X, its mesh-primary and only node-admin, with
/// the lowest NodeId of the fabric (so an election across meshes would name X, never N). [`FabricPrimary`]
/// says who is the fabric-primary at revision 7.
struct Seats {
    p: Admin,
    n: Admin,
    m: Admin,
    x: Admin,
}

/// Who is the fabric-primary of a [`Seats`] fabric.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FabricPrimary {
    /// P is the mesh-primary and the fabric-primary: it moves its own primaries (it has nothing above it).
    P,
    /// X is the fabric-primary and commands P's update.
    X,
}

async fn primaries_fabric(fp: FabricPrimary) -> Seats {
    primaries_fabric_over(fp, true).await
}

/// The seats fabric whose N holds no view at the first call it is made (its door `NotReady`) and holds
/// it from its first answered probe on.
async fn fabric_n_ready_on_probe(fp: FabricPrimary) -> Seats {
    let f = primaries_fabric_over(fp, false).await;
    let _ = f.n.ready_on_ping.set(f.n.door.clone());
    f
}

async fn primaries_fabric_over(fp: FabricPrimary, n_ready: bool) -> Seats {
    let (fabric_id, status) = (FabricId::mint(), Arc::<MemoryFabricStorage>::default());
    let p = admin(&fabric_id, "mesh1", "mesh1.admin.1", "000000000002", true, status.clone()).await;
    let n = admin(&fabric_id, "mesh1", "mesh1.admin.2", "000000000003", n_ready, status.clone()).await;
    let m = admin(&fabric_id, "mesh1", "mesh1.admin.3", "000000000009", true, status.clone()).await;
    let x = admin(&fabric_id, "mesh2", "mesh2.admin.1", "000000000001", true, status).await;
    let pn = node_of(&p, NodeStatus::Ready, fp == FabricPrimary::P);
    let mut nn = node_of(&n, NodeStatus::Ready, false);
    nn.is_primary = false;
    let mut mn = node_of(&m, NodeStatus::Ready, false);
    mn.is_primary = false;
    let xn = node_of(&x, NodeStatus::Ready, fp == FabricPrimary::X);
    let nodes = vec![pn, nn, mn, xn];
    let fabric_primary = match fp {
        FabricPrimary::P => primary_of(&p, 7),
        FabricPrimary::X => primary_of(&x, 7),
    };
    for a in [&p, &n, &m, &x] {
        *a.door.topology.write().await = view(nodes.clone());
        for b in [&p, &n, &m, &x] {
            a.resolver.insert(b.resolved.clone());
        }
        primary_at(a, Seat::FabricPrimary, &fabric_primary).await;
        primary_at(a, Seat::MeshPrimary, &primary_of(&p, 3)).await;
        primary_at(a, Seat::MeshPrimary, &primary_of(&x, 1)).await;
    }
    Seats { p, n, m, x }
}

/// A member node of a node type that is not node-admin, serving the 0x21 door every such node mounts
/// (`fabric_handover::serve_member`).
struct Member {
    door: Arc<rdm_admin::fabric_handover::MemberDoor>,
    resolved: ResolvedNode,
    resolver: Arc<StaticResolver>,
    fabric: FabricId,
    _router: Router,
}

async fn member(fabric: &FabricId, mesh: &str, path: &str, node_id: &str) -> Member {
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let time = rdm_transport::clock::RafkaTime::unadopted();
    time.adopt(5_000_000);
    let (node_id, incarnation) = (NodeId::parse(node_id).unwrap(), IncarnationId::mint());
    let topology_view = TopologyView::join(&gossip, &ep, fabric, mesh, &MeshId::mint(), path, Arc::new(time), vec![], &[]).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    let client = Arc::new(NodeRpcClient::new(ep.clone(), resolver.clone()));
    let slot: rdm_admin::fabric_handover::MemberSlot = Arc::new(OnceLock::new());
    let server = rdm_admin::fabric_handover::serve_member(ServerBuilder::new(), slot.clone()).build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() }).expect("the catalog is built");
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip).accept(rdm_transport::ALPN, server).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { node_id: node_id.clone(), name: path.parse().unwrap(), endpoint_id: ep.id(), transport_addr: addr, incarnation: incarnation.clone() };
    let door = Arc::new(rdm_admin::fabric_handover::MemberDoor::new(path.parse().unwrap(), node_id, incarnation, topology_view, client));
    let _ = slot.set(door.clone());
    Member { door, resolved, resolver, fabric: fabric.clone(), _router: router }
}

fn member_node(m: &Member, primary: bool) -> Node {
    let mut n = Node::allocated(m.resolved.name.clone());
    n.node_id = m.resolved.node_id.clone();
    n.incarnation_id = Some(m.resolved.incarnation.clone());
    n.endpoint_id = Some(EndpointId(m.resolved.endpoint_id.to_string()));
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n
}

/// The birth `r` names as its digest says it, Ready: what a member's view holds of a peer of
/// its mesh from its mesh-primary's projection.
fn digest_of(r: &ResolvedNode, fabric: &FabricId) -> rdm_core::MeshDigest {
    rdm_core::MeshDigest {
        fabric_id: fabric.clone(),
        node: rdm_core::MeshNode { node_id: r.node_id.clone(), name: r.name.clone(), endpoint_id: rdm_core::EndpointId(r.endpoint_id.to_string()), transport_addr: r.transport_addr, incarnation: r.incarnation.clone(), supersedes: None, runtime: None },
        status: rdm_core::MemberStatus::Ready,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 1,
        emitted_at_rafka_ms: 1,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None,
        held_hash: Default::default(),
        is_primary: None,
        is_fabric_primary: None,
    }
}

fn member_primary(m: &Member, revision: u64) -> Primary {
    Primary { mesh: m.resolved.name.mesh.clone(), node_id: m.resolved.node_id.clone(), incarnation: m.resolved.incarnation.clone(), revision }
}

const RPC: rdm_admin::model::NodeKind = rdm_admin::model::NodeKind::RpcNode;

/// P is mesh1's mesh-primary (revision 3), X is mesh2's and the fabric-primary (revision 7). B1
/// (`mesh1.rpc.1`) is mesh1's rpc-node primary at revision 2; B2 (`mesh1.rpc.2`) is that type's other
/// node, born only when `with_b2`; every node's view holds the other rpc node Ready.
struct TypeFabric {
    p: Admin,
    x: Admin,
    b1: Member,
    b2: Option<Member>,
}

impl TypeFabric {
    fn b2(&self) -> &Member {
        self.b2.as_ref().expect("B2 was born")
    }
}

async fn type_fabric(with_b2: bool) -> TypeFabric {
    type_fabric_heard(with_b2, true).await
}

/// [`type_fabric`], B1's own view holding B2 (its digest, Ready) only when `b1_hears_b2`: the
/// mesh-primary's view holds B2 Ready either way.
async fn type_fabric_heard(with_b2: bool, b1_hears_b2: bool) -> TypeFabric {
    let (fabric_id, status) = (FabricId::mint(), Arc::<MemoryFabricStorage>::default());
    let p = admin(&fabric_id, "mesh1", "mesh1.admin.1", "000000000008", true, status.clone()).await;
    let x = admin(&fabric_id, "mesh2", "mesh2.admin.1", "000000000001", true, status).await;
    let b1 = member(&fabric_id, "mesh1", "mesh1.rpc.1", "000000000004").await;
    let b2 = if with_b2 { Some(member(&fabric_id, "mesh1", "mesh1.rpc.2", "000000000006").await) } else { None };
    let mut pn = node_of(&p, NodeStatus::Ready, false);
    pn.is_primary = true;
    let xn = node_of(&x, NodeStatus::Ready, true);
    let mut nodes = vec![pn, xn, member_node(&b1, true)];
    nodes.extend(b2.iter().map(|b| member_node(b, false)));
    let resolved: Vec<ResolvedNode> = [p.resolved.clone(), x.resolved.clone(), b1.resolved.clone()].into_iter().chain(b2.iter().map(|b| b.resolved.clone())).collect();
    for a in [&p, &x] {
        *a.door.topology.write().await = view(nodes.clone());
        for r in &resolved {
            a.resolver.insert(r.clone());
        }
        primary_at(a, Seat::FabricPrimary, &primary_of(&x, 7)).await;
        primary_at(a, Seat::MeshPrimary, &primary_of(&p, 3)).await;
        primary_at(a, Seat::MeshPrimary, &primary_of(&x, 1)).await;
        primary_at(a, Seat::Cohort(RPC), &member_primary(&b1, 2)).await;
    }
    let members: Vec<&Member> = std::iter::once(&b1).chain(b2.iter()).collect();
    for m in &members {
        for r in &resolved {
            m.resolver.insert(r.clone());
        }
        m.door.topology_view().primaries().take(Seat::Cohort(RPC), &member_primary(&b1, 2));
        for other in members.iter().filter(|o| o.resolved.node_id != m.resolved.node_id && (b1_hears_b2 || m.resolved.node_id != b1.resolved.node_id)) {
            assert!(m.door.topology_view().digest_view.record(digest_of(&other.resolved, &other.fabric)));
            // Its mesh-primary's projection holds it Ready.
            assert!(m.door.topology_view().digest_view.started(&other.resolved.node_id, &other.resolved.incarnation, &other.resolved.name));
        }
    }
    TypeFabric { p, x, b1, b2 }
}

/// The node at `name` in `a`'s view.
async fn node_at(a: &Admin, name: &str) -> Node {
    a.door.topology.read().await.members().find(|n| n.name.to_string() == name).cloned().unwrap_or_else(|| panic!("{name} is in the view"))
}

/// CONTRACT (node-primary-update.md steps 2-4 and "Both primaries": "When the mesh-primary is also the
/// fabric-primary and the successor is in the same mesh, the successor becomes both and records both on
/// its own row"; step 6: "The commanding Build continues on the successor's direct acknowledgement of step
/// 4, never on gossip"; gossip.md "A primary change on gossip": "The successor's next `Digest` carries
/// `is_primary` (and `is_fabric_primary`) at the next revision"): P, mesh1's mesh-primary and the
/// fabric-primary, moves both to N, the successor the election over its own view names with P excluded
/// (never X, the lowest NodeId of the fabric, of another mesh). In order: P finishes the writes it
/// issued, then N installs the election's result and answers node.primary.updated, which P installs: N
/// is mesh-primary at 4 and fabric-primary at 8 in N's view and P's, P's view names N both, and N's next
/// digest carries `is_primary` 4 and `is_fabric_primary` 8, which its rows writer records on its own row
/// (`admin::tests::a_row_the_store_refused_is_written_on_the_writers_own_clock_once_the_store_recovers`).
/// M, which nobody called, holds what it held. What must NOT happen: a call before the writes finished,
/// P naming itself after the answer, or a record of anyone's but the successor's.
#[test]
fn a_mesh_primary_hands_over_to_its_same_mesh_successor_which_answers_directly_and_is_both_primaries() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    let _file = SpanFile(spans.clone());
    cap.run(async {
        let f = primaries_fabric(FabricPrimary::P).await;
        let op = update_operation(Seat::MeshPrimary, "b1", 1, "node.stop:mesh1.admin.1");
        let revisions = f.p.door.update_own_primaries(Moving::BothSeats, "b1", 1, &op, None).await.expect("both primaries move");
        assert_eq!(revisions, vec![entry("node-admin", &f.p.resolved, &f.n.resolved, 4), entry("fabric", &f.p.resolved, &f.n.resolved, 8)]);
        for a in [&f.p, &f.n] {
            assert_eq!(a.door.topology_view.primaries().mesh("mesh1"), Some(primary_of(&f.n, 4)), "{}", a.resolved.name);
            assert_eq!(a.door.topology_view.primaries().fabric(), Some(primary_of(&f.n, 8)), "{}", a.resolved.name);
        }
        let pv = f.p.door.topology.read().await.clone();
        assert_eq!(pv.node_type_primary("mesh1", rdm_admin::model::NodeKind::NodeAdmin).map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.2"), "P's view names N mesh-primary");
        assert_eq!(fabric_primary_of(&pv).as_deref(), Some("mesh1.admin.2"), "P's view names N fabric-primary");
        let mut d = digest_of(&f.n.resolved, &FabricId::mint());
        f.n.door.digest_view.recorded_primaries(&mut d);
        assert_eq!((d.is_primary, d.is_fabric_primary), (Some(4), Some(8)), "N's next digest carries both at their revisions");
        let (finished, installed) = (spans.position(WRITES_FINISHED).expect("P finished its writes"), spans.position(UPDATED).expect("N installed"));
        assert!(finished < installed, "P finished its writes before N installed: {:?}", spans.names());
        assert_eq!(f.p.finished(), 1);
        assert_eq!(f.m.door.topology_view.primaries().mesh("mesh1"), Some(primary_of(&f.p, 3)), "M, which nobody called, takes the change from N's digest");
        assert!(refusals(&spans).is_empty(), "{:?}", refusals(&spans));
        let _ = &f.x;
    });
}

/// CONTRACT (node-primary-update.md failure contract, "Discovery is not installation": "The resuming
/// executor installs from the election locally and obtains the successor's `node.primary.updated` ...
/// before it commands the departing primary's destructive step"): only a departing primary's update is
/// acknowledged. M (`mesh1.admin.3`) was never mesh1's primary: X, resuming an attempt whose node.stop
/// acts on M, finds no update to acknowledge and makes no call. Once P's update to N happened (P
/// replaced by N at 4 in X's view), the same read of the update named for P obtains N's
/// node.primary.updated on a call. Must NOT happen: a node that was never primary read as a deleted
/// primary, or a 0x21 call made for it.
#[test]
fn an_update_named_for_a_node_that_was_never_primary_is_no_update_and_calls_no_one() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    let _file = SpanFile(spans.clone());
    cap.run(async {
        let f = primaries_fabric(FabricPrimary::X).await;
        let m_name: rdm_admin::model::PathName = "mesh1.admin.3".parse().unwrap();
        let op = update_operation(Seat::MeshPrimary, "b1", 1, "node.stop:mesh1.admin.3");
        assert_eq!(f.x.door.row_acknowledged("mesh1", &op, Some(&m_name), "b1", 2).await, Ok(false), "M was never primary: no update to acknowledge");
        let calls = |spans: &Spans| spans.named(CALL).into_iter().filter(|c| c["op"] == "33").count();
        assert_eq!(calls(&spans), 0, "no 0x21 call for a node that was never primary");
        // P's update to N happened: X holds N at 4, P replaced.
        primary_at(&f.x, Seat::MeshPrimary, &primary_of(&f.n, 4)).await;
        let p_name: rdm_admin::model::PathName = "mesh1.admin.1".parse().unwrap();
        let p_op = update_operation(Seat::MeshPrimary, "b1", 1, "node.stop:mesh1.admin.1");
        assert_eq!(f.x.door.row_acknowledged("mesh1", &p_op, Some(&p_name), "b1", 2).await, Ok(true), "P's update to N is acknowledged on N's node.primary.updated");
        assert_eq!(calls(&spans), 1, "one 0x21 call, to N");
        let _ = &f.m;
    });
}

/// CONTRACT (node-primary-update.md Roles: "For the node-admin type the primary is the mesh-primary and
/// the commander is the fabric-primary"; node-rpc-envelope.md: "the fabric-primary commands a
/// mesh-primary's update with `UpdatePrimary { node_type, build_id, run, operation, successor }` on the
/// primary, `successor` none: a node-admin computes the election itself ...
/// The primary runs the sequence and answers `PrimaryUpdated { revisions }`"): X, the fabric-primary,
/// commands P's update. P names N by the election over its own view, calls it, and answers X with N's
/// node.primary.updated, which X installs before its Build continues: X, P and N hold N mesh-primary of
/// mesh1 at 4; the fabric-primary stays X at 7. One command span at X, sent to P.
#[test]
fn the_fabric_primary_commands_a_mesh_primarys_update_and_installs_the_successors_answer() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    let _file = SpanFile(spans.clone());
    cap.run(async {
        let f = primaries_fabric(FabricPrimary::X).await;
        let p = node_at(&f.x, "mesh1.admin.1").await;
        let op = update_operation(Seat::MeshPrimary, "b1", 1, "node.stop:mesh1.admin.1");
        let revisions = f.x.door.command_update_primary(&p, "b1", 1, &op).await.expect("the command answers node.primary.updated");
        assert_eq!(revisions, vec![entry("node-admin", &f.p.resolved, &f.n.resolved, 4)]);
        for a in [&f.x, &f.p, &f.n] {
            assert_eq!(a.door.topology_view.primaries().mesh("mesh1"), Some(primary_of(&f.n, 4)), "{}", a.resolved.name);
            assert_eq!(a.door.topology_view.primaries().fabric(), Some(primary_of(&f.x, 7)), "{}: the fabric-primary stays X", a.resolved.name);
        }
        let commands = spans.named(COMMAND);
        assert_eq!(commands.iter().map(|c| (c["to"].clone(), c["outcome"].clone())).collect::<Vec<_>>(), vec![("mesh1.admin.1".to_string(), "updated".to_string())], "{commands:?}");
        let _ = &f.m;
    });
}

/// CONTRACT (node-rpc-envelope.md 0x21: "An identical retry answers the same PrimaryUpdated before any
/// revision check. A different payload for the same operation is RejectedConflictingPayload"): P's call
/// on N, repeated identical, is answered as the first was; a call naming another revision under the same
/// operation is refused by name and installs nothing.
#[test]
fn an_identical_retry_is_answered_as_the_first_and_another_payload_for_the_operation_is_refused() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    cap.run(async {
        let f = primaries_fabric(FabricPrimary::X).await;
        let op = "node.primary.update:b1:1:node.stop:mesh1.admin.1";
        let call = |revision| Request::PrimaryUpdate { primaries: vec![entry("node-admin", &f.p.resolved, &f.n.resolved, revision)], build_id: "b1".into(), run: 1, operation: op.into() };
        let first = send(&f.p, &f.n, call(4)).await;
        assert_eq!(first, Reply::PrimaryUpdated { revisions: vec![entry("node-admin", &f.p.resolved, &f.n.resolved, 4)] });
        assert_eq!(send(&f.p, &f.n, call(4)).await, first, "an identical retry answers the same");
        assert_eq!(send(&f.p, &f.n, call(5)).await, Reply::RejectedConflictingPayload { operation: op.into() });
        assert_eq!(f.n.door.topology_view.primaries().mesh("mesh1"), Some(primary_of(&f.n, 4)), "the conflicting payload installs nothing");
        assert_eq!(spans.named(UPDATED).len(), 1, "one install");
        let _ = (&f.m, &f.x);
    });
}

/// CONTRACT (node-primary-update.md failure contract: "The receiver never refuses the call for the birth
/// it names (R-UC1a, R-RX5); it installs from the election's result the call carries and answers"): P's
/// call reaches M, a node-admin that is not the successor it names (N). M installs the election's result
/// it carries and answers node.primary.updated; nothing is refused.
#[test]
fn a_primary_update_call_is_never_refused_for_the_birth_it_names() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    cap.run(async {
        let f = primaries_fabric(FabricPrimary::X).await;
        let op = "node.primary.update:b1:1:node.stop:mesh1.admin.1";
        let r = send(&f.p, &f.m, Request::PrimaryUpdate { primaries: vec![entry("node-admin", &f.p.resolved, &f.n.resolved, 4)], build_id: "b1".into(), run: 1, operation: op.into() }).await;
        assert_eq!(r, Reply::PrimaryUpdated { revisions: vec![entry("node-admin", &f.p.resolved, &f.n.resolved, 4)] });
        assert_eq!(f.m.door.topology_view.primaries().mesh("mesh1"), Some(primary_of(&f.n, 4)));
        assert!(refusals(&spans).is_empty(), "{:?}", refusals(&spans));
        let _ = &f.x;
    });
}

/// A fabric of two meshes, each with one node-admin: P of mesh1, mesh-primary at 3 and the fabric-primary
/// at 7, and X of mesh2, mesh-primary at 1, at `x_status` in every view.
async fn lone_fabric(x_status: NodeStatus) -> (Admin, Admin) {
    let (fabric_id, status) = (FabricId::mint(), Arc::<MemoryFabricStorage>::default());
    let p = admin(&fabric_id, "mesh1", "mesh1.admin.1", "000000000009", true, status.clone()).await;
    let x = admin(&fabric_id, "mesh2", "mesh2.admin.1", "000000000005", true, status).await;
    let nodes = vec![node_of(&p, NodeStatus::Ready, true), node_of(&x, x_status, false)];
    for a in [&p, &x] {
        *a.door.topology.write().await = view(nodes.clone());
        for b in [&p, &x] {
            a.resolver.insert(b.resolved.clone());
        }
        primary_at(a, Seat::FabricPrimary, &primary_of(&p, 7)).await;
        primary_at(a, Seat::MeshPrimary, &primary_of(&p, 3)).await;
        primary_at(a, Seat::MeshPrimary, &primary_of(&x, 1)).await;
    }
    (p, x)
}

/// CONTRACT (node-primary-update.md "No-successor cases": "`node.primary.update` commanded for a type
/// with no other node of that type in the mesh is refused by name, `cannot transfer: no secondary of type
/// <node_type> in mesh <mesh>` ... The primary stays primary"; "The fabric-primary" search step 3:
/// "neither found: refused by name, `cannot transfer fabric-primary: no other node-admin Ready in
/// mesh <mesh>, no other mesh-primary Ready`"; node-rpc-envelope.md: the commanded arm ends
/// `Failed { step, reason, code }`): P, the only node-admin of mesh1, cannot move its mesh-primary; with
/// X not Ready it cannot move the fabric-primary either. Each fails by its exact text, no writes are
/// finished and P stays both.
#[test]
fn an_update_with_no_successor_fails_by_its_exact_text_and_nothing_moves() {
    use rdm_admin::build_drive::SeatMoveEnd;
    let cap = trace_capture("fabric-handover");
    cap.run(async {
        let (p, x) = lone_fabric(NodeStatus::Pending).await;
        let op = update_operation(Seat::MeshPrimary, "b1", 1, "node.stop:mesh1.admin.1");
        let mesh = p.door.update_own_primaries(Moving::MeshSeat, "b1", 1, &op, None).await;
        assert!(matches!(&mesh, Err(SeatMoveEnd::Failed { step, reason, .. }) if step == "primary-update" && reason == "cannot transfer: no secondary of type node-admin in mesh mesh1"), "{mesh:?}");
        let op = update_operation(Seat::FabricPrimary, "b1", 1, "node.stop:mesh1.admin.1");
        let fabric = p.door.update_own_primaries(Moving::FabricSeatOutside, "b1", 1, &op, None).await;
        assert!(matches!(&fabric, Err(SeatMoveEnd::Failed { reason, .. }) if reason == "cannot transfer fabric-primary: no other node-admin Ready in mesh mesh1, no other mesh-primary Ready"), "{fabric:?}");
        assert_eq!(p.finished(), 0, "no writes finished for a refused update");
        assert_eq!((p.door.topology_view.primaries().mesh("mesh1"), p.door.topology_view.primaries().fabric()), (Some(primary_of(&p, 3)), Some(primary_of(&p, 7))), "P stays both");
        let _ = &x;
    });
}

/// CONTRACT (node-primary-update.md "The fabric-primary": "A successor in another mesh (search step 2)
/// becomes fabric-primary and records `is_fabric_primary` on its own row ... under mesh.delete stays
/// mesh-primary until its own node.delete"; mesh-delete.md step 1): P, the only node-admin of mesh1 and
/// the fabric-primary, moves the fabric-primary under mesh.delete of mesh1 to X, mesh2's mesh-primary. X
/// is fabric-primary at 8 in both views and P stays mesh1's mesh-primary at 3; the mesh.delete gate finds
/// the fabric-primary no node-admin of mesh1.
#[test]
fn the_fabric_primary_moved_outside_its_mesh_leaves_its_old_primary_mesh_primary() {
    let cap = trace_capture("fabric-handover");
    cap.run(async {
        let (p, x) = lone_fabric(NodeStatus::Ready).await;
        let op = update_operation(Seat::FabricPrimary, "b1", 1, "mesh.delete:mesh1");
        assert!(p.door.fabric_primary_updated("mesh1", &op).await.is_err(), "before the update the fabric-primary is of mesh1");
        let revisions = p.door.update_own_primaries(Moving::FabricSeatOutside, "b1", 1, &op, None).await.expect("the fabric-primary moves to mesh2");
        assert_eq!(revisions, vec![entry("fabric", &p.resolved, &x.resolved, 8)]);
        for a in [&p, &x] {
            assert_eq!(a.door.topology_view.primaries().fabric(), Some(primary_of(&x, 8)), "{}", a.resolved.name);
            assert_eq!(a.door.topology_view.primaries().mesh("mesh1"), Some(primary_of(&p, 3)), "{}: P stays mesh1's mesh-primary", a.resolved.name);
        }
        p.door.fabric_primary_updated("mesh1", &op).await.expect("the fabric-primary is no node-admin of mesh1 any more");
    });
}

/// CONTRACT (node-primary-update.md failure contract: "The call fails by name, and node investigation on
/// the successor's exact birth decides (Q111): reached, the call is retried against it"): N holds no view
/// at P's first call (its door answers `NotReady`); node investigation reaches N's exact birth, and the
/// call made again is answered node.primary.updated. One investigation span, verdict `reached`, retry
/// `primary-updated`.
#[test]
fn a_successor_that_does_not_answer_is_investigated_and_reached_the_call_is_made_again() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    let _file = SpanFile(spans.clone());
    cap.run(async {
        let f = fabric_n_ready_on_probe(FabricPrimary::P).await;
        let op = update_operation(Seat::MeshPrimary, "b1", 1, "node.stop:mesh1.admin.1");
        let revisions = f.p.door.update_own_primaries(Moving::BothSeats, "b1", 1, &op, None).await.expect("the call made again is answered");
        assert_eq!(revisions, vec![entry("node-admin", &f.p.resolved, &f.n.resolved, 4), entry("fabric", &f.p.resolved, &f.n.resolved, 8)]);
        let looked = spans.named(INVESTIGATION);
        assert_eq!(looked.iter().map(|l| (l["verdict"].clone(), l["retry"].clone())).collect::<Vec<_>>(), vec![("reached".to_string(), "primary-updated".to_string())], "{looked:?}");
        let _ = (&f.m, &f.x);
    });
}

/// CONTRACT (node-primary-update.md failure contract: "The successor is unheard after the update. When
/// node investigation's verdict is anything but reached, the step fails by name, `the successor is
/// unheard after the update: <verdict>`"): N is gone (its endpoint closed). P's call is not answered,
/// node investigation does not reach N, and the update ends `Failed` with that text; P is primary still in
/// its own view, which the ordinary election resolves.
#[test]
fn an_update_whose_successor_is_unheard_fails_by_name_with_the_investigations_verdict() {
    use rdm_admin::build_drive::SeatMoveEnd;
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    let _file = SpanFile(spans.clone());
    cap.run(async {
        let f = primaries_fabric(FabricPrimary::P).await;
        let _ = f.n._router.shutdown().await;
        let op = update_operation(Seat::MeshPrimary, "b1", 1, "node.stop:mesh1.admin.1");
        let r = f.p.door.update_own_primaries(Moving::BothSeats, "b1", 1, &op, None).await;
        assert!(matches!(&r, Err(SeatMoveEnd::Failed { step, reason, .. }) if step == "primary-update" && reason.starts_with("the successor is unheard after the update: mesh1.admin.2")), "{r:?}");
        assert_eq!(f.p.door.topology_view.primaries().mesh("mesh1"), Some(primary_of(&f.p, 3)), "nothing was answered, so nothing was installed at P");
        assert_eq!(spans.named(INVESTIGATION).len(), 1);
        let _ = (&f.m, &f.x);
    });
}

/// CONTRACT (node-primary-update.md: "one primitive for every node type"; Roles: "For any other type the
/// mesh-primary is commander and writer, and the primary only makes the call"; step 2: the writer
/// pre-checks; steps 4-5; node-rpc-envelope.md: "the mesh-primary commands another type's primary the same
/// way, `successor` the exact birth its pre-check named"): P, mesh1's mesh-primary, commands B1, mesh1's
/// rpc-node primary, naming B2. B1 calls B2, and answers P with B2's node.primary.updated, which P
/// installs: P, B1 and B2 hold B2 rpc-node primary of mesh1 at 3. P finished no writes for it: a non-admin
/// primary has no mesh rows, and B2's row is recorded from its digest.
#[test]
fn a_mesh_primary_commands_its_rpc_primary_naming_the_successor_its_pre_check_found_and_the_primary_calls_it() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    let _file = SpanFile(spans.clone());
    cap.run(async {
        let f = type_fabric(true).await;
        let b1 = node_at(&f.p, "mesh1.rpc.1").await;
        let op = update_operation(Seat::Cohort(RPC), "b1", 1, "node.stop:mesh1.rpc.1");
        let revisions = f.p.door.update_node_type_primary(&b1, None, "b1", 1, &op).await.expect("mesh1's rpc-node primary moves");
        assert_eq!(revisions, vec![entry("rpc-node", &f.b1.resolved, &f.b2().resolved, 3)]);
        assert_eq!(f.p.door.topology_view.primaries().cohort("mesh1", RPC), Some(member_primary(f.b2(), 3)));
        assert_eq!(f.b1.door.topology_view().primaries().cohort("mesh1", RPC), Some(member_primary(f.b2(), 3)), "B1 installed B2's answer");
        assert_eq!(f.b2().door.topology_view().primaries().cohort("mesh1", RPC), Some(member_primary(f.b2(), 3)), "B2 installed itself");
        assert_eq!(f.p.finished(), 0, "the mesh-primary finished no writes for another type's update");
        let _ = &f.x;
    });
}

/// CONTRACT (Q129; node-primary-update.md step 4: "for a type other than `node-admin`, the birth the
/// mesh-primary's command names (step 2)"): P's view holds B2 Ready; B1's own view has not heard
/// B2 at all. P's stop-time update of B1 completes: B1 calls the B2 P named and answers with B2's
/// node.primary.updated, with no election of its own and no `cannot transfer` refusal. A command naming no
/// successor at B1 is refused by name. What must NOT happen: B1 refusing because its own view holds no
/// secondary (the land-gate red at 1918053cb, memory_estate).
#[test]
fn an_rpc_primary_whose_own_view_has_not_heard_the_successor_hands_over_to_the_one_its_mesh_primary_named() {
    let cap = trace_capture("fabric-handover");
    cap.run(async {
        let f = type_fabric_heard(true, false).await;
        let b1 = node_at(&f.p, "mesh1.rpc.1").await;
        let op = update_operation(Seat::Cohort(RPC), "b1", 1, "node.stop:mesh1.rpc.1");
        let revisions = f.p.door.update_node_type_primary(&b1, None, "b1", 1, &op).await.expect("the hand-over completes on the successor P named");
        assert_eq!(revisions, vec![entry("rpc-node", &f.b1.resolved, &f.b2().resolved, 3)]);
        assert_eq!(f.b1.door.topology_view().primaries().cohort("mesh1", RPC), Some(member_primary(f.b2(), 3)), "B1 installed B2's answer");
        let bare = f.b1.door.serve(Request::UpdatePrimary { node_type: "rpc-node".into(), build_id: "b1".into(), run: 1, operation: update_operation(Seat::Cohort(RPC), "b1", 1, "node.stop:mesh1.rpc.2"), successor: None }).await;
        assert!(matches!(&bare, Reply::Failed { reason, .. } if reason == "mesh1.rpc.1 is a rpc-node primary: its mesh-primary's command names no successor"), "{bare:?}");
        let _ = &f.x;
    });
}

/// CONTRACT (CLAUDE.md "commands go down": fabric-primary -> mesh-primary -> node; node-primary-update.md
/// Commander: "a command the fabric-primary originates for one of them reaches it as a forwarded command
/// through the mesh-primary, never directly"): X, the fabric-primary, commands the update of mesh1's
/// rpc-node primary: its command goes to P, mesh1's mesh-primary, which commands B1. X installs B2's answer.
#[test]
fn the_fabric_primary_commands_an_rpc_primary_only_through_its_mesh_primary() {
    let cap = trace_capture("fabric-handover");
    let spans = cap.spans.clone();
    let _file = SpanFile(spans.clone());
    cap.run(async {
        let f = type_fabric(true).await;
        let b1 = node_at(&f.x, "mesh1.rpc.1").await;
        let op = update_operation(Seat::Cohort(RPC), "b1", 1, "node.stop:mesh1.rpc.1");
        let revisions = f.x.door.command_update_primary(&b1, "b1", 1, &op).await.expect("the command answers");
        assert_eq!(revisions, vec![entry("rpc-node", &f.b1.resolved, &f.b2().resolved, 3)]);
        assert_eq!(f.x.door.topology_view.primaries().cohort("mesh1", RPC), Some(member_primary(f.b2(), 3)));
        let commands = spans.named(COMMAND);
        assert_eq!(commands.iter().map(|c| c["to"].clone()).collect::<Vec<_>>(), vec!["mesh1.admin.1".to_string()], "X's command goes to mesh1's mesh-primary: {commands:?}");
    });
}

/// CONTRACT (node-primary-update.md "No-successor cases": "refused by name, `cannot transfer: no
/// secondary of type <node_type> in mesh <mesh>`"; step 2: the writer's pre-check): mesh1 has one rpc node,
/// B1. P's command of its update fails by that text before B1 is commanded; B1 stays the rpc-node primary.
#[test]
fn an_rpc_type_with_no_other_node_fails_node_primary_update_by_its_exact_text() {
    let cap = trace_capture("fabric-handover");
    cap.run(async {
        let f = type_fabric(false).await;
        let b1 = node_at(&f.p, "mesh1.rpc.1").await;
        let op = update_operation(Seat::Cohort(RPC), "b1", 1, "node.stop:mesh1.rpc.1");
        let r = f.p.door.update_node_type_primary(&b1, None, "b1", 1, &op).await;
        assert!(matches!(&r, Err(Reply::Failed { reason, .. }) if reason == "cannot transfer: no secondary of type rpc-node in mesh mesh1"), "{r:?}");
        assert_eq!(f.p.door.topology_view.primaries().cohort("mesh1", RPC), Some(member_primary(&f.b1, 2)), "B1 stays the rpc-node primary");
        let _ = &f.x;
    });
}

/// CONTRACT (docs/architecture/lifecycles/node-primary-update.md:47-60, 86-120):
/// "3. For the `node-admin` type, NA1 finishes the writes it has issued before it calls NA2 ...
/// 4. The primary calls `node.primary.update` on the successor's exact birth ...
/// The call fails by name, and node investigation on the successor's exact birth decides (Q111):
/// reached, the call is retried against it ...
/// The successor is unheard after the update. When node investigation's verdict is anything but
/// reached, the step fails by name, `the successor is unheard after the update: <verdict>` ...
/// The commanded arm. When a node.primary.update the fabric-primary commanded ends, the old primary
/// answers it with the Build carrier's terminal, `Failed { step, reason }` on op `0x21` ...
/// Indeterminate after the update: the election resolves it. An identical retry of the call replays
/// the same result before any revision check; a call with a conflicting payload for the same
/// operation is refused by name."
#[test]
fn node_primary_update_step_faults_recover_or_fail_as_the_page_specifies() {
    use rdm_admin::build_drive::SeatMoveEnd;

    let cap = trace_capture("fabric-handover-step-faults");
    let spans = cap.spans.clone();
    let _file = SpanFile(spans.clone());
    cap.run(async {
        // 1. Cut while finishing issued writes (step 3): while `writes_finish` is held `false`,
        // a cut cancels `update_own_primaries` before any `0x21` call reaches the successor N.
        let f = fabric_n_ready_on_probe(FabricPrimary::X).await;
        f.p.writes_finish.send_replace(false);
        let op = update_operation(Seat::MeshPrimary, "b-fault", 1, "node.stop:mesh1.admin.1");
        tokio::select! {
            res = f.p.door.update_own_primaries(Moving::MeshSeat, "b-fault", 1, &op, None) => {
                panic!("update must wait on issued writes before calling successor, got {res:?}");
            }
            _ = async {
                until("P enters finish_writes", || f.p.finished() == 1).await;
            } => {}
        }
        assert!(
            spans.named(UPDATED).is_empty(),
            "cut at finish_writes must not call or install on successor N"
        );

        // 2. Release `writes_finish`, and inject a transient `NotReady` fault on N's first `0x21`
        // call (`fabric_n_ready_on_probe`): P finishes writes, calls N (which fails `NotReady`),
        // runs node investigation on N (`verdict = reached`), retries `PrimaryUpdate`, and succeeds.
        f.p.writes_finish.send_replace(true);
        let p_at_x = node_at(&f.x, "mesh1.admin.1").await;
        let revisions = f
            .x
            .door
            .command_update_primary(&p_at_x, "b-fault", 1, &op)
            .await
            .expect("commanded update recovers via node investigation retry when N is reached");
        assert_eq!(
            revisions,
            vec![entry("node-admin", &f.p.resolved, &f.n.resolved, 4)]
        );
        let looked = spans.named(INVESTIGATION);
        assert_eq!(
            looked
                .iter()
                .map(|l| (l["verdict"].clone(), l["retry"].clone()))
                .collect::<Vec<_>>(),
            vec![("reached".to_string(), "primary-updated".to_string())],
            "{looked:?}"
        );

        // 3. Lost reply / indeterminate retry after installation: repeating the identical call to N
        // replays `PrimaryUpdated` without re-installing, while a conflicting payload for the same
        // operation is refused `RejectedConflictingPayload`.
        let replay = send(
            &f.p,
            &f.n,
            Request::PrimaryUpdate {
                primaries: vec![entry("node-admin", &f.p.resolved, &f.n.resolved, 4)],
                build_id: "b-fault".into(),
                run: 1,
                operation: op.clone(),
            },
        )
        .await;
        assert_eq!(
            replay,
            Reply::PrimaryUpdated {
                revisions: vec![entry("node-admin", &f.p.resolved, &f.n.resolved, 4)]
            }
        );
        let conflict = send(
            &f.p,
            &f.n,
            Request::PrimaryUpdate {
                primaries: vec![entry("node-admin", &f.p.resolved, &f.n.resolved, 9)],
                build_id: "b-fault".into(),
                run: 1,
                operation: op.clone(),
            },
        )
        .await;
        assert_eq!(
            conflict,
            Reply::RejectedConflictingPayload {
                operation: op.clone()
            }
        );

        // 4. Commanded arm when the successor is killed (`_router.shutdown()`): X commands N (now
        // mesh1's mesh-primary at revision 4, with M as its only Ready successor once P is marked
        // Dead in N's view) after M's router is shut down -> N investigates M, finds M unheard, and
        // answers X with `Failed { step, reason }` on op `0x21`.
        {
            let mut tv = f.n.door.topology.write().await;
            for node in &mut tv.nodes {
                if node.name.to_string() == "mesh1.admin.1" {
                    node.status = NodeStatus::Dead;
                    node.is_primary = false;
                } else if node.name.to_string() == "mesh1.admin.2" {
                    node.is_primary = true;
                }
            }
        }
        let _ = f.m._router.shutdown().await;
        let n_at_x = node_at(&f.x, "mesh1.admin.2").await;
        let op2 = update_operation(Seat::MeshPrimary, "b-fault", 2, "node.stop:mesh1.admin.2");
        let err = f
            .x
            .door
            .command_update_primary(&n_at_x, "b-fault", 2, &op2)
            .await
            .expect_err("commanded update must fail when successor M is killed and unheard");
        assert!(
            matches!(
                &err,
                SeatMoveEnd::Failed { step, reason, .. }
                    if step == "primary-update"
                        && reason.contains("the successor is unheard after the update: mesh1.admin.3")
            ),
            "unexpected error: {err:?}"
        );
        let commands = spans.named(COMMAND);
        assert!(
            commands.iter().any(|c| {
                c["to"] == "mesh1.admin.2"
                    && c["outcome"] == "ended"
                    && c["detail"].contains("the successor is unheard after the update: mesh1.admin.3")
            }),
            "expected ended command span at X naming the unheard successor: {commands:?}"
        );
    });
}

/// CONTRACT:
/// - `docs/architecture/lifecycles/node-primary-update.md:45-46` ("1. The primary is commanded from above, as a step of the Build that stops, deletes, replaces or runs `node.connections.delete { scope: All }` on it.")
/// - `docs/architecture/lifecycles/node-primary-update.md:74-75` ("- **Refused before the row** (step 2, or the writer's own pre-check of the call's revision): refused by name, nothing changed, the Build's step fails by name.")
/// - `docs/architecture/lifecycles/node-primary-update.md:111-114` ("- **The old primary lost after the update:** the successor and every reader take the primary from the election; the successor's digest carries its role, and the mesh-primary (the successor itself for the `node-admin` type) records it. Completion needs neither gossip delivery nor the old primary's survival.")
/// - `docs/architecture/lifecycles/node-primary-update.md:157-158` ("1. the fabric-primary's own mesh: the lowest Ready node-admin NodeId there, where the fabric-primary stays by preference;")
/// - `docs/architecture/lifecycles/node-primary-update.md:223-226` ("Trace propagation, step spans and correlation follow the [runtime spans](../node-rpc-envelope.md#runtime-spans). The update's spans carry the operation, the node type, both exact births and the revisions: the successor's install, the record's write, and every refusal by its typed reason.")
#[test]
fn primary_update_prefers_own_mesh_lowest_ready_admin_survives_old_primary_loss_and_records_observability_spans() {
    use rdm_admin::build_drive::SeatMoveEnd;

    let cap = trace_capture("fabric-handover-conformance");
    let spans = cap.spans.clone();
    let _file = SpanFile(spans.clone());
    cap.run(async {
        // 1. Refused before the row (step 2): when no Ready successor exists, the update is refused
        // by name, nothing changes, the Build's step fails by name (`SeatMoveEnd::Failed`), and the
        // refusal span carries `operation` and the typed `reason` (`no-successor`).
        let (lone_p, _lone_x) = lone_fabric(NodeStatus::Pending).await;
        let refused_op = update_operation(
            Seat::MeshPrimary,
            "b-conformance",
            1,
            "node.connections.delete:mesh1.admin.1",
        );
        let refused = lone_p
            .door
            .update_own_primaries(Moving::MeshSeat, "b-conformance", 1, &refused_op, None)
            .await;
        assert!(
            matches!(
                &refused,
                Err(SeatMoveEnd::Failed { step, reason, .. })
                    if step == "primary-update"
                        && reason == "cannot transfer: no secondary of type node-admin in mesh mesh1"
            ),
            "refused before the row must fail the step by name: {refused:?}"
        );
        assert_eq!(
            lone_p.door.topology_view.primaries().mesh("mesh1"),
            Some(primary_of(&lone_p, 3)),
            "nothing changed when refused before the row"
        );
        let rejected = spans.named(REJECTED);
        assert!(
            rejected
                .iter()
                .any(|s| s["operation"] == refused_op && s["reason"] == "no-successor"),
            "refusal span carries operation and typed reason: {rejected:?}"
        );

        // 2. Fabric-primary search step 1 & commanded step: in `primaries_fabric(FabricPrimary::P)`,
        // X (`mesh2.admin.1`, NodeId `000000000001`) has a lower NodeId than N (`mesh1.admin.2`,
        // NodeId `000000000003`) and M (`mesh1.admin.3`, NodeId `000000000009`), but the
        // fabric-primary stays in its own mesh (`mesh1`) by preference and chooses N, the lowest
        // Ready node-admin NodeId there.
        let f = primaries_fabric(FabricPrimary::P).await;
        let op = update_operation(
            Seat::MeshPrimary,
            "b-conformance",
            2,
            "node.delete:mesh1.admin.1",
        );
        let revisions = f
            .p
            .door
            .update_own_primaries(Moving::BothSeats, "b-conformance", 2, &op, None)
            .await
            .expect("fabric-primary and mesh-primary move to lowest Ready node-admin in own mesh");
        assert_eq!(
            revisions,
            vec![
                entry("node-admin", &f.p.resolved, &f.n.resolved, 4),
                entry("fabric", &f.p.resolved, &f.n.resolved, 8),
            ],
            "fabric-primary stays in own mesh (N over X) and selects lowest Ready NodeId (N over M)"
        );

        // 3. Old primary lost after the update: shut down P's router immediately after the update.
        // N and every reader take the primary from the election; N's digest carries its role
        // (`is_primary = Some(4)`, `is_fabric_primary = Some(8)`), and completion needs neither
        // gossip delivery nor P's survival.
        let _ = f.p._router.shutdown().await;
        assert_eq!(
            f.n.door.topology_view.primaries().mesh("mesh1"),
            Some(primary_of(&f.n, 4))
        );
        assert_eq!(
            f.n.door.topology_view.primaries().fabric(),
            Some(primary_of(&f.n, 8))
        );
        let mut digest = digest_of(&f.n.resolved, &FabricId::mint());
        f.n.door.digest_view.recorded_primaries(&mut digest);
        assert_eq!(
            (digest.is_primary, digest.is_fabric_primary),
            (Some(4), Some(8)),
            "successor's digest carries its role at the next revisions without the old primary's survival"
        );

        // 4. Observability contract: the successor's install span (`via-primary-updated`) carries
        // `operation`, the node type (`node-admin`, `fabric`), both exact births (`P` and `N`), and
        // the revisions (`4`, `8`).
        let updated = spans.named(UPDATED);
        let p_birth = format!("{}/{}", f.p.resolved.node_id, f.p.resolved.incarnation);
        let n_birth = format!("{}/{}", f.n.resolved.node_id, f.n.resolved.incarnation);
        assert!(
            updated.iter().any(|s| {
                s["operation"] == op
                    && s["primaries"].contains("node-admin")
                    && s["primaries"].contains("fabric")
                    && s["primaries"].contains(&p_birth)
                    && s["primaries"].contains(&n_birth)
                    && s["primaries"].contains("@4")
                    && s["primaries"].contains("@8")
            }),
            "via-primary-updated span carries operation, node type, both exact births, and revisions: {updated:?}"
        );
        let _ = (&f.m, &f.x);
    });
}

