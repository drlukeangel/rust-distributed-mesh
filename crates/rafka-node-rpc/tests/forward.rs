//! i143.e6.s4: generic one-hop carried execution — the carrier makes exactly one direct inner
//! call, a non-forwardable family is refused by type, and certainty composes across the hop.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{CarrierEdges, ServedBirth, CallOptions, HandlerFault, NodeRpcClient, NodeTarget, ResolvedNode, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::{LedgerEntry, OpOwner, OpState};
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::testing::trace::InMemorySpanExporter;
use opentelemetry_sdk::trace::{SimpleSpanProcessor, TracerProvider};
use rafka_node_rpc_contract::forward::{Forward, ForwardReply, ForwardRequest, FORWARD_REPLY_RESERVE};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use rafka_node_rpc_contract::outcome::{IndeterminateReason, MalformedKind, NotSentReason, ReplyKind, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// A forwardable test family on a ledgered test op.
struct Probe;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum ProbeRequest {
    Probe { payload: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum ProbeReply {
    /// The payload, and the transport id the target saw as its caller.
    Probed { payload: Vec<u8>, caller: String },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

impl NodeProtocol for Probe {
    const OP: u8 = 0x5E;
    const NAME: &'static str = "probe";
    const MAX_REQUEST_FRAME_BYTES: usize = 4096;
    const MAX_REPLY_FRAME_BYTES: usize = 4096;
    const FORWARDABLE: bool = true;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 7;
    type Request = ProbeRequest;
    type Reply = ProbeReply;
    fn classify_reply(r: &ProbeReply) -> ReplyKind {
        match r {
            ProbeReply::Probed { .. } => ReplyKind::Success,
            ProbeReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            ProbeReply::NotReady { .. } => ReplyKind::NotReady,
            ProbeReply::Busy { .. } => ReplyKind::Busy,
            ProbeReply::Draining { .. } => ReplyKind::Draining,
            ProbeReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            ProbeReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }
    fn peer_unresolved(reason: String) -> ProbeReply {
        ProbeReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> ProbeReply {
        ProbeReply::NotReady { reason }
    }
    fn busy(reason: String) -> ProbeReply {
        ProbeReply::Busy { reason }
    }
    fn draining(reason: String) -> ProbeReply {
        ProbeReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> ProbeReply {
        ProbeReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> ProbeReply {
        ProbeReply::Unauthorized { reason }
    }
}

/// The builder starts from the core ledger; the test family adds its own row.
fn test_ledger() -> Vec<LedgerEntry> {
    vec![LedgerEntry { op: Probe::OP, family: "probe".into(), owner: OpOwner::Product("test".into()), state: OpState::Live }]
}

struct Node {
    router: Router,
    key: SecretKey,
    resolved: ResolvedNode,
}

/// A birth's identity, minted before its server seals.
fn birth() -> (NodeId, IncarnationId) {
    (NodeId::mint(), IncarnationId::mint())
}

fn served(b: &(NodeId, IncarnationId)) -> ServedBirth {
    ServedBirth { node_id: b.0.to_string(), incarnation: b.1 .0.clone() }
}

async fn start(server: rafka_node_rpc::NodeRpcServer, key: SecretKey, name: &str, b: (NodeId, IncarnationId)) -> Node {
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolved = ResolvedNode { node_id: b.0, name: name.parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation: b.1 };
    Node { router, key, resolved }
}

async fn client_with(nodes: &[&ResolvedNode]) -> NodeRpcClient {
    let resolver = Arc::new(StaticResolver::new());
    for n in nodes {
        resolver.insert((*n).clone());
    }
    let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    NodeRpcClient::new(ep, resolver)
}

struct Rig {
    origin: NodeRpcClient,
    carrier: Node,
    target: Node,
    handled: Arc<AtomicU64>,
    applied: Arc<tokio::sync::Notify>,
    carrier_server: rafka_node_rpc::NodeRpcServer,
}

/// target serves Probe and Ping; the carrier serves forward and carries Probe (and Ping, which is
/// not forwardable and so is never carried); the origin knows only the carrier and the target.
async fn rig() -> Rig {
    rig_with(None, false).await
}

/// `edges`: the carrier's account of its Direct edges. `carrier_misdials`: the carrier's own view
/// of the target names another transport identity, so its dial fails at the handshake.
async fn rig_with(edges: Option<Arc<dyn CarrierEdges>>, carrier_misdials: bool) -> Rig {
    let handled = Arc::new(AtomicU64::new(0));
    let applied = Arc::new(tokio::sync::Notify::new());
    let (h, a) = (handled.clone(), applied.clone());
    let target_birth = birth();
    let target_server = ServerBuilder::new()
        .ledger(test_ledger())
        .serve::<Probe, _, _>(OpOwner::Product("test".into()), move |peer, req: ProbeRequest| {
            let (h, a) = (h.clone(), a.clone());
            async move {
                h.fetch_add(1, Ordering::SeqCst);
                a.notify_one();
                let ProbeRequest::Probe { payload } = req;
                if payload == b"hold" {
                    std::future::pending::<()>().await;
                }
                Ok::<_, HandlerFault>(ProbeReply::Probed { payload, caller: peer.endpoint_id.to_string() })
            }
        })
        .serve::<Ping, _, _>(OpOwner::Core, |_p, req: PingRequest| async move {
            let PingRequest::Ping { payload, .. } = req;
            Ok(PingReply::Pong { payload })
        })
        .seal(served(&target_birth))
        .unwrap();
    let target = start(target_server, SecretKey::generate(), "mesh1.rpc.3", target_birth).await;

    // The carrier calls out on the endpoint it serves on, as a node does: the target sees the
    // carrier's own transport identity.
    let carrier_key = SecretKey::generate();
    let carrier_ep = rafka_node_rpc::endpoint::bind(carrier_key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let carrier_addr = carrier_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let carrier_resolver = Arc::new(StaticResolver::new());
    let mut seen = target.resolved.clone();
    if carrier_misdials {
        seen.endpoint_id = SecretKey::generate().public();
    }
    carrier_resolver.insert(seen);
    let carrier_client = Arc::new(NodeRpcClient::new(carrier_ep.clone(), carrier_resolver));
    let carrier_birth = birth();
    let carrier_builder = ServerBuilder::new().ledger(test_ledger()).carry::<Probe>().carry::<Ping>();
    let carrier_server = match edges {
        Some(e) => carrier_builder.serve_forward_with_edges(carrier_client, e),
        None => carrier_builder.serve_forward(carrier_client),
    }
    .seal(served(&carrier_birth))
    .unwrap();
    let carrier = Node {
        router: Router::builder(carrier_ep).accept(rafka_node_rpc::ALPN, carrier_server.clone()).spawn(),
        key: carrier_key.clone(),
        resolved: ResolvedNode {
            node_id: carrier_birth.0,
            name: "mesh1.rpc.2".parse().unwrap(),
            endpoint_id: carrier_key.public(),
            transport_addr: carrier_addr,
            incarnation: carrier_birth.1,
        },
    };
    let origin = client_with(&[&carrier.resolved, &target.resolved]).await;
    Rig { origin, carrier, target, handled, applied, carrier_server }
}

fn probe(p: &[u8]) -> ProbeRequest {
    ProbeRequest::Probe { payload: p.to_vec() }
}

fn carrier_of(r: &Rig) -> NodeTarget {
    NodeTarget::ExactNode(r.carrier.resolved.node_id.clone())
}

#[tokio::test]
async fn a_carried_call_reaches_the_target_once_and_the_target_sees_the_carrier() {
    let r = rig().await;
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &CallOptions::default()).await;
    let reply = out.reply().unwrap_or_else(|| panic!("a reply through the carrier: {out:?}")).value().clone();
    assert_eq!(reply, ProbeReply::Probed { payload: b"ping".to_vec(), caller: r.carrier.key.public().to_string() });
    assert_eq!(r.handled.load(Ordering::SeqCst), 1, "exactly one inner call");
    let _ = &r.target.router;
}

#[tokio::test]
async fn a_non_forwardable_family_is_refused_by_type_at_the_origin_and_at_the_carrier() {
    let r = rig().await;
    let echo = PingRequest::Ping { payload: b"x".to_vec() };
    let (out, _) = r.origin.call_via::<Ping>(&carrier_of(&r), &r.target.resolved.node_id, &echo, &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::NotForwardable { op: Ping::OP }), "{out:?}");

    // A forward built by hand for a non-forwardable op: the carrier refuses it by type.
    let forward = ForwardRequest::Forward {
        target: r.target.resolved.node_id.clone(),
        inner_op: Ping::OP,
        inner: Ping::encode_request(&echo).unwrap(),
        remaining_ms: 5_000,
    };
    let (out, _) = r.origin.call::<Forward>(&carrier_of(&r), &forward, &CallOptions::default()).await;
    assert_eq!(out.reply().map(|x| x.value().clone()), Some(ForwardReply::NotForwardable { op: Ping::OP }));
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_carrier_that_is_gone_before_commit_is_not_sent() {
    let r = rig().await;
    r.carrier.router.shutdown().await.unwrap();
    let opts = CallOptions { budget: rafka_node_rpc::Budget::Overall(Duration::from_secs(2)), ..CallOptions::default() };
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &opts).await;
    assert!(out.proves_not_dispatched(), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_inner_call_applied_whose_outer_reply_is_lost_is_indeterminate() {
    let r = rig().await;
    let target = r.target.resolved.node_id.clone();
    let carrier = carrier_of(&r);
    let call = async { r.origin.call_via::<Probe>(&carrier, &target, &probe(b"hold"), &CallOptions::default()).await };
    let kill = async {
        r.applied.notified().await;
        r.carrier.router.shutdown().await.unwrap();
    };
    let ((out, _), ()) = tokio::join!(call, kill);
    assert!(matches!(&out, RpcOutcome::Indeterminate(_)), "the inner call applied, the outer reply was lost: {out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_target_the_carrier_cannot_resolve_is_not_sent() {
    let r = rig().await;
    let stranger = NodeId::mint();
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &stranger, &probe(b"ping"), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::Carried(_))), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_inner_reply_lost_at_the_target_is_indeterminate_through_the_carrier() {
    let r = rig().await;
    let target = r.target.resolved.node_id.clone();
    let carrier = carrier_of(&r);
    let call = async { r.origin.call_via::<Probe>(&carrier, &target, &probe(b"hold"), &CallOptions::default()).await };
    let kill = async {
        r.applied.notified().await;
        r.target.router.shutdown().await.unwrap();
    };
    let ((out, _), ()) = tokio::join!(call, kill);
    assert!(
        matches!(&out, RpcOutcome::Indeterminate(i) if matches!(i.reason(), IndeterminateReason::Carried(_))),
        "the carrier could not learn the inner outcome: {out:?}"
    );
}

/// A carrier's own account of its Direct edge to the final target.
struct EdgeFact(Option<String>);

#[async_trait::async_trait]
impl CarrierEdges for EdgeFact {
    async fn edge_not_active(&self, _target: &NodeId) -> Option<String> {
        self.0.clone()
    }
}

/// A carrier whose inner dial to the final target fails while its own latest Direct fact toward
/// it is not Active answers the typed `CarrierEdgeLost`, naming the fact; the origin sees
/// `NotSent(CarrierEdgeLost)` and the target never handled the call.
///
/// CONTRACT: the third Proxy validity condition (connections.md section 8) is learned from the
/// carried call: no rewording of an inner NotSent stands in for it.
#[tokio::test]
async fn a_carrier_whose_own_edge_to_the_target_is_not_active_refuses_carrier_edge_lost() {
    let r = rig_with(Some(Arc::new(EdgeFact(Some("Direct Failed (dial failed)".into())))), true).await;
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::CarrierEdgeLost("Direct Failed (dial failed)".into())), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

/// The same dial failure with the carrier holding no non-Active edge fact stays the carrier's
/// plain `InnerNotSent`: the refusal is the edge fact, not the failure.
///
/// CONTRACT: a carrier that cannot name its own non-Active edge never claims `carrier-edge-lost`.
#[tokio::test]
async fn a_carrier_with_no_non_active_edge_fact_keeps_the_plain_inner_not_sent() {
    let r = rig_with(Some(Arc::new(EdgeFact(None))), true).await;
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::Carried(_))), "{out:?}");
}

/// The source of a Proxy `origin -> carrier -> target` and the held projection that records it.
fn proxied_held(r: &Rig) -> (rafka_mesh_entity::connections::ConnectionsHeld, rafka_mesh_entity::PathName, rafka_mesh_entity::connections::NodeConnection) {
    use rafka_mesh_entity::connections::{ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, NodeConnection};
    let own: rafka_mesh_entity::PathName = "mesh1.rpc.1".parse().unwrap();
    let end = |n: &ResolvedNode| ConnectionEnd { name: n.name.clone(), node_id: n.node_id.clone(), incarnation: Some(n.incarnation.clone()) };
    let proxy = NodeConnection {
        source: ConnectionEnd { name: own.clone(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) },
        destination: end(&r.target.resolved),
        kind: ConnectionKind::Proxy,
        state: ConnectionState::Connected,
        carrier: Some(end(&r.carrier.resolved)),
        recovery: None,
        reason: None,
        logged_at_ms: 10,
    };
    let mut held = ConnectionsHeld::new();
    held.set_own_source(own.clone());
    held.mark_complete();
    held.apply(proxy.clone()).unwrap();
    (held, own, proxy)
}

/// A call over a Proxy whose carrier answers `CarrierEdgeLost` hands the Proxy back for
/// retirement with the structural reason `carrier-edge-lost`, and the seam writes nothing; a
/// plain `InnerNotSent` retires nothing.
///
/// CONTRACT: the source learns the third Proxy validity condition from the carried call itself
/// (connections.md section 8) and from nothing else.
#[tokio::test]
async fn a_carried_call_answering_carrier_edge_lost_hands_the_proxy_back_for_retirement() {
    use rafka_mesh_entity::connections::{resolve, CarrierPolicy, INVALID_CARRIER_EDGE_LOST};
    let policy = CarrierPolicy::Forwardable { carrier_kind: rafka_mesh_entity::NodeKind::RpcNode };
    for (edge, retired) in [(Some("Direct Failed".to_string()), true), (None, false)] {
        let r = rig_with(Some(Arc::new(EdgeFact(edge))), true).await;
        let (held, own, proxy) = proxied_held(&r);
        let resolution = resolve(&held, &own, &r.target.resolved.name, policy);
        let call = r.origin.call_resolved::<Probe>(resolution, &own, &r.target.resolved.name, &r.target.resolved.node_id, &probe(b"ping"), &CallOptions::default()).await;
        assert!(matches!(call.outcome, RpcOutcome::NotSent(_)), "{:?}", call.outcome);
        assert_eq!(call.retire, retired.then_some((proxy, INVALID_CARRIER_EDGE_LOST)));
    }
}

/// The origin's whole budget equals the carrier's default call budget (10 s) and the final target
/// is unreachable: the carrier's inner dial runs to its own deadline.
///
/// CONTRACT: the carrier's inner call is bounded by what the origin has left, so the carrier's
/// `CarrierEdgeLost` arrives inside the origin's budget and the origin records the named
/// `NotSent(CarrierEdgeLost)`, never `Indeterminate(ReplyDeadline)`. Wall: the 10 s is the
/// carrier's inner dial running to its bound (rdm.node_rpc.request.update.via-call, outcome and
/// reason); it is the cell's subject, not setup.
#[tokio::test]
async fn an_unreachable_target_under_the_origins_default_budget_is_carrier_edge_lost_never_indeterminate() {
    let r = rig_with(Some(Arc::new(EdgeFact(Some("Direct Failed (dial failed)".into())))), false).await;
    r.target.router.shutdown().await.unwrap();
    let opts = CallOptions { budget: rafka_node_rpc::Budget::Overall(Duration::from_secs(10)), ..CallOptions::default() };
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &opts).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::CarrierEdgeLost("Direct Failed (dial failed)".into())), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

fn spans() -> InMemorySpanExporter {
    static EXPORTER: OnceLock<InMemorySpanExporter> = OnceLock::new();
    EXPORTER
        .get_or_init(|| {
            let exporter = InMemorySpanExporter::default();
            let provider = TracerProvider::builder().with_span_processor(SimpleSpanProcessor::new(Box::new(exporter.clone()))).build();
            let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "forward-test");
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer)).init();
            exporter
        })
        .clone()
}

/// The names of the spans a cell's own trace finished.
fn trace_span_names(trace: &str) -> Vec<String> {
    spans().get_finished_spans().unwrap().into_iter().filter(|s| s.span_context.trace_id().to_string() == trace).map(|s| s.name.to_string()).collect()
}

fn overall(d: Duration) -> CallOptions {
    CallOptions { budget: rafka_node_rpc::Budget::Overall(d), ..CallOptions::default() }
}

/// A caller budget far under the carrier's default still gets the named reason.
///
/// CONTRACT: the carrier's inner call is bounded by the origin's remaining budget less the reply
/// reserve, so an unreachable target under a 500 ms budget answers `CarrierEdgeLost` inside it.
#[tokio::test]
async fn an_unreachable_target_under_a_500ms_budget_is_carrier_edge_lost_inside_it() {
    let r = rig_with(Some(Arc::new(EdgeFact(Some("Direct Failed (dial failed)".into())))), false).await;
    r.target.router.shutdown().await.unwrap();
    let started = std::time::Instant::now();
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &overall(Duration::from_millis(500))).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::CarrierEdgeLost(_))), "{out:?}");
    assert!(started.elapsed() < Duration::from_millis(500), "the named answer arrived inside the budget: {:?}", started.elapsed());
}

/// A budget already spent down to the carrier's reserve when the frame is written.
///
/// CONTRACT: the carrier makes no inner call and refuses by name before dispatch: the origin
/// sees `NotSent(CarrierNoBudget)`, the target handled nothing, and the trace holds the
/// carrier's `reject.via-forward-budget-spent` span and no `serve.via-carried-inner` span.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_budget_spent_to_the_reply_reserve_is_refused_by_name_with_no_inner_call() {
    spans();
    let r = rig().await;
    let (target, carrier) = (r.target.resolved.node_id.clone(), carrier_of(&r));
    // Warm the carrier connection so the budgeted call's dial is instant.
    let (warm, _) = r.origin.call_via::<Probe>(&carrier, &target, &probe(b"warm"), &CallOptions::default()).await;
    assert!(warm.reply().is_some(), "{warm:?}");
    let handled_before = r.handled.load(Ordering::SeqCst);
    let caller = tracing::info_span!("test.caller");
    let trace = caller.context().span().span_context().trace_id().to_string();
    let (out, _) = tracing::Instrument::instrument(r.origin.call_via::<Probe>(&carrier, &target, &probe(b"late"), &overall(FORWARD_REPLY_RESERVE)), caller).await;
    assert!(
        matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::CarrierNoBudget { reserve_ms, .. } if *reserve_ms == FORWARD_REPLY_RESERVE.as_millis() as u64)),
        "{out:?}"
    );
    assert_eq!(r.handled.load(Ordering::SeqCst), handled_before, "no inner call reached the target");
    let names = trace_span_names(&trace);
    assert!(names.iter().any(|n| n == "rdm.node_rpc.request.reject.via-forward-budget-spent"), "the refusal is spanned in the origin's trace: {names:?}");
    assert!(!names.iter().any(|n| n == "rdm.node_rpc.request.serve.via-carried-inner"), "no inner call span: {names:?}");
}

/// A carrier that records the budget each forward carries and answers without an inner call.
async fn spy_carrier() -> (NodeRpcClient, NodeTarget, Arc<std::sync::Mutex<Vec<u64>>>, Node) {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let s = seen.clone();
    let birth = birth();
    let server = ServerBuilder::new()
        .ledger(test_ledger())
        .serve::<Forward, _, _>(OpOwner::Core, move |_peer, req: ForwardRequest| {
            let s = s.clone();
            async move {
                let ForwardRequest::Forward { remaining_ms, .. } = req;
                s.lock().unwrap().push(remaining_ms);
                Ok::<_, HandlerFault>(ForwardReply::InnerNotSent { reason: "spy".into() })
            }
        })
        .seal(served(&birth))
        .unwrap();
    let node = start(server, SecretKey::generate(), "mesh1.rpc.2", birth).await;
    let origin = client_with(&[&node.resolved]).await;
    let target = NodeTarget::ExactNode(node.resolved.node_id.clone());
    (origin, target, seen, node)
}

/// CONTRACT: the frame carries what the origin has left, never the default: an overall budget
/// forwards deadline minus now, a split budget forwards its reply budget.
#[tokio::test]
async fn the_forward_frame_carries_the_budget_that_remains_never_the_default() {
    let (origin, carrier, seen, _node) = spy_carrier().await;
    let stranger = NodeId::mint();
    let split = CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(10), reply: Duration::from_millis(700) }, ..CallOptions::default() };
    for opts in [overall(Duration::from_millis(2000)), split] {
        let _ = origin.call_via::<Probe>(&carrier, &stranger, &probe(b"x"), &opts).await;
    }
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert!((1_800..=2_000).contains(&seen[0]), "overall: deadline minus now: {seen:?}");
    assert_eq!(seen[1], 700, "split: the reply budget, not the 10 s send bound or the default: {seen:?}");
}

/// CONTRACT: a bounded carried call forwards the inner bound plus the reply reserve, never more
/// than the origin has left: the carrier's inner call ends before the origin's deadline by the
/// margin the origin's budget holds beyond the bound.
#[tokio::test]
async fn a_bounded_carried_call_forwards_its_inner_bound_plus_the_reserve_under_the_origins_deadline() {
    let (origin, carrier, seen, _node) = spy_carrier().await;
    let stranger = NodeId::mint();
    let _ = origin.call_via_bounded::<Probe>(&carrier, &stranger, &probe(b"x"), &overall(Duration::from_millis(5000)), Some(Duration::from_millis(2000))).await;
    let _ = origin.call_via_bounded::<Probe>(&carrier, &stranger, &probe(b"y"), &overall(Duration::from_millis(1000)), Some(Duration::from_millis(2000))).await;
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "{seen:?}");
    let reserve = FORWARD_REPLY_RESERVE.as_millis() as u64;
    assert!((2_000..=2_000 + reserve).contains(&seen[0]), "the bound plus the reserve, not the 5 s the origin has: {seen:?}");
    assert!(seen[1] <= 1_000, "never more than the origin has left: {seen:?}");
}

/// CONTRACT: a forward naming Forward is refused `NotForwardable` and a draining carrier refuses
/// a forward `Draining`, each before any inner call.
#[tokio::test]
async fn a_nested_forward_and_a_draining_carrier_are_still_refused_by_name() {
    let r = rig().await;
    let nested = ForwardRequest::Forward { target: r.target.resolved.node_id.clone(), inner_op: Forward::OP, inner: Vec::new(), remaining_ms: 5_000 };
    let (out, _) = r.origin.call::<Forward>(&carrier_of(&r), &nested, &CallOptions::default()).await;
    assert_eq!(out.reply().map(|x| x.value().clone()), Some(ForwardReply::NotForwardable { op: Forward::OP }));
    r.carrier_server.drain();
    let (out, _) = r.origin.call_via::<Probe>(&carrier_of(&r), &r.target.resolved.node_id, &probe(b"ping"), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::Carried(why) if why.contains("draining"))), "{out:?}");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}
