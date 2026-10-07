//! i143.e6.s6 functional: the routing composition seam executes exactly the target a domain
//! selected, over exactly the route connections chose.
//!
//! A tiny exact-target selector stands in for a domain (never `orgs.topics`): it chooses B or
//! C by name. Connections' answer is handed to the seam as a [`RouteChoice`]. The cells prove
//! the seam never substitutes a target, never starts a leg on `NoActiveRoute`, never lets
//! reachability change the selection, never lets a selection change fabricate connection
//! state, and hands certainty outcomes back without replaying them anywhere else.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::connections::{
    resolve, CarrierPolicy, ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, DirectRecovery, EffectiveRoute, NodeConnection,
};
use rafka_mesh_entity::{IncarnationId, NodeId, NodeKind, PathName};
use rafka_node_rpc::{CallOptions, HandlerFault, NodeRpcClient, NodeTarget, PeerContext, ResolvedNode, RouteChoice, RouteLeg, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::{LedgerEntry, TagOwner, TagState};
use rafka_node_rpc_contract::outcome::{MalformedKind, NotSentReason, ReplyKind, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A forwardable test family: the reply names the node that served it.
struct Probe;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum ProbeRequest {
    Probe { payload: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum ProbeReply {
    Probed { payload: Vec<u8>, served_by: String, caller: String },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

impl NodeProtocol for Probe {
    const TAG: u8 = 0x5E;
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

fn test_ledger() -> Vec<LedgerEntry> {
    vec![LedgerEntry { tag: Probe::TAG, family: "probe".into(), owner: TagOwner::Product("test".into()), state: TagState::Live }]
}

struct Node {
    _router: Router,
    key: SecretKey,
    resolved: ResolvedNode,
    handled: Arc<AtomicU64>,
}

/// A node serving Probe (answering with its own name) and, with `client`, carrying Probe for others.
async fn node(name: &str, carrier_client: Option<Arc<NodeRpcClient>>, key: SecretKey, ep: iroh::Endpoint) -> Node {
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let handled = Arc::new(AtomicU64::new(0));
    let (h, me) = (handled.clone(), name.to_string());
    let mut b = ServerBuilder::new().ledger(test_ledger()).serve::<Probe, _, _>(TagOwner::Product("test".into()), move |peer: PeerContext, req: ProbeRequest| {
        let (h, me) = (h.clone(), me.clone());
        async move {
            h.fetch_add(1, Ordering::SeqCst);
            let ProbeRequest::Probe { payload } = req;
            if payload == b"hold" {
                std::future::pending::<()>().await;
            }
            Ok::<_, HandlerFault>(ProbeReply::Probed { payload, served_by: me, caller: peer.endpoint_id.to_string() })
        }
    });
    if let Some(c) = carrier_client {
        b = b.carry::<Probe>().serve_forward(c);
    }
    let server = b.seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() }).unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolved = ResolvedNode { node_id, name: name.parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation };
    Node { _router: router, key, resolved, handled }
}

async fn bind() -> (SecretKey, iroh::Endpoint) {
    let key = SecretKey::generate();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    (key, ep)
}

struct Rig {
    origin: NodeRpcClient,
    b: Node,
    c: Node,
    p: Node,
}

/// B and C are two semantically eligible targets; P is a carrier that can reach both.
async fn rig() -> Rig {
    let (bk, be) = bind().await;
    let b = node("mesh1.rpc.1", None, bk, be).await;
    let (ck, ce) = bind().await;
    let c = node("mesh1.rpc.2", None, ck, ce).await;
    let (pk, pe) = bind().await;
    let p_resolver = Arc::new(StaticResolver::new());
    p_resolver.insert(b.resolved.clone());
    p_resolver.insert(c.resolved.clone());
    let p_client = Arc::new(NodeRpcClient::new(pe.clone(), p_resolver));
    let p = node("mesh1.rpc.3", Some(p_client), pk, pe).await;
    let resolver = Arc::new(StaticResolver::new());
    for n in [&b, &c, &p] {
        resolver.insert(n.resolved.clone());
    }
    let (_, oe) = bind().await;
    Rig { origin: NodeRpcClient::new(oe, resolver), b, c, p }
}

/// The domain: a selector that chooses by name and nothing else.
fn select<'a>(rig: &'a Rig, choice: &str) -> &'a Node {
    match choice {
        "B" => &rig.b,
        "C" => &rig.c,
        _ => unreachable!(),
    }
}

fn probe(p: &[u8]) -> ProbeRequest {
    ProbeRequest::Probe { payload: p.to_vec() }
}

fn served_by(out: &RpcOutcome<ProbeReply>) -> Option<(String, String)> {
    match out {
        RpcOutcome::Reply(r) => match r.value() {
            ProbeReply::Probed { served_by, caller, .. } => Some((served_by.clone(), caller.clone())),
            _ => None,
        },
        _ => None,
    }
}

async fn routed(rig: &Rig, target: &Node, route: RouteChoice, payload: &[u8]) -> (RpcOutcome<ProbeReply>, Option<rafka_node_rpc::CallEvidence>, RouteLeg) {
    rig.origin.call_routed::<Probe>(&target.resolved.node_id, &route, &probe(payload), &CallOptions { budget: rafka_node_rpc::Budget::Overall(Duration::from_millis(1500)), ..Default::default() }).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_exact_target_is_preserved_over_direct() {
    let r = rig().await;
    let target = select(&r, "B");
    let (out, ev, leg) = routed(&r, target, RouteChoice::Direct, b"direct").await;
    assert_eq!(served_by(&out), Some(("mesh1.rpc.1".into(), r.origin.endpoint().id().to_string())), "B executed, called by the origin itself: {out:?}");
    assert_eq!(leg, RouteLeg::Direct);
    assert_eq!(ev.unwrap().node_id, target.resolved.node_id, "the leg's evidence names the selected target");
    assert_eq!((r.b.handled.load(Ordering::SeqCst), r.c.handled.load(Ordering::SeqCst), r.p.handled.load(Ordering::SeqCst)), (1, 0, 0), "no other node was attempted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_exact_target_is_preserved_over_via_peer_and_the_carrier_cannot_substitute_or_recurse() {
    let r = rig().await;
    let target = select(&r, "B");
    let route = RouteChoice::ViaPeer { carrier: r.p.resolved.node_id.clone(), path: r.p.resolved.name.clone() };
    let (out, ev, leg) = routed(&r, target, route, b"carried").await;
    assert_eq!(served_by(&out), Some(("mesh1.rpc.1".into(), r.p.key.public().to_string())), "B executed exactly once, called by P: {out:?}");
    assert_eq!(leg, RouteLeg::ViaPeer { carrier: "mesh1.rpc.3".into() });
    assert_eq!(ev.unwrap().node_id, r.p.resolved.node_id, "the one leg the origin ran went to the carrier");
    assert_eq!((r.b.handled.load(Ordering::SeqCst), r.c.handled.load(Ordering::SeqCst)), (1, 0), "P carried to B, never to C");
    // The carrier is handed the exact final target; a Forward tag is itself never forwardable, so
    // P cannot be asked to carry a carry (recursion is refused by type at the origin).
    let (out, _) = r.origin.call_via::<rafka_node_rpc_contract::forward::Forward>(&NodeTarget::ExactNode(r.p.resolved.node_id.clone()), &target.resolved.node_id, &rafka_node_rpc_contract::forward::ForwardRequest::Forward { target: target.resolved.node_id.clone(), inner_tag: Probe::TAG, inner: vec![] }, &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::NotForwardable { .. })), "{out:?}");
    // The carrier is exact: a route naming a carrier process the resolver no longer knows (its path
    // taken by another birth) is not sent to the path's holder.
    let replaced = RouteChoice::ViaPeer { carrier: NodeId::mint(), path: r.p.resolved.name.clone() };
    let (out, ev, _) = routed(&r, target, replaced, b"stale-carrier").await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::Resolve(_))), "{out:?}");
    assert!(ev.is_none());
    assert_eq!(r.b.handled.load(Ordering::SeqCst), 1, "nothing reached B through a substituted carrier");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_route_means_no_send() {
    let r = rig().await;
    let target = select(&r, "B");
    let (out, ev, leg) = routed(&r, target, RouteChoice::NoActiveRoute, b"never").await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::NoActiveRoute), "the named outcome: {out:?}");
    assert!(out.proves_not_dispatched());
    assert_eq!(leg, RouteLeg::None);
    assert!(ev.is_none(), "no leg: nothing resolved, dialled or sent");
    assert!(r.origin.pooled().is_empty(), "no connection was opened");
    assert_eq!((r.b.handled.load(Ordering::SeqCst), r.c.handled.load(Ordering::SeqCst), r.p.handled.load(Ordering::SeqCst)), (0, 0, 0));
}

/// Connections fixtures, as the held projection records them.
fn end(n: &Node) -> ConnectionEnd {
    ConnectionEnd { name: n.resolved.name.clone(), node_id: n.resolved.node_id.clone(), incarnation: Some(n.resolved.incarnation.clone()) }
}

fn own_end(own: &PathName) -> ConnectionEnd {
    ConnectionEnd { name: own.clone(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) }
}

/// A connections fact at stamp `at`, in the shape the writer records it.
fn edge(from: ConnectionEnd, to: ConnectionEnd, kind: ConnectionKind, state: ConnectionState, carrier: Option<ConnectionEnd>, at: u64) -> NodeConnection {
    let recovery = (kind == ConnectionKind::Direct && state == ConnectionState::Failed).then_some(DirectRecovery { recovery_epoch: 1, attempt_ordinal: 1 });
    NodeConnection { source: from, destination: to, kind, state, carrier, recovery, reason: None, logged_at_ms: at }
}

fn held_for(own: &PathName) -> ConnectionsHeld {
    let mut held = ConnectionsHeld::new();
    held.set_own_source(own.clone());
    held.mark_complete();
    held
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn semantic_selection_is_not_connection_selection() {
    let r = rig().await;
    let own: PathName = "mesh1.admin.1".parse().unwrap();
    let me = own_end(&own);
    // B: Direct failed, a valid Proxy through P; C: Direct active.
    let mut held = held_for(&own);
    held.apply(edge(me.clone(), end(&r.b), ConnectionKind::Direct, ConnectionState::Failed, None, 10)).unwrap();
    held.apply(edge(end(&r.p), end(&r.b), ConnectionKind::Direct, ConnectionState::Connected, None, 11)).unwrap();
    held.apply(edge(me.clone(), end(&r.b), ConnectionKind::Proxy, ConnectionState::Connected, Some(end(&r.p)), 12)).unwrap();
    held.apply(edge(me.clone(), end(&r.c), ConnectionKind::Direct, ConnectionState::Connected, None, 13)).unwrap();
    let policy = CarrierPolicy::Forwardable { carrier_kind: NodeKind::RpcNode };

    // The domain chose B: B's reachability changed to Proxy and the seam still executes B.
    let target = select(&r, "B");
    let route: RouteChoice = (&resolve(&held, &own, &target.resolved.name, policy).route).into();
    assert_eq!(route, RouteChoice::ViaPeer { carrier: r.p.resolved.node_id.clone(), path: r.p.resolved.name.clone() });
    let (out, _, _) = routed(&r, target, route, b"b").await;
    assert_eq!(served_by(&out).map(|s| s.0).as_deref(), Some("mesh1.rpc.1"), "reachability cannot move the selection to C: {out:?}");
    assert_eq!(r.c.handled.load(Ordering::SeqCst), 0);

    // B becomes Direct again: same target, a different leg, still B.
    held.apply(edge(me.clone(), end(&r.b), ConnectionKind::Direct, ConnectionState::Connected, None, 20)).unwrap();
    let route: RouteChoice = (&resolve(&held, &own, &target.resolved.name, policy).route).into();
    let (out, _, leg) = routed(&r, target, route, b"b-again").await;
    assert_eq!(served_by(&out).map(|s| s.0).as_deref(), Some("mesh1.rpc.1"), "{out:?}");
    assert!(matches!(leg, RouteLeg::Direct | RouteLeg::ViaPeer { .. }), "connections, not the seam, picks the leg: {leg:?}");
    assert_eq!(r.c.handled.load(Ordering::SeqCst), 0, "C is never attempted while B is selected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_changes_do_not_touch_the_selection_and_selection_changes_fabricate_no_connection_state() {
    let r = rig().await;
    let own: PathName = "mesh1.admin.1".parse().unwrap();
    let me = own_end(&own);
    let mut held = held_for(&own);
    held.apply(edge(me.clone(), end(&r.b), ConnectionKind::Direct, ConnectionState::Connected, None, 10)).unwrap();
    held.apply(edge(me.clone(), end(&r.c), ConnectionKind::Direct, ConnectionState::Connected, None, 11)).unwrap();
    let policy = CarrierPolicy::Forwardable { carrier_kind: NodeKind::RpcNode };
    // The selector is a value the seam only reads: Direct -> Proxy -> Direct on B changes it never.
    let selected = "B";
    let mut at = 20;
    let steps = [
        // Direct failed, no Proxy yet: connections says no route, and the seam sends nothing.
        (ConnectionKind::Direct, ConnectionState::Failed, None, RouteChoice::NoActiveRoute),
        (ConnectionKind::Proxy, ConnectionState::Connected, Some(end(&r.p)), RouteChoice::ViaPeer { carrier: r.p.resolved.node_id.clone(), path: r.p.resolved.name.clone() }),
        // Direct is back, and the valid own Proxy still wins until it is dropped: connections' rule.
        (ConnectionKind::Direct, ConnectionState::Connected, None, RouteChoice::ViaPeer { carrier: r.p.resolved.node_id.clone(), path: r.p.resolved.name.clone() }),
        (ConnectionKind::Proxy, ConnectionState::Disconnected, Some(end(&r.p)), RouteChoice::Direct),
    ];
    for (kind, state, carrier, expected) in steps {
        at += 10;
        if carrier.is_some() {
            held.apply(edge(end(&r.p), end(&r.b), ConnectionKind::Direct, ConnectionState::Connected, None, at)).unwrap();
        }
        held.apply(edge(me.clone(), end(&r.b), kind, state, carrier, at + 1)).unwrap();
        let target = select(&r, selected);
        let route: RouteChoice = (&resolve(&held, &own, &target.resolved.name, policy).route).into();
        assert_eq!(route, expected, "connections decide the leg");
        let (out, _, leg) = routed(&r, target, route, b"x").await;
        match expected {
            RouteChoice::NoActiveRoute => {
                assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::NoActiveRoute), "{out:?}");
                assert_eq!(leg, RouteLeg::None);
            }
            _ => assert_eq!(served_by(&out).map(|s| s.0).as_deref(), Some("mesh1.rpc.1"), "{out:?}"),
        }
        assert_eq!(r.c.handled.load(Ordering::SeqCst), 0, "B's reachability never sends anything to C");
    }
    assert_eq!(selected, "B", "the selection is the domain's and never moved");

    // The domain moves B -> C: the seam executes C and writes nothing about B's connections. The
    // held projection is the seam's read-only input: what it answers for B before and after, and
    // how many facts it holds, are the same.
    let before = (resolve(&held, &own, &r.b.resolved.name, policy), held.keys(), held.active_len(), held.own_active_proxies().len());
    let target = select(&r, "C");
    let route: RouteChoice = (&resolve(&held, &own, &target.resolved.name, policy).route).into();
    let (out, _, leg) = routed(&r, target, route, b"c").await;
    assert_eq!(served_by(&out).map(|s| s.0).as_deref(), Some("mesh1.rpc.2"), "{out:?}");
    assert_eq!(leg, RouteLeg::Direct);
    let after = (resolve(&held, &own, &r.b.resolved.name, policy), held.keys(), held.active_len(), held.own_active_proxies().len());
    assert_eq!(after, before, "a selection change emits no Direct Failed, Proxy Connected, reconnect or retirement for B");
    assert_eq!(after.0.route, EffectiveRoute::Direct { known: true }, "B's route is as the connections left it");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn certainty_stays_downstream_an_indeterminate_is_never_replayed_through_another_route() {
    let r = rig().await;
    let target = select(&r, "B");
    // B commits and never answers within the budget: Indeterminate for B, handed back as is.
    let (out, ev, leg) = routed(&r, target, RouteChoice::Direct, b"hold").await;
    assert!(matches!(out, RpcOutcome::Indeterminate(_)), "{out:?}");
    assert!(ev.unwrap().committed);
    assert_eq!(leg, RouteLeg::Direct);
    assert_eq!((r.b.handled.load(Ordering::SeqCst), r.c.handled.load(Ordering::SeqCst), r.p.handled.load(Ordering::SeqCst)), (1, 0, 0), "nothing was replayed through C or P");
    // A proven NotSent is likewise one outcome for one leg: the seam chose nothing else.
    let (out, _, leg) = routed(&r, target, RouteChoice::NoActiveRoute, b"none").await;
    assert!(matches!(out, RpcOutcome::NotSent(_)));
    assert_eq!(leg, RouteLeg::None);
    assert_eq!(r.b.handled.load(Ordering::SeqCst), 1);
}
