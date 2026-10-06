//! i143.e6.s4: generic one-hop carried execution — the carrier makes exactly one direct inner
//! call, a non-forwardable family is refused by type, and certainty composes across the hop.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{EndpointSlot, IncarnationId, NodeId};
use rafka_node_rpc::{CallOptions, HandlerFault, NodeRpcClient, NodeTarget, ResolvedNode, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::{LedgerEntry, TagOwner, TagState};
use rafka_node_rpc_contract::echo::{Echo, EchoReply, EchoRequest};
use rafka_node_rpc_contract::forward::{Forward, ForwardReply, ForwardRequest};
use rafka_node_rpc_contract::outcome::{IndeterminateReason, MalformedKind, NotSentReason, ReplyKind, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A forwardable test family on a ledgered test tag.
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
    const TAG: u8 = 0x7E;
    const NAME: &'static str = "probe";
    const MAX_REQUEST_FRAME_BYTES: usize = 4096;
    const MAX_REPLY_FRAME_BYTES: usize = 4096;
    const FORWARDABLE: bool = true;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 7;
    type Request = ProbeRequest;
    type Reply = ProbeReply;
    fn traceparent(_: &ProbeRequest) -> Option<&str> {
        None
    }
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
    vec![LedgerEntry { tag: Probe::TAG, family: "probe".into(), owner: TagOwner::Product("test".into()), state: TagState::Live }]
}

struct Node {
    router: Router,
    key: SecretKey,
    resolved: ResolvedNode,
}

async fn start(server: rafka_node_rpc::NodeRpcServer, key: SecretKey, name: &str) -> Node {
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolved = ResolvedNode {
        node_id: NodeId::mint(),
        name: name.parse().unwrap(),
        transport_id: key.public(),
        incarnation: IncarnationId::mint(),
        endpoints: vec![EndpointSlot::assign("rpc-0", addr)],
    };
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
}

/// target serves Probe and Echo; the carrier serves forward and carries Probe (and Echo, which is
/// not forwardable and so is never carried); the origin knows only the carrier and the target.
async fn rig() -> Rig {
    let handled = Arc::new(AtomicU64::new(0));
    let applied = Arc::new(tokio::sync::Notify::new());
    let (h, a) = (handled.clone(), applied.clone());
    let target_server = ServerBuilder::new()
        .ledger(test_ledger())
        .serve::<Probe, _, _>(TagOwner::Product("test".into()), move |peer, req: ProbeRequest| {
            let (h, a) = (h.clone(), a.clone());
            async move {
                h.fetch_add(1, Ordering::SeqCst);
                a.notify_one();
                let ProbeRequest::Probe { payload } = req;
                if payload == b"hold" {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
                Ok::<_, HandlerFault>(ProbeReply::Probed { payload, caller: peer.transport_id.to_string() })
            }
        })
        .serve::<Echo, _, _>(TagOwner::Core, |_p, req: EchoRequest| async move {
            let EchoRequest::Echo { payload, .. } = req;
            Ok(EchoReply::Echoed { payload })
        })
        .seal("rpc-0")
        .unwrap();
    let target = start(target_server, SecretKey::generate(), "mesh1.rpc.3").await;

    // The carrier calls out on the endpoint it serves on, as a node does: the target sees the
    // carrier's own transport identity.
    let carrier_key = SecretKey::generate();
    let carrier_ep = rafka_node_rpc::endpoint::bind(carrier_key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let carrier_addr = carrier_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let carrier_resolver = Arc::new(StaticResolver::new());
    carrier_resolver.insert(target.resolved.clone());
    let carrier_client = Arc::new(NodeRpcClient::new(carrier_ep.clone(), carrier_resolver));
    let carrier_server = ServerBuilder::new()
        .ledger(test_ledger())
        .carry::<Probe>()
        .carry::<Echo>()
        .serve_forward(carrier_client)
        .seal("rpc-0")
        .unwrap();
    let carrier = Node {
        router: Router::builder(carrier_ep).accept(rafka_node_rpc::ALPN, carrier_server).spawn(),
        key: carrier_key.clone(),
        resolved: ResolvedNode {
            node_id: NodeId::mint(),
            name: "mesh1.rpc.2".parse().unwrap(),
            transport_id: carrier_key.public(),
            incarnation: IncarnationId::mint(),
            endpoints: vec![EndpointSlot::assign("rpc-0", carrier_addr)],
        },
    };
    let origin = client_with(&[&carrier.resolved, &target.resolved]).await;
    Rig { origin, carrier, target, handled, applied }
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
    let echo = EchoRequest::Echo { traceparent: None, payload: b"x".to_vec() };
    let (out, _) = r.origin.call_via::<Echo>(&carrier_of(&r), &r.target.resolved.node_id, &echo, &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::NotForwardable { tag: Echo::TAG }), "{out:?}");

    // A forward built by hand for a non-forwardable tag: the carrier refuses it by type.
    let forward = ForwardRequest::Forward {
        traceparent: None,
        target: r.target.resolved.node_id.as_str().to_string(),
        inner_tag: Echo::TAG,
        inner: Echo::encode_request(&echo).unwrap(),
    };
    let (out, _) = r.origin.call::<Forward>(&carrier_of(&r), &forward, &CallOptions::default()).await;
    assert_eq!(out.reply().map(|x| x.value().clone()), Some(ForwardReply::NotForwardable { tag: Echo::TAG }));
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
