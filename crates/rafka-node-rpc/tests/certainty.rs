//! i143.e6.s1 network integration: the commit cut over real Iroh streams.
//!
//! A real server endpoint and a real client endpoint on 127.0.0.1. Every cell
//! asserts the caller's `RpcOutcome` and the server's own counters, so
//! "never dispatched" is proven on the receiver, not inferred from the caller.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{EndpointSlot, IncarnationId, NodeId};
use rafka_node_rpc::admission::Limits;
use rafka_node_rpc::{Budget, CallOptions, Decode, HandlerFault, NodeRpcClient, NodeTarget, ResolvedNode, ServerBuilder, ServerStats, StaticResolver};
use rafka_node_rpc_contract::catalog::TagOwner;
use rafka_node_rpc_contract::echo::{Echo, EchoReply, EchoRequest};
use rafka_node_rpc_contract::outcome::{IndeterminateReason, MalformedKind, NotSentReason, ReplyKind, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct Rig {
    _router: Router,
    stats: Arc<ServerStats>,
    handled: Arc<AtomicU64>,
    client: NodeRpcClient,
    target: NodeTarget,
}

async fn rig(echo_cap: Option<Limits>) -> Rig {
    let handled = Arc::new(AtomicU64::new(0));
    let h = handled.clone();
    let mut b = ServerBuilder::new().serve::<Echo, _, _>(TagOwner::Core, move |_peer, req: EchoRequest| {
        let h = h.clone();
        async move {
            h.fetch_add(1, Ordering::SeqCst);
            let EchoRequest::Echo { payload, .. } = req;
            match payload.as_slice() {
                b"hang" => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    unreachable!()
                }
                b"fault" => Err(HandlerFault::invariant_broken("test fault")),
                b"panic" => panic!("test panic"),
                _ => Ok(EchoReply::Echoed { payload }),
            }
        }
    });
    if let Some(l) = echo_cap {
        b = b.limits(Echo::TAG, l);
    }
    let server = b.seal("rpc-0").unwrap();
    let stats = server.stats();
    let key = SecretKey::generate();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolver = Arc::new(StaticResolver::new());
    let node_id = NodeId::mint();
    resolver.insert(ResolvedNode {
        node_id: node_id.clone(),
        name: "mesh1.rpc.1".parse().unwrap(),
        fabric_id: key.public(),
        incarnation: IncarnationId::mint(),
        endpoints: vec![EndpointSlot::assign("rpc-0", addr)],
    });
    let cep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    Rig { _router: router, stats, handled, client: NodeRpcClient::new(cep, resolver), target: NodeTarget::ExactNode(node_id) }
}

fn echo(p: &[u8]) -> EchoRequest {
    EchoRequest::Echo { traceparent: None, payload: p.to_vec() }
}

async fn eventually(what: &str, f: impl Fn() -> bool) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("never observed: {what}");
}

#[tokio::test]
async fn a_complete_request_with_a_valid_reply_is_reply() {
    let r = rig(None).await;
    let (out, ev) = r.client.call::<Echo>(&r.target, &echo(b"ping"), &CallOptions::default()).await;
    let reply = out.reply().expect("Reply");
    assert_eq!(reply.value(), &EchoReply::Echoed { payload: b"ping".to_vec() });
    assert_eq!(reply.class(), ReplyKind::Success);
    assert!(ev.unwrap().committed);
    assert_eq!(ServerStats::get(&r.stats.dispatched), 1);
}

#[tokio::test]
async fn an_unfinished_send_resets_with_499_is_not_sent_and_is_never_dispatched() {
    let r = rig(None).await;
    let opts = CallOptions { cut_before_finish: true, ..Default::default() };
    let (out, ev) = r.client.call::<Echo>(&r.target, &echo(&[7u8; 4096]), &opts).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::FrameNotSent), "{out:?}");
    assert!(!ev.unwrap().committed);
    eventually("the receiver dropped the unfinished request", || ServerStats::get(&r.stats.dropped_unfinished) == 1).await;
    assert_eq!(ServerStats::get(&r.stats.dispatched), 0, "the partial frame never reached dispatch");
    assert_eq!(r.handled.load(Ordering::SeqCst), 0, "no handler ran");
    // The connection stays healthy for the next call.
    let (out, _) = r.client.call::<Echo>(&r.target, &echo(b"after"), &CallOptions::default()).await;
    assert!(out.reply().is_some(), "{out:?}");
}

#[tokio::test]
async fn reply_loss_after_a_complete_send_is_indeterminate() {
    let r = rig(None).await;
    let opts = CallOptions { budget: Budget::Split { send: Duration::from_secs(5), reply: Duration::from_millis(300) }, ..Default::default() };
    let (out, ev) = r.client.call::<Echo>(&r.target, &echo(b"hang"), &opts).await;
    assert!(matches!(&out, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::ReplyDeadline), "{out:?}");
    assert!(ev.unwrap().committed, "the request crossed the commit cut");
    eventually("the handler ran", || r.handled.load(Ordering::SeqCst) == 1).await;
    assert!(!out.proves_not_dispatched());
}

#[tokio::test]
async fn a_handler_fault_or_panic_after_dispatch_is_indeterminate_423() {
    let r = rig(None).await;
    for p in [&b"fault"[..], b"panic"] {
        let (out, _) = r.client.call::<Echo>(&r.target, &echo(p), &CallOptions::default()).await;
        assert!(matches!(&out, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::Reset(423)), "{out:?}");
    }
    assert_eq!(ServerStats::get(&r.stats.faults), 2);
}

#[tokio::test]
async fn an_unknown_tag_is_unserved_by_421() {
    let r = rig(None).await;
    let (out, _) = r
        .client
        .invoke_raw::<EchoReply, _>(&r.target, 0x42, vec![1, 2, 3], 1024, &CallOptions::default(), |d| match d {
            Decode::Committed(c, b) => c.reply::<Echo>(b),
            Decode::Early(e, b) => e.reply::<Echo>(b),
        })
        .await;
    assert!(matches!(&out, RpcOutcome::Unserved(u) if u.tag() == 0x42), "{out:?}");
    assert_eq!(ServerStats::get(&r.stats.unserved), 1);
    assert_eq!(ServerStats::get(&r.stats.dispatched), 0);
}

#[tokio::test]
async fn a_known_oversize_request_gets_typed_too_large_before_its_body_is_read() {
    let r = rig(None).await;
    let big = echo(&vec![0u8; Echo::MAX_REQUEST_FRAME_BYTES + 1024]);
    let (out, _) = r.client.call::<Echo>(&r.target, &big, &CallOptions::default()).await;
    assert_eq!(out.reply().expect("typed reply").class(), ReplyKind::Malformed(MalformedKind::TooLarge), "{out:?}");
    assert_eq!(ServerStats::get(&r.stats.dispatched), 0);
    assert_eq!(r.handled.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn admission_full_is_an_immediate_typed_busy_and_the_handler_never_runs() {
    let r = rig(Some(Limits { node_wide: 1, per_caller: 1 })).await;
    let c = Arc::new(r);
    let c2 = c.clone();
    let hang = tokio::spawn(async move {
        let opts = CallOptions { budget: Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(2) }, ..Default::default() };
        c2.client.call::<Echo>(&c2.target, &echo(b"hang"), &opts).await.0
    });
    eventually("the first call holds the only permit", || c.handled.load(Ordering::SeqCst) == 1).await;
    let (out, _) = c.client.call::<Echo>(&c.target, &echo(b"second"), &CallOptions::default()).await;
    assert_eq!(out.reply().expect("typed reply").class(), ReplyKind::Busy, "{out:?}");
    assert_eq!(c.handled.load(Ordering::SeqCst), 1, "the refused call never ran");
    assert_eq!(ServerStats::get(&c.stats.busy), 1);
    let _ = hang.await;
}

#[tokio::test]
async fn resolution_failures_and_a_superseded_pin_are_not_sent_before_any_dial() {
    let r = rig(None).await;
    let (out, ev) = r.client.call::<Echo>(&NodeTarget::ExactNode(NodeId::mint()), &echo(b"x"), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::Resolve(_))), "{out:?}");
    assert!(ev.is_none());
    let opts = CallOptions { pin: Some(("rpc-0".into(), rafka_mesh_entity::FreshnessToken::mint())), ..Default::default() };
    let (out, _) = r.client.call::<Echo>(&r.target, &echo(b"x"), &opts).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::Superseded { slot: "rpc-0".into() }), "{out:?}");
    assert_eq!(ServerStats::get(&r.stats.dispatched), 0);
}
