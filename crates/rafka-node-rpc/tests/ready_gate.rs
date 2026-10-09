//! A server whose ready gate is closed refuses every op, ping included, with the protocol's
//! typed `NotReady`, and serves normally once the gate opens (R-S2: a node-admin's router
//! accepts before its Status authority is filled).

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{CallOptions, LiveNodeResolver, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// CONTRACT: while the gate is closed a Ping is answered `NotReady` by name and the handler is
/// never entered; the same call after the gate opens is answered `Pong`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_ready_gate_refuses_ping_typed_and_an_open_gate_serves_it() {
    let key = SecretKey::generate();
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let ready = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(AtomicU64::new(0));
    let seen = entered.clone();
    let server = ServerBuilder::new()
        .serve::<Ping, _, _>(OpOwner::Core, move |_peer, req: PingRequest| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                let PingRequest::Ping { payload, .. } = req;
                Ok(PingReply::Pong { payload })
            }
        })
        .with_ready_gate(ready.clone())
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
        .unwrap();
    let stats = server.stats();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let _router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolver = Arc::new(LiveNodeResolver::default());
    resolver.apply(ResolvedNode { node_id: node_id.clone(), name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation }, None);
    let client = NodeRpcClient::new(rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap(), resolver);
    let target = NodeTarget::ExactNode(node_id);
    let ping = || async { client.call::<Ping>(&target, &PingRequest::Ping { payload: b"p".to_vec() }, &CallOptions::default()).await.0 };

    match ping().await {
        RpcOutcome::Reply(r) => assert!(matches!(r.value(), PingReply::NotReady { .. }), "a closed gate answers NotReady: {:?}", r.value()),
        other => panic!("a closed gate answers a typed reply, not {}", other.name()),
    }
    assert_eq!(entered.load(Ordering::SeqCst), 0, "the handler never ran");
    assert_eq!(rafka_node_rpc::ServerStats::get(&stats.not_ready), 1);

    ready.store(true, Ordering::SeqCst);
    match ping().await {
        RpcOutcome::Reply(r) => assert!(matches!(r.value(), PingReply::Pong { .. }), "an open gate serves: {:?}", r.value()),
        other => panic!("an open gate answers, not {}", other.name()),
    }
    assert_eq!(entered.load(Ordering::SeqCst), 1);
}
