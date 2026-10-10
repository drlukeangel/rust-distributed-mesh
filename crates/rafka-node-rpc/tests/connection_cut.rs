//! A node's hard cut of its mesh connections (node.connections.delete) spares the caller whose call is
//! being answered, and a client's own cut closes everything it holds.
//!
//! CONTRACT: `close_connections_except(spare)` closes every connection the server accepted except those
//! of `spare`; the endpoint stays bound, so the cut peer's next call dials afresh and is served. The
//! spared peer's connection is untouched. `close_pooled` closes every connection a client holds and
//! `close_pooled_to` the ones to one peer. What must NOT happen: the spared connection closed, a cut peer
//! unable to dial again, or a connection left pooled after a cut.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{CallOptions, LiveNodeResolver, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use std::sync::Arc;

async fn client_of(resolver: Arc<LiveNodeResolver>) -> (Arc<NodeRpcClient>, iroh::PublicKey) {
    let key = SecretKey::generate();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    (Arc::new(NodeRpcClient::new(ep, resolver)), key.public())
}

async fn ping(client: &NodeRpcClient, target: &NodeTarget) -> bool {
    matches!(client.call::<Ping>(target, &PingRequest::Ping { payload: b"x".to_vec() }, &CallOptions::default()).await.0, RpcOutcome::Reply(r) if matches!(r.value(), PingReply::Pong { .. }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_closes_every_accepted_connection_but_the_spared_peers_and_the_endpoint_still_takes_a_fresh_dial() {
    let key = SecretKey::generate();
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let server = ServerBuilder::new()
        .serve::<Ping, _, _>(OpOwner::Core, |_peer, req: PingRequest| async move {
            let PingRequest::Ping { payload, .. } = req;
            Ok(PingReply::Pong { payload })
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
        .unwrap();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let _router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server.clone()).spawn();
    let resolver = Arc::new(LiveNodeResolver::default());
    resolver.apply(ResolvedNode { node_id: node_id.clone(), name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation }, None);
    let target = NodeTarget::ExactNode(node_id);
    let (spared, spared_key) = client_of(resolver.clone()).await;
    let (cut, _) = client_of(resolver.clone()).await;
    assert!(ping(&spared, &target).await && ping(&cut, &target).await);
    assert_eq!((spared.pooled().len(), cut.pooled().len()), (1, 1));

    let closed = server.close_connections_except(Some(spared_key), "node.connections.delete");
    assert_eq!(closed, 1, "one connection was not the spared peer's");
    // The spared peer's connection still carries a call, on the connection it already held.
    assert!(ping(&spared, &target).await, "the spared peer's connection is open");
    // The cut peer's connection is closed; its next call dials afresh and the endpoint serves it.
    // A call that races the close ends typed on the closed connection (the pool then evicts it); the one after dials afresh.
    let _ = cut.call::<Ping>(&target, &PingRequest::Ping { payload: b"x".to_vec() }, &CallOptions::default()).await;
    assert!(ping(&cut, &target).await, "the endpoint is still bound: a fresh dial is served");

    // The client's own cut.
    assert_eq!(cut.close_pooled("node.connections.delete"), 1);
    assert!(cut.pooled().is_empty(), "nothing is left pooled after a cut");
    assert!(ping(&cut, &target).await, "the next call dials afresh");
    assert_eq!(cut.close_pooled_to(&key.public(), "node.stopped"), 1, "the connections to one peer are the ones closed");
    assert!(cut.pooled().is_empty());
}
