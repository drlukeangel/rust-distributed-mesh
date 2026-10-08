//! A birth known to have exited is dialled nowhere (i143 R-J1 follow-up).
//!
//! A restarted node keeps its key and binds a port the operating system assigns; its old port is
//! free for any other process. Between the old birth's proven exit and the new birth's join the
//! resolver names no current birth for the node, so the client dials nothing (a dial's
//! `replace_direct_addrs(peer, [addr])` would re-pin the retired address for the key).

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{CallOptions, LiveNodeResolver, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::{NotSentReason, ResolveFailure, RpcOutcome};
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use std::sync::Arc;

async fn ping(c: &NodeRpcClient, t: &NodeTarget) -> RpcOutcome<PingReply> {
    c.call::<Ping>(t, &PingRequest::Ping { payload: b"p".to_vec() }, &CallOptions::default()).await.0
}

/// CONTRACT: after `retire_birth`, a call to the exited node ends `NotSent(Resolve(Unavailable))`
/// at once and starts no dial (a dial to the old address would install it for the key again); the successor's birth, applied with the old one as its lineage, is dialled normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_to_an_exited_birth_dials_nothing_and_the_successor_is_dialled() {
    let key = SecretKey::generate();
    let node_id = NodeId::mint();
    let serve = |incarnation: &IncarnationId| {
        ServerBuilder::new()
            .serve::<Ping, _, _>(OpOwner::Core, |_peer, req: PingRequest| async move {
                let PingRequest::Ping { payload, .. } = req;
                Ok(PingReply::Pong { payload })
            })
            .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
            .unwrap()
    };

    // The first birth, at a port the OS assigned.
    let old_inc = IncarnationId::mint();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let old_addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let ep_handle = ep.clone();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, serve(&old_inc)).spawn();
    let old = ResolvedNode { node_id: node_id.clone(), name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: old_addr, incarnation: old_inc.clone() };

    let resolver = Arc::new(LiveNodeResolver::default());
    resolver.apply(old.clone(), None);
    let client_ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = NodeRpcClient::new(client_ep, resolver.clone());
    let target = NodeTarget::ExactNode(node_id.clone());
    assert!(matches!(ping(&client, &target).await, RpcOutcome::Reply(_)), "the live birth answers");

    // The birth exits; another process takes its port.
    router.shutdown().await.unwrap();
    ep_handle.close().await;
    // The exit is proven.
    resolver.retire_birth(&node_id, &old_inc);
    let out = ping(&client, &target).await;
    match out {
        RpcOutcome::NotSent(n) => assert!(matches!(n.reason(), NotSentReason::Resolve(ResolveFailure::Unavailable)), "{:?}", n.reason()),
        other => panic!("a call to an exited birth must resolve nothing: {}", other.name()),
    }
    assert!(client.dialing().is_empty(), "no dial was started");
    // The successor: a new birth of the same key and node at a new port.
    let new_inc = IncarnationId::mint();
    let ep2 = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let new_addr = ep2.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let _router2 = Router::builder(ep2).accept(rafka_node_rpc::ALPN, serve(&new_inc)).spawn();
    let new = ResolvedNode { transport_addr: new_addr, incarnation: new_inc.clone(), ..old.clone() };
    resolver.apply(new, Some(&old_inc));
    assert!(matches!(ping(&client, &target).await, RpcOutcome::Reply(_)), "the successor birth is dialled at its own address");
}
