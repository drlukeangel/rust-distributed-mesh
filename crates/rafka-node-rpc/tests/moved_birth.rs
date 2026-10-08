//! A node that restarts on a fresh port is dialled at that port and nowhere else.
//!
//! The client's Iroh endpoint remembers every address it was ever given for a key and sends a
//! connection's first packet to all of them until a path is selected. A restarted birth keeps its
//! key and binds a fresh port; its old port is free for any other process, and a process that
//! binds it answers the first packet with a handshake under its OWN key, which the client rejects
//! as `UnknownIssuer` before the real birth's answer is the one it takes.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use std::net::SocketAddr;
use std::sync::Arc;

struct Birth {
    router: Router,
    record: ResolvedNode,
}

/// A node serving Ping under `key` at `addr` (port 0 = a fresh one).
async fn birth(key: &SecretKey, node_id: &NodeId, addr: SocketAddr) -> Birth {
    let incarnation = IncarnationId::mint();
    // A port a closed endpoint held is released by the kernel a moment after `close` returns.
    let ep = {
        let mut tries = 0;
        loop {
            match rafka_node_rpc::endpoint::bind(key.clone(), addr).await {
                Ok(ep) => break ep,
                Err(_) if addr.port() != 0 && tries < 100 => {
                    tries += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                Err(e) => panic!("bind {addr}: {e:#}"),
            }
        }
    };
    let bound = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let server = ServerBuilder::new()
        .serve::<Ping, _, _>(OpOwner::Core, |_peer, req: PingRequest| async move {
            let PingRequest::Ping { payload, .. } = req;
            Ok(PingReply::Pong { payload })
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
        .unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let record = ResolvedNode { node_id: node_id.clone(), name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: bound, incarnation };
    Birth { router, record }
}

/// CONTRACT: a client that has called a node at one port, after the node restarted on another
/// port while a different process took the first, calls the new birth every time: a process that
/// answers at an address the resolver no longer names for the node never takes part in the dial.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_reaches_a_restarted_birth_whose_old_port_another_process_took() {
    const ROUNDS: usize = 12;
    let mut failures = Vec::new();
    for round in 0..ROUNDS {
        let key = SecretKey::generate();
        let node_id = NodeId::mint();
        let first = birth(&key, &node_id, "127.0.0.1:0".parse().unwrap()).await;
        let old_port = first.record.transport_addr;

        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(first.record.clone());
        let cep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = NodeRpcClient::new(cep, resolver.clone());
        let target = NodeTarget::ExactNode(node_id.clone());
        let (out, _) = client.call::<Ping>(&target, &PingRequest::Ping { payload: b"before".to_vec() }, &CallOptions::default()).await;
        assert!(matches!(out, RpcOutcome::Reply(_)), "round {round}: the first birth answers: {out:?}");

        // The birth stops; another process (its own key) takes its port; the key restarts elsewhere.
        first.router.shutdown().await.unwrap();
        first.router.endpoint().close().await;
        drop(first);
        let foreign = birth(&SecretKey::generate(), &NodeId::mint(), old_port).await;
        assert_eq!(foreign.record.transport_addr, old_port, "round {round}: the foreign process holds the old port");
        let second = birth(&key, &node_id, "127.0.0.1:0".parse().unwrap()).await;
        assert_ne!(second.record.transport_addr, old_port);
        resolver.insert(second.record.clone());

        let (out, _) = client.call::<Ping>(&target, &PingRequest::Ping { payload: b"after".to_vec() }, &CallOptions::default()).await;
        if !matches!(out, RpcOutcome::Reply(_)) {
            failures.push(format!("round {round}: {out:?}"));
        }
        foreign.router.shutdown().await.unwrap();
        second.router.shutdown().await.unwrap();
    }
    assert!(failures.is_empty(), "{} of {ROUNDS} calls to the restarted birth failed:\n{}", failures.len(), failures.join("\n"));
}
