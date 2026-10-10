//! A node that closes its endpoint with a reply unsettled takes the reply down with the
//! connection: the caller reads `connection lost` where the node answered. The server names the
//! moment every call it dispatched has its reply settled, and a closing node waits for it.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{CallOptions, LiveNodeResolver, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use std::sync::Arc;
use tokio::sync::{Notify, Semaphore};

/// CONTRACT: `settled` is pending while a call dispatched before it is still being served, and
/// resolves once every such call's reply has reached its caller, without waiting for a call
/// dispatched after it was asked. What must NOT happen: `settled` resolving while the handler of
/// an earlier call has not answered, or never resolving because a later call is still served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settled_waits_for_the_replies_dispatched_before_it_and_for_no_later_one() {
    let key = SecretKey::generate();
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    // Each call waits on its own gate: which handler answers is the test's to say.
    let (first_gate, second_gate) = (Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0)));
    let entered = Arc::new(Notify::new());
    let (handler_gates, handler_entered) = ((first_gate.clone(), second_gate.clone()), entered.clone());
    let server = ServerBuilder::new()
        .serve::<Ping, _, _>(OpOwner::Core, move |_peer, req: PingRequest| {
            let (gates, entered) = (handler_gates.clone(), handler_entered.clone());
            async move {
                let PingRequest::Ping { payload, .. } = req;
                let gate = if payload == b"first" { gates.0 } else { gates.1 };
                entered.notify_one();
                gate.acquire().await.expect("the gate stays open").forget();
                Ok(PingReply::Pong { payload })
            }
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
        .unwrap();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let _router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server.clone()).spawn();
    let resolver = Arc::new(LiveNodeResolver::default());
    resolver.apply(ResolvedNode { node_id: node_id.clone(), name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation }, None);
    let client = Arc::new(NodeRpcClient::new(rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap(), resolver));
    let target = NodeTarget::ExactNode(node_id);
    let call = |payload: &'static [u8]| {
        let (client, target) = (client.clone(), target.clone());
        tokio::spawn(async move { client.call::<Ping>(&target, &PingRequest::Ping { payload: payload.to_vec() }, &CallOptions::default()).await.0 })
    };

    // Nothing dispatched: nothing to wait for.
    server.settled().await;

    let first = call(b"first");
    entered.notified().await;
    let mut settled = Box::pin(server.settled());
    assert!(tokio::time::timeout(std::time::Duration::ZERO, &mut settled).await.is_err(), "settled is pending while the first call's handler has not answered");

    // A call dispatched after `settled` was asked is not waited for.
    let second = call(b"second");
    entered.notified().await;
    first_gate.add_permits(1);
    match first.await.unwrap() {
        RpcOutcome::Reply(r) => assert!(matches!(r.value(), PingReply::Pong { payload } if payload == b"first"), "the first call is answered: {:?}", r.value()),
        other => panic!("the first call is answered, not {}", other.name()),
    }
    settled.await;

    second_gate.add_permits(1);
    match second.await.unwrap() {
        RpcOutcome::Reply(r) => assert!(matches!(r.value(), PingReply::Pong { payload } if payload == b"second"), "{:?}", r.value()),
        other => panic!("the second call is answered, not {}", other.name()),
    }
    server.settled().await;
}
