//! i143.e6.s2 functional: a dial is shared by every caller of a process birth (PRD §1.19–20, §14;
//! node-rpc-rdm-ownership.md §8–§9), over real Iroh endpoints on 127.0.0.1 with the scenario
//! crate's UDP [`Tap`] between the caller and the node. The pool's other cells are
//! `rafka-node-rpc/tests/pool.rs`; this one drives the dial through the tap, so it lives with the
//! crate that owns the tap.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{Budget, CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::{NotSentReason, RpcOutcome};
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn echo(p: &[u8]) -> PingRequest {
    PingRequest::Ping { payload: p.to_vec() }
}

/// A node serving Ping from one endpoint: every payload is echoed at once.
struct Node {
    _router: Router,
    _server: rafka_node_rpc::NodeRpcServer,
    resolved: ResolvedNode,
}

async fn node() -> Node {
    let key = SecretKey::generate();
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let server = ServerBuilder::new()
        .serve::<Ping, _, _>(OpOwner::Core, move |_peer, req: PingRequest| async move {
            let PingRequest::Ping { payload, .. } = req;
            Ok(PingReply::Pong { payload })
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server.clone()).spawn();
    let resolved = ResolvedNode { node_id, name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation };
    Node { _router: router, _server: server, resolved }
}

/// Until `client` holds a dial in flight (no timer: the dial is registered by the call's first poll).
async fn until_dialing(client: &NodeRpcClient) {
    while client.dialing().is_empty() {
        tokio::task::yield_now().await;
    }
}

struct Rig {
    client: NodeRpcClient,
    target: NodeTarget,
}

/// A client whose resolver holds `record`.
async fn rig(record: ResolvedNode) -> Rig {
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(record.clone());
    let cep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    Rig { client: NodeRpcClient::new(cep, resolver), target: NodeTarget::ExactNode(record.node_id) }
}

/// CONTRACT: a dial is shared by every caller of a birth, and each caller's own budget bounds its
/// own wait. A caller that joins a dial another caller started, with a longer budget, is not
/// ended by the starter's shorter one: the starter ends `NotSent(Deadline)` at its own deadline
/// and the joiner is answered once the network lets the dial complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_caller_joining_a_dial_is_not_ended_by_the_starters_shorter_budget() {
    use rafka_test_scenario::sim::{Mode, Tap};
    let n = node().await;
    let tap = Tap::start(n.resolved.transport_addr).await.unwrap();
    tap.set_to_server(Mode::Drop);
    let mut behind = n.resolved.clone();
    behind.transport_addr = tap.addr();
    let r = rig(behind).await;
    let short = CallOptions { budget: Budget::Split { send: Duration::from_millis(300), reply: Duration::from_secs(1) }, ..Default::default() };
    let rr = &r;
    let tapr = &tap;
    let ((starter, starter_ended_ms), (joiner, joiner_ended_ms)) = tokio::join!(
        async {
            let started = Instant::now();
            let out = rr.client.call::<Ping>(&rr.target, &echo(b"starter"), &short).await.0;
            let ended = started.elapsed().as_millis();
            // The network heals once the starter's budget is spent.
            tapr.set_to_server(Mode::Pass);
            (out, ended)
        },
        async {
            until_dialing(&rr.client).await;
            let started = Instant::now();
            let out = rr.client.call::<Ping>(&rr.target, &echo(b"joiner"), &CallOptions::default()).await.0;
            (out, started.elapsed().as_millis())
        }
    );
    assert!(matches!(&starter, RpcOutcome::NotSent(x) if *x.reason() == NotSentReason::Deadline), "the starter ends at its own deadline: {starter:?}");
    assert!(starter_ended_ms >= 300, "the starter waited its whole budget: {starter_ended_ms} ms");
    assert!(joiner.reply().is_some(), "the joiner holds a 10 s budget; the starter's 300 ms does not end it (ended after {joiner_ended_ms} ms): {joiner:?} tap to_server (forwarded, held, dropped) = {:?}", tap.stats().to_server.snapshot());
}
