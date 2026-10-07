//! i143.e6.s11 functional: the client reports every Direct fact about its own pooled
//! connections to the source-owned connections writer (connections.md sections 9 and 10) and
//! decides nothing: a new pooled connection is `direct_connected` once, a reused one is not
//! reported, and a dial that ends with no connection is `direct_failed` with its reason.

mod common;
use common::*;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{Budget, CallOptions, ConnectionObserver, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Book {
    events: Mutex<Vec<(String, String, String)>>,
}
impl ConnectionObserver for Book {
    fn direct_connected(&self, node: &ResolvedNode) {
        self.events.lock().unwrap().push(("connected".into(), node.name.to_string(), String::new()));
    }
    fn direct_failed(&self, node: &ResolvedNode, reason: &str) {
        self.events.lock().unwrap().push(("failed".into(), node.name.to_string(), reason.into()));
    }
    fn direct_broken(&self, node: &ResolvedNode, reason: &str) {
        self.events.lock().unwrap().push(("broken".into(), node.name.to_string(), reason.into()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_observes_its_own_direct_connections_opening_and_failing_once_each() {
    let (bk, be) = bind().await;
    let b = node("mesh1.rpc.1", None, bk, be).await;
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(b.resolved.clone());
    // An unreachable birth: a key nobody serves, at a port nobody listens on.
    let nobody = ResolvedNode {
        node_id: NodeId::mint(),
        name: "mesh1.rpc.9".parse().unwrap(),
        endpoint_id: iroh::SecretKey::generate().public(),
        transport_addr: "127.0.0.1:1".parse().unwrap(),
        incarnation: IncarnationId::mint(),
    };
    resolver.insert(nobody.clone());
    let (_, oe) = bind().await;
    let book = Arc::new(Book::default());
    let origin = NodeRpcClient::new(oe, resolver).with_connection_observer(book.clone());
    let opts = CallOptions { budget: Budget::Overall(Duration::from_millis(1500)), ..Default::default() };
    let (out, ev) = origin.call::<Probe>(&NodeTarget::ExactNode(b.resolved.node_id.clone()), &probe(b"one"), &opts).await;
    assert!(matches!(out, RpcOutcome::Reply(_)), "{out:?}");
    assert!(!ev.unwrap().reused);
    let (out, ev) = origin.call::<Probe>(&NodeTarget::ExactNode(b.resolved.node_id.clone()), &probe(b"two"), &opts).await;
    assert!(matches!(out, RpcOutcome::Reply(_)), "{out:?}");
    assert!(ev.unwrap().reused, "the second call rode the pooled connection");
    let (out, _) = origin.call::<Probe>(&NodeTarget::ExactNode(nobody.node_id.clone()), &probe(b"none"), &opts).await;
    assert!(matches!(out, RpcOutcome::NotSent(_)), "{out:?}");
    let events = book.events.lock().unwrap().clone();
    assert_eq!(events.iter().filter(|e| e.0 == "connected").map(|e| e.1.as_str()).collect::<Vec<_>>(), vec!["mesh1.rpc.1"], "one open, reported once: {events:?}");
    let failed: Vec<&(String, String, String)> = events.iter().filter(|e| e.0 == "failed").collect();
    assert_eq!(failed.len(), 1, "{events:?}");
    assert_eq!(failed[0].1, "mesh1.rpc.9");
    assert!(!failed[0].2.is_empty(), "the failure carries its reason");
}
