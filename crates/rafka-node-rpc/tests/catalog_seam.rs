//! The one sealed effective catalog per process (ownership amendment §12): a product composes its
//! transitional adapters into the server's catalog, reads the sealed catalog back for its own
//! dispatcher, and on this server's ALPN an adapter's op is unserved (421), never dispatched.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_rpc::{CallOptions, Decode, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::{CatalogEntry, EntryKind, OpOwner};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use std::sync::Arc;

#[tokio::test]
async fn a_products_adapter_is_catalogued_readable_and_unserved_on_the_node_rpc_alpn() {
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let server = ServerBuilder::new()
        .serve::<Ping, _, _>(OpOwner::Core, |_peer, req: PingRequest| async move {
            let PingRequest::Ping { payload } = req;
            Ok(PingReply::Pong { payload })
        })
        .adapter(CatalogEntry::transitional(0x12, "data-frame", "rafka", "i142 U6", 64 * 1024))
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
        .unwrap();
    // The product's dispatcher reads the same sealed catalog.
    let held = server.catalog().lookup(0x12).expect("the adapter is catalogued");
    assert_eq!(held.owner, OpOwner::Product("rafka".into()));
    assert!(matches!(&held.kind, EntryKind::Transitional { migration_unit } if migration_unit == "i142 U6"));
    assert!(server.catalog().lookup(0x11).is_none(), "a retired op is never catalogued");
    assert!(server.catalog().lookup(0x01).is_some(), "core ping is served");

    let key = SecretKey::generate();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let stats = server.stats();
    let _router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(ResolvedNode { node_id: node_id.clone(), name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation });
    let cep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = NodeRpcClient::new(cep, resolver);
    let target = NodeTarget::ExactNode(node_id);

    // On the Node RPC ALPN the adapter's op has no handler: unserved, as the catalog's 421.
    let (out, _) = client
        .invoke_raw::<PingReply, _>(&target, 0x12, vec![1, 2, 3], 1024, &CallOptions::default(), |d| match d {
            Decode::Committed(c, b) => c.reply::<Ping>(b),
            Decode::Early(e, b) => e.reply::<Ping>(b),
        })
        .await;
    assert!(matches!(&out, RpcOutcome::Unserved(u) if u.op() == 0x12), "{out:?}");
    assert_eq!(rafka_node_rpc::ServerStats::get(&stats.dispatched), 0);
    // Core ping is dispatched.
    let (out, _) = client.call::<Ping>(&target, &PingRequest::Ping { payload: b"x".to_vec() }, &CallOptions::default()).await;
    assert!(out.reply().is_some(), "{out:?}");
}

/// A transitional entry a product tries to seal under the core owner is refused at seal, by name.
#[test]
fn a_core_owned_transitional_entry_is_refused_at_seal() {
    let mut entry = CatalogEntry::transitional(0x12, "data-frame", "rafka", "i142 U6", 1024);
    entry.owner = OpOwner::Core;
    let errors = ServerBuilder::new().adapter(entry).seal(ServedBirth { node_id: "n".into(), incarnation: "i".into() }).err().expect("refused");
    assert!(errors.iter().any(|e| matches!(e, rafka_node_rpc_contract::catalog::SealError::OwnerMismatch { op: 0x12, .. } | rafka_node_rpc_contract::catalog::SealError::CoreTransitional { op: 0x12, .. })), "{errors:?}");
}
