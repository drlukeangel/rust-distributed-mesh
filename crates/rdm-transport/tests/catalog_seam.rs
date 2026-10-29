//! The one built effective catalog per process (ownership amendment §12): a product composes its
//! transitional adapters into the server's catalog, reads the built catalog back for its own
//! dispatcher, and on this server's ALPN an adapter's op is unserved (421), never dispatched.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::{IncarnationId, NodeId};
use rdm_transport::{CallOptions, Decode, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rdm_core::catalog::{CatalogEntry, EntryKind, OpOwner};
use rdm_core::outcome::RpcOutcome;
use rdm_core::ping::{Ping, PingReply, PingRequest};
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
        .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .unwrap();
    // The product's dispatcher reads the same built catalog.
    let held = server.catalog().lookup(0x12).expect("the adapter is catalogued");
    assert_eq!(held.owner, OpOwner::Product("rafka".into()));
    assert!(matches!(&held.kind, EntryKind::Transitional { migration_unit } if migration_unit == "i142 U6"));
    assert!(server.catalog().lookup(0x11).is_none(), "a reserved op is never catalogued");
    assert!(server.catalog().lookup(0x01).is_some(), "core ping is served");

    let key = SecretKey::generate();
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let stats = server.stats();
    let _router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(ResolvedNode { node_id: node_id.clone(), name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation });
    let cep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
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
    assert_eq!(rdm_transport::ServerStats::get(&stats.dispatched), 0);
    // Core ping is dispatched.
    let (out, _) = client.call::<Ping>(&target, &PingRequest::Ping { payload: b"x".to_vec() }, &CallOptions::default()).await;
    assert!(out.reply().is_some(), "{out:?}");
}

/// A transitional entry a product tries to build under the core owner is refused at build, by name.
#[test]
fn a_core_owned_transitional_entry_is_refused_at_build() {
    let mut entry = CatalogEntry::transitional(0x12, "data-frame", "rafka", "i142 U6", 1024);
    entry.owner = OpOwner::Core;
    let errors = ServerBuilder::new().adapter(entry).build(ServedBirth { node_id: "n".into(), incarnation: "i".into() }).err().expect("refused");
    assert!(errors.iter().any(|e| matches!(e, rdm_core::catalog::CatalogError::OwnerMismatch { op: 0x12, .. } | rdm_core::catalog::CatalogError::CoreTransitional { op: 0x12, .. })), "{errors:?}");
}
