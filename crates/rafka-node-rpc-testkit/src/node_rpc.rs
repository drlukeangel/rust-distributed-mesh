//! The rpc node's one Node RPC client and the live resolver it dials through,
//! fed by the process's own membership.
//!
//! This is process composition: membership (`rafka-mesh-transport`) and Node
//! RPC (`rafka-node-rpc`) never depend on each other. Every process that
//! calls Node RPC (node-admin, the rpc node) builds one [`ProcessNodeRpc`] on
//! its endpoint when it starts, holds it in its running state and hands it to
//! every caller by clone; nothing builds a resolver or a client per call.
//! Node-admin composes the same mapping in `rafka_node_admin_core::node_rpc`;
//! the rpc node never links node-admin.

use rafka_mesh_entity::MeshDigest;
use rafka_mesh_transport::membership::DigestBook;
use rafka_node_rpc::{Applied, LiveNodeResolver, NodeRpcClient, ResolvedNode};
use std::sync::Arc;

/// The process's resolver and client, shared by clone.
#[derive(Clone)]
pub struct ProcessNodeRpc {
    pub resolver: Arc<LiveNodeResolver>,
    pub client: Arc<NodeRpcClient>,
}

impl ProcessNodeRpc {
    /// One resolver and one client on `endpoint`, fed from `book` for as long
    /// as the returned task runs.
    pub fn start(endpoint: iroh::Endpoint, book: &DigestBook, node: &str) -> (Self, tokio::task::JoinHandle<()>) {
        Self::start_with(Arc::new(LiveNodeResolver::default()), endpoint, book, node)
    }

    /// The same, on a resolver made earlier (a server handler may hold it
    /// before the endpoint is bound).
    pub fn start_with(resolver: Arc<LiveNodeResolver>, endpoint: iroh::Endpoint, book: &DigestBook, node: &str) -> (Self, tokio::task::JoinHandle<()>) {
        let client = Arc::new(NodeRpcClient::new(endpoint, resolver.clone()).with_caller_system("rdm"));
        Self::with_client(resolver, client, book, node)
    }

    /// The same, on a client made earlier (a server that carries for others holds the
    /// process's one client before it seals).
    pub fn with_client(resolver: Arc<LiveNodeResolver>, client: Arc<NodeRpcClient>, book: &DigestBook, node: &str) -> (Self, tokio::task::JoinHandle<()>) {
        let feed = spawn_feed(book.clone(), resolver.clone(), node.to_string());
        (Self { resolver, client }, feed)
    }
}

/// The resolver's view of a held birth; `None` when its transport id is not an
/// Iroh key (it cannot be dialed).
pub fn resolved(d: &MeshDigest) -> Option<ResolvedNode> {
    Some(ResolvedNode {
        node_id: d.node.node_id.clone(),
        name: d.node.name.clone(),
        endpoint_id: d.node.endpoint_id.0.parse().ok()?,
        transport_addr: d.node.transport_addr,
        incarnation: d.node.incarnation.clone(),
    })
}

/// Apply every departure and every birth `book` holds to `resolver`, naming what changed.
pub fn feed_once(book: &DigestBook, resolver: &LiveNodeResolver, node: &str) {
    for op in book.departed() {
        if resolver.depart(&op.node_id, &op.incarnation, &op.name) == Applied::Departed {
            tracing::info_span!("rafka.node_rpc.node.remove.via-membership", node, target = %op.name, target_id = %op.node_id, incarnation_id = %op.incarnation.0)
                .in_scope(|| tracing::info!("the resolver holds the departure: Gone"));
        }
    }
    for d in book.all() {
        let Some(birth) = resolved(&d) else { continue };
        let (target, target_id, incarnation) = (birth.name.to_string(), birth.node_id.to_string(), birth.incarnation.0.clone());
        match resolver.apply(birth, d.node.supersedes.as_ref()) {
            Applied::Unchanged | Applied::Departed => {}
            Applied::Refused(r) => tracing::info_span!("rafka.node_rpc.node.reject.via-membership", node, target = %target, target_id = %target_id, incarnation_id = %incarnation, reason = ?r)
                .in_scope(|| tracing::info!("a held birth the resolver does not take")),
            change => tracing::info_span!("rafka.node_rpc.node.update.via-membership", node, target = %target, target_id = %target_id, incarnation_id = %incarnation, change = ?change)
                .in_scope(|| tracing::info!("the resolver holds the birth")),
        }
    }
}

/// Feed `resolver` now and again on every birth change `book` holds.
fn spawn_feed(book: DigestBook, resolver: Arc<LiveNodeResolver>, node: String) -> tokio::task::JoinHandle<()> {
    let mut births = book.birth_changes();
    tokio::spawn(async move {
        loop {
            births.borrow_and_update();
            feed_once(&book, &resolver, &node);
            if births.changed().await.is_err() {
                return;
            }
        }
    })
}
