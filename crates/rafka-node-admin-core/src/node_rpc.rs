//! A process's one Node RPC client and the live resolver it dials through,
//! fed by the process's own membership.
//!
//! This is process composition: membership (`rafka-mesh-transport`) and Node
//! RPC (`rafka-node-rpc`) never depend on each other. Every process that
//! calls Node RPC (node-admin, the rpc node) builds one [`ProcessNodeRpc`] on
//! its endpoint when it starts, holds it in its running state and hands it to
//! every caller by clone; nothing builds a resolver or a client per call.

use rafka_mesh_entity::MeshDigest;
use rafka_mesh_transport::membership::DigestBook;
use rafka_node_rpc::{Applied, LiveNodeResolver, NodeRpcClient, ResolvedNode};
use std::sync::Arc;

/// The process's resolver and client, shared by clone.
#[derive(Clone)]
pub struct ProcessNodeRpc {
    /// The process's live resolver.
    pub resolver: Arc<LiveNodeResolver>,
    /// The process's client.
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
        Self::start_observed(resolver, endpoint, book, node, None)
    }

    /// The same, with this process's source-owned connections writer hearing every Direct fact
    /// the client observes about its own pooled connections.
    pub(crate) fn start_observed(
        resolver: Arc<LiveNodeResolver>,
        endpoint: iroh::Endpoint,
        book: &DigestBook,
        node: &str,
        observer: Option<Arc<dyn rafka_node_rpc::ConnectionObserver>>,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let me = Self::new(resolver, endpoint, observer);
        let feed = me.feed(book, node);
        (me, feed)
    }

    /// The process's one client on `endpoint`, dialling through `resolver`, before any membership
    /// exists: a node's first call (its `JoinNode`) goes through it, and [`Self::feed`] starts the
    /// resolver's membership feed once the process holds a book.
    pub fn new(resolver: Arc<LiveNodeResolver>, endpoint: iroh::Endpoint, observer: Option<Arc<dyn rafka_node_rpc::ConnectionObserver>>) -> Self {
        let mut client = NodeRpcClient::new(endpoint, resolver.clone()).with_caller_system("rdm");
        if let Some(o) = observer {
            client = client.with_connection_observer(o);
        }
        Self { resolver, client: Arc::new(client) }
    }

    /// Feed this process's resolver from `book` for as long as the returned task runs.
    pub fn feed(&self, book: &DigestBook, node: &str) -> tokio::task::JoinHandle<()> {
        spawn_feed(book.clone(), self.resolver.clone(), node.to_string())
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
        // A birth that departed with its dead mesh is Gone for as long as the mesh is dead.
        let applied = if op.operation.starts_with(rafka_mesh_transport::membership::MESH_SHUTDOWN_PREFIX) { resolver.depart_for_good(&op.node_id, &op.incarnation, &op.name) } else { resolver.depart(&op.node_id, &op.incarnation, &op.name) };
        if applied == Applied::Departed {
            tracing::info_span!("rdm.node_rpc.node.remove.via-membership", node, target = %op.name, target_id = %op.node_id, incarnation_id = %op.incarnation.0)
                .in_scope(|| tracing::info!("the resolver holds the departure: Gone"));
        }
    }
    for d in book.all() {
        let Some(birth) = resolved(&d) else { continue };
        let (target, target_id, incarnation) = (birth.name.to_string(), birth.node_id.to_string(), birth.incarnation.0.clone());
        match resolver.apply(birth, d.node.supersedes.as_ref()) {
            Applied::Unchanged | Applied::Departed => {}
            Applied::Refused(r) => tracing::info_span!("rdm.node_rpc.node.reject.via-membership", node, target = %target, target_id = %target_id, incarnation_id = %incarnation, reason = ?r)
                .in_scope(|| tracing::info!("a held birth the resolver does not take")),
            change => tracing::info_span!("rdm.node_rpc.node.update.via-membership", node, target = %target, target_id = %target_id, incarnation_id = %incarnation, change = ?change)
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
