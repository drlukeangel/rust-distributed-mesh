//! A process's one Node RPC client and the live resolver it dials through,
//! fed by the process's own membership.
//!
//! This is process composition: membership (`rafka-mesh-transport`) and Node
//! RPC (`rafka-node-rpc`) never depend on each other. Every process that
//! calls Node RPC (node-admin, the rpc node) builds one [`ProcessNodeRpc`] on
//! its endpoint when it starts, holds it in its running state and hands it to
//! every caller by clone; nothing builds a resolver or a client per call.

use rdm_core::MeshDigest;
use rdm_transport::membership::DigestBook;
use rdm_transport::{Applied, LiveNodeResolver, NodeRpcClient, ResolvedNode};
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
        let me = Self::new(resolver, endpoint);
        let feed = me.feed(book, node);
        (me, feed)
    }

    /// The process's one client on `endpoint`, dialling through `resolver`, before any membership
    /// exists: a node's first call (its `JoinNode`) goes through it, and [`Self::feed`] starts the
    /// resolver's membership feed once the process holds a book. The client writes nothing to the
    /// connections log: Node RPC is control plane, no connection (R-CR5).
    pub fn new(resolver: Arc<LiveNodeResolver>, endpoint: iroh::Endpoint) -> Self {
        let client = NodeRpcClient::new(endpoint, resolver.clone()).with_caller_system("rdm");
        Self { resolver, client: Arc::new(client) }
    }

    /// Send as the process `delivery` is: its birth, its window, the open invocations its server
    /// shares (node-rpc.md §43).
    pub fn with_delivery(self, delivery: rdm_transport::Delivery) -> Self {
        let client = (*self.client).clone().with_delivery(delivery);
        Self { resolver: self.resolver, client: Arc::new(client) }
    }

    /// Feed this process's resolver from `book` for as long as the returned task runs.
    pub fn feed(&self, book: &DigestBook, node: &str) -> tokio::task::JoinHandle<()> {
        spawn_feed(book.clone(), self.resolver.clone(), node.to_string())
    }

    /// The same, on a client made earlier (a server that carries for others holds the
    /// process's one client before it is built).
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
    let held_holder = book.holder_name();
    let node = held_holder.as_deref().unwrap_or(node);
    for op in book.deleted() {
        let applied = resolver.depart(&op.node_id, &op.incarnation, &op.name);
        if applied == Applied::Departed {
            tracing::info_span!("rdm.node_rpc.node.remove.via-membership", node, target = %op.name, target_id = %op.node_id, incarnation_id = %op.incarnation.as_str())
                .in_scope(|| tracing::info!("the resolver holds the departure: Gone"));
        }
    }
    // A parked birth (its stop published): the book dropped it, and so does the path it held.
    let parked = book.stopped_digests().into_iter().map(|d| (d.node.node_id, d.node.incarnation, d.node.name)).chain(book.stopped_unheard().into_iter().map(|op| (op.node_id, op.incarnation, op.name)));
    for (node_id, incarnation, name) in parked {
        if resolver.stop(&node_id, &incarnation) {
            tracing::info_span!("rdm.node_rpc.node.update.via-membership", node, target = %name, target_id = %node_id, incarnation_id = %incarnation.as_str(), change = "Parked")
                .in_scope(|| tracing::info!("the resolver holds the parked birth by its exact id only: its path is given up"));
        }
    }
    for d in book.all().into_iter().chain(book.projected_unheard_at(std::time::Instant::now()).into_iter().map(|(d, _)| d)) {
        let Some(birth) = resolved(&d) else { continue };
        let (target, target_id, incarnation) = (birth.name.to_string(), birth.node_id.to_string(), birth.incarnation.to_string());
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
