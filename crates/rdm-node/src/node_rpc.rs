//! A process's one Node RPC client and the live resolver it dials through,
//! fed by the process's own topology view.
//!
//! This is process composition: the topology view (`rafka-mesh-transport`) and Node
//! RPC (`rafka-node-rpc`) never depend on each other. Every process that
//! calls Node RPC (node-admin, the rpc node) builds one [`ProcessNodeRpc`] on
//! its endpoint when it starts, holds it in its running state and hands it to
//! every caller by clone; nothing builds a resolver or a client per call.

use rdm_core::MeshDigest;
use rdm_transport::topology::DigestView;
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
    /// One resolver and one client on `endpoint`, fed from `digest_view` for as long
    /// as the returned task runs.
    pub fn start(endpoint: iroh::Endpoint, digest_view: &DigestView, node: &str) -> (Self, tokio::task::JoinHandle<()>) {
        Self::start_with(Arc::new(LiveNodeResolver::default()), endpoint, digest_view, node)
    }

    /// The same, on a resolver made earlier (a server handler may hold it
    /// before the endpoint is bound).
    pub fn start_with(resolver: Arc<LiveNodeResolver>, endpoint: iroh::Endpoint, digest_view: &DigestView, node: &str) -> (Self, tokio::task::JoinHandle<()>) {
        let me = Self::new(resolver, endpoint);
        let feed = me.feed(digest_view, node);
        (me, feed)
    }

    /// The process's one client on `endpoint`, dialling through `resolver`, before any topology view
    /// exists: a node's first call (its `JoinNode`) goes through it, and [`Self::feed`] starts the
    /// resolver's topology feed once the process holds a view. The client writes nothing to the
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

    /// Feed this process's resolver from `digest_view` for as long as the returned task runs.
    pub fn feed(&self, digest_view: &DigestView, node: &str) -> tokio::task::JoinHandle<()> {
        spawn_feed(digest_view.clone(), self.resolver.clone(), node.to_string())
    }

    /// The same, on a client made earlier (a server that carries for others holds the
    /// process's one client before it is built).
    pub fn with_client(resolver: Arc<LiveNodeResolver>, client: Arc<NodeRpcClient>, digest_view: &DigestView, node: &str) -> (Self, tokio::task::JoinHandle<()>) {
        let feed = spawn_feed(digest_view.clone(), resolver.clone(), node.to_string());
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

/// Apply every departure and every birth `digest_view` holds to `resolver`, naming what changed.
pub fn feed_once(digest_view: &DigestView, resolver: &LiveNodeResolver, node: &str) {
    let held_holder = digest_view.observer_name();
    let node = held_holder.as_deref().unwrap_or(node);
    for op in digest_view.deleted() {
        let applied = resolver.depart(&op.node_id, &op.incarnation, &op.name);
        if applied == Applied::Deleted {
            tracing::info_span!("rdm.node_rpc.node.remove.via-topology", node, target = %op.name, target_id = %op.node_id, incarnation_id = %op.incarnation.as_str())
                .in_scope(|| tracing::info!("the resolver holds the departure: Gone"));
        }
    }
    // A stopped birth (its stop published): the view dropped it, and so does the path it held.
    let stopped = digest_view.stopped_digests().into_iter().map(|d| (d.node.node_id, d.node.incarnation, d.node.name)).chain(digest_view.stopped_unheard().into_iter().map(|op| (op.node_id, op.incarnation, op.name)));
    for (node_id, incarnation, name) in stopped {
        if resolver.stop(&node_id, &incarnation) {
            tracing::info_span!("rdm.node_rpc.node.update.via-topology", node, target = %name, target_id = %node_id, incarnation_id = %incarnation.as_str(), change = "Stopped")
                .in_scope(|| tracing::info!("the resolver holds the stopped birth by its exact id only: its path is given up"));
        }
    }
    for d in digest_view.all().into_iter().chain(digest_view.projected_unheard_at(std::time::Instant::now()).into_iter().map(|(d, _)| d)) {
        let Some(birth) = resolved(&d) else { continue };
        let (target, target_id, incarnation) = (birth.name.to_string(), birth.node_id.to_string(), birth.incarnation.to_string());
        match resolver.apply(birth, d.node.supersedes.as_ref()) {
            Applied::Unchanged | Applied::Deleted => {}
            Applied::Refused(r) => tracing::info_span!("rdm.node_rpc.node.reject.via-topology", node, target = %target, target_id = %target_id, incarnation_id = %incarnation, reason = ?r)
                .in_scope(|| tracing::info!("a held birth the resolver does not take")),
            change => tracing::info_span!("rdm.node_rpc.node.update.via-topology", node, target = %target, target_id = %target_id, incarnation_id = %incarnation, change = ?change)
                .in_scope(|| tracing::info!("the resolver holds the birth")),
        }
    }
}

/// Feed `resolver` now and again on every birth change `digest_view` holds.
fn spawn_feed(digest_view: DigestView, resolver: Arc<LiveNodeResolver>, node: String) -> tokio::task::JoinHandle<()> {
    let mut births = digest_view.birth_changes();
    tokio::spawn(async move {
        loop {
            births.borrow_and_update();
            feed_once(&digest_view, &resolver, &node);
            if births.changed().await.is_err() {
                return;
            }
        }
    })
}
