//! Exact targets and the resolver contract (node-rpc.md §5–§6).
//!
//! The domain chooses one exact semantic target; the resolver answers where
//! it is now. It never chooses a role, a fallback or a retry.

use rdm_core::{IncarnationId, NodeId, PathName};
use rdm_core::outcome::ResolveFailure;
use std::collections::HashMap;
use std::sync::RwLock;

/// The exact node a call is for. The domain chooses it; the resolver never chooses a role, fallback
/// or retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeTarget {
    /// Whoever holds this stable path at the dial cut.
    CurrentPath(PathName),
    /// Exactly this logical node; never follows a replacement.
    ExactNode(NodeId),
    /// The process at this endpoint key, reached at this address: the endpoint and path of a call
    /// that reached this node, the way a stopped node told to start joins back to the admin that
    /// commanded it (node-start.md step 2). The caller holds no record of that process: no resolver
    /// answers for it and nothing supersedes it, and the request's target names no node (no receiver
    /// refuses a call for the node id it names, R-UC1a).
    Endpoint {
        /// The process's Iroh public key, as the transport proved it for the call.
        endpoint_id: iroh::PublicKey,
        /// The address the call came in on.
        addr: std::net::SocketAddr,
    },
}

/// Where a node is now: its identity, its endpoint and its current birth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedNode {
    /// The node's minted id.
    pub node_id: NodeId,
    /// The node's `path.name`.
    pub name: PathName,
    /// The node's Iroh public key.
    pub endpoint_id: iroh::PublicKey,
    /// The one address of the node's Iroh endpoint.
    pub transport_addr: std::net::SocketAddr,
    /// The incarnation of the node's current birth.
    pub incarnation: IncarnationId,
}

/// Answers where an exact target is now.
pub trait NodeResolver: Send + Sync {
    /// The node `target` names now, or the named reason it cannot be reached.
    fn resolve(&self, target: &NodeTarget) -> Result<ResolvedNode, ResolveFailure>;

    /// The birth this resolver names at `endpoint`, if it names one: what a connection to the process
    /// at that endpoint is recorded under (connections.md §10). Never consulted to decide a call.
    fn by_endpoint(&self, endpoint: &iroh::PublicKey) -> Option<ResolvedNode> {
        let _ = endpoint;
        None
    }

    /// Every live node this resolver holds now, as the topology it is fed holds them: what a source
    /// takes a Proxy's carrier from (connections.md §6, Q227). A node stopped, exited or deleted is
    /// none of them. Empty for a resolver that holds no topology.
    fn live_nodes(&self) -> Vec<ResolvedNode> {
        Vec::new()
    }

    /// Ticks whenever an answer may have changed: a dial in flight re-resolves
    /// on each tick and is cancelled the moment its exact target moved. `None`:
    /// changes are seen on the next call only.
    fn changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        None
    }
}

/// A resolver over a fixed table (tests, probes fed from a node view).
#[derive(Debug)]
pub struct StaticResolver {
    nodes: RwLock<HashMap<NodeId, ResolvedNode>>,
    gone: RwLock<Vec<NodeId>>,
    changed: tokio::sync::watch::Sender<u64>,
}

impl Default for StaticResolver {
    fn default() -> Self {
        Self { nodes: RwLock::default(), gone: RwLock::default(), changed: tokio::sync::watch::Sender::new(0) }
    }
}

impl StaticResolver {
    /// An empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or replace the node `n`, keyed by its id.
    pub fn insert(&self, n: ResolvedNode) {
        self.nodes.write().unwrap().insert(n.node_id.clone(), n);
        self.changed.send_modify(|v| *v += 1);
    }

    /// Remove the node `id` and answer `Gone` for it from now on.
    pub fn remove_gone(&self, id: &NodeId) {
        self.nodes.write().unwrap().remove(id);
        self.gone.write().unwrap().push(id.clone());
        self.changed.send_modify(|v| *v += 1);
    }
}

impl NodeResolver for StaticResolver {
    fn resolve(&self, target: &NodeTarget) -> Result<ResolvedNode, ResolveFailure> {
        let nodes = self.nodes.read().unwrap();
        match target {
            NodeTarget::ExactNode(id) => nodes.get(id).cloned().ok_or_else(|| {
                if self.gone.read().unwrap().contains(id) {
                    ResolveFailure::Gone
                } else {
                    ResolveFailure::Unknown
                }
            }),
            NodeTarget::CurrentPath(p) => nodes.values().find(|n| &n.name == p).cloned().ok_or(ResolveFailure::Unknown),
            // An endpoint target names a process no record holds: the client dials it as named.
            NodeTarget::Endpoint { .. } => Err(ResolveFailure::Unknown),
        }
    }

    fn by_endpoint(&self, endpoint: &iroh::PublicKey) -> Option<ResolvedNode> {
        self.nodes.read().unwrap().values().find(|n| &n.endpoint_id == endpoint).cloned()
    }

    fn live_nodes(&self) -> Vec<ResolvedNode> {
        self.nodes.read().unwrap().values().cloned().collect()
    }

    fn changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.changed.subscribe())
    }
}
