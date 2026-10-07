//! Exact targets and the resolver contract (node-rpc.md §5–§6).
//!
//! The domain chooses one exact semantic target; the resolver answers where
//! it is now. It never chooses a role, a fallback or a retry.

use rafka_mesh_entity::{IncarnationId, NodeId, PathName};
use rafka_node_rpc_contract::outcome::ResolveFailure;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeTarget {
    /// Whoever holds this stable path at the dial cut.
    CurrentPath(PathName),
    /// Exactly this logical node; never follows a replacement.
    ExactNode(NodeId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedNode {
    pub node_id: NodeId,
    pub name: PathName,
    /// The node's Iroh public key.
    pub endpoint_id: iroh::PublicKey,
    /// The one address of the node's Iroh endpoint.
    pub transport_addr: std::net::SocketAddr,
    pub incarnation: IncarnationId,
}

pub trait NodeResolver: Send + Sync {
    fn resolve(&self, target: &NodeTarget) -> Result<ResolvedNode, ResolveFailure>;

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
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, n: ResolvedNode) {
        self.nodes.write().unwrap().insert(n.node_id.clone(), n);
        self.changed.send_modify(|v| *v += 1);
    }

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
        }
    }

    fn changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.changed.subscribe())
    }
}
