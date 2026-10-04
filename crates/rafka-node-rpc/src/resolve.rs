//! Exact targets and the resolver contract (node-rpc.md §5–§6).
//!
//! The domain chooses one exact semantic target; the resolver answers where
//! it is now. It never chooses a role, a fallback or a retry.

use rafka_mesh_entity::{EndpointSlot, IncarnationId, NodeId, PathName};
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
    pub fabric_id: iroh::PublicKey,
    pub incarnation: IncarnationId,
    pub endpoints: Vec<EndpointSlot>,
}

pub trait NodeResolver: Send + Sync {
    fn resolve(&self, target: &NodeTarget) -> Result<ResolvedNode, ResolveFailure>;
}

/// A resolver over a fixed table (tests, probes fed from a node view).
#[derive(Debug, Default)]
pub struct StaticResolver {
    nodes: RwLock<HashMap<NodeId, ResolvedNode>>,
    gone: RwLock<Vec<NodeId>>,
}

impl StaticResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, n: ResolvedNode) {
        self.nodes.write().unwrap().insert(n.node_id.clone(), n);
    }

    pub fn remove_gone(&self, id: &NodeId) {
        self.nodes.write().unwrap().remove(id);
        self.gone.write().unwrap().push(id.clone());
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
}
