//! The identity a versioned topology belongs to.

use crate::ids::{IncarnationId, NodeId};
use serde::{Deserialize, Serialize};

/// The author of a versioned topology: its descriptive name and exact node/incarnation birth.
/// The author supplies this identity; receipt, read installation and forwarding retain it even
/// when the publication has no members. A name is never used to reconstruct the author's birth.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PublisherId {
    /// The publisher's node name.
    pub node: String,
    /// The incarnation of the birth that holds the seat.
    pub incarnation: IncarnationId,
    /// The author's logical node, supplied at publication, never resolved from its name.
    pub node_id: NodeId,
}

impl From<&crate::MeshNode> for PublisherId {
    fn from(author: &crate::MeshNode) -> Self {
        Self { node: author.name.to_string(), incarnation: author.incarnation.clone(), node_id: author.node_id.clone() }
    }
}

impl std::fmt::Display for PublisherId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}@{}", self.node, self.node_id, self.incarnation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeId;

    /// The author is a birth, even when a reused path and incarnation text coincide. The node id
    /// is supplied by that author, never reconstructed from the path or a receiver's held members.
    #[test]
    fn distinct_author_nodes_are_distinct_publishers() {
        let incarnation = IncarnationId::mint();
        let author = |node_id: NodeId| serde_json::from_value::<PublisherId>(serde_json::json!({
            "node": "mesh1.admin.1", "node_id": node_id, "incarnation": incarnation,
        })).unwrap();
        let first = author(NodeId::mint());
        let second = author(NodeId::mint());
        assert_ne!(first, second, "publisher identity must retain the supplied author's node id");
    }
}
