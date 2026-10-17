//! The identity a versioned topology belongs to.

use crate::ids::IncarnationId;
use serde::{Deserialize, Serialize};

/// A publisher's EXACT identity: its path name AND the birth that holds the seat. It is the epoch
/// of the versions it publishes (gossip.md §3.1): a restarted primary is a new publisher, so its
/// versions start again without ever being compared with its previous birth's.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PublisherId {
    /// The publisher's node name.
    pub node: String,
    /// The incarnation of the birth that holds the seat.
    pub incarnation: IncarnationId,
}

impl std::fmt::Display for PublisherId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.node, self.incarnation)
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
