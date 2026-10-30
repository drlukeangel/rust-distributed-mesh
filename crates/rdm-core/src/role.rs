//! Primary roles: who holds a primary role, as a node announces it and a joiner is told it.
//!
//! A primary role is a mesh's node-admin primary ([`PrimaryRole::MeshPrimary`]), the fabric primary
//! ([`PrimaryRole::FabricPrimary`]), or the node-type primary of one other `(mesh, node type)` ([`PrimaryRole::NodeTypePrimary`]:
//! an rpc, broker, gateway or compute type; the approved proposal
//! node-primary-update-fabric-mesh-and-node-type.md Part B1). A primary is an EXACT birth, `(node_id, incarnation)`, in
//! a mesh, with a `revision`: the number of times the role changed hands as the announcer saw it.
//! A role does not change hands while its primary lives, so the record a node holds is an input to
//! every election (`rafka-node-admin-core` `election.rs`), never a result recomputed from
//! statuses alone.

use crate::ids::{IncarnationId, NodeId};
use crate::path::NodeKind;
use serde::{Deserialize, Serialize};

/// A primary role. The discriminants are the wire positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PrimaryRole {
    /// The node-admin primary of one mesh (named by [`Primary::mesh`]).
    ///
    /// Position 0 on the wire.
    MeshPrimary,
    /// The fabric primary: a mesh primary that also holds the fabric's control.
    ///
    /// Position 1 on the wire.
    FabricPrimary,
    /// The node-type primary of the primary's mesh for this node type, every type but `node_admin`
    /// (whose role is [`PrimaryRole::MeshPrimary`]): its mesh's primary writes and announces its record
    /// (R-MT1 Q3: the mesh-primary is the mesh's one row writer and sole author of member status).
    ///
    /// Position 2 on the wire.
    NodeTypePrimary(NodeKind),
}

impl PrimaryRole {
    /// The role's name as it appears in spans and evidence.
    pub fn name(self) -> &'static str {
        match self {
            Self::MeshPrimary | Self::NodeTypePrimary(NodeKind::NodeAdmin) => "mesh-primary",
            Self::FabricPrimary => "fabric-primary",
            Self::NodeTypePrimary(NodeKind::RpcNode) => "rpc_node-primary",
            Self::NodeTypePrimary(NodeKind::Broker) => "broker-primary",
            Self::NodeTypePrimary(NodeKind::Gateway) => "gateway-primary",
            Self::NodeTypePrimary(NodeKind::Compute) => "compute-primary",
        }
    }

    /// The primary role of node type `kind` in a mesh: the mesh-primary role for `node_admin`, the
    /// node-type primary role for every other type.
    pub fn of_node_type(kind: NodeKind) -> Self {
        match kind {
            NodeKind::NodeAdmin => Self::MeshPrimary,
            other => Self::NodeTypePrimary(other),
        }
    }
}

/// A primary as it is recorded (`is_primary`, `is_fabric_primary` at its revision) and as the topology
/// answers it (node-rpc-envelope.md `Primary`): the exact birth, the mesh it belongs to and the revision
/// of the record. The election's result; the record does not grant the role.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Primary {
    /// The mesh the primary belongs to.
    pub mesh: String,
    /// The primary's node.
    pub node_id: NodeId,
    /// The primary's exact birth.
    pub incarnation: IncarnationId,
    /// The record's revision: a later revision supersedes an earlier one.
    pub revision: u64,
}

impl Primary {
    /// Whether this record replaces `held`: a later revision, or the same revision announced by a
    /// different birth with the lower NodeId (two births that took the same vacancy at once; the
    /// order is the one every election uses, so every observer settles on the same one). The same
    /// record again, or an older one, replaces nothing.
    pub fn supersedes(&self, held: &Primary) -> bool {
        if self.revision != held.revision {
            return self.revision > held.revision;
        }
        (self.node_id != held.node_id || self.incarnation != held.incarnation) && self.node_id < held.node_id
    }

    /// The same exact birth, whatever the revision.
    pub fn is_birth(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        &self.node_id == node_id && &self.incarnation == incarnation
    }
}

impl std::fmt::Display for Primary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}@{} (revision {})", self.mesh, self.node_id, self.incarnation, self.revision)
    }
}

/// An exact birth as op `0x21`'s entries name it.
pub mod transfer {
    use crate::ids::{IncarnationId, NodeId};
    use serde::{Deserialize, Serialize};

    /// An exact birth: its mesh, node and incarnation.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct HandoverBirth {
        /// The mesh the birth belongs to.
        pub mesh: String,
        /// The birth's node.
        pub node_id: NodeId,
        /// The birth's incarnation.
        pub incarnation: IncarnationId,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn primary(node: &str, inc: &str, revision: u64) -> Primary {
        Primary { mesh: "mesh1".into(), node_id: NodeId::parse(node).unwrap(), incarnation: IncarnationId::parse(&crate::ids::fixture_crockford60(inc)).unwrap(), revision }
    }

    #[test]
    fn a_later_revision_supersedes_and_the_same_record_or_an_older_one_does_not() {
        let held = primary("200000000000", "a", 3);
        assert!(primary("900000000000", "b", 4).supersedes(&held), "a later revision wins whatever the ids");
        assert!(!primary("100000000000", "b", 2).supersedes(&held), "an older revision never replaces");
        assert!(!held.clone().supersedes(&held), "the same record changes nothing");
    }

    #[test]
    fn two_births_that_took_one_vacancy_settle_on_the_lower_node_id() {
        let first = primary("200000000000", "a", 5);
        let rival = primary("100000000000", "b", 5);
        assert!(rival.supersedes(&first));
        assert!(!first.supersedes(&rival));
        // The same node id, another birth: the record already held stands.
        assert!(!primary("200000000000", "z", 5).supersedes(&first));
    }
}
