//! Seat holders: who holds a primary seat, as a node announces it and a joiner is told it.
//!
//! A seat is a mesh's node-admin primary ([`Seat::MeshPrimary`]), the fabric primary
//! ([`Seat::FabricPrimary`]), or the node-type primary of one other `(mesh, node type)` ([`Seat::NodeTypePrimary`]:
//! an rpc, broker, gateway or compute type; the approved proposal
//! node-primary-update-fabric-mesh-and-node-type.md Part B1). The holder of a seat is an EXACT birth, `(node_id, incarnation)`, in
//! a mesh, with a `revision`: the number of times the seat changed hands as the announcer saw it.
//! A seat does not change hands while its holder lives, so the record a node holds is an input to
//! every election (`rafka-node-admin-core` `election.rs`), never a result recomputed from
//! statuses alone.

use crate::ids::{IncarnationId, NodeId};
use crate::path::NodeKind;
use serde::{Deserialize, Serialize};

/// A primary seat. The discriminants are the wire positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Seat {
    /// The node-admin primary of one mesh (named by [`Primary::mesh`]).
    ///
    /// Position 0 on the wire.
    MeshPrimary,
    /// The fabric primary: a mesh primary that also holds the fabric's control.
    ///
    /// Position 1 on the wire.
    FabricPrimary,
    /// The node-type primary of the holder's mesh for this node type, every type but `node_admin`
    /// (whose seat is [`Seat::MeshPrimary`]): its mesh's primary writes and announces its record
    /// (R-MT1 Q3: the mesh-primary is the mesh's one row writer and sole author of member status).
    ///
    /// Position 2 on the wire.
    #[serde(rename = "Cohort")]
    NodeTypePrimary(NodeKind),
}

impl Seat {
    /// The seat's name as it appears in spans and evidence.
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

    /// The primary seat of node type `kind` in a mesh: the mesh-primary seat for `node_admin`, the
    /// node-type primary seat for every other type.
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

/// An exact birth as op `0x21`'s entries name it, and the seat names of the `seat-move/1`
/// rows an older store root holds, decoded and never written (durable-format rule).
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

    /// A seat as a `seat-move/1` row names it. The discriminants are its positions.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub enum Seat {
        /// The fabric primary.
        FabricPrimary,
        /// The node-admin primary of `mesh`.
        MeshPrimary {
            /// The mesh whose primary seat it is.
            mesh: String,
        },
        /// The node-type primary of `mesh` for node type `kind` (every type but `node_admin`).
        #[serde(rename = "Cohort")]
        NodeTypePrimary {
            /// The mesh whose node-type primary it is.
            mesh: String,
            /// The node type.
            kind: crate::path::NodeKind,
        },
    }

    impl Seat {
        /// The transfer's name for the record seat `seat` of `mesh`.
        pub fn of(seat: super::Seat, mesh: &str) -> Self {
            match seat {
                super::Seat::FabricPrimary => Self::FabricPrimary,
                super::Seat::MeshPrimary | super::Seat::NodeTypePrimary(crate::path::NodeKind::NodeAdmin) => Self::MeshPrimary { mesh: mesh.to_string() },
                super::Seat::NodeTypePrimary(kind) => Self::NodeTypePrimary { mesh: mesh.to_string(), kind },
            }
        }
        /// The record seat it names.
        pub fn kind(&self) -> super::Seat {
            match self {
                Self::FabricPrimary => super::Seat::FabricPrimary,
                Self::MeshPrimary { .. } => super::Seat::MeshPrimary,
                Self::NodeTypePrimary { kind, .. } => super::Seat::of_node_type(*kind),
            }
        }
        /// The seat's name as it appears in spans and evidence.
        pub fn name(&self) -> &'static str {
            self.kind().name()
        }
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
