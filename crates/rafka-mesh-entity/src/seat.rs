//! Seat holders: who holds a primary seat, as a node announces it and a joiner is told it.
//!
//! A seat is a mesh's node-admin primary ([`Seat::MeshPrimary`]) or the fabric primary
//! ([`Seat::FabricPrimary`]). The holder of a seat is an EXACT birth, `(node_id, incarnation)`, in
//! a mesh, with an `epoch`: the number of times the seat changed hands as the announcer saw it.
//! A seat does not change hands while its holder lives, so the record a node holds is an input to
//! every election (`rafka-node-admin-core` `election.rs`), never a result recomputed from
//! statuses alone.

use crate::ids::{IncarnationId, NodeId};
use serde::{Deserialize, Serialize};

/// A primary seat. The discriminants are the wire positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Seat {
    /// The node-admin primary of one mesh (named by [`SeatHolder::mesh`]).
    MeshPrimary,
    /// The fabric primary: a mesh primary that also holds the fabric's control.
    FabricPrimary,
}

impl Seat {
    /// The seat's name as it appears in spans and evidence.
    pub fn name(self) -> &'static str {
        match self {
            Self::MeshPrimary => "mesh-primary",
            Self::FabricPrimary => "fabric-primary",
        }
    }
}

/// The holder of a seat: the exact birth, the mesh it belongs to, and the epoch of the record.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SeatHolder {
    /// The mesh the holder belongs to.
    pub mesh: String,
    /// The holder's node.
    pub node_id: NodeId,
    /// The holder's exact birth.
    pub incarnation: IncarnationId,
    /// The record's epoch: a later epoch supersedes an earlier one.
    pub epoch: u64,
}

impl SeatHolder {
    /// Whether this record replaces `held`: a later epoch, or the same epoch announced by a
    /// different birth with the lower NodeId (two births that took the same vacancy at once; the
    /// order is the one every election uses, so every observer settles on the same one). The same
    /// record again, or an older one, replaces nothing.
    pub fn supersedes(&self, held: &SeatHolder) -> bool {
        if self.epoch != held.epoch {
            return self.epoch > held.epoch;
        }
        (self.node_id != held.node_id || self.incarnation != held.incarnation) && self.node_id < held.node_id
    }

    /// The same exact birth, whatever the epoch.
    pub fn is_birth(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        &self.node_id == node_id && &self.incarnation == incarnation
    }
}

impl std::fmt::Display for SeatHolder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}@{} (epoch {})", self.mesh, self.node_id, self.incarnation, self.epoch)
    }
}

/// The seat update's own wire types (the approved proposal node-primary-update-fabric-mesh-and-node-type.md,
/// Part C3 edits 5 and 6: node.primary.update on op `0x21`, and the "primary updating" and "primary
/// updated" gossip frames): the shapes the 0x21 family and the `PrimaryUpdating` / `PrimaryUpdated`
/// frames carry, in the doc's field order and enum positions. They convert to and from the seat records ([`super::Seat`], [`SeatHolder`]) at the
/// edges; nothing else reads them.
pub mod transfer {
    use super::SeatHolder;
    use crate::ids::{IncarnationId, NodeId};
    use serde::{Deserialize, Serialize};

    /// An exact birth named by a handover or a seat transfer: its mesh, node and incarnation.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct HandoverBirth {
        /// The mesh the birth belongs to.
        pub mesh: String,
        /// The birth's node.
        pub node_id: NodeId,
        /// The birth's incarnation.
        pub incarnation: IncarnationId,
    }

    /// A seat as a transfer names it. The discriminants are the wire positions.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub enum Seat {
        /// The fabric primary.
        FabricPrimary,
        /// The node-admin primary of `mesh`.
        MeshPrimary {
            /// The mesh whose primary seat it is.
            mesh: String,
        },
    }

    impl Seat {
        /// The transfer's name for the record seat `seat` of `mesh`.
        pub fn of(seat: super::Seat, mesh: &str) -> Self {
            match seat {
                super::Seat::FabricPrimary => Self::FabricPrimary,
                super::Seat::MeshPrimary => Self::MeshPrimary { mesh: mesh.to_string() },
            }
        }
        /// The record seat it names.
        pub fn kind(&self) -> super::Seat {
            match self {
                Self::FabricPrimary => super::Seat::FabricPrimary,
                Self::MeshPrimary { .. } => super::Seat::MeshPrimary,
            }
        }
        /// The seat's name as it appears in spans and evidence.
        pub fn name(&self) -> &'static str {
            self.kind().name()
        }
    }

    /// One seat a transfer committed: the seat, its committed holder's exact birth and the epoch.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct CommittedSeat {
        /// The seat that moved.
        pub seat: Seat,
        /// Its committed holder.
        pub holder: HandoverBirth,
        /// The epoch the transfer committed.
        pub epoch: u64,
    }

    impl CommittedSeat {
        /// The committed seat of the record `holder` holds for `seat`.
        pub fn of(seat: super::Seat, holder: &SeatHolder) -> Self {
            Self { seat: Seat::of(seat, &holder.mesh), holder: HandoverBirth { mesh: holder.mesh.clone(), node_id: holder.node_id.clone(), incarnation: holder.incarnation.clone() }, epoch: holder.epoch }
        }
        /// The seat record it commits.
        pub fn record(&self) -> SeatHolder {
            SeatHolder { mesh: self.holder.mesh.clone(), node_id: self.holder.node_id.clone(), incarnation: self.holder.incarnation.clone(), epoch: self.epoch }
        }
        /// The mesh the record is keyed by: the seat's mesh for a mesh primary, none for the fabric's.
        pub fn mesh_key(&self) -> Option<&str> {
            match &self.seat {
                Seat::MeshPrimary { mesh } => Some(mesh),
                Seat::FabricPrimary => None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holder(node: &str, inc: &str, epoch: u64) -> SeatHolder {
        SeatHolder { mesh: "mesh1".into(), node_id: NodeId::parse(node).unwrap(), incarnation: IncarnationId::parse(&crate::ids::fixture_crockford60(inc)).unwrap(), epoch }
    }

    #[test]
    fn a_later_epoch_supersedes_and_the_same_record_or_an_older_one_does_not() {
        let held = holder("200000000000", "a", 3);
        assert!(holder("900000000000", "b", 4).supersedes(&held), "a later epoch wins whatever the ids");
        assert!(!holder("100000000000", "b", 2).supersedes(&held), "an older epoch never replaces");
        assert!(!held.clone().supersedes(&held), "the same record changes nothing");
    }

    #[test]
    fn two_births_that_took_one_vacancy_settle_on_the_lower_node_id() {
        let first = holder("200000000000", "a", 5);
        let rival = holder("100000000000", "b", 5);
        assert!(rival.supersedes(&first));
        assert!(!first.supersedes(&rival));
        // The same node id, another birth: the record already held stands.
        assert!(!holder("200000000000", "z", 5).supersedes(&first));
    }
}
