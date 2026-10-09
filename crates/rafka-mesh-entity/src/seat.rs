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
        write!(f, "{}:{}@{} (epoch {})", self.mesh, self.node_id, self.incarnation.0, self.epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holder(node: &str, inc: &str, epoch: u64) -> SeatHolder {
        SeatHolder { mesh: "mesh1".into(), node_id: NodeId::parse(node).unwrap(), incarnation: IncarnationId(inc.into()), epoch }
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
