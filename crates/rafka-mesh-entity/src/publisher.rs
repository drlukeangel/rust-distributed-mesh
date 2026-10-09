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
        write!(f, "{}@{}", self.node, self.incarnation.0)
    }
}
