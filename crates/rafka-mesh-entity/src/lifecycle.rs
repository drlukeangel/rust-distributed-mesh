//! Lifecycle events: what the Mesh executor that holds an operation publishes
//! around it, keyed by the operation it belongs to.
//!
//! One retirement runs many deployment steps and publishes one `NodeDeleting`
//! (after the executor's own journal holds its Claim for the attempt) and one
//! `NodeDeleted` (after the provider's exact inspection says the runtime is
//! terminal). Both carry the same [`LifecycleOp`]; a receiver applies a
//! repeated or doubly-sourced copy once, by its key.

use crate::ids::{IncarnationId, NodeId};
use crate::path::PathName;
use serde::{Deserialize, Serialize};

/// One lifecycle operation on one exact birth, as its events name it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleOp {
    pub build_id: String,
    pub attempt: u32,
    /// The operation's idempotency key (`retire-node:mesh2.rpc.2`), never a pipeline step.
    pub operation: String,
    pub node_id: NodeId,
    pub incarnation: IncarnationId,
    pub name: PathName,
    /// When the event happened, in Rafka-time; evidence only, never liveness, order or expiry.
    pub event_at_rafka_ms: u64,
}

impl LifecycleOp {
    /// A restart operation (`restart-node:<path>`): the logical node is kept, a later birth
    /// follows; every other operation here removes the node.
    pub fn is_restart(&self) -> bool {
        self.operation.starts_with("restart-node:")
    }

    /// The operation's identity: the same for its `NodeDeleting` and `NodeDeleted`.
    pub fn key(&self) -> (String, u32, String) {
        (self.build_id.clone(), self.attempt, self.operation.clone())
    }
}

/// How long a process holds a departure after it accepted it (its own local
/// age, never another machine's clock). It covers the membership repair
/// horizon: a process cut off for less than this still learns the departure
/// once it rejoins.
pub const DEPARTED_RETENTION: std::time::Duration = std::time::Duration::from_secs(10 * 60);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_op_round_trips_and_its_key_is_the_operation_not_a_step() {
        let op = LifecycleOp {
            build_id: "b1".into(),
            attempt: 2,
            operation: "retire-node:mesh2.rpc.2".into(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            name: "mesh2.rpc.2".parse().unwrap(),
            event_at_rafka_ms: 7,
        };
        let back: LifecycleOp = serde_json::from_str(&serde_json::to_string(&op).unwrap()).unwrap();
        assert_eq!(back, op);
        assert_eq!(op.key(), ("b1".to_string(), 2, "retire-node:mesh2.rpc.2".to_string()));
    }
}
