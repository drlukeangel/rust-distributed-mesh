//! Lifecycle events: what the Mesh executor that holds an operation publishes
//! around it, keyed by the operation it belongs to.
//!
//! One deletion runs many deployment steps and publishes one `NodeDeleting`
//! (after the executor's own journal holds its Claim for the attempt) and one
//! `NodeDeleted` (after the provider's exact inspection says the runtime is
//! terminal). Both carry the same [`LifecycleOp`]; a receiver applies a
//! repeated or doubly-sourced copy once, by its key.

use crate::ids::{FabricId, IncarnationId, NodeId};
use crate::path::PathName;
use serde::{Deserialize, Serialize};

/// One lifecycle operation on one exact birth, as its events name it.
///
/// Its equality is its fields but the trace context: two copies of one operation heard through
/// different carriers are the same operation (telemetry.md §2, trace context decides nothing).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleOp {
    /// The Build the operation belongs to.
    pub build_id: String,
    /// The attempt of the Build.
    pub attempt: u32,
    /// The operation's idempotency key (`node.delete:mesh2.rpc.2`), never a pipeline step.
    pub operation: String,
    /// The logical node.
    pub node_id: NodeId,
    /// The incarnation of the birth.
    pub incarnation: IncarnationId,
    /// The node's `path.name`.
    pub name: PathName,
    /// When the event happened, in Rafka-time; evidence only, never liveness or order. One expiry
    /// reads it: a standalone stop's receipted op is the age of the birth's stopped period, which
    /// the stopped-node garbage collector compares with `stopped_node_ttl` (R-GC1).
    pub event_at_rafka_ms: u64,
    /// The operation's W3C `traceparent`, its caller's context (telemetry.md §3, the deletion
    /// operation): taken from the envelope of the lifecycle frame that brought the operation (gossip.md
    /// §3.4), so an application span made from it is the operation's child. Observability only. Never
    /// written: it is no part of the frame's body or of a receipt, and an operation learned only from a
    /// `MeshTopology` carries none.
    #[serde(skip)]
    pub traceparent: Option<String>,
    /// The operation's W3C `tracestate`, beside [`Self::traceparent`] and as it is.
    #[serde(skip)]
    pub tracestate: Option<String>,
}

impl PartialEq for LifecycleOp {
    fn eq(&self, other: &Self) -> bool {
        let Self { build_id, attempt, operation, node_id, incarnation, name, event_at_rafka_ms, traceparent: _, tracestate: _ } = self;
        *build_id == other.build_id
            && *attempt == other.attempt
            && *operation == other.operation
            && *node_id == other.node_id
            && *incarnation == other.incarnation
            && *name == other.name
            && *event_at_rafka_ms == other.event_at_rafka_ms
    }
}

impl Eq for LifecycleOp {}

/// The fields the reserved gossip positions `Reserved15` to `Reserved18` keep (gossip.md, "Reserved
/// lifecycle frame positions"): the state hooks of the removed state-commit (Q171) and open-traffic
/// (Q174) checklists. None is sent; a receiver refuses each by name before it decodes the fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateHook {
    /// The fabric the checklist belongs to.
    pub fabric_id: FabricId,
    /// The subject: the commanded or reporting node.
    pub node_id: NodeId,
    /// The incarnation of that birth.
    pub incarnation: IncarnationId,
    /// The Build the checklist belongs to.
    pub build_id: String,
    /// The Build attempt that holds the checklist.
    pub attempt: u32,
    /// The operation.
    pub operation: String,
    /// The author at this hop, by `path.name`.
    pub publisher: String,
    /// When the event happened, in Rafka-time; evidence only.
    pub event_at_rafka_ms: u64,
}

impl LifecycleOp {
    /// A stop operation (`node.stop:<path>`): the birth is stopped and rejoins as itself; its
    /// departure is `NodeStopped`, never `NodeDeleted`.
    pub fn is_stop(&self) -> bool {
        self.operation.starts_with("node.stop:")
    }

    /// This operation carrying the trace context `traceparent` and `tracestate` (the carrier's).
    pub fn traced(self, traceparent: Option<String>, tracestate: Option<String>) -> Self {
        Self { traceparent, tracestate, ..self }
    }

    /// The operation's identity: the same for its `NodeDeleting` and `NodeDeleted`.
    pub fn key(&self) -> (String, u32, String) {
        (self.build_id.clone(), self.attempt, self.operation.clone())
    }
}

/// How long a process holds a departure after it accepted it (its own local
/// age, never another machine's clock). It covers the topology repair
/// horizon: a process cut off for less than this still learns the departure
/// once it rejoins.
pub const DELETED_RETENTION: std::time::Duration = std::time::Duration::from_secs(10 * 60);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_op_round_trips_and_its_key_is_the_operation_not_a_step() {
        let op = LifecycleOp {
            build_id: "b1".into(),
            attempt: 2,
            operation: "node.delete:mesh2.rpc.2".into(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            name: "mesh2.rpc.2".parse().unwrap(),
            event_at_rafka_ms: 7,
            traceparent: None,
            tracestate: None,
        };
        let back: LifecycleOp = serde_json::from_str(&serde_json::to_string(&op).unwrap()).unwrap();
        assert_eq!(back, op);
        assert_eq!(op.key(), ("b1".to_string(), 2, "node.delete:mesh2.rpc.2".to_string()));
    }
}
