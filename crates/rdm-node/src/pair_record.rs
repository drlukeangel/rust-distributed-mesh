//! The record at a path and name (node-identity-and-state.md §2: "A current-pair lookup reads the node
//! records by their path and name; it never uses the last writer at a path"; Q109: storage persists,
//! and every reader folds the stored records the same way). A store answers the records standing at a
//! pair ([`crate::nodes_storage::NodesStorage::records_at`]); [`resolve`] is the one fold every reader of
//! a pair goes through.

use crate::model::PathName;
use crate::nodes_storage::NodeRecord;
use crate::storage_error::StorageError;

/// The reading process's view of the births it holds deleted (their `NodeDeleted` applied): set once,
/// by the process's start ([`read_deleted_from`]). Unset: none held.
static HELD_DELETED: std::sync::OnceLock<Box<dyn Fn(&rdm_core::NodeId) -> bool + Send + Sync>> = std::sync::OnceLock::new();

/// Read the births this process's view holds deleted from `deleted` (its digest view): a record whose
/// birth it holds deleted is no live record at its pair. The first call in a process stands.
pub fn read_deleted_from(deleted: impl Fn(&rdm_core::NodeId) -> bool + Send + Sync + 'static) {
    let _ = HELD_DELETED.set(Box::new(deleted));
}

/// Whether `row` holds its path and name: it is neither `Deleted` (a delete frees the pair,
/// node-delete.md) nor `Withdrawn`, and the reader's view does not hold its birth deleted (its
/// `NodeDeleted` applied): a returned birth's own line written after its delete is not live at its
/// pair (node-investigation.md: its re-created birth holds its path and name, and it is deleted; Q207).
pub fn holds_its_name(row: &NodeRecord) -> bool {
    !matches!(row.state, crate::model::NodeStatus::Deleted | crate::model::NodeStatus::Withdrawn) && !HELD_DELETED.get().is_some_and(|deleted| deleted(&row.node_id))
}

/// The one record of `records` that holds `name`: a record holds its name while [`holds_its_name`].
/// Two that hold it are refused by name, naming both, never answered as either (Q356: a deleted node
/// whose own later line reads live is this case, seen in its span).
pub fn resolve(records: Vec<NodeRecord>, name: &PathName) -> Result<Option<NodeRecord>, StorageError> {
    let mut holding = records.into_iter().filter(|r| r.location() == *name && holds_its_name(r));
    let first = holding.next();
    if let (Some(a), Some(b)) = (&first, holding.next()) {
        // One span per refusal, naming the pair and both node ids: the readers that fail on it name it
        // only inside their own reasons.
        tracing::info_span!("rdm.node.record.reject.via-two-live-records", node_path = %name.path, node_name = %name.name, node_id = %a.node_id, other_node_id = %b.node_id, node_state = ?a.state, other_state = ?b.state, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("two live records hold the path and name: the read is refused, naming both"));
        return Err(StorageError::Refused {
            file: format!("the node records at path {:?}, name {:?}", name.path, name.name),
            reason: format!("two live records hold it, nodes {} and {}", a.node_id, b.node_id),
        });
    }
    Ok(first)
}
