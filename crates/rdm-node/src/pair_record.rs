//! The record at a path and name (node-identity-and-state.md §2: "A current-pair lookup reads the node
//! records by their path and name; it never uses the last writer at a path"; Q109: storage persists,
//! and every reader folds the stored records the same way). A store answers the records standing at a
//! pair ([`crate::nodes_storage::NodesStorage::records_at`]); [`resolve`] is the one fold every reader of
//! a pair goes through.

use crate::model::PathName;
use crate::nodes_storage::NodeRecord;
use crate::storage_error::StorageError;

/// Whether `row` holds its path and name: it is neither `Deleted` (a delete frees the pair,
/// node-delete.md) nor `Withdrawn`.
pub fn holds_its_name(row: &NodeRecord) -> bool {
    !matches!(row.state, crate::model::NodeStatus::Deleted | crate::model::NodeStatus::Withdrawn)
}

/// The one record of `records` that holds `name`: a record holds its name while [`holds_its_name`].
/// Two that hold it are refused by name, naming both, never answered as either (Q356: a deleted node
/// whose own later line reads live is this case, seen in its span).
pub fn resolve(records: Vec<NodeRecord>, name: &PathName) -> Result<Option<NodeRecord>, StorageError> {
    let mut holding = records.into_iter().filter(|r| r.location() == *name && holds_its_name(r));
    let first = holding.next();
    if let (Some(a), Some(b)) = (&first, holding.next()) {
        return Err(StorageError::Refused {
            file: format!("the node records at path {:?}, name {:?}", name.path, name.name),
            reason: format!("two live records hold it, nodes {} and {}", a.node_id, b.node_id),
        });
    }
    Ok(first)
}
