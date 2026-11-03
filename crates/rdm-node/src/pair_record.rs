//! The record at a path and name (node-identity-and-state.md §2: "A current-pair lookup reads the node
//! records by their path and name; it never uses the last writer at a path"; Q109: storage persists,
//! and every reader folds the stored rows the same way). A store answers the records standing at a pair
//! ([`crate::nodes_storage::NodesStorage::records_at`]); [`resolve`] is the one fold every reader of a
//! pair goes through.

use crate::model::{NodeId, PathName};
use crate::nodes_storage::{BirthRef, NodeRow};
use crate::storage_error::StorageError;
use std::collections::HashMap;

/// Whether `row` holds its path and name: it is neither `Deleted` (a delete frees the pair,
/// node-delete.md) nor `Withdrawn` (a create withdrawn before its node ran).
pub fn holds_its_name(row: &NodeRow) -> bool {
    !matches!(row.status, crate::model::NodeStatus::Deleted | crate::model::NodeStatus::Withdrawn)
}

/// The births re-created at their path and name among `records`: each predecessor a record names, with
/// the node that names it (the re-created birth).
pub fn re_created(records: &[NodeRow]) -> HashMap<BirthRef, NodeId> {
    records.iter().filter_map(|r| r.predecessor.clone().map(|p| (p, r.node_id.clone()))).collect()
}

/// The node that re-created `row`'s node, when `row`'s version was written by its own birth and a record
/// of `re_created` names that birth its predecessor (Q109, build.md: a deleted birth's writes every reader
/// refuses): that version is not the node's record.
pub fn superseded<'a>(row: &NodeRow, re_created: &'a HashMap<BirthRef, NodeId>) -> Option<&'a NodeId> {
    row.writer.as_ref().filter(|w| w.node_id == row.node_id).and_then(|w| re_created.get(w))
}

/// The births another record at the pair names its predecessor, by its re-creation (`predecessor`) or by
/// its create's admitted provenance (`allocated.predecessor`, a recovery's new birth), with the node
/// that names each.
fn succeeded(records: &[NodeRow]) -> HashMap<BirthRef, NodeId> {
    records
        .iter()
        .flat_map(|r| r.predecessor.iter().chain(r.allocated.as_ref().and_then(|a| a.predecessor.as_ref())).map(move |p| (p.clone(), r.node_id.clone())))
        .filter(|(p, by)| p.node_id != *by)
        .collect()
}

/// The one record of `records` that holds `name`, folded as every reader folds them: a record holds its
/// name while [`holds_its_name`]; a version a re-created birth's own birth wrote is not a record
/// ([`superseded`]); and a record whose newest birth another record at the pair names its predecessor
/// holds the pair no longer, whoever wrote its line (its node was re-created or replaced there, and a
/// node never gets a birth after its successor's). Two that still hold it are refused by name, naming
/// both, never answered as either.
pub fn resolve(records: Vec<NodeRow>, name: &PathName) -> Result<Option<NodeRow>, StorageError> {
    let at: Vec<NodeRow> = records.into_iter().filter(|r| r.location() == *name).collect();
    let (re_created, succeeded) = (re_created(&at), succeeded(&at));
    let replaced = |r: &NodeRow| r.newest_birth().is_some_and(|b| succeeded.contains_key(&BirthRef { node_id: r.node_id.clone(), incarnation_id: b.incarnation_id.clone() }));
    let mut holding = at.into_iter().filter(|r| holds_its_name(r) && superseded(r, &re_created).is_none() && !replaced(r));
    let first = holding.next();
    if let (Some(a), Some(b)) = (&first, holding.next()) {
        return Err(StorageError::Refused {
            file: format!("the node records at path {:?}, name {:?}", name.path, name.name),
            reason: format!("two live records hold it, nodes {} and {}, and neither names the other's birth its predecessor", a.node_id, b.node_id),
        });
    }
    Ok(first)
}
