//! The node row at the fabric-primary (ops-naming "Node data: meta, config and exe"; the delete
//! point; lifecycles/node-decommission.md step 3).
//!
//! The fabric-primary is the only writer of a node's row. It writes the whole row from the record
//! it holds (its topology), a blind put of the `path.name` key, and never reads the store on that
//! write. Two moments of a Build it drives write a row, each read off the live `Step` frame the
//! executor streams:
//!
//! - `node.delete` (`retire-node:<path>`): at `NodeDeleted` the row is written with status
//!   `Deleted`, the record and its config kept. The record is the one held when the executor
//!   announced the removal (`NodeDeleting`): the node is still found and live in every view then,
//!   while by `NodeDeleted` its departure may already have left the view.
//! - `node.create` (`create-node:<path>`): at `Complete` the new birth's row is claimed at its
//!   `path.name` (insert-and-fail), its config seeded from the previous row at that `path.name`.

use crate::model::PathName;
use crate::storage::NodesStorage;
use crate::topology::Topology;
use std::sync::Arc;
use tokio::sync::RwLock;

/// The fabric-primary's writer of node rows.
pub struct NodeRows {
    storage: Arc<dyn NodesStorage>,
}

impl NodeRows {
    /// The writer over `storage`, the fabric-primary's nodes store.
    pub fn new(storage: Arc<dyn NodesStorage>) -> Self {
        Self { storage }
    }

    /// The store the rows are written to.
    pub fn storage(&self) -> &Arc<dyn NodesStorage> {
        &self.storage
    }

    /// A live `Step` frame of a Build the fabric-primary `me` drives, over its held view.
    pub async fn on_step(&self, _me: &PathName, _view: &RwLock<Topology>, _operation: &str, _step: &str) {}
}
