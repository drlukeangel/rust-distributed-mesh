//! The node record reader (query.md §1, Q323, Q366): "A current-pair lookup resolves the path/name claim
//! to a node ID. The reader reads the node's record at that node ID: one record per node, append-only,
//! its latest line current." The node writes its record itself (Q356). The node's mesh is the first
//! location of its path.

use crate::model::{NodeId, PathName};
use crate::storage::{NodeLine, NodeRecord, NodesStorage};

/// The store a current record is read from: the nodes store, each node's record at its node id.
#[derive(Clone, Copy)]
pub struct CurrentRecords<'a> {
    /// The nodes store.
    pub nodes: &'a dyn NodesStorage,
}

/// [`CurrentRecords`]' store, held: what a reader that outlives one call keeps.
#[derive(Clone)]
pub struct RecordStores {
    /// The nodes store.
    pub nodes: std::sync::Arc<dyn NodesStorage>,
}

impl RecordStores {
    /// The reader over this store.
    pub fn reader(&self) -> CurrentRecords<'_> {
        CurrentRecords { nodes: &*self.nodes }
    }
}

/// Where a reader that outlives one call takes its stores from, once they are open: `None` before.
pub type RecordStoresSource = std::sync::Arc<dyn Fn() -> Option<RecordStores> + Send + Sync>;

impl CurrentRecords<'_> {
    /// The record of `node_id`, its latest line with its time. `Ok(None)`: the node has no record yet.
    /// `Err`: a read failed, named.
    ///
    /// The shared resolver (i150 § OTLP span-assertion bar): one
    /// `rdm.node_admin.node.resolve.via-lifecycle-record` span per read, naming the reading node
    /// (`node_id`, this process's own), the subject, its mesh and the outcome: `read` (the node's
    /// latest record is read), `none` (the node has no record yet) or `unavailable` (the reason named).
    /// No value of the record is logged.
    pub async fn line(&self, node_id: &NodeId) -> Result<Option<NodeLine>, String> {
        use tracing::Instrument as _;
        let span = tracing::info_span!(
            "rdm.node_admin.node.resolve.via-lifecycle-record",
            node_id = rdm_transport::service_instance_id().unwrap_or_default(), subject_id = %node_id,
            mesh = tracing::field::Empty, outcome = tracing::field::Empty, reason = tracing::field::Empty, "otel.kind" = "internal",
        );
        let read = self.nodes.node(node_id).instrument(span.clone()).await.map_err(|e| format!("the record of node {node_id} could not be read: {e}"));
        match &read {
            Ok(Some(line)) => span.record("mesh", line.record.mesh().as_str()).record("outcome", "read"),
            Ok(None) => span.record("outcome", "none").record("reason", "the node has no record yet"),
            Err(why) => span.record("outcome", "unavailable").record("reason", why.as_str()),
        };
        read
    }

    /// The record of `node_id` ([`Self::line`]). `Ok(None)`: the node has no record yet.
    pub async fn row(&self, node_id: &NodeId) -> Result<Option<NodeRecord>, String> {
        Ok(self.line(node_id).await?.map(|l| l.record))
    }

    /// The current-pair lookup: the live record standing at `name`, read from the node records by their
    /// path and name (node-identity-and-state.md §2). `Ok(None)` when none does.
    pub async fn row_at(&self, name: &PathName) -> Result<Option<NodeRecord>, String> {
        let Some(at) = crate::node_rows::record_at(self.nodes, name).await.map_err(|e| format!("the record at path {:?}, name {:?} could not be read: {e}", name.path, name.name))? else {
            return Ok(None);
        };
        Ok(self.row(&at.node_id).await?.filter(|r| r.location() == *name))
    }
}

/// The store a reader reads in a unit cell: `rows`, each live record found at its path and name.
#[cfg(test)]
pub(crate) async fn stores_over(rows: std::sync::Arc<crate::storage::MemoryNodesStorage>, _t: &crate::topology::Topology) -> RecordStores {
    RecordStores { nodes: rows }
}
