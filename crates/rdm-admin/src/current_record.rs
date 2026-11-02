//! The node record reader (query.md §1, Q323): "A current-pair lookup resolves the path/name claim to
//! a node ID. The reader reads the node's record at that node ID: one record per node, append-only, its
//! latest line current ... A node that has not written yet has no properties yet." The record is in two
//! parts at the node ID in the nodes store (Q333): its mesh-primary's whole row line and the node's own
//! line, the reader taking the latest of each. The node's mesh is the mesh its record names.

use crate::model::{NodeId, PathName};
use crate::storage::{NodeRow, NodesStorage};
use rdm_node::node_records::NodeProperties;

/// The store a current record is read from: the nodes store, each node's record at its node id in its
/// two parts, and the name claims a pair resolves through.
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
    /// The mesh-primary's part of the record of `node_id`, its latest line, with its mesh's name.
    /// `Ok(None)`: the node has no record yet. `Err`: a read failed, named.
    ///
    /// The shared resolver (i150 § OTLP span-assertion bar): one
    /// `rdm.node_admin.node.resolve.via-lifecycle-record` span per read, naming the reading node
    /// (`node_id`, this process's own), the subject, its mesh and the outcome: `read` (the node's
    /// latest record is read), `none` (the node has no record yet) or `unavailable` (the reason named).
    /// No value of the record is logged.
    pub async fn lifecycle(&self, node_id: &NodeId) -> Result<Option<(NodeRow, String)>, String> {
        use tracing::Instrument as _;
        let span = tracing::info_span!(
            "rdm.node_admin.node.resolve.via-lifecycle-record",
            node_id = rdm_transport::service_instance_id().unwrap_or_default(), subject_id = %node_id,
            mesh = tracing::field::Empty, outcome = tracing::field::Empty, reason = tracing::field::Empty, "otel.kind" = "internal",
        );
        let read = self.nodes.node(node_id).instrument(span.clone()).await.map_err(|e| format!("the record of node {node_id} could not be read: {e}"));
        let read = read.map(|row| row.map(|row| { let mesh = row.mesh.clone(); (row, mesh) }));
        match &read {
            Ok(Some((_, mesh))) => span.record("mesh", mesh.as_str()).record("outcome", "read"),
            Ok(None) => span.record("outcome", "none").record("reason", "the node has no record yet"),
            Err(why) => span.record("outcome", "unavailable").record("reason", why.as_str()),
        };
        read
    }

    /// The mesh-primary's part of the record of `node_id` ([`Self::lifecycle`]). `Ok(None)`: the node
    /// has no record yet.
    pub async fn row(&self, node_id: &NodeId) -> Result<Option<NodeRow>, String> {
        Ok(self.lifecycle(node_id).await?.map(|(row, _)| row))
    }

    /// The node's own part of the record of `node_id`, its latest line (build.md §5.3: "Each node writes
    /// its own config, tags, bound addresses and declarations through its property writer"). `Ok(None)`:
    /// the node has written none.
    pub async fn properties(&self, node_id: &NodeId) -> Result<Option<NodeProperties>, String> {
        self.nodes.properties(node_id).await.map_err(|e| format!("the own part of the record of node {node_id} could not be read: {e}"))
    }

    /// The mesh-primary's part of the latest record of every node of `mesh` (query.md §1: "`node.get`
    /// materializes records"; historical records, Deleted ones included, are among them). `Err`: a read
    /// failed.
    pub async fn lifecycle_rows(&self, mesh: &str) -> Result<Vec<NodeRow>, String> {
        self.nodes.nodes(mesh).await.map_err(|e| format!("the records of mesh {mesh} could not be read: {e}"))
    }

    /// The current-pair lookup: the live record standing at `name`, read from the node records by their
    /// path and name (node-identity-and-state.md §2). `Ok(None)` when none does.
    pub async fn row_at(&self, name: &PathName) -> Result<Option<NodeRow>, String> {
        let Some(at) = self.nodes.node_at(name).await.map_err(|e| format!("the record at path {:?}, name {:?} could not be read: {e}", name.path, name.name))? else {
            return Ok(None);
        };
        Ok(self.row(&at.node_id).await?.filter(|r| r.name == *name))
    }
}

/// `properties`, a node's own part of its record, when it is the part of `node_id`'s birth
/// `incarnation`: what the node wrote at that birth (its bound addresses and join declaration).
pub fn of_birth<'a>(properties: Option<&'a NodeProperties>, node_id: &NodeId, incarnation: &crate::model::IncarnationId) -> Option<&'a NodeProperties> {
    properties.filter(|p| p.birth.node_id == *node_id && p.birth.incarnation_id == *incarnation)
}

/// The store a reader reads in a unit cell: `rows`, each live record found at its path and name.
#[cfg(test)]
pub(crate) async fn stores_over(rows: std::sync::Arc<crate::storage::MemoryNodesStorage>, _t: &crate::topology::Topology) -> RecordStores {
    RecordStores { nodes: rows }
}
