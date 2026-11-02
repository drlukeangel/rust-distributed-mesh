//! The node record reader (query.md §1, Q323): "A current-pair lookup resolves the path/name claim to
//! a node ID. The reader reads the node's record at that node ID: one record per node, append-only, its
//! latest line current ... A node that has not written yet has no properties yet." The node's mesh is
//! the mesh its record names.

use crate::model::{NodeId, PathName};
use crate::storage::{NodeRow, NodesStorage};
use rdm_node::node_records::{NodeProperties, NodeRecordsStorage};

/// The stores a current record is read from: the nodes store (each node's record at its node id and
/// the name claims a pair resolves through) and the node's own property records.
#[derive(Clone, Copy)]
pub struct CurrentRecords<'a> {
    /// The nodes store: each node's record and the name claims.
    pub claims: &'a dyn NodesStorage,
    /// The property records the nodes write.
    pub records: &'a dyn NodeRecordsStorage,
}

/// [`CurrentRecords`]' stores, held: what a reader that outlives one call keeps.
#[derive(Clone)]
pub struct RecordStores {
    /// The nodes store: each node's record and the name claims.
    pub claims: std::sync::Arc<dyn NodesStorage>,
    /// The property records the nodes write.
    pub records: std::sync::Arc<dyn NodeRecordsStorage>,
}

impl RecordStores {
    /// The reader over these stores.
    pub fn reader(&self) -> CurrentRecords<'_> {
        CurrentRecords { claims: &*self.claims, records: &*self.records }
    }
}

/// Where a reader that outlives one call takes its stores from, once they are open: `None` before.
pub type RecordStoresSource = std::sync::Arc<dyn Fn() -> Option<RecordStores> + Send + Sync>;

impl CurrentRecords<'_> {
    /// The latest record of `node_id` in the nodes store, with its mesh's name. `Ok(None)`: the node
    /// has no record yet. `Err`: a read failed, named.
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
        let read = self.claims.node(node_id).instrument(span.clone()).await.map_err(|e| format!("the record of node {node_id} could not be read: {e}"));
        let read = read.map(|row| row.map(|row| { let mesh = row.mesh.clone(); (row, mesh) }));
        match &read {
            Ok(Some((_, mesh))) => span.record("mesh", mesh.as_str()).record("outcome", "read"),
            Ok(None) => span.record("outcome", "none").record("reason", "the node has no record yet"),
            Err(why) => span.record("outcome", "unavailable").record("reason", why.as_str()),
        };
        read
    }

    /// The record of `node_id`, with what the node writes itself taken from its own property record,
    /// read separately at the node id ([`row_of`]). `Ok(None)`: the node has no record yet.
    pub async fn row(&self, node_id: &NodeId) -> Result<Option<NodeRow>, String> {
        let Some((row, _)) = self.lifecycle(node_id).await? else { return Ok(None) };
        let properties = self.records.properties(node_id).await.map_err(|e| format!("the property record of node {node_id} could not be read: {e}"))?;
        Ok(Some(row_of(row, properties.as_ref())))
    }

    /// The latest record of every node of `mesh` (query.md §1: "`node.get` materializes records";
    /// historical records, Deleted ones included, are among them), each without what its node writes
    /// itself: no property record is read. `Err`: a read failed.
    pub async fn lifecycle_rows(&self, mesh: &str) -> Result<Vec<NodeRow>, String> {
        let written = self.claims.nodes(mesh).await.map_err(|e| format!("the records of mesh {mesh} could not be read: {e}"))?;
        Ok(written.into_iter().map(|row| row_of(row, None)).collect())
    }

    /// The current-pair lookup: the record of the node whose name claim holds `name`, while that record
    /// stands at `name`. `Ok(None)` when no claim holds the name, or its claimant's current record stands
    /// at another pair or was never written (the claim decides, never a record last written there).
    pub async fn row_at(&self, name: &PathName) -> Result<Option<NodeRow>, String> {
        let Some(node_id) = self.claims.name_claim(name).await.map_err(|e| format!("the name claim at path {:?}, name {:?} could not be read: {e}", name.path, name.name))? else {
            return Ok(None);
        };
        Ok(self.row(&node_id).await?.filter(|r| r.name == *name))
    }
}

/// `row`, a node's record, with what the node writes itself taken from `properties`, its own property
/// record (build.md §5.3: "Each node writes its own config, tags, bound addresses and declarations
/// through its property writer"): the bound addresses of its newest birth and its join declaration,
/// taken only from a property record of the same node and birth; no birth's addresses and no
/// declaration are taken from anywhere else.
pub fn row_of(mut row: NodeRow, properties: Option<&NodeProperties>) -> NodeRow {
    let own = properties.filter(|p| p.birth.node_id == row.node_id);
    for birth in &mut row.births {
        birth.endpoint_id = None;
        birth.transport_addr = None;
        birth.listeners.clear();
    }
    if let (Some(p), Some(birth)) = (own, row.births.last_mut()) {
        if p.birth.incarnation_id == birth.incarnation_id {
            birth.endpoint_id = p.endpoint_id.clone();
            birth.transport_addr = p.transport_addr;
            birth.listeners = p.listeners.clone();
        }
    }
    row.app_types = own.and_then(|p| p.app_types.clone());
    row
}

/// The stores a reader reads in a unit cell: `rows`' name claims (each live record's name claimed, as its
/// writer's put claims it) and an empty property store.
#[cfg(test)]
pub(crate) async fn stores_over(rows: std::sync::Arc<crate::storage::MemoryNodesStorage>, t: &crate::topology::Topology) -> RecordStores {
    let records = std::sync::Arc::new(rdm_node::node_records::MemoryNodeRecords::default());
    let names: std::collections::BTreeSet<String> = t.nodes.iter().map(|n| n.mesh.clone()).chain(t.meshes.iter().map(|m| m.name.clone())).collect();
    for mesh in names {
        for row in rows.nodes(&mesh).await.expect("the nodes store reads") {
            // A live record holds its name's claim, as its writer's put takes it.
            if !crate::node_rows::released_its_name(&row) {
                rows.claim_node_name(&row.name, &row.node_id).await.expect("the nodes store writes").expect("one live record at each name");
            }
        }
    }
    RecordStores { claims: rows, records }
}
