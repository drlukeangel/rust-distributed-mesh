//! The node record reader (query.md §1, Q323, Q366): "A current-pair lookup resolves the path/name claim
//! to a node ID. The reader reads the node's record at that node ID: one record per node, append-only,
//! its latest line current." The node writes its record itself (Q356). The node's mesh is the first
//! location of its path.

use crate::model::{NodeId, PathName};
use crate::storage::{NodeLine, NodeRecord, NodesStorage};
use std::collections::{BTreeMap, BTreeSet};

/// The shared resolver's span of the record of `node_id` (i150 § OTLP span-assertion bar): the reading
/// node (`node_id`, this process's own) and the subject, its mesh, outcome and reason recorded once
/// known. No value of the record is logged.
fn resolve_span(node_id: &NodeId) -> tracing::Span {
    tracing::info_span!(
        "rdm.node_admin.node.resolve.via-lifecycle-record",
        node_id = rdm_transport::service_instance_id().unwrap_or_default(), subject_id = %node_id,
        mesh = tracing::field::Empty, outcome = tracing::field::Empty, reason = tracing::field::Empty, "otel.kind" = "internal",
    )
}

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
    /// `rdm.node_admin.node.resolve.via-lifecycle-record` span per record resolved, naming the reading
    /// node (`node_id`, this process's own), the subject, its mesh and the outcome: `read` (the node's
    /// latest record is read), `none` (the node has no record yet) or `unavailable` (the reason named).
    /// No value of the record is logged.
    pub async fn line(&self, node_id: &NodeId) -> Result<Option<NodeLine>, String> {
        use tracing::Instrument as _;
        let span = resolve_span(node_id);
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

    /// The records of `births`, each a node id beside its mesh: the records of each of their meshes read
    /// as one store read ([`NodesStorage::nodes`], a `Withdrawn` one skipped), the meshes read together,
    /// never one read per node. Each birth's record is resolved as [`Self::line`] resolves one, with its
    /// own span: `read` with its mesh, or `none`. `Err` names the mesh whose records could not be read.
    pub async fn rows_of<'n>(&self, births: impl IntoIterator<Item = (&'n NodeId, &'n str)>) -> Result<BTreeMap<NodeId, NodeRecord>, String> {
        let births: Vec<(&NodeId, &str)> = births.into_iter().collect();
        let mut held: BTreeMap<NodeId, NodeRecord> = self.of_meshes(births.iter().map(|(_, mesh)| *mesh).collect()).await?.into_iter().map(|r| (r.node_id.clone(), r)).collect();
        let mut out = BTreeMap::new();
        for (node_id, _) in births {
            let span = resolve_span(node_id);
            match held.remove(node_id) {
                Some(record) => {
                    span.record("mesh", record.mesh().as_str()).record("outcome", "read");
                    out.insert(node_id.clone(), record);
                }
                None => {
                    span.record("outcome", "none").record("reason", "the node has no record yet");
                }
            }
        }
        Ok(out)
    }

    /// The current-pair lookup of each of `names` ([`Self::row_at`]), many pairs at one store read per
    /// mesh: the records of each pair's mesh (the first location of its path) read as one store read, the
    /// meshes read together, never one read per pair; the live record standing at each pair found among
    /// them (node-identity-and-state.md §2), its record the one read at its node ID, resolved with its own
    /// span as [`Self::line`] resolves one. A pair no live record stands at is not answered. `Err` names
    /// the mesh whose records could not be read, or a pair two live records hold.
    pub async fn rows_at<'n>(&self, names: impl IntoIterator<Item = &'n PathName>) -> Result<BTreeMap<PathName, NodeRecord>, String> {
        let names: BTreeSet<&PathName> = names.into_iter().collect();
        let meshes: BTreeSet<String> = names.iter().map(|at| rdm_core::path::mesh_of(&at.path)).collect();
        let mut standing: BTreeMap<PathName, Vec<NodeRecord>> = BTreeMap::new();
        for r in self.of_meshes(meshes.iter().map(String::as_str).collect()).await? {
            standing.entry(r.location()).or_default().push(r);
        }
        let mut out = BTreeMap::new();
        for name in names {
            let at = rdm_node::pair_record::resolve(standing.remove(name).unwrap_or_default(), name).map_err(|e| format!("the record at path {:?}, name {:?} could not be read: {e}", name.path, name.name))?;
            if let Some(record) = at {
                resolve_span(&record.node_id).record("mesh", record.mesh().as_str()).record("outcome", "read");
                out.insert(name.clone(), record);
            }
        }
        Ok(out)
    }

    /// The records of `meshes`, each mesh's as one store read ([`NodesStorage::nodes`], a `Withdrawn` one
    /// skipped), the meshes read together. `Err` names the mesh whose records could not be read.
    async fn of_meshes(&self, meshes: BTreeSet<&str>) -> Result<Vec<NodeRecord>, String> {
        let read = futures_util::future::try_join_all(meshes.into_iter().map(|mesh| async move { self.nodes.nodes(mesh).await.map_err(|e| format!("the records of mesh {mesh} could not be read: {e}")) })).await?;
        Ok(read.into_iter().flatten().collect())
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
