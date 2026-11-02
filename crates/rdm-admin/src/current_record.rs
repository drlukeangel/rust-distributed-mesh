//! The node record reader (query.md §1, Q323): "A current-pair lookup resolves the path/name claim to
//! a node ID. The reader reads the node's record at that node ID: one record per node, append-only, its
//! latest line current ... A node that has not written yet has no properties yet." The node's mesh is
//! the `mesh_id` its record carries, and the mesh's name is its Mesh record's, by that id.

use crate::model::{NodeId, PathName};
use crate::storage::{BirthFacts, BirthRef, MeshStorage, NodeRow, NodesStorage};
use rdm_node::node_records::{NodeLifecycle, NodeProperties, NodeRecordsStorage, RecordBirth};

/// The stores a current record is read from: the name claims a pair resolves through, the Mesh
/// records a mesh id names, and the node records (lifecycle and properties).
#[derive(Clone, Copy)]
pub struct CurrentRecords<'a> {
    /// The name claims.
    pub claims: &'a dyn NodesStorage,
    /// The Mesh records, by mesh id.
    pub meshes: &'a dyn MeshStorage,
    /// The lifecycle and property records.
    pub records: &'a dyn NodeRecordsStorage,
}

/// [`CurrentRecords`]' stores, held: what a reader that outlives one call keeps.
#[derive(Clone)]
pub struct RecordStores {
    /// The name claims.
    pub claims: std::sync::Arc<dyn NodesStorage>,
    /// The Mesh records, by mesh id.
    pub meshes: std::sync::Arc<dyn MeshStorage>,
    /// The lifecycle and property records.
    pub records: std::sync::Arc<dyn NodeRecordsStorage>,
}

impl RecordStores {
    /// The reader over these stores.
    pub fn reader(&self) -> CurrentRecords<'_> {
        CurrentRecords { claims: &*self.claims, meshes: &*self.meshes, records: &*self.records }
    }
}

/// Where a reader that outlives one call takes its stores from, once they are open: `None` before.
pub type RecordStoresSource = std::sync::Arc<dyn Fn() -> Option<RecordStores> + Send + Sync>;

impl CurrentRecords<'_> {
    /// The latest lifecycle record of `node_id`, with its mesh's name. `Ok(None)`: the node has no
    /// record yet. `Err`: a read failed, named.
    pub async fn lifecycle(&self, node_id: &NodeId) -> Result<Option<(NodeLifecycle, String)>, String> {
        let Some(record) = self.records.lifecycle(node_id).await.map_err(|e| format!("the lifecycle record of node {node_id} could not be read: {e}"))? else { return Ok(None) };
        let mesh_id = record.mesh_id.clone();
        let mesh = self
            .meshes
            .mesh(&mesh_id)
            .await
            .map_err(|e| format!("the Mesh record {mesh_id} of node {node_id} could not be read: {e}"))?
            .ok_or_else(|| format!("node {node_id}'s mesh {mesh_id} has no Mesh record"))?
            .name;
        Ok(Some((record, mesh)))
    }

    /// The record of `node_id`, its own property record read separately at the node id ([`row_of`]).
    /// `Ok(None)`: the node has no lifecycle record yet.
    pub async fn row(&self, node_id: &NodeId) -> Result<Option<NodeRow>, String> {
        let Some((lifecycle, mesh)) = self.lifecycle(node_id).await? else { return Ok(None) };
        let properties = self.records.properties(node_id).await.map_err(|e| format!("the property record of node {node_id} could not be read: {e}"))?;
        row_of(&lifecycle, properties.as_ref(), mesh).map(Some)
    }

    /// The latest lifecycle record of every node of `mesh` (query.md §1: "`node.get` materializes
    /// records"; historical records, Deleted ones included, are among them), each as a row of its
    /// lifecycle facts alone: no property record is read. `Ok(empty)` when the mesh has no Mesh record.
    /// `Err`: a read failed.
    pub async fn lifecycle_rows(&self, mesh: &str) -> Result<Vec<NodeRow>, String> {
        let Some(record) = self.meshes.mesh_named(mesh).await.map_err(|e| format!("the Mesh record of {mesh} could not be read: {e}"))? else { return Ok(Vec::new()) };
        let written = self.records.lifecycles(&record.mesh_id).await.map_err(|e| format!("the lifecycle records of mesh {mesh} could not be read: {e}"))?;
        written.iter().map(|l| row_of(l, None, mesh.to_string())).collect()
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

/// `lifecycle`, the latest lifecycle record of a node in `mesh`, and `properties`, the node's own
/// property record, as one row: the lifecycle facts are its mesh-primary's; the bound addresses and the join
/// declaration are the node's own (build.md §5.3: "Each node writes its own config, tags, bound
/// addresses and declarations through its property writer"), taken only from a property record of the
/// same node and birth. A claim's record names its allocated incarnation before any fact of that birth
/// is written: it holds no birth yet.
pub fn row_of(lifecycle: &NodeLifecycle, properties: Option<&NodeProperties>, mesh: String) -> Result<NodeRow, String> {
    let l = lifecycle;
    let pair = |path: &str, name: &str| PathName::new(path, name).map_err(|e| format!("node {}'s lifecycle record holds path {path:?}, name {name:?}, which is not a pair: {e}", l.node_id));
    let name = pair(&l.path, &l.name)?;
    let own = properties.filter(|p| p.birth.node_id == l.node_id);
    let mut birth = BirthFacts::new(l.incarnation_id.clone(), name.clone());
    let empty = birth.clone();
    birth.declared = l.declared.clone();
    birth.operations = l.operations.clone();
    birth.silence = l.silence;
    birth.data_dir_lost = l.data_dir_lost.clone();
    birth.data_dir_unreadable = l.data_dir_unreadable.clone();
    birth.cert = l.cert.clone();
    birth.signing_cert = l.signing_cert.clone();
    if let Some(p) = own.filter(|p| p.birth.incarnation_id == l.incarnation_id) {
        birth.endpoint_id = p.endpoint_id.clone();
        birth.transport_addr = p.transport_addr;
        birth.listeners = p.listeners.clone();
    }
    let claim_only = l.allocated.as_ref().is_some_and(|a| a.incarnation == l.incarnation_id) && birth == empty;
    let birth_ref = |b: &RecordBirth| BirthRef { node_id: b.node_id.clone(), incarnation_id: b.incarnation_id.clone() };
    let mut row = NodeRow::new(name, mesh, l.kind, l.node_id.clone(), l.status);
    row.allocated = l.allocated.clone();
    row.initial_properties = l.initial_properties.clone();
    row.renamed_from = l.renamed_from.as_ref().map(|(path, name)| pair(path, name)).transpose()?;
    row.replacement_from = l.replacement_from.clone();
    row.inherited_by = l.inherited_by.clone();
    row.app_types = own.and_then(|p| p.app_types.clone());
    row.executable = l.executable.clone();
    row.births = if claim_only { Vec::new() } else { vec![birth] };
    row.mesh_id = Some(l.mesh_id.clone());
    row.predecessor = l.predecessor.as_ref().map(birth_ref);
    row.storage_persistence = l.storage_persistence;
    row.topology_storage_provider = l.topology_storage_provider.clone();
    row.placement = l.placement;
    Ok(row)
}

/// The stores a reader reads in a unit cell: `rows`' name claims (each live record's name claimed, as its
/// writer's put claims it), and every record `rows` holds of a mesh `t` names written as its lifecycle
/// record, each mesh given its Mesh record and name claim (`t`'s id for it, else a minted one).
#[cfg(test)]
pub(crate) async fn stores_over(rows: std::sync::Arc<crate::storage::MemoryNodesStorage>, t: &crate::topology::Topology) -> RecordStores {
    let meshes = std::sync::Arc::new(crate::storage::MemoryMeshStorage::default());
    let records = std::sync::Arc::new(rdm_node::node_records::MemoryNodeRecords::default());
    let writer = RecordBirth { node_id: NodeId::mint(), incarnation_id: crate::model::IncarnationId::mint() };
    let names: std::collections::BTreeSet<String> = t.nodes.iter().map(|n| n.mesh.clone()).chain(t.meshes.iter().map(|m| m.name.clone())).collect();
    for mesh in names {
        let id = t.meshes.iter().find(|m| m.name == mesh).and_then(|m| m.id.clone()).unwrap_or_else(crate::model::MeshId::mint);
        meshes.put_mesh(&crate::storage::MeshRecord::new(id.clone(), mesh.clone())).await.expect("the Mesh record is written");
        meshes.claim_mesh_name(&mesh, &id).await.expect("the mesh store writes").expect("the mesh's name is free");
        for row in rows.nodes(&mesh).await.expect("the nodes store reads") {
            if let Some(record) = crate::node_rows::lifecycle_record(&row, &t.fabric.id, Some(&id), row.node_type().to_string(), (false, false)) {
                records.put_lifecycle(&record, &writer).await.expect("the lifecycle record is written");
            }
            // A live record holds its name's claim, as its writer's put takes it.
            if !crate::node_rows::released_its_name(&row) {
                rows.claim_node_name(&row.name, &row.node_id).await.expect("the nodes store writes").expect("one live record at each name");
            }
        }
    }
    RecordStores { claims: rows, meshes, records }
}
