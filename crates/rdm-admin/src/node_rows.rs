//! The node records a mesh-primary writes (Q356): a node writes every field of its own record itself,
//! and its mesh-primary, or the fabric-primary for a mesh with no mesh-primary, writes a node's record
//! only when the node cannot: a node that is lost, killed or deleted (node-identity-and-state.md §2.5).
//!
//! Each such write reads the node's latest line (a runtime point read, Q237 A) and appends it whole
//! with the state the delete or the investigation's verdict decides (storage-whole-records.md):
//!
//! - node.delete's admission: `Deleting` ([`NodeRows::deleting`]).
//! - `NodeDeleted` (node-delete.md step 3), a mesh delete's member, node recovery's lost birth and
//!   fabric recovery's: `Deleted`, the record and its config kept ([`NodeRows::deleted`],
//!   [`NodeRows::deleted_unreached`], [`NodeRows::deleted_birth`]).
//!
//! Every other admin refuses by name (`not-mesh-primary`), another mesh's primary included. A record a
//! node never wrote (its create ended before its start) is no record, and nothing is written for it.

use crate::model::{Node, NodeId, NodeStatus, PathName};
use crate::storage::{NameHeld, NodeRecord, NodesStorage};
use crate::topology::Topology;
use std::sync::Arc;
use tokio::sync::RwLock;

/// The writer of the records of a mesh's lost, killed and deleted nodes.
pub struct NodeRows {
    /// This admin's path, the name its spans and refusals carry.
    me_path: String,
    /// This node's name, beside its path.
    me_name: String,
    /// The node id this admin was born with: it writes a mesh's records only while its view names that
    /// node the mesh's primary, or the fabric-primary of a mesh with none.
    me_id: NodeId,
    /// The birth this admin writes as: what fabric recovery's executor check names.
    birth: std::sync::OnceLock<crate::storage::BirthRef>,
    /// This admin's view, read at each write to decide whether it writes the record's mesh.
    view: Arc<RwLock<Topology>>,
    storage: Arc<dyn NodesStorage>,
    /// Fabric recovery in a view: its executor writes the records its own operations require while
    /// the target mesh has no mesh-primary (build.md §5.3, Q122).
    fabric_recovery: Option<crate::fabric_recovery::Reader>,
}

impl NodeRows {
    /// The `(path, name)` lookup index key of me_path and me_name.
    pub fn me(&self) -> PathName {
        PathName { path: self.me_path.clone(), name: self.me_name.clone() }
    }
}

/// The tag labels as one span field.
pub(crate) fn labels(tags: &std::collections::BTreeSet<String>) -> String {
    tags.iter().map(String::as_str).collect::<Vec<_>>().join(",")
}

/// Why the writer refused a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowRefused {
    /// This admin is not the mesh-primary of the record's mesh (nor the fabric-primary of a mesh with
    /// none): why, naming the mesh and its mesh-primary.
    NotMeshPrimary(String),
    /// Another node's live record holds the name: nothing was written.
    NameHeld(NameHeld),
    /// The record could not be read or written: why, whole.
    Unwritten(String),
}

impl RowRefused {
    /// The span outcome that names the refusal.
    pub fn outcome(&self) -> &'static str {
        match self {
            Self::NotMeshPrimary(_) => "not-mesh-primary",
            Self::NameHeld(_) => "name-held",
            Self::Unwritten(_) => "row-refused",
        }
    }
}

impl std::fmt::Display for RowRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotMeshPrimary(why) | Self::Unwritten(why) => f.write_str(why),
            Self::NameHeld(held) => write!(f, "name-held: {held}"),
        }
    }
}

/// Whether `me_id` acts as `mesh`'s mesh-primary in view `t`: the mesh's primary node-admin, or the
/// fabric-primary while the mesh has none. A stopped primary is no primary (R-NS1 item 2: from the stop
/// of a mesh's last admin the fabric-primary acts for a mesh with no primary).
pub fn acts_as_mesh_primary(t: &Topology, me_id: &NodeId, mesh: &str) -> bool {
    t.is_mesh_primary(mesh, me_id) || (t.is_fabric_primary(me_id) && t.node_type_primary(mesh, "node-admin").filter(|p| !p.stopped).is_none())
}

impl NodeRows {
    /// The writer of the admin `me`, born as `me_id`, deciding over its view `view`, writing to
    /// `storage`, its nodes store.
    pub fn new(me: PathName, me_id: NodeId, view: Arc<RwLock<Topology>>, storage: Arc<dyn NodesStorage>) -> Self {
        Self { me_path: me.path.clone(), me_name: me.name.clone(), me_id, birth: std::sync::OnceLock::new(), view, storage, fabric_recovery: None }
    }

    /// Read fabric recovery through `read`: while this admin runs it, it writes the target mesh's records
    /// its own operations require (build.md §5.3, Q122).
    pub fn with_fabric_recovery(mut self, read: crate::fabric_recovery::Reader) -> Self {
        self.fabric_recovery = Some(read);
        self
    }

    /// Write as `birth`, this admin's own (node id and incarnation); the first call stands.
    pub fn writes_as(&self, birth: crate::storage::BirthRef) {
        let _ = self.birth.set(birth);
    }

    /// The store the records are written to.
    pub fn storage(&self) -> &Arc<dyn NodesStorage> {
        &self.storage
    }

    /// This admin's currently held `(path, name)` in `t`, falling back to `self.me` before its birth
    /// is in the view.
    fn held_name_in(&self, t: &Topology) -> PathName {
        t.own(&self.me_id).map(|n| n.location()).unwrap_or_else(|| self.me())
    }

    /// Why this admin does not write `mesh`'s records in view `t` (the record at `subject`), naming the
    /// mesh-primary the view holds; `None` when it writes them.
    fn not_writer(&self, t: &Topology, mesh: &str, subject: &PathName) -> Option<String> {
        if acts_as_mesh_primary(t, &self.me_id, mesh) || self.birth.get().is_some_and(|birth| crate::fabric_recovery::executes_for(self.fabric_recovery.as_ref(), t, birth, mesh)) {
            return None;
        }
        let me = self.held_name_in(t);
        let writer = match (t.node_type_primary(mesh, "node-admin"), t.fabric_primary()) {
            (Some(p), _) => format!("{mesh}'s primary node-admin {} writes them", p.node_id),
            (None, Some(fp)) => format!("{mesh} has no primary node-admin, and fabric-primary {} writes them for it", fp.node_id),
            (None, None) => format!("{mesh} has no primary node-admin and the view holds no fabric-primary to write them for it"),
        };
        Some(format!("path {me_path:?}, name {me_name:?} ({}) is not the mesh-primary of {mesh} and refuses the record at path {subject_path:?}, name {subject_name:?}: {writer}", self.me_id, me_path = &me.path, me_name = &me.name, subject_path = &subject.path, subject_name = &subject.name))
    }

    /// Whether this admin, the fabric-primary, writes the record of the node-admin `node_id` as acting
    /// primary of its mesh: the mesh holds no primary but that node (node-identity-and-state.md §4.3).
    fn acting_for(&self, t: &Topology, mesh: &str, node_id: &NodeId) -> bool {
        t.is_fabric_primary(&self.me_id) && !t.node_type_primary(mesh, "node-admin").is_some_and(|p| p.node_id != *node_id)
    }

    /// The writer check of the record of `node_id` at `name` in view `t`: this admin writes it as its
    /// mesh's mesh-primary or, `acting` (a node-admin's record), as the fabric-primary acting for a mesh
    /// that holds no primary but that node; any other admin refuses it by name. Answers the name this
    /// admin holds.
    fn writer_in(&self, t: &Topology, name: &PathName, mesh: &str, node_id: &NodeId, acting: bool) -> Result<PathName, RowRefused> {
        if acting && self.acting_for(t, mesh, node_id) {
            return Ok(self.held_name_in(t));
        }
        match self.not_writer(t, mesh, name) {
            Some(why) => Err(RowRefused::NotMeshPrimary(why)),
            None => Ok(self.held_name_in(t)),
        }
    }

    /// A create's check of the path and name `name` for the node `node_id` of `mesh` (node-create.md:
    /// "the mesh-primary checks the path and name against its view and the creates it has in flight"):
    /// this admin acts as `mesh`'s mesh-primary (a node-admin's recovery at the fabric-primary
    /// included), else `NotMeshPrimary`; another node its view holds live at `name`, other than the
    /// birth `predecessor` the create replaces, or `in_flight`, the node another create this admin has
    /// in flight deploys at `name`, refuses it `NameHeld` by name. Nothing is read from the store (Q227)
    /// and nothing is written: the node writes its own record at its start (Q356).
    pub async fn check_create(&self, name: &PathName, mesh: &str, r#type: &str, node_id: &NodeId, predecessor: Option<&NodeId>, in_flight: Option<NodeId>) -> Result<(), RowRefused> {
        let t = self.view.read().await;
        if !(predecessor.is_some() && rdm_core::is_node_admin(r#type) && t.is_fabric_primary(&self.me_id)) {
            if let Some(why) = self.not_writer(&t, mesh, name) {
                return Err(RowRefused::NotMeshPrimary(why));
            }
        }
        let held = t.births().find(|n| n.location() == *name && n.node_id != *node_id && Some(&n.node_id) != predecessor && !matches!(n.status, NodeStatus::Deleted | NodeStatus::Withdrawn)).map(|n| n.node_id.clone());
        match held.or(in_flight.filter(|id| id != node_id)) {
            Some(by) => Err(RowRefused::NameHeld(NameHeld { path: name.path.clone(), name: name.name.clone(), by })),
            None => Ok(()),
        }
    }

    /// Whether this admin writes the `Deleted` record of the unreached birth `record` now
    /// ([`Self::deleted_unreached`]).
    pub async fn writes_unreached(&self, record: &Node) -> bool {
        self.writer_in(&*self.view.read().await, &record.location(), &record.mesh, &record.node_id, rdm_core::is_node_admin(&record.r#type)).is_ok()
    }

    /// Whether this admin writes `mesh`'s lost, killed and deleted nodes' records now.
    pub async fn writes_now(&self, mesh: &str, subject: &PathName) -> bool {
        self.not_writer(&*self.view.read().await, mesh, subject).is_none()
    }

    /// node.delete's admission (states.md: node deleting, the node's mesh-primary, the fabric-primary
    /// for a mesh with no primary): the node's latest record with state `Deleting`. One
    /// `rdm.node_admin.node.update.via-node-row` span.
    pub async fn deleting(&self, record: &Node) -> Result<(), RowRefused> {
        let me = self.writer_in(&*self.view.read().await, &record.location(), &record.mesh, &record.node_id, rdm_core::is_node_admin(&record.r#type))?;
        let span = tracing::info_span!("rdm.node_admin.node.update.via-node-row", node_path = %me.path, node_name = %me.name, subject_path = %record.path, subject_name = %record.name, node_id = %record.node_id, state = "deleting", outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal");
        let written = async {
            let Some(mut row) = self.storage.node(&record.node_id).await.map_err(|e| RowRefused::Unwritten(e.to_string()))?.map(|l| l.record) else {
                return Ok(false);
            };
            if !row.set_state(NodeStatus::Deleting) {
                return Ok(false);
            }
            self.storage.put_node(&row).await.map_err(|e| RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: the deleting state of node {} is not written: {e}", record.node_id, me_path = &me.path, me_name = &me.name)))?;
            Ok(true)
        };
        let r: Result<bool, RowRefused> = tracing::Instrument::instrument(written, span.clone()).await;
        match &r {
            Ok(true) => span.record("outcome", "written"),
            Ok(false) => span.record("outcome", "unchanged"),
            Err(e) => span.record("outcome", e.outcome()).record("detail", e.to_string().as_str()),
        };
        r.map(|_| ())
    }

    /// `NodeDeleted` (lifecycles/node-delete.md step 3): the node's record with state `Deleted`, its
    /// config kept. One `via-node-row` span.
    pub async fn deleted(&self, record: &Node, operation: &rdm_core::OperationId) -> Result<(), RowRefused> {
        self.deleted_record(&record.location(), &record.node_id, operation, false).await
    }

    /// [`Self::deleted`] of an unreached birth (node-recovery.md step 3): a node-admin's record is written
    /// by its mesh's elected primary, and by the fabric-primary only as acting primary of a mesh with no
    /// primary (node-identity-and-state.md §4.3; R-OW1).
    pub async fn deleted_unreached(&self, record: &Node, operation: &rdm_core::OperationId) -> Result<(), RowRefused> {
        self.deleted_record(&record.location(), &record.node_id, operation, rdm_core::is_node_admin(&record.r#type)).await
    }

    /// The `Deleted` record of a birth the view holds no more (a mesh leave's member that left the view
    /// after its exact Exited receipt, mesh-delete.md steps 3 and 7): the record of `node_id` at `name`.
    pub async fn deleted_birth(&self, name: &PathName, node_id: &NodeId, operation: &rdm_core::OperationId) -> Result<(), RowRefused> {
        self.deleted_record(name, node_id, operation, false).await
    }

    /// The `Deleted` record of `node_id` at `name`, from its latest line, or the refusal that names why
    /// it is not written.
    async fn deleted_record(&self, name: &PathName, node_id: &NodeId, operation: &rdm_core::OperationId, acting: bool) -> Result<(), RowRefused> {
        let me = self.held_name_in(&*self.view.read().await);
        let span = tracing::info_span!(
            "rdm.node_admin.node.delete.via-node-row",
            node_path = %me.path, node_name = %me.name,
            subject_path = %name.path, subject_name = %name.name,
            operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step,
            node_id = %node_id,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let r = tracing::Instrument::instrument(
            async {
                let mesh = rdm_core::path::mesh_of(&name.path);
                let me = self.writer_in(&*self.view.read().await, name, &mesh, node_id, acting)?;
                write_deleted(&*self.storage, name, node_id).await.map(|row| row.is_some()).map_err(|e| match e {
                    DeletedRefused::NameHeld(held) => RowRefused::NameHeld(held),
                    DeletedRefused::Unwritten(why) => RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: nodes.storage refused the Deleted record of path {name_path:?}, name {name_name:?} ({node_id}): {why}", me_path = &me.path, me_name = &me.name, name_path = &name.path, name_name = &name.name)),
                })
            },
            span.clone(),
        )
        .await;
        match &r {
            Ok(true) => span.record("outcome", "row-deleted"),
            // The node wrote no record (Q356): none holds its node.id or name, and nothing is written.
            Ok(false) => span.record("outcome", "no-record"),
            Err(e) => span.record("outcome", e.outcome()).record("detail", e.to_string().as_str()),
        };
        span.in_scope(|| tracing::info!("node.delete: the Deleted record decided; the record stays and the name is released"));
        r.map(|_| ())
    }
}

/// The record at `name` (node-identity-and-state.md §2: "A current-pair lookup reads the node records
/// by their path and name"): the records standing at `name` ([`NodesStorage::records_at`]) folded by the
/// one resolver every reader of a pair goes through ([`rdm_node::pair_record::resolve`]). `None` when
/// none holds it (a `Deleted` record frees its pair).
pub async fn record_at(storage: &dyn NodesStorage, name: &PathName) -> Result<Option<NodeRecord>, crate::storage_error::StorageError> {
    rdm_node::pair_record::resolve(storage.records_at(name).await?, name)
}

/// [`record_at`] as a method of any nodes store.
#[async_trait::async_trait]
pub trait RecordAt {
    /// The record at `name` ([`record_at`]).
    async fn record_at(&self, name: &PathName) -> Result<Option<NodeRecord>, crate::storage_error::StorageError>;
}

#[async_trait::async_trait]
impl<T: NodesStorage + ?Sized> RecordAt for T {
    async fn record_at(&self, name: &PathName) -> Result<Option<NodeRecord>, crate::storage_error::StorageError> {
        rdm_node::pair_record::resolve(self.records_at(name).await?, name)
    }
}

/// Check `name` for `node_id` against the node records at runtime (node-name-update.md, "Check and
/// move": `NameHeld { by: node.id }`; a runtime read, Q237 A): another node's live record at `name`
/// refuses it by name, and nothing is written; a `Deleted` or `Withdrawn` record frees its pair.
pub async fn check_name(storage: &dyn NodesStorage, name: &PathName, node_id: &NodeId) -> Result<Result<(), NameHeld>, crate::storage_error::StorageError> {
    Ok(match record_at(storage, name).await? {
        Some(held) if held.node_id != *node_id => Err(NameHeld { path: name.path.clone(), name: name.name.clone(), by: held.node_id }),
        _ => Ok(()),
    })
}

/// Whether `row`'s node released its name: its record is `Deleted` (or `Withdrawn`), so another node's
/// record may take the name.
pub fn released_its_name(row: &NodeRecord) -> bool {
    !rdm_node::pair_record::holds_its_name(row)
}

/// Whether the record of this exact node proves deletion. A create at a historical location says
/// nothing about the old node: it may have been renamed and still be live. An absent exact-ID record is
/// unknown, not deletion evidence. `name` is diagnostic only.
pub async fn deleted_birth(storage: &dyn NodesStorage, name: &PathName, node_id: &NodeId) -> Result<Option<String>, crate::storage_error::StorageError> {
    let own = storage.node(node_id).await?.map(|l| l.record);
    if let Some(row) = own.as_ref().filter(|r| released_its_name(r)) {
        return Ok(Some(format!("path {:?}, name {:?} (node {node_id}) is a deleted birth: its exact record is {:?}", name.path, name.name, row.state)));
    }
    // Re-created at its path and name (a fabric recovery whose old writer never wrote the record
    // Deleted): another node's live record holds the name its record still stands at (Q181).
    let Some(own) = own.filter(|r| r.location() == *name) else { return Ok(None) };
    Ok(storage.nodes(&own.mesh()).await?.into_iter().find(|r| r.location() == *name && r.node_id != *node_id && !released_its_name(r)).map(|by| format!("path {:?}, name {:?} (node {node_id}) is a deleted birth: node {} holds the name", name.path, name.name, by.node_id)))
}

/// Why a `Deleted` record was not written.
#[derive(Debug)]
pub enum DeletedRefused {
    /// Another node's live record holds the name.
    NameHeld(NameHeld),
    /// The record could not be written: why.
    Unwritten(String),
}

impl std::fmt::Display for DeletedRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NameHeld(held) => write!(f, "name-held: {held}"),
            Self::Unwritten(why) => f.write_str(why),
        }
    }
}

/// The `Deleted` record of `node_id` at `name` (node-delete.md step 3): the node's latest line, its
/// fields kept, with state `Deleted`, written whole: the `Deleted` record frees the pair. Another node's
/// live record at `name` refuses it by name and nothing is written. A node that never wrote its record
/// (a launch killed before its join answered, Q356) has none to delete: nothing is written, no record
/// holds its node.id or name, and the delete goes on (`None`), as [`NodeRows::deleting`] leaves it
/// unchanged.
pub async fn write_deleted(storage: &dyn NodesStorage, name: &PathName, node_id: &NodeId) -> Result<Option<NodeRecord>, DeletedRefused> {
    let unwritten = |e: crate::storage_error::StorageError| DeletedRefused::Unwritten(e.to_string());
    if let Some(by) = record_at(storage, name).await.map_err(unwritten)?.filter(|by| by.node_id != *node_id) {
        return Err(DeletedRefused::NameHeld(NameHeld { path: name.path.clone(), name: name.name.clone(), by: by.node_id }));
    }
    let Some(mut row) = storage.node(node_id).await.map_err(unwritten)?.map(|l| l.record) else {
        return Ok(None);
    };
    if row.state == NodeStatus::Deleted {
        return Ok(Some(row));
    }
    row.state = NodeStatus::Deleted;
    storage.put_node(&row).await.map_err(unwritten)?;
    Ok(Some(row))
}

/// The record of the exact birth `(node_id, incarnation)`: the node's latest record when it is of that
/// incarnation (a point read, Q237 A); `None` when the node wrote none, or its record is of another
/// incarnation.
pub async fn birth_record(storage: &dyn NodesStorage, node_id: &NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<NodeRecord>, crate::storage_error::StorageError> {
    Ok(storage.node(node_id).await?.map(|l| l.record).filter(|r| r.incarnation == *incarnation))
}
