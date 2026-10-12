//! The node rows of a mesh, written by that mesh's primary node-admin (R13; ops-naming "Node
//! data: meta, config and exe"; lifecycles/node-create.md step 1; node-decommission.md step 3;
//! fabric-node-lifecycle.md §2.7: "It writes its own members' `Node` rows; no other node-admin
//! writes them").
//!
//! The writer of a mesh's rows is that mesh's primary node-admin; the fabric-primary writes them
//! only as the acting primary of a mesh with no primary. Every other admin refuses by name
//! (`not-mesh-primary`), another mesh's primary included, and no node writes its own row. Writers
//! never share a key: a row's `path.name` is `<mesh>.<kind>.<ordinal>`, so every key is
//! mesh-scoped by construction. The check reads the writer's own view at the moment of the write.
//!
//! The rows are written where the work is done, by the executor of the Build (`executor_for`
//! names the mesh's primary, or the fabric-primary for a mesh with none), from the exact record it
//! acts on:
//!
//! - `node.create`: the name is claimed at the start of `PrepareStorage`, once the
//!   `AllocateIdentity` receipt names the node id, and before anything is prepared or launched
//!   ([`NodeRows::claim`]): insert-and-fail inside the store adapter,
//!   `NameHeld { by }` and nothing launched. The row's config is seeded from the previous row at
//!   the `path.name` (a `Deleted` row) or the `<name>.old` row. A create that fails after its claim
//!   withdraws it ([`NodeRows::withdraw`]), the holder's own key only.
//! - A replace's `RenamePredecessor`: the predecessor's row moves to `<path>.old` and it lets go
//!   of `<path>` ([`NodeRows::moved_to_old`]).
//! - `NodeDeleted`: the row with status `Deleted`, the record and its config kept
//!   ([`NodeRows::deleted`]).
//! - A recovery's `node.update` (node-recovery.md step 6): the new node's row, put blind with the
//!   caller-owned meta of the exited birth's `Deleted` row ([`NodeRows::copy_caller_owned`]); the
//!   one write of a row that keeps its name is [`put_meta`], which `node.update`'s door makes too.
//! - A mesh delete (R-MN1, mesh-delete.md steps 3 and 7): each member's row `Deleted` on its exact
//!   Exited receipt, by the deleting mesh's primary, and the final primary's in its own node.delete,
//!   by the fabric-primary as acting primary of a mesh with no primary; a birth the view no longer
//!   names is written from this writer's record of its row ([`NodeRows::deleted_birth`]).
//! - A birth no create of this writer claimed (the bootstrap admin, a birth made before this
//!   admin took its mesh's seat): written when the writer's view first holds it
//!   ([`NodeRows::arrivals`]); an admin that takes a mesh's seat writes every member's row of that
//!   mesh, and a row a node already holds is never overwritten.
//!
//! The fabric-primary admits `node.delete` of a parked predecessor at `<path>.old` from the record
//! it holds of it ([`NodeRows::parked_at`]): no view names that path but the replace's executor's.
//! It holds that record from the replace's frames as its drive reads them ([`NodeRows::on_step`]),
//! and writes nothing from them.

use crate::model::{IncarnationId, Node, NodeId, NodeKind, NodeStatus, PathName};
use crate::storage::{Allocation, NameHeld, NodeRow, NodesStorage};
use crate::topology::Topology;
use rafka_node_admin_client::NodeMeta;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

/// The writer of a mesh's node rows, and the fabric-primary's record of parked predecessors.
pub struct NodeRows {
    /// This admin's `path.name`, the name its spans and refusals carry.
    me: PathName,
    /// The node id this admin was born with: it writes a mesh's rows only while its view names that
    /// birth the mesh's primary (never by path.name: a replace keeps the path.name and mints a new
    /// node id).
    me_id: NodeId,
    /// This admin's view, read at each write to decide whether it is the writer of the row's mesh.
    view: Arc<RwLock<Topology>>,
    storage: Arc<dyn NodesStorage>,
    /// The config door a seeded birth is told its config through (`crate::node_config`).
    configs: Option<crate::node_config::NodeConfigsSlot>,
    /// The record held when each removal was announced (`NodeDeleting`), by operation, until the
    /// replace's rename or the removal's `NodeDeleted` (the fabric-primary's drive).
    announced: Mutex<HashMap<String, Node>>,
    /// Per mesh whose rows this admin writes, the births its views have held in that tenure: each
    /// row is written once, when a view first holds it, and a later view install that holds it
    /// again is not an arrival.
    held: Mutex<BTreeMap<String, HashSet<(NodeId, IncarnationId)>>>,
    /// The record of each parked predecessor a replace left at `<path>.old`, by that `.old` path,
    /// from the replace's `RenamePredecessor` until its `NodeDeleted`: the fabric-primary's held
    /// record of a node no view names, which admits its `node.delete`.
    parked_old: Mutex<HashMap<PathName, Node>>,
    /// This writer's record of each node's row: what it last wrote for the node, or found the node
    /// holding at a claim. A later row of the node takes its config from here, never from a record
    /// that does not carry it.
    written: Arc<WrittenRows>,
}

/// A writer's record of the rows it holds authoritatively (fabric-node-lifecycle.md "A rewrite is
/// lossless for every field its writer knows"): per node, the row the writer last put for it (a
/// claim, an arrival, a rename, a config put, a Deleted row) or found the node holding at a claim.
/// The writer of a mesh's rows and its config door share one record, so a config put through the
/// door is what the node's next row (its `<path>.old` move, its `Deleted` row) carries. It is fed by
/// this writer's own puts, and by the point read a claim or an arrival makes of the row at the
/// node's `path.name` (R13: rebuild after a seat change reads the mesh's own rows): a row the node
/// already holds is held as found. The move and `Deleted` rows are written from this record; for a
/// node it holds no record of, the one read is the same point read of the row the node holds
/// ([`WrittenRows::known_row`]), for the tags no executor record carries. A `Deleted` row stays held
/// for its node: a rerun of the delete writes the same row.
#[derive(Debug, Default)]
pub struct WrittenRows(Mutex<HashMap<NodeId, NodeRow>>);

impl WrittenRows {
    /// Hold `row` as the writer's record of its node.
    pub fn hold(&self, row: &NodeRow) {
        self.0.lock().unwrap().insert(row.node_id.clone(), row.clone());
    }

    /// The node let go of its row (a withdrawn claim): the writer holds no record of it.
    pub fn forget(&self, node_id: &NodeId) {
        self.0.lock().unwrap().remove(node_id);
    }

    /// The config the writer holds for `node_id`, if it holds a record of it.
    pub fn config_of(&self, node_id: &NodeId) -> Option<BTreeMap<String, String>> {
        self.0.lock().unwrap().get(node_id).map(|r| r.config.clone())
    }

    /// The row the writer holds for `node_id`, if it holds a record of it.
    pub fn row_of(&self, node_id: &NodeId) -> Option<NodeRow> {
        self.0.lock().unwrap().get(node_id).cloned()
    }

    /// The row of `node_id` of `mesh` as this writer knows it: its record, else the row the node
    /// holds now, a point read through the node row's id index and then the row's own key (R13:
    /// "Rebuild after a seat change reads the mesh's own rows", the same point read a claim or an
    /// arrival makes), held as found. `None`: the node holds no row. A read that fails is named.
    pub async fn known_row(&self, storage: &dyn NodesStorage, mesh: &str, node_id: &NodeId) -> Result<Option<NodeRow>, String> {
        if let Some(row) = self.row_of(node_id) {
            return Ok(Some(row));
        }
        let Some(name) = storage.node_row_name(mesh, node_id).await.map_err(|e| format!("the id index entry of {node_id} in {mesh} could not be read: {e}"))? else { return Ok(None) };
        let row = storage.node_row_at(&name).await.map_err(|e| format!("the row of {node_id} at {name} could not be read: {e}"))?.filter(|r| r.node_id == *node_id);
        if let Some(r) = &row {
            self.hold(r);
        }
        Ok(row)
    }
}

/// `tags` as a span carries them: the labels, comma-joined.
pub(crate) fn labels(tags: &std::collections::BTreeSet<String>) -> String {
    tags.iter().map(String::as_str).collect::<Vec<_>>().join(",")
}

/// Why the writer refused a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowRefused {
    /// This admin is not the writer of the row's mesh: why, naming the mesh and its writer.
    NotMeshPrimary(String),
    /// Another node's live row holds the name: the store adapter's insert-and-fail refused it and
    /// nothing was written.
    NameHeld(NameHeld),
    /// The row could not be read or written: why, whole.
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

/// What a claim of a name did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claimed {
    /// The row was written at the name: the claim is new and the claimant may withdraw it.
    Written {
        /// The row written.
        row: NodeRow,
        /// The node whose previous row seeded its config, if one did.
        seeded_from: Option<NodeId>,
    },
    /// The node already holds the row at the name (a restart, or a claim made by an earlier attempt
    /// of the same create): nothing is written, and the row stays its own.
    HeldByNode(NodeRow),
}

/// Whether `me_id` writes `mesh`'s node rows in view `t`: the mesh's primary node-admin, or the
/// fabric-primary while the mesh has no primary. A stopped seat holder is no primary (R-NS1 item 2:
/// from the stop of a mesh's last admin the fabric-primary is the writer of a mesh with no primary):
/// it writes nothing, stopped, and the fabric-primary writes as acting primary (mesh-delete.md step
/// 7, the final primary's own row Deleted in its node.delete).
pub fn writes_for(t: &Topology, me_id: &NodeId, mesh: &str) -> bool {
    t.holds_mesh_seat(mesh, me_id) || (t.holds_fabric_seat(me_id) && t.cohort_primary(mesh, NodeKind::NodeAdmin).filter(|p| !p.parked).is_none())
}

impl NodeRows {
    /// The writer of the admin `me`, born as `me_id`, deciding over its view `view`, writing to
    /// `storage`, its nodes store.
    pub fn new(me: PathName, me_id: NodeId, view: Arc<RwLock<Topology>>, storage: Arc<dyn NodesStorage>) -> Self {
        Self { me, me_id, view, storage, configs: None, announced: Mutex::new(HashMap::new()), held: Mutex::new(BTreeMap::new()), parked_old: Mutex::new(HashMap::new()), written: Arc::new(WrittenRows::default()) }
    }

    /// Share `written`, the record the config door holds its puts in, as this writer's record.
    pub fn with_written(mut self, written: Arc<WrittenRows>) -> Self {
        self.written = written;
        self
    }

    /// The config of the row `record`'s node is written with: the one this writer holds for it,
    /// else the record's own; and which of the two it is (`writer-record`, `executor-record`).
    fn known_config(&self, record: &Node) -> (BTreeMap<String, String>, &'static str) {
        match self.written.config_of(&record.node_id) {
            Some(config) => (config, "writer-record"),
            None => (record.config.clone(), "executor-record"),
        }
    }

    /// Tell each birth whose row is seeded its config through `configs`, the config door.
    pub fn with_configs(mut self, configs: crate::node_config::NodeConfigsSlot) -> Self {
        self.configs = Some(configs);
        self
    }

    /// The store the rows are written to.
    pub fn storage(&self) -> &Arc<dyn NodesStorage> {
        &self.storage
    }

    /// The record this fabric-primary holds of the parked predecessor at `old` (a `<path>.old`): the
    /// exact birth a `node.delete` of that path is fenced to.
    pub fn parked_at(&self, old: &PathName) -> Option<Node> {
        self.parked_old.lock().unwrap().get(old).cloned()
    }

    /// Why this admin does not write `mesh`'s rows in view `t` (the row at `subject`), naming the
    /// writer the view holds; `None` when it is the writer.
    fn not_writer(&self, t: &Topology, mesh: &str, subject: &PathName) -> Option<String> {
        if writes_for(t, &self.me_id, mesh) {
            return None;
        }
        let writer = match (t.cohort_primary(mesh, NodeKind::NodeAdmin), t.fabric_primary()) {
            (Some(p), _) => format!("{mesh}'s primary node-admin {} ({}) writes them", p.name, p.node_id),
            (None, Some(fp)) => format!("{mesh} has no primary node-admin, and the fabric-primary {} ({}) writes them for it", fp.name, fp.node_id),
            (None, None) => format!("{mesh} has no primary node-admin and the view holds no fabric-primary to write them for it"),
        };
        Some(format!("{} ({}) is not the writer of {mesh}'s node rows and refuses the row at {subject}: {writer}", self.me, self.me_id))
    }

    /// The writer check at the moment of a write, over this admin's view as it is now.
    async fn writer_check(&self, mesh: &str, subject: &PathName) -> Result<(), RowRefused> {
        let t = self.view.read().await;
        match self.not_writer(&t, mesh, subject) {
            Some(why) => Err(RowRefused::NotMeshPrimary(why)),
            None => Ok(()),
        }
    }

    /// `node.create`'s name claim (lifecycles/node-create.md step 1): at the start of `PrepareStorage`,
    /// once the `AllocateIdentity` receipt names `node_id` and before anything is prepared or
    /// launched for the birth, the writer of the mesh's rows puts the birth's row at `name`, status
    /// `Pending`, its config seeded from the previous row at the `path.name` (a `Deleted` row) or the
    /// `<name>.old` row. The store adapter's insert-and-fail
    /// refuses a name another node's live row holds, `NameHeld { by }`, and nothing is written. A row
    /// the node holds already is left as it is. `allocated`: what the create allocated for the birth,
    /// written into the claim row with it ([`NodeRow::allocated`]), so the identity the claim is made
    /// with is durable wherever the claim is. One `via-node-row` span, `moment` `claim`.
    pub async fn claim(&self, name: &PathName, node_id: &NodeId, operation: &str, allocated: Option<&Allocation>) -> Result<Claimed, RowRefused> {
        let span = tracing::info_span!(
            "rdm.node_admin.node.create.via-node-row",
            node = %self.me,
            subject = %name,
            node_id = %node_id,
            operation,
            moment = "claim",
            seeded_from = tracing::field::Empty,
            keys = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let r = tracing::Instrument::instrument(
            async {
                self.writer_check(&name.mesh, name).await?;
                self.put_seeded(name, node_id, NodeStatus::Pending, &BTreeMap::new(), allocated, &span).await
            },
            span.clone(),
        )
        .await;
        record_claim(&span, &r);
        span.in_scope(|| tracing::info!("node.create: the name claim decided"));
        r
    }

    /// Hold the row `node_id` holds at `name` as this writer's record of it, read from the store (R13:
    /// "Rebuild after a seat change reads the mesh's own rows"), the same point read a claim or an
    /// arrival makes: a mesh leave's roster birth this writer's views never held (the admin created
    /// for a leaving mesh with no admin left joined after that birth stopped), whose `Deleted` row it
    /// is about to write. Nothing is written. `Ok(true)`: the row is held; `Ok(false)`: the store
    /// holds no row of this node at `name`. `Err` names the read that failed.
    pub async fn hold_row_of(&self, name: &PathName, node_id: &NodeId) -> Result<bool, String> {
        let row = self.storage.node_row_at(name).await.map_err(|e| format!("{}: the row at {name} could not be read for {node_id}: {e}", self.me))?;
        match row.filter(|r| r.node_id == *node_id) {
            Some(row) => {
                self.written.hold(&row);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// The birth an earlier run of `operation` of the Build `build_id` claimed `name` for, with what
    /// that run allocated ([`NodeRow::allocated`]): a point read of the row at `name` through its
    /// `path.name` key. `Some` only when the row there holds the name (it is not `Deleted`) and its
    /// claim was made by that same operation of that same Build; any other row, or none, answers
    /// `None`. Nothing is written. A read that fails is named.
    pub async fn allocation_at(&self, name: &PathName, build_id: &crate::build::BuildId, operation: &str) -> Result<Option<(NodeId, Allocation)>, String> {
        let row = self.storage.node_row_at(name).await.map_err(|e| format!("{}: the row at {name} could not be read for the allocation of {operation} (Build {build_id}): {e}", self.me))?;
        Ok(row.filter(|r| r.status != NodeStatus::Deleted).and_then(|r| r.allocated.filter(|a| &a.build_id == build_id && a.operation == operation).map(|a| (r.node_id, a))))
    }

    /// A create that claimed `name` for `node_id` and did not complete lets go of its claim, so the
    /// next birth at the name is not refused by it: a release of the holder's own key only, never
    /// another node's row. One `via-claim-withdrawn` span says why.
    pub async fn withdraw(&self, name: &PathName, node_id: &NodeId, operation: &str, why: &str) {
        let span = tracing::info_span!(
            "rdm.node_admin.node.remove.via-claim-withdrawn",
            node = %self.me,
            subject = %name,
            node_id = %node_id,
            operation,
            why,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let r = match self.writer_check(&name.mesh, name).await {
            Err(e) => Err(e),
            Ok(()) => self.storage.release_node_row(name, node_id).await.map_err(|e| RowRefused::Unwritten(format!("{}: nodes.storage refused the release of {name} by {node_id}: {e}", self.me))),
        };
        if let Ok(true) = r {
            self.written.forget(node_id);
        }
        match &r {
            Ok(true) => span.record("outcome", "withdrawn"),
            Ok(false) => span.record("outcome", "not-held").record("detail", format!("{}: {node_id} holds no row at {name}; nothing is released", self.me).as_str()),
            Err(e) => span.record("outcome", e.outcome()).record("detail", e.to_string().as_str()),
        };
        span.in_scope(|| tracing::info!("node.create: the claim is withdrawn"));
    }

    /// A replace's `RenamePredecessor` (node-replace.md 2): the predecessor `record` (held at its
    /// `path.name`) gives up the name, its row moved to `<path>.old`, parked, a blind put of its
    /// record, then it lets go of `<path>`. One `via-rename` span.
    pub async fn moved_to_old(&self, record: &Node, operation: &str) -> Result<(), RowRefused> {
        let name = PathName { old: false, ..record.name.clone() };
        let to = name.renamed();
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-rename",
            node = %self.me,
            node_id = %record.node_id,
            from = %name,
            to = %to,
            operation,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let r: Result<Moved, RowRefused> = tracing::Instrument::instrument(
            async {
                self.writer_check(&name.mesh, &name).await?;
                let parked = parked_record(record, &to);
                let (config, _) = self.known_config(&parked);
                let tags = self.known_tags(&parked).await?;
                let (moved, written) = move_row(&*self.storage, &self.me, &parked, config, tags, &name, &to).await.map_err(|e| match e {
                    RenameRefused::NameHeld(h) => RowRefused::NameHeld(h),
                    RenameRefused::Unwritten(why) => RowRefused::Unwritten(why),
                })?;
                if let Some(row) = &written {
                    self.written.hold(row);
                }
                Ok(moved)
            },
            span.clone(),
        )
        .await;
        match &r {
            Ok(Moved::Renamed) => {
                span.record("outcome", "renamed");
            }
            Ok(Moved::AlreadyMoved) => {
                span.record("outcome", "already-moved").record("detail", format!("{}: the predecessor {} holds {to} and no longer holds {name}; nothing is written", self.me, record.node_id).as_str());
            }
            Err(e) => {
                span.record("outcome", e.outcome()).record("detail", e.to_string().as_str());
            }
        }
        span.in_scope(|| tracing::info!("node.replace: the predecessor's row moved"));
        r.map(|_| ())
    }

    /// `NodeDeleted` (node-decommission.md step 3): the row at the node's `path.name` (its `.old`
    /// path for a parked predecessor) with status `Deleted`, the exact record the executor acted on
    /// and its config kept, a blind put. One `via-node-row` span.
    pub async fn deleted(&self, record: &Node, operation: &str) -> Result<(), RowRefused> {
        let (config, config_from) = self.known_config(record);
        let tags = match self.known_tags(record).await {
            Ok(tags) => tags,
            Err(e) => return self.deleted_row(&record.name, &record.node_id, Err(e), operation).await,
        };
        self.deleted_row(&record.name, &record.node_id, Ok(Some((config, tags, config_from))), operation).await
    }

    /// A recovery's `node.update` (node-recovery.md step 6: "node[2].update copies the exited node's
    /// caller-owned meta (such as `otel_debug`) as a blind put; config was seeded by the create"):
    /// the caller-owned group of the exited birth `from`, as the `Deleted` row it left holds it
    /// ([`NodesStorage::exited_row`], a point read by the exited node's id: the new node's claim
    /// replaced the row at the path.name, not that row), copied onto the new node `onto` the create
    /// made at its path.name, through [`NodeMeta::with_caller_owned_of`] (the declared copy: nothing
    /// RDM owns moves, the name least of all), and put as a blind write of the new node's row
    /// ([`put_meta`]), one change to a node record at a time with the config door. The row carries
    /// the tag labels; `otel_debug` has no row field, so nothing of it is put
    /// ([`rafka_node_admin_client::CallEnd::NotBackedToday`] says so to a caller that puts it).
    ///
    /// The read is the store's, so it holds whichever executor runs the step: the one that wrote the
    /// `Deleted` row or the one that took its seat. A store that holds none refuses by name, writing
    /// nothing. One `via-caller-owned-copy` span names the exited birth and the labels copied.
    pub async fn copy_caller_owned(&self, onto: &Node, from: &Node, operation: &str) -> Result<NodeRow, RowRefused> {
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-caller-owned-copy",
            node = %self.me,
            subject = %onto.name,
            node_id = %onto.node_id,
            from_node_id = %from.node_id,
            operation,
            tags = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let r = tracing::Instrument::instrument(self.copy_caller_owned_row(onto, from), span.clone()).await;
        match &r {
            Ok(row) => {
                span.record("tags", labels(&row.tags).as_str()).record("outcome", "meta-copied");
            }
            Err(e) => {
                span.record("outcome", e.outcome()).record("detail", e.to_string().as_str());
            }
        }
        span.in_scope(|| tracing::info!("node.update: the caller-owned meta of the exited birth is put on the new node's row"));
        r
    }

    async fn copy_caller_owned_row(&self, onto: &Node, from: &Node) -> Result<NodeRow, RowRefused> {
        let me = &self.me;
        self.writer_check(&onto.name.mesh, &onto.name).await?;
        let deleted = match self.storage.exited_row(&from.mesh, &from.node_id).await {
            Ok(Some(row)) => row,
            Ok(None) => {
                return Err(RowRefused::Unwritten(format!(
                    "{me}: nodes.storage holds no Deleted row of the exited birth {} at {}: its caller-owned meta is not copied onto {} and nothing is written",
                    from.node_id, from.name, onto.node_id
                )))
            }
            Err(e) => return Err(RowRefused::Unwritten(format!("{me}: the Deleted row of the exited birth {} could not be read: {e}: nothing is written", from.node_id))),
        };
        let exited = NodeMeta { tags: deleted.tags.clone(), ..crate::node_update::meta_of(from) };
        let copied = crate::node_update::meta_of(onto).with_caller_owned_of(&exited);
        // One change to a node record at a time (node-update.md "Config update"), with the door's puts.
        let door = self.configs.as_ref().and_then(|slot| slot.get().cloned());
        let _one = match &door {
            Some(door) => Some(door.one_at_a_time().await),
            None => None,
        };
        let new = match self.written.known_row(&*self.storage, &onto.mesh, &onto.node_id).await {
            Ok(Some(row)) => row,
            Ok(None) => return Err(RowRefused::Unwritten(format!("{me} holds no row of the new node {} at {}: its claim is the row the exited birth's caller-owned meta is put on, and nothing is written", onto.node_id, onto.name))),
            Err(why) => return Err(RowRefused::Unwritten(format!("{me}: {why}: the new node's row is not put and nothing is written"))),
        };
        match put_meta(&*self.storage, &self.written, &new.name, &onto.node_id, new.status, new.config, copied.tags).await {
            Ok(Ok(row)) => Ok(row),
            Ok(Err(held)) => Err(RowRefused::NameHeld(held)),
            Err(e) => Err(RowRefused::Unwritten(format!("{me}: nodes.storage refused the row of {} ({}): {e}", onto.name, onto.node_id))),
        }
    }

    /// The tags of the row `record`'s node is written with: the ones this writer holds for it, else
    /// those of the row the node holds now (a point read through the id index); none when it holds
    /// no row. A node's tags are never in the executor's record: they are meta only its rows carry.
    async fn known_tags(&self, record: &Node) -> Result<std::collections::BTreeSet<String>, RowRefused> {
        match self.written.known_row(&*self.storage, &record.mesh, &record.node_id).await {
            Ok(row) => Ok(row.map(|r| r.tags).unwrap_or_default()),
            Err(why) => Err(RowRefused::Unwritten(format!("{}: {why}: the row of {} is not written without the tags it carries", self.me, record.name))),
        }
    }

    /// The `Deleted` row of a birth the executor holds no record of any more (a mesh leave's
    /// member that departed the view after its exact Exited receipt, mesh-leave.md steps 3 and
    /// 7): the row at `name` for `node_id`, the config this writer holds for the node. A writer that
    /// holds no record of the node's row refuses by name and writes nothing: a `Deleted` row is never
    /// written with a config the writer does not know. One `via-node-row` span.
    pub async fn deleted_birth(&self, name: &PathName, node_id: &NodeId, operation: &str) -> Result<(), RowRefused> {
        let known = self.written.row_of(node_id).map(|r| (r.config, r.tags, "writer-record"));
        self.deleted_row(name, node_id, Ok(known), operation).await
    }

    /// The `Deleted` row of `node_id` at `name`, written from what the writer knows of it: its
    /// config and tags (the row keeps both, as it keeps the record), or the refusal that names why
    /// it knows neither. The store tombstones every tags index row of its labels with the write.
    async fn deleted_row(&self, name: &PathName, node_id: &NodeId, known: Result<Option<(BTreeMap<String, String>, std::collections::BTreeSet<String>, &'static str)>, RowRefused>, operation: &str) -> Result<(), RowRefused> {
        let span = tracing::info_span!(
            "rdm.node_admin.node.delete.via-node-row",
            node = %self.me,
            subject = %name,
            operation,
            node_id = %node_id,
            keys = tracing::field::Empty,
            config_from = tracing::field::Empty,
            tags = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        if let Ok(Some((config, tags, config_from))) = &known {
            span.record("keys", config.len() as u64).record("config_from", *config_from).record("tags", labels(tags).as_str());
        }
        let r = tracing::Instrument::instrument(
            async {
                self.writer_check(&name.mesh, name).await?;
                let Some((config, tags, _)) = known? else {
                    return Err(RowRefused::Unwritten(format!("{}: holds no record of the row of {name} ({node_id}) and no executor record of it: its Deleted row is not written with a config this writer does not know", self.me)));
                };
                let row = NodeRow { name: name.clone(), node_id: node_id.clone(), status: NodeStatus::Deleted, config, tags, allocated: None };
                match self.storage.put_node_row(&row).await {
                    Ok(Ok(())) => {
                        self.written.hold(&row);
                        Ok(())
                    }
                    Ok(Err(held)) => Err(RowRefused::NameHeld(held)),
                    Err(e) => Err(RowRefused::Unwritten(format!("{}: nodes.storage refused the Deleted row of {name} ({node_id}): {e}", self.me))),
                }
            },
            span.clone(),
        )
        .await;
        match &r {
            Ok(()) => span.record("outcome", "row-deleted"),
            Err(e) => span.record("outcome", e.outcome()).record("detail", e.to_string().as_str()),
        };
        span.in_scope(|| tracing::info!("node.delete: the Deleted row decided; the record, its config and its tags stay, its labels leave the tags index"));
        r
    }

    /// The row of `node_id` at `name`, `status`, its config seeded from the previous row at the
    /// `path.name` (another node's row there: the store adapter refuses it unless it is `Deleted`),
    /// else the `<name>.old` row, else `own`. A row the same node holds already is left as it is.
    async fn put_seeded(&self, name: &PathName, node_id: &NodeId, status: NodeStatus, own: &BTreeMap<String, String>, allocated: Option<&Allocation>, span: &tracing::Span) -> Result<Claimed, RowRefused> {
        let me = &self.me;
        let previous = self.storage.node_row_at(name).await.map_err(|e| RowRefused::Unwritten(format!("{me}: the previous row at {name} could not be read for {node_id}: {e}")))?;
        if let Some(p) = previous.as_ref().filter(|p| p.node_id == *node_id) {
            self.written.hold(p);
            return Ok(Claimed::HeldByNode(p.clone()));
        }
        let seed = match previous {
            Some(p) => Some(p),
            None => self.storage.node_row_at(&name.renamed()).await.map_err(|e| RowRefused::Unwritten(format!("{me}: the .old row at {} could not be read for {node_id}: {e}", name.renamed())))?.filter(|p| p.node_id != *node_id),
        };
        let config = seed.as_ref().map_or_else(|| own.clone(), |p| p.config.clone());
        if let Some(p) = &seed {
            span.record("seeded_from", p.node_id.as_str());
        }
        span.record("keys", config.len() as u64);
        // A new node carries no tags: they are its caller's meta, set through node.update (a
        // recovery's copy of the caller-owned meta is its own step).
        let row = NodeRow { name: name.clone(), node_id: node_id.clone(), status, config, tags: Default::default(), allocated: allocated.cloned() };
        match self.storage.put_node_row(&row).await {
            Ok(Ok(())) => {
                self.written.hold(&row);
                Ok(Claimed::Written { row, seeded_from: seed.map(|p| p.node_id) })
            }
            Ok(Err(held)) => Err(RowRefused::NameHeld(held)),
            Err(e) => Err(RowRefused::Unwritten(format!("{me}: nodes.storage refused the row of {name} ({node_id}): {e}"))),
        }
    }

    /// A completed step of a Build the fabric-primary drives, over its held view, handled once per
    /// drive: its live `Step` frame, or its receipt read back. It writes nothing: it holds the record
    /// of a replace's predecessor from the replace's first step, and the parked predecessor at
    /// `<path>.old` from its `RenamePredecessor` until its delete's `NodeDeleted`, which is what
    /// admits that delete ([`Self::parked_at`]). `subject` is the node a receipt names; a live frame
    /// names none, and the node is the one the view holds at the operation's `path.name`.
    pub async fn on_step(&self, view: &RwLock<Topology>, operation: &str, step: &str, subject: Option<&NodeId>) {
        let Some((verb, path)) = operation.split_once(':') else { return };
        let Ok(name) = path.parse::<PathName>() else { return };
        let is_subject = |n: &Node| subject.is_none_or(|id| &n.node_id == id);
        match (verb, step) {
            ("replace-node", "NodeDeleting" | "DrainNode") => {
                let held = match subject {
                    Some(id) => held_by(view, id).await,
                    None => held_at(view, &name).await,
                };
                if let Some(n) = held {
                    self.announced.lock().unwrap().entry(operation.to_string()).or_insert(n);
                }
            }
            ("replace-node", "RenamePredecessor") => {
                let to = name.renamed();
                let announced = self.announced.lock().unwrap().get(operation).cloned().filter(|n| is_subject(n));
                let record = match announced {
                    Some(n) => Some(n),
                    None => match subject {
                        Some(id) => held_by(view, id).await,
                        None => match held_at(view, &to).await {
                            Some(n) => Some(n),
                            None => held_at(view, &name).await,
                        },
                    },
                };
                if let Some(n) = record {
                    self.parked_old.lock().unwrap().entry(to.clone()).or_insert_with(|| parked_record(&n, &to));
                }
            }
            ("delete-node", "NodeDeleted") => {
                self.parked_old.lock().unwrap().retain(|old, n| !(old == &name && is_subject(n)));
            }
            ("replace-node", "Complete" | "NodeDeleted") => {
                self.announced.lock().unwrap().remove(operation);
            }
            _ => {}
        }
    }

    /// The births of `now`, the view being installed over `prev`, whose rows this admin writes: the
    /// members of each mesh it writes for in `now` (its primary, or the fabric-primary's for a mesh
    /// with none), never its own birth. Every member of a mesh whose writing it took with this
    /// install (the rebuild after a seat change, which never overwrites a row a node holds); else
    /// each birth of that mesh this tenure's views have not held before. A birth is an arrival once
    /// per tenure: one the view dropped and holds again was written when it first arrived. Read
    /// under the view lock, so of two installs each sees the arrivals its own change made.
    pub fn arrivals(&self, prev: &Topology, now: &Topology) -> Vec<Node> {
        if !now.holds_fabric_seat(&self.me_id) {
            self.parked_old.lock().unwrap().clear();
        }
        let me = &self.me_id;
        let mut held = self.held.lock().unwrap();
        held.retain(|mesh, _| writes_for(now, me, mesh));
        let births = |t: &Topology, mesh: &str| t.members().filter(|n| n.mesh == mesh).filter_map(|n| n.incarnation_id.clone().map(|inc| (n.node_id.clone(), inc))).collect::<HashSet<_>>();
        let meshes: std::collections::BTreeSet<String> = now.members().map(|n| n.mesh.clone()).filter(|m| writes_for(now, me, m)).collect();
        let mut out = Vec::new();
        for mesh in meshes {
            let took = !writes_for(prev, me, &mesh);
            // The tenure began with this install (every member arrives), or before this admin
            // installed any view through this door (the births `prev` holds were held already).
            let tenure = held.entry(mesh.clone()).or_insert_with(|| if took { HashSet::new() } else { births(prev, &mesh) });
            if took {
                tenure.clear();
            }
            out.extend(now.members().filter(|n| n.mesh == mesh && n.node_id != *me && n.incarnation_id.clone().is_some_and(|inc| tenure.insert((n.node_id.clone(), inc)))).cloned());
        }
        out
    }

    /// Write the row of each birth in `arrivals`, the writer's view `now` having first held it; a
    /// birth whose row carries a config it does not (seeded from a previous row, or claimed with
    /// one at its create) is told that config.
    pub async fn write_arrivals(&self, now: &Topology, arrivals: Vec<Node>) {
        for birth in arrivals {
            let span = tracing::info_span!(
                "rdm.node_admin.node.create.via-node-row",
                node = %self.me,
                subject = %birth.name,
                node_id = %birth.node_id,
                moment = "arrival",
                seeded_from = tracing::field::Empty,
                keys = tracing::field::Empty,
                outcome = tracing::field::Empty,
                told = tracing::field::Empty,
                detail = tracing::field::Empty,
                "otel.kind" = "internal",
            );
            let r = tracing::Instrument::instrument(self.put_seeded(&birth.name, &birth.node_id, birth.status, &birth.config, None, &span), span.clone()).await;
            record_claim(&span, &r);
            span.in_scope(|| tracing::info!("node.create: the birth's row decided"));
            match r {
                Ok(Claimed::Written { row, .. } | Claimed::HeldByNode(row)) if row.config != birth.config => self.tell(now, &birth, row, &span),
                _ => {}
            }
        }
    }

    /// Tell a birth the config its row carries (ops-naming "Node data": the node applies it and
    /// carries it in its next digest) through the config door, which writes the record and calls
    /// the birth through its mesh-primary. On its own task, under the row write's span: the view
    /// install that found the birth never waits on a call to it (the join door installs the view
    /// before it answers the joiner).
    fn tell(&self, now: &Topology, birth: &Node, row: NodeRow, span: &tracing::Span) {
        let Some(door) = self.configs.as_ref().and_then(|slot| slot.get().cloned()) else {
            span.record("told", "no-config-door");
            return;
        };
        let Some(incarnation) = birth.incarnation_id.clone() else { return };
        span.record("told", "config-door");
        let req = rafka_node_rpc_contract::status::StatusRequest::UpdateNodeConfig { node_id: birth.node_id.clone(), incarnation, config: row.config };
        let (me, me_id, view, storage) = (self.me.clone(), self.me_id.clone(), now.clone(), self.storage.clone());
        tokio::spawn(tracing::Instrument::instrument(
            async move {
                door.update(&me, &me_id, &view, &*storage, &req).await;
            },
            span.clone(),
        ));
    }
}

/// The outcome of a row put on its span.
fn record_claim(span: &tracing::Span, r: &Result<Claimed, RowRefused>) {
    match r {
        Ok(Claimed::Written { seeded_from, .. }) => {
            span.record("outcome", if seeded_from.is_some() { "row-seeded" } else { "row-written" });
        }
        Ok(Claimed::HeldByNode(_)) => {
            span.record("outcome", "row-held-by-node");
        }
        Err(e) => {
            span.record("outcome", e.outcome()).record("detail", e.to_string().as_str());
        }
    }
}


/// Why a node row's move was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenameRefused {
    /// The new name is held by another node: the store adapter's insert-and-fail refused it and
    /// nothing was written.
    NameHeld(crate::storage::NameHeld),
    /// The move could not be written: why, whole.
    Unwritten(String),
}

/// `node.update`'s write of a row that keeps its name (node-update.md step 4): a blind put of the
/// whole row at `name`, carrying `config` and `tags`, which the writer then holds as its record of
/// `node_id`. The store adapter refuses it `NameHeld` when another node's live row holds `name`.
/// The one write of a row's meta: the door's `node.update` and a recovery's copy of the caller-owned
/// meta ([`NodeRows::copy_caller_owned`]) both put here.
pub(crate) async fn put_meta(
    storage: &dyn NodesStorage,
    written: &WrittenRows,
    name: &PathName,
    node_id: &NodeId,
    status: NodeStatus,
    config: BTreeMap<String, String>,
    tags: std::collections::BTreeSet<String>,
) -> Result<Result<NodeRow, NameHeld>, crate::record_store::StorageError> {
    let row = NodeRow { name: name.clone(), node_id: node_id.clone(), status, config, tags, allocated: None };
    Ok(match storage.put_node_row(&row).await? {
        Ok(()) => {
            written.hold(&row);
            Ok(row)
        }
        Err(held) => Err(held),
    })
}

/// The node's row moves from `from` to `to` in `storage`: written at `to` from its held `record`
/// with `config` and `tags` (a put the store adapter refuses `NameHeld` when another node's live row holds
/// `to`; a row the node holds there already is left as it is, its id index entry naming `to`,
/// [`NodesStorage::hold_node_row`]), then the node lets go of `from`
/// (only its own row there goes). The one writer of a rename (R13: `node.update` is one write
/// path): `node.update` at the node's mesh-primary (`crate::node_update`) and a replace's
/// `RenamePredecessor` ([`NodeRows::moved_to_old`]) both move here. Answers what it did and the
/// row written, if it wrote one.
pub(crate) async fn move_row(storage: &dyn NodesStorage, me: &PathName, record: &Node, config: BTreeMap<String, String>, tags: std::collections::BTreeSet<String>, from: &PathName, to: &PathName) -> Result<(Moved, Option<NodeRow>), RenameRefused> {
    if from == to {
        return Ok((Moved::Renamed, None));
    }
    let held_there = storage.hold_node_row(to, &record.node_id).await.map_err(|e| RenameRefused::Unwritten(format!("{me}: the row at {to} could not be read or named for the rename of {} from {from}: {e}", record.node_id)))?;
    let mut written = None;
    if !held_there {
        let row = NodeRow { name: to.clone(), node_id: record.node_id.clone(), status: record.status, config, tags, allocated: None };
        storage.put_node_row(&row).await.map_err(|e| RenameRefused::Unwritten(format!("{me}: nodes.storage refused the row of {} at {to}: {e}", record.node_id)))?.map_err(RenameRefused::NameHeld)?;
        written = Some(row);
    }
    // The new name is the node's: it lets go of the old key.
    let released = storage.release_node_row(from, &record.node_id).await.map_err(|e| RenameRefused::Unwritten(format!("{me}: {} holds {to} and nodes.storage refused the release of {from}: {e}", record.node_id)))?;
    Ok((if held_there && !released { Moved::AlreadyMoved } else { Moved::Renamed }, written))
}

/// What a move of a node's row did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Moved {
    /// The node holds the new name now and let go of the old one.
    Renamed,
    /// The node held the new name already and not the old one: the move was made before.
    AlreadyMoved,
}

/// The record of a replace's predecessor at its rename to `to`: parked there. A live branch's
/// predecessor was held live at its `DrainNode`, and its stop (`StopNode`, `AwaitNodeLeft`) precedes
/// the rename, so it is `Leaving` and parked now; a recovery's predecessor keeps the status it was held
/// with (its exit is what the recovery proves).
fn parked_record(record: &Node, to: &PathName) -> Node {
    let mut n = record.clone();
    n.name = to.clone();
    if n.status.is_live() {
        n.status = NodeStatus::Leaving;
        n.parked = true;
    }
    n
}

/// The record the view holds of the node `id`.
async fn held_by(view: &RwLock<Topology>, id: &NodeId) -> Option<Node> {
    view.read().await.nodes.iter().find(|n| &n.node_id == id).cloned()
}

/// The record the view holds at `name`.
async fn held_at(view: &RwLock<Topology>, name: &PathName) -> Option<Node> {
    view.read().await.nodes.iter().find(|n| &n.name == name).cloned()
}
