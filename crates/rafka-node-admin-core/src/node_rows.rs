//! The node rows of a mesh, written by that mesh's primary node-admin (R13; ops-naming "Node
//! data: meta, config and exe"; lifecycles/node-create.md step 1; node-decommission.md step 3;
//! fabric-node-lifecycle.md §2.7: "It writes its own members' `Node` rows; no other node-admin
//! writes them").
//!
//! The writer of a mesh's rows is that mesh's primary node-admin, for every member of its mesh, itself
//! included (R-OW1: "The mesh-primary writes every row in its mesh, including its own"); the
//! fabric-primary writes them only as the acting primary of a mesh with no primary. Every other admin
//! refuses by name (`not-mesh-primary`), another mesh's primary included: a member never writes its row
//! because it is the subject. Writers never share a key: a row's `path.name` is `<mesh>.<kind>.<ordinal>`, so every key is
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
//! - `node.update` of a name (R13, `crate::node_update`): the row moves to the new name and the
//!   node lets go of the old one ([`move_row`]); a node taking `<name>` from `<name>.new` has its
//!   config seeded from the `<name>.old` row (lifecycles/node-replace.md step 4).
//! - `NodeDeleted`: the row with status `Deleted`, the record and its config kept
//!   ([`NodeRows::deleted`]).
//! - A mesh delete (R-MN1, mesh-delete.md steps 3 and 7): each member's row `Deleted` on its exact
//!   Exited receipt, by the deleting mesh's primary, and the final primary's in its own node.delete,
//!   by the fabric-primary as acting primary of a mesh with no primary; a birth the view no longer
//!   names is written from this writer's record of its row ([`NodeRows::deleted_birth`]).
//! - Day 0 (R-OW1 item 2): the bootstrap admin, mesh1's primary, claims its own `path.name` by
//!   insert-and-fail as the first write of its birth, before Ready ([`claim_day0_name`]).
//! - A birth no create of this writer claimed (a birth made before this admin took its mesh's
//!   seat, this admin's own included): written when the writer's view first holds it
//!   ([`NodeRows::arrivals`]); an admin that takes a mesh's seat writes every member's row of that
//!   mesh, its own included, and a row a node already holds is never overwritten.

use crate::model::{IncarnationId, Node, NodeId, NodeKind, NodeStatus, PathName};
use crate::storage::{Allocation, NameHeld, NodeRow, NodesStorage};
use crate::topology::Topology;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

/// The writer of a mesh's node rows.
pub struct NodeRows {
    /// This admin's `path.name`, the name its spans and refusals carry.
    me: PathName,
    /// The node id this admin was born with: it writes a mesh's rows only while its view names that
    /// birth the mesh's primary (never by path.name: a successor takes the path.name with a new
    /// node id).
    me_id: NodeId,
    /// This admin's view, read at each write to decide whether it is the writer of the row's mesh.
    view: Arc<RwLock<Topology>>,
    storage: Arc<dyn NodesStorage>,
    /// The config door a seeded birth is told its config through (`crate::node_config`).
    configs: Option<crate::node_config::NodeConfigsSlot>,
    /// Per mesh whose rows this admin writes, the births its views have held in that tenure: each
    /// row is written once, when a view first holds it, and a later view install that holds it
    /// again is not an arrival.
    held: Mutex<BTreeMap<String, HashSet<(NodeId, IncarnationId)>>>,
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

/// Which create a name claim is made for, which decides what its row takes from the `Deleted` row
/// at its `path.name` beside the config (proposal node-properties-own-commands.md §1, tags: "set by
/// the create spec (may be empty); inherited with the config on recover (from the Deleted row at the
/// claim)").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimFor {
    /// A `node.create`: the row carries its create spec's tags (RDM's create spec names none).
    Create,
    /// Node recovery's create over the exited birth's data dir (node-recovery.md step 4): the row
    /// takes the tags with the config from the `Deleted` row at the key, indexed under the new node's
    /// id in that one write (§8, recovery).
    Recovery,
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
        Self { me, me_id, view, storage, configs: None, held: Mutex::new(BTreeMap::new()), written: Arc::new(WrittenRows::default()) }
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
    /// `<name>.old` row, and its tags from that `Deleted` row only for a recovery's claim (`of`,
    /// [`ClaimFor`]). The store adapter's insert-and-fail
    /// refuses a name another node's live row holds, `NameHeld { by }`, and nothing is written. A row
    /// the node holds already is left as it is. `allocated`: what the create allocated for the birth,
    /// written into the claim row with it ([`NodeRow::allocated`]), so the identity the claim is made
    /// with is durable wherever the claim is. One `via-node-row` span, `moment` `claim`.
    pub async fn claim(&self, name: &PathName, node_id: &NodeId, operation: &str, allocated: Option<&Allocation>, of: ClaimFor) -> Result<Claimed, RowRefused> {
        let span = tracing::info_span!(
            "rdm.node_admin.node.create.via-node-row",
            node = %self.me,
            subject = %name,
            node_id = %node_id,
            operation,
            moment = "claim",
            awaiting = "writer-check",
            awaited = tracing::field::Empty,
            seeded_from = tracing::field::Empty,
            keys = tracing::field::Empty,
            tags = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let mut guard = ClaimGuard { span: span.clone(), done: false };
        let r = tracing::Instrument::instrument(
            async {
                self.writer_check(&name.mesh, name).await?;
                span.record("awaited", "writer-check");
                self.put_seeded(name, node_id, NodeStatus::Pending, &BTreeMap::new(), allocated, of, &span, "writer-check").await
            },
            span.clone(),
        )
        .await;
        guard.done = true;
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
    /// member that departed the view after its exact Exited receipt, mesh-delete.md steps 3 and
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
                let row = NodeRow { name: name.clone(), node_id: node_id.clone(), status: NodeStatus::Deleted, config, tags, allocated: None, renamed_from: None, inherited_by: None };
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
    /// else the `<name>.old` row, else `own`. Its tags: for a recovery's claim ([`ClaimFor::Recovery`])
    /// those of that previous `Deleted` row (proposal node-properties-own-commands.md §8, recovery:
    /// "the successor's claim over it seeds config and tags from that Deleted row and writes the tags
    /// index entries under the successor's id, in the same write that seeds config"); for a plain
    /// create its create spec's, none (§1: tags are "set by the create spec (may be empty)"). The
    /// store indexes a live row's labels under its node in the put itself (`NodeRow::tag_moves`), so
    /// this one write is the inheritance: nothing is copied later. A row the same node holds already,
    /// at whatever name, is left as it is: it is found through the node's id index
    /// ([`WrittenRows::known_row`]), so a node renamed while stopped (told nothing of it, R12) keeps
    /// its one row where the rename put it, whatever name a view still lists it at.
    #[allow(clippy::too_many_arguments)]
    async fn put_seeded(&self, name: &PathName, node_id: &NodeId, status: NodeStatus, own: &BTreeMap<String, String>, allocated: Option<&Allocation>, of: ClaimFor, span: &tracing::Span, prefix: &str) -> Result<Claimed, RowRefused> {
        let me = &self.me;
        match self.written.known_row(&*self.storage, &name.mesh, node_id).await {
            Ok(Some(held)) => return Ok(Claimed::HeldByNode(held)),
            Ok(None) => {}
            Err(e) => return Err(RowRefused::Unwritten(format!("{me}: the row {node_id} holds could not be read before its row at {name} was written: {e}"))),
        }
        let claimed = seeded_put(&*self.storage, &self.me, name, node_id, status, own, allocated, of, span, prefix).await?;
        match &claimed {
            Claimed::Written { row, .. } | Claimed::HeldByNode(row) => self.written.hold(row),
        }
        Ok(claimed)
    }

    /// The births of `now`, the view being installed over `prev`, whose rows this admin writes: the
    /// members of each mesh it writes for in `now` (its primary, or the fabric-primary's for a mesh
    /// with none), its own birth included (R-OW1). Every member of a mesh whose writing it took with this
    /// install (the rebuild after a seat change, which never overwrites a row a node holds); else
    /// each birth of that mesh this tenure's views have not held before. A birth is an arrival once
    /// per tenure: one the view dropped and holds again was written when it first arrived. Read
    /// under the view lock, so of two installs each sees the arrivals its own change made.
    pub fn arrivals(&self, prev: &Topology, now: &Topology) -> Vec<Node> {
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
            out.extend(now.members().filter(|n| n.mesh == mesh && n.incarnation_id.clone().is_some_and(|inc| tenure.insert((n.node_id.clone(), inc)))).cloned());
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
                awaiting = tracing::field::Empty,
                awaited = tracing::field::Empty,
                seeded_from = tracing::field::Empty,
                keys = tracing::field::Empty,
                tags = tracing::field::Empty,
                outcome = tracing::field::Empty,
                told = tracing::field::Empty,
                detail = tracing::field::Empty,
                "otel.kind" = "internal",
            );
            let r = tracing::Instrument::instrument(self.rebuilt_row(&birth, &span), span.clone()).await;
            record_claim(&span, &r);
            span.in_scope(|| tracing::info!("node.create: the birth's row decided"));
            match r {
                Ok(Claimed::Written { row, .. } | Claimed::HeldByNode(row)) if row.config != birth.config => self.tell(now, &birth, row, &span),
                _ => {}
            }
        }
    }

    /// The row of `birth` as the writer meets it at its arrival: the row the node holds wherever it
    /// stands (a rename may have moved it off the name the view holds, found through the id index), any
    /// release its `renamed_from` names finished when still pending (proposal
    /// node-properties-own-commands.md §2.1: "a mesh-primary's rebuild from its mesh's rows after a
    /// seat change"); else the row seeded at the view's name ([`Self::put_seeded`]).
    async fn rebuilt_row(&self, birth: &Node, span: &tracing::Span) -> Result<Claimed, RowRefused> {
        let me = &self.me;
        let holds = match self.storage.node_row_name(&birth.mesh, &birth.node_id).await.map_err(|e| RowRefused::Unwritten(format!("{me}: the id index entry of {} in {} could not be read: {e}", birth.node_id, birth.mesh)))? {
            Some(key) if key != birth.name => self.storage.node_row_at(&key).await.map_err(|e| RowRefused::Unwritten(format!("{me}: the row of {} at {key} could not be read: {e}", birth.node_id)))?.filter(|r| r.node_id == birth.node_id),
            _ => None,
        };
        let claimed = match holds {
            Some(row) => {
                self.written.hold(&row);
                Claimed::HeldByNode(row)
            }
            // A recovery's claim precedes its birth's arrival (the create claims before it launches),
            // so an arrival that finds no row of its node is never a recovery's: a plain create's seed.
            None => self.put_seeded(&birth.name, &birth.node_id, birth.status, &birth.config, None, ClaimFor::Create, span, "").await?,
        };
        if let Claimed::HeldByNode(row) = &claimed {
            if let Err(why) = finish_release(&*self.storage, me, row, "rebuild").await {
                span.record("detail", format!("{me}: the release {} of {} is still pending: {why}", row.renamed_from.as_ref().map(|f| f.to_string()).unwrap_or_default(), row.node_id).as_str());
            }
        }
        Ok(claimed)
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

struct ClaimGuard {
    span: tracing::Span,
    done: bool,
}

impl Drop for ClaimGuard {
    fn drop(&mut self) {
        if !self.done {
            self.span.record("outcome", "cancelled");
        }
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

fn push_awaited(awaited: &mut String, step: &str) {
    if !awaited.is_empty() {
        awaited.push(',');
    }
    awaited.push_str(step);
}

/// [`NodeRows::put_seeded`]'s one write, by `me` into `storage`: the row of `node_id` at `name`, its
/// config and tags seeded as that method says. A row the same node holds already is left as it is.
#[allow(clippy::too_many_arguments)]
async fn seeded_put(storage: &dyn NodesStorage, me: &PathName, name: &PathName, node_id: &NodeId, status: NodeStatus, own: &BTreeMap<String, String>, allocated: Option<&Allocation>, of: ClaimFor, span: &tracing::Span, prefix: &str) -> Result<Claimed, RowRefused> {
    let mut awaited = prefix.to_string();
    span.record("awaiting", "read-previous-row");
    let previous = storage.node_row_at(name).await.map_err(|e| RowRefused::Unwritten(format!("{me}: the previous row at {name} could not be read for {node_id}: {e}")))?;
    push_awaited(&mut awaited, "read-previous-row");
    span.record("awaited", awaited.as_str());
    if let Some(p) = previous.as_ref().filter(|p| p.node_id == *node_id) {
        span.record("awaiting", "done");
        return Ok(Claimed::HeldByNode(p.clone()));
    }
    // The tags come with the config only for a recovery's claim, from the Deleted row at this key.
    let tags = previous.as_ref().filter(|p| of == ClaimFor::Recovery && p.status == NodeStatus::Deleted).map(|p| p.tags.clone()).unwrap_or_default();
    // A successor born at `<name>.new` (node-replace.md step 1) reads no `.old` row: it inherits
    // at its cutover (`crate::node_name_update::Cutover`). The `.old` row seeds the create of an
    // exited predecessor's successor at the path (the replace of an exited predecessor).
    let seed = match previous {
        Some(p) => Some(p),
        None if name.new => None,
        None => {
            span.record("awaiting", "read-old-row");
            let old = storage.node_row_at(&name.renamed()).await.map_err(|e| RowRefused::Unwritten(format!("{me}: the .old row at {} could not be read for {node_id}: {e}", name.renamed())))?.filter(|p| p.node_id != *node_id);
            push_awaited(&mut awaited, "read-old-row");
            span.record("awaited", awaited.as_str());
            old
        }
    };
    let config = seed.as_ref().map_or_else(|| own.clone(), |p| p.config.clone());
    if let Some(p) = &seed {
        span.record("seeded_from", p.node_id.as_str());
    }
    span.record("keys", config.len() as u64).record("tags", labels(&tags).as_str());
    let row = NodeRow { name: name.clone(), node_id: node_id.clone(), status, config, tags, allocated: allocated.cloned(), renamed_from: None, inherited_by: None };
    span.record("awaiting", "put-node-row");
    let put = storage.put_node_row(&row).await;
    push_awaited(&mut awaited, "put-node-row");
    span.record("awaited", awaited.as_str());
    match put {
        Ok(Ok(())) => {
            span.record("awaiting", "done");
            Ok(Claimed::Written { row, seeded_from: seed.map(|p| p.node_id) })
        }
        Ok(Err(held)) => Err(RowRefused::NameHeld(held)),
        Err(e) => Err(RowRefused::Unwritten(format!("{me}: nodes.storage refused the row of {name} ({node_id}): {e}"))),
    }
}

/// Fabric recovery's hand-over of the `path.name` an admin started on an inherited root takes
/// (fabric-recovery.md: "The relaunched node-admin is a new node over its inherited data dir: a new
/// node.id and incarnation, the same path.name"; "Nothing alive": "node.delete of the exited instance,
/// then node.create at the same path.name, with a new node.id over the inherited data dir", "A node's
/// name is released by node recovery's node.delete and taken by its node.create, which carries a new
/// node.id"). Fabric recovery starts after every node-admin is lost, so this admin, `node_id`, is the
/// only writer its mesh has, and these are the first writes of its birth, before anything else of it
/// is written or published:
///
/// 1. node.delete of the inherited birth: its live row at the name written `Deleted`, its config and
///    tags kept (one `rdm.node_admin.node.delete.via-node-row` span), and its contact dropped from its
///    mesh's contacts (a deleted birth is no node to reach), a blind removal of its own key;
/// 2. node.create's claim of the name under `node_id`: insert-and-fail in the store adapter, the row
///    `Pending`, its config and tags seeded from the `Deleted` row ([`ClaimFor::Recovery`]; one
///    `rdm.node_admin.node.create.via-node-row` span, `moment` `claim`).
///
/// Another node's live row at the name is `NameHeld` naming it, and nothing of this birth is written.
pub async fn take_inherited_name(storage: &dyn NodesStorage, inherited: &crate::storage::NodeRecord, node_id: &NodeId) -> Result<Claimed, RowRefused> {
    let (me, name) = (&inherited.name, &inherited.name);
    let operation = "fabric-recovery";
    let held = storage.node_row_at(name).await.map_err(|e| RowRefused::Unwritten(format!("{me}: the row at {name} could not be read for the inherited birth {}: {e}", inherited.node_id)))?;
    if let Some(p) = held.filter(|p| p.node_id == inherited.node_id && p.status != NodeStatus::Deleted) {
        let span = tracing::info_span!(
            "rdm.node_admin.node.delete.via-node-row",
            node = %me,
            subject = %name,
            operation,
            node_id = %p.node_id,
            keys = p.config.len() as u64,
            config_from = "inherited-row",
            tags = labels(&p.tags).as_str(),
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let row = NodeRow { status: NodeStatus::Deleted, allocated: None, renamed_from: None, inherited_by: None, ..p };
        let r = match tracing::Instrument::instrument(storage.put_node_row(&row), span.clone()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(held)) => Err(RowRefused::NameHeld(held)),
            Err(e) => Err(RowRefused::Unwritten(format!("{me}: nodes.storage refused the Deleted row of the inherited birth at {name} ({}): {e}", row.node_id))),
        };
        match &r {
            Ok(()) => span.record("outcome", "row-deleted"),
            Err(e) => span.record("outcome", e.outcome()).record("detail", e.to_string().as_str()),
        };
        span.in_scope(|| tracing::info!("node.delete: the inherited birth's Deleted row decided; its record, config and tags stay"));
        r?;
    }
    storage.remove_contact(&name.mesh, &inherited.node_id).await.map_err(|e| RowRefused::Unwritten(format!("{me}: the contact of the inherited birth {} could not be dropped from {}'s contacts: {e}", inherited.node_id, name.mesh)))?;
    own_claim(None, storage, name, node_id, operation, ClaimFor::Recovery).await
}

/// Day 0's claim of the bootstrap admin's own `path.name` (R-OW1 item 2: "Day 0: the bootstrap admin,
/// as mesh1's primary, claims (mesh1.admin, 1) by insert-and-fail and writes its own row through the
/// same validation and storage path as any create, the first write of its birth, before Ready. No
/// bypass; a conflicting holder is NameHeld, never overwritten"). The bootstrap admin, `node_id` at
/// `name`, is its mesh's primary from its birth and the only admin of its fabric, so it is the writer of
/// its own row; the claim is the first write of its birth, made before anything else of it is written or
/// published. It is the create door's claim ([`ClaimFor::Create`]): insert-and-fail in the store
/// adapter, the row `Pending`, one `rdm.node_admin.node.create.via-node-row` span, `moment` `claim`.
/// Another node's live row at the name is `NameHeld` naming it, and nothing is written.
pub async fn claim_day0_name(storage: &dyn NodesStorage, name: &PathName, node_id: &NodeId) -> Result<Claimed, RowRefused> {
    own_claim(None, storage, name, node_id, "fabric-create", ClaimFor::Create).await
}

/// [`claim_day0_name`] as a child of `boot` without entering `boot` (the boot root is never entered so
/// background tasks spawned during start do not inherit it and its close time stays at `via-ready`).
pub async fn claim_day0_name_in_boot(boot: &tracing::Span, storage: &dyn NodesStorage, name: &PathName, node_id: &NodeId) -> Result<Claimed, RowRefused> {
    own_claim(Some(boot), storage, name, node_id, "fabric-create", ClaimFor::Create).await
}

/// The claim of `name` by the admin born at it as `node_id`, the writer of its own row before it holds a
/// view: [`seeded_put`] under one `rdm.node_admin.node.create.via-node-row` span, `moment` `claim`, the
/// one write a claim is wherever it is made.
async fn own_claim(parent: Option<&tracing::Span>, storage: &dyn NodesStorage, name: &PathName, node_id: &NodeId, operation: &str, of: ClaimFor) -> Result<Claimed, RowRefused> {
    let span = match parent {
        Some(p) => tracing::info_span!(
            parent: p,
            "rdm.node_admin.node.create.via-node-row",
            node = %name,
            subject = %name,
            node_id = %node_id,
            operation,
            moment = "claim",
            awaiting = "read-previous-row",
            awaited = tracing::field::Empty,
            seeded_from = tracing::field::Empty,
            keys = tracing::field::Empty,
            tags = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        ),
        None => tracing::info_span!(
            "rdm.node_admin.node.create.via-node-row",
            node = %name,
            subject = %name,
            node_id = %node_id,
            operation,
            moment = "claim",
            awaiting = "read-previous-row",
            awaited = tracing::field::Empty,
            seeded_from = tracing::field::Empty,
            keys = tracing::field::Empty,
            tags = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        ),
    };
    let mut guard = ClaimGuard { span: span.clone(), done: false };
    let r = tracing::Instrument::instrument(seeded_put(storage, name, name, node_id, NodeStatus::Pending, &BTreeMap::new(), None, of, &span, ""), span.clone()).await;
    guard.done = true;
    record_claim(&span, &r);
    span.in_scope(|| tracing::info!("node.create: the name claim decided"));
    r
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

/// The node's row moves from `from` to `to` in `storage` (proposal node-properties-own-commands.md
/// §2.1 "Claim and move"): put at `to` from its held `record` with `config` and `tags`, carrying
/// `renamed_from: from` (a put the store adapter refuses `NameHeld` when another node's live row holds
/// `to`), then the node lets go of `from` (only its own row there goes). A row the node holds at `to`
/// already is a move made before: it is left as it is, its id index entry naming `to`
/// ([`NodesStorage::hold_node_row`]), and the release its `renamed_from` names is finished when still
/// pending ([`finish_release`]). A release that fails after the put leaves both keys held and answers
/// [`Moved::ReleasePending`]: nothing is rolled back and no second row is written. The one writer of a
/// rename (R13: `node.name.update` is one write path): `node.name.update` at the node's mesh-primary
/// (`crate::node_name_update`) moves here. Answers what it did and the row written, if it wrote one.
pub(crate) async fn move_row(storage: &dyn NodesStorage, me: &PathName, record: &Node, config: BTreeMap<String, String>, tags: std::collections::BTreeSet<String>, from: &PathName, to: &PathName) -> Result<(Moved, Option<NodeRow>), RenameRefused> {
    if from == to {
        return Ok((Moved::Renamed, None));
    }
    let held_there = storage.hold_node_row(to, &record.node_id).await.map_err(|e| RenameRefused::Unwritten(format!("{me}: the row at {to} could not be read or named for the rename of {} from {from}: {e}", record.node_id)))?;
    if held_there {
        // A move made before: its destination row names the key it came from (a row written before
        // `renamed_from` existed names none, and the key is this move's own `from`).
        let held = storage.node_row_at(to).await.map_err(|e| RenameRefused::Unwritten(format!("{me}: the row of {} at {to} could not be read for the rename from {from}: {e}", record.node_id)))?;
        let source = held.as_ref().and_then(|r| r.renamed_from.clone()).unwrap_or_else(|| from.clone());
        return Ok(match release_source(storage, me, &record.node_id, to, &source, "retry").await {
            Ok(true) => (Moved::Renamed, held),
            Ok(false) => (Moved::AlreadyMoved, held),
            Err(why) => (Moved::ReleasePending { why }, held),
        });
    }
    let row = NodeRow { name: to.clone(), node_id: record.node_id.clone(), status: record.status, config, tags, allocated: None, renamed_from: Some(from.clone()), inherited_by: None };
    storage.put_node_row(&row).await.map_err(|e| RenameRefused::Unwritten(format!("{me}: nodes.storage refused the row of {} at {to}: {e}", record.node_id)))?.map_err(RenameRefused::NameHeld)?;
    // The new name is the node's: it lets go of the old key.
    match storage.release_node_row(from, &record.node_id).await {
        Ok(_) => Ok((Moved::Renamed, Some(row))),
        Err(e) => Ok((Moved::ReleasePending { why: format!("nodes.storage refused the release of {from}: {e}") }, Some(row))),
    }
}

/// The release a row's `renamed_from` names, finished when still pending (proposal
/// node-properties-own-commands.md §2.1: "a row whose `renamed_from` key still holds the same node id
/// has a pending release, and the writer releases it"): run by the writer that meets the row, a
/// retry of the command or the mesh-primary's rebuild after a seat change (`moment`). A row that names
/// no source, or whose source no longer holds its node, has nothing pending and nothing is done.
/// Answers whether it released a key, or why the release failed.
pub(crate) async fn finish_release(storage: &dyn NodesStorage, me: &PathName, row: &NodeRow, moment: &'static str) -> Result<bool, String> {
    match row.renamed_from.as_ref().filter(|from| **from != row.name) {
        Some(from) => release_source(storage, me, &row.node_id, &row.name, from, moment).await,
        None => Ok(false),
    }
}

/// `node_id`, holding `to`, lets go of `from` when `from` still holds it: one
/// `rdm.node_admin.node.name.update.via-pending-release` span when it did or could not.
async fn release_source(storage: &dyn NodesStorage, me: &PathName, node_id: &NodeId, to: &PathName, from: &PathName, moment: &'static str) -> Result<bool, String> {
    let still = match storage.node_row_at(from).await {
        Ok(row) => row.is_some_and(|r| r.node_id == *node_id),
        Err(e) => return Err(pending_release_span(me, node_id, to, from, moment, Err(format!("the row at {from} could not be read: {e}")))),
    };
    if !still {
        return Ok(false);
    }
    match storage.release_node_row(from, node_id).await {
        Ok(released) => {
            let _ = pending_release_span(me, node_id, to, from, moment, Ok(released));
            Ok(released)
        }
        Err(e) => Err(pending_release_span(me, node_id, to, from, moment, Err(format!("nodes.storage refused the release of {from}: {e}")))),
    }
}

/// The span of one pending release, its outcome `released`, `not-held` or `release-refused`; answers
/// the refusal's words.
fn pending_release_span(me: &PathName, node_id: &NodeId, to: &PathName, from: &PathName, moment: &'static str, r: Result<bool, String>) -> String {
    let (outcome, detail) = match &r {
        Ok(true) => ("released", String::new()),
        Ok(false) => ("not-held", format!("{node_id} no longer held {from} when its release was made")),
        Err(why) => ("release-refused", why.clone()),
    };
    tracing::info_span!("rdm.node_admin.node.name.update.via-pending-release", node = %me, node_id = %node_id, to = %to, from = %from, moment, outcome, detail = %detail, "otel.kind" = "internal")
        .in_scope(|| tracing::info!("a rename's pending release was met by its writer"));
    detail
}

/// What a move of a node's row did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Moved {
    /// The node holds the new name now and let go of the old one.
    Renamed,
    /// The node held the new name already and not the old one: the move was made before.
    AlreadyMoved,
    /// The node holds the new name and still holds the old one: the release failed, and the next
    /// writer that meets the row finishes it ([`finish_release`]).
    ReleasePending {
        /// Why the release failed.
        why: String,
    },
}
