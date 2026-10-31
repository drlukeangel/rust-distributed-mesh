//! The node rows of a mesh, written by that mesh's primary node-admin (R13; ops-naming "Node
//! data: meta, config and exe"; lifecycles/node-create.md step 1; lifecycles/node-delete.md step 3;
//! node-identity-and-state.md §2.7: "It writes its own members' `Node` rows; no other node-admin
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
//!   ([`NodeRows::claim`]): the writer reads the row at the name and claims it,
//!   `NameHeld { by }` and nothing launched. The row's config is seeded from the previous row at
//!   the `path.name` (a `Deleted` row) or the `<name>.old` row. A create that fails after its claim
//!   withdraws it ([`NodeRows::withdraw`]), the holder's own key only.
//! - `node.update` of a name (R13, `crate::node_update`): the row moves to the new name and the
//!   node lets go of the old one ([`move_row`]). A replace's successor is created at `<name>`, its
//!   claim seeded from the `<name>.old` row ([`ClaimFor::Successor`], lifecycles/node-replace.md
//!   step 3).
//! - `NodeDeleted`: the row with status `Deleted`, the record and its config kept
//!   ([`NodeRows::deleted`]).
//! - A mesh delete (R-MN1, mesh-delete.md steps 3 and 7): each member's row `Deleted` on its exact
//!   Exited receipt, by the deleting mesh's primary, and the final primary's in its own node.delete,
//!   by the fabric-primary as acting primary of a mesh with no primary; a birth the view no longer
//!   names is written from this writer's record of its row ([`NodeRows::deleted_birth`]).
//! - Day 0 (R-OW1 item 2): the bootstrap admin, mesh1's primary, claims its own `path.name`
//!   as the first write of its birth, before Ready ([`claim_day0_name`]).
//! - A birth no create of this writer claimed (a birth made before this admin took its mesh's
//!   role, this admin's own included): written when the writer's view first holds it
//!   ([`NodeRows::arrivals`]); an admin that takes a mesh's role writes every member's row of that
//!   mesh, its own included, and a row a node already holds is never overwritten.

use crate::model::{IncarnationId, Node, NodeId, NodeKind, NodeStatus, PathName};
use crate::storage::{Allocation, IndexMoves, NameHeld, NodeRow, NodesStorage};
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
    /// Fabric recovery in a view: its executor writes the rows its own attempt's operations require
    /// while the target mesh has no mesh-primary (build.md §5.3, Q122).
    fabric_recovery: Option<crate::fabric_recovery::Reader>,
    /// The meshes whose writing this admin took with a view install, whose records it fills from
    /// storage before it writes any of them (storage-whole-records.md: the cache is filled whenever
    /// write ownership is acquired, never kept from before).
    to_fill: Mutex<Vec<String>>,
}

/// The node records this admin writes, as it holds them (storage-whole-records.md, "The rule"): per
/// node, the whole record it last wrote or found the node holding, the copy every write of that node's
/// record starts from. The writer of a mesh's rows, its config door, its status authority, its offline
/// tick and its cert renewal share this one copy, so a fact one of them writes is on the record the
/// next one writes. Each node's record is mutated under that node's own lock, from the read of the
/// copy through the whole write and the replacement of the copy, so two updates of one node never
/// overlap. Storage is read only to fill the copy: a mesh's records when this admin takes its writing
/// ([`Self::fill`]), and a node it holds no copy of when it first writes it ([`Self::known_row`]).
#[derive(Debug, Default)]
pub struct WrittenRows {
    rows: crate::record_cache::RecordCache<NodeId, NodeRow>,
    node_types: Mutex<HashMap<NodeId, String>>,
    /// Per node, the index moves of a record write the indexes have not acknowledged yet: written
    /// with that node's next record write, never recomputed from the already-updated copy.
    owed: Mutex<HashMap<NodeId, IndexMoves>>,
    /// The `topology_storage_provider` this admin writes (storage.md §2), set once its storage is
    /// open and again each time a storage change moves it: a name claim it writes records it on the
    /// claimed row, the provider the node it launches writes.
    topology_storage_provider: std::sync::RwLock<Option<String>>,
    /// This admin's atlas: every record the writer holds is written to it (Q203: each op
    /// writes the atlas), so a node read answers what this writer wrote without reading the store.
    atlas: std::sync::OnceLock<Arc<crate::atlas::Atlas>>,
    /// The meshes whose records were filled from storage in this admin's tenure of their writing
    /// ([`Self::fill`]): each is read once when its writing is taken, never again before it is given up
    /// ([`Self::release`]).
    filled: Mutex<std::collections::BTreeSet<String>>,
    /// The birth this admin writes as (Q109: the record names its writer's birth), set once at its boot
    /// ([`Self::writes_as`]): every record it puts carries it as `writer`.
    birth: std::sync::OnceLock<crate::storage::BirthRef>,
    /// Moves each time a record this writer writes moves its node's name or releases it
    /// ([`released_its_name`]), and each time it lets a record go ([`Self::forget`]): the view lists a
    /// birth that does not publish at the name its row holds, and a released row gives its path to
    /// another birth (`crate::admin::projection::RowNames`). What the view rebuild waits on
    /// ([`Self::subscribe_names`]); nothing is sent.
    names: tokio::sync::watch::Sender<u64>,
}

impl WrittenRows {
    /// Write every record this writer holds from now on to `atlas` as well, the ones it holds now
    /// included.
    pub fn feeds(&self, atlas: Arc<crate::atlas::Atlas>) {
        for r in self.rows.all() {
            atlas.write_record(&r);
        }
        let _ = self.atlas.set(atlas);
    }

    /// Hold `provider` as the `topology_storage_provider` this admin writes: at its boot, and at each
    /// move a storage change makes.
    pub fn writes_to(&self, provider: &str) {
        *self.topology_storage_provider.write().unwrap_or_else(|e| e.into_inner()) = Some(provider.to_string());
    }

    /// Write every record as `birth`, this admin's own (node id and incarnation), from now on: each put
    /// carries it as the record's `writer`. The first call stands.
    pub fn writes_as(&self, birth: crate::storage::BirthRef) {
        let _ = self.birth.set(birth);
    }

    /// The birth this admin writes as, once its boot set it.
    pub fn writer(&self) -> Option<&crate::storage::BirthRef> {
        self.birth.get()
    }

    /// `read`, a version of a node's record a read of storage answered, as this writer folds it
    /// (build.md: "a deleted birth whose writes every reader refuses"; Q109: "a row that does not follow
    /// the fold is ignored by every reader"): a version written by a birth this writer holds deleted
    /// (its copy of that birth's node record is `Deleted` or `Withdrawn` and keeps the birth) is ignored
    /// where this writer holds its own copy of the record, and the copy is answered; one
    /// `rdm.node_admin.node.reject.via-deleted-birth` span names it. Every other version is answered as
    /// read: with no copy of the record held there is nothing to answer instead.
    pub fn fold_read(&self, me: &PathName, read: Option<NodeRow>) -> Option<NodeRow> {
        let r = read?;
        let Some(w) = r.writer.clone() else { return Some(r) };
        let deleted = self.row_of(&w.node_id).is_some_and(|d| matches!(d.status, NodeStatus::Deleted | NodeStatus::Withdrawn) && d.birth(&w.incarnation_id).is_some());
        match self.row_of(&r.node_id).filter(|_| deleted) {
            Some(held) => {
                ignored_span(&me.to_string(), "writer-record", &r, &w, &format!("{} {:?} at revision {}", held.node_id, held.status, held.revision));
                Some(held)
            }
            None => Some(r),
        }
    }

    /// The `topology_storage_provider` this admin writes, once its boot set it.
    pub fn topology_storage_provider(&self) -> Option<String> {
        self.topology_storage_provider.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// The atlas this writer feeds, once [`Self::feeds`] named it.
    pub fn atlas(&self) -> Option<&Arc<crate::atlas::Atlas>> {
        self.atlas.get()
    }

    /// Hold `row` as the writer's record of its node.
    pub fn hold(&self, row: &NodeRow) {
        self.rows.hold(row.node_id.clone(), row.clone());
        if let Some(atlas) = self.atlas.get() {
            atlas.write_record(row);
        }
    }

    /// Hold `node_type` as the writer's record of `node_id`'s immutable `node_type`.
    pub fn hold_node_type(&self, node_id: &NodeId, node_type: impl Into<String>) {
        self.node_types.lock().unwrap().insert(node_id.clone(), node_type.into());
    }

    /// The `node_type` the writer holds for `node_id`, if any.
    pub fn node_type_of(&self, node_id: &NodeId) -> Option<String> {
        self.node_types.lock().unwrap().get(node_id).cloned()
    }

    /// The watch [`Self::put`] and [`Self::forget`] move when a record's name or its release moved.
    pub fn subscribe_names(&self) -> tokio::sync::watch::Receiver<u64> {
        self.names.subscribe()
    }

    /// The node let go of its record (a withdrawn claim): the writer holds no record of it.
    pub fn forget(&self, node_id: &NodeId) {
        if self.rows.get(node_id).is_some() {
            self.names.send_modify(|v| *v += 1);
        }
        self.rows.forget(node_id);
        if let Some(atlas) = self.atlas.get() {
            atlas.forget_record(node_id);
        }
        self.node_types.lock().unwrap().remove(node_id);
    }


    /// The record the writer holds for `node_id`, if it holds one.
    pub fn row_of(&self, node_id: &NodeId) -> Option<NodeRow> {
        self.rows.get(node_id)
    }

    /// Every record the writer holds of `mesh`.
    pub fn rows_of(&self, mesh: &str) -> Vec<NodeRow> {
        self.rows.all().into_iter().filter(|r| r.name.mesh == mesh).collect()
    }

    /// `node_id`'s own lock, held by a mutation of its record from the read of the copy through the
    /// write and the replacement of the copy.
    pub async fn lock(&self, node_id: &NodeId) -> tokio::sync::OwnedMutexGuard<()> {
        self.rows.lock(node_id).await
    }

    /// Fill the copy of every record of `mesh` from `storage`, at the moment this admin takes the
    /// writing of `mesh` (its mesh's role; the fabric-primary for a mesh left with none): the records
    /// as they stand then, never a copy held from before. Each is replaced under its node's lock.
    pub async fn fill(&self, storage: &dyn NodesStorage, mesh: &str) -> Result<usize, crate::storage_error::StorageError> {
        let all = storage.nodes(mesh).await?;
        let gone = re_created(&all);
        let listed = fold_records("writer-fill", all);
        for r in &listed {
            // Read again under the node's lock: a write that landed after the listing is in storage and
            // in the copy, and the listing's older record never replaces it.
            let _one = self.lock(&r.node_id).await;
            // A version written after the listing by a birth the listing holds re-created is its own
            // write as a deleted birth: the listed record is held, as the fold read it.
            match storage.node(&r.node_id).await? {
                Some(now) if now.writer.as_ref().is_some_and(|w| w.node_id == now.node_id && gone.contains_key(w)) => {
                    let w = now.writer.clone().expect("checked");
                    ignored_span("", "writer-fill", &now, &w, &format!("{} re-created it at {}", gone[&w], now.name));
                    self.hold(r)
                }
                Some(now) => self.hold(&now),
                None => self.forget(&r.node_id),
            }
        }
        self.filled.lock().unwrap().insert(mesh.to_string());
        Ok(listed.len())
    }

    /// Whether `mesh`'s records were filled in this tenure of its writing.
    pub fn filled(&self, mesh: &str) -> bool {
        self.filled.lock().unwrap().contains(mesh)
    }

    /// The meshes filled in this tenure of their writing.
    pub fn filled_meshes(&self) -> Vec<String> {
        self.filled.lock().unwrap().iter().cloned().collect()
    }

    /// The writing of `mesh` was given up: its next take fills its records again.
    pub fn release(&self, mesh: &str) {
        self.filled.lock().unwrap().remove(mesh);
    }

    /// The record of `node_id` as this writer knows it: its copy, else the record storage holds for the
    /// node (a fill of the copy), held as found. `None`: the node holds no record. A read that fails is
    /// named.
    pub async fn known_row(&self, storage: &dyn NodesStorage, node_id: &NodeId) -> Result<Option<NodeRow>, String> {
        if let Some(row) = self.row_of(node_id) {
            return Ok(Some(row));
        }
        let row = storage.node(node_id).await.map_err(|e| format!("the record of {node_id} could not be read: {e}"))?;
        if let Some(r) = &row {
            self.hold(r);
        }
        Ok(row)
    }

    /// The facts the record of `node_id` holds of its birth `incarnation`, as this writer knows it.
    pub async fn birth_facts(&self, storage: &dyn NodesStorage, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::BirthFacts>, String> {
        Ok(self.known_row(storage, node_id).await?.and_then(|r| r.birth(incarnation).cloned()))
    }

    /// Apply `change` to the facts of `node`'s birth `incarnation` on its record, written whole: the
    /// record held, else one made of `node` (its name claimed for it).
    pub async fn update_birth<R>(&self, storage: &dyn NodesStorage, node: &Node, incarnation: &IncarnationId, change: impl FnOnce(&mut crate::storage::BirthFacts) -> R) -> Result<Option<R>, String> {
        let base = NodeRow::new(node.name.clone(), node.node_id.clone(), node.status);
        self.update(storage, &node.node_id, Some(base), |r| change(r.birth_mut(incarnation, &node.name))).await
    }

    /// The one write of `row`, its node's lock held by the caller: the whole record written at the next
    /// revision (Q235 P1: this writer, the mesh-primary, steps it on every write of the row, past the
    /// copy's and the row's own), the copy replaced once storage answers, then the index moves of this
    /// write over the copy it replaced, with any a write before it still owes. An index write that fails
    /// keeps every move owed for the node's next write and is named; the record stands.
    pub async fn put(&self, storage: &dyn NodesStorage, row: &NodeRow) -> Result<(), crate::storage_error::StorageError> {
        let previous = self.row_of(&row.node_id);
        let mut row = row.clone();
        row.revision = previous.as_ref().map_or(0, |p| p.revision).max(row.revision) + 1;
        row.writer = self.birth.get().cloned();
        let row = &row;
        storage.put_node(row).await?;
        self.hold(row);
        if previous.as_ref().is_none_or(|p| p.name != row.name || released_its_name(p) != released_its_name(row)) {
            self.names.send_modify(|v| *v += 1);
        }
        let owed = self.owed.lock().unwrap().remove(&row.node_id).unwrap_or_default();
        let moves = owed.then(IndexMoves::of(row, previous.as_ref()));
        if let Err(e) = crate::storage::write_index_moves(storage, &row.name.mesh, &row.node_id, &moves).await {
            self.owed.lock().unwrap().insert(row.node_id.clone(), moves);
            return Err(e);
        }
        Ok(())
    }

    /// The one mutation of `node_id`'s record, under its lock: the copy (filled from storage when this
    /// writer holds none, else `base`, whose name is claimed for the node first), `change` applied, and
    /// the whole record written when it changed or an index move is still owed. `None`: no record of
    /// the node is held anywhere and no `base` was given; nothing is written.
    pub async fn update<R>(&self, storage: &dyn NodesStorage, node_id: &NodeId, base: Option<NodeRow>, change: impl FnOnce(&mut NodeRow) -> R) -> Result<Option<R>, String> {
        Ok(self.update_at_revision(storage, node_id, base, change).await?.map(|(r, _)| r))
    }

    /// [`Self::update`], with the revision the record carries once it finished, read under the node's
    /// lock: the revision this update wrote, else the one it found. The writes of one record are made
    /// one at a time under that lock, so the revisions order them as they were made, whichever of their
    /// callers reports first.
    pub async fn update_at_revision<R>(&self, storage: &dyn NodesStorage, node_id: &NodeId, base: Option<NodeRow>, change: impl FnOnce(&mut NodeRow) -> R) -> Result<Option<(R, u64)>, String> {
        let _one = self.lock(node_id).await;
        let (held, new) = match self.known_row(storage, node_id).await? {
            Some(row) => (Some(row), false),
            None => (base, true),
        };
        let Some(before) = held else { return Ok(None) };
        if new {
            take_name(storage, &before.name, node_id).await.map_err(|e| format!("nodes.storage refused the claim of {} for {node_id}: {e}", before.name))?.map_err(|held| format!("{held}"))?;
        }
        let mut row = before.clone();
        let r = change(&mut row);
        let owes = self.owed.lock().unwrap().contains_key(node_id);
        if row != before || owes || new {
            self.put(storage, &row).await.map_err(|e| format!("nodes.storage refused the record of {node_id}: {e}"))?;
        }
        let revision = self.row_of(node_id).map_or(row.revision, |held| held.revision);
        Ok(Some((r, revision)))
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
    /// Another node's live row holds the name: the claim refused it and nothing was written.
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
    /// A `node.create`: the row carries its create spec's tags and config (Q166), whatever row stood at
    /// its `path.name` before.
    Create,
    /// A birth no claim of this writer's wrote a row for, its row written from the birth's own record
    /// as the writer meets it at its arrival or its declaration: the create spec is not known here, so
    /// the row carries no tags and its config is seeded from the previous row at the `path.name` (else
    /// the `<name>.old` row), else the birth's own.
    Arrival,
    /// Node recovery's create over the exited birth's data dir (node-recovery.md step 4): the row
    /// takes the tags with the config from the `Deleted` row at the key, indexed under the new node's
    /// id in that one write (§8, recovery).
    Recovery,
    /// A Build's create of a member it replaces at its own path (build-deployment.md §3: the reconcile's
    /// replacement, over the data dir its delete kept or a fresh one): claimed over the `Deleted` row at
    /// the key as a recovery's is (its node type and its predecessor seeded from it), but its config and
    /// tags are its Build entry's (Q305), never the predecessor's.
    Replacement,
    /// A Build's node.create of a replace's successor at the `<name>` its predecessor released
    /// (node-replace.md step 3, Properties): the row takes its Build entry's config and tags (Q305),
    /// never the predecessor's; the predecessor's row, the retained `<name>.old` row or else the `Deleted` row at the key, seeds the rest, its labels leaving the active index, the successor's tags indexed under
    /// the new node's id in that one write, and the predecessor's `node_type`; a retained `.old` row is
    /// then marked inherited by the successor, its labels out of the active tags index.
    Successor,
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
/// fabric-primary while the mesh has no primary. A stopped primary is no primary (R-NS1 item 2:
/// from the stop of a mesh's last admin the fabric-primary is the writer of a mesh with no primary):
/// it writes nothing, stopped, and the fabric-primary writes as acting primary (mesh-delete.md step
/// 7, the final primary's own row Deleted in its node.delete).
pub fn writes_for(t: &Topology, me_id: &NodeId, mesh: &str) -> bool {
    t.is_mesh_primary(mesh, me_id) || (t.is_fabric_primary(me_id) && t.node_type_primary(mesh, NodeKind::NodeAdmin).filter(|p| !p.stopped).is_none())
}

impl NodeRows {
    /// The writer of the admin `me`, born as `me_id`, deciding over its view `view`, writing to
    /// `storage`, its nodes store.
    pub fn new(me: PathName, me_id: NodeId, view: Arc<RwLock<Topology>>, storage: Arc<dyn NodesStorage>) -> Self {
        Self { me, me_id, view, storage, configs: None, held: Mutex::new(BTreeMap::new()), written: Arc::new(WrittenRows::default()), fabric_recovery: None, to_fill: Mutex::new(Vec::new()) }
    }

    /// Read fabric recovery through `read`: while this admin runs it, it writes the target mesh's rows its
    /// own operations require (build.md §5.3, Q122).
    pub fn with_fabric_recovery(mut self, read: crate::fabric_recovery::Reader) -> Self {
        self.fabric_recovery = Some(read);
        self
    }

    /// Share `written`, the record the config door holds its puts in, as this writer's record.
    pub fn with_written(mut self, written: Arc<WrittenRows>) -> Self {
        self.written = written;
        self
    }

    /// Tell each birth whose row is seeded its config through `configs`, the config door.
    pub fn with_configs(mut self, configs: crate::node_config::NodeConfigsSlot) -> Self {
        self.configs = Some(configs);
        self
    }

    /// The records this writer holds, the copy every write of a node's record starts from.
    pub fn written(&self) -> &Arc<WrittenRows> {
        &self.written
    }

    /// The store the rows are written to.
    pub fn storage(&self) -> &Arc<dyn NodesStorage> {
        &self.storage
    }

    /// This admin's currently held `path.name` in `t`, falling back to `self.me` before its birth
    /// is in the view.
    fn held_name_in(&self, t: &Topology) -> PathName {
        t.own(&self.me_id).map(|n| n.name.clone()).unwrap_or_else(|| self.me.clone())
    }

    /// This admin's currently held `path.name` if the view is readable without waiting, else `self.me`.
    fn held_name_now(&self) -> PathName {
        self.view.try_read().ok().map(|t| self.held_name_in(&t)).unwrap_or_else(|| self.me.clone())
    }

    /// This admin's currently held `path.name` in its view.
    async fn held_name(&self) -> PathName {
        self.held_name_in(&*self.view.read().await)
    }

    /// Why this admin does not write `mesh`'s rows in view `t` (the row at `subject`), naming the
    /// writer the view holds; `None` when it is the writer.
    fn not_writer(&self, t: &Topology, mesh: &str, subject: &PathName) -> Option<String> {
        if writes_for(t, &self.me_id, mesh) || crate::fabric_recovery::executes_for(self.fabric_recovery.as_ref(), t, &self.held_name_in(t), mesh) {
            return None;
        }
        let me = self.held_name_in(t);
        let writer = match (t.node_type_primary(mesh, NodeKind::NodeAdmin), t.fabric_primary()) {
            (Some(p), _) => format!("{mesh}'s primary node-admin {} ({}) writes them", p.name, p.node_id),
            (None, Some(fp)) => format!("{mesh} has no primary node-admin, and the fabric-primary {} ({}) writes them for it", fp.name, fp.node_id),
            (None, None) => format!("{mesh} has no primary node-admin and the view holds no fabric-primary to write them for it"),
        };
        Some(format!("{me} ({}) is not the writer of {mesh}'s node rows and refuses the row at {subject}: {writer}", self.me_id))
    }

    /// Whether this admin, the fabric-primary, writes the row of the node-admin `node_id` at `name` as
    /// acting primary of its mesh: the mesh holds no primary but that node (node-identity-and-state.md §4.3).
    fn acting_for(&self, t: &Topology, name: &PathName, node_id: &NodeId) -> bool {
        t.is_fabric_primary(&self.me_id) && !t.node_type_primary(&name.mesh, NodeKind::NodeAdmin).is_some_and(|p| p.node_id != *node_id)
    }

    /// The writer check of the row of `node_id` at `name` in view `t`, the one predicate of a delete's
    /// writes of the node's row (its `Deleting` status, its `Deleted` row): this admin writes it as the
    /// writer of its mesh's rows or, `acting` (a node-admin's row), as the fabric-primary acting for a mesh
    /// that holds no primary but that node; any other admin refuses it by name, naming the writer
    /// (node-identity-and-state.md §4.3: "The fabric-primary writes a row only as acting primary of a mesh
    /// with no primary. No other process writes a `Node` row"; R-OW1). Answers the name this admin holds.
    fn row_writer_in(&self, t: &Topology, name: &PathName, node_id: &NodeId, acting: bool) -> Result<PathName, RowRefused> {
        if acting && self.acting_for(t, name, node_id) {
            return Ok(self.held_name_in(t));
        }
        match self.not_writer(t, &name.mesh, name) {
            Some(why) => Err(RowRefused::NotMeshPrimary(why)),
            None => Ok(self.held_name_in(t)),
        }
    }

    /// [`Self::row_writer_in`] of `record`'s row over this admin's view as it is now, acting for a
    /// node-admin's row.
    async fn row_writer(&self, record: &Node) -> Result<PathName, RowRefused> {
        self.row_writer_in(&*self.view.read().await, &record.name, &record.node_id, record.kind == NodeKind::NodeAdmin)
    }

    /// Whether this admin writes the `Deleted` row of the unreached birth `record` now, over its view as it
    /// is now ([`Self::deleted_unreached`]): as the writer of its mesh's rows, or, for a node-admin, as the
    /// fabric-primary acting for a mesh with no primary but that node.
    pub async fn writes_unreached(&self, record: &Node) -> bool {
        self.row_writer(record).await.is_ok()
    }

    /// This admin's `path.name`.
    pub fn me(&self) -> &PathName {
        &self.me
    }

    /// Whether this admin writes `mesh`'s rows now, over its view as it is now.
    pub async fn writes_now(&self, mesh: &str, subject: &PathName) -> bool {
        self.writer_check(mesh, subject).await.is_ok()
    }

    /// The writer check at the moment of a write, over this admin's view as it is now.
    async fn writer_check(&self, mesh: &str, subject: &PathName) -> Result<PathName, RowRefused> {
        let t = self.view.read().await;
        match self.not_writer(&t, mesh, subject) {
            Some(why) => Err(RowRefused::NotMeshPrimary(why)),
            None => Ok(self.held_name_in(&t)),
        }
    }

    /// `node.create`'s name claim (lifecycles/node-create.md step 1): at the start of `PrepareStorage`,
    /// once the `AllocateIdentity` receipt names `node_id` and before anything is prepared or
    /// launched for the birth, the writer of the mesh's rows puts the birth's row at `name`, status
    /// `Pending`, with no tags and no config: a recovery's claim (`of`, [`ClaimFor`]) inherits the
    /// config and tags of the `Deleted` row at the `path.name`, a successor's those of the `<name>.old`
    /// row ([`Self::claim_spec`] writes a plain create's from its create spec). The claim ([`take_name`])
    /// refuses a name another node's live row holds, `NameHeld { by }`, and nothing is written. A row
    /// the node holds already is left as it is. `allocated`: what the create allocated for the birth,
    /// written into the claim row with it ([`NodeRow::allocated`]), so the identity the claim is made
    /// with is durable wherever the claim is. One `via-node-row` span, `moment` `claim`.
    pub async fn claim(&self, name: &PathName, node_id: &NodeId, operation: &str, allocated: Option<&Allocation>, executable: Option<&rdm_core::binding::Binding>, of: ClaimFor) -> Result<Claimed, RowRefused> {
        self.claim_spec(name, node_id, operation, allocated, executable, &std::collections::BTreeSet::new(), &BTreeMap::new(), None, of).await
    }

    /// [`Self::claim`] of a create whose create spec names `tags` and `config` (Q166, build.md §1; proposal
    /// executable-bindings-at-create.md rule 1): a plain create's row carries them, written at the claim,
    /// so its first join hands it the config (node-config-update.md); a replace's successor's row carries
    /// them too (node-replace.md "Properties", Q305); a recovery's carries its seed's (node-recovery.md
    /// "Retained"). `placed`: the
    /// spec's storage and placement, written onto the row whatever the claim (build.md §1); `None`
    /// takes the seed's.
    #[allow(clippy::too_many_arguments)]
    pub async fn claim_spec(&self, name: &PathName, node_id: &NodeId, operation: &str, allocated: Option<&Allocation>, executable: Option<&rdm_core::binding::Binding>, tags: &std::collections::BTreeSet<String>, config: &BTreeMap<String, String>, placed: Option<(rdm_core::meta::StorageMeta, rdm_core::meta::PlacementMeta)>, of: ClaimFor) -> Result<Claimed, RowRefused> {
        let me = self.held_name_now();
        let span = tracing::info_span!(
            "rdm.node_admin.node.create.via-node-row",
            node = %me,
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
                let me = if matches!(of, ClaimFor::Recovery | ClaimFor::Replacement) && name.kind == NodeKind::NodeAdmin {
                    let t = self.view.read().await;
                    if t.is_fabric_primary(&self.me_id) {
                        self.held_name_in(&t)
                    } else {
                        drop(t);
                        self.writer_check(&name.mesh, name).await?
                    }
                } else {
                    self.writer_check(&name.mesh, name).await?
                };
                span.record("node", me.to_string().as_str());
                span.record("awaited", "writer-check");
                // A create's claim is the record's first write, and the node is starting (states.md "Who
                // sets each state": a create's starting from its launch, where no process can acknowledge).
                self.put_seeded(&me, name, node_id, NodeStatus::Starting, config, allocated, executable, tags, placed, of, &span, "writer-check").await
            },
            span.clone(),
        )
        .await;
        guard.done = true;
        record_claim(&span, &r);
        span.in_scope(|| tracing::info!("node.create: the name claim decided"));
        r
    }

    /// `node.create`'s name claim with an explicit `node_type` (normalizing `""` to `"node"`,
    /// refusing any value outside the shared identifier grammar by name `node_type`, and copying
    /// the exited node's `node_type` on recovery).
    pub async fn claim_with_node_type(&self, name: &PathName, node_id: &NodeId, node_type: &str, operation: &str, allocated: Option<&Allocation>, executable: Option<&rdm_core::binding::Binding>, of: ClaimFor) -> Result<(Claimed, String), RowRefused> {
        let me = self.held_name_now();
        let normalized = rdm_core::status::normalize_node_type(node_type);
        if let Some(reason) = rdm_core::status::node_type_refusal(&normalized) {
            return Err(RowRefused::Unwritten(format!("{me}: {reason}; nothing is claimed or written")));
        }
        let inherited_type = if matches!(of, ClaimFor::Recovery | ClaimFor::Replacement) {
            record_at(&*self.storage, name)
                .await
                .ok()
                .flatten()
                .filter(|p| p.status == NodeStatus::Deleted)
                .map(|p| self.written.node_type_of(&p.node_id).unwrap_or_else(|| p.node_type().to_string()))
        } else {
            None
        };
        let claimed = self.claim(name, node_id, operation, allocated, executable, of).await?;
        let effective = inherited_type.unwrap_or(normalized);
        self.written.hold_node_type(node_id, effective.clone());
        Ok((claimed, effective))
    }

    /// Attempt to update `node_type` through `node.update`: always refused by name (`node_type` is
    /// immutable once set at `node.create`).
    pub async fn update_node_type(&self, name: &PathName, node_id: &NodeId, requested_node_type: &str) -> Result<(), RowRefused> {
        let me = self.writer_check(&name.mesh, name).await?;
        let held_type = self.node_type_of(node_id, name);
        Err(RowRefused::Unwritten(format!("{me}: {}", rdm_core::status::node_type_update_refusal(&held_type, requested_node_type))))
    }

    /// The `node_type` this writer holds for `node_id`, falling back to `fallback.node_type()`.
    pub fn node_type_of(&self, node_id: &NodeId, fallback: &PathName) -> String {
        self.written.node_type_of(node_id).unwrap_or_else(|| fallback.node_type().to_string())
    }

    /// Move `record`'s row to `to` (`node.name.update`), preserving its config, tags and `node_type`.
    pub async fn rename_row(&self, record: &Node, to: &PathName) -> Result<NodeRow, RowRefused> {
        let me = self.writer_check(&record.mesh, to).await?;
        let known = self.written.known_row(&*self.storage, &record.node_id).await.map_err(|why| RowRefused::Unwritten(format!("{me}: {why}")))?;
        let from = known.as_ref().map_or_else(|| record.name.clone(), |r| r.name.clone());
        match move_row(&*self.storage, &self.written, &me, record, &from, to).await {
            Ok((_, Some(written))) => Ok(written),
            Ok((_, None)) => self.written.row_of(&record.node_id).filter(|r| r.name == *to).ok_or_else(|| RowRefused::Unwritten(format!("{me}: no record of {} stands at {to}", record.node_id))),
            Err(RenameRefused::NameHeld(h)) => Err(RowRefused::NameHeld(h)),
            Err(RenameRefused::Unwritten(why)) => Err(RowRefused::Unwritten(why)),
        }
    }

    /// Hold the row `node_id` holds at `name` as this writer's record of it, read from the store (R13:
    /// "Rebuild after a role change reads the mesh's own rows"), the same point read a claim or an
    /// arrival makes: a mesh leave's roster birth this writer's views never held (the admin created
    /// for a leaving mesh with no admin left joined after that birth stopped), whose `Deleted` row it
    /// is about to write. Nothing is written. `Ok(true)`: the row is held; `Ok(false)`: the store
    /// holds no row of this node at `name`. `Err` names the read that failed.
    pub async fn hold_row_of(&self, name: &PathName, node_id: &NodeId) -> Result<bool, String> {
        let row = self.storage.node(node_id).await.map_err(|e| format!("{}: the record of {node_id} could not be read at {name}: {e}", self.held_name_now()))?;
        match row.filter(|r| r.name == *name) {
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
    /// Beside the answer, the row the read returned (`None`: none stands at `name`), whatever it holds.
    pub async fn allocation_at(&self, name: &PathName, build_id: &crate::build::BuildId, operation: &str) -> Result<(Option<(NodeId, Allocation)>, Option<NodeRow>), String> {
        let row = record_at(&*self.storage, name).await.map_err(|e| format!("{}: the record at {name} could not be read for the allocation of {operation} (Build {build_id}): {e}", self.held_name_now()))?;
        let claimed = row.clone().filter(|r| r.status != NodeStatus::Deleted).and_then(|r| r.allocated.filter(|a| &a.build_id == build_id && a.operation == operation).map(|a| (r.node_id, a)));
        Ok((claimed, row))
    }

    /// A create that claimed `name` for `node_id` and did not complete lets go of its claim, so the
    /// next birth at the name is not refused by it: a release of the holder's own key only, never
    /// another node's row. One `via-claim-withdrawn` span says why.
    pub async fn withdraw(&self, name: &PathName, node_id: &NodeId, operation: &str, why: &str) {
        let mut me = self.held_name_now();
        let span = tracing::info_span!(
            "rdm.node_admin.node.remove.via-claim-withdrawn",
            node = %me,
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
            Ok(held) => {
                me = held;
                span.record("node", me.to_string().as_str());
                release_record(&*self.storage, &self.written, name, node_id).await.map_err(|e| RowRefused::Unwritten(format!("{me}: nodes.storage refused the release of {name} by {node_id}: {e}")))
            }
        };
        if let Ok(true) = r {
            self.written.forget(node_id);
        }
        match &r {
            Ok(true) => span.record("outcome", "withdrawn"),
            Ok(false) => span.record("outcome", "not-held").record("detail", format!("{me}: {node_id} holds no row at {name}; nothing is released").as_str()),
            Err(e) => span.record("outcome", e.outcome()).record("detail", e.to_string().as_str()),
        };
        span.in_scope(|| tracing::info!("node.create: the claim is withdrawn"));
    }

    /// Persist a birth's immutable declaration in its existing row before answering its join.
    pub async fn declare_app_types(&self, d: &rdm_core::MeshDigest, types: std::collections::BTreeSet<String>) -> Result<(), String> {
        let me = self.writer_check(&d.node.name.mesh, &d.node.name).await.map_err(|e| e.to_string())?;
        let configs = self.configs.as_ref().and_then(|slot| slot.get());
        let _one = match configs { Some(door) => Some(door.one_at_a_time().await), None => None };
        let mut row = match self.put_seeded(&me, &d.node.name, &d.node.node_id, crate::admin::node_status(d.status), &BTreeMap::new(), None, None, &std::collections::BTreeSet::new(), None, ClaimFor::Arrival, &tracing::Span::current(), "declaration").await.map_err(|e| e.to_string())? {
            Claimed::Written { row, .. } | Claimed::HeldByNode(row) => row,
        };
        if let Some(held) = &row.app_types {
            if held.incarnation == d.node.incarnation {
                if held.types == types { return Ok(()); }
                return Err("the exact birth already declared different app types".into());
            }
        }
        let declared = crate::storage::AppTypes { incarnation: d.node.incarnation.clone(), types };
        row = self.written.update(&*self.storage, &d.node.node_id, Some(row), |r| {
            r.app_types = Some(declared);
            r.clone()
        }).await?.ok_or_else(|| format!("{me}: no record of {} is held to keep its declaration", d.node.node_id))?;
        tracing::info_span!("rdm.node_admin.node.update.via-types", node = %d.node.name, node_id = %d.node.node_id, incarnation = %d.node.incarnation, types = row.app_types.as_ref().unwrap().types.len()).in_scope(|| tracing::info!("the birth's declaration is recorded"));
        Ok(())
    }

    /// The status `status` of `record`'s birth `incarnation` on its record, written whole
    /// ([`crate::storage::NodeRow::set_status`]) by the writer of its mesh's rows, the writer check of
    /// its `Deleted` row ([`Self::writes_unreached`]): any other admin refuses it by name, `not-mesh-primary`,
    /// and writes nothing (node-identity-and-state.md §4.3; states.md "Who sets each state"). A node with
    /// no record held anywhere is written nothing. One `rdm.node_admin.node.update.via-row-status` span.
    pub async fn status(&self, record: &Node, incarnation: &IncarnationId, status: NodeStatus) -> Result<(), RowRefused> {
        let me = match self.row_writer(record).await {
            Ok(me) => me,
            Err(refused) => {
                crate::status_rpc::row_status_refused_span(&self.held_name().await, &record.name, &record.node_id, incarnation, status, &refused);
                return Err(refused);
            }
        };
        let written = self.written.update_at_revision(&*self.storage, &record.node_id, None, |r| r.set_status(incarnation, status)).await;
        crate::status_rpc::row_status_span(&me, &record.name, &record.node_id, incarnation, status, &written);
        written.map(|_| ()).map_err(|e| RowRefused::Unwritten(format!("{me}: the {status:?} status of {} ({}) is not written: {e}", record.name, record.node_id)))
    }

    /// `NodeDeleted` (lifecycles/node-delete.md step 3): the row at the node's `path.name` (its `.old`
    /// path for a stopped predecessor) with status `Deleted`, the exact record the executor acted on
    /// and its config kept, a blind put. One `via-node-row` span.
    pub async fn deleted(&self, record: &Node, operation: &str) -> Result<(), RowRefused> {
        self.deleted_as(record, operation, false).await
    }

    /// [`Self::deleted`] of an unreached birth (node-recovery.md step 3): a node-admin's row is written by
    /// its mesh's elected primary, the mesh's one writer, and by the fabric-primary only as acting primary
    /// of a mesh with no primary (node-identity-and-state.md §4.3; R-OW1).
    pub async fn deleted_unreached(&self, record: &Node, operation: &str) -> Result<(), RowRefused> {
        self.deleted_as(record, operation, record.kind == NodeKind::NodeAdmin).await
    }

    async fn deleted_as(&self, record: &Node, operation: &str, fabric_recovery: bool) -> Result<(), RowRefused> {
        let known = self.known_meta(record).await;
        self.deleted_row(&record.name, &record.node_id, known.map(Some), operation, fabric_recovery).await
    }

    /// The config and tags of the row `record`'s node is written with: the ones this writer holds for
    /// it (`writer-record`), else those of the row the node holds now, a point read through the id
    /// index (`node-row`); none when it holds no row (`no-row`). A node's config and tags are never in
    /// the executor's record: they are properties only its record carries (Q203).
    async fn known_meta(&self, record: &Node) -> Result<(BTreeMap<String, String>, std::collections::BTreeSet<String>, &'static str), RowRefused> {
        let me = self.held_name().await;
        let held = self.written.row_of(&record.node_id).is_some();
        match self.written.known_row(&*self.storage, &record.node_id).await {
            Ok(Some(r)) => Ok((r.config, r.tags, if held { "writer-record" } else { "node-row" })),
            Ok(None) => Ok((BTreeMap::new(), Default::default(), "no-row")),
            Err(why) => Err(RowRefused::Unwritten(format!("{me}: {why}: the row of {} is not written without the config and tags it carries", record.name))),
        }
    }

    /// The `Deleted` row of a birth the executor holds no record of any more (a mesh leave's
    /// member that deleted the view after its exact Exited receipt, mesh-delete.md steps 3 and
    /// 7): the row at `name` for `node_id`, the config this writer holds for the node. A writer that
    /// holds no record of the node's row refuses by name and writes nothing: a `Deleted` row is never
    /// written with a config the writer does not know. One `via-node-row` span.
    pub async fn deleted_birth(&self, name: &PathName, node_id: &NodeId, operation: &str) -> Result<(), RowRefused> {
        let known = self.written.row_of(node_id).map(|r| (r.config, r.tags, "writer-record"));
        self.deleted_row(name, node_id, Ok(known), operation, false).await
    }

    /// The `Deleted` row of `node_id` at `name`, written from what the writer knows of it: its
    /// config and tags (the row keeps both, as it keeps the record), or the refusal that names why
    /// it knows neither. The store tombstones every tags index row of its labels with the write.
    async fn deleted_row(&self, name: &PathName, node_id: &NodeId, known: Result<Option<(BTreeMap<String, String>, std::collections::BTreeSet<String>, &'static str)>, RowRefused>, operation: &str, fabric_recovery: bool) -> Result<(), RowRefused> {
        let me = self.held_name_now();
        let span = tracing::info_span!(
            "rdm.node_admin.node.delete.via-node-row",
            node = %me,
            subject = %name,
            operation,
            node_id = %node_id,
            keys = tracing::field::Empty,
            config_from = tracing::field::Empty,
            tags = tracing::field::Empty,
            revision = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        if let Ok(Some((config, tags, config_from))) = &known {
            span.record("keys", config.len() as u64).record("config_from", *config_from).record("tags", labels(tags).as_str());
        }
        let r = tracing::Instrument::instrument(
            async {
                // "The fabric-primary writes a row only as acting primary of a mesh with no primary. No
                // other process writes a Node row" (node-identity-and-state.md §4.3): the fabric-primary
                // writes the deleted node-admin's row itself only while its mesh holds no primary but that
                // node; a mesh with an elected primary has its row written by that primary, its one writer.
                let me = self.row_writer_in(&*self.view.read().await, name, node_id, fabric_recovery)?;
                span.record("node", me.to_string().as_str());
                let Some((config, tags, _)) = known? else {
                    return Err(RowRefused::Unwritten(format!("{me}: holds no record of the row of {name} ({node_id}) and no executor record of it: its Deleted row is not written with a config this writer does not know")));
                };
                // The record is the one held, its facts, executable and app types kept: the node created
                // next at the name runs the executable (node-recovery.md "Retained").
                write_deleted(&*self.storage, &self.written, name, node_id, Some((config, tags))).await.map(|_| ()).map_err(|e| match e {
                    DeletedRefused::NameHeld(held) => RowRefused::NameHeld(held),
                    DeletedRefused::Unwritten(why) => RowRefused::Unwritten(format!("{me}: nodes.storage refused the Deleted row of {name} ({node_id}): {why}")),
                })
            },
            span.clone(),
        )
        .await;
        match &r {
            Ok(()) => span.record("outcome", "row-deleted").record("revision", self.written.row_of(node_id).map_or(0, |w| w.revision)),
            Err(e) => span.record("outcome", e.outcome()).record("detail", e.to_string().as_str()),
        };
        span.in_scope(|| tracing::info!("node.delete: the Deleted row decided; the record, its config and its tags stay, its labels leave the tags index"));
        r
    }

    /// The row of `node_id` at `name`, `status`: a plain create's config is `own`, its create spec's
    /// (Q166); any other's is seeded from the previous row at the `path.name` (another node's row
    /// there: the claim refuses it unless it is `Deleted`), else the `<name>.old` row, else `own`. Its
    /// tags: for a recovery's claim ([`ClaimFor::Recovery`])
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
    async fn put_seeded(&self, me: &PathName, name: &PathName, node_id: &NodeId, status: NodeStatus, own: &BTreeMap<String, String>, allocated: Option<&Allocation>, executable: Option<&rdm_core::binding::Binding>, tags: &std::collections::BTreeSet<String>, placed: Option<(rdm_core::meta::StorageMeta, rdm_core::meta::PlacementMeta)>, of: ClaimFor, span: &tracing::Span, prefix: &str) -> Result<Claimed, RowRefused> {
        let claimed = match self.written.known_row(&*self.storage, node_id).await {
            Ok(Some(held)) => Claimed::HeldByNode(held),
            Ok(None) => seeded_put(&*self.storage, &self.written, me, name, node_id, status, own, allocated, executable, tags, placed, of, span, prefix).await?,
            Err(e) => return Err(RowRefused::Unwritten(format!("{me}: the row {node_id} holds could not be read before its row at {name} was written: {e}"))),
        };
        if of == ClaimFor::Successor {
            if let Claimed::Written { seeded_from: Some(from), .. } = &claimed {
                if let Some(node_type) = self.written.node_type_of(from) {
                    self.written.hold_node_type(node_id, node_type);
                }
            }
            // The claim's last write, on its first run and on any repeat: the retained row is marked.
            mark_retained(&*self.storage, me, &self.written, name, node_id).await.map_err(RowRefused::Unwritten)?;
        }
        Ok(claimed)
    }

    /// The births of `now`, the view being installed over `prev`, whose rows this admin writes: the
    /// members of each mesh it writes for in `now` (its primary, or the fabric-primary's for a mesh
    /// with none), its own birth included (R-OW1). Every member of a mesh whose writing it took with this
    /// install (the rebuild after a role change, which never overwrites a row a node holds); else
    /// each birth of that mesh this tenure's views have not held before. A birth is an arrival once
    /// per tenure: one the view dropped and holds again was written when it first arrived. Read
    /// under the view lock, so of two installs each sees the arrivals its own change made.
    pub fn arrivals(&self, prev: &Topology, now: &Topology) -> Vec<Node> {
        let me = &self.me_id;
        let mut held = self.held.lock().unwrap();
        held.retain(|mesh, _| writes_for(now, me, mesh));
        // A mesh whose writing this admin gave up: its next take reads its records again.
        for mesh in self.written.filled_meshes().into_iter().filter(|m| !writes_for(now, me, m)) {
            self.written.release(&mesh);
        }
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
                self.to_fill.lock().unwrap().push(mesh.clone());
            }
            out.extend(now.members().filter(|n| n.mesh == mesh && n.incarnation_id.clone().is_some_and(|inc| tenure.insert((n.node_id.clone(), inc)))).cloned());
        }
        out
    }

    /// Write the row of each birth in `arrivals`, the writer's view `now` having first held it; a
    /// birth whose row is seeded at its arrival with a config (from a previous row at its name), one
    /// its join did not hand it, is told that config.
    pub async fn write_arrivals(&self, now: &Topology, arrivals: Vec<Node>) {
        let me = self.held_name_in(now);
        let taken: Vec<String> = std::mem::take(&mut *self.to_fill.lock().unwrap());
        for mesh in taken {
            // A mesh this take already filled (the mesh-primary's own, read whole as its status rows are
            // folded, `crate::status_rpc::RoleFold::on_install`) is not read again.
            let (filled, outcome) = if self.written.filled(&mesh) {
                (Ok(self.written.rows_of(&mesh).len()), "held")
            } else {
                let filled = self.written.fill(&*self.storage, &mesh).await;
                let outcome = if filled.is_ok() { "filled" } else { "fill-refused" };
                (filled, outcome)
            };
            let pushed = filled.is_ok();
            tracing::info_span!("rdm.node_admin.node.update.via-records-filled", node = %me, mesh = %mesh, records = filled.as_ref().map(|n| *n as u64).unwrap_or(0), outcome, detail = %filled.err().map(|e| e.to_string()).unwrap_or_default(), "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the writing of a mesh's records was taken: its records are read whole, once"));
            // A successor owes every row's push up to the fabric-primary again, from the row (Q235 P3).
            if let (true, Some(door)) = (pushed, self.configs.as_ref().and_then(|slot| slot.get())) {
                door.owe_pushes(&me, &self.me_id, now, &mesh);
            }
        }
        for birth in arrivals {
            let span = tracing::info_span!(
                "rdm.node_admin.node.create.via-node-row",
                node = %me,
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
            let r = tracing::Instrument::instrument(self.rebuilt_row(&me, &birth, &span), span.clone()).await;
            record_claim(&span, &r);
            span.in_scope(|| tracing::info!("node.create: the birth's row decided"));
            match r {
                // A row seeded at the arrival (from the previous row at its name) carries a config the birth's
                // join did not hand it; a row the node held was handed at its join (Q203: no digest
                // carries config to compare).
                Ok(Claimed::Written { row, .. }) if !row.config.is_empty() => self.tell(&me, now, &birth, row, &span),
                _ => {}
            }
        }
    }

    /// The row of `birth` as the writer meets it at its arrival: the row the node holds wherever it
    /// stands (a rename may have moved it off the name the view holds, found through the id index), any
    /// release its `renamed_from` names finished when still pending (proposal
    /// node-properties-own-commands.md §2.1: "a mesh-primary's rebuild from its mesh's rows after a
    /// role change"); else the row seeded at the view's name ([`Self::put_seeded`]).
    async fn rebuilt_row(&self, me: &PathName, birth: &Node, span: &tracing::Span) -> Result<Claimed, RowRefused> {
        let holds = self.written.known_row(&*self.storage, &birth.node_id).await.map_err(|why| RowRefused::Unwritten(format!("{me}: {why}")))?;
        let claimed = match holds {
            Some(row) => Claimed::HeldByNode(row),
            // A recovery's claim precedes its birth's arrival (the create claims before it launches),
            // so an arrival that finds no row of its node is never a recovery's: a plain create's seed.
            None => self.put_seeded(me, &birth.name, &birth.node_id, birth.status, &BTreeMap::new(), None, None, &std::collections::BTreeSet::new(), None, ClaimFor::Arrival, span, "").await?,
        };
        if let Claimed::HeldByNode(row) = &claimed {
            if let Err(why) = finish_release(&*self.storage, &self.written, me, row, "rebuild").await {
                span.record("detail", format!("{me}: the release {} of {} is still pending: {why}", row.renamed_from.as_ref().map(|f| f.to_string()).unwrap_or_default(), row.node_id).as_str());
            }
        }
        Ok(claimed)
    }

    /// Tell a birth the config its row carries (node-config-update.md step 3: the node applies the
    /// map) through the config door, which writes the record and calls
    /// the birth through its mesh-primary. On its own task, under the row write's span: the view
    /// install that found the birth never waits on a call to it (the join door installs the view
    /// before it answers the joiner).
    fn tell(&self, me: &PathName, now: &Topology, birth: &Node, row: NodeRow, span: &tracing::Span) {
        let Some(door) = self.configs.as_ref().and_then(|slot| slot.get().cloned()) else {
            span.record("told", "no-config-door");
            return;
        };
        let Some(incarnation) = birth.incarnation_id.clone() else { return };
        span.record("told", "config-door");
        let req = rdm_core::status::StatusRequest::UpdateNodeConfig { node_id: birth.node_id.clone(), incarnation, config: row.config };
        let (me, me_id, view, storage) = (me.clone(), self.me_id.clone(), now.clone(), self.storage.clone());
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

/// The births `records` hold deleted: each birth a record names its predecessor, re-created at its
/// path.name under another node id (node-recovery.md: "A birth that comes back after it was re-created
/// is a deleted birth"), with the node id of the record that names it.
fn re_created(records: &[NodeRow]) -> HashMap<crate::storage::BirthRef, NodeId> {
    records.iter().filter_map(|r| r.predecessor.clone().map(|p| (p, r.node_id.clone()))).collect()
}

/// The records a reader of a mesh's node records folds from `records`, the versions a read of storage
/// answered (Q109: "every reader folds the stored rows the same way, and a row that does not follow the
/// fold is ignored by every reader, refused by no storage"; build.md: an os-stopped executor that
/// resumes after the next attempt is claimed "is a deleted birth whose writes every reader refuses"):
/// a node's record whose version was written by its own birth after another record named that birth
/// its predecessor (the birth re-created at its path.name) is ignored, one
/// `rdm.node_admin.node.reject.via-deleted-birth` span naming it. The record of a re-created birth
/// stands `Deleted`, written by its mesh-primary before the re-creation's claim, so no version its own
/// birth writes follows it. A version of another node's record that birth wrote is not judged by it:
/// a whole record keeps no earlier version, and that birth's writes from before it was deleted are the
/// record's as much as any (storage-whole-records.md). `read` names the read on the span.
pub fn fold_records(read: &str, records: Vec<NodeRow>) -> Vec<NodeRow> {
    let deleted = re_created(&records);
    if deleted.is_empty() {
        return records;
    }
    records
        .into_iter()
        .filter(|r| {
            let Some(w) = r.writer.as_ref().filter(|w| w.node_id == r.node_id) else { return true };
            match deleted.get(w) {
                Some(by) => {
                    ignored_span("", read, r, w, &format!("{by} re-created it at {}", r.name));
                    false
                }
                None => true,
            }
        })
        .collect()
}

/// [`ignored_span`] for a reader outside this module: `read` names the read.
pub(crate) fn ignored_version(read: &str, row: &NodeRow, writer: &crate::storage::BirthRef, held: &str) {
    ignored_span("", read, row, writer, held);
}

/// The span of a version of `row`'s record a reader ignored: written by `writer`, a birth the reader's
/// fold holds deleted (`held`, what holds it so).
fn ignored_span(me: &str, read: &str, row: &NodeRow, writer: &crate::storage::BirthRef, held: &str) {
    let span = tracing::info_span!(
        "rdm.node_admin.node.reject.via-deleted-birth",
        node = tracing::field::Empty,
        read,
        subject = %row.name,
        node_id = %row.node_id,
        status = ?row.status,
        revision = row.revision,
        writer = %format!("{}@{}", writer.node_id, writer.incarnation_id.as_str()),
        held,
        outcome = "ignored",
        "otel.kind" = "internal",
    );
    if !me.is_empty() {
        span.record("node", me);
    }
    span.in_scope(|| tracing::info!("a version of the record written by a deleted birth is ignored on read; storage keeps it as written"));
}

/// The record at `name`: the one the `path.name` index names there while that record is still at
/// `name` (a record a rename moved off it is at its new name, not here); `None` otherwise.
pub async fn record_at(storage: &dyn NodesStorage, name: &PathName) -> Result<Option<NodeRow>, crate::storage_error::StorageError> {
    Ok(storage.node_at(name).await?.filter(|r| r.name == *name))
}

/// [`write_record`] as a method of any nodes store: a claim another node holds is the error, named.
#[async_trait::async_trait]
pub trait WriteRecord {
    /// Write `row` whole as its node's record ([`write_record`]).
    async fn write_record(&self, row: &NodeRow) -> Result<(), String>;
}

#[async_trait::async_trait]
impl<T: NodesStorage + Sized> WriteRecord for T {
    async fn write_record(&self, row: &NodeRow) -> Result<(), String> {
        write_record(self, row).await?.map_err(|held| held.to_string())
    }
}

#[async_trait::async_trait]
impl WriteRecord for dyn NodesStorage {
    async fn write_record(&self, row: &NodeRow) -> Result<(), String> {
        write_record(self, row).await?.map_err(|held| held.to_string())
    }
}

/// [`record_at`] as a method of any nodes store.
#[async_trait::async_trait]
pub trait RecordAt {
    /// The record at `name` ([`record_at`]).
    async fn record_at(&self, name: &PathName) -> Result<Option<NodeRow>, crate::storage_error::StorageError>;
}

#[async_trait::async_trait]
impl<T: NodesStorage + ?Sized> RecordAt for T {
    async fn record_at(&self, name: &PathName) -> Result<Option<NodeRow>, crate::storage_error::StorageError> {
        Ok(self.node_at(name).await?.filter(|r| r.name == *name))
    }
}

/// Write `row` whole as its node's record by a writer that holds no copy of it: its name claimed for
/// the node ([`take_name`], `NameHeld` naming another node's live claim, nothing written), the record
/// written, and its index entries moved from the record storage held for the node.
pub async fn write_record(storage: &dyn NodesStorage, row: &NodeRow) -> Result<Result<(), NameHeld>, String> {
    let written = WrittenRows::default();
    let _one = written.lock(&row.node_id).await;
    written.known_row(storage, &row.node_id).await?;
    if let Err(held) = take_name(storage, &row.name, &row.node_id).await.map_err(|e| e.to_string())? {
        return Ok(Err(held));
    }
    written.put(storage, row).await.map_err(|e| e.to_string())?;
    Ok(Ok(()))
}

/// Claim `name` for `node_id` (node-name-update.md, "Claim and move": `NameHeld { by: node.id }`). A
/// claim another node holds is taken over only when that node's record is `Deleted` (a delete releases
/// its name, node-delete.md, and one it did not finish releasing is released here); otherwise
/// `NameHeld` naming it, and nothing is written.
pub async fn take_name(storage: &dyn NodesStorage, name: &PathName, node_id: &NodeId) -> Result<Result<(), NameHeld>, crate::storage_error::StorageError> {
    match storage.claim_node_name(name, node_id).await? {
        Ok(()) => Ok(Ok(())),
        Err(held) => {
            if !storage.node(&held.by).await?.is_some_and(|r| released_its_name(&r)) {
                return Ok(Err(held));
            }
            storage.release_node_name(name, &held.by).await?;
            storage.claim_node_name(name, node_id).await
        }
    }
}

/// `node_id` lets go of `name`: when its record is still at `name` (a create's withdrawn claim: the
/// node never ran), the record is appended `Withdrawn` (R-SR2: nothing is removed), then its claim
/// there goes; a claim whose release a crash cut off is released by the next claimant over the
/// `Withdrawn` record ([`take_name`]). Returns whether it held the name.
pub async fn release_record(storage: &dyn NodesStorage, written: &WrittenRows, name: &PathName, node_id: &NodeId) -> Result<bool, crate::storage_error::StorageError> {
    let _one = written.lock(node_id).await;
    if storage.name_claim(name).await? != Some(node_id.clone()) {
        return Ok(false);
    }
    let held = match written.row_of(node_id) {
        Some(r) => Some(r),
        None => storage.node(node_id).await?,
    };
    if held.as_ref().map_or(true, |r| r.name == *name) {
        let mut row = held.unwrap_or_else(|| NodeRow::new(name.clone(), node_id.clone(), NodeStatus::Withdrawn));
        row.status = NodeStatus::Withdrawn;
        row.revision += 1;
        storage.put_node(&row).await?;
        written.forget(node_id);
    }
    storage.release_node_name(name, node_id).await?;
    Ok(true)
}

/// Whether `row`'s node released its name: its record is `Deleted`, or `Withdrawn` (a create withdrawn
/// before its node ran), so another node's claim takes the name over it.
pub fn released_its_name(row: &NodeRow) -> bool {
    matches!(row.status, NodeStatus::Deleted | NodeStatus::Withdrawn)
}

/// Why the birth of `node_id` at `name` is a deleted birth on the store's node rows, if it is one
/// (node-investigation.md: "A node that comes back after it was re-created is a deleted birth"; Q181):
/// its own record released its name (`Deleted` or `Withdrawn`), or another node's live claim holds
/// `name` (a create at the path.name re-created it under a new node.id). `None` while the rows hold it
/// live, or hold nothing of it (a birth no row was written for yet). Read only: two point reads, nothing
/// written, no timer.
pub async fn deleted_birth(storage: &dyn NodesStorage, name: &PathName, node_id: &NodeId) -> Result<Option<String>, crate::storage_error::StorageError> {
    if let Some(row) = storage.node(node_id).await?.filter(released_its_name) {
        return Ok(Some(format!("{name} ({node_id}) is a deleted birth: its row at {} is {:?}", row.name, row.status)));
    }
    if let Some(by) = storage.name_claim(name).await?.filter(|by| by != node_id) {
        if !storage.node(&by).await?.is_some_and(|r| released_its_name(&r)) {
            return Ok(Some(format!("{name} ({node_id}) is a deleted birth: {by} holds the name")));
        }
    }
    Ok(None)
}

/// Why a `Deleted` record was not written.
pub enum DeletedRefused {
    /// Another node's live claim holds the name.
    NameHeld(NameHeld),
    /// The record could not be written: why.
    Unwritten(String),
}

/// The `Deleted` record of `node_id` at `name` (node-delete.md step 3): the record held for it, its
/// facts kept, with `known` (config and tags) when given, written whole; then its name's claim is
/// released ("after releasing node.id and name"). A live claim of another node at `name` refuses it
/// by name and nothing is written.
pub async fn write_deleted(storage: &dyn NodesStorage, written: &WrittenRows, name: &PathName, node_id: &NodeId, known: Option<(BTreeMap<String, String>, std::collections::BTreeSet<String>)>) -> Result<NodeRow, DeletedRefused> {
    let unwritten = |e: crate::storage_error::StorageError| DeletedRefused::Unwritten(e.to_string());
    let _one = written.lock(node_id).await;
    if let Some(by) = storage.name_claim(name).await.map_err(unwritten)?.filter(|by| by != node_id) {
        if !storage.node(&by).await.map_err(unwritten)?.is_some_and(|r| released_its_name(&r)) {
            return Err(DeletedRefused::NameHeld(NameHeld { name: name.clone(), by }));
        }
    }
    let held = written.known_row(storage, node_id).await.map_err(DeletedRefused::Unwritten)?;
    let mut row = held.unwrap_or_else(|| NodeRow::new(name.clone(), node_id.clone(), NodeStatus::Deleted));
    row.name = name.clone();
    row.status = NodeStatus::Deleted;
    row.allocated = None;
    row.renamed_from = None;
    row.inherited_by = None;
    if let Some((config, tags)) = known {
        row.config = config;
        row.tags = tags;
    }
    written.put(storage, &row).await.map_err(unwritten)?;
    storage.release_node_name(name, node_id).await.map_err(unwritten)?;
    Ok(row)
}

/// [`NodeRows::put_seeded`]'s one write, by `me` into `storage`: the record of `node_id` at `name`, its
/// config and tags seeded as that method says, written under the node's lock after its name's claim.
/// A record the same node holds already is left as it is. A recovery's record names the birth it
/// replaced, the newest birth of the `Deleted` record at the name (`predecessor`).
#[allow(clippy::too_many_arguments)]
async fn seeded_put(storage: &dyn NodesStorage, written: &WrittenRows, me: &PathName, name: &PathName, node_id: &NodeId, status: NodeStatus, own: &BTreeMap<String, String>, allocated: Option<&Allocation>, executable: Option<&rdm_core::binding::Binding>, created_with: &std::collections::BTreeSet<String>, placed: Option<(rdm_core::meta::StorageMeta, rdm_core::meta::PlacementMeta)>, of: ClaimFor, span: &tracing::Span, prefix: &str) -> Result<Claimed, RowRefused> {
    let _one = written.lock(node_id).await;
    let mut awaited = prefix.to_string();
    span.record("awaiting", "read-previous-row");
    let previous = written.fold_read(me, record_at(storage, name).await.map_err(|e| RowRefused::Unwritten(format!("{me}: the previous record at {name} could not be read for {node_id}: {e}")))?);
    push_awaited(&mut awaited, "read-previous-row");
    span.record("awaited", awaited.as_str());
    if let Some(p) = previous.as_ref().filter(|p| p.node_id == *node_id) {
        span.record("awaiting", "done");
        written.hold(p);
        return Ok(Claimed::HeldByNode(p.clone()));
    }
    // The `.old` record seeds the create at the name a replaced predecessor released (node-replace.md
    // step 3) and the create of an exited predecessor's successor at the path.
    let seed = match previous {
        Some(p) => Some(p),
        None => {
            span.record("awaiting", "read-old-row");
            let old = record_at(storage, &name.renamed()).await.map_err(|e| RowRefused::Unwritten(format!("{me}: the .old record at {} could not be read for {node_id}: {e}", name.renamed())))?.filter(|p| p.node_id != *node_id);
            push_awaited(&mut awaited, "read-old-row");
            span.record("awaited", awaited.as_str());
            old
        }
    };
    // The config and the tags of a Build's create, a replace's successor and a reconcile's replacement
    // included, are its entry's (`own`, `created_with`; Q305): a Build provides the whole map of a node it
    // replaces, never the predecessor's, and an empty one is empty. A recovery's claim takes the Deleted record's at this
    // name (Q166) and an arrival's the seed's: no entry names them.
    let inherited = seed.as_ref().filter(|p| match of {
        ClaimFor::Create | ClaimFor::Arrival => false,
        ClaimFor::Recovery | ClaimFor::Replacement => p.name == *name && p.status == NodeStatus::Deleted,
        ClaimFor::Successor => p.name != *name || p.status == NodeStatus::Deleted,
    });
    let tags = match of {
        ClaimFor::Create | ClaimFor::Successor | ClaimFor::Replacement => created_with.clone(),
        ClaimFor::Arrival | ClaimFor::Recovery => inherited.map(|p| p.tags.clone()).unwrap_or_default(),
    };
    let config = match of {
        ClaimFor::Create | ClaimFor::Successor | ClaimFor::Replacement => own.clone(),
        ClaimFor::Arrival | ClaimFor::Recovery => inherited.or(seed.as_ref()).map_or_else(|| own.clone(), |p| p.config.clone()),
    };
    // The executable beside the config: the one the create launches (`executable`), else the seed's.
    let executable = executable.cloned().or_else(|| seed.as_ref().and_then(|p| p.executable.clone()));
    let predecessor = seed.as_ref().filter(|p| matches!(of, ClaimFor::Recovery | ClaimFor::Replacement) && p.status == NodeStatus::Deleted).and_then(|p| p.births.last().map(|b| crate::storage::BirthRef { node_id: p.node_id.clone(), incarnation_id: b.incarnation_id.clone() }));
    if let Some(p) = &seed {
        span.record("seeded_from", p.node_id.as_str());
    }
    span.record("keys", config.len() as u64).record("tags", labels(&tags).as_str());
    // The storage and placement the claim is handed (its create spec's), else the seed's.
    let (storage_meta, placement) = placed.map_or_else(|| (seed.as_ref().and_then(|p| p.storage_persistence), seed.as_ref().and_then(|p| p.placement)), |(s, p)| (Some(s), Some(p)));
    let row = NodeRow { config, tags, allocated: allocated.cloned(), executable, predecessor, storage_persistence: storage_meta, placement, topology_storage_provider: written.topology_storage_provider(), ..NodeRow::new(name.clone(), node_id.clone(), status) };
    span.record("awaiting", "put-node-row");
    let claimed = take_name(storage, name, node_id).await;
    let put = match claimed {
        Ok(Ok(())) => written.put(storage, &row).await.map(Ok),
        other => other,
    };
    push_awaited(&mut awaited, "put-node-row");
    span.record("awaited", awaited.as_str());
    match put {
        Ok(Ok(())) => {
            span.record("awaiting", "done");
            Ok(Claimed::Written { row, seeded_from: seed.map(|p| p.node_id) })
        }
        Ok(Err(held)) => Err(RowRefused::NameHeld(held)),
        Err(e) => Err(RowRefused::Unwritten(format!("{me}: nodes.storage refused the record of {name} ({node_id}): {e}"))),
    }
}

/// A replace successor's claim's last write (node-replace.md Inherited: "Retained predecessor labels
/// are removed from the active index, not erased from its row"): the retained predecessor's record at
/// `<name>.old` is written again carrying `inherited_by: successor`, its config and tags kept; every
/// label of it leaves the tags index with that write ([`NodeRow::tag_moves`]). A record that says it
/// already, or no retained record (the predecessor was deleted), writes nothing. One
/// `rdm.node_admin.node.update.via-inherited-row` span, its outcome `inherited`, `already-inherited`,
/// `none-retained` or `row-refused`; `Err` names the refusal.
async fn mark_retained(storage: &dyn NodesStorage, me: &PathName, written: &WrittenRows, name: &PathName, successor: &NodeId) -> Result<(), String> {
    let retained = name.renamed();
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-inherited-row",
        node = %me,
        subject = %retained,
        node_id = tracing::field::Empty,
        inherited_by = %successor,
        tags = tracing::field::Empty,
        outcome = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let r = tracing::Instrument::instrument(
        async {
            let row = match record_at(storage, &retained).await {
                Ok(Some(row)) if row.node_id != *successor => row,
                Ok(_) => return Ok("none-retained"),
                Err(e) => return Err(format!("{me}: the retained record at {retained} could not be read to mark it inherited by {successor}: {e}")),
            };
            span.record("node_id", row.node_id.as_str()).record("tags", labels(&row.tags).as_str());
            if row.inherited_by.as_ref() == Some(successor) {
                return Ok("already-inherited");
            }
            let node_id = row.node_id.clone();
            written.update(storage, &node_id, Some(row), |r| r.inherited_by = Some(successor.clone())).await.map_err(|why| format!("{me}: the retained record at {retained}: {why}"))?;
            Ok("inherited")
        },
        span.clone(),
    )
    .await;
    match &r {
        Ok(outcome) => span.record("outcome", *outcome),
        Err(why) => span.record("outcome", "row-refused").record("detail", why.as_str()),
    };
    span.in_scope(|| tracing::info!("node.replace: the retained row's labels leave the tags index"));
    r.map(|_| ())
}

/// Fabric recovery's hand-over of the `path.name` an admin started on an inherited root takes
/// (fabric-recovery.md: the re-created node-admin is a new node over its inherited data dir: a new
/// node.id and incarnation, the same path.name; "Nothing alive": "node.delete of the exited instance,
/// then node.create at the same path.name, with a new node.id over the inherited data dir", "A node's
/// name is released by node recovery's node.delete and taken by its node.create, which carries a new
/// node.id"). Fabric recovery starts after every node-admin is lost, so this admin, `node_id`, is the
/// only writer its mesh has, and these are the first writes of its birth, before anything else of it
/// is written or published:
///
/// 1. node.delete of the inherited birth: its live row at the name written `Deleted`, its config and
///    tags kept (one `rdm.node_admin.node.delete.via-node-row` span): a `Deleted` record names no node
///    to reach;
/// 2. node.create's claim of the name under `node_id` ([`take_name`]), the row
///    `Pending`, its config and tags seeded from the `Deleted` row ([`ClaimFor::Recovery`]; one
///    `rdm.node_admin.node.create.via-node-row` span, `moment` `claim`).
///
/// Another node's live row at the name is `NameHeld` naming it, and nothing of this birth is written.
pub async fn take_inherited_name(storage: &dyn NodesStorage, written: &WrittenRows, inherited: &crate::storage::NodeRecord, node_id: &NodeId) -> Result<Claimed, RowRefused> {
    let (me, name) = (&inherited.name, &inherited.name);
    let operation = "fabric-recovery";
    let held = storage.node(&inherited.node_id).await.map_err(|e| RowRefused::Unwritten(format!("{me}: the record of the inherited birth {} could not be read: {e}", inherited.node_id)))?;
    if let Some(p) = held.filter(|p| p.name == *name && p.status != NodeStatus::Deleted) {
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
        written.hold(&p);
        let r = match tracing::Instrument::instrument(write_deleted(storage, written, name, &p.node_id, None), span.clone()).await {
            Ok(_) => Ok(()),
            Err(DeletedRefused::NameHeld(held)) => Err(RowRefused::NameHeld(held)),
            Err(DeletedRefused::Unwritten(why)) => Err(RowRefused::Unwritten(format!("{me}: nodes.storage refused the Deleted row of the inherited birth at {name} ({}): {why}", p.node_id))),
        };
        match &r {
            Ok(()) => span.record("outcome", "row-deleted"),
            Err(e) => span.record("outcome", e.outcome()).record("detail", e.to_string().as_str()),
        };
        span.in_scope(|| tracing::info!("node.delete: the inherited birth's Deleted row decided; its record, config and tags stay"));
        r?;
    }
    own_claim(None, storage, written, name, node_id, operation, ClaimFor::Recovery, None, None).await
}

/// Day 0's claim of the bootstrap admin's own `path.name` (R-OW1 item 2: the bootstrap admin, as
/// mesh1's primary, claims (mesh1.admin, 1) and writes its own row through the same validation and
/// storage path as any create, the first write of its birth, before Ready; no bypass; a conflicting
/// holder is NameHeld, never overwritten). The bootstrap admin, `node_id` at
/// `name`, is its mesh's primary from its birth and the only admin of its fabric, so it is the writer of
/// its own row; the claim is the first write of its birth, made before anything else of it is written or
/// published. It is the create door's claim ([`ClaimFor::Create`], [`take_name`]), the record
/// `Pending`, one `rdm.node_admin.node.create.via-node-row` span, `moment` `claim`.
/// Another node's live claim of the name is `NameHeld` naming it, and nothing is written. The record
/// carries `executable`, the binding the bootstrap admin's birth runs (build-deployment.md §2.3), as a
/// create's claim carries a launched node's.
pub async fn claim_day0_name(storage: &dyn NodesStorage, written: &WrittenRows, name: &PathName, node_id: &NodeId, executable: Option<&rdm_core::binding::Binding>) -> Result<Claimed, RowRefused> {
    own_claim(None, storage, written, name, node_id, "fabric-create", ClaimFor::Create, executable, Some(crate::accepted::root_placed(name))).await
}

/// The adoption launch's claim of its own new `path.name` under an in-force fabric.delete (Q159;
/// fabric-recovery.md "Nothing alive": "the adoption launch is created at a new path.name of the
/// node-admin type in an existing, not-yet-deleted mesh ... this is the one create a delete admits"): the
/// create door's claim ([`ClaimFor::Create`]), the first write of its birth, operation `fabric-recovery`,
/// one `rdm.node_admin.node.create.via-node-row` span, `moment` `claim`. Another node's live claim of the
/// name is `NameHeld` naming it, and nothing is written.
pub async fn claim_adopted_name(storage: &dyn NodesStorage, written: &WrittenRows, name: &PathName, node_id: &NodeId) -> Result<Claimed, RowRefused> {
    own_claim(None, storage, written, name, node_id, "fabric-recovery", ClaimFor::Create, None, None).await
}

/// [`claim_day0_name`] as a child of `boot` without entering `boot` (the boot root is never entered so
/// background tasks spawned during start do not inherit it and its close time stays at `via-ready`).
pub async fn claim_day0_name_in_boot(boot: &tracing::Span, storage: &dyn NodesStorage, written: &WrittenRows, name: &PathName, node_id: &NodeId, executable: Option<&rdm_core::binding::Binding>) -> Result<Claimed, RowRefused> {
    own_claim(Some(boot), storage, written, name, node_id, "fabric-create", ClaimFor::Create, executable, Some(crate::accepted::root_placed(name))).await
}

/// The claim of `name` by the admin born at it as `node_id`, the writer of its own row before it holds a
/// view: [`seeded_put`] under one `rdm.node_admin.node.create.via-node-row` span, `moment` `claim`, the
/// one write a claim is wherever it is made.
#[allow(clippy::too_many_arguments)]
async fn own_claim(parent: Option<&tracing::Span>, storage: &dyn NodesStorage, written: &WrittenRows, name: &PathName, node_id: &NodeId, operation: &str, of: ClaimFor, executable: Option<&rdm_core::binding::Binding>, placed: Option<(rdm_core::meta::StorageMeta, rdm_core::meta::PlacementMeta)>) -> Result<Claimed, RowRefused> {
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
    let r = tracing::Instrument::instrument(seeded_put(storage, written, name, name, node_id, NodeStatus::Pending, &BTreeMap::new(), None, executable, &std::collections::BTreeSet::new(), placed, of, &span, ""), span.clone()).await;
    guard.done = true;
    record_claim(&span, &r);
    span.in_scope(|| tracing::info!("node.create: the name claim decided"));
    r
}

/// Why a node row's move was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenameRefused {
    /// The new name is held by another node: the claim refused it and nothing was written.
    NameHeld(crate::storage::NameHeld),
    /// The move could not be written: why, whole.
    Unwritten(String),
}

/// The node's record moves from `from` to `to` (node-name-update.md, "Claim and move"): the name claim
/// at `to` (`NameHeld { by: node.id }` when another node's live claim holds it), then one write of the
/// node's record carrying `renamed_from: from`, then the release of the old claim. The step resumes from
/// its receipts: a retry, or a successor writer resuming it, finds `to` claimed by this node and
/// continues from the record write; a record already at `to` is a move made before, and the release its
/// `renamed_from` names is finished when still pending (`finish_release`). A release that fails after
/// the write leaves both names held and answers [`Moved::ReleasePending`]: nothing is undone and no
/// second record is written. The record written starts from the one the writer holds, its facts kept;
/// a node it holds no record of is written from `record`'s config. The one writer of a rename (R13:
/// `node.name.update` is one write path, `crate::node_name_update`). Answers what it did and the
/// record written, if it wrote one.
pub async fn move_row(storage: &dyn NodesStorage, written: &WrittenRows, me: &PathName, record: &Node, from: &PathName, to: &PathName) -> Result<(Moved, Option<NodeRow>), RenameRefused> {
    if from == to {
        return Ok((Moved::Renamed, None));
    }
    let node_id = &record.node_id;
    let _one = written.lock(node_id).await;
    let held = written.known_row(storage, node_id).await.map_err(|why| RenameRefused::Unwritten(format!("{me}: {why}")))?;
    if let Some(held) = held.as_ref().filter(|r| r.name == *to) {
        // A move made before: its record names the name it came from (a record written before
        // `renamed_from` existed names none, and the name is this move's own `from`).
        let source = held.renamed_from.clone().unwrap_or_else(|| from.clone());
        return Ok(match release_source(storage, me, node_id, to, &source, "retry").await {
            Ok(true) => (Moved::Renamed, Some(held.clone())),
            Ok(false) => (Moved::AlreadyMoved, Some(held.clone())),
            Err(why) => (Moved::ReleasePending { why }, Some(held.clone())),
        });
    }
    take_name(storage, to, node_id).await.map_err(|e| RenameRefused::Unwritten(format!("{me}: nodes.storage refused the claim of {to} for {node_id}: {e}")))?.map_err(RenameRefused::NameHeld)?;
    let mut row = held.unwrap_or_else(|| NodeRow::new(from.clone(), node_id.clone(), record.status));
    row.name = to.clone();
    row.allocated = None;
    row.renamed_from = Some(from.clone());
    row.inherited_by = None;
    written.put(storage, &row).await.map_err(|e| RenameRefused::Unwritten(format!("{me}: nodes.storage refused the record of {node_id} at {to}: {e}")))?;
    // The new name is the node's: it lets go of the old one.
    match storage.release_node_name(from, node_id).await {
        Ok(()) => Ok((Moved::Renamed, Some(row))),
        Err(e) => Ok((Moved::ReleasePending { why: format!("nodes.storage refused the release of {from}: {e}") }, Some(row))),
    }
}

/// The release a record's `renamed_from` names, finished when still pending (node-name-update.md: the
/// old name's release "is finished by whichever writer next meets the row"): run by the writer that
/// meets the record, a retry of the command or the mesh-primary's rebuild after a role change
/// (`moment`). A record that names no source, or whose source's claim no longer names its node, has
/// nothing pending and nothing is done. Answers whether it released a name, or why the release failed.
pub(crate) async fn finish_release(storage: &dyn NodesStorage, _written: &WrittenRows, me: &PathName, row: &NodeRow, moment: &'static str) -> Result<bool, String> {
    match row.renamed_from.as_ref().filter(|from| **from != row.name) {
        Some(from) => release_source(storage, me, &row.node_id, &row.name, from, moment).await,
        None => Ok(false),
    }
}

/// `node_id`, holding `to`, lets go of `from` when `from`'s claim still names it: one
/// `rdm.node_admin.name.update.via-pending-release` span when it did or could not.
async fn release_source(storage: &dyn NodesStorage, me: &PathName, node_id: &NodeId, to: &PathName, from: &PathName, moment: &'static str) -> Result<bool, String> {
    let still = match storage.name_claim(from).await {
        Ok(held) => held.as_ref() == Some(node_id),
        Err(e) => return Err(pending_release_span(me, node_id, to, from, moment, Err(format!("the claim of {from} could not be read: {e}")))),
    };
    if !still {
        return Ok(false);
    }
    match storage.release_node_name(from, node_id).await {
        Ok(()) => {
            let _ = pending_release_span(me, node_id, to, from, moment, Ok(true));
            Ok(true)
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
    tracing::info_span!("rdm.node_admin.name.update.via-pending-release", node = %me, node_id = %node_id, to = %to, from = %from, moment, outcome, detail = %detail, "otel.kind" = "internal")
        .in_scope(|| tracing::info!("a rename's pending release was met by its writer"));
    detail
}

/// What a move of a node's row did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Moved {
    /// The node holds the new name now and let go of the old one.
    Renamed,
    /// The node held the new name already and not the old one: the move was made before.
    AlreadyMoved,
    /// The node holds the new name and still holds the old one: the release failed, and the next
    /// writer that meets the row finishes it (`finish_release`).
    ReleasePending {
        /// Why the release failed.
        why: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, FabricId, Mesh, MeshId, ProviderKind, ScopeStatus};
    use crate::storage::MemoryNodesStorage;

    fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.status = NodeStatus::Ready;
        n.is_primary = primary;
        n.is_fabric_primary = fabric_primary;
        n.incarnation_id = Some(IncarnationId::mint());
        n
    }

    fn view(nodes: Vec<Node>) -> Topology {
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }, Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready }],
            nodes,
        }
    }

    /// The status `n`'s row carries in `storage`.
    async fn status_of(storage: &MemoryNodesStorage, n: &Node) -> Option<NodeStatus> {
        record_at(storage, &n.name).await.unwrap().map(|r| r.status)
    }

    /// CONTRACT: a node row's status is written only by its mesh's writer (node-identity-and-state.md
    /// §4.3: "The fabric-primary writes a row only as acting primary of a mesh with no primary. No other
    /// process writes a `Node` row"; states.md "Who sets each state": node deleting, "the node's
    /// mesh-primary (the fabric-primary for a mesh with no primary)"). mesh2's final primary
    /// `mesh2.admin.1` is lost and mesh2's election named `mesh2.admin.2`: the fabric-primary
    /// `mesh1.admin.1`, the executor of the lost primary's explicit delete, writes neither its row's
    /// `Deleting` nor `mesh2.rpc.1`'s, and both rows stay as they were; `mesh2.admin.2` writes
    /// `mesh2.rpc.1`'s. With no primary in mesh2 but the lost one, the fabric-primary writes the lost
    /// primary's own row as acting primary, and still not `mesh2.rpc.1`'s. Each status write agrees with the writer predicate of the same node's `Deleted`
    /// row ([`NodeRows::writes_unreached`]). The `rdm.node_admin.node.update.via-row-status` spans name the
    /// executor (`node`) and the outcome.
    #[tokio::test]
    async fn a_rows_status_is_written_only_by_its_meshs_writer() {
        let capture = crate::span_capture::Capture::new("node-rows-status");
        let _d = tracing::dispatcher::set_default(&capture.dispatch);
        let fp = node("mesh1.admin.1", true, true);
        let lost = node("mesh2.admin.1", false, false);
        let elected = node("mesh2.admin.2", true, false);
        let rpc = node("mesh2.rpc.1", false, false);
        let storage = Arc::new(MemoryNodesStorage::default());
        for n in [&lost, &rpc] {
            write_record(&*storage, &crate::storage::NodeRow::new(n.name.clone(), n.node_id.clone(), NodeStatus::Ready)).await.unwrap().unwrap();
        }
        let shared = Arc::new(RwLock::new(view(vec![fp.clone(), lost.clone(), elected.clone(), rpc.clone()])));
        let at_fp = NodeRows::new(fp.name.clone(), fp.node_id.clone(), shared.clone(), storage.clone());
        let at_writer = NodeRows::new(elected.name.clone(), elected.node_id.clone(), shared.clone(), storage.clone());
        let root = tracing::info_span!("cell.row-status-writer");
        let _in_root = root.enter();

        let lost_inc = lost.incarnation_id.clone().unwrap();
        let rpc_inc = rpc.incarnation_id.clone().unwrap();
        // The fabric-primary, mesh2 holding an elected primary: neither row is its to write.
        let fp_lost = at_fp.status(&lost, &lost_inc, NodeStatus::Deleting).await;
        assert_eq!(at_fp.writes_unreached(&lost).await, false, "the Deleted row's predicate: the fabric-primary does not write mesh2.admin.1's row");
        assert!(matches!(fp_lost, Err(RowRefused::NotMeshPrimary(_))), "the fabric-primary is not mesh2's writer: mesh2.admin.1's Deleting is refused: {fp_lost:?}");
        assert_eq!(status_of(&storage, &lost).await, Some(NodeStatus::Ready), "nothing is written: the row stays as it was");
        let fp_rpc = at_fp.status(&rpc, &rpc_inc, NodeStatus::Deleting).await;
        assert!(matches!(fp_rpc, Err(RowRefused::NotMeshPrimary(_))), "nor mesh2.rpc.1's: {fp_rpc:?}");
        assert_eq!(status_of(&storage, &rpc).await, Some(NodeStatus::Ready));
        // mesh2's elected primary, its one writer, writes mesh2.rpc.1's Deleting.
        assert!(at_writer.writes_unreached(&rpc).await);
        at_writer.status(&rpc, &rpc_inc, NodeStatus::Deleting).await.unwrap_or_else(|e| panic!("mesh2's writer writes the row: {e}"));
        assert_eq!(status_of(&storage, &rpc).await, Some(NodeStatus::Deleting));
        // mesh2 holds no primary but the lost one: the fabric-primary writes the lost primary's own row as
        // acting primary, and still no other row of mesh2, as its Deleted row's predicate says.
        {
            let mut t = shared.write().await;
            t.nodes.retain(|n| n.name != elected.name);
            t.nodes.iter_mut().filter(|n| n.name == lost.name).for_each(|n| n.is_primary = true);
        }
        assert!(at_fp.writes_unreached(&lost).await);
        at_fp.status(&lost, &lost_inc, NodeStatus::Deleting).await.unwrap_or_else(|e| panic!("acting primary of mesh2: {e}"));
        assert_eq!(status_of(&storage, &lost).await, Some(NodeStatus::Deleting));
        assert_eq!(at_fp.writes_unreached(&rpc).await, false);
        let fp_rpc = at_fp.status(&rpc, &rpc_inc, NodeStatus::Deleting).await;
        assert!(matches!(fp_rpc, Err(RowRefused::NotMeshPrimary(_))), "{fp_rpc:?}");
        drop(_in_root);
        drop(root);

        let captured = capture.named("rdm.node_admin.node.update.via-row-status");
        let trace_id = captured.first().map(|s| s.trace_id.clone()).unwrap_or_default();
        assert!(captured.iter().all(|s| s.trace_id == trace_id), "one trace, the cell's root");
        let spans: Vec<(String, String, String)> = captured.into_iter().map(|s| (s.attributes["node"].clone(), s.attributes["subject"].clone(), s.attributes["outcome"].clone())).collect();
        let expected = [("mesh1.admin.1", "mesh2.admin.1", "not-mesh-primary"), ("mesh1.admin.1", "mesh2.rpc.1", "not-mesh-primary"), ("mesh2.admin.2", "mesh2.rpc.1", "written"), ("mesh1.admin.1", "mesh2.admin.1", "written"), ("mesh1.admin.1", "mesh2.rpc.1", "not-mesh-primary")].map(|(a, b, c)| (a.to_string(), b.to_string(), c.to_string()));
        assert_eq!(spans, expected, "via-row-status: node (the executor) vs mesh2's writer");
        for (node, subject, outcome) in &spans {
            eprintln!("trace {trace_id}: rdm.node_admin.node.update.via-row-status node={node} subject={subject} status=Deleting outcome={outcome}");
        }
    }
}
