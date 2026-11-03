//! The node rows of a mesh, written by that mesh's primary node-admin (R13; ops-naming "Node
//! data: meta, config and exe"; lifecycles/node-create.md step 1; lifecycles/node-delete.md step 3;
//! node-identity-and-state.md §2.7: "It writes its own members' `Node` rows; no other node-admin
//! writes them").
//!
//! The writer of a mesh's rows is that mesh's primary node-admin, for every member of its mesh, itself
//! included (R-OW1: "The mesh-primary writes every row in its mesh, including its own"); the
//! fabric-primary writes them only as the acting primary of a mesh with no primary. Every other admin
//! refuses by name (`not-mesh-primary`), another mesh's primary included: a member never writes its row
//! because it is the subject. Node IDs select rows; path and name are separate fields and their
//! composite index finds the record that holds a name. Mesh and kind come from explicit metadata.
//! The check reads the writer's own view at the moment of the write.
//!
//! The rows are written where the work is done, by the executor of the Build (`executor_for`
//! names the mesh's primary, or the fabric-primary for a mesh with none), from the exact record it
//! acts on:
//!
//! - `node.create`: the birth's record is first written at the start of `PrepareStorage`, once the
//!   `AllocateIdentity` receipt names the node id, and before anything is prepared or launched
//!   ([`NodeRows::create_row`]): the writer reads the records at the name and writes the birth's,
//!   or answers `NameHeld { by }` and launches nothing. A predecessor is named only by the create's admitted
//!   provenance (`Allocation.predecessor`), never by what a location last held. A create that fails
//!   after that write withdraws its record ([`NodeRows::withdraw`]), the holder's own key only.
//! - `node.update` of a name (R13, `crate::node_update`): the row moves to the new name and the
//!   node lets go of the old one ([`move_row`]). A replace's successor is created at `<name>`, its
//!   row seeded from the `<name>-old` row ([`RowFor::Successor`], lifecycles/node-replace.md
//!   step 3).
//! - `NodeDeleted`: the row with status `Deleted`, the record and its config kept
//!   ([`NodeRows::deleted`]).
//! - A mesh delete (R-MN1, mesh-delete.md steps 3 and 7): each member's row `Deleted` on its exact
//!   Exited receipt, by the deleting mesh's primary, and the final primary's in its own node.delete,
//!   by the fabric-primary as acting primary of a mesh with no primary; a birth the view no longer
//!   names is written from this writer's record of its row ([`NodeRows::deleted_birth`]).
//! - Day 0 (R-OW1 item 2): the bootstrap admin, mesh1's primary, writes its own record at its
//!   `(path, name)` as the first write of its birth, before Ready ([`create_day0_row`]).
//! - A birth no create of this writer wrote a row for (a birth made before this admin took its mesh's
//!   role, this admin's own included): written when the writer's view first holds it
//!   ([`NodeRows::arrivals`]); an admin that takes a mesh's role writes every member's row of that
//!   mesh, its own included, and a row a node already holds is never overwritten.

use crate::model::{IncarnationId, Node, NodeId, NodeStatus, PathName};
use crate::storage::{Allocation, NameHeld, NodeRow, NodesStorage};
use crate::topology::Topology;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

/// The writer of a mesh's node rows.
pub struct NodeRows {
    /// This admin's path, the name its spans and refusals carry.
    me_path: String,
    /// This node's name, beside its path.
    me_name: String,
    /// The node id this admin was born with: it writes a mesh's rows only while its view names that
    /// birth the mesh's primary (never by path and name: a successor takes the path and name with a new
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
    /// holding at a create. A later row of the node takes its config from here, never from a record
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

impl NodeRows {
    /// The `(path, name)` lookup index key of me_path and me_name.
    pub fn me(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.me_path.clone(), name: self.me_name.clone() } }
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
    /// The `topology_storage_provider` this admin writes (storage.md §2), set once its storage is
    /// open and again each time a storage change moves it: a create's row it writes records it, the
    /// provider the node it launches writes.
    topology_storage_provider: std::sync::RwLock<Option<String>>,
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
        self.fold(Some(me), read)
    }

    /// [`Self::fold_read`]; with no `me` (a check that reads the record again) it names no span.
    fn fold(&self, me: Option<&PathName>, read: Option<NodeRow>) -> Option<NodeRow> {
        let r = read?;
        let Some(w) = r.writer.clone() else { return Some(r) };
        let deleted = self.row_of(&w.node_id).is_some_and(|d| matches!(d.status, NodeStatus::Deleted | NodeStatus::Withdrawn) && d.birth(&w.incarnation_id).is_some());
        match self.row_of(&r.node_id).filter(|_| deleted) {
            Some(held) => {
                if let Some(me) = me {
                    ignored_span(Some(me), "writer-record", &r, &w, &format!("{} {:?}", held.node_id, held.status));
                }
                Some(held)
            }
            None => Some(r),
        }
    }

    /// The `topology_storage_provider` this admin writes, once its boot set it.
    pub fn topology_storage_provider(&self) -> Option<String> {
        self.topology_storage_provider.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Hold `row` as the writer's record of its node.
    pub fn hold(&self, row: &NodeRow) {
        self.rows.hold(row.node_id.clone(), row.clone());
    }

    /// The watch [`Self::put`] and [`Self::forget`] move when a record's name or its release moved.
    pub fn subscribe_names(&self) -> tokio::sync::watch::Receiver<u64> {
        self.names.subscribe()
    }

    /// The node let go of its record (a withdrawn create): the writer holds no record of it.
    pub fn forget(&self, node_id: &NodeId) {
        if self.rows.get(node_id).is_some() {
            self.names.send_modify(|v| *v += 1);
        }
        self.rows.forget(node_id);
    }


    /// The record the writer holds for `node_id`, if it holds one.
    pub fn row_of(&self, node_id: &NodeId) -> Option<NodeRow> {
        self.rows.get(node_id)
    }

    /// Every record the writer holds.
    pub fn rows(&self) -> Vec<NodeRow> {
        self.rows.all()
    }

    /// Every record the writer holds of `mesh`.
    pub fn rows_of(&self, mesh: &str) -> Vec<NodeRow> {
        self.rows.all().into_iter().filter(|r| r.mesh == mesh).collect()
    }

    /// The record this writer holds of the node whose newest birth holds the endpoint key `endpoint_id`:
    /// the key is the node's own part of its record at that birth (Q333), read at each held record's id
    /// in `nodes`. `Err`: a read failed.
    pub async fn record_of_endpoint(&self, nodes: &dyn NodesStorage, endpoint_id: &str) -> Result<Option<NodeRow>, crate::storage_error::StorageError> {
        for r in self.rows.all() {
            let Some(b) = r.newest_birth() else { continue };
            let own = nodes.properties(&r.node_id).await?;
            if crate::current_record::of_birth(own.as_ref(), &r.node_id, &b.incarnation_id).and_then(|o| o.endpoint_id.as_ref()).is_some_and(|e| e.0 == endpoint_id) {
                return Ok(Some(r));
            }
        }
        Ok(None)
    }

    /// The record a join at `path` of the node `node_id` is reconciled to, from this writer's own copy
    /// (Q234; never a store read, Q227; never an atlas copy, query.md §1): the live record at `path` (the
    /// node's own first), else a `Deleted` one there (the node's own first): another node's live record
    /// at the name stands over the joiner's deleted record. A `Withdrawn` record names nothing (a
    /// withdrawn create).
    pub fn record_for_join(&self, path: &PathName, node_id: &NodeId) -> Option<NodeRow> {
        let at: Vec<NodeRow> = self.rows.all().into_iter().filter(|r| r.location() == *path && r.status != NodeStatus::Withdrawn).collect();
        let (live, deleted): (Vec<NodeRow>, Vec<NodeRow>) = at.into_iter().partition(|r| r.status != NodeStatus::Deleted);
        let own_first = |rows: &[NodeRow]| rows.iter().find(|r| r.node_id == *node_id).or_else(|| rows.first()).cloned();
        own_first(&live).or_else(|| own_first(&deleted))
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
                    ignored_span(None, "writer-fill", &now, &w, &format!("{} re-created node {}", gone[&w], now.node_id));
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
    /// record held, else one made of `node`.
    pub async fn update_birth<R>(&self, storage: &dyn NodesStorage, node: &Node, incarnation: &IncarnationId, change: impl FnOnce(&mut crate::storage::BirthFacts) -> R) -> Result<Option<R>, String> {
        let base = NodeRow::new(node.location(), node.mesh.clone(), &node.r#type, node.node_id.clone(), node.status);
        self.update(storage, &node.node_id, Some(base), |r| change(r.birth_mut(incarnation, &node.location()))).await
    }

    /// The one write of `row`, its node's lock held by the caller: the whole record written (it carries
    /// no revision: node-identity-and-state.md §2.5), the copy replaced once storage answers.
    pub async fn put(&self, storage: &dyn NodesStorage, row: &NodeRow) -> Result<(), crate::storage_error::StorageError> {
        let previous = self.row_of(&row.node_id);
        let mut row = row.clone();
        row.writer = self.birth.get().cloned();
        let row = &row;
        storage.put_node(row).await?;
        self.hold(row);
        if previous.as_ref().is_none_or(|p| p.location() != row.location() || released_its_name(p) != released_its_name(row)) {
            self.names.send_modify(|v| *v += 1);
        }
        Ok(())
    }

    /// The one mutation of `node_id`'s record, under its lock: the copy (filled from storage when this
    /// writer holds none, else `base`, whose name is checked against the records first), `change` applied, and
    /// the whole record written when it changed. `None`: no record of
    /// the node is held anywhere and no `base` was given; nothing is written.
    pub async fn update<R>(&self, storage: &dyn NodesStorage, node_id: &NodeId, base: Option<NodeRow>, change: impl FnOnce(&mut NodeRow) -> R) -> Result<Option<R>, String> {
        self.update_spanned(storage, node_id, base, change, |_| {}).await
    }

    /// [`Self::update`], its span opened by `span` from the outcome while the node's lock is still
    /// held: the record carries no revision (node-identity-and-state.md §2.5), so its spans are the
    /// order of its writes only when each is opened before the next write of the node begins.
    pub async fn update_spanned<R>(&self, storage: &dyn NodesStorage, node_id: &NodeId, base: Option<NodeRow>, change: impl FnOnce(&mut NodeRow) -> R, span: impl FnOnce(&Result<Option<R>, String>)) -> Result<Option<R>, String> {
        let _one = self.lock(node_id).await;
        let written = self.update_held(storage, node_id, base, change).await;
        span(&written);
        written
    }

    /// The body of [`Self::update`], `node_id`'s lock held by the caller.
    async fn update_held<R>(&self, storage: &dyn NodesStorage, node_id: &NodeId, base: Option<NodeRow>, change: impl FnOnce(&mut NodeRow) -> R) -> Result<Option<R>, String> {
        let (held, new) = match self.known_row(storage, node_id).await? {
            Some(row) => (Some(row), false),
            None => (base, true),
        };
        let Some(before) = held else { return Ok(None) };
        if new {
            check_name(storage, Some(self), &before.location(), node_id).await.map_err(|e| format!("nodes.storage did not answer the read of path {:?}, name {:?} for {node_id}: {e}", before.path, before.name))?.map_err(|held| format!("{held}"))?;
        }
        let mut row = before.clone();
        let r = change(&mut row);
        if row != before || new {
            self.put(storage, &row).await.map_err(|e| format!("nodes.storage refused the record of {node_id}: {e}"))?;
        }
        Ok(Some(r))
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
    /// Another node's live row holds the name: the create is refused and nothing was written.
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

/// Which create a row is written for, which decides what the row takes from the `Deleted` row at its
/// `(path, name)` beside the config (proposal node-properties-own-commands.md §1, tags: "set by the
/// create spec (may be empty); inherited with the config on recover").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowFor {
    /// A `node.create`: the row carries its create spec's tags and config (Q166), whatever row stood at
    /// its `(path, name)` before.
    Create,
    /// A birth no create of this writer's wrote a row for, its row written from the birth's own record
    /// as the writer meets it at its arrival or its declaration: the create spec is not known here, and
    /// no location is provenance, so the row is seeded from nothing; the birth's properties are its own.
    Arrival,
    /// Node recovery's create over the exited birth's data dir (node-recovery.md step 4): the exited
    /// birth its admitted provenance names (`Allocation.predecessor`) seeds the row, its launch
    /// snapshot that birth's own acknowledged properties.
    Recovery,
    /// A Build's create of a member it replaces at its own path (build-deployment.md §3: the reconcile's
    /// replacement, over the data dir its delete kept or a fresh one): seeded from the exited birth its
    /// admitted provenance names, as a recovery's is (its executable and its predecessor), but its
    /// config and tags are its Build entry's (Q305), never the predecessor's.
    Replacement,
    /// A Build's node.create of a replace's successor at the `<name>` its predecessor released
    /// (node-replace.md step 3, Properties): its launch snapshot is its Build entry's config and tags
    /// (Q305), never the predecessor's; the predecessor its admitted provenance names seeds the rest,
    /// and a retained `<name>-old` row at the unchanged path is then marked inherited by the successor.
    Successor,
}

/// What a create's write of its row did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowCreated {
    /// The row was written at the name: the record is new and its create may withdraw it.
    Written {
        /// The row written.
        row: NodeRow,
        /// The node whose previous row seeded its config, if one did.
        seeded_from: Option<NodeId>,
    },
    /// The node already holds the row at the name (a restart, or a row written by an earlier attempt
    /// of the same create): nothing is written, and the row stays its own.
    HeldByNode(NodeRow),
}

/// Whether `me_id` writes `mesh`'s node rows in view `t`: the mesh's primary node-admin, or the
/// fabric-primary while the mesh has no primary. A stopped primary is no primary (R-NS1 item 2:
/// from the stop of a mesh's last admin the fabric-primary is the writer of a mesh with no primary):
/// it writes nothing, stopped, and the fabric-primary writes as acting primary (mesh-delete.md step
/// 7, the final primary's own row Deleted in its node.delete).
pub fn writes_for(t: &Topology, me_id: &NodeId, mesh: &str) -> bool {
    t.is_mesh_primary(mesh, me_id) || (t.is_fabric_primary(me_id) && t.node_type_primary(mesh, "node-admin").filter(|p| !p.stopped).is_none())
}

impl NodeRows {
    /// The writer of the admin `me`, born as `me_id`, deciding over its view `view`, writing to
    /// `storage`, its nodes store.
    pub fn new(me: PathName, me_id: NodeId, view: Arc<RwLock<Topology>>, storage: Arc<dyn NodesStorage>) -> Self {
        Self { me_path: me.path.clone(), me_name: me.name.clone(), me_id, view, storage, configs: None, held: Mutex::new(BTreeMap::new()), written: Arc::new(WrittenRows::default()), fabric_recovery: None, to_fill: Mutex::new(Vec::new()) }
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

    /// This admin's currently held `(path, name)` in `t`, falling back to `self.me` before its birth
    /// is in the view.
    fn held_name_in(&self, t: &Topology) -> PathName {
        t.own(&self.me_id).map(|n| n.location()).unwrap_or_else(|| self.me())
    }

    /// This admin's currently held `(path, name)` if the view is readable without waiting, else `self.me`.
    fn held_name_now(&self) -> PathName {
        self.view.try_read().ok().map(|t| self.held_name_in(&t)).unwrap_or_else(|| self.me())
    }

    /// This admin's currently held `(path, name)` in its view.
    async fn held_name(&self) -> PathName {
        self.held_name_in(&*self.view.read().await)
    }

    /// Why this admin does not write `mesh`'s rows in view `t` (the row at `subject`), naming the
    /// writer the view holds; `None` when it is the writer.
    fn not_writer(&self, t: &Topology, mesh: &str, subject: &PathName) -> Option<String> {
        if writes_for(t, &self.me_id, mesh) || self.written.writer().is_some_and(|birth| crate::fabric_recovery::executes_for(self.fabric_recovery.as_ref(), t, birth, mesh)) {
            return None;
        }
        let me = self.held_name_in(t);
        let writer = match (t.node_type_primary(mesh, "node-admin"), t.fabric_primary()) {
            (Some(p), _) => format!("{mesh}'s primary node-admin {} writes them", p.node_id),
            (None, Some(fp)) => format!("{mesh} has no primary node-admin, and fabric-primary {} writes them for it", fp.node_id),
            (None, None) => format!("{mesh} has no primary node-admin and the view holds no fabric-primary to write them for it"),
        };
        Some(format!("path {me_path:?}, name {me_name:?} ({}) is not the writer of {mesh}'s node rows and refuses the row at path {subject_path:?}, name {subject_name:?}: {writer}", self.me_id, me_path = &me.path, me_name = &me.name, subject_path = &subject.path, subject_name = &subject.name))
    }

    /// Whether this admin, the fabric-primary, writes the row of the node-admin `node_id` at `name` as
    /// acting primary of its mesh: the mesh holds no primary but that node (node-identity-and-state.md §4.3).
    fn acting_for(&self, t: &Topology, mesh: &str, node_id: &NodeId) -> bool {
        t.is_fabric_primary(&self.me_id) && !t.node_type_primary(mesh, "node-admin").is_some_and(|p| p.node_id != *node_id)
    }

    /// The writer check of the row of `node_id` at `name` in view `t`, the one predicate of a delete's
    /// writes of the node's row (its `Deleting` status, its `Deleted` row): this admin writes it as the
    /// writer of its mesh's rows or, `acting` (a node-admin's row), as the fabric-primary acting for a mesh
    /// that holds no primary but that node; any other admin refuses it by name, naming the writer
    /// (node-identity-and-state.md §4.3: "The fabric-primary writes a row only as acting primary of a mesh
    /// with no primary. No other process writes a `Node` row"; R-OW1). Answers the name this admin holds.
    fn row_writer_in(&self, t: &Topology, name: &PathName, mesh: &str, node_id: &NodeId, acting: bool) -> Result<PathName, RowRefused> {
        if acting && self.acting_for(t, mesh, node_id) {
            return Ok(self.held_name_in(t));
        }
        match self.not_writer(t, mesh, name) {
            Some(why) => Err(RowRefused::NotMeshPrimary(why)),
            None => Ok(self.held_name_in(t)),
        }
    }

    /// [`Self::row_writer_in`] of `record`'s row over this admin's view as it is now, acting for a
    /// node-admin's row.
    async fn row_writer(&self, record: &Node) -> Result<PathName, RowRefused> {
        self.row_writer_in(&*self.view.read().await, &record.location(), &record.mesh, &record.node_id, rdm_core::is_node_admin(&record.r#type))
    }

    /// Whether this admin writes the `Deleted` row of the unreached birth `record` now, over its view as it
    /// is now ([`Self::deleted_unreached`]): as the writer of its mesh's rows, or, for a node-admin, as the
    /// fabric-primary acting for a mesh with no primary but that node.
    pub async fn writes_unreached(&self, record: &Node) -> bool {
        self.row_writer(record).await.is_ok()
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

    /// `node.create`'s first write of the birth's record (lifecycles/node-create.md step 1): at the start of `PrepareStorage`,
    /// once the `AllocateIdentity` receipt names `node_id` and before anything is prepared or
    /// launched for the birth, the writer of the mesh's rows puts the birth's row at `name`, status
    /// `Pending`, with no tags and no config: a recovery's row (`of`, [`RowFor`]) inherits the
    /// config and tags of the `Deleted` row at the `(path, name)`, a successor's those of the `<name>-old`
    /// row ([`Self::create_row_with_spec`] writes a plain create's from its create spec). The write
    /// refuses a name another node's live row holds, `NameHeld { by }`, and nothing is written. A row
    /// the node holds already is left as it is. `allocated`: what the create allocated for the birth,
    /// written into the row with it ([`NodeRow::allocated`]), so the identity the create is made
    /// with is durable wherever the row is. One `via-node-row` span, `moment` `create`.
    pub async fn create_row(&self, name: &PathName, mesh: &str, r#type: &str, node_id: &NodeId, operation: &rdm_core::OperationId, allocated: Option<&Allocation>, executable: Option<&rdm_core::binding::Binding>, of: RowFor) -> Result<RowCreated, RowRefused> {
        self.create_row_with_spec(name, mesh, r#type, node_id, operation, allocated, executable, &std::collections::BTreeSet::new(), &BTreeMap::new(), None, of).await
    }

    /// [`Self::create_row`] of a create whose create spec names `tags` and `config` (Q166, build.md §1; proposal
    /// executable-bindings-at-create.md rule 1): a plain create's row carries them, written at its first write,
    /// so its first join hands it the config (node-config-update.md); a replace's successor's row carries
    /// them too (node-replace.md "Properties", Q305); a recovery's carries its seed's (node-recovery.md
    /// "Retained"). `storage_persistence`: the spec's, written onto the row whatever the create
    /// (build.md §1); `None` takes the seed's.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_row_with_spec(&self, name: &PathName, mesh: &str, r#type: &str, node_id: &NodeId, operation: &rdm_core::OperationId, allocated: Option<&Allocation>, executable: Option<&rdm_core::binding::Binding>, tags: &std::collections::BTreeSet<String>, config: &BTreeMap<String, String>, storage_persistence: Option<rdm_core::meta::StoragePersistence>, of: RowFor) -> Result<RowCreated, RowRefused> {
        let me = self.held_name_now();
        let span = tracing::info_span!(
            "rdm.node_admin.node.create.via-node-row",
            node_path = %me.path, node_name = %me.name,
            subject_path = %name.path, subject_name = %name.name,
            node_id = %node_id,
            operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.recipe_step,
            moment = "create",
            awaiting = "writer-check",
            awaited = tracing::field::Empty,
            seeded_from = tracing::field::Empty,
            keys = tracing::field::Empty,
            tags = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let mut guard = RowCreateGuard { span: span.clone(), done: false };
        let r = tracing::Instrument::instrument(
            async {
                let me = if matches!(of, RowFor::Recovery | RowFor::Replacement) && rdm_core::is_node_admin(&r#type) {
                    let t = self.view.read().await;
                    if t.is_fabric_primary(&self.me_id) {
                        self.held_name_in(&t)
                    } else {
                        drop(t);
                        self.writer_check(mesh, name).await?
                    }
                } else {
                    self.writer_check(mesh, name).await?
                };
                span.record("node_path", me.path.as_str()).record("node_name", me.name.as_str());
                span.record("awaited", "writer-check");
                // A create's row is the record's first write, and the node is starting (states.md "Who
                // sets each state": a create's starting from its launch, where no process can acknowledge).
                self.put_seeded(&me, name, mesh, r#type, node_id, NodeStatus::Starting, config, allocated, executable, tags, storage_persistence, of, &span, "writer-check").await
            },
            span.clone(),
        )
        .await;
        guard.done = true;
        record_row_created(&span, &r);
        span.in_scope(|| tracing::info!("node.create: the first write of the record decided"));
        r
    }

    /// Move `record`'s row to `to` (`node.name.update`), preserving its config, tags and `type`.
    pub async fn rename_row(&self, record: &Node, to: &PathName) -> Result<NodeRow, RowRefused> {
        self.rename_row_with_intent(record, to, &crate::accepted::RenameIntent::Ordinary).await
    }

    /// [`Self::rename_row`] under the intent Build admission fixed: a replace step's
    /// `RenameIntent::Replacement` retains the predecessor its successor is created from.
    pub async fn rename_row_with_intent(&self, record: &Node, to: &PathName, intent: &crate::accepted::RenameIntent) -> Result<NodeRow, RowRefused> {
        let me = self.writer_check(&record.mesh, to).await?;
        let known = self.written.known_row(&*self.storage, &record.node_id).await.map_err(|why| RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: {why}", me_path = &me.path, me_name = &me.name)))?;
        let from = known.as_ref().map_or_else(|| record.location(), |r| r.location());
        match move_row_with_intent(&*self.storage, &self.written, &me, record, &from, to, intent).await {
            Ok((_, Some(written))) => Ok(written),
            Ok((_, None)) => self.written.row_of(&record.node_id).filter(|r| r.location() == *to).ok_or_else(|| RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: no record of {} stands at path {to_path:?}, name {to_name:?}", record.node_id, me_path = &me.path, me_name = &me.name, to_path = &to.path, to_name = &to.name))),
            Err(RenameRefused::NameHeld(h)) => Err(RowRefused::NameHeld(h)),
            Err(RenameRefused::Unwritten(why)) => Err(RowRefused::Unwritten(why)),
        }
    }

    /// Hold the row `node_id` holds at `name` as this writer's record of it, read from the store (R13:
    /// "Rebuild after a role change reads the mesh's own rows"), the same point read a create or an
    /// arrival makes: a mesh leave's roster birth this writer's views never held (the admin created
    /// for a leaving mesh with no admin left joined after that birth stopped), whose `Deleted` row it
    /// is about to write. Nothing is written. `Ok(true)`: the row is held; `Ok(false)`: the store
    /// holds no row of this node at `name`. `Err` names the read that failed.
    pub async fn hold_row_of(&self, name: &PathName, node_id: &NodeId) -> Result<bool, String> {
        let row = self.storage.node(node_id).await.map_err(|e| format!("{}: the record of {node_id} could not be read at path {name_path:?}, name {name_name:?}: {e}", self.me_id, name_path = &name.path, name_name = &name.name))?;
        match row.filter(|r| r.location() == *name) {
            Some(row) => {
                self.written.hold(&row);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// The birth an earlier run of `operation` of the Build `build_id` wrote `name`'s row for, with what
    /// that run allocated ([`NodeRow::allocated`]): a point read of the row at `name` through its
    /// `(path, name)` key. `Some` only when the row there holds the name (it is not `Deleted`) and its
    /// row was written by that same operation of that same Build; any other row, or none, answers
    /// `None`. Nothing is written. A read that fails is named.
    /// Beside the answer, the row the read returned (`None`: none stands at `name`), whatever it holds.
    pub async fn allocation_at(&self, name: &PathName, build_id: Option<&crate::build::BuildId>, operation: &rdm_core::OperationId) -> Result<(Option<(NodeId, Allocation)>, Option<NodeRow>), String> {
        let row = record_at(&*self.storage, name).await.map_err(|e| format!("{}: the record at path {name_path:?}, name {name_name:?} could not be read for action {action}, subject {operation_subject:?}, step {operation_step:?} (Build {build}): {e}", self.me_id, name_path = &name.path, name_name = &name.name, action = &operation.action, operation_subject = &operation.subject, operation_step = operation.recipe_step, build = crate::deployment::pipeline::build_log(build_id)))?;
        let written = row.clone().filter(|r| r.status != NodeStatus::Deleted).and_then(|r| r.allocated.filter(|a| a.build_id.as_ref() == build_id && a.operation == *operation).map(|a| (r.node_id, a)));
        Ok((written, row))
    }

    /// A create that wrote `name`'s row for `node_id` and did not complete lets go of it, so the
    /// next birth at the name is not refused by it: a release of the holder's own key only, never
    /// another node's row. One `via-row-withdrawn` span says why.
    pub async fn withdraw(&self, name: &PathName, node_id: &NodeId, operation: &rdm_core::OperationId, why: &str) {
        let mut me = self.held_name_now();
        let span = tracing::info_span!(
            "rdm.node_admin.node.remove.via-row-withdrawn",
            node_path = %me.path, node_name = %me.name,
            subject_path = %name.path, subject_name = %name.name,
            node_id = %node_id,
            operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.recipe_step,
            why,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let owner = self.written.known_row(&*self.storage, node_id).await;
        let checked = match owner {
            Ok(Some(row)) => self.writer_check(&row.mesh, name).await,
            Ok(None) => Err(RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: no record of {node_id} supplies the owner of path {name_path:?}, name {name_name:?}; nothing is released", me_path = &me.path, me_name = &me.name, name_path = &name.path, name_name = &name.name))),
            Err(why) => Err(RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: {why}", me_path = &me.path, me_name = &me.name))),
        };
        let r = match checked {
            Err(e) => Err(e),
            Ok(held) => {
                me = held;
                span.record("node_path", me.path.as_str()).record("node_name", me.name.as_str());
                release_record(&*self.storage, &self.written, name, node_id).await.map_err(|e| RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: nodes.storage refused the release of path {name_path:?}, name {name_name:?} by {node_id}: {e}", me_path = &me.path, me_name = &me.name, name_path = &name.path, name_name = &name.name)))
            }
        };
        if let Ok(true) = r {
            self.written.forget(node_id);
        }
        match &r {
            Ok(true) => span.record("outcome", "withdrawn"),
            Ok(false) => span.record("outcome", "not-held").record("detail", format!("path {me_path:?}, name {me_name:?}: {node_id} holds no row at path {name_path:?}, name {name_name:?}; nothing is released", me_path = &me.path, me_name = &me.name, name_path = &name.path, name_name = &name.name).as_str()),
            Err(e) => span.record("outcome", e.outcome()).record("detail", e.to_string().as_str()),
        };
        span.in_scope(|| tracing::info!("node.create: the row is withdrawn"));
    }

    /// Seed the arriving birth's row (`RowFor::Arrival`) before its join is answered: the row its
    /// mesh's writer holds of it, written seeded when it holds none. Its declared types are the node's
    /// own part of its record (Q333), never written here. One
    /// `rdm.node_admin.node.create.via-arrival-seed` span.
    pub async fn seed_at_arrival(&self, d: &rdm_core::MeshDigest) -> Result<(), String> {
        let me = self.writer_check(&d.node.mesh, &d.node.location()).await.map_err(|e| e.to_string())?;
        let configs = self.configs.as_ref().and_then(|slot| slot.get());
        let _one = match configs { Some(door) => Some(door.one_at_a_time().await), None => None };
        let seeded = match self.put_seeded(&me, &d.node.location(), &d.node.mesh, &d.node.r#type, &d.node.node_id, crate::admin::node_status(d.status), &BTreeMap::new(), None, None, &std::collections::BTreeSet::new(), None, RowFor::Arrival, &tracing::Span::current(), "declaration").await.map_err(|e| e.to_string())? {
            RowCreated::Written { .. } => "seeded",
            RowCreated::HeldByNode(_) => "held",
        };
        tracing::info_span!("rdm.node_admin.node.create.via-arrival-seed", node_path = %d.node.path, node_name = %d.node.name, node_id = %d.node.node_id, incarnation = %d.node.incarnation, outcome = seeded).in_scope(|| tracing::info!("the arriving birth's row is seeded"));
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
                crate::status_rpc::row_status_refused_span(&self.held_name().await, &record.location(), &record.node_id, incarnation, status, &refused);
                return Err(refused);
            }
        };
        let written = self.written.update_spanned(&*self.storage, &record.node_id, None, |r| r.set_status(incarnation, status), |written| crate::status_rpc::row_status_span(&me, &record.location(), &record.node_id, incarnation, status, written)).await;
        written.map(|_| ()).map_err(|e| RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: the {status:?} status of node {} is not written: {e}", record.node_id, me_path = &me.path, me_name = &me.name)))
    }

    /// `NodeDeleted` (lifecycles/node-delete.md step 3): the row at the node's `(path, name)` (its retained predecessor name
    /// path for a stopped predecessor) with status `Deleted`, the exact record the executor acted on
    /// and its config kept, a blind put. One `via-node-row` span.
    pub async fn deleted(&self, record: &Node, operation: &rdm_core::OperationId) -> Result<(), RowRefused> {
        self.deleted_as(record, operation, false).await
    }

    /// [`Self::deleted`] of an unreached birth (node-recovery.md step 3): a node-admin's row is written by
    /// its mesh's elected primary, the mesh's one writer, and by the fabric-primary only as acting primary
    /// of a mesh with no primary (node-identity-and-state.md §4.3; R-OW1).
    pub async fn deleted_unreached(&self, record: &Node, operation: &rdm_core::OperationId) -> Result<(), RowRefused> {
        self.deleted_as(record, operation, rdm_core::is_node_admin(&record.r#type)).await
    }

    async fn deleted_as(&self, record: &Node, operation: &rdm_core::OperationId, fabric_recovery: bool) -> Result<(), RowRefused> {
        self.deleted_row(&record.location(), &record.node_id, false, operation, fabric_recovery).await
    }

    /// The `Deleted` row of a birth the executor holds no record of any more (a mesh leave's
    /// member that deleted the view after its exact Exited receipt, mesh-delete.md steps 3 and
    /// 7): the row at `name` for `node_id`, from the record this writer holds for the node. A writer
    /// that holds no record of the node's row refuses by name and writes nothing. One `via-node-row`
    /// span.
    pub async fn deleted_birth(&self, name: &PathName, node_id: &NodeId, operation: &rdm_core::OperationId) -> Result<(), RowRefused> {
        self.deleted_row(name, node_id, true, operation, false).await
    }

    /// The `Deleted` row of `node_id` at `name`, written from the lifecycle record the writer holds or
    /// reads for it (`held_only`: only from one it holds), or the refusal that names why it has none.
    async fn deleted_row(&self, name: &PathName, node_id: &NodeId, held_only: bool, operation: &rdm_core::OperationId, fabric_recovery: bool) -> Result<(), RowRefused> {
        let me = self.held_name_now();
        let span = tracing::info_span!(
            "rdm.node_admin.node.delete.via-node-row",
            node_path = %me.path, node_name = %me.name,
            subject_path = %name.path, subject_name = %name.name,
            operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.recipe_step,
            node_id = %node_id,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let r = tracing::Instrument::instrument(
            async {
                if held_only && self.written.row_of(node_id).is_none() {
                    return Err(RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: holds no record of the row of path {name_path:?}, name {name_name:?} ({node_id}) and no executor record of it: its Deleted row is not written from a record this writer does not hold", me_path = &me.path, me_name = &me.name, name_path = &name.path, name_name = &name.name)));
                }
                // "The fabric-primary writes a row only as acting primary of a mesh with no primary. No
                // other process writes a Node row" (node-identity-and-state.md §4.3): the fabric-primary
                // writes the deleted node-admin's row itself only while its mesh holds no primary but that
                // node; a mesh with an elected primary has its row written by that primary, its one writer.
                let row = self.written.known_row(&*self.storage, node_id).await.map_err(RowRefused::Unwritten)?.ok_or_else(|| RowRefused::Unwritten(format!("path {name_path:?}, name {name_name:?}: no record of {node_id} supplies its mesh", name_path = &name.path, name_name = &name.name)))?;
                let me = self.row_writer_in(&*self.view.read().await, name, &row.mesh, node_id, fabric_recovery)?;
                span.record("node_path", me.path.as_str()).record("node_name", me.name.as_str());
                // The record is the one held, its facts, executable and app types kept: the node created
                // next at the name runs the executable (node-recovery.md "Retained").
                write_deleted(&*self.storage, &self.written, name, node_id).await.map(|_| ()).map_err(|e| match e {
                    DeletedRefused::NameHeld(held) => RowRefused::NameHeld(held),
                    DeletedRefused::Unwritten(why) => RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: nodes.storage refused the Deleted row of path {name_path:?}, name {name_name:?} ({node_id}): {why}", me_path = &me.path, me_name = &me.name, name_path = &name.path, name_name = &name.name)),
                })
            },
            span.clone(),
        )
        .await;
        match &r {
            Ok(()) => span.record("outcome", "row-deleted"),
            Err(e) => span.record("outcome", e.outcome()).record("detail", e.to_string().as_str()),
        };
        span.in_scope(|| tracing::info!("node.delete: the Deleted row decided; the record stays and the name is released"));
        r
    }

    /// The row of `node_id` at `name`, `status`: a Build's create's config and tags are `own` and `tags`,
    /// its entry's (Q166, Q305); a recovery's are those of the exited birth its admitted provenance names
    /// (`Allocation.predecessor`), never of a record last written at the location. A row the same node holds already,
    /// at whatever name, is left as it is: it is found through the node's id index
    /// ([`WrittenRows::known_row`]), so a node renamed while stopped (told nothing of it, R12) keeps
    /// its one row where the rename put it, whatever name a view still lists it at.
    #[allow(clippy::too_many_arguments)]
    async fn put_seeded(&self, me: &PathName, name: &PathName, mesh: &str, r#type: &str, node_id: &NodeId, status: NodeStatus, own: &BTreeMap<String, String>, allocated: Option<&Allocation>, executable: Option<&rdm_core::binding::Binding>, tags: &std::collections::BTreeSet<String>, storage_persistence: Option<rdm_core::meta::StoragePersistence>, of: RowFor, span: &tracing::Span, prefix: &str) -> Result<RowCreated, RowRefused> {
        let created = match self.written.known_row(&*self.storage, node_id).await {
            Ok(Some(held)) => RowCreated::HeldByNode(held),
            Ok(None) => seeded_put(&*self.storage, &self.written, me, name, mesh, r#type, node_id, status, own, allocated, allocated.and_then(|allocation| allocation.predecessor.as_ref()), executable, tags, storage_persistence, of, span, prefix).await?,
            Err(e) => return Err(RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: the row {node_id} holds could not be read before its row at path {name_path:?}, name {name_name:?} was written: {e}", me_path = &me.path, me_name = &me.name, name_path = &name.path, name_name = &name.name))),
        };
        if of == RowFor::Successor {
            // A successor's type is its own explicit declaration, never copied from the predecessor.
            // The create's last write, on its first run and on any repeat: the retained row is marked.
            let row = match &created { RowCreated::Written { row, .. } | RowCreated::HeldByNode(row) => row };
            let predecessor = row.allocated.as_ref().and_then(|allocation| allocation.predecessor.as_ref());
            mark_retained(&*self.storage, me, &self.written, mesh, name, node_id, predecessor).await.map_err(RowRefused::Unwritten)?;
        }
        Ok(created)
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

    /// Write the row of each birth in `arrivals`, the writer's view `now` having first held it.
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
            tracing::info_span!("rdm.node_admin.node.update.via-records-filled", node_path = %me.path, node_name = %me.name, mesh = %mesh, records = filled.as_ref().map(|n| *n as u64).unwrap_or(0), outcome, detail = %filled.err().map(|e| e.to_string()).unwrap_or_default(), "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the writing of a mesh's records was taken: its records are read whole, once"));
        }
        for birth in arrivals {
            let span = tracing::info_span!(
                "rdm.node_admin.node.create.via-node-row",
                node_path = %me.path, node_name = %me.name,
                subject_path = %birth.path, subject_name = %birth.name,
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
            record_row_created(&span, &r);
            span.in_scope(|| tracing::info!("node.create: the birth's row decided"));
        }
    }

    /// The row of `birth` as the writer meets it at its arrival: the row the node holds wherever it
    /// stands (a rename may have moved it off the name the view holds, found through the id index);
    /// else the row seeded at the view's name ([`Self::put_seeded`]).
    async fn rebuilt_row(&self, me: &PathName, birth: &Node, span: &tracing::Span) -> Result<RowCreated, RowRefused> {
        let holds = self.written.known_row(&*self.storage, &birth.node_id).await.map_err(|why| RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: {why}", me_path = &me.path, me_name = &me.name)))?;
        let created = match holds {
            Some(row) => RowCreated::HeldByNode(row),
            // A recovery's row precedes its birth's arrival (the create writes it before it launches),
            // so an arrival that finds no row of its node is never a recovery's: a plain create's seed.
            None => self.put_seeded(me, &birth.location(), &birth.mesh, &birth.r#type, &birth.node_id, birth.status, &BTreeMap::new(), None, None, &std::collections::BTreeSet::new(), None, RowFor::Arrival, span, "").await?,
        };
        Ok(created)
    }
}

struct RowCreateGuard {
    span: tracing::Span,
    done: bool,
}

impl Drop for RowCreateGuard {
    fn drop(&mut self) {
        if !self.done {
            self.span.record("outcome", "cancelled");
        }
    }
}

/// The outcome of a row put on its span.
fn record_row_created(span: &tracing::Span, r: &Result<RowCreated, RowRefused>) {
    match r {
        Ok(RowCreated::Written { seeded_from, .. }) => {
            span.record("outcome", if seeded_from.is_some() { "row-seeded" } else { "row-written" });
        }
        Ok(RowCreated::HeldByNode(_)) => {
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
/// path and name under another node id (node-recovery.md: "A birth that comes back after it was re-created
/// is a deleted birth"), with the node id of the record that names it.
pub(crate) fn re_created(records: &[NodeRow]) -> HashMap<crate::storage::BirthRef, NodeId> {
    rdm_node::pair_record::re_created(records)
}

/// The records a reader of a mesh's node records folds from `records`, the versions a read of storage
/// answered (Q109: "every reader folds the stored rows the same way, and a row that does not follow the
/// fold is ignored by every reader, refused by no storage"; build.md: an os-stopped executor that
/// resumes after the next attempt is claimed "is a deleted birth whose writes every reader refuses"):
/// a node's record whose version was written by its own birth after another record named that birth
/// its predecessor (the birth re-created at its path and name) is ignored, one
/// `rdm.node_admin.node.reject.via-deleted-birth` span naming it. The record of a re-created birth
/// stands `Deleted`, written by its mesh-primary before the re-creation's first write, so no version its own
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
        .filter(|r| match rdm_node::pair_record::superseded(r, &deleted) {
            Some(by) => {
                ignored_span(None, read, r, r.writer.as_ref().expect("a superseded version names its writer"), &format!("{by} re-created node {}", r.node_id));
                false
            }
            None => true,
        })
        .collect()
}

/// [`ignored_span`] for a reader outside this module: `read` names the read.
pub(crate) fn ignored_version(read: &str, row: &NodeRow, writer: &crate::storage::BirthRef, held: &str) {
    ignored_span(None, read, row, writer, held);
}

/// The span of a version of `row`'s record a reader ignored: written by `writer`, a birth the reader's
/// fold holds deleted (`held`, what holds it so).
fn ignored_span(me: Option<&PathName>, read: &str, row: &NodeRow, writer: &crate::storage::BirthRef, held: &str) {
    let span = tracing::info_span!(
        "rdm.node_admin.node.reject.via-deleted-birth",
        node_path = tracing::field::Empty, node_name = tracing::field::Empty,
        read,
        subject_path = %row.path, subject_name = %row.name,
        node_id = %row.node_id,
        status = ?row.status,
        writer_node_id = %writer.node_id, writer_incarnation = %writer.incarnation_id,
        held,
        outcome = "ignored",
        "otel.kind" = "internal",
    );
    if let Some(me) = me {
        span.record("node_path", me.path.as_str()).record("node_name", me.name.as_str());
    }
    span.in_scope(|| tracing::info!("a version of the record written by a deleted birth is ignored on read; storage keeps it as written"));
}

/// The record at `name` (node-identity-and-state.md §2: "A current-pair lookup reads the node records
/// by their path and name; it never uses the last writer at a path"): the records standing at `name`
/// ([`NodesStorage::records_at`]) folded by the one resolver every reader of a pair goes through
/// ([`rdm_node::pair_record::resolve`]). `None` when none holds it (a `Deleted` record frees its pair).
pub async fn record_at(storage: &dyn NodesStorage, name: &PathName) -> Result<Option<NodeRow>, crate::storage_error::StorageError> {
    rdm_node::pair_record::resolve(storage.records_at(name).await?, name)
}

/// Read the admitted predecessor by ID and validate its exact birth and unchanged path.
/// Successor mesh and kind do not identify or constrain the predecessor's own metadata.
pub(crate) async fn predecessor_by_id(storage: &dyn NodesStorage, original: &PathName, birth: &crate::storage::BirthRef) -> Result<NodeRow, String> {
    let row = storage.node(&birth.node_id).await.map_err(|e| format!("predecessor node {} could not be read: {e}", birth.node_id))?
        .ok_or_else(|| format!("admitted predecessor node {} has no record", birth.node_id))?;
    if row.path != original.path || (row.location() != *original && row.replacement_from.as_ref() != Some(original))
        || row.newest_birth().is_none_or(|held| held.incarnation_id != birth.incarnation_id) {
        return Err(format!("predecessor node {} does not match the admitted birth and original location", birth.node_id));
    }
    Ok(row)
}

/// [`write_record`] as a method of any nodes store: a name another node's record holds is the error, named.
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
        rdm_node::pair_record::resolve(self.records_at(name).await?, name)
    }
}

/// Write `row` whole as its node's record by a writer that holds no copy of it: its name checked
/// against the records ([`check_name`], `NameHeld` naming another node's live record, nothing
/// written), then the record written.
pub async fn write_record(storage: &dyn NodesStorage, row: &NodeRow) -> Result<Result<(), NameHeld>, String> {
    let written = WrittenRows::default();
    let _one = written.lock(&row.node_id).await;
    written.known_row(storage, &row.node_id).await?;
    if let Err(held) = check_name(storage, Some(&written), &row.location(), &row.node_id).await.map_err(|e| e.to_string())? {
        return Ok(Err(held));
    }
    written.put(storage, row).await.map_err(|e| e.to_string())?;
    Ok(Ok(()))
}

/// Check `name` for `node_id` against the records (node-name-update.md, "Check and move": `NameHeld {
/// by: node.id }`): another node's live record at `name` refuses it by name, and nothing is written; a
/// `Deleted` or `Withdrawn` record frees its pair. The record read is folded through `written`, the
/// writer's held records, when given ([`WrittenRows::fold_read`]): a version written by a birth the
/// writer holds deleted is not the node's record.
pub async fn check_name(storage: &dyn NodesStorage, written: Option<&WrittenRows>, name: &PathName, node_id: &NodeId) -> Result<Result<(), NameHeld>, crate::storage_error::StorageError> {
    let read = storage.records_at(name).await?;
    let folded = match written {
        Some(w) => read.into_iter().filter_map(|r| w.fold(None, Some(r))).collect(),
        None => read,
    };
    Ok(match rdm_node::pair_record::resolve(folded, name)? {
        Some(held) if held.node_id != *node_id => Err(NameHeld { path: name.path.clone(), name: name.name.clone(), by: held.node_id }),
        _ => Ok(()),
    })
}

/// `node_id` lets go of `name`: when its live record is still at `name` (a create withdrawn before its
/// node ran), the record is appended `Withdrawn` (R-SR2: nothing is removed), which frees the pair.
/// Returns whether it held the name.
pub async fn release_record(storage: &dyn NodesStorage, written: &WrittenRows, name: &PathName, node_id: &NodeId) -> Result<bool, crate::storage_error::StorageError> {
    let _one = written.lock(node_id).await;
    let held = match written.row_of(node_id) {
        Some(r) => Some(r),
        None => storage.node(node_id).await?,
    };
    let Some(mut row) = held.filter(|r| r.location() == *name && rdm_node::pair_record::holds_its_name(r)) else { return Ok(false) };
    row.status = NodeStatus::Withdrawn;
    written.put(storage, &row).await?;
    written.forget(node_id);
    Ok(true)
}

/// Whether `row`'s node released its name: its record is `Deleted`, or `Withdrawn` (a create withdrawn
/// before its node ran), so another node's record may take the name.
pub fn released_its_name(row: &NodeRow) -> bool {
    !rdm_node::pair_record::holds_its_name(row)
}

/// Whether the retained record of this exact node proves deletion. A create at a historical
/// location says nothing about the old node: it may have been renamed and still be live.
/// An absent exact-ID record is unknown, not deletion evidence. `name` is diagnostic only.
pub async fn deleted_birth(storage: &dyn NodesStorage, name: &PathName, node_id: &NodeId) -> Result<Option<String>, crate::storage_error::StorageError> {
    let own = storage.node(node_id).await?;
    if let Some(row) = own.as_ref().filter(|r| released_its_name(r)) {
        return Ok(Some(format!("path {:?}, name {:?} (node {node_id}) is a deleted birth: its exact record is {:?}", name.path, name.name, row.status)));
    }
    // Re-created at its path and name (a fabric recovery whose old writer never wrote the record
    // Deleted): another node's live record holds the name its record still stands at (Q181). A node
    // whose record was renamed away (a replacement's predecessor) is not deleted by a record at the
    // name it left.
    let Some(own) = own.filter(|r| r.location() == *name) else { return Ok(None) };
    Ok(storage.nodes(&own.mesh).await?.into_iter().find(|r| r.location() == *name && r.node_id != *node_id && !released_its_name(r)).map(|by| format!("path {:?}, name {:?} (node {node_id}) is a deleted birth: node {} holds the name", name.path, name.name, by.node_id)))
}

/// Why a `Deleted` record was not written.
#[derive(Debug)]
pub enum DeletedRefused {
    /// Another node's live record holds the name.
    NameHeld(NameHeld),
    /// The record could not be written: why.
    Unwritten(String),
}

/// The `Deleted` record of `node_id` at `name` (node-delete.md step 3): the record held for it, its
/// facts kept, with `known` (config and tags) when given, written whole: the `Deleted` record frees the
/// pair. Another node's live record at `name` refuses it by name and nothing is written.
pub async fn write_deleted(storage: &dyn NodesStorage, written: &WrittenRows, name: &PathName, node_id: &NodeId) -> Result<NodeRow, DeletedRefused> {
    let unwritten = |e: crate::storage_error::StorageError| DeletedRefused::Unwritten(e.to_string());
    let _one = written.lock(node_id).await;
    if let Some(by) = record_at(storage, name).await.map_err(unwritten)?.filter(|by| by.node_id != *node_id) {
        return Err(DeletedRefused::NameHeld(NameHeld { path: name.path.clone(), name: name.name.clone(), by: by.node_id }));
    }
    let held = written.known_row(storage, node_id).await.map_err(DeletedRefused::Unwritten)?;
    let mut row = held.ok_or_else(|| DeletedRefused::Unwritten(format!("path {name_path:?}, name {name_name:?} ({node_id}): cannot delete an absent lifecycle record", name_path = &name.path, name_name = &name.name)))?;
    { let at = name.clone(); row.path = at.path; row.name = at.name; }
    row.status = NodeStatus::Deleted;
    row.allocated = None;
    row.renamed_from = None;
    row.inherited_by = None;
    written.put(storage, &row).await.map_err(unwritten)?;
    Ok(row)
}

/// [`NodeRows::put_seeded`]'s one write, by `me` into `storage`: the record of `node_id` at `name`, its
/// config and tags seeded as that method says, written under the node's lock after its name's check.
/// A record the same node holds already is left as it is. A recovery's record names the birth it
/// replaced, the newest birth of the `Deleted` record at the name (`predecessor`).
#[allow(clippy::too_many_arguments)]
async fn seeded_put(storage: &dyn NodesStorage, written: &WrittenRows, me: &PathName, name: &PathName, mesh: &str, r#type: &str, node_id: &NodeId, status: NodeStatus, own: &BTreeMap<String, String>, allocated: Option<&Allocation>, predecessor: Option<&crate::storage::BirthRef>, executable: Option<&rdm_core::binding::Binding>, created_with: &std::collections::BTreeSet<String>, storage_persistence: Option<rdm_core::meta::StoragePersistence>, of: RowFor, span: &tracing::Span, prefix: &str) -> Result<RowCreated, RowRefused> {
    let _one = written.lock(node_id).await;
    let mut awaited = prefix.to_string();
    span.record("awaiting", "read-previous-row");
    let previous = written.fold_read(me, record_at(storage, name).await.map_err(|e| RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: the previous record at path {name_path:?}, name {name_name:?} could not be read for {node_id}: {e}", me_path = &me.path, me_name = &me.name, name_path = &name.path, name_name = &name.name)))?);
    push_awaited(&mut awaited, "read-previous-row");
    span.record("awaited", awaited.as_str());
    if let Some(p) = previous.as_ref().filter(|p| p.node_id == *node_id) {
        span.record("awaiting", "done");
        written.hold(p);
        return Ok(RowCreated::HeldByNode(p.clone()));
    }
    // Only explicit replacement provenance (`predecessor`: the create's admitted `Allocation.predecessor`,
    // or the inherited birth an admin started on an inherited root takes over) identifies a predecessor:
    // the one that released the name (node-replace.md step 3), or the exited birth a recovery or a
    // Build's replacement re-creates at the path. A location is never provenance: no record last
    // written at it seeds a row.
    let seed = match predecessor {
        Some(predecessor) => {
            // The predecessor's record as this writer folds it: a version a deleted birth wrote after
            // its delete (a resumed birth) is not its record (build.md; Q109).
            let row = predecessor_by_id(storage, name, predecessor).await.map_err(RowRefused::Unwritten)?;
            let row = written.fold_read(me, Some(row.clone())).unwrap_or(row);
            if of == RowFor::Successor && row.mesh != mesh {
                return Err(RowRefused::Unwritten("replacement across meshes is unsupported; the predecessor's mesh writer must own its update".into()));
            }
            Some(row)
        }
        None => None,
    };
    // The config and the tags of a Build's create, a replace's successor and a reconcile's replacement
    // included, are its entry's (`own`, `created_with`; Q305): a Build provides the whole map of a node it
    // replaces, never the predecessor's, and an empty one is empty; an arrival names none. A recovery
    // inherits the predecessor's acknowledged properties from that node's own part of its record at its
    // exact ID (node-config-update.md "Build application": "True recovery inherits the predecessor's
    // acknowledged properties."): a lifecycle record never stands in for it, and a predecessor that
    // wrote none acknowledged none.
    let (config, tags) = match (of, seed.as_ref()) {
        (RowFor::Recovery, Some(seed)) => match storage.properties(&seed.node_id).await.map_err(|error| RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: predecessor {} properties could not be read: {error}", seed.node_id, me_path = &me.path, me_name = &me.name)))? {
            Some(record) => (record.config, record.tags),
            None => Default::default(),
        },
        (RowFor::Recovery, None) => Default::default(),
        _ => (own.clone(), created_with.clone()),
    };
    // The executable beside the config: the one the create launches (`executable`), else the seed's.
    let executable = executable.cloned().or_else(|| seed.as_ref().and_then(|p| p.executable.clone()));
    let predecessor = seed.as_ref().filter(|p| matches!(of, RowFor::Recovery | RowFor::Replacement) && p.status == NodeStatus::Deleted).and_then(|p| p.births.last().map(|b| crate::storage::BirthRef { node_id: p.node_id.clone(), incarnation_id: b.incarnation_id.clone() }));
    if let Some(p) = &seed {
        span.record("seeded_from", p.node_id.as_str());
    }
    span.record("keys", config.len() as u64).record("tags", labels(&tags).as_str());
    // The storage_persistence the create is handed (its create spec's), else the seed's.
    let storage_persistence = storage_persistence.or_else(|| seed.as_ref().and_then(|p| p.storage_persistence));
    let initial_properties = Some(rdm_core::launch::InitialProperties { config: config.clone(), tags: tags.clone() });
    let row = NodeRow { initial_properties, allocated: allocated.cloned(), executable, predecessor, storage_persistence, topology_storage_provider: written.topology_storage_provider(), ..NodeRow::new(name.clone(), mesh, r#type, node_id.clone(), status) };
    span.record("awaiting", "put-node-row");
    let checked = check_name(storage, Some(written), name, node_id).await;
    let put = match checked {
        Ok(Ok(())) => written.put(storage, &row).await.map(Ok),
        other => other,
    };
    push_awaited(&mut awaited, "put-node-row");
    span.record("awaited", awaited.as_str());
    match put {
        Ok(Ok(())) => {
            span.record("awaiting", "done");
            Ok(RowCreated::Written { row, seeded_from: seed.map(|p| p.node_id) })
        }
        Ok(Err(held)) => Err(RowRefused::NameHeld(held)),
        Err(e) => Err(RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: nodes.storage refused the record of path {name_path:?}, name {name_name:?} ({node_id}): {e}", me_path = &me.path, me_name = &me.name, name_path = &name.path, name_name = &name.name))),
    }
}

/// A replace successor's create's last write (node-replace.md step 3): the retained predecessor's record
/// at `<name>-old` is written again carrying `inherited_by: successor`; its properties stay in its own
/// record. A record that says it
/// already, or no retained record (the predecessor was deleted), writes nothing. One
/// `rdm.node_admin.node.update.via-inherited-row` span, its outcome `inherited`, `already-inherited`,
/// `none-retained` or `row-refused`; `Err` names the refusal.
async fn mark_retained(storage: &dyn NodesStorage, me: &PathName, written: &WrittenRows, mesh: &str, name: &PathName, successor: &NodeId, predecessor: Option<&crate::storage::BirthRef>) -> Result<(), String> {
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-inherited-row",
        node_path = %me.path, node_name = %me.name,
        original_path = %name.path, original_name = %name.name,
        node_id = tracing::field::Empty,
        inherited_by = %successor,
        outcome = tracing::field::Empty,
        detail = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let r = tracing::Instrument::instrument(
        async {
            let row = match predecessor {
                Some(birth) => Some(predecessor_by_id(storage, name, birth).await?),
                None => None,
            };
            let Some(row) = row.filter(|row| !matches!(row.status, NodeStatus::Deleted | NodeStatus::Withdrawn) && row.replacement_from.as_ref() == Some(name)) else { return Ok("none-retained") };
            span.record("node_id", row.node_id.as_str());
            let node_id = row.node_id.clone();
            let expected_birth = row.newest_birth().map(|birth| birth.incarnation_id.clone());
            written.update(storage, &node_id, None, |current| {
                if current.mesh != mesh { return Err("retained predecessor belongs to another mesh writer".to_string()); }
                if current.path != name.path || current.replacement_from.as_ref() != Some(name)
                    || current.newest_birth().map(|birth| &birth.incarnation_id) != expected_birth.as_ref() {
                    return Err(format!("predecessor node {node_id} changed its admitted birth or replacement provenance before marking"));
                }
                if matches!(current.status, NodeStatus::Deleted | NodeStatus::Withdrawn) { return Ok("none-retained"); }
                if let Some(id) = &current.inherited_by {
                    return if id == successor { Ok("already-inherited") } else { Err(format!("predecessor node {node_id} is already inherited by another successor")) };
                }
                current.inherited_by = Some(successor.clone());
                Ok("inherited")
            }).await.map_err(|why| format!("retained predecessor node {node_id}: {why}"))?
                .ok_or_else(|| format!("retained predecessor node {node_id} disappeared before marking"))?

        },
        span.clone(),
    )
    .await;
    match &r {
        Ok(outcome) => span.record("outcome", *outcome),
        Err(why) => span.record("outcome", "row-refused").record("detail", why.as_str()),
    };
    span.in_scope(|| tracing::info!("node.replace: the retained row is marked inherited by its successor"));
    r.map(|_| ())
}

/// Fabric recovery's hand-over of the `(path, name)` an admin started on an inherited root takes
/// (fabric-recovery.md: the re-created node-admin is a new node over its inherited data dir: a new
/// node.id and incarnation, the same path and name; "Nothing alive": "node.delete of the exited instance,
/// then node.create at the same path and name, with a new node.id over the inherited data dir", "A node's
/// name is released by node recovery's node.delete and taken by its node.create, which carries a new
/// node.id"). Fabric recovery starts after every node-admin is lost, so this admin, `node_id`, is the
/// only writer its mesh has, and these are the first writes of its birth, before anything else of it
/// is written or published:
///
/// 1. node.delete of the inherited birth: its live row at the name written `Deleted`, its config and
///    tags kept (one `rdm.node_admin.node.delete.via-node-row` span): a `Deleted` record names no node
///    to reach;
/// 2. node.create's first write of the record under `node_id`, the row
///    `Pending`, its launch snapshot the inherited birth's own acknowledged properties, read from
///    `properties` at its node id ([`RowFor::Recovery`]; one
///    `rdm.node_admin.node.create.via-node-row` span, `moment` `create`).
///
/// Another node's live row at the name is `NameHeld` naming it, and nothing of this birth is written.
pub async fn take_inherited_name(storage: &dyn NodesStorage, written: &WrittenRows, inherited: &crate::storage::NodeRecord, node_id: &NodeId) -> Result<RowCreated, RowRefused> {
    let (me, name) = (&inherited.location(), &inherited.location());
    let operation = rdm_core::OperationId::node("fabric-recovery", node_id);
    let held = storage.node(&inherited.node_id).await.map_err(|e| RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: the record of the inherited birth {} could not be read: {e}", inherited.node_id, me_path = &me.path, me_name = &me.name)))?;
    if let Some(p) = held.filter(|p| p.location() == *name && p.status != NodeStatus::Deleted) {
        let span = tracing::info_span!(
            "rdm.node_admin.node.delete.via-node-row",
            node_path = %me.path, node_name = %me.name,
            subject_path = %name.path, subject_name = %name.name,
            operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.recipe_step,
            node_id = %p.node_id,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        written.hold(&p);
        let r = match tracing::Instrument::instrument(write_deleted(storage, written, name, &p.node_id), span.clone()).await {
            Ok(_) => Ok(()),
            Err(DeletedRefused::NameHeld(held)) => Err(RowRefused::NameHeld(held)),
            Err(DeletedRefused::Unwritten(why)) => Err(RowRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: nodes.storage refused the Deleted row of the inherited birth at path {name_path:?}, name {name_name:?} ({}): {why}", p.node_id, me_path = &me.path, me_name = &me.name, name_path = &name.path, name_name = &name.name))),
        };
        match &r {
            Ok(()) => span.record("outcome", "row-deleted"),
            Err(e) => span.record("outcome", e.outcome()).record("detail", e.to_string().as_str()),
        };
        span.in_scope(|| tracing::info!("node.delete: the inherited birth's Deleted row decided; its record stays"));
        r?;
    }
    // The inherited birth is this create's explicit provenance, never the record last written at the name.
    let predecessor = crate::storage::BirthRef { node_id: inherited.node_id.clone(), incarnation_id: inherited.incarnation_id.clone() };
    create_own_row(None, storage, written, name, &inherited.mesh, node_id, &operation, RowFor::Recovery, Some(&predecessor), None, None).await
}

/// Day 0's first write of the bootstrap admin's own record at its `(path, name)` (R-OW1 item 2: the
/// bootstrap admin, as mesh1's primary, writes its own row at (mesh1.node-admin, 1) through the same validation and
/// storage path as any create, the first write of its birth, before Ready; no bypass; a conflicting
/// holder is NameHeld, never overwritten). The bootstrap admin, `node_id` at
/// `name`, is its mesh's primary from its birth and the only admin of its fabric, so it is the writer of
/// its own row; the row is the first write of its birth, made before anything else of it is written or
/// published. It is the create door's write ([`RowFor::Create`]), the record
/// `Pending`, one `rdm.node_admin.node.create.via-node-row` span, `moment` `create`.
/// Another node's live record at the name is `NameHeld` naming it, and nothing is written. The record
/// carries `executable`, the binding the bootstrap admin's birth runs (build-deployment.md §2.3), as a
/// create's row carries a launched node's.
pub async fn create_day0_row(storage: &dyn NodesStorage, written: &WrittenRows, name: &PathName, mesh: &str, node_id: &NodeId, executable: Option<&rdm_core::binding::Binding>) -> Result<RowCreated, RowRefused> {
    create_own_row(None, storage, written, name, mesh, node_id, &rdm_core::OperationId::node("fabric-create", node_id), RowFor::Create, None, executable, Some(crate::accepted::root_storage())).await
}

/// The adoption launch's first write of its own record at its new `(path, name)` under an in-force fabric.delete (Q159;
/// fabric-recovery.md "Nothing alive": "the adoption launch is created at a new path and name of the
/// node-admin type in an existing, not-yet-deleted mesh ... this is the one create a delete admits"): the
/// create door's write ([`RowFor::Create`]), the first write of its birth, operation `fabric-recovery`,
/// one `rdm.node_admin.node.create.via-node-row` span, `moment` `create`. Another node's live record at the
/// name is `NameHeld` naming it, and nothing is written.
pub async fn create_adopted_row(storage: &dyn NodesStorage, written: &WrittenRows, name: &PathName, mesh: &str, node_id: &NodeId) -> Result<RowCreated, RowRefused> {
    create_own_row(None, storage, written, name, mesh, node_id, &rdm_core::OperationId::node("fabric-recovery", node_id), RowFor::Create, None, None, None).await
}

/// [`create_day0_row`] as a child of `boot` without entering `boot` (the boot root is never entered so
/// background tasks spawned during start do not inherit it and its close time stays at `via-ready`).
pub async fn create_day0_row_in_boot(boot: &tracing::Span, storage: &dyn NodesStorage, written: &WrittenRows, name: &PathName, mesh: &str, node_id: &NodeId, executable: Option<&rdm_core::binding::Binding>) -> Result<RowCreated, RowRefused> {
    create_own_row(Some(boot), storage, written, name, mesh, node_id, &rdm_core::OperationId::node("fabric-create", node_id), RowFor::Create, None, executable, Some(crate::accepted::root_storage())).await
}

/// The first write of the record at `name` by the admin born at it as `node_id`, the writer of its own
/// row before it holds a view: [`seeded_put`] under one `rdm.node_admin.node.create.via-node-row` span,
/// `moment` `create`, the one write a create is wherever it is made.
#[allow(clippy::too_many_arguments)]
async fn create_own_row(parent: Option<&tracing::Span>, storage: &dyn NodesStorage, written: &WrittenRows, name: &PathName, mesh: &str, node_id: &NodeId, operation: &rdm_core::OperationId, of: RowFor, predecessor: Option<&crate::storage::BirthRef>, executable: Option<&rdm_core::binding::Binding>, storage_persistence: Option<rdm_core::meta::StoragePersistence>) -> Result<RowCreated, RowRefused> {
    let span = match parent {
        Some(p) => tracing::info_span!(
            parent: p,
            "rdm.node_admin.node.create.via-node-row",
            node_path = %name.path, node_name = %name.name,
            subject_path = %name.path, subject_name = %name.name,
            node_id = %node_id,
            operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.recipe_step,
            moment = "create",
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
            node_path = %name.path, node_name = %name.name,
            subject_path = %name.path, subject_name = %name.name,
            node_id = %node_id,
            operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.recipe_step,
            moment = "create",
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
    let mut guard = RowCreateGuard { span: span.clone(), done: false };
    let r = tracing::Instrument::instrument(seeded_put(storage, written, name, name, mesh, "node-admin", node_id, NodeStatus::Pending, &BTreeMap::new(), None, predecessor, executable, &std::collections::BTreeSet::new(), storage_persistence, of, &span, ""), span.clone()).await;
    guard.done = true;
    record_row_created(&span, &r);
    span.in_scope(|| tracing::info!("node.create: the first write of the record decided"));
    r
}

/// Why a node row's move was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenameRefused {
    /// The new name is held by another node: the move is refused and nothing was written.
    NameHeld(crate::storage::NameHeld),
    /// The move could not be written: why, whole.
    Unwritten(String),
}

/// The node's record moves from `from` to `to` (node-name-update.md, "Check and move"): `to` checked
/// against the records (`NameHeld { by: node.id }` when another node's live record holds it), then one
/// write of the node's record carrying `renamed_from: from`: the name move is that one write. A retry,
/// or a successor writer resuming the command, reads the record: one that already stands at `to` is
/// the move done, and nothing is written. The record written starts from the one the writer holds, its facts kept;
/// a node it holds no record of is written from `record`'s config. The one writer of a rename (R13:
/// `node.name.update` is one write path, `crate::node_name_update`). Answers what it did and the
/// record written, if it wrote one.
pub async fn move_row(storage: &dyn NodesStorage, written: &WrittenRows, me: &PathName, record: &Node, from: &PathName, to: &PathName) -> Result<(Moved, Option<NodeRow>), RenameRefused> {
    move_row_with_intent(storage, written, me, record, from, to, &crate::accepted::RenameIntent::Ordinary).await
}

/// Apply the intent fixed by Build admission through the shared rename writer.
pub(crate) async fn move_row_with_intent(storage: &dyn NodesStorage, written: &WrittenRows, me: &PathName, record: &Node, from: &PathName, to: &PathName, intent: &crate::accepted::RenameIntent) -> Result<(Moved, Option<NodeRow>), RenameRefused> {
    if from.path != to.path || record.path != to.path {
        return Err(RenameRefused::Unwritten("a node's path is immutable; nothing is written".into()));
    }
    PathName::new(&to.path, &to.name).map_err(|error| RenameRefused::Unwritten(error.to_string()))?;

    if from == to {
        return Ok((Moved::Renamed, None));
    }
    let node_id = &record.node_id;
    let _one = written.lock(node_id).await;
    let held = written.known_row(storage, node_id).await.map_err(|why| RenameRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: {why}", me_path = &me.path, me_name = &me.name)))?;
    if let Some(held) = held.as_ref() {
        if held.path != to.path {
            return Err(RenameRefused::Unwritten("a node's stored path is immutable; nothing is written".into()));
        }
    }
    if let Some(held) = held.as_ref().filter(|r| r.location() == *to) {
        // A move made before: its record stands at `to`.
        return Ok((Moved::AlreadyMoved, Some(held.clone())));
    }
    let provenance = intent.provenance(&held.as_ref().and_then(|r| r.replacement_from.clone()), from, to).map_err(RenameRefused::Unwritten)?;
    check_name(storage, Some(written), to, node_id).await.map_err(|e| RenameRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: nodes.storage did not answer the read of path {to_path:?}, name {to_name:?} for {node_id}: {e}", me_path = &me.path, me_name = &me.name, to_path = &to.path, to_name = &to.name)))?.map_err(RenameRefused::NameHeld)?;
    let mut row = held.unwrap_or_else(|| NodeRow::new(from.clone(), record.mesh.clone(), &record.r#type, node_id.clone(), record.status));
    { let at = to.clone(); row.path = at.path; row.name = at.name; }
    row.allocated = None;
    row.replacement_from = provenance;
    row.renamed_from = Some(from.name.clone());
    row.inherited_by = None;
    written.put(storage, &row).await.map_err(|e| RenameRefused::Unwritten(format!("path {me_path:?}, name {me_name:?}: nodes.storage refused the record of {node_id} at path {to_path:?}, name {to_name:?}: {e}", me_path = &me.path, me_name = &me.name, to_path = &to.path, to_name = &to.name)))?;
    Ok((Moved::Renamed, Some(row)))
}

/// What a move of a node's row did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Moved {
    /// The node's record stands at the new name now.
    Renamed,
    /// The node's record stood at the new name already: the move was made before.
    AlreadyMoved,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, FabricId, Mesh, MeshId, ProviderKind, ScopeStatus};
    use crate::storage::MemoryNodesStorage;

    /// CONTRACT: a successor's record at an old location never proves the previous node deleted.
    /// The previous node's retained ID record, including after rename, is the evidence.
    // @feature: node-records
    #[tokio::test]
    async fn deletion_proof_requires_the_exact_nodes_released_record() {
        let storage = MemoryNodesStorage::default();
        let location = PathName::new("arbitrary.location", "worker").unwrap();
        let mut predecessor = NodeRow::new(location.renamed().unwrap(), "explicit-mesh", "node", NodeId::mint(), NodeStatus::Ready);
        storage.put_node(&predecessor).await.unwrap();
        let successor = NodeRow::new(location.clone(), "explicit-mesh", "node", NodeId::mint(), NodeStatus::Ready);
        storage.put_node(&successor).await.unwrap();
        assert!(deleted_birth(&storage, &location, &predecessor.node_id).await.unwrap().is_none());
        assert!(deleted_birth(&storage, &location, &NodeId::mint()).await.unwrap().is_none());
        predecessor.status = NodeStatus::Deleted;
        storage.put_node(&predecessor).await.unwrap();
        assert!(deleted_birth(&storage, &location, &predecessor.node_id).await.unwrap().is_some());
        predecessor.status = NodeStatus::Withdrawn;
        storage.put_node(&predecessor).await.unwrap();
        assert!(deleted_birth(&storage, &location, &predecessor.node_id).await.unwrap().is_some());
    }

    // @feature: node-records
    #[tokio::test]
    async fn admitted_predecessor_id_survives_kind_change_and_rejects_another_mesh_writer() {
        let storage = MemoryNodesStorage::default();
        let written = WrittenRows::default();
        let original = PathName::new("opaque.path", "north").unwrap();
        let mut predecessor = NodeRow::new(original.renamed().unwrap(), "previous-mesh", "gateway", NodeId::mint(), NodeStatus::Stopped);
        let incarnation = IncarnationId::mint();
        predecessor.birth_mut(&incarnation, &predecessor.location());
        predecessor.replacement_from = Some(original.clone());
        storage.put_node(&predecessor).await.unwrap();
        let birth = crate::storage::BirthRef { node_id: predecessor.node_id.clone(), incarnation_id: incarnation };
        let successor = NodeId::mint();
        assert!(mark_retained(&storage, &original, &written, "new-mesh", &original, &successor, Some(&birth)).await.is_err());
        assert_eq!(storage.node(&birth.node_id).await.unwrap().unwrap().inherited_by, None);
        mark_retained(&storage, &original, &written, "previous-mesh", &original, &successor, Some(&birth)).await.unwrap();
        mark_retained(&storage, &original, &written, "previous-mesh", &original, &successor, Some(&birth)).await.unwrap();
        assert_eq!(storage.node(&birth.node_id).await.unwrap().unwrap().inherited_by, Some(successor));
    }

    #[tokio::test]
    async fn retained_mark_rechecks_the_writers_current_provenance_under_lock() {
        let storage = MemoryNodesStorage::default();
        let original = PathName::new("opaque.path", "north").unwrap();
        let mut stored = NodeRow::new(original.renamed().unwrap(), "mesh1", "gateway", NodeId::mint(), NodeStatus::Stopped);
        let incarnation = IncarnationId::mint();
        stored.birth_mut(&incarnation, &stored.location());
        stored.replacement_from = Some(original.clone());
        storage.put_node(&stored).await.unwrap();
        let pin = crate::storage::BirthRef { node_id: stored.node_id.clone(), incarnation_id: incarnation };
        let successor = NodeId::mint();
        let mut rollback = stored.clone();
        { let at = original.clone(); rollback.path = at.path; rollback.name = at.name; }
        rollback.replacement_from = None;
        let mut another_successor = stored.clone();
        another_successor.inherited_by = Some(NodeId::mint());
        for current in [rollback, another_successor] {
            let written = WrittenRows::default();
            written.hold(&current);
            assert!(mark_retained(&storage, &original, &written, "mesh1", &original, &successor, Some(&pin)).await.is_err());
            assert_eq!(written.row_of(&pin.node_id), Some(current));
            assert_eq!(storage.node(&pin.node_id).await.unwrap(), Some(stored.clone()));
        }
    }

    /// A released name permits a new create, but does not supply launch metadata.
    // @feature: node-records
    #[tokio::test]
    async fn fresh_creates_use_explicit_inputs_and_preserve_historical_rows() {
        for of in [RowFor::Create, RowFor::Successor] {
            for retained in [false, true] {
                let storage = MemoryNodesStorage::default();
                let written = WrittenRows::default();
                let name = PathName::new("opaque.path", "north").unwrap();
                let old_name = if retained { name.renamed().unwrap() } else { name.clone() };
                let mut old = NodeRow::new(old_name, "mesh1", "node", NodeId::mint(), if retained { NodeStatus::Stopped } else { NodeStatus::Deleted });
                old.executable = Some(rdm_core::binding::Binding { launch_id: "old-launch".into(), executable: "/old-executable".into(), sha256: "old-sha".into(), image: None });
                old.storage_persistence = Some(rdm_core::meta::StoragePersistence::Ephemeral);
                old.birth_mut(&IncarnationId::mint(), &old.location());
                if retained { old.replacement_from = Some(name.clone()); }
                storage.put_node(&old).await.unwrap();
                let id = NodeId::mint();
                let config = BTreeMap::from([("new-config".into(), "new-value".into())]);
                let tags = ["new-tag".into()].into();
                let created = seeded_put(&storage, &written, &name, &name, "mesh1", "node", &id, NodeStatus::Pending, &config, None, None, None, &tags, None, of, &tracing::Span::none(), "").await.unwrap();
                let RowCreated::Written { row, seeded_from } = created else { panic!("a new ID writes a fresh row") };
                assert_eq!(seeded_from, None);
                assert_eq!(row.initial_properties.as_ref().map(|p| (&p.config, &p.tags)), Some((&config, &tags)));
                assert_eq!((row.executable.clone(), row.storage_persistence), (None, None));
                assert!(row.births.is_empty());
                assert_eq!(row.predecessor, None);
                assert_eq!(row.replacement_from, None);
                mark_retained(&storage, &name, &written, "mesh1", &name, &id, None).await.unwrap();
                assert_eq!(storage.node(&old.node_id).await.unwrap(), Some(old));
                let durable = storage.node(&id).await.unwrap().unwrap();
                let retry = seeded_put(&storage, &written, &name, &name, "mesh1", "node", &id, NodeStatus::Pending, &BTreeMap::new(), None, None, None, &Default::default(), None, of, &tracing::Span::none(), "").await.unwrap();
                let RowCreated::HeldByNode(retried) = retry else { panic!("same ID retry keeps the existing row") };
                assert_eq!(retried, durable);
                assert_eq!(storage.node(&id).await.unwrap(), Some(durable));
            }
        }
    }

    /// An exact predecessor reference can supply launch metadata; config and tags remain inputs.
    // @feature: node-records
    #[tokio::test]
    async fn explicit_predecessor_preserves_launch_metadata_and_requires_its_incarnation() {
        for of in [RowFor::Create, RowFor::Successor] {
            let storage = MemoryNodesStorage::default();
            let written = WrittenRows::default();
            let name = PathName::new("opaque.path", "north").unwrap();
            let retained = of == RowFor::Successor;
            let mut old = NodeRow::new(if retained { name.renamed().unwrap() } else { name.clone() }, "mesh1", "node", NodeId::mint(), if retained { NodeStatus::Stopped } else { NodeStatus::Deleted });
            old.executable = Some(rdm_core::binding::Binding { launch_id: "previous-launch".into(), executable: "/previous-executable".into(), sha256: "previous-sha".into(), image: None });
            old.storage_persistence = Some(rdm_core::meta::StoragePersistence::Ephemeral);
            let incarnation = IncarnationId::mint();
            old.birth_mut(&incarnation, &old.location());
            if retained { old.replacement_from = Some(name.clone()); }
            storage.put_node(&old).await.unwrap();
            let id = NodeId::mint();
            let allocation = Allocation { predecessor: Some(crate::storage::BirthRef { node_id: old.node_id.clone(), incarnation_id: incarnation }), build_id: Some(rdm_core::BuildId::mint()), operation: rdm_core::OperationId::location("node.create", &name), incarnation: IncarnationId::mint(), deployment_id: crate::model::DeploymentId::mint() };
            let mut wrong = allocation.clone();
            wrong.predecessor.as_mut().unwrap().incarnation_id = IncarnationId::mint();
            assert!(seeded_put(&storage, &written, &name, &name, "mesh1", "node", &id, NodeStatus::Pending, &BTreeMap::new(), Some(&wrong), wrong.predecessor.as_ref(), None, &Default::default(), None, of, &tracing::Span::none(), "").await.is_err());
            assert!(storage.node(&id).await.unwrap().is_none());
            let config = BTreeMap::from([("new-config".into(), "new-value".into())]);
            let tags = ["new-tag".into()].into();
            let created = seeded_put(&storage, &written, &name, &name, "mesh1", "node", &id, NodeStatus::Pending, &config, Some(&allocation), allocation.predecessor.as_ref(), None, &tags, None, of, &tracing::Span::none(), "").await.unwrap();
            let RowCreated::Written { row, seeded_from } = created else { panic!("new ID writes its own record") };
            assert_eq!(seeded_from, Some(old.node_id.clone()));
            assert_eq!(row.allocated.as_ref().unwrap().predecessor, allocation.predecessor);
            assert_eq!(row.initial_properties.as_ref().map(|p| (&p.config, &p.tags)), Some((&config, &tags)));
            assert_eq!((&row.executable, row.storage_persistence), (&old.executable, old.storage_persistence));
            assert_eq!(storage.node(&old.node_id).await.unwrap(), Some(old));
            let durable = storage.node(&id).await.unwrap().unwrap();
            let retry = seeded_put(&storage, &written, &name, &name, "mesh1", "node", &id, NodeStatus::Pending, &BTreeMap::new(), Some(&allocation), allocation.predecessor.as_ref(), None, &Default::default(), None, of, &tracing::Span::none(), "").await.unwrap();
            let RowCreated::HeldByNode(retried) = retry else { panic!("same ID retry keeps its record") };
            assert_eq!(retried, durable);
            assert_eq!(storage.node(&id).await.unwrap(), Some(durable));
        }
    }

    /// An ordinary name ending in -old is not replacement provenance.
    #[tokio::test]
    async fn an_unrelated_old_looking_pair_neither_seeds_nor_marks_a_predecessor() {
        let storage = MemoryNodesStorage::default();
        let written = WrittenRows::default();
        let name = PathName::new("independent.path", "member").unwrap();
        let unrelated = NodeRow::new(name.renamed().unwrap(), "another-mesh", "gateway", NodeId::mint(), NodeStatus::Ready);
        check_name(&storage, None, &unrelated.location(), &unrelated.node_id).await.unwrap().unwrap();
        written.put(&storage, &unrelated).await.unwrap();
        let successor = NodeId::mint();
        let created = seeded_put(&storage, &written, &name, &name, "mesh1", "node", &successor, NodeStatus::Pending, &BTreeMap::new(), None, None, None, &Default::default(), None, RowFor::Arrival, &tracing::Span::none(), "").await.unwrap();
        let RowCreated::Written { row, seeded_from } = created else { panic!("a fresh row must be written") };
        assert_eq!(seeded_from, None);
        assert!(row.initial_properties.as_ref().is_some_and(|p| p.config.is_empty() && p.tags.is_empty()));
        mark_retained(&storage, &name, &written, "mesh1", &name, &successor, None).await.unwrap();
        assert_eq!(storage.node(&unrelated.node_id).await.unwrap().unwrap().inherited_by, None);
    }

    #[tokio::test]
    async fn a_rename_cannot_change_the_stored_path_or_infer_replacement_from_a_name() {
        let storage = MemoryNodesStorage::default();
        let written = WrittenRows::default();
        let location = PathName::new("apps.east", "worker").unwrap();
        let record = Node::allocated(location.clone(), "explicit-mesh", "node");
        let row = NodeRow::new(location.clone(), record.mesh.clone(), &record.r#type, record.node_id.clone(), NodeStatus::Ready);
        written.put(&storage, &row).await.unwrap();
        let elsewhere = PathName::new("apps.west", "worker-old").unwrap();
        assert!(matches!(move_row_with_intent(&storage, &written, &location, &record, &location, &elsewhere, &crate::accepted::RenameIntent::Replacement { original_path: location.path.clone(), original_name: location.name.clone()}).await, Err(RenameRefused::Unwritten(_))));
        assert_eq!(record_at(&storage, &elsewhere).await.unwrap(), None);
        assert_eq!(storage.node(&record.node_id).await.unwrap().unwrap().location(), location);
        // Even a stale caller whose two arguments agree cannot move the persisted path.
        let stale = Node { path: elsewhere.path.clone(), name: elsewhere.name.clone(), ..record.clone() };
        assert!(matches!(move_row(&storage, &written, &location, &stale, &elsewhere, &elsewhere.with_name("another").unwrap()).await, Err(RenameRefused::Unwritten(_))));
        let renamed = location.renamed().unwrap();
        move_row(&storage, &written, &location, &record, &location, &renamed).await.unwrap();
        let after = storage.node(&record.node_id).await.unwrap().unwrap();
        assert_eq!(after.path, location.path);
        assert_eq!(after.name, "worker-old");
        assert_eq!(after.replacement_from, None);
    }

    /// Replacement provenance survives a later name edit, and the pair the record left is free.
    #[tokio::test]
    async fn a_retained_predecessor_keeps_its_origin_through_another_move() {
        let storage = MemoryNodesStorage::default();
        let written = WrittenRows::default();
        let original = PathName::new("free.path", "member").unwrap();
        let retained = original.renamed().unwrap();
        let renamed = PathName::new(retained.path.clone(), "renamed").unwrap();
        let record = Node::allocated(original.clone(), "unrelated-mesh", "node");
        let mut row = NodeRow::new(retained.clone(), "unrelated-mesh", "node", record.node_id.clone(), NodeStatus::Ready);
        row.renamed_from = Some(original.name.clone());
        row.replacement_from = Some(original.clone());
        written.put(&storage, &row).await.unwrap();
        let me: PathName = rdm_core::PathName::new("elsewhere.node-admin", "writer").unwrap();
        move_row(&storage, &written, &me, &record, &retained, &renamed).await.unwrap();
        assert_eq!(record_at(&storage, &original).await.unwrap(), None);
        let after = storage.node(&record.node_id).await.unwrap().unwrap();
        assert_eq!(after.replacement_from, Some(original.clone()));
        assert_eq!(after.renamed_from, Some(retained.name));
        move_row_with_intent(&storage, &written, &me, &record, &renamed, &original, &crate::accepted::RenameIntent::Rollback { original_path: original.path.clone(), original_name: original.name.clone()}).await.unwrap();
        assert_eq!(storage.node(&record.node_id).await.unwrap().unwrap().replacement_from, None);
    }

    // @feature: node-records
    /// CONTRACT (i150 R8): recovery captures config and tags at its first write; repeating that write
    /// after the predecessor changes hands on the original snapshot.
    #[tokio::test]
    async fn a_recovery_row_preserves_its_complete_property_snapshot() {
        use rdm_node::node_records::{NodeProperties, RecordBirth};
        let storage = MemoryNodesStorage::default();
        let written = WrittenRows::default();
        let name: PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
        let (old, old_incarnation) = (NodeId::mint(), IncarnationId::mint());
        let mut exited = NodeRow::new(name.clone(), "mesh1", "node", old.clone(), NodeStatus::Deleted);
        exited.birth_mut(&old_incarnation, &name);
        storage.write_record(&exited).await.unwrap();
        let build_id = crate::build::BuildId::mint();
        let mut own = NodeProperties {
            birth: RecordBirth { node_id: old.clone(), incarnation_id: old_incarnation.clone() },
            config: [("source".into(), "node".into())].into(), tags: ["node".into()].into(),
            endpoint_id: None, transport_addr: None, listeners: vec![], app_types: None,
            held: Default::default(),
        };
        storage.put_properties(&own).await.unwrap();
        let new = NodeId::mint();
        let allocated = Allocation { predecessor: Some(crate::storage::BirthRef { node_id: old.clone(), incarnation_id: old_incarnation.clone() }), build_id: Some(build_id), operation: rdm_core::OperationId::location("node.create", &name), incarnation: IncarnationId::mint(), deployment_id: crate::model::DeploymentId::mint() };
        let empty_config = BTreeMap::new();
        let empty_tags = Default::default();
        let span = tracing::Span::none();
        let create = || seeded_put(&storage, &written, &name, &name, "mesh1", "node", &new, NodeStatus::Starting, &empty_config, Some(&allocated), allocated.predecessor.as_ref(), None, &empty_tags, None, RowFor::Recovery, &span, "");
        let first = create().await.unwrap();
        let first = match first { RowCreated::Written { row, .. } => row, _ => panic!("the first create writes") };
        let snapshot = first.initial_properties.clone().unwrap();
        assert_eq!(snapshot.config, own.config);
        assert_eq!(snapshot.tags, own.tags);
        own.config.clear(); own.tags.clear();
        storage.put_properties(&own).await.unwrap();
        let second = create().await.unwrap();
        let second = match second { RowCreated::HeldByNode(row) => row, _ => panic!("the same exact create is held") };
        assert_eq!(second.initial_properties, Some(snapshot), "a lost create receipt cannot choose another snapshot");
    }

    fn node(name: PathName, mesh: &str, r#type: &str, primary: bool, fabric_primary: bool) -> Node {
        let mut n = Node::allocated(name, mesh, r#type);
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
        record_at(storage, &n.location()).await.unwrap().map(|r| r.status)
    }

    /// CONTRACT: a node row's status is written only by its mesh's writer (node-identity-and-state.md
    /// §4.3: "The fabric-primary writes a row only as acting primary of a mesh with no primary. No other
    /// process writes a `Node` row"; states.md "Who sets each state": node deleting, "the node's
    /// mesh-primary (the fabric-primary for a mesh with no primary)"). mesh2's final primary
    /// `mesh2.node-admin.1` is lost and mesh2's election named `mesh2.node-admin.2`: the fabric-primary
    /// `mesh1.node-admin.1`, the executor of the lost primary's explicit delete, writes neither its row's
    /// `Deleting` nor `mesh2.node.1`'s, and both rows stay as they were; `mesh2.node-admin.2` writes
    /// `mesh2.node.1`'s. With no primary in mesh2 but the lost one, the fabric-primary writes the lost
    /// primary's own row as acting primary, and still not `mesh2.node.1`'s. Each status write agrees with the writer predicate of the same node's `Deleted`
    /// row ([`NodeRows::writes_unreached`]). The `rdm.node_admin.node.update.via-row-status` spans name the
    /// executor (`node`) and the outcome.
    #[tokio::test]
    async fn a_rows_status_is_written_only_by_its_meshs_writer() {
        let capture = crate::span_capture::Capture::new("node-rows-status");
        let _d = tracing::dispatcher::set_default(&capture.dispatch);
        let fp = node(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, true);
        let lost = node(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", false, false);
        let elected = node(PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", true, false);
        let rpc = node(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", false, false);
        let storage = Arc::new(MemoryNodesStorage::default());
        for n in [&lost, &rpc] {
            write_record(&*storage, &crate::storage::NodeRow::new(n.location(), n.mesh.clone(), &n.r#type, n.node_id.clone(), NodeStatus::Ready)).await.unwrap().unwrap();
        }
        let shared = Arc::new(RwLock::new(view(vec![fp.clone(), lost.clone(), elected.clone(), rpc.clone()])));
        let at_fp = NodeRows::new(fp.location(), fp.node_id.clone(), shared.clone(), storage.clone());
        let at_writer = NodeRows::new(elected.location(), elected.node_id.clone(), shared.clone(), storage.clone());
        let root = tracing::info_span!("cell.row-status-writer");
        let _in_root = root.enter();

        let lost_inc = lost.incarnation_id.clone().unwrap();
        let rpc_inc = rpc.incarnation_id.clone().unwrap();
        // The fabric-primary, mesh2 holding an elected primary: neither row is its to write.
        let fp_lost = at_fp.status(&lost, &lost_inc, NodeStatus::Deleting).await;
        assert_eq!(at_fp.writes_unreached(&lost).await, false, "the Deleted row's predicate: the fabric-primary does not write mesh2.node-admin.1's row");
        assert!(matches!(fp_lost, Err(RowRefused::NotMeshPrimary(_))), "the fabric-primary is not mesh2's writer: mesh2.node-admin.1's Deleting is refused: {fp_lost:?}");
        assert_eq!(status_of(&storage, &lost).await, Some(NodeStatus::Ready), "nothing is written: the row stays as it was");
        let fp_rpc = at_fp.status(&rpc, &rpc_inc, NodeStatus::Deleting).await;
        assert!(matches!(fp_rpc, Err(RowRefused::NotMeshPrimary(_))), "nor mesh2.node.1's: {fp_rpc:?}");
        assert_eq!(status_of(&storage, &rpc).await, Some(NodeStatus::Ready));
        // mesh2's elected primary, its one writer, writes mesh2.node.1's Deleting.
        assert!(at_writer.writes_unreached(&rpc).await);
        at_writer.status(&rpc, &rpc_inc, NodeStatus::Deleting).await.unwrap_or_else(|e| panic!("mesh2's writer writes the row: {e}"));
        assert_eq!(status_of(&storage, &rpc).await, Some(NodeStatus::Deleting));
        // mesh2 holds no primary but the lost one: the fabric-primary writes the lost primary's own row as
        // acting primary, and still no other row of mesh2, as its Deleted row's predicate says.
        {
            let mut t = shared.write().await;
            t.nodes.retain(|n| n.location() != elected.location());
            t.nodes.iter_mut().filter(|n| n.location() == lost.location()).for_each(|n| n.is_primary = true);
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
        let spans: Vec<(PathName, PathName, String)> = captured.into_iter().map(|s| (
            PathName::new(&s.attributes["node_path"], &s.attributes["node_name"]).unwrap(),
            PathName::new(&s.attributes["subject_path"], &s.attributes["subject_name"]).unwrap(),
            s.attributes["outcome"].clone(),
        )).collect();
        let expected = [(&fp, &lost, "not-mesh-primary"), (&fp, &rpc, "not-mesh-primary"), (&elected, &rpc, "written"), (&fp, &lost, "written"), (&fp, &rpc, "not-mesh-primary")]
            .map(|(a, b, outcome)| (a.location(), b.location(), outcome.to_string()));
        assert_eq!(spans, expected, "via-row-status: separate executor and subject fields");

    }
}
