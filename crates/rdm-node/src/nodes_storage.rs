//! `nodes.storage`'s rows (storage-whole-records.md, "The objects"): each node's record at its node id,
//! in two parts (Q333), its mesh-primary's whole row line and the node's own line, the reader folding
//! the latest of each; every birth's runtime row; and the point reads of a node's record by its id or
//! by its `(path, name)` through the name claim (query.md §1, Q237). The node-admin writes the rows
//! ([`NodesStorage`]) and each node writes its own part through the same store; any node reads another
//! node's row here, the store opened on its first read and never at its start, join or create (Q227).

use crate::model::{IncarnationId, MeshId, NodeId, PathName};
use crate::storage_error::StorageError;
use async_trait::async_trait;
use rdm_core::status::NodeState;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
pub use rdm_client::NameHeld;

/// A row written before `topology_storage_provider` was recorded reads as `file`, the one backend there
/// was.
fn file_provider() -> Option<String> {
    Some(rdm_core::launch::TOPOLOGY_STORAGE_FILE.into())
}

/// An operation effect applied to one birth (Q49), on its node's record: independent of the birth's
/// current cycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeOperation {
    /// The operation that produced this effect.
    pub operation: rdm_core::status::DeclaredOperation,
    /// The effect applied.
    pub state: NodeState,
}

/// The `data-dir-lost` fact on a birth (R-FM2): the inherited data dir was absent for
/// `RDM_DATA_DIR_GONE_FLOORS` staleness floors while the provider reported the birth `Exited`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataDirLost {
    /// The absent data dir path.
    pub path: String,
    /// When the absent data dir was first observed, ms since the Unix epoch.
    pub since: u64,
}

/// The `data-dir-unreadable` fact on a birth (R-FM2): the inherited data dir exists on disk, but
/// `node-key` is missing, unreadable, or corrupt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataDirUnreadable {
    /// The broken data dir path.
    pub path: String,
    /// Why `node-key` could not be loaded from the data dir.
    pub cause: String,
}

/// A birth's member cert dates (fabric-certs.md §Tracking), written by the node's mesh-primary when
/// the birth holds the cert. The written expiry is the receipt (Q141); the dates are the birth's
/// renewal state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertDates {
    /// When the cert was issued, rafka-time ms.
    pub issued_at: u64,
    /// When it expires, rafka-time ms.
    pub expires_at: u64,
    /// When its issuing cert expires, rafka-time ms.
    pub issuer_expires_at: u64,
    /// When its issuer renews it (`rdm_core::certs::renew_at`), rafka-time ms.
    pub renew_at: u64,
}

/// A node-admin birth's signing cert dates (mesh-issuer-renew.md): the cert it signs its mesh's member
/// certs with stays in its process, like the root; its dates are written by the admin's mesh-primary
/// when the admin holds it. The written expiry is the receipt (Q141).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SigningCertDates {
    /// When the signing cert was minted, rafka-time ms.
    pub issued_at: u64,
    /// When it expires, rafka-time ms.
    pub expires_at: u64,
    /// When the fabric-primary renews it, rafka-time ms.
    pub renew_at: u64,
}

/// One birth's facts on its node's record (storage-whole-records.md, "The objects"), each kept by its
/// full identity so no fact erases another: one declared entry per state (a late Pending never removes
/// Ready; the reader takes the greatest), one effect per (build_id, attempt, operation).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BirthFacts {
    /// The birth.
    pub incarnation_id: IncarnationId,
    /// The `(path, name)` the birth held when its first fact was written.
    pub name: PathName,
    /// The states the birth declared and its authority applied in its current cycle.
    #[serde(default)]
    pub declared: BTreeSet<NodeState>,
    /// The operation effects applied to the birth, of every cycle.
    #[serde(default)]
    pub operations: Vec<NodeOperation>,
    /// The silence status its mesh-primary holds (node-identity-and-state.md §7.3): `PendingReconnect`,
    /// then `Dead`; `None` while heard. Observer-inferred: never itself a reason to act.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silence: Option<crate::model::NodeStatus>,
    /// `data-dir-lost` (R-FM2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir_lost: Option<DataDirLost>,
    /// `data-dir-unreadable` (R-FM2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir_unreadable: Option<DataDirUnreadable>,
    /// The member cert dates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert: Option<CertDates>,
    /// A node-admin birth's signing cert dates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signing_cert: Option<SigningCertDates>,
    /// The revision at which this birth is its node type's primary in its mesh, when it is: the
    /// election's result, recorded by the mesh-primary (node-identity-and-state.md §2.7); the write
    /// records the role and does not grant it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_primary: Option<u64>,
    /// The revision at which this birth is the fabric-primary, when it is, recorded on the node-admin's
    /// own row by its mesh-primary (node-identity-and-state.md §2.8; node-primary-update.md: for a
    /// fabric-primary that is its own mesh's primary, itself), as [`Self::is_primary`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_fabric_primary: Option<u64>,
}

impl BirthFacts {
    /// A birth that holds no fact yet.
    pub fn new(incarnation_id: IncarnationId, name: PathName) -> Self {
        Self { incarnation_id, name, declared: BTreeSet::new(), operations: Vec::new(), silence: None, data_dir_lost: None, data_dir_unreadable: None, cert: None, signing_cert: None, is_primary: None, is_fabric_primary: None }
    }

    /// The greatest state declared in the birth's current cycle.
    pub fn declared_state(&self) -> Option<NodeState> {
        self.declared.iter().next_back().copied()
    }

    /// Keep the effect `state` of `operation`, once per (build_id, attempt, operation): an effect
    /// already held for that identity stands.
    pub fn apply_operation(&mut self, operation: &rdm_core::status::DeclaredOperation, state: NodeState) {
        if !self.operations.iter().any(|o| o.operation == *operation) {
            self.operations.push(NodeOperation { operation: operation.clone(), state });
        }
    }
}

/// One exact birth, by node and incarnation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BirthRef {
    /// The node.
    pub node_id: NodeId,
    /// The birth's incarnation.
    pub incarnation_id: IncarnationId,
}

/// How many births a record keeps the facts of: its current incarnation and the one a restart replaced
/// (storage-whole-records.md, "The objects"); the Build journal keeps the history.
pub const BIRTHS_KEPT: usize = 2;
/// One birth's exact runtime as this admin first held it: the durable map a successor fabric
/// primary proves an exit from when its own topology view never heard the birth, and the validated
/// report its accepting authority admitted (written first at the join's admission,
/// `rdm_admin::join::Joins::keep_admitted`, before the report is released: a re-run of the create after
/// that admin restarted completes WaitForMeshJoin from it, node-create.md step 5). Its own keyed row
/// (NodeId + IncarnationId), written once by a blind put, never rewritten.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeRow {
    /// Explicit birth ownership and runtime kind.
    pub mesh: String,
    /// The explicit runtime kind of this birth.
    pub kind: rdm_core::NodeKind,
    /// The node's minted id.
    pub node_id: NodeId,
    /// The node's `(path, name)`.
    pub name: PathName,
    /// The incarnation of the birth.
    pub incarnation_id: IncarnationId,
    /// The runtime the birth ran in.
    pub runtime: rdm_core::RuntimeFact,
    /// The data dir the birth reported: where its process runtime records an exit code.
    pub data_dir: Option<String>,
}

impl RuntimeRow {
    /// The row's key: the birth it describes.
    pub fn key(&self) -> String {
        format!("{}--{}", self.node_id, self.incarnation_id.as_str())
    }
}

/// The lifecycle record of one node (storage-whole-records.md, "The objects"): kept at its minted node
/// id and written whole by its writer, the node's mesh-primary (the fabric-primary only as the acting
/// primary of a mesh with none, `rdm_admin::node_rows`), from the copy it caches: its `(path, name)`,
/// status and executable, and the facts of its current birth and of the one a restart replaced: the
/// mesh-primary's part of the node's record (Q333). What the node writes itself (config, tags,
/// listeners, endpoint, transport address, declared types, held) is the node's own part at the same
/// key (`crate::node_records::NodeProperties`). A `Deleted` record stands. A `(path, name)` resolves to
/// the node whose live record stands there ([`NodeRowsStorage::node_at`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRow {
    /// The explicitly declared owning mesh.
    pub mesh: String,
    /// The explicitly declared runtime kind.
    pub kind: rdm_core::NodeKind,
    /// The node's `(path, name)`.
    pub name: PathName,
    /// The node that held the name when the row was written.
    pub node_id: NodeId,
    /// The node's status: `Deleted` once its runtime exit is proven.
    pub status: crate::model::NodeStatus,
    /// What the create that claimed the name allocated (lifecycles/node-create.md step 1: the claim
    /// precedes everything prepared or launched for the birth): the claim row only. The claim is the
    /// one write of the create that outlives a lost executor (its Build receipts are in that
    /// executor's own journal, build.md §5.2), so the identity the claim was made with is durable in
    /// it, and a later attempt of the same operation of the same Build that never heard the
    /// `AllocateIdentity` receipt continues under that identity: its claim is the same birth's
    /// (`rdm_admin::node_rows::Claimed::HeldByNode`). Every later row of the node is written after its
    /// launch and carries none; a row written before allocations existed carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocated: Option<Allocation>,
    /// The immutable property snapshot selected by this exact claim, handed to its launch.
    /// It includes application knowledge from the same read as recovered config and tags.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_properties: Option<rdm_core::launch::InitialProperties>,
    /// The `(path, name)` a rename moved this row from (proposal node-properties-own-commands.md §2.1
    /// "Claim and move"), written by the put at the new key. While the key it names still holds this
    /// row's node, the release of that key is pending and the writer that meets the row releases it
    /// (`rdm_admin::node_rows::finish_release`); once that key no longer holds the node it says nothing,
    /// so it is never cleared. A row no rename wrote carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renamed_from: Option<PathName>,
    /// The original pair of this exact subject retained by a replacement. Independent of the
    /// most recent claim release; cleared only when the replacement rolls back to this pair.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacement_from: Option<PathName>,
    /// The successor whose claim at this row's original pair named it as its predecessor (node-replace.md
    /// step 3), written on the retained predecessor's row once the successor's claim stands at the name.
    /// A rename of the row writes none: the node takes a name again (node-replace.md "Rollback" step 3).
    /// A row no successor marked carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_by: Option<NodeId>,
    /// The executable binding the node runs, beside its config (node-replace.md step 3: "the row
    /// records it beside `config`"): written at the create's claim with the binding that create
    /// launches, kept by every later write of the row, a `Deleted` row's included, and seeded from the
    /// previous row like the config, so a node recovered at the path runs the bin its row records,
    /// never the recovering admin's (node-recovery.md "Retained"). A row written before it was
    /// recorded carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<rdm_core::binding::Binding>,
    /// The facts of the node's births, at most [`BIRTHS_KEPT`], the newest last.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub births: Vec<BirthFacts>,
    /// The Mesh the node belongs to, by its minted id (node-identity-and-state.md §2.7: `Node.mesh_id`),
    /// written by its mesh's writer with its births' addresses (`rdm_admin::admin::rows_writer`): a mesh
    /// created again under the name is another mesh with another id (R-MN1). A record no writer has
    /// written it on carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mesh_id: Option<MeshId>,
    /// The exact birth this node replaced, written at its create from the recovery attempt that named
    /// it: what a birth recovered over a fresh data dir reads its predecessor's `data-dir-lost` through.
    /// `inherited_by` points the other way and is not followed backwards.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predecessor: Option<BirthRef>,
    /// The node's `storage_persistence` as its create spec names it (storage.md §1; Q166, build.md §1:
    /// "onto the row, effective at the node's next birth"): written at the claim and by a Build whose
    /// spec names another, inherited with the config by a recovery and a replace's successor. A row
    /// written before the field carries none, and no Build compares it. A row written before storage.md
    /// §1's rename carries it as `storage` (durable format moved).
    #[serde(default, alias = "storage", skip_serializing_if = "Option::is_none")]
    pub storage_persistence: Option<rdm_core::meta::StorageMeta>,
    /// The `topology_storage_provider` the node writes (storage.md §2): the fact, beside the Fabric
    /// row's intent, recorded by RDM at the node's name claim from the provider the claiming admin
    /// writes, the one it writes into the node's launch; never put by a caller. A row written before
    /// the field read as `file`, the one backend there was; `None` (written as `null`) on a row no claim
    /// recorded it on.
    #[serde(default = "file_provider")]
    pub topology_storage_provider: Option<String>,
    /// The node's placement as its create spec names it, beside `storage_persistence` and written as it is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<rdm_core::meta::PlacementMeta>,
    /// The birth that wrote this version of the record (Q109: "the row's key names its writer's
    /// birth"), stamped by its writer on every write (`rdm_admin::node_rows::WrittenRows::put`): a reader
    /// ignores a version written by a birth its fold holds deleted (build.md: "a deleted birth whose
    /// writes every reader refuses"; `rdm_admin::node_rows::fold_records`). A row written before the
    /// field carries none, and no reader ignores it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer: Option<BirthRef>,
}

/// The application types a birth declared at join, on the node's own part of its record (Q333).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppTypes {
    /// Exact birth that declared the types.
    pub incarnation: IncarnationId,
    /// Immutable declared application types.
    pub types: BTreeSet<String>,
}

/// The identity a create's `AllocateIdentity` decided for the birth its claim names (beside the
/// row's `node_id`), and the operation of the Build that decided it: keyed by `(build_id,
/// operation)`, never by attempt, because every attempt of the operation continues the same birth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Allocation {
    /// The exact predecessor admitted for this create, independent of successor ownership.
    pub predecessor: Option<BirthRef>,
    /// The Build whose operation claimed the name.
    pub build_id: rdm_core::BuildId,
    /// The operation that claimed it (`node.create:<path>`).
    pub operation: rdm_core::OperationId,
    /// The incarnation minted for the birth.
    pub incarnation: IncarnationId,
    /// The exact deployment minted for the birth: what the provider finds its runtime by, if one
    /// was ever launched.
    pub deployment_id: crate::model::DeploymentId,
}


impl NodeRow {
    /// A record of `node_id` at `name` with `status`, nothing else on it yet.
    pub fn new(name: PathName, mesh: impl Into<String>, kind: rdm_core::NodeKind, node_id: NodeId, status: crate::model::NodeStatus) -> Self {
        Self { name, mesh: mesh.into(), kind, node_id, status, allocated: None, initial_properties: None, renamed_from: None, replacement_from: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, topology_storage_provider: None, writer: None }
    }

    /// Write `to` as the node's status, set by its mesh-primary at the transition states.md "Who sets
    /// each state" names for the birth `birth`: whether the status moved. A `Deleted` or `Withdrawn`
    /// record stands; a birth that is not the record's newest moves nothing. A start of a stopped node
    /// (`Starting`) and a new join (`Pending`, from any status but `Deleting`) begin its next cycle (a
    /// create's claim writes `Starting` as the record's first status). `Deleting` is the mesh-primary's,
    /// from node.delete's claim, over any status (states.md, the paragraph after "Who sets each state",
    /// Q233). A stop's acknowledgement (`Stopping`) sets stopping over any status but `Stopped`, a node
    /// already connections-deleted or left included: the node's `Stopped` declaration may reach the
    /// mesh-primary before the stop's own frames (node-stop.md step 5), and a stopped row stands. Every
    /// other status only moves forward along node.stop's frames (node-stop.md steps 1-4, Q205: starting,
    /// pending, ready, deleting, stopping, connections-deleting, connections-deleted,
    /// leaving, left, stopped), so each primitive's frame moves the row on from stopping, and the stop a
    /// delete composes moves it on from deleting to stopped before the delete writes it deleted.
    pub fn set_status(&mut self, birth: &IncarnationId, to: crate::model::NodeStatus) -> bool {
        use crate::model::NodeStatus as S;
        fn order(s: S) -> u8 {
            match s {
                S::Starting => 0,
                S::Pending => 1,
                S::Ready | S::PendingReconnect | S::Dead | S::Reserved5 => 2,
                S::Deleting => 3,
                S::Stopping => 4,
                S::ConnectionsDeleting => 5,
                S::ConnectionsDeleted => 6,
                S::Leaving => 7,
                S::Left => 8,
                S::Stopped => 9,
                S::Deleted | S::Withdrawn => 10,
            }
        }
        let current = self.status;
        let newest = self.newest_birth().is_none_or(|b| b.incarnation_id == *birth);
        let moves = match to {
            _ if matches!(current, S::Deleted | S::Withdrawn) || !newest => false,
            S::Starting => matches!(current, S::Stopped | S::Leaving),
            S::Pending => current != S::Deleting,
            S::Deleting => true,
            S::Stopping => current != S::Stopped,
            _ => order(to) > order(current),
        };
        if moves {
            self.status = to;
        }
        moves && current != to
    }

    /// The facts of the node's newest birth, if the record keeps any.
    pub fn newest_birth(&self) -> Option<&BirthFacts> {
        self.births.last()
    }

    /// The node's path (`<mesh>.<kind>`), separate from its name within that path.
    pub fn path(&self) -> String {
        self.name.path()
    }

    /// The node's name within its path (an ordinary name, or `<name>-old` for an explicitly replaced predecessor).
    pub fn node_name(&self) -> String {
        self.name.name()
    }

    /// The node's `node_type`.
    pub fn node_type(&self) -> &'static str {
        self.kind.node_type()
    }

    /// The facts of the birth `incarnation_id`, if the record keeps them.
    pub fn birth(&self, incarnation_id: &IncarnationId) -> Option<&BirthFacts> {
        self.births.iter().find(|b| b.incarnation_id == *incarnation_id)
    }

    /// The facts of the birth `incarnation_id`, made when the record keeps none, at `name`: a birth
    /// new to the record is the newest, and the oldest is let go past [`BIRTHS_KEPT`].
    pub fn birth_mut(&mut self, incarnation_id: &IncarnationId, name: &PathName) -> &mut BirthFacts {
        let at = match self.births.iter().position(|b| b.incarnation_id == *incarnation_id) {
            Some(at) => at,
            None => {
                self.births.push(BirthFacts::new(incarnation_id.clone(), name.clone()));
                while self.births.len() > BIRTHS_KEPT {
                    self.births.remove(0);
                }
                self.births.len() - 1
            }
        };
        &mut self.births[at]
    }
}

/// The point reads of the nodes storage: a node's record by its id or by its `(path, name)`.
#[async_trait]
pub trait NodeRowsStorage: Send + Sync {
    /// The record of `node_id`: a point read of its key, a `Withdrawn` one included; `None` when none
    /// was ever written.
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, StorageError>;
    /// The record that holds `name` ([`holds_its_name`]): the one live record standing at that path
    /// and name, read from the records (node-identity-and-state.md: "A current-pair lookup reads the
    /// node records by their path and name"). `None` when none does (a `Deleted` record frees its
    /// pair); two live records at one pair are refused by name, never answered as either.
    async fn node_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError>;
}

/// Whether `row` holds its path and name: it is neither `Deleted` (a delete frees the pair,
/// node-delete.md) nor `Withdrawn` (a create withdrawn before its node ran).
pub fn holds_its_name(row: &NodeRow) -> bool {
    !matches!(row.status, crate::model::NodeStatus::Deleted | crate::model::NodeStatus::Withdrawn)
}

/// The births re-created at their path and name among `records`: each predecessor a record names, with
/// the node that names it (the re-created birth).
pub fn re_created(records: &[NodeRow]) -> std::collections::HashMap<BirthRef, NodeId> {
    records.iter().filter_map(|r| r.predecessor.clone().map(|p| (p, r.node_id.clone()))).collect()
}

/// The node that re-created `row`'s node, when `row`'s version was written by its own birth and a record
/// of `re_created` names that birth its predecessor (Q109, build.md: a deleted birth's writes every reader
/// refuses): that version is not the node's record.
pub fn superseded<'a>(row: &NodeRow, re_created: &'a std::collections::HashMap<BirthRef, NodeId>) -> Option<&'a NodeId> {
    row.writer.as_ref().filter(|w| w.node_id == row.node_id).and_then(|w| re_created.get(w))
}

/// The one record of `rows` that holds `name`, folded as every reader folds them: a version a
/// re-created birth's own birth wrote ([`superseded`]) is not a record, and a record holds its name
/// while [`holds_its_name`]. Two that hold it are refused by name (`file` the store they were read
/// from), never answered as either.
pub fn record_holding(rows: impl IntoIterator<Item = NodeRow>, name: &PathName, file: &str) -> Result<Option<NodeRow>, StorageError> {
    let at: Vec<NodeRow> = rows.into_iter().filter(|r| r.name == *name).collect();
    let re_created = re_created(&at);
    let mut holding = at.into_iter().filter(|r| holds_its_name(r) && superseded(r, &re_created).is_none());
    let first = holding.next();
    if let (Some(a), Some(b)) = (&first, holding.next()) {
        return Err(StorageError::Unrecognised { file: file.to_string(), reason: format!("two live records hold path {:?}, name {:?}: nodes {} and {}", name.path, name.name, a.node_id, b.node_id) });
    }
    Ok(first)
}

/// A node's record in `nodes.jsonl`, at its node id.
pub const RECORD_FORMAT: &str = "node-record/1";
/// The kind of [`RECORD_FORMAT`].
pub const RECORD_KIND: &str = "node-record";
/// A birth's runtime row in `nodes.jsonl`, at `<node id>--<incarnation>`.
pub const RUNTIME_FORMAT: &str = "node-runtime/1";
/// The kind of [`RUNTIME_FORMAT`].
pub const RUNTIME_KIND: &str = "node-runtime";
/// The format of the node's own part of its record (Q333): the line the node appends at its node id,
/// carrying only what it writes ([`crate::node_records::NodeProperties`]).
pub const PROPERTIES_FORMAT: &str = "node-properties/1";
/// The kind of [`PROPERTIES_FORMAT`].
pub const PROPERTIES_KIND: &str = "node-properties";
#[cfg(feature = "file-backend")]
/// The formats `nodes.jsonl` holds: every reader of the file folds all of them.
pub fn formats() -> crate::store_log::Formats {
    crate::store_log::Formats::new().with::<NodeRow>(RECORD_FORMAT).with::<RuntimeRow>(RUNTIME_FORMAT).with::<crate::node_records::NodeProperties>(PROPERTIES_FORMAT)
}

#[cfg(feature = "file-backend")]
/// The nodes' records of the fabric's store root's `nodes.jsonl` (R-SR1), folded from byte 0 at the
/// first read or append, never at the open (rdm#89, Q227), and from the consumed offset at each read
/// after it; a `(path, name)` resolves to the live record standing there.
#[derive(Debug, Clone)]
pub struct FileNodeRows {
    records: crate::store_log::StoreLog,
}

#[cfg(feature = "file-backend")]
impl FileNodeRows {
    /// Open the store on the fabric's store `root`, reading none of it: its first read folds it.
    pub async fn open(root: &crate::store_root::StoreRoot) -> Result<Self, StorageError> {
        let records = crate::store_log::StoreLog::open_on_first_read(&root.dir().join(crate::store_root::NODES_FILE), formats()).await?;
        Ok(Self { records })
    }

    /// The store's file, read and appended to.
    pub fn log(&self) -> &crate::store_log::StoreLog {
        &self.records
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl NodeRowsStorage for FileNodeRows {
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, StorageError> {
        self.records.latest(RECORD_KIND, node_id.as_str()).await?.map(|l| l.decode(self.records.path())).transpose()
    }
    async fn node_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError> {
        let rows = self.records.latest_of(RECORD_KIND).await?.iter().map(|l| l.decode(self.records.path())).collect::<Result<Vec<NodeRow>, _>>()?;
        record_holding(rows, name, &self.records.path().display().to_string())
    }
}

#[cfg(feature = "file-backend")]
/// [`FileNodeRows`] on the fabric's store root, opened on its first read (Q237: never at the node's
/// start, join or create, Q227); an open refused is answered to that read and the next read opens
/// again.
pub struct FileNodeRowsOnFirstRead {
    root: crate::store_root::StoreRoot,
    opened: tokio::sync::OnceCell<FileNodeRows>,
}

#[cfg(feature = "file-backend")]
impl FileNodeRowsOnFirstRead {
    /// The rows of the store root `root`, nothing read yet.
    pub fn at(root: crate::store_root::StoreRoot) -> Self {
        Self { root, opened: tokio::sync::OnceCell::new() }
    }

    async fn rows(&self) -> Result<&FileNodeRows, StorageError> {
        self.opened.get_or_try_init(|| FileNodeRows::open(&self.root)).await
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl NodeRowsStorage for FileNodeRowsOnFirstRead {
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, StorageError> {
        self.rows().await?.node(node_id).await
    }
    async fn node_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError> {
        self.rows().await?.node_at(name).await
    }
}

/// The durable record of the nodes: each node's record at its id, through which a `(path, name)`
/// resolves to the node whose live record stands there, and every birth's runtime row. Nothing is removed (R-SR2): a
/// create withdrawn before its node ran leaves its record `Withdrawn`.
#[async_trait]
pub trait NodesStorage: Send + Sync {
    /// Keep the runtime of one birth (a blind put of its own row).
    async fn put_runtime(&self, row: &RuntimeRow) -> Result<(), StorageError>;
    /// Every birth's runtime row the store holds; folded by the reader.
    async fn runtimes(&self) -> Result<Vec<RuntimeRow>, StorageError>;
    /// The runtime row of the exact birth `(node_id, incarnation)`: a point read by its key.
    async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<RuntimeRow>, StorageError>;
    /// The record of `node_id`: a point read of its key, a `Withdrawn` one included; `None` when none
    /// was ever written.
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, StorageError>;
    /// Every node record of `mesh`, a `Withdrawn` one skipped.
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRow>, StorageError>;
    /// Write `row` whole at its node id, as given (Q109: storage stores and returns what it is given):
    /// the mesh-primary's part of the node's record (Q333). Durable before it answers (Q54).
    async fn put_node(&self, row: &NodeRow) -> Result<(), StorageError>;
    /// Write `record` whole at its node id: the node's own part of its record (Q333), appended by the
    /// node itself beside its mesh-primary's part. Durable before it answers (Q54).
    async fn put_properties(&self, record: &crate::node_records::NodeProperties) -> Result<(), StorageError>;
    /// The node's own part of the record of `node_id`, its latest line; `None` when the node has
    /// written none.
    async fn properties(&self, node_id: &NodeId) -> Result<Option<crate::node_records::NodeProperties>, StorageError>;
    /// The record that holds `name`, read from the records ([`NodeRowsStorage::node_at`]).
    async fn node_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError>;
}

/// A nodes store held in memory.
#[derive(Debug, Default)]
pub struct MemoryNodesStorage {
    runtimes: Mutex<BTreeMap<String, RuntimeRow>>,
    records: Mutex<BTreeMap<NodeId, NodeRow>>,
    /// The nodes' own parts of their records.
    properties: Mutex<BTreeMap<NodeId, crate::node_records::NodeProperties>>,
}

#[async_trait]
impl NodesStorage for MemoryNodesStorage {
    async fn put_runtime(&self, row: &RuntimeRow) -> Result<(), StorageError> {
        self.runtimes.lock().unwrap().insert(row.key(), row.clone());
        Ok(())
    }
    async fn runtimes(&self) -> Result<Vec<RuntimeRow>, StorageError> {
        Ok(self.runtimes.lock().unwrap().values().cloned().collect())
    }
    async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<RuntimeRow>, StorageError> {
        Ok(self.runtimes.lock().unwrap().get(&format!("{}--{}", node_id, incarnation.as_str())).cloned())
    }
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, StorageError> {
        Ok(self.records.lock().unwrap().get(node_id).cloned())
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRow>, StorageError> {
        Ok(self.records.lock().unwrap().values().filter(|r| r.mesh == mesh && r.status != crate::model::NodeStatus::Withdrawn).cloned().collect())
    }
    async fn put_node(&self, row: &NodeRow) -> Result<(), StorageError> {
        self.records.lock().unwrap().insert(row.node_id.clone(), row.clone());
        Ok(())
    }
    async fn put_properties(&self, record: &crate::node_records::NodeProperties) -> Result<(), StorageError> {
        self.properties.lock().unwrap().insert(record.birth.node_id.clone(), record.clone());
        Ok(())
    }
    async fn properties(&self, node_id: &NodeId) -> Result<Option<crate::node_records::NodeProperties>, StorageError> {
        Ok(self.properties.lock().unwrap().get(node_id).cloned())
    }
    async fn node_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError> {
        record_holding(self.records.lock().unwrap().values().cloned(), name, "nodes.storage (memory)")
    }
}

#[cfg(feature = "file-backend")]
/// The nodes' records and their runtime rows, each write one line of the
/// root's `nodes.jsonl` (R-SR1, R-SR2), appended straight to the file: a record at its node id, written by its mesh's writer, durable before the answer
/// (Q54); a runtime row at `<node id>--<incarnation>`, written once by its birth's launching admin, the
/// durable map a successor proves exits from, as durable as when it stood in the launcher's data dir.
/// A `(path, name)` resolves to the live record standing there. The admin's own row stays its own seed in its
/// data dir, `nodes/self.json` (`rdm_admin::storage::own_in`).
#[derive(Debug)]
pub struct FileNodesStorage {
    /// The records and the runtime rows, and their point reads.
    rows: FileNodeRows,
    /// The rows' file, appended to.
    records: crate::store_log::StoreLog,
    /// The same file opened to append ([`crate::store_log::StoreLog::open_append`]): the node's own
    /// part of its record is appended through it with no fold before the append, as a node's start
    /// writes it and reads nothing (Q227).
    own_parts: crate::store_log::StoreLog,
    log_writer: crate::store_log::LogWriter,
}

#[cfg(feature = "file-backend")]
impl FileNodesStorage {
    /// Open the store on the fabric's store `root`, written by `writer`. `own_data_dir` and `own_mesh`
    /// are the opening admin's (its own row is read from the one, `rdm_admin::storage::own_in`); the store is the
    /// fabric's, every mesh's nodes in one file.
    pub async fn open(own_data_dir: &std::path::Path, root: &crate::store_root::StoreRoot, own_mesh: &str, writer: &crate::store_log::LogWriter) -> Result<Self, StorageError> {
        let _ = (own_data_dir, own_mesh);
        let rows = FileNodeRows::open(root).await?;
        let records = rows.log().clone();
        let own_parts = root.log_append(crate::store_root::NODES_FILE, formats()).await?;
        Ok(Self { rows, records, own_parts, log_writer: writer.clone() })
    }

    /// Finishes every write of the node records and the runtime rows this store has
    /// issued (node-primary-update.md step 3).
    pub async fn finish_issued_writes(&self) {
        self.records.finish_issued_writes().await;
        self.own_parts.finish_issued_writes().await;
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl NodesStorage for FileNodesStorage {
    /// A blind put of the birth's own row: one append, nothing read first (its one writer is the admin
    /// that launched the birth, build.md §5.3).
    async fn put_runtime(&self, row: &RuntimeRow) -> Result<(), StorageError> {
        self.records.append(&self.log_writer, RUNTIME_FORMAT, &row.key(), row).await
    }
    async fn runtimes(&self) -> Result<Vec<RuntimeRow>, StorageError> {
        self.records.latest_of(RUNTIME_KIND).await?.iter().map(|l| l.decode(self.records.path())).collect()
    }
    async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<RuntimeRow>, StorageError> {
        self.records.latest(RUNTIME_KIND, &format!("{}--{}", node_id, incarnation.as_str())).await?.map(|l| l.decode(self.records.path())).transpose()
    }
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, StorageError> {
        NodeRowsStorage::node(&self.rows, node_id).await
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRow>, StorageError> {
        let mut out = Vec::new();
        for l in self.records.latest_of(RECORD_KIND).await? {
            let row: NodeRow = l.decode(self.records.path())?;
            if row.mesh == mesh && row.status != crate::model::NodeStatus::Withdrawn {
                out.push(row);
            }
        }
        Ok(out)
    }
    async fn put_node(&self, row: &NodeRow) -> Result<(), StorageError> {
        self.records.append(&self.log_writer, RECORD_FORMAT, row.node_id.as_str(), row).await
    }
    async fn put_properties(&self, record: &crate::node_records::NodeProperties) -> Result<(), StorageError> {
        self.own_parts.append(&self.log_writer, PROPERTIES_FORMAT, record.birth.node_id.as_str(), record).await
    }
    async fn properties(&self, node_id: &NodeId) -> Result<Option<crate::node_records::NodeProperties>, StorageError> {
        self.records.latest(PROPERTIES_KIND, node_id.as_str()).await?.map(|l| l.decode(self.records.path())).transpose()
    }
    async fn node_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError> {
        NodeRowsStorage::node_at(&self.rows, name).await
    }
}

/// A node's `id` or its `(path, name)`: what a point read of its row names (`node_row(id | name)`,
/// query.md §1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdOrName {
    /// The node's id.
    Id(NodeId),
    /// The node's `(path, name)`.
    Name(PathName),
}

impl From<NodeId> for IdOrName {
    fn from(id: NodeId) -> Self {
        Self::Id(id)
    }
}

impl From<PathName> for IdOrName {
    fn from(name: PathName) -> Self {
        Self::Name(name)
    }
}

impl IdOrName {
    /// The row `rows` holds at this id or name: [`NodeRowsStorage::node`] or [`NodeRowsStorage::node_at`].
    pub async fn read(&self, rows: &dyn NodeRowsStorage) -> Result<Option<NodeRow>, StorageError> {
        match self {
            Self::Id(id) => rows.node(id).await,
            Self::Name(name) => rows.node_at(name).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: a birth's declared state is the greatest it declared in its current cycle.
    #[test]
    fn declared_state_is_the_greatest_declared() {
        let inc = IncarnationId::mint();
        let name: PathName = rdm_core::PathName::new("mesh1.rpc", "1").unwrap();
        let mut birth = BirthFacts::new(inc, name.clone());
        assert_eq!(birth.declared_state(), None);

        birth.declared.insert(NodeState::Ready);
        assert_eq!(birth.declared_state(), Some(NodeState::Ready));
        birth.declared.insert(NodeState::Stopped);
        assert_eq!(birth.declared_state(), Some(NodeState::Stopped));
    }
}
