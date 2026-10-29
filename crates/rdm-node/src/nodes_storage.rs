//! `nodes.storage`'s rows (storage-whole-records.md, "The objects"): each node's record at its node id,
//! written whole by its mesh-primary, every birth's runtime row and the tags index's changes, and the
//! point reads of a node's record by its id or by its `path.name` (query.md §1, Q237). The node-admin
//! writes them (`rdm_admin::storage::NodesStorage`); any node reads another node's row here, the store
//! opened on its first read and never at its start, join or create (Q227).

use crate::model::{EndpointId, IncarnationId, MeshId, NodeId, PathName};
use crate::storage_error::StorageError;
use async_trait::async_trait;
use rdm_core::status::NodeState;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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
    /// The `path.name` the birth held when its first fact was written.
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
    /// The birth's fabric endpoint id, as its mesh's writer's view holds it (the mesh rows writer,
    /// `rdm_admin::admin::rows_writer`): with [`Self::transport_addr`], where the birth is reached. A birth
    /// its writer has not written it for carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_id: Option<EndpointId>,
    /// The one address of the birth's Iroh endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_addr: Option<std::net::SocketAddr>,
    /// Non-Iroh listeners.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listeners: Vec<(String, std::net::SocketAddr)>,
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
        Self { incarnation_id, name, declared: BTreeSet::new(), operations: Vec::new(), silence: None, data_dir_lost: None, data_dir_unreadable: None, cert: None, signing_cert: None, endpoint_id: None, transport_addr: None, listeners: Vec::new(), is_primary: None, is_fabric_primary: None }
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
    /// The node's minted id.
    pub node_id: NodeId,
    /// The node's `path.name`.
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

/// The record of one node (storage-whole-records.md, "The objects"): kept at its minted node id and
/// written whole by its writer, the node's mesh-primary (the fabric-primary only as the acting primary
/// of a mesh with none, `rdm_admin::node_rows`), from the copy it caches: its `path.name`, status, config,
/// tags and executable, and the facts of its current birth and of the one a restart replaced. A
/// `Deleted` record stands and keeps the config for the next node created at the same `path.name`.
/// Beside it: the node's name claim (`NodesStorage::claim_node_name`), the `path.name` index
/// ([`NodeRowsStorage::node_at`]) and the tags index (`NodesStorage::tagged`), whose entries RDM writes
/// from the moves it computes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRow {
    /// The node's `path.name`.
    pub name: PathName,
    /// The node that held the name when the row was written.
    pub node_id: NodeId,
    /// The node's status: `Deleted` once its runtime exit is proven.
    pub status: crate::model::NodeStatus,
    /// The application's own key/values on the node; RDM never reads it.
    pub config: BTreeMap<String, String>,
    /// The node's tag labels (R-TG1), set through `node.tag.create` / `node.tag.delete`, each one a
    /// row of the tags index while the row is live. A `Deleted` row keeps them, as it keeps the config;
    /// a row written before tags existed carries none.
    #[serde(default)]
    pub tags: BTreeSet<String>,
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
    /// The `path.name` a rename moved this row from (proposal node-properties-own-commands.md §2.1
    /// "Claim and move"), written by the put at the new key. While the key it names still holds this
    /// row's node, the release of that key is pending and the writer that meets the row releases it
    /// (`rdm_admin::node_rows::finish_release`); once that key no longer holds the node it says nothing,
    /// so it is never cleared. A row no rename wrote carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renamed_from: Option<PathName>,
    /// The successor that took this row's config and tags at a replace (node-replace.md step 3 and
    /// Inherited: the successor's row "inherits config and tags from the predecessor row ... whose
    /// labels leave the active tags index"), written on the retained predecessor's row once the
    /// successor's claim stands at the name. The row keeps its tags, as a `Deleted` row does, and the
    /// tags index holds none of them: every label leaves with that write and none arrives with a later
    /// write at the same key. A rename of the row writes none: the node takes a name again
    /// (node-replace.md "Rollback" step 3). A row no successor marked carries none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_by: Option<NodeId>,
    /// Immutable application declaration, scoped to the exact incarnation that reported it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_types: Option<AppTypes>,
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
    /// The row's revision (node-config-update.md; Q235 P1): stepped by its one writer, the node's
    /// mesh-primary, on every write of the row (`rdm_admin::node_rows::WrittenRows::put`). Every carrier of
    /// the row's config names it, and every install, at the node and in each atlas, is made only over
    /// an older revision. A row written before the field reads 0.
    #[serde(default)]
    pub revision: u64,
    /// The birth that wrote this version of the record (Q109: "the row's key names its writer's
    /// birth"), stamped by its writer on every write (`rdm_admin::node_rows::WrittenRows::put`): a reader
    /// ignores a version written by a birth its fold holds deleted (build.md: "a deleted birth whose
    /// writes every reader refuses"; `rdm_admin::node_rows::fold_records`). A row written before the
    /// field carries none, and no reader ignores it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer: Option<BirthRef>,
}

/// The application's declaration held beside its node row.
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
    /// The Build whose operation claimed the name.
    pub build_id: rdm_core::BuildId,
    /// The operation that claimed it (`node.create:<path>`).
    pub operation: String,
    /// The incarnation minted for the birth.
    pub incarnation: IncarnationId,
    /// The exact deployment minted for the birth: what the provider finds its runtime by, if one
    /// was ever launched.
    pub deployment_id: crate::model::DeploymentId,
}


impl NodeRow {
    /// A record of `node_id` at `name` with `status`, nothing else on it yet.
    pub fn new(name: PathName, node_id: NodeId, status: crate::model::NodeStatus) -> Self {
        Self { name, node_id, status, config: BTreeMap::new(), tags: BTreeSet::new(), allocated: None, renamed_from: None, inherited_by: None, app_types: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, topology_storage_provider: None, revision: 0, writer: None }
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

    /// The node's name within its path (`<base>[.old]`).
    pub fn node_name(&self) -> String {
        self.name.name()
    }

    /// The node's `node_type`.
    pub fn node_type(&self) -> &'static str {
        self.name.node_type()
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

    /// The labels the tags index holds for this node once this record is written over `previous`,
    /// the node's record as its writer held it before, split into (leaving, arriving): a label the
    /// previous record carried and this one does not, or every label of a `Deleted` record or of a
    /// record a successor inherited ([`Self::inherited_by`]), leaves; every label of any other live
    /// record arrives.
    pub fn tag_moves(&self, previous: Option<&NodeRow>) -> (BTreeSet<String>, BTreeSet<String>) {
        let held: BTreeSet<String> = previous.filter(|p| p.node_id == self.node_id).map(|p| p.tags.clone()).unwrap_or_default();
        if self.status == crate::model::NodeStatus::Deleted || self.inherited_by.is_some() {
            return (held.union(&self.tags).cloned().collect(), BTreeSet::new());
        }
        (held.difference(&self.tags).cloned().collect(), self.tags.clone())
    }
}

/// The point reads of the nodes storage: a node's record by its id or by its `path.name`, the same
/// answers the node-admin's `NodesStorage` gives.
#[async_trait]
pub trait NodeRowsStorage: Send + Sync {
    /// The record of `node_id`: a point read of its key, a `Withdrawn` one included; `None` when none
    /// was ever written.
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, StorageError>;
    /// The record the `path.name` index names at `name`: the node whose record was last written at it,
    /// a `Deleted` one included; `None` when no record was ever written there, or when that record is
    /// `Withdrawn`.
    async fn node_at(&self, name: &PathName) -> Result<Option<NodeRow>, StorageError>;
}

/// A node's record in `nodes.jsonl`, at its node id.
pub const RECORD_FORMAT: &str = "node-record/1";
/// The kind of [`RECORD_FORMAT`].
pub const RECORD_KIND: &str = "node-record";
/// A birth's runtime row in `nodes.jsonl`, at `<node id>--<incarnation>`.
pub const RUNTIME_FORMAT: &str = "node-runtime/1";
/// The kind of [`RUNTIME_FORMAT`].
pub const RUNTIME_KIND: &str = "node-runtime";
/// A change of a node's row of the tags index in `nodes.jsonl`, at `<node id>/<label>` (R-TG1).
pub const TAG_FORMAT: &str = "node-tag/1";
/// The kind of [`TAG_FORMAT`].
pub const TAG_KIND: &str = "node-tag";

/// One change of the tags index: the node carries the label from this line on, or (`carried` false)
/// it no longer does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagRow {
    /// The label.
    pub label: String,
    /// The node.
    pub node_id: NodeId,
    /// Whether the node carries the label from this line on.
    pub carried: bool,
}

#[cfg(feature = "file-backend")]
/// The formats `nodes.jsonl` holds: every reader of the file folds all of them.
pub fn formats() -> crate::store_log::Formats {
    crate::store_log::Formats::new().with::<NodeRow>(RECORD_FORMAT).with::<RuntimeRow>(RUNTIME_FORMAT).with::<TagRow>(TAG_FORMAT)
}

#[cfg(feature = "file-backend")]
/// The `path.name` index, folded from `nodes.jsonl`: each name's node is the node whose record line
/// was last appended at it, a `Deleted` or `Withdrawn` one included.
type PathIndex = std::sync::Arc<std::sync::Mutex<BTreeMap<String, NodeId>>>;

#[cfg(feature = "file-backend")]
/// The nodes' records of the fabric's store root's `nodes.jsonl` (R-SR1), folded from byte 0 at the
/// first read or append, never at the open (rdm#89, Q227), and from the consumed offset at each read
/// after it; the `path.name` index is the fold of the record lines.
#[derive(Debug, Clone)]
pub struct FileNodeRows {
    records: crate::store_log::StoreLog,
    paths: PathIndex,
}

#[cfg(feature = "file-backend")]
impl FileNodeRows {
    /// Open the store on the fabric's store `root`, reading none of it: its first read folds it.
    pub async fn open(root: &crate::store_root::StoreRoot) -> Result<Self, StorageError> {
        let paths: PathIndex = Default::default();
        let index = paths.clone();
        let observe: crate::store_log::Observer = std::sync::Arc::new(move |l: &crate::store_log::Line| {
            if l.kind() == RECORD_KIND {
                if let Ok(row) = NodeRow::deserialize(&l.record) {
                    index.lock().unwrap_or_else(|e| e.into_inner()).insert(row.name.to_string(), row.node_id);
                }
            }
        });
        let records = crate::store_log::StoreLog::open_observed_on_first_read(&root.dir().join(crate::store_root::NODES_FILE), formats(), observe).await?;
        Ok(Self { records, paths })
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
        self.records.advance().await?;
        let Some(node_id) = self.paths.lock().unwrap_or_else(|e| e.into_inner()).get(&name.to_string()).cloned() else { return Ok(None) };
        Ok(self.node(&node_id).await?.filter(|r| r.status != crate::model::NodeStatus::Withdrawn))
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

/// A node's `id` or its `path.name`: what a point read of its row names (`node_row(id | name)`,
/// query.md §1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdOrName {
    /// The node's id.
    Id(NodeId),
    /// The node's `path.name`.
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

    /// CONTRACT: docs/architecture/node-identity-and-state.md:149
    /// "node.delete releases the node's identity and name; its row stays. The node's mesh-primary writes
    /// the row with status deleted, keeping the record and its config; the deleted row releases..."
    #[test]
    fn declared_state_and_tag_moves_compute_cycle_state_and_tag_diffs() {
        let inc = IncarnationId::mint();
        let name: PathName = "mesh1.rpc.1".parse().unwrap();
        let mut birth = BirthFacts::new(inc, name.clone());
        assert_eq!(birth.declared_state(), None);

        birth.declared.insert(NodeState::Ready);
        assert_eq!(birth.declared_state(), Some(NodeState::Ready));
        birth.declared.insert(NodeState::Stopped);
        assert_eq!(birth.declared_state(), Some(NodeState::Stopped));

        let node_id = NodeId::mint();
        let mut prev = NodeRow::new(name.clone(), node_id.clone(), crate::model::NodeStatus::Ready);
        prev.tags = BTreeSet::from(["role-rpc".to_string(), "zone-a".to_string()]);

        let mut next = prev.clone();
        next.tags = BTreeSet::from(["role-rpc".to_string(), "zone-b".to_string()]);
        let (leaving, arriving) = next.tag_moves(Some(&prev));
        assert_eq!(leaving, BTreeSet::from(["zone-a".to_string()]));
        assert_eq!(arriving, BTreeSet::from(["role-rpc".to_string(), "zone-b".to_string()]));

        // A Deleted record or a record with `inherited_by` removes all tags and arrives none.
        let mut deleted = next.clone();
        deleted.status = crate::model::NodeStatus::Deleted;
        let (del_leaving, del_arriving) = deleted.tag_moves(Some(&prev));
        assert_eq!(
            del_leaving,
            BTreeSet::from(["role-rpc".to_string(), "zone-a".to_string(), "zone-b".to_string()])
        );
        assert!(del_arriving.is_empty());
    }
}

