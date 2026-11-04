//! `nodes.storage` (storage-whole-records.md, "The objects"): each node's record at its node id
//! (Q366), written whole by the node itself (Q356), and the point reads of a node's record by its id or
//! by its `(path, name)` (query.md §1, Q237). A node's mesh-primary, or the fabric-primary for a mesh
//! with none, writes a node's record only when the node cannot: a node that is lost, killed or
//! deleted. Any node reads another node's record here at runtime, the store opened on its first read
//! and never at its start, join or create (Q227).

use crate::model::{IncarnationId, NodeId, PathName};
use crate::storage_error::StorageError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
pub use rdm_client::NameHeld;
pub use rdm_core::certs::{CertDates, SigningCertDates};

/// One exact birth, by node and incarnation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BirthRef {
    /// The node.
    pub node_id: NodeId,
    /// The birth's incarnation.
    pub incarnation_id: IncarnationId,
}

/// A node's record (Q366): one record per node, keyed by its node id, append-only, its latest line
/// current. The node writes every field of it itself (Q356); its mesh-primary, or the fabric-primary
/// for a mesh with none, writes it only for a node that is lost, killed or deleted, from the node's
/// latest line. A node's mesh is the first location of its `path` ([`Self::mesh`]). A record with a
/// field outside these is refused by name at decode. A `Deleted` record stands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeRecord {
    /// The node's minted id: the record's key.
    pub node_id: NodeId,
    /// The node's path; its first location is its mesh.
    pub path: String,
    /// The name, beside its path; it changes only by node.name.update.
    pub name: String,
    /// The node's type.
    pub r#type: String,
    /// The node's incarnation: the same through stop and start, new for a new node.
    pub incarnation: IncarnationId,
    /// The node's lifecycle state.
    pub state: crate::model::NodeStatus,
    /// The node's bound fabric endpoint.
    pub endpoint_id: crate::model::EndpointId,
    /// The node's bound fabric transport address.
    pub transport_addr: std::net::SocketAddr,
    /// The node's bound non-Iroh listeners, by name.
    pub listeners: Vec<(String, std::net::SocketAddr)>,
    /// The runtime the node runs in: what its provider finds it by.
    pub runtime: rdm_core::RuntimeFact,
    /// The revision at which the node is its type's primary in its mesh, when it is (a node-admin's:
    /// the mesh-primary): the election's result, recorded by the node; the write records the role and
    /// does not grant it.
    pub is_primary: Option<u64>,
    /// The revision at which the node is the fabric-primary, when it is, as [`Self::is_primary`].
    pub is_fabric_primary: Option<u64>,
    /// The node's member cert dates (fabric-certs.md §Tracking), when it holds a cert.
    pub cert: Option<CertDates>,
    /// A node-admin's signing cert dates, when it holds one.
    pub signing_cert: Option<SigningCertDates>,
    /// The node's own storage (storage.md §1).
    pub storage: rdm_core::meta::Storage,
    /// The executable binding the node runs (build-deployment.md §2), when its create named one.
    pub executable: Option<rdm_core::binding::Binding>,
    /// The application's complete configuration map.
    pub config: BTreeMap<String, String>,
    /// The node's tag labels.
    pub tags: BTreeSet<String>,
}

impl NodeRecord {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> PathName {
        PathName { path: self.path.clone(), name: self.name.clone() }
    }

    /// The node's mesh: the first location of its path (Q366).
    pub fn mesh(&self) -> String {
        rdm_core::path::mesh_of(&self.path)
    }

    /// The node's exact birth.
    pub fn birth(&self) -> BirthRef {
        BirthRef { node_id: self.node_id.clone(), incarnation_id: self.incarnation.clone() }
    }

    /// Set `to` as the node's state: whether it moved. A `Deleted` or `Withdrawn` record stands. A
    /// start of a stopped node (`Starting`) and a new join (`Pending`, from any state but `Deleting`)
    /// begin its next cycle. `Deleting` is the mesh-primary's, from node.delete's admission, over any
    /// state (states.md). A stop's acknowledgement (`Stopping`) sets stopping over any state but
    /// `Stopped`. Every other state only moves forward along node.stop's frames (node-stop.md steps
    /// 1-4, Q205: starting, pending, ready, deleting, stopping, connections-deleting,
    /// connections-deleted, leaving, left, stopped).
    pub fn set_state(&mut self, to: crate::model::NodeStatus) -> bool {
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
        let current = self.state;
        let moves = match to {
            _ if matches!(current, S::Deleted | S::Withdrawn) => false,
            S::Starting => matches!(current, S::Stopped | S::Leaving),
            S::Pending => current != S::Deleting,
            S::Deleting => true,
            S::Stopping => current != S::Stopped,
            _ => order(to) > order(current),
        };
        if moves {
            self.state = to;
        }
        moves && current != to
    }
}

/// A node's record as a point read answers it: the record of its latest line and that line's `time`
/// (its CloudEvents `time`, the writer's rafka-time at the write; `None` when the writer held none or
/// the store keeps no time).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLine {
    /// The node's record.
    pub record: NodeRecord,
    /// The latest line's write time, RFC 3339 in UTC milliseconds.
    pub time: Option<String>,
}

/// The point reads of the nodes storage: a node's record by its id or by its `(path, name)`.
#[async_trait]
pub trait NodeRowsStorage: Send + Sync {
    /// The record of `node_id` with its latest line's time: a point read of its key, a `Withdrawn` one
    /// included; `None` when none was ever written.
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeLine>, StorageError>;
    /// Every node's latest record standing at the path and name `name`, in any state: persistence
    /// only, the reader folds them ([`crate::pair_record::resolve`], Q109).
    async fn records_at(&self, name: &PathName) -> Result<Vec<NodeRecord>, StorageError>;
}

/// The `dataschema` of a node's record line in `nodes.jsonl`, its `subject` the node id.
pub const RECORD_FORMAT: &str = "node-record/2";
/// The `type` of [`RECORD_FORMAT`]'s lines.
pub const RECORD_KIND: &str = "node-record";
#[cfg(feature = "file-backend")]
/// The `dataschema`s `nodes.jsonl` holds: its one, the node record.
pub fn formats() -> crate::store_log::Formats {
    crate::store_log::Formats::new().with::<NodeRecord>(RECORD_FORMAT)
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
fn line_of(log: &crate::store_log::StoreLog, line: &crate::store_log::Line) -> Result<NodeLine, StorageError> {
    Ok(NodeLine { record: line.decode(log.path())?, time: line.attributes.time.clone() })
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl NodeRowsStorage for FileNodeRows {
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeLine>, StorageError> {
        self.records.latest(RECORD_KIND, node_id.as_str()).await?.map(|l| line_of(&self.records, &l)).transpose()
    }
    async fn records_at(&self, name: &PathName) -> Result<Vec<NodeRecord>, StorageError> {
        let rows = self.records.latest_of(RECORD_KIND).await?.iter().map(|l| l.decode(self.records.path())).collect::<Result<Vec<NodeRecord>, _>>()?;
        Ok(rows.into_iter().filter(|r| r.location() == *name).collect())
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
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeLine>, StorageError> {
        self.rows().await?.node(node_id).await
    }
    async fn records_at(&self, name: &PathName) -> Result<Vec<NodeRecord>, StorageError> {
        self.rows().await?.records_at(name).await
    }
}

/// The durable record of the nodes: each node's record at its id, through which a `(path, name)`
/// resolves to the node whose live record stands there. Nothing is removed (R-SR2).
#[async_trait]
pub trait NodesStorage: Send + Sync {
    /// The record of `node_id` with its latest line's time: a point read of its key; `None` when none
    /// was ever written.
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeLine>, StorageError>;
    /// Every node record of `mesh` (its path's first location), a `Withdrawn` one skipped.
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRecord>, StorageError>;
    /// Write `record` whole at its node id, as given (Q109: storage stores and returns what it is
    /// given). Durable before it answers (Q54).
    async fn put_node(&self, record: &NodeRecord) -> Result<(), StorageError>;
    /// Every node's latest record standing at `name`, in any state ([`NodeRowsStorage::records_at`]).
    async fn records_at(&self, name: &PathName) -> Result<Vec<NodeRecord>, StorageError>;
}

/// A nodes store held in memory: it keeps no write time.
#[derive(Debug, Default)]
pub struct MemoryNodesStorage {
    records: Mutex<BTreeMap<NodeId, NodeRecord>>,
}

#[async_trait]
impl NodesStorage for MemoryNodesStorage {
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeLine>, StorageError> {
        Ok(self.records.lock().unwrap().get(node_id).cloned().map(|record| NodeLine { record, time: None }))
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRecord>, StorageError> {
        Ok(self.records.lock().unwrap().values().filter(|r| r.mesh() == mesh && r.state != crate::model::NodeStatus::Withdrawn).cloned().collect())
    }
    async fn put_node(&self, record: &NodeRecord) -> Result<(), StorageError> {
        self.records.lock().unwrap().insert(record.node_id.clone(), record.clone());
        Ok(())
    }
    async fn records_at(&self, name: &PathName) -> Result<Vec<NodeRecord>, StorageError> {
        Ok(self.records.lock().unwrap().values().filter(|r| r.location() == *name).cloned().collect())
    }
}

#[cfg(feature = "file-backend")]
/// The nodes' records, each write one line of the root's `nodes.jsonl` (R-SR1, R-SR2), appended
/// straight to the file with no fold before it (a node's start writes its record and reads nothing,
/// Q227), durable before the answer (Q54). A `(path, name)` resolves to the live record standing
/// there. The admin's own row stays its own seed in its data dir, `nodes/self.json`
/// (`rdm_admin::storage::own_in`).
#[derive(Debug)]
pub struct FileNodesStorage {
    /// The records and their point reads.
    rows: FileNodeRows,
    /// The same file opened to append ([`crate::store_log::StoreLog::open_append`]).
    appends: crate::store_log::StoreLog,
    log_writer: crate::store_log::LogWriter,
}

#[cfg(feature = "file-backend")]
impl FileNodesStorage {
    /// Open the store on the fabric's store `root`, written by `writer`: the fabric's store, every
    /// mesh's nodes in one file.
    pub async fn open(root: &crate::store_root::StoreRoot, writer: &crate::store_log::LogWriter) -> Result<Self, StorageError> {
        let rows = FileNodeRows::open(root).await?;
        let appends = root.log_append(crate::store_root::NODES_FILE, formats()).await?;
        Ok(Self { rows, appends, log_writer: writer.clone() })
    }

    /// Finishes every write of the node records this store has issued (node-primary-update.md step 3).
    pub async fn finish_issued_writes(&self) {
        self.rows.log().finish_issued_writes().await;
        self.appends.finish_issued_writes().await;
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl NodesStorage for FileNodesStorage {
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeLine>, StorageError> {
        NodeRowsStorage::node(&self.rows, node_id).await
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRecord>, StorageError> {
        let log = self.rows.log();
        let mut out = Vec::new();
        for l in log.latest_of(RECORD_KIND).await? {
            let row: NodeRecord = l.decode(log.path())?;
            if row.mesh() == mesh && row.state != crate::model::NodeStatus::Withdrawn {
                out.push(row);
            }
        }
        Ok(out)
    }
    async fn put_node(&self, record: &NodeRecord) -> Result<(), StorageError> {
        self.appends.append(&self.log_writer, RECORD_KIND, RECORD_FORMAT, record.node_id.as_str(), record).await
    }
    async fn records_at(&self, name: &PathName) -> Result<Vec<NodeRecord>, StorageError> {
        NodeRowsStorage::records_at(&self.rows, name).await
    }
}

/// A node's `id` or its `(path, name)`: what a point read of its record names (`node_row(id | name)`,
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
    /// The record `rows` holds at this id or name: [`NodeRowsStorage::node`], or the records at the
    /// name folded by [`crate::pair_record::resolve`].
    pub async fn read(&self, rows: &dyn NodeRowsStorage) -> Result<Option<NodeRecord>, StorageError> {
        match self {
            Self::Id(id) => Ok(rows.node(id).await?.map(|l| l.record)),
            Self::Name(name) => crate::pair_record::resolve(rows.records_at(name).await?, name),
        }
    }
}

/// A record of every Q366 field for a cell: `node_id` at `name` of `type` in `state`, bound at
/// `127.0.0.1:1`, an all-zero endpoint and a process runtime of deployment `fixture-deployment`.
pub fn fixture_record(name: &PathName, r#type: &str, node_id: NodeId, incarnation: IncarnationId, state: crate::model::NodeStatus) -> NodeRecord {
    NodeRecord {
        node_id,
        path: name.path.clone(),
        name: name.name.clone(),
        r#type: r#type.to_string(),
        incarnation,
        state,
        endpoint_id: crate::model::EndpointId("0".repeat(64)),
        transport_addr: "127.0.0.1:1".parse().expect("an address"),
        listeners: Vec::new(),
        runtime: rdm_core::RuntimeFact { deployment_id: "fixture-deployment".into(), provider: rdm_core::RuntimeProvider::Process, control_domain: "fixture".into(), locator: rdm_core::RuntimeLocator::Process { pid: 1, start: 1 } },
        is_primary: None,
        is_fabric_primary: None,
        cert: None,
        signing_cert: None,
        storage: Default::default(),
        executable: None,
        config: BTreeMap::new(),
        tags: BTreeSet::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT (Q366): a node record holds exactly Q366's fields: it decodes back to itself from its JSON, and
    /// a record with any other field is refused by name at decode. Must NOT happen: a line carrying a
    /// field outside the schema decoded as a record.
    #[test]
    fn a_record_decodes_with_exactly_its_fields_and_any_other_field_is_refused() {
        let name = PathName::new("mesh1.node", "1").unwrap();
        let record = fixture_record(&name, "node", NodeId::mint(), IncarnationId::mint(), crate::model::NodeStatus::Ready);
        let json = serde_json::to_value(&record).unwrap();
        let fields: Vec<&str> = json.as_object().unwrap().keys().map(String::as_str).collect();
        let mut expected = vec!["node_id", "path", "name", "type", "incarnation", "state", "endpoint_id", "transport_addr", "listeners", "runtime", "is_primary", "is_fabric_primary", "cert", "signing_cert", "storage", "executable", "config", "tags"];
        expected.sort();
        let mut fields_sorted = fields.clone();
        fields_sorted.sort();
        assert_eq!(fields_sorted, expected, "the record's fields are Q366's");
        assert_eq!(serde_json::from_value::<NodeRecord>(json.clone()).unwrap(), record);
        let mut extra = json;
        extra.as_object_mut().unwrap().insert("mesh".into(), serde_json::json!("mesh1"));
        let refused = serde_json::from_value::<NodeRecord>(extra).unwrap_err().to_string();
        assert!(refused.contains("unknown field `mesh`"), "the field is refused by name: {refused}");
    }

    /// CONTRACT: a node's mesh is the first location of its path.
    #[test]
    fn a_nodes_mesh_is_its_paths_first_location() {
        let record = fixture_record(&PathName::new("mesh2.broker", "1").unwrap(), "broker", NodeId::mint(), IncarnationId::mint(), crate::model::NodeStatus::Ready);
        assert_eq!(record.mesh(), "mesh2");
    }

    /// CONTRACT (node-identity-and-state.md §2; storage.md §2; Q365, Q366): the nodes store's line in
    /// the root's `nodes.jsonl` is frozen byte for byte: a CloudEvents event whose `source` is its
    /// writer, `subject` its node id, `type` `node-record`, `dataschema` `node-record/2`, and `data` the
    /// whole record; its `id` and `time` the write's. A layout that moves moves this cell with it
    /// (`durable format moved`). Must NOT happen: a line whose attributes or record bytes moved while
    /// this cell still passes.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn the_nodes_store_line_is_frozen_byte_for_byte() {
        let d = crate::record_store::tempdir("nodes-store-layout");
        let root = crate::store_root::StoreRoot::at(&d);
        let writer = crate::store_log::LogWriter::mint();
        let store = FileNodesStorage::open(&root, &writer).await.unwrap();
        let name = PathName::new("mesh1.node", "1").unwrap();
        let node_id = NodeId::parse(&rdm_core::fixture_crockford60("node-1")).unwrap();
        let incarnation = IncarnationId::parse(&rdm_core::fixture_crockford60("node-1-inc")).unwrap();
        let mut record = fixture_record(&name, "node", node_id.clone(), incarnation.clone(), crate::model::NodeStatus::Ready);
        store.put_node(&record).await.unwrap();
        store.finish_issued_writes().await;
        let file = d.join(crate::store_root::NODES_FILE);
        let lines = crate::store_log::read_file(&file).unwrap();
        assert_eq!(lines.len(), 1);
        let attributes = crate::store_log::Attributes { id: lines[0].attributes.id.clone(), source: writer.source(), r#type: "node-record".into(), subject: Some(node_id.to_string()), time: None, dataschema: "node-record/2".into(), traceparent: None, tracestate: None };
        let zero = "0".repeat(64);
        let data = format!(r#"{{"node_id":"{node_id}","path":"mesh1.node","name":"1","type":"node","incarnation":"{incarnation}","state":"ready","endpoint_id":"{zero}","transport_addr":"127.0.0.1:1","listeners":[],"runtime":{{"deployment_id":"fixture-deployment","provider":"process","control_domain":"fixture","locator":{{"process":{{"pid":1,"start":1}}}}}},"is_primary":null,"is_fabric_primary":null,"cert":null,"signing_cert":null,"storage":{{"persistence":null,"data_dir":null}},"executable":null,"config":{{}},"tags":[]}}"#);
        assert_eq!(String::from_utf8(std::fs::read(&file).unwrap()).unwrap(), String::from_utf8(crate::store_log::encode_line(&attributes, data.as_bytes())).unwrap());
        assert_eq!(lines[0].attributes.source_node_id(), Some(writer.node_id.as_str()), "the source names the writer's node id");
    }

    /// CONTRACT (Q365, Q366: `dataschema` is the record's kind and schema version): a line of a schema
    /// written before the one node record, `node-properties/1` here, is refused by name when the nodes
    /// store is read, naming the dataschema and the one this build reads (`durable format moved`). Must
    /// NOT happen: the old line decoded as a node record with its fields dropped.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn a_line_of_a_schema_before_the_node_record_is_refused_by_name() {
        let d = crate::record_store::tempdir("nodes-store-properties-1");
        let root = crate::store_root::StoreRoot::at(&d);
        let writer = crate::store_log::LogWriter::mint();
        let node_id = NodeId::parse(&rdm_core::fixture_crockford60("node-1")).unwrap();
        let incarnation_id = IncarnationId::parse(&rdm_core::fixture_crockford60("node-1-inc")).unwrap();
        let old = serde_json::json!({"birth": {"node_id": node_id, "incarnation_id": incarnation_id}, "config": {}, "tags": [], "endpoint_id": null, "transport_addr": null, "listeners": []});
        let raw = crate::store_log::StoreLog::open(&d.join(crate::store_root::NODES_FILE), crate::store_log::Formats::any()).await.unwrap();
        raw.append(&writer, "node-properties", "node-properties/1", node_id.as_str(), &old).await.unwrap();
        drop(raw);
        let read = match FileNodesStorage::open(&root, &writer).await {
            Ok(store) => store.node(&node_id).await.map(|_| ()),
            Err(e) => Err(e),
        };
        let why = read.expect_err("a node-properties/1 line is refused").to_string();
        assert!(why.contains("\"node-properties/1\"") && why.contains(RECORD_FORMAT), "refused naming the old dataschema and the one this build reads: {why}");
    }
}
