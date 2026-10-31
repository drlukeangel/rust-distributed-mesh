//! Node-owned properties and lifecycle records selected by the writing primary's birth (i150).
//!
//! The store persists records at their explicit keys; it does not elect a primary or infer that
//! one record supersedes another. Callers resolve the current primary before reading lifecycle.

use crate::storage_error::StorageError;
use async_trait::async_trait;
use rdm_core::{IncarnationId, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The identity already assigned to a running node, also identifying a lifecycle writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordBirth {
    /// The node instance.
    pub node_id: NodeId,
    /// Its runtime incarnation.
    pub incarnation_id: IncarnationId,
}

/// Properties written by the node itself, separate from its primary's lifecycle decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeProperties {
    /// The node writing this record; the durable key is its node ID.
    pub birth: RecordBirth,
    /// The application's complete configuration map.
    pub config: BTreeMap<String, String>,
    /// The node's current tag set.
    pub tags: BTreeSet<String>,
    /// The bound fabric endpoint.
    pub endpoint_id: Option<crate::model::EndpointId>,
    /// The actual fabric transport address.
    pub transport_addr: Option<std::net::SocketAddr>,
    /// Actual non-Iroh listener addresses.
    pub listeners: Vec<(String, std::net::SocketAddr)>,
    /// The application types the node declared at join.
    pub app_types: Option<crate::nodes_storage::AppTypes>,
}

/// Lifecycle state for one subject, written only under its writer's own birth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeLifecycle {
    /// The subject instance.
    pub node_id: NodeId,
    /// The exact primary that authored this record; part of its durable key.
    pub writer: RecordBirth,
    /// The subject's path, independent of its mesh and node type.
    pub path: String,
    /// The subject's name within that path.
    pub name: String,
    /// The node type, never inferred from path or name.
    pub node_type: rdm_core::NodeKind,
    /// The fabric to which the node belongs.
    pub fabric_id: rdm_core::FabricId,
    /// The mesh to which the node belongs.
    pub mesh_id: rdm_core::MeshId,
    /// The subject incarnation.
    pub incarnation_id: IncarnationId,
    /// The primary's lifecycle status for the subject.
    pub status: crate::model::NodeStatus,
    /// Self-declarations accepted by the lifecycle workflow.
    pub declared: BTreeSet<rdm_core::status::NodeState>,
    /// Committed operation effects, preserving their exact Build identities.
    pub operations: Vec<crate::nodes_storage::NodeOperation>,
    /// Observer-inferred silence, never proof of exit.
    pub silence: Option<crate::model::NodeStatus>,
    /// An inherited data directory is absent.
    pub data_dir_lost: Option<crate::nodes_storage::DataDirLost>,
    /// An inherited data directory cannot be read.
    pub data_dir_unreadable: Option<crate::nodes_storage::DataDirUnreadable>,
    /// Issued member certificate dates.
    pub cert: Option<crate::nodes_storage::CertDates>,
    /// Issued admin signing certificate dates.
    pub signing_cert: Option<crate::nodes_storage::SigningCertDates>,
    /// The election's node-type primary result; this is a flag, not a counter.
    pub is_primary: bool,
    /// The election's fabric-primary result; this is a flag, not a counter.
    pub is_fabric_primary: bool,
    /// The executable selected by the Build or retained for recovery.
    pub executable: Option<rdm_core::binding::Binding>,
    /// The node's data retention policy.
    pub storage_persistence: Option<rdm_core::meta::StorageMeta>,
    /// The placement selected by the Build.
    pub placement: Option<rdm_core::meta::PlacementMeta>,
    /// The registered topology backend used by the node.
    pub topology_storage_provider: Option<String>,
    /// Prerequisites allocated at creation, before runtime launch.
    pub allocated: Option<crate::nodes_storage::Allocation>,
    /// The previous birth named by recovery.
    pub predecessor: Option<RecordBirth>,
    /// The path/name pair moved by a rename, if any.
    pub renamed_from: Option<(String, String)>,
    /// The successor that replaced this instance.
    pub inherited_by: Option<NodeId>,
}

/// The common persistence boundary for node properties and lifecycle records.
///
/// Successful writes are durable. Implementations return the requested exact key; they never
/// select a primary, substitute an earlier writer, or treat an I/O error as absence.
#[async_trait]
pub trait NodeRecordsStorage: Send + Sync {
    /// Persist the node's complete property record at its node ID.
    async fn put_properties(&self, record: &NodeProperties) -> Result<(), StorageError>;
    /// Read the property record at an exact node ID.
    async fn properties(&self, node: &NodeId) -> Result<Option<NodeProperties>, StorageError>;
    /// Persist lifecycle state at `(subject ID, writer ID, writer incarnation)`.
    async fn put_lifecycle(&self, record: &NodeLifecycle) -> Result<(), StorageError>;
    /// Read only the lifecycle record of the selected primary's birth.
    async fn lifecycle(
        &self,
        node: &NodeId,
        writer: &RecordBirth,
    ) -> Result<Option<NodeLifecycle>, StorageError>;
    /// List all stored lifecycle records for a mesh, including predecessor writers.
    /// Selection of the current writer remains the reader's responsibility.
    async fn lifecycles(&self, mesh: &rdm_core::MeshId)
        -> Result<Vec<NodeLifecycle>, StorageError>;
}

/// In-memory adapter for fixtures and backends without restart survival.
#[derive(Default)]
pub struct MemoryNodeRecords {
    properties: std::sync::Mutex<BTreeMap<NodeId, NodeProperties>>,
    lifecycle: std::sync::Mutex<BTreeMap<(NodeId, NodeId, String), NodeLifecycle>>,
}

#[async_trait]
impl NodeRecordsStorage for MemoryNodeRecords {
    async fn put_properties(&self, record: &NodeProperties) -> Result<(), StorageError> {
        self.properties
            .lock()
            .unwrap()
            .insert(record.birth.node_id.clone(), record.clone());
        Ok(())
    }
    async fn properties(&self, node: &NodeId) -> Result<Option<NodeProperties>, StorageError> {
        Ok(self.properties.lock().unwrap().get(node).cloned())
    }
    async fn put_lifecycle(&self, record: &NodeLifecycle) -> Result<(), StorageError> {
        self.lifecycle.lock().unwrap().insert(
            (
                record.node_id.clone(),
                record.writer.node_id.clone(),
                record.writer.incarnation_id.to_string(),
            ),
            record.clone(),
        );
        Ok(())
    }
    async fn lifecycle(
        &self,
        node: &NodeId,
        writer: &RecordBirth,
    ) -> Result<Option<NodeLifecycle>, StorageError> {
        Ok(self
            .lifecycle
            .lock()
            .unwrap()
            .get(&(
                node.clone(),
                writer.node_id.clone(),
                writer.incarnation_id.to_string(),
            ))
            .cloned())
    }
    async fn lifecycles(
        &self,
        mesh: &rdm_core::MeshId,
    ) -> Result<Vec<NodeLifecycle>, StorageError> {
        Ok(self
            .lifecycle
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.mesh_id == *mesh)
            .cloned()
            .collect())
    }
}

/// The shared log containing the two record kinds.
#[cfg(feature = "file-backend")]
pub const NODE_RECORDS_FILE: &str = "node-records.jsonl";

#[cfg(feature = "file-backend")]
const PROPERTIES_FORMAT: &str = "node-properties/1";
#[cfg(feature = "file-backend")]
const LIFECYCLE_FORMAT: &str = "node-lifecycle/1";

/// File adapter using the existing checksummed, durable store log.
#[cfg(feature = "file-backend")]
pub struct FileNodeRecords {
    log: crate::store_log::StoreLog,
}

#[cfg(feature = "file-backend")]
impl FileNodeRecords {
    /// Open without reading the log; reads and writes fold it at their own boundary.
    pub async fn open(root: &crate::store_root::StoreRoot) -> Result<Self, StorageError> {
        let formats = crate::store_log::Formats::new()
            .with::<NodeProperties>(PROPERTIES_FORMAT)
            .with::<NodeLifecycle>(LIFECYCLE_FORMAT);
        Ok(Self {
            log: root.log(NODE_RECORDS_FILE, formats).await?,
        })
    }

    fn key(node: &NodeId, writer: &RecordBirth) -> String {
        format!("{}/{}/{}", node, writer.node_id, writer.incarnation_id)
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl NodeRecordsStorage for FileNodeRecords {
    async fn put_properties(&self, record: &NodeProperties) -> Result<(), StorageError> {
        let writer =
            crate::store_log::LogWriter::new(&record.birth.node_id, &record.birth.incarnation_id);
        self.log
            .append(
                &writer,
                PROPERTIES_FORMAT,
                record.birth.node_id.as_str(),
                record,
            )
            .await
    }
    async fn properties(&self, node: &NodeId) -> Result<Option<NodeProperties>, StorageError> {
        self.log
            .latest("node-properties", node.as_str())
            .await?
            .map(|l| l.decode(self.log.path()))
            .transpose()
    }
    async fn put_lifecycle(&self, record: &NodeLifecycle) -> Result<(), StorageError> {
        let writer =
            crate::store_log::LogWriter::new(&record.writer.node_id, &record.writer.incarnation_id);
        self.log
            .append(
                &writer,
                LIFECYCLE_FORMAT,
                &Self::key(&record.node_id, &record.writer),
                record,
            )
            .await
    }
    async fn lifecycle(
        &self,
        node: &NodeId,
        writer: &RecordBirth,
    ) -> Result<Option<NodeLifecycle>, StorageError> {
        self.log
            .latest("node-lifecycle", &Self::key(node, writer))
            .await?
            .map(|l| l.decode(self.log.path()))
            .transpose()
    }
    async fn lifecycles(
        &self,
        mesh: &rdm_core::MeshId,
    ) -> Result<Vec<NodeLifecycle>, StorageError> {
        self.log
            .latest_of("node-lifecycle")
            .await?
            .into_iter()
            .map(|l| l.decode::<NodeLifecycle>(self.log.path()))
            .filter_map(|r| match r {
                Ok(r) if r.mesh_id != *mesh => None,
                other => Some(other),
            })
            .collect()
    }
}
