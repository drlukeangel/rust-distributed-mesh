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
    /// The entities the node declared it holds at join.
    pub held: BTreeSet<String>,
    /// Build operations already applied to this node. Persisted with their effect so a lost
    /// reply cannot repeat an old Build over a later node or caller update. Attempts are not
    /// part of this identity: another attempt is a repeat of the same operation.
    #[serde(default)]
    pub applied: BTreeSet<PropertyApplication>,
}

/// Existing Build and operation identities, scoped by the property record's node ID.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PropertyApplication {
    /// The Build applying the entry.
    pub build_id: String,
    /// The operation within that Build.
    pub operation: String,
}

/// One running node's serialized property write path. All its property commands use this
/// instance. The task owning the lock also owns the provider call: cancelling a caller never
/// releases write ordering while that call can still commit.
#[derive(Clone)]
pub struct NodePropertyWriter {
    records: std::sync::Arc<dyn NodeRecordsStorage>,
    current: std::sync::Arc<tokio::sync::Mutex<Option<NodeProperties>>>,
}

/// A durable property effect, or recognition of an earlier durable application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyWritten {
    /// The complete durable record, including fields untouched by this command.
    pub record: NodeProperties,
    /// The Build operation was already applied and its effect was not repeated.
    pub already_applied: bool,
}

/// A tag command applied at the node, alongside its other property changes.
#[derive(Debug, Clone)]
pub enum TagChange {
    /// Add the named labels.
    Create(BTreeSet<String>),
    /// Remove all labels or the named labels.
    Delete(rdm_core::status::TagScope),
}

impl NodePropertyWriter {
    /// Create the single writer for a running node. The first command reads its own retained
    /// record, or uses the launch/join properties when no record has been written yet.
    pub fn new(records: std::sync::Arc<dyn NodeRecordsStorage>) -> Self {
        Self {
            records,
            current: Default::default(),
        }
    }

    /// Publish the actual bound addresses and join declaration at this start, preserving
    /// retained config, tags and application recognition for the same node ID.
    pub async fn bound(
        &self,
        initial: NodeProperties,
        publish: impl FnOnce(&NodeProperties) + Send + 'static,
    ) -> Result<PropertyWritten, StorageError> {
        self.update(
            initial.clone(),
            None,
            move |record| {
                record.birth = initial.birth;
                record.endpoint_id = initial.endpoint_id;
                record.transport_addr = initial.transport_addr;
                record.listeners = initial.listeners;
                record.app_types = initial.app_types;
                record.held = initial.held;
            },
            publish,
        )
        .await
    }

    /// Decide tag deltas against the latest serialized property state. The label grammar and
    /// complete resulting set are checked before persistence; a refusal changes nothing.
    pub async fn tags(
        &self,
        initial: NodeProperties,
        change: TagChange,
        application: Option<PropertyApplication>,
        publish: impl FnOnce(&NodeProperties) + Send + 'static,
    ) -> Result<rdm_core::status::StatusReply, StorageError> {
        use rdm_core::status::{StatusReply, TagScope};
        let refusal = match &change {
            TagChange::Create(labels) => rdm_core::status::create_tags_refusal(labels),
            TagChange::Delete(scope) => scope.refusal(),
        };
        if let Some(reason) = refusal {
            return Ok(rdm_core::reply::status::bounded_rejected_tags(&reason));
        }
        let answer = std::sync::Arc::new(std::sync::Mutex::new(None));
        let result = answer.clone();
        let written = self
            .update_checked(
                initial,
                application,
                move |record| {
                    let node_id = record.birth.node_id.clone();
                    let incarnation = record.birth.incarnation_id.clone();
                    let reply = match change {
                        TagChange::Create(labels) => {
                            let already_carried =
                                labels.intersection(&record.tags).cloned().collect();
                            record.tags.extend(labels);
                            if let Some(reason) = rdm_core::status::tags_refusal(&record.tags) {
                                return Err(StorageError::Refused {
                                    file: "node.tags".into(),
                                    reason,
                                });
                            }
                            StatusReply::TagsCreated {
                                node_id,
                                incarnation,
                                already_carried,
                            }
                        }
                        TagChange::Delete(scope) => {
                            let not_carried = match scope {
                                TagScope::All => {
                                    record.tags.clear();
                                    BTreeSet::new()
                                }
                                TagScope::Labels(labels) => {
                                    let absent = labels.difference(&record.tags).cloned().collect();
                                    record.tags.retain(|tag| !labels.contains(tag));
                                    absent
                                }
                            };
                            StatusReply::TagsDeleted {
                                node_id,
                                incarnation,
                                not_carried,
                            }
                        }
                    };
                    *result.lock().unwrap() = Some(reply);
                    Ok(())
                },
                publish,
            )
            .await;
        match written {
            Ok(written) if written.already_applied => Ok(rdm_core::reply::already_applied()),
            Ok(_) => Ok(answer
                .lock()
                .unwrap()
                .take()
                .expect("the applied tag command answered")),
            Err(StorageError::Refused { file, reason }) if file == "node.tags" => {
                Ok(rdm_core::reply::status::bounded_rejected_tags(&reason))
            }
            Err(error) => Err(error),
        }
    }

    /// Apply a command to the latest owned properties. `publish` installs the durable result
    /// into the application's local view while still holding the same ordering lock. Neither
    /// the command's future nor its receiver owns the lifetime of a pending provider write.
    pub async fn update(
        &self,
        initial: NodeProperties,
        application: Option<PropertyApplication>,
        change: impl FnOnce(&mut NodeProperties) + Send + 'static,
        publish: impl FnOnce(&NodeProperties) + Send + 'static,
    ) -> Result<PropertyWritten, StorageError> {
        self.update_checked(
            initial,
            application,
            move |record| {
                change(record);
                Ok(())
            },
            publish,
        )
        .await
    }

    /// Validate and apply a property command against the same serialized current record.
    /// A refused edit writes nothing and leaves both the durable and local value unchanged.
    pub async fn update_checked(
        &self,
        initial: NodeProperties,
        application: Option<PropertyApplication>,
        change: impl FnOnce(&mut NodeProperties) -> Result<(), StorageError> + Send + 'static,
        publish: impl FnOnce(&NodeProperties) + Send + 'static,
    ) -> Result<PropertyWritten, StorageError> {
        use tracing::Instrument;
        let (records, current) = (self.records.clone(), self.current.clone());
        // Dropping JoinHandle detaches the admitted write; it does not cancel it. In
        // particular no timeout may allow a second command to overtake the first call.
        tokio::spawn(
            async move {
                let mut held = current.lock().await;
                if held.is_none() {
                    *held = records.properties(&initial.birth.node_id).await?;
                }
                let mut next = held.as_ref().cloned().unwrap_or(initial);
                let already_applied = application
                    .as_ref()
                    .is_some_and(|a| next.applied.contains(a));
                if !already_applied {
                    change(&mut next)?;
                    if let Some(application) = application {
                        next.applied.insert(application);
                    }
                    if held.as_ref() != Some(&next) {
                        if let Err(error) = records.put_properties(&next).await {
                            // An error may be a lost acknowledgement. The next admitted command must
                            // resolve the exact durable record, including application identities,
                            // before it can change anything. Never reuse a pre-write cached value.
                            *held = None;
                            return Err(error);
                        }
                    }
                    *held = Some(next.clone());
                }
                publish(&next);
                Ok(PropertyWritten {
                    record: next,
                    already_applied,
                })
            }
            .instrument(tracing::Span::current()),
        )
        .await
        .expect("property writer task panicked")
    }
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
///
/// A put owns the submitted backend operation until that operation has ended. On **every**
/// return, including an error, that issued operation can no longer commit in the future. An
/// error may have lost the acknowledgement of an already committed record, so readers still
/// resolve it by its exact key. A transport deadline with a pending backend mutation is not a
/// completed put: the adapter must retain the original operation until its outcome resolves.
/// Callers retain the future through cancellation; adapters retain any lower-level operation.
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
pub use crate::store_root::NODE_RECORDS_FILE;

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

    /// Wait until every append already issued through this file has ended.
    pub async fn finish_issued_writes(&self) {
        self.log.finish_issued_writes().await;
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
