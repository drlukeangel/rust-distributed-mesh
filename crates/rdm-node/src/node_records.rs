//! The property record each node writes itself (i150): its config, tags, bound addresses and join
//! declaration, at its node id. The store persists records at their explicit keys; it decides nothing.

use crate::storage_error::StorageError;
use async_trait::async_trait;
use rdm_core::{IncarnationId, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The identity already assigned to a running node: a birth.
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
}

/// One running node's serialized property write path. All its property commands use this
/// instance. The task owning the lock also owns the provider call: cancelling a caller never
/// releases write ordering while that call can still commit.
#[derive(Clone)]
pub struct NodePropertyWriter {
    records: std::sync::Arc<dyn NodeRecordsStorage>,
    current: std::sync::Arc<tokio::sync::Mutex<Option<NodeProperties>>>,
    /// The writing node's current path and name, once its topology view holds them
    /// ([`Self::named_by`]): what each write's span names it by. Unset: the span names none.
    name: std::sync::Arc<std::sync::OnceLock<NameOf>>,
}

/// Where a writer reads its node's current path and name from.
type NameOf = std::sync::Arc<dyn Fn() -> Option<rdm_core::PathName> + Send + Sync>;

/// The existing property writer's admission lock retained through a provider copy and move.
/// Releasing it invalidates the cached record while still excluding subsequent mutations.
pub struct PropertyFreeze(tokio::sync::OwnedMutexGuard<Option<NodeProperties>>);

impl Drop for PropertyFreeze {
    fn drop(&mut self) { *self.0 = None; }
}

/// A durable property effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyWritten {
    /// The complete durable record, including fields untouched by this command.
    pub record: NodeProperties,
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
            name: Default::default(),
        }
    }

    /// Name each write's span by `name`, the writing node's current path and name (a rename moves
    /// it), `None` while it holds none. The first call stands.
    pub fn named_by(&self, name: impl Fn() -> Option<rdm_core::PathName> + Send + Sync + 'static) {
        let _ = self.name.set(std::sync::Arc::new(name));
    }

    /// Close property admission and finish every earlier admitted put. The storage transition
    /// owns this guard until its exact change moves or rolls back.
    pub async fn freeze(&self) -> PropertyFreeze {
        PropertyFreeze(self.current.clone().lock_owned().await)
    }

    /// Publish the actual bound addresses and join declaration at this start, preserving
    /// retained config and tags for the same node ID.
    pub async fn bound(
        &self,
        initial: NodeProperties,
        publish: impl FnOnce(&NodeProperties) + Send + 'static,
    ) -> Result<PropertyWritten, StorageError> {
        self.update(
            initial.clone(),
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

    /// Apply the whole Build entry through the same serialized writer as local changes.
    /// Both property fields are one durable effect.
    pub async fn entry(
        &self,
        initial: NodeProperties,
        entry: rdm_core::properties::EntryProperties,
        publish: impl FnOnce(&NodeProperties) + Send + 'static,
    ) -> Result<PropertyWritten, StorageError> {
        self.update_checked(initial, move |record| {
            if let Some(reason) = rdm_core::status::tags_refusal(&entry.tags) {
                return Err(StorageError::Refused { file: "node.tags".into(), reason });
            }
            record.config = entry.config;
            record.tags = entry.tags;
            Ok(())
        }, publish).await
    }

    /// Decide tag deltas against the latest serialized property state. The label grammar and
    /// complete resulting set are checked before persistence; a refusal changes nothing.
    pub async fn tags(
        &self,
        initial: NodeProperties,
        change: TagChange,
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
        change: impl FnOnce(&mut NodeProperties) + Send + 'static,
        publish: impl FnOnce(&NodeProperties) + Send + 'static,
    ) -> Result<PropertyWritten, StorageError> {
        self.update_checked(
            initial,
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
    ///
    /// One span per command, a child of the caller's (i150 § OTLP span-assertion bar): a write is
    /// `rdm.node.record.update.via-properties` (the node's birth, path and name, the backend, and
    /// the outcome: `durable`, `unchanged`, `refused` or `failed`). It never carries the config map,
    /// a tag or any other value.
    pub async fn update_checked(
        &self,
        initial: NodeProperties,
        change: impl FnOnce(&mut NodeProperties) -> Result<(), StorageError> + Send + 'static,
        publish: impl FnOnce(&NodeProperties) + Send + 'static,
    ) -> Result<PropertyWritten, StorageError> {
        use tracing::Instrument;
        let (records, current) = (self.records.clone(), self.current.clone());
        let at = self.name.get().and_then(|name| name());
        let (path, name) = at.map(|at| (at.path, at.name)).unwrap_or_default();
        let (node_id, incarnation_id) = (initial.birth.node_id.clone(), initial.birth.incarnation_id.clone());
        let write = move |records: &dyn NodeRecordsStorage| tracing::info_span!("rdm.node.record.update.via-properties", node_id = %node_id, incarnation_id = %incarnation_id, path = %path, name = %name, backend = %records.provider(), outcome = tracing::field::Empty, cause = tracing::field::Empty, "otel.kind" = "internal");
        // Dropping JoinHandle detaches the admitted write; it does not cancel it. In
        // particular no timeout may allow a second command to overtake the first call.
        tokio::spawn(
            async move {
                let mut held = current.lock().await;
                if held.is_none() {
                    *held = records.properties(&initial.birth.node_id).await.inspect_err(|e| {
                        write(&*records).record("outcome", "failed").record("cause", e.to_string());
                    })?;
                }
                let mut next = held.as_ref().cloned().unwrap_or(initial);
                let write = write(&*records);
                change(&mut next).inspect_err(|e| {
                    write.record("outcome", "refused").record("cause", e.to_string());
                })?;
                if held.as_ref() == Some(&next) {
                    write.record("outcome", "unchanged");
                } else if let Err(error) = records.put_properties(&next).instrument(write.clone()).await {
                    // An error may be a lost acknowledgement. The next admitted command must
                    // resolve the exact durable record before it can change anything. Never
                    // reuse a pre-write cached value.
                    *held = None;
                    write.record("outcome", "failed").record("cause", error.to_string());
                    return Err(error);
                } else {
                    write.record("outcome", "durable");
                }
                *held = Some(next.clone());
                publish(&next);
                Ok(PropertyWritten { record: next })
            }
            .instrument(tracing::Span::current()),
        )
        .await
        .expect("property writer task panicked")
    }
}

/// The persistence boundary of the property records the nodes write.
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
    /// The registered name of the topology storage provider these records are persisted to
    /// (storage.md §2), as a write's span names its backend.
    fn provider(&self) -> String;
    /// Persist the node's complete property record at its node ID.
    async fn put_properties(&self, record: &NodeProperties) -> Result<(), StorageError>;
    /// Read the property record at an exact node ID.
    async fn properties(&self, node: &NodeId) -> Result<Option<NodeProperties>, StorageError>;
}

/// In-memory adapter for fixtures and backends without restart survival.
#[derive(Default)]
pub struct MemoryNodeRecords {
    properties: std::sync::Mutex<BTreeMap<NodeId, NodeProperties>>,
}

#[async_trait]
impl NodeRecordsStorage for MemoryNodeRecords {
    fn provider(&self) -> String {
        "memory".into()
    }
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
}

/// The log of the property records.
#[cfg(feature = "file-backend")]
pub use crate::store_root::NODE_RECORDS_FILE;

#[cfg(feature = "file-backend")]
const PROPERTIES_FORMAT: &str = "node-properties/1";

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
            .with::<NodeProperties>(PROPERTIES_FORMAT);
        Ok(Self {
            log: root.log(NODE_RECORDS_FILE, formats).await?,
        })
    }


    /// Wait until every append already issued through this file has ended.
    pub async fn finish_issued_writes(&self) {
        self.log.finish_issued_writes().await;
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl NodeRecordsStorage for FileNodeRecords {
    fn provider(&self) -> String {
        "file".into()
    }
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
}
