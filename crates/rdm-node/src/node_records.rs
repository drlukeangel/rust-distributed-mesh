//! The node's record (Q366), written by the node itself (Q356): every field of it, one whole line at
//! its node id in the nodes store ([`crate::nodes_storage::NodesStorage::put_node`]). The store persists
//! lines at their explicit keys; it decides nothing.

use crate::nodes_storage::NodeRecord;
use crate::storage_error::StorageError;
use std::collections::BTreeSet;

/// One running node's serialized property write path. All its property commands use this
/// instance. The task owning the lock also owns the provider call: cancelling a caller never
/// releases write ordering while that call can still commit.
#[derive(Clone)]
pub struct NodePropertyWriter {
    records: std::sync::Arc<dyn crate::nodes_storage::NodesStorage>,
    /// The registered name of the topology storage provider the node writes now (storage.md §2), as a
    /// write's span names its backend.
    provider: std::sync::Arc<dyn Fn() -> String + Send + Sync>,
    current: std::sync::Arc<tokio::sync::Mutex<Option<NodeRecord>>>,
    /// The writing node's current path and name, once its topology view holds them
    /// ([`Self::named_by`]): what each write's span names it by. Unset: the span names none.
    name: std::sync::Arc<std::sync::OnceLock<NameOf>>,
    /// The record this process's start wrote ([`Self::bound`]): what a later command starts from when
    /// the writer holds no record (after an unacknowledged write it reads the store first).
    start: std::sync::Arc<std::sync::OnceLock<NodeRecord>>,
    /// Whether this birth's own view holds it deleted ([`Self::deleted_by`]): a node told it is deleted
    /// stops writing (S1). Unset: never.
    deleted: std::sync::Arc<std::sync::OnceLock<DeletedOf>>,
    /// The node's own record as this process holds it ([`OwnRecord`]): what the node reads its own
    /// properties from (Q234), kept by each write this writer makes.
    own: std::sync::Arc<std::sync::Mutex<OwnRecord>>,
}

/// A node's own record as the node holds it (Q234, Q356): the config its writer last wrote, else the
/// one its hydration sync's row carried, and that whole row (`node.topology.get`'s `OwnRow`), kept
/// current by every write. No digest carries the config (Q203).
#[derive(Debug, Default)]
pub(crate) struct OwnRecord {
    /// The config held, once written or installed.
    pub(crate) config: Option<std::collections::BTreeMap<String, String>>,
    /// The whole row the last hydration sync carried; `None` until a sync carried it.
    pub(crate) row: Option<rdm_core::views::NodeView>,
}

impl OwnRecord {
    /// The hydration sync's `row`; its config is taken only while no config is held. Whether it was.
    pub(crate) fn install(&mut self, row: rdm_core::views::NodeView) -> bool {
        let first = self.take(row.config.clone());
        self.row = Some(row);
        self.show();
        first
    }

    /// A hydration sync's `config`, taken only while no config is held; whether it was.
    fn take(&mut self, config: std::collections::BTreeMap<String, String>) -> bool {
        let first = self.config.is_none();
        if first {
            self.config = Some(config);
        }
        first
    }

    /// `record`, written: its config and tags held, on the row too.
    pub(crate) fn written(&mut self, record: &NodeRecord) {
        self.config = Some(record.config.clone());
        if let Some(row) = self.row.as_mut() {
            row.tags = record.tags.clone();
        }
        self.show();
    }

    /// The config held, written to the row too.
    fn show(&mut self) {
        if let (Some(row), Some(config)) = (self.row.as_mut(), self.config.as_ref()) {
            row.config = config.clone();
        }
    }
}

#[cfg(test)]
mod own_record_tests {
    use super::OwnRecord;
    use std::collections::BTreeMap;

    fn map(v: &str) -> BTreeMap<String, String> {
        [("k".to_string(), v.to_string())].into()
    }

    /// CONTRACT: a hydration sync's map is installed only while no map is held. Must NOT happen: a
    /// delayed hydration sync overwriting the map already held.
    #[test]
    fn a_sync_map_installs_only_while_no_map_is_held() {
        let mut held = OwnRecord::default();
        assert!(held.take(map("joined")), "the first map installs");
        assert!(!held.take(map("old")), "a later sync's map installs nothing");
        assert_eq!(held.config, Some(map("joined")));
    }
}

/// Where a writer reads whether its own birth is held deleted.
type DeletedOf = std::sync::Arc<dyn Fn() -> bool + Send + Sync>;

/// Where a writer reads its node's current path and name from.
type NameOf = std::sync::Arc<dyn Fn() -> Option<rdm_core::PathName> + Send + Sync>;

/// The existing property writer's admission lock retained through a provider copy and move.
/// Releasing it invalidates the cached record while still excluding subsequent mutations.
pub struct PropertyFreeze(tokio::sync::OwnedMutexGuard<Option<NodeRecord>>);

impl Drop for PropertyFreeze {
    fn drop(&mut self) { *self.0 = None; }
}

/// The roles this birth holds as `primaries` names them: `is_primary` at the revision its type's primary
/// record in `mesh` names this birth (a node-admin's: the mesh-primary's), `is_fabric_primary` at the
/// fabric-primary record's; `None` where the view names another birth or none.
pub fn own_roles(primaries: &rdm_gossip::primary::PrimaryView, birth: &crate::nodes_storage::BirthRef, mesh: &str, r#type: &str) -> (Option<u64>, Option<u64>) {
    let mine = |p: Option<rdm_core::Primary>| p.filter(|p| p.node_id == birth.node_id && p.incarnation == birth.incarnation_id).map(|p| p.revision);
    (mine(primaries.node_type_primary(mesh, r#type)), mine(primaries.fabric()))
}

/// Record this birth's own roles on its own record (Q356; node-primary-update.md step 5): the roles
/// `primaries` names it ([`own_roles`]), written when they differ from `written`, what this birth last
/// wrote; a role given up is cleared the same way. The write records the role; it does not grant it. A
/// write the store refuses leaves `written` as it was, so the view's next change writes again. Nothing is
/// written before the start's record ([`NodePropertyWriter::bound`]).
pub async fn write_own_roles(writer: &NodePropertyWriter, primaries: &rdm_gossip::primary::PrimaryView, birth: &crate::nodes_storage::BirthRef, mesh: &str, r#type: &str, written: &mut (Option<u64>, Option<u64>)) {
    let roles = own_roles(primaries, birth, mesh, r#type);
    if roles == *written {
        return;
    }
    let Some(initial) = writer.start_record() else { return };
    if writer.update(initial, move |record| { record.is_primary = roles.0; record.is_fabric_primary = roles.1; }, |_| {}).await.is_ok() {
        *written = roles;
    }
}

/// A durable property effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyWritten {
    /// The complete durable record, including fields untouched by this command.
    pub record: NodeRecord,
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
    /// Create the single writer for a running node. Its start's [`Self::bound`] writes the part the
    /// start holds; a runtime command with no part held reads the node's own part first.
    /// `provider` answers the provider the node writes at each write.
    pub fn new(records: std::sync::Arc<dyn crate::nodes_storage::NodesStorage>, provider: impl Fn() -> String + Send + Sync + 'static) -> Self {
        Self {
            records,
            provider: std::sync::Arc::new(provider),
            current: Default::default(),
            name: Default::default(),
            start: Default::default(),
            deleted: Default::default(),
            own: Default::default(),
        }
    }

    /// Write nothing once `deleted` answers true: this birth's own view holds its birth deleted (S1;
    /// the deleted node's record is its mesh-primary's to write, node-identity-and-state.md §2.5). Each
    /// write refused for it is named on its span, `outcome = refused`. The first call stands.
    pub fn deleted_by(&self, deleted: impl Fn() -> bool + Send + Sync + 'static) {
        let _ = self.deleted.set(std::sync::Arc::new(deleted));
    }

    /// Name each write's span by `name`, the writing node's current path and name (a rename moves
    /// it), `None` while it holds none. The first call stands.
    pub fn named_by(&self, name: impl Fn() -> Option<rdm_core::PathName> + Send + Sync + 'static) {
        let _ = self.name.set(std::sync::Arc::new(name));
    }

    /// The record this process's start wrote, with the config this node holds ([`Self::config`]): what
    /// a command after the start hands the writer as its `initial` record. `None` before the start
    /// wrote one.
    pub fn start_record(&self) -> Option<NodeRecord> {
        let config = self.config().unwrap_or_default();
        self.start.get().cloned().map(|mut record| { record.config = config; record })
    }

    /// Hold `row` as this node's own whole row, the one its mesh-primary's hydration sync carried
    /// (Q234); its config is taken only while this node holds none yet.
    pub fn set_own_row(&self, row: rdm_core::views::NodeView) {
        self.own.lock().unwrap().install(row);
    }

    /// This node's own whole row as its last hydration sync carried it and its writes since kept it
    /// (Q234); `None` until a sync carried it.
    pub fn own_row(&self) -> Option<rdm_core::views::NodeView> {
        self.own.lock().unwrap().row.clone()
    }

    /// The config this node holds, as this writer last wrote it or its hydration sync installed it;
    /// `None` until either: the map the application reads.
    pub fn config(&self) -> Option<std::collections::BTreeMap<String, String>> {
        self.own.lock().unwrap().config.clone()
    }

    /// Close property admission and finish every earlier admitted put. The storage transition
    /// owns this guard until its exact change moves or rolls back.
    pub async fn freeze(&self) -> PropertyFreeze {
        PropertyFreeze(self.current.clone().lock_owned().await)
    }

    /// Write the node's record at this start: `initial` is the whole record as the start holds it (the
    /// hydration sync's own row, Q234, else the launch), its bound addresses, runtime, data dir and
    /// state this start's; the start reads nothing from the store (Q227). A record already written by
    /// this process keeps its config, tags, roles, cert dates and name under the start's facts.
    pub async fn bound(
        &self,
        initial: NodeRecord,
        publish: impl FnOnce(&NodeRecord) + Send + 'static,
    ) -> Result<PropertyWritten, StorageError> {
        let _ = self.start.set(initial.clone());
        let bound = initial.clone();
        self.write(
            initial,
            false,
            move |record| {
                record.incarnation = bound.incarnation;
                record.state = bound.state;
                record.endpoint_id = bound.endpoint_id;
                record.transport_addr = bound.transport_addr;
                record.listeners = bound.listeners;
                record.runtime = bound.runtime;
                record.storage.data_dir = bound.storage.data_dir;
                Ok(())
            },
            publish,
        )
        .await
    }

    /// Write `to` as the node's state ([`NodeRecord::set_state`]), the node's own record first, before
    /// the node reports it (Q356). A state the record already holds, or one its order refuses, writes
    /// nothing.
    pub async fn state(
        &self,
        initial: NodeRecord,
        to: crate::model::NodeStatus,
    ) -> Result<PropertyWritten, StorageError> {
        self.update(initial, move |record| { record.set_state(to); }, |_| {}).await
    }

    /// Write `to` as this node's status on its own record (Q356) under one `Send` (Q344: the only
    /// retry; a refusal by name is an answer, never sent again): the status the record holds once the
    /// store answers it, by the record's one status order, which is what the node's Digest and answers
    /// carry. `None` while this process holds no start record: nothing of its record is written yet. A
    /// refusal, or a `Send` spent, is the end, named on its `rdm.node.record.update.via-status` span;
    /// the record keeps its last status.
    pub async fn write_status(&self, to: crate::model::NodeStatus) -> Option<Result<crate::model::NodeStatus, rdm_transport::client::SendEnd<StorageError>>> {
        let initial = self.start_record()?;
        let span = tracing::info_span!("rdm.node.record.update.via-status", node_id = %initial.node_id, incarnation_id = %initial.incarnation.as_str(), status = ?to, outcome = tracing::field::Empty, held = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal");
        let send = rdm_transport::client::Send::default();
        let written = tracing::Instrument::instrument(send.run(StorageError::answered, || self.state(initial.clone(), to)), span.clone()).await;
        Some(match written {
            Ok(w) => {
                span.record("outcome", "written").record("held", tracing::field::debug(w.record.state));
                Ok(w.record.state)
            }
            Err(end) => {
                let outcome = match &end {
                    rdm_transport::client::SendEnd::Answered(_) => "refused",
                    rdm_transport::client::SendEnd::Spent { .. } => "send-spent",
                };
                span.record("outcome", outcome).record("detail", end.to_string().as_str());
                span.in_scope(|| tracing::info!(outcome, "the status is not written on this node's record: the record keeps its last status, and the Digest with it"));
                Err(end)
            }
        })
    }

    /// Apply the whole Build entry through the same serialized writer as local changes.
    /// Both property fields are one durable effect.
    pub async fn entry(
        &self,
        initial: NodeRecord,
        entry: rdm_core::properties::EntryProperties,
        publish: impl FnOnce(&NodeRecord) + Send + 'static,
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
        initial: NodeRecord,
        change: TagChange,
        publish: impl FnOnce(&NodeRecord) + Send + 'static,
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
                    let node_id = record.node_id.clone();
                    let incarnation = record.incarnation.clone();
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
        initial: NodeRecord,
        change: impl FnOnce(&mut NodeRecord) + Send + 'static,
        publish: impl FnOnce(&NodeRecord) + Send + 'static,
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
        initial: NodeRecord,
        change: impl FnOnce(&mut NodeRecord) -> Result<(), StorageError> + Send + 'static,
        publish: impl FnOnce(&NodeRecord) + Send + 'static,
    ) -> Result<PropertyWritten, StorageError> {
        self.write(initial, true, change, publish).await
    }

    /// [`Self::update_checked`]'s write. With no part held by this process, a runtime command
    /// (`read_retained`) first reads the node's own part at its node id (Q237: after an unacknowledged
    /// write, or before this start's [`Self::bound`]); the start's [`Self::bound`] takes `initial` as
    /// the retained part and reads nothing (Q227).
    async fn write(
        &self,
        initial: NodeRecord,
        read_retained: bool,
        change: impl FnOnce(&mut NodeRecord) -> Result<(), StorageError> + Send + 'static,
        publish: impl FnOnce(&NodeRecord) + Send + 'static,
    ) -> Result<PropertyWritten, StorageError> {
        use tracing::Instrument;
        let (records, current, own) = (self.records.clone(), self.current.clone(), self.own.clone());
        let backend = (self.provider)();
        let at = self.name.get().and_then(|name| name());
        let (path, name) = at.map(|at| (at.path, at.name)).unwrap_or_default();
        let (node_id, incarnation_id) = (initial.node_id.clone(), initial.incarnation.clone());
        let deleted = self.deleted.get().is_some_and(|deleted| deleted());
        let write = move || tracing::info_span!("rdm.node.record.update.via-properties", node_id = %node_id, incarnation_id = %incarnation_id, path = %path, name = %name, backend = %backend, outcome = tracing::field::Empty, cause = tracing::field::Empty, state = tracing::field::Empty, "otel.kind" = "internal");
        // Dropping JoinHandle detaches the admitted write; it does not cancel it. In
        // particular no timeout may allow a second command to overtake the first call.
        tokio::spawn(
            async move {
                if deleted {
                    let reason = format!("node {} is deleted in its own view: its record is its mesh-primary's to write", initial.node_id);
                    write().record("outcome", "refused").record("cause", reason.as_str());
                    return Err(StorageError::Refused { file: "node record".into(), reason });
                }
                let mut held = current.lock().await;
                if held.is_none() && read_retained {
                    *held = records.node(&initial.node_id).await.map(|line| line.map(|l| l.record)).inspect_err(|e| {
                        write().record("outcome", "failed").record("cause", e.to_string());
                    })?;
                }
                let mut next = held.as_ref().cloned().unwrap_or(initial);
                let write = write();
                change(&mut next).inspect_err(|e| {
                    write.record("outcome", "refused").record("cause", e.to_string());
                })?;
                // The state the line carries: a birth deleted by its mesh-primary that writes itself live
                // again is read from this field.
                write.record("state", tracing::field::debug(&next.state));
                if held.as_ref() == Some(&next) {
                    write.record("outcome", "unchanged");
                } else if let Err(error) = records.put_node(&next).instrument(write.clone()).await {
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
                own.lock().unwrap().written(&next);
                publish(&next);
                Ok(PropertyWritten { record: next })
            }
            .instrument(tracing::Span::current()),
        )
        .await
        .expect("property writer task panicked")
    }
}
