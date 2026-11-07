//! `events.storage` (build.md §5): what happened while executing each Build, and each node's
//! investigations. One append-only store of events, each its own record with one writer, never
//! updated and never gossiped.
//!
//! An event is a CloudEvents 1.0 envelope with gossip.md §3.4's attributes: `specversion` `1.0`;
//! `id` a time-ordered UUIDv7 (RFC 9562); `source` the writer's exact birth,
//! `rdm://<fabric_id>/<node_id>/<incarnation>`; `type`; `subject` the node.id the event is about,
//! when it is about one; `time` rafka-time; `datacontenttype` `application/json`; `traceparent` and
//! `tracestate` from the writer's span; `data` a JSON object.
//!
//! The store answers four reads: by event.id, the events of one Build (by `data.build_id`), the
//! events of one node (by `subject`) and the storage changes' events (`type` `rdm.storage.*`), each in
//! event.id order. The file backend is the root's `events.jsonl`, one whole event per line; its reads
//! fold three ordered maps, `(build.id, event.id)`, `(node.id, event.id)` and the storage changes'
//! events by event.id, and answer each read as one of them or a range of one.

use rdm_core::status::MeshState;
use rdm_core::{BuildId, ChangeId, FabricId, IncarnationId, MeshId, NodeId};
use crate::storage_error::StorageError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Mutex;

/// `rdm.build.started`: the fabric-primary started a Build. `data` {build_id}.
pub const BUILD_STARTED: &str = "rdm.build.started";
/// `rdm.build.dispatched`: the fabric-primary dispatched a mesh's section of a Build. `data` {build_id, mesh}.
pub const BUILD_DISPATCHED: &str = "rdm.build.dispatched";
/// `rdm.build.step`: a mesh's mesh-primary ended a step of a Build on the node in `subject`. `data`
/// {build_id, mesh, step, op, state}.
pub const BUILD_STEP: &str = "rdm.build.step";
/// `rdm.build.ended`: the fabric-primary ended a Build. `data` {build_id, state}.
pub const BUILD_ENDED: &str = "rdm.build.ended";
/// `rdm.node.investigated`: the primary above a node investigated it; `subject` the node.id. `data`
/// {incarnation, path, name, verdict, next}.
pub const NODE_INVESTIGATED: &str = "rdm.node.investigated";
/// `rdm.storage.started`: the fabric-primary started a storage change. `data` {change_id, topology_storage_provider}.
pub const STORAGE_STARTED: &str = "rdm.storage.started";
/// `rdm.storage.committed`: the fabric-primary switched a storage change to its new provider. `data`
/// {change_id, topology_storage_provider}.
pub const STORAGE_COMMITTED: &str = "rdm.storage.committed";
/// `rdm.storage.ended`: the fabric-primary ended a storage change. `data` {change_id, topology_storage_provider, state}.
pub const STORAGE_ENDED: &str = "rdm.storage.ended";

/// `rdm.mesh.pending`: a mesh's record was written pending. `data` {mesh_id, mesh}.
pub const MESH_PENDING: &str = "rdm.mesh.pending";
/// `rdm.mesh.ready`: a mesh's record moved to ready. `data` {mesh_id, mesh}.
pub const MESH_READY: &str = "rdm.mesh.ready";
/// `rdm.mesh.deleting`: a mesh's record moved to deleting. `data` {mesh_id, mesh}.
pub const MESH_DELETING: &str = "rdm.mesh.deleting";
/// `rdm.mesh.deleted`: a mesh's record moved to deleted. `data` {mesh_id, mesh}.
pub const MESH_DELETED: &str = "rdm.mesh.deleted";
/// `rdm.fabric.pending`: the Fabric record was written pending. `data` {fabric_id}.
pub const FABRIC_PENDING: &str = "rdm.fabric.pending";
/// `rdm.fabric.ready`: the Fabric record moved to ready. `data` {fabric_id}.
pub const FABRIC_READY: &str = "rdm.fabric.ready";

/// The event types this build writes.
pub const TYPES: [&str; 14] = [BUILD_STARTED, BUILD_DISPATCHED, BUILD_STEP, BUILD_ENDED, NODE_INVESTIGATED, STORAGE_STARTED, STORAGE_COMMITTED, STORAGE_ENDED, MESH_PENDING, MESH_READY, MESH_DELETING, MESH_DELETED, FABRIC_PENDING, FABRIC_READY];

/// The CloudEvents version every event carries.
pub const SPEC_VERSION: &str = "1.0";
/// The content type of every event's `data`.
pub const DATA_CONTENT_TYPE: &str = "application/json";

/// An event's identity, a UUIDv7: every store line's `id` (Q365).
pub use rdm_core::EventId;

/// The writer of an event: its exact birth, the event's `source`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSource {
    /// The fabric.
    pub fabric_id: FabricId,
    /// The writer's node id.
    pub node_id: NodeId,
    /// The writer's incarnation.
    pub incarnation: IncarnationId,
}

impl EventSource {
    /// `rdm://<fabric_id>/<node_id>/<incarnation>`.
    pub fn uri(&self) -> String {
        format!("rdm://{}/{}/{}", self.fabric_id, self.node_id, self.incarnation)
    }
}

/// How a Build's step, or a storage change, ended: `complete`, or `failed` with its reason and code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StepState {
    /// It completed.
    ///
    /// Serialized as `complete`.
    Complete,
    /// It failed.
    ///
    /// Serialized as `failed`.
    Failed {
        /// Why.
        reason: String,
        /// The gRPC code name of the failure.
        code: String,
    },
}

/// How a Build ended (Q346): `complete`; `incomplete`, naming each section it did not finish; or
/// `failed`, why it could not start.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EndedState {
    /// Every section finished.
    ///
    /// Serialized as `complete`.
    Complete,
    /// The Build ran and not every section finished; what ran stands.
    ///
    /// Serialized as `incomplete`.
    Incomplete {
        /// Each section that ended short of its end.
        sections: Vec<EndedSection>,
    },
    /// The Build could not start.
    ///
    /// Serialized as `failed`.
    Failed {
        /// Why.
        reason: String,
        /// The gRPC code name.
        code: String,
    },
}

/// One unfinished section of an `incomplete` Build, as `rdm.build.ended` names it (build.md §5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndedSection {
    /// The mesh.
    pub mesh: String,
    /// The operation the section stopped at.
    pub op: String,
    /// The step it stopped at.
    pub step: String,
    /// Why.
    pub reason: String,
    /// The gRPC code name.
    pub code: String,
}

/// One event: a CloudEvents 1.0 envelope (gossip.md §3.4's attributes) in its JSON form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// `1.0`.
    pub specversion: String,
    /// A time-ordered UUIDv7.
    pub id: EventId,
    /// The writer's exact birth, `rdm://<fabric_id>/<node_id>/<incarnation>`.
    pub source: String,
    /// One of [`TYPES`].
    pub r#type: String,
    /// The node.id the event is about, when it is about one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<NodeId>,
    /// Rafka-time of the write, RFC 3339 in UTC milliseconds.
    pub time: String,
    /// `application/json`.
    pub datacontenttype: String,
    /// The writer's W3C trace parent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// The writer's W3C trace state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,
    /// The event's data: a JSON object.
    pub data: serde_json::Map<String, serde_json::Value>,
}

/// `ms` of rafka-time as RFC 3339 in UTC milliseconds.
fn rfc3339(ms: u64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms as i64).unwrap_or_default().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// `data` as the JSON object it serialises to.
fn object(data: impl Serialize) -> serde_json::Map<String, serde_json::Value> {
    match serde_json::to_value(data) {
        Ok(serde_json::Value::Object(m)) => m,
        _ => serde_json::Map::new(),
    }
}

#[cfg(feature = "file-backend")]
impl Event {
    /// The event's line attributes in the events store: its own, `dataschema` [`EVENT_FORMAT`].
    pub fn attributes(&self) -> crate::store_log::Attributes {
        crate::store_log::Attributes { id: self.id.clone(), source: self.source.clone(), r#type: self.r#type.clone(), subject: self.subject.as_ref().map(|n| n.to_string()), time: Some(self.time.clone()), dataschema: EVENT_FORMAT.into(), traceparent: self.traceparent.clone(), tracestate: self.tracestate.clone() }
    }

    /// The event a line of the events store is; `None` for a line that is not one.
    pub fn of_line(line: &crate::store_log::Line) -> Option<Self> {
        let a = &line.attributes;
        let subject = match &a.subject {
            Some(s) => Some(NodeId::parse(s).ok()?),
            None => None,
        };
        Some(Self { specversion: SPEC_VERSION.into(), id: a.id.clone(), source: a.source.clone(), r#type: a.r#type.clone(), subject, time: a.time.clone()?, datacontenttype: DATA_CONTENT_TYPE.into(), traceparent: a.traceparent.clone(), tracestate: a.tracestate.clone(), data: line.data.as_object()?.clone() })
    }
}

impl Event {
    /// An event of `kind` by `source` at rafka-time `time_ms` about `subject`, with `data`, its id
    /// minted and its trace context the writer's current span's.
    pub fn new(source: &EventSource, time_ms: u64, kind: &str, subject: Option<NodeId>, data: serde_json::Map<String, serde_json::Value>) -> Self {
        Self {
            specversion: SPEC_VERSION.into(),
            id: EventId::mint(),
            source: source.uri(),
            r#type: kind.into(),
            subject,
            time: rfc3339(time_ms),
            datacontenttype: DATA_CONTENT_TYPE.into(),
            traceparent: rdm_transport::current_traceparent(),
            tracestate: rdm_transport::current_tracestate(),
            data,
        }
    }

    /// `rdm.build.started` {build_id}.
    pub fn build_started(source: &EventSource, time_ms: u64, build_id: &BuildId) -> Self {
        #[derive(Serialize)]
        struct Data<'a> {
            build_id: &'a BuildId,
        }
        Self::new(source, time_ms, BUILD_STARTED, None, object(Data { build_id }))
    }

    /// `rdm.build.dispatched` {build_id, mesh}.
    pub fn build_dispatched(source: &EventSource, time_ms: u64, build_id: &BuildId, mesh: &str) -> Self {
        #[derive(Serialize)]
        struct Data<'a> {
            build_id: &'a BuildId,
            mesh: &'a str,
        }
        Self::new(source, time_ms, BUILD_DISPATCHED, None, object(Data { build_id, mesh }))
    }

    /// `rdm.build.step` on the node `node_id` acted on, none for a mesh or fabric operation {build_id,
    /// mesh, step, op, state}.
    #[allow(clippy::too_many_arguments)]
    pub fn build_step(source: &EventSource, time_ms: u64, node_id: Option<&NodeId>, build_id: &BuildId, mesh: &str, step: &str, op: &str, state: &StepState) -> Self {
        #[derive(Serialize)]
        struct Data<'a> {
            build_id: &'a BuildId,
            mesh: &'a str,
            step: &'a str,
            op: &'a str,
            state: &'a StepState,
        }
        Self::new(source, time_ms, BUILD_STEP, node_id.cloned(), object(Data { build_id, mesh, step, op, state }))
    }

    /// `rdm.build.ended` {build_id, state}.
    pub fn build_ended(source: &EventSource, time_ms: u64, build_id: &BuildId, state: &EndedState) -> Self {
        #[derive(Serialize)]
        struct Data<'a> {
            build_id: &'a BuildId,
            state: &'a EndedState,
        }
        Self::new(source, time_ms, BUILD_ENDED, None, object(Data { build_id, state }))
    }

    /// `rdm.node.investigated` on the node `node_id` {incarnation, path, name, verdict, next, reason,
    /// trigger}: `reason` the investigation's, `self-heal` or `ordered`, and `trigger` what started it,
    /// `silence` for a self-heal (Q393; [`crate::node_investigation::Reason`]).
    #[allow(clippy::too_many_arguments)]
    pub fn node_investigated(source: &EventSource, time_ms: u64, node_id: &NodeId, incarnation: &IncarnationId, path: &str, name: &str, verdict: &str, next: &str, reason: crate::node_investigation::Reason) -> Self {
        #[derive(Serialize)]
        struct Data<'a> {
            incarnation: &'a IncarnationId,
            path: &'a str,
            name: &'a str,
            verdict: &'a str,
            next: &'a str,
            reason: &'a str,
            trigger: &'a str,
        }
        Self::new(source, time_ms, NODE_INVESTIGATED, Some(node_id.clone()), object(Data { incarnation, path, name, verdict, next, reason: reason.as_str(), trigger: reason.trigger() }))
    }

    /// `rdm.storage.started` {change_id, topology_storage_provider}: the change's start, appended to the
    /// old provider's events store before its first command (storage.md §2 "Changing it");
    /// `topology_storage_provider` the provider the change names.
    pub fn storage_started(source: &EventSource, time_ms: u64, change_id: &ChangeId, topology_storage_provider: &str) -> Self {
        Self::new(source, time_ms, STORAGE_STARTED, None, object(StorageData { change_id, topology_storage_provider, state: None }))
    }

    /// `rdm.storage.committed` {change_id, topology_storage_provider}: the change's commit, appended to
    /// the new provider's events store (storage.md §2 step 3).
    pub fn storage_committed(source: &EventSource, time_ms: u64, change_id: &ChangeId, topology_storage_provider: &str) -> Self {
        Self::new(source, time_ms, STORAGE_COMMITTED, None, object(StorageData { change_id, topology_storage_provider, state: None }))
    }

    /// Whether this is the `rdm.storage.committed` event of the change `change_id`.
    pub fn commits(&self, change_id: &ChangeId) -> bool {
        self.r#type == STORAGE_COMMITTED && self.change_id().as_ref() == Some(change_id)
    }

    /// `rdm.storage.ended` {change_id, topology_storage_provider, state}: the change's end, appended to
    /// the events store of the provider it decided; `topology_storage_provider` that provider.
    pub fn storage_ended(source: &EventSource, time_ms: u64, change_id: &ChangeId, topology_storage_provider: &str, state: &StepState) -> Self {
        Self::new(source, time_ms, STORAGE_ENDED, None, object(StorageData { change_id, topology_storage_provider, state: Some(state) }))
    }

    /// The storage change the event is of: its `data.change_id`, on an `rdm.storage.*` event.
    pub fn change_id(&self) -> Option<ChangeId> {
        if !self.is_storage() {
            return None;
        }
        self.data.get("change_id").and_then(|v| v.as_str()).and_then(|s| ChangeId::parse(s).ok())
    }

    /// The event's `data.topology_storage_provider`, on an `rdm.storage.*` event.
    pub fn topology_storage_provider(&self) -> Option<&str> {
        self.data.get("topology_storage_provider").and_then(|v| v.as_str())
    }

    /// Whether the event is one of a storage change's three.
    pub fn is_storage(&self) -> bool {
        matches!(self.r#type.as_str(), STORAGE_STARTED | STORAGE_COMMITTED | STORAGE_ENDED)
    }

    /// The mesh's move to `state`: `rdm.mesh.pending`, `rdm.mesh.ready`, `rdm.mesh.deleting` or
    /// `rdm.mesh.deleted` {mesh_id, mesh}.
    pub fn mesh_moved(source: &EventSource, time_ms: u64, mesh_id: &MeshId, mesh: &str, state: MeshState) -> Self {
        #[derive(Serialize)]
        struct Data<'a> {
            mesh_id: &'a MeshId,
            mesh: &'a str,
        }
        let kind = match state {
            MeshState::Pending => MESH_PENDING,
            MeshState::Ready => MESH_READY,
            MeshState::Deleting => MESH_DELETING,
            MeshState::Deleted => MESH_DELETED,
        };
        Self::new(source, time_ms, kind, None, object(Data { mesh_id, mesh }))
    }

    /// The Fabric's move to `state`: `rdm.fabric.pending` or `rdm.fabric.ready` {fabric_id}.
    pub fn fabric_moved(source: &EventSource, time_ms: u64, fabric_id: &FabricId, state: crate::fabric_state::FabricState) -> Self {
        #[derive(Serialize)]
        struct Data<'a> {
            fabric_id: &'a FabricId,
        }
        let kind = match state {
            crate::fabric_state::FabricState::Pending => FABRIC_PENDING,
            crate::fabric_state::FabricState::Ready => FABRIC_READY,
        };
        Self::new(source, time_ms, kind, None, object(Data { fabric_id }))
    }

    /// The Build the event is of: its `data.build_id`.
    pub fn build_id(&self) -> Option<BuildId> {
        self.data.get("build_id").and_then(|v| v.as_str()).and_then(|s| BuildId::parse(s).ok())
    }
}

/// The `data` of the storage change's three events.
#[derive(Serialize)]
struct StorageData<'a> {
    change_id: &'a ChangeId,
    topology_storage_provider: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<&'a StepState>,
}

/// What one writer appends its events with: the events store, its exact birth (each event's
/// `source`) and its rafka-time (each event's `time`).
#[derive(Clone)]
pub struct EventsWriter {
    /// `events.storage`.
    pub events: std::sync::Arc<dyn EventsStorage>,
    /// The writer's exact birth: each event's `source`.
    pub source: EventSource,
    /// The writer's rafka-time: each event's `time`.
    pub time: rdm_transport::clock::RafkaTime,
}

impl std::fmt::Debug for EventsWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventsWriter").field("source", &self.source.uri()).finish()
    }
}

impl EventsWriter {
    /// A writer on an events store held in memory, by the birth `node_id` of `fabric_id`, at
    /// rafka-time `1`: what a rig with no topology storage writes its events with.
    pub fn in_memory(fabric_id: &FabricId, node_id: &NodeId) -> Self {
        Self::on(std::sync::Arc::new(MemoryEventsStorage::new()), fabric_id, node_id)
    }

    /// A writer on `events`, by the birth `node_id` of `fabric_id`, at rafka-time `1`.
    pub fn on(events: std::sync::Arc<dyn EventsStorage>, fabric_id: &FabricId, node_id: &NodeId) -> Self {
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(1);
        Self { events, source: EventSource { fabric_id: fabric_id.clone(), node_id: node_id.clone(), incarnation: IncarnationId::mint() }, time }
    }

    /// Append the event `make` builds from this writer's birth and rafka-time; its id, or why none
    /// was written.
    pub async fn append(&self, make: impl FnOnce(&EventSource, u64) -> Event) -> Result<EventId, String> {
        let time_ms = self.time.try_now_ms().ok_or_else(|| "this writer holds no rafka-time".to_string())?;
        let event = make(&self.source, time_ms);
        self.events.append(&event).await.map_err(|e| e.to_string())?;
        Ok(event.id)
    }
}

/// The events store (build.md §5): append-only, each event its own record, never updated.
#[async_trait]
pub trait EventsStorage: Send + Sync {
    /// Append `event`.
    async fn append(&self, event: &Event) -> Result<(), StorageError>;
    /// The event `id`, `None` when the store holds none.
    async fn event(&self, id: &EventId) -> Result<Option<Event>, StorageError>;
    /// The events of the Build `build_id` (by `data.build_id`), in event.id order.
    async fn of_build(&self, build_id: &BuildId) -> Result<Vec<Event>, StorageError>;
    /// The events of the node `node_id` (by `subject`), in event.id order.
    async fn of_node(&self, node_id: &NodeId) -> Result<Vec<Event>, StorageError>;
    /// The storage changes' events (`type` `rdm.storage.*`), in event.id order.
    async fn of_storage_changes(&self) -> Result<Vec<Event>, StorageError>;
}

/// The events a store holds, folded: by event.id, the two ordered maps its range reads use,
/// `(build.id, event.id)` and `(node.id, event.id)`, and the storage changes' events by event.id.
#[derive(Debug, Default)]
struct EventsFold {
    by_id: BTreeMap<EventId, Event>,
    by_build: BTreeMap<BuildId, BTreeMap<EventId, Event>>,
    by_node: BTreeMap<NodeId, BTreeMap<EventId, Event>>,
    storage: BTreeMap<EventId, Event>,
}

impl EventsFold {
    fn insert(&mut self, event: Event) {
        if let Some(build_id) = event.build_id() {
            self.by_build.entry(build_id).or_default().insert(event.id.clone(), event.clone());
        }
        if let Some(node_id) = event.subject.clone() {
            self.by_node.entry(node_id).or_default().insert(event.id.clone(), event.clone());
        }
        if event.is_storage() {
            self.storage.insert(event.id.clone(), event.clone());
        }
        self.by_id.insert(event.id.clone(), event);
    }

    fn of_build(&self, build_id: &BuildId) -> Vec<Event> {
        self.by_build.get(build_id).map(|of| of.values().cloned().collect()).unwrap_or_default()
    }

    fn of_node(&self, node_id: &NodeId) -> Vec<Event> {
        self.by_node.get(node_id).map(|of| of.values().cloned().collect()).unwrap_or_default()
    }
}

/// An events store held in memory.
#[derive(Debug, Default)]
pub struct MemoryEventsStorage(Mutex<EventsFold>);

impl MemoryEventsStorage {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl EventsStorage for MemoryEventsStorage {
    async fn append(&self, event: &Event) -> Result<(), StorageError> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).insert(event.clone());
        Ok(())
    }
    async fn event(&self, id: &EventId) -> Result<Option<Event>, StorageError> {
        Ok(self.0.lock().unwrap_or_else(|e| e.into_inner()).by_id.get(id).cloned())
    }
    async fn of_build(&self, build_id: &BuildId) -> Result<Vec<Event>, StorageError> {
        Ok(self.0.lock().unwrap_or_else(|e| e.into_inner()).of_build(build_id))
    }
    async fn of_node(&self, node_id: &NodeId) -> Result<Vec<Event>, StorageError> {
        Ok(self.0.lock().unwrap_or_else(|e| e.into_inner()).of_node(node_id))
    }
    async fn of_storage_changes(&self) -> Result<Vec<Event>, StorageError> {
        Ok(self.0.lock().unwrap_or_else(|e| e.into_inner()).storage.values().cloned().collect())
    }
}

#[cfg(feature = "file-backend")]
/// The `dataschema` of an event's line in the root's `events.jsonl` (Q365): the line is the event, its
/// `data` the event's data.
pub const EVENT_FORMAT: &str = "event/1";

#[cfg(feature = "file-backend")]
/// The events store on the fabric's store root: `events.jsonl`, one event per line, the line the
/// event itself (R-SR3, Q365). Its open reads nothing (Q227); its first read folds the file, and each read after it
/// folds the lines appended since, into the store's [`EventsFold`].
#[derive(Debug)]
pub struct FileEventsStorage {
    log: crate::store_log::StoreLog,
    fold: std::sync::Arc<Mutex<EventsFold>>,
}

#[cfg(feature = "file-backend")]
impl FileEventsStorage {
    /// Open the store on the fabric's store root: each event's line is written by the event's own
    /// `source`.
    pub async fn open(root: &crate::store_root::StoreRoot) -> Result<Self, StorageError> {
        let fold: std::sync::Arc<Mutex<EventsFold>> = Default::default();
        let observer: crate::store_log::Observer = {
            let fold = fold.clone();
            std::sync::Arc::new(move |line: &crate::store_log::Line| {
                // The fold reads only the lines its formats check: an event line decodes.
                if let Some(event) = Event::of_line(line) {
                    fold.lock().unwrap_or_else(|e| e.into_inner()).insert(event);
                }
            })
        };
        let path = root.dir().join(crate::store_root::EVENTS_FILE);
        let log = crate::store_log::StoreLog::open_observed_on_first_read(&path, crate::store_log::Formats::new().with::<serde_json::Map<String, serde_json::Value>>(EVENT_FORMAT), observer).await?;
        Ok(Self { log, fold })
    }

    /// Finishes every write this store has issued.
    pub async fn finish_issued_writes(&self) {
        self.log.finish_issued_writes().await;
    }

    /// The fold, after reading the lines appended since the last read.
    async fn read(&self) -> Result<std::sync::MutexGuard<'_, EventsFold>, StorageError> {
        self.log.advance_for("events").await?;
        Ok(self.fold.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl EventsStorage for FileEventsStorage {
    async fn append(&self, event: &Event) -> Result<(), StorageError> {
        self.log.append_with(event.attributes(), &event.data).await
    }
    async fn event(&self, id: &EventId) -> Result<Option<Event>, StorageError> {
        Ok(self.read().await?.by_id.get(id).cloned())
    }
    async fn of_build(&self, build_id: &BuildId) -> Result<Vec<Event>, StorageError> {
        Ok(self.read().await?.of_build(build_id))
    }
    async fn of_node(&self, node_id: &NodeId) -> Result<Vec<Event>, StorageError> {
        Ok(self.read().await?.of_node(node_id))
    }
    async fn of_storage_changes(&self) -> Result<Vec<Event>, StorageError> {
        Ok(self.read().await?.storage.values().cloned().collect())
    }
}

/// The events an events store was given, in the order appended: what a cell reads the state moves
/// from (they name no Build and no node, so the store's two range reads hold none).
#[cfg(test)]
#[derive(Default)]
pub(crate) struct AppendedEvents(pub(crate) Mutex<Vec<Event>>);

#[cfg(test)]
#[async_trait]
impl EventsStorage for AppendedEvents {
    async fn append(&self, event: &Event) -> Result<(), StorageError> {
        self.0.lock().unwrap().push(event.clone());
        Ok(())
    }
    async fn event(&self, id: &EventId) -> Result<Option<Event>, StorageError> {
        Ok(self.0.lock().unwrap().iter().find(|e| &e.id == id).cloned())
    }
    async fn of_build(&self, _: &BuildId) -> Result<Vec<Event>, StorageError> {
        Ok(Vec::new())
    }
    async fn of_node(&self, _: &NodeId) -> Result<Vec<Event>, StorageError> {
        Ok(Vec::new())
    }
    async fn of_storage_changes(&self) -> Result<Vec<Event>, StorageError> {
        Ok(self.0.lock().unwrap().iter().filter(|e| e.is_storage()).cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(label: &str) -> EventSource {
        EventSource { fabric_id: FabricId::parse(&rdm_core::fixture_crockford60("fabric-events")).unwrap(), node_id: NodeId::parse(&rdm_core::fixture_crockford60(label)).unwrap(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60(&format!("{label}-inc"))).unwrap() }
    }

    /// Two Builds' events and two nodes' events, written interleaved: what each read must answer.
    fn written() -> (Vec<Event>, BuildId, BuildId, NodeId, NodeId) {
        let fp = source("fabric-primary");
        let mp = source("mesh-primary");
        let (b1, b2) = (BuildId::parse(&rdm_core::fixture_crockford60("build-1")).unwrap(), BuildId::parse(&rdm_core::fixture_crockford60("build-2")).unwrap());
        let (n1, n2) = (NodeId::parse(&rdm_core::fixture_crockford60("node-1")).unwrap(), NodeId::parse(&rdm_core::fixture_crockford60("node-2")).unwrap());
        let inc = IncarnationId::parse(&rdm_core::fixture_crockford60("node-inc")).unwrap();
        let events = vec![
            Event::build_started(&fp, 1_000, &b1),
            Event::build_started(&fp, 1_001, &b2),
            Event::build_dispatched(&fp, 1_002, &b1, "mesh1"),
            Event::build_step(&mp, 1_003, Some(&n1), &b1, "mesh1", "create", "node.create", &StepState::Complete),
            Event::node_investigated(&mp, 1_004, &n2, &inc, "mesh1.node", "2", "exited", "recreate", crate::node_investigation::Reason::SelfHeal),
            Event::build_dispatched(&fp, 1_005, &b2, "mesh1"),
            Event::build_step(&mp, 1_006, Some(&n2), &b2, "mesh1", "create", "node.create", &StepState::Failed { reason: "the provider refused".into(), code: "FAILED_PRECONDITION".into() }),
            Event::build_step(&mp, 1_007, Some(&n1), &b2, "mesh1", "delete", "node.delete", &StepState::Complete),
            Event::build_ended(&fp, 1_008, &b1, &EndedState::Complete),
            Event::node_investigated(&mp, 1_009, &n1, &inc, "mesh1.node", "1", "reached", "rejoin", crate::node_investigation::Reason::SelfHeal),
            Event::build_ended(&fp, 1_010, &b2, &EndedState::Failed { reason: "the provider refused".into(), code: "FAILED_PRECONDITION".into() }),
        ];
        (events, b1, b2, n1, n2)
    }

    fn ids(events: &[Event]) -> Vec<String> {
        events.iter().map(|e| format!("{} {}", e.r#type, e.id)).collect()
    }

    /// Each read answers exactly the events written of its Build or node, in event.id order, and the
    /// point read answers each event whole.
    async fn reads_back(store: &dyn EventsStorage, written: &[Event], b1: &BuildId, b2: &BuildId, n1: &NodeId, n2: &NodeId) {
        let expect = |keep: &dyn Fn(&Event) -> bool| -> Vec<Event> {
            let mut kept: Vec<Event> = written.iter().filter(|e| keep(e)).cloned().collect();
            kept.sort_by(|a, b| a.id.cmp(&b.id));
            kept
        };
        for b in [b1, b2] {
            let read = store.of_build(b).await.unwrap();
            let want = expect(&|e| e.build_id().as_ref() == Some(b));
            assert_eq!(ids(&read), ids(&want), "the events of Build {b}, in event.id order");
            assert_eq!(read, want, "each event of Build {b} read back whole");
        }
        for n in [n1, n2] {
            let read = store.of_node(n).await.unwrap();
            let want = expect(&|e| e.subject.as_ref() == Some(n));
            assert_eq!(ids(&read), ids(&want), "the events of node {n}, in event.id order");
            assert_eq!(read, want, "each event of node {n} read back whole");
        }
        assert_eq!(store.of_build(b1).await.unwrap().len(), 4, "b1: started, dispatched, one step, ended");
        assert_eq!(store.of_build(b2).await.unwrap().len(), 5, "b2: started, dispatched, two steps, ended");
        assert_eq!(store.of_node(n1).await.unwrap().len(), 3, "n1: two steps and one investigation");
        assert_eq!(store.of_node(n2).await.unwrap().len(), 2, "n2: one step and one investigation");
        for e in written {
            assert_eq!(store.event(&e.id).await.unwrap().as_ref(), Some(e), "the event {} by its id", e.id);
        }
    }

    /// CONTRACT (build.md §5: "The store answers three reads: by event.id, the events of one Build (by
    /// `data.build_id`, in event.id order) and the events of one node (by `subject`, in event.id
    /// order)"): the events of two Builds and two nodes, appended interleaved, read back by event.id,
    /// by build.id and by node.id, each in event.id order, from the memory store and the file store.
    /// Must NOT happen: an event of another Build or node in a read, an event missing, or an order
    /// other than event.id's.
    // @feature: build
    #[tokio::test]
    async fn the_events_of_two_builds_and_two_nodes_read_back_in_event_id_order_by_build_and_by_node() {
        let (mut events, b1, b2, n1, n2) = written();
        // Appended out of mint order: the reads order by event.id, never by the order of the writes.
        events.swap(0, 7);
        events.swap(2, 9);
        let memory = MemoryEventsStorage::new();
        for e in &events {
            memory.append(e).await.unwrap();
        }
        reads_back(&memory, &events, &b1, &b2, &n1, &n2).await;
        #[cfg(feature = "file-backend")]
        {
            let dir = crate::test_support::tempdir("events-read-back");
            let root = crate::store_root::StoreRoot::at(&dir);
            let file = FileEventsStorage::open(&root).await.unwrap();
            for e in &events {
                file.append(e).await.unwrap();
            }
            reads_back(&file, &events, &b1, &b2, &n1, &n2).await;
            let lines = crate::store_log::read_file(&dir.join(crate::store_root::EVENTS_FILE)).unwrap();
            assert_eq!(lines.len(), events.len(), "one line per event in events.jsonl");
            assert!(lines.iter().all(|l| l.attributes.dataschema == EVENT_FORMAT), "every line an event/1 line");
            for (line, event) in lines.iter().zip(&events) {
                assert_eq!(Event::of_line(line).as_ref(), Some(event), "the line is the event itself");
            }
        }
    }

    /// CONTRACT (build.md §5; Q365): an event's line in the root's `events.jsonl` is frozen byte for
    /// byte: the event itself, a CloudEvents event with its own `id`, `source`, `type`, `subject`,
    /// `time` and trace context, `dataschema` `event/1` and `data` the event's data. A layout that
    /// moves moves this cell with it (`durable format moved`). Must NOT happen: an event wrapped in
    /// another envelope, or a line whose attributes or data bytes moved while this cell still passes.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn the_events_store_line_is_frozen_byte_for_byte() {
        let dir = crate::test_support::tempdir("events-store-layout");
        let store = FileEventsStorage::open(&crate::store_root::StoreRoot::at(&dir)).await.unwrap();
        let mp = source("mesh-primary");
        let (n1, inc) = (NodeId::parse(&rdm_core::fixture_crockford60("node-1")).unwrap(), IncarnationId::parse(&rdm_core::fixture_crockford60("node-inc")).unwrap());
        let mut event = Event::node_investigated(&mp, 1_000, &n1, &inc, "mesh1.node", "1", "exited", "recreate", crate::node_investigation::Reason::SelfHeal);
        event.id = EventId::parse("0190a8f0-1234-7abc-8def-0123456789ab").unwrap();
        (event.traceparent, event.tracestate) = (Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()), None);
        store.append(&event).await.unwrap();
        let line = format!(r#"{{"len":LEN,"specversion":"1.0","id":"0190a8f0-1234-7abc-8def-0123456789ab","source":"{}","type":"rdm.node.investigated","subject":"{n1}","time":"1970-01-01T00:00:01.000Z","datacontenttype":"application/json","dataschema":"event/1","traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01","data":{{"incarnation":"{inc}","name":"1","next":"recreate","path":"mesh1.node","reason":"self-heal","trigger":"silence","verdict":"exited"}}"#, mp.uri());
        let written = String::from_utf8(std::fs::read(dir.join(crate::store_root::EVENTS_FILE)).unwrap()).unwrap();
        let object = written.trim_matches('\n');
        let len = object.len();
        assert_eq!(&object[..object.len() - 20], line.replace("LEN", &len.to_string()), "the event is the line, its data last");
        assert_eq!(&object[object.len() - 20..], format!(",\"crc32\":\"{:08x}\"}}", crate::store_log::crc32(&object.as_bytes()[..len - 20])));
    }

    /// CONTRACT (build.md §5.3: the events store is one of the boundaries on the fabric's shared store
    /// root, `events.jsonl`): the events a writer appended survive a restart of the reading admin: a
    /// store opened again on the same root (a new process's open, reading nothing at it) reads back
    /// every event, by event.id, by build.id and by node.id, in event.id order, and appends after them.
    /// Must NOT happen: an event lost across the restart, or the reopened store answering only what it
    /// appended itself.
    // @feature: build
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn the_file_events_store_reads_back_every_event_after_a_restart_of_the_reading_admin() {
        let (events, b1, b2, n1, n2) = written();
        let dir = crate::test_support::tempdir("events-restart");
        let first = source("fabric-primary");
        {
            let root = crate::store_root::StoreRoot::at(&dir);
            let file = FileEventsStorage::open(&root).await.unwrap();
            for e in &events {
                file.append(e).await.unwrap();
            }
            file.finish_issued_writes().await;
        }
        // The reading admin restarted: a new incarnation of another node opens the root again.
        let after = source("restarted-admin");
        let root = crate::store_root::StoreRoot::at(&dir);
        let reopened = FileEventsStorage::open(&root).await.unwrap();
        reads_back(&reopened, &events, &b1, &b2, &n1, &n2).await;
        let later = Event::build_ended(&after, 2_000, &b1, &EndedState::Complete);
        reopened.append(&later).await.unwrap();
        let of_b1 = reopened.of_build(&b1).await.unwrap();
        assert_eq!(of_b1.last(), Some(&later), "an event appended after the restart reads last of its Build");
        assert_eq!(of_b1.len(), 5, "the Build's four events before the restart and the one after it");
    }

    /// CONTRACT (build.md §5: `id` a time-ordered UUIDv7, RFC 9562): every minted id is a canonical
    /// lowercase UUIDv7 whose timestamp is its mint's Unix milliseconds, and ids minted one after
    /// another order lexically in mint order, the same millisecond included. Must NOT happen: a
    /// version other than 7, a variant other than 0b10, or two ids of one millisecond out of order.
    // @feature: build
    #[test]
    fn event_ids_are_canonical_uuidv7_in_mint_order() {
        let before = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        let minted: Vec<EventId> = (0..2000).map(|_| EventId::mint()).collect();
        let after = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        for id in &minted {
            assert_eq!(EventId::parse(id.as_str()).as_ref(), Ok(id), "{id} is a canonical UUIDv7");
            assert!((before..=after).contains(&id.unix_ms()), "{id}'s timestamp {} is its mint's ms in [{before}, {after}]", id.unix_ms());
        }
        assert!(minted.windows(2).all(|w| w[0] < w[1]), "each id orders after the one minted before it");
        assert!(EventId::parse("0190a8f0-1234-4abc-8def-0123456789ab").is_err(), "a UUIDv4 is refused by name");
    }

    /// CONTRACT (build.md §5: "The store answers four reads: ... and the storage changes' events
    /// (`type` `rdm.storage.*`, in event.id order)"; the `rdm.storage.*` row: written by the
    /// fabric-primary, `subject` none, `data` `change_id`, `topology_storage_provider`, and for the
    /// ending its `state`): two storage changes' events written among a Build's and a node's read back
    /// as the storage changes' events alone, in event.id order, from memory and from `events.jsonl`
    /// reopened; each carries its change and provider and no Build, so no Build's read answers one;
    /// `commits` names only its own change's `rdm.storage.committed`. Must NOT happen: a storage event
    /// carrying a build_id, or a Build's or a node's event among the storage changes' events.
    // @feature: topology-storage-provider
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn the_storage_changes_events_read_back_alone_in_event_id_order() {
        let (mut events, b1, _, n1, _) = written();
        let fp = source("fabric-primary");
        let (c1, c2) = (ChangeId::mint(), ChangeId::mint());
        let storage = vec![
            Event::storage_started(&fp, 3_000, &c1, "test-target"),
            Event::storage_committed(&fp, 3_001, &c1, "test-target"),
            Event::storage_ended(&fp, 3_002, &c1, "test-target", &StepState::Complete),
            Event::storage_started(&fp, 3_003, &c2, "file"),
        ];
        events.insert(2, storage[0].clone());
        events.insert(5, storage[1].clone());
        events.push(storage[2].clone());
        events.push(Event::build_ended(&fp, 3_004, &b1, &EndedState::Complete));
        events.push(storage[3].clone());
        let memory = MemoryEventsStorage::new();
        let dir = crate::test_support::tempdir("events-storage-changes");
        let file = FileEventsStorage::open(&crate::store_root::StoreRoot::at(&dir)).await.unwrap();
        for e in &events {
            memory.append(e).await.unwrap();
            file.append(e).await.unwrap();
        }
        file.finish_issued_writes().await;
        let reopened = FileEventsStorage::open(&crate::store_root::StoreRoot::at(&dir)).await.unwrap();
        let stores: [(&str, &dyn EventsStorage); 2] = [("memory", &memory), ("file", &reopened)];
        for (backend, store) in stores {
            let read = store.of_storage_changes().await.unwrap();
            assert_eq!(ids(&read), ids(&storage), "{backend}: the storage changes' events alone, in event.id order");
            assert!(read.iter().all(|e| e.build_id().is_none() && e.subject.is_none()), "{backend}: no storage event names a Build or a node");
            let carried: Vec<(Option<ChangeId>, Option<String>)> = read.iter().map(|e| (e.change_id(), e.topology_storage_provider().map(String::from))).collect();
            let expected: Vec<(Option<ChangeId>, Option<String>)> = vec![(Some(c1.clone()), Some("test-target".into())), (Some(c1.clone()), Some("test-target".into())), (Some(c1.clone()), Some("test-target".into())), (Some(c2.clone()), Some("file".into()))];
            assert_eq!(carried, expected, "{backend}");
            assert!(read[1].commits(&c1) && !read[1].commits(&c2) && !read[0].commits(&c1), "{backend}: commits names its own change's commit alone");
            assert!(!store.of_build(&b1).await.unwrap().iter().any(Event::is_storage) && !store.of_node(&n1).await.unwrap().iter().any(Event::is_storage), "{backend}: no Build's or node's read answers a storage event");
        }
    }
}
