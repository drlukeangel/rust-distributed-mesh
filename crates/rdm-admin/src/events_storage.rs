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

/// An event's identity: a UUIDv7 (RFC 9562), its 48 leading bits the Unix milliseconds of its mint,
/// written in the canonical lowercase 8-4-4-4-12 form, so lexical order is mint order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EventId(String);

impl EventId {
    /// Mint a fresh id: the Unix milliseconds now, then random bits, monotonic in this process
    /// (RFC 9562 §6.2, method 2: an id minted in the same millisecond as the last, or with the clock
    /// behind it, is the last one plus one).
    pub fn mint() -> Self {
        // The 122 bits a UUIDv7 carries besides its version and variant: 48 of Unix milliseconds,
        // then 74 random. The last one minted is held, so the next is past it.
        static LAST: Mutex<u128> = Mutex::new(0);
        let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0) & ((1u64 << 48) - 1);
        let fresh = ((now_ms as u128) << 74) | (rand::random::<u128>() & ((1u128 << 74) - 1));
        let mut last = LAST.lock().unwrap_or_else(|e| e.into_inner());
        let payload = if fresh > *last { fresh } else { *last + 1 };
        *last = payload;
        Self(Self::render(Self::stamped(payload)))
    }

    /// The UUID of the 122 bits `p` (48 of time, 12 of `rand_a`, 62 of `rand_b`): version 7, variant
    /// 0b10. The version and variant sit at fixed positions, so the order of `p` is the UUID's.
    fn stamped(p: u128) -> u128 {
        let rand_b = p & ((1u128 << 62) - 1);
        let rand_a = (p >> 62) & 0x0fff;
        let ts = (p >> 74) & ((1u128 << 48) - 1);
        (ts << 80) | (0x7u128 << 76) | (rand_a << 64) | (0b10u128 << 62) | rand_b
    }

    fn render(v: u128) -> String {
        let h = format!("{v:032x}");
        format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
    }

    /// The id `value`, refused by name when it is not a canonical lowercase UUIDv7.
    pub fn parse(value: &str) -> Result<Self, String> {
        let b = value.as_bytes();
        let canonical = b.len() == 36
            && b.iter().enumerate().all(|(i, c)| if matches!(i, 8 | 13 | 18 | 23) { *c == b'-' } else { c.is_ascii_digit() || (b'a'..=b'f').contains(c) })
            && b[14] == b'7'
            && matches!(b[19], b'8' | b'9' | b'a' | b'b');
        if canonical {
            Ok(Self(value.to_string()))
        } else {
            Err(format!("event id {value:?} is not a canonical lowercase UUIDv7 (RFC 9562)"))
        }
    }

    /// The Unix milliseconds of the id's mint.
    pub fn unix_ms(&self) -> u64 {
        u64::from_str_radix(&self.0[0..8], 16).unwrap_or(0) << 16 | u64::from_str_radix(&self.0[9..13], 16).unwrap_or(0)
    }

    /// The id as its string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for EventId {
    type Error = String;
    fn try_from(value: String) -> Result<Self, String> {
        Self::parse(&value)
    }
}

impl From<EventId> for String {
    fn from(id: EventId) -> String {
        id.0
    }
}

impl std::fmt::Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

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

/// How a Build ended: `complete`, or `failed` at its step with its reason and code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EndedState {
    /// The Build completed.
    ///
    /// Serialized as `complete`.
    Complete,
    /// The Build failed.
    ///
    /// Serialized as `failed`.
    Failed {
        /// The step that failed.
        step: String,
        /// Why.
        reason: String,
        /// The gRPC code name of the failure.
        code: String,
    },
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

    /// `rdm.build.step` on the node `node_id` {build_id, mesh, step, op, state}.
    #[allow(clippy::too_many_arguments)]
    pub fn build_step(source: &EventSource, time_ms: u64, node_id: &NodeId, build_id: &BuildId, mesh: &str, step: &str, op: &str, state: &StepState) -> Self {
        #[derive(Serialize)]
        struct Data<'a> {
            build_id: &'a BuildId,
            mesh: &'a str,
            step: &'a str,
            op: &'a str,
            state: &'a StepState,
        }
        Self::new(source, time_ms, BUILD_STEP, Some(node_id.clone()), object(Data { build_id, mesh, step, op, state }))
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

    /// `rdm.node.investigated` on the node `node_id` {incarnation, path, name, verdict, next}.
    #[allow(clippy::too_many_arguments)]
    pub fn node_investigated(source: &EventSource, time_ms: u64, node_id: &NodeId, incarnation: &IncarnationId, path: &str, name: &str, verdict: &str, next: &str) -> Self {
        #[derive(Serialize)]
        struct Data<'a> {
            incarnation: &'a IncarnationId,
            path: &'a str,
            name: &'a str,
            verdict: &'a str,
            next: &'a str,
        }
        Self::new(source, time_ms, NODE_INVESTIGATED, Some(node_id.clone()), object(Data { incarnation, path, name, verdict, next }))
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
    by_build: BTreeMap<(BuildId, EventId), Event>,
    by_node: BTreeMap<(NodeId, EventId), Event>,
    storage: BTreeMap<EventId, Event>,
}

impl EventsFold {
    fn insert(&mut self, event: Event) {
        if let Some(build_id) = event.build_id() {
            self.by_build.insert((build_id, event.id.clone()), event.clone());
        }
        if let Some(node_id) = event.subject.clone() {
            self.by_node.insert((node_id, event.id.clone()), event.clone());
        }
        if event.is_storage() {
            self.storage.insert(event.id.clone(), event.clone());
        }
        self.by_id.insert(event.id.clone(), event);
    }

    fn of_build(&self, build_id: &BuildId) -> Vec<Event> {
        self.by_build.range((build_id.clone(), EventId(String::new()))..).take_while(|((b, _), _)| b == build_id).map(|(_, e)| e.clone()).collect()
    }

    fn of_node(&self, node_id: &NodeId) -> Vec<Event> {
        self.by_node.range((node_id.clone(), EventId(String::new()))..).take_while(|((n, _), _)| n == node_id).map(|(_, e)| e.clone()).collect()
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
/// An event's line in the root's `events.jsonl`: the whole event, at its id.
pub const EVENT_FORMAT: &str = "event/1";

#[cfg(feature = "file-backend")]
/// The events store on the fabric's store root: `events.jsonl`, one whole event per line at its id
/// (R-SR3). Its open reads nothing (Q227); its first read folds the file, and each read after it
/// folds the lines appended since, into the store's [`EventsFold`].
#[derive(Debug)]
pub struct FileEventsStorage {
    log: crate::store_log::StoreLog,
    writer: crate::store_log::LogWriter,
    fold: std::sync::Arc<Mutex<EventsFold>>,
}

#[cfg(feature = "file-backend")]
impl FileEventsStorage {
    /// Open the store on the fabric's store root, written by `writer`.
    pub async fn open(root: &crate::store_root::StoreRoot, writer: &crate::store_log::LogWriter) -> Result<Self, StorageError> {
        let fold: std::sync::Arc<Mutex<EventsFold>> = Default::default();
        let observer: crate::store_log::Observer = {
            let fold = fold.clone();
            std::sync::Arc::new(move |line: &crate::store_log::Line| {
                // The fold reads only the lines its formats check: an event line decodes.
                if let Ok(event) = serde_json::from_value::<Event>(line.record.clone()) {
                    fold.lock().unwrap_or_else(|e| e.into_inner()).insert(event);
                }
            })
        };
        let path = root.dir().join(crate::store_root::EVENTS_FILE);
        let log = crate::store_log::StoreLog::open_observed_on_first_read(&path, crate::store_log::Formats::new().with::<Event>(EVENT_FORMAT), observer).await?;
        Ok(Self { log, writer: writer.clone(), fold })
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
        self.log.append(&self.writer, EVENT_FORMAT, event.id.as_str(), event).await
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
            Event::build_step(&mp, 1_003, &n1, &b1, "mesh1", "create", "node.create", &StepState::Complete),
            Event::node_investigated(&mp, 1_004, &n2, &inc, "mesh1.node", "2", "exited", "recreate"),
            Event::build_dispatched(&fp, 1_005, &b2, "mesh1"),
            Event::build_step(&mp, 1_006, &n2, &b2, "mesh1", "create", "node.create", &StepState::Failed { reason: "the provider refused".into(), code: "FAILED_PRECONDITION".into() }),
            Event::build_step(&mp, 1_007, &n1, &b2, "mesh1", "delete", "node.delete", &StepState::Complete),
            Event::build_ended(&fp, 1_008, &b1, &EndedState::Complete),
            Event::node_investigated(&mp, 1_009, &n1, &inc, "mesh1.node", "1", "reached", "rejoin"),
            Event::build_ended(&fp, 1_010, &b2, &EndedState::Failed { step: "create".into(), reason: "the provider refused".into(), code: "FAILED_PRECONDITION".into() }),
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
            let writer = crate::store_log::LogWriter::new(&source("fabric-primary").node_id, &source("fabric-primary").incarnation);
            let file = FileEventsStorage::open(&root, &writer).await.unwrap();
            for e in &events {
                file.append(e).await.unwrap();
            }
            reads_back(&file, &events, &b1, &b2, &n1, &n2).await;
            let lines = crate::store_log::read_file(&dir.join(crate::store_root::EVENTS_FILE)).unwrap();
            assert_eq!(lines.len(), events.len(), "one line per event in events.jsonl");
            assert!(lines.iter().all(|l| l.format == EVENT_FORMAT), "every line an event/1 line");
        }
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
            let file = FileEventsStorage::open(&root, &crate::store_log::LogWriter::new(&first.node_id, &first.incarnation)).await.unwrap();
            for e in &events {
                file.append(e).await.unwrap();
            }
            file.finish_issued_writes().await;
        }
        // The reading admin restarted: a new incarnation of another node opens the root again.
        let after = source("restarted-admin");
        let root = crate::store_root::StoreRoot::at(&dir);
        let reopened = FileEventsStorage::open(&root, &crate::store_log::LogWriter::new(&after.node_id, &after.incarnation)).await.unwrap();
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
        let file = FileEventsStorage::open(&crate::store_root::StoreRoot::at(&dir), &crate::store_log::LogWriter::new(&fp.node_id, &fp.incarnation)).await.unwrap();
        for e in &events {
            memory.append(e).await.unwrap();
            file.append(e).await.unwrap();
        }
        file.finish_issued_writes().await;
        let reopened = FileEventsStorage::open(&crate::store_root::StoreRoot::at(&dir), &crate::store_log::LogWriter::mint()).await.unwrap();
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
