//! The copy of each object its writer holds (storage-whole-records.md, "The rule"): a write takes the
//! object from here, applies its change, writes the whole object to storage and, once storage answers,
//! replaces the copy here. Every mutation of one object holds that object's own lock from the read of
//! the copy through the write and the replacement, so two updates of one object never overlap; storage
//! is read only to fill the copy when write ownership of the object is acquired.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

/// The cached objects of one kind, by key, each with its own lock.
#[derive(Debug)]
pub struct RecordCache<K, V> {
    held: Mutex<HashMap<K, V>>,
    locks: Mutex<HashMap<K, Arc<tokio::sync::Mutex<()>>>>,
}

impl<K, V> Default for RecordCache<K, V> {
    fn default() -> Self {
        Self { held: Mutex::new(HashMap::new()), locks: Mutex::new(HashMap::new()) }
    }
}

impl<K: Eq + Hash + Clone, V: Clone> RecordCache<K, V> {
    /// The copy held of `key`, if any.
    pub fn get(&self, key: &K) -> Option<V> {
        self.held.lock().unwrap_or_else(|e| e.into_inner()).get(key).cloned()
    }

    /// Hold `value` as the copy of `key`.
    pub fn hold(&self, key: K, value: V) {
        self.held.lock().unwrap_or_else(|e| e.into_inner()).insert(key, value);
    }

    /// Hold no copy of `key`.
    pub fn forget(&self, key: &K) {
        self.held.lock().unwrap_or_else(|e| e.into_inner()).remove(key);
    }

    /// Every copy held.
    pub fn all(&self) -> Vec<V> {
        self.held.lock().unwrap_or_else(|e| e.into_inner()).values().cloned().collect()
    }

    /// `key`'s own lock: held by a mutation from the read of its copy through the write and the
    /// replacement of the copy.
    pub async fn lock(&self, key: &K) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = self.locks.lock().unwrap_or_else(|e| e.into_inner()).entry(key.clone()).or_default().clone();
        lock.lock_owned().await
    }
}

/// The records this admin writes, as it holds them: its node records (shared with the writer of its
/// mesh's rows and its `NodeConfigs`), its mesh records and the Fabric record.
#[derive(Debug, Clone, Default)]
pub struct Caches {
    /// The mesh records, by mesh id.
    pub meshes: Arc<RecordCache<crate::model::MeshId, crate::storage::MeshRecord>>,
    /// The Fabric record, by fabric id.
    pub fabric: Arc<RecordCache<crate::model::FabricId, crate::fabric_storage::FabricRecord>>,
    /// What this admin writes the moves of these records with: each move of a mesh's or the Fabric's
    /// `state` is also an event (`rdm.mesh.<state>`, `rdm.fabric.<state>`; build.md §5; node-identity-and-state.md
    /// §2.7). Set once at the boot ([`Caches::writes_events`]).
    pub events: Arc<std::sync::OnceLock<crate::events_storage::EventsWriter>>,
}

impl Caches {
    /// Write the moves of the records this admin writes with `writer`; a second call keeps the first.
    pub fn writes_events(&self, writer: crate::events_storage::EventsWriter) {
        let _ = self.events.set(writer);
    }

    /// The move of a mesh's record from `before` (`None`: the record is new) to `after`, written:
    /// the event of the state it moved to (`rdm.mesh.pending`, `.ready`, `.deleting`, `.deleted`), under
    /// `rdm.node_admin.mesh.update.via-state-event`, which names the event or why none was written.
    pub async fn mesh_moved(&self, before: Option<&crate::storage::MeshRecord>, after: &crate::storage::MeshRecord) {
        if before.is_some_and(|b| b.state == after.state) {
            return;
        }
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-state-event", mesh = %after.name, mesh_id = %after.mesh_id, state = after.state.name(), event_id = tracing::field::Empty, outcome = tracing::field::Empty, "otel.kind" = "internal");
        let written = match self.events.get() {
            Some(w) => w.append(|source, time_ms| crate::events_storage::Event::mesh_moved(source, time_ms, &after.mesh_id, &after.name, after.state)).await,
            None => Err("this admin holds no events writer".into()),
        };
        state_event_outcome(&span, written, "the mesh's state moved: its event");
    }

    /// The move of the Fabric record from `before` (`None`: the record is new) to `after`, written:
    /// the event of the state it moved to (`rdm.fabric.pending`, `.ready`), under
    /// `rdm.node_admin.fabric.update.via-state-event`, which names the event or why none was written.
    pub async fn fabric_moved(&self, before: Option<&crate::fabric_storage::FabricRecord>, after: &crate::fabric_storage::FabricRecord) {
        if before.is_some_and(|b| b.state == after.state) {
            return;
        }
        let span = tracing::info_span!("rdm.node_admin.fabric.update.via-state-event", fabric = %after.name, fabric_id = %after.fabric_id, state = after.state.as_str(), event_id = tracing::field::Empty, outcome = tracing::field::Empty, "otel.kind" = "internal");
        let written = match self.events.get() {
            Some(w) => w.append(|source, time_ms| crate::events_storage::Event::fabric_moved(source, time_ms, &after.fabric_id, after.state)).await,
            None => Err("this admin holds no events writer".into()),
        };
        state_event_outcome(&span, written, "the Fabric's state moved: its event");
    }

    /// The one mutation of `mesh_id`'s record, under its lock: the copy (filled from storage when none
    /// is held, else `base`), `change` applied, and the whole record written when it changed, the copy
    /// replaced once storage answers. `None`: no record of the mesh is held anywhere and no `base`
    /// was given; nothing is written.
    pub async fn update_mesh<R>(&self, storage: &dyn crate::storage::MeshStorage, mesh_id: &crate::model::MeshId, base: Option<crate::storage::MeshRecord>, change: impl FnOnce(&mut crate::storage::MeshRecord) -> R) -> Result<Option<R>, String> {
        let _one = self.meshes.lock(mesh_id).await;
        let (held, new) = match self.meshes.get(mesh_id) {
            Some(r) => (Some(r), false),
            None => match storage.mesh(mesh_id).await.map_err(|e| format!("mesh.storage could not read the record of {mesh_id}: {e}"))? {
                Some(r) => (Some(r), false),
                None => (base, true),
            },
        };
        let Some(before) = held else { return Ok(None) };
        let mut record = before.clone();
        let r = change(&mut record);
        if record != before || new {
            storage.put_mesh(&record).await.map_err(|e| format!("mesh.storage refused the record of {mesh_id}: {e}"))?;
            self.mesh_moved((!new).then_some(&before), &record).await;
        }
        self.meshes.hold(mesh_id.clone(), record);
        Ok(Some(r))
    }

    /// The one mutation of the Fabric record `fabric_id`, under its lock: the copy (filled from storage
    /// when none is held, else `base`), `change` applied, and the whole record written when it changed,
    /// the copy replaced once storage answers. `None`: no Fabric record is held anywhere and no `base`
    /// was given; nothing is written.
    pub async fn update_fabric<R>(&self, storage: &dyn crate::fabric_storage::FabricStorage, fabric_id: &crate::model::FabricId, base: Option<crate::fabric_storage::FabricRecord>, change: impl FnOnce(&mut crate::fabric_storage::FabricRecord) -> R) -> Result<Option<R>, String> {
        let _one = self.fabric.lock(fabric_id).await;
        let (held, new) = match self.fabric.get(fabric_id) {
            Some(r) => (Some(r), false),
            None => match storage.fabric().await.map_err(|e| format!("fabric.storage could not read the Fabric record: {e}"))?.filter(|r| r.fabric_id == *fabric_id) {
                Some(r) => (Some(r), false),
                None => (base, true),
            },
        };
        let Some(before) = held else { return Ok(None) };
        let mut record = before.clone();
        let r = change(&mut record);
        if record != before || new {
            storage.put_fabric(&record).await.map_err(|e| format!("fabric.storage refused the Fabric record: {e}"))?;
            self.fabric_moved((!new).then_some(&before), &record).await;
        }
        self.fabric.hold(fabric_id.clone(), record);
        Ok(Some(r))
    }

    /// Fill the copies of every mesh record and the Fabric record from storage, at the moment this
    /// admin takes their writing: the records as they stand then, never a copy held from before.
    pub async fn fill_meshes_and_fabric(&self, meshes: &dyn crate::storage::MeshStorage, fabric: &dyn crate::fabric_storage::FabricStorage) -> Result<(), String> {
        // Each record is read again under its own lock: a write that landed after the listing is never
        // replaced by the listing's older record.
        for listed in meshes.meshes().await.map_err(|e| format!("mesh.storage: {e}"))? {
            let _one = self.meshes.lock(&listed.mesh_id).await;
            match meshes.mesh(&listed.mesh_id).await.map_err(|e| format!("mesh.storage: {e}"))? {
                Some(r) => self.meshes.hold(r.mesh_id.clone(), r),
                None => self.meshes.forget(&listed.mesh_id),
            }
        }
        if let Some(listed) = fabric.fabric().await.map_err(|e| format!("fabric.storage: {e}"))? {
            let _one = self.fabric.lock(&listed.fabric_id).await;
            if let Some(r) = fabric.fabric().await.map_err(|e| format!("fabric.storage: {e}"))?.filter(|r| r.fabric_id == listed.fabric_id) {
                self.fabric.hold(r.fabric_id.clone(), r);
            }
        }
        Ok(())
    }
}

/// `span` records a state event's write: its id, or why none was written.
fn state_event_outcome(span: &tracing::Span, written: Result<crate::events_storage::EventId, String>, message: &str) {
    match written {
        Ok(id) => {
            span.record("event_id", id.as_str());
            span.record("outcome", "written");
        }
        Err(e) => {
            span.record("outcome", "event-not-written");
            span.in_scope(|| tracing::info!(cause = %e, "{message} was not written"));
            return;
        }
    }
    span.in_scope(|| tracing::info!("{message} is written"));
}
