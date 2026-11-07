//! Versioned cross-Mesh topology (rafka gossip.md §3.1 and §3.3): the `topology_version` a source
//! Mesh's primary puts on its backbone mesh topology, the chunked snapshot a receiver installs
//! atomically, the delta a forwarding primary derives between two fulls it holds, and the
//! baseline an entry top-up hands a node.
//!
//! Everything here is a pure state machine over values: no socket, no clock, no timer. The
//! gossip wiring in [`crate::topology`] feeds it frames and sends what it returns.
//!
//! ```text
//! SOURCE PRIMARY   Full -> SourceVersion::observe -> version -> chunks_of -> TopologyFull on the backbone
//! RECEIVER         TopologyFull chunks -> SnapshotReceiver -> installed only once EVERY chunk of one
//!                  (mesh, version, author) is held
//! FORWARDER        held source Full, last-published Full -> Forwarder -> TopologyDelta chunks
//! RECEIVER         TopologyDelta chunks -> applied once EVERY chunk is held, at exactly base_version,
//!                  else the source desyncs
//! ```

use crate::frame::{Frame, MAX_FRAME};
use rdm_core::framing::Birth as NodeBirth;
use rdm_core::wire::{NodeEntry, NodeStatusEntry};
use rdm_core::{LifecycleOp, MeshDigest};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

pub use rdm_core::PublisherId;

type OpKey = (String, rdm_core::OperationId);

/// The digest as the topology projection sees it: what `topology_version` orders.
///
/// CONTRACT (gossip.md §3.1 "DOES bump / DOES NOT bump"): the view KEEPS the birth (identity,
/// incarnation, the one transport address), the status routing reads and the primary roles. It DROPS
/// `digest_seq` and `emitted_at_rafka_ms` (heartbeat order and a stamp) and the lineage, which an
/// aggregate's node entry does not carry (a read's does): one list of nodes is one topology, whichever
/// carried it.
pub(crate) fn topology_view(d: &MeshDigest) -> MeshDigest {
    let mut v = d.clone();
    v.digest_seq = 0;
    v.emitted_at_rafka_ms = 0;
    v.node.supersedes = None;
    v
}

/// One source Mesh as one publication holds it: its members, its open overlays, its retained
/// departures.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Full {
    digests: BTreeMap<String, MeshDigest>,
    in_flight: BTreeMap<OpKey, LifecycleOp>,
    deleted: BTreeMap<String, LifecycleOp>,
    /// The members' node ids by endpoint key, kept in step with `digests` ([`Full::member_at`]).
    by_endpoint: BTreeMap<String, BTreeSet<String>>,
    /// The id of the mesh, as its frame names it beside the mesh's name: a later mesh of the same name
    /// has another id ([`another_mesh`]).
    mesh_id: Option<rdm_core::MeshId>,
}

impl Full {
    /// A full from its members, the overlays still open and the departures retained; each keyed by
    /// its node id.
    pub fn new(digests: Vec<MeshDigest>, in_flight: Vec<LifecycleOp>, deleted: Vec<LifecycleOp>) -> Self {
        let digests: BTreeMap<String, MeshDigest> = digests.into_iter().map(|d| (d.node.node_id.to_string(), d)).collect();
        let mut by_endpoint: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (id, d) in &digests {
            by_endpoint.entry(d.node.endpoint_id.0.clone()).or_default().insert(id.clone());
        }
        Self {
            digests,
            in_flight: in_flight.into_iter().map(|o| (o.key(), o)).collect(),
            deleted: deleted.into_iter().map(|o| (o.node_id.to_string(), o)).collect(),
            by_endpoint,
            mesh_id: None,
        }
    }

    /// The member at endpoint key `endpoint_id`: of two at one key, the first by node id, as a
    /// scan of [`Self::digests`] finds it. A map read.
    pub fn member_at(&self, endpoint_id: &str) -> Option<&MeshDigest> {
        self.by_endpoint.get(endpoint_id).and_then(|ids| ids.first()).and_then(|id| self.digests.get(id))
    }

    /// Hold `d` as its node id's member, its endpoint key listed.
    fn put(&mut self, d: MeshDigest) {
        let id = d.node.node_id.to_string();
        self.unlist(&id);
        self.by_endpoint.entry(d.node.endpoint_id.0.clone()).or_default().insert(id.clone());
        self.digests.insert(id, d);
    }

    /// Take `id`'s endpoint key out of the listing.
    fn unlist(&mut self, id: &str) {
        let Some(held) = self.digests.get(id) else { return };
        let key = held.node.endpoint_id.0.clone();
        if let Some(ids) = self.by_endpoint.get_mut(&key) {
            ids.remove(id);
            if ids.is_empty() {
                self.by_endpoint.remove(&key);
            }
        }
    }

    /// The members, ordered by node id.
    pub fn digests(&self) -> Vec<MeshDigest> {
        self.digests.values().cloned().collect()
    }

    /// The overlays still open.
    pub fn in_flight(&self) -> Vec<LifecycleOp> {
        self.in_flight.values().cloned().collect()
    }

    /// The retained departures.
    pub fn deleted(&self) -> Vec<LifecycleOp> {
        self.deleted.values().cloned().collect()
    }

    /// The number of members.
    pub fn member_count(&self) -> usize {
        self.digests.len()
    }

    /// Whether `other` is the same topology: the same members by `topology_view`, the same
    /// overlays, the same departures. A heartbeat, a stamp, a load or a republish is not a
    /// difference.
    pub fn same_topology(&self, other: &Full) -> bool {
        self.digests.len() == other.digests.len()
            && self.digests.iter().all(|(id, d)| other.digests.get(id).is_some_and(|o| topology_view(d) == topology_view(o)))
            && self.in_flight == other.in_flight
            && self.deleted == other.deleted
    }

    /// The id of the mesh these births belong to, as the frame that carried them names it next to the
    /// mesh's name; `None` when it named none.
    pub fn mesh_id(&self) -> Option<rdm_core::MeshId> {
        self.mesh_id.clone()
    }

    /// This full as the topology of the mesh whose id is `mesh_id`.
    pub fn of_mesh(mut self, mesh_id: Option<rdm_core::MeshId>) -> Self {
        self.mesh_id = mesh_id;
        self
    }

    /// The complete difference from this full to `new` (gossip.md §3.3: one differential per move
    /// from a held version to a newer one, never a replay of the versions between). `added` are the
    /// births `new` lists that this one does not list as the same exact birth, or lists with other
    /// details; `changed` the births both list whose status or primary roles moved; `removed` the
    /// births this one lists that `new` does not. `in_flight` and `deleted` carry the overlays and
    /// departures `new` holds that this one does not. A cleared stop overlay travels as its birth's
    /// own entry: a source's primary never lists a stopped birth, and lists it again only after its
    /// `node.joined` cleared the stop ([`Full::apply`]). An expired departure is a local fact of the
    /// receiver's age.
    pub fn delta_to(&self, new: &Full) -> Delta {
        let (mut added, mut changed) = (Vec::new(), Vec::new());
        for (id, d) in &new.digests {
            match self.digests.get(id).filter(|o| o.node.incarnation == d.node.incarnation) {
                None => added.push(d.clone()),
                Some(o) if topology_view(o) == topology_view(d) => {}
                Some(o) if o.node == d.node && o.fabric_id == d.fabric_id => changed.push(NodeStatusEntry::of(d)),
                Some(_) => added.push(d.clone()),
            }
        }
        Delta {
            added,
            changed,
            removed: self.digests.values().filter(|o| new.digests.get(o.node.node_id.as_str()).is_none_or(|d| d.node.incarnation != o.node.incarnation)).map(|o| NodeBirth { node_id: o.node.node_id.clone(), incarnation: o.node.incarnation.clone() }).collect(),
            in_flight: new.in_flight.iter().filter(|(k, _)| !self.in_flight.contains_key(*k)).map(|(_, o)| o.clone()).collect(),
            deleted: new.deleted.iter().filter(|(k, _)| !self.deleted.contains_key(*k)).map(|(_, o)| o.clone()).collect(),
            mesh_id: new.mesh_id.clone(),
        }
    }

    /// This full moved by `delta` (gossip.md §3.1, `TopologyDelta`), all of it or nothing: the
    /// births added or changed, as this full now holds them. A removal takes out its exact birth
    /// only. An added birth this full holds keeps the Digest sequence and lineage held of it, which a
    /// differential does not carry. A `changed` entry moves the held birth's status and primary roles;
    /// one naming a birth neither held nor added is [`Gap::ChangedBirthNotHeld`] and moves nothing. An added
    /// or changed birth clears that exact birth's stop overlay (`NodeStopped`, `node.stop:`): the
    /// source lists the birth again once its primary's `node.joined` ended the stop. A departure ends
    /// every overlay of its exact birth, its stop included: node.delete replaces NodeStopped with the
    /// deleted retention (gossip.md §3.1).
    pub fn apply(&mut self, delta: &Delta) -> Result<Vec<MeshDigest>, Gap> {
        let mut next = self.clone();
        if delta.mesh_id.is_some() {
            next.mesh_id = delta.mesh_id.clone();
        }
        for b in &delta.removed {
            if next.digests.get(b.node_id.as_str()).is_some_and(|d| d.node.incarnation == b.incarnation) {
                next.unlist(b.node_id.as_str());
                next.digests.remove(b.node_id.as_str());
            }
        }
        let mut moved = Vec::new();
        for d in &delta.added {
            let mut d = d.clone();
            if let Some(held) = next.digests.get(d.node.node_id.as_str()).filter(|h| h.node.incarnation == d.node.incarnation) {
                d.digest_seq = held.digest_seq;
                d.emitted_at_rafka_ms = held.emitted_at_rafka_ms;
                d.node.supersedes = held.node.supersedes.clone();
            }
            moved.push(d);
            next.listed(moved.last().expect("just pushed").clone());
        }
        for c in &delta.changed {
            let Some(held) = next.digests.get(c.node_id.as_str()).filter(|h| h.node.incarnation == c.incarnation) else { return Err(Gap::ChangedBirthNotHeld) };
            let mut d = held.clone();
            d.status = c.status;
            d.primary = c.primary.clone();
            moved.push(d);
            next.listed(moved.last().expect("just pushed").clone());
        }
        for o in &delta.in_flight {
            next.in_flight.insert(o.key(), o.clone());
        }
        for o in &delta.deleted {
            next.in_flight.retain(|_, x| !(x.node_id == o.node_id && x.incarnation == o.incarnation));
            next.deleted.insert(o.node_id.to_string(), o.clone());
        }
        *self = next;
        Ok(moved)
    }

    /// Hold `d` as listed: its exact birth's stop overlay ends ([`Self::apply`]).
    fn listed(&mut self, d: MeshDigest) {
        self.in_flight.retain(|_, o| !(o.is_stop() && o.node_id == d.node.node_id && o.incarnation == d.node.incarnation));
        self.put(d);
    }
}

/// What moves a held source projection from `base_version` to the version its differential names.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Delta {
    /// Births added, with their details: a node new to the mesh, or a held birth whose details changed.
    pub added: Vec<MeshDigest>,
    /// Held births whose status or primary roles changed.
    pub changed: Vec<NodeStatusEntry>,
    /// Births no longer listed.
    pub removed: Vec<NodeBirth>,
    /// The overlays opened or advanced.
    pub in_flight: Vec<LifecycleOp>,
    /// The departures recorded.
    pub deleted: Vec<LifecycleOp>,
    /// The id of the mesh the delta moves, as its frame names it.
    pub mesh_id: Option<rdm_core::MeshId>,
}

impl Delta {
    /// Whether the delta moves nothing.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.changed.is_empty() && self.removed.is_empty() && self.in_flight.is_empty() && self.deleted.is_empty()
    }

    /// The differential `parts` make, in chunk order: what a receiver applies once it holds them all.
    pub fn joined(parts: impl IntoIterator<Item = Delta>) -> Delta {
        let mut whole = Delta::default();
        for p in parts {
            whole.added.extend(p.added);
            whole.changed.extend(p.changed);
            whole.removed.extend(p.removed);
            whole.in_flight.extend(p.in_flight);
            whole.deleted.extend(p.deleted);
            whole.mesh_id = whole.mesh_id.or(p.mesh_id);
        }
        whole
    }

    /// A differential chunk's body (`TopologyDelta` data) as a receiver reads it: an added node is held
    /// of fabric `fabric` (its envelope's) and `mesh` (its frame's).
    pub fn of_frame(f: &Frame, fabric: &rdm_core::FabricId) -> Option<Delta> {
        let Frame::TopologyDelta { mesh, mesh_id, added, changed, removed, in_flight, deleted, .. } = f.data() else { return None };
        Some(Delta {
            added: added.iter().map(|e| e.digest(fabric, mesh)).collect(),
            changed: changed.clone(),
            removed: removed.clone(),
            in_flight: in_flight.clone(),
            deleted: deleted.clone(),
            mesh_id: mesh_id.clone(),
        })
    }
}

/// One item of a differential, as chunks carry it.
#[derive(Clone)]
enum DeltaItem {
    Added(NodeEntry),
    Changed(NodeStatusEntry),
    Removed(NodeBirth),
    InFlight(LifecycleOp),
    Deleted(LifecycleOp),
}

/// The chunks of `delta`, the differential of `mesh` from `base_version` to `version`, authored by
/// the forwarding mesh-primary `author` at `now_ms` (gossip.md §3.1 `TopologyDelta`): each chunk
/// encodes within one gossip message, sized as a complete message with the widest index and count,
/// and an empty differential is one empty chunk. `Err` names the encoded size of each node entry,
/// overlay or departure that fits no message on its own: the differential is not sent.
pub fn delta_chunks(mesh: &str, base_version: u64, version: u64, delta: &Delta, author: &crate::envelope::Birth, now_ms: u64) -> Result<Vec<Frame>, Vec<usize>> {
    let items: Vec<DeltaItem> = delta.added.iter().map(|d| DeltaItem::Added(NodeEntry::of(d)))
        .chain(delta.changed.iter().cloned().map(DeltaItem::Changed))
        .chain(delta.removed.iter().cloned().map(DeltaItem::Removed))
        .chain(delta.in_flight.iter().cloned().map(DeltaItem::InFlight))
        .chain(delta.deleted.iter().cloned().map(DeltaItem::Deleted))
        .collect();
    let shape = |run: &[DeltaItem], chunk_index: u32, chunk_count: u32| {
        let (mut added, mut changed, mut removed, mut in_flight, mut deleted) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for it in run.iter().cloned() {
            match it {
                DeltaItem::Added(v) => added.push(v),
                DeltaItem::Changed(v) => changed.push(v),
                DeltaItem::Removed(v) => removed.push(v),
                DeltaItem::InFlight(v) => in_flight.push(v),
                DeltaItem::Deleted(v) => deleted.push(v),
            }
        }
        Frame::TopologyDelta { mesh: mesh.to_string(), mesh_id: delta.mesh_id.clone(), base_version, chunk_index, chunk_count, added, changed, removed, in_flight, deleted }.authored_delta(author, version, now_ms)
    };
    let (sized, refused) = rdm_transport::chunking::pack_in_order(items, |run: &[DeltaItem]| {
        let bytes = shape(run, u32::MAX, u32::MAX).encode();
        if bytes.len() > MAX_FRAME { Err(bytes.len()) } else { Ok(bytes.into()) }
    });
    if !refused.is_empty() {
        return Err(refused);
    }
    let runs: Vec<Vec<DeltaItem>> = sized.into_iter().map(|(run, _)| run).collect();
    let runs = if runs.is_empty() { vec![Vec::new()] } else { runs };
    let count = runs.len() as u32;
    Ok(runs.iter().enumerate().map(|(k, run)| shape(run, k as u32, count)).collect())
}

/// A source primary's version counter: `topology_version` bumps when the normalized projection
/// changes and never otherwise (gossip.md §3.1). The first projection observed is version 1.
/// `snapshot_id` is a counter of this publisher's own, never a clock reading.
#[derive(Debug, Default)]
pub struct SourceVersion {
    version: u64,
    last: Option<Full>,
    snapshots: Arc<AtomicU64>,
}

impl SourceVersion {
    pub(crate) fn with_publication_counter(snapshots: Arc<AtomicU64>) -> Self {
        Self { version: 0, last: None, snapshots }
    }

    /// Continue the mesh's version from `version`, the last one this node holds of its mesh, with
    /// `full` its topology (Q391: a node-admin taking the mesh-primary role continues the mesh's
    /// version): an unchanged topology is published at `version`, a changed one at the next. A
    /// version below the one this counter holds moves nothing.
    pub fn continue_from(&mut self, version: u64, full: Full) {
        if version > self.version {
            self.version = version;
            self.last = Some(full);
        }
    }

    /// The version of `full`: the held one, or the next when `full` is a different topology.
    pub fn observe(&mut self, full: &Full) -> u64 {
        if self.last.as_ref().is_none_or(|l| !l.same_topology(full)) {
            self.version += 1;
            self.last = Some(full.clone());
        }
        self.version
    }

    /// Whether observing `full` would bump the version: it is a different topology from the last
    /// one observed, or none was observed yet. Nothing moves.
    pub fn would_bump(&self, full: &Full) -> bool {
        self.last.as_ref().is_none_or(|l| !l.same_topology(full))
    }

    /// The version of the last topology observed, 0 before any.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The next snapshot id of this publisher.
    pub fn next_snapshot_id(&mut self) -> u64 {
        self.snapshots.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// `full`'s one list of nodes as the chunks of ONE aggregate: `shape(nodes, chunk_index,
/// chunk_count)` builds each frame. The list is split only when it does not fit one message (Q390):
/// each chunk encodes within one gossip message (sized with the widest index and count, so a
/// filled-in chunk never overflows), and an empty list is one empty chunk: an empty Mesh is a
/// complete aggregate.
pub fn chunks_of(full: &Full, shape: impl Fn(Vec<MeshDigest>, u32, u32) -> Frame) -> Vec<Frame> {
    chunks_measured(full, shape, |f| f.encode())
}

/// Split a complete list of nodes using the exact complete-message encoding of its channel.
/// Gossip uses its envelope; a directed read measures its own reply schema.
pub fn chunks_measured<T>(full: &Full, shape: impl Fn(Vec<MeshDigest>, u32, u32) -> T, encode: impl Fn(&T) -> Vec<u8>) -> Vec<T> {
    let (sized, refused) = rdm_transport::chunking::pack_in_order(full.digests(), |run: &[MeshDigest]| {
        let bytes = encode(&shape(run.to_vec(), u32::MAX, u32::MAX));
        if bytes.len() > MAX_FRAME { Err(bytes.len()) } else { Ok(bytes.into()) }
    });
    for len in refused {
        tracing::info_span!("rdm.mesh.topology.reject.via-item-too-large", frame_bytes = len, limit = MAX_FRAME)
            .in_scope(|| tracing::warn!("a node entry fits no complete message"));
    }
    let sized: Vec<_> = sized.into_iter().map(|(run, _)| run).collect();
    let runs = if sized.is_empty() { vec![Vec::new()] } else { sized };
    let count = runs.len() as u32;
    runs.into_iter().enumerate().map(|(k, run)| shape(run, k as u32, count)).collect()
}

/// Whether `offered` is another mesh than `held` under the same name: a mesh is created once, so a
/// later mesh of that name has another id, and the held mesh's version orders nothing of it (gossip.md
/// "Mesh delete"). An id unknown on either side is the same mesh.
pub fn another_mesh(held: Option<&rdm_core::MeshId>, offered: Option<&rdm_core::MeshId>) -> bool {
    matches!((held, offered), (Some(h), Some(o)) if h != o)
}

/// One source Mesh's one list of nodes as a node holds or serves it: a topology read's answer
/// (gossip.md §3.3: reads initialise and repair views).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSnapshot {
    /// The source mesh's name.
    pub mesh: String,
    /// The source mesh's id, as its topology's frame named it.
    pub mesh_id: Option<rdm_core::MeshId>,
    /// The mesh's version, whichever mesh-primary sent it (Q389).
    pub topology_version: u64,
    /// The nodes, each with its status: a node under a stop or delete, or deleted, says so by it (Q390).
    pub digests: Vec<MeshDigest>,
}

impl SourceSnapshot {
    /// The snapshot as a [`Full`].
    pub fn full(&self) -> Full {
        Full::new(self.digests.clone(), Vec::new(), Vec::new()).of_mesh(self.mesh_id.clone())
    }
}

/// One chunk of a mesh's list of nodes, as a receiver reads it from a `TopologyFull` frame or a
/// topology read's reply.
#[derive(Debug, Clone)]
pub struct Chunk {
    /// The source mesh's name.
    pub mesh: String,
    /// The source mesh's id, as its frame names it.
    pub mesh_id: Option<rdm_core::MeshId>,
    /// The aggregate's author, its envelope's `source`; `None` for a topology read, whose chunks
    /// belong to that one answer.
    pub author: Option<crate::envelope::Birth>,
    /// The mesh's version: the aggregate's envelope `sequence` (Q389), or the read's `topology_version`.
    pub topology_version: u64,
    /// When it was published, in Rafka-time: the aggregate's envelope `time`, or the rafka-time a
    /// topology read's answerer served.
    pub time_ms: u64,
    /// This chunk's index within the list.
    pub chunk_index: u32,
    /// The number of chunks in the list.
    pub chunk_count: u32,
    /// The nodes this chunk carries.
    pub digests: Vec<MeshDigest>,
    /// The runtime and data dir a topology read carried of each birth it serves (hydration); none on gossip.
    pub runtimes: Vec<(rdm_core::NodeId, rdm_core::IncarnationId, rdm_core::NodeRuntime)>,
}

impl Chunk {
    /// The author as spans name it: its exact birth, or the read that answered it.
    pub fn author_name(&self) -> String {
        self.author.as_ref().map(|b| format!("{}@{}", b.node, b.incarnation)).unwrap_or_else(|| "topology-read".into())
    }
}

/// A complete list of nodes, installed.
#[derive(Debug, Clone)]
pub struct Install {
    /// The source mesh's name.
    pub mesh: String,
    /// The mesh-primary that authored the installed aggregate (its envelope's `source`), as spans and
    /// the forwarder name it; `None` when a topology read installed it and no author of that mesh's
    /// version was heard.
    pub publisher: Option<PublisherId>,
    /// The installed `topology_version`.
    pub topology_version: u64,
    /// When the installed list was published, in Rafka-time.
    pub time_ms: u64,
    /// The installed topology.
    pub full: Full,
    /// Nothing of this mesh was held before: the first topology of it this node installed.
    pub first: bool,
    /// This source was desynchronized and is resumed by this snapshot.
    pub resumed: bool,
    /// A new snapshot publication or directed read at the held material version was heard.
    /// A repeated publication's chunks never install and never return this flag.
    pub refreshed: bool,
}

/// Why a chunk or snapshot was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// An identical chunk or complete publication is already held; no effect is repeated.
    Duplicate,
    /// The held revision or chunk identity has different content; nothing is installed.
    Inconsistent,

    /// The chunk names an index outside its count, or a count that disagrees with its snapshot's.
    /// The chunk is malformed.
    MalformedChunk {
        /// The chunk's index.
        chunk_index: u32,
        /// The chunk's count.
        chunk_count: u32,
        /// What is wrong with it.
        why: &'static str,
    },
    /// A topology read chunk below the version this node installed of the mesh: the receive path
    /// ignored it (R-RX3, `crate::receive_order`).
    OlderThanHeld {
        /// The version held.
        held_version: u64,
        /// The version offered.
        offered_version: u64,
    },
    /// The chunk belongs to an older version than the one being assembled from the same author.
    OlderThanPending {
        /// The version being assembled.
        pending_version: u64,
        /// The version the chunk names.
        offered_version: u64,
    },
}

/// What a chunk did.
#[derive(Debug, Clone)]
pub enum Taken {
    /// The snapshot is not complete: nothing is installed and no version moved.
    /// Chunks are still missing.
    Waiting {
        /// The chunks held.
        held: u32,
        /// The chunks the snapshot has.
        of: u32,
    },
    /// The snapshot completed and was installed.
    Installed(Box<Install>),
    /// The chunk was refused.
    Refused(Refusal),
}

/// Why a delta did not apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Gap {
    /// No snapshot of this source is held.
    NoBaseline,
    /// The held version is not the delta's base.
    Version {
        /// The version held.
        held: u64,
        /// The version the delta moves from.
        base: u64,
    },
    /// The source was already desynchronized: its deltas are not applied until a snapshot or a top-up.
    AlreadyDesynced,
    /// A node of this mesh held stopped is heard again, by a Digest newer than its stop: a sync issue,
    /// pulled from the mesh-primary by a read, which ends the stop once that primary took its
    /// `node.joined` (gossip.md §3.3, Q387).
    StoppedBirthHeard,
    /// A node-admin holds a birth of its mesh with no runtime fact of it: its mesh-primary took the
    /// fact from the birth's join report, and a read of its own mesh from that primary serves it
    /// (Q400); with no live primary to read from (its primary record names a birth proven gone, or it
    /// is its mesh's primary), the birth's record in node-storage holds it (Q404 A). Pulled or read
    /// again on the one backoff rule until it is held.
    NoRuntimeFact,
    /// A differential's `changed` entry names a birth this node does not hold at the differential's
    /// base: what it holds of the mesh at that version is not the forwarder's, a sync issue topped up
    /// by a read (gossip.md §3.3).
    ChangedBirthNotHeld,
}

impl std::fmt::Display for Gap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Gap::NoBaseline => f.write_str("no-baseline"),
            Gap::Version { .. } => f.write_str("version-gap"),
            Gap::AlreadyDesynced => f.write_str("already-desynced"),
            Gap::StoppedBirthHeard => f.write_str("stopped-birth-heard"),
            Gap::NoRuntimeFact => f.write_str("no-runtime-fact"),
            Gap::ChangedBirthNotHeld => f.write_str("changed-birth-not-held"),
        }
    }
}

/// What a delta did.
#[derive(Debug, Clone)]
pub enum Moved {
    /// Below the actual source version; no delta is applied.
    Stale,
    /// Equal delta identity/revision with conflicting content; nothing is applied.
    Inconsistent,

    /// Applied at exactly its base: the held version is now `topology_version`.
    /// The delta applied.
    Applied {
        /// The source mesh's name.
        mesh: String,
        /// The version it moved from.
        base_version: u64,
        /// The version it moved to.
        topology_version: u64,
        /// The delta applied.
        delta: Delta,
        /// The births it added or changed, as the receiver now holds them.
        moved: Vec<MeshDigest>,
    },
    /// A chunk of a differential not yet complete: nothing applied, no version moved.
    Waiting {
        /// The chunks held.
        held: u32,
        /// The chunks the differential has.
        of: u32,
    },
    /// The chunk names an index outside its count, or a count other than its differential's.
    Malformed {
        /// The chunk's index.
        chunk_index: u32,
        /// The chunk's count.
        chunk_count: u32,
    },
    /// The held version is this delta's: a copy of the delta held. A no-op.
    Duplicate {
        /// The version held.
        held_version: u64,
    },
    /// Not applied: the source is desynchronized and its node tops up.
    /// The source is desynchronized and its node tops up.
    Desynced {
        /// The source mesh's name.
        mesh: String,
        /// Why the delta did not apply.
        gap: Gap,
    },
}

/// One chunk of a differential, as a receiver reads it from a `TopologyDelta` frame and its envelope.
#[derive(Debug, Clone)]
pub struct DeltaChunk {
    /// The peer mesh's name.
    pub mesh: String,
    /// The forwarding mesh-primary that authored it: the envelope's `source`.
    pub author: crate::envelope::Birth,
    /// The version a receiver must hold for the differential to apply.
    pub base_version: u64,
    /// The version it moves the mesh to: the envelope's `sequence`.
    pub version: u64,
    /// This chunk's index within the differential.
    pub chunk_index: u32,
    /// The number of chunks in the differential.
    pub chunk_count: u32,
    /// What this chunk carries.
    pub delta: Delta,
}

impl DeltaChunk {
    /// The chunk an authored `TopologyDelta` is: its author and version from its envelope, an added
    /// node held of the author's fabric. `None` for another frame or one with no authored context.
    pub fn of_frame(f: &Frame) -> Option<Self> {
        let (Frame::TopologyDelta { mesh, base_version, chunk_index, chunk_count, .. }, Some(c), Some(version)) = (f.data(), f.context(), f.delta_version()) else { return None };
        Some(Self { mesh: mesh.clone(), author: c.source.clone(), base_version: *base_version, version, chunk_index: *chunk_index, chunk_count: *chunk_count, delta: Delta::of_frame(f, &c.source.fabric)? })
    }
}

/// A differential being assembled: its mesh, its base and its version, and who authored it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DeltaKey {
    mesh: String,
    base_version: u64,
    version: u64,
    author: crate::envelope::Birth,
}

#[derive(Debug, Clone)]
struct PendingDelta {
    count: u32,
    chunks: BTreeMap<u32, Delta>,
}

/// The birth that sent a held version, as the receiver's shared records name it: never compared to
/// order anything (Q389).
pub(crate) fn sent_by(author: &crate::envelope::Birth) -> PublisherId {
    PublisherId { node_id: author.node.clone(), incarnation: author.incarnation.clone() }
}

/// The chunk set a chunk belongs to: one mesh's list from one author (its version is the set's).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SourceKey {
    mesh: String,
    author: Option<crate::envelope::Birth>,
    // Read chunk ordinals belong to that directed answer, not the author's gossip publication.
    from_read: bool,
}

/// One (mesh, version, author) chunk set: its chunks, and the latest publication time among them.
/// Every publication of one version by one author lists the same nodes, so its chunks are one set.
#[derive(Debug, Clone)]
struct Pending {
    author: Option<crate::envelope::Birth>,
    version: u64,
    time_ms: u64,
    count: u32,
    chunks: BTreeMap<u32, Vec<MeshDigest>>,
}

#[derive(Debug)]
struct Held {
    publisher: Option<PublisherId>,
    version: u64,
    full: Full,
    desynced: Option<Gap>,
    publication: Option<Pending>,
    last_delta: Option<(u64, Delta)>,
    /// Installed by a read this node made of another mesh's node (the fabric-primary at its start):
    /// what this node holds, not what its own Mesh was given. A later delta or gossip install clears it.
    peer_read: bool,
}

fn same_parts(parts: &[MeshDigest], c: &Chunk) -> bool {
    Full::new(parts.to_vec(), Vec::new(), Vec::new()).same_topology(&Full::new(c.digests.clone(), Vec::new(), Vec::new()))
}

fn same_delta(a: &Delta, b: &Delta) -> bool {
    let removed = |d: &Delta| d.removed.iter().map(|b| (b.node_id.to_string(), b.incarnation.to_string())).collect::<BTreeSet<_>>();
    let changed = |d: &Delta| d.changed.iter().map(|c| (c.node_id.to_string(), c.incarnation.to_string(), format!("{:?} {:?}", c.status, c.primary))).collect::<BTreeSet<_>>();
    removed(a) == removed(b)
        && changed(a) == changed(b)
        && Full::new(a.added.clone(), a.in_flight.clone(), a.deleted.clone()).same_topology(&Full::new(b.added.clone(), b.in_flight.clone(), b.deleted.clone()))
}

/// Borrow the complete image under ReceiveOrder's shared installation lock; inspecting a chunk
/// does not copy the installed publication just to compare its version or completed content: the
/// author's label, the version, the list, and the (author, time) of the last aggregate installed.
pub(crate) type InstalledProjection<'a> = (Option<&'a PublisherId>, u64, &'a Full, Option<&'a (crate::envelope::Birth, u64)>);

/// A node's held cross-Mesh projection, one source Mesh at a time.
///
/// CONTRACT (gossip.md §3.1, §3.3, proofs 25-27): an aggregate is installed only once EVERY chunk
/// of its (mesh, version, author) is held; until then the held list and the held version stand and no delta
/// is derived from a partial. A delta applies only when the held version is exactly its
/// `base_version`; a copy of the delta held (its `topology_version` is the held one) is a no-op;
/// an ahead-base delta is a gap that desynchronizes that source alone. Classification against the
/// installed floor and content happens under this receiver's installation lock. A repeated gossip
/// publication installs nothing and never completes repair; a directed read can resume the source
/// at its held version. No delta is saved for later; the topology caller requests the repair.
#[derive(Debug, Default)]
pub struct SnapshotReceiver {
    pending: HashMap<SourceKey, Pending>,
    /// The chunks of each differential not yet complete, by (mesh, base, version, author).
    pending_deltas: HashMap<DeltaKey, PendingDelta>,
    held: HashMap<String, Held>,
    /// Sources a delta named before any snapshot of them was held: desynchronized, with no baseline.
    unbased: BTreeMap<String, Gap>,
    /// The highest version of each source a delta reached while it was dropped with its source
    /// already desynchronized (a pull in flight): a copy installed older than it is still behind (gossip.md §3.3, Q130: "A node
    /// that installs a pulled copy older than a delta it dropped while the pull was in flight pulls
    /// again"). Only the version is kept, never the delta.
    dropped: BTreeMap<String, u64>,
}

impl SnapshotReceiver {
    /// A gossip chunk. A new complete publication at the held material version (a later `time` of
    /// the same author) carries fresh hearing evidence; its exact repeat does nothing, including no
    /// repair completion. Repeated chunks of an unfinished set leave its other chunks admissible.
    pub fn take_chunk(&mut self, c: Chunk) -> Taken {
        self.chunk(c, false, None)
    }

    /// The directed topology read is the repair completion: an identical held version may resume
    /// a desynchronized source. It still cannot replace different content at the same revision.
    pub fn take_read_chunk(&mut self, c: Chunk) -> Taken {
        self.chunk(c, true, None)
    }

    /// Classify against the node's complete installed image, including an install on another
    /// channel. This receiver's pending chunks still assemble independently of that image.
    pub(crate) fn take_chunk_against(&mut self, c: Chunk, from_read: bool, installed: Option<InstalledProjection<'_>>) -> Taken {
        self.chunk(c, from_read, installed)
    }

    fn chunk(&mut self, c: Chunk, from_read: bool, installed: Option<InstalledProjection<'_>>) -> Taken {
        use crate::receive_order::{publication_duplicate, publication_inconsistent, publication_stale};
        if c.chunk_count == 0 || c.chunk_index >= c.chunk_count {
            return Taken::Refused(Refusal::MalformedChunk { chunk_index: c.chunk_index, chunk_count: c.chunk_count, why: "the index is outside the count" });
        }
        let author = c.author_name();
        let identity = format!("{} {} topology_version={} time_ms={} chunk_index={}", c.mesh, author, c.topology_version, c.time_ms, c.chunk_index);
        let conflict = || {
            publication_inconsistent("mesh-topology", &c.mesh, &author, c.topology_version);
            Taken::Refused(Refusal::Inconsistent)
        };
        let duplicate = || {
            publication_duplicate("mesh-topology", &identity);
            Taken::Refused(Refusal::Duplicate)
        };
        // A chunk of a publication this receiver installed: the same author, no later in time.
        let installed_publication = |p: &Pending| !from_read && p.author == c.author && c.time_ms <= p.time_ms;
        // The mesh's version orders its topology, whichever mesh-primary sent it (Q389); a later mesh of
        // the same name is another mesh, which the held one's version orders nothing of.
        let offered_id = c.mesh_id.clone();
        if let Some(h) = self.held.get(&c.mesh).filter(|h| !another_mesh(h.full.mesh_id().as_ref(), offered_id.as_ref())) {
            if c.topology_version < h.version {
                publication_stale("mesh-topology", &c.mesh, &author, h.version, c.topology_version);
                return Taken::Refused(Refusal::OlderThanHeld { held_version: h.version, offered_version: c.topology_version });
            }
            if !from_read && c.topology_version == h.version {
                if let Some(p) = h.publication.as_ref().filter(|p| installed_publication(p)) {
                    if p.count != c.chunk_count { return conflict(); }
                    if let Some(parts) = p.chunks.get(&c.chunk_index) {
                        return if same_parts(parts, &c) { duplicate() } else { conflict() };
                    }
                }
            }
        }
        let key = SourceKey { mesh: c.mesh.clone(), author: c.author.clone(), from_read };
        if let Some(p) = self.pending.get(&key) {
            if c.topology_version < p.version {
                publication_stale("mesh-topology", &c.mesh, &author, p.version, c.topology_version);
                return Taken::Refused(Refusal::OlderThanPending { pending_version: p.version, offered_version: c.topology_version });
            }
            if c.topology_version > p.version { self.pending.remove(&key); }
            else {
                if p.count != c.chunk_count { return conflict(); }
                if let Some(parts) = p.chunks.get(&c.chunk_index) {
                    return if same_parts(parts, &c) { duplicate() } else { conflict() };
                }
            }
        }
        let p = self.pending.entry(key.clone()).or_insert_with(|| Pending { author: c.author.clone(), version: c.topology_version, time_ms: c.time_ms, count: c.chunk_count, chunks: BTreeMap::new() });
        p.time_ms = p.time_ms.max(c.time_ms);
        p.chunks.insert(c.chunk_index, c.digests.clone());
        if (p.chunks.len() as u32) < p.count { return Taken::Waiting { held: p.chunks.len() as u32, of: p.count }; }
        let publication = self.pending.remove(&key).expect("just assembled");
        let digests: Vec<MeshDigest> = publication.chunks.values().flatten().cloned().collect();
        let full = Full::new(digests, Vec::new(), Vec::new()).of_mesh(c.mesh_id.clone());
        let offered_id = full.mesh_id();
        if let Some(h) = self.held.get(&c.mesh).filter(|h| h.version == c.topology_version && !another_mesh(h.full.mesh_id().as_ref(), offered_id.as_ref())) {
            if !h.full.same_topology(&full) { return conflict(); }
        }
        if let Some((_, _, installed_full, last)) = installed.filter(|(_, v, f, _)| *v == c.topology_version && !another_mesh(f.mesh_id().as_ref(), offered_id.as_ref())) {
            if !installed_full.same_topology(&full) { return conflict(); }
            if !from_read && last.is_some_and(|(a, t)| Some(a) == publication.author.as_ref() && publication.time_ms <= *t) { return duplicate(); }
        }
        self.install(c.mesh, publication, full, from_read)
    }

    fn install(&mut self, mesh: String, publication: Pending, full: Full, from_read: bool) -> Taken {
        let (version, time_ms) = (publication.version, publication.time_ms);
        // A later mesh of the same name is another mesh: what was held of the earlier one is dropped.
        if self.held.get(&mesh).is_some_and(|h| another_mesh(h.full.mesh_id().as_ref(), full.mesh_id().as_ref())) {
            self.held.remove(&mesh);
            self.dropped.remove(&mesh);
        }
        let unbased = self.unbased.contains_key(&mesh);
        let (first, resumed, refreshed) = match self.held.get(&mesh) {
            None => (true, unbased, false),
            Some(h) => (false, h.desynced.is_some() || unbased, version == h.version),
        };
        let same = self.held.get(&mesh).filter(|h| h.version == version);
        let last_delta = same.and_then(|h| h.last_delta.clone());
        let author = publication.author.clone();
        // The author's label: the aggregate's author, else (a read) the one held of the mesh.
        let publisher = author.as_ref().map(|b| PublisherId { node_id: b.node.clone(), incarnation: b.incarnation.clone() }).or_else(|| self.held.get(&mesh).and_then(|h| h.publisher.clone()));
        let retained = if from_read { same.and_then(|h| h.publication.clone()) } else { Some(publication) };
        self.unbased.remove(&mesh);
        // An install drops what was assembling below its version and its own set; a read, the repair
        // completion, also every set at its version: their stragglers install nothing.
        self.pending.retain(|k, p| !(k.mesh == mesh && (p.version < version || (p.version == version && (from_read || k.author == author)))));
        // A copy older than a delta dropped while the source was desynchronized is still behind it:
        // the source stays desynchronized, and its holder pulls again (Q130).
        let behind = self.dropped.get(&mesh).copied().filter(|d| *d > version);
        if behind.is_none() {
            self.dropped.remove(&mesh);
        }
        self.held.insert(mesh.clone(), Held { publisher: publisher.clone(), version, full: full.clone(), desynced: behind.map(|d| Gap::Version { held: version, base: d }), publication: retained, last_delta, peer_read: false });
        Taken::Installed(Box::new(Install { mesh, publisher, topology_version: version, time_ms, full, first, resumed, refreshed }))
    }

    /// Use the actual installed source image as the delta base. A channel-local assembly cannot
    /// turn an exact-base delta into a gap after another channel installed that same source.
    pub(crate) fn take_delta_chunk_against(&mut self, c: DeltaChunk, installed: Option<InstalledProjection<'_>>) -> Moved {
        if let Some((publisher, installed_version, full, _)) = installed {
            if self.held.get(&c.mesh).is_none_or(|local| local.version < installed_version) {
                self.held.insert(c.mesh.clone(), Held { publisher: publisher.cloned(), version: installed_version, full: full.clone(), desynced: None, publication: None, last_delta: None, peer_read: false });
            }
        }
        self.take_delta_chunk(c)
    }

    /// One chunk of a differential (gossip.md §3.1 `TopologyDelta`). It is judged against what is held
    /// at once: a chunk of a version held, or below it, is a copy or stale, and one whose base is not
    /// the version held shows the gap before its other chunks arrive. Otherwise it is held until every
    /// chunk of its (mesh, base, version, author) is, and the differential they make is then applied
    /// at exactly its base ([`Self::take_delta`]); until then nothing is applied and no version moves.
    pub fn take_delta_chunk(&mut self, c: DeltaChunk) -> Moved {
        if c.chunk_count == 0 || c.chunk_index >= c.chunk_count {
            return Moved::Malformed { chunk_index: c.chunk_index, chunk_count: c.chunk_count };
        }
        let mesh = c.mesh.clone();
        let another = self.held.get(&mesh).is_some_and(|h| another_mesh(h.full.mesh_id().as_ref(), c.delta.mesh_id.as_ref()));
        match self.held.get(&mesh).filter(|_| !another) {
            Some(h) if c.version <= h.version || h.desynced.is_some() || h.version != c.base_version => {
                return self.take_delta(&mesh, &c.author, c.base_version, c.version, &c.delta);
            }
            None if c.base_version != 0 => return self.take_delta(&mesh, &c.author, c.base_version, c.version, &c.delta),
            _ => {}
        }
        // What is held moved past every older assembly of this mesh.
        let floor = self.held.get(&mesh).filter(|_| !another).map(|h| h.version);
        self.pending_deltas.retain(|k, _| k.mesh != mesh || floor.is_none_or(|f| k.version > f));
        let key = DeltaKey { mesh: mesh.clone(), base_version: c.base_version, version: c.version, author: c.author.clone() };
        let identity = format!("{mesh} {} base_version={} version={} chunk_index={}", c.author.uri(), c.base_version, c.version, c.chunk_index);
        let p = self.pending_deltas.entry(key.clone()).or_insert_with(|| PendingDelta { count: c.chunk_count, chunks: BTreeMap::new() });
        if p.count != c.chunk_count {
            return Moved::Malformed { chunk_index: c.chunk_index, chunk_count: c.chunk_count };
        }
        if let Some(held) = p.chunks.get(&c.chunk_index) {
            if same_delta(held, &c.delta) {
                crate::receive_order::publication_duplicate("mesh-topology-delta", &identity);
                return Moved::Duplicate { held_version: floor.unwrap_or_default() };
            }
            crate::receive_order::publication_inconsistent("mesh-topology-delta", &mesh, &c.author.uri(), c.version);
            return Moved::Inconsistent;
        }
        p.chunks.insert(c.chunk_index, c.delta);
        if (p.chunks.len() as u32) < p.count {
            return Moved::Waiting { held: p.chunks.len() as u32, of: p.count };
        }
        let assembled = self.pending_deltas.remove(&key).expect("just assembled");
        let delta = Delta::joined(assembled.chunks.into_values());
        self.take_delta(&mesh, &c.author, c.base_version, c.version, &delta)
    }

    /// A differential, whole or as one chunk judged against what is held: applied only at exactly its
    /// base. One dropped because its source was already desynchronized (its pull in flight) leaves its
    /// version as the floor that pull must reach.
    pub fn take_delta(&mut self, mesh: &str, author: &crate::envelope::Birth, base_version: u64, topology_version: u64, delta: &Delta) -> Moved {
        let moved = self.take_delta_at_base(mesh, author, base_version, topology_version, delta);
        if matches!(moved, Moved::Desynced { gap: Gap::AlreadyDesynced, .. }) {
            let floor = self.dropped.entry(mesh.to_string()).or_insert(topology_version);
            *floor = (*floor).max(topology_version);
        }
        moved
    }

    fn take_delta_at_base(&mut self, mesh: &str, author: &crate::envelope::Birth, base_version: u64, topology_version: u64, delta: &Delta) -> Moved {
        let source = &author.uri();
        // A differential from version 0 is a mesh's first into this mesh channel: its forwarder held
        // nothing of it before, and a node that holds nothing of it starts from nothing.
        let another = self.held.get(mesh).is_some_and(|h| another_mesh(h.full.mesh_id().as_ref(), delta.mesh_id.as_ref()));
        if base_version == 0 && (another || !self.held.contains_key(mesh)) {
            self.unbased.remove(mesh);
            self.dropped.remove(mesh);
            self.held.insert(mesh.to_string(), Held { publisher: Some(sent_by(author)), version: 0, full: Full::default(), desynced: None, publication: None, last_delta: None, peer_read: false });
        }
        let Some(h) = self.held.get_mut(mesh) else {
            let already = self.unbased.contains_key(mesh);
            self.unbased.entry(mesh.to_string()).or_insert(Gap::NoBaseline);
            return Moved::Desynced { mesh: mesh.to_string(), gap: if already { Gap::AlreadyDesynced } else { Gap::NoBaseline } };
        };
        // A copy of the delta held: gossip can deliver one delta twice, and a top-up may have brought
        // this node to it. Neither is a gap. An earlier one never reaches here (R-RX3, the receive path).
        if topology_version < h.version {
            crate::receive_order::publication_stale("mesh-topology-delta", mesh, source, h.version, topology_version);
            return Moved::Stale;
        }
        if h.version == topology_version {
            // The differential applied, or one chunk of it: what it carries is held already.
            let same = h.last_delta.as_ref().is_some_and(|(base, applied)| *base == base_version && same_delta(applied, delta))
                || { let mut candidate = h.full.clone(); candidate.apply(delta).is_ok() && h.full.same_topology(&candidate) };
            if !same {
                crate::receive_order::publication_inconsistent("mesh-topology-delta", mesh, source, topology_version);
                return Moved::Inconsistent;
            }
            crate::receive_order::publication_duplicate("mesh-topology-delta", &format!("{mesh} {} base_version={base_version} topology_version={topology_version}", author.uri()));
            return Moved::Duplicate { held_version: h.version };
        }
        if h.desynced.is_some() {
            return Moved::Desynced { mesh: mesh.to_string(), gap: Gap::AlreadyDesynced };
        }
        if h.version != base_version {
            let gap = Gap::Version { held: h.version, base: base_version };
            return Self::desync(mesh, h, gap);
        }
        let moved = match h.full.apply(delta) {
            Ok(moved) => moved,
            Err(gap) => return Self::desync(mesh, h, gap),
        };
        h.version = topology_version;
        h.publisher = Some(sent_by(author));
        h.publication = None;
        h.peer_read = false;
        h.last_delta = Some((base_version, delta.clone()));
        Moved::Applied { mesh: mesh.to_string(), base_version, topology_version, delta: delta.clone(), moved }
    }

    fn desync(mesh: &str, h: &mut Held, gap: Gap) -> Moved {
        let already = h.desynced.is_some();
        if !already {
            h.desynced = Some(gap.clone());
        }
        Moved::Desynced { mesh: mesh.to_string(), gap: if already { Gap::AlreadyDesynced } else { gap } }
    }

    /// Mark the source `mesh` desynchronized by `gap`, so a top-up pulls it, held or not yet held:
    /// `false` when it is desynchronized already.
    pub fn mark_desynced(&mut self, mesh: &str, gap: Gap) -> bool {
        match self.held.get_mut(mesh) {
            Some(h) if h.desynced.is_none() => {
                h.desynced = Some(gap);
                true
            }
            Some(_) => false,
            None if self.unbased.contains_key(mesh) => false,
            None => {
                self.unbased.insert(mesh.to_string(), gap);
                true
            }
        }
    }

    /// End the desynchronization of `mesh` when `gap` is what desynchronized it, its cause met by
    /// something other than a read (a birth's runtime fact taken from its node record, Q404 A): a source
    /// desynchronized by another gap stays so until a read installs it. `true` when it ended.
    pub fn end_gap(&mut self, mesh: &str, gap: &Gap) -> bool {
        match self.held.get_mut(mesh) {
            Some(h) if h.desynced.as_ref() == Some(gap) => {
                h.desynced = None;
                true
            }
            Some(_) => false,
            None if self.unbased.get(mesh) == Some(gap) => {
                self.unbased.remove(mesh);
                true
            }
            None => false,
        }
    }

    /// Forget the source projection held of `mesh`: the mesh left, and nothing of it is kept.
    pub fn remove(&mut self, mesh: &str) {
        self.held.remove(mesh);
        self.unbased.remove(mesh);
        self.dropped.remove(mesh);
    }

    /// The version held of `mesh`, with its author's label when one was heard.
    pub fn held_version(&self, mesh: &str) -> Option<(Option<PublisherId>, u64)> {
        self.held.get(mesh).map(|h| (h.publisher.clone(), h.version))
    }

    /// The projection held of `mesh`.
    pub fn held_full(&self, mesh: &str) -> Option<&Full> {
        self.held.get(mesh).map(|h| &h.full)
    }

    /// The meshes whose source is desynchronized, with the gap that desynchronized each.
    pub fn desynced(&self) -> Vec<(String, Gap)> {
        let mut v: Vec<(String, Gap)> = self.held.iter().filter_map(|(m, h)| h.desynced.clone().map(|g| (m.clone(), g))).chain(self.unbased.iter().map(|(m, g)| (m.clone(), g.clone()))).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    /// Every projection held, as the baselines a node serves.
    pub fn snapshots(&self) -> Vec<SourceSnapshot> {
        self.snapshots_of(None)
    }

    /// [`Self::snapshots`] of `mesh` alone when one is named: a source of another mesh is not copied.
    pub fn snapshots_of(&self, mesh: Option<&str>) -> Vec<SourceSnapshot> {
        let mut v: Vec<SourceSnapshot> = self
            .held
            .iter()
            .filter(|(m, h)| h.desynced.is_none() && mesh.is_none_or(|want| m.as_str() == want))
            .map(|(m, h)| SourceSnapshot { mesh: m.clone(), mesh_id: h.full.mesh_id(), topology_version: h.version, digests: h.full.digests() })
            .collect();
        v.sort_by(|a, b| a.mesh.cmp(&b.mesh));
        v
    }

    /// The member at endpoint key `endpoint_id` in the first source, by mesh name, that lists one
    /// ([`Full::member_at`]): what a scan of [`Self::sources`] finds, without copying a source.
    pub fn member_at(&self, endpoint_id: &str) -> Option<MeshDigest> {
        self.held.iter().filter_map(|(m, h)| h.full.member_at(endpoint_id).map(|d| (m, d))).min_by(|a, b| a.0.cmp(b.0)).map(|(_, d)| d.clone())
    }

    /// The source `mesh` was installed by a read of another mesh's node: what this node holds, not
    /// what its Mesh was given ([`Self::sources_in_mesh`] leaves it out).
    pub fn note_peer_read(&mut self, mesh: &str) {
        if let Some(h) = self.held.get_mut(mesh) {
            h.peer_read = true;
        }
    }

    /// What this node holds of each mesh from its own mesh channel, or a read of its own Mesh: the
    /// versions its Mesh was given by its forwarder, as `(mesh, author label, version, full)`. A source
    /// installed by a read of another mesh's node is left out.
    pub fn sources_in_mesh(&self) -> Vec<(String, Option<PublisherId>, u64, Full)> {
        let mut v: Vec<_> = self.held.iter().filter(|(_, h)| !h.peer_read).map(|(m, h)| (m.clone(), h.publisher.clone(), h.version, h.full.clone())).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    /// Every complete source held, as `(mesh, author label, version, full)`.
    pub fn sources(&self) -> Vec<(String, Option<PublisherId>, u64, Full)> {
        let mut v: Vec<_> = self.held.iter().map(|(m, h)| (m.clone(), h.publisher.clone(), h.version, h.full.clone())).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }
}

#[derive(Debug, Clone)]
struct Published {
    version: u64,
    full: Full,
    /// The last differential put into this mesh channel, `(base_version, delta)`: what a
    /// neighbour-up replay sends again, as its chunks.
    last: Option<(u64, Delta)>,
}

/// What the forwarding primary puts on its own Mesh's channel for a peer mesh it holds.
#[derive(Debug)]
pub enum Forward {
    /// The peer mesh moved from the version last published into this Mesh to a newer one: the
    /// differential, as its chunks, in order.
    Delta(Vec<Frame>),
    /// Nothing to say: the same version, an older one, a move that changes nothing a Mesh
    /// member holds, or a differential with a node entry, overlay or departure too large for one
    /// message.
    Nothing(&'static str),
}

/// The forwarding primary's side (gossip.md §3.3): for each peer Mesh it holds, what it last
/// PUBLISHED into its own Mesh. `base_version` is that, so a forwarder's own missed backbone
/// frames never break the chain inside the Mesh; the delta is the difference between the two
/// topologies the forwarder holds, not a replay of the versions between. The version is the peer
/// mesh's, whichever of its mesh-primaries sent it (Q389), and the baseline is the mesh's, whichever
/// forwarder last published into it: a forwarder taking the role starts from what this Mesh already
/// holds ([`Forwarder::start_from`]). Nothing on the mesh channel is a full. A differential that does
/// not fit one message is sent as chunks ([`delta_chunks`]).
#[derive(Debug, Default)]
pub struct Forwarder {
    author: Option<crate::envelope::Birth>,
    published: HashMap<String, Published>,
    snapshots: Arc<AtomicU64>,
    /// Whether this node holds the forwarder role (it is its mesh's primary) now.
    role_on: bool,
    /// The term of the role: bumped on every take and every loss, so a decision made in one term
    /// is recognisably not of the next.
    role_revision: u64,
    /// This primary's own mesh: held as a source, so a topology read serves it, and never put into
    /// the mesh channel, whose nodes publish themselves (gossip.md §2, Q387).
    own_mesh: String,
}

impl Forwarder {
    /// This primary's own mesh (see `own_mesh`).
    pub fn set_own_mesh(&mut self, mesh: &str) { self.own_mesh = mesh.to_string(); }

    /// Supply this publisher's owned exact birth before making a publication.
    pub fn set_author(&mut self, author: crate::envelope::Birth) { self.author = Some(author); }

    fn author(&self) -> &crate::envelope::Birth {
        self.author.as_ref().expect("forwarder requires its supplied author birth")
    }

    pub(crate) fn author_frame(&self, frame: Frame, now_ms: u64) -> Frame {
        assert!(frame.context().is_some() || !matches!(frame.data(), Frame::MeshStatus { .. } | Frame::FabricStatus { .. }), "a forwarded status retains its original author and revision");
        frame.authored(self.author(), 0, now_ms)
    }

    pub(crate) fn publication_counter(&self) -> Arc<AtomicU64> { self.snapshots.clone() }

    /// Forget every baseline: the role was taken or lost.
    pub fn reset(&mut self) {
        self.published.clear();
    }

    /// Take (`true`) or lose (`false`) the forwarder role. A change clears the baseline and begins a
    /// new term; a forwarder taking the role starts from what its Mesh holds ([`Self::start_from`]).
    /// Returns whether the role changed.
    pub fn set_role(&mut self, on: bool) -> bool {
        if self.role_on == on {
            return false;
        }
        self.reset();
        self.role_on = on;
        self.role_revision += 1;
        true
    }

    /// The current term of the forwarder role, while this node holds it.
    pub fn role(&self) -> Option<u64> {
        self.role_on.then_some(self.role_revision)
    }

    /// The baseline on taking the role: `held`, `(mesh, version, topology)`, what this node holds of
    /// each peer mesh from its own mesh channel or a read, the version its Mesh's nodes reached from
    /// the forwarder before it. The next move of each peer mesh is a differential from there; a node
    /// that holds another version repairs by a read (gossip.md §3.3).
    pub fn start_from(&mut self, held: &[(String, u64, Full)]) {
        for (mesh, version, full) in held {
            if mesh != &self.own_mesh {
                self.published.insert(mesh.clone(), Published { version: *version, full: full.clone(), last: None });
            }
        }
    }

    /// The chunks of `delta`, the differential of `mesh` from `base_version` to `version`, or the
    /// span naming each item too large for one message.
    fn delta_frames(&self, me: &str, mesh: &str, base_version: u64, version: u64, delta: &Delta, now_ms: u64) -> Option<Vec<Frame>> {
        match delta_chunks(mesh, base_version, version, delta, self.author(), now_ms) {
            Ok(frames) => Some(frames),
            Err(sizes) => {
                for bytes in sizes {
                    tracing::info_span!("rdm.mesh.topology.reject.via-delta-too-large", node = %me, mesh = %mesh, base_version, topology_version = version, frame_bytes = bytes, limit = MAX_FRAME)
                        .in_scope(|| tracing::warn!("a node entry, overlay or departure fits no message on its own: the differential is not sent; a node behind it repairs by a read"));
                }
                None
            }
        }
    }

    /// The peer `mesh` is now held at `version` (a complete topology). What to forward: the
    /// differential from the version last published into this Mesh, whoever published it, in as many
    /// chunks as it takes; from version 0 when this Mesh was never given the peer mesh. A differential
    /// with one node entry, overlay or departure that fits no message on its own is not sent: the
    /// baseline still moves, the span names it, and a node behind it repairs by a read at the next
    /// differential's base.
    pub fn source(&mut self, me: &str, mesh: &str, version: u64, full: &Full, now_ms: u64) -> Forward {
        if mesh == self.own_mesh {
            // Held for a topology read; its own nodes publish themselves on the mesh channel.
            self.published.insert(mesh.to_string(), Published { version, full: full.clone(), last: None });
            return Forward::Nothing("its own mesh: its nodes publish themselves");
        }
        // Never published into this Mesh, or a later mesh of the same name (another id): from nothing.
        let held = self.published.get(mesh).filter(|p| !another_mesh(p.full.mesh_id().as_ref(), full.mesh_id().as_ref()));
        let first = held.is_none();
        let basis = held.cloned().unwrap_or(Published { version: 0, full: Full::default(), last: None });
        if version == basis.version {
            return Forward::Nothing("the version is the one last published");
        }
        if version < basis.version {
            return Forward::Nothing("the version is older than the one last published");
        }
        let delta = basis.full.delta_to(full);
        // The first differential of a peer mesh is sent even when empty: it gives the Mesh the mesh.
        if !first && delta.is_empty() && version == basis.version + 1 {
            // Nothing a Mesh member holds moved: no frame, and `base_version` stays what receivers hold.
            self.published.insert(mesh.to_string(), Published { version: basis.version, full: full.clone(), last: basis.last });
            return Forward::Nothing("the move changes nothing a member of this Mesh holds");
        }
        let frames = self.delta_frames(me, mesh, basis.version, version, &delta, now_ms);
        self.published.insert(mesh.to_string(), Published { version, full: full.clone(), last: Some((basis.version, delta)) });
        match frames {
            Some(frames) => Forward::Delta(frames),
            None => Forward::Nothing("a node entry, overlay or departure of the differential fits no message"),
        }
    }

    /// Forget what was published of `mesh`: the mesh left.
    pub fn remove(&mut self, mesh: &str) {
        self.published.remove(mesh);
    }

    /// The last differential of each peer mesh, sent again as its chunks (a join, heal or refeed
    /// replay): a node at its base applies it, one that holds it already takes nothing, and one further
    /// behind repairs by a read. The published versions do not move.
    pub fn replay(&mut self, now_ms: u64) -> Vec<Frame> {
        let mut meshes: Vec<String> = self.published.iter().filter(|(m, p)| **m != self.own_mesh && p.last.is_some()).map(|(m, _)| m.clone()).collect();
        meshes.sort();
        let author = self.author().clone();
        meshes
            .into_iter()
            .filter_map(|m| {
                let p = &self.published[&m];
                let (base, delta) = p.last.as_ref()?;
                delta_chunks(&m, *base, p.version, delta, &author, now_ms).ok()
            })
            .flatten()
            .collect()
    }

    /// What this primary has published into its Mesh, as the baselines a top-up serves.
    pub fn snapshots(&self) -> Vec<SourceSnapshot> {
        self.snapshots_of(None)
    }

    /// [`Self::snapshots`] of `mesh` alone when one is named: a source of another mesh is not copied.
    /// A copy names this primary as the birth it is served from.
    pub fn snapshots_of(&self, mesh: Option<&str>) -> Vec<SourceSnapshot> {
        let mut v: Vec<SourceSnapshot> = self
            .published
            .iter()
            .filter(|(m, _)| mesh.is_none_or(|want| m.as_str() == want))
            .map(|(m, p)| SourceSnapshot { mesh: m.clone(), mesh_id: p.full.mesh_id(), topology_version: p.version, digests: p.full.digests() })
            .collect();
        v.sort_by(|a, b| a.mesh.cmp(&b.mesh));
        v
    }

    /// The version last published for `mesh`, when any.
    pub fn published_version(&self, mesh: &str) -> Option<u64> {
        self.published.get(mesh).map(|p| p.version)
    }
}

#[cfg(test)]
mod tests {

    /// CONTRACT (Q389, Q391): a node-admin taking the mesh-primary role continues the mesh's version
    /// from the last one it holds: an unchanged topology is published at that version, a changed one at
    /// the next; a held version below its own count moves nothing. Must NOT happen: a new mesh-primary's
    /// aggregate numbered from 1, below what every node holds.
    #[test]
    fn a_new_mesh_primary_continues_the_mesh_version_from_the_last_it_holds() {
        let empty = Full::new(vec![], vec![], vec![]);
        let mut v = SourceVersion::default();
        v.continue_from(41, empty.clone());
        assert_eq!(v.observe(&empty), 41, "the unchanged topology is published at the version held");
        let changed = Full::new(vec![], vec![], vec![op("node.delete", &rdm_core::NodeId::mint(), &IncarnationId::mint())]);
        assert_eq!(v.observe(&changed), 42, "a change is the next version");
        v.continue_from(7, empty.clone());
        assert_eq!(v.version(), 42, "a lower held version moves nothing");
    }

    use super::*;
    use rdm_core::IncarnationId;

    fn op(action: &str, node_id: &rdm_core::NodeId, incarnation: &IncarnationId) -> LifecycleOp {
        LifecycleOp { r#type: "node".into(), mesh: "mesh1".into(), build_id: "b1".into(), operation: rdm_core::OperationId::new(action, rdm_core::OperationSubject::Birth { node_id: node_id.clone(), incarnation: incarnation.clone() }), node_id: node_id.clone(), incarnation: incarnation.clone(), path: "mesh1.node".into(), name: "2".into(), event_at_rafka_ms: 1, traceparent: None, tracestate: None }
    }

    /// CONTRACT (gossip.md §3.1 NodeStopped: "stays in the in-flight overlay until the next lifecycle
    /// op on the same (node.id, incarnation) … node.delete replaces it with NodeDeleted's deleted
    /// retention"): a delta that carries a birth's departure ends every overlay of that exact birth
    /// at the receiver, its stop included, so the receiver's full equals the publisher's. Must NOT
    /// happen: the stop overlay kept beside the departure, and every later full of that version
    /// refused as inconsistent.
    #[test]
    fn a_departure_ends_every_overlay_of_its_birth_so_the_receiver_holds_the_publishers_full() {
        let (node, inc) = (rdm_core::NodeId::mint(), IncarnationId::mint());
        let stopped = Full::new(vec![], vec![op("node.stop", &node, &inc)], vec![]);
        let deleted = Full::new(vec![], vec![], vec![op("node.delete", &node, &inc)]);
        let mut held = stopped.clone();
        held.apply(&stopped.delta_to(&deleted));
        assert!(held.in_flight().is_empty(), "the departure ended the birth's stop overlay: {:?}", held.in_flight());
        assert!(held.same_topology(&deleted), "the receiver holds the publisher's full");
    }

    /// CONTRACT (gossip.md §3.3, Q130: "A node that installs a pulled copy older than a delta it
    /// dropped while the pull was in flight pulls again"; the red of `node_delete` at e6s8-1918053cb:
    /// mesh1.node.1 dropped mesh2's delta 9 -> 10 while its pull was in flight, installed v9 from that
    /// pull and resumed as current): a source desynchronized by a gap, then sent a delta it drops,
    /// stays desynchronized when the read installs a copy older than that delta, and resumes only at a
    /// copy that reaches it. Must NOT happen: the older read clearing the gap.
    #[test]
    fn a_read_older_than_a_delta_dropped_while_it_was_in_flight_leaves_the_source_desynchronized() {
        let p = PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() };
        let mut rx = SnapshotReceiver::default();
        assert!(matches!(rx.take_chunk(chunk(&p, 7)), Taken::Installed(_)));
        // The 7 -> 8 delta is lost; 8 -> 9 shows the gap, 9 -> 10 is dropped while the pull is in flight.
        assert!(matches!(rx.take_delta("mesh1", &author_of(&p), 8, 9, &Delta::default()), Moved::Desynced { gap: Gap::Version { held: 7, base: 8 }, .. }));
        assert!(matches!(rx.take_delta("mesh1", &author_of(&p), 9, 10, &Delta::default()), Moved::Desynced { gap: Gap::AlreadyDesynced, .. }));
        // The pull answers v9, read before its server installed v10.
        assert!(matches!(rx.take_chunk_against(chunk(&p, 9), true, None), Taken::Installed(_)));
        assert_eq!(rx.desynced().into_iter().map(|(m, _)| m).collect::<Vec<_>>(), vec!["mesh1".to_string()], "a read older than the dropped delta leaves the source behind: it pulls again");
        // The next pull answers v10: the source resumes.
        assert!(matches!(rx.take_chunk_against(chunk(&p, 10), true, None), Taken::Installed(_)));
        assert!(rx.desynced().is_empty(), "a read that reaches the dropped delta resumes the source");
        assert_eq!(rx.held_version("mesh1").map(|(_, v)| v), Some(10));
    }

    /// The birth `p` names, of the test fabric: the author of a differential.
    fn author_of(p: &PublisherId) -> crate::envelope::Birth {
        crate::envelope::Birth { fabric: rdm_core::FabricId::parse("fab000000001").unwrap(), node: p.node_id.clone(), incarnation: p.incarnation.clone() }
    }

    /// A forwarding mesh-primary of mesh1 holding the role, and its birth.
    fn forwarder() -> (Forwarder, crate::envelope::Birth) {
        let author = author_of(&PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() });
        let mut f = Forwarder::default();
        f.set_own_mesh("mesh1");
        f.set_author(author.clone());
        f.set_role(true);
        (f, author)
    }

    /// mesh2's node `name`, ready.
    fn member(name: &str) -> MeshDigest {
        MeshDigest {
            fabric_id: rdm_core::FabricId::parse("fab000000001").unwrap(),
            node: rdm_core::MeshNode { mesh: "mesh2".into(), r#type: "node".into(), node_id: rdm_core::NodeId::mint(), path: "mesh2.node".into(), name: name.into(), endpoint_id: rdm_core::EndpointId(rdm_core::NodeId::mint().to_string()), transport_addr: "127.0.0.1:41000".parse().unwrap(), incarnation: IncarnationId::mint(), supersedes: None },
            status: rdm_core::model::NodeStatus::Ready,
            primary: Vec::new(),
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
        }
    }

    /// `frames` as a receiver reads them off the wire: each encoded, decoded and read as its chunk.
    fn heard(frames: &[Frame]) -> Vec<DeltaChunk> {
        frames.iter().map(|f| DeltaChunk::of_frame(&Frame::decode(&f.encode()).expect("the chunk decodes")).expect("a differential chunk")).collect()
    }

    /// CONTRACT (gossip.md §3.1 `TopologyDelta`, §Size: "a differential that does not fit one message is
    /// sent in chunks and applied once every chunk is held"): a peer mesh of 400 nodes goes into the
    /// mesh channel as a differential from nothing in several chunks, each a complete message within
    /// `MAX_FRAME`, each naming its index and the count, the forwarder as its envelope's source and the
    /// peer mesh's version as its sequence. A receiver holding every chunk but one holds nothing of the
    /// mesh and moves no version; a repeated chunk is a copy; the last chunk applies the differential, and
    /// the receiver then holds the forwarder's topology. A receiver missing one chunk never applies it,
    /// and the next differential is a gap for it. Must NOT happen: a partial differential applied; a
    /// differential too large for one message not sent.
    #[test]
    fn a_differential_larger_than_one_message_is_sent_in_chunks_and_applied_only_once_every_chunk_is_held() {
        let (mut fwd, author) = forwarder();
        let nodes: Vec<MeshDigest> = (0..400).map(|k| member(&k.to_string())).collect();
        let full = Full::new(nodes.clone(), vec![], vec![]);
        let Forward::Delta(frames) = fwd.source("mesh1.node-admin.1", "mesh2", 7, &full, 1) else { panic!("the peer mesh goes in") };
        let n = frames.len();
        assert!(n > 1, "400 nodes do not fit one message: {n} chunk(s)");
        for (k, f) in frames.iter().enumerate() {
            assert!(f.encode().len() <= MAX_FRAME, "chunk {k} is {} B", f.encode().len());
            let Frame::TopologyDelta { base_version, chunk_index, chunk_count, .. } = f.data() else { panic!("a differential") };
            assert_eq!((*base_version, *chunk_index, *chunk_count), (0, k as u32, n as u32));
            let c = f.context().expect("authored");
            assert_eq!((&c.source, c.version(), c.kind.as_str()), (&author, Some(7), "rdm.mesh.members.delta"));
        }
        let chunks = heard(&frames);
        let mut rx = SnapshotReceiver::default();
        for c in &chunks[..n - 1] {
            assert!(matches!(rx.take_delta_chunk(c.clone()), Moved::Waiting { .. }));
        }
        assert_eq!(rx.held_version("mesh2"), None, "nothing is applied before every chunk is held");
        assert!(matches!(rx.take_delta_chunk(chunks[0].clone()), Moved::Duplicate { .. }), "a repeated chunk is a copy");
        let applied = rx.take_delta_chunk(chunks[n - 1].clone());
        assert!(matches!(&applied, Moved::Applied { base_version: 0, topology_version: 7, moved, .. } if moved.len() == 400), "the last chunk applies the differential: {applied:?}");
        assert!(rx.held_full("mesh2").expect("held").same_topology(&full), "the receiver holds the forwarder's topology");

        let mut missing = SnapshotReceiver::default();
        for c in chunks.iter().skip(1) {
            assert!(matches!(missing.take_delta_chunk(c.clone()), Moved::Waiting { .. }));
        }
        assert_eq!(missing.held_version("mesh2"), None, "with one chunk missing the differential is never applied");
        let mut moved = nodes.clone();
        moved[0].status = rdm_core::model::NodeStatus::ConnectionsDeleting;
        let Forward::Delta(next) = fwd.source("mesh1.node-admin.1", "mesh2", 8, &Full::new(moved, vec![], vec![]), 2) else { panic!("the move goes in") };
        assert_eq!(next.len(), 1, "one node's status change is one chunk");
        let next = heard(&next).remove(0);
        assert!(matches!(missing.take_delta_chunk(next.clone()), Moved::Desynced { gap: Gap::NoBaseline, .. }), "the next differential is a gap for the receiver that missed a chunk");
        assert!(matches!(rx.take_delta_chunk(next), Moved::Applied { base_version: 7, topology_version: 8, .. }));
    }

    /// CONTRACT (gossip.md §3.1 `TopologyDelta`: "applied only when the receiver holds exactly
    /// `base_version`"; §3.3: "a delta whose base version the receiver does not hold triggers GetTopology
    /// repair"): a receiver holding the peer mesh at v5 hears a differential in chunks from base 6 to 7:
    /// its first chunk raises the gap at once, naming the version held and the base, before the other
    /// chunks arrive; the others find the source already desynchronized, and the held version stays 5.
    /// Must NOT happen: a differential at another base applied, or its gap shown only when every chunk
    /// arrived.
    #[test]
    fn a_differential_at_another_base_raises_the_gap_at_its_first_chunk() {
        let (mut fwd, _) = forwarder();
        let at5 = Full::new(vec![member("1")], vec![], vec![]);
        let Forward::Delta(first) = fwd.source("mesh1.node-admin.1", "mesh2", 5, &at5, 1) else { panic!("the peer mesh goes in") };
        let mut rx = SnapshotReceiver::default();
        assert!(matches!(rx.take_delta_chunk(heard(&first).remove(0)), Moved::Applied { topology_version: 5, .. }));
        let (mut other, _) = forwarder();
        other.start_from(&[("mesh2".into(), 6, at5.clone())]);
        let big = Full::new((0..400).map(|k| member(&k.to_string())).collect(), vec![], vec![]);
        let Forward::Delta(frames) = other.source("mesh1.node-admin.2", "mesh2", 7, &big, 2) else { panic!("the move goes in") };
        let chunks = heard(&frames);
        assert!(chunks.len() > 1);
        assert!(matches!(rx.take_delta_chunk(chunks[0].clone()), Moved::Desynced { gap: Gap::Version { held: 5, base: 6 }, .. }), "the first chunk shows the gap");
        assert!(matches!(rx.take_delta_chunk(chunks[1].clone()), Moved::Desynced { gap: Gap::AlreadyDesynced, .. }));
        assert_eq!(rx.held_version("mesh2").map(|(_, v)| v), Some(5));
        assert_eq!(rx.desynced().into_iter().map(|(m, _)| m).collect::<Vec<_>>(), vec!["mesh2".to_string()], "the source tops up");
    }

    /// CONTRACT (gossip.md §3.1 `TopologyDelta`; Q387 rule 3: "An addition carries node details; a
    /// later status change carries only member identity and status"): mesh2 moves from v1 (nodes 1, 2,
    /// 3) to v2 (node 1 connections-deleting, node 2 renamed, node 3 gone, node 4 new). The
    /// differential lists node 1 in `changed` with its birth, status and primary roles only; nodes 2
    /// and 4 in `added` with their details; node 3's exact birth in `removed`. A receiver at v1 applies
    /// it onto the details it holds and then holds the forwarder's v2. A status change of a birth the
    /// receiver does not hold is a gap. Must NOT happen: a status change resending a node's details; a
    /// status change applied to a birth not held.
    #[test]
    fn a_status_change_carries_the_birth_and_its_status_and_applies_onto_the_details_held() {
        let (mut fwd, _) = forwarder();
        let (one, two, three, four) = (member("1"), member("2"), member("3"), member("4"));
        let v1 = Full::new(vec![one.clone(), two.clone(), three.clone()], vec![], vec![]);
        let Forward::Delta(first) = fwd.source("mesh1.node-admin.1", "mesh2", 1, &v1, 1) else { panic!("the peer mesh goes in") };
        let mut rx = SnapshotReceiver::default();
        assert!(matches!(rx.take_delta_chunk(heard(&first).remove(0)), Moved::Applied { topology_version: 1, .. }));
        let mut deleting = one.clone();
        deleting.status = rdm_core::model::NodeStatus::ConnectionsDeleting;
        let mut renamed = two.clone();
        renamed.node.name = "two".into();
        let v2 = Full::new(vec![deleting.clone(), renamed.clone(), four.clone()], vec![], vec![]);
        let Forward::Delta(frames) = fwd.source("mesh1.node-admin.1", "mesh2", 2, &v2, 2) else { panic!("the move goes in") };
        let Frame::TopologyDelta { added, changed, removed, .. } = frames[0].data() else { panic!("a differential") };
        assert_eq!(changed, &vec![NodeStatusEntry::of(&deleting)], "a status change is the birth, its status and its primary roles");
        let mut names: Vec<&str> = added.iter().map(|e| e.name.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["4", "two"], "a new node and a renamed one carry their details");
        assert_eq!(removed, &vec![NodeBirth { node_id: three.node.node_id.clone(), incarnation: three.node.incarnation.clone() }]);
        assert!(matches!(rx.take_delta_chunk(heard(&frames).remove(0)), Moved::Applied { topology_version: 2, .. }));
        let held = rx.held_full("mesh2").expect("held");
        assert!(held.same_topology(&v2), "the receiver holds v2");
        assert_eq!(held.digests().into_iter().find(|d| d.node.node_id == one.node.node_id).map(|d| d.node), Some(one.node.clone()), "node 1 keeps the details held");

        let stranger = Delta { changed: vec![NodeStatusEntry::of(&member("5"))], ..Delta::default() };
        assert!(matches!(rx.take_delta("mesh2", &author_of(&PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() }), 2, 3, &stranger), Moved::Desynced { gap: Gap::ChangedBirthNotHeld, .. }), "a status change of a birth not held is a gap");
        assert_eq!(rx.held_version("mesh2").map(|(_, v)| v), Some(2));
    }

    fn chunk(publisher: &PublisherId, topology_version: u64) -> Chunk {
        let author = crate::envelope::Birth::publisher(&rdm_core::FabricId::parse("fab000000001").unwrap(), publisher);
        Chunk { mesh: "mesh1".into(), mesh_id: None, author: Some(author), topology_version, time_ms: topology_version, chunk_index: 0, chunk_count: 1, digests: vec![], runtimes: Vec::new() }
    }

    /// CONTRACT: the receiver enforces its installed floor, including for direct callers. An
    /// already-applied delta is a duplicate; lower snapshots/deltas cannot regress or request repair.
    /// An ahead-base delta requests repair; a new complete publication or directed read resumes
    /// that source. Repeated gossip of its held version does nothing.
    #[test]
    fn a_receiver_keeps_the_material_floor_and_requires_a_new_publication_or_read_to_resume() {
        let p = PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() };
        let mut rx = SnapshotReceiver::default();
        assert!(matches!(rx.take_chunk(chunk(&p, 7)), Taken::Installed(_)));
        assert!(matches!(rx.take_delta("mesh1", &author_of(&p), 7, 8, &Delta::default()), Moved::Applied { topology_version: 8, .. }));
        assert!(matches!(rx.take_delta("mesh1", &author_of(&p), 7, 8, &Delta::default()), Moved::Duplicate { held_version: 8 }));
        assert!(matches!(rx.take_delta("mesh1", &author_of(&p), 6, 7, &Delta::default()), Moved::Stale));
        assert!(matches!(rx.take_chunk(chunk(&p, 6)), Taken::Refused(Refusal::OlderThanHeld { held_version: 8, offered_version: 6 })));
        assert_eq!(rx.held_version("mesh1"), Some((Some(p.clone()), 8)));
        assert!(rx.desynced().is_empty());
        assert!(matches!(rx.take_chunk(chunk(&p, 8)), Taken::Installed(i) if i.refreshed), "a new publication at the held material version is heard");
        assert!(matches!(rx.take_delta("mesh1", &author_of(&p), 9, 10, &Delta::default()), Moved::Desynced { gap: Gap::Version { held: 8, base: 9 }, .. }));
        assert!(matches!(rx.take_chunk(chunk(&p, 8)), Taken::Refused(Refusal::Duplicate)));
        assert!(!rx.desynced().is_empty(), "gossip repeats do not complete repair");
        assert!(matches!(rx.take_read_chunk(chunk(&p, 8)), Taken::Installed(i) if i.resumed));
        assert_eq!(rx.held_version("mesh1"), Some((Some(p), 8)));
        assert!(rx.desynced().is_empty());
    }
}
