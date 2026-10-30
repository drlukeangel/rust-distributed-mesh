//! Versioned cross-Mesh topology (rafka gossip.md §3.1 and §3.3): the `topology_version` a source
//! Mesh's primary puts on its backbone mesh topology, the chunked snapshot a receiver installs
//! atomically, the delta a forwarding primary derives between two fulls it holds, and the
//! baseline an entry top-up hands a node.
//!
//! Everything here is a pure state machine over values: no socket, no clock, no timer. The
//! gossip wiring in [`crate::topology`] feeds it frames and sends what it returns.
//!
//! ```text
//! SOURCE PRIMARY   Full -> SourceVersion::observe -> version -> chunks_of -> MeshTopology on the backbone
//! RECEIVER         MeshTopology chunks -> SnapshotReceiver -> installed only once EVERY chunk is held
//! FORWARDER        held source Full, last-published Full -> Forwarder -> MeshTopologyDelta | forwarded full
//! RECEIVER         MeshTopologyDelta -> applied only at exactly base_version, else the source desyncs
//! ```

use crate::topology::{Frame, MAX_FRAME};
use rdm_core::{LifecycleOp, MeshDigest};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

pub use rdm_core::PublisherId;

type OpKey = (String, u32, String);

/// The digest as the topology projection sees it: what `topology_version` orders.
///
/// CONTRACT (gossip.md §3.1 "DOES bump / DOES NOT bump"): the view KEEPS the birth (identity,
/// incarnation and lineage, the one transport address, the runtime fact and its metadata), the
/// status routing reads, the control API base, the mesh id, the data dir and the descriptive tags.
/// It DROPS `digest_seq` and `emitted_at_rafka_ms` (heartbeat order and a stamp), `node_state` (the
/// liveness the birth self-publishes on its heartbeat, which the bump list does not name),
/// `in_flight` (the in-flight work count), `load` (the process's CPU and RAM) and `gossip` (the
/// channel's heard/neighbour/frame counts): the digest's load and gossip stats.
pub(crate) fn topology_view(d: &MeshDigest) -> MeshDigest {
    let mut v = without_load(d);
    v.digest_seq = 0;
    v.emitted_at_rafka_ms = 0;
    v.node_state = rdm_core::NodeState::default();
    v
}

/// The digest without its load and gossip stats: what an ordinary node holds of a member of a remote Mesh.
pub(crate) fn without_load(d: &MeshDigest) -> MeshDigest {
    let mut v = d.clone();
    v.in_flight = None;
    v.load = None;
    v.gossip = None;
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

    /// This full without any member's load.
    pub fn without_loads(&self) -> Full {
        Full { digests: self.digests.iter().map(|(k, d)| (k.clone(), without_load(d))).collect(), in_flight: self.in_flight.clone(), deleted: self.deleted.clone(), by_endpoint: self.by_endpoint.clone() }
    }

    /// The complete difference from this full to `new`, loads omitted (gossip.md §3.3: one delta
    /// per move from a held version to a newer one, never a replay of the versions between).
    /// `in_flight` and `deleted` carry the overlays and departures `new` holds that this one does
    /// not. A cleared stop overlay travels as its birth's own `changed` entry: a source's primary
    /// never lists a stopped birth, and lists it again only after its `node.joined` cleared the
    /// stop ([`Full::apply`]). An expired departure is a local fact of the receiver's age.
    pub fn delta_to(&self, new: &Full) -> Delta {
        let changed: Vec<MeshDigest> = new.digests.iter().filter(|(id, d)| self.digests.get(*id).is_none_or(|o| topology_view(o) != topology_view(d))).map(|(_, d)| without_load(d)).collect();
        Delta {
            removed: self.digests.keys().filter(|id| !new.digests.contains_key(*id)).cloned().collect(),
            in_flight: new.in_flight.iter().filter(|(k, _)| !self.in_flight.contains_key(*k)).map(|(_, o)| o.clone()).collect(),
            deleted: new.deleted.iter().filter(|(k, _)| !self.deleted.contains_key(*k)).map(|(_, o)| o.clone()).collect(),
            changed,
        }
    }

    /// This full moved by `delta` (gossip.md §3.1, `MeshTopologyDelta` carries "overlay and deleted
    /// changes"). A `changed` entry of a birth clears that exact birth's stop overlay
    /// (`NodeStopped`, `node.stop:`): the source lists the birth again once its primary's
    /// `node.joined` ended the stop. A departure ends every overlay of its exact birth, its stop
    /// included: node.delete replaces NodeStopped with the deleted retention (gossip.md §3.1).
    pub fn apply(&mut self, delta: &Delta) {
        for d in &delta.changed {
            self.in_flight.retain(|_, o| !(o.is_stop() && o.node_id == d.node.node_id && o.incarnation == d.node.incarnation));
            self.put(d.clone());
        }
        for id in &delta.removed {
            self.unlist(id);
            self.digests.remove(id);
        }
        for o in &delta.in_flight {
            self.in_flight.insert(o.key(), o.clone());
        }
        for o in &delta.deleted {
            self.in_flight.retain(|_, x| !(x.node_id == o.node_id && x.incarnation == o.incarnation));
            self.deleted.insert(o.node_id.to_string(), o.clone());
        }
    }
}

/// What moves a held source projection from `base_version` to `topology_version`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Delta {
    /// Births added or changed.
    pub changed: Vec<MeshDigest>,
    /// NodeIds no longer listed.
    pub removed: Vec<String>,
    /// The overlays opened or advanced.
    pub in_flight: Vec<LifecycleOp>,
    /// The departures recorded.
    pub deleted: Vec<LifecycleOp>,
}

impl Delta {
    /// Whether the delta moves nothing.
    pub fn is_empty(&self) -> bool {
        self.changed.is_empty() && self.removed.is_empty() && self.in_flight.is_empty() && self.deleted.is_empty()
    }
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

#[derive(Clone)]
enum Item {
    Digest(MeshDigest),
    InFlight(LifecycleOp),
    Deleted(LifecycleOp),
}

fn split(run: Vec<Item>) -> (Vec<MeshDigest>, Vec<LifecycleOp>, Vec<LifecycleOp>) {
    let (mut d, mut i, mut x) = (Vec::new(), Vec::new(), Vec::new());
    for it in run {
        match it {
            Item::Digest(v) => d.push(v),
            Item::InFlight(v) => i.push(v),
            Item::Deleted(v) => x.push(v),
        }
    }
    (d, i, x)
}

/// `full` as the chunks of ONE snapshot: `shape(digests, in_flight, deleted, chunk_index,
/// chunk_count)` builds each frame. Every list rides inside the sizing, each chunk encodes within
/// one gossip message (sized with the widest index and count, so a filled-in chunk never
/// overflows), and an empty full is one empty chunk: an empty Mesh is a complete snapshot.
pub fn chunks_of(full: &Full, shape: impl Fn(Vec<MeshDigest>, Vec<LifecycleOp>, Vec<LifecycleOp>, u32, u32) -> Frame) -> Vec<Frame> {
    chunks_measured(full, shape, |f| f.encode())
}

/// Split a complete snapshot using the exact complete-message encoding of its channel.
/// Gossip uses its envelope; a directed read measures its own reply schema.
pub fn chunks_measured<T>(full: &Full, shape: impl Fn(Vec<MeshDigest>, Vec<LifecycleOp>, Vec<LifecycleOp>, u32, u32) -> T, encode: impl Fn(&T) -> Vec<u8>) -> Vec<T> {
    let items: Vec<Item> = full
        .digests()
        .into_iter()
        .map(Item::Digest)
        .chain(full.in_flight().into_iter().map(Item::InFlight))
        .chain(full.deleted().into_iter().map(Item::Deleted))
        .collect();
    let (sized, refused) = crate::chunking::pack_in_order(items, |run: &[Item]| {
        let (d, i, x) = split(run.to_vec());
        let bytes = encode(&shape(d, i, x, u32::MAX, u32::MAX));
        if bytes.len() > MAX_FRAME { Err(bytes.len()) } else { Ok(bytes.into()) }
    });
    for len in refused {
        tracing::info_span!("rdm.mesh.topology.reject.via-item-too-large", frame_bytes = len, limit = MAX_FRAME)
            .in_scope(|| tracing::warn!("a snapshot item fits no complete message"));
    }
    let sized: Vec<_> = sized.into_iter().map(|(run, _)| run).collect();
    let runs = if sized.is_empty() { vec![Vec::new()] } else { sized };
    let count = runs.len() as u32;
    runs.into_iter()
        .enumerate()
        .map(|(k, run)| {
            let (d, i, x) = split(run);
            shape(d, i, x, k as u32, count)
        })
        .collect()
}

/// One source Mesh's snapshot as a node holds or serves it: the entry answer's baseline
/// (gossip.md §3.3 "records the publisher and version baseline").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSnapshot {
    /// The source mesh's name.
    pub mesh: String,
    /// The publisher that put the snapshot on the backbone.
    pub publisher: PublisherId,
    /// The snapshot's `topology_version`.
    pub topology_version: u64,
    /// The members.
    pub digests: Vec<MeshDigest>,
    /// The overlays still open.
    pub in_flight: Vec<LifecycleOp>,
    /// The retained departures.
    pub deleted: Vec<LifecycleOp>,
}

impl SourceSnapshot {
    /// The snapshot as a [`Full`].
    pub fn full(&self) -> Full {
        Full::new(self.digests.clone(), self.in_flight.clone(), self.deleted.clone())
    }
}

/// One chunk of a snapshot, as a receiver reads it from a `MeshTopology` frame.
#[derive(Debug, Clone)]
pub struct Chunk {
    /// The source mesh's name.
    pub mesh: String,
    /// The publisher of the snapshot.
    pub publisher: PublisherId,
    /// `None` on the backbone; the forwarding primary on a Mesh's own channel.
    pub forwarded_by: Option<String>,
    /// The snapshot's `topology_version`.
    pub topology_version: u64,
    /// The publisher's own snapshot counter.
    pub snapshot_id: u64,
    /// This chunk's index within the snapshot.
    pub chunk_index: u32,
    /// The number of chunks in the snapshot.
    pub chunk_count: u32,
    /// The members this chunk carries.
    pub digests: Vec<MeshDigest>,
    /// The overlays this chunk carries.
    pub in_flight: Vec<LifecycleOp>,
    /// The departures this chunk carries.
    pub deleted: Vec<LifecycleOp>,
}

/// A complete snapshot, installed.
#[derive(Debug, Clone)]
pub struct Install {
    /// The source mesh's name.
    pub mesh: String,
    /// The publisher of the installed snapshot.
    pub publisher: PublisherId,
    /// The installed `topology_version`.
    pub topology_version: u64,
    /// The installed snapshot's id.
    pub snapshot_id: u64,
    /// The installed topology.
    pub full: Full,
    /// The held publisher was another (or none): this snapshot is a new revision.
    pub new_revision: bool,
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
    /// A topology read chunk below the version this node installed from the same publisher: the
    /// receive path ignored it (R-RX3, `crate::receive_order`).
    OlderThanHeld {
        /// The version held.
        held_version: u64,
        /// The version offered.
        offered_version: u64,
    },
    /// A chunk of a snapshot older than the one being assembled from the same source.
    /// The chunk belongs to a snapshot older than the one being assembled from the same source.
    OlderThanPending {
        /// The version being assembled.
        pending_version: u64,
        /// The snapshot id being assembled.
        pending_snapshot: u64,
        /// The version the chunk names.
        offered_version: u64,
        /// The snapshot id the chunk names.
        offered_snapshot: u64,
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
    /// The held snapshot is of another publisher (a new revision): its full comes first.
    /// The held snapshot is of another publisher.
    OtherRevision {
        /// The publisher held.
        held: PublisherId,
    },
    /// The held version is not the delta's base.
    /// The held version is not the delta's base.
    Version {
        /// The version held.
        held: u64,
        /// The version the delta moves from.
        base: u64,
    },
    /// The source was already desynchronized: its deltas are not applied until a snapshot or a top-up.
    AlreadyDesynced,
}

impl std::fmt::Display for Gap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Gap::NoBaseline => f.write_str("no-baseline"),
            Gap::OtherRevision { .. } => f.write_str("other-revision"),
            Gap::Version { .. } => f.write_str("version-gap"),
            Gap::AlreadyDesynced => f.write_str("already-desynced"),
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SourceKey {
    owner: Option<crate::envelope::Birth>,
    mesh: String,
    publisher: PublisherId,
    // Read chunk ordinals belong to that directed answer, not the author's gossip publication.
    from_read: bool,
}

#[derive(Debug, Clone)]
struct Pending {
    owner: Option<crate::envelope::Birth>,
    version: u64,
    snapshot_id: u64,
    count: u32,
    chunks: BTreeMap<u32, (Vec<MeshDigest>, Vec<LifecycleOp>, Vec<LifecycleOp>)>,
}

#[derive(Debug)]
struct Held {
    publisher: PublisherId,
    version: u64,
    full: Full,
    desynced: Option<Gap>,
    publication: Option<Pending>,
    last_delta: Option<(u64, Delta)>,
}

fn same_parts(parts: &(Vec<MeshDigest>, Vec<LifecycleOp>, Vec<LifecycleOp>), c: &Chunk) -> bool {
    Full::new(parts.0.clone(), parts.1.clone(), parts.2.clone()).same_topology(&Full::new(c.digests.clone(), c.in_flight.clone(), c.deleted.clone()))
}

fn same_delta(a: &Delta, b: &Delta) -> bool {
    a.removed.iter().collect::<std::collections::BTreeSet<_>>() == b.removed.iter().collect()
        && Full::new(a.changed.clone(), a.in_flight.clone(), a.deleted.clone()).same_topology(&Full::new(b.changed.clone(), b.in_flight.clone(), b.deleted.clone()))
}

/// Borrow the complete image under ReceiveOrder's shared installation lock; inspecting a chunk
/// does not copy the installed publication just to compare its version or completed content.
pub(crate) type InstalledProjection<'a> = (&'a PublisherId, u64, &'a Full, Option<u64>);

/// A node's held cross-Mesh projection, one source Mesh at a time.
///
/// CONTRACT (gossip.md §3.1, §3.3, proofs 25-27): a snapshot is installed only once EVERY chunk
/// of it is held; until then the held projection and the held source version stand and no delta
/// is derived from a partial. A delta applies only when the held version is exactly its
/// `base_version`; a copy of the delta held (its `topology_version` is the held one) is a no-op;
/// an ahead-base delta is a gap that desynchronizes that source alone. Classification against the
/// installed floor and content happens under this receiver's installation lock. A repeated gossip
/// publication installs nothing and never completes repair; a directed read can resume the source
/// at its held version. No delta is saved for later; the topology caller requests the repair.
#[derive(Debug, Default)]
pub struct SnapshotReceiver {
    pending: HashMap<SourceKey, Pending>,
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
    /// A gossip chunk. A new complete publication at the held material version carries fresh
    /// hearing evidence; its exact repeat does nothing, including no repair completion. Repeated
    /// chunks of an unfinished publication leave its other chunks admissible.
    pub fn take_chunk(&mut self, c: Chunk) -> Taken {
        self.chunk(c, false, None, None)
    }

    /// The directed topology read is the repair completion: an identical held version may resume
    /// a desynchronized source. It still cannot replace different content at the same revision.
    pub fn take_read_chunk(&mut self, c: Chunk) -> Taken {
        self.chunk(c, true, None, None)
    }

    /// Classify against the node's complete installed image, including an install on another
    /// channel. This receiver's pending chunks still assemble independently of that image.
    pub(crate) fn take_chunk_against(&mut self, c: Chunk, from_read: bool, installed: Option<InstalledProjection<'_>>) -> Taken {
        self.chunk(c, from_read, installed, None)
    }

    pub(crate) fn take_authored_chunk(&mut self, c: Chunk, owner: Option<crate::envelope::Birth>, installed: Option<InstalledProjection<'_>>) -> Taken {
        self.chunk(c, false, installed, owner)
    }

    fn chunk(&mut self, c: Chunk, from_read: bool, installed: Option<InstalledProjection<'_>>, owner: Option<crate::envelope::Birth>) -> Taken {
        use crate::receive_order::{publication_duplicate, publication_inconsistent, publication_stale};
        if c.chunk_count == 0 || c.chunk_index >= c.chunk_count {
            return Taken::Refused(Refusal::MalformedChunk { chunk_index: c.chunk_index, chunk_count: c.chunk_count, why: "the index is outside the count" });
        }
        let identity = format!("{} {} topology_version={} snapshot_id={} chunk_index={}", c.mesh, c.publisher, c.topology_version, c.snapshot_id, c.chunk_index);
        let conflict = || {
            publication_inconsistent("mesh-topology", &c.mesh, &c.publisher, c.topology_version);
            Taken::Refused(Refusal::Inconsistent)
        };
        let duplicate = || {
            publication_duplicate("mesh-topology", &identity);
            Taken::Refused(Refusal::Duplicate)
        };
        if let Some(h) = self.held.get(&c.mesh).filter(|h| h.publisher == c.publisher) {
            if c.topology_version < h.version {
                publication_stale("mesh-topology", &c.mesh, &c.publisher, h.version, c.topology_version);
                return Taken::Refused(Refusal::OlderThanHeld { held_version: h.version, offered_version: c.topology_version });
            }
            if !from_read && c.topology_version == h.version {
                if let Some(p) = h.publication.as_ref().filter(|p| p.snapshot_id == c.snapshot_id && p.owner == owner) {
                    if p.count != c.chunk_count { return conflict(); }
                    if let Some(parts) = p.chunks.get(&c.chunk_index) {
                        return if same_parts(parts, &c) { duplicate() } else { conflict() };
                    }
                }
            }
        }
        let key = SourceKey { owner: owner.clone(), mesh: c.mesh.clone(), publisher: c.publisher.clone(), from_read };
        if let Some(p) = self.pending.get(&key) {
            let same = p.version == c.topology_version && p.snapshot_id == c.snapshot_id;
            if !same && (c.topology_version, c.snapshot_id) < (p.version, p.snapshot_id) {
                publication_stale("mesh-topology", &c.mesh, &c.publisher, p.version, c.topology_version);
                return Taken::Refused(Refusal::OlderThanPending { pending_version: p.version, pending_snapshot: p.snapshot_id, offered_version: c.topology_version, offered_snapshot: c.snapshot_id });
            }
            if !same { self.pending.remove(&key); }
            else {
                if p.count != c.chunk_count { return conflict(); }
                if let Some(parts) = p.chunks.get(&c.chunk_index) {
                    return if same_parts(parts, &c) { duplicate() } else { conflict() };
                }
            }
        }
        let p = self.pending.entry(key.clone()).or_insert_with(|| Pending { owner: owner.clone(), version: c.topology_version, snapshot_id: c.snapshot_id, count: c.chunk_count, chunks: BTreeMap::new() });
        p.chunks.insert(c.chunk_index, (c.digests.clone(), c.in_flight.clone(), c.deleted.clone()));
        if (p.chunks.len() as u32) < p.count { return Taken::Waiting { held: p.chunks.len() as u32, of: p.count }; }
        let publication = self.pending.remove(&key).expect("just assembled");
        let mut digests = Vec::new(); let mut in_flight = Vec::new(); let mut deleted = Vec::new();
        for (d, i, x) in publication.chunks.values() { digests.extend(d.clone()); in_flight.extend(i.clone()); deleted.extend(x.clone()); }
        let full = Full::new(digests, in_flight, deleted);
        if let Some(h) = self.held.get(&c.mesh).filter(|h| h.publisher == c.publisher && h.version == c.topology_version) {
            if !h.full.same_topology(&full) { return conflict(); }
        }
        if let Some((_, _, installed_full, snapshot_id)) = installed.filter(|(p, v, _, _)| *p == &c.publisher && *v == c.topology_version) {
            if !installed_full.same_topology(&full) { return conflict(); }
            if !from_read && snapshot_id == Some(c.snapshot_id) { return duplicate(); }
        }
        self.install(c.mesh, c.publisher, publication, full, from_read)
    }

    fn install(&mut self, mesh: String, publisher: PublisherId, publication: Pending, full: Full, from_read: bool) -> Taken {
        let version = publication.version; let snapshot_id = publication.snapshot_id;
        let unbased = self.unbased.contains_key(&mesh);
        let (new_revision, resumed, refreshed) = match self.held.get(&mesh) {
            None => (true, unbased, false),
            Some(h) if h.publisher != publisher => (true, h.desynced.is_some() || unbased, false),
            Some(h) => (false, h.desynced.is_some(), version == h.version),
        };
        let same = self.held.get(&mesh).filter(|h| h.publisher == publisher && h.version == version);
        let last_delta = same.and_then(|h| h.last_delta.clone());
        let owner = publication.owner.clone();
        let retained = if from_read { same.and_then(|h| h.publication.clone()) } else { Some(publication) };
        self.unbased.remove(&mesh);
        self.pending.retain(|k, p| !(k.mesh == mesh && k.publisher == publisher && (p.version < version || (k.owner == owner && p.version == version))));
        // A copy older than a delta dropped while the source was desynchronized is still behind it:
        // the source stays desynchronized, and its holder pulls again (Q130).
        let behind = self.dropped.get(&mesh).copied().filter(|d| *d > version);
        if behind.is_none() {
            self.dropped.remove(&mesh);
        }
        self.held.insert(mesh.clone(), Held { publisher: publisher.clone(), version, full: full.clone(), desynced: behind.map(|d| Gap::Version { held: version, base: d }), publication: retained, last_delta });
        Taken::Installed(Box::new(Install { mesh, publisher, topology_version: version, snapshot_id, full, new_revision, resumed, refreshed }))
    }

    /// Use the actual installed source image as the delta base. A channel-local assembly cannot
    /// turn an exact-base delta into a gap after another channel installed that same source.
    pub(crate) fn take_delta_against(&mut self, mesh: &str, source: &PublisherId, base: u64, version: u64, delta: &Delta, installed: Option<InstalledProjection<'_>>) -> Moved {
        if let Some((publisher, installed_version, full, _)) = installed.filter(|(p, _, _, _)| *p == source) {
            if self.held.get(mesh).is_none_or(|local| &local.publisher != publisher || local.version < installed_version) {
                self.held.insert(mesh.into(), Held { publisher: publisher.clone(), version: installed_version, full: full.clone(), desynced: None, publication: None, last_delta: None });
            }
        }
        self.take_delta(mesh, source, base, version, delta)
    }

    /// A delta: applied only at exactly its base. One dropped because its source was already
    /// desynchronized (its pull in flight) leaves its version as the floor that pull must reach.
    pub fn take_delta(&mut self, mesh: &str, source: &PublisherId, base_version: u64, topology_version: u64, delta: &Delta) -> Moved {
        let moved = self.take_delta_at_base(mesh, source, base_version, topology_version, delta);
        if matches!(moved, Moved::Desynced { gap: Gap::AlreadyDesynced, .. }) {
            let floor = self.dropped.entry(mesh.to_string()).or_insert(topology_version);
            *floor = (*floor).max(topology_version);
        }
        moved
    }

    fn take_delta_at_base(&mut self, mesh: &str, source: &PublisherId, base_version: u64, topology_version: u64, delta: &Delta) -> Moved {
        let Some(h) = self.held.get_mut(mesh) else {
            let already = self.unbased.contains_key(mesh);
            self.unbased.entry(mesh.to_string()).or_insert(Gap::NoBaseline);
            return Moved::Desynced { mesh: mesh.to_string(), gap: if already { Gap::AlreadyDesynced } else { Gap::NoBaseline } };
        };
        if &h.publisher != source {
            let gap = Gap::OtherRevision { held: h.publisher.clone() };
            return Self::desync(mesh, h, gap);
        }
        // A copy of the delta held: gossip can deliver one delta twice, and a top-up may have brought
        // this node to it. Neither is a gap. An earlier one never reaches here (R-RX3, the receive path).
        if topology_version < h.version {
            crate::receive_order::publication_stale("mesh-topology-delta", mesh, source, h.version, topology_version);
            return Moved::Stale;
        }
        if h.version == topology_version {
            let same = match &h.last_delta {
                Some((base, applied)) => *base == base_version && same_delta(applied, delta),
                None => { let mut candidate = h.full.clone(); candidate.apply(delta); h.full.same_topology(&candidate) },
            };
            if !same {
                crate::receive_order::publication_inconsistent("mesh-topology-delta", mesh, source, topology_version);
                return Moved::Inconsistent;
            }
            crate::receive_order::publication_duplicate("mesh-topology-delta", &format!("{mesh} {source} base_version={base_version} topology_version={topology_version}"));
            return Moved::Duplicate { held_version: h.version };
        }
        if h.desynced.is_some() {
            return Moved::Desynced { mesh: mesh.to_string(), gap: Gap::AlreadyDesynced };
        }
        if h.version != base_version {
            let gap = Gap::Version { held: h.version, base: base_version };
            return Self::desync(mesh, h, gap);
        }
        h.full.apply(delta);
        h.version = topology_version;
        h.publication = None;
        h.last_delta = Some((base_version, delta.clone()));
        Moved::Applied { mesh: mesh.to_string(), base_version, topology_version, delta: delta.clone() }
    }

    fn desync(mesh: &str, h: &mut Held, gap: Gap) -> Moved {
        let already = h.desynced.is_some();
        if !already {
            h.desynced = Some(gap.clone());
        }
        Moved::Desynced { mesh: mesh.to_string(), gap: if already { Gap::AlreadyDesynced } else { gap } }
    }

    /// Forget the source projection held of `mesh`: the mesh left, and nothing of it is kept.
    pub fn remove(&mut self, mesh: &str) {
        self.held.remove(mesh);
        self.unbased.remove(mesh);
        self.dropped.remove(mesh);
    }

    /// The version held of `mesh` and its publisher.
    pub fn held_version(&self, mesh: &str) -> Option<(PublisherId, u64)> {
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
            .map(|(m, h)| SourceSnapshot { mesh: m.clone(), publisher: h.publisher.clone(), topology_version: h.version, digests: h.full.digests(), in_flight: h.full.in_flight(), deleted: h.full.deleted() })
            .collect();
        v.sort_by(|a, b| a.mesh.cmp(&b.mesh));
        v
    }

    /// The member at endpoint key `endpoint_id` in the first source, by mesh name, that lists one
    /// ([`Full::member_at`]): what a scan of [`Self::sources`] finds, without copying a source.
    pub fn member_at(&self, endpoint_id: &str) -> Option<MeshDigest> {
        self.held.iter().filter_map(|(m, h)| h.full.member_at(endpoint_id).map(|d| (m, d))).min_by(|a, b| a.0.cmp(b.0)).map(|(_, d)| d.clone())
    }

    /// Every complete source held, as `(mesh, publisher, version, full)`.
    pub fn sources(&self) -> Vec<(String, PublisherId, u64, Full)> {
        let mut v: Vec<_> = self.held.iter().map(|(m, h)| (m.clone(), h.publisher.clone(), h.version, h.full.clone())).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }
}

#[derive(Debug, Clone)]
struct Published {
    publisher: PublisherId,
    version: u64,
    full: Full,
}

/// What the forwarding primary puts on its own Mesh's channel for a source it holds.
#[derive(Debug)]
pub enum Forward {
    /// The first publication of this source (or of a new revision): a stripped full, chunked.
    Full(Vec<Frame>),
    /// The source moved from the version last published into this Mesh to a newer one.
    Delta(Box<Frame>),
    /// Nothing to say: the same version, an older one, or a move that changes nothing a Mesh
    /// member holds.
    Nothing(&'static str),
}

/// The forwarding primary's side (gossip.md §3.3): for each source Mesh it holds, what it last
/// PUBLISHED into its own Mesh. `base_version` is that, so a forwarder's own missed backbone
/// frames never break the chain inside the Mesh; the delta is the difference between the two
/// fulls the forwarder holds, not a replay of the versions between.
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
}

impl Forwarder {
    /// Supply this publisher's owned exact birth before making a publication.
    pub fn set_author(&mut self, author: crate::envelope::Birth) { self.author = Some(author); }

    pub(crate) fn author_frame(&self, frame: Frame, now_ms: u64) -> Frame {
        assert!(frame.context().is_some() || !matches!(frame.data(), Frame::MeshStatus { .. } | Frame::FabricStatus { .. }), "a forwarded status retains its original author and revision");
        frame.authored(self.author.as_ref().expect("forwarder requires its supplied author birth"), 0, now_ms)
    }

    pub(crate) fn publication_counter(&self) -> Arc<AtomicU64> { self.snapshots.clone() }

    /// A forwarder that takes (or loses) the role has published nothing: the first publication of
    /// every source is a full.
    pub fn reset(&mut self) {
        self.published.clear();
    }

    /// Take (`true`) or lose (`false`) the forwarder role. A change clears what was published
    /// (the first publication of every source after taking the role is a full) and begins a new
    /// term. Returns whether the role changed.
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

    fn full_frames(&mut self, me: &str, mesh: &str, publisher: &PublisherId, version: u64, full: &Full, now_ms: u64) -> Vec<Frame> {
        let snapshot_id = self.snapshots.fetch_add(1, Ordering::Relaxed) + 1;
        let author = self.author.as_ref().expect("forwarder requires its supplied author birth");
        chunks_of(full, |digests, in_flight, deleted, chunk_index, chunk_count| Frame::MeshTopology {
            mesh: mesh.to_string(),
            publisher: publisher.clone(),
            forwarded_by: Some(me.to_string()),
            topology_version: version,
            published_at_rafka_ms: now_ms,
            snapshot_id,
            chunk_index,
            chunk_count,
            digests,
            in_flight,
            deleted,
        }.authored(author, 0, now_ms))
    }

    /// The source `mesh` is now held at `version` (a complete snapshot with loads). What to
    /// forward: a full when this source was never published (or its publisher changed), else a
    /// delta from the last published version, or a full when the delta would not fit one message.
    pub fn source(&mut self, me: &str, mesh: &str, publisher: &PublisherId, version: u64, full: &Full, now_ms: u64) -> Forward {
        let stripped = full.without_loads();
        let basis = match self.published.get(mesh) {
            Some(p) if &p.publisher == publisher => p.clone(),
            _ => {
                self.published.insert(mesh.to_string(), Published { publisher: publisher.clone(), version, full: stripped.clone() });
                return Forward::Full(self.full_frames(me, mesh, publisher, version, &stripped, now_ms));
            }
        };
        if version == basis.version {
            return Forward::Nothing("the version is the one last published");
        }
        if version < basis.version {
            return Forward::Nothing("the version is older than the one last published");
        }
        let delta = basis.full.delta_to(&stripped);
        if delta.is_empty() && version == basis.version + 1 {
            // Nothing a Mesh member holds moved: no frame, and `base_version` stays what receivers hold.
            self.published.insert(mesh.to_string(), Published { publisher: publisher.clone(), version: basis.version, full: stripped });
            return Forward::Nothing("the move changes nothing a member of this Mesh holds");
        }
        let frame = Frame::MeshTopologyDelta {
            mesh: mesh.to_string(),
            source_publisher: publisher.clone(),
            base_version: basis.version,
            topology_version: version,
            published_at_rafka_ms: now_ms,
            changed: delta.changed,
            removed: delta.removed,
            in_flight: delta.in_flight,
            deleted: delta.deleted,
        }.authored(self.author.as_ref().expect("forwarder requires its supplied author birth"), 0, now_ms);
        self.published.insert(mesh.to_string(), Published { publisher: publisher.clone(), version, full: stripped.clone() });
        // A delta is an optimization: one too large for a message is the full instead.
        if frame.encode().len() > MAX_FRAME {
            return Forward::Full(self.full_frames(me, mesh, publisher, version, &stripped, now_ms));
        }
        Forward::Delta(Box::new(frame))
    }

    /// Forget what was published of `mesh`: the mesh left.
    pub fn remove(&mut self, mesh: &str) {
        self.published.remove(mesh);
    }

    /// The first publication, on taking the role, of every source in `held`, `(mesh, publisher,
    /// version, full)`: sets each source's last-published version to the one sent.
    pub fn fulls(&mut self, me: &str, held: &[(String, PublisherId, u64, Full)], now_ms: u64) -> Vec<Frame> {
        let mut out = Vec::new();
        for (mesh, publisher, version, full) in held {
            let stripped = full.without_loads();
            self.published.insert(mesh.clone(), Published { publisher: publisher.clone(), version: *version, full: stripped.clone() });
            out.extend(self.full_frames(me, mesh, publisher, *version, &stripped, now_ms));
        }
        out
    }

    /// The fulls of what is already published (a join, heal or refeed replay): the published
    /// versions do not move.
    pub fn replay(&mut self, me: &str, now_ms: u64) -> Vec<Frame> {
        let mut meshes: Vec<String> = self.published.keys().cloned().collect();
        meshes.sort();
        let mut out = Vec::new();
        for m in meshes {
            let p = self.published[&m].clone();
            out.extend(self.full_frames(me, &m, &p.publisher, p.version, &p.full, now_ms));
        }
        out
    }

    /// What this primary has published into its Mesh, as the baselines a top-up serves.
    pub fn snapshots(&self) -> Vec<SourceSnapshot> {
        self.snapshots_of(None)
    }

    /// [`Self::snapshots`] of `mesh` alone when one is named: a source of another mesh is not copied.
    pub fn snapshots_of(&self, mesh: Option<&str>) -> Vec<SourceSnapshot> {
        let mut v: Vec<SourceSnapshot> = self
            .published
            .iter()
            .filter(|(m, _)| mesh.is_none_or(|want| m.as_str() == want))
            .map(|(m, p)| SourceSnapshot { mesh: m.clone(), publisher: p.publisher.clone(), topology_version: p.version, digests: p.full.digests(), in_flight: p.full.in_flight(), deleted: p.full.deleted() })
            .collect();
        v.sort_by(|a, b| a.mesh.cmp(&b.mesh));
        v
    }

    /// The publisher and version last published for `mesh`, when any.
    pub fn published_version(&self, mesh: &str) -> Option<(PublisherId, u64)> {
        self.published.get(mesh).map(|p| (p.publisher.clone(), p.version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::IncarnationId;

    fn op(operation: &str, node_id: &rdm_core::NodeId, incarnation: &IncarnationId) -> LifecycleOp {
        LifecycleOp { build_id: "b1".into(), attempt: 1, operation: operation.into(), node_id: node_id.clone(), incarnation: incarnation.clone(), name: "mesh1.rpc.2".parse().unwrap(), event_at_rafka_ms: 1, traceparent: None, tracestate: None }
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
        let stopped = Full::new(vec![], vec![op("node.stop:mesh1.rpc.2", &node, &inc)], vec![]);
        let deleted = Full::new(vec![], vec![], vec![op("node.delete:mesh1.rpc.2", &node, &inc)]);
        let mut held = stopped.clone();
        held.apply(&stopped.delta_to(&deleted));
        assert!(held.in_flight().is_empty(), "the departure ended the birth's stop overlay: {:?}", held.in_flight());
        assert!(held.same_topology(&deleted), "the receiver holds the publisher's full");
    }

    /// CONTRACT (gossip.md §3.3, Q130: "A node that installs a pulled copy older than a delta it
    /// dropped while the pull was in flight pulls again"; the red of `node_delete` at e6s8-1918053cb:
    /// mesh1.rpc.1 dropped mesh2's delta 9 -> 10 while its pull was in flight, installed v9 from that
    /// pull and resumed as current): a source desynchronized by a gap, then sent a delta it drops,
    /// stays desynchronized when the read installs a copy older than that delta, and resumes only at a
    /// copy that reaches it. Must NOT happen: the older read clearing the gap.
    #[test]
    fn a_read_older_than_a_delta_dropped_while_it_was_in_flight_leaves_the_source_desynchronized() {
        let p = PublisherId { node: "mesh2.admin.1".into(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() };
        let mut rx = SnapshotReceiver::default();
        assert!(matches!(rx.take_chunk(chunk(&p, 7)), Taken::Installed(_)));
        // The 7 -> 8 delta is lost; 8 -> 9 shows the gap, 9 -> 10 is dropped while the pull is in flight.
        assert!(matches!(rx.take_delta("mesh1", &p, 8, 9, &Delta::default()), Moved::Desynced { gap: Gap::Version { held: 7, base: 8 }, .. }));
        assert!(matches!(rx.take_delta("mesh1", &p, 9, 10, &Delta::default()), Moved::Desynced { gap: Gap::AlreadyDesynced, .. }));
        // The pull answers v9, read before its server installed v10.
        assert!(matches!(rx.take_chunk_against(chunk(&p, 9), true, None), Taken::Installed(_)));
        assert_eq!(rx.desynced().into_iter().map(|(m, _)| m).collect::<Vec<_>>(), vec!["mesh1".to_string()], "a read older than the dropped delta leaves the source behind: it pulls again");
        // The next pull answers v10: the source resumes.
        assert!(matches!(rx.take_chunk_against(chunk(&p, 10), true, None), Taken::Installed(_)));
        assert!(rx.desynced().is_empty(), "a read that reaches the dropped delta resumes the source");
        assert_eq!(rx.held_version("mesh1").map(|(_, v)| v), Some(10));
    }

    fn chunk(publisher: &PublisherId, topology_version: u64) -> Chunk {
        Chunk { mesh: "mesh1".into(), publisher: publisher.clone(), forwarded_by: Some("mesh1.admin.1".into()), topology_version, snapshot_id: topology_version, chunk_index: 0, chunk_count: 1, digests: vec![], in_flight: vec![], deleted: vec![] }
    }

    /// CONTRACT: the receiver enforces its installed floor, including for direct callers. An
    /// already-applied delta is a duplicate; lower snapshots/deltas cannot regress or request repair.
    /// An ahead-base delta requests repair; a new complete publication or directed read resumes
    /// that source. Repeated gossip of its held version does nothing.
    #[test]
    fn a_receiver_keeps_the_material_floor_and_requires_a_new_publication_or_read_to_resume() {
        let p = PublisherId { node: "mesh1.admin.1".into(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() };
        let mut rx = SnapshotReceiver::default();
        assert!(matches!(rx.take_chunk(chunk(&p, 7)), Taken::Installed(_)));
        assert!(matches!(rx.take_delta("mesh1", &p, 7, 8, &Delta::default()), Moved::Applied { topology_version: 8, .. }));
        assert!(matches!(rx.take_delta("mesh1", &p, 7, 8, &Delta::default()), Moved::Duplicate { held_version: 8 }));
        assert!(matches!(rx.take_delta("mesh1", &p, 6, 7, &Delta::default()), Moved::Stale));
        assert!(matches!(rx.take_chunk(chunk(&p, 6)), Taken::Refused(Refusal::OlderThanHeld { held_version: 8, offered_version: 6 })));
        assert_eq!(rx.held_version("mesh1"), Some((p.clone(), 8)));
        assert!(rx.desynced().is_empty());
        assert!(matches!(rx.take_chunk(chunk(&p, 8)), Taken::Installed(i) if i.refreshed), "a new publication at the held material version is heard");
        assert!(matches!(rx.take_delta("mesh1", &p, 9, 10, &Delta::default()), Moved::Desynced { gap: Gap::Version { held: 8, base: 9 }, .. }));
        assert!(matches!(rx.take_chunk(chunk(&p, 8)), Taken::Refused(Refusal::Duplicate)));
        assert!(!rx.desynced().is_empty(), "gossip repeats do not complete repair");
        assert!(matches!(rx.take_read_chunk(chunk(&p, 8)), Taken::Installed(i) if i.resumed));
        assert_eq!(rx.held_version("mesh1"), Some((p, 8)));
        assert!(rx.desynced().is_empty());
    }
}
