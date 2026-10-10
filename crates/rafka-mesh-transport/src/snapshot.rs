//! Versioned cross-Mesh topology (rafka gossip.md §3.1 and §3.3): the `topology_version` a source
//! Mesh's primary puts on its backbone aggregate, the chunked snapshot a receiver installs
//! atomically, the delta a forwarding primary derives between two fulls it holds, and the
//! baseline an entry top-up hands a node.
//!
//! Everything here is a pure state machine over values: no socket, no clock, no timer. The
//! gossip wiring in [`crate::membership`] feeds it frames and sends what it returns.
//!
//! ```text
//! SOURCE PRIMARY   Full -> SourceVersion::observe -> version -> chunks_of -> Members on the backbone
//! RECEIVER         Members chunks -> SnapshotReceiver -> installed only once EVERY chunk is held
//! FORWARDER        held source Full, last-published Full -> Forwarder -> MembersDelta | forwarded full
//! RECEIVER         MembersDelta -> applied only at exactly base_version, else the source desyncs
//! ```

use crate::membership::{pack, Frame, MAX_FRAME};
use rafka_mesh_entity::{LifecycleOp, MeshDigest};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

pub use rafka_mesh_entity::PublisherId;

type OpKey = (String, u32, String);

/// The digest as the topology projection sees it: what `topology_version` orders.
///
/// CONTRACT (gossip.md §3.1 "DOES bump / DOES NOT bump"): the view KEEPS the birth (identity,
/// incarnation and lineage, the one transport address, the runtime fact and its metadata), the
/// status routing reads, the control API base, the mesh id, the data dir and the descriptive tags.
/// It DROPS `digest_seq` and `emitted_at_rafka_ms` (heartbeat order and a stamp), `in_flight` (the
/// draining work count), `load` (the process's CPU and RAM) and `gossip` (the channel's
/// heard/neighbour/frame counts): the digest's load and gossip stats.
pub(crate) fn topology_view(d: &MeshDigest) -> MeshDigest {
    let mut v = without_load(d);
    v.digest_seq = 0;
    v.emitted_at_rafka_ms = 0;
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
    departed: BTreeMap<String, LifecycleOp>,
}

impl Full {
    /// A full from its members, the overlays still open and the departures retained; each keyed by
    /// its node id.
    pub fn new(digests: Vec<MeshDigest>, in_flight: Vec<LifecycleOp>, departed: Vec<LifecycleOp>) -> Self {
        Self {
            digests: digests.into_iter().map(|d| (d.node.node_id.to_string(), d)).collect(),
            in_flight: in_flight.into_iter().map(|o| (o.key(), o)).collect(),
            departed: departed.into_iter().map(|o| (o.node_id.to_string(), o)).collect(),
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
    pub fn departed(&self) -> Vec<LifecycleOp> {
        self.departed.values().cloned().collect()
    }

    /// The number of members.
    pub fn member_count(&self) -> usize {
        self.digests.len()
    }

    /// Whether `other` is the same topology: the same members by [`topology_view`], the same
    /// overlays, the same departures. A heartbeat, a stamp, a load or a republish is not a
    /// difference.
    pub fn same_topology(&self, other: &Full) -> bool {
        self.digests.len() == other.digests.len()
            && self.digests.iter().all(|(id, d)| other.digests.get(id).is_some_and(|o| topology_view(d) == topology_view(o)))
            && self.in_flight == other.in_flight
            && self.departed == other.departed
    }

    /// This full without any member's load.
    pub fn without_loads(&self) -> Full {
        Full { digests: self.digests.iter().map(|(k, d)| (k.clone(), without_load(d))).collect(), in_flight: self.in_flight.clone(), departed: self.departed.clone() }
    }

    /// The complete difference from this full to `new`, loads omitted (gossip.md §3.3: one delta
    /// per move from a held version to a newer one, never a replay of the versions between).
    /// `in_flight` and `departed` carry the overlays and departures `new` holds that this one does
    /// not; a cleared overlay or an expired departure is a local fact of the receiver's age.
    pub fn delta_to(&self, new: &Full) -> Delta {
        Delta {
            changed: new.digests.iter().filter(|(id, d)| self.digests.get(*id).is_none_or(|o| topology_view(o) != topology_view(d))).map(|(_, d)| without_load(d)).collect(),
            removed: self.digests.keys().filter(|id| !new.digests.contains_key(*id)).cloned().collect(),
            in_flight: new.in_flight.iter().filter(|(k, _)| !self.in_flight.contains_key(*k)).map(|(_, o)| o.clone()).collect(),
            departed: new.departed.iter().filter(|(k, _)| !self.departed.contains_key(*k)).map(|(_, o)| o.clone()).collect(),
        }
    }

    /// This full moved by `delta`.
    pub fn apply(&mut self, delta: &Delta) {
        for d in &delta.changed {
            self.digests.insert(d.node.node_id.to_string(), d.clone());
        }
        for id in &delta.removed {
            self.digests.remove(id);
        }
        for o in &delta.in_flight {
            self.in_flight.insert(o.key(), o.clone());
        }
        for o in &delta.departed {
            self.in_flight.remove(&o.key());
            self.departed.insert(o.node_id.to_string(), o.clone());
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
    pub departed: Vec<LifecycleOp>,
}

impl Delta {
    /// Whether the delta moves nothing.
    pub fn is_empty(&self) -> bool {
        self.changed.is_empty() && self.removed.is_empty() && self.in_flight.is_empty() && self.departed.is_empty()
    }
}

/// A source primary's version counter: `topology_version` bumps when the normalized projection
/// changes and never otherwise (gossip.md §3.1). The first projection observed is version 1.
/// `snapshot_id` is a counter of this publisher's own, never a clock reading.
#[derive(Debug, Default)]
pub struct SourceVersion {
    version: u64,
    last: Option<Full>,
    snapshots: u64,
}

impl SourceVersion {
    /// The version of `full`: the held one, or the next when `full` is a different topology.
    pub fn observe(&mut self, full: &Full) -> u64 {
        if self.last.as_ref().is_none_or(|l| !l.same_topology(full)) {
            self.version += 1;
            self.last = Some(full.clone());
        }
        self.version
    }

    /// The version of the last topology observed, 0 before any.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The next snapshot id of this publisher.
    pub fn next_snapshot_id(&mut self) -> u64 {
        self.snapshots += 1;
        self.snapshots
    }
}

#[derive(Clone)]
enum Item {
    Digest(MeshDigest),
    InFlight(LifecycleOp),
    Departed(LifecycleOp),
}

fn split(run: Vec<Item>) -> (Vec<MeshDigest>, Vec<LifecycleOp>, Vec<LifecycleOp>) {
    let (mut d, mut i, mut x) = (Vec::new(), Vec::new(), Vec::new());
    for it in run {
        match it {
            Item::Digest(v) => d.push(v),
            Item::InFlight(v) => i.push(v),
            Item::Departed(v) => x.push(v),
        }
    }
    (d, i, x)
}

/// `full` as the chunks of ONE snapshot: `shape(digests, in_flight, departed, chunk_index,
/// chunk_count)` builds each frame. Every list rides inside the sizing, each chunk encodes within
/// one gossip message (sized with the widest index and count, so a filled-in chunk never
/// overflows), and an empty full is one empty chunk: an empty Mesh is a complete snapshot.
pub fn chunks_of(full: &Full, shape: impl Fn(Vec<MeshDigest>, Vec<LifecycleOp>, Vec<LifecycleOp>, u32, u32) -> Frame) -> Vec<Frame> {
    let items: Vec<Item> = full
        .digests()
        .into_iter()
        .map(Item::Digest)
        .chain(full.in_flight().into_iter().map(Item::InFlight))
        .chain(full.departed().into_iter().map(Item::Departed))
        .collect();
    let sized = pack(items, |run| {
        let (d, i, x) = split(run);
        shape(d, i, x, u32::MAX, u32::MAX)
    });
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
    pub departed: Vec<LifecycleOp>,
}

impl SourceSnapshot {
    /// The snapshot as a [`Full`].
    pub fn full(&self) -> Full {
        Full::new(self.digests.clone(), self.in_flight.clone(), self.departed.clone())
    }
}

/// One chunk of a snapshot, as a receiver reads it from a `Members` frame.
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
    pub departed: Vec<LifecycleOp>,
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
    /// The held publisher was another (or none): this snapshot is a new epoch.
    pub new_epoch: bool,
    /// This source was desynchronized and is resumed by this snapshot.
    pub resumed: bool,
    /// The same publisher and version were already held: nothing moved, the members were heard again.
    pub refreshed: bool,
}

/// Why a chunk or snapshot was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
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
    /// A complete snapshot older than the version held from the same publisher.
    /// The snapshot is older than the version held from the same publisher.
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
    /// The held snapshot is of another publisher (a new epoch): its full comes first.
    /// The held snapshot is of another publisher.
    OtherEpoch {
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
            Gap::OtherEpoch { .. } => f.write_str("other-epoch"),
            Gap::Version { .. } => f.write_str("version-gap"),
            Gap::AlreadyDesynced => f.write_str("already-desynced"),
        }
    }
}

/// What a delta did.
#[derive(Debug, Clone)]
pub enum Moved {
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
    /// The held version is at or past this delta's: a copy already applied. A no-op.
    /// The delta was already applied.
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
    mesh: String,
    publisher: PublisherId,
    forwarded_by: Option<String>,
}

#[derive(Debug)]
struct Pending {
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
}

/// A node's held cross-Mesh projection, one source Mesh at a time.
///
/// CONTRACT (gossip.md §3.1, §3.3, proofs 25-27): a snapshot is installed only once EVERY chunk
/// of it is held; until then the held projection and the held source version stand and no delta
/// is derived from a partial. A delta applies only when the held version is exactly its
/// `base_version`; a copy already applied is a no-op; any other is a gap that desynchronizes that
/// source alone. A desynchronized source applies no delta until a complete snapshot or a top-up
/// resumes it. Nothing is held for later and nothing is requested here.
#[derive(Debug, Default)]
pub struct SnapshotReceiver {
    pending: HashMap<SourceKey, Pending>,
    held: HashMap<String, Held>,
    /// Sources a delta named before any snapshot of them was held: desynchronized, with no baseline.
    unbased: BTreeMap<String, Gap>,
}

impl SnapshotReceiver {
    /// One chunk. The snapshot installs when it is complete and no older than what is held.
    pub fn take_chunk(&mut self, c: Chunk) -> Taken {
        if c.chunk_count == 0 || c.chunk_index >= c.chunk_count {
            return Taken::Refused(Refusal::MalformedChunk { chunk_index: c.chunk_index, chunk_count: c.chunk_count, why: "the index is outside the count" });
        }
        if let Some(h) = self.held.get(&c.mesh) {
            if h.publisher == c.publisher && c.topology_version < h.version {
                return Taken::Refused(Refusal::OlderThanHeld { held_version: h.version, offered_version: c.topology_version });
            }
        }
        let key = SourceKey { mesh: c.mesh.clone(), publisher: c.publisher.clone(), forwarded_by: c.forwarded_by.clone() };
        // One snapshot per source is assembled at a time: a newer one drops the stale partial.
        if let Some(p) = self.pending.get(&key) {
            let same = p.version == c.topology_version && p.snapshot_id == c.snapshot_id;
            if !same && (c.snapshot_id, c.topology_version) < (p.snapshot_id, p.version) {
                return Taken::Refused(Refusal::OlderThanPending {
                    pending_version: p.version,
                    pending_snapshot: p.snapshot_id,
                    offered_version: c.topology_version,
                    offered_snapshot: c.snapshot_id,
                });
            }
            if !same {
                self.pending.remove(&key);
            } else if p.count != c.chunk_count {
                return Taken::Refused(Refusal::MalformedChunk { chunk_index: c.chunk_index, chunk_count: c.chunk_count, why: "the count disagrees with the snapshot's earlier chunks" });
            }
        }
        let p = self.pending.entry(key.clone()).or_insert_with(|| Pending { version: c.topology_version, snapshot_id: c.snapshot_id, count: c.chunk_count, chunks: BTreeMap::new() });
        p.chunks.entry(c.chunk_index).or_insert((c.digests, c.in_flight, c.departed));
        if (p.chunks.len() as u32) < p.count {
            return Taken::Waiting { held: p.chunks.len() as u32, of: p.count };
        }
        let p = self.pending.remove(&key).expect("just assembled");
        let (mut digests, mut in_flight, mut departed) = (Vec::new(), Vec::new(), Vec::new());
        for (_, (d, i, x)) in p.chunks {
            digests.extend(d);
            in_flight.extend(i);
            departed.extend(x);
        }
        let full = Full::new(digests, in_flight, departed);
        self.install(c.mesh, c.publisher, p.version, p.snapshot_id, full)
    }

    fn install(&mut self, mesh: String, publisher: PublisherId, version: u64, snapshot_id: u64, full: Full) -> Taken {
        let unbased = self.unbased.contains_key(&mesh);
        let (new_epoch, resumed, refreshed) = match self.held.get(&mesh) {
            None => (true, unbased, false),
            Some(h) if h.publisher != publisher => (true, h.desynced.is_some() || unbased, false),
            Some(h) if version < h.version => return Taken::Refused(Refusal::OlderThanHeld { held_version: h.version, offered_version: version }),
            Some(h) => (false, h.desynced.is_some(), version == h.version),
        };
        self.unbased.remove(&mesh);
        // An incomplete snapshot this one supersedes is dropped, never completed later.
        self.pending.retain(|k, p| !(k.mesh == mesh && k.publisher == publisher && p.version <= version));
        self.held.insert(mesh.clone(), Held { publisher: publisher.clone(), version, full: full.clone(), desynced: None });
        Taken::Installed(Box::new(Install { mesh, publisher, topology_version: version, snapshot_id, full, new_epoch, resumed, refreshed }))
    }

    /// A delta: applied only at exactly its base.
    pub fn take_delta(&mut self, mesh: &str, source: &PublisherId, base_version: u64, topology_version: u64, delta: &Delta) -> Moved {
        let Some(h) = self.held.get_mut(mesh) else {
            let already = self.unbased.contains_key(mesh);
            self.unbased.entry(mesh.to_string()).or_insert(Gap::NoBaseline);
            return Moved::Desynced { mesh: mesh.to_string(), gap: if already { Gap::AlreadyDesynced } else { Gap::NoBaseline } };
        };
        if &h.publisher != source {
            let gap = Gap::OtherEpoch { held: h.publisher.clone() };
            return Self::desync(mesh, h, gap);
        }
        // A copy of what is already held: gossip can deliver one delta twice, and a top-up may have
        // brought this node past it. Neither is a gap.
        if h.version >= topology_version {
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
        Moved::Applied { mesh: mesh.to_string(), base_version, topology_version, delta: delta.clone() }
    }

    fn desync(mesh: &str, h: &mut Held, gap: Gap) -> Moved {
        let already = h.desynced.is_some();
        if !already {
            h.desynced = Some(gap.clone());
        }
        Moved::Desynced { mesh: mesh.to_string(), gap: if already { Gap::AlreadyDesynced } else { gap } }
    }

    /// One chunk of a topology read. Installs like [`SnapshotReceiver::take_chunk`]; a complete
    /// snapshot older than a version held from the same publisher moves nothing but still ends the
    /// source's desynchronization: this node is at or past it.
    pub fn take_read_chunk(&mut self, c: Chunk) -> Taken {
        let mesh = c.mesh.clone();
        match self.take_chunk(c) {
            Taken::Refused(r @ Refusal::OlderThanHeld { .. }) => {
                if let Some(h) = self.held.get_mut(&mesh) {
                    h.desynced = None;
                }
                Taken::Refused(r)
            }
            other => other,
        }
    }

    /// Forget the source projection held of `mesh`: the mesh left, and nothing of it is kept.
    pub fn remove(&mut self, mesh: &str) {
        self.held.remove(mesh);
        self.unbased.remove(mesh);
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
        let mut v: Vec<SourceSnapshot> = self
            .held
            .iter()
            .filter(|(_, h)| h.desynced.is_none())
            .map(|(m, h)| SourceSnapshot { mesh: m.clone(), publisher: h.publisher.clone(), topology_version: h.version, digests: h.full.digests(), in_flight: h.full.in_flight(), departed: h.full.departed() })
            .collect();
        v.sort_by(|a, b| a.mesh.cmp(&b.mesh));
        v
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
    /// The first publication of this source (or of a new epoch): a stripped full, chunked.
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
    published: HashMap<String, Published>,
    snapshots: u64,
}

impl Forwarder {
    /// A forwarder that takes (or loses) the seat has published nothing: the first publication of
    /// every source is a full.
    pub fn reset(&mut self) {
        self.published.clear();
    }

    fn full_frames(&mut self, me: &str, mesh: &str, publisher: &PublisherId, version: u64, full: &Full, now_ms: u64) -> Vec<Frame> {
        self.snapshots += 1;
        let snapshot_id = self.snapshots;
        chunks_of(full, |digests, in_flight, departed, chunk_index, chunk_count| Frame::Members {
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
            departed,
        })
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
        if delta.is_empty() {
            // Nothing a Mesh member holds moved: no frame, and `base_version` stays what receivers hold.
            self.published.insert(mesh.to_string(), Published { publisher: publisher.clone(), version: basis.version, full: stripped });
            return Forward::Nothing("the move changes nothing a member of this Mesh holds");
        }
        let frame = Frame::MembersDelta {
            mesh: mesh.to_string(),
            source_publisher: publisher.clone(),
            base_version: basis.version,
            topology_version: version,
            published_at_rafka_ms: now_ms,
            changed: delta.changed,
            removed: delta.removed,
            in_flight: delta.in_flight,
            departed: delta.departed,
        };
        self.published.insert(mesh.to_string(), Published { publisher: publisher.clone(), version, full: stripped.clone() });
        // A delta is an optimization: one too large for a message is the full instead.
        if frame.encode().len() > MAX_FRAME {
            return Forward::Full(self.full_frames(me, mesh, publisher, version, &stripped, now_ms));
        }
        Forward::Delta(Box::new(frame))
    }

    /// Forget what was published of `mesh`: the mesh left, or a forward of it was not sent.
    pub fn remove(&mut self, mesh: &str) {
        self.published.remove(mesh);
    }

    /// The first publication, on taking the seat, of every source in `held`, `(mesh, publisher,
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
        let mut v: Vec<SourceSnapshot> = self
            .published
            .iter()
            .map(|(m, p)| SourceSnapshot { mesh: m.clone(), publisher: p.publisher.clone(), topology_version: p.version, digests: p.full.digests(), in_flight: p.full.in_flight(), departed: p.full.departed() })
            .collect();
        v.sort_by(|a, b| a.mesh.cmp(&b.mesh));
        v
    }

    /// The publisher and version last published for `mesh`, when any.
    pub fn published_version(&self, mesh: &str) -> Option<(PublisherId, u64)> {
        self.published.get(mesh).map(|p| (p.publisher.clone(), p.version))
    }
}
