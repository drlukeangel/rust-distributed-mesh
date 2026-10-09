//! Hierarchical fabric membership over iroh-gossip (HyParView + Plumtree;
//! never a hand-rolled delivery layer). rafka `gossip.md` §6:
//!
//! - each mesh has its own membership channel ([`mesh_topic`]); a member
//!   publishes its [`MeshDigest`] there, and only there;
//! - node-admins alone also join the fabric's backbone ([`backbone_topic`]),
//!   through every node-admin they know, whatever its mesh;
//! - a mesh's admin primary publishes its mesh's members on the backbone; a
//!   peer mesh's primary forwards those onto its own mesh channel; the fabric
//!   primary alone publishes the fabric's status, which peer primaries forward;
//! - so every node holds every mesh's nodes without a fabric-wide membership
//!   topic. A member the publisher stops forwarding goes silent everywhere.
//!
//! Every member heard is registered with the endpoint at its gossip address
//! (its one endpoint), so HyParView reaches a peer it learned by id.

use crate::clock::SharedClock;
use crate::snapshot::{Chunk, Delta, Forward, Forwarder, Full, Gap, Install, Moved, PublisherId, SnapshotReceiver, SourceSnapshot, SourceVersion, Taken};
use anyhow::Result;
use bytes::Bytes;
use futures_lite::StreamExt as _;
use iroh::address_lookup::MemoryLookup;
use iroh::{Endpoint, EndpointAddr};
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use rafka_mesh_entity::{FabricId, IncarnationId, LifecycleOp, MeshDigest, MeshId, NodeId, Seat, SeatHolder, DEPARTED_RETENTION};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub use rafka_mesh_entity::cadence::{backbone_gossip_interval, gossip_interval, staleness_floor};

/// How often a node's publish loop marks its heartbeat in the trace (the digest itself goes out
/// every gossip interval).
pub const HEARTBEAT_EVERY: Duration = Duration::from_secs(5);

/// The largest encoded frame: the payload that fits iroh-gossip's frame limit with its framing.
pub const MAX_FRAME: usize = crate::chunking::MAX_MESSAGE_BYTES;

/// A mesh's membership channel.
pub fn mesh_topic(fabric: &FabricId, mesh_id: &MeshId) -> TopicId {
    TopicId::from_bytes(*blake3::hash(format!("rafka-mesh-membership:{fabric}:{mesh_id}").as_bytes()).as_bytes())
}

/// The fabric's backbone: node-admins only.
pub fn backbone_topic(fabric: &FabricId) -> TopicId {
    TopicId::from_bytes(*blake3::hash(format!("rafka-fabric-backbone:{fabric}").as_bytes()).as_bytes())
}

/// Tell `endpoint` where `peers` are (no discovery service runs).
pub fn learn_addresses(endpoint: &Endpoint, peers: &[EndpointAddr]) -> Result<()> {
    let lookup = MemoryLookup::new();
    for p in peers {
        lookup.add_endpoint_info(p.clone());
    }
    endpoint.address_lookup().map_err(|e| anyhow::anyhow!("address_lookup: {e}"))?.add(lookup);
    Ok(())
}

/// A member's gossip address: its key at its one transport address.
pub fn gossip_addr(d: &MeshDigest) -> Option<EndpointAddr> {
    let key = d.node.endpoint_id.0.parse::<iroh::PublicKey>().ok()?;
    Some(EndpointAddr::new(key).with_ip_addr(d.node.transport_addr))
}

/// The newest birth and heartbeat each key's address was registered from (rafka-v2 node-base
/// `peer_location_wall_time`): governs the address book only, never the held view. Ordered as the
/// held view is: by `digest_seq` within one incarnation, and along the lineage across incarnations.
type LocationMark = (rafka_mesh_entity::IncarnationId, u64, Option<rafka_mesh_entity::IncarnationId>);

fn location_watermarks() -> &'static Mutex<HashMap<iroh::PublicKey, LocationMark>> {
    static W: std::sync::OnceLock<Mutex<HashMap<iroh::PublicKey, LocationMark>>> = std::sync::OnceLock::new();
    W.get_or_init(Mutex::default)
}

/// Register where the birth `d` names is (rafka-v2 node-base `register_peer_location_if_fresher`).
/// A digest newer than the last one this key was registered from REPLACES the key's addresses
/// (`set_endpoint_info`, never a union): a node that restarts keeps its key and binds a fresh
/// port, and its old port, now free for anyone, must never be dialled for it again. Newer is
/// decided by `digest_seq` for the same incarnation, and by lineage across incarnations (the
/// successor restarts its sequence at 1; the incarnation it replaced is older). A digest that is
/// not newer (replayed, late, or an aggregate copy of an older view) only fills a key that has no
/// address, so it never puts an old socket back.
///
/// Returns the key and the new socket when a newer birth MOVED the key: the caller then retires
/// the endpoint's paths to every other socket (`retire_superseded`).
pub fn register_location(lookup: &MemoryLookup, d: &MeshDigest) -> Option<(iroh::PublicKey, std::net::SocketAddr)> {
    let addr = gossip_addr(d)?;
    let key = addr.id;
    let fresher = {
        let mut w = location_watermarks().lock().unwrap();
        let fresher = match w.get(&key) {
            None => true,
            Some((incarnation, seq, _)) if *incarnation == d.node.incarnation => d.digest_seq > *seq,
            Some((_, _, supersedes)) => supersedes.as_ref() != Some(&d.node.incarnation),
        };
        if fresher {
            w.insert(key, (d.node.incarnation.clone(), d.digest_seq, d.node.supersedes.clone()));
        }
        fresher
    };
    let old = lookup.get_endpoint_info(key).and_then(|e| e.ip_addrs().next().cloned());
    let mut moved = None;
    if fresher {
        if old.is_some_and(|o| o != d.node.transport_addr) {
            moved = Some((key, d.node.transport_addr));
            tracing::info_span!(
                "rdm.mesh.connection.update.via-peer-location-refreshed",
                node = %d.node.name,
                incarnation_id = %d.node.incarnation.0,
                old_socket = %old.map(|o| o.to_string()).unwrap_or_default(),
                new_socket = %d.node.transport_addr,
            )
            .in_scope(|| tracing::info!("a newer birth of this key moved: its old socket is no longer dialled"));
        }
        lookup.set_endpoint_info(addr);
    } else if old.is_none() {
        lookup.add_endpoint_info(addr);
    }
    moved
}

/// A newer birth moved `key` to `new`: the endpoint retires every direct path it holds to any other
/// socket of the key, open ones included (iroh `Endpoint::replace_direct_addrs`), so a connection
/// to the old socket, or a dial's first packet to it, can no longer reach whoever took that port.
fn retire_superseded(endpoint: &Endpoint, moved: Option<(iroh::PublicKey, std::net::SocketAddr)>) {
    let Some((key, new)) = moved else { return };
    let endpoint = endpoint.clone();
    tokio::spawn(async move { endpoint.replace_direct_addrs(key, [new]).await });
}

/// `RDM_LEAVE_LINGER_MS` (default 1000): how long a stopping node keeps
/// announcing `Leaving` before it closes. Every node kind takes the same one.
pub fn leave_linger_from_env() -> Duration {
    Duration::from_millis(std::env::var("RDM_LEAVE_LINGER_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000))
}

/// How often a leaving node repeats its `Leaving` during the linger.
pub const LEAVE_EVERY: Duration = Duration::from_millis(200);

/// How long this process's threads, summed, have spent runnable but waiting for a CPU since
/// they started (`/proc/self/task/*/schedstat`, second field); `None` where the kernel does not
/// expose it. Every thread is summed because the runtime's workers are what run a future and the
/// main thread sits in `block_on`; the difference across a bounded operation says whether a late
/// wakeup was the process waiting to run, from the process's own evidence.
pub fn runqueue_wait() -> Option<Duration> {
    let tasks = std::fs::read_dir("/proc/self/task").ok()?;
    let mut total: u64 = 0;
    let mut any = false;
    for t in tasks.flatten() {
        if let Ok(s) = std::fs::read_to_string(t.path().join("schedstat")) {
            if let Some(ns) = s.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()) {
                total += ns;
                any = true;
            }
        }
    }
    any.then(|| Duration::from_nanos(total))
}

/// The runqueue wait accrued since `since` (a [`runqueue_wait`] reading), in ms; 0 where unknown.
pub fn runqueue_wait_since_ms(since: Option<Duration>) -> u64 {
    match (since, runqueue_wait()) {
        (Some(a), Some(b)) => b.saturating_sub(a).as_millis() as u64,
        _ => 0,
    }
}

/// Announce a departure: `say` the `Leaving` digest now and again every
/// `every` until `linger` has passed, then return. Bounded by `linger`: a
/// `say` that does not finish in time is abandoned. Returns how many
/// announcements finished.
///
/// iroh-gossip acknowledges nothing and closing drops what is unsent, so one
/// announcement can be lost; repeating it for the linger is the node's own
/// shutdown, not a delivery layer (nothing is retried after the node leaves).
pub async fn announce_leaving<F, Fut>(linger: Duration, every: Duration, mut say: F) -> u32
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let until = tokio::time::Instant::now() + linger;
    let mut said = 0;
    loop {
        let left = until.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            return said;
        }
        if tokio::time::timeout(left, say()).await.is_ok() {
            said += 1;
        }
        let left = until.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            return said;
        }
        tokio::time::sleep(every.min(left)).await;
    }
}

/// What travels on a mesh channel or the backbone: one postcard frame ([`crate::wire`]). A digest
/// travels as its wire shape (`#[serde(with)]`), never as the JSON-shaped `MeshDigest`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Frame {
    /// A member's own digest (its mesh channel only).
    Digest {
        /// The member's digest, in its wire shape.
        #[serde(with = "crate::wire::digest")]
        digest: MeshDigest,
    },
    /// One chunk of a snapshot of `mesh`'s members (gossip.md §3.1). On the backbone it is the
    /// complete Mesh with its loads, published by that Mesh's current primary (`publisher`, the
    /// exact birth that holds the seat, which is the epoch of `topology_version`); on a peer
    /// Mesh's own channel it is the forwarded full without loads, `publisher` still the SOURCE
    /// primary and `forwarded_by` the local primary that sends it (the backbone frame carries
    /// `None`). `topology_version` bumps on a material change of the normalized projection and
    /// never on a heartbeat, a stamp, a load or an unchanged republish. All chunks of one
    /// snapshot share `snapshot_id` (the sender's own counter, never a clock reading) and carry
    /// the open lifecycle overlays (`in_flight`) and retained proven departures (`departed`)
    /// split across them; a receiver installs the snapshot only once it holds every chunk.
    /// `published_at_rafka_ms` makes each publication distinct.
    Members {
        /// The source mesh's name.
        mesh: String,
        /// The source mesh's primary birth that published the snapshot, the epoch of
        /// `topology_version`.
        publisher: PublisherId,
        /// The local primary that forwards the snapshot onto its own channel; `None` on the
        /// backbone.
        forwarded_by: Option<String>,
        /// The topology version the snapshot carries.
        topology_version: u64,
        /// The Rafka-time of the publication, making each publication distinct.
        published_at_rafka_ms: u64,
        /// The publisher's own snapshot counter, shared by every chunk of the snapshot.
        snapshot_id: u64,
        /// This chunk's index within the snapshot.
        chunk_index: u32,
        /// The number of chunks in the snapshot.
        chunk_count: u32,
        /// The members this chunk carries.
        #[serde(with = "crate::wire::digests")]
        digests: Vec<MeshDigest>,
        /// The open lifecycle overlays this chunk carries.
        in_flight: Vec<LifecycleOp>,
        /// The retained proven departures this chunk carries.
        departed: Vec<LifecycleOp>,
    },
    /// What moved a source Mesh's held projection from `base_version` to `topology_version`,
    /// published on the forwarding primary's own mesh channel (gossip.md §3.1, §3.3). `base_version`
    /// is the version that primary last published into its Mesh; a receiver applies the delta only
    /// when it holds exactly that version of `source_publisher`'s Mesh. Loads are omitted.
    /// `in_flight` and `departed` are the overlays and departures added since `base_version`.
    MembersDelta {
        /// The source mesh's name.
        mesh: String,
        /// The source mesh's primary birth the delta is derived from.
        source_publisher: PublisherId,
        /// The version a receiver must hold for the delta to apply.
        base_version: u64,
        /// The version the delta moves to.
        topology_version: u64,
        /// The Rafka-time of the publication.
        published_at_rafka_ms: u64,
        /// The births added or changed.
        #[serde(with = "crate::wire::digests")]
        changed: Vec<MeshDigest>,
        /// The node ids no longer listed.
        removed: Vec<String>,
        /// The overlays added since the base version.
        in_flight: Vec<LifecycleOp>,
        /// The departures added since the base version.
        departed: Vec<LifecycleOp>,
    },
    /// The mesh executor holding `op` has started removing its exact birth:
    /// the node is still found, and application routing stops selecting it.
    /// Published on the executor's own mesh channel and the backbone; a peer
    /// mesh primary forwards it onto its own channel (`forwarded_by`).
    NodeDeleting {
        /// The removal being executed.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
    },
    /// The provider proved `op`'s exact birth terminal: it has left. Same
    /// channels as `NodeDeleting`; retained afterwards in `Members.departed`.
    NodeDeleted {
        /// The removal that completed.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
    },
    /// The mesh executor holding `op` (`restart-node:<path>`) is restarting its exact birth:
    /// commanded silence (fabric-node-lifecycle.md: node-admin took it down and owns bringing it
    /// back). The node stays held through its own `Leaving` and silence, not routable, until a
    /// later birth of the same NodeId is heard. Same channels as `NodeDeleting`; carried in
    /// `Members.in_flight` while open.
    NodeRestarting {
        /// The restart being executed.
        op: LifecycleOp,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
    },
    /// A mesh's status, authored by that mesh's primary alone, on its own mesh channel and the
    /// backbone; a peer mesh's primary forwards it onto its own channel (`forwarded_by`) and never
    /// restates it as its own. `changed_at_rafka_ms` is the instant of the CHANGE: all five sends
    /// of one change carry the same value (gossip.md §3.1, §3.2).
    MeshStatus {
        /// The mesh the status belongs to.
        mesh: String,
        /// The status.
        status: String,
        /// The mesh primary that authored the status.
        publisher: String,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
        /// The Rafka-time of the change.
        changed_at_rafka_ms: u64,
    },
    /// The fabric's status, authored by the fabric primary alone; same channels, forwarding and
    /// `changed_at_rafka_ms` as [`Frame::MeshStatus`].
    FabricStatus {
        /// The fabric the status belongs to.
        fabric: FabricId,
        /// The status.
        status: String,
        /// The fabric primary that authored the status.
        publisher: String,
        /// The local primary that forwards the frame onto its own channel.
        forwarded_by: Option<String>,
        /// The Rafka-time of the change.
        changed_at_rafka_ms: u64,
    },
    /// A seat changed hands: its new holder announces itself, once, on its own mesh channel and the
    /// backbone (a mesh primary's seat), or on the backbone (the fabric primary's). Every node-admin
    /// holds the record with the later epoch; a record that does not supersede the held one is
    /// refused by name. Replayed by a primary to a neighbour that comes up, like a status.
    Seated {
        /// The seat that changed hands.
        seat: Seat,
        /// Its new holder: the exact birth and the record's epoch.
        holder: SeatHolder,
    },
    /// A mesh primary marked the exact birth `node_id`/`incarnation` of `seat`'s holder
    /// `PendingReconnect` (silent past the staleness floor) and says so on the backbone, once per
    /// mark. A WARNING: it names a birth to look at and never makes the seat vacant.
    Concern {
        /// The seat whose holder looks silent.
        seat: Seat,
        /// The silent holder's node.
        node_id: NodeId,
        /// The silent holder's exact birth.
        incarnation: IncarnationId,
        /// The mesh primary that marked the birth silent.
        observer: String,
    },
}

impl Frame {
    /// The frame as one postcard message.
    pub fn encode(&self) -> Vec<u8> {
        crate::wire::encode(self).expect("frame serializes")
    }
    /// The frame `bytes` carry, or why they are not one.
    pub fn decode(bytes: &[u8]) -> Result<Self, crate::wire::WireError> {
        crate::wire::decode(bytes)
    }
}

/// The fabric status a node holds, with its publisher and the instant of the change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricStatus {
    /// The status.
    pub status: String,
    /// The node that authored it.
    pub publisher: String,
    /// The Rafka-time of the change.
    pub changed_at_rafka_ms: u64,
}

/// A mesh's status a node holds, with its publisher and the instant of the change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshStatus {
    /// The status.
    pub status: String,
    /// The node that authored it.
    pub publisher: String,
    /// The Rafka-time of the change.
    pub changed_at_rafka_ms: u64,
}

/// How many times one status change is sent: at once, then once per [`STATUS_EVERY`]
/// (gossip.md §3.2). After the fifth send there is nothing more until the next change.
pub const STATUS_SENDS: u32 = 5;

/// The spacing of a status change's reinforcing sends.
pub const STATUS_EVERY: Duration = Duration::from_secs(1);

/// A status change: the status and the instant it changed. Every send of one change carries both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusFact {
    /// The status.
    pub status: String,
    /// The Rafka-time of the change.
    pub changed_at_rafka_ms: u64,
}

/// The bounded reinforcement of one status: a change is sent at once, then once per second, five
/// sends in all, then nothing; a status that did not change sends nothing. A pure state machine
/// over an explicit clock: the caller says when it observes the status and when it asks what is
/// due, and sends what it is given.
#[derive(Debug, Default)]
pub(crate) struct StatusReinforcement {
    held: Option<StatusFact>,
    sent: u32,
    next_at: Option<Instant>,
    /// The held fact was adopted from the previous publisher and has not been republished: it waits
    /// for the new publisher's round to complete.
    awaiting_round: bool,
}

impl StatusReinforcement {
    /// The status as observed at `now` (`now_ms` stamps a change). A change returns the fact to
    /// send at once (the first of its five sends) and replaces any change still being reinforced;
    /// the same status returns nothing.
    pub fn observe(&mut self, status: &str, now: Instant, now_ms: u64) -> Option<StatusFact> {
        if self.held.as_ref().is_some_and(|h| h.status == status) {
            return None;
        }
        let fact = StatusFact { status: status.to_string(), changed_at_rafka_ms: now_ms };
        self.held = Some(fact.clone());
        self.awaiting_round = false;
        self.sent = 1;
        self.next_at = (self.sent < STATUS_SENDS).then(|| now + STATUS_EVERY);
        Some(fact)
    }

    /// Hold `fact` as heard from the previous publisher: no transition is invented and nothing is
    /// sent until [`Self::republish`] says the new publisher's round completed.
    pub fn adopt(&mut self, fact: StatusFact) {
        self.held = Some(fact);
        self.sent = STATUS_SENDS;
        self.next_at = None;
        self.awaiting_round = true;
    }

    /// Whether the held fact is adopted and not yet republished.
    pub fn awaiting_round(&self) -> bool {
        self.awaiting_round
    }

    /// The adopted fact, sent for the first of its five sends at `now`: the same status and the
    /// same `changed_at_rafka_ms` the previous publisher sent, because the state did not change.
    /// Nothing when no adopted fact waits.
    pub fn republish(&mut self, now: Instant) -> Option<StatusFact> {
        if !self.awaiting_round {
            return None;
        }
        self.awaiting_round = false;
        self.sent = 1;
        self.next_at = (self.sent < STATUS_SENDS).then(|| now + STATUS_EVERY);
        self.held.clone()
    }

    /// The next reinforcing send due at `now`, if any.
    pub fn due(&mut self, now: Instant) -> Option<StatusFact> {
        let at = self.next_at?;
        if now < at {
            return None;
        }
        self.sent += 1;
        self.next_at = (self.sent < STATUS_SENDS).then(|| at + STATUS_EVERY);
        self.held.clone()
    }

    /// When the next reinforcing send is due; `None` once the five sends are done.
    pub fn next_due(&self) -> Option<Instant> {
        self.next_at
    }

    pub fn held(&self) -> Option<&StatusFact> {
        self.held.as_ref()
    }

    /// Forget the status and any sends still to come: the publisher role ended.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Which status a [`StatusPublisher`] authors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusScope {
    /// A mesh's status, by mesh name.
    Mesh(String),
    /// The fabric's status.
    Fabric(FabricId),
}

/// The author of one status frame: `node`, while it holds the role (the mesh primary for a mesh's
/// status, the fabric primary for the fabric's) and only then. Losing the role ends the sends still
/// to come.
#[derive(Debug)]
pub struct StatusPublisher {
    node: String,
    scope: StatusScope,
    role: bool,
    schedule: StatusReinforcement,
}

impl StatusPublisher {
    /// A publisher run by `node` for `scope`; it holds no role until [`StatusPublisher::set_role`].
    pub fn new(node: &str, scope: StatusScope) -> Self {
        Self { node: node.to_string(), scope, role: false, schedule: StatusReinforcement::default() }
    }

    /// Hold (or lose) the publisher role. Losing it forgets the status and the sends still to come.
    pub fn set_role(&mut self, primary: bool) {
        if !primary {
            self.schedule.reset();
        }
        self.role = primary;
    }

    /// Whether the node holds the publisher role.
    pub fn has_role(&self) -> bool {
        self.role
    }

    fn frame(&self, fact: &StatusFact) -> Frame {
        match &self.scope {
            StatusScope::Mesh(mesh) => {
                Frame::MeshStatus { mesh: mesh.clone(), status: fact.status.clone(), publisher: self.node.clone(), forwarded_by: None, changed_at_rafka_ms: fact.changed_at_rafka_ms }
            }
            StatusScope::Fabric(fabric) => {
                Frame::FabricStatus { fabric: fabric.clone(), status: fact.status.clone(), publisher: self.node.clone(), forwarded_by: None, changed_at_rafka_ms: fact.changed_at_rafka_ms }
            }
        }
    }

    /// The status as observed: a change is the frame to send at once. A publisher that holds the
    /// role for the first time and observes the status `heard` from the previous publisher adopts
    /// it: the lifecycle state did not change, so the fact keeps its old `changed_at_rafka_ms`. It
    /// is silent until `round_complete` (every member of the publisher's checklist checked in for
    /// this round); then the adopted fact is the frame to send, the first of its five sends, under
    /// this publisher. A status that differs from the adopted one is a change like any other.
    /// Without the role: nothing.
    pub fn observe(&mut self, status: &str, heard: Option<StatusFact>, round_complete: bool, now: Instant, now_ms: u64) -> Option<Frame> {
        if !self.role {
            return None;
        }
        if self.schedule.held().is_none() {
            if let Some(h) = heard.filter(|h| h.status == status) {
                self.schedule.adopt(h);
            }
        }
        if self.schedule.awaiting_round() && self.schedule.held().is_some_and(|h| h.status == status) {
            return round_complete.then(|| self.schedule.republish(now)).flatten().map(|f| self.frame(&f));
        }
        self.schedule.observe(status, now, now_ms).map(|f| self.frame(&f))
    }

    /// Whether the publisher holds an adopted status it has not republished: its round is open.
    pub fn awaiting_round(&self) -> bool {
        self.role && self.schedule.awaiting_round()
    }

    /// The reinforcing send due at `now`: the same fact, the same instant, as the first send.
    pub fn due(&mut self, now: Instant) -> Option<Frame> {
        if !self.role {
            return None;
        }
        self.schedule.due(now).map(|f| self.frame(&f))
    }

    /// When the next send of the status is due, when the role is held and a send remains.
    pub fn next_due(&self) -> Option<Instant> {
        self.role.then(|| self.schedule.next_due()).flatten()
    }
}

/// The statuses a node holds, each until the next change: a mesh's by its name, the fabric's. A
/// frame older than the one held is refused by name, the same one again is taken once.
#[derive(Debug, Clone, Default)]
pub struct StatusBook {
    meshes: Arc<Mutex<HashMap<String, MeshStatus>>>,
    fabric: Arc<Mutex<Option<FabricStatus>>>,
}

impl StatusBook {
    /// Hold what `f` carries if it is newer than what is held; whether it was taken.
    pub fn take(&self, f: &Frame, fabric: &FabricId) -> bool {
        match f {
            Frame::MeshStatus { mesh, status, publisher, changed_at_rafka_ms, .. } => {
                let mut held = self.meshes.lock().unwrap();
                if Self::refuse(held.get(mesh).map(|h| (h.publisher.as_str(), h.changed_at_rafka_ms)), (publisher.as_str(), *changed_at_rafka_ms), &format!("mesh {mesh}")) {
                    return false;
                }
                held.insert(mesh.clone(), MeshStatus { status: status.clone(), publisher: publisher.clone(), changed_at_rafka_ms: *changed_at_rafka_ms });
                true
            }
            Frame::FabricStatus { fabric: f, status, publisher, changed_at_rafka_ms, .. } if f == fabric => {
                let mut held = self.fabric.lock().unwrap();
                if Self::refuse(held.as_ref().map(|h| (h.publisher.as_str(), h.changed_at_rafka_ms)), (publisher.as_str(), *changed_at_rafka_ms), "the fabric") {
                    return false;
                }
                *held = Some(FabricStatus { status: status.clone(), publisher: publisher.clone(), changed_at_rafka_ms: *changed_at_rafka_ms });
                true
            }
            _ => false,
        }
    }

    /// A repeat of the change already held (same publisher, same `changed_at_rafka_ms`) changes
    /// nothing; a change older than the held one is refused by name.
    fn refuse(held: Option<(&str, u64)>, offered: (&str, u64), what: &str) -> bool {
        match held {
            Some((_, h)) if offered.1 < h => {
                tracing::info!(reason = "older-than-held", what, held_at_rafka_ms = h, offered_at_rafka_ms = offered.1, "a status older than the one held is refused");
                true
            }
            Some(h) => h == offered,
            None => false,
        }
    }

    /// The status held for `mesh`.
    pub fn mesh(&self, mesh: &str) -> Option<MeshStatus> {
        self.meshes.lock().unwrap().get(mesh).cloned()
    }

    /// The fabric status held.
    pub fn fabric(&self) -> Option<FabricStatus> {
        self.fabric.lock().unwrap().clone()
    }

    /// Every status held, as the frames its holder `me` replays: the original publisher and the
    /// original instant are kept; only a status `me` did not author names `me` as `forwarded_by`.
    pub fn held_frames(&self, fabric: &FabricId, me: &str) -> Vec<Frame> {
        let by = |publisher: &str| (publisher != me).then(|| me.to_string());
        let mut out: Vec<Frame> = self
            .meshes
            .lock()
            .unwrap()
            .iter()
            .map(|(mesh, h)| Frame::MeshStatus { mesh: mesh.clone(), status: h.status.clone(), publisher: h.publisher.clone(), forwarded_by: by(&h.publisher), changed_at_rafka_ms: h.changed_at_rafka_ms })
            .collect();
        out.sort_by_key(|f| if let Frame::MeshStatus { mesh, .. } = f { mesh.clone() } else { String::new() });
        if let Some(h) = self.fabric.lock().unwrap().as_ref() {
            out.push(Frame::FabricStatus { fabric: fabric.clone(), status: h.status.clone(), publisher: h.publisher.clone(), forwarded_by: by(&h.publisher), changed_at_rafka_ms: h.changed_at_rafka_ms });
        }
        out
    }
}

/// The seat holders a node holds: each seat's record with the latest epoch, until a record that
/// supersedes it (`SeatHolder::supersedes`). A record that does not is refused by name.
#[derive(Debug, Clone)]
pub struct SeatBook {
    inner: Arc<Mutex<HeldSeats>>,
    /// Ticks whenever a record is held: whoever persists the records, or looks at the seats, wakes on it.
    changed: Arc<tokio::sync::watch::Sender<u64>>,
}

impl Default for SeatBook {
    fn default() -> Self {
        Self { inner: Arc::default(), changed: Arc::new(tokio::sync::watch::Sender::new(0)) }
    }
}

#[derive(Debug, Default)]
struct HeldSeats {
    fabric: Option<SeatHolder>,
    meshes: HashMap<String, SeatHolder>,
    /// Seat holders' exact births a peer told this node are proven gone (an entry read): the proof
    /// the peer holds, which this node never heard itself.
    gone: std::collections::HashSet<IncarnationId>,
}

/// What [`SeatBook::take`] did with a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeatTaken {
    /// The record was held; `previous` is the record it superseded.
    Held {
        /// The record it superseded.
        previous: Option<SeatHolder>,
    },
    /// The record is the one already held.
    Same,
    /// The record does not supersede the one held, which is named.
    Refused {
        /// The record held, which this one does not supersede.
        held: SeatHolder,
    },
}

impl SeatBook {
    /// Hold `holder` for `seat` if it supersedes what is held.
    pub fn take(&self, seat: Seat, holder: &SeatHolder) -> SeatTaken {
        let mut held = self.inner.lock().unwrap();
        let slot: &mut Option<SeatHolder> = match seat {
            Seat::FabricPrimary => &mut held.fabric,
            Seat::MeshPrimary => {
                let current = held.meshes.get(&holder.mesh).cloned();
                return match current {
                    Some(c) if &c == holder => SeatTaken::Same,
                    Some(c) if !holder.supersedes(&c) => SeatTaken::Refused { held: c },
                    previous => {
                        held.meshes.insert(holder.mesh.clone(), holder.clone());
                        self.changed.send_modify(|v| *v += 1);
                        SeatTaken::Held { previous }
                    }
                };
            }
        };
        match slot.clone() {
            Some(c) if &c == holder => SeatTaken::Same,
            Some(c) if !holder.supersedes(&c) => SeatTaken::Refused { held: c },
            previous => {
                *slot = Some(holder.clone());
                self.changed.send_modify(|v| *v += 1);
                SeatTaken::Held { previous }
            }
        }
    }

    /// The fabric seat's held record.
    pub fn fabric(&self) -> Option<SeatHolder> {
        self.inner.lock().unwrap().fabric.clone()
    }

    /// Hold that the peer proved `incarnation`, a seat holder's birth, gone.
    pub fn mark_gone(&self, incarnation: IncarnationId) {
        if self.inner.lock().unwrap().gone.insert(incarnation) {
            self.changed.send_modify(|v| *v += 1);
        }
    }

    /// The seat holders' births this node was told are gone.
    pub fn gone(&self) -> std::collections::HashSet<IncarnationId> {
        self.inner.lock().unwrap().gone.clone()
    }

    /// A receiver that wakes each time a record is held.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }

    /// The held record of `mesh`'s primary seat.
    pub fn mesh(&self, mesh: &str) -> Option<SeatHolder> {
        self.inner.lock().unwrap().meshes.get(mesh).cloned()
    }

    /// Every mesh primary record held, by mesh name.
    pub fn meshes(&self) -> std::collections::BTreeMap<String, SeatHolder> {
        self.inner.lock().unwrap().meshes.iter().map(|(m, h)| (m.clone(), h.clone())).collect()
    }

    /// Every record held, as the frames a primary replays to a neighbour that came up.
    pub fn held_frames(&self) -> Vec<Frame> {
        let held = self.inner.lock().unwrap();
        let mut out: Vec<Frame> = held.meshes.values().map(|h| Frame::Seated { seat: Seat::MeshPrimary, holder: h.clone() }).collect();
        out.sort_by_key(|f| if let Frame::Seated { holder, .. } = f { holder.mesh.clone() } else { String::new() });
        if let Some(h) = &held.fabric {
            out.push(Frame::Seated { seat: Seat::FabricPrimary, holder: h.clone() });
        }
        out
    }
}

/// A Concern heard on the backbone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConcernHeard {
    /// The seat whose holder looks silent.
    pub seat: Seat,
    /// The silent holder's node.
    pub node_id: NodeId,
    /// The silent holder's exact birth.
    pub incarnation: IncarnationId,
    /// The mesh primary that marked the birth silent.
    pub observer: String,
}

/// The Concerns a node heard and has not yet looked at, and what wakes the one looking.
#[derive(Debug, Clone, Default)]
pub struct ConcernInbox {
    heard: Arc<Mutex<Vec<ConcernHeard>>>,
    wake: Arc<tokio::sync::Notify>,
}

impl ConcernInbox {
    fn put(&self, c: ConcernHeard) {
        self.heard.lock().unwrap().push(c);
        self.wake.notify_one();
    }

    /// Every Concern heard since the last drain.
    pub fn drain(&self) -> Vec<ConcernHeard> {
        std::mem::take(&mut *self.heard.lock().unwrap())
    }

    /// Resolves when a Concern has been heard since the last wake.
    pub async fn heard(&self) {
        self.wake.notified().await
    }
}

/// What a peer mesh's primary puts on its own mesh channel for a lifecycle or status frame it
/// heard on the backbone: the frame, unchanged but for `forwarded_by`, or nothing. A source Mesh's
/// members are never forwarded as heard: the [`Forwarder`] derives the delta or the stripped full
/// from the two snapshots the primary holds (gossip.md §3.3). `me` is the forwarding node, `own_mesh`
/// its mesh. A status keeps its publisher and its instant: a forwarder never restates one as its
/// own, and forwards only what its author sent (a replay by a holder is for the backbone only).
pub fn forward_of(frame: Frame, me: &str, own_mesh: &str) -> Option<Frame> {
    match frame {
        Frame::NodeDeleting { op, .. } if op.name.mesh != own_mesh => Some(Frame::NodeDeleting { op, forwarded_by: Some(me.to_string()) }),
        Frame::NodeDeleted { op, .. } if op.name.mesh != own_mesh => Some(Frame::NodeDeleted { op, forwarded_by: Some(me.to_string()) }),
        Frame::NodeRestarting { op, .. } if op.name.mesh != own_mesh => Some(Frame::NodeRestarting { op, forwarded_by: Some(me.to_string()) }),
        Frame::MeshStatus { mesh, status, publisher, forwarded_by: None, changed_at_rafka_ms } if mesh != own_mesh => {
            Some(Frame::MeshStatus { mesh, status, publisher, forwarded_by: Some(me.to_string()), changed_at_rafka_ms })
        }
        Frame::FabricStatus { fabric, status, publisher, forwarded_by: None, changed_at_rafka_ms } if publisher != me => {
            Some(Frame::FabricStatus { fabric, status, publisher, forwarded_by: Some(me.to_string()), changed_at_rafka_ms })
        }
        _ => None,
    }
}

/// This process's mesh transport has stopped for good: iroh-gossip refused a subscription (its
/// actor ended, as it does once the endpoint's transports all fail). The process can no longer
/// be heard or answer; it ends, so its exit is the death proof recovery acts on. Set once, with
/// the reason; a node binary waits on it beside its stop signal.
pub fn transport_stopped() -> &'static tokio::sync::watch::Sender<Option<String>> {
    static STOPPED: std::sync::OnceLock<tokio::sync::watch::Sender<Option<String>>> = std::sync::OnceLock::new();
    STOPPED.get_or_init(|| tokio::sync::watch::Sender::new(None))
}

/// Record that the mesh transport stopped (`transport_stopped`), keeping the first reason.
pub fn mark_transport_stopped(reason: String) {
    transport_stopped().send_if_modified(|r| {
        if r.is_none() {
            *r = Some(reason);
            true
        } else {
            false
        }
    });
}

/// Until this process's mesh transport stops; the reason.
pub async fn until_transport_stopped() -> String {
    let mut rx = transport_stopped().subscribe();
    let reason = match rx.wait_for(Option::is_some).await {
        Ok(r) => r.clone(),
        Err(_) => None,
    };
    match reason {
        Some(r) => r,
        None => std::future::pending().await,
    }
}

/// The peers of the live list `live` to join now: those not joined while
/// they stayed live. `joined` forgets a peer that left the list, so one that
/// returns is joined again (`gossip.md` §6).
/// The wait before the next refeed of a channel that is still alone, after waiting `prev`:
/// doubled, never more than one staleness floor (PRD §6.2). A failed rejoin is retried on this
/// bounded schedule forever and decides nothing about any member: the refeed hands peers to
/// iroh-gossip and holds no view, so silence is never death proof.
pub fn refeed_backoff(prev: Duration) -> Duration {
    (prev * 2).min(staleness_floor())
}

/// A held member a channel re-feeds (gossip.md §6): its address, its logical name and id, and
/// how long its coverage has been stale.
#[derive(Debug, Clone)]
pub struct RepairTarget {
    /// The member's gossip address.
    pub addr: EndpointAddr,
    /// The member's logical name.
    pub node: String,
    /// The member's node id.
    pub node_id: String,
    /// How long the member has gone unheard.
    pub silent_for: Duration,
}

/// Which held members are due a repair attempt now (gossip.md §6): a held member silent for the
/// repair window gets one join attempt per window, whatever the channel's neighbours; a member that
/// is no longer held (a retired or departed birth) is never a target, and its schedule entry is
/// dropped with it. Pure: the caller makes the attempts.
pub struct RepairSchedule {
    window: Duration,
    last: std::collections::HashMap<iroh::EndpointId, Instant>,
}

impl RepairSchedule {
    /// A schedule that attempts a member once per `window`.
    pub fn new(window: Duration) -> Self {
        Self { window, last: Default::default() }
    }

    /// The targets due at `now`, out of `held` (every held member with its silence): those silent
    /// for at least the window and not attempted within it.
    pub fn due(&mut self, held: Vec<RepairTarget>, now: Instant) -> Vec<RepairTarget> {
        let held_ids: BTreeSet<iroh::EndpointId> = held.iter().map(|t| t.addr.id).collect();
        self.last.retain(|id, _| held_ids.contains(id));
        let mut out = Vec::new();
        for t in held.into_iter().filter(|t| t.silent_for >= self.window) {
            let recent = self.last.get(&t.addr.id).is_some_and(|at| now.saturating_duration_since(*at) < self.window);
            if !recent {
                self.last.insert(t.addr.id, now);
                out.push(t);
            }
        }
        out
    }
}

/// The held members of `book` that `pick` selects, each with how long it has gone unheard; a
/// terminal `Leaving` is a graceful departure, never a repair target.
pub(crate) fn held_targets(book: &DigestBook, pick: impl Fn(&MeshDigest) -> bool) -> Vec<RepairTarget> {
    let now = Instant::now();
    // A member under an open restart or removal is held through it, but its address is a birth's
    // that is going or gone: it is never handed to a channel again.
    let overlaid = overlaid_nodes(book);
    book.silent_held(now)
        .into_iter()
        .filter(|(d, _)| pick(d) && d.status != rafka_mesh_entity::MemberStatus::Leaving && !overlaid.contains(d.node.node_id.as_str()))
        .filter_map(|(d, silent_for)| gossip_addr(&d).map(|addr| RepairTarget { addr, node: d.node.name.to_string(), node_id: d.node.node_id.to_string(), silent_for }))
        .collect()
}

/// The nodes `book` holds under an open lifecycle overlay (a restart or a removal in flight).
fn overlaid_nodes(book: &DigestBook) -> BTreeSet<String> {
    book.in_flight().into_iter().map(|op| op.node_id.to_string()).collect()
}

/// What a channel reads from its book each repair tick: the held members due a repair attempt,
/// and the keys whose birth is under an open restart or removal, whose addresses the channel
/// forgets.
pub struct Held {
    /// The members due a repair attempt.
    pub targets: Vec<RepairTarget>,
    /// The keys of births under an open restart or removal, whose addresses the channel forgets.
    pub retired: Vec<iroh::EndpointId>,
}

/// [`Held`] from `book` for the members `pick` selects.
pub(crate) fn held_view(book: &DigestBook, pick: impl Fn(&MeshDigest) -> bool + Copy) -> Held {
    let overlaid = overlaid_nodes(book);
    let retired = book
        .all()
        .into_iter()
        .filter(|d| pick(d) && overlaid.contains(d.node.node_id.as_str()))
        .filter_map(|d| d.node.endpoint_id.0.parse::<iroh::PublicKey>().ok())
        .collect();
    Held { targets: held_targets(book, pick), retired }
}

/// The members of `live` not yet in `joined`, recording them as joined; members that left `live`
/// are dropped from `joined`.
pub fn rejoin(joined: &Mutex<BTreeSet<iroh::EndpointId>>, live: &[iroh::EndpointId]) -> Vec<iroh::EndpointId> {
    let mut joined = joined.lock().unwrap();
    joined.retain(|id| live.contains(id));
    live.iter().copied().filter(|id| joined.insert(*id)).collect()
}

/// One joined gossip topic. iroh-gossip closes a subscriber that falls behind
/// (`Lagged`) and expects it to be re-opened; a subscription can also end.
/// Either way it is re-subscribed through its seeds and every peer it was
/// asked to join, so it never silently stops hearing the topic.
///
/// Membership through iroh-gossip's own API (`gossip.md` §6): a peer that
/// leaves the live list it is asked to join and returns is joined again, and
/// a channel with no neighbour for one staleness floor is handed every peer it
/// knows again, once per window while that holds. A node whose neighbours all
/// dropped (a partition longer than the connections' idle timeout) is
/// otherwise never dialed again. Nothing is sent.
#[derive(Clone)]
struct Channel {
    sender: Arc<tokio::sync::RwLock<GossipSender>>,
    /// Every peer this channel was seeded with or asked to join.
    peers: Arc<Mutex<BTreeSet<iroh::EndpointId>>>,
    /// The peers asked while they stayed on the live list.
    joined: Arc<Mutex<BTreeSet<iroh::EndpointId>>>,
    lookup: MemoryLookup,
    endpoint: Endpoint,
}

impl Channel {
    /// Register where the birth `d` names is, and retire the paths a moved key leaves behind.
    fn register(&self, d: &MeshDigest) {
        retire_superseded(&self.endpoint, register_location(&self.lookup, d));
    }

    async fn join(
        gossip: &Gossip,
        endpoint: &Endpoint,
        topic: TopicId,
        fabric: &str,
        node: &str,
        channel: &str,
        seeds: Vec<EndpointAddr>,
        on_frame: Arc<dyn Fn(Frame) + Send + Sync>,
        targets: Arc<dyn Fn() -> Held + Send + Sync>,
        replay: Arc<dyn Fn() -> Vec<Frame> + Send + Sync>,
    ) -> Result<Self> {
        let lookup = MemoryLookup::new();
        endpoint.address_lookup().map_err(|e| anyhow::anyhow!("address_lookup: {e}"))?.add(lookup.clone());
        for s in &seeds {
            lookup.add_endpoint_info(s.clone());
        }
        let peers: BTreeSet<iroh::EndpointId> = seeds.iter().map(|s| s.id).collect();
        let sub = gossip.subscribe(topic, peers.iter().copied().collect()).await?;
        tracing::info_span!("rdm.mesh.membership.update.via-subscribe", node, channel, fabric, peers = peers.len())
            .in_scope(|| tracing::info!("subscribed"));
        let (sender, mut receiver) = sub.split();
        let joined = Arc::new(Mutex::new(peers.clone()));
        let me = Self { sender: Arc::new(tokio::sync::RwLock::new(sender)), peers: Arc::new(Mutex::new(peers)), joined, lookup, endpoint: endpoint.clone() };
        let neighbors: Arc<Mutex<BTreeSet<iroh::EndpointId>>> = Arc::default();
        me.refeed(node.to_string(), channel.to_string(), neighbors.clone(), targets);
        let (shared, known, gossip, fabric, channel, node) = (me.sender.clone(), me.peers.clone(), gossip.clone(), fabric.to_string(), channel.to_string(), node.to_string());
        tokio::spawn(async move {
            loop {
                let reason = loop {
                    let ev = receiver.next().await;
                    match &ev {
                        Some(Ok(Event::Received(m))) => {
                            match Frame::decode(&m.content) {
                                Ok(f) => on_frame(f),
                                // A frame this build does not read is refused by name, with who
                                // sent it, never dropped as if it had not arrived.
                                Err(e) => tracing::info_span!(
                                    "rdm.mesh.membership.reject.via-undecodable-frame",
                                    node = %node,
                                    channel = %channel,
                                    sender = %m.delivered_from.fmt_short(),
                                    bytes = m.content.len(),
                                    error = %e
                                )
                                .in_scope(|| tracing::info!("a gossip frame does not decode")),
                            }
                            continue;
                        }
                        // A neighbour coming up or going down is named: when a channel found its
                        // first neighbour after a join is what a stalled join is read from.
                        Some(Ok(Event::NeighborUp(p))) => {
                            let count = {
                                let mut n = neighbors.lock().unwrap();
                                n.insert(*p);
                                n.len()
                            };
                            tracing::info_span!("rdm.mesh.connection.update.via-neighbour-up", node = %node, channel = %channel, peer = %p.fmt_short(), neighbours = count)
                                .in_scope(|| tracing::info!("a neighbour came up on this channel"));
                            // A join, a heal or a refeed brings a neighbour up: the holder says the
                            // statuses it holds once (gossip.md §3.1), the original authors intact.
                            let sender = shared.read().await.clone();
                            let frames = replay();
                            if !frames.is_empty() {
                                let bytes: usize = frames.iter().map(|f| f.encode().len()).sum();
                                let fulls = frames.iter().filter(|f| matches!(f, Frame::Members { .. })).count();
                                tracing::info_span!("rdm.mesh.membership.update.via-neighbour-replay", node = %node, channel = %channel, peer = %p.fmt_short(), frames = frames.len(), full_chunks = fulls, bytes)
                                    .in_scope(|| tracing::info!("a neighbour came up: this primary says what it holds once"));
                            }
                            for f in frames {
                                let _ = sender.broadcast(Bytes::from(f.encode())).await;
                            }
                        }
                        Some(Ok(Event::NeighborDown(p))) => {
                            let count = {
                                let mut n = neighbors.lock().unwrap();
                                n.remove(p);
                                n.len()
                            };
                            tracing::info_span!("rdm.mesh.connection.update.via-neighbour-down", node = %node, channel = %channel, peer = %p.fmt_short(), neighbours = count)
                                .in_scope(|| tracing::info!("a neighbour went down on this channel"));
                        }
                        _ => {}
                    }
                    if let Some(r) = ended(ev) {
                        break r;
                    }
                };
                neighbors.lock().unwrap().clear();
                let peers: Vec<iroh::EndpointId> = known.lock().unwrap().iter().copied().collect();
                let span = tracing::info_span!("rdm.mesh.membership.update.via-resubscribe", fabric = %fabric, channel = %channel, reason = %reason, peers = peers.len());
                // A refused subscribe means the gossip actor itself has stopped.
                let reopened = match gossip.subscribe(topic, peers).await {
                    Ok(t) => t,
                    Err(e) => {
                        span.in_scope(|| tracing::info!(error = %e, "gossip has stopped; the channel ends"));
                        mark_transport_stopped(format!("gossip refused to re-open channel {channel}: {e}"));
                        return;
                    }
                };
                span.in_scope(|| tracing::info!("subscription re-opened"));
                let (s, r) = reopened.split();
                *shared.write().await = s;
                receiver = r;
            }
        });
        Ok(me)
    }

    async fn broadcast(&self, f: &Frame) -> Result<()> {
        let sender = self.sender.read().await.clone();
        sender.broadcast(Bytes::from(f.encode())).await?;
        Ok(())
    }

    /// Register `peer`'s address; `true` when it is new to this channel.
    fn learn(&self, peer: &EndpointAddr) -> bool {
        self.lookup.add_endpoint_info(peer.clone());
        self.peers.lock().unwrap().insert(peer.id)
    }

    /// Join the peers of the live list `peers` not asked while live: a peer
    /// that left the list and returns is asked again.
    async fn join_peers(&self, peers: Vec<EndpointAddr>) -> Result<usize> {
        for p in &peers {
            self.learn(p);
        }
        let live: Vec<iroh::EndpointId> = peers.iter().map(|p| p.id).collect();
        let fresh = rejoin(&self.joined, &live);
        if !fresh.is_empty() {
            let sender = self.sender.read().await.clone();
            sender.join_peers(fresh.clone()).await?;
        }
        Ok(fresh.len())
    }

    /// While the channel has no neighbour, hand it every peer it knows again: a bounded
    /// join attempt one gossip interval after it was left alone, then backed off (doubling, at
    /// most one staleness floor apart) while that holds (PRD §6.2). A neighbour coming up resets
    /// it. A node whose connections all timed out (a wedge, a network gap longer than the idle
    /// timeout) is otherwise heard again only a whole floor later.
    fn refeed(&self, node: String, channel: String, neighbors: Arc<Mutex<BTreeSet<iroh::EndpointId>>>, targets: Arc<dyn Fn() -> Held + Send + Sync>) {
        let me = self.clone();
        tokio::spawn(async move {
            let mut alone_since: Option<Instant> = None;
            let mut backoff = backbone_gossip_interval();
            let mut schedule = RepairSchedule::new(staleness_floor());
            let mut retired: BTreeSet<iroh::EndpointId> = BTreeSet::new();
            loop {
                tokio::time::sleep(backbone_gossip_interval()).await;
                // Held-member repair (gossip.md §6): every held member whose coverage has gone stale
                // for the repair window gets a bounded join attempt, whatever this channel's
                // neighbours; a failed attempt decides nothing about the member.
                let held = targets();
                // A birth under an open restart or removal: this channel keeps no address for
                // its key and no path to it, so nothing it does re-pins the retired socket.
                for key in &held.retired {
                    if retired.insert(*key) {
                        me.lookup.remove_endpoint_info(*key);
                        me.peers.lock().unwrap().remove(key);
                        me.joined.lock().unwrap().remove(key);
                        let endpoint = me.endpoint.clone();
                        let key = *key;
                        tokio::spawn(async move { endpoint.replace_direct_addrs(key, []).await });
                        tracing::info_span!("rdm.mesh.connection.update.via-restart-address-retired", node = %node, channel = %channel, peer = %key.fmt_short())
                            .in_scope(|| tracing::info!("a birth under an open restart or removal: its address and paths are dropped from this channel"));
                    }
                }
                retired.retain(|k| held.retired.contains(k));
                for t in schedule.due(held.targets, Instant::now()) {
                    me.lookup.add_endpoint_info(t.addr.clone());
                    me.peers.lock().unwrap().insert(t.addr.id);
                    let sender = me.sender.read().await.clone();
                    let joined = sender.join_peers(vec![t.addr.id]).await.is_ok();
                    tracing::info_span!(
                        "rdm.mesh.connection.update.via-refeed",
                        node = %node,
                        channel = %channel,
                        reason = "stale-held-member",
                        peer = %t.addr.id,
                        peer_node = %t.node,
                        peer_node_id = %t.node_id,
                        coverage_age_ms = t.silent_for.as_millis() as u64,
                        peers = 1u64,
                        joined,
                    )
                    .in_scope(|| tracing::info!("a held member's coverage is stale: it is handed to the channel again"));
                }
                if !neighbors.lock().unwrap().is_empty() {
                    alone_since = None;
                    backoff = backbone_gossip_interval();
                    continue;
                }
                let since = *alone_since.get_or_insert_with(Instant::now);
                if since.elapsed() < backoff {
                    continue;
                }
                backoff = refeed_backoff(backoff);
                let peers: Vec<iroh::EndpointId> = me.peers.lock().unwrap().iter().copied().collect();
                if peers.is_empty() {
                    continue;
                }
                let sender = me.sender.read().await.clone();
                let joined = sender.join_peers(peers.clone()).await.is_ok();
                tracing::info_span!("rdm.mesh.connection.update.via-refeed", node = %node, channel = %channel, reason = "no-neighbours-fallback", peers = peers.len(), joined)
                    .in_scope(|| tracing::info!("no neighbour: every known peer handed to the channel again"));
                alone_since = Some(Instant::now());
            }
        });
    }
}

/// Split `items` into runs whose frame (as `frame` builds it, sized as a forward would be)
/// encodes within [`MAX_FRAME`], by the one method the Build facts share
/// ([`crate::chunking::pack_in_order`]). An item that fits no message on its own is left out and
/// named.
pub(crate) fn pack<T: Clone>(items: Vec<T>, frame: impl Fn(Vec<T>) -> Frame) -> Vec<Vec<T>> {
    let (runs, refused) = crate::chunking::pack_in_order(items, |run: &[T]| {
        let bytes = frame(run.to_vec()).encode();
        if bytes.len() > MAX_FRAME {
            Err(bytes.len())
        } else {
            Ok(Bytes::from(bytes))
        }
    });
    for len in refused {
        tracing::info_span!("rdm.mesh.membership.reject.via-item-too-large", frame_bytes = len, limit = MAX_FRAME)
            .in_scope(|| tracing::warn!("a member, overlay or departure that fits no gossip message on its own: left out"));
    }
    runs.into_iter().map(|(run, _)| run).collect()
}

/// Why a subscription was re-opened, if it was.
fn ended(ev: Option<Result<Event, iroh_gossip::api::ApiError>>) -> Option<String> {
    match ev {
        None => Some("subscription ended".into()),
        Some(Err(e)) => Some(format!("subscription error: {e}")),
        Some(Ok(Event::Lagged)) => Some("lagged: events were dropped".into()),
        Some(Ok(_)) => None,
    }
}

/// Which channel a snapshot frame arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    /// The node-admin backbone: every source Mesh complete, loads included.
    Backbone,
    /// This node's own Mesh channel: the other sources as its primary forwards them, no loads.
    MeshChannel,
    /// A topology read (Node RPC op `0x1E`) from a node that holds the topology: installed into
    /// the Mesh channel's receiver. The answerer is a member of this node's mesh: its own mesh's
    /// members are held as the answerer hears them.
    Read,
    /// A topology read from a node of another mesh: everything it serves is a source it holds, not
    /// something it hears. A member of another mesh is topology (R-G2); a member of this node's
    /// own mesh is held only once this node hears it on its mesh channel, so the read leaves it
    /// out of the book (the reader maps it from the read itself).
    ReadPeer,
}

impl Side {
    fn name(self) -> &'static str {
        match self {
            Side::Backbone => "backbone",
            Side::MeshChannel => "mesh-channel",
            Side::Read | Side::ReadPeer => "topology-read",
        }
    }

    fn via(self) -> &'static str {
        match self {
            Side::Backbone => "backbone",
            Side::MeshChannel => "forwarded",
            Side::Read | Side::ReadPeer => "read",
        }
    }
}

/// What a snapshot frame left behind.
#[derive(Default)]
struct SnapshotTaken {
    /// The digests applied to the book, for their addresses.
    carried: Vec<MeshDigest>,
    /// A complete backbone snapshot of a source Mesh: `(mesh, publisher, version, full)`, for the
    /// forwarding primary.
    source: Option<(String, PublisherId, u64, Full)>,
}

/// What a node's view holds, fed by its mesh channel (and, for an admin, the
/// backbone): every member's latest digest, the fabric status, and where it
/// learned each mesh from.
#[derive(Clone, Default)]
struct View {
    /// This node's own mesh: its members are heard directly on the mesh channel.
    mesh: String,
    /// This node's path name, for the spans it emits.
    node: String,
    book: DigestBook,
    statuses: StatusBook,
    concerns: ConcernInbox,
    /// Whether this node is its mesh's primary now: only a primary replays the statuses it holds.
    primary: Arc<AtomicBool>,
    via: Arc<Mutex<HashMap<String, &'static str>>>,
    /// The cross-Mesh projection the backbone carries: each source complete, loads included.
    backbone_rx: Arc<Mutex<SnapshotReceiver>>,
    /// The cross-Mesh projection this Mesh's own channel carries: what the local primary forwards.
    mesh_rx: Arc<Mutex<SnapshotReceiver>>,
    /// What this node, as its Mesh's primary, has published into its Mesh.
    forwarder: Arc<Mutex<Forwarder>>,
    /// Wakes the top-up when a source of the Mesh channel desynchronizes.
    desync: Arc<tokio::sync::Notify>,
}

impl View {
    fn new(mesh: &str, node: &str) -> Self {
        Self { mesh: mesh.to_string(), node: node.to_string(), ..Self::default() }
    }

    /// Hold what `f` carries; the digests it carries (for their addresses). A snapshot frame is
    /// not taken here: see [`View::take_snapshot`].
    fn take(&self, f: &Frame, fabric: &FabricId, via: &'static str) -> Vec<MeshDigest> {
        match f {
            Frame::Digest { digest } if &digest.fabric_id == fabric => {
                if self.book.record(digest.clone()) {
                    self.note(digest, via);
                }
                vec![digest.clone()]
            }
            Frame::NodeDeleting { op, .. } => {
                self.book.deleting(op.clone());
                Vec::new()
            }
            Frame::NodeDeleted { op, .. } => {
                self.book.depart(op.clone());
                Vec::new()
            }
            Frame::NodeRestarting { op, .. } => {
                self.book.restarting(op.clone());
                Vec::new()
            }
            Frame::MeshStatus { .. } | Frame::FabricStatus { .. } => {
                self.statuses.take(f, fabric);
                Vec::new()
            }
            Frame::Seated { seat, holder } => {
                self.note_seat(*seat, holder, via);
                Vec::new()
            }
            Frame::Concern { seat, node_id, incarnation, observer } => {
                self.concerns.put(ConcernHeard { seat: *seat, node_id: node_id.clone(), incarnation: incarnation.clone(), observer: observer.clone() });
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    /// A `Members` chunk or a `MembersDelta`, arrived on `side`. A snapshot is applied to the book
    /// only once complete; a delta only at exactly its base. A frame that does not belong on its
    /// channel is refused by name.
    fn take_snapshot(&self, f: &Frame, fabric: &FabricId, side: Side) -> SnapshotTaken {
        match f {
            Frame::Members { mesh, publisher, forwarded_by, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, departed, .. } => {
                if forwarded_by.is_some() == (side == Side::Backbone) {
                    tracing::info_span!(
                        "rdm.mesh.membership.reject.via-members-on-wrong-channel",
                        node = %self.node,
                        channel = side.name(),
                        mesh = %mesh,
                        publisher = %publisher,
                        forwarded_by = forwarded_by.as_deref().unwrap_or(""),
                    )
                    .in_scope(|| tracing::warn!("a Members frame on a channel it is not published on: refused"));
                    return SnapshotTaken::default();
                }
                let chunk = Chunk {
                    mesh: mesh.clone(),
                    publisher: publisher.clone(),
                    forwarded_by: forwarded_by.clone(),
                    topology_version: *topology_version,
                    snapshot_id: *snapshot_id,
                    chunk_index: *chunk_index,
                    chunk_count: *chunk_count,
                    digests: digests.clone(),
                    in_flight: in_flight.clone(),
                    departed: departed.clone(),
                };
                // The receiver's lock is held until the book holds what the version names: a reader of
                // the held version never sees a version ahead of the book it describes.
                let mut rx = self.receiver(side).lock().unwrap();
                let taken = rx.take_chunk(chunk);
                match taken {
                    Taken::Waiting { .. } => SnapshotTaken::default(),
                    Taken::Refused(r) => {
                        drop(rx);
                        tracing::info_span!(
                            "rdm.mesh.membership.reject.via-snapshot",
                            node = %self.node,
                            channel = side.name(),
                            mesh = %mesh,
                            publisher = %publisher,
                            topology_version = *topology_version,
                            snapshot_id = *snapshot_id,
                            reason = ?r,
                        )
                        .in_scope(|| tracing::info!("a snapshot chunk refused: the held projection stands"));
                        SnapshotTaken::default()
                    }
                    Taken::Installed(i) => {
                        let installed = self.installed(&i, fabric, side);
                        drop(rx);
                        installed
                    }
                }
            }
            Frame::MembersDelta { mesh, source_publisher, base_version, topology_version, changed, removed, in_flight, departed, .. } => {
                if side == Side::Backbone {
                    tracing::info_span!("rdm.mesh.membership.reject.via-members-on-wrong-channel", node = %self.node, channel = side.name(), mesh = %mesh, publisher = %source_publisher, forwarded_by = "")
                        .in_scope(|| tracing::warn!("a MembersDelta on the backbone, where only complete snapshots travel: refused"));
                    return SnapshotTaken::default();
                }
                let delta = Delta { changed: changed.clone(), removed: removed.clone(), in_flight: in_flight.clone(), departed: departed.clone() };
                let mut rx = self.mesh_rx.lock().unwrap();
                let moved = rx.take_delta(mesh, source_publisher, *base_version, *topology_version, &delta);
                match moved {
                    Moved::Applied { delta, .. } => {
                        tracing::info_span!(
                            "rdm.mesh.membership.update.via-delta",
                            node = %self.node,
                            mesh = %mesh,
                            source_publisher = %source_publisher,
                            base_version = *base_version,
                            topology_version = *topology_version,
                            changed = delta.changed.len(),
                            removed = delta.removed.len(),
                            in_flight = delta.in_flight.len(),
                            departed = delta.departed.len(),
                        )
                        .in_scope(|| tracing::info!("a delta applied at exactly its base version"));
                        let full = Full::new(delta.changed.clone(), delta.in_flight.clone(), delta.departed.clone());
                        let carried = self.apply_full(mesh, &full, fabric, side);
                        drop(rx);
                        SnapshotTaken { carried, source: None }
                    }
                    Moved::Duplicate { held_version } => {
                        drop(rx);
                        tracing::info_span!("rdm.mesh.membership.update.via-delta-already-held", node = %self.node, mesh = %mesh, held_version, topology_version = *topology_version)
                            .in_scope(|| tracing::info!("a delta at or below the held version: a copy already applied"));
                        SnapshotTaken::default()
                    }
                    Moved::Desynced { gap, .. } => {
                        drop(rx);
                        let (held_version, held_publisher) = match &gap {
                            Gap::Version { held, .. } => (*held as i64, String::new()),
                            Gap::OtherEpoch { held } => (-1, held.to_string()),
                            _ => (-1, String::new()),
                        };
                        tracing::info_span!(
                            "rdm.mesh.membership.update.via-version-gap",
                            node = %self.node,
                            mesh = %mesh,
                            source_publisher = %source_publisher,
                            gap = %gap,
                            held_version,
                            held_publisher = %held_publisher,
                            base_version = *base_version,
                            topology_version = *topology_version,
                        )
                        .in_scope(|| tracing::info!("the delta does not follow what is held: this source is desynchronized and tops up from its own primary"));
                        self.desync.notify_one();
                        SnapshotTaken::default()
                    }
                }
            }
            _ => SnapshotTaken::default(),
        }
    }

    fn receiver(&self, side: Side) -> &Arc<Mutex<SnapshotReceiver>> {
        match side {
            Side::Backbone => &self.backbone_rx,
            Side::MeshChannel | Side::Read | Side::ReadPeer => &self.mesh_rx,
        }
    }

    /// A complete snapshot is held: apply it to the book.
    fn installed(&self, i: &Install, fabric: &FabricId, side: Side) -> SnapshotTaken {
        tracing::info_span!(
            "rdm.mesh.membership.update.via-snapshot-installed",
            node = %self.node,
            channel = side.name(),
            mesh = %i.mesh,
            publisher = %i.publisher,
            topology_version = i.topology_version,
            snapshot_id = i.snapshot_id,
            members = i.full.member_count(),
            new_epoch = i.new_epoch,
            resumed = i.resumed,
            refreshed = i.refreshed,
        )
        .in_scope(|| tracing::debug!("a complete snapshot installed"));
        let carried = self.apply_full(&i.mesh, &i.full, fabric, side);
        let source = (side == Side::Backbone && i.mesh != self.mesh).then(|| (i.mesh.clone(), i.publisher.clone(), i.topology_version, i.full.clone()));
        SnapshotTaken { carried, source }
    }

    /// Apply the members, overlays and departures of a snapshot or a delta to the book. Departures
    /// first: a digest of a departed birth in the same snapshot is refused.
    fn apply_full(&self, _mesh: &str, full: &Full, fabric: &FabricId, side: Side) -> Vec<MeshDigest> {
        for op in full.departed() {
            self.book.depart(op);
        }
        for op in full.in_flight() {
            if op.is_restart() {
                self.book.restarting(op);
            } else {
                self.book.deleting(op);
            }
        }
        let mut carried = Vec::new();
        for d in full.digests().into_iter().filter(|d| &d.fabric_id == fabric) {
            // A read carries this node's own digest as the node read holds it: this node's own
            // book is the authority on itself.
            if matches!(side, Side::Read | Side::ReadPeer) && d.node.name.to_string() == self.node {
                continue;
            }
            // A member of this node's own mesh is heard directly on its mesh channel: an
            // aggregate copy is never "heard only through forwarded topology", so it earns no
            // forwarded grace (gossip.md §3.2), and the Mesh channel's copy of it adds nothing.
            let own = d.node.name.mesh == self.mesh;
            let taken = match (side, own) {
                (Side::Backbone, true) => self.book.record(d.clone()),
                (Side::Backbone, false) => self.book.record_forwarded(d.clone()),
                (Side::MeshChannel, true) => false,
                (Side::MeshChannel, false) => self.book.record_topology(d.clone()),
                // A read installs what an entry answer installed, by the rule `learn` applies
                // (R-G2): a peer mesh's member is topology and refreshes no liveness; a member of
                // this node's own mesh is held as the answerer heard it.
                (Side::Read, true) => self.book.record_forwarded(d.clone()),
                (Side::Read, false) | (Side::ReadPeer, false) => self.book.record_topology(d.clone()),
                (Side::ReadPeer, true) => false,
            };
            if taken {
                self.note(&d, side.via());
                carried.push(d);
            }
        }
        carried
    }

    fn note(&self, d: &MeshDigest, via: &'static str) {
        self.via.lock().unwrap().entry(d.node.name.mesh.clone()).or_insert(via);
    }

    /// Hold a seat record this node heard, naming what it did with it.
    fn note_seat(&self, seat: Seat, holder: &SeatHolder, via: &'static str) -> SeatTaken {
        let taken = self.book.seats.take(seat, holder);
        let (outcome, previous) = match &taken {
            SeatTaken::Held { previous } => ("held", previous.as_ref().map(ToString::to_string).unwrap_or_default()),
            SeatTaken::Same => ("same", String::new()),
            SeatTaken::Refused { held } => ("refused-not-newer", held.to_string()),
        };
        if !matches!(taken, SeatTaken::Same) {
            tracing::info_span!("rdm.mesh.seat.update.via-announcement", node = %self.node, seat = seat.name(), holder = %holder, via, outcome, previous = %previous)
                .in_scope(|| tracing::info!("a seat record was heard"));
        }
        taken
    }
}

/// A node's membership: its mesh channel and what it holds.
#[derive(Clone)]
pub struct Membership {
    mesh: Channel,
    view: View,
    fabric: FabricId,
    cut_off: Arc<Mutex<CutOff>>,
    /// The digest book this membership holds.
    pub book: DigestBook,
    /// The Rafka-time this process composes; every gossip stamp on this membership reads it.
    clock: SharedClock,
    /// The last `digest_seq` this birth published: sender-local, from 1 for this process's birth.
    digest_seq: Arc<std::sync::atomic::AtomicU64>,
    /// This process's CPU and RAM, sampled into every digest it publishes.
    load: Arc<crate::load::LoadSampler>,
}

impl Membership {
    /// The process's iroh endpoint this membership rides: for read-only observations
    /// (`crate::iroh_obs`), never for sending.
    pub fn endpoint(&self) -> Endpoint {
        self.mesh.endpoint.clone()
    }

    /// Join `mesh`'s channel (`mesh_id` names it) through `seeds`, as `node`. `clock` is the
    /// Rafka-time the process composes: every timestamp this membership and its backbone put on a
    /// frame reads it.
    pub async fn join(gossip: &Gossip, endpoint: &Endpoint, fabric: &FabricId, mesh: &str, mesh_id: &MeshId, node: &str, clock: SharedClock, seeds: Vec<EndpointAddr>) -> Result<Self> {
        let view = View::new(mesh, node);
        let (replay_view, replay_fabric, replay_me, replay_clock) = (view.clone(), fabric.clone(), node.to_string(), clock.clone());
        // A neighbour coming up on this channel (a join, a heal, a refeed) hears what the primary
        // holds: the statuses, and the forwarded full of every source it has published (gossip.md
        // §3.1, §3.3).
        let replay: Arc<dyn Fn() -> Vec<Frame> + Send + Sync> = Arc::new(move || {
            if replay_view.primary.load(Ordering::Relaxed) {
                let mut frames = replay_view.statuses.held_frames(&replay_fabric, &replay_me);
                frames.extend(replay_view.book.seats.held_frames());
                frames.extend(replay_view.forwarder.lock().unwrap().replay(&replay_me, replay_clock.now_rafka_ms()));
                frames
            } else {
                Vec::new()
            }
        });
        let lookup_slot: Arc<Mutex<Option<(MemoryLookup, Endpoint)>>> = Arc::default();
        let (v, f, ls) = (view.clone(), fabric.clone(), lookup_slot.clone());
        let on_frame: Arc<dyn Fn(Frame) + Send + Sync> = Arc::new(move |frame: Frame| {
            let carried = match &frame {
                Frame::Members { .. } | Frame::MembersDelta { .. } => v.take_snapshot(&frame, &f, Side::MeshChannel).carried,
                _ => v.take(&frame, &f, "mesh-channel"),
            };
            if let Some((l, ep)) = ls.lock().unwrap().as_ref() {
                for d in &carried {
                    retire_superseded(ep, register_location(l, d));
                }
            }
        });
        let (book, own_mesh, me_name) = (view.book.clone(), mesh.to_string(), node.to_string());
        let targets: Arc<dyn Fn() -> Held + Send + Sync> =
            Arc::new(move || held_view(&book, |d| d.node.name.mesh == own_mesh && d.node.name.to_string() != me_name));
        let channel = Channel::join(gossip, endpoint, mesh_topic(fabric, mesh_id), fabric.as_str(), node, &format!("mesh:{mesh}"), seeds, on_frame, targets, replay).await?;
        *lookup_slot.lock().unwrap() = Some((channel.lookup.clone(), channel.endpoint.clone()));
        let me = Self { mesh: channel, book: view.book.clone(), view, fabric: fabric.clone(), cut_off: Arc::default(), clock, digest_seq: Arc::default(), load: Arc::new(crate::load::LoadSampler::for_this_process()) };
        me.watch_meshes(node.to_string());
        Ok(me)
    }

    /// Evidence of what this node holds: a mesh it first hears, or hears
    /// again, and a mesh none of whose members it still hears. And whether it
    /// is cut off: it has heard other members, and now hears none.
    fn watch_meshes(&self, node: String) {
        let (book, via, cut_off) = (self.book.clone(), self.view.via.clone(), self.cut_off.clone());
        tokio::spawn(async move {
            let mut held: BTreeSet<String> = BTreeSet::new();
            let mut heard_others = false;
            let mut heard: BTreeMap<String, (String, bool)> = BTreeMap::new();
            loop {
                let current = book.current(book.staleness_floor());
                let now_heard: BTreeMap<String, (String, bool)> = current.iter().map(|d| (d.node.node_id.to_string(), (d.node.name.to_string(), d.status == rafka_mesh_entity::MemberStatus::Leaving))).collect();
                for (id, (name, leaving)) in heard.iter().filter(|(id, _)| !now_heard.contains_key(*id)) {
                    if *leaving || *name == node || book.is_departed(id) {
                        continue;
                    }
                    let silent_ms = book.get(id).map(|(_, silent)| silent.as_millis() as u64).unwrap_or(0);
                    tracing::info_span!("rdm.mesh.membership.update.via-member-stale", node = %node, member = %name, member_id = %id, silent_ms, staleness_ms = book.staleness_floor().as_millis() as u64)
                        .in_scope(|| tracing::info!("a member is no longer heard within the staleness floor: held, not current"));
                }
                heard = now_heard;
                let others = current.iter().filter(|d| d.node.name.to_string() != node).count();
                heard_others |= others > 0;
                let off = heard_others && others == 0;
                if let Some(role) = cut_off.lock().unwrap().observe(off, Instant::now()) {
                    tracing::info_span!("rdm.mesh.membership.update.via-cut-off", node = %node, role)
                        .in_scope(|| tracing::info!("no other member is heard: what this node holds is not acted on"));
                }
                let now: BTreeSet<String> = current.into_iter().map(|d| d.node.name.mesh).collect();
                for m in now.difference(&held) {
                    let v = via.lock().unwrap().get(m).copied().unwrap_or("mesh-channel");
                    tracing::info_span!("rdm.mesh.membership.update.via-mesh-learned", node = %node, mesh = %m, via = v)
                        .in_scope(|| tracing::info!("this node holds the mesh"));
                }
                for m in held.difference(&now) {
                    tracing::info_span!("rdm.mesh.membership.update.via-mesh-silent", node = %node, mesh = %m)
                        .in_scope(|| tracing::info!("no member of the mesh is heard"));
                    via.lock().unwrap().remove(m);
                }
                held = now;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
    }

    /// Hold `d` as heard (an entry pull, or a frame another channel carried),
    /// and register where it is.
    pub fn learn(&self, d: MeshDigest, via: &'static str) {
        if d.fabric_id != self.fabric {
            return;
        }
        self.mesh.register(&d);
        // An own-mesh member is heard directly and ages from that; a peer mesh's member an entry
        // answer names is topology.
        let taken = if d.node.name.mesh == self.view.mesh { self.book.record_forwarded(d.clone()) } else { self.book.record_topology(d.clone()) };
        if taken {
            self.view.note(&d, via);
        }
    }

    /// This node's path name, as it publishes itself.
    pub fn node(&self) -> &str {
        &self.view.node
    }

    /// May this node's view authorize anything now: not cut off, and not
    /// within one silence window of healing ([`CutOff::authorizes`]).
    pub fn authorizes(&self) -> bool {
        self.cut_off.lock().unwrap().authorizes(Instant::now())
    }

    /// How many meshes this node holds a live member of.
    pub fn meshes_held(&self) -> usize {
        self.book.current(self.book.staleness_floor()).into_iter().map(|d| d.node.name.mesh).collect::<BTreeSet<_>>().len()
    }

    /// The fabric status this node holds: the last one published, until the next change.
    pub fn fabric_status(&self) -> Option<FabricStatus> {
        self.view.statuses.fabric()
    }

    /// The status of `mesh` this node holds: the last one published, until the next change.
    pub fn mesh_status(&self, mesh: &str) -> Option<MeshStatus> {
        self.view.statuses.mesh(mesh)
    }

    /// The statuses this node holds, as the frames it replays (original publisher and instant
    /// kept): what an entry pull answers (`entry::EntryAnswer::statuses`).
    pub fn status_frames(&self, me: &str) -> Vec<Frame> {
        self.view.statuses.held_frames(&self.fabric, me)
    }

    /// Hold the statuses an entry pull answered.
    pub fn learn_statuses(&self, frames: &[Frame]) {
        for f in frames {
            self.view.statuses.take(f, &self.fabric);
        }
    }

    /// The seat holders this node holds.
    pub fn seats(&self) -> &SeatBook {
        &self.view.book.seats
    }

    /// The Concerns this node heard.
    pub fn concerns(&self) -> &ConcernInbox {
        &self.view.concerns
    }

    /// Hold the seat records an entry read named (`topology_read`): each only if it supersedes what
    /// this node holds.
    pub fn learn_seats(&self, records: &[(Seat, SeatHolder, bool)], via: &'static str) {
        for (seat, holder, gone) in records {
            self.view.note_seat(*seat, holder, via);
            if *gone {
                self.view.book.seats.mark_gone(holder.incarnation.clone());
            }
        }
    }

    /// The Rafka-time this process composed.
    pub fn clock(&self) -> &SharedClock {
        &self.clock
    }

    /// Broadcast this member's digest on its mesh channel (and record it). The heartbeat is
    /// stamped here, once, for every caller: `digest_seq` is the next of this birth's own
    /// sequence (from 1), and `emitted_at_rafka_ms` is the composed clock's reading. The order a
    /// receiver acts on is the sequence, whatever the clock reads.
    pub async fn publish(&self, d: &MeshDigest) -> Result<()> {
        let mut d = d.clone();
        d.digest_seq = self.digest_seq.fetch_add(1, Ordering::SeqCst) + 1;
        d.emitted_at_rafka_ms = self.clock.now_rafka_ms();
        d.load = Some(self.load.sample());
        self.book.record(d.clone());
        self.mesh.broadcast(&Frame::Digest { digest: d }).await
    }

    /// Broadcast a frame onto this mesh's channel (a forward).
    pub async fn forward(&self, f: &Frame) -> Result<()> {
        self.mesh.broadcast(f).await
    }

    /// Put what the forwarder decided for `source_mesh` onto this mesh's channel, and name it.
    async fn send_forward(&self, me: &str, source_mesh: &str, publisher: &PublisherId, version: u64, out: Forward) {
        match out {
            Forward::Full(frames) => {
                let bytes: usize = frames.iter().map(|f| f.encode().len()).sum();
                tracing::info_span!("rdm.mesh.membership.update.via-forwarded-full", node = %me, mesh = %source_mesh, source_publisher = %publisher, topology_version = version, reason = "first-for-source", chunks = frames.len(), bytes)
                    .in_scope(|| tracing::info!("the first publication of this source into the mesh is a full, loads omitted"));
                for f in &frames {
                    let _ = self.forward(f).await;
                }
            }
            Forward::Delta(frame) => {
                if let Frame::MembersDelta { base_version, changed, removed, in_flight, departed, .. } = frame.as_ref() {
                    tracing::info_span!(
                        "rdm.mesh.membership.update.via-forwarded-delta",
                        node = %me,
                        mesh = %source_mesh,
                        source_publisher = %publisher,
                        base_version = *base_version,
                        topology_version = version,
                        changed = changed.len(),
                        removed = removed.len(),
                        in_flight = in_flight.len(),
                        departed = departed.len(),
                    )
                    .in_scope(|| tracing::info!("a source moved: one delta from the version last published into this mesh"));
                }
                let _ = self.forward(&frame).await;
            }
            Forward::Nothing(_) => {}
        }
    }

    /// Publish a lifecycle event this node authored on its own mesh channel,
    /// applying it to its own view first.
    pub async fn publish_lifecycle(&self, f: &Frame) -> Result<()> {
        let _ = self.view.take(f, &self.fabric, "mesh-channel");
        self.mesh.broadcast(f).await
    }

    /// The baselines of the source meshes this node holds: while it is its Mesh's primary, what
    /// it last published into its Mesh; otherwise what it holds from its primary.
    fn held_sources(&self) -> Vec<SourceSnapshot> {
        if self.view.primary.load(Ordering::Relaxed) {
            self.view.forwarder.lock().unwrap().snapshots()
        } else {
            self.view.mesh_rx.lock().unwrap().snapshots()
        }
    }

    /// The topology this node holds, one snapshot per mesh (Node RPC op `0x1E` serves it). A
    /// remote mesh is the source this node holds, with the publisher and version it holds it at.
    /// This node's own mesh is its members as its book hears them, `own` among them, at the
    /// publisher and version its primary last put into the mesh. A mesh this node holds no
    /// published snapshot of is absent: no version is invented for it.
    pub fn held_topology(&self, own: &MeshDigest) -> Vec<SourceSnapshot> {
        // A held source is topology, served as published: whether its members are alive is the
        // reader's to learn (a read installs them as topology and refreshes no liveness).
        let mut held = self.held_sources();
        let current = self.book.current(self.book.staleness_floor());
        let mesh = &self.view.mesh;
        if let Some(at) = held.iter_mut().find(|s| &s.mesh == mesh) {
            let mut members: Vec<MeshDigest> = current
                .into_iter()
                .filter(|d| &d.node.name.mesh == mesh && d.fabric_id == self.fabric && d.node.node_id != own.node.node_id)
                .collect();
            members.push(own.clone());
            members.sort_by(|a, b| a.node.node_id.cmp(&b.node.node_id));
            let (in_flight, departed) = self.book.overlays_of(mesh);
            at.digests = members;
            at.in_flight = in_flight;
            at.departed = departed;
        }
        held
    }

    /// One chunk of a topology read from another node. The mesh installs atomically once every
    /// chunk of its snapshot is held, through the same receiver the Mesh channel's snapshots
    /// install into; a source it ends the desynchronization of is resumed. Every member the
    /// snapshot carries is held as heard and its location registered.
    pub fn take_read_chunk(&self, chunk: Chunk, answerer_mesh: &str) -> Taken {
        let mut rx = self.view.mesh_rx.lock().unwrap();
        let taken = rx.take_read_chunk(chunk);
        if let Taken::Installed(i) = &taken {
            let side = if answerer_mesh == self.view.mesh { Side::Read } else { Side::ReadPeer };
            self.view.installed(i, &self.fabric, side);
            drop(rx);
            for d in i.full.digests().into_iter().filter(|d| d.fabric_id == self.fabric) {
                self.mesh.register(&d);
            }
        }
        taken
    }

    /// The sources of the Mesh channel that are desynchronized now, with the gap that did it.
    pub fn desynced_sources(&self) -> Vec<(String, Gap)> {
        self.view.mesh_rx.lock().unwrap().desynced()
    }

    /// The version this node holds of `mesh` from its Mesh channel and the publisher it is of.
    pub fn held_source_version(&self, mesh: &str) -> Option<(PublisherId, u64)> {
        self.view.mesh_rx.lock().unwrap().held_version(mesh)
    }

    /// This node's own Mesh's primary node-admin as this node's view holds it: the Ready
    /// node-admin of the Mesh with the lowest NodeId, the election the node-admins make
    /// (rafka-node-admin-core `election::elect`). The node tops up from it and from no other.
    pub fn mesh_primary(&self) -> Option<MeshDigest> {
        self.book
            .current(self.book.staleness_floor())
            .into_iter()
            .filter(|d| d.node.name.mesh == self.view.mesh && d.node.name.kind == rafka_mesh_entity::NodeKind::NodeAdmin && d.status == rafka_mesh_entity::MemberStatus::ReadyForTraffic)
            .min_by(|a, b| a.node.node_id.cmp(&b.node.node_id))
    }

    /// Resume desynchronized sources from this Mesh's own primary (gossip.md §3.3): when a delta
    /// did not follow what is held, the node reads the current topology of each desynchronized
    /// mesh from its own mesh-primary (Node RPC op `0x1E`), installs it atomically, and resumes.
    /// Never the remote Mesh, never over gossip. One attempt per gap signal: nothing is retried or
    /// held for later; the next delta or full from the primary signals or resumes it again.
    pub fn spawn_top_up(&self, fetch: TopUpFetch) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                // One attempt per signal: a delta heard on a desynchronized source signals again,
                // so a top-up that failed is attempted when the primary next speaks, never in a loop.
                me.view.desync.notified().await;
                let gaps = me.desynced_sources();
                if !gaps.is_empty() {
                    me.top_up(&fetch, gaps).await;
                }
            }
        })
    }

    async fn top_up(&self, fetch: &TopUpFetch, gaps: Vec<(String, Gap)>) {
        let meshes = gaps.iter().map(|(m, _)| m.as_str()).collect::<Vec<_>>().join(",");
        let reasons = gaps.iter().map(|(m, g)| format!("{m}:{g}")).collect::<Vec<_>>().join(",");
        let node = self.view.node.clone();
        let Some(primary) = self.mesh_primary() else {
            tracing::info_span!("rdm.mesh.entry.reject.via-top-up", node = %node, meshes = %meshes, reasons = %reasons, reason = "no-mesh-primary-held")
                .in_scope(|| tracing::info!("no Ready node-admin of this Mesh is held: nothing to top up from"));
            return;
        };
        let names: Vec<String> = gaps.iter().map(|(m, _)| m.clone()).collect();
        match fetch(self.clone(), primary.clone(), names).await {
            Ok(done) => {
                let still: Vec<String> = self.desynced_sources().into_iter().map(|(m, _)| m).collect();
                tracing::info_span!(
                    "rdm.mesh.entry.update.via-top-up",
                    node = %node,
                    meshes = %meshes,
                    reasons = %reasons,
                    mesh_primary = %primary.node.name,
                    served_by = %primary.node.name,
                    installed = done.installed.iter().map(|(m, v)| format!("{m}@{v}")).collect::<Vec<_>>().join(","),
                    still_desynced = still.join(","),
                )
                .in_scope(|| tracing::info!("topped up from this Mesh's own primary"));
            }
            Err(e) => {
                tracing::info_span!("rdm.mesh.entry.reject.via-top-up", node = %node, meshes = %meshes, reasons = %reasons, mesh_primary = %primary.node.name, reason = "pull-refused", error = %e)
                    .in_scope(|| tracing::info!("the Mesh's primary did not answer the top-up"));
            }
        }
    }

    /// Re-broadcast `digest()` every `every` until the returned handle is aborted.
    pub fn publish_every<F>(&self, every: Duration, digest: F) -> tokio::task::JoinHandle<()>
    where
        F: Fn() -> MeshDigest + Send + 'static,
    {
        let me = self.clone();
        tokio::spawn(async move {
            let mut last_heartbeat: Option<Instant> = None;
            loop {
                let d = digest();
                let _ = me.publish(&d).await;
                if last_heartbeat.is_none_or(|at| at.elapsed() >= HEARTBEAT_EVERY) {
                    last_heartbeat = Some(Instant::now());
                    me.heartbeat(&d);
                }
                tokio::time::sleep(every).await;
            }
        })
    }

    /// One heartbeat span, a root of its own: this node is alive and publishing, how many other
    /// members its book holds as heard, and what its clock reads against the host's wall clock.
    fn heartbeat(&self, d: &MeshDigest) {
        let peer_count = self.book.current(self.book.staleness_floor()).iter().filter(|m| m.node.node_id != d.node.node_id).count() as u64;
        let wall_time_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|t| t.as_millis() as i64).unwrap_or(0);
        let rafka_time_ms = self.clock.now_rafka_ms() as i64;
        tracing::info_span!(
            parent: None,
            "rdm.mesh.node.update.via-heartbeat",
            node = %d.node.name,
            node_id = %d.node.node_id,
            mesh = %d.node.name.mesh,
            peer_count,
            wall_time_ms,
            rafka_time_ms,
            clock_skew_ms = rafka_time_ms - wall_time_ms,
            digest_seq = self.digest_seq.load(Ordering::SeqCst),
        )
        .in_scope(|| tracing::info!("heartbeat"));
    }

    /// Join this mesh channel through more peers (its members, once known).
    pub async fn join_peers(&self, peers: Vec<EndpointAddr>) -> Result<usize> {
        self.mesh.join_peers(peers).await
    }
}

/// What a top-up read installed.
#[derive(Debug, Clone, Default)]
pub struct TopUpDone {
    /// `(mesh, topology_version)` of each mesh installed.
    pub installed: Vec<(String, u64)>,
}

/// Reads the topology of `meshes` from a mesh primary and installs it into the given membership
/// (the topology read, over Node RPC): membership holds no Node RPC client, the process
/// composition hands it one.
pub(crate) type TopUpFetch = std::sync::Arc<dyn Fn(Membership, MeshDigest, Vec<String>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<TopUpDone, String>> + Send>> + Send + Sync>;

/// Which of a backbone's adopted statuses still wait for their round to complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Announced {
    /// The mesh status still waits for its round to complete.
    pub mesh_awaiting_round: bool,
    /// The fabric status still waits for its round to complete.
    pub fabric_awaiting_round: bool,
}

/// A node-admin's place on the backbone. What it hears there it holds; while
/// it is its mesh's primary it publishes its mesh's members and forwards the
/// other meshes' onto its mesh channel; while it is the fabric primary it
/// publishes the fabric's status.
#[derive(Clone)]
pub struct Backbone {
    channel: Channel,
    node: String,
    mesh: String,
    fabric: FabricId,
    forwarding: Arc<AtomicBool>,
    publishing: Arc<AtomicBool>,
    status_publishing: Arc<AtomicBool>,
    membership: Membership,
    mesh_status: Arc<Mutex<StatusPublisher>>,
    fabric_status: Arc<Mutex<StatusPublisher>>,
    /// Wakes the status sender when a change starts its reinforcement.
    wake: Arc<tokio::sync::Notify>,
    /// This admin's exact identity as a publisher: its path name and the birth that holds it.
    publisher: PublisherId,
    /// Set when the seat is taken: the next round puts the full of every source into the Mesh.
    full_due: Arc<AtomicBool>,
    /// The version counter of this Mesh's aggregate, and the complete projection it was last bumped on.
    version: Arc<Mutex<SourceVersion>>,
    /// This Mesh as the aggregate last published it (publisher, version, full): the forwarder's
    /// own-Mesh source.
    own: Arc<Mutex<Option<(u64, Full)>>>,
}

impl Backbone {
    /// Join the backbone as `node`, whose current birth is `incarnation`: the exact identity every
    /// aggregate this admin publishes names as its publisher.
    pub async fn join(gossip: &Gossip, endpoint: &Endpoint, membership: &Membership, mesh: &str, node: &str, incarnation: IncarnationId, seeds: Vec<EndpointAddr>) -> Result<Self> {
        let forwarding = Arc::new(AtomicBool::new(false));
        let (m, fw, me, own) = (membership.clone(), forwarding.clone(), node.to_string(), mesh.to_string());
        let on_frame: Arc<dyn Fn(Frame) + Send + Sync> = Arc::new(move |frame: Frame| {
            if matches!(frame, Frame::Members { .. } | Frame::MembersDelta { .. }) {
                let taken = m.view.take_snapshot(&frame, &m.fabric, Side::Backbone);
                for d in &taken.carried {
                    register_location(&m.mesh.lookup, d);
                }
                // A complete snapshot of a source Mesh: the forwarding primary says what moved
                // since it last published that source into its own Mesh.
                if let (Some((source_mesh, publisher, version, full)), true) = (taken.source, fw.load(Ordering::Relaxed)) {
                    let sent = m.view.forwarder.lock().unwrap().source(&me, &source_mesh, &publisher, version, &full, m.clock.now_rafka_ms());
                    let (m, me) = (m.clone(), me.clone());
                    tokio::spawn(async move { m.send_forward(&me, &source_mesh, &publisher, version, sent).await });
                }
                return;
            }
            for d in m.view.take(&frame, &m.fabric, "backbone") {
                m.mesh.register(&d);
            }
            if !fw.load(Ordering::Relaxed) {
                return;
            }
            if let Some(f) = forward_of(frame, &me, &own) {
                let m = m.clone();
                tokio::spawn(async move {
                    let _ = m.forward(&f).await;
                });
            }
        });
        let (replay_view, replay_fabric, replay_me) = (membership.view.clone(), membership.fabric.clone(), node.to_string());
        let replay: Arc<dyn Fn() -> Vec<Frame> + Send + Sync> = Arc::new(move || {
            if replay_view.primary.load(Ordering::Relaxed) {
                let mut frames = replay_view.statuses.held_frames(&replay_fabric, &replay_me);
                frames.extend(replay_view.book.seats.held_frames());
                frames
            } else {
                Vec::new()
            }
        });
        let (book, own_mesh) = (membership.book.clone(), mesh.to_string());
        let targets: Arc<dyn Fn() -> Held + Send + Sync> =
            Arc::new(move || held_view(&book, |d| d.node.name.mesh != own_mesh && d.node.name.kind == rafka_mesh_entity::NodeKind::NodeAdmin));
        let channel = Channel::join(gossip, endpoint, backbone_topic(&membership.fabric), membership.fabric.as_str(), node, "backbone", seeds, on_frame, targets, replay).await?;
        let me = Self {
            channel,
            node: node.to_string(),
            mesh: mesh.to_string(),
            fabric: membership.fabric.clone(),
            forwarding,
            publishing: Arc::default(),
            status_publishing: Arc::default(),
            membership: membership.clone(),
            mesh_status: Arc::new(Mutex::new(StatusPublisher::new(node, StatusScope::Mesh(mesh.to_string())))),
            fabric_status: Arc::new(Mutex::new(StatusPublisher::new(node, StatusScope::Fabric(membership.fabric.clone())))),
            wake: Arc::default(),
            publisher: PublisherId { node: node.to_string(), incarnation },
            full_due: Arc::default(),
            version: Arc::default(),
            own: Arc::default(),
        };
        me.spawn_status_sender();
        Ok(me)
    }

    /// Sends the reinforcing repeats of a status change, one per second until its fifth send. It
    /// sleeps with nothing due and wakes on a change: no status traffic without a change.
    fn spawn_status_sender(&self) {
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                let next = [me.mesh_status.lock().unwrap().next_due(), me.fabric_status.lock().unwrap().next_due()].into_iter().flatten().min();
                match next {
                    Some(at) => {
                        tokio::select! {
                            _ = me.wake.notified() => {}
                            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(at)) => {}
                        }
                    }
                    None => me.wake.notified().await,
                }
                let now = Instant::now();
                let due: Vec<Frame> = [me.mesh_status.lock().unwrap().due(now), me.fabric_status.lock().unwrap().due(now)].into_iter().flatten().collect();
                for f in due {
                    me.send_status(&f).await;
                }
            }
        });
    }

    /// One status send: the backbone, the author's own mesh channel, and its own view.
    async fn send_status(&self, f: &Frame) {
        let (scope, status, changed_at) = match f {
            Frame::MeshStatus { mesh, status, changed_at_rafka_ms, .. } => (format!("mesh:{mesh}"), status.as_str(), *changed_at_rafka_ms),
            Frame::FabricStatus { fabric, status, changed_at_rafka_ms, .. } => (format!("fabric:{fabric}"), status.as_str(), *changed_at_rafka_ms),
            _ => return,
        };
        tracing::info_span!("rdm.mesh.fabric.update.via-status-send", node = %self.node, scope = %scope, status, changed_at_rafka_ms = changed_at)
            .in_scope(|| tracing::info!("status sent"));
        let _ = self.channel.broadcast(f).await;
        let _ = self.membership.forward(f).await;
        let _ = self.membership.view.take(f, &self.fabric, "mesh-channel");
    }

    /// The statuses as this node's topology states them now. While this node is its mesh's primary
    /// its mesh's status is sent when it changed, and while it is the fabric primary the fabric's;
    /// a status that did not change sends nothing. A change is sent at once and reinforced by the
    /// status sender (five sends in all). A status adopted from the previous publisher is sent
    /// only once that scope's round is complete (`mesh_round` / `fabric_round`), with its old
    /// instant. The answer says which adopted statuses still wait for their round.
    pub async fn announce_statuses(&self, mesh_status: &str, fabric_status: &str, mesh_round: bool, fabric_round: bool) -> Announced {
        let (now, now_ms) = (Instant::now(), self.membership.clock.now_rafka_ms());
        let (first, announced): (Vec<Frame>, Announced) = {
            let view = &self.membership.view;
            let heard_mesh = view.statuses.mesh(&self.mesh).map(|h| StatusFact { status: h.status, changed_at_rafka_ms: h.changed_at_rafka_ms });
            let heard_fabric = view.statuses.fabric().map(|h| StatusFact { status: h.status, changed_at_rafka_ms: h.changed_at_rafka_ms });
            let mut mesh = self.mesh_status.lock().unwrap();
            let mut fabric = self.fabric_status.lock().unwrap();
            let frames = [mesh.observe(mesh_status, heard_mesh, mesh_round, now, now_ms), fabric.observe(fabric_status, heard_fabric, fabric_round, now, now_ms)].into_iter().flatten().collect();
            (frames, Announced { mesh_awaiting_round: mesh.awaiting_round(), fabric_awaiting_round: fabric.awaiting_round() })
        };
        if !first.is_empty() {
            self.wake.notify_one();
        }
        for f in first {
            self.send_status(&f).await;
        }
        announced
    }

    fn role(flag: &AtomicBool, on: bool) -> Option<&'static str> {
        (flag.swap(on, Ordering::Relaxed) != on).then_some(if on { "start" } else { "stop" })
    }

    /// The publisher and version of `mesh`'s complete snapshot this admin holds from the backbone.
    pub fn held_source(&self, mesh: &str) -> Option<(PublisherId, u64)> {
        self.membership.view.backbone_rx.lock().unwrap().held_version(mesh)
    }

    /// Whether this backbone publishes its mesh's members now (it is the mesh's primary).
    pub fn is_mesh_primary(&self) -> bool {
        self.publishing.load(Ordering::Relaxed)
    }

    /// Be (or stop being) this mesh's publisher and forwarder: its primary.
    pub fn set_mesh_primary(&self, primary: bool) {
        self.membership.view.primary.store(primary, Ordering::Relaxed);
        if self.publishing.load(Ordering::Relaxed) != primary {
            // A forwarder that takes (or loses) the seat has published nothing into its Mesh: the
            // first publication of every source after taking it is a full (gossip.md §3.3).
            self.membership.view.forwarder.lock().unwrap().reset();
            self.full_due.store(primary, Ordering::Relaxed);
        }
        self.mesh_status.lock().unwrap().set_role(primary);
        if let Some(role) = Self::role(&self.publishing, primary) {
            tracing::info_span!("rdm.mesh.backbone.update.via-aggregate-publisher", node = %self.node, mesh = %self.mesh, role)
                .in_scope(|| tracing::info!("mesh aggregate publication"));
        }
        if let Some(role) = Self::role(&self.forwarding, primary) {
            tracing::info_span!("rdm.mesh.backbone.update.via-forwarder", node = %self.node, mesh = %self.mesh, role)
                .in_scope(|| tracing::info!("peer meshes forwarded onto this mesh's channel"));
        }
    }

    /// Be (or stop being) the fabric-status publisher: the fabric primary.
    pub fn set_fabric_primary(&self, primary: bool) {
        self.fabric_status.lock().unwrap().set_role(primary);
        if let Some(role) = Self::role(&self.status_publishing, primary) {
            tracing::info_span!("rdm.mesh.fabric.update.via-status-publisher", node = %self.node, fabric = %self.fabric, role)
                .in_scope(|| tracing::info!("fabric status publication"));
        }
    }

    /// One publication round: this mesh's `members` (while its primary) on the backbone, as one
    /// chunked snapshot at the Mesh's `topology_version` (gossip.md §3.1). The version bumps when
    /// the normalized projection changed since the last round and never otherwise, so a quiet Mesh
    /// republishes the same version as passive anti-entropy. The same projection is this Mesh's
    /// own source: what its members need of it goes into the Mesh as a delta or a full. A status
    /// is not part of a round: [`Backbone::announce_statuses`] sends one when it changes.
    pub async fn publish(&self, membership: &Membership, members: Vec<MeshDigest>) {
        let sent = membership.clock.now_rafka_ms();
        if !self.publishing.load(Ordering::Relaxed) {
            return;
        }
        // This mesh's own overlays and departures only; a peer mesh's travel in that mesh's
        // aggregate.
        let (in_flight, departed) = membership.book.overlays_of(&self.mesh);
        let full = Full::new(members, in_flight, departed);
        let (version, snapshot_id, bumped) = {
            let mut v = self.version.lock().unwrap();
            let before = v.version();
            let version = v.observe(&full);
            (version, v.next_snapshot_id(), version != before)
        };
        if bumped {
            tracing::info_span!("rdm.mesh.backbone.update.via-topology-version", node = %self.node, mesh = %self.mesh, publisher = %self.publisher, topology_version = version, members = full.member_count())
                .in_scope(|| tracing::info!("the normalized projection of this mesh changed: its topology_version bumped"));
        }
        let publisher = self.publisher.clone();
        let mesh = self.mesh.clone();
        for frame in crate::snapshot::chunks_of(&full, |digests, in_flight, departed, chunk_index, chunk_count| Frame::Members {
            mesh: mesh.clone(),
            publisher: publisher.clone(),
            forwarded_by: None,
            topology_version: version,
            published_at_rafka_ms: sent,
            snapshot_id,
            chunk_index,
            chunk_count,
            digests,
            in_flight,
            departed,
        }) {
            let _ = self.channel.broadcast(&frame).await;
        }
        // This Mesh's members hear every birth directly: its own source goes into the Mesh as the
        // overlays and departures a member that missed an event resynchronizes from (gossip.md
        // §3.3), so the forwarded projection of it names no member.
        let overlays = Full::new(Vec::new(), full.in_flight(), full.departed());
        *self.own.lock().unwrap() = Some((version, overlays.clone()));
        if self.full_due.swap(false, Ordering::Relaxed) {
            self.forward_fulls().await;
        } else {
            let out = self.membership.view.forwarder.lock().unwrap().source(&self.node, &self.mesh, &self.publisher, version, &overlays, sent);
            self.membership.send_forward(&self.node, &self.mesh, &self.publisher, version, out).await;
        }
    }

    /// The forwarded full of every source this primary holds (its own Mesh and each peer Mesh),
    /// without loads, on taking the seat: the first publication of every source into this Mesh.
    /// Nothing sends it again on a timer; a held source whose peer Mesh is unheard is not
    /// re-sent, so no member's coverage is renewed by a word its primary did not receive.
    pub async fn forward_fulls(&self) {
        if !self.forwarding.load(Ordering::Relaxed) {
            return;
        }
        let mut held: Vec<(String, PublisherId, u64, Full)> = self.membership.view.backbone_rx.lock().unwrap().sources().into_iter().filter(|(m, ..)| m != &self.mesh).collect();
        if let Some((version, full)) = self.own.lock().unwrap().clone() {
            held.push((self.mesh.clone(), self.publisher.clone(), version, full));
        }
        let now = self.membership.clock.now_rafka_ms();
        let frames = self.membership.view.forwarder.lock().unwrap().fulls(&self.node, &held, now);
        let bytes: usize = frames.iter().map(|f| f.encode().len()).sum();
        tracing::info_span!("rdm.mesh.membership.update.via-forwarded-full", node = %self.node, mesh = %self.mesh, reason = "seat", sources = held.len(), chunks = frames.len(), bytes)
            .in_scope(|| tracing::info!("the full of every source put into this mesh, loads omitted"));
        for f in frames {
            let _ = self.membership.forward(&f).await;
        }
    }

    /// Publish a lifecycle event this admin authored on the backbone.
    pub async fn publish_lifecycle(&self, f: &Frame) -> Result<()> {
        self.channel.broadcast(f).await
    }

    /// This admin took `seat`: it holds the record and says it once. A mesh primary's record goes
    /// onto its mesh channel and the backbone; the fabric primary's onto the backbone. Nothing
    /// repeats it: a neighbour that comes up is replayed it by the primary, like a status.
    pub async fn announce_seat(&self, seat: Seat, holder: SeatHolder) {
        let frame = Frame::Seated { seat, holder: holder.clone() };
        self.membership.view.note_seat(seat, &holder, "own");
        tracing::info_span!("rdm.mesh.seat.update.via-announce", node = %self.node, seat = seat.name(), holder = %holder).in_scope(|| tracing::info!("seat taken, announced"));
        let _ = self.channel.broadcast(&frame).await;
        if seat == Seat::MeshPrimary {
            let _ = self.membership.forward(&frame).await;
        }
    }

    /// Say on the backbone that `seat`'s holder birth looks silent from here. A warning, once per
    /// call; the caller decides when a call is owed.
    ///
    /// `holder_key` is the holder's iroh key, when known: the span carries iroh's local view of
    /// it (`crate::iroh_obs`), read once; it decides nothing.
    pub async fn concern(&self, seat: Seat, node_id: NodeId, incarnation: IncarnationId, holder_key: Option<iroh::EndpointId>) {
        let seen = crate::iroh_obs::observe_remote(&self.channel.endpoint, holder_key).await;
        tracing::info_span!(
            "rdm.mesh.seat.update.via-concern",
            node = %self.node,
            seat = seat.name(),
            holder_node_id = %node_id,
            holder_incarnation = %incarnation.0,
            iroh_known_addrs = %seen.known_addrs,
            iroh_active_addrs = %seen.active_addrs,
        )
        .in_scope(|| tracing::info!("a seat holder looks silent: Concern published"));
        let _ = self.channel.broadcast(&Frame::Concern { seat, node_id, incarnation, observer: self.node.clone() }).await;
    }

    /// Join the backbone through every node-admin known, once each.
    pub async fn join_admins(&self, admins: Vec<EndpointAddr>) {
        if let Ok(n) = self.channel.join_peers(admins).await {
            if n > 0 {
                tracing::info_span!("rdm.mesh.connection.update.via-backbone-peers-joined", node = %self.node, peers = n)
                    .in_scope(|| tracing::info!("backbone peers joined"));
            }
        }
    }
}


/// Whether a node's view may authorize anything. Cut off: it heard other
/// members and now hears none. Healed: it hears one again, but the rest of
/// its view still holds every member it lost as silent until each publishes
/// again, which every live member does within one staleness floor. A view
/// authorizes nothing while cut off, nor for one staleness floor after it healed.
#[derive(Debug)]
pub(crate) struct CutOff {
    off: bool,
    healed_at: Option<Instant>,
    staleness_floor: Duration,
}

impl Default for CutOff {
    fn default() -> Self {
        Self::with_floor(staleness_floor())
    }
}

impl CutOff {
    pub(crate) fn with_floor(staleness_floor: Duration) -> Self {
        Self { off: false, healed_at: None, staleness_floor }
    }

    /// Record whether the node is cut off at `now`; the change, if any
    /// (`"start"` / `"stop"`).
    pub fn observe(&mut self, off: bool, now: Instant) -> Option<&'static str> {
        if self.off == off {
            return None;
        }
        self.off = off;
        if !off {
            self.healed_at = Some(now);
        }
        Some(if off { "start" } else { "stop" })
    }

    /// May the view authorize anything at `now`?
    pub fn authorizes(&self, now: Instant) -> bool {
        !self.off && self.healed_at.is_none_or(|t| now.saturating_duration_since(t) >= self.staleness_floor)
    }
}

/// How a held member was last heard. Only `Direct` and `Forwarded` are liveness: a word received
/// on the mesh channel or the backbone. `Topology` is a member known only from forwarded topology
/// (a delta, a full, a replay, an entry answer): it is held, and never ages to silent for want of
/// a forwarded frame (gossip.md §3.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Heard {
    /// The member's own digest, received here.
    Direct,
    /// A peer mesh's member listed in an aggregate received on the backbone: it ages, with the
    /// forwarded grace.
    Forwarded,
    /// Known only from topology forwarded onto the mesh channel.
    Topology,
}

/// The latest digest held per logical node: when it was taken, and how it was heard.
#[derive(Debug, Clone)]
pub struct DigestBook {
    inner: Arc<Mutex<HashMap<String, (MeshDigest, Instant, Heard)>>>,
    /// Ticks when a member's birth (`MeshDigest::node`) is first held or
    /// changes, or a departure is accepted; a fresher digest of the same
    /// birth does not tick.
    births: Arc<tokio::sync::watch::Sender<u64>>,
    /// Proven departures by NodeId, with when this book accepted each: held
    /// for `retention` of local age, then forgotten.
    departed: Arc<Mutex<HashMap<String, (LifecycleOp, Instant)>>>,
    /// Open lifecycle overlays by operation key: the node is held, and not routable.
    in_flight: Arc<Mutex<HashMap<(String, u32, String), LifecycleOp>>>,
    retention: Duration,
    /// The staleness floor.
    staleness_floor: Duration,
    /// The backbone topic's gossip interval.
    backbone_gossip_interval: Duration,
    /// The seat holders this node holds (`Frame::Seated`, an entry read): an input to every election.
    pub seats: SeatBook,
}

impl Default for DigestBook {
    fn default() -> Self {
        Self::with_retention(DEPARTED_RETENTION)
    }
}

/// Why a digest was not taken.
fn reject_departed(d: &MeshDigest, via: &'static str) {
    tracing::info_span!(
        "rdm.mesh.membership.reject.via-departed-birth",
        node = %d.node.name,
        node_id = %d.node.node_id,
        incarnation_id = %d.node.incarnation.0,
        via,
    )
    .in_scope(|| tracing::info!("a digest of a departed node: refused, the departure stands"));
}

/// How long a member has gone unheard at `now`, measured against the staleness floor: a forwarded
/// copy is measured against the forwarded staleness floor, so the difference is taken off. A
/// terminal `Leaving` is never succeeded, so it earns no forwarded extra.
fn unheard(d: &MeshDigest, at: &Instant, heard: Heard, extra: Duration, now: Instant) -> Duration {
    let age = now.saturating_duration_since(*at);
    match heard {
        // A terminal `Leaving` is a proven departure, not silence: it ages from its receipt.
        Heard::Topology if d.status != rafka_mesh_entity::MemberStatus::Leaving => Duration::ZERO,
        Heard::Forwarded if d.status != rafka_mesh_entity::MemberStatus::Leaving => age.saturating_sub(extra),
        _ => age,
    }
}

/// One birth has one runtime: a digest of the held incarnation that names
/// another runtime locator or control domain is refused by name, and the
/// held one stays.
fn runtime_changed(held: &MeshDigest, d: &MeshDigest) -> bool {
    let changed = held.node.incarnation == d.node.incarnation
        && held.node.runtime.is_some()
        && d.node.runtime.is_some()
        && held.node.runtime != d.node.runtime;
    if changed {
        tracing::info_span!(
            "rdm.mesh.runtime.reject.via-locator-changed",
            node = %d.node.name,
            node_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.0,
            held_locator_fingerprint = %held.node.runtime.as_ref().map(|r| r.locator_fingerprint()).unwrap_or_default(),
            offered_locator_fingerprint = %d.node.runtime.as_ref().map(|r| r.locator_fingerprint()).unwrap_or_default(),
        )
        .in_scope(|| tracing::info!("a birth offered another runtime"));
    }
    changed
}

impl DigestBook {
    pub(crate) fn with_retention(retention: Duration) -> Self {
        Self::with_floor(retention, staleness_floor(), backbone_gossip_interval())
    }

    /// A book with this staleness floor and backbone gossip interval.
    pub(crate) fn with_floor(retention: Duration, staleness_floor: Duration, backbone_gossip_interval: Duration) -> Self {
        Self {
            inner: Arc::default(),
            births: Arc::new(tokio::sync::watch::Sender::new(0)),
            departed: Arc::default(),
            in_flight: Arc::default(),
            retention,
            staleness_floor,
            backbone_gossip_interval,
            seats: SeatBook::default(),
        }
    }

    /// The staleness floor: a member unheard for longer is `PendingReconnect`.
    pub fn staleness_floor(&self) -> Duration {
        self.staleness_floor
    }

    /// The staleness floor of a member heard only through forwarded topology: the staleness floor,
    /// plus one backbone gossip interval, plus the time its mesh's next primary takes to forward it
    /// (the staleness floor and one backbone gossip interval), gossip.md §3.2.
    pub(crate) fn forwarded_staleness_floor(&self) -> Duration {
        self.staleness_floor + self.backbone_gossip_interval + self.staleness_floor + self.backbone_gossip_interval
    }

    fn expire_departed(&self) {
        self.expire_departed_at(Instant::now())
    }

    fn expire_departed_at(&self, now: Instant) {
        self.departed.lock().unwrap().retain(|_, (_, at)| now.duration_since(*at) < self.retention);
    }

    /// Has this node been accepted as departed, within the retention?
    pub fn is_departed(&self, node_id: &str) -> bool {
        self.is_departed_at(node_id, Instant::now())
    }

    /// Is `node_id` held as departed at `now`?
    pub(crate) fn is_departed_at(&self, node_id: &str, now: Instant) -> bool {
        self.expire_departed_at(now);
        self.departed.lock().unwrap().contains_key(node_id)
    }

    /// Accept a proven departure: the birth leaves the book, the NodeId is
    /// held departed for the retention, and every overlay of the operation
    /// clears. `false` when the departure was already held.
    pub fn depart(&self, op: LifecycleOp) -> bool {
        self.depart_at(op, Instant::now())
    }

    /// Accept the departure `op` at `now`.
    pub fn depart_at(&self, op: LifecycleOp, now: Instant) -> bool {
        self.expire_departed_at(now);
        let id = op.node_id.to_string();
        {
            let mut departed = self.departed.lock().unwrap();
            if departed.contains_key(&id) {
                return false;
            }
            departed.insert(id.clone(), (op.clone(), now));
        }
        self.in_flight.lock().unwrap().remove(&op.key());
        let removed = self.inner.lock().unwrap().remove(&id).is_some();
        tracing::info_span!(
            "rdm.mesh.membership.remove.via-node-deleted",
            node = %op.name,
            node_id = %op.node_id,
            incarnation_id = %op.incarnation.0,
            build_id = %op.build_id,
            attempt = op.attempt,
            operation = %op.operation,
            was_held = removed,
        )
        .in_scope(|| tracing::info!("the exact birth has left: removed from the view, held departed"));
        self.births.send_modify(|v| *v += 1);
        true
    }

    /// Accept an open restart (`op` is `restart-node:<path>`, naming the birth being restarted):
    /// that birth stays held through its `Leaving` and silence and is not routable, until a later
    /// birth of its NodeId is heard. `false` when already held, when the node departed, or when a
    /// later birth is already held (the restart is over).
    pub fn restarting(&self, op: LifecycleOp) -> bool {
        if self.is_departed(op.node_id.as_str()) {
            return false;
        }
        if self.inner.lock().unwrap().get(op.node_id.as_str()).is_some_and(|(held, _, _)| held.node.incarnation != op.incarnation && held.node.supersedes.as_ref() == Some(&op.incarnation)) {
            return false;
        }
        let new = self.in_flight.lock().unwrap().insert(op.key(), op.clone()).is_none();
        if new {
            tracing::info_span!(
                "rdm.mesh.membership.update.via-node-restarting",
                node = %op.name,
                node_id = %op.node_id,
                incarnation_id = %op.incarnation.0,
                build_id = %op.build_id,
                attempt = op.attempt,
                operation = %op.operation,
            )
            .in_scope(|| tracing::info!("the node is being restarted: held through its Leaving, not routable"));
        }
        new
    }

    /// The open restart of `node_id`'s birth `incarnation`, if any.
    pub fn restart_of(&self, node_id: &str, incarnation: &rafka_mesh_entity::IncarnationId) -> Option<LifecycleOp> {
        self.in_flight.lock().unwrap().values().find(|op| op.is_restart() && op.node_id.as_str() == node_id && &op.incarnation == incarnation).cloned()
    }

    /// A later birth of `d`'s NodeId is held: every open restart of an earlier birth is over.
    fn close_restarts(&self, d: &MeshDigest) {
        let mut in_flight = self.in_flight.lock().unwrap();
        let before = in_flight.len();
        in_flight.retain(|_, op| !(op.is_restart() && op.node_id == d.node.node_id && op.incarnation != d.node.incarnation));
        if in_flight.len() != before {
            tracing::info_span!("rdm.mesh.membership.update.via-node-restarted", node = %d.node.name, node_id = %d.node.node_id, incarnation_id = %d.node.incarnation.0)
                .in_scope(|| tracing::info!("the restarted node's new birth is heard: the restart is over"));
        }
    }

    /// Accept an open lifecycle overlay: the node stays held and stops being
    /// routable. `false` when the overlay was already held or the node has
    /// already departed.
    pub fn deleting(&self, op: LifecycleOp) -> bool {
        if self.is_departed(op.node_id.as_str()) {
            return false;
        }
        let new = self.in_flight.lock().unwrap().insert(op.key(), op.clone()).is_none();
        if new {
            tracing::info_span!(
                "rdm.mesh.membership.update.via-node-deleting",
                node = %op.name,
                node_id = %op.node_id,
                build_id = %op.build_id,
                attempt = op.attempt,
                operation = %op.operation,
            )
            .in_scope(|| tracing::info!("the node is being removed: held, not routable"));
            self.births.send_modify(|v| *v += 1);
        }
        new
    }

    /// The open overlays this book holds.
    pub fn in_flight(&self) -> Vec<LifecycleOp> {
        let mut v: Vec<LifecycleOp> = self.in_flight.lock().unwrap().values().cloned().collect();
        v.sort_by(|a, b| a.key().cmp(&b.key()));
        v
    }

    /// The departures this book holds within the retention.
    pub fn departed(&self) -> Vec<LifecycleOp> {
        self.expire_departed();
        let mut v: Vec<LifecycleOp> = self.departed.lock().unwrap().values().map(|(op, _)| op.clone()).collect();
        v.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        v
    }

    /// The open overlays and retained departures of `mesh`'s own nodes: what
    /// that mesh's primary publishes for it.
    pub fn overlays_of(&self, mesh: &str) -> (Vec<LifecycleOp>, Vec<LifecycleOp>) {
        let mine = |ops: Vec<LifecycleOp>| ops.into_iter().filter(|op| op.name.mesh == mesh).collect::<Vec<_>>();
        (mine(self.in_flight()), mine(self.departed()))
    }

    /// May application routing select this node: held, not departed, and
    /// under no open lifecycle overlay. The resolver never reads this.
    pub fn routable(&self, node_id: &str) -> bool {
        !self.is_departed(node_id)
            && self.inner.lock().unwrap().contains_key(node_id)
            && !self.in_flight.lock().unwrap().values().any(|op| op.node_id.as_str() == node_id)
    }

    /// Hold `d` as its member's latest word, unless it is older than what is
    /// held: a digest of the same birth whose `digest_seq` is no higher than the
    /// held one's (the birth's own heartbeat order; Rafka-time stamps never
    /// decide it), or a digest of the birth the held one supersedes. Gossip can deliver
    /// a digest late; a late one never refreshes a silent member or reverts
    /// its status. A digest of a departed node is refused: the departure
    /// stands for the retention, whatever incarnation the digest names.
    /// `false` when `d` was not taken.
    pub fn record(&self, d: MeshDigest) -> bool {
        self.record_at(d, Instant::now())
    }

    /// [`Self::record`], heard at `now`.
    pub fn record_at(&self, d: MeshDigest, now: Instant) -> bool {
        if self.is_departed_at(d.node.node_id.as_str(), now) {
            reject_departed(&d, "mesh-channel");
            return false;
        }
        let mut inner = self.inner.lock().unwrap();
        if let Some((held, _, _)) = inner.get(d.node.node_id.as_str()) {
            if runtime_changed(held, &d) {
                return false;
            }
            let older = if held.node.incarnation == d.node.incarnation {
                d.digest_seq <= held.digest_seq
            } else {
                held.node.supersedes.as_ref() == Some(&d.node.incarnation)
            };
            if older {
                return false;
            }
        }
        let birth_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, _, _)| held.node != d.node);
        inner.insert(d.node.node_id.to_string(), (d.clone(), now, Heard::Direct));
        drop(inner);
        if birth_changed {
            self.close_restarts(&d);
            self.births.send_modify(|v| *v += 1);
        }
        true
    }

    /// Digests heard within `fresh` of now.
    /// Hold `d` as forwarded by its mesh's primary, which only forwards a
    /// member it hears: an equal copy of the held digest keeps the member
    /// heard (the primary's word that it still is). Otherwise as [`Self::record`].
    pub(crate) fn record_forwarded(&self, d: MeshDigest) -> bool {
        self.record_forwarded_at(d, Instant::now())
    }

    /// Hold `d` as topology forwarded onto the mesh channel (a peer Mesh's projection, loads
    /// omitted, or an entry answer). A forwarded word is not liveness: it never refreshes when a
    /// held member was heard, and a member held only from forwards never ages to silent. A
    /// member already heard keeps its instant and how it was heard; only its digest moves on.
    pub(crate) fn record_topology(&self, d: MeshDigest) -> bool {
        let now = Instant::now();
        if self.is_departed_at(d.node.node_id.as_str(), now) {
            reject_departed(&d, "forwarded");
            return false;
        }
        let mut inner = self.inner.lock().unwrap();
        let mut held_entry = None;
        if let Some((held, at, heard)) = inner.get(d.node.node_id.as_str()) {
            if runtime_changed(held, &d) {
                return false;
            }
            let older = if held.node.incarnation == d.node.incarnation {
                d.digest_seq < held.digest_seq
            } else {
                held.node.supersedes.as_ref() == Some(&d.node.incarnation)
            };
            if older {
                return false;
            }
            if held.node.incarnation == d.node.incarnation && d.digest_seq == held.digest_seq && held.node == d.node {
                return true;
            }
            held_entry = Some((*at, *heard));
        }
        let birth_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, _, _)| held.node != d.node);
        let (at, heard) = held_entry.unwrap_or((now, Heard::Topology));
        inner.insert(d.node.node_id.to_string(), (d.clone(), at, heard));
        drop(inner);
        if birth_changed {
            self.close_restarts(&d);
            self.births.send_modify(|v| *v += 1);
        }
        true
    }

    /// [`Self::record_forwarded`], heard at `now`.
    pub fn record_forwarded_at(&self, d: MeshDigest, now: Instant) -> bool {
        self.record_forwarded_inner(d, now)
    }

    fn record_forwarded_inner(&self, d: MeshDigest, now: Instant) -> bool {
        if self.is_departed_at(d.node.node_id.as_str(), now) {
            reject_departed(&d, "forwarded");
            return false;
        }
        let mut inner = self.inner.lock().unwrap();
        if let Some((held, _, _)) = inner.get(d.node.node_id.as_str()) {
            if runtime_changed(held, &d) {
                return false;
            }
            let older = if held.node.incarnation == d.node.incarnation {
                d.digest_seq < held.digest_seq
            } else {
                held.node.supersedes.as_ref() == Some(&d.node.incarnation)
            };
            if older {
                return false;
            }
        }
        let birth_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, _, _)| held.node != d.node);
        inner.insert(d.node.node_id.to_string(), (d.clone(), now, Heard::Forwarded));
        drop(inner);
        if birth_changed {
            self.close_restarts(&d);
            self.births.send_modify(|v| *v += 1);
        }
        true
    }

    /// Ticks each time a member's birth is first held or changes (a new
    /// incarnation, a moved address, a runtime published): what a process's
    /// live resolver is fed from. A fresher digest of the same birth does not.
    pub fn birth_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.births.subscribe()
    }

    /// The digests heard within `fresh`.
    pub fn current(&self, fresh: Duration) -> Vec<MeshDigest> {
        self.current_at(fresh, Instant::now())
    }

    /// Digests heard within `fresh` of `now`. A graceful departure (a terminal `Leaving`, then
    /// nothing for one gossip interval) is gone within that tick (fabric-node-lifecycle.md §6): it
    /// is not current, so no view and no aggregate carries it on.
    pub(crate) fn current_at(&self, fresh: Duration, now: Instant) -> Vec<MeshDigest> {
        let extra = self.forwarded_staleness_floor() - self.staleness_floor;
        self.inner
            .lock()
            .unwrap()
            .values()
            .filter(|(d, at, fw)| unheard(d, at, *fw, extra, now) <= fresh)
            .filter(|(d, at, _)| !(d.status == rafka_mesh_entity::MemberStatus::Leaving && now.saturating_duration_since(*at) > gossip_interval()))
            .map(|(d, _, _)| d.clone())
            .collect()
    }

    /// The digests this node received straight from their members (their own word, on the channel
    /// they publish to) at or after `since`: the members that checked in since a round began. A
    /// member known only from forwarded topology, or last heard before `since`, is not here.
    pub fn heard_direct_since(&self, since: Instant) -> Vec<MeshDigest> {
        self.inner.lock().unwrap().values().filter(|(_, at, heard)| *heard == Heard::Direct && *at >= since).map(|(d, _, _)| d.clone()).collect()
    }

    /// The member's latest digest and how long it has been silent.
    pub fn get(&self, node_id: &str) -> Option<(MeshDigest, Duration)> {
        self.get_at(node_id, Instant::now())
    }

    /// The member's latest digest and how long it has gone unheard at `now`.
    pub fn get_at(&self, node_id: &str, now: Instant) -> Option<(MeshDigest, Duration)> {
        let extra = self.forwarded_staleness_floor() - self.staleness_floor;
        self.inner.lock().unwrap().get(node_id).map(|(d, at, fw)| (d.clone(), unheard(d, at, *fw, extra, now)))
    }

    /// Every digest held.
    pub fn all(&self) -> Vec<MeshDigest> {
        self.inner.lock().unwrap().values().map(|(d, _, _)| d.clone()).collect()
    }

    /// How long it has been since `mesh` was last heard on the backbone at `now`: the youngest
    /// receipt of any of its members listed in an aggregate received here. `None` when no member of
    /// the mesh was ever received that way (a mesh known only from forwarded topology, an entry
    /// answer or this node's own mesh has no backbone receipt: topology is not liveness, R-G2).
    pub fn mesh_unheard(&self, mesh: &str, now: Instant) -> Option<Duration> {
        self.inner
            .lock()
            .unwrap()
            .values()
            .filter(|(d, _, heard)| d.node.name.mesh == mesh && *heard == Heard::Forwarded)
            .map(|(_, at, _)| now.saturating_duration_since(*at))
            .min()
    }

    /// The meshes with a backbone receipt held (see [`Self::mesh_unheard`]).
    pub fn backbone_meshes(&self) -> BTreeSet<String> {
        self.inner.lock().unwrap().values().filter(|(_, _, heard)| *heard == Heard::Forwarded).map(|(d, _, _)| d.node.name.mesh.clone()).collect()
    }

    /// Every held member with how long it has gone unheard at `now` (a departed birth is not held).
    pub(crate) fn silent_held(&self, now: Instant) -> Vec<(MeshDigest, Duration)> {
        let extra = self.forwarded_staleness_floor() - self.staleness_floor;
        self.inner.lock().unwrap().values().map(|(d, at, fw)| (d.clone(), unheard(d, at, *fw, extra, now))).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafka_mesh_entity::{EndpointId, IncarnationId, MemberStatus, MeshNode, NodeId};

    fn digest(node_id: &NodeId, incarnation: &IncarnationId, supersedes: Option<IncarnationId>, status: MemberStatus, at: u64) -> MeshDigest {
        MeshDigest {
            fabric_id: FabricId::parse("fab000000001").unwrap(),
            node: MeshNode {
                node_id: node_id.clone(),
                name: "mesh1.rpc.1".parse().unwrap(),
                endpoint_id: EndpointId("key".into()),
                transport_addr: "127.0.0.1:41000".parse().unwrap(),
                incarnation: incarnation.clone(),
                supersedes,
                runtime: None,
            },
            status,
            admin_api_base: None,
            emitted_at_rafka_ms: at,
            digest_seq: at,
            mesh_id: None,
            in_flight: None,
            extra: Default::default(),
            load: None,
            data_dir: None,
        }
    }

    /// CONTRACT (rafka-v2 `register_peer_location_if_fresher`): a restart keeps the key and binds a
    /// fresh port; the newer birth's digest replaces the key's address, and a late digest of the
    /// old birth never puts the old socket back.
    #[test]
    fn a_newer_birth_replaces_its_keys_address_and_an_older_digest_never_restores_it() {
        let key = iroh::SecretKey::generate().public();
        // One logical node and two births of it: the second supersedes the first and starts its
        // own heartbeat sequence again at 1.
        let node_id = NodeId::mint();
        let (first, second) = (IncarnationId::mint(), IncarnationId::mint());
        let at = |port: u16, incarnation: &IncarnationId, supersedes: Option<IncarnationId>, seq: u64| {
            let mut d = digest(&node_id, incarnation, supersedes, MemberStatus::ReadyForTraffic, seq);
            d.node.endpoint_id = EndpointId(key.to_string());
            d.node.transport_addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
            d
        };
        let lookup = MemoryLookup::new();
        let addrs = |l: &MemoryLookup| l.get_endpoint_info(key).map(|e| e.ip_addrs().cloned().collect::<Vec<_>>()).unwrap_or_default();
        register_location(&lookup, &at(41001, &first, None, 40));
        register_location(&lookup, &at(41002, &second, Some(first.clone()), 1));
        assert_eq!(addrs(&lookup), vec![std::net::SocketAddr::from(([127, 0, 0, 1], 41002))], "the newer birth replaces, never joins, the old socket");
        register_location(&lookup, &at(41001, &first, None, 41));
        assert_eq!(addrs(&lookup), vec![std::net::SocketAddr::from(([127, 0, 0, 1], 41002))], "a late digest of the old birth never restores its socket");
    }

    #[test]
    fn a_birth_keeps_its_runtime_and_a_restart_brings_a_new_one() {
        use rafka_mesh_entity::{RuntimeFact, RuntimeLocator, RuntimeProvider};
        let fact = |pid: u32, domain: &str| RuntimeFact {
            deployment_id: "dep".into(),
            provider: RuntimeProvider::Process,
            control_domain: domain.into(),
            locator: RuntimeLocator::Process { pid, start: 7 },
        };
        let book = DigestBook::default();
        let (id, birth) = (NodeId::mint(), IncarnationId::mint());
        let mut first = digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 100);
        first.node.runtime = Some(fact(11, "process:a:1"));
        assert!(book.record(first.clone()));
        // Same birth, another locator or another control domain: refused, the held one stays.
        for other in [fact(12, "process:a:1"), fact(11, "process:b:1")] {
            let mut moved = digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 200);
            moved.node.runtime = Some(other.clone());
            assert!(!book.record(moved.clone()), "{other:?}");
            assert!(!book.record_forwarded(moved));
        }
        assert_eq!(book.get(id.as_str()).unwrap().0.node.runtime, Some(fact(11, "process:a:1")));
        // A restart: a new birth with a new runtime is current; the old birth's fact never returns.
        let next = IncarnationId::mint();
        let mut restarted = digest(&id, &next, Some(birth.clone()), MemberStatus::ReadyForTraffic, 300);
        restarted.node.runtime = Some(fact(13, "process:a:1"));
        assert!(book.record(restarted));
        assert!(!book.record(first), "the superseded birth and its runtime are refused");
        assert_eq!(book.get(id.as_str()).unwrap().0.node.runtime, Some(fact(13, "process:a:1")));
    }

    #[test]
    fn a_late_digest_never_refreshes_a_member_or_reverts_its_status() {
        let book = DigestBook::default();
        let (id, birth) = (NodeId::mint(), IncarnationId::mint());
        let t0 = Instant::now();
        let later = t0 + Duration::from_millis(30);
        assert!(book.record_at(digest(&id, &birth, None, MemberStatus::Leaving, 200), t0));
        assert!(!book.record_at(digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 100), later), "an older digest of the same birth");
        assert!(!book.record_at(digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 200), later), "a duplicate");
        let (held, age) = book.get_at(id.as_str(), later).unwrap();
        assert_eq!(held.status, MemberStatus::Leaving, "the status is not reverted");
        assert_eq!(age, Duration::from_millis(30), "the member's unheard time is not reset");
        assert!(book.record_at(digest(&id, &birth, None, MemberStatus::Leaving, 300), later), "a newer digest is taken");
    }

    /// `f` (a `members` frame) as the one chunk of a one-chunk snapshot.
    fn single_chunk(f: Frame) -> Frame {
        match f {
            Frame::Members { mesh, publisher, forwarded_by, topology_version, published_at_rafka_ms, snapshot_id, digests, in_flight, departed, .. } => {
                Frame::Members { mesh, publisher, forwarded_by, topology_version, published_at_rafka_ms, snapshot_id, chunk_index: 0, chunk_count: 1, digests, in_flight, departed }
            }
            other => other,
        }
    }

    /// One `Members` chunk as a single-chunk snapshot at version 1.
    fn members(mesh: &str, publisher: &str, forwarded_by: Option<&str>, digests: Vec<MeshDigest>, in_flight: Vec<LifecycleOp>, departed: Vec<LifecycleOp>) -> Frame {
        Frame::Members {
            mesh: mesh.into(),
            publisher: PublisherId { node: publisher.into(), incarnation: IncarnationId("birth".into()) },
            forwarded_by: forwarded_by.map(String::from),
            topology_version: 1,
            published_at_rafka_ms: 1,
            snapshot_id: 1,
            chunk_index: u32::MAX,
            chunk_count: u32::MAX,
            digests,
            in_flight,
            departed,
        }
    }

    fn op(id: &NodeId, inc: &IncarnationId, operation: &str) -> LifecycleOp {
        LifecycleOp {
            build_id: "b1".into(),
            attempt: 1,
            operation: operation.into(),
            node_id: id.clone(),
            incarnation: inc.clone(),
            name: "mesh1.rpc.1".parse().unwrap(),
            event_at_rafka_ms: 1,
        }
    }

    #[test]
    fn a_deleting_overlay_keeps_the_node_held_and_not_routable_and_a_departure_removes_it() {
        let book = DigestBook::default();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 100)));
        assert!(book.routable(id.as_str()));
        let o = op(&id, &inc, "retire-node:mesh1.rpc.1");
        assert!(book.deleting(o.clone()));
        assert!(!book.deleting(o.clone()), "the same overlay twice is applied once");
        assert!(book.get(id.as_str()).is_some(), "still held");
        assert!(!book.routable(id.as_str()), "not routable");
        assert_eq!(book.in_flight(), vec![o.clone()]);
        assert!(book.depart(o.clone()));
        assert!(book.get(id.as_str()).is_none(), "removed from the view");
        assert!(book.in_flight().is_empty(), "the overlay cleared with the departure");
        assert_eq!(book.departed(), vec![o.clone()]);
        assert!(!book.depart(o), "a second copy of the departure is a no-op");
    }

    /// CONTRACT (fabric-node-lifecycle.md Restarting, fabric-mesh-ops.md §3 pre-event): a
    /// `NodeRestarting` for the exact birth holds it as restarting and not routable through its
    /// own Leaving; the later birth of the same NodeId (superseding it) is taken and closes the
    /// restart; a repeat of the event after that is refused, and nothing departs.
    #[test]
    fn a_restart_holds_the_birth_until_its_later_birth_is_heard() {
        let book = DigestBook::default();
        let (id, old, new) = (NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &old, None, MemberStatus::ReadyForTraffic, 100)));
        let r = op(&id, &old, "restart-node:mesh1.rpc.1");
        assert!(book.restarting(r.clone()));
        assert!(!book.restarting(r.clone()), "the same event twice is applied once");
        assert!(book.restart_of(id.as_str(), &old).is_some());
        assert!(!book.routable(id.as_str()), "a restarting birth is not routable");
        assert!(book.record(digest(&id, &old, None, MemberStatus::Leaving, 200)), "its own Leaving is taken");
        assert!(book.restart_of(id.as_str(), &old).is_some(), "the Leaving does not end the restart");
        assert!(book.record(digest(&id, &new, Some(old.clone()), MemberStatus::Pending, 300)), "the later birth is taken");
        assert!(book.restart_of(id.as_str(), &old).is_none(), "the later birth closes the restart");
        assert!(book.routable(id.as_str()));
        assert!(!book.restarting(r), "a late copy of the event after the later birth is refused");
        assert!(book.departed().is_empty(), "a restart departs nothing");
    }

    #[test]
    fn a_departed_node_id_never_returns_under_any_incarnation_until_the_retention_passes() {
        let book = DigestBook::with_retention(Duration::from_millis(40));
        let (id, inc, next) = (NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        let t0 = Instant::now();
        book.record_at(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 100), t0);
        book.depart_at(op(&id, &inc, "retire-node:mesh1.rpc.1"), t0);
        assert!(!book.record_at(digest(&id, &inc, None, MemberStatus::Leaving, 900), t0), "the departed birth's late digest");
        assert!(!book.record_forwarded_at(digest(&id, &next, Some(inc.clone()), MemberStatus::ReadyForTraffic, 950), t0), "a later incarnation of the deleted NodeId");
        assert!(book.is_departed_at(id.as_str(), t0));
        let after = t0 + Duration::from_millis(60);
        assert!(!book.is_departed_at(id.as_str(), after), "forgotten after the retention");
        assert!(book.record_at(digest(&id, &next, Some(inc), MemberStatus::ReadyForTraffic, 960), after), "after the retention the id is unknown, not departed");
    }

    #[test]
    fn departures_and_overlays_pack_into_frames_that_fit_and_a_publisher_carries_only_its_mesh() {
        let book = DigestBook::default();
        let mut other = op(&NodeId::mint(), &IncarnationId::mint(), "retire-node:mesh2.rpc.1");
        other.name = "mesh2.rpc.1".parse().unwrap();
        book.depart(other);
        for _ in 0..60 {
            book.depart(op(&NodeId::mint(), &IncarnationId::mint(), "retire-node:mesh1.rpc.1"));
        }
        let (in_flight, departed) = book.overlays_of("mesh1");
        assert!(in_flight.is_empty());
        assert_eq!(departed.len(), 60, "mesh2's departure is not mesh1's to publish");
        let frame = |departed| members("mesh1", "mesh1.admin.1", Some("mesh2.admin.1"), Vec::new(), Vec::new(), departed);
        let runs = pack(departed, frame);
        assert!(runs.len() > 1, "sixty departures do not fit one message");
        assert_eq!(runs.iter().map(Vec::len).sum::<usize>(), 60, "every departure travels");
        for r in &runs {
            assert!(frame(r.clone()).encode().len() <= MAX_FRAME, "a run fits one message");
        }
    }

    #[test]
    fn a_departure_heard_before_the_birth_still_fences_it() {
        let book = DigestBook::default();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(book.depart(op(&id, &inc, "retire-node:mesh1.rpc.1")));
        assert!(!book.record(digest(&id, &inc, None, MemberStatus::ReadyForTraffic, 100)));
        assert!(!book.deleting(op(&id, &inc, "retire-node:mesh1.rpc.1")), "no overlay on a departed node");
    }

    #[test]
    fn members_are_packed_into_frames_that_fit_one_gossip_message() {
        let ds: Vec<MeshDigest> = (0..40).map(|i| digest(&NodeId::mint(), &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, i)).collect();
        let in_flight: Vec<LifecycleOp> = (0..3).map(|_| op(&NodeId::mint(), &IncarnationId::mint(), "retire-node:mesh1.rpc.1")).collect();
        let departed: Vec<LifecycleOp> = (0..5).map(|_| op(&NodeId::mint(), &IncarnationId::mint(), "retire-node:mesh1.rpc.1")).collect();
        let frame = |digests| members("mesh1", "mesh1.admin.1", Some("mesh2.admin.1"), digests, in_flight.clone(), departed.clone());
        let runs = pack(ds.clone(), frame);
        assert!(runs.len() > 1, "forty digests do not fit one message");
        assert_eq!(runs.iter().map(Vec::len).sum::<usize>(), 40, "every digest travels");
        for r in &runs {
            assert!(frame(r.clone()).encode().len() <= MAX_FRAME, "a run fits one message");
        }
    }

    #[test]
    fn digests_carrying_the_largest_runtime_fact_still_pack_within_one_message() {
        use rafka_mesh_entity::runtime::MAX_FACT_BYTES;
        use rafka_mesh_entity::{RuntimeFact, RuntimeLocator, RuntimeProvider};
        // The largest fact a birth may publish: a container id in a long domain.
        let mut fact = RuntimeFact {
            deployment_id: "d".repeat(64),
            provider: RuntimeProvider::Container,
            control_domain: String::new(),
            locator: RuntimeLocator::Container { id: "f".repeat(64) },
        };
        let base = serde_json::to_vec(&fact).unwrap().len();
        fact.control_domain = "c".repeat(MAX_FACT_BYTES - base);
        assert_eq!(fact.validate(), Ok(()), "exactly at the bound");
        let ds: Vec<MeshDigest> = (0..40)
            .map(|i| {
                let mut d = digest(&NodeId::mint(), &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, i);
                d.node.runtime = Some(fact.clone());
                d.data_dir = Some(format!("/var/lib/rafka/{}", "x".repeat(64)));
                d
            })
            .collect();
        let frame = |digests| members("mesh1", "mesh1.admin.1", Some("mesh2.admin.1"), digests, Vec::new(), Vec::new());
        let runs = pack(ds, frame);
        assert_eq!(runs.iter().map(Vec::len).sum::<usize>(), 40, "every digest travels with its runtime");
        for r in &runs {
            assert!(frame(r.clone()).encode().len() <= MAX_FRAME, "a run fits one message");
        }
    }

    /// CONTRACT (R-G2): a member known only from topology forwarded onto the mesh channel is held
    /// and never ages to silent; a forwarded copy of a member already heard leaves when it was
    /// heard alone, so a forwarded word is never liveness.
    #[test]
    fn forwarded_topology_holds_a_member_and_refreshes_no_liveness() {
        let book = DigestBook::default();
        let (topo, heard, birth) = (NodeId::mint(), NodeId::mint(), IncarnationId::mint());
        let t0 = Instant::now();
        assert!(book.record_topology(digest(&topo, &birth, None, MemberStatus::ReadyForTraffic, 10)));
        assert!(book.record_at(digest(&heard, &birth, None, MemberStatus::ReadyForTraffic, 10), t0));
        let far = t0 + Duration::from_secs(3600);
        assert_eq!(book.get_at(topo.as_str(), far).unwrap().1, Duration::ZERO, "topology never ages");
        assert!(book.current_at(book.staleness_floor(), far).iter().any(|d| d.node.node_id == topo), "a topology member stays current");
        let before = book.get_at(heard.as_str(), t0 + Duration::from_secs(10)).unwrap().1;
        assert!(book.record_topology(digest(&heard, &birth, None, MemberStatus::ReadyForTraffic, 10)), "an equal forwarded copy");
        assert!(book.record_topology(digest(&heard, &birth, None, MemberStatus::Draining, 11)), "a newer forwarded copy");
        let after = book.get_at(heard.as_str(), t0 + Duration::from_secs(10)).unwrap().1;
        assert_eq!(before, after, "a forwarded copy does not refresh when the member was heard");
        assert!(after >= Duration::from_secs(10), "the member stays as old as its last direct receipt");
    }

    #[test]
    fn a_forwarded_copy_keeps_a_member_heard_and_never_reverts_it() {
        let book = DigestBook::default();
        let (id, birth) = (NodeId::mint(), IncarnationId::mint());
        let t0 = Instant::now();
        let later = t0 + Duration::from_millis(30);
        assert!(book.record_at(digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 200), t0));
        assert!(book.record_forwarded_at(digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 200), later), "an equal forwarded copy");
        assert_eq!(book.get_at(id.as_str(), later).unwrap().1, Duration::ZERO, "keeps the member heard");
        assert!(!book.record_forwarded_at(digest(&id, &birth, None, MemberStatus::Pending, 100), later), "an older copy is not taken");
    }

    #[test]
    fn a_new_or_changed_birth_ticks_and_a_fresher_digest_of_the_same_birth_does_not() {
        let book = DigestBook::default();
        let mut births = book.birth_changes();
        let (id, first, second) = (NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &first, None, MemberStatus::Pending, 100)));
        assert!(births.has_changed().unwrap(), "first held");
        births.borrow_and_update();
        assert!(book.record(digest(&id, &first, None, MemberStatus::ReadyForTraffic, 200)));
        assert!(!births.has_changed().unwrap(), "the same birth, fresher and with a new status");
        assert!(book.record_forwarded(digest(&id, &second, Some(first.clone()), MemberStatus::Pending, 50)));
        assert!(births.has_changed().unwrap(), "the next birth");
    }

    #[test]
    fn a_successor_birth_is_taken_and_its_predecessors_late_digests_are_not() {
        let book = DigestBook::default();
        let (id, first, second) = (NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &first, None, MemberStatus::ReadyForTraffic, 500)));
        assert!(book.record(digest(&id, &second, Some(first.clone()), MemberStatus::ReadyForTraffic, 100)), "a new birth, whatever its clock");
        assert!(!book.record(digest(&id, &first, None, MemberStatus::ReadyForTraffic, 900)), "the superseded birth's late digest");
        assert_eq!(book.get(id.as_str()).unwrap().0.node.incarnation, second);
    }

    /// A peer mesh's members are heard through its primary's forwarded aggregate. When that primary
    /// is lost its successor takes over only after it hears the loss (the floor) and publishes on
    /// its next round: a forwarded member stays heard through that grace, while a member heard
    /// directly falls silent at the floor (gossip.md §3.2). Small windows here; the defaults are
    /// the documented ones.
    #[test]
    fn a_forwarded_member_stays_heard_through_one_primary_succession() {
        let book = DigestBook::default();
        let (floor, backbone) = (staleness_floor(), backbone_gossip_interval());
        assert_eq!(book.forwarded_staleness_floor(), floor + backbone + floor + backbone);
        let (direct, forwarded) = (NodeId::mint(), NodeId::mint());
        assert!(book.record(digest(&direct, &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, 100)));
        assert!(book.record_forwarded(digest(&forwarded, &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, 100)));
        // Just past the staleness floor: the direct member is unheard, the forwarded one is not.
        let now = Instant::now() + floor + Duration::from_millis(1);
        let heard: Vec<String> = book.current_at(floor, now).into_iter().map(|d| d.node.node_id.to_string()).collect();
        assert!(!heard.contains(&direct.to_string()), "a member heard directly is unheard past the staleness floor");
        assert!(heard.contains(&forwarded.to_string()), "a forwarded member is still heard while its mesh's next primary takes over");
        // Past the forwarded staleness floor it is unheard too.
        let later = Instant::now() + book.forwarded_staleness_floor() + Duration::from_millis(1);
        assert!(book.current_at(floor, later).is_empty());
    }

    /// A peer mesh's member that said `Leaving` is not succeeded: its forwarded copy earns no
    /// extra, so it leaves every view one gossip interval after it stops, like a direct one.
    #[test]
    fn a_forwarded_leaving_member_earns_no_extra() {
        let book = DigestBook::default();
        let id = NodeId::mint();
        let t0 = Instant::now();
        assert!(book.record_forwarded_at(digest(&id, &IncarnationId::mint(), None, MemberStatus::Leaving, 100), t0));
        let later = t0 + Duration::from_secs(3);
        assert_eq!(book.get_at(id.as_str(), later).unwrap().1, Duration::from_secs(3), "measured from when it was heard");
    }

    /// A graceful departure is not current one gossip interval after its last `Leaving`, whether
    /// it was heard directly or forwarded: no aggregate carries a departed member on.
    #[test]
    fn a_departed_member_is_not_current_one_gossip_interval_after_its_leaving() {
        let book = DigestBook::default();
        let (direct, forwarded) = (NodeId::mint(), NodeId::mint());
        let t0 = Instant::now();
        book.record_at(digest(&direct, &IncarnationId::mint(), None, MemberStatus::Leaving, 100), t0);
        book.record_forwarded_at(digest(&forwarded, &IncarnationId::mint(), None, MemberStatus::Leaving, 100), t0);
        assert_eq!(book.current_at(book.staleness_floor(), t0 + gossip_interval()).len(), 2, "within the tick it is still saying Leaving");
        assert!(book.current_at(book.staleness_floor(), t0 + gossip_interval() + Duration::from_millis(1)).is_empty());
    }

    /// The documented defaults: a 30 s staleness floor and 2 s gossip intervals.
    #[test]
    fn the_windows_default_to_the_documented_values() {
        if std::env::var("RDM_STALENESS_MS").is_err() {
            assert_eq!(staleness_floor(), Duration::from_secs(30));
        }
        if std::env::var("RDM_GOSSIP_INTERVAL_MS").is_err() {
            assert_eq!(gossip_interval(), Duration::from_secs(2));
        }
        if std::env::var("RDM_BACKBONE_INTERVAL_MS").is_err() {
            assert_eq!(backbone_gossip_interval(), Duration::from_secs(2));
        }
    }

    /// A member of this node's own mesh is heard directly on the mesh channel. An aggregate carrying
    /// it as well (its mesh primary's backbone `Members`) is not "heard only through forwarded
    /// topology" (gossip.md §3.2), so it never earns the forwarded grace: the node's own primary
    /// falls silent to its successor at the floor, before a peer mesh's grace for the same members
    /// runs out. Found by the seeded soak, seed 7 round 5: the successor held its own dead primary
    /// as forwarded and noticed the loss together with the peer mesh.
    #[test]
    fn an_own_mesh_member_in_an_aggregate_is_still_heard_directly() {
        let view = View::new("mesh1", "mesh1.admin.1");
        let fabric = FabricId::parse("fab000000001").unwrap();
        let id = NodeId::mint();
        let d = digest(&id, &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, 100);
        assert!(view.book.record(d.clone()));
        let aggregate = members("mesh1", "mesh1.admin.1", None, vec![d], vec![], vec![]);
        let _ = view.take_snapshot(&single_chunk(aggregate), &fabric, Side::Backbone);
        let (_, at, forwarded) = view.book.inner.lock().unwrap().get(id.as_str()).cloned().unwrap();
        assert!(forwarded == Heard::Direct, "its own mesh's member is held as heard directly, never forwarded");
        let _ = at;
        // A peer mesh's member in the same kind of aggregate is forwarded.
        let peer = NodeId::mint();
        let mut p = digest(&peer, &IncarnationId::mint(), None, MemberStatus::ReadyForTraffic, 100);
        p.node.name = "mesh2.rpc.1".parse().unwrap();
        let aggregate = members("mesh2", "mesh2.admin.1", None, vec![p], vec![], vec![]);
        let _ = view.take_snapshot(&single_chunk(aggregate), &fabric, Side::Backbone);
        assert!(view.book.inner.lock().unwrap().get(peer.as_str()).unwrap().2 == Heard::Forwarded, "a peer mesh's member is forwarded");
    }

    /// CONTRACT: the version a node holds of a source Mesh names a projection its book already holds. A
    /// reader that sees version `k` and then reads the book finds the change of version `k` (or a
    /// later one), never an older one: a delta moves the version and the book together.
    #[test]
    fn a_held_source_version_never_runs_ahead_of_the_book_it_names() {
        let view = View::new("mesh1", "mesh1.rpc.1");
        let fabric = FabricId::parse("fab000000001").unwrap();
        let id = NodeId::mint();
        let incarnation = IncarnationId::mint();
        let at = |n: u64| {
            let mut d = digest(&id, &incarnation, None, MemberStatus::ReadyForTraffic, n);
            d.node.name = "mesh2.rpc.1".parse().unwrap();
            d
        };
        let publisher = PublisherId { node: "mesh2.admin.1".into(), incarnation: IncarnationId("birth".into()) };
        let first = Frame::Members {
            mesh: "mesh2".into(),
            publisher: publisher.clone(),
            forwarded_by: Some("mesh1.admin.1".into()),
            topology_version: 1,
            published_at_rafka_ms: 1,
            snapshot_id: 1,
            chunk_index: 0,
            chunk_count: 1,
            digests: vec![at(1)],
            in_flight: vec![],
            departed: vec![],
        };
        let _ = view.take_snapshot(&first, &fabric, Side::MeshChannel);
        const LAST: u64 = 20_000;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader = {
            let (view, id, stop) = (view.clone(), id.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut behind = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let Some((_, version)) = view.mesh_rx.lock().unwrap().held_version("mesh2") else { continue };
                    let held = view.book.get(id.as_str()).map(|(d, _)| d.digest_seq).unwrap_or(0);
                    if held < version {
                        behind.push((version, held));
                    }
                }
                behind
            })
        };
        for v in 2..=LAST {
            let delta = Frame::MembersDelta {
                mesh: "mesh2".into(),
                source_publisher: publisher.clone(),
                base_version: v - 1,
                topology_version: v,
                published_at_rafka_ms: v,
                changed: vec![at(v)],
                removed: vec![],
                in_flight: vec![],
                departed: vec![],
            };
            let _ = view.take_snapshot(&delta, &fabric, Side::MeshChannel);
        }
        stop.store(true, Ordering::Relaxed);
        let behind = reader.join().unwrap();
        assert!(behind.is_empty(), "a reader saw a version whose change the book did not hold yet: {} times, first (version, held) {:?}", behind.len(), behind.first());
    }

    /// The first `Leaving` is lost; one of the later announcements in the
    /// linger still reaches the fabric.
    #[tokio::test]
    async fn a_lost_first_leaving_is_followed_by_one_that_arrives() {
        let said = Arc::new(Mutex::new(0u32));
        let heard = Arc::new(Mutex::new(0u32));
        let (s, h) = (said.clone(), heard.clone());
        announce_leaving(Duration::from_millis(300), Duration::from_millis(50), move || {
            let (s, h) = (s.clone(), h.clone());
            async move {
                let n = {
                    let mut s = s.lock().unwrap();
                    *s += 1;
                    *s
                };
                if n > 1 {
                    *h.lock().unwrap() += 1;
                }
            }
        })
        .await;
        assert!(*heard.lock().unwrap() >= 1, "only the lost first Leaving was said ({} said)", said.lock().unwrap());
    }

    /// Announcing never keeps the node past its linger, even when an
    /// announcement never finishes.
    #[tokio::test]
    async fn announcing_never_outlives_the_linger() {
        let linger = Duration::from_millis(300);
        let start = Instant::now();
        let n = announce_leaving(linger, Duration::from_millis(50), || std::future::pending::<()>()).await;
        let took = start.elapsed();
        assert!(took < linger + Duration::from_millis(150), "a stuck announcement held the node {took:?}");
        assert_eq!(n, 0, "no announcement finished");

        let start = Instant::now();
        let n = announce_leaving(linger, Duration::from_millis(50), || async {}).await;
        let took = start.elapsed();
        assert!(took < linger + Duration::from_millis(150), "repeating held the node {took:?}");
        assert!((2..=8).contains(&n), "{n} announcements in a 300 ms linger at 50 ms");
    }

    /// CONTRACT (i143 export gate: a failed rejoin is bounded and never marks Dead): a channel left
    /// alone is refed on a doubling schedule that never waits more than one staleness floor and
    /// reaches it; the schedule is the refeed's only state, so no failed rejoin touches a member.
    #[test]
    fn a_failed_rejoin_is_retried_on_a_bounded_schedule() {
        let floor = staleness_floor();
        let mut wait = backbone_gossip_interval();
        let mut waits = vec![wait];
        for _ in 0..64 {
            wait = refeed_backoff(wait);
            waits.push(wait);
        }
        assert!(waits.windows(2).all(|w| w[1] >= w[0]), "never shrinks while alone: {waits:?}");
        assert!(waits.iter().all(|w| *w <= floor), "never waits past one staleness floor: {waits:?}");
        assert_eq!(*waits.last().unwrap(), floor, "settles at one floor, retried forever");
    }

    #[test]
    fn a_peer_that_leaves_the_live_list_and_returns_is_joined_again() {
        let joined = Mutex::new(BTreeSet::new());
        let (a, b) = (iroh::SecretKey::generate().public(), iroh::SecretKey::generate().public());
        assert_eq!(rejoin(&joined, &[a, b]), vec![a, b], "both joined");
        assert!(rejoin(&joined, &[a, b]).is_empty(), "a live peer is joined once");
        assert!(rejoin(&joined, &[b]).is_empty(), "a leaves the live list");
        assert_eq!(rejoin(&joined, &[a, b]), vec![a], "a returns and is joined again");
    }

    #[test]
    fn a_healed_view_authorizes_nothing_until_every_member_could_be_heard_again() {
        let t0 = Instant::now();
        let floor = Duration::from_secs(3);
        let mut c = CutOff::with_floor(floor);
        assert!(c.authorizes(t0), "never cut off");
        assert_eq!(c.observe(true, t0), Some("start"));
        assert!(!c.authorizes(t0), "cut off");
        let healed = t0 + Duration::from_secs(10);
        assert_eq!(c.observe(false, healed), Some("stop"));
        assert!(!c.authorizes(healed + Duration::from_millis(250)), "healed, but the view still holds lost members as silent");
        assert!(!c.authorizes(healed + floor - Duration::from_millis(1)));
        assert!(c.authorizes(healed + floor), "every live member has published again");
        assert_eq!(c.observe(false, healed + floor), None);
    }

    /// CONTRACT (i143 R-J1 follow-up): a member whose restart is open is held through it, but its
    /// address is the address of a birth that is going or gone: the repair refeed never hands it
    /// to the channel again, however long the member has been silent. The later birth closes the
    /// restart and the member is a target again at its new address.
    #[test]
    fn a_member_under_an_open_restart_is_never_a_repair_target() {
        let book = DigestBook::default();
        let (id, old, new) = (NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        let key = iroh::SecretKey::generate().public().to_string();
        let long_ago = Instant::now().checked_sub(Duration::from_secs(600)).expect("the clock has run ten minutes");
        let mut d = digest(&id, &old, None, MemberStatus::ReadyForTraffic, 100);
        d.node.endpoint_id = EndpointId(key.clone());
        assert!(book.record_at(d.clone(), long_ago));
        assert_eq!(held_targets(&book, |_| true).len(), 1, "a silent held member is a repair target");
        assert!(book.restarting(op(&id, &old, "restart-node:mesh1.rpc.1")));
        assert!(held_targets(&book, |_| true).is_empty(), "its restart is open: its old address is never handed to the channel");
        let mut next = digest(&id, &new, Some(old.clone()), MemberStatus::Pending, 300);
        next.node.endpoint_id = EndpointId(key);
        next.node.transport_addr = "127.0.0.1:41999".parse().unwrap();
        assert!(book.record_at(next, long_ago));
        let targets = held_targets(&book, |_| true);
        assert_eq!(targets.len(), 1, "the later birth closes the restart");
        assert_eq!(targets[0].addr.ip_addrs().next().map(|a| a.port()), Some(41999), "and is a target at its own address");
    }
}
